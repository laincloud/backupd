package backup

import (
	"bytes"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io"
	"io/ioutil"
	"github.com/laincloud/backupd/crond"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	metaFile       = ".meta"
	MODE_FULL      = "full"
	MODE_INCREMENT = "increment"

	StateBackuping  = "backuping"
	StateRecovering = "recovering"
	StateFree       = "free"
)

var (
	stopLock      sync.Mutex
	drivers       map[string]Storage
	driverRunning Storage = nil
	ip            string  // server ip
	meta          *Meta
	namespace     string
	bstats        *BackupStats
)

func init() {
	drivers = make(map[string]Storage)
	bstats = &BackupStats{
		stats: make(map[string]string),
	}
}

type BackupStats struct {
	stats map[string]string
	lock  sync.Mutex
}

func (b *BackupStats) Set(path, stat string) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if s, ok := b.stats[path]; ok && s != StateFree {
		return fmt.Errorf("Directory is now %s, give up.", s)
	}
	b.stats[path] = stat
	return nil
}

func (b *BackupStats) Free(path string) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.stats[path] = StateFree
}

type Storage interface {
	// the storage's name
	Name() string

	Upload(reader io.Reader, dest string) error

	Download(writer io.Writer, src string) error

	List(dir string) ([]os.FileInfo, error)

	Delete(file string) error

	FileInfo(name string) (os.FileInfo, error)

	Rsync(src, dest string) error
}

// A Entity is a backup
type Entity struct {
	Mode       string    `json:"mode"`
	Source     string    `json:"source"`
	Volume     string    `json:"volume"`
	Name       string    `json:"name"`
	Server     string    `json:"server"`
	Size       uint64    `json:"size"`
	Created    time.Time `json:"created"`
	workDir    string    `json:"-"`
	Containers []string  `json:"containers"`
	InstanceNo int       `json:"instanceNo"`
}

func NewEntity(src, archive string, instanceNo int, containers []string, volume string, mode string) *Entity {
	now := time.Now()
	if archive == "" {
		archive = strings.Replace(src, "/", "_", -1)
	}
	ret := &Entity{
		Mode:       mode,
		Source:     src,
		Volume:     volume,
		Name:       fmt.Sprintf("%s-%d.tar.gz", archive, time.Now().Unix()),
		Server:     ip,
		Created:    now,
		workDir:    path.Dir(path.Clean(src)),
		Containers: containers,
		InstanceNo: instanceNo,
	}
	if mode == MODE_INCREMENT { // it is a directory, not a tar file
		ret.Name = archive
	}
	return ret
}

func (ent *Entity) IncrementRecover(files []string) error {
	var (
		fileList, tmp []string
		err           error
		pathBase      string = path.Join(namespace, ent.Name)
	)

	if len(files) == 1 && files[0] == "*" {
		fileList, err = findAllFiles(pathBase, "*", driverRunning)
		if err != nil {
			return err
		}

	} else {
		for _, f := range files {
			tmp, err = findAllFiles(pathBase, f, driverRunning)
			if err != nil {
				return err
			}
			fileList = append(fileList, tmp...)
		}
	}
	for _, file := range fileList {
		destFile := path.Join(ent.Source, file[len(pathBase):])
		finfo, err := driverRunning.FileInfo(file)
		if err != nil {
			return err
		}
		os.MkdirAll(path.Dir(destFile), 0666) // create directory, ignore the errors
		fhandle, err := os.Create(destFile)
		if err != nil {
			return err
		}
		if err := driverRunning.Download(fhandle, file); err != nil {
			return err
		}
		fhandle.Chmod(finfo.Mode())
		os.Chtimes(destFile, time.Now(), finfo.ModTime())
		fhandle.Sync()
		fhandle.Close()
	}
	return nil
}

func (ent *Entity) Recover(driver Storage, ns, file string) error {

	var (
		downloadError chan error = make(chan error)
		output        bytes.Buffer
	)
	ent.workDir = path.Dir(ent.Source)

	// create a recovering directory to extract the backup file
	recoverDir := ent.Source + ".recovering"
	if fileExist(recoverDir) {
		if err := os.RemoveAll(recoverDir); err != nil {
			return err
		}
	}
	if err := os.Mkdir(recoverDir, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(recoverDir) // remove source.recovering/

	cmd := exec.Command("tar", "-zxf", "-", "-C", recoverDir)
	cmd.Stdout = &output
	cmd.Stderr = &output
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Errorf("Fail to get the stdin pipe of extract command, %s", err.Error())
		return err
	}

	go func() {
		defer stdin.Close()
		downloadError <- driver.Download(stdin, path.Join(ns, ent.Name))
	}()

	log.Debugf("Running command %s", cmd.Args)
	if err := cmd.Start(); err != nil {
		log.Errorf("Fail to run recover command: %s, %s", cmd.Args, err.Error())
		os.RemoveAll(recoverDir)
		return err
	}

	// same with Backup(), cmd.Wait() will close the stdin pipe, we must waiting for download action
	if err := <-downloadError; err != nil {
		log.Errorf("Fail to download backup file %s, %s", ent.Name, err.Error())
		return err
	}

	if err := cmd.Wait(); err != nil {
		log.Errorf("Command run failed, %s, \nOutput:%s", err.Error(), output.String())
		return err
	}

	// begin to mv data
	backDir := ent.Source + ".bak"
	if fileExist(backDir) {
		if err := os.RemoveAll(backDir); err != nil { // remove source.bak/
			return err
		}
	}
	defer os.RemoveAll(backDir)                           // remove source.bak/
	if err := cloneDir(ent.Source, backDir); err != nil { // copy source/* => source.bak/
		return err
	}
	if err := cloneDir(path.Join(recoverDir, path.Base(ent.Source)), ent.Source); err != nil {
		log.Debugf("Recover action failed, now recover %s from %s", ent.Source, backDir)
		err = cloneDir(backDir, ent.Source) // copy source.bak/* => source/
		if err != nil {
			// fail to rsync direcory, rename directly
			// rename will cause volume disappeared in container, so container must restart
			if err := os.Rename(backDir, ent.Source); err != nil {
				// if os.Rename failed again? God can't help you, too. check your filesystem
				log.Errorf("Fail to rename %s to %s, %s, this is a fatal error, please check your server's filesystem", backDir, ent.Source, err.Error())
				return err
			}
		}
	}
	return nil
}

func (ent *Entity) IncrementBackup() error {
	if err := driverRunning.Rsync(ent.Source, path.Join(namespace, ent.Name)); err != nil {
		log.Errorf("Fail to rsync %s to backends, %s", ent.Source, err.Error())
		return err
	}
	meta.Set(ent.Source+"@increment", []Entity{*ent})
	if err := meta.Sync(); err != nil {
		log.Errorf("Fail to sync meta file to backends, %s", err.Error())
		return err
	}
	log.Debugf("Success increment backup task for %s", ent.Source)
	return nil
}

func (ent *Entity) Backup(driver Storage) error {

	var (
		tarFile     string      = path.Join(ent.workDir, ent.Name)
		uploadError chan error  = make(chan error, 1)
		stderr      chan string = make(chan string, 1)
		destFile    string      = path.Join(namespace, path.Base(tarFile))
	)

	cmd := exec.Command("tar", "-Szcf", "-", path.Base(ent.Source))
	cmd.Dir = ent.workDir
	stdoutPipe, err := cmd.StdoutPipe() // stdout pipe
	if err != nil {
		return err
	}
	stderrPipe, err := cmd.StderrPipe() // stderr pipe
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		log.Errorf("Fail to run archive command: %s, %s", cmd.Args, err.Error())
		return err
	}

	go func() {
		uploadError <- driver.Upload(stdoutPipe, destFile)
	}()
	go func() {
		errOutput, err := ioutil.ReadAll(stderrPipe)
		if err != nil {
			log.Errorf("Fail to read stderr from command: %s, %s", cmd.Args, err.Error())
		}
		stderr <- string(errOutput)
	}()

	// this must run before cmd.Wait(), Wait() will close stdoutPipe,
	// so we must wait for upload finished,
	// otherwise upload will get bad file descripter error because of stdoutPipe was closed by Wait()
	if err := <-uploadError; err != nil {
		log.Errorf("Fail to upload tarball, %s", err.Error())
		return err
	}

	if err := cmd.Wait(); err != nil {
		log.Errorf("Fail to run command %s. %s\n stderr: %s", cmd.Args, err.Error(), <-stderr)
		return err
	}
	if info, err := driver.FileInfo(destFile); err != nil {
		ent.Size = 0
	} else {
		ent.Size = uint64(info.Size())
	}

	// update meta data, and sync it onto backend storage
	meta.Add(*ent)
	if err := meta.Sync(); err != nil {
		meta.Delete(ent.Name)
	}
	log.Debugf("Succeed the backup task for %s", ent.Source)
	return nil
}

func Delete(name string) error {
	// update meta
	meta.Delete(name)
	if err := meta.Sync(); err != nil {
		return err
	}
	log.Infof("Deleting %s", name)
	if err := driverRunning.Delete(path.Join(namespace, name)); err != nil {
		log.Errorf("Fail to delete backup file in backend:%s", err.Error())
		// not return error, this is a idempotent action
		// we think it's not exist as long as it not exist in meta, no matter it's existence in backend
	}
	return nil
}

func List(dir ...string) ([]Entity, error) {
	log.Infof("Getting backup list for %v", dir)
	return meta.Array(dir...), nil
}

func Info(name string) (Entity, error) {
	data := meta.Array()
	for _, item := range data {
		if item.Name == name {
			return item, nil
		}
	}
	return Entity{}, fmt.Errorf("backup named %s not found", name)
}

func FileList(name string) ([]os.FileInfo, error) {
	log.Infof("Getting file list of %s", name)
	flist, err := driverRunning.List(path.Join(namespace, name))
	if err != nil {
		// do not return the full name of path
		if e, ok := err.(*os.PathError); ok {
			e.Path = name
			return nil, e
		}
		return nil, err
	}
	return flist, nil
}

// release the backup
func Release() {
	// meta.Sync() will use lock, so we must get the lock before stop
	stopLock.Lock()
	defer stopLock.Unlock()
}

// Storage driver register
func Register(driver Storage) {
	if driver == nil {
		panic(fmt.Sprintf("Backup driver[%s] can not be a nil value", driver.Name()))
	}
	name := driver.Name()
	if _, ok := drivers[name]; ok {
		panic(fmt.Sprintf("Backup driver named %s already exist", driver.Name()))
	}
	drivers[name] = driver
}

// backup task initialize
func Init(localip, driver string) {
	ip = localip
	namespace = ip
	crond.Register("backup", backup)
	crond.Register("backup_expire", expire)
	crond.Register("backup_recover", backup_recover)

	if driverRunning == nil {
		ok, name := false, driver
		driverRunning, ok = drivers[name]
		if !ok {
			panic(fmt.Sprintf("Backup driver \"%s\" not exist", name))
		}
	}

	meta = NewMeta(driverRunning, namespace)
	if err := meta.LoadFromBackend(); err != nil {
		log.Warnf("Fail to load meta data: %s", err.Error())
	}
}
