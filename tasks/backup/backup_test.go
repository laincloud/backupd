package backup

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"
)

var (
	testEntity          *Entity
	rootDir             = "/data/backup"
	testDir             = "/etc"
	fullRecoverDir      = "/data/etc"
	incrementRecoverDir = "/data/etc-increment"
)

// LocalDriver is a test Storage Driver
type LocalDriver struct{}

func (driver *LocalDriver) Name() string {
	return "local"
}

func (driver *LocalDriver) Upload(reader io.Reader, dest string) error {
	dest = path.Join(rootDir, dest)
	if err := os.MkdirAll(path.Dir(dest), 0666); err != nil {
		return err
	}

	out, err := os.Create(dest)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, reader); err != nil {
		return err
	}

	if err := out.Sync(); err != nil {
		return err
	}

	if err := out.Close(); err != nil {
		return err
	}

	return nil
}

func (driver *LocalDriver) Download(writer io.Writer, src string) error {
	src = path.Join(rootDir, src)
	in, err := os.Open(src)
	if err != nil {
		return err
	}

	if _, err := io.Copy(writer, in); err != nil {
		return err
	}

	if err := in.Close(); err != nil {
		return err
	}

	return nil
}

func (driver *LocalDriver) List(dir string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(path.Join(rootDir, dir))
}

func (driver *LocalDriver) Delete(file string) error {
	return os.Remove(path.Join(rootDir, file))
}

func (driver *LocalDriver) FileInfo(name string) (os.FileInfo, error) {
	file := path.Join(rootDir, name)
	return os.Stat(file)
}

func (driver *LocalDriver) Rsync(src, dest string) error {
	src, dest = path.Join(src, "*"), path.Join(rootDir, dest)
	cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("rsync -az --safe-links %s %s", src, dest))
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.New(err.Error() + ", Output:" + string(output))
	}
	return nil
}

func copyFile(src string, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}

	out, err := os.Create(dest)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		return err
	}

	if err := out.Sync(); err != nil {
		return err
	}

	if err := in.Close(); err != nil {
		return err
	}

	if err := out.Close(); err != nil {
		return err
	}

	return nil
}

func init() {
	os.Setenv("BACKUPD_BACKUP_DRIVER", "local")
	os.Setenv("BACKUPD_IP", "192.168.77.10")
	namespace = config.Get().IP
	Register(&LocalDriver{})

	if err := os.MkdirAll(rootDir, 0666); err != nil {
		panic(err)
	}

	TestRelease(nil)
}

func TestNewEntity(t *testing.T) {
	Init()
	testEntity = NewEntity(testDir, "etc-bak", 0, []string{}, testDir, MODE_FULL)
}

func ExampleDurationParser() {
	testItems := []string{
		"3m", "23h", "2d",
	}

	for _, item := range testItems {
		tm, err := durationParser(item)
		if err != nil {
			panic(err)
		}
		fmt.Println(tm)
	}
	if _, err := durationParser("234a"); err != nil {
		fmt.Println("error")
	}
	if _, err := durationParser("23aa"); err != nil {
		fmt.Println("error")
	}
	// Output:
	// 3m0s
	// 23h0m0s
	// 48h0m0s
	// error
	// error
}

func TestBackup(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Error(r)
		}
	}()
	backup(map[string]interface{}{
		"path": testDir,
	})
}

func TestExpire(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Error(r)
		}
	}()

	expire(map[string]interface{}{
		"info": []string{testDir, "3m"},
	})
}

func TestRecover(t *testing.T) {
	his, err := driverRunning.List(namespace)
	if err != nil {
		t.Error(err)
	}
	f := ""
	for _, item := range his {
		if item.Name() != ".meta" {
			f = item.Name()
			break
		}
	}
	if f == "" {
		t.Error("No backup file to recover")
	}
	ent := meta.Get(f)
	if ent == nil {
		t.Error("backup not exist")
	}
	if err := os.Mkdir(fullRecoverDir, 0666); err != nil {
		t.Error(err)
	}
	ent.Source = fullRecoverDir
	if err := ent.Recover(driverRunning, namespace, f); err != nil {
		t.Error(err)
	}
	if !fileExist(fullRecoverDir + "/issue") {
		t.Error("recover failed, " + fullRecoverDir + "/issue not exist")
	}
}

func TestIncrementBackup(t *testing.T) {
	testEntity = NewEntity(testDir, "etc-increment-bak", 0, []string{}, testDir, MODE_INCREMENT)
	assert.Equal(t, testEntity.Name, "etc-increment-bak")

	if err := testEntity.IncrementBackup(); err != nil {
		t.Error(err)
	}

}
func TestFindAllFiles(t *testing.T) {
	data, err := findAllFiles(path.Join(namespace, "etc-increment-bak"), "*", driverRunning)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 10 && i < len(data); i++ {
		t.Log(data[i])
	}
}

func TestIncrementBackupFilelist(t *testing.T) {
	l, err := FileList("etc-increment-bak")
	if err != nil {
		t.Error(err)
	}
	t.Log(len(l))
}

func TestIncremenRecover(t *testing.T) {
	testEntity.Source = "/data/etc-increment"
	checkList := []string{"sudo.conf", "fstab", "filesystems", "ssh/ssh_config"}
	if err := testEntity.IncrementRecover([]string{"sudo.conf", "fstab", "filesystems", "ssh"}); err != nil {
		t.Error(err)
	}
	for _, item := range checkList {
		if !fileExist(testEntity.Source + "/" + item) {
			t.Errorf("%s not exist", item)
		}
	}
}

func TestRelease(t *testing.T) {
	os.RemoveAll(rootDir)
	os.RemoveAll(fullRecoverDir)
	os.RemoveAll(incrementRecoverDir)
}
