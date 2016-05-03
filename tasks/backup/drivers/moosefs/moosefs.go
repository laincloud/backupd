package moosefs

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"github.com/laincloud/backupd/tasks/backup"
	"os"
	"os/exec"
	"path"
	"strings"
)

var (
	moosefsDir string
)

func checkMFS() error {
	cmd := exec.Command("mfsdirinfo", moosefsDir)
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.New(err.Error() + ";" + string(output))
	}
	return nil
}

func Init(dir string) error {
	moosefsDir = dir
	if moosefsDir == "" { // moosefs not be configed
		return errors.New("moosefs directory is empty")
	}
	if err := checkMFS(); err != nil {
		return err
	}
	info, err := os.Stat(moosefsDir)
	if err != nil { // directory not exist, create it
		if err := os.MkdirAll(moosefsDir, 0666); err != nil {
			return fmt.Errorf("Fail to create dir %s on moosefs:%s", moosefsDir, err.Error())
		}
	} else if !info.IsDir() { // is it not a direcotry?
		return fmt.Errorf("%s on moosefs already exist, but it's not a directory", moosefsDir)
	}
	backup.Register(&MoosefsDriver{})
	return nil
}
func errorFilter(err error) error {
	if perr, ok := err.(*os.PathError); ok {
		fields := strings.Split(perr.Path, "/")
		if len(fields) > 5 {
			fields = fields[5:]
		} else if len(fields) > 4 {
			fields = fields[4:]
		}
		perr.Path = path.Join(fields...)
		return perr
	}
	return err
}

type MoosefsDriver struct{}

func (driver *MoosefsDriver) Name() string {
	return "moosefs"
}

func (driver *MoosefsDriver) Upload(reader io.Reader, dest string) error {
	if err := checkMFS(); err != nil {
		return err
	}
	dest = path.Join(moosefsDir, dest)
	if err := os.MkdirAll(path.Dir(dest), 0666); err != nil {
		return err
	}

	out, err := os.Create(dest)
	if err != nil {
		return errorFilter(err)
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

func (driver *MoosefsDriver) Download(writer io.Writer, src string) error {
	if err := checkMFS(); err != nil {
		return err
	}
	src = path.Join(moosefsDir, src)
	in, err := os.Open(src)
	if err != nil {
		return errorFilter(err)
	}

	if _, err := io.Copy(writer, in); err != nil {
		return err
	}

	if err := in.Close(); err != nil {
		return err
	}

	return nil

}

func (driver *MoosefsDriver) List(dir string) ([]os.FileInfo, error) {
	if err := checkMFS(); err != nil {
		return nil, err
	}
	if ret, err := ioutil.ReadDir(path.Join(moosefsDir, dir)); err != nil {
		return ret, errorFilter(err)
	} else {
		return ret, nil
	}
}

func (driver *MoosefsDriver) Delete(file string) error {
	if err := checkMFS(); err != nil {
		return err
	}
	if err := os.RemoveAll(path.Join(moosefsDir, file)); err != nil {
		return errorFilter(err)
	}
	return nil
}

func (driver *MoosefsDriver) Rsync(src, dest string) error {
	if err := checkMFS(); err != nil {
		return err
	}
	src, dest = src+"/", path.Join(moosefsDir, dest)+"/"
	if err := os.MkdirAll(path.Dir(dest), 0666); err != nil {
		return err
	}
	cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("rsync -az --safe-links %s %s", src, dest))
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.New(err.Error() + ", Output:" + string(output))
	}
	return nil
}

func (driver *MoosefsDriver) FileInfo(name string) (os.FileInfo, error) {
	if err := checkMFS(); err != nil {
		return nil, err
	}
	file := path.Join(moosefsDir, name)
	if info, err := os.Stat(file); err != nil {
		return info, errorFilter(err)
	} else {
		return info, nil
	}
}
