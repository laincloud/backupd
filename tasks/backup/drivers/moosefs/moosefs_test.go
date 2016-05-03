package moosefs

import (
	"fmt"
	"os"
	"testing"
)

func init() {
	moosefsDir = "/tmp/mfs"
	os.Mkdir(moosefsDir, os.ModePerm)
}

func TestUpload(t *testing.T) {
	driver := &MoosefsDriver{}
	if err := driver.Upload("/etc/bashrc"); err != nil {
		t.Error(err)
	}
}

func TestDownload(t *testing.T) {
	driver := &MoosefsDriver{}
	if err := driver.Download("bashrc", "/tmp/basrch_from_moosefs"); err != nil {
		t.Error(err)
	}
}

func TestList(t *testing.T) {
	driver := &MoosefsDriver{}
	l, err := driver.List()
	if err != nil {
		t.Error(err)
	}
	fmt.Println(l)
}

func TestDelete(t *testing.T) {
	driver := &MoosefsDriver{}
	if err := driver.Delete("bashrc"); err != nil {
		t.Error(err)
	}
}

func TestRsync(t *testing.T) {
	driver := &MoosefsDriver{}
	if err := driver.Rsync("/etc", "/etc"); err != nil {
		t.Error(err)
	}
}
