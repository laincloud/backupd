package controller

import (
	"fmt"
	"testing"
)

func init() {
	config.BackendPort = 9002
}

func TestGetBackup(t *testing.T) {
	ctl := NewController("webrouter", NewLainlet("192.168.77.21:9000"))
	backups, err := ctl.GetBackup("webrouter.worker.worker", "/etc/nginx/upstreams", "/etc/nginx/conf.d")
	if err != nil {
		t.Error(err)
	}
	for _, backup := range backups {
		fmt.Printf("%+v\n", backup)
	}
}
