package cli

import (
	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/laincloud/backupd/api"
	"github.com/laincloud/backupd/crond"
	"github.com/laincloud/backupd/tasks/backup"
	"github.com/laincloud/backupd/tasks/backup/drivers/moosefs"
	_ "github.com/laincloud/backupd/tasks/test"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

func daemonMain(c *cli.Context) {

	log.Infof("Initialize and Start crond service...")
	crond.Init(c.String("ip")) // crond initialization
	crond.Start()              // start default crond server

	// all backup driver should init before backup package
	// backup driver moosefs, initialization
	log.Infof("Initialize moosefs-backup-driver...")
	if err := moosefs.Init(c.String("backup-moosefs-dir")); err != nil {
		panic(err)
	}
	log.Infof("Initialize backup-crond-task...")
	backup.Init(c.String("ip"), c.String("backup-driver")) // backup task init

	log.Infof("Run API server...")
	go api.Serve(c.String("addr"))

	sigCh := make(chan os.Signal)
	signal.Notify(sigCh)
	for sig := range sigCh {
		switch sig {
		case syscall.SIGKILL, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM:
			log.Infof("Get exit signall: %s", sig.String())
			log.Infof("Stop crond service")
			crond.Stop()
			log.Infof("Release backup data")
			backup.Release()
			//TODO checking if having task is running
			for {
				if n := atomic.LoadInt32(&crond.RunningCount); n > 0 {
					log.Debugf("Crond having %d tasks is still running ,wait...", n)
					time.Sleep(time.Second * 2)
				} else {
					break
				}
			}
			log.Infof("Exit")
			return
		}
	}
}
