package cli

import (
	"github.com/codegangsta/cli"
	"github.com/laincloud/backupd/controller"
	"github.com/laincloud/backupd/controller/records"
)

func controllerMain(c *cli.Context) {
	if err := records.Init(c.String("data")); err != nil {
		panic(err)
	}
	defer records.Release()
	controller.DaemonPort = c.Int("dport")
	controller.Advertise = c.String("advertise")
	controller.Serve(c.String("addr"), controller.NewLainlet(c.String("lainlet")))
}
