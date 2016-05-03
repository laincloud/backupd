package cli

import (
	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"os"
	"path"
)

var commands = []cli.Command{
	{
		Name:   "daemon",
		Usage:  "run backup daemon",
		Action: daemonMain,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "addr",
				Value: ":9002",
				Usage: "The addr backup daemon serve on",
			},
			cli.StringFlag{
				Name:  "ip",
				Value: "127.0.0.1",
				Usage: "The ip value daemon is running on",
			},
			cli.StringFlag{
				Name:  "backup-driver",
				Value: "moosefs",
				Usage: "Set backup driver",
			},
			cli.StringFlag{
				Name:  "backup-moosefs-dir",
				Value: "/mfs/lain/backup",
				Usage: "The direcotry path mount on moosefs, only used when backup-driver is moosefs",
			},
		},
	},
	{
		Name:   "controller",
		Usage:  "run backup controller",
		Action: controllerMain,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "addr",
				Value: ":9002",
				Usage: "The addr backup controller serve on",
			},
			cli.StringFlag{
				Name:  "lainlet",
				Value: "lainlet.lain:9001",
				Usage: "The lainlet's address",
			},
			cli.StringFlag{
				Name:  "advertise",
				Value: "127.0.0.1:9002",
				Usage: "The advertise address, controller will tell daemon to use report result to this addr",
			},
			cli.StringFlag{
				Name:  "data",
				Value: ".",
				Usage: "The directory controller store some data",
			},
			cli.IntFlag{
				Name:  "dport",
				Value: 9002,
				Usage: "The daemon port, controller use this port connect with daemon",
			},
		},
	},
}

// Run the process
func Run() {
	app := cli.NewApp()
	app.Name = path.Base(os.Args[0])
	app.Usage = "Backup component for lain"
	app.Version = "2.0.0"

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "debug",
			Usage: "open debug log",
		},
	}

	app.Before = func(c *cli.Context) error {
		if c.BoolT("debug") {
			log.SetLevel(log.DebugLevel)
		}
		log.Debug("Run in debug mode")
		return nil
	}

	app.Commands = commands
	app.Run(os.Args)
}
