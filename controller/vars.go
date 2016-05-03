package controller

const (
	VOLUME_ROOT    = "/data/lain/volumes"
	BackupFunc     = "backup"
	ExpireFunc     = "backup_expire"
	ExpireSchedule = "* * * * *"
	NotifyURI      = "/api/v2/system/notify"
)

var (
	Advertise       = "127.0.0.1"
	DaemonPort      = 9002
	DaemonApiPrefix = "/api/v1"
)
