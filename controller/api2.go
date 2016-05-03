package controller

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"
	"io/ioutil"
	"github.com/laincloud/backupd/controller/records"
	"github.com/laincloud/backupd/crond"
	"net/http"
	"strconv"
	"strings"
)

func BackupFileInfoOrFileList(r render.Render, params martini.Params, let *Lainlet, req *http.Request) {
	var (
		data interface{}
		err  error
	)
	ctl := NewController(params["app"], let)
	if req.URL.Query().Get("open") == "true" {
		data, err = ctl.IncrementBackupFileList(params["proc"], params["file"])
	} else {
		data, err = ctl.BackupFileInfo(params["proc"], params["file"])
	}
	if err != nil {
		r.JSON(500, err)
		return
	}
	r.JSON(200, data)
}

func BackupDelete(r render.Render, params martini.Params, let *Lainlet, req *http.Request) {
	files := req.PostForm["files"]
	if len(files) > 0 {
		ctl := NewController(params["app"], let)
		vs, err := let.Volumes(params["app"], params["proc"])
		if err != nil {
			r.JSON(500, err.Error())
			return
		}

		backups, err := ctl.GetBackup(params["proc"], vs...)
		if err != nil {
			r.JSON(500, err)
			return
		}

		for _, file := range files {
			for _, entity := range backups {
				if strings.Split(file, "/")[0] == entity.Name {
					goto valid
				}
			}
			r.JSON(400, "No permission to delete "+file)
			return
		valid:
			continue
		}

		if err := ctl.DeleteBackup(params["proc"], files); err != nil {
			r.JSON(500, err)
			return
		}
	}
	r.JSON(204, "DELETED")
}

func GetCronJob(r render.Render, params martini.Params, let *Lainlet) {
	ctl := NewController(params["app"], let)
	data, err := ctl.GetCronJob(params["id"])
	if err != nil {
		r.JSON(400, err)
		return
	}
	r.JSON(200, data)
}

func CronAction(r render.Render, params martini.Params, let *Lainlet) {
	ctl := NewController(params["app"], let)
	data, err := ctl.CronAction(params["id"], params["action"])
	if err != nil {
		r.JSON(400, err)
		return
	}
	// FIXME, it's not good to do like this, lainlet give a method to change sleep
	switch params["action"] {
	case "sleep", "wakeup":
		ip, _ := crond.ParseIPFromID(params["id"])
		jobs := let.GetJobs()
		for i, j := range jobs[ip] {
			if j.ID == params["id"] {
				jobs[ip][i].Sleep = params["action"] == "sleep"
			}
		}
	}

	r.JSON(200, data)
}

func ServerCronJobs(r render.Render, params martini.Params, let *Lainlet) {
	jobs := let.GetJobs()
	data, ok := jobs[params["ip"]]
	if ok {
		r.JSON(200, data)
		return
	}
	r.JSON(200, []string{})
}

func ServerCronStats(r render.Render, params martini.Params) {
	if !validIP(params["ip"]) {
		r.JSON(400, "unvalid ip addr")
		return
	}
	addr := fmt.Sprintf("%s:%d", params["ip"], DaemonPort)
	backend := NewBackend(addr, DaemonApiPrefix)
	var err error
	switch params["action"] {
	case "start":
		err = backend.StartCrond()
	case "stop":
		err = backend.StopCrond()
	}
	if err != nil {
		r.JSON(400, err)
		return
	}
	r.JSON(200, "OK")
}

func GetCronRecordsV2(r render.Render, params martini.Params, req *http.Request) {
	total, month, year := 100, 0, 0
	if s := req.URL.Query().Get("total"); s != "" {
		if i, err := strconv.Atoi(s); err == nil {
			total = i
		}
	}
	if s := req.URL.Query().Get("month"); s != "" {
		if i, err := strconv.Atoi(s); err == nil {
			month = i
		}
	}
	if s := req.URL.Query().Get("year"); s != "" {
		if i, err := strconv.Atoi(s); err == nil {
			year = i
		}
	}
	data, err := records.Get(params["app"], total, month, year)
	if err != nil {
		r.JSON(500, err)
		return
	}
	r.JSON(200, data)
}

func GetCronRecordV2(r render.Render, params martini.Params) {
	data, err := records.GetById(params["app"], params["id"])
	if err != nil {
		if err == records.ErrNotFound {
			r.JSON(404, err)
		} else {
			r.JSON(500, err)
		}
		return
	}
	r.JSON(200, data)
}

func ServerDebug(r render.Render, params martini.Params) {
	if !validIP(params["ip"]) {
		r.JSON(400, "unvalid ip addr")
		return
	}
	addr := fmt.Sprintf("%s:%d", params["ip"], DaemonPort)
	data, err := NewBackend(addr, DaemonApiPrefix).Debug()
	if err != nil {
		r.JSON(400, err)
		return
	}
	r.JSON(200, data)
}

func Notify(r render.Render, req *http.Request) {
	content, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errorf("Fail to read request body, %s", err.Error())
		r.JSON(400, err)
		return
	}
	defer req.Body.Close()

	var (
		record crond.JobRecord
		app    string
	)
	if err := json.Unmarshal(content, &record); err != nil {
		log.Errorf("Fail to unmarshal notify content, %s, %s", string(content), err.Error())
		r.JSON(400, err)
		return
	}
	if a, ok := record.Job.Args["app"]; ok {
		app, _ = a.(string)
	}
	if record.Job.Action == "backup_expire" {
		app = "backupctl"
	}
	log.Debugf("Put records for app %s: %v", app, record)
	if err := records.Put(app, record); err != nil {
		log.Errorf("Fail to store record, %s", err.Error())
		r.JSON(500, err)
		return
	}
}

func BackupDB(r render.Render, req *http.Request) {
	dir := req.Form.Get("dir")
	if dir == "" {
		r.JSON(403, "dir is empty")
		return
	}
	if err := records.Backup(dir); err != nil {
		r.JSON(500, err)
		return
	}
	r.JSON(200, "CREATED")
}

func v2(r martini.Router) {
	r.Get("/app/:app/proc/:proc/backups", GetBackup)                                              //
	r.Get("/app/:app/proc/:proc/backups/(?P<file>.+)", BackupFileInfoOrFileList)                  //
	r.Post("/app/:app/proc/:proc/backups/(?P<file>.+\\.tar\\.gz)/actions/recover", BackupRecover) //
	r.Post("/app/:app/proc/:proc/backups/(?P<file>.+\\.tar\\.gz)/actions/migrate", BackupMigrate)
	r.Post("/app/:app/proc/:proc/backups/:dir/actions/recover", BackupRecoverIncrement) //
	r.Post("/app/:app/proc/:proc/backups/:dir/actions/migrate", BackupMigrateIncrement) //
	r.Post("/app/:app/proc/:proc/backups/actions/delete", BackupDelete)                 //

	r.Get("/app/:app/cron/jobs", GetCronJobs)                     //
	r.Get("/app/:app/cron/jobs/:id", GetCronJob)                  //
	r.Get("/app/:app/cron/records", GetCronRecordsV2)             //
	r.Get("/app/:app/cron/records/:id", GetCronRecordV2)          //
	r.Post("/app/:app/cron/jobs/:id/actions/:action", CronAction) //

	r.Get("/server/:ip/cron/jobs", ServerCronJobs)             //
	r.Put("/server/:ip/cron/actions/:action", ServerCronStats) //
	r.Get("/server/:ip/debug", ServerDebug)                    //

	r.Get("/system/cron/jobs", GetAllCronJobs) // TODO, having bug, if controller restart, it lose the job's status
	r.Post("/system/notify", Notify)
	r.Post("/system/backup", BackupDB)
}
