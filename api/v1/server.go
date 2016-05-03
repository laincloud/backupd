package v1

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"
	"github.com/laincloud/backupd/crond"
	"github.com/laincloud/backupd/tasks/backup"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

const (
	errUnvalidArg  = "Unvalid Argument"
	errBackupError = "Error happend when doing backup action"
)

var (
	newError       = func(title string, cause string) string { return title + ": " + cause }
	startTime      time.Time
	cronUpdateTime time.Time
)

func init() {
	startTime = time.Now()
}

type FInfo struct {
	Name    string    `json:"name"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
	Dir     bool      `json:"dir"`
}

func CronEntriesGet(r render.Render, req *http.Request) {
	query := make(map[string]string)
	for k, v := range req.URL.Query() {
		query[k] = v[0]
	}
	entries := crond.Entries(query)
	r.JSON(200, entries)
}

func CronEntryGet(r render.Render, params martini.Params) {
	id := params["id"]
	if id == "" {
		r.JSON(400, newError(errUnvalidArg, "cron job id is empty"))
		return
	}
	job, err := crond.FindById(id)
	if err != nil {
		r.JSON(400, newError(errUnvalidArg, err.Error()))
		return
	}
	r.JSON(200, job)
}

func CronEntriesSet(r render.Render, req *http.Request) {
	var tasks []crond.Job
	data := req.FormValue("data")
	if err := json.Unmarshal([]byte(data), &tasks); err != nil {
		r.JSON(400, newError(errUnvalidArg, "given data is not a valid json string"))
		return
	}
	version := req.FormValue("version")
	if version != "" && version == crond.Version() {
		log.Debugf("Same task version, do not update")
		r.JSON(200, "OK")
		return
	}
	log.Infof("Cron tasks changed to %+v", tasks)
	crond.Update(tasks, version)
	cronUpdateTime = time.Now()
	r.JSON(200, "OK")
}

func CronEntriesCount(r render.Render) {
	r.JSON(200, map[string]int{
		"count": crond.Count(),
	})
}

func CrondStop(r render.Render) {
	crond.Stop()
	r.JSON(201, "")
}

func CrondStart(r render.Render) {
	crond.Start()
	r.JSON(201, "")
}

func CronOnce(params martini.Params, r render.Render, req *http.Request) {
	id := params["id"]
	if id == "" {
		r.JSON(400, newError(errUnvalidArg, "cron job id is empty"))
		return
	}
	job, err := crond.FindById(id)
	if err != nil {
		r.JSON(400, newError(errUnvalidArg, err.Error()))
		return
	}
	r.JSON(202, map[string]string{
		"rid": crond.Once(job),
	})
}

func CronAction(params martini.Params, r render.Render, req *http.Request) {
	id := params["id"]
	if id == "" {
		r.JSON(400, newError(errUnvalidArg, "cron job id is empty"))
		return
	}

	switch strings.ToLower(params["action"]) {
	case "run":
		CronOnce(params, r, req)
	case "sleep":
		crond.Sleep(id, true)
		r.JSON(201, "")
	case "wakeup":
		crond.Sleep(id, false)
		r.JSON(204, "")
	}
}

func BackupJson(r render.Render, req *http.Request) {
	var (
		err error
		ret []backup.Entity
	)
	if dirs, ok := req.URL.Query()["dir"]; ok {
		ret, err = backup.List(dirs...)
	} else {
		ret, err = backup.List()
	}
	if err != nil {
		r.JSON(503, newError(errBackupError, err.Error()))
		return
	}
	r.JSON(200, ret)
}

func BackupInfo(r render.Render, params martini.Params) {
	fileName := params["name"]
	info, err := backup.Info(fileName)
	if err != nil {
		r.JSON(503, newError(errBackupError, err.Error()))
		return
	}
	r.JSON(200, info)
}

func BackupFileList(req *http.Request, params martini.Params, r render.Render) {
	dir := params["_1"]
	data, err := backup.FileList(dir)
	if err != nil {
		r.JSON(503, newError(errBackupError, err.Error()))
		return
	}
	ret := make([]FInfo, len(data))
	for i, item := range data {
		ret[i] = FInfo{
			Name:    item.Name(),
			Size:    item.Size(),
			ModTime: item.ModTime(),
			Dir:     item.IsDir(),
		}
	}
	r.JSON(200, ret)
}

func BackupDelete(r render.Render, req *http.Request) {
	req.ParseForm()
	for _, file := range req.PostForm["files"] {
		if err := backup.Delete(file); err != nil {
			r.JSON(503, newError(errBackupError, err.Error()))
			return
		}
	}
	r.JSON(204, "DELETED")
}

func BackupRecover(r render.Render, req *http.Request, params martini.Params) {
	req.ParseForm()
	rid, err := crond.RawOnce("backup_recover", map[string]interface{}{
		"namespace": req.FormValue("namespace"),
		"backup":    params["file"],
		"files":     req.PostForm["files"],
		"destDir":   req.FormValue("destDir"),
		"app":       req.FormValue("app"),
		"proc":      req.FormValue("proc"),
	})
	if err != nil {
		r.JSON(503, newError(errBackupError, err.Error()))
		return
	}
	r.JSON(202, map[string]string{
		"rid": rid,
	})
}

func SetNotifyAddr(r render.Render, req *http.Request) {
	crond.AddNotifyAddr(req.FormValue("addr"))
	r.JSON(200, "OK")
}

func RemoveNotifyAddr(r render.Render, req *http.Request) {
	crond.RemoveNotifyAddr(req.FormValue("addr"))
	r.JSON(203, "OK")
}

func GetNotifyAddr(r render.Render) {
	r.JSON(200, map[string][]string{
		"addr": crond.NotifyAddrs(),
	})
}

// TODO add some more system info, move this feature to a independency lib
func Debug(r render.Render) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	r.JSON(200, map[string]interface{}{
		"startTime":     startTime,
		"updateTime":    cronUpdateTime,
		"crond_status":  crond.Status(),
		"goroutines":    runtime.NumGoroutine(),
		"running_tasks": atomic.LoadInt32(&crond.RunningCount),
		"mem_stats":     memStats,
	})
}

func Router(r martini.Router) {
	r.Get("/cron/jobs", CronEntriesGet)
	r.Get("/cron/jobs/:id", CronEntryGet)
	r.Put("/cron/jobs", CronEntriesSet)
	r.Get("/cron/jobs/count", CronEntriesCount)
	r.Put("/cron/stop", CrondStop)
	r.Put("/cron/start", CrondStart)
	r.Post("/cron/once/:id", CronOnce)
	r.Post("/cron/jobs/:id/actions/:action", CronAction)

	r.Get("/backup/json", BackupJson)
	r.Get("/backup/info/file/:name", BackupInfo)
	r.Get("/backup/filelist/dir/**", BackupFileList)
	r.Post("/backup/delete", BackupDelete)
	r.Post("/backup/full/recover/file/:file", BackupRecover)
	r.Post("/backup/increment/recover/dir/:file", BackupRecover)
	r.Put("/notify", SetNotifyAddr)
	r.Get("/notify", GetNotifyAddr)
	r.Post("/notify/actions/remove", RemoveNotifyAddr)

	r.Get("/debug", Debug)
}
