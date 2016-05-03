package controller

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"
	"net/http"
	"strconv"
)

func GetBackup(r render.Render, params martini.Params, req *http.Request, let *Lainlet) {
	var err error
	app := params["app"]
	if app == "" {
		r.JSON(400, errors.New("app name can not be empty"))
		return
	}
	proc := params["proc"]
	if proc == "" {
		r.JSON(400, errors.New("proc name can not be empty"))
		return
	}
	volumes := req.URL.Query()["volume"]
	if len(volumes) == 0 { // no volume
		r.JSON(200, []string{})
		return
	}
	ctl := NewController(app, let)
	data, err := ctl.GetBackup(proc, volumes...)
	if err != nil {
		r.JSON(500, err)
		return
	}
	if data == nil {
		r.JSON(200, []string{})
	} else {
		r.JSON(200, data)
	}
}

func GetCronJobs(r render.Render, req *http.Request, params martini.Params, let *Lainlet) {
	app := params["app"]
	if app == "" {
		r.JSON(400, errors.New("app name can not be empty"))
		return
	}
	query := make(map[string]string)
	for k, v := range req.URL.Query() {
		query[k] = v[0]
	}
	ctl := NewController(app, let)
	data, err := ctl.GetCronJobs(query)
	if err != nil {
		r.JSON(500, err)
		return
	}
	if data == nil {
		r.JSON(200, []string{})
	} else {
		r.JSON(200, data)
	}
}

func GetCronRecords(r render.Render, req *http.Request, params martini.Params, let *Lainlet) {
	app := params["app"]
	if app == "" {
		r.JSON(400, errors.New("app name can not be empty"))
		return
	}
	query := make(map[string]string)
	for k, v := range req.URL.Query() {
		query[k] = v[0]
	}
	ctl := NewController(app, let)
	data, err := ctl.GetCronRecords(query)
	if err != nil {
		r.JSON(500, err)
		return
	}
	if data == nil {
		r.JSON(200, []string{})
	} else {
		r.JSON(200, data)
	}
}

func GetCronRecord(r render.Render, params martini.Params, let *Lainlet) {
	app := params["app"]
	if app == "" {
		r.JSON(400, errors.New("app name can not be empty"))
		return
	}
	id := params["id"]
	if id == "" {
		r.JSON(400, errors.New("record id is empty"))
		return
	}
	ret, err := NewController(app, let).GetCronRecordById(id)
	if err != nil {
		r.JSON(500, err)
		return
	}
	r.JSON(200, ret)
}

func CronOnce(r render.Render, params martini.Params, let *Lainlet) {
	app := params["app"]
	if app == "" {
		r.JSON(400, errors.New("app name can not be empty"))
		return
	}
	id := params["id"]
	if id == "" {
		r.JSON(400, errors.New("job id can not be empty"))
		return
	}
	ctl := NewController(app, let)
	rid, err := ctl.CronOnce(id)
	if err != nil {
		r.JSON(500, err)
		return
	}
	r.JSON(200, rid)
}

func BackupRecover(r render.Render, req *http.Request, params martini.Params, let *Lainlet) {
	app := params["app"]
	if app == "" {
		r.JSON(400, errors.New("app name can not be empty"))
		return
	}
	proc := params["proc"]
	if proc == "" {
		r.JSON(400, errors.New("proc name can not be empty"))
		return
	}
	file := params["file"]
	if file == "" {
		r.JSON(400, errors.New("recover file can not be empty"))
		return
	}

	ctl := NewController(app, let)

	entity, err := ctl.BackupFileInfo(proc, file)
	if err != nil {
		r.JSON(500, errors.New("fail to get backup info for "+file))
		return
	}

	id, err := ctl.BackupRecover(proc, "", file, entity.InstanceNo, entity.InstanceNo)
	if err != nil {
		r.JSON(500, err)
		return
	}
	r.JSON(200, id)
}

func BackupRecoverIncrement(r render.Render, req *http.Request, params martini.Params, let *Lainlet) {
	app := params["app"]
	if app == "" {
		r.JSON(400, errors.New("app name can not be empty"))
		return
	}
	proc := params["proc"]
	if proc == "" {
		r.JSON(400, errors.New("proc name can not be empty"))
		return
	}
	files := req.PostForm["files"]
	if len(files) == 0 {
		r.JSON(400, errors.New("no files given"))
		return
	}
	dir := params["dir"]
	if dir == "" {
		r.JSON(400, errors.New("backup dir not given"))
		return
	}
	ctl := NewController(app, let)

	entity, err := ctl.BackupFileInfo(proc, dir)
	if err != nil {
		r.JSON(500, errors.New("fail to get backup info for "+dir))
		return
	}

	id, err := ctl.IncrementBackupRecover(proc, "", entity.InstanceNo, entity.InstanceNo, dir, files)
	if err != nil {
		r.JSON(500, err)
		return
	}
	r.JSON(200, id)
}

func BackupMigrate(r render.Render, req *http.Request, params martini.Params, let *Lainlet) {
	app := params["app"]
	if app == "" {
		r.JSON(400, errors.New("app name can not be empty"))
		return
	}
	proc := params["proc"]
	if proc == "" {
		r.JSON(400, errors.New("proc name can not be empty"))
		return
	}
	file := params["file"]
	if file == "" {
		r.JSON(400, errors.New("file must be given"))
		return
	}
	volume := req.FormValue("volume")
	if volume == "" {
		r.JSON(400, errors.New("volume can not be empty"))
		return
	}
	to := req.FormValue("to")
	toi, err := strconv.Atoi(to)
	if err != nil {
		r.JSON(400, errors.New("to value must be a integer"))
		return
	}
	ctl := NewController(app, let)

	entity, err := ctl.BackupFileInfo(proc, file)
	if err != nil {
		r.JSON(500, err)
		return
	}
	id, err := ctl.BackupRecover(proc, volume, file, entity.InstanceNo, toi)
	if err != nil {
		r.JSON(500, err)
		return
	}
	r.JSON(200, id)
}

func BackupMigrateIncrement(r render.Render, req *http.Request, params martini.Params, let *Lainlet) {
	app := params["app"]
	if app == "" {
		r.JSON(400, errors.New("app name can not be empty"))
		return
	}
	proc := params["proc"]
	if proc == "" {
		r.JSON(400, errors.New("proc name can not be empty"))
		return
	}
	files := req.PostForm["files"]
	if len(files) == 0 {
		r.JSON(400, errors.New("no files given"))
		return
	}
	volume := req.FormValue("volume")
	if volume == "" {
		r.JSON(400, errors.New("volume can not be empty"))
		return
	}
	to := req.FormValue("to")
	toi, err := strconv.Atoi(to)
	if err != nil {
		r.JSON(400, errors.New("to value must be a integer"))
		return
	}
	dir := params["dir"]
	if dir == "" {
		r.JSON(400, errors.New("backup dir not given"))
		return
	}
	ctl := NewController(app, let)
	entity, err := ctl.BackupFileInfo(proc, dir)
	if err != nil {
		r.JSON(500, err)
		return
	}
	id, err := ctl.IncrementBackupRecover(proc, volume, entity.InstanceNo, toi, dir, files)
	if err != nil {
		r.JSON(500, err)
		return
	}
	r.JSON(200, id)

}

func GetAllCronJobs(r render.Render, let *Lainlet) {
	jobs := let.GetJobs()
	r.JSON(200, jobs)
}

func GetIncrementBackupFileList(r render.Render, params martini.Params, let *Lainlet) {
	app := params["app"]
	if app == "" {
		r.JSON(400, errors.New("app name can not be empty"))
		return
	}
	proc := params["proc"]
	if proc == "" {
		r.JSON(400, errors.New("proc name can not be empty"))
		return
	}
	dir := params["dir"]
	if dir == "" {
		r.JSON(400, errors.New("backup dir not given"))
		return
	}

	ctl := NewController(app, let)
	data, err := ctl.IncrementBackupFileList(proc, dir)
	if err != nil {
		r.JSON(500, err)
		return
	}
	r.JSON(200, data)
}

func NotFound(r render.Render) {
	r.JSON(404, map[string]string{
		"msg": "404 not found",
	})
}

func v1(r martini.Router) {
	r.Get("/backup/json/app/:app/proc/:proc", GetBackup)
	r.Get("/backup/filelist/app/:app/proc/:proc/dir/:dir", GetIncrementBackupFileList)
	r.Post("/backup/recover/app/:app/proc/:proc/file/:file", BackupRecover)
	r.Post("/backup/migrate/app/:app/proc/:proc/file/:file", BackupMigrate)
	r.Post("/backup/recover/increment/app/:app/proc/:proc/dir/:dir", BackupRecoverIncrement)
	r.Post("/backup/migrate/increment/app/:app/proc/:proc/dir/:dir", BackupMigrateIncrement)
	r.Get("/cron/jobs/app/:app", GetCronJobs)
	r.Get("/cron/jobs/all", GetAllCronJobs)
	r.Get("/cron/records/app/:app", GetCronRecords)
	r.Get("/cron/record/app/:app/id/:id", GetCronRecord)
	r.Post("/cron/once/app/:app/id/:id", CronOnce)
}

func Serve(addr string, lainlet *Lainlet) {
	m := martini.New()
	m.Use(func(c martini.Context, req *http.Request) {
		req.ParseForm()
		log.Infof("Request for [%s]%s, data is %+v", req.Method, req.URL, req.Form)
		c.Next()
	})
	m.Use(render.Renderer())
	m.Use(martini.Recovery())

	r := martini.NewRouter()
	m.MapTo(r, (*martini.Routes)(nil))
	m.Map(lainlet)

	m.Action(r.Handle)

	r.Group("/api/v1", v1)
	r.Group("/api/v2", v2)
	r.NotFound(NotFound)

	m.RunOnAddr(addr)
}
