package api

import (
	log "github.com/Sirupsen/logrus"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"
	"github.com/laincloud/backupd/api/v1"
	"net/http"
)

func notFound(r render.Render) {
	r.JSON(404, map[string]string{
		"msg": "404 not found",
	})
}

func Serve(addr string) {

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

	m.Action(r.Handle)

	r.Group("/api/v1", v1.Router)
	r.NotFound(notFound)

	m.RunOnAddr(addr)
}
