package controller

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	api "github.com/laincloud/backupd/api/v1"
	"github.com/laincloud/backupd/crond"
	"github.com/laincloud/backupd/tasks/backup"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type BackupEntity struct {
	Volume     string    `json:"volume"`
	Name       string    `json:"name"`
	Size       uint64    `json:"size"`
	Created    time.Time `json:"created"`
	InstanceNo int       `json:"instanceNo"`
}

type Backend struct {
	Addr   string
	Prefix string
}

func NewBackend(addr, prefix string) *Backend {
	if addr[:7] != "http://" {
		addr = "http://" + addr
	}
	if prefix[0] != '/' {
		prefix = "/" + prefix
	}
	return &Backend{
		Addr:   addr,
		Prefix: prefix,
	}
}

func (end *Backend) FixCronJobArgs(action string, args crond.FuncArg) {
	if action == "backup" {
		delete(args, "path")
		delete(args, "archive")
	}
}

func (end *Backend) StartCrond() error {
	_, err := end.RawRequest("PUT", "/cron/start", nil)
	return err
}
func (end *Backend) StopCrond() error {
	_, err := end.RawRequest("PUT", "/cron/stop", nil)
	return err
}

func (end *Backend) GetCronJobs(appname string, query map[string]string) ([]crond.EntrySpec, error) {
	args := url.Values{"args_app": []string{appname}}
	for k, v := range query {
		args.Set(k, v)
	}
	url := fmt.Sprintf("%s?%s", "/cron/jobs", args.Encode())
	content, err := end.RawRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	var ret []crond.EntrySpec
	if err := json.Unmarshal(content, &ret); err != nil {
		return nil, err
	}
	for i, _ := range ret {
		end.FixCronJobArgs(ret[i].J.Action, ret[i].J.Args)
	}
	return ret, nil
}

func (end *Backend) GetCronJob(id string) (crond.Job, error) {
	var ret crond.Job
	url := fmt.Sprintf("/cron/jobs/%s", id)
	content, err := end.RawRequest("GET", url, nil)
	if err != nil {
		return ret, err
	}
	err = json.Unmarshal(content, &ret)
	if err != nil {
		end.FixCronJobArgs(ret.Action, ret.Args)
	}
	return ret, err
}

func (end *Backend) CronAction(id, action string) (string, error) {
	url := fmt.Sprintf("/cron/jobs/%s/actions/%s", id, action)
	content, err := end.RawRequest("POST", url, nil)
	if err != nil {
		return "", err
	}
	if action == "run" {
		var ret map[string]string
		if err := json.Unmarshal(content, &ret); err != nil {
			return "", err
		}
		id, ok := ret["rid"]
		if !ok {
			return "", fmt.Errorf("unexpected response from backupd: %s", string(content))
		}
		return id, nil
	}
	return "OK", nil
}

func (end *Backend) SetCronJobs(jobs []crond.Job) error {
	content, err := json.Marshal(jobs)
	if err != nil {
		return err
	}
	hashV := md5.Sum(content)
	args := url.Values{}
	args.Add("data", string(content))
	args.Add("version", string(hashV[:]))
	_, err = end.RawRequest("PUT", "/cron/jobs", args)
	return err
}

func (end *Backend) GetCronRecords(appname string, query map[string]string) ([]crond.JobRecord, error) {
	args := url.Values{"args_app": []string{appname}}
	for k, v := range query {
		args.Set(k, v)
	}
	url := fmt.Sprintf("%s?%s", "/cron/records", args.Encode())
	content, err := end.RawRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	var ret []crond.JobRecord
	if err := json.Unmarshal(content, &ret); err != nil {
		return nil, err
	}
	for i, _ := range ret {
		end.FixCronJobArgs(ret[i].Action, ret[i].Args)
	}
	return ret, nil
}

func (end *Backend) GetCronRecordById(id string) (crond.JobRecord, error) {
	var ret crond.JobRecord
	url := fmt.Sprintf("/cron/records/%s", id)
	content, err := end.RawRequest("GET", url, nil)
	if err != nil {
		return ret, err
	}
	if err := json.Unmarshal(content, &ret); err != nil {
		return ret, err
	}
	end.FixCronJobArgs(ret.Action, ret.Args)
	return ret, nil
}

func (end *Backend) GetBackup(volumes ...string) ([]BackupEntity, error) {
	var (
		volume url.Values = url.Values{}
		url    string     = "/backup/json"
	)
	if len(volumes) > 0 {
		for _, v := range volumes {
			volume.Add("dir", v)
		}
		url = fmt.Sprintf("%s?%s", url, volume.Encode())
	}
	content, err := end.RawRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	var ret []BackupEntity
	if err := json.Unmarshal(content, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (end *Backend) GetBackupInfo(file string) (BackupEntity, error) {
	var ret BackupEntity
	url := fmt.Sprintf("/backup/info/file/%s", file)
	content, err := end.RawRequest("GET", url, nil)
	if err != nil {
		return ret, err
	}
	if err := json.Unmarshal(content, &ret); err != nil {
		return ret, err
	}
	return ret, nil
}

func (end *Backend) BackupDelete(files []string) error {
	args := url.Values{}
	for _, file := range files {
		args.Add("files", file)
	}
	uri := fmt.Sprintf("/backup/delete")
	_, err := end.RawRequest("POST", uri, args)
	return err
}

func (end *Backend) BackupRecover(namespace, file, destDir string, extra map[string]string) (string, error) {
	uri := fmt.Sprintf("/backup/full/recover/file/%s", file)
	args := url.Values{}
	args.Add("namespace", namespace)
	args.Add("destDir", destDir)
	for k, v := range extra {
		args.Add(k, v)
	}

	content, err := end.RawRequest("POST", uri, args)
	if err != nil {
		return "", err
	}
	var ret map[string]string
	if err := json.Unmarshal(content, &ret); err != nil {
		return "", err
	}
	id, ok := ret["rid"]
	if !ok {
		return "", fmt.Errorf("unexpected response from backupd: %s", string(content))
	}
	return id, nil
}

func (end *Backend) BackupRecoverIncrement(namespace, dir, destDir string, files []string, extra map[string]string) (string, error) {
	uri := fmt.Sprintf("/backup/increment/recover/dir/%s", dir)
	args := url.Values{}
	args.Add("namespace", namespace)
	args.Add("destDir", destDir)
	for _, file := range files {
		args.Add("files", file)
	}
	for k, v := range extra {
		args.Add(k, v)
	}

	content, err := end.RawRequest("POST", uri, args)
	if err != nil {
		return "", err
	}
	var ret map[string]string
	if err := json.Unmarshal(content, &ret); err != nil {
		return "", err
	}
	id, ok := ret["rid"]
	if !ok {
		return "", fmt.Errorf("unexpected response from backupd: %s", string(content))
	}
	return id, nil
}

func (end *Backend) CronOnce(id string) (string, error) {
	content, err := end.RawRequest("POST", "/cron/once/"+id, nil)
	if err != nil {
		return "", err
	}
	var ret map[string]string
	if err := json.Unmarshal(content, &ret); err != nil {
		return "", err
	}
	id, ok := ret["rid"]
	if !ok {
		return "", fmt.Errorf("unexpected response from backupd: %s", string(content))
	}
	return id, nil
}

func (end *Backend) Debug() (map[string]interface{}, error) {
	content, err := end.RawRequest("GET", "/debug", nil)
	if err != nil {
		return nil, err
	}
	var ret map[string]interface{}
	if err := json.Unmarshal(content, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (end *Backend) BackupInfo(file string) (BackupEntity, error) {
	var entity backup.Entity
	uri := fmt.Sprintf("/backup/info/file/%s", file)
	content, err := end.RawRequest("GET", uri, nil)
	if err != nil {
		return BackupEntity{}, err
	}
	if err := json.Unmarshal(content, &entity); err != nil {
		return BackupEntity{}, err
	}
	return BackupEntity{
		Volume:     entity.Volume,
		Name:       entity.Name,
		Size:       entity.Size,
		Created:    entity.Created,
		InstanceNo: entity.InstanceNo,
	}, nil
}

func (end *Backend) IncrementBackupFileList(dir string) ([]api.FInfo, error) {
	var ret []api.FInfo
	uri := fmt.Sprintf("/backup/filelist/dir/%s", dir)
	content, err := end.RawRequest("GET", uri, nil)
	if err != nil {
		return ret, err
	}
	if err := json.Unmarshal(content, &ret); err != nil {
		return ret, err
	}
	return ret, nil
}

func (end *Backend) SetNotify(addr string) error {
	args := url.Values{}
	args.Add("addr", addr)
	_, err := end.RawRequest("PUT", "/notify", args)
	if err != nil {
		return err
	}
	return nil
}

func (end *Backend) GetNotify() ([]string, error) {
	content, err := end.RawRequest("GET", "/notify", nil)
	if err != nil {
		return nil, err
	}
	var ret map[string][]string
	if err := json.Unmarshal(content, &ret); err != nil {
		return nil, err
	}
	return ret["addr"], nil
}

func (end *Backend) RemoveNotify(addr string) error {
	args := url.Values{}
	args.Add("addr", addr)
	_, err := end.RawRequest("POST", "/notify/actions/remove", args)
	if err != nil {
		return err
	}
	return nil
}

func (end *Backend) RawRequest(method, uri string, data url.Values) ([]byte, error) {
	if uri[0] != '/' {
		uri = "/" + uri
	}
	var (
		resp *http.Response
		err  error
		url  string = fmt.Sprintf("%s%s%s", end.Addr, end.Prefix, uri)
	)

	log.Infof("Request for [%s]%s, %s", method, url, data.Encode())
	req, err := http.NewRequest(method, url, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err = (&http.Client{Timeout: time.Second * 10}).Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if len(content) > 500 {
		log.Infof("Response is: %s...", string(content[:500]))
	} else {
		log.Infof("Response is: %s", string(content))
	}
	if resp.StatusCode >= 300 { // http code not success
		return nil, fmt.Errorf("%s", content)
	}
	return content, nil
}
