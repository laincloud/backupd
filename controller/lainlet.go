package controller

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
	"github.com/laincloud/backupd/crond"
	"github.com/laincloud/backupd/tasks/backup"
	lainlet "github.com/laincloud/lainlet/api/v2"
	"github.com/laincloud/lainlet/client"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Lainlet struct {
	Addr         string
	data         lainlet.CoreInfoForBackupctl
	cronJobs     map[string][]crond.Job
	volumes      map[string][]string // use this to store volumes for every proc, parse from annotation
	lock         sync.RWMutex
	procFullName map[string]string
}

func NewLainlet(addr string) *Lainlet {
	ret := &Lainlet{
		Addr:         addr,
		data:         lainlet.CoreInfoForBackupctl{Data: make(map[string][]lainlet.PodInfoForBackupctl)},
		cronJobs:     make(map[string][]crond.Job),
		volumes:      make(map[string][]string),
		procFullName: make(map[string]string),
	}
	go ret.Watcher()
	go ret.CheckBackend()
	return ret
}

func (ll *Lainlet) ProcFullName(app, proc string) (string, bool) {
	if len(strings.Split(proc, ".")) > 1 {
		return proc, true
	}
	key := fmt.Sprintf("%s.%s", app, proc)
	ret, ok := ll.procFullName[key]
	return ret, ok
}

func (ll *Lainlet) CheckBackend() {
	for {
		ll.lock.RLock()
		for node, _ := range ll.cronJobs {
			addr := fmt.Sprintf("%s:%d", node, DaemonPort)
			go func() {
				if data, err := NewBackend(addr, DaemonApiPrefix).Debug(); err == nil && data["updateTime"].(string)[0] == '0' {
					log.Infof("Find backupd on %s having no jobs, update it", node)
					ll.BroadcastCronJobs([]string{node})
				}
			}()
		}
		ll.lock.RUnlock()
		time.Sleep(time.Second * 30)
	}
}

func (ll *Lainlet) Watcher() {
	letClient := client.New(ll.Addr)
	uri := "/v2/backupspec"
reconnect:
	ctx, cancel := context.WithCancel(context.Background())
	log.Infof("start to watch lainlet, %s", uri)
	respCh, err := letClient.Watch(uri, ctx)
	if err != nil {
		log.Errorf("Fail to watch from %s,%s, retry after 3 seconds", uri, err.Error())
		cancel()
		time.Sleep(time.Second * 3)
		goto reconnect
	}
	for item := range respCh {
		if item.Event == "heartbeat" || len(item.Data) == 0 {
			continue
		}
		log.Infof("Get app data from lainlet")
		ll.lock.Lock()
		if err := ll.data.Decode(item.Data); err != nil {
			log.Errorf("Fail to unmarshal data from lainlet, %s", err.Error())
			ll.lock.Unlock()
			// stop to watch, and retry after 3 seconds
			cancel()
			time.Sleep(time.Second * 3)
			goto reconnect
		}
		log.Debugf("update cron jobs")
		changedNodes := ll.UpdateCronJobs() // update cron jobs
		log.Debugf("job changed nodes is %v", changedNodes)
		go ll.BroadcastCronJobs(changedNodes) // broadcast jobs to every node
		ll.lock.Unlock()
	}
	log.Warnf("The lainlet watcher's channel was closed, retry after 3 seconds")
	cancel()
	time.Sleep(time.Second * 3)
	goto reconnect
}

// iterate the Lainlet.data, generate jobs for every node
// jobs stored into Lainlet.cronJobs
func (ll *Lainlet) UpdateCronJobs() []string {
	var (
		backupDict   = make(map[string][]BackupInfo)
		newJobs      = make(map[string][]crond.Job)
		expireAction = make(map[string][]string)
		changed      []string
	)
	for prock, pods := range ll.data.Data {
		// store the proc's name and type
		fields := strings.Split(prock, ".") // "<appname>.<proctype>.<procname>"
		if len(fields) != 3 {
			continue
		}
		appname, procname := fields[0], fields[2]
		ll.procFullName[fmt.Sprintf("%s.%s", appname, procname)] = prock

		for _, podInfo := range pods {
			var (
				annotation Annotation // redeclare every time
				cids       []string
			)
			if err := json.Unmarshal([]byte(podInfo.Annotation), &annotation); err != nil {
				log.Errorf("Fail to unmarshal annotation for %s, %s", prock, err.Error())
				continue
			}
			for _, ci := range podInfo.Containers {
				cids = append(cids, ci.Id)
			}
			ll.volumes[ll.DictKey(appname, procname)] = []string{}
			for _, b := range annotation.Backup {
				if b.Valid() {
					b.AppName = appname
					b.InstanceNo = podInfo.InstanceNo
					b.Containers = cids
					for _, cInfo := range podInfo.Containers {
						if !validIP(cInfo.NodeIp) {
							continue
						}
						backupDict[cInfo.NodeIp] = append(backupDict[cInfo.NodeIp], b)
						log.Debugf("%s having backup job %+v", cInfo.NodeIp, b)
					}
					ll.volumes[ll.DictKey(appname, procname)] = append(ll.volumes[ll.DictKey(appname, procname)], b.Volume)
				} else {
					log.Warnf("BackupSpec uncorrected, %+v", b)
				}
			}
		}
	}
	// update jobs from backup info
	for nodeIp, backups := range backupDict {
		for _, item := range backups {
			newOne := crond.Job{
				Spec:   item.Schedule,
				Action: BackupFunc,
				Args: map[string]interface{}{
					"path":       item.Dir(),
					"archive":    item.ArchiveName(),
					"instanceNo": item.InstanceNo,
					"preRun":     item.PreRun,
					"postRun":    item.PostRun,
					"containers": item.Containers,
					"app":        item.AppName,
					"proc":       item.ProcName,
					"volume":     item.Volume,
					"mode":       item.Mode,
				},
				Type: crond.TypeCron,
			}
			newOne.ID = newOne.GenerateID(nodeIp)
			newJobs[nodeIp] = append(newJobs[nodeIp], newOne)
			if item.Mode == backup.MODE_FULL {
				expireAction[nodeIp] = append(expireAction[nodeIp], item.Dir(), item.Expire)
			} else if item.Mode == backup.MODE_INCREMENT {
				expireAction[nodeIp] = append(expireAction[nodeIp], item.Dir()+"@increment", item.Expire)
			}
		}
		if len(expireAction[nodeIp]) > 0 {
			newJobs[nodeIp] = append(newJobs[nodeIp], crond.Job{
				Spec:   ExpireSchedule,
				Action: ExpireFunc,
				Args:   map[string]interface{}{"info": expireAction[nodeIp]},
				Type:   crond.TypeCron,
			})
		}

		// check if changed
		if len(ll.cronJobs[nodeIp]) != len(newJobs[nodeIp]) {
			changed = append(changed, nodeIp)
		} else {
			for i := 0; i < len(newJobs[nodeIp]); i += 1 {
				if !reflect.DeepEqual(ll.cronJobs[nodeIp][i], newJobs[nodeIp][i]) {
					changed = append(changed, nodeIp)
				}
			}
		}
	}
	ll.cronJobs = newJobs
	return changed
}

func (ll *Lainlet) BroadcastCronJobs(nodes []string) {
	var (
		addr string
	)
	for _, node := range nodes {
		addr = fmt.Sprintf("%s:%d", node, DaemonPort)
		backend := NewBackend(addr, DaemonApiPrefix)
		if err := backend.SetCronJobs(ll.cronJobs[node]); err != nil {
			log.Errorf("Fail to update cron jobs to %s, %s", node, err.Error())
		}
		if err := backend.SetNotify(Advertise + NotifyURI); err != nil {
			log.Errorf("Fail to set the notify address for %s", node)
		}
	}
}

func (ll *Lainlet) GetCoreInfo(app string) map[string][]lainlet.PodInfoForBackupctl {
	ll.lock.RLock()
	defer ll.lock.RUnlock()
	ret, prefix := make(map[string][]lainlet.PodInfoForBackupctl), app+"."
	for k, proc := range ll.data.Data {
		if strings.HasPrefix(k, prefix) {
			ret[k] = proc
		}
	}
	return ret
}

func (ll *Lainlet) VolumeAbs(app, proc string, volumes ...string) ([]string, error) {
	var ret []string
	for key, pods := range ll.GetCoreInfo(app) {
		keyFields := strings.Split(key, ".")
		if proc != keyFields[len(keyFields)-1] {
			continue
		}
		for _, pod := range pods {
			for _, volume := range volumes {
				ret = append(ret, ll.AbsDir(app, proc, pod.InstanceNo, volume))
			}
		}
	}
	return ret, nil
}

func (ll *Lainlet) GetNodes(app, proc string) ([]string, error) {
	info := ll.GetCoreInfo(app)
	if proc != "" {
		pods, ok := info[ll.DictKey(app, proc)]
		if !ok {
			return nil, fmt.Errorf("proc \"%s\" not exist in %s", proc, app)
		}
		info = map[string][]lainlet.PodInfoForBackupctl{
			proc: pods,
		}
	}
	var nodes []string
	for _, pods := range info {
		for _, pod := range pods {
			for _, container := range pod.Containers {
				nodes = append(nodes, container.NodeIp)
			}
		}
	}
	return distinct(nodes), nil
}

func (ll *Lainlet) GetNode(app, proc string, instanceNo int) (string, error) {
	pods, ok := ll.GetCoreInfo(app)[ll.DictKey(app, proc)]
	if !ok {
		return "", fmt.Errorf("proc \"%s\" not exist in %s", proc, app)
	}
	for _, pod := range pods {
		if pod.InstanceNo == instanceNo {
			return pod.Containers[0].NodeIp, nil
		}
	}
	return "", fmt.Errorf("node not exist for app=%s, proc=%s, instanceNo=%d", app, proc, instanceNo)
}

func (ll *Lainlet) DictKey(app, proc string) string {
	fullName, ok := ll.ProcFullName(app, proc)
	if !ok {
		return ""
	}
	return fullName
}

func (ll *Lainlet) AbsDir(app, proc string, instanceNo int, volume string) string {
	fullName, ok := ll.ProcFullName(app, proc)
	if !ok {
		return ""
	}
	return path.Join(VOLUME_ROOT, app, fullName,
		fmt.Sprintf("%d", instanceNo), volume)
}

func (ll *Lainlet) GetJobs() map[string][]crond.Job {
	ll.lock.RLock()
	defer ll.lock.RUnlock()
	return ll.cronJobs
}

func (ll *Lainlet) Volumes(app, proc string) ([]string, error) {
	if volume, ok := ll.volumes[ll.DictKey(app, proc)]; ok {
		return volume, nil
	}
	return nil, fmt.Errorf("%s %s having no backup volumes", app, proc)
}

/*
annotation in body shoule be like this:
{
  "mountpoint": [
    "hello.lain.local/foo",
    "hello.lain"
  ],
  "backup": [
    {
      "procname": "hello.web.web",
      "expire": "30d",
      "schedule": "0 0 5 0 0",
      "volume": "/dev/registry",
      "preRun": "./backup.sh",
      "postRun": "end.sh",
    }
  ]
}
*/
type Annotation struct {
	Mountpoint []string     `json:"mountpoint"`
	Backup     []BackupInfo `json:"backup"`
}

type BackupInfo struct {
	AppName    string   `json:"appname"`
	ProcName   string   `json:"procname"`
	Containers []string `json:"containers"`
	InstanceNo int      `json:"instanceNo"`
	Volume     string   `json:"volume"`
	Expire     string   `json:"expire"`
	Schedule   string   `json:"schedule"`
	PreRun     string   `json:"preRun"`
	PostRun    string   `json:"postRun"`
	Mode       string   `json:"mode"`
}

func (bi *BackupInfo) Dir() string {
	return path.Join(VOLUME_ROOT, bi.AppName, bi.ProcName, fmt.Sprintf("%d", bi.InstanceNo), bi.Volume)
}

func (bi *BackupInfo) ArchiveName() string {
	v := path.Join(bi.AppName, bi.ProcName, fmt.Sprintf("%d", bi.InstanceNo), bi.Volume)
	return strings.Replace(v, "/", "-", -1)
}

func (bi *BackupInfo) Valid() bool {
	return bi.ProcName != "" && bi.Volume != "" &&
		bi.Expire != "" && bi.Schedule != ""
}

func distinct(arr []string) []string {
	m := make(map[string]struct{})
	for _, item := range arr {
		m[item] = struct{}{}
	}
	if len(m) <= 0 {
		return []string{}
	}
	ret := make([]string, 0, len(m))
	for k, _ := range m {
		ret = append(ret, k)
	}
	return ret
}

func validIP(s string) bool {
	fields := strings.Split(s, ".")
	if len(fields) != 4 {
		return false
	}
	for _, field := range fields {
		if i, err := strconv.Atoi(field); err != nil {
			return false
		} else {
			if i < 0 || i > 255 {
				return false
			}
		}
	}
	return true
}
