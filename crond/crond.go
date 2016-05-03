package crond

import (
	"crypto/md5"
	"fmt"
	log "github.com/Sirupsen/logrus"
	cron "gopkg.in/robfig/cron.v2"
	"math"
	"net"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StateRunning = "running"
	StateSuccess = "success"
	StateFail    = "failed"

	TypeCron = "cron"
	TypeOnce = "once"
)

var (
	crond             *Crond = nil
	jobRecordQueryReg *regexp.Regexp
	ip                string
	RunningCount      int32 = 0
	randCounter             = 0
)

func init() {
	crond = New()
	jobRecordQueryReg, _ = regexp.Compile(`args_\w+`)
}

func Init(localIP string) {
	ip = localIP
}

// Cron task's func type and arg type and result type
type Func func(FuncArg) (FuncResult, error)
type FuncArg map[string]interface{}
type FuncResult map[string]interface{}

func (fa FuncArg) GetString(key string, defaultv string) string {
	iv, ok := fa[key]
	if !ok {
		return defaultv
	}
	ret, ok := iv.(string)
	if !ok {
		return defaultv
	}
	return ret
}
func (fa FuncArg) GetInt(key string, defaultv int) int {
	iv, ok := fa[key]
	if !ok {
		return defaultv
	}
	switch iv.(type) {
	case int:
		return iv.(int)
	case string:
		if i, err := strconv.Atoi(iv.(string)); err != nil {
			return i
		}
	case float64:
		return int(iv.(float64))
	case float32:
		return int(iv.(float32))
	}
	return defaultv
}

func (fa FuncArg) GetStringSlice(key string, defaultv []string) []string {
	iv, ok := fa[key]
	if !ok {
		return defaultv
	}
	switch iv.(type) {
	case []string:
		return iv.([]string)
	case []interface{}:
		var arr []string
		for _, item := range iv.([]interface{}) {
			if tmp, ok := item.(string); ok {
				arr = append(arr, tmp)
			}
		}
		return arr
	}
	return defaultv
}

type JobState string
type JobType string

type Job struct {
	id     cron.EntryID
	ID     string  `json:"id"`
	Spec   string  `json:"spec"`   // string like "0 0 * * *"
	Action string  `json:"action"` // task's name
	Args   FuncArg `json:"args"`
	Type   JobType `json:"type"` // cronJob or onceJob
	Sleep  bool    `json:"sleep"`
}

func (job *Job) GenerateID(ip string) string {
	keys := make([]string, 0, 10)
	values := make([]interface{}, 0, 10)
	for k, _ := range job.Args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		values = append(values, job.Args[k])
	}
	sumv := md5.Sum([]byte(fmt.Sprintf("%s/%s/%v", job.Spec, job.Action, values)))
	content := []byte(append(net.ParseIP(ip).To4()[:], sumv[:]...))
	return fmt.Sprintf("%x", content)
}

func (job *Job) Match(query map[string]string) bool {
	if query == nil {
		return true
	}
	for k, v := range query {
		switch {
		case k == "id":
			if v != job.ID {
				return false
			}
		case k == "type":
			if JobType(v) != job.Type {
				return false
			}
		case k == "action":
			if v != job.Action {
				return false
			}
		case jobRecordQueryReg.Match([]byte(k)):
			value, ok := job.Args[k[5:]]
			if !ok {
				return false
			}
			if v2, _ := value.(string); v2 != v {
				return false
			}
		default:
			// unkown search key, ignore it
			continue
		}
	}
	return true
}

type JobRecord struct {
	Job
	RecordID string     `json:"rid"`
	Result   FuncResult `json:"result"`
	State    JobState   `json:"state"`
	Start    time.Time  `json:"start"`
	End      time.Time  `json:"end"`
	Reason   string     `json:"reason"`
}

func (record *JobRecord) Value() interface{} {
	return record
}

func (record *JobRecord) Match(query map[string]string) bool {
	if query == nil {
		return true
	}
	rid, ok := query["rid"]
	if ok && rid != record.RecordID {
		return false
	}
	state, ok := query["state"]
	if ok && JobState(state) != record.State {
		return false
	}
	return record.Job.Match(query)
}

type Crond struct {

	// taskName => taskFunc
	// other module could call crond.Register() to register a func to crond
	// registered func recorded in tasks
	// after func registered into tasks, cronList will support the name
	functions map[string]Func

	// running is the jobs scheduled by cron
	jobs []*Job

	Version string

	// cron service
	scheduler *cron.Cron

	// locker used for update runningJobs and functions
	locker sync.Mutex

	// mark for scheduler's running stat, it started in init(), so this is true
	started bool
}

type EntrySpec struct {
	Prev time.Time `json:"prev"`
	Next time.Time `json:"next"`
	J    Job       `json:"job"`
}

func New() *Crond {
	ret := &Crond{
		functions: make(map[string]Func),
		scheduler: cron.New(),
		jobs:      make([]*Job, 0),
		started:   false,
	}
	return ret
}

// TODO job record should be persistent, and support search
func (cd *Crond) WrapFunc(job *Job, rid string) func() {
	return func() {
		if job.Sleep && job.Type == TypeCron {
			return
		}
		atomic.AddInt32(&RunningCount, 1)
		defer atomic.AddInt32(&RunningCount, -1)

		if job.Type == TypeCron { // crontype job do not use given rid, generate random rid for each run
			rid = fmt.Sprintf("%d%s", time.Now().Unix(), random())
		}
		jr := &JobRecord{
			Job:      *job,
			RecordID: rid,
			Result:   nil,
			Start:    time.Now(),
			State:    StateRunning,
		}
		notify(jr) // notify the record

		defer func(jr *JobRecord) {
			jr.End = time.Now()
			if r := recover(); r != nil {
				log.Warnf("Task run failed, %v, %v", job.Action, r)
				jr.State = StateFail
				jr.Reason = fmt.Sprintf("%v", r)
			} else {
				jr.State = StateSuccess
			}
			notify(jr) // notify the record
		}(jr)

		result, err := cd.functions[job.Action](job.Args)
		if err != nil {
			panic(err)
		}
		jr.Result = result
	}
}

func (cd *Crond) RawOnce(name string, args FuncArg) (string, error) {
	if _, ok := cd.functions[name]; !ok {
		return "", fmt.Errorf("Unknown task name \"%s\"", name)
	}
	job := &Job{
		Action: name,
		Args:   args,
		Type:   TypeOnce,
	}
	rid := fmt.Sprintf("%d%s", time.Now().Unix(), random())
	go cd.WrapFunc(job, rid)()
	return rid, nil
}

func (cd *Crond) Once(job *Job) string {
	tmp := *job
	tmp.Type = TypeOnce
	rid := fmt.Sprintf("%d%s", time.Now().Unix(), random())
	go cd.WrapFunc(&tmp, rid)()
	return rid
}

func (cd *Crond) FindById(id string) (*Job, error) {
	// TODO, deepcopy a new one to return
	for _, item := range cd.jobs {
		if item.ID == id {
			return item, nil
		}
	}
	return nil, fmt.Errorf("Job not found with id=\"%s\"", id)
}

func (cd *Crond) Find(name string, args FuncArg) (*Job, error) {
	var (
		ok     bool
		tmpArg FuncArg = make(FuncArg)
	)
	// TODO deepcopy to return
	for _, item := range cd.jobs {
		if item.Action != name {
			continue
		}
		if args == nil {
			return item, nil
		}
		for k, _ := range args {
			tmpArg[k], ok = item.Args[k]
			if !ok {
				goto next
			}
		}
		if reflect.DeepEqual(tmpArg, args) {
			return item, nil
		}
	next:
	}
	return nil, fmt.Errorf("Job named \"%s\" not found", name)
}

// update the job list
func (cd *Crond) Update(jobs []Job, version string) error {
	// If having job will be run soon, wait for it
	// Because update action need to stop scheduler and start again, the time point may passed during this,
	// then the job must wait for next loop, lost a running change for this loop.
	// So, only update job when cron-scheduler is idle
	for {
		entries := cd.Entries(nil)
		if len(entries) > 0 && entries[0].Next.Sub(time.Now()) < 5*time.Second {
			// there may be some jobs running in 5 seconds, wait their scheduled
			// give cron 15 seconds to finish scheduling the jobs, then we update
			time.Sleep(time.Second)
			continue
		}
		break
	}

	cd.locker.Lock()
	defer cd.locker.Unlock()

	cd.Stop()
	defer cd.Start()

	var err error

	// we should remember pause stats for every job
	// and reset the pause stat for jobs which exist before
	pauseInfo := make(map[string]bool)
	// remove all the jobs
	for _, job := range cd.jobs {
		pauseInfo[job.ID] = job.Sleep
		cd.scheduler.Remove(job.id)
	}

	cd.jobs = make([]*Job, 0, len(jobs))

	// add all the new jobs
	for i, _ := range jobs {
		if _, ok := cd.functions[jobs[i].Action]; !ok {
			log.Warnf("Unkown function %s", jobs[i].Action)
			continue
		}
		jobs[i].Type = TypeCron
		// reset the sleep stat
		if s, ok := pauseInfo[jobs[i].ID]; ok {
			jobs[i].Sleep = s
		} else {
			jobs[i].Sleep = false
		}
		if jobs[i].id, err = cd.scheduler.AddFunc(jobs[i].Spec, cd.WrapFunc(&jobs[i], "")); err == nil {
			cd.jobs = append(cd.jobs, &jobs[i])
		} else {
			// fail to add job, the spec may be not correct
			log.Warnf("Fail to add job, %s", err.Error())
		}
	}
	cd.Version = version
	return nil
}

// start scheduler
func (cd *Crond) Start() {
	if !cd.started {
		cd.scheduler.Start()
		cd.started = true
	}
}

// stop scheduler
func (cd *Crond) Stop() {
	if cd.started {
		cd.scheduler.Stop()
		cd.started = false
	}
}

func (cd *Crond) Register(name string, f Func) error {
	cd.locker.Lock()
	defer cd.locker.Unlock()
	if _, ok := cd.functions[name]; ok {
		return fmt.Errorf("function named %s already exist")
	}
	cd.functions[name] = f
	return nil
}

func (cd *Crond) Entries(query map[string]string) []EntrySpec {
	cd.locker.Lock()
	defer cd.locker.Unlock()

	var (
		ret     []EntrySpec = make([]EntrySpec, 0, 100)
		jobIter Job
	)

	for _, entry := range cd.scheduler.Entries() {
		for _, job := range cd.jobs {
			if entry.ID == job.id {
				jobIter = *job
				break
			}
		}
		if !jobIter.Match(query) {
			continue
		}
		ret = append(ret, EntrySpec{
			Prev: entry.Prev,
			Next: entry.Next,
			J:    jobIter,
		})
	}
	return ret
}

func (cd *Crond) Sleep(ID string, sleep bool) {
	for _, item := range cd.jobs {
		if item.ID == ID {
			item.Sleep = sleep
			return
		}
	}
	log.Warnf("Unkown job id %s", ID)
}

func (cd *Crond) Count() int {
	cd.locker.Lock()
	defer cd.locker.Unlock()
	return len(cd.jobs)
}

func Start() {
	crond.Start()
}

func Stop() {
	crond.Stop()
}

func Status() string {
	if crond.started {
		return "started"
	}
	return "stopped"
}

func Update(jobs []Job, version string) error {
	return crond.Update(jobs, version)
}

func Entries(query map[string]string) []EntrySpec {
	return crond.Entries(query)
}

func Register(name string, f Func) error {
	return crond.Register(name, f)
}

func RawOnce(name string, args FuncArg) (string, error) {
	return crond.RawOnce(name, args)
}

func Find(name string, args FuncArg) (*Job, error) {
	return crond.Find(name, args)
}

func FindById(id string) (*Job, error) {
	return crond.FindById(id)
}

func Sleep(ID string, sleep bool) {
	crond.Sleep(ID, sleep)
}

func Once(job *Job) string {
	return crond.Once(job)
}

func Count() int {
	return crond.Count()
}

func Version() string {
	return crond.Version
}

func ParseIPFromID(id string) (string, error) {
	b1, err := strconv.ParseUint(id[:2], 16, 8)
	if err != nil {
		return "", fmt.Errorf("unvalid id")
	}
	b2, err := strconv.ParseUint(id[2:4], 16, 8)
	if err != nil {
		return "", fmt.Errorf("unvalid id")
	}
	b3, err := strconv.ParseUint(id[4:6], 16, 8)
	if err != nil {
		return "", fmt.Errorf("unvalid id")
	}
	b4, err := strconv.ParseUint(id[6:8], 16, 8)
	if err != nil {
		return "", fmt.Errorf("unvalid id")
	}
	return net.IPv4(byte(b1), byte(b2), byte(b3), byte(b4)).String(), nil
}

func random() string {
	if randCounter >= math.MaxInt16 {
		randCounter = 0
	}
	randCounter += 1
	s := md5.Sum([]byte(fmt.Sprintf("%d%d", time.Now().UnixNano(), randCounter)))
	return fmt.Sprintf("%x", s)
}
