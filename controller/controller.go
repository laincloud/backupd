package controller

import (
	"errors"
	"fmt"
	api "github.com/laincloud/backupd/api/v1"
	"github.com/laincloud/backupd/crond"
)

type Controller struct {
	App string
	let *Lainlet
}

func NewController(app string, lainlet *Lainlet) *Controller {
	return &Controller{
		App: app,
		let: lainlet,
	}
}

func (c *Controller) GetBackup(proc string, volumes ...string) ([]BackupEntity, error) {
	var data []BackupEntity
	nodes, err := c.let.GetNodes(c.App, proc)
	if err != nil {
		return nil, err
	}

	absVolumes, err := c.let.VolumeAbs(c.App, proc, volumes...)
	if err != nil {
		return nil, err
	}

	for _, node := range nodes {
		tmp, err := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix).GetBackup(absVolumes...)
		if err != nil {
			return nil, err
		}
		data = append(data, tmp...)
	}
	return data, nil
}

func (c *Controller) DeleteBackup(proc string, files []string) error {
	var (
		err   error
		nodes []string
	)
	nodes, err = c.let.GetNodes(c.App, proc)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		backend := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix)
		if err := backend.BackupDelete(files); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) GetCronJobs(query map[string]string) ([]crond.EntrySpec, error) {
	nodes, err := c.let.GetNodes(c.App, "")
	if err != nil {
		return nil, err
	}
	var ret []crond.EntrySpec
	for _, node := range nodes {
		tmp, err := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix).GetCronJobs(c.App, query)
		if err != nil {
			return nil, err
		}
		ret = append(ret, tmp...)
	}
	return ret, nil
}

func (c *Controller) GetCronRecords(query map[string]string) ([]crond.JobRecord, error) {
	nodes, err := c.let.GetNodes(c.App, "")
	if err != nil {
		return nil, err
	}
	var ret []crond.JobRecord
	for _, node := range nodes {
		tmp, err := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix).GetCronRecords(c.App, query)
		if err != nil {
			return nil, err
		}
		ret = append(ret, tmp...)

	}
	return ret, nil
}

func (c *Controller) GetCronRecordById(id string) (crond.JobRecord, error) {
	var (
		ret crond.JobRecord
		err error
	)
	nodes, err := c.let.GetNodes(c.App, "")
	if err != nil {
		return ret, err
	}
	for _, node := range nodes {
		ret, err = NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix).GetCronRecordById(id)
		if err == nil {
			return ret, nil
		}

	}
	return ret, errors.New("record not found by id " + id)
}

func (c *Controller) IncrementBackupRecover(proc, volume string, from, to int, backupDir string, files []string) (string, error) {
	node, err := c.let.GetNode(c.App, proc, to)
	if err != nil {
		return "", err
	}
	namespace, err := c.let.GetNode(c.App, proc, from)
	if err != nil {
		return "", err
	}
	volumeAbs := ""
	if volume != "" {
		volumeAbs = c.let.AbsDir(c.App, proc, to, volume)
	}
	args := map[string]string{
		"app":        c.App,
		"instanceNo": fmt.Sprintf("%d", to),
		"proc":       proc,
	}
	backend := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix)
	id, err := backend.BackupRecoverIncrement(namespace, backupDir, volumeAbs, files, args)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (c *Controller) BackupRecover(proc, volume, file string, from int, to int) (string, error) {
	node, err := c.let.GetNode(c.App, proc, to)
	if err != nil {
		return "", err
	}
	namespace, err := c.let.GetNode(c.App, proc, from)
	if err != nil {
		return "", err
	}
	volumeAbs := ""
	if volume != "" {
		volumeAbs = c.let.AbsDir(c.App, proc, to, volume)
	}
	backend := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix)
	id, err := backend.BackupRecover(namespace, file, volumeAbs, map[string]string{
		"app":  c.App,
		"proc": proc,
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (c *Controller) CronOnce(id string) (string, error) {
	node, err := crond.ParseIPFromID(id)
	if err != nil {
		return "", err
	}
	backend := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix)
	rid, err := backend.CronOnce(id)
	if err != nil {
		return "", err
	}
	return rid, nil
}

func (c *Controller) IncrementBackupFileList(proc, dir string) ([]api.FInfo, error) {
	nodes, err := c.let.GetNodes(c.App, proc)
	if err != nil {
		return nil, err
	}

	for _, node := range nodes {
		backend := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix)
		l, err := backend.IncrementBackupFileList(dir)
		if err != nil {
			return nil, err
		}
		if len(l) > 0 {
			return l, nil
		}
	}
	return []api.FInfo{}, nil
}

func (c *Controller) BackupFileInfo(proc, file string) (BackupEntity, error) {
	var (
		ret   BackupEntity
		err   error
		nodes []string
	)
	nodes, err = c.let.GetNodes(c.App, proc)
	if err != nil {
		return ret, err
	}
	for _, node := range nodes {
		backend := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix)
		ret, err = backend.BackupInfo(file)
		if err == nil {
			return ret, nil
		}
	}
	return ret, errors.New("Can not find backup file by name " + file)
}

func (c *Controller) GetCronJob(id string) (crond.Job, error) {
	var job crond.Job
	node, err := crond.ParseIPFromID(id)
	if err != nil {
		return job, err
	}
	backend := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix)
	job, err = backend.GetCronJob(id)
	if err != nil {
		return job, err
	}
	return job, nil
}

func (c *Controller) CronAction(id, action string) (string, error) {
	node, err := crond.ParseIPFromID(id)
	if err != nil {
		return "", err
	}
	backend := NewBackend(fmt.Sprintf("%s:%d", node, DaemonPort), DaemonApiPrefix)
	return backend.CronAction(id, action)
}
