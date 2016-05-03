package backup

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/laincloud/backupd/crond"
	"time"
)

// the task function called by crond
// {
//     "path": string	    directory path backuped
//     "archive": string	    backup-file's name
//     "instanceNo": int	    instance number
//     "preRun": string	    script run in docker before backup
//     "postRun": string	    script run in docker after backup
//     "containers": []string  docker container ids
//     "mode": increment or mode
// }
func backup(args crond.FuncArg) (crond.FuncResult, error) {
	path := args.GetString("path", "")
	archive := args.GetString("archive", "")
	instanceNo := args.GetInt("instanceNo", 0)
	preRun := args.GetString("preRun", "")
	postRun := args.GetString("postRun", "")
	containers := args.GetStringSlice("containers", []string{})
	volume := args.GetString("volume", "")
	mode := args.GetString("mode", MODE_FULL)

	// check path
	if !fileExist(path) {
		log.Errorf("Directory %s not exist, can not bakcup it", path)
		return nil, fmt.Errorf("Directory %s not exist", path)
	}

	// if it's doing recovering or backuping for <path>, give up
	if err := bstats.Set(path, StateBackuping); err != nil {
		return nil, err
	}
	defer bstats.Free(path)

	log.Infof("Running a backup task for %s", path)

	// run before
	if preRun != "" {
		for _, cid := range containers {
			if err := dockerExec(cid, preRun); err != nil {
				return nil, fmt.Errorf("preHook %s in %s run failed: %s", preRun, cid, err.Error())
			}
		}
	}

	// run backup
	entity := NewEntity(path, archive, instanceNo, containers, volume, mode)
	var err error
	switch entity.Mode {
	case MODE_INCREMENT:
		err = entity.IncrementBackup()
	default:
		err = entity.Backup(driverRunning)
	}
	if err != nil {
		return nil, err
	}

	// run after
	if postRun != "" {
		for _, cid := range containers {
			if err := dockerExec(cid, postRun); err != nil {
				return nil, fmt.Errorf("postHook %s run failed: %s", preRun, err.Error())
			}
		}
	}
	return crond.FuncResult{
		"file": entity.Name,
		"size": entity.Size,
	}, nil
}

func expire(args crond.FuncArg) (crond.FuncResult, error) {
	info := args.GetStringSlice("info", []string{})

	log.Infof("Running a backup expire task")
	expireMap := make(map[string]time.Duration)
	for i := 0; i < len(info); i += 2 {
		dur, err := durationParser(info[i+1])
		if err != nil {
			log.Warnf("Fail to parse backup's exipre setting %s:%s, abandon",
				info[i+1], err.Error())
			continue
		}
		expireMap[info[i]] = dur
	}

	now := time.Now()
	counter := 0
	for _, item := range meta.Array() {
		if item.Mode != MODE_INCREMENT { // do not support expire for increment backup
			expireTime, ok := expireMap[item.Source]
			if ok && now.Sub(item.Created) > expireTime { // expired
				counter++
				log.Debugf("Backup %s expired, delete it", item.Name)
				if err := Delete(item.Name); err != nil {
					log.Warnf("Fail to delete backup file %s:%s", item.Name, err.Error())
				}
			}
		}
	}
	log.Infof("Backup expire task finished, %d file deleted", counter)
	return nil, nil
}

func backup_recover(args crond.FuncArg) (crond.FuncResult, error) {
	ns := args.GetString("namespace", "")
	file := args.GetString("backup", "")
	destDir := args.GetString("destDir", "")
	if file == "" {
		return nil, fmt.Errorf("Empty recover file")
	}

	log.Infof("Recovering from %s/%s", ns, file)
	var mt *Meta
	// namespace != ns means it's a migrate, move backup from other server
	if ns != namespace && ns != "" {
		mt := NewMeta(driverRunning, ns)
		if err := mt.LoadFromBackend(); err != nil {
			return nil, fmt.Errorf("Fail to read meta data from backend, %s", err.Error())
		}
	} else {
		ns, mt = namespace, meta
	}
	ent := mt.Get(file)
	if ent == nil {
		return nil, fmt.Errorf("Unkown backup file %s in %s", file, ns)
	}

	if destDir != "" {
		ent.Source = destDir
	}

	// if it's now recovering or backuping for <path>, give up
	if err := bstats.Set(ent.Source, StateBackuping); err != nil {
		return nil, err
	}
	defer bstats.Free(ent.Source)

	if ent.Mode == MODE_INCREMENT {
		files := args.GetStringSlice("files", []string{})
		log.Debugf("Increment backup, recover files %v", files)
		return nil, ent.IncrementRecover(files)
	}
	return nil, ent.Recover(driverRunning, ns, file)
}
