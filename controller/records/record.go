package records

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/laincloud/backupd/crond"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

var (
	lock           sync.RWMutex // it's bad here, global lock makes serious effect to performance, but controller do not have performance problem now
	dataDir        string
	backuping      bool
	dbs            map[int]*bolt.DB
	ErrNotFound    error = fmt.Errorf("record not found")
	ErrExisted     error = fmt.Errorf("existed")
	ErrDBNotExists error = fmt.Errorf("database not exists")
)

func GetDB(year, month int, create bool) (*bolt.DB, error) {
	var (
		now time.Time = time.Now()
	)
	if year < 2015 {
		year = now.Year()
	}
	if month < 1 || month > 12 {
		month = int(now.Month())
	}
	key := year*100 + month
	dbfile := path.Join(dataDir, fmt.Sprintf("records%d.db", key))
	if db, ok := dbs[key]; ok {
		return db, nil
	}

	// check if dbfile exists
	f, err := os.Open(dbfile)
	if err != nil && !create {
		return nil, ErrDBNotExists
	}
	f.Close()

	// open database
	if tmp, err := bolt.Open(dbfile, 0600, nil); err != nil {
		return nil, err
	} else {
		dbs[key] = tmp
		return tmp, nil
	}
}

func Init(dir string) error {
	dbs, dataDir = make(map[int]*bolt.DB), dir
	if err := os.MkdirAll(dataDir, 0777); err != nil {
		return err
	}
	return nil
}

// limits is count-limit, month and year
func Get(app string, limits ...int) ([]crond.JobRecord, error) {
	var (
		records []crond.JobRecord
		tmp     crond.JobRecord
		limit   int = 20
		year    int = 0
		month   int = 0
	)
	if len(limits) > 0 && limits[0] > 0 {
		limit = limits[0]
	}
	if len(limits) > 1 {
		month = limits[1]
	}
	if len(limits) > 2 {
		year = limits[2]
	}
	db, err := GetDB(year, month, false)
	if err != nil {
		if err == ErrDBNotExists {
			return []crond.JobRecord{}, nil
		} else {
			return nil, err
		}
	}

	records = make([]crond.JobRecord, 0, limit)
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(app))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		i := 0
		for k, v := c.Last(); k != nil && i < limit; k, v = c.Prev() {
			if err := json.Unmarshal(v, &tmp); err == nil {
				records = append(records, tmp)
				i++
			}
		}
		return nil
	})
	return records, nil
}

func GetById(app, id string) (crond.JobRecord, error) {
	var (
		record = new(crond.JobRecord)
		err    error
	)
	for _, db := range dbs {
		err = db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(app))
			if b == nil {
				return ErrNotFound
			}
			if v := b.Get([]byte(id)); v != nil {
				if err := json.Unmarshal(v, record); err != nil {
					return err
				}
			}
			return nil
		})
		if err == nil {
			return *record, nil
		}
	}
	return *record, ErrNotFound
}

func Put(app string, record crond.JobRecord) error {
	if record.RecordID == "" {
		return fmt.Errorf("record id is empty")
	}
	t := record.Start
	if t.IsZero() {
		return fmt.Errorf("unvalid record, start time is zero")
	}
	db, err := GetDB(t.Year(), int(t.Month()), true)
	if err != nil {
		return err
	}

	content, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(app))
		if b == nil {
			b, err = tx.CreateBucket([]byte(app))
			if err != nil {
				return err
			}
		} else {
			// check if already exist, running state may come latter than success state, do not update if this happend
			v := b.Get([]byte(record.RecordID))
			if v != nil && record.State == crond.StateRunning {
				return nil
			}
		}
		return b.Put([]byte(record.RecordID), content)
	})
}

func Backup(to string) error {
	if backuping {
		return fmt.Errorf("Backup is already running")
	}
	lock.Lock()
	backuping = true
	defer func() {
		backuping = false
		lock.Unlock()
	}()

	if err := os.MkdirAll(to, 0777); err != nil {
		return err
	}

	dir, err := os.Open(dataDir)
	if err != nil {
		return err
	}
	names, err := dir.Readdirnames(0)
	if err != nil {
		return err
	}
	for _, name := range names {
		if len(name) < 14 {
			continue
		}
		year, month := name[7:11], name[11:13]
		yeari, err := strconv.Atoi(year)
		if err != nil {
			continue
		}
		monthi, err := strconv.Atoi(month)
		if err != nil {
			continue
		}
		db, err := GetDB(yeari, monthi, false)
		if err != nil {
			return err
		}
		err = db.View(func(tx *bolt.Tx) error {
			return tx.CopyFile(path.Join(to, name), 0666)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func Release() {
	for k, db := range dbs {
		if db != nil {
			db.Close()
		}
		delete(dbs, k)
	}
}
