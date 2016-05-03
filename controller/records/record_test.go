package records

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

var (
	testapp string = "testapp"
	id      string
	testDir string     = "./backupctl_records_test"
	backDir string     = "./backupctl_records_backup"
	testj   *JobRecord = &JobRecord{
		Job: Job{
			ID:     "0a648fe79d9a81ba012b2b1c7a1c7029e9050303",
			Spec:   "0 0 * * *",
			Action: "backup",
			Args: FuncArg{
				"archive": "backupctl-backupctl.web.web-1-var-backupctl",
				"containers": []string{
					"46e1b9a8daa58520c5e6ae12789caf27571e0004cfac71e09739ebbefd9a6761",
				},
				"info": []string{
					"/data/lain/volumes/backupctl/backupctl.web.web/1/var/backupctl",
					"10000d",
				},
				"instanceNo": 1,
				"mode":       "full",
				"path":       "/data/lain/volumes/backupctl/backupctl.web.web/1/var/backupctl",
				"postRun":    "",
				"preRun":     "",
				"proc":       "backupctl.web.web",
				"volume":     "/var/backupctl",
			},
			Type:  "cron",
			Sleep: false,
		},
		RecordID: "c8411d1df992c713c76685dd0a",
		Result:   nil,
		State:    StateSuccess,
		Start:    time.Now(),
		End:      time.Now(),
		Reason:   "",
	}
)

func init() {
	os.Mkdir(testDir, 0666)
	Init(testDir, backDir)
}

func TestPut(t *testing.T) {
	for i := 0; i < 100; i++ {
		testRecord := JobRecord{
			RecordID: rand.Random(),
			State:    StateSuccess,
			Start:    time.Now().Add(time.Hour * time.Duration(i)),
		}
		if err := Put(testapp, testRecord); err != nil {
			t.Error(err)
			break
		}
		id = testRecord.RecordID
	}
}

func TestGet(t *testing.T) {
	data, err := Get(testapp, 10)
	if err != nil {
		t.Error(err)
		return
	}
	for _, item := range data {
		t.Log(item)
	}
}

func TestGetById(t *testing.T) {
	data, err := GetById(testapp, id)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(data)
}

func TestBackup(t *testing.T) {
	if err := Backup(); err != nil {
		t.Error(err)
	}
}

func BenchmarkJSON(b *testing.B) {
	var tmp JobRecord
	for i := 0; i < b.N; i++ {
		content, err := json.Marshal(testj)
		if err != nil {
			b.Error(err)
		}
		if err := json.Unmarshal(content, &tmp); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkPut(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testj.RecordID = fmt.Sprintf("%dc8411d1df992c713c76685dd0a%d", i, i)
		if err := Put(testapp, *testj); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if r, err := Get(testapp, 100); err != nil {
			fmt.Println(r)
			b.Error(err)
		}
	}
}

/*
func BenchmarkPutAndGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testj.RecordID = fmt.Sprintf("%dc8411d1df992c713c76685dd0a%d", i, i)
		if err := Put(testapp, *testj); err != nil {
			b.Error(err)
		}
		if _, err := Get(testapp, 1000); err != nil {
			b.Error(err)
		}

	}
}

func BenchmarkPutXGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testj.RecordID = fmt.Sprintf("%dc8411d1df992c713c76685dd0a%d", i, i)
		if i%2 == 0 {
			if err := Put(testapp, *testj); err != nil {
				b.Error(err)
			}
		} else {
			if _, err := Get(testapp, 100); err != nil {
				b.Error(err)
			}
		}
	}
}

/*
func TestRelease(t *testing.T) {
	Release()
	if len(dbs[READ]) > 0 {
		t.Error("release failed")
	}
	if len(dbs[WRITE]) > 0 {
		t.Error("release failed")
	}
	os.RemoveAll(testDir)
}
*/
