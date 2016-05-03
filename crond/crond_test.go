package crond

import (
	"fmt"
	"testing"
	"time"
)

func testf(args FuncArg) (FuncResult, error) {
	fmt.Println(args.GetString("test", "no thing"))
	return nil, nil
}

func TestRegister(t *testing.T) {
	if err := Register("test", testf); err != nil {
		t.Error(err)
	}
}

func TestUpdate(t *testing.T) {
	jobs := []Job{
		Job{
			Spec:   "* * * * * *",
			Action: "test",
			Args: map[string]interface{}{
				"test": "job one",
			}},
		Job{
			Spec:   "* * * * * *",
			Action: "test",
			Args: map[string]interface{}{
				"test": "job two",
			}},
	}
	if err := Update(jobs, ""); err != nil {
		t.Error(err)
	}
}

func TestEntries(t *testing.T) {
	ret := Entries(nil)
	for _, item := range ret {
		fmt.Println(item)
	}
}

func TestCronSleep(t *testing.T) {
	time.Sleep(time.Second * 1)
	data := Entries(nil)
	Sleep(data[0].J.ID, true)
	fmt.Println("***** job one was set into sleep *****")
	time.Sleep(time.Second * 3)
	Sleep(data[0].J.ID, false)
	fmt.Println("***** job one was set into alive *****")
}

func TestRawOnce(t *testing.T) {
	if _, err := RawOnce("test", map[string]interface{}{
		"test": "hello world",
	}); err != nil {
		t.Error(err)
	}
}

func TestOnce(t *testing.T) {
	job, err := Find("test", map[string]interface{}{
		"test": "job one",
	})
	if err != nil {
		t.Error(err)
		return
	}
	Once(job)
}

func TestStop(t *testing.T) {
	time.Sleep(time.Second * 5)
	Stop()
	println("crond stoped now, there should be no any output, wait 2 second")
	time.Sleep(time.Second * 2)
}

func TestParseIPFromID(t *testing.T) {
	ip, err := ParseIPFromID("c0a84d1574cfe65db33faf0ffe98abae12f73579")
	if err != nil {
		t.Error(err)
	}
	if ip != "192.168.77.21" {
		t.Error("parsed not correct: %s != %s", ip, "192.168.77.21")
	}
	fmt.Println("Parsed ip is:", ip)
}
