package backup

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	APP_ROOT = "/lain/app"
)

func durationParser(s string) (time.Duration, error) {
	if len(s) == 0 {
		return time.Hour * 1000000, fmt.Errorf("empty string")
	}
	unit := s[len(s)-1]
	num, err := strconv.Atoi(s[:len(s)-1])
	if err != nil {
		return 0, err
	}
	dur := time.Minute

	switch unit {
	case 'm':
		dur = time.Minute
	case 'h':
		dur = time.Hour
	case 'd':
		dur = time.Hour * 24
	default:
		return 0, fmt.Errorf("Unknown time unit %c", unit)
		dur = time.Hour * 24
	}
	return time.Duration(num) * dur, nil
}

func dockerExec(id string, script string) error {
	fields := strings.Fields(script)
	if len(fields) == 0 { // nothing to do
		return nil
	}

	// always add /lain/app prefix if it's not a absolute path
	if !path.IsAbs(fields[0]) {
		fields[0] = path.Join(APP_ROOT, fields[0])
	}

	//get the pid of container
	pidAndEnv, err := exec.Command("docker", "inspect", "--format", "{{.State.Pid}} {{.Config.Env}}", id).CombinedOutput()
	if err != nil {
		return fmt.Errorf("Fail to get container %s's pid and environ:%s,", id, err.Error())
	}
	inspectFields := strings.Fields(string(pidAndEnv))
	lastEnv := inspectFields[len(inspectFields)-1]
	inspectFields[1] = inspectFields[1][1:] // the return value is like [ENV=1 ENV2=2], remove the `[` and `]`
	inspectFields[len(inspectFields)-1] = lastEnv[:len(lastEnv)-1]

	args := append([]string{"-t", strings.TrimSpace(inspectFields[0]), "--mount", "--uts", "--ipc", "--net", "--pid"}, fields...)
	cmd := exec.Command("nsenter", args...)
	cmd.Env = inspectFields[1:]
	log.Debugf("Run command: nsenter %v", args)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s", err.Error(), string(output))
	}
	return err
}

func fileExist(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
}

func containerNotRunning(id string) bool {
	cmd := fmt.Sprintf(`docker inspect %s | grep  "\"Running\": false\|\"Paused\": true"`, id)
	command := exec.Command("/bin/bash", "-c", cmd)
	if err := command.Run(); err != nil {
		return false
	}
	return true
}

func cloneDir(src, dest string) error {
	sh := fmt.Sprintf("rsync -rIptgo --delete-before %s %s", src+"/", dest+"/")
	cmd := exec.Command("/bin/bash", "-c", sh)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Errorf("Fail to rsync files back to source: %s. \nOutput:\n%s", cmd.Args, output)
		return err
	}
	return nil
}

func findAllFiles(root, file string, store Storage) ([]string, error) {
	var ret []string
	if file == "*" {
		tmp, err := store.List(root)
		if err != nil {
			log.Errorf("Fail to get file list in %s, %s", root, err.Error())
			return nil, err
		}
		for _, fi := range tmp {
			tmp, err := findAllFiles(root, fi.Name(), store)
			if err != nil {
				return nil, err
			}
			ret = append(ret, tmp...)
		}

	} else {
		filePath := path.Join(root, file)
		info, err := store.FileInfo(filePath)
		if err != nil {
			log.Errorf("Fail to get file info of %s, %s", filePath, err.Error())
			return nil, err
		}
		if info.IsDir() {
			return findAllFiles(filePath, "*", store)
		} else {
			ret = []string{filePath}
		}
	}
	return ret, nil
}
