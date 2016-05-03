package crond

import (
	"bytes"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"net/http"
	"sync"
	"time"
)

var (
	lock  sync.RWMutex
	addrs []string
)

func AddNotifyAddr(addr string) {
	lock.Lock()
	defer lock.Unlock()
	for _, item := range addrs {
		if item == addr {
			return
		}
	}
	addrs = append(addrs, addr)
}
func RemoveNotifyAddr(addr string) {
	lock.Lock()
	defer lock.Unlock()
	for i := 0; i < len(addrs); i++ {
		if addrs[i] == addr {
			addrs = append(addrs[0:i], addrs[i+1:]...)
		}
	}
}

func NotifyAddrs() []string {
	lock.RLock()
	defer lock.RUnlock()
	return addrs
}

func notify(data interface{}) {
	if len(addrs) == 0 {
		log.Debugf("notify address is empty, ignore.")
		return
	}
	lock.RLock()
	defer lock.RUnlock()
	remotes := make([]string, len(addrs))
	copy(remotes, addrs)
	if content, err := json.Marshal(data); err == nil {
		for _, addr := range remotes {
			if addr == "" {
				continue
			}
			go func(address string) {
				// retry 3 times, request have 1 minute timeout
				for i := 0; i < 3; i++ {
					if resp, err := http.Post(address, "application/json", bytes.NewBuffer(content)); err != nil {
						log.Warnf("Fail to notify %v to %s, %s, retried %d times, ", data, addr, err.Error(), i)
					} else if resp.StatusCode >= 500 {
						log.Warnf("Fail to notify %v to %s, return %s, retried %d times", data, addr, resp.Status, i)
					} else {
						break
					}
					time.Sleep(time.Minute)
				}
			}(addr)
		}
	}
}
