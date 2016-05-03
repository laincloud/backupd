package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
)

type Meta struct {
	Entities  map[string][]Entity
	file      string
	backend   Storage
	namespace string
}

func NewMeta(backend Storage, namespace string) *Meta {
	return &Meta{
		Entities:  make(map[string][]Entity),
		file:      ".meta",
		backend:   backend,
		namespace: namespace,
	}
}

func (meta *Meta) Add(ent Entity) {
	meta.Entities[ent.Source] = append(meta.Entities[ent.Source], ent)
}
func (meta *Meta) Set(src string, ents []Entity) {
	meta.Entities[src] = ents
}

func (meta *Meta) Get(name string) *Entity {
	for _, arr := range meta.Entities {
		for _, item := range arr {
			if item.Name == name {
				return &item
			}
		}
	}
	return nil
}

func (meta *Meta) Delete(name string) {
	for k, arr := range meta.Entities {
		for i, item := range arr {
			if item.Name == name {
				meta.Entities[k] = append(meta.Entities[k][:i], meta.Entities[k][i+1:]...)
				return
			}
		}
	}
}

func (meta *Meta) Array(src ...string) []Entity {
	var ret []Entity
	if len(src) > 0 {
		for _, item := range src {
			tmp, ok := meta.Entities[item]
			if ok {
				ret = append(ret, tmp...)
			}
			tmp, ok = meta.Entities[item+"@increment"]
			if ok {
				ret = append(ret, tmp...)
			}
		}
		return ret
	}
	ret = make([]Entity, 0, 100)
	for _, arr := range meta.Entities {
		ret = append(ret, arr...)
	}
	return ret
}

func (meta *Meta) Sync() error {
	stopLock.Lock()
	defer stopLock.Unlock()
	content, err := json.Marshal(meta.Entities)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(content)
	if err := meta.backend.Upload(buf, path.Join(meta.namespace, meta.file)); err != nil {
		return fmt.Errorf("Fail to upload meta file:%s", err.Error())
	}
	return nil
}

func (meta *Meta) LoadFromBackend() error {
	var buf bytes.Buffer
	if err := driverRunning.Download(&buf, path.Join(meta.namespace, meta.file)); err != nil {
		return fmt.Errorf("Fail to download the meta data from backend, %s", err.Error())
	}
	if err := json.Unmarshal(bytes.Trim(buf.Bytes(), "\x00"), &meta.Entities); err != nil {
		panic(fmt.Sprintf("Unvalid meta file, please repaire the meta by hand, %s", err.Error()))
	}
	return nil
}
