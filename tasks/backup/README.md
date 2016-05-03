备份功能

备份使用的后台Storage以插件的形式集成到功能中

实现的driver必须满足下面接口:

```golang
type Storage interface {
	// the storage's name
	Name() string

	Upload(file string) error

	Download(file, localFile string) error

	List() ([]string, error)

	Delete(file string) error
}
```
实现`Storage`接口，然后调用`crond.Register()`注册到crond服务上.
