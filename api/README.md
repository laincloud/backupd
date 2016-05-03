API模块
====

提供对外的Restful API以实现对backupd的状态查看和动作执行

一下的api都需要加上前缀`/api/v1`

##Cron

### 更新cron任务(幂等)

```
PUT /cron/jobs -d <json string>
```

### 获取cron任务列表

```
GET /cron/jobs

```
### 获取一个cron任务的信息

```
GET /cron/jobs/:id
```

### 对一个cron任务执行指定动作
目前 action 支持`run`, `sleep`, `wakeup`

```
POST /cron/jobs/:id/actions/:action
```
- `run` 等同once, 立刻执行一个cron任务
- `sleep` 将任务置于睡眠态,cron不会对sleep的任务进行调度
- `wakeup` 唤醒, sleep的逆操作

### 停止cron调度(幂等)

```
PUT /cron/stop
```

### 开启cron调度(幂等)

```
PUT /cron/start
```

### 获取调度记录

```
GET /cron/records
```

### 获取指定id的调度记录

```
GET /cron/record/:rid
```

### 即刻执行一个调度任务

```
POST /cron/once/:id
```

## Backup

### 备份列表

```
GET /backup/json
```

### 一个备份的详细信息

```
GET /backup/info/file/:name
```

### 备份目录下得文件(递增备份专用)

```
GET /backup/filelist/dir/:dir
```

### 删除备份

```
POST /backup/delete -d files=<f1> -d files2=<f2>
```

### 恢复一个全量备份

```
POST /backup/full/recover/file/:file
```

### 恢复增量备份目录下的某个文件

```
POST /backup/increment/recover/dir/:dir -d files=<f1> -d files=<f2>
```
