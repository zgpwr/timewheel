# timewheel
golang实现的时间轮

### 实现
循环数组 + 大顶堆

### 用途
1. 延迟任务
2. 心跳失效检测

### 单元测试
go test timewheel_test.go -v

### 使用
```go
package main

import (
	"fmt"
	"github.com/zgpwr/timewheel"
	"time"
)

type TaskParam struct {
	StartTime time.Time
	Name      string
}

func main() {
	th := timewheel.NewTimeWheel(time.Second, 60)
	th.Start()

	handle := func(data interface{}) {
		param := data.(TaskParam)
		now := time.Now().Unix()
		fmt.Printf("exec task %s at %d, delay %d s\n", param.Name, now, now-param.StartTime.Unix())
	}
	th.AddTask(time.Second*5, handle, TaskParam{StartTime: time.Now(), Name: "1"})
	th.AddTask(time.Minute, handle, TaskParam{StartTime: time.Now(), Name: "2"})
	jobId, _ := th.AddTask(time.Second*30, handle, TaskParam{StartTime: time.Now(), Name: "3"})
	time.Sleep(time.Second * 20)
	th.DelTask(jobId)

	time.Sleep(time.Minute * 2)

	th.Stop()
}

```
