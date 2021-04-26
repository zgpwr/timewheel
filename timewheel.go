package timewheel

import (
	"container/heap"
	// "fmt"
	"github.com/google/uuid"
	"math"
	"time"
)

type Handler func(interface{})

type TimeWheel struct {
	// 时间间隔 s
	interval time.Duration
	// 计时器
	ticker *time.Ticker
	// 槽的数量
	slotNum int
	// 每个槽是一个大顶堆
	slots []*TaskHeap
	// 指针当前位置
	curPos int
	// taskMap，用于快速查找当前task所在的槽
	taskMap     map[string]int
	addTaskChan chan Task
	delTaskChan chan string
	stopChan    chan bool
	isRun       bool
}

type Task struct {
	// unique key
	id string
	// exec func
	handler Handler
	data    interface{}
	// 轮数，等于0时才被执行
	circle int
	// 延时执行时间
	delay time.Duration
}

// task 大顶堆
type TaskHeap []Task

func (h TaskHeap) Len() int            { return len(h) }
func (h TaskHeap) Less(i, j int) bool  { return h[i].circle > h[j].circle }
func (h TaskHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *TaskHeap) Push(v interface{}) { *h = append(*h, v.(Task)) }
func (h *TaskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func NewTimeWheel(interval time.Duration, slotNum int) *TimeWheel {
	tw := &TimeWheel{
		interval:    interval,
		slotNum:     slotNum,
		curPos:      0,
		slots:       make([]*TaskHeap, slotNum),
		taskMap:     make(map[string]int),
		addTaskChan: make(chan Task),
		delTaskChan: make(chan string),
		stopChan:    make(chan bool),
	}

	tw.initSlots()

	return tw
}

func (tw *TimeWheel) Start() {
	if tw.isRun {
		return
	}
	tw.ticker = time.NewTicker(tw.interval)
	go tw.run()
	// fmt.Printf("TimeWheel start at %d\n", time.Now().Unix())
}

func (tw *TimeWheel) Stop() {
	if !tw.isRun {
		return
	}
	tw.stopChan <- true
	// fmt.Printf("TimeWheel stop at %d\n", time.Now().Unix())
}

func (tw *TimeWheel) AddTask(delay time.Duration, h Handler, data interface{}) (string, bool) {
	if delay < tw.interval {
		return "", false
	}
	taskId := uuid.New().String()
	task := Task{id: taskId, delay: delay, handler: h, data: data}
	tw.addTaskChan <- task
	return taskId, true
}

func (tw *TimeWheel) DelTask(taskId string) {
	if taskId == "" {
		return
	}
	tw.delTaskChan <- taskId
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		h := &TaskHeap{}
		heap.Init(h)
		tw.slots[i] = h
	}
}

func (tw *TimeWheel) run() {
	tw.isRun = true
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandle()
		case task := <-tw.addTaskChan:
			tw.addTask(&task)
		case taskKey := <-tw.delTaskChan:
			tw.delTask(taskKey)
		case <-tw.stopChan:
			tw.ticker.Stop()
			break
		}
	}
	tw.isRun = false
}

func (tw *TimeWheel) tickHandle() {
	// fmt.Printf("TimeWheel tickHandle at %d\n", time.Now().Unix())
	if tw.curPos == tw.slotNum-1 {
		tw.curPos = 0
	} else {
		tw.curPos++
	}
	taskHeap := tw.slots[tw.curPos]
	go tw.handleTasks(taskHeap)
}

func (tw *TimeWheel) handleTasks(taskHeap *TaskHeap) {
	n := len(*taskHeap)
	if n <= 0 {
		return
	}
	for len(*taskHeap) > 0 {
		task := heap.Pop(taskHeap).(Task)
		if task.circle == 0 {
			go task.handler(task.data)
		} else {
			heap.Push(taskHeap, task)
			break
		}
	}

	for i := 0; i < len(*taskHeap); i++ {
		(*taskHeap)[i].circle--
	}
}

func (tw *TimeWheel) addTask(task *Task) (bool, string) {
	if task.delay <= 0 {
		return false, ""
	}
	circle, pos := tw.getTaskPos(task)
	if task.id == "" {
		task.id = uuid.New().String()
	}
	task.circle = circle
	taskHeap := tw.slots[pos]
	heap.Push(taskHeap, *task)
	tw.taskMap[task.id] = pos
	// fmt.Printf("add task: %s at %d\n", task.id, time.Now().Unix())
	return true, task.id
}

func (tw *TimeWheel) getTaskPos(task *Task) (circle int, pos int) {
	delaySec := int(task.delay.Seconds())
	intervalSec := int(tw.interval.Seconds())
	// 向上取整
	delay := int(math.Ceil(float64(delaySec) / float64(intervalSec)))
	circle = int(delay / tw.slotNum)
	pos = int(tw.curPos+delay) % tw.slotNum
	return circle, pos
}

func (tw *TimeWheel) delTask(taskId string) {
	pos, ok := tw.taskMap[taskId]
	if !ok {
		return
	}
	taskHeap := tw.slots[pos]
	n := len(*taskHeap)
	if n <= 0 {
		return
	}
	idx := -1
	for i := 0; i < n; i++ {
		if (*taskHeap)[i].id == taskId {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	heap.Remove(taskHeap, idx)
	delete(tw.taskMap, taskId)
	// fmt.Printf("del task: %s at %d\n", taskId, time.Now().Unix())
}
