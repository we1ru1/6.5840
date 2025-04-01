package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 自定义error：没有找到对应的task
var TaskNotFoundErr = errors.New("Task Not Found")

// 任务状态：pending in-progress completed unallocated all-completed
const (
	PENDING       = "pending"
	IN_PROGRESS   = "in-progress"
	COMPLETED     = "completed"
	UNALLOCATED   = "unallocated"
	ALL_COMPLETED = "all-completed"
)

// Coordinator结构
type Coordinator struct {
	// Your definitions here.
	files       []string
	phase       string
	nmap        int
	nreduce     int
	mapTasks    map[int]*Task
	reduceTasks map[int]*Task
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// 分配Map任务给worker	RPC注册方法
func (c *Coordinator) Allocate(args *Args, reply *Reply) error {
	c.mu.Lock()

	defer c.mu.Unlock()
	log.Println("coordinator is allocating task ...")
	// phase: map reduce exit
	// Type：map reduce、wait、exit
	// status：PENDING、IN_PROGRESS、COMPLETED UNALLOCATED ALL_COMPLETED
	reply.Task = &Task{}

	// 所有任务已完成，让worker退出
	if c.phase == "exit" {
		reply.Task.Type = "exit"
		reply.Task.Status = ALL_COMPLETED
		return nil
	}

	// 根据当前阶段确定任务集合和相关参数
	var tasks map[int]*Task
	// 相当于 ? :表达式
	isMapPhase := c.phase == "map"

	if isMapPhase {
		tasks = c.mapTasks
	} else {
		tasks = c.reduceTasks
	}

	for id, task := range tasks {
		if task.Status == PENDING {
			// 设置reply通用属性
			reply.Task.Id = id
			reply.Task.Type = c.phase
			reply.Task.Status = IN_PROGRESS
			reply.Task.StartTime = time.Now()

			// 根据任务类型设置特定属性
			if isMapPhase {
				reply.Task.FileName = task.FileName
				reply.NReduce = c.nreduce
			} else {
				reply.NMap = c.nmap
			}

			// 更新任务状态
			tasks[id] = reply.Task

			log.Printf("Sending reply to worker: Task={Id:%d, Type:%s, Status:%s, FileName:%s}",
				reply.Task.Id, reply.Task.Type, reply.Task.Status, reply.Task.FileName)
			return nil
		}
	}
	// 没有待处理任务，让worker等待
	reply.Task.Type = "wait"
	reply.Task.Status = UNALLOCATED
	log.Println("No pending tasks available, worker needs to wait...")
	return nil
}

// 报告完成的函数RPC注册方法
func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	c.mu.Lock()

	defer c.mu.Unlock()

	if args.TaskType == "map" {
		task, ok := c.mapTasks[args.TaskId]
		if !ok {
			return TaskNotFoundErr
		}

		duration := args.EndTime.Sub(task.StartTime)
		if duration > 10*time.Second {
			// 超时
		}
		task.Status = COMPLETED
		// 检查是否所有的map 任务都完成了，然后切换到reduce 阶段
		allMapsDone := true
		for _, map_task := range c.mapTasks {
			if map_task.Status != COMPLETED {
				allMapsDone = false
				break
			}
		}
		if allMapsDone {
			c.phase = "reduce"
		}
	} else if args.TaskType == "reduce" {
		c.reduceTasks[args.TaskId].Status = COMPLETED
		// 检查是否reduce 任务是否都完成了，切换到 exit 阶段
		allReducesDone := true
		for _, reduce_task := range c.reduceTasks {
			if reduce_task.Status != COMPLETED {
				allReducesDone = false
				break
			}
		}
		if allReducesDone {
			c.phase = "exit"
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) checkTimeouts() {

	for {

		// 这里尚未配置锁

		time.Sleep(1 * time.Second) // 每秒检查一次
		c.mu.Lock()

		if c.phase == "map" {
			for id, task := range c.mapTasks {
				if task.Status == IN_PROGRESS && time.Since(task.StartTime) > 10*time.Second {
					task.Status = PENDING
					log.Printf("Map task %d timed out, resetting.\n", id)
				}
			}
		} else if c.phase == "reduce" {
			for id, task := range c.reduceTasks {
				if task.Status == IN_PROGRESS && time.Since(task.StartTime) > 10*time.Second {
					task.Status = PENDING
					log.Printf("Reduce task %d timed out, resetting.\n", id)
				}
			}
		}
		c.mu.Unlock()
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()

	defer c.mu.Unlock()
	ret := false
	// Your code here.
	if c.phase == "map" {
		return ret
	} else { // c.phase == "reduce"
		for _, task := range c.reduceTasks {
			if task.Status != COMPLETED {

				ret = false
				return ret
			}
		}
		ret = true
	}
	log.Println("all tasks have been completed...")
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		phase:       "map",
		nmap:        len(files),
		nreduce:     nReduce,
		mapTasks:    make(map[int]*Task),
		reduceTasks: make(map[int]*Task),
		// mu不需要初始化
	}

	// Your code here.
	log.Println("Initialize coordinator...")
	// 初始化map任务（并不是创建map worker，只是生成map任务的元数据），每个map worker处理一个文件
	for i, file := range files {
		c.mapTasks[i] = &Task{
			Id:       i,
			Type:     "map",
			Status:   PENDING,
			FileName: file,
		}
	}
	// 初始化reduce任务
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &Task{
			Id:     i,
			Type:   "reduce",
			Status: PENDING,
		}
	}

	// 启动超时检测goroutine
	go c.checkTimeouts()
	c.server()

	log.Printf("Coordinator Status:\n"+
		"- Phase: %s\n"+
		"- Files: %v\n"+
		"- Map Tasks: %d (total)\n"+
		"- Reduce Tasks: %d (total)",
		c.phase, c.files, len(c.mapTasks), len(c.reduceTasks))

	log.Println("Map Tasks Details:")
	for id, task := range c.mapTasks {
		log.Printf("  Task[%d]: {Id: %d, Type: %s, Status: %s, FileName: %s}",
			id, task.Id, task.Type, task.Status, task.FileName)
	}
	log.Println("-----------------------------------")
	for id, task := range c.reduceTasks {
		log.Printf("  Task[%d]: {Id: %d, Type: %s, Status: %s, FileName: %s}",
			id, task.Id, task.Type, task.Status)
	}
	log.Println("The server is ready...")
	return &c
}
