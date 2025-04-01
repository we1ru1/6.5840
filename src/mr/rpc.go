package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	Id        int
	Type      string
	Status    string
	StartTime time.Time
	FileName  string
}

type Args struct {
}

type Reply struct {
	Task *Task
	// 便于map worker知道要将中间文件分成 NReduce 份
	NReduce int
	// 便于reduce worker知道中间文件的命名，因为中间文件的命名是mr-X-Y，其中X是Map任务编号，Y是reduce任务编号
	NMap int
}

type FinishArgs struct {
	TaskId   int
	TaskType string
	EndTime  time.Time
}

type FinishReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
