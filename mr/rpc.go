package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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
// 任务类型
type taskType int

const (
	MapTask taskType = iota
	ReduceTask
	Wait
	CompleteTask
)

func GetTaskTypeName(code taskType) (typeName string) {

	switch code {
	case MapTask:
		typeName = "MapTask"
	case ReduceTask:
		typeName = "ReduceTask"
	case Wait:
		typeName = "Wait"
	case CompleteTask:
		typeName = "CompleteTask"
	}
	return
}

// TaskRequest worker的请求
type TaskRequest struct {
	// task信息
	Task Task
}

// TaskResponse hearbeat response
type TaskResponse struct {
	// task 当前的类型
	Task Task
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
