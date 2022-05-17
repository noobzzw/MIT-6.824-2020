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

type taskType int

const (
	MapTask taskType = iota
	ReduceTask
	Wait
	CompleteTask
)

// TaskRequest worke的请求
type TaskRequest struct {
	// 当前worker的id
	WorkerId string
}

// TaskResponse hearbeat response
type TaskResponse struct {
	// task 当前的类型
	JobType taskType
	// 文件地址
	FilePath string
	// reduce个数
	NReduce int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
