package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type WorkerInfo struct {
	// 上次心跳的时间
	LastHeartBeat time.Time
	// 处理的文件内容
	ProccessFile string
	// 正在处理的任务类型
	JobType taskType
	// reduce数量
	NReduce int
}
type Master struct {
	// Your definitions here.
	// 存储worker信息,k：workerid
	workers map[string]WorkerInfo
	// 要处理的文件内容
	files []string
	// reduce的数量
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) GetTask(args *TaskRequest, reply *TaskResponse) error {
	fmt.Printf("Accept worker %s request\n", args.WorkerId)
	worker, ok := m.workers[args.WorkerId]
	if !ok {
		// worker未注册过，先注册
		worker = WorkerInfo{
			LastHeartBeat: time.Now(),
			ProccessFile:  m.files[0],
			NReduce:       m.NReduce,
			JobType:       3,
		}
		// worker注册
		m.workers[args.WorkerId] = worker
	}
	// test 先发出一个wait指令
	*reply = TaskResponse{
		JobType:  3,
		FilePath: m.files[0],
		NReduce:  m.NReduce,
	}
	fmt.Println(reply)
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	fmt.Println("accept files is ", files)
	m := Master{
		workers: make(map[string]WorkerInfo),
		files:   files,
		NReduce: nReduce,
	}

	m.server()
	return &m
}
