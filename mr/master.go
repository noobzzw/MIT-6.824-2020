package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Task 任务信息
type Task struct {
	// 文件路径
	File string
	// map个数
	MapNum int
	// reduce个数
	ReduceNum int
	// 任务类型{MapTask, ReduceTask, Wait, CompleteTask}
	TaskType taskType
	// 与Master的心跳时间
	HearBeat time.Time
	// 是否被分配
	HasAssigned bool
	// 当前任务的index
	TaskIndex int
	// 是否已完成
	IsFinish bool
}
type Master struct {
	// Your definitions here.
	// 存储MapTask信息
	MapTasks []Task
	// 存储ReduceTask信息
	ReduceTasks []Task
	// reduce的数量
	ReduceNum int
	// lock
	// todo 将粗粒度的锁换为细粒度的
	lock *sync.RWMutex
	// 已分配的MapTask index
	AssignedMapTaskIndex int
	// Map任务完成个数
	FinishedMapTasks int
	// 已分配的ReduceTaskIndex
	AssignedReduceTaskIndex int
	// Reduce任务完成个数
	FinishedReduceTasks int
	// 失败任务队列
	crashTask LinkedQueue
}

// 分配Map任务
func (m *Master) AssignMapTask() Task {
	m.lock.Lock()
	defer m.lock.Unlock()
	task := m.MapTasks[m.AssignedMapTaskIndex]
	m.MapTasks[m.AssignedMapTaskIndex].HasAssigned = true
	m.AssignedMapTaskIndex++
	return task
}

// 分配reduce任务
func (m *Master) AssignReduceTask() Task {
	m.lock.Lock()
	defer m.lock.Unlock()
	task := m.ReduceTasks[m.AssignedReduceTaskIndex]
	m.ReduceTasks[m.AssignedReduceTaskIndex].HasAssigned = true
	m.AssignedReduceTaskIndex++
	return task
}

// map任务是否已经分配完毕
func (m *Master) MapTaskAllocated() bool {
	// read lock
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.AssignedMapTaskIndex >= len(m.MapTasks)
}

// reduce任务是否已经分配完毕
func (m *Master) ReduceTaskAllocated() bool {
	// read lock
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.AssignedReduceTaskIndex >= len(m.ReduceTasks)
}

// 检测Task时间，当超过10s后将任务重新分配
func (m *Master) checkTaskStatus() {
	for {
		// 当Map任务未完成时，检测Map任务
		if !m.MapTaskDone() {
			m.lock.Lock()
			for index, mapTask := range m.MapTasks {
				// 如果mapTask未完成且已经分配时，检查时间是否超时
				if !mapTask.IsFinish && mapTask.HasAssigned {
					now := time.Now()
					duration := now.Sub(mapTask.HearBeat)
					if duration.Seconds() > 10 {
						//将任务回收（状态置为未分配），加入失败任务队列
						mapTask.HasAssigned = false
						mapTask.HearBeat = time.Now()
						m.MapTasks[index] = mapTask
						// 加入失败任务队列
						m.crashTask.Push(mapTask)
						log.Println("map task : ", mapTask, "crash")
					}
				}
			}
			m.lock.Unlock()
		} else if !m.ReduceTaskDone() {
			// todo 还是存在data race的问题
			m.lock.Lock()
			for index, reduceTask := range m.ReduceTasks {
				if !reduceTask.IsFinish && reduceTask.HasAssigned {
					now := time.Now()
					duration := now.Sub(reduceTask.HearBeat)
					if duration.Seconds() > 10 {
						reduceTask.HasAssigned = false
						reduceTask.HearBeat = time.Now()
						m.ReduceTasks[index] = reduceTask
						m.crashTask.Push(reduceTask)
						log.Println("reduce task : ", reduceTask, "crash")
					}
				}
			}
			m.lock.Unlock()
		}
		time.Sleep(1 * time.Second)
	}
}

// Your code here -- RPC handlers for the worker to call.

// Worker调用该方法获取任务
// todo 这个if-else太丑陋了
func (m *Master) GetTask(args *TaskRequest, reply *TaskResponse) error {
	task := Task{}
	// 先检查失败任务队列是否有元素，有则直接分配失败的任务
	data, err := m.crashTask.Poll()
	if err == nil {
		crashTask, ok := data.(Task)
		if ok {
			if crashTask.TaskType == MapTask {
				crashTask.HasAssigned = true
				m.lock.Lock()
				m.MapTasks[crashTask.TaskIndex] = crashTask
				task = crashTask
				m.lock.Unlock()
			} else if crashTask.TaskType == ReduceTask {
				crashTask.HasAssigned = true
				m.lock.Lock()
				m.ReduceTasks[crashTask.TaskIndex] = crashTask
				task = crashTask
				m.lock.Unlock()
			}
		}
	} else {
		// MapTask未全部完成
		if !m.MapTaskDone() {
			// Map Task未全部分配完成
			if !m.MapTaskAllocated() {
				// 分配map task
				task = m.AssignMapTask()
			} else {
				// Map Task任务分配完成，Worker等待其他Worker完成Map任务
				task.TaskType = Wait
				fmt.Println("[Master]Worker Map任务已完成，等待其他Worker完成")
			}
		} else if !m.ReduceTaskDone() {
			// Reduce Task 未全部分配完成
			if !m.ReduceTaskAllocated() {
				// 分配reduce task
				task = m.AssignReduceTask()
			} else {
				// Reduce任务分配完成，Worker等待其他Worker完成任务
				task.TaskType = Wait
				fmt.Println("[Master]Worker Reduce任务已完成，等待其他Worker完成")
			}
		} else {
			// map 和reduce任务均完成，告诉Worker可以退出了
			task.TaskType = CompleteTask
		}
	}
	reply.Task = task
	// 更新心跳时间
	reply.Task.HearBeat = time.Now()
	fmt.Println("[Master]分配任务: ", task, " 任务类型: ", GetTaskTypeName(task.TaskType))
	return nil
}

func (m *Master) ReportTask(args *TaskRequest, reply *TaskResponse) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	// Worker汇报工作，更新Master Task信息
	taskIndex := args.Task.TaskIndex
	if args.Task.TaskType == MapTask {
		if args.Task.IsFinish && !m.MapTasks[taskIndex].IsFinish {
			m.FinishedMapTasks++
		}
		m.MapTasks[taskIndex] = args.Task
	} else {
		if args.Task.IsFinish && !m.ReduceTasks[taskIndex].IsFinish {
			m.FinishedReduceTasks++
		}
		m.ReduceTasks[taskIndex] = args.Task
	}
	reply.Task = Task{}
	reply.Task.TaskType = Wait
	return nil
}

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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	ret = m.MapTaskDone() && m.ReduceTaskDone()
	return ret
}

func (m *Master) MapTaskDone() bool {
	// read lock
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.FinishedMapTasks >= len(m.MapTasks)
}

func (m *Master) ReduceTaskDone() bool {
	// read lock
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.FinishedReduceTasks >= len(m.ReduceTasks)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	// 初始化MapTask
	mapTasks := make([]Task, len(files))
	for i := range files {
		mapTask := Task{
			File:        files[i],
			MapNum:      len(files),
			ReduceNum:   nReduce,
			TaskType:    MapTask,
			HearBeat:    time.Now(),
			HasAssigned: false,
			TaskIndex:   i,
			IsFinish:    false,
		}
		mapTasks[i] = mapTask
	}
	// 初始化ReduceTask
	reduceTasks := make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		// 文件名称为：mr-*-partitionId
		fileName := fmt.Sprintf("mr-*-%d", i)
		reduceTask := Task{
			File:        fileName,
			MapNum:      len(files),
			ReduceNum:   nReduce,
			TaskType:    ReduceTask,
			HearBeat:    time.Now(),
			HasAssigned: false,
			TaskIndex:   i,
			IsFinish:    false,
		}
		reduceTasks[i] = reduceTask
	}
	// 创建Master
	m := Master{
		MapTasks:                mapTasks,
		ReduceTasks:             reduceTasks,
		ReduceNum:               nReduce,
		AssignedMapTaskIndex:    0,
		FinishedMapTasks:        0,
		AssignedReduceTaskIndex: 0,
		FinishedReduceTasks:     0,
		crashTask:               newLinkedQueue(),
		lock:                    new(sync.RWMutex),
	}
	m.server()
	// 定期检测任务状态
	go m.checkTaskStatus()
	return &m
}
