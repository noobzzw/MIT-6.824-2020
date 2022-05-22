package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerInfo struct {
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
	Task       *Task
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := WorkerInfo{
		MapFunc:    mapf,
		ReduceFunc: reducef,
		Task:       &Task{},
	}

	// 不断像master发送心跳，轮询任务
	for {
		response := w.doHearBeat()
		fmt.Println("receive task : ", response.Task.TaskIndex, ", task type is ", GetTaskTypeName(response.Task.TaskType))
		switch response.Task.TaskType {
		case MapTask:
			// do map
			w.Map()
		case ReduceTask:
			w.Reduce()
		case Wait:
			// Wait表示Worker需等待指令（例如其他Worker的Map任务还未完成）
			// Wait 5 second
			time.Sleep(5 * time.Second)
		case CompleteTask:
			// all done
			fmt.Println("我滴任务完成啦！")
			return
		}
		//sleepTime := rand.Intn(10)
		//time.Sleep(time.Duration(sleepTime) * time.Second)
	}
}

// 获取任务信息
func (w *WorkerInfo) doHearBeat() TaskResponse {
	taskResponse := TaskResponse{}
	taskRequest := TaskRequest{*w.Task}
	// 如果为空结构体或当前Worker分配的Task已完成，则继续向Master请求任务
	call("Master.GetTask", &taskRequest, &taskResponse)
	// 更新当前task信息
	w.Task = &taskResponse.Task
	return taskResponse
}

// 上报任务信息
func (w *WorkerInfo) Report() {
	// 更新任务的时间戳
	w.Task.HearBeat = time.Now()
	taskResponse := TaskResponse{}
	taskRequest := TaskRequest{*w.Task}
	// 汇报任务情况
	call("Master.ReportTask", &taskRequest, &taskResponse)
}

// 执行reduceTask
func (w *WorkerInfo) Reduce() {
	// open file
	reduceIndex := w.Task.TaskIndex
	outName := fmt.Sprintf("mr-out-%v", reduceIndex)
	format := "mr-%v-%v"
	intermediate := make([]KeyValue, 0)
	for i := 0; i < w.Task.MapNum; i++ {
		/* ---copy file --- */
		fileName := fmt.Sprintf(format, i, reduceIndex)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", w.Task.File)
		}
		dec := json.NewDecoder(file)
		kva := make([]KeyValue, 0)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close %v", file.Name())
		}
		// ... 代表将当前集合unpack后append进result
		intermediate = append(intermediate, kva...)
	}
	/* ---- sort --- */
	sort.Sort(ByKey(intermediate))
	/* --- reduce --*/
	w.doReduce(intermediate, outName)
	w.Task.IsFinish = true
	w.Task.HasAssigned = true
	// 上报任务信息
	w.Report()
}

func (w *WorkerInfo) doReduce(intermediate []KeyValue, outName string) {
	tempFile, err := os.CreateTemp(".", "mrtemp")
	if err != nil {
		log.Fatalf("cannot create tempfile for %v\n", outName)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.ReduceFunc(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("write tempfile failed for %v\n", outName)
		}
		i = j
	}
	/* --rename temp file to official file */
	// 先检测文件是否存在，如果存在则删除（预防其他crash任务已写入部分文件）
	_, err = os.Stat(outName)
	if err == nil {
		// 如果文件存在，则删除
		err := os.Remove(outName)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = os.Rename(tempFile.Name(), outName)
	if err != nil {
		log.Fatalf("rename tempfile failed for %v\n", outName)
	}
}

// 执行MapTask
func (w *WorkerInfo) Map() {
	reduceNum := w.Task.ReduceNum
	/* -----map---------- */
	// 执行Map函数
	kva := w.doMap()
	/* -----partition---- */
	// 存储分区后的[]kv
	partitionedKva := make([][]KeyValue, reduceNum)
	// 初始化partitionKva
	for i := 0; i < reduceNum; i++ {
		partitionedKva[i] = make([]KeyValue, 0)
	}
	for _, value := range kva {
		// 计算分区数
		index := ihash(value.Key) % reduceNum
		partitionedKva[index] = append(partitionedKva[index], value)
	}
	/* -----sort--------- */
	for i := 0; i < reduceNum; i++ {
		sort.Sort(ByKey(partitionedKva[i]))
	}
	/* --write intermediate file-- */
	for i := 0; i < reduceNum; i++ {
		tempFile, err := os.CreateTemp(".", "mrtemp")
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range partitionedKva[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		outName := fmt.Sprintf("mr-%v-%v", w.Task.TaskIndex, i)
		_, err = os.Stat(outName)
		if err == nil {
			// 如果文件存在，则删除
			err := os.Remove(outName)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = os.Rename(tempFile.Name(), outName)
		if err != nil {
			fmt.Printf("rename tempfile failed for %v\n", outName)
		}
	}
	//for {
	//}
	w.Task.IsFinish = true
	w.Task.HasAssigned = true
	// 上报任务信息
	w.Report()
}

// 执行Map函数
func (w *WorkerInfo) doMap() []KeyValue {
	path := w.Task.File
	// test script manages relative path structure
	// open file
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}
	// read file content
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", w.Task.File)
	}
	err = file.Close()
	if err != nil {
		return nil
	}
	// generate kv array
	kva := w.MapFunc(w.Task.File, string(content))
	return kva
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
