package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func getcontent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	curWorker := callworkerInit()
	//number := curWorker.WorkerNumber
	nReduce := curWorker.NReduce
	var fmapts []string //已完成的map任务名称
	// Your worker implementation here.

	// worker循环请求master 获取map任务
	for {
		reply := callTask()
		if reply.Status == 0 {
			midout := mapf(reply.FileName, getcontent(reply.FileName))
			err := writeIntermed(&midout, nReduce, reply.TaskID)
			if err != nil {
				log.Fatalf("写入中间文件失败 %s", err)
			}
			callFinishMap(0, reply.TaskID)
			fmapts = append(fmapts, reply.FileName)
		} else if reply.Status == 1 {
			// 方法一: worker sleep
			time.Sleep(5 * time.Second)
		} else {
			fmt.Printf(fmt.Sprintf("worker:%d 完成的map任务：%v \n", workerID, fmapts))
			break
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

func callTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	call("Coordinator.SendTask", &args, &reply)
	//tasks := ""
	//for _, task := range reply.TaskFiles {
	//	tasks += task
	//}
	fmt.Println("callTask reply: ", reply)
	return reply
	//out := fmt.Sprintf("%s nreduce=%d", tasks, reply.NReduce)
	//fmt.Println(out)
}
func callFinishMap(FinishID int, taskID int) {
	args := FinishArgs{FinishID: FinishID, TaskID: taskID}
	reply := TaskReply{}
	call("Coordinator.FinishMap", &args, &reply)
	fmt.Println("call finishMap reply ")
}
func callworkerInit() WokerInitReply {
	args := TaskArgs{}
	reply := WokerInitReply{}
	call("Coordinator.InitWorker", &args, &reply)
	fmt.Println("call worker init reply ")
	return reply
}

func writeIntermed(midout *[]KeyValue, nReduce int, taskId int) error {
	midfile := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	var err error
	for i := 0; i < nReduce; i++ {
		intermediateFileName := fmt.Sprintf("./mr-%d%d", taskId, i)
		midfile[i], err = os.Create(intermediateFileName)
		if err != nil {
			return err
		}
		encoders[i] = json.NewEncoder(midfile[i])
	}
	for _, kv := range *midout {
		bucket := ihash(kv.Key) % nReduce
		err = encoders[bucket].Encode(&kv)
		if err != nil {
			return err
		}
	}
	for _, file := range midfile {
		err = file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
