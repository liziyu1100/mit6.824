package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
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
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// worker循环请求master 获取任务
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
			time.Sleep(1 * time.Second)
		} else if reply.Status == 2 {
			fmt.Printf(fmt.Sprintf("worker:%d 执行reduce任务:%d \n", workerID, reply.TaskID))
			kva, err := findFilesWithSuffixY("./", reply.FileName)
			if err != nil {
				log.Fatalf("读取reduce任务失败:" + err.Error())
			}
			//shuffle
			sort.Sort(ByKey(*kva))
			oname := "mr-out-" + reply.FileName
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(*kva) {
				j := i + 1
				for j < len(*kva) && (*kva)[j].Key == (*kva)[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, (*kva)[k].Value)
				}
				//reduce
				output := reducef((*kva)[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", (*kva)[i].Key, output)
				i = j
			}
			ofile.Close()
			callFinishMap(1, reply.TaskID)
		} else if reply.Status == 3 {
			// 方法一: worker sleep
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	fmt.Printf("job执行完成\n")

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
	return reply
	//out := fmt.Sprintf("%s nreduce=%d", tasks, reply.NReduce)
	//fmt.Println(out)
}
func callFinishMap(FinishID int, taskID int) {
	args := FinishArgs{FinishID: FinishID, TaskID: taskID}
	reply := TaskReply{}
	call("Coordinator.FinishMap", &args, &reply)
	fmt.Println(fmt.Sprintf("worker: %d 完成任务种类: %d(0 map 1 reduce) 任务ID：%d", workerID, FinishID, taskID))
}
func callworkerInit() WokerInitReply {
	args := TaskArgs{}
	reply := WokerInitReply{}
	call("Coordinator.InitWorker", &args, &reply)
	fmt.Println(fmt.Sprintf("worker:%d 初始化完毕", workerID))
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

// findFilesWithSuffixY 查找目录下所有以 mr-xy 命名并且 y 等于 a 的文件，并返回文件内容
func findFilesWithSuffixY(dirPath string, a string) (*[]KeyValue, error) {
	// 定义文件名模式的正则表达式
	pattern := regexp.MustCompile(`^mr-\d+` + a + `$`)

	// 遍历目录，查找匹配的文件
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}
	var kva []KeyValue
	for _, file := range files {
		if !file.IsDir() && pattern.MatchString(file.Name()) {
			// 打印匹配的文件名
			fmt.Printf("Found file: %s\n", file.Name())
			midfile, err := os.Open(fmt.Sprintf("%s/%s", dirPath, file.Name()))
			if err != nil {
				return nil, fmt.Errorf("failed to open file: %w", err)
			}
			dec := json.NewDecoder(midfile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
	}

	return &kva, nil
}
