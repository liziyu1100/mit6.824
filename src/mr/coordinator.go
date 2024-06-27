package mr

import (
	"log"
	"strconv"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

}

type Task struct {
	id       int
	isFinish bool
	taskName string // 对于map任务为文件名称，对于reduce任务为任务编号
	isAlloc  bool
	taskType int // 0 map 1 reduce
}

var nReduces int
var tasksMap []Task
var tasksReduce []Task
var workers []int
var workerID int
var finishedTask int
var finishedMapSign bool
var finishedReduceSign bool
var finishedReduce bool
var allocMap int
var allocReduce int

// Your code here -- RPC handlers for the worker to call.

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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = finishedReduceSign
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	finishedTask = 0
	finishedReduceSign = false
	finishedMapSign = false
	// Your code here.
	taskN := 0
	for _, file := range files { //初始化map任务
		temp := Task{id: taskN, isFinish: false, taskName: file, isAlloc: false, taskType: 0}
		tasksMap = append(tasksMap, temp)
		taskN++
	}
	nReduces = nReduce
	taskN = 0
	for ; taskN < nReduces; taskN++ { // 初始化reduce任务
		temp := Task{id: taskN, isFinish: false, taskName: strconv.Itoa(taskN), isAlloc: false, taskType: 1}
		tasksMap = append(tasksMap, temp)
		taskN++
	}
	c.server()
	return &c
}

func (c *Coordinator) InitWorker(args *TaskArgs, reply *WokerInitReply) error {
	reply.NReduce = nReduces
	reply.WorkerNumber = workerID
	workerID++
	return nil
}

func (c *Coordinator) SendTask(args *TaskArgs, reply *TaskReply) error {
	if !finishedMapSign {
		//job的map任务尚未完成
		if allocMap < len(tasksMap) {
			for index, task := range tasksMap {
				if !task.isAlloc {
					reply.FileName = task.taskName
					reply.Status = 0
					reply.TaskID = task.id
					tasksMap[index].isAlloc = true
					allocMap++
					break
				}
			}
		} else {
			reply.Status = 1
		}
	} else {
		//job的reduce任务尚未完成
		if allocReduce < len(tasksReduce) {
			for index, task := range tasksReduce {
				if !task.isAlloc {
					reply.FileName = task.taskName
					reply.Status = 2
					reply.TaskID = task.id
					tasksReduce[index].isAlloc = true
					allocReduce++
					break
				}
			}
		} else {
			reply.Status = 3
		}

	}

	return nil
}

func (c *Coordinator) FinishMap(args *FinishArgs, reply *TaskReply) error {
	if args.FinishID == 0 {
		tasksMap[args.TaskID].isFinish = true
	} else {
		tasksReduce[args.TaskID].isFinish = true
	}
	finishedTask++
	if !finishedMapSign {
		if finishedTask == len(tasksMap) {
			finishedMapSign = true
			finishedTask = 0
		}
	} else {
		if finishedTask == len(tasksReduce) {
			finishedReduceSign = true
		}
	}

	return nil
}
