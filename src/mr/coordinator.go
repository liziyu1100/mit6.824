package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

}

type Task struct {
	id             int
	isFinishMap    bool
	isFinishReduce bool
	fileName       string
	isAlloc        bool
}

var nReduces int
var tasks []Task
var workers []int
var workerID int
var finishedTask int
var finishedMapSign bool
var finishedReduce bool
var allocMap int

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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	finishedTask = 0
	// Your code here.
	taskN := 0
	for _, file := range files {
		temp := Task{id: taskN, isFinishMap: false, isFinishReduce: false, fileName: file, isAlloc: false}
		tasks = append(tasks, temp)
		taskN++
	}
	nReduces = nReduce
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
		if allocMap < len(tasks) {
			for index, task := range tasks {
				if !task.isAlloc {
					reply.FileName = task.fileName
					reply.Status = 0
					reply.TaskID = task.id
					tasks[index].isAlloc = true
					allocMap++
					break
				}
			}
		} else {
			reply.Status = 1
		}

	} else {
		reply.Status = 2
		//job的reduce任务尚未完成

	}

	return nil
}

func (c *Coordinator) FinishMap(args *FinishArgs, reply *TaskReply) error {
	if args.FinishID == 0 {
		tasks[args.TaskID].isFinishMap = true
	} else {
		tasks[args.TaskID].isFinishReduce = true
	}
	finishedTask++
	if finishedTask == len(tasks) {
		finishedMapSign = true
		finishedTask = 0
	}
	return nil
}
