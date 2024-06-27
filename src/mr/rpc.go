package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type TaskArgs struct {
}
type FinishArgs struct {
	FinishID int // 0 map 1 reduce
	TaskID   int
}
type WokerInitReply struct {
	WorkerNumber int //worker编号
	NReduce      int //reduce任务数量
}
type TaskReply struct {
	FileName string
	Status   int //0 task map 1 wait other map 2 task reduce 3 wait other reduce 4 finish Job
	TaskID   int
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
