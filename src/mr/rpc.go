package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
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
type RpcArgs struct {
	//intermediate []KeyValue
	//workerId     int
	WorkerType int
	CurTime    time.Time
	//ReduceOutId      int
	WorkerId         int
	ReduceId         int
	AssignmentCnt    int
	ReduceCnt        int
	FailedAssignment []string
}
type RpcReply struct {
	NReduce          int
	Assignments      []string
	FilesAllFinished bool
	WorkerId         int

	ReduceAssignments []KeyValue
	ReduceOutId       int
	KVAllFinished     bool
	AllOutFinished    bool
	IsExit            bool
	MapIdUsed         int

	//HeartDetection bool
	//CurAssignmentFinished bool
	//GetWorker   bool
	//isAggregate bool

	// reduce
	MapIntermediateNum int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
