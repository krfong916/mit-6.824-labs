package mr

import "os"
import "strconv"

// Add your RPC definitions here.
type MRTaskArgs struct {}

type MRTaskReply struct {
	MapTaskID int
  ReduceTaskID int
  NReduce int
  File string
  TaskType taskType
  Files []string
}

type MRTaskUpdate struct {
	Files 			[]string
	MapTaskID 	int
	Success 		bool
	TaskType taskType
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
  s := "/var/tmp/824-mr-"
  s += strconv.Itoa(os.Getuid())
  return s
}
