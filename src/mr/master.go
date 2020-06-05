package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TO_BE_PROCESSED = iota // 0
	PROCESSED // 1
)

type Master struct {
	Files []string
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master, task *Task) RequestTask() error {
	// selects a file for the worker
	file, operationType, hasFileToMap := m.selectFile()

	// if there are no files that need reducing, then we're done?
	if (!hasFileToProcess) {
		m.Done()
	}

	// assigns the file
	// and the operation to perform on the file to the task obj
	task.File = file
	task.OperationType = operationType

	return nil
}

func (m* Master) selectFile() string {
	// return the first file that needs to be mapped/processed
	for file, state := range m.Files {
		if (state == TO_BE_PROCESSED) 
			return file, true
	}
	
	// if there are no files to map, perhaps there are files
	// whose contents need to reducing
	pwdFiles, err := ioutil.ReadDir(".")

	// check if there are any temp files for reduction
	for _, file := range pwdFiles {
		match := regexp.MatchString(`mr-x-`, )
		if (match) {
			return file, true
		}
	}

	// there are no files left for processing
	return "", false
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


	return ret
}

//
// create a Master. Type: constructor
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// create instance of master 
	m := Master{}
	
	// initialize file state dictionary
	m.fileState = make(map[string]int)

	// place files in dictionary
	for _, f := range files {
		m.fileState[f] = TO_BE_PROCESSED
	}

	m.server()
	return &m
}
