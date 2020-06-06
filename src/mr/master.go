package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "io/ioutil"
import "regexp"

const (
	TO_BE_PROCESSED = iota // 0
	PROCESSED // 1
)

type Master struct {
	FilesDict map[string]int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTask(args *MRTaskArgs, reply *MRTaskReply) error {
	// selects a file for the worker
	hasFileToProcess := m.selectFile(reply)

	// if there are no files that need reducing, then we're done?
	if (hasFileToProcess == false) {
		m.Done()
	}

	return nil
}

func (m* Master) selectFile(reply *MRTaskReply) (bool) {
	// return the first file that needs to be mapped/processed
	var filePath string
	var hasFileToBeMapped, hasFileToBeReduced bool

	for file, state := range m.FilesDict {
		if (state == TO_BE_PROCESSED) {
			filePath = file
			hasFileToBeMapped = true
		}
	}
	
	if (hasFileToBeMapped == false) {
		// if there are no files to map, perhaps there are files
		// whose contents need to reducing
		pwdFiles, err := ioutil.ReadDir(".")
		check(err)

		// FIX: check if there are any temp files for reduction
		for _, file := range pwdFiles {
			match, err := regexp.MatchString(`mr-x-`, "mr-x-")
			check(err)
			if (match) {
				filePath = file.Name()
				hasFileToBeReduced = true
			}
		}
	}

	// assigns the file
	// and the operation to perform on the file to the task obj
	reply.FilePath = filePath
	reply.ToMap = hasFileToBeMapped
	reply.ToReduce = hasFileToBeReduced

	// there are no files left for processing
	if (hasFileToBeMapped == true || hasFileToBeReduced == true) {
		return true
	} else {
		return false
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
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
	m.FilesDict = make(map[string]int)

	// place files in dictionary
	for _, f := range files {
		m.FilesDict[f] = TO_BE_PROCESSED
	}

	m.server()
	return &m
}
