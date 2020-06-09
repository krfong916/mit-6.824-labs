package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TO_BE_PROCESSED = iota // 0
	PROCESSED // 1
)

type File struct {
	MapTaskID int
	ReduceTaskID int
	Name string
	State int
}

type Master struct {
	FilesDict map[string]File
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
	var mapID, reduceID int
	var hasFileToBeMapped bool

	for _, file := range m.FilesDict {
		if (file.State == TO_BE_PROCESSED) {
			fmt.Println("Map task number: ", file.MapTaskID)
			fmt.Println("Reduce task number: ", file.ReduceTaskID)
			mapID = file.MapTaskID
			reduceID = file.ReduceTaskID
			filePath = file.Name
			hasFileToBeMapped = true
		}
	}	

	// assigns the file
	// and the operation to perform on the file to the task obj
	reply.MapTaskID = mapID
	reply.ReduceTaskID = reduceID
	reply.FilePath = filePath
	reply.ToMap = hasFileToBeMapped
	
	// there are no files left for processing
	if (hasFileToBeMapped == true) {
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
	fmt.Println("nReduce: ", nReduce)
	// create instance of master 
	m := Master{}
	
	// initialize file state dictionary
	m.FilesDict = make(map[string]File)

	// place files in dictionary
	for i, f := range files {
		newFile := File{MapTaskID: i, ReduceTaskID: nReduce, State: TO_BE_PROCESSED, Name: f}
		m.FilesDict[f] = newFile
	}

	m.server()
	return &m
}
