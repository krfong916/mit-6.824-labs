package mr

// import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
  MapTasks []Task
  ReduceTasks [][]string
  NReduce int
  HasFinishedJob bool
}

// INSPECT - do we need a task type if it's only for a map task?
// how can we extend for reduce tasks as well? we need an array
type Task struct {
  ID int
  File string
  State int
}

const (
  TO_BE_PROCESSED = iota
  PROCESSING
  PROCESSED
)

// Your code here -- RPC handlers for the worker to call.

func (m* Master) RequestTask(args *MRTaskArgs, reply *MRTaskReply) error {  
  if (len(m.MapTasks) != 0) {
  	m.assignMapTask(reply)
  	return nil
  }

  if (len(m.ReduceTasks) != 0) {
  	m.assignReduceTask(reply)
  	return nil
  }

  // if there are no more map or reduce tasks to process
  // change job state from pending to finished
  m.HasFinishedJob = true

  return nil
}

func (m* Master) assignMapTask(reply *MRTaskReply) {
  for _, mapTask := range m.MapTasks {
    if (mapTask.State == TO_BE_PROCESSED) {
      // assign information to execute a map task to the worker
      reply.MapTaskID = mapTask.ID
      reply.File = mapTask.File
      reply.TaskType = MapTask
      reply.NReduce = m.NReduce

      // change the map task state to processing
      // therefore, the task cannot be assigned to another worker
      mapTask.State = PROCESSING
      break;
    }
  }
}

func (m* Master) assignReduceTask(reply *MRTaskReply) {
  // for _, reduceTask := range m.ReduceTasks {
  //   if (reduceTask.State == TO_BE_PROCESSED) {
  //     // assign the map task to the worker
  //     reply.ReduceTaskID = reduceTask.ID
  //     reply.File = reduceTask.File
  //     reply.TaskType = ReduceTask

  //     // change the map task state to processing
  //     // therefore, the task cannot be assigned to another worker
  //     reduceTask.State = PROCESSING
  //     break;
  //   }
  // }
}

func (m* Master) UpdateTask(args *MRTaskUpdate, reply *MRTaskUpdate) error {
	m.MapTasks[args.MapTaskID].State = PROCESSED
	reply.Success = true
	m.ReduceTasks[args.MapTaskID] = args.Files
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
  // ret := true

  // Your code here.
  // return ret

  return m.HasFinishedJob
}

//
// create a Master. Type: constructor
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(tasks []string, nReduce int) *Master {
  // create instance of master 
  m := Master{}
  
  // initialize map and reduce dictionaries
  m.MapTasks = make([]Task, len(tasks))
  m.ReduceTasks = make([][]string, nReduce)

  // initialize number of files to output
  m.NReduce = nReduce

  // place tasks in map dictionary, key = file name, value = task
  for i, file := range tasks {
    mapTask := Task{ID: i, State: TO_BE_PROCESSED, File: file}
    m.MapTasks[i] = mapTask
  }

  m.server()
  return &m
}
