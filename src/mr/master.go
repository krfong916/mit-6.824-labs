package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Master struct {
  MapTasks map[int]Task
  ReduceTasks map[int]bool
  NMapTasks int
  NReduce int
  HasFinishedJob bool
  Mu sync.Mutex // the master has a Mutex instance because we want one mutex to share amongst workers, and not a different instance each time a function is called
}

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

func (m *Master) RequestTask(args *MRTaskArgs, reply *MRTaskReply) (err error) {  
  
  m.Mu.Lock()
  defer m.Mu.Unlock()

  if (len(m.MapTasks) != 0) {
    taskID := m.assignMapTask(reply)
    m.checkMapTask(taskID)
    return
  }

  if (len(m.ReduceTasks) != 0) {
    m.assignReduceTask(reply)
    return
  }

  reply.TaskType = ExitTask
  return
}

func (m *Master) assignMapTask(reply *MRTaskReply) int {
  var taskID = -1
  for _, task := range m.MapTasks {
    if (task.State == TO_BE_PROCESSED) {
      // assign information to execute a map task to the worker
      reply.MapTaskID = task.ID
      reply.File = task.File
      reply.NReduce = m.NReduce
      reply.TaskType = MapTask

      // change the map task state to processing
      // therefore, the task cannot be assigned to another worker
      task.State = PROCESSING
      taskID = task.ID
      break
    }
  }
  return taskID
}

// create a lock that will modify shared data
// i want to make a function call with the data
// spawn a go routine - a seperate thread that will be a timer for a duration of time
// i want this timer to run in the background, in its own thread, and then at 10 seconds, wakeup and do a thing
// i don't want to block other requests acquiring the lock

// check if the map task has been completed within a timed duration
func (m *Master) checkMapTask(mapTaskId int) {
  durationOfTime := time.Duration(10) * time.Second
  timer := time.AfterFunc(durationOfTime, func() {
    m.Mu.Lock()
    defer m.Mu.Unlock()
    if (m.MapTasks[mapTaskId].State == PROCESSING) {
      var mapTask = m.MapTasks[mapTaskId]
      mapTask.State = TO_BE_PROCESSED
      m.MapTasks[mapTaskId] = mapTask
    }
    fmt.Println("timer")
  })
  defer timer.Stop() // time.AfterFunc returns a timer that waits and after a duration calls a function in its own goroutine, we defer stopping the time until the function runs
}

func (m *Master) assignReduceTask(reply *MRTaskReply) {
  for i := range m.ReduceTasks {
    // assign the map task to the worker
    reply.ReduceTaskID = i
    reply.NMapTasks = m.NMapTasks
    reply.TaskType = ReduceTask
    // remove the reduce task from the list of tasks to be complete
    delete(m.ReduceTasks, i)
    break;
  }
}

func (m *Master) UpdateMapTask(args *MRTaskUpdate, reply *MRTaskUpdate) error {
  m.Mu.Lock()
  defer m.Mu.Unlock()
  // this is the equivalent of marking the map task state as PROCESSED
  delete(m.MapTasks, args.MapTaskID)
  reply.Success = true
  
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
  if (len(m.MapTasks) == 0 && len(m.ReduceTasks) == 0) {
    m.HasFinishedJob = true
  }
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
  
  // initialize map and reduce dictionaries - alternatively, could've used a slice and 2D slice...
  m.MapTasks = make(map[int]Task)
  m.ReduceTasks = make(map[int]bool, nReduce)

  // initialize number of tasks
  m.NMapTasks = len(tasks)
  // initialize number of files to output
  m.NReduce = nReduce

  // place tasks in map dictionary, key = file name, value = task
  for i, file := range tasks {
    mapTask := Task{ID: i, State: TO_BE_PROCESSED, File: file}
    m.MapTasks[i] = mapTask
  }

  // create n-many reduce tasks
  for i := 0; i < nReduce; i++ {
    m.ReduceTasks[i] = true
  }

  m.server()
  return &m
}