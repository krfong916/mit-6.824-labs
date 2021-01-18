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
  ReduceTasks map[int][]string
  NReduce int
  HasFinishedJob bool
  Mu sync.Mutex // the master has a Mutex instance because we want one mutex to share amongst workers, and not a different instance each time a function is called
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

func (m *Master) RequestTask(args *MRTaskArgs, reply *MRTaskReply) error {  
  
  m.Mu.Lock()
  defer m.Mu.Unlock()

  if (len(m.MapTasks) != 0) {  	
  	taskID := m.assignMapTask(reply)
  	m.checkMapTask(taskID)
  	return nil
  }

  if (len(m.ReduceTasks) != 0) {
  	m.assignReduceTask(reply)
  	return nil
  }

  m.HasFinishedJob = true

  return nil  
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
  for i, reduceTask := range m.ReduceTasks {
    // assign the map task to the worker
    reply.ReduceTaskID = i
    reply.Files = reduceTask
    reply.TaskType = ReduceTask
    // remove the reduce task from the list of tasks to be complete
    delete(m.ReduceTasks, i)
    break;
  }
}

// TODO: we must guard against updates from workers who have been declared crashed by the master
// if the wake up and send their call to the master, then they 
func (m *Master) UpdateMapTask(args *MRTaskUpdate, reply *MRTaskUpdate) error {
	// must lock this task-state because it is shared and we are performing an update
	m.Mu.Lock()
	defer m.Mu.Unlock()
	// we aren't concerned about ordering, so we can remove the task from the map task slice
	// this is the equivalent of marking the map task state as PROCESSED
	delete(m.MapTasks, args.MapTaskID)
	m.ReduceTasks[args.MapTaskID] = args.Files
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
 	// m.Mu.Lock()
 	// if (len(MapTasks) == 0 && len(ReduceTasks == 0)) {
 	// 	m.HasFinishedJob = true
 	// }
 	// m.Mu.Unlock()
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
  m.ReduceTasks = make(map[int][]string)

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

  // I assume that we should start the clock here when we hand out a task
  // possible solutions
  // A:
  // we initialize a clock for the worker that was issued the map task
  // when we see the update task rpc call from the worker
  // we verify the time we started and the time that we see the udapte rpc call
  // -> problem: this solution relies on the worker to eventually reply - what if the worker crashes and loses data in memory?

  // B:
  

  // workers will request for tasks
  // we want to wait for workers to complete the task
  // if a worker does not complete a task within 10 seconds
  // then we want to reissue that task to another worker

  // we also have two types of tasks
  // map tasks and reduce tasks
  // we want to wait until we have completed all the map tasks
  // before we hand out map tasks