package mr

import (
  "log"
  "net"
  "net/http"
  "net/rpc"
  "os"
  "sync"
  "time"
)

type Master struct {
  MapTasks       map[int]Task
  ReduceTasks    map[int]Task
  NMapTasks      int
  NReduce        int
  HasFinishedJob bool
  Mu             sync.Mutex // the master has a Mutex instance because we want one mutex to share amongst workers, and not a different instance each time a function is called
}

type Task struct {
  ID          int
  File        string
  State       int
  taskTimeout *time.Timer
  isAssigned  bool
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

  if len(m.MapTasks) != 0 {
    m.assignMap(reply)
    return
  }

  if len(m.ReduceTasks) != 0 {
    m.assignReduce(reply)
    return
  }

  reply.TaskType = ExitTask
  return
}

func (m *Master) assignMap(reply *MRTaskReply) {
  for k, task := range m.MapTasks {
    if task.State == TO_BE_PROCESSED {
      // assign information to execute a map task to the worker
      reply.MapTaskID = task.ID
      reply.File = task.File
      reply.NReduce = m.NReduce
      reply.TaskType = MapTask

      // change the map task state to processing
      // therefore, the task cannot be assigned to another worker
      task.State = PROCESSING
      task.isAssigned = true
      m.MapTasks[k] = task

      m.setMapTimer(task.ID)
      break
    }
  }
}

// create a lock that will modify shared data
// i want to make a function call with the data
// spawn a go routine - a seperate thread that will be a timer for a duration of time
// i want this timer to run in the background, in its own thread, and then at 10 seconds, wakeup and do a thing
// i don't want to block other requests acquiring the lock

// check if the map task has been completed within a timed duration
func (m *Master) setMapTimer(mapId int) {
  task := m.MapTasks[mapId]
  task.taskTimeout = time.AfterFunc(10*time.Second, func(mapId int) func() {
    return func() {
      m.Mu.Lock()
      defer m.Mu.Unlock()
      if m.MapTasks[mapId].State == PROCESSING {
        mTask := m.MapTasks[mapId]
        mTask.State = TO_BE_PROCESSED
        mTask.isAssigned = false
        m.MapTasks[mapId] = mTask
      }
    }
  }(mapId))
  m.MapTasks[mapId] = task
}

func (m *Master) assignReduce(reply *MRTaskReply) {
  for k, task := range m.ReduceTasks {
    if task.State == TO_BE_PROCESSED {
      // assign the map task to the worker
      reply.ReduceTaskID = task.ID
      reply.NMapTasks = m.NMapTasks
      reply.TaskType = ReduceTask
      // change the reduce task state to processing
      // therefore, the task cannot be assigned to another worker
      task.State = PROCESSING
      task.isAssigned = true
      m.ReduceTasks[k] = task
      m.setReduceTimer(task.ID)
      break
    }
  }
}

func (m *Master) setReduceTimer(reduceId int) {
  task := m.ReduceTasks[reduceId]
  task.taskTimeout = time.AfterFunc(10*time.Second, func(reduceId int) func() {
    return func() {
      m.Mu.Lock()
      defer m.Mu.Unlock()
      if m.ReduceTasks[reduceId].State == PROCESSING {
        rTask := m.ReduceTasks[reduceId]
        rTask.State = TO_BE_PROCESSED
        rTask.isAssigned = false
        m.ReduceTasks[reduceId] = rTask
      }
    }
  }(reduceId))
  m.ReduceTasks[reduceId] = task
}

func (m *Master) UpdateMapTask(args *MRTaskUpdate, reply *MRTaskUpdate) error {
  m.Mu.Lock()
  defer m.Mu.Unlock()
  if m.MapTasks[args.MapTaskID].isAssigned == true {
    m.MapTasks[args.MapTaskID].taskTimeout.Stop() // stop the timer thereby preventing the function from reassigning the map task
    // this is the equivalent of marking the map task state as PROCESSED
    delete(m.MapTasks, args.MapTaskID)
    reply.Success = true
  }

  return nil
}

func (m *Master) UpdateReduceTask(args *MRTaskUpdate, reply *MRTaskUpdate) error {
  m.Mu.Lock()
  defer m.Mu.Unlock()
  if m.ReduceTasks[args.ReduceTaskID].isAssigned == true {
    m.ReduceTasks[args.ReduceTaskID].taskTimeout.Stop() // stop the timer thereby preventing the function from reassigning the map task
    // this is the equivalent of marking the reduce task state as PROCESSED
    // remove the reduce task from the list of tasks to be complete
    delete(m.ReduceTasks, args.ReduceTaskID)
    reply.Success = true
  }

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
  m.Mu.Lock()
  defer m.Mu.Unlock()
  if len(m.MapTasks) == 0 && len(m.ReduceTasks) == 0 {
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
  m.ReduceTasks = make(map[int]Task, nReduce)

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
    reduceTask := Task{ID: i, State: TO_BE_PROCESSED}
    m.ReduceTasks[i] = reduceTask
  }

  m.server()
  return &m
}
