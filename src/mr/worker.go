package mr

import (
  "encoding/json"
  "fmt"
  "hash/fnv"
  "io/ioutil"
  "log"
  "net/rpc"
  "os"
  "sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
  Key   string
  Value string
}

// Sort implementation for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int {
  return len(a)
}

func (a ByKey) Swap(i, j int) {
  a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
  return a[i].Key < a[j].Key
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
  for {
    response, reply := requestTask()

    if response == false {
      terminateProcess()
    }

    switch reply.TaskType {
    case MapTask:
      _ = executeMapTask(mapf, reply)
    case ReduceTask:
      _ = executeReduceTask(reducef, reply)
    case ExitTask:
      terminateProcess()
    }
  }
}

func requestTask() (bool, MRTaskReply) {
  args := MRTaskArgs{}
  reply := MRTaskReply{}
  rpcReq := call("Master.RequestTask", &args, &reply)
  return rpcReq, reply
}

func terminateProcess() {
  os.Exit(3)
}

func executeMapTask(mapf func(string, string) []KeyValue, task MRTaskReply) bool {
  reply := MRTaskReply{}
  contents := getFileContents(task.File)

  // Pass the contents to the map function and accumulate the intermediate output
  kvarr := mapf(task.File, string(contents))

  // Partition contents into n-many "Buckets" for Reduce Workers to consume
  kvBuckets := partition(kvarr, task.NReduce)

  // Write the map output to disk
  createIntermediateMRFiles(kvBuckets, task.MapTaskID)

  // Send an task update to the master
  update := MRTaskUpdate{}
  update.MapTaskID = task.MapTaskID
  update.TaskType = task.TaskType

  result := call("Master.UpdateMapTask", &update, &reply)

  if !result {
    fmt.Println("error notifying master the status of map task")
  }

  return result
}

func partition(kvarr []KeyValue, nReduce int) [][]KeyValue {
  kvBuckets := make([][]KeyValue, nReduce)
  for _, kv := range kvarr {
    reduceTaskNum := ihash(kv.Key) % nReduce
    kvBuckets[reduceTaskNum] = append(kvBuckets[reduceTaskNum], kv)
  }
  return kvBuckets
}

//
// Create intermediate files for reduce workers to read from
// each bucket signifies an intermediate file that must be created
// for each array in the partition
// create a temp file
// encode instances of a kv to json objects
// iterate over each kv pair
// encode the kv pair to json
// and append each kv pair to the file
//
func createIntermediateMRFiles(kvBuckets [][]KeyValue, mapTaskID int) {
  for reduceTaskNum, kvarr := range kvBuckets {
    // create a temporary file
    tempFileName := fmt.Sprintf("mr-%v-%v-", mapTaskID, reduceTaskNum)
    temp, err := ioutil.TempFile("./", tempFileName)
    check(err)
    // write the kv pairs to the temp file
    enc := json.NewEncoder(temp)
    for _, kv := range kvarr {
      err := enc.Encode(&kv)
      check(err)
    }

    // atomically write the encoded data into an intermediate map file
    intermediateFileName := fmt.Sprintf("mr-%v-%v", mapTaskID, reduceTaskNum)
    os.Rename(temp.Name(), intermediateFileName)
  }
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
  h := fnv.New32a()
  h.Write([]byte(key))
  return int(h.Sum32() & 0x7fffffff)
}

//
// open file and read contents into memory
//
func getFileContents(fileName string) []byte {
  file, err := os.Open(fileName)
  if err != nil {
    log.Fatalf("cannot read %v", fileName)
  }
  contents, err := ioutil.ReadAll(file)
  if err != nil {
    log.Fatalf("cannot read %v", fileName)
  }
  file.Close()
  return contents
}

func executeReduceTask(reducef func(string, []string) string, task MRTaskReply) bool {
  reply := MRTaskReply{}

  reduceIntermFiles(reducef, task)

  // Send an task update to the master
  update := MRTaskUpdate{}
  update.ReduceTaskID = task.ReduceTaskID
  update.TaskType = task.TaskType

  result := call("Master.UpdateReduceTask", &update, &reply)

  if !result {
    fmt.Println("error notifying master the status of reduce task")
  }

  return result
}

func reduceIntermFiles(reducef func(string, []string) string, task MRTaskReply) {
  kva := make([]KeyValue, 0)

  // iterate over the list of intermediate files to reduce
  for i := 0; i < task.NMapTasks; i++ {
    intermFile := fmt.Sprintf("mr-%v-%v", i, task.ReduceTaskID)
    // open the file
    iFile, err := os.Open(intermFile)
    if err != nil {
      log.Fatalf("cannot read %v", iFile)
    }
    // read the contents of the encoded file back
    dec := json.NewDecoder(iFile)
    // convert to KeyValue pairs
    for {
      var kv KeyValue
      if err := dec.Decode(&kv); err != nil {
        break
      }
      kva = append(kva, kv)
    }
  }

  sort.Sort(ByKey(kva))

  // create an output file to write reduced output to
  outputFile := fmt.Sprintf("mr-out-%d", task.ReduceTaskID)
  ofile, err := os.Create(outputFile + ".tmp")
  check(err)

  // call Reduce on each distinct key in kva ([] KeyValue)
  i := 0
  for i < len(kva) {
    j := i + 1
    for j < len(kva) && kva[j].Key == kva[i].Key {
      j++
    }
    values := []string{}
    for k := i; k < j; k++ {
      values = append(values, kva[k].Value)
    }

    output := reducef(kva[i].Key, values)

    // write the key's accumulated value to the output file
    fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

    i = j
  }
  ofile.Close()
  os.Rename(outputFile+".tmp", outputFile)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
  // c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
  sockname := masterSock()
  c, err := rpc.DialHTTP("unix", sockname)
  if err != nil {
    log.Fatal("dialing:", err)
  }
  defer c.Close()
  err = c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }
  fmt.Println(err)
  return false
}
