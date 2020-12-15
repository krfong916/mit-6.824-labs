package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
// import "sort"
import "encoding/json"

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
func Worker(mapf func(string, string) []KeyValue,
  reducef func(string, []string) string) {
  args := MRTaskArgs{}
  reply := MRTaskReply{}

  call("Master.RequestTask", &args, &reply)
  switch reply.TaskType {
  case MapTask:
    _ = ExecuteMapTask(mapf, reply)
 	case ReduceTask:
 		// ExecuteReduceTask(reducef, reply)

 	default:
 		fmt.Println("No task assigned")
  }
}

func ExecuteMapTask(mapf func(string, string) []KeyValue, task MRTaskReply) bool {
  update := MRTaskUpdate{}
  reply := MRTaskReply{}

  contents := getFileContents(task.File)

  // Pass the contents to mapf and accumulate the intermediate Map output
  kvarr := mapf(task.File, string(contents))

  // Partition contents into n-many Reduce "Buckets" for Reduce Workers to consume
  kvBuckets := Partition(kvarr, task.NReduce)

  // write the map output to disk
  fileNames := createIntermediateMRFiles(kvBuckets, task.MapTaskID)
  update.Files = fileNames
  update.MapTaskID = task.MapTaskID

  result := call("Master.UpdateTask", &update, &reply)

  if !result {
  	fmt.Println("error sending intermediate files to master")
  }

  return result
}

func Partition(kvarr []KeyValue, nReduce int) [][]KeyValue {
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
func createIntermediateMRFiles(kvBuckets [][]KeyValue, mapTaskID int) []string {
	var fileNames []string

  for reduceTaskNum, kvarr := range kvBuckets {
  	// if there are kv pairs for the reduce task number : INSPECT (do we need this check?)
  	if (len(kvarr) > 0) {
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
		  intermediateFileName := fmt.Sprintf("mr-%v-%v", mapTaskID,reduceTaskNum)
		  os.Rename(temp.Name(), intermediateFileName)
		  fileNames = append(fileNames, intermediateFileName)
	  }
  }
  return fileNames
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
