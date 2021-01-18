package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "sort"
import "encoding/json"
import "strings"

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
  	
  	if (response == false) {
  		terminateProcess()
  	}

  	switch reply.TaskType {
	  case MapTask:
	    _ = executeMapTask(mapf, reply)
	 	case ReduceTask:
	 		_ = executeReduceTask(reducef, reply)
	 	default:
	 		fmt.Println("No task assigned")
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
  fileNames := createIntermediateMRFiles(kvBuckets, task.MapTaskID)

  // Send an task update to the master
  update := MRTaskUpdate{}
  update.Files = fileNames
  update.MapTaskID = task.MapTaskID
  update.TaskType = task.TaskType
  
  call("Master.UpdateMapTask", &update, &reply)

  result := call("Master.UpdateMapTask", &update, &reply)

  if !result {
  	fmt.Println("error sending intermediate files to master")
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

func executeReduceTask(reducef func(string, []string) string, task MRTaskReply) bool {
	var kva []KeyValue
	outputFile := fmt.Sprintf("mr-out-%v", task.ReduceTaskID)

	// iterate over the list of intermediate files to reduce
	for _, file := range task.Files {
		// open the file
		file, err := os.Open(file)
		if err != nil {
	    log.Fatalf("cannot read %v", file)
	  }
	  // read the contents of the encoded file back
		dec := json.NewDecoder(file)
		// convert to KeyValue pairs
		for {
	    var kv KeyValue
	    if err := dec.Decode(&kv); err != nil {
	      break
	    }
	    kva = append(kva, kv)
	  }
	  file.Close()
	}
	
	sort.Sort(ByKey(kva))
	
	dict := make(map[string][]string)
	for _, pair := range kva {
		values, ok := dict[pair.Key]
		if (ok) {
			dict[pair.Key] = append(values, pair.Value)
		} else {
			dict[pair.Key] = []string{pair.Value}
		}
	}

	of, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		log.Fatalf("cannot create or open file %v", outputFile)
	}
	defer of.Close()
	for key, value := range dict {
		acc := reducef(key, value)
		uK := strings.ToUpper(key)
		res := fmt.Sprintf("%s %s\n", uK, acc)
		if _, err := of.WriteString(res); err != nil {
    	log.Println(err)
		}
	}
	// create a file with the following format MR-OUT-task.ID, this will be the output file
	// parse over the list of files
	// for each file
	//  read the contents into memory
	// after reading the contents into memory, sort the contents
	// call reducef with the sorted contents
	// write the string output to the file

	return true
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
	