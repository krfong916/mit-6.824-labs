package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "sort"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	task := MRTaskArgs{}
	reply := MRTaskReply{}
	call("Master.RequestTask", &task, &reply)
	Map(mapf, reply)
	// Reduce(reducef)
	// call("Master.Done")
}

func Map(mapf func(string, string) []KeyValue, reply MRTaskReply) {

	// get the file at the filepath
	file, err := os.Open(reply.FilePath)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FilePath)
	}

	// read all the contents of the input file
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FilePath)
	}
	file.Close()

	// Pass the contents to mapf
	// accumulate the intermediate Map output
	kvarr := mapf(reply.FilePath, string(content))

	// sort the array of key and values
	sort.Sort(ByKey(kvarr))

	// create a temporary file
	tempFileName := fmt.Sprintf("temp-%v", reply.MapTaskID)
	temp, err := ioutil.TempFile("./", tempFileName)
	if err != nil {
		log.Fatalf("cannot create file")
	}

	// store k/v pairs in an encoding that can be read by reduce tasks
	// encode the temporary file
	enc := json.NewEncoder(temp)

	// write the array of objects as a json object
	for _, kv := range kvarr {
		fmt.Println(kv)
		reduceTaskNum := chooseReduceTaskNumber(kv, reply.ReduceTaskID)
		fmt.Println(reduceTaskNum)
		enc.Encode(&kv)
	}

	// atomically write the encoded data into an intermediate map file
	intermediateFileName := fmt.Sprintf("mr-%v-%v", reply.MapTaskID, reply.ReduceTaskID)
	os.Rename(temp.Name(), intermediateFileName)
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

func chooseReduceTaskNumber(kv KeyValue, nReduce int) int {
	return ihash(kv.Key) % nReduce
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


// func Reduce(reducef func(string, []string) string) {
// 	getIntermediateFile()

// 	// read the contents in as a data format
	
// 	// reduce over the contents
// 	output := reducef(intermediate[i].Key, values)
// 	// place the reduced values into a file
// 	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)


// 	// Remove the intermediate file from the directory
// 	os.Remove(filePath)
// }

// func getIntermediateFile() {
// 	// read an intermediate file from the directory
// 	pwdFiles, err := ioutil.ReadDir(".")
// 	check(err)

// 	// check if there is an intermediate file that we can perform a reduce on
// 	for _, file := range pwdFiles {
// 		match, err := regexp.MatchString(`mr-\d`, "mr-")
// 		check(err)
// 		if (match) {
// 			filePath = file.Name()
// 			break;
// 		}
// 	}
// }
