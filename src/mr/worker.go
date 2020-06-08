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
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	// create a task
	task := MRTaskArgs{}
	reply := MRTaskReply{}

	// send an RPC request to the master for a task and wait for a reply
	call("Master.RequestTask", &task, &reply)

	// if the type of task is to map over the contents of a file
	if (reply.ToMap == true) {
		// get the file at the filepath
		file, err := os.Open(reply.FilePath)
		if err != nil {
			log.Fatalf("cannot read %v", reply.FilePath)
		}
		// open the file and read all the contents into one byte array
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.FilePath)
		}
		file.Close()
		kvarr := mapf(reply.FilePath, string(content))

		// sort the array of key and values
		sort.Sort(ByKey(kvarr))

		fmt.Println(reply.ID)
		fmt.Println(reply.FilePath)

		// create a temporary file
		tempFileName := fmt.Sprintf("mr-%v-", reply.ID)
		temp, err := ioutil.TempFile("./", tempFileName)
		if err != nil {
			log.Fatalf("cannot create file")
		}

		// write to that file
		// an array of objects that contain key:value strings

		// store k/v pairs in an encoding that can be read by reduce tasks
		// encode the temporary file
		enc := json.NewEncoder(temp)

		// write the array of objects as a json object
		for _, kv := range kvarr {
			enc.Encode(&kv)
		}
		os.Rename(temp.Name(), "")
	}

	// if the type of task is to reduce the contents of a file
	// call reduce
	// if (reply.ToReduce == true) {
	// 	var numOccurrencesOfWord = reducef(reply.FilePath,)
	// }
	
	// do we send the result of the task to the master?
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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