package kvraft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"github.com/fatih/color"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Operation that Raft replicates
type Operation struct {
	Key       string
	Value     string
	Op        KVOperation
	ClientID  int
	RequestID int
}

// Client operation that the kv server records and executes just once
type ClientOperation struct {
	ClientID int
	Response interface{}
}

type KVServer struct {
	mu                     sync.Mutex
	me                     int
	rf                     *raft.Raft
	applyCh                chan raft.ApplyMsg
	dead                   int32                    // set by Kill()
	maxraftstate           int                      // snapshot if log grows this big
	kvStore                map[string]string        // in-memory key-value state machine
	result                 map[int](chan Operation) // a map of buffered channels that is used to send the result of replicated client operations
	clientOperationHistory map[int]ClientOperation  // the largest serial number processed for each client (latest request) + the associated response
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// prepare operation and response
	op := Operation{
		Key:       args.Key,
		Op:        args.Operation,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	ok, value := kv.waitForOpToComplete(op)

	if !ok {
		reply.Err = ErrWrongLeader
	} else {
		// execute the client's request by updating the kv store
		reply.Value = value
		reply.Err = OK
	}
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// prepare operation and response
	op := Operation{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Operation,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	color.New(color.FgWhite).Printf("[SERVER][%v] pending operation[%v] key: %v value: %v\n", kv.me, op.Op, op.Key, op.Value)
	ok, _ := kv.waitForOpToComplete(op)

	if !ok {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
	return
}

func (kv *KVServer) waitForOpToComplete(op Operation) (bool, string) {
	// submit the operation to Raft
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false, ""
	}

	kv.mu.Lock()
	if _, exists := kv.result[index]; !exists {
		kv.result[index] = make(chan Operation, 1)
	}
	kv.mu.Unlock()
	// insert the channel in the map of channels
	// how do we know when a request is finished so that we can notify the RPC?
	// well, when the request IS finished, then we know
	// exactly which channel to send the reply on

	select {
	case operation := <-kv.result[index]:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		color.New(color.FgGreen).Printf("Succeeded[%v] %v\n", kv.me, operation)
		color.New(color.FgGreen).Printf("from channel, operation: %v\n", operation.Op)
		color.New(color.FgGreen).Printf("from channel, value: %v\n", operation.Value)

		color.New(color.FgGreen).Printf("from channel, RequestID: %v\n", operation.RequestID)
		color.New(color.FgGreen).Printf("from channel, ClientID: %v\n", operation.ClientID)
		color.New(color.FgGreen).Printf("client request, RequestID: %v\n", op.RequestID)
		color.New(color.FgGreen).Printf("client request, ClientID: %v\n", op.ClientID)

		return isSameOperation(operation, op), operation.Value
	case <-time.After(800 * time.Millisecond):
		color.New(color.FgRed).Println("FAILED")
		return false, ""
	}
}

func isSameOperation(applyChannelOp Operation, clientOp Operation) bool {
	return applyChannelOp.ClientID == clientOp.ClientID &&
		applyChannelOp.RequestID == clientOp.RequestID
}

// We need this long-running task to commit commands to the state machine as they arrive on the apply channel
func (kv *KVServer) applyReplicatedCommands() {
	for !kv.killed() {
		select {

		// we will periodically receive messages on the applyCh for us to commit to our kvServer
		// sometimes our Raft module may be the leader, other times it may be the follower
		// when we receive a message, that means that the command has been replicated on a majority of
		case applyChannelResponse := <-kv.applyCh:
			color.New(color.FgMagenta).Printf("Server # %v received on apply channel %v\n", kv.me, applyChannelResponse)

			index := applyChannelResponse.CommandIndex
			op := applyChannelResponse.Command.(Operation)

			// notify the channel that was waiting for the response
			// i.e. send a response to that channel

			kv.mu.Lock()
			color.New(color.FgRed).Printf("SERVER %v ACQUIRED LOCK\n", kv.me)
			op.Value = kv.applyReplicatedOpToStateMachine(op)
			if _, ok := kv.result[index]; !ok {
				kv.result[index] = make(chan Operation, 1)
			} else {
				kv.result[index] <- op
			}
			kv.mu.Unlock()
			color.New(color.FgRed).Printf("SERVER %v RELEASED THE LOCK\n", kv.me)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// This is the only function that "touches" the application state
func (kv *KVServer) applyReplicatedOpToStateMachine(op Operation) string {
	var newValue string
	color.New(color.FgMagenta).Printf("Applying Operation to state machine%v\n", op)
	switch op.Op {
	case "GET":
		newValue = kv.kvStore[op.Key]
	case "PUT":
		kv.kvStore[op.Key] = op.Value
		newValue = op.Value
	case "APPEND":
		var buffer bytes.Buffer
		buffer.WriteString(kv.kvStore[op.Key])
		buffer.WriteString(op.Value)
		newValue := buffer.String()
		color.New(color.FgCyan).Printf("Newly appended value %v\n", newValue)
		kv.kvStore[op.Key] = newValue
	}
	fmt.Println("server #", kv.me)
	prettyPrint(kv.kvStore)
	return newValue
}

func prettyPrint(v interface{}) (err error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err == nil {
		fmt.Println(string(b))
	}
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Operation{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.result = make(map[int](chan Operation))
	kv.kvStore = make(map[string]string)
	kv.clientOperationHistory = make(map[int]ClientOperation)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// initialize long-running tasks
	go kv.applyReplicatedCommands()

	return kv
}
