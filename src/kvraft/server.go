package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ClientCommand string

const (
	PUT    = "PUT"
	APPEND = "APPEND"
	GET    = "GET"
)

type Op struct {
	Command ClientCommand
}

type ClientPayload struct {
	SerialNumber int
	Response     string
}

type KVServer struct {
	mu                 sync.Mutex
	me                 int
	rf                 *raft.Raft
	applyCh            chan raft.ApplyMsg
	dead               int32                 // set by Kill()
	maxraftstate       int                   // snapshot if log grows this big
	kvStore            map[string]string     // in-memory key-value state machine
	clientRequestStore map[int]ClientPayload // the largest serial number processed for each client (latest request) + the associated response

}

type RaftReply struct {
	index    int
	term     int
	isLeader bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.serviceRequest(args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.serviceRequest(args, reply)
}

func (kv *KVServer) serviceRequest(args *PutAppendArgs, reply *PutAppendReply) {
	if !hasKey(args.Key) {
		reply.Err = ErrNoKey
		return
	}

	raftReply := RaftInitReply{}

	// submit the command to Raft
	_, _, isLeader := kv.rf.Start(args)
	raftReply.isLeader = isLeader

	if !raftReply.isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Wait for the command to be replicated in the Raft cluster
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		raftCommittedReply <- kv.applyCh
		wg.Done()
	}()
	wg.Wait()

	// First check for Raft failure or leadership change
	// based on the commitment reply
	// i.e. compare the committed reply and the initial submit reply
	// if we have mismatch terms, this implies Raft underwent a leadership change
	// the initial leader that received our command is no longer leader of the cluster
	if raftReply.term != raftCommittedReply.CommandTerm {
		reply.Err = ErrWrongLeader // signal client to retry on a different server/Raft peer
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args.Op {
	case "PUT":
		kv.kvStore[args.Key] = args.Value
	case "APPEND":
		var buffer bytes.Buffer
		value := kv.kvStore[args.Key]
		buffer.WriteString(value)
		buffer.WriteString(args.Value)         // append
		kv.kvStore[args.Key] = buffer.String() // write back to the kv store
	case "GET":
		reply.Value = kv.kvStore[args.Key]
	}
	return
}

func hasKey(str string) bool {
	return str == ""
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvStore = make(map[string]string)
	kv.clientRequestStore = make(map[string]string)
	// You may need initialization code here.

	return kv
}
