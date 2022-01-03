package kvraft

import (
  "bytes"
  "encoding/json"
  "fmt"
  "log"
  "sync"
  "sync/atomic"
  "time"

  "github.com/fatih/color"
  "github.com/krfong916/mit-6.824-labs/src/labgob"
  "github.com/krfong916/mit-6.824-labs/src/labrpc"
  "github.com/krfong916/mit-6.824-labs/src/raft"
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

// Map of client operations that the kv server records before replicating
// this structure ensures operations are ordered and executed just once
type ClientOperation struct {
  ClientID int
  Op       KVOperation
}

type KVServer struct {
  mu              sync.Mutex              // a shared lock for managing concurrent-accessed state and cache
  me              int                     // our server ID
  rf              *raft.Raft              // raft instance
  applyCh         chan raft.ApplyMsg      // channel that the Raft module uses to send replicated operations to servers
  dead            int32                   // set by Kill()
  maxraftstate    int                     // snapshot if log grows this big
  kvStore         map[string]string       // in-memory key-value store
  clientOpHistory map[int]ClientOperation // the largest serial number processed for each client (latest request) + the associated response
  opComplete      map[int](chan Operation)          // map of channels for replicated ops
  // quit                   chan bool               // close long-running goroutines
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
  // prepare operation and response
  op := Operation{
    Key:       args.Key,
    Op:        args.Operation,
    ClientID:  args.ClientID,
    RequestID: args.RequestID,
  }

  ok, value, err := kv.waitForOpToComplete(op)

  if !ok {
    reply.Err = err
  } else {
    reply.Value = value
    reply.Err = OK
  }
  return
}

func (kv *KVServer) keyExists(key string) bool {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  if _, present := kv.kvStore[key]; present == false {
    return false
  } else {
    return true
  }
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
  color.New(color.FgCyan).Printf("[SERVER][%v] pending operation[%v] key: %v value: %v\n", kv.me, op.Op, op.Key, op.Value)
  ok, _, err := kv.waitForOpToComplete(op)
  // color.New(color.FgCyan).Printf("[SERVER][%v] completed operation[%v] key: %v value: %v status: %v\n", kv.me, op.Op, op.Key, op.Value, ok)
  if !ok {
    reply.Err = err
  } else {
    reply.Err = OK
  }
  return
}

func (kv *KVServer) waitForOpToComplete(op Operation) (bool, string, Err) {
  // submit the operation to Raft
  _, _, isLeader := kv.rf.Start(op)
  if !isLeader {
    return false, "", ErrWrongLeader
  }
  kv.mu.Lock()
  if kv.seenOperation(op.RequestID) {
    kv.mu.Unlock()
    return false, "", ErrDuplicateRequest
  }
  clientOp := ClientOperation{
    ClientID: op.ClientID,
    Op:       op.Op,
  }
  kv.clientOpHistory[op.RequestID] = clientOp
  kv.mu.Unlock()
  
  // insert the channel in the map of channels
  // how do we know when a request is finished so that we can notify the RPC?
  // well, when the request IS finished, then we know
  // exactly which channel to send the reply on
  kv.mu.Lock()
  if _, exists := kv.result[index]; !exists {
    kv.result[index] = make(chan Operation, 1)
  }
  kv.mu.Unlock()

  /*
     create a goroutine called handleResult
     handleResult will handle the result of messages sent on the applyChannel and return the value received

     create an unbuffered channel that is simply for reading
     we'll send the result from applyChannel to this unbuffered channel
     in this way: we are not reading and writing
     we want the channel to be non-blocking

     ch := make chan(bool)
     go func() {
       res := handleSthg()
       ch <- res
     }()

     wait to receive on ch
     select {
     case ch <- res:
     case <- time.After(ms):
       close(ch)
       or create two channels one for the send of a result and one for quit
       return
     }

     create a channel if one does not already exist
     pass the channel as an argument



  */
  readOpComplete := make(chan Operation, 1)
  go func() {
    res := kv.readReplicatedOp()
    // color.New(color.FgGreen).Printf("from channel, operation: %v\n", res)
    readOpComplete <- res
  }()

  for {
    select {
    case completedOp := <-readOpComplete:
      sameOp := isSameOperation(completedOp, op)
      color.New(color.FgGreen).Printf("COMPLETED, me: %v, operation: %v, requestID: %v, clientID: %v\n", kv.me, completedOp.Op, completedOp.RequestID, completedOp.ClientID)
      color.New(color.FgWhite).Printf("COMPLETED, me: %v, value: %v\n", kv.me, completedOp.Value)
      color.New(color.FgWhite).Printf("COMPLETED, me: %v, client request, RequestID: %v\n", kv.me, op.RequestID)
      color.New(color.FgWhite).Printf("COMPLETED, me: %v, client request, ClientID: %v\n", kv.me, op.ClientID)
      if sameOp {

        return sameOp, completedOp.Value, OK
      }

    case <-time.After(800 * time.Millisecond):
      color.New(color.FgRed).Println("FAILED")
      close(readOpComplete)
      return false, "", ErrTimedOut
   }
  }

}func (kv *KVServer) seenOperation(requestID int) bool {
  if _, exists := kv.clientOpHistory[requestID]; exists == true {
    return true
  } else {
    return false
  }
}

func (kv *KVServer) readReplicatedOp() Operation {
  // kv.mu.Lock()
  // defer kv.mu.Unlock()
  return <-kv.opComplete
}

/*
  declare a shared channel for the server instance

  func handleSthg(ch) {
    // use a mutex to read on the shared channel
    // if there is a result on the shared channel
    // send it to the channel as arg

    consider channel blocking
    buffered vs unbuffered channel

    with this shared channel, we could receive ops that the client did not submit, but were applied to the server
    also, if the channel was buffered, could it get full? the only way to drain is to read from it
    would we have to wait until the next goroutine came about in order to read from it?
    this issue arises because we have a timer that exits in the 800 ms frame

    so suppose we create a thread to that calls this fn
    the function returns the read from this channel
    what if the channel takes over 800ms for a response?
    we'd have a shared channel that has an item on it
    and we'd close the channel that we send the result on
    the code returns and the client receives a response

    we wait until the next client req comes around to kick off a new goroutine
    to read from the shared channel
    we read from the shared channel, but that

    but we could read many times from the channel.................. with the select case
    it just has to be within the 800ms window
    so there could be n-many req sitting on the channel that we could read from

    we might have to wrap our select case statement in a for-loop
    we need to drain the channel when other ops arrive on the channel

    we need separate send and receive goroutines

    // apply channel go routine will lock over the this shared channel and send on it
  }

*/

func isSameOperation(applyChannelOp Operation, clientOp Operation) bool {
  return applyChannelOp.ClientID == clientOp.ClientID &&
    applyChannelOp.RequestID == clientOp.RequestID
}

// what if we're applying replicated commands but the client has already returned?
func (kv *KVServer) applyReplicatedCommands() {
  for !kv.killed() {
    select {
    case applyChannelResponse := <-kv.applyCh:
      color.New(color.FgMagenta).Printf("Server # %v received on apply channel %v\n", kv.me, applyChannelResponse)
      op := applyChannelResponse.Command.(Operation)

      // color.New(color.FgRed).Printf("ApplyReplica %v ACQUIRED LOCK\n", kv.me)
      kv.mu.Lock()
      op.Value = kv.applyReplicatedOpToStateMachine(op)
      kv.mu.Unlock()
      // color.New(color.FgRed).Printf("ApplyReplica %v RELEASED THE LOCK\n", kv.me)

      // drain op-complete channel of any previous, stale client requests
    DRAIN_CHANNEL:
      for {
        select {
        case <-kv.opComplete:
        default:
          break DRAIN_CHANNEL
        }
      }
      // send the replicated op to the op-complete channel
      kv.opComplete <- op

    default:
      time.Sleep(10 * time.Millisecond)
    }
  }
}

// This is the only function that "touches" the application state
func (kv *KVServer) applyReplicatedOpToStateMachine(op Operation) string {
  var newValue string
  // color.New(color.FgMagenta).Printf("Applying Operation to state machine%v\n", op)
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
    // color.New(color.FgCyan).Printf("Newly appended value %v\n", newValue)
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
  // kv.quit <- true
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
  // kv.quit = make(chan bool)
  // kv.result = make(map[int](chan Operation))
  kv.opComplete = make(chan Operation, 1)
  kv.kvStore = make(map[string]string)
  kv.clientOpHistory = make(map[int]ClientOperation)
  kv.rf = raft.Make(servers, me, persister, kv.applyCh)

  // initialize long-running tasks
  go kv.applyReplicatedCommands()

  return kv
}

// operation
// index
//
