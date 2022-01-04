package kvraft

import (
  "bytes"
  "encoding/json"
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

// Operation that Raft replicates.
type Operation struct {
  Key       string
  Value     string
  Name      KVOperation
  ClientID  int64
  RequestID int
}

// An object that describes the client operation that has been replicated.
type ReplicatedOperation struct {
  Operation Operation
  Err       Err
}

// The Key Value Service.
type KVServer struct {
  mu                      sync.Mutex                         // a shared lock for managing concurrent-accessed state and cache
  me                      int                                // our server ID
  rf                      *raft.Raft                         // raft instance
  applyCh                 chan raft.ApplyMsg                 // channel that the Raft module uses to send replicated operations to servers
  dead                    int32                              // set by Kill()
  maxraftstate            int                                // snapshot if log grows this big
  kvStore                 map[string]string                  // in-memory key-value store
  lastApplied             map[int64]int                      // the largest serial number processed for each client (latest request)
  replicatedOperationsMap map[int](chan ReplicatedOperation) // map of channels for replicated ops
  quit                    chan bool                          // close long-running goroutines
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
  // prepare operation and response
  op := Operation{
    Key:       args.Key,
    Name:      args.Name,
    ClientID:  args.ClientID,
    RequestID: args.RequestID,
  }

  value, err := kv.waitForOpToComplete(op)

  if err != "" {
    reply.Err = err
  } else {
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
    Name:      args.Name,
    ClientID:  args.ClientID,
    RequestID: args.RequestID,
  }
  color.New(color.FgCyan).Printf("[SERVER][%v] pending operation[%v] key: %v value: %v\n", kv.me, op.Name, op.Key, op.Value)
  _, err := kv.waitForOpToComplete(op)
  // color.New(color.FgCyan).Printf("[SERVER][%v] completed operation[%v] key: %v value: %v status: %v\n", kv.me, op.Op, op.Key, op.Value, ok)
  if err != "" {
    reply.Err = err
  } else {
    reply.Err = OK
  }
  return
}

func (kv *KVServer) waitForOpToComplete(op Operation) (string, Err) {
  // submit the operation to Raft
  index, _, isLeader := kv.rf.Start(op)
  if !isLeader {
    return "", ErrWrongLeader
  }

  kv.mu.Lock()
  // insert a new channel in the map of channels
  // how do we know when a request is finished so that we can notify the RPC?
  // well, when the client request has been replicated
  // we'll receive on a channel contained in this map
  replicatedOpsCh, exists := kv.replicatedOperationsMap[index]
  if exists == false {
    replicatedOpsCh = make(chan ReplicatedOperation, 1)
    kv.replicatedOperationsMap[index] = replicatedOpsCh
  }
  kv.mu.Unlock()

  select {
  case result := <-replicatedOpsCh:
    completedOp := result.Operation
    sameOp := isSameOperation(completedOp, op)
    color.New(color.FgGreen).Printf("COMPLETED, me: %v, operation: %v, requestID: %v, clientID: %v\n", kv.me, completedOp.Name, completedOp.RequestID, completedOp.ClientID)
    color.New(color.FgGreen).Printf("COMPLETED, me: %v, client request, RequestID: %v ClientID: %v\n", kv.me, op.RequestID, op.ClientID)
    color.New(color.FgGreen).Printf("COMPLETED, me: %v, value: %v\n", kv.me, completedOp.Value)
    if sameOp && result.Err == "" {
      return completedOp.Value, ""
    }
    // fix what we return
    return "", result.Err
  case <-time.After(800 * time.Millisecond):
    kv.mu.Lock()
    delete(kv.replicatedOperationsMap, index)
    kv.mu.Unlock()
    color.New(color.FgRed).Println("FAILED")
    return "", ErrTimedOut
  }
}

func isSameOperation(applyChannelOp Operation, clientOp Operation) bool {
  return applyChannelOp.ClientID == clientOp.ClientID &&
    applyChannelOp.RequestID == clientOp.RequestID
}

// Checks whether or not a key exists in the store.
func (kv *KVServer) keyExists(key string) bool {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  _, present := kv.kvStore[key]
  return present
}

// Applies replicated commands to the kv store, if applicable,
// and sends the result of this application to the corresponding channel waiting to synchronize with the result.
func (kv *KVServer) applyReplicatedCommands() {
  for !kv.killed() {
    select {
    case applyChannelResponse := <-kv.applyCh:
      color.New(color.FgMagenta).Printf("Server # %v received on apply channel %v\n", kv.me, applyChannelResponse)
      kv.mu.Lock()
      op := applyChannelResponse.Command.(Operation)
      index := applyChannelResponse.CommandIndex
      result := ReplicatedOperation{Operation: op}

      // Apply operation to the state machine
      if op.Name == "GET" {
        value, err := kv.applyReplicatedOpToStateMachine(op)
        if err != "" {
          result.Err = err
        } else {
          result.Operation.Value = value
        }
      } else {
        if kv.isStaleRequest(op.RequestID, op.ClientID) {
          result.Err = ErrStaleRequest
        } else {
          value, _ := kv.applyReplicatedOpToStateMachine(op)
          result.Operation.Value = value
          kv.lastApplied[op.ClientID] = op.RequestID
        }
      }

      kv.notifyReplicatedOpCh(index, result)
      kv.mu.Unlock()
    // case <-kv.quit:
    //   return
    default:
      time.Sleep(10 * time.Millisecond)
    }
  }
}

func (kv *KVServer) notifyReplicatedOpCh(index int, result ReplicatedOperation) {
  // Send the replicated operation result on the replicated result channel
  replicatedOpsCh, exists := kv.replicatedOperationsMap[index]
  if exists == false {
    replicatedOpsCh = make(chan ReplicatedOperation, 1)
    kv.replicatedOperationsMap[index] = replicatedOpsCh
  } else if exists == true && len(replicatedOpsCh) == 1 {
    color.New(color.FgRed).Println("DRAINING")
  DRAIN_CHANNEL:
    for {
      select {
      case <-replicatedOpsCh:
      default:
        break DRAIN_CHANNEL
      }
    }
  }

  kv.replicatedOperationsMap[index] <- result
}

// This is the only function that "touches" the application state
func (kv *KVServer) applyReplicatedOpToStateMachine(op Operation) (string, Err) {
  var value string
  var error Err
  color.New(color.FgMagenta).Printf("Applying Operation to state machine%v\n", op)
  switch op.Name {
  case "GET":
    val, ok := kv.kvStore[op.Key]
    if !ok {
      error = ErrNoKey
    } else {
      value = val
    }
  case "PUT":
    value = op.Value
    kv.kvStore[op.Key] = op.Value
  case "APPEND":
    var buffer bytes.Buffer
    buffer.WriteString(kv.kvStore[op.Key])
    buffer.WriteString(op.Value)
    value := buffer.String()
    color.New(color.FgCyan).Printf("Newly appended value %v\n", value)
    kv.kvStore[op.Key] = value
  }
  color.New(color.FgWhite).Printf("server #%v\n", kv.me)
  prettyPrint(kv.kvStore)
  return value, error
}

func (kv *KVServer) isStaleRequest(requestID int, clientID int64) bool {
  lastApplied, present := kv.lastApplied[clientID]
  if !present {
    return false
  } else {
    return lastApplied >= requestID
  }
}

func prettyPrint(v interface{}) (err error) {
  b, err := json.MarshalIndent(v, "", "  ")
  if err == nil {
    color.New(color.FgWhite).Printf("%v\n", string(b))
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
  kv.replicatedOperationsMap = make(map[int](chan ReplicatedOperation))
  kv.kvStore = make(map[string]string)
  kv.lastApplied = make(map[int64]int)
  kv.quit = make(chan bool)
  kv.rf = raft.Make(servers, me, persister, kv.applyCh)

  // initialize long-running tasks
  go kv.applyReplicatedCommands()

  return kv
}
