package kvraft

import (
  "crypto/rand"
  "math/big"
  "sync"
  "time"

  "github.com/fatih/color"
  "github.com/krfong916/mit-6.824-labs/src/labrpc"
)

type Clerk struct {
  mu        sync.Mutex
  servers   []*labrpc.ClientEnd
  leaderID  int
  me        int64
  requestID int
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  ck.leaderID = 0  // assign the client a random server to make a request to
  ck.me = nrand()  // generate a unique ID for this client, the server uses this ID to identify a client's request and maintain request history
  ck.requestID = 0 // monotonically-increasing requestID
  return ck
}

func (ck *Clerk) Get(key string) string {
  ck.mu.Lock()
  ck.requestID++
  args := &GetArgs{
    Key:       key,
    Name:      "GET",
    ClientID:  ck.me,
    RequestID: ck.requestID,
  }
  ck.mu.Unlock()
  reply := &GetReply{}
  color.New(color.FgYellow).Printf("[GET_REQUEST][%v][%v]: key[%v]\n ", args.ClientID, args.RequestID, args.Key)
  for {
    ok := ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)
    if ok && reply.Err == OK {
      return reply.Value
    }
    // our request can fail for a number of reasons:
    // we can't contact the server (either because of network partition or leader failure),
    // our RPC call wasn't made to the current leader,
    // or we made a concurrent request and the server code made the choice to service a request other than ours,
    // in the event of failure, retry
    time.Sleep(10 * time.Millisecond)
    // given that we sleep after making a request, when we wake, reassign the server we believe to be the leader
    ck.assignNewLeader()
    reply.Err = ""
  }
}

func (ck *Clerk) PutAppend(key string, value string, op KVOperation) {
  ck.mu.Lock()
  ck.requestID++
  args := &PutAppendArgs{
    Key:       key,
    Value:     value,
    Name:      op,
    ClientID:  ck.me,
    RequestID: ck.requestID,
  }
  ck.mu.Unlock()
  reply := &PutAppendReply{}
  color.New(color.FgYellow).Printf("New Client Request[%v]: [PUT_APPEND_REQUEST]: %v\n", ck.me, args)
  for {
    // color.New(color.FgYellow).Printf("[PUT_APPEND_REQUEST]: leaderID[%v], clientID[%v], requestID[%v], key[%v], value[%v]\n ", ck.leaderID, ck.me, args.RequestID, args.Key, args.Value)
    ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)
    if ok && reply.Err == OK {
      return
    }
    // color.New(color.FgYellow).Printf("[PUT_APPEND_REQUEST]: WRONG_LEADER leaderID[%v]\n", ck.leaderID)

    // our request can fail for a number of reasons:
    // we can't contact the server (either because of network partition or leader failure),
    // our RPC call wasn't made to the current leader,
    // or we made a concurrent request and the server code made the choice to service a request other than ours,
    // in the event of failure, retry
    time.Sleep(10 * time.Millisecond)
    // given that we sleep after making a request, when we wake, reassign the server we believe to be the leader
    ck.assignNewLeader()
    reply.Err = ""
  }
}

func (ck *Clerk) assignNewLeader() {
  ck.mu.Lock()
  defer ck.mu.Unlock()
  ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
  return
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
  ck.PutAppend(key, value, "APPEND")
}

// color.New(color.FgYellow).Printf("[GET_REQUEST][%v][%v]: key[%v]\n ", args.ClientID, args.RequestID, args.Key)
// // color.New(color.FgGreen).Printf("[GET_REQUEST][%v][%v]: ok[%v]\n ", args.ClientID, args.RequestID, reply.Value)
