package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"../labrpc"
	"github.com/fatih/color"
)

type Clerk struct {
	mu        sync.Mutex
	servers   []*labrpc.ClientEnd
	leaderID  int
	me        int
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
	ck.leaderID = 0      // assign the client a random server to make a request to
	ck.me = int(nrand()) // generate a unique ID for this client, the server uses this ID to identify a client's request and maintain request history
	ck.requestID = 0     // monotonically-increasing requestID
	return ck
}

func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.requestID++
	args := &GetArgs{
		Key:       key,
		Operation: "GET",
		ClientID:  ck.me,
		RequestID: ck.requestID,
	}
	ck.mu.Unlock()
	reply := &GetReply{}
	// color.New(color.FgYellow).Printf("[GET_REQUEST][%v][%v]: key[%v]\n ", args.ClientID, args.RequestID, args.Key)

	for {
		color.New(color.FgYellow).Printf("[GET_REQUEST][%v][%v]: key[%v]\n ", args.ClientID, args.RequestID, args.Key)
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)
		if ok && reply.Err != ErrWrongLeader {
			color.New(color.FgGreen).Printf("[GET_REQUEST][%v][%v]: ok[%v]\n ", args.ClientID, args.RequestID, reply.Value)
			return reply.Value
		}
		// retry if we can't contact the server or our RPC call wasn't to the leader
		ck.assignNewLeader()
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op KVOperation) {
	ck.mu.Lock()
	ck.requestID++
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Operation: op,
		ClientID:  ck.me,
		RequestID: ck.requestID,
	}
	ck.mu.Unlock()
	reply := &PutAppendReply{}
	color.New(color.FgYellow).Printf("New Client Request[%v]: [PUT_APPEND_REQUEST]: %v\n", ck.me, args)
	for {
		color.New(color.FgYellow).Printf("[PUT_APPEND_REQUEST]: leaderID[%v], clientID[%v], requestID[%v], key[%v], value[%v]\n ", ck.leaderID, ck.me, args.RequestID, args.Key, args.Value)
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)
		if ok && reply.Err != ErrWrongLeader {
			return
		}
		color.New(color.FgYellow).Printf("[PUT_APPEND_REQUEST]: WRONG_LEADER leaderID[%v]\n", ck.leaderID)
		// retry if we can't contact the server or our RPC call wasn't to the leader
		ck.assignNewLeader()
		time.Sleep(10 * time.Millisecond)
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
