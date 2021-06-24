package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderID int
	me       int
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
	ck.leaderID = int(nrand()) % len(servers) // assign a random server at runtime
	ck.me = int(nrand())                      // generate a unique ID for this client, server uses this ID to identify a client's request/maintain information

	return ck
}

func (ck *Clerk) Get(key string) string {
	// prepare args
	args := &GetArgs{
		Key:      key,
		Op:       "GET",
		ClientID: ck.me,
	}
	reply := &GetReply{}

	// RPC call to the server
	ok := ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)

	if reply.Err == ErrNoKey {
		return ""
	}

	// retry if we can't contact the server or our RPC call wasn't to the leader
	if !ok || reply.Err == ErrWrongLeader {
		ck.assignNewLeader()
		ok = ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)
	}

	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// prepare args
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.me,
	}
	reply := &PutAppendReply{}

	// RPC call to the server
	ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)

	// retry if we can't contact the server or our RPC call wasn't to the leader
	for !ok || reply.Err == ErrWrongLeader {
		ck.assignNewLeader()
		ok = ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)
	}
}

func (ck *Clerk) assignNewLeader() {
	// if ck.leaderID != reply.leaderID {
	// 	ck.leaderID = reply.leaderID
	// } else {
	ck.assignRandomServer()
	// }
}

func (ck *Clerk) assignRandomServer() {
	oldLeaderID := ck.leaderID
	for oldLeaderID == ck.leaderID {
		ck.leaderID = nrand() % int64(len(ck.servers))
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "APPEND")
}
