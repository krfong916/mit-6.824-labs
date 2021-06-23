package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderID int
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
	ck.leaderID = nrand() % len(servers)
	return ck
}

func (ck *Clerk) Get(key string) string {
	// prepare args
	args := &GetArgs{
		Key: key,
		Op:  "Get",
	}
	reply := &GetReply{}

	// RPC call to the server
	ok := ck.servers[i].Call("KVServer.Get", args, reply)

	if reply.Err == ErrNoKey {
		return ""
	}

	// retry if we can't contact the server or our RPC call wasn't to the leader
	if !ok || reply.Err == ErrWrongLeader {
		rf.getLeaderId()
		ok = ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)
		time.Sleep(10 * time.Millisecond)
	}

	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// prepare args
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := &PutAppendReply{}

	// RPC call to the server
	ok := ck.servers[leaderID].Call("KVServer.PutAppend", args, reply)

	// retry if we can't contact the server or our RPC call wasn't to the leader
	for !ok || reply.Err == ErrWrongLeader {
		rf.getLeaderId()
		ok = ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) getLeaderId() {
	if ck.leaderID != reply.leaderID {
		ck.leaderID = reply.leaderID
	} else {
		ck.getRandomServer()
	}
}

func (ck *Clerk) getRandomServer() {
	oldLeaderID := ck.leaderId
	for oldLeaderID == ck.leaderId {
		ck.leaderId = nrand() % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
