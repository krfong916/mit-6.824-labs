package kvraft

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
)

type PutAppendArgs struct {
	Key       string
	Value     string
	Operation KVOperation // "Put" or "Append"
	RequestID int
	ClientID  int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	Operation KVOperation
	RequestID int
	ClientID  int
}

type GetReply struct {
	Err   Err
	Value string
}

type KVOperation string

const (
	PUT    KVOperation = "PUT"
	APPEND KVOperation = "APPEND"
	GET    KVOperation = "GET"
)
