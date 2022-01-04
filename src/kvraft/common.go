package kvraft

type Err string

const (
	OK              Err = "OK"              // client request was replicated, no errors to report!
	ErrNoKey        Err = "ErrNoKey"        // no key exists within our key value store
	ErrWrongLeader  Err = "ErrWrongLeader"  // a client amde a request to the server that's not the elader
	ErrStaleRequest Err = "ErrStaleRequest" // a server has already serviced a request with the same or lower request ID. This can arise as a result of handling concurrent requests, failure recovery etc.
	ErrTimedOut     Err = "ErrTimedOut"     // the Raft module could not replicate the request within the window fo time
)

type PutAppendArgs struct {
	Key       string
	Value     string
	Name      KVOperation // "Put" or "Append"
	RequestID int
	ClientID  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	Name      KVOperation
	RequestID int
	ClientID  int64
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
