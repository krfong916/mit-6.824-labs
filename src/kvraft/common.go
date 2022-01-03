package kvraft

type Err string

const (
	OK                  Err = "OK"                  // client request was replicated, no errors to report!
	ErrNoKey            Err = "ErrNoKey"            // no key exists within our key value store
	ErrWrongLeader      Err = "ErrWrongLeader"      // a client amde a request to the server that's not the elader
	ErrDuplicateRequest Err = "ErrDuplicateRequest" // a server has already serviced a request with the same request ID, as result of handling concurrent requests, failure recovery etc.
	ErrTimedOut         Err = "ErrTimedOut"         // the Raft module could not replicate the request within the window fo time
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
