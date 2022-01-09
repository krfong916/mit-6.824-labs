// Key Value Service using Raft Module - Types and Constants
package kvraft

type Err string

const (
	OK              Err = "OK"              // client request was replicated, no errors to report!
	ErrNoKey        Err = "ErrNoKey"        // no key exists within our key value store
	ErrWrongLeader  Err = "ErrWrongLeader"  // a client amde a request to the server that's not the elader
	ErrStaleRequest Err = "ErrStaleRequest" // a server has already serviced a request with the same or lower request ID. This can arise as a result of handling concurrent requests, failure recovery etc.
	ErrTimedOut     Err = "ErrTimedOut"     // the Raft module could not replicate the request within the window of time
)

// We send and recieve on channels waiting for the result of replicating an operation.
// Replication Result is the object that contains result of replicating an operation.
type ReplicationResult struct {
	OK        bool   // signals that the operation was successfully replicated
	Err       Err    // if there were any complications with replicating the result, we specify the error
	RequestID int    // the replicated operation has a corresponding request ID, we use this ID to mark the latest seen ID
	ClientID  int64  // each replicated operation has a corresponding client ID. We keep track of the latest request made by a client
	Value     string // represents the value of replicated GET operation
}

type KVResult struct {
	OK    bool   // signals that the key value store was able to apply the operation
	Err   Err    // describes any issues that arise from applying the operation to the store
	Value string // represents the value of a key lookup in the key value store
}

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
