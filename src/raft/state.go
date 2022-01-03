package raft

import (
	"sync"
	"time"

	"github.com/krfong916/mit-6.824-labs/src/labrpc"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile state on all servers
	commitIndex     int       // Index of the highest log entry known to be committed (init to 0, increases monotonically)
	lastApplied     int       // Index of the highest log entry applied to state machine (initialized to 0, increases monotonically)
	electionTimeout time.Time // Election timeout
	state           string

	// Volatile state on leaders
	nextIndex  []int // index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyCh chan ApplyMsg
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := false
	if rf.state == LEADER {
		isLeader = true
	}
	return rf.currentTerm, isLeader
}
