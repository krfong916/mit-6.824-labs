package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// each Entry contains the term in which it was created
// and a command for the state machine
type Entry struct {
	term    int
	command int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers
	state                   string
	currentTerm             int
	votedFor                int
	timeLastHeardFromLeader time.Time

	// election
	randomizedElectionTimeout time.Duration
}

// this timeout is the average time between failures for a single-server
const electionTimeout = 500 * time.Millisecond

const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

func (rf *Raft) convertToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = FOLLOWER
	rf.timeLastHeardFromLeader = time.now() // could be a source of headache
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	rf.state = CANDIDATE
	rf.timeLastHeardFromLeader = time.Now() // reset the election timeout
}

func (rf *Raft) convertToLeader() {

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var isLeader bool

	if rf.state == LEADER {
		isLeader = true
	}

	return rf.currentTerm, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of Candidate's last log entry
	LastLogTerm  int // Term of Candidate's last entry
}

type RequestVoteReply struct {
	Term        int  // CurrentTerm, for Candidate to update itself
	VoteGranted bool // True means Candidate received vote
}

/**
 * RequestVote is a candidate's attempt to request votes from peers within
 * the cluster. A candidate's term must be >= this peer's current term
 * for a vote to be granted.
 *
 * @param  args
 * @param  reply
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// long-running goroutine, will most likely call kill() or killed()

	staleRequestVote := false
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	if staleRequestVote {
		return
	}

	//
	// grant vote if voted for == null,
	// or both, the candidate's id and log
	// is as @ least up-to-date as this peer's log
	//
	if rf.votedFor == 0 && rf.currentTerm <= args.Term {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.LastLogTerm
	}

	//
	// Enforce election safety property:
	// Update to the current term
	// if this peer's current term < the candidate's term
	//
	if rf.currentTerm < args.LastLogTerm {
		rf.currentTerm = args.LastLogTerm
	}
}

// caller implementation here: invoked by candidate to gather votes
// call request vote on other raft peers in parallel
// change state when we get replies
// see go-concurrency condvar vote-count-2-3-4
func (rf *Raft) performLeaderElection() {
	rf.convertToCandidate()

	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}

	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.Term = rf.currentTerm

	// we vote for ourself
	// and initialize a term variable updated on peer response
	rf.mu.Lock()
	rf.votedFor = rf.me
	largestTermAmongstPeers := rf.Term
	rf.mu.Unlock()
	count := 1
	finished := 1

	cond := sync.NewCond(&rf.mu)

	/* How do we detect if a follower's term is greater than our term? */
	for i := 0; i < len(rf.peers); i++ {
		go func(peer int) {
			nodeReceivedMessage := rf.sendRequestVote(peer, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// our candidate is out of date, update our term
			if nodeReceivedMessage && reply.Term > largestTermAmongstPeers {
				largestTermAmongstPeers = reply.Term
			}
			if nodeReceivedMessage && reply.VoteGranted {
				count++
			}
			finished++

			// when the reply comes back
			// wake up those who are waiting for our response
			cond.Broadcast()
		}(i)
	}

	// check if we've won the election, if not, chill, continue sending requests
	rf.mu.Lock()
	for count < len(rf.peers)/2 && finished != 10 {
		cond.Wait()
	}

	if count >= 5 && largestTermAmongstPeers == rf.Term {
		rf.convertToLeader()
		rf.establishAuthority()
		// lock and update to leader state, send heartbeats to establish leadership
	} else {
		// ?
	}
	rf.Term = largestTermAmongstPeers
	rf.mu.Unlock()
}

func establishAuthority() {

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int     // Leader's Term
	LeaderId int     // Needed so follower can redirect clients
	Entries  []Entry // Log entries to store, empty if heartbeat
}

type AppendEntriesReply struct {
	Success bool // True if follower contained entry matching log index and term
	Term    int  // The current term for the leader to update itself
}

/**
 * As of Lab 2A, we only care about the heartbeat mechanism.
 * No longer entries or commit indices.
 *
 * @param  args
 * @param	 reply
 */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// if the append entries message is stale, disregard
	// we may need to
	leaderIsBehind := false
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	rf.mu.Unlock()
	if leaderIsBehind {
		return
	}

	// IS THIS SECTION NECESSARY?
	// if the append entries message is from a leader and we ASSUME to be leader or candidate, step down
	nodeIsOutOfDate := false
	rf.mu.Lock()
	_, isLeader = rf.GetState()
	if (isLeader || rf.state == CANDIDATE) && args.Term > rf.currentTerm {
		rf.convertToFollower()
		rf.currentTerm = args.Term
		// do we restart our election timeout?
	}
	rf.mu.Unlock()
	if nodeIsOutOfDate {
		return
	}

	// check if this is a heartbeat mechanism
	if heartbeat(args.Entries) {
		rf.recognizeLeader(args)
	}
}

/**
 * A heartbeat is an AppendEntries message with no entries
 *
 * @return  boolean
 */
func heartbeat(log []Entry) bool {
	return len(log) == 0
}

/**
 * Assuming the message is valid,
 * updates this Raft peer's term and time.
 *
 * @param  args we need the leader's term from the heartbeat object
 */
func (rf *Raft) recognizeLeader(args *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timeLastHeardFromLeader = time.Now()
	rf.currentTerm = args.Term
	rf.votedFor = -1
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Println("make called")

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize Peer's in-memory state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER

	// election timeout
	rf.assignRandomizedSleepTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.kickOffElectionTimeout()

	return rf
}

// election timer
// we only restart when:
// - we begin a new election
// - we get an AppendEntries from the *current* leader, if the args are outdated, we do not reset our timer
// - we grant a vote to another peer
func (rf *Raft) kickOffElectionTimeout() {
	for {
		go func() {

			rf.mu.Lock()

			initialState := rf.timeLastHeardFromLeader.IsZero()
			timeElapsed := time.Now().Sub(rf.timeLastHeardFromLeader)

			rf.mu.Unlock()

			// assumes we're candidate and the election timeout has elapsed so we must begin a new election
			// this could be a place for bugs/headache in the future
			if rf.state == CANDIDATE || initialState || timeElapsed >= electionTimeout {
				rf.performLeaderElection()
			}
		}()

		time.Sleep(time.Duration(rf.randomizedElectionTimeout) * time.Millisecond)
	}
}

func (rf *Raft) assignRandomizedSleepTimeout() {
	rand.Seed(time.Now().UnixNano())
	min := 150
	max := 300
	rf.randomizedElectionTimeout = time.Duration(rand.Intn(max-min+1) + min)
}

// intialize
//
// use this for setting a randomized timeout
// rand.Seed(time.Now().UnixNano())
// min := 150
// max := 300
// fmt.Println(rand.Intn(max-min+1) + min)

// how do we deal with arrival of events
// appendEntries, requestVote, rpc replies - like listen for it
//
// - concurrent goroutines
//
// periodic tasks
// heartbeats and elections
//
// the election timeout should be driven by a separate goroutine
//
//
// electionTimeout goroutine
// 	use time.sleep to periodically wakeup
// 	wakeup from a sleep - or the timeout ends
//
// 	calculate t := current time - time last heard
// 	if t >= out bounds of timeout || rf.timeLastHeard.IsZero()

// 	implementation:
/* for {
	go func() {
		fmt.Println("Wakeup!")
		timeElapsedSinceHeard := time.Now() - rf.timeLastHeader
		if (rf.timeLastHeard.IsZero() || timeElapsedSinceHeard >= leaderElectionTimeout) {
			// start an election
			// restart the timeout
		}
	}()
	time.Sleep(electionTimeout * time.Second)
}
*/
// 	func main() {
// 	rf := &Raft{}
// 	rf.timeLastHeard = time.Now()
// //	timeElapsed := time.Now().Sub(rf.timeLastHeard)
// 	timeElapsed := time.Now().Add(5 * time.Second).Sub(rf.timeLastHeard)
// 	fmt.Println(timeElapsed)
// 	if rf.timeLastHeard.IsZero() || timeElapsed >= leaderElectionTimeout {
// 		fmt.Println("yeah, not initialized")
// 	}
// }
