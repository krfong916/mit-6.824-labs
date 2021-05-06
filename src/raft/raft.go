package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	term    int
	command int
}

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

	electionTimeout time.Time
}

const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

func (rf *Raft) convertToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = FOLLOWER
	rf.timeLastHeardFromLeader = time.Now() // could be a source of headache
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	rf.state = CANDIDATE
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = LEADER
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var isLeader = false
	if rf.state == LEADER {
		isLeader = true
	}
	return rf.currentTerm, isLeader
}

func (rf *Raft) persist() {}
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
	// long-running goroutine, will most likely call kill() or killed()

	////////////////////////////////////////////////////////
	// if we have a stale request vote
	// update the candidate's terms, disregard the request
	////////////////////////////////////////////////////////
	staleRequestVote := false
	rf.mu.Lock()
	log.Printf("[%d] receieved a request vote from %d", rf.me, args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	if staleRequestVote {
		return
	}

	////////////////////////////////////////////////////////////////////////
	// grant vote under the following conditions:
	// we haven't voted for anyone yet, and
	// the candidate's id and log is AT LEAST up-to-date as this peer's log
	////////////////////////////////////////////////////////////////////////
	rf.mu.Lock()
	log.Printf("[%d] granting a vote to %d", rf.me, args.CandidateId)
	if rf.votedFor == 0 && rf.currentTerm < args.Term {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.LastLogTerm
	}
	rf.mu.Unlock()
	return

	// Enforce election safety property:
	// what happens if we hear from a candidate that has a higher term number
	// and we've already granted a vote?
}

// caller implementation here: invoked by candidate to gather votes
// call request vote on other raft peers in parallel
// change state when we get replies
func (rf *Raft) performLeaderElection() {
	rf.convertToCandidate()

	/////////////////////////////////////////////////////////////////////////////
	// - Create Request Vote structs
	// - Vote for ourself, and initialize a term variable
	// We need the term variable because we may have to update our candidate
	// based on the follower's highest term, ie our candidate may be out of date
	/////////////////////////////////////////////////////////////////////////////
	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}

	rf.mu.Lock()
	log.Printf("[%d] attempting an election at term %d", rf.me, rf.currentTerm)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.Term = rf.currentTerm

	rf.votedFor = rf.me
	largestTermAmongstPeers := rf.currentTerm

	rf.mu.Unlock()

	////////////////////////////////////////////////////////////////////////
	// Grant vote under the following conditions:
	// we haven't voted for anyone yet, and
	// the candidate's id and log is AT LEAST up-to-date as this peer's log
	////////////////////////////////////////////////////////////////////////
	votes := 1
	finished := 1
	cond := sync.NewCond(&rf.mu)

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			log.Printf("[%d] granting a vote to %d", rf.me, args.CandidateId)
			nodeReceivedMessage := rf.sendRequestVote(peer, args, reply)
			// our candidate is out of date, update our term
			if nodeReceivedMessage && reply.Term > largestTermAmongstPeers {
				largestTermAmongstPeers = reply.Term
			}
			if nodeReceivedMessage && reply.VoteGranted {
				votes++
			}
			finished++
			// when the reply comes back
			// wake up those who are waiting for our response
			cond.Broadcast()
		}(peer)
	}

	// check if we've won the election, if not, chill, continue sending requests
	rf.mu.Lock()
	// look this part over - we really only need to hear from a majority, and not have to wait for all servers
	// that'd be a mistake if we had to wait
	for votes < len(rf.peers)/2 && finished != 10 {
		fmt.Printf("votes: %v and finished: %v", votes, finished)
		cond.Wait()
	}

	// lock and update to leader state, send heartbeats to establish leadership
	if votes >= 5 && largestTermAmongstPeers == rf.currentTerm {
		fmt.Println("elected leader")
		rf.convertToLeader()
		rf.establishAuthority()
	} // else ?

	rf.currentTerm = largestTermAmongstPeers
	rf.mu.Unlock()
}

// what happens if we've voted for a candidate
// and we recieve a new requestvote from a higher termed candidate?

// what if we can't establish authority? will that influence
// we've already receieved a majority of request votes and we've been recognized as leader
// whether or not we can reach peers is not our concern here because other nodes will detect an election timeout
// and rise to candidacy
// we broadcast to a majority of peers

func (rf *Raft) establishAuthority() {

	/* Create Heartbeat Objects */
	// QUESTION: is the append entries shared??
	// go is by reference, not by copy
	// many peers will be reading and possibly writing to the reply object

	rf.mu.Lock()
	largestTermAmongstPeers := rf.currentTerm
	// args.Entries = is intialized to empty by default

	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)

	finished := 1

	// this should be on a timer to establish authority once every "go around"
	for {
		for i := 0; i < len(rf.peers); i++ {

			args := &AppendEntriesArgs{}
			reply := &AppendEntriesReply{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me

			go func(peer int) {
				/* Send Heartbeats! */
				// how do we send to the peers?
				nodeReceivedHeartbeat := rf.sendAppendEntries(peer, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if nodeReceivedHeartbeat && reply.Term > largestTermAmongstPeers {
					largestTermAmongstPeers = reply.Term
				}

				finished++

				cond.Broadcast()
			}(i)
		}

		///////////////////////////////////////////////////////////////////////////
		// If we found out that we're no longer the leader via heartbeat messages
		// then step down as a leader, else continue to send out heartbeats
		///////////////////////////////////////////////////////////////////////////
		rf.mu.Lock()

		for largestTermAmongstPeers < rf.currentTerm || finished != len(rf.peers) {
			cond.Wait()
		}

		if largestTermAmongstPeers > rf.currentTerm {
			rf.convertToFollower()
		}
		_, isLeader := rf.GetState()

		rf.mu.Unlock()

		if isLeader == false {
			break
		}

		time.Sleep(time.Duration(1 * time.Second))
	}

	// do something with the result of the heartbeats
	// that is, if our reply term is > our term and success is false
	// step down to follower, if we step down will all other replies know? does cond.broadcast stop? do we disregard all other replies?

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("Sending Request Vote to: %v\n", server)
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

	/* Sender Implementation */

	/* Receiver Implementation */

	////////////////////////////////////////////////////////
	// candidate and follower
	// if the append entries message is stale, disregard
	////////////////////////////////////////////////////////
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

	////////////////////////////////////////////////////////
	// candidate
	// if the append entries message is from a leader,
	// and we ASSUME to be leader or candidate - step down
	///////////////////////////////////////////////////////
	nodeIsOutOfDate := false
	rf.mu.Lock()
	// bug here, must check if candidate or leader not just leader
	_, isLeader := rf.GetState()
	if (isLeader || rf.state == CANDIDATE) && args.Term > rf.currentTerm {
		rf.convertToFollower()
		rf.currentTerm = args.Term
		// restart our election timeout because we updated our
	}
	rf.mu.Unlock()
	if nodeIsOutOfDate {
		return
	}

	////////////////////////////////////////////////////////
	// follower
	// check if this is a heartbeat mechanism
	// if so, then recognize the new leader
	////////////////////////////////////////////////////////
	if heartbeat(args.Entries) {
		fmt.Println("heartbeat")
		rf.recognizeLeader(args)
	}

	////////////////////////////////////////////////////////
	// follower
	// We ain't done!! After determining if a heartbeat,
	// we still have some work to do...
	////////////////////////////////////////////////////////
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Println("Creating Peer")

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize Peer's in-memory state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER

	// election timeout
	rf.setElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.kickOffElectionTimeout()

	return rf
}

// this timeout is the average time between failures for a single-server
const lengthOfTimeout = 50 * time.Millisecond

func (rf *Raft) kickOffElectionTimeout() {
	for rf.killed() == false {
		rf.checkTimeElapsed()
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) checkTimeElapsed() {
	// assumes we're candidate and the election timeout has elapsed so we must begin a new election
	// this could be a place for bugs/headache in the future
	// if (rf.state == CANDIDATE || timeElapsed) && rf.state != LEADER {

	if time.Now().After(rf.electionTimeout) {
		fmt.Printf("Candidate %v starting election\n", rf.me)
		rf.setElectionTimeout() // reset the election timeout
		rf.performLeaderElection()
	}

}

/**
 * election timer
 * we only restart when:
 * - we begin a new election
 * - we get an AppendEntries from the *current* leader, if the args are outdated, we do not reset our time
 * - we grant a vote to another peer
 */
func (rf *Raft) setElectionTimeout() {
	timeout := time.Now()
	timeout = timeout.Add(lengthOfTimeout)
	ms := rand.Int63() % 400
	timeout = timeout.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTimeout = timeout
}
