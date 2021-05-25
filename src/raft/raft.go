package raft

import (
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
	CommandTerm  int
}

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

const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

func (rf *Raft) persist() {}
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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

	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	newEntry := Entry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, newEntry)
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.mu.Unlock()

	go rf.attemptCommitEntry()
	return index, term, isLeader
}

func (rf *Raft) attemptCommitEntry() {
	for rf.killed() == false {
		_, isLeader := rf.GetState()
		if !isLeader {
			return
		}

		// color.New(color.FgCyan).Printf("Leader[%v]: attempting to commit entry\n", rf.me)

		for peer := 0; peer < len(rf.peers); peer++ {
			if peer == rf.me {
				continue
			}

			rf.mu.Lock()

			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[peer] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[rf.nextIndex[peer]:]

			rf.mu.Unlock()
			go func(peer int) {
				reply := &AppendEntriesReply{}
				messageReceived := rf.sendAppendEntries(peer, args, reply)

				if !messageReceived {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				// if we're no longer leader, we halt execution immediately
				if rf.state != LEADER {
					return
				}
				// if the peer's term is larger than our term when we sent the request
				// we must step down to follower, our term is out of date
				if reply.Success == false && reply.Term > args.Term {
					rf.convertToFollower(reply.Term)
					return
				}
				// we must retry
				if reply.Success == false && reply.Term == args.Term {
					rf.nextIndex[peer] = rf.nextIndex[peer] - 1
				}
				// we've successfully replicated the entry
				if reply.Success && reply.Term == args.Term {
					rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
					leaderLogIdx := len(rf.log) - 1
					if leaderLogIdx > rf.commitIndex {
						majorityReplicated := 0
						for peer := 0; peer < len(rf.peers); peer++ {
							if rf.matchIndex[peer] >= leaderLogIdx || peer == rf.me {
								majorityReplicated++
							}
						}
						if majorityReplicated > len(rf.peers)/2 && (rf.log[leaderLogIdx].Term == rf.currentTerm) {
							// color.New(color.FgRed).Printf("Leader[%v]: new commit index: %v\n", rf.me, leaderLogIdx)
							rf.commitIndex = leaderLogIdx
							rf.applyEntry()
						}
					}
				}
				return
			}(peer)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// Rule 4 for Rules for Leaders
// func (rf *Raft) updateCommitIndex() {
// leaderLogIdx := len(rf.log) - 1
// if leaderLogIdx > rf.commitIndex {
// 	majorityReplicated := 0
// 	for peer := 0; peer < len(rf.peers); peer++ {
// 		if rf.matchIndex[peer] >= leaderLogIdx || peer == rf.me {
// 			majorityReplicated++
// 		}
// 	}
// 	if majorityReplicated > len(rf.peers)/2 && (rf.log[leaderLogIdx].Term == rf.currentTerm) {
// 		color.New(color.FgRed).Printf("Leader[%v]: new commit index: %v\n", rf.me, leaderLogIdx)
// 		rf.commitIndex = leaderLogIdx
// 		go rf.applyEntry()
// 	}
// }
// }

// func (rf *Raft) applyEntry() {
// 	rf.mu.Lock()
// 	defer rf.mu.Lock()
// 	for ; rf.lastApplied <= rf.commitIndex; rf.lastApplied++ {
// 		entry := rf.log[rf.lastApplied]
// 		msg := ApplyMsg{
// 			CommandValid: true,
// 			Command:      entry.Command,
// 			CommandIndex: rf.lastApplied,
// 		}
// 		color.New(color.FgRed).Printf("Sent a reply message\n")
// 		rf.applyCh <- msg
// 	}
// 	color.New(color.FgRed).Printf("lastApplied: %v", rf.lastApplied)
// 	return
// }

func (rf *Raft) applyEntry() {
	// for !rf.killed() {
	// time.Sleep(10 * time.Millisecond)
	// rf.mu.Lock()
	// color.New(color.FgRed).Printf("(%v)[%v]: commitIndex: %v, lastApplied: %v\n", rf.state, rf.me, rf.commitIndex, rf.lastApplied)
	for rf.commitIndex > rf.lastApplied {

		// i := rf.lastApplied; i <= rf.commitIndex; i++ {
		rf.lastApplied += 1
		entry := rf.log[rf.lastApplied]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.lastApplied,
			CommandTerm:  entry.Term,
		}
		// color.New(color.FgRed).Printf("msg: %v\n", msg)
		// color.New(color.FgRed).Printf("lastApplied: %v\n, ", rf.lastApplied, rf.commitIndex)
		rf.applyCh <- msg
		// }

		// color.New(color.FgRed).Printf("(%v)[%v]: commitIndex: %v, lastApplied: %v\n", rf.state, rf.me, rf.commitIndex, rf.lastApplied)
	}
	// rf.mu.Unlock()
	return
	// }
}

func (rf *Raft) GetState() (int, bool) {
	var isLeader = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		isLeader = true
	}
	return rf.currentTerm, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) convertToFollower(term int) {
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.setElectionTimeout()
}

func (rf *Raft) convertToLeader() {
	rf.state = LEADER
	rf.setElectionTimeout()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// fmt.Printf("I am the captain now. Log length: %v\n", len(rf.log))
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
	}
}

func (rf *Raft) convertToCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.setElectionTimeout()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// fmt.Printf("Peer[%v]: creating peer\n", me)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize Peer's in-memory state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	rf.log = []Entry{{0, nil}}

	// election timeout
	rf.setElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.kickOffElectionTimeout()
	// go rf.applyEntry()
	return rf
}

func (rf *Raft) setElectionTimeout() {
	interm := rand.NewSource(time.Now().UnixNano())
	random := rand.New(interm)
	timeout := time.Duration(380+random.Int63()%150) * time.Millisecond
	rf.electionTimeout = time.Now().Add(timeout)
}

func sleep() {
	ms := 100
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) kickOffElectionTimeout() {
	for rf.killed() == false {
		rf.checkTimeElapsed()
		sleep()
	}
}

func (rf *Raft) checkTimeElapsed() {
	rf.mu.Lock()
	elapsed := time.Now().After(rf.electionTimeout)
	rf.mu.Unlock()
	_, isLeader := rf.GetState()

	if elapsed && isLeader == false {
		rf.performLeaderElection()
	}
}

func (rf *Raft) performLeaderElection() {
	rf.mu.Lock()
	/////////////////////////////////////////////////////////////////////////////
	// Convert To Candidate
	/////////////////////////////////////////////////////////////////////////////
	rf.convertToCandidate()
	// color.New(color.FgMagenta).Printf("Peer[%v]: converting from Follower -> Candidate\n", rf.me)
	// color.New(color.FgMagenta).Printf("Candidate[%v]: starting an election at Term: %v\n", rf.me, rf.currentTerm)
	/////////////////////////////////////////////////////////////////////////////
	// - Create Request Vote structs
	// - Vote for ourself, and initialize a term variable
	// We need the term variable because we may have to update our candidate
	// based on the follower's highest term, ie our candidate may be out of date
	/////////////////////////////////////////////////////////////////////////////
	args := &RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.votedFor = rf.me
	termChanged := false
	higherTerm := rf.currentTerm
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
			reply := &RequestVoteReply{}
			nodeReceivedMessage := rf.sendRequestVote(peer, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if nodeReceivedMessage && reply.VoteGranted && reply.Term == rf.currentTerm && rf.state == CANDIDATE {
				votes++
			} else if reply.Term > rf.currentTerm {
				termChanged = true
				higherTerm = reply.Term
			}
			finished++
			cond.Broadcast()
		}(peer)
	}
	rf.mu.Lock()

	for votes <= len(rf.peers)/2 && finished != len(rf.peers) && termChanged == false {
		cond.Wait()
	}

	// it may be the case that we've gained many votes, but our term was out of date
	if termChanged && higherTerm != rf.currentTerm {
		// color.New(color.FgRed).Printf("Candidate[%v]: stepping down to follower\n", rf.me)
		rf.convertToFollower(higherTerm)
	}
	// fmt.Printf("Candidate[%v] has %v votes\n", rf.me, votes)
	/* FIX: in the presence of partial network partition, and we are the sole peer, we must assume leadership in order to make progress
	   however, our election handler does not explicitly handle this case
	*/
	// if (votes > len(rf.peers)/2 || finished == len(rf.peers)) && rf.state == CANDIDATE && higherTerm == rf.currentTerm {
	if votes > len(rf.peers)/2 && rf.state == CANDIDATE && higherTerm == rf.currentTerm {
		// color.New(color.FgCyan).Printf("Candidate[%v]: Won Election!\n", rf.me)
		rf.convertToLeader()
	}
	rf.mu.Unlock()

	_, isLeader := rf.GetState()
	if isLeader {
		go rf.establishAuthority()
	}
}

func (rf *Raft) establishAuthority() {
	// color.New(color.FgCyan).Printf("Leader[%v]: establishAuthority\n", rf.me)
	for rf.killed() == false {
		_, isLeader := rf.GetState()
		if isLeader == false {
			return
		}
		termChanged := false
		higherTerm := rf.currentTerm
		cond := sync.NewCond(&rf.mu)
		finished := 1

		for peer := 0; peer < len(rf.peers); peer++ {
			if rf.me == peer {
				continue
			}
			_, isLeader := rf.GetState()

			rf.mu.Lock()
			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[peer] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			rf.mu.Unlock()

			if isLeader {
				go func(peer int) {
					/* Send Heartbeats! */
					reply := &AppendEntriesReply{}
					nodeReceivedHeartbeat := rf.sendAppendEntries(peer, args, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if nodeReceivedHeartbeat && reply.Success == false && reply.Term > rf.currentTerm {
						termChanged = true
						higherTerm = reply.Term
					}

					finished++

					cond.Broadcast()
				}(peer)
			}
		}

		rf.mu.Lock()
		for finished != len(rf.peers) && termChanged == false {
			cond.Wait()
		}
		///////////////////////////////////////////////////////////////////////////
		// If we found out that we're no longer the leader via heartbeat messages
		// then step down as a leader, else continue to send out heartbeats
		///////////////////////////////////////////////////////////////////////////
		if termChanged && higherTerm > rf.currentTerm {
			rf.convertToFollower(higherTerm)
		}

		rf.mu.Unlock()

		_, isLeader = rf.GetState()
		if isLeader == false {
			return
		} else {
			// we sleep because the tester limits us to 10 heartbeats/sec
			// but here's a scenario
			// we go to sleep as the leader
			// and wake back up as a follower/candidate
			time.Sleep(time.Duration(120 * time.Millisecond))
		}
	}
}

/*
  - retry indefinetly
  - decrement index on retry
  - receive an entry
  - determine when to commit an entry (entries)
*/
