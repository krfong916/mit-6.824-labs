package raft

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

func (rf *Raft) convertToFollower(term int) {
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.setElectionTimeout()
	rf.persist()
}

func (rf *Raft) convertToCandidate() {
	rf.state = CANDIDATE
	rf.votedFor = rf.me // Vote for ourself
	rf.currentTerm += 1 // Initialize a term variable
	rf.setElectionTimeout()
	rf.persist()
}

func (rf *Raft) convertToLeader() {
	rf.state = LEADER
	rf.setElectionTimeout()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
	}
}

func (rf *Raft) setElectionTimeout() {
	interm := rand.NewSource(time.Now().UnixNano())
	random := rand.New(interm)
	timeout := time.Duration(350+random.Int63()%150) * time.Millisecond
	rf.electionTimeout = time.Now().Add(timeout)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       FOLLOWER,
		currentTerm: 0,
		log:         []Entry{{0, nil}},
		votedFor:    -1,
		applyCh:     applyCh,
	}
	rf.setElectionTimeout()
	rf.readPersist(persister.ReadRaftState())
	go rf.kickOffElectionTimeout()
	go rf.attemptCommitEntry()
	go rf.applyEntry()
	return rf
}

type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []Entry
}

func (rf *Raft) persist() {
	raftState := &PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}
	var serializedState bytes.Buffer                    // create a new buffer object
	enc := labgob.NewEncoder(&serializedState)          // create a new byte array encoding?
	enc.Encode(raftState)                               // encode struct into a byte array
	rf.persister.SaveRaftState(serializedState.Bytes()) // save the encoded raft state to the persistence object
	return
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reader := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(reader)
	persistentState := &PersistentState{}
	dec.Decode(&persistentState)
	rf.log = persistentState.Log
	rf.currentTerm = persistentState.CurrentTerm
	rf.votedFor = persistentState.VotedFor
	return
}

func (rf *Raft) kickOffElectionTimeout() {
	for rf.killed() == false {
		rf.checkTimeElapsed()
		sleep()
	}
}

func sleep() {
	time.Sleep(time.Duration(20) * time.Millisecond)
}

func (rf *Raft) checkTimeElapsed() {
	rf.mu.Lock()
	elapsed := time.Now().After(rf.electionTimeout)
	// renew our lease as Leader
	isLeader := false
	if elapsed && rf.state == LEADER {
		rf.setElectionTimeout()
		isLeader = true
	}
	rf.mu.Unlock()

	if elapsed && !isLeader {
		rf.performLeaderElection()
	}
}

func (rf *Raft) performLeaderElection() {
	rf.mu.Lock()
	// color.New(color.FgMagenta).Printf("[%v][%v][%v]: converting from %v -> Candidate\n", rf.state, rf.me, rf.currentTerm, rf.state)
	rf.convertToCandidate()
	// color.New(color.FgMagenta).Printf("Candidate[%v][%v]: starting an election: %v\n", rf.me, rf.currentTerm, rf.log)
	args := &RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	// indicates if we should step down, new leader may be elected b/t the time we send a RequestVote and the time we recieve a reply
	higherTerm := rf.currentTerm
	termChanged := false
	rf.mu.Unlock()

	// vote for ourself
	votes := 1
	finished := 1
	cond := sync.NewCond(&rf.mu)

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			// rf.mu.Lock()
			// color.New(color.FgCyan).Printf("Candidate[%v][%v]: sent a request vote to %v\n", rf.me, rf.currentTerm, peer)
			// rf.mu.Unlock()
			reply := &RequestVoteReply{}
			nodeReceivedMessage := rf.sendRequestVote(peer, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// We were granted a vote and still have candidacy
			if nodeReceivedMessage && reply.VoteGranted && reply.Term == rf.currentTerm && rf.state == CANDIDATE {
				votes++
			}

			// If our term is outdated
			if reply.Term > rf.currentTerm {
				termChanged = true
				higherTerm = reply.Term
			}

			finished++
			cond.Broadcast()
		}(peer)
	}
	rf.mu.Lock()

	// If we haven't received enough votes yet
	for votes <= len(rf.peers)/2 && finished != len(rf.peers) && termChanged == false {
		cond.Wait()
	}

	// Our term is out of date
	if termChanged && higherTerm > rf.currentTerm {
		// color.New(color.FgRed).Printf("Candidate[%v][%v]: stepping down to follower, new term%v\n", rf.me, rf.currentTerm, higherTerm)
		rf.convertToFollower(higherTerm)
	}

	// We received a majority vote
	if votes > len(rf.peers)/2 && rf.state == CANDIDATE && higherTerm == rf.currentTerm {
		// color.New(color.FgMagenta).Printf("Candidate[%v][%v]: Won Election! Log: %v\n", rf.me, rf.currentTerm, rf.log)
		rf.convertToLeader()
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

/*
  the service using Raft (e.g. a k/v server) wants to start
  agreement on the next command to be appended to Raft's log. if this
  server isn't the leader, returns false. otherwise start the
  agreement and return immediately. there is no guarantee that this
  command will ever be committed to the Raft log, since the leader
  may fail or lose an election. even if the Raft instance has been killed,
  this function should return gracefully.
  the first return value is the index that the command will appear at
  if it's ever committed. the second return value is the current
  term. the third return value is true if this server believes it is
  the leader.
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}
	rf.mu.Lock()

	// color.New(color.FgWhite).Printf("CReq Leader[%v][%v]: received client request: %v\n", rf.me, rf.currentTerm, command)

	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	// color.New(color.FgWhite).Printf("CReq Leader[%v][%v]: log: %v\n", rf.me, rf.currentTerm, rf.log)
	index := len(rf.log) - 1
	term = rf.currentTerm

	// needed to determine if an entry has been replicated on a majority of peers
	rf.matchIndex[rf.me] = len(rf.log) - 1

	rf.persist()

	rf.mu.Unlock()

	return index, term, isLeader
}

func (rf *Raft) getEntriesLength(peer int) int {
	if rf.nextIndex[peer] > len(rf.log) {
		return 0
	} else {
		return (len(rf.log) - rf.nextIndex[peer]) + 1
	}
}

/**
 * attemptCommitEntry is a long-running goroutine for replicating entries on a peer.
 * If the peer's log is up-to-date, then we prevent sending new AppendEntries to the peer.
 * One way to interpret replicating an entry on a peer: it's the peer's way
 * of pledging allegiance to the leader.
 */
func (rf *Raft) attemptCommitEntry() {
	for !rf.killed() {
		_, isLeader := rf.GetState()
		if isLeader {
			for peer := 0; peer < len(rf.peers); peer++ {
				if peer == rf.me {
					continue
				}

				shouldSend := true
				args := &AppendEntriesArgs{}
				rf.mu.Lock()
				if rf.state != LEADER {
					shouldSend = false
				} else {
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = rf.nextIndex[peer] - 1
					args.PrevLogTerm = rf.log[rf.nextIndex[peer]-1].Term
					args.Entries = make([]Entry, rf.getEntriesLength(peer))
					// color.New(color.FgYellow).Printf("AESender (%v)[%v][%v]: AppendEntriesArgs: len(rf.log)=%v, rf.nextIndex[%v]=%v, len(entries)=%v\n", rf.state, rf.me, rf.currentTerm, len(rf.log), peer, rf.nextIndex[peer], len(args.Entries))
					args.Entries = append(rf.log[rf.nextIndex[peer]:])
				}
				rf.mu.Unlock()

				if shouldSend {
					go func(peer int) {
						rf.mu.Lock()
						// color.New(color.FgCyan).Printf("AESender Leader[%v][%v]: sending an AE to %v, term: %v, index: %v\n", rf.me, rf.currentTerm, peer, args.PrevLogTerm, args.PrevLogIndex)
						rf.mu.Unlock()
						reply := &AppendEntriesReply{}
						rf.sendAppendEntries(peer, args, reply)

						rf.mu.Lock()

						///////////////////////////////////////////////////////////////////////////
						// If the peer's term is larger than our term when we sent the request
						//   we must step down to follower, our term is out of date
						///////////////////////////////////////////////////////////////////////////
						if reply.Success == false && reply.Term > args.Term {
							// color.New(color.FgRed).Printf("AESender Leader[%v][%v]: peer: %v has a term %v that's larger than ours: %v, stepping down!\n", rf.me, args.Term, peer, reply.Term, args.Term)
							rf.convertToFollower(reply.Term)
						}

						///////////////////////////////////////////////////////////////////////////
						// Retry if we sent an incorrect prevLogIndex, nextIndex is optimistic,
						//   matchIndex is conservative.
						//   This code captures the Retry optimization protocol defined in section
						//   5.3. If we have an entry whose term conflicts, or the PrevLogIndex
						//   doesn't exist within the peer's log, then we make a send only the
						//   necessary log entries, rather than decrement nextIndex one-at-a-time.
						///////////////////////////////////////////////////////////////////////////
						if reply.Success == false && args.Term == reply.Term && rf.state == LEADER {
							// do we have a conflicting term?
							if reply.ConflictTerm > 0 {
								// find the conflicting index
								found := false
								for i := len(rf.log) - 1; i >= 0; i-- {
									if rf.log[i].Term == reply.ConflictTerm {
										// fmt.Printf("(%v)[%v]: here?: %v\n", rf.state, rf.me, i+1)
										// fmt.Printf("(%v)[%v]: nextIndex: %v\n", rf.state, rf.me, rf.nextIndex[peer])
										rf.nextIndex[peer] = i + 1
										found = true
										break
									}
								}
								if !found {
									// fmt.Printf("(%v)[%v]: hm: %v\n", rf.state, rf.me, reply.ConflictIndex)
									// fmt.Printf("(%v)[%v]: nextIndex: %v\n", rf.state, rf.me, rf.nextIndex[peer])
									rf.nextIndex[peer] = reply.ConflictIndex
								}
							} else if reply.ConflictIndex > 0 && reply.ConflictTerm == 0 {
								// fmt.Printf("(%v)[%v]: conflict? %v\n", rf.state, rf.me, reply.ConflictIndex)
								// fmt.Printf("(%v)[%v]: nextIndex: %v\n", rf.state, rf.me, rf.nextIndex[peer])
								rf.nextIndex[peer] = reply.ConflictIndex
							}
						}
						///////////////////////////////////////////////////////////////////////////
						// We've successfully replicated the entry:
						// 1. Update the index of the highest log entry
						//   - known to replicated in the server (matchIndex): Conservative
						//   - to send to that server (nextIndex): Optimistic
						// 2. Update the commit index if we have replicated on a majority of servers
						// Additionally: we enforce a rule for committing entries
						//   if there exists entries in the leader's log that have not been
						//   committed or fully replicated, the leader will replicate and commit
						//   those entries under the following safety property. If the leader
						//   is able to replicate an entry on a majority of servers within its
						//   current term, then the leader can safely commit all previous
						//   uncommitted entries in its log
						//   This situation can arise when a previous leader replicates entries on
						//   some (maybe on a majority of servers), but gets disconnected from the
						//   network or fails BEFORE it's able to update its commit index or apply
						//   those entries to the state machine
						///////////////////////////////////////////////////////////////////////////
						if reply.Success && reply.Term == args.Term && rf.state == LEADER {
							rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
							// color.New(color.FgYellow).Printf("AESender Leader[%v][%v]: successfully replicated %v on Peer[%v]\n", rf.me, args.Term, args.Entries, peer)
							// color.New(color.FgYellow).Printf("AESender Leader[%v][%v]: Peer[%v] matchIndex=%v\n", rf.me, args.Term, peer, rf.matchIndex[peer])
							// color.New(color.FgYellow).Printf("AESender Leader[%v][%v]: Peer[%v] nextIndex=%v\n", rf.me, args.Term, peer, rf.nextIndex[peer])
							if len(rf.log)-1 > rf.commitIndex { // if we have a new entry in our log, our commitIndex needs to be updated
								rf.calculateCommitIndex()
							}
						}
						rf.mu.Unlock()
						return
					}(peer)
				}
			}
			time.Sleep(60 * time.Millisecond)
		}
	}
}

func (rf *Raft) calculateCommitIndex() {
	ok, index := rf.getReplicatedMajorityIndex()
	// color.New(color.FgYellow).Printf("ok: %v\n", ok)
	// color.New(color.FgYellow).Printf("index: %v\n", index)
	if ok {
		// color.New(color.FgYellow).Printf("Leader[%v][%v]: new commit index: %v\n", rf.me, rf.currentTerm, len(rf.log)-1)
		rf.commitIndex = index
	}
}

func (rf *Raft) getReplicatedMajorityIndex() (bool, int) {
	arr := make([]int, 0, len(rf.matchIndex))
	for _, idx := range rf.matchIndex {
		arr = append(arr, idx)
	}
	sort.Ints(arr)
	newCommitIndex := -1
	if len(arr)%2 == 0 {
		newCommitIndex = arr[((len(arr)-1)/2)+1]
	} else {
		newCommitIndex = arr[(len(arr)-1)/2]
	}
	if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex].Term == rf.currentTerm {
		return true, newCommitIndex
	} else {
		return false, newCommitIndex
	}
}

func (rf *Raft) currAndLogTermMatch(commitIndex int) bool {
	return rf.log[commitIndex].Term == rf.currentTerm
}

func (rf *Raft) applyEntry() {
	for !rf.killed() {
		entriesToCommit := false
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			entriesToCommit = true
		}
		rf.mu.Unlock()

		if entriesToCommit {
			rf.mu.Lock()
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				entry := rf.log[rf.lastApplied]
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  entry.Term,
				}
				// color.New(color.FgBlue).Printf("APPL (%v)[%v][%v]: updated lastapplied: %v, commitIndex: %v\n", rf.state, rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
				rf.applyCh <- msg

			}
			// color.New(color.FgBlue).Printf("APPL (%v)[%v][%v]: Final state commitIndex: %v, lastApplied: %v\n", rf.state, rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
			// color.New(color.FgWhite).Printf("APPL (%v)[%v][%v]: log: %v\n", rf.state, rf.me, rf.currentTerm, rf.log)
			rf.mu.Unlock()
		}
		time.Sleep(25 * time.Millisecond)
	}
}
