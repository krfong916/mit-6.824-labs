package raft

import (
	"fmt"

	"github.com/fatih/color"
)

type Entry struct {
	Term    int         // The leader's term that requested this Entry to be replicated
	Command interface{} // The client command to apply to the peer's state machine
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

type AppendEntriesArgs struct {
	Term         int     // Leader's Term
	LeaderId     int     // Needed so follower can redirect clients
	Entries      []Entry // Log entries to store, empty if heartbeat
	PrevLogIndex int     // Index of the log Entry immediately preceding new ones
	PrevLogTerm  int     // Term of the prevLogIndex Entry
	LeaderCommit int     // Leader's CommitIndex
}

type AppendEntriesReply struct {
	Success       bool // True if follower contained entry matching log index and term
	Term          int  // The current term for the leader to update itself
	ConflictIndex int  // The conflicting entry's index in the peer's log to send back to the leader. If no conflicting index is present, send back the len(log)
	ConflictTerm  int  // The term of the conflicting entry
}

// func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	reply.Term = rf.currentTerm
// 	reply.VoteGranted = false

// 	// the request is outdated, disregard
// 	if args.Term < rf.currentTerm {
// 		return
// 	}

// 	// rules for all servers: if we see a new term, step down and reset timer
// 	if args.Term > rf.currentTerm {
// 		color.New(color.FgRed).Printf("RVReceiver (%v)[%d][%v]: received a larger term: %v, stepping down from %v to Follower\n", rf.state, rf.me, rf.currentTerm, args.Term, rf.state)
// 		rf.convertToFollower(args.Term)
// 		rf.persist()
// 	}

// 	if rf.votedFor != -1 || rf.votedFor != args.CandidateId {
// 		return
// 	}

// 	lastIndex, lastTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
// 	if lastTerm > args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
// 		return
// 	}

// 	color.New(color.FgCyan).Printf("RVReceiver (%v)[%v][%d]: granting a vote to %d\n", rf.state, rf.me, rf.currentTerm, args.CandidateId)
// 	rf.setElectionTimeout()
// 	rf.votedFor = args.CandidateId
// 	reply.VoteGranted = true

// 	rf.persist()

// 	return
// }
/**
 * RequestVote is a candidate's attempt to request votes from peers
 * For a peer to give a candidate their vote
 * A candidate's term must be >= this peer's term and the peer cannot have voted on someone else already
 *
 * @param  args
 * @param  reply
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rules for all servers: if we see a new term, step down and reset timer
	if args.Term > rf.currentTerm {
		color.New(color.FgRed).Printf("RVReceiver (%v)[%d][%v]: received a larger term: %v, stepping down from %v to Follower\n", rf.state, rf.me, rf.currentTerm, args.Term, rf.state)
		rf.convertToFollower(args.Term)
		rf.persist()
	}

	// the request is outdated, disregard
	if args.Term < rf.currentTerm {
		return
	}

	// grant vote if the request's log is up to date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		color.New(color.FgCyan).Printf("RVReceiver (%v)[%v][%d]: granting a vote to %d\n", rf.state, rf.me, rf.currentTerm, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.persist()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}

	return
}
func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	lastIndex, lastTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	color.New(color.FgCyan).Printf("RVReceiver (%v)[%v][%d]: candidateTerm= %v, lastTerm= %v, candidateIndex= %v, lastIndex= %v\n", rf.state, rf.me, rf.currentTerm, candidateTerm, lastTerm, candidateIndex, lastIndex)
	if candidateTerm > lastTerm || (candidateTerm == lastTerm && candidateIndex >= lastIndex) || (rf.log[lastIndex].Term == 0 && rf.log[lastIndex].Command == nil) {
		rf.setElectionTimeout()
		return true
	} else {
		return false
	}
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	///////////////////////////////////////////////////////////////////////////
	// Rule 1
	// if the append entries message is stale, disregard
	///////////////////////////////////////////////////////////////////////////
	rf.mu.Lock()
	color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: received an append entry request from [%v], term %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
	color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: commitIndex: %v, lastApplied: %v\n", rf.state, rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	color.New(color.FgGreen).Printf("AEReceiver PrevLogIndex: %v, PrevLogTerm: %v, LeaderCommit:%v\n", args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	isStale := false
	if args.Term < rf.currentTerm {
		color.New(color.FgRed).Printf("AEReceiver (%v)[%v][%v]: received a STALE append entry request from [%v], term %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		isStale = true
	}
	rf.mu.Unlock()
	if isStale {
		return
	}

	///////////////////////////////////////////////////////////////////////////
	// Rule for all servers
	// step down and reset timer
	///////////////////////////////////////////////////////////////////////////
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		color.New(color.FgRed).Printf("AEReceiver (%v)[%d][%v]: received a larger term: %v (received term), stepping down from %v to Follower\n", rf.state, rf.me, rf.currentTerm, args.Term, rf.state)
		rf.convertToFollower(args.Term)
	}
	rf.mu.Unlock()

	///////////////////////////////////////////////////////////////////////////
	// Section 5.2 Additionally, a candidate or follower may receive an AE RPC
	// from another server claiming to be leader. If the leader's term is at
	// least as large as the candidate's then the candidate recognizes the
	// leader as legitmate and steps down
	///////////////////////////////////////////////////////////////////////////
	rf.mu.Lock()
	if rf.state == CANDIDATE && args.Term >= rf.currentTerm {
		color.New(color.FgRed).Printf("AEReceiver (%v)[%d][%v]: received a larger term: %v (received term), stepping down from %v to Follower\n", rf.state, rf.me, rf.currentTerm, args.Term, rf.state)
		rf.convertToFollower(args.Term)
	}
	rf.mu.Unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	///////////////////////////////////////////////////////////////////////////
	// Rule 2
	// Credit to Jon Gjengset @jonhoo
	//   https://thesquareplanet.com/blog/students-guide-to-raft/
	//   Accelerated log backtracking optimization:
	//   If the previous log index does not exist within our log, the leader
	//     will update prevlogindex with conflictIndex and resend the updated
	//     list of entries
	///////////////////////////////////////////////////////////////////////////
	if args.PrevLogIndex >= len(rf.log) {
		color.New(color.FgRed).Printf("AEReceiver (%v)[%v][%d]: PrevLogIndex DNE, Leader[%v][%v]: index: %v, term: %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = len(rf.log)
		/* reply.ConflictTerm = None */
		return
	}

	///////////////////////////////////////////////////////////////////////////
	// If the previous log index does exist within our log, but the terms
	//   don't match, mark the conflicting term, and search the log for the
	//   first index whose entry has term equal to conflictTerm
	///////////////////////////////////////////////////////////////////////////
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {

		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for firstOcc := args.PrevLogIndex; rf.log[firstOcc].Term == reply.ConflictTerm && firstOcc >= 0; firstOcc-- {
			reply.ConflictIndex = firstOcc
		}
		color.New(color.FgRed).Printf("AEReceiver (%v)[%v][%d]: Terms Conflict, ConflictIndex %v Leader[%v][%v]: index: %v, term: %v\n", rf.state, rf.me, rf.currentTerm, reply.ConflictIndex, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		return
	}

	///////////////////////////////////////////////////////////////////////////
	// Check if this is a heartbeat mechanism
	//   If so, the set the flag. The leader uses the result of AppendEntries
	//   to help decide whether or not should commit an entry.
	//   if heartbeat(args.Entries) && args.Term >= rf.currentTerm {
	// Respond to the heartbeat mechanism
	//   We are safe to reply success because we've performed two checks
	//   above^: if this peer's log contains the prev-log-index, and if the
	//   prev-log-term matches this peer's term.
	///////////////////////////////////////////////////////////////////////////
	if heartbeat(args.Entries) {
		color.New(color.FgYellow).Printf("(%v)[%v][%d]: recognizes heartbeat from Leader[%v][%v]\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.setElectionTimeout()
		reply.Success = true
		reply.Term = rf.currentTerm
	}

	///////////////////////////////////////////////////////////////////////////
	// Rule 3
	// We've now established that this peer's log DOES contain a prev log
	//   index. Our goal now is to satisfy the Log Matching Property.
	// If an existing entry in this peers' log conflicts with an entry in
	//   entries[] (same index, but different term), delete the existing
	//   entry and all that follow it
	///////////////////////////////////////////////////////////////////////////
	peerIdx := args.PrevLogIndex + 1
	leaderIdx := 0

	for ; leaderIdx < len(args.Entries) && peerIdx < len(rf.log); leaderIdx, peerIdx = leaderIdx+1, peerIdx+1 {
		if rf.entriesConflict(args.Entries, leaderIdx, peerIdx) {
			color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: received AE from leader: %v, term: %v. Truncating log(len=%v) before: %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, len(rf.log), rf.log)
			rf.truncateLog(peerIdx)
			rf.persist()
			color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: received AE from leader: %v, term: %v. Truncating log(len=%v) after: %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, len(rf.log), rf.log)
			break
		}
	}

	///////////////////////////////////////////////////////////////////////////
	// Rule 4
	// We've resolved all conflicting entries that may exist between the leader
	//   and this peer. Now, append any remaining entries to this peer.
	//   i.e. Tack on the new entries that the leader wants us to replicate.
	// We choose the method of making a new slice and copying the contents over
	//   because we want to free the current slice/underlying array that is
	//   being referenced by rf.log, in order for this outdated log to be
	//   garbage collected. Granted, this method has a linear complexity,
	//   we can make optimizations in the future if necessary
	///////////////////////////////////////////////////////////////////////////
	if leaderIdx <= len(args.Entries)-1 {
		rf.setElectionTimeout()
		logWithReplicatedEntries := make([]Entry, len(rf.log)+len(args.Entries)-leaderIdx)
		copy(logWithReplicatedEntries, rf.log)
		rf.log = logWithReplicatedEntries
		for ; leaderIdx < len(args.Entries); leaderIdx, peerIdx = leaderIdx+1, peerIdx+1 {
			rf.log[peerIdx] = args.Entries[leaderIdx]
		}
		rf.persist()
	}
	reply.Success = true
	reply.Term = rf.currentTerm

	///////////////////////////////////////////////////////////////////////////
	// Rule 5
	// Update the commit index of the follower.
	// Before this code, we've used the Leader's entries arr to replicate
	//   new entries on this peer. The last entry that we've replicated is
	//   the last log entry on this peer's log. As a result, update this
	//   peer's commit index.
	///////////////////////////////////////////////////////////////////////////
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minOf(args.LeaderCommit, len(rf.log)-1)
	}
	color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: received AE from leader: %v, term: %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
	fmt.Printf("(%v)[%v][%v]: Commit Index %v\n", rf.state, rf.me, rf.currentTerm, rf.commitIndex)
	fmt.Printf("(%v)[%v][%v]: Log %v\n", rf.state, rf.me, rf.currentTerm, rf.log)
	fmt.Println("")
	return
}

func heartbeat(log []Entry) bool {
	return len(log) == 0
}

/* Same index but different terms */
func (rf *Raft) entriesConflict(entries []Entry, leaderIdx int, peerIdx int) bool {
	return entries[leaderIdx].Term != rf.log[peerIdx].Term
}

/* delete the existing entry and all that follow it */
func (rf *Raft) truncateLog(idx int) {
	// create an entirely new log so the old one can be garbage collected
	newLog := make([]Entry, idx)
	copy(newLog, rf.log)
	rf.log = newLog
}
