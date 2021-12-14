package raft

import "github.com/fatih/color"

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
		// color.New(color.FgRed).Printf("RVReceiver (%v)[%d][%v]: received a larger term: %v, stepping down from %v to Follower\n", rf.state, rf.me, rf.currentTerm, args.Term, rf.state)
		rf.convertToFollower(args.Term)
	}

	// the request is outdated, disregard
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// grant vote if the request's log is up to date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		// color.New(color.FgCyan).Printf("RVReceiver (%v)[%v][%d]: granting a vote to %d\n", rf.state, rf.me, rf.currentTerm, args.CandidateId)
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

// The paper defines "up-to-date" in the following way:
// - if the last entries have the same index and different terms, then the log with the later term is up-to-date
// - if the logs entries end with the same term, then the log with the higher index is more up-to-date
func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	lastIndex, lastTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	// color.New(color.FgCyan).Printf("RVReceiver (%v)[%v][%d]: candidateTerm= %v, lastTerm= %v, candidateIndex= %v, lastIndex= %v\n", rf.state, rf.me, rf.currentTerm, candidateTerm, lastTerm, candidateIndex, lastIndex)
	if candidateTerm > lastTerm || (candidateTerm == lastTerm && candidateIndex >= lastIndex) {
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: received an append entry request from [%v][%v]\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
	// color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: Leader[%v][%v] sent args.PrevLogIndex=%v, args.PrevLogTerm=%v, args.LeaderCommit=%v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	// color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: Leader[%v][%v] sent entries: %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, args.Entries)
	// color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: commitIndex=%v, lastApplied=%v\n", rf.state, rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)

	/* Rule 1 if the append entries message is stale, disregard */
	if args.Term < rf.currentTerm {
		// color.New(color.FgRed).Printf("AEReceiver (%v)[%v][%v]: received a STALE append entry request from [%v], term %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	/* Rule for all servers: step down and reset timer */
	if args.Term > rf.currentTerm {
		// color.New(color.FgRed).Printf("AEReceiver (%v)[%d][%v]: received a larger term: %v (received term), stepping down from %v to Follower\n", rf.state, rf.me, rf.currentTerm, args.Term, rf.state)
		rf.convertToFollower(args.Term)
	}

	/* At this point, it's implied that we share the same term as the leader */

	// Section 5.2 a candidate or follower may receive an AE RPC from
	// another server claiming to be leader. If the leader's term is at
	// least as large as the candidate's then the candidate recognizes the
	// leader as legitmate and steps down
	if rf.state == CANDIDATE {
		// color.New(color.FgRed).Printf("AEReceiver (%v)[%d][%v]: received a larger term: %v (received term), stepping down from %v to Follower\n", rf.state, rf.me, rf.currentTerm, args.Term, rf.state)
		rf.convertToFollower(args.Term)
	}

	/* Prepare our reply */
	rf.setElectionTimeout()
	reply.Success = false
	reply.Term = rf.currentTerm

	entry, containsLogIndex := rf.getPrevLogIndex(args)
	/* Log Matching Safety Property */
	satisfiesLogMatchingProperty := args.PrevLogIndex == 0 || containsLogIndex && args.PrevLogTerm == entry.Term

	// Rule 3
	// We've now established follower's log DOES contain the PreviousLogIndex
	// but perhaps not a matching entry. Our goal now is to satisfy the Log
	// Matching Property. If an existing entry in this peers' log conflicts
	// with an entry in entries[] (same index, but different term), delete
	// the existing entry and all that follow it
	if satisfiesLogMatchingProperty {

		reply.Success = true

		// color.New(color.FgRed).Printf("AEReceiver (%v)[%v][%d]: log contains prev log index and term\n", rf.state, rf.me, rf.currentTerm)
		argsEntriesIndex := 0
		followerIndex := args.PrevLogIndex + 1
		for ; argsEntriesIndex < len(args.Entries) && followerIndex < len(rf.log); argsEntriesIndex, followerIndex = argsEntriesIndex+1, followerIndex+1 {
			if rf.entriesConflict(args.Entries, argsEntriesIndex, followerIndex) {
				// color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: received AE from leader: %v, term: %v. Truncating log(len=%v) before: %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, len(rf.log), rf.log)
				rf.truncateLog(followerIndex)
				// color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: received AE from leader: %v, term: %v. Truncating log(len=%v) after: %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, len(rf.log), rf.log)
				break
			}
		}

		// Rule 4
		// - We've resolved all conflicting entries that may exist between the leader
		//   and this peer. Now, append any remaining entries to this peer.
		//   i.e. Tack on the new entries that the leader wants us to replicate.
		// - We choose the method of making a new slice and copying the contents over
		//   because we want to free the current slice/underlying array that is
		//   being referenced by rf.log, in order for this outdated log to be
		//   garbage collected. Granted, this method has a linear complexity,
		//   we can make optimizations in the future if necessary
		if argsEntriesIndex <= len(args.Entries)-1 {
			logWithReplicatedEntries := make([]Entry, len(rf.log)+len(args.Entries)-argsEntriesIndex)
			copy(logWithReplicatedEntries, rf.log)
			rf.log = logWithReplicatedEntries
			for ; argsEntriesIndex < len(args.Entries); argsEntriesIndex, followerIndex = argsEntriesIndex+1, followerIndex+1 {
				rf.log[followerIndex] = args.Entries[argsEntriesIndex]
			}
			// color.New(color.FgGreen).Printf("AEReceiver (%v)[%v][%v]: new log: %v\n", rf.state, rf.me, rf.currentTerm, rf.log)
		}

		rf.persist()

		// Rule 5
		// Update the commit index of the follower.
		// Before this code, we've used the Leader's entries arr to replicate
		// new entries on this peer. The last entry that we've replicated is
		// the last log entry on this peer's log. As a result, update this
		// peer's commit index.
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = minOf(args.LeaderCommit, len(rf.log)-1)
		}
	} else if !satisfiesLogMatchingProperty && containsLogIndex {
		// Accelerated log optimization protocol:
		// If the previous log index exists within our log, but the term doesn't
		// match, return the conflicting term, search the follower's log for
		// the *first* index whose entry has term equal to the conflicting term
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for firstOcc := args.PrevLogIndex; firstOcc >= 0 && rf.log[firstOcc].Term == reply.ConflictTerm; firstOcc-- {
			reply.ConflictIndex = firstOcc
		}
		// color.New(color.FgRed).Printf("AEReceiver (%v)[%v][%d]: Leader[%v][%v]: PrevLogIndex Exists, Term doesn't match. PrevLogIndex=%v, PrevLogTerm=%v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		// color.New(color.FgRed).Printf("AEReceiver (%v)[%v][%d]: Leader[%v][%v]: first index of an entry that has a term equal to %v, entry=%v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.ConflictIndex, rf.log[reply.ConflictIndex])
		// color.New(color.FgRed).Printf("AEReceiver (%v)[%v][%d]: log: %v\n", rf.state, rf.me, rf.currentTerm, rf.log)

	} else if !satisfiesLogMatchingProperty && !containsLogIndex {
		// Accelerated log optimization protocol:
		// - If the previous log index does not exist within our log, the leader
		//   will update prevlogindex with conflictIndex and resend the updated
		//   list of entries
		// - Credit to Jon Gjengset @jonhoo
		//   https://thesquareplanet.com/blog/students-guide-to-raft/
		reply.ConflictIndex = len(rf.log)
		/* reply.ConflictTerm = None */
	}
	return
}

func (rf *Raft) getPrevLogIndex(args *AppendEntriesArgs) (Entry, bool) {
	if args.PrevLogIndex >= len(rf.log) {
		// color.New(color.FgRed).Printf("AEReceiver (%v)[%v][%d]: PrevLogIndex DNE, Leader[%v][%v]: index: %v, term: %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		// color.New(color.FgRed).Printf("AEReceiver (%v)[%v][%d]: log: %v\n", rf.state, rf.me, rf.currentTerm, rf.log)
		return Entry{}, false
	}
	return rf.log[args.PrevLogIndex], true
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
