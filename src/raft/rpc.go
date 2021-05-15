package raft

import "github.com/fatih/color"

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
	Term     int     // Leader's Term
	LeaderId int     // Needed so follower can redirect clients
	Entries  []Entry // Log entries to store, empty if heartbeat
}

type AppendEntriesReply struct {
	Success bool // True if follower contained entry matching log index and term
	Term    int  // The current term for the leader to update itself
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
	color.New(color.FgGreen).Printf("Peer[%d]: received a request vote from %d\n", rf.me, args.CandidateId)

	////////////////////////////////////////////////////////////////////////
	// rule for all servers
	////////////////////////////////////////////////////////////////////////
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		color.New(color.FgYellow).Printf("Peer[%d]: %v (our term), %v (received term), stepping down from %v to Follower\n", rf.me, rf.currentTerm, args.Term, rf.state)
		// step down and reset timer
		rf.convertToFollower(args.Term)
	}
	rf.mu.Unlock()
	////////////////////////////////////////////////////////
	// if we have a stale request vote
	// update the candidate's terms, disregard the request
	////////////////////////////////////////////////////////
	staleTerm := false
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		color.New(color.FgRed).Printf("Peer[%d]: refused to grant vote %d\n", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		staleTerm = true
	}
	rf.mu.Unlock()
	if staleTerm {
		return
	}

	rf.mu.Lock()
	// grant vote under the following conditions:
	// we haven't voted for anyone yet, and
	// the candidate's id and log is AT LEAST up-to-date as this peer's log
	if rf.votedFor == -1 && args.Term >= rf.currentTerm {
		color.New(color.FgGreen).Printf("Peer[%d]: granting a vote to %d\n", rf.me, args.CandidateId)
		// when we grant a vote to a peer we MUST reset our election timeout
		rf.setElectionTimeout()
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = args.Term
	}
	rf.mu.Unlock()
	return
}

/**
 * As of Lab 2A, we only care about the heartbeat mechanism.
 * No longer entries or commit indices.
 *
 * @param  args
 * @param	 reply
 */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	color.New(color.FgRed).Printf("Peer[%v]: received heartbeat from Leader[%v]\n", rf.me, args.LeaderId)
	////////////////////////////////////////////////////////
	// Rule 1
	// if the append entries message is stale, disregard
	////////////////////////////////////////////////////////
	rf.mu.Lock()
	isStale := false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		isStale = true
	}
	rf.mu.Unlock()
	if isStale {
		return
	}

	///////////////////////////////////////////////////////////////////////////
	// rule for all servers
	///////////////////////////////////////////////////////////////////////////
	/* CHECK!!! may be a leader and receive an append entries, forcing a step down */
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		// step down and reset timer
		rf.convertToFollower(args.Term)
	}
	rf.mu.Unlock()

	////////////////////////////////////////////////////////
	// follower
	// check if this is a heartbeat mechanism
	// if so, then recognize the new leader
	////////////////////////////////////////////////////////
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if heartbeat(args.Entries) {
		if args.Term >= rf.currentTerm {
			color.New(color.FgYellow).Printf("Peer[%d]: (%v) recognizes heartbeat from Leader[%v]\n", rf.me, rf.state, args.LeaderId)
			rf.convertToFollower(args.Term)
			reply.Success = true
			reply.Term = args.Term
			return
		}
	}

	////////////////////////////////////////////////////////
	// follower
	// We ain't done!! After determining if a heartbeat,
	// we still have some work to do...
	////////////////////////////////////////////////////////

}

func heartbeat(log []Entry) bool {
	return len(log) == 0
}
