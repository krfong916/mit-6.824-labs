package raft

type Entry struct {
  Term    int         // The leader's term that requested this Entry to be replicated
  Command interface{} // The client command to apply to the peer's state machine
  // Index   int         // The Entry's index in the leader's log
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
  Success bool // True if follower contained entry matching log index and term
  Term    int  // The current term for the leader to update itself
}

/**
 * RequestVote is a candidate's attempt to request votes from peers
 * For a peer to give a candidate their vote
 * A candidate's term must be >= this peer's term and the peer cannot have voted on someone else already
 *
 * @param  args
 * @param  reply
 */
/* If we reject the request vote via "Rule For All Servers", is there an update to apply to our peer using lastlogindex/term? */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  ///////////////////////////////////////////////////////////////////////////
  // if we have a stale request vote update the candidate's terms, disregard
  //   the request
  ///////////////////////////////////////////////////////////////////////////
  staleTerm := false
  rf.mu.Lock()
  // color.New(color.FgCyan).Printf("(%v)[%d][%v]: received a request vote from %d at term: %v\n", rf.state, rf.me, rf.currentTerm, args.CandidateId, args.Term)
  if args.Term < rf.currentTerm {
    reply.Term = rf.currentTerm
    reply.VoteGranted = false
    staleTerm = true
  }
  rf.mu.Unlock()
  if staleTerm {
    return
  }

  ///////////////////////////////////////////////////////////////////////////
  // Rules for all servers
  // step down and reset timer
  ///////////////////////////////////////////////////////////////////////////
  rf.mu.Lock()
  if args.Term > rf.currentTerm {
    // color.New(color.FgRed).Printf("(%v)[%d][%v]: received a larger term: %v (received term), stepping down from %v to Follower\n", rf.state, rf.me, rf.currentTerm, args.Term, rf.state)
    rf.convertToFollower(args.Term)
  }
  rf.mu.Unlock()

  ///////////////////////////////////////////////////////////////////////////
  // grant vote under the following conditions:
  // - we haven't voted for anyone yet, and
  // - the candidate's id and log is AT LEAST up-to-date as this peer's
  //   log
  // - Up-to-date is defined as: compare the index and term of the last
  //   entries in the logs.
  //   - If the logs have last entries with different
  //     terms, then the log with the later term is more up-to-date
  //   - If the logs end with the same term, whichever log is longer
  //     is more up to date
  ///////////////////////////////////////////////////////////////////////////
  rf.mu.Lock()
  if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.candidateIsMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
    // color.New(color.FgCyan).Printf("(%v)[%v][%d]: granting a vote to %d\n", rf.state, rf.me, rf.currentTerm, args.CandidateId)
    rf.setElectionTimeout()
    // rf.convertToFollower(args.Term)
    rf.votedFor = args.CandidateId
    reply.VoteGranted = true
    reply.Term = rf.currentTerm
  } else {
    reply.VoteGranted = false
    reply.Term = args.Term
  }
  rf.mu.Unlock()
  return
}

/* determine which of the two logs are more up to date
   this peer's or the leaders */
func (rf *Raft) candidateIsMoreUpToDate(candidateIdx int, candidateTerm int) bool {
  peerIdx := len(rf.log) - 1
  peerTerm := rf.log[len(rf.log)-1].Term
  return candidateTerm > peerTerm || (candidateTerm == peerTerm && candidateIdx >= peerIdx)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

  ///////////////////////////////////////////////////////////////////////////
  // Rule 1
  // if the append entries message is stale, disregard
  ///////////////////////////////////////////////////////////////////////////
  rf.mu.Lock()
  // color.New(color.FgGreen).Printf("(%v)[%v][%v]: received an append entry request from [%v], term %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
  isStale := false
  if args.Term < rf.currentTerm {
    // color.New(color.FgRed).Printf("(%v)[%v][%v]: received a STALE append entry request from [%v], term %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
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
    // color.New(color.FgRed).Printf("(%v)[%d][%v]: received a larger term: %v (received term), stepping down from %v to Follower\n", rf.state, rf.me, rf.currentTerm, args.Term, rf.state)
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
    // color.New(color.FgRed).Printf("(%v)[%d][%v]: received a larger term: %v (received term), stepping down from %v to Follower\n", rf.state, rf.me, rf.currentTerm, args.Term, rf.state)
    rf.convertToFollower(args.Term)
  }
  rf.mu.Unlock()

  ///////////////////////////////////////////////////////////////////////////
  // Rule 2
  // if the previous log index does not exist within our log, return false
  //   (the leader will decrement the prevlogindex and resend the updated
  //   list of entries)
  ///////////////////////////////////////////////////////////////////////////
  rf.mu.Lock()
  defer rf.mu.Unlock()
  if args.PrevLogIndex >= len(rf.log) {
    reply.Success = false
    reply.Term = rf.currentTerm
    return
  }

  if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
    reply.Success = false
    reply.Term = rf.currentTerm
    return
  }

  ///////////////////////////////////////////////////////////////////////////
  // Check if this is a heartbeat mechanism
  //   If so, the set the flag. The leader uses the result of AppendEntries
  //   to help decide whether or not should commit an entry.
  //   if heartbeat(args.Entries) && args.Term >= rf.currentTerm {
  // Respond to the heartbeat mechanism
  //     We are safe to reply success because we've performed two checks
  //     above^: if this peer's log contains the prev-log-index, and if the
  //     prev-log-term matches this peer's term.
  ///////////////////////////////////////////////////////////////////////////
  if heartbeat(args.Entries) {
    // color.New(color.FgYellow).Printf("(%v)[%v][%d]: recognizes heartbeat from Leader[%v][%v]\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
    rf.setElectionTimeout()
    reply.Success = true
    reply.Term = rf.currentTerm
  }

  ///////////////////////////////////////////////////////////////////////////
  // Rule 3
  // We've now established that this peer's log DOES contain a prev log
  //     index. Our goal now is to satisfy the Log Matching Property.
  // If an existing entry in this peers' log conflicts with an entry in
  //     entries[] (same index, but different term), delete the existing
  //     entry and all that follow it
  ///////////////////////////////////////////////////////////////////////////
  peerIdx := args.PrevLogIndex + 1
  leaderIdx := 0

  for ; leaderIdx < len(args.Entries) && peerIdx < len(rf.log); leaderIdx, peerIdx = leaderIdx+1, peerIdx+1 {
    if rf.entriesConflict(args.Entries, leaderIdx, peerIdx) {
      // color.New(color.FgGreen).Printf("(%v)[%v][%v]: received AE from leader: %v, term: %v. Truncating log before: %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.log)
      rf.truncateLog(peerIdx)
      // color.New(color.FgGreen).Printf("(%v)[%v][%v]: received AE from leader: %v, term: %v. Truncating log after: %v\n", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.log)
      break
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Rule 4
  // We've resolved all conflicting entries that may exist between the leader
  //     and this peer. Now, append any remaining entries to this peer.
  //     i.e. Tack on the new entries that the leader wants us to replicate.
  // We choose the method of making a new slice and copying the contents over
  //     because we want to free the current slice/underlying array that is
  //     being referenced by rf.log, in order for this outdated log to be
  //     garbage collected. Granted, this method has a linear complexity,
  //     we can make optimizations in the future if necessary
  ///////////////////////////////////////////////////////////////////////////
  if leaderIdx <= len(args.Entries)-1 {
    logWithReplicatedEntries := make([]Entry, len(rf.log)+len(args.Entries)-leaderIdx)
    copy(logWithReplicatedEntries, rf.log)
    rf.log = logWithReplicatedEntries
    for ; leaderIdx < len(args.Entries); leaderIdx, peerIdx = leaderIdx+1, peerIdx+1 {
      rf.log[peerIdx] = args.Entries[leaderIdx]
    }
  }
  reply.Success = true
  reply.Term = rf.currentTerm

  ///////////////////////////////////////////////////////////////////////////
  // Rule 5
  // Update the commit index of the follower.
  // Before this code, we've used the Leader's entries arr to replicate
  //     new entries on this peer. The last entry that we've replicated is
  //     the last log entry on this peer's log. As a result, update this
  //     peer's commit index.
  ///////////////////////////////////////////////////////////////////////////
  if args.LeaderCommit > rf.commitIndex {
    rf.commitIndex = minOf(args.LeaderCommit, len(rf.log)-1)
    go rf.applyEntry()
  }
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
