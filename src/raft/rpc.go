package raft

import "github.com/fatih/color"

type Entry struct {
  Term    int
  Command interface{}
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
  color.New(color.FgGreen).Printf("Peer[%d]: received a request vote from %d\n", rf.me, args.CandidateId)

  ///////////////////////////////////////////////////////////////////////////
  // if we have a stale request vote update the candidate's terms, disregard
  //     the request
  ///////////////////////////////////////////////////////////////////////////
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

  ///////////////////////////////////////////////////////////////////////////
  // rule for all servers
  ///////////////////////////////////////////////////////////////////////////
  rf.mu.Lock()
  if args.Term > rf.currentTerm {
    color.New(color.FgYellow).Printf("Peer[%d]: %v (our term), %v (received term), stepping down from %v to Follower\n", rf.me, rf.currentTerm, args.Term, rf.state)
    // step down and reset timer
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
  if rf.votedFor == -1 && rf.isCandidateUpToDate(args.LastLogIndex, args.LastLogTerm) {
    color.New(color.FgGreen).Printf("Peer[%d]: granting a vote to %d\n", rf.me, args.CandidateId)
    rf.setElectionTimeout() // when we grant a vote to a peer we MUST reset our election timeout
    rf.votedFor = args.CandidateId
    reply.VoteGranted = true
    reply.Term = args.Term
  }
  rf.mu.Unlock()
  return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  color.New(color.FgRed).Printf("Peer[%v]: received heartbeat from Leader[%v]\n", rf.me, args.LeaderId)
  ///////////////////////////////////////////////////////////////////////////
  // Rule 1
  // if the append entries message is stale, disregard
  ///////////////////////////////////////////////////////////////////////////
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
  // Rule 2
  // if the previous log index does not exist within our log, return false
  //     (the leader will decrement the prevlogindex and resend the updated
  //     list of entries)
  ///////////////////////////////////////////////////////////////////////////
  if args.PrevLogIndex > len(rf.log) {
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
  // follower
  // Check if this is a heartbeat mechanism
  //     If so, the set the flag. The leader uses the result of AppendEntries
  //     to help decide whether or not should commit an entry.
  //     if heartbeat(args.Entries) && args.Term >= rf.currentTerm {
  ///////////////////////////////////////////////////////////////////////////
  rf.mu.Lock()
  defer rf.mu.Unlock()
  isHeartbeat := false
  if heartbeat(args.Entries) {
    isHeartbeat = true
  }

  ///////////////////////////////////////////////////////////////////////////
  // rule for all servers
  ///////////////////////////////////////////////////////////////////////////
  rf.mu.Lock()
  if args.Term > rf.currentTerm {
    // step down and reset timer
    rf.convertToFollower(args.Term)
  }
  rf.mu.Unlock()

  ///////////////////////////////////////////////////////////////////////////
  // Respond to the heartbeat mechanism
  //     We are safe to reply success because we've performed two checks
  //     above^: if this peer's log contains the prev-log-index, and if the
  //     prev-log-term matches this peer's term.
  ///////////////////////////////////////////////////////////////////////////
  if isHeartbeat {
    color.New(color.FgYellow).Printf("Peer[%d]: (%v) recognizes heartbeat from Leader[%v]\n", rf.me, rf.state, args.LeaderId)
    rf.convertToFollower(args.Term)
    reply.Success = true
    reply.Term = args.Term
  }

  ///////////////////////////////////////////////////////////////////////////
  // Rule 3
  // We've now established that this peer's log DOES contain a prev log
  //     index. Our goal now is to satisfy the Log Matching Property.
  // If an existing entry in this peers' log conflicts with an entry in
  //     entries[] (same index, but different term), delete the existing
  //     entry and all that follow, until
  //     this peers' log index and entry === args.Entries[i]
  ///////////////////////////////////////////////////////////////////////////
  ourIdx := args.PrevLogIndex + 1
  leaderIdx := 0
  for ; leaderIdx < len(args.Entries) && ourIdx < len(rf.log); leaderIdx, ourIdx = leaderIdx+1, ourIdx+1 {
    if (args.Entries[leaderIdx].Term != rf.log[ourIdx].Term) || (args.Entries[leaderIdx].Command != rf.log[ourIdx].Command) {
      rf.log[ourIdx] = args.Entries[leaderIdx]
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
    for ; leaderIdx < len(args.Entries); leaderIdx, ourIdx = leaderIdx+1, ourIdx+1 {
      rf.log[ourIdx] = args.Entries[leaderIdx]
    }
  }

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
  }
}

func heartbeat(log []Entry) bool {
  return len(log) == 0
}

func minOf(args ...int) int {
  min := args[0]
  for _, i := range args {
    if i < min {
      min = i
    }
  }
  return min
}

/* determine which of the two logs are more up to date
   this peer's or the leaders */
func (rf *Raft) isCandidateUpToDate(candidatesLastLogIndex int, candidatesLastLogTerm int) bool {
  lastEntry := rf.log[len(rf.log)-1]
  // if this raft peer's term is higher, return false
  //     don't grant vote
  if lastEntry.Term > candidatesLastLogTerm {
    return false
  }

  // if the raft peer's term is == or <, return true
  //     grant the vote
  if lastEntry.Term < candidatesLastLogTerm {
    return true
  }

  // implicit: lastEntry.Term == candidatesLastLogTerm
  // whichever log is longer, the leader or the peer's
  //     determines who is MORE up-to-date
  // if len(rf.log) <= candidatesLastLogIndex {
  //   return true
  // } else {
  //   return false
  // }

  // however, we only care if the candidate's log is
  // AT-LEAST up-to-date as this peer
  return true
}
