package raft

import (
  "fmt"
)

type Entry struct {
  Term int // The leader's term that requested this Entry to be replicated
  // Index   int         // The Entry's index in the leader's log
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
  // color.New(color.FgGreen).Printf("Peer[%d]: received a request vote from %d\n", rf.me, args.CandidateId)

  ///////////////////////////////////////////////////////////////////////////
  // if we have a stale request vote update the candidate's terms, disregard
  //     the request
  ///////////////////////////////////////////////////////////////////////////
  staleTerm := false
  rf.mu.Lock()
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
  // rule for all servers
  ///////////////////////////////////////////////////////////////////////////
  rf.mu.Lock()
  if args.Term > rf.currentTerm {
    // color.New(color.FgYellow).Printf("Peer[%d]: %v (our term), %v (received term), stepping down from %v to Follower\n", rf.me, rf.currentTerm, args.Term, rf.state)
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
  if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) || rf.candidateIsMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
    // color.New(color.FgGreen).Printf("Peer[%d]: granting a vote to %d\n", rf.me, args.CandidateId)
    rf.setElectionTimeout() // when we grant a vote to a peer we MUST reset our election timeout
    rf.votedFor = args.CandidateId
    reply.VoteGranted = true
    reply.Term = args.Term
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
  // color.New(color.FgRed).Printf("Peer[%v]: received heartbeat from Leader[%v]\n", rf.me, args.LeaderId)
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
  // rule for all servers
  ///////////////////////////////////////////////////////////////////////////
  rf.mu.Lock()
  // if args.Term > rf.currentTerm {
  if args.Term >= rf.currentTerm {
    // step down and reset timer
    rf.convertToFollower(args.Term)
  }
  rf.mu.Unlock()

  ///////////////////////////////////////////////////////////////////////////
  // Rule 2
  // if the previous log index does not exist within our log, return false
  //     (the leader will decrement the prevlogindex and resend the updated
  //     list of entries)
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
  // follower
  // Check if this is a heartbeat mechanism
  //     If so, the set the flag. The leader uses the result of AppendEntries
  //     to help decide whether or not should commit an entry.
  //     if heartbeat(args.Entries) && args.Term >= rf.currentTerm {
  ///////////////////////////////////////////////////////////////////////////
  isHeartbeat := false
  if heartbeat(args.Entries) {
    isHeartbeat = true
  }

  // fmt.Printf("AppendEntries[%v]:\n leaderCommit: %v\n prevlogindex: %v\n prevlogterm: %v\n -----------\n currentTerm: %v\n commitIndex: %v\n", rf.me, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, rf.currentTerm, rf.commitIndex)

  ///////////////////////////////////////////////////////////////////////////
  // Respond to the heartbeat mechanism
  //     We are safe to reply success because we've performed two checks
  //     above^: if this peer's log contains the prev-log-index, and if the
  //     prev-log-term matches this peer's term.
  ///////////////////////////////////////////////////////////////////////////
  if isHeartbeat {
    // color.New(color.FgYellow).Printf("Peer[%d]: (%v) recognizes heartbeat from Leader[%v]\n", rf.me, rf.state, args.LeaderId)
    rf.convertToFollower(args.Term)
    reply.Success = true
    reply.Term = args.Term
  } else {
    // color.New(color.FgGreen).Printf("Peer[%d]: replicating entries from Leader[%v]\n", rf.me, args.LeaderId)
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
        fmt.Printf("truncate log before: %v\n", rf.log)
        rf.truncateLog(peerIdx)
        fmt.Printf("truncate log after: %v\n", rf.log)
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
    // fmt.Printf("Peer[%v]: leaderCommit: %v, commit index: %v\n", rf.me, args.LeaderCommit, rf.commitIndex)
    // go rf.applyEntry()

    // rf.commitIndex = args.LeaderCommit
    // if len(rf.log)-1 < rf.commitIndex {
    //   rf.commitIndex = len(rf.log) - 1
    // }
  }
  rf.applyEntry()
  // color.New(color.FgGreen).Printf("Peer[%d]: log%v\n", rf.me, rf.log)
  return
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

// if this raft peer's term is higher, return false
//     don't grant vote

// if the raft peer's term is == or <, return true
//     grant the vote
// return candidatesLastLogTerm >= lastEntry.Term
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
// (args.LastLogIndex > len(rf.log)-1 && args.LastLogTerm > rf.log[len(rf.log)-1].Term)
