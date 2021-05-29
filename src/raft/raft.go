package raft

import (
  "fmt"
  "math/rand"
  "sync"
  "sync/atomic"
  "time"

  "../labrpc"
  "github.com/fatih/color"
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
  // color.New(color.FgRed).Printf("(%v)[%d][%v]: (new term) %v, stepping down from %v -> Follower\n", rf.state, rf.me, rf.currentTerm, term, rf.state)
  rf.currentTerm = term
  rf.state = FOLLOWER
  rf.votedFor = -1
  rf.setElectionTimeout()
}

func (rf *Raft) convertToCandidate() {
  rf.state = CANDIDATE
  rf.votedFor = rf.me
  rf.currentTerm += 1
  rf.setElectionTimeout()
}

func (rf *Raft) convertToLeader() {
  rf.state = LEADER
  rf.setElectionTimeout()
  rf.nextIndex = make([]int, len(rf.peers))
  rf.matchIndex = make([]int, len(rf.peers))
  // color.New(color.FgMagenta).Printf("New Leader[%v][%v]: log length: %v, and log:%v\n", rf.me, rf.currentTerm, len(rf.log), rf.log)
  for peer := 0; peer < len(rf.peers); peer++ {
    rf.nextIndex[peer] = len(rf.log)
    rf.matchIndex[peer] = 0
  }
}

func (rf *Raft) persist() {}
func (rf *Raft) readPersist(data []byte) {
  if data == nil || len(data) < 1 { // bootstrap without any state?
    return
  }
}

func (rf *Raft) Kill() {
  atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
  z := atomic.LoadInt32(&rf.dead)
  return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
  persister *Persister, applyCh chan ApplyMsg) *Raft {
  fmt.Printf("Peer[%v]: creating peer\n", me)

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
  return rf
}

func (rf *Raft) setElectionTimeout() {
  interm := rand.NewSource(time.Now().UnixNano())
  random := rand.New(interm)
  timeout := time.Duration(300+random.Int63()%170) * time.Millisecond
  rf.electionTimeout = time.Now().Add(timeout)
}

func sleep() {
  ms := 20
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  return ok
}

func (rf *Raft) performLeaderElection() {
  rf.mu.Lock()
  /////////////////////////////////////////////////////////////////////////////
  // Convert To Candidate
  /////////////////////////////////////////////////////////////////////////////
  color.New(color.FgMagenta).Printf("Peer[%v][%v]: converting from %v -> Candidate\n", rf.me, rf.currentTerm, rf.state)
  rf.convertToCandidate()
  color.New(color.FgMagenta).Printf("Candidate[%v][%v]: starting an election: %v\n", rf.me, rf.currentTerm, rf.log)
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
      rf.mu.Lock()
      color.New(color.FgCyan).Printf("Candidate[%v][%v]: sent a request vote to %v\n", rf.me, rf.currentTerm, peer)
      rf.mu.Unlock()
      reply := &RequestVoteReply{}
      nodeReceivedMessage := rf.sendRequestVote(peer, args, reply)
      rf.mu.Lock()
      defer rf.mu.Unlock()

      if nodeReceivedMessage && reply.VoteGranted && reply.Term == rf.currentTerm && rf.state == CANDIDATE {
        votes++
      } else if reply.Term > rf.currentTerm {
        termChanged = true
        higherTerm = reply.Term
      } else {
        // received stale request vote response, we've moved on
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
    color.New(color.FgRed).Printf("Candidate[%v][%v]: stepping down to follower, new term%v\n", rf.me, rf.currentTerm, higherTerm)
    rf.convertToFollower(higherTerm)
  }

  if votes > len(rf.peers)/2 && rf.state == CANDIDATE && higherTerm == rf.currentTerm {
    color.New(color.FgMagenta).Printf("Candidate[%v][%v]: Won Election! Log: %v\n", rf.me, rf.currentTerm, rf.log)
    rf.convertToLeader()
  }
  rf.mu.Unlock()

  _, isLeader := rf.GetState()
  if isLeader {
    go rf.establishAuthority()
  }
}

func (rf *Raft) establishAuthority() {
  for rf.killed() == false {
    _, isLeader := rf.GetState()
    if isLeader == false {
      return
    }
    rf.mu.Lock()
    color.New(color.FgMagenta).Printf("Leader[%v][%v]: establishAuthority! Log: %v\n", rf.me, rf.currentTerm, rf.log)
    termChanged := false
    higherTerm := rf.currentTerm
    originalTerm := rf.currentTerm
    rf.mu.Unlock()
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
          rf.mu.Lock()
          color.New(color.FgYellow).Printf("Leader[%v][%v]: sent a heartbeat message to %v\n", rf.me, rf.currentTerm, peer)
          rf.mu.Unlock()
          reply := &AppendEntriesReply{}
          nodeReceivedHeartbeat := rf.sendAppendEntries(peer, args, reply)
          rf.mu.Lock()
          defer rf.mu.Unlock()

          if nodeReceivedHeartbeat && reply.Success == false && reply.Term > args.Term {
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
    if termChanged && higherTerm > originalTerm {
      color.New(color.FgRed).Printf("Leader[%v][%v]: by virtue of heartbeat, stepping down to follower, new term%v\n", rf.me, rf.currentTerm, higherTerm)
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
      time.Sleep(time.Duration(80 * time.Millisecond))
    }
  }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
  ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
  return ok
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
  term, isLeader := rf.GetState()
  if !isLeader {
    return -1, term, isLeader
  }
  color.New(color.FgGreen).Printf("Leader[%v][%v]: received client request: %v\n", rf.me, rf.currentTerm, command)
  rf.mu.Lock()
  defer rf.mu.Unlock()

  rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
  index := len(rf.log) - 1
  term = rf.currentTerm

  go rf.attemptCommitEntry()
  return index, term, isLeader
}

func (rf *Raft) attemptCommitEntry() {
  // the only reason why we have the num-replicated is due to our need to retry sending entries
  // the long-running for loop must terminate somehow
  // replicatedAllPeers := false
  // numReplicated := 1 // initialized to 1 because we've replicated the entry on our own log
  // for !rf.killed() || !replicatedAllPeers {
  for !rf.killed() {
    _, isLeader := rf.GetState()
    if !isLeader {
      return
    }

    for peer := 0; peer < len(rf.peers); peer++ {
      if peer == rf.me {
        continue
      }

      shouldSend := true
      rf.mu.Lock()

      if len(rf.log)-1 < rf.nextIndex[peer] {
        shouldSend = false
      }

      args := &AppendEntriesArgs{
        Term:         rf.currentTerm,
        LeaderId:     rf.me,
        LeaderCommit: rf.commitIndex,
        PrevLogIndex: rf.nextIndex[peer] - 1,
        PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
        Entries:      rf.log[rf.nextIndex[peer]:],
      }

      rf.mu.Unlock()
      if shouldSend {
        go func(peer int) {
          rf.mu.Lock()
          color.New(color.FgGreen).Printf("Leader[%v][%v]: sending an AE to %v, entries: %v\n", rf.me, rf.currentTerm, peer, args.Entries)
          rf.mu.Unlock()
          reply := &AppendEntriesReply{}
          messageReceived := rf.sendAppendEntries(peer, args, reply)

          if !messageReceived {
            color.New(color.FgRed).Printf("Leader[%v][%v]: message could not be delivered to %v. Message: %v \n", rf.me, args.Term, peer, args.Entries)
            return
          }

          rf.mu.Lock()
          defer rf.mu.Unlock()

          ///////////////////////////////////////////////////////////////////////////
          // if we're no longer leader, we halt execution immediately
          ///////////////////////////////////////////////////////////////////////////
          if rf.state != LEADER {
            color.New(color.FgRed).Printf("FRAUD Leader[%v][%v]: halt execution\n", rf.me, args.Term)
            return
          }

          ///////////////////////////////////////////////////////////////////////////
          // if the peer's term is larger than our term when we sent the request
          // we must step down to follower, our term is out of date
          ///////////////////////////////////////////////////////////////////////////
          if reply.Success == false && reply.Term > args.Term {
            color.New(color.FgRed).Printf("Leader[%v][%v]: peer: %v has a term %v that's larger than ours: %v, stepping down!\n", rf.me, args.Term, peer, reply.Term, args.Term)
            rf.convertToFollower(reply.Term)
            return
          }

          ///////////////////////////////////////////////////////////////////////////
          // Retry if we sent an incorrect prevLogIndex, nextIndex is optimistic
          ///////////////////////////////////////////////////////////////////////////
          if reply.Success == false && reply.Term == args.Term {
            color.New(color.FgGreen).Printf("Leader[%v][%v]: RETRY! decrement nextIndex. We sent: %v to %v\n", rf.me, args.Term, args.Entries, peer)
            rf.nextIndex[peer] = args.PrevLogIndex - 1
          }

          ///////////////////////////////////////////////////////////////////////////
          // We've successfully replicated the entry:
          // 1. Update the index of the highest log entry
          //   - known to replicated in the server: Conservative
          //   - to send to that server: Optimistic
          // 2. Update the commit index if we have replicated on a majority of servers
          // Additionally: we enforce a rule for committing entries
          // only log entries from the leader's current term are committed
          ///////////////////////////////////////////////////////////////////////////
          if reply.Success && reply.Term == args.Term {
            color.New(color.FgGreen).Printf("Leader[%v][%v]: successfully replicated %v on Peer[%v]\n", rf.me, args.Term, args.Entries, peer)
            rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
            rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
            // numReplicated++
            if len(rf.log)-1 > rf.commitIndex {
              if rf.hasReplicatedMajority() && rf.currAndLogTermMatch() {
                rf.updateCommitIndex()
                go rf.applyEntry()
              }
            }
          }
          return
        }(peer)
      }
    }
    // if numReplicated == len(rf.peers) {
    //   replicatedAllPeers = true
    // }
    time.Sleep(30 * time.Millisecond)
  }
}

func (rf *Raft) hasReplicatedMajority() bool {
  majorityReplicated := 0
  for peer := 0; peer < len(rf.peers); peer++ {
    if rf.matchIndex[peer] >= len(rf.log)-1 || peer == rf.me {
      majorityReplicated++
    }
  }
  return majorityReplicated > len(rf.peers)/2
}

func (rf *Raft) currAndLogTermMatch() bool {
  return rf.log[len(rf.log)-1].Term == rf.currentTerm
}

func (rf *Raft) updateCommitIndex() {
  color.New(color.FgGreen).Printf("Leader[%v][%v]: new commit index: %v\n", rf.me, rf.currentTerm, len(rf.log)-1)
  rf.commitIndex = len(rf.log) - 1
}

func (rf *Raft) applyEntry() {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  for rf.commitIndex > rf.lastApplied {
    rf.lastApplied += 1
    entry := rf.log[rf.lastApplied]
    msg := ApplyMsg{
      CommandValid: true,
      Command:      entry.Command,
      CommandIndex: rf.lastApplied,
      CommandTerm:  entry.Term,
    }
    color.New(color.FgGreen).Printf("(%v)[%v][%v]: updated lastapplied: %v, commitIndex: %v\n", rf.state, rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
    rf.applyCh <- msg
  }
  color.New(color.FgGreen).Printf("(%v)[%v][%v]: Final state commitIndex: %v, lastApplied: %v\n", rf.state, rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
  return
}
