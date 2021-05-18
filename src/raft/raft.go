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
}

type Raft struct {
  mu        sync.Mutex          // Lock to protect shared access to this peer's state
  peers     []*labrpc.ClientEnd // RPC end points of all peers
  persister *Persister          // Object to hold this peer's persisted state
  me        int                 // this peer's index into peers[]
  dead      int32               // set by Kill()

  // Persistent state on all servers
  currentTerm int
  votedFor    int
  log         []Entry

  // volatile state on all servers
  electionTimeout time.Time // Election timeout
  commitIndex     int       // index of the highest log entry known to be committed (init to 0, increases monotonically)
  nextIndex       []int     // index of the next log entry to send to that server (initialized to leader last log index + 1)
  matchIndex      []int     // index of the highest log entry known ot be replicated on server (initialized to 0, increases monotonically)
  state           string
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

  return index, term, isLeader
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
  // Your code here, if desired.
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
}

func (rf *Raft) convertToCandidate() {
  rf.state = CANDIDATE
  rf.currentTerm += 1
  rf.setElectionTimeout()
}

func (rf *Raft) setElectionTimeout() {
  interm := rand.NewSource(time.Now().UnixNano())
  random := rand.New(interm)
  timeout := time.Duration(200+random.Int63()%300) * time.Millisecond
  rf.electionTimeout = time.Now().Add(timeout)
}

func sleep() {
  ms := 50
  time.Sleep(time.Duration(ms) * time.Millisecond)
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

  // election timeout
  rf.setElectionTimeout()

  // initialize from state persisted before a crash
  rf.readPersist(persister.ReadRaftState())

  go rf.kickOffElectionTimeout()

  return rf
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
  // if isLeader {
  //  go rf.establishAuthority()
  // } else

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
  color.New(color.FgMagenta).Printf("Peer[%v]: converting from Follower -> Candidate\n", rf.me)
  color.New(color.FgMagenta).Printf("Candidate[%v]: starting an election at Term: %v\n", rf.me, rf.currentTerm)
  /////////////////////////////////////////////////////////////////////////////
  // - Create Request Vote structs
  // - Vote for ourself, and initialize a term variable
  // We need the term variable because we may have to update our candidate
  // based on the follower's highest term, ie our candidate may be out of date
  /////////////////////////////////////////////////////////////////////////////
  args := &RequestVoteArgs{
    CandidateId: rf.me,
    Term:        rf.currentTerm,
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
    color.New(color.FgRed).Printf("Candidate[%v]: stepping down to follower\n", rf.me)
    rf.convertToFollower(higherTerm)
  }
  fmt.Printf("Candidate[%v] has %v votes", rf.me, votes)
  /* FIX: in the presence of partial network partition, and we are the sole peer, we must assume leadership in order to make progress
     however, our election handler does not explicitly handle this case
  */
  // if (votes > len(rf.peers)/2 || finished == len(rf.peers)) && rf.state == CANDIDATE && higherTerm == rf.currentTerm {
  if votes > len(rf.peers)/2 && rf.state == CANDIDATE && higherTerm == rf.currentTerm {
    color.New(color.FgCyan).Printf("Candidate[%v]: Won Election!\n", rf.me)
    rf.convertToLeader()
  }
  rf.mu.Unlock()

  _, isLeader := rf.GetState()
  if isLeader {
    go rf.establishAuthority()
  }
}

func (rf *Raft) establishAuthority() {
  color.New(color.FgCyan).Printf("Leader[%v]: establishAuthority\n", rf.me)
  for rf.killed() == false {
    _, isLeader := rf.GetState()
    if isLeader == false {
      break
    }
    rf.mu.Lock()
    args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
    termChanged := false
    higherTerm := rf.currentTerm
    rf.mu.Unlock()

    cond := sync.NewCond(&rf.mu)
    finished := 1

    for peer := 0; peer < len(rf.peers); peer++ {
      if rf.me == peer {
        continue
      }
      _, isLeader := rf.GetState()
      if isLeader {
        go func(peer int) {
          /* Send Heartbeats! */
          // how do we send to the peers?
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

    // _, isLeader := rf.GetState()
    // if isLeader == false {
    //  break
    // }

    _, isLeader = rf.GetState()
    if isLeader == false {
      break
    } else {
      // we sleep because the tester limits us to 10 heartbeats/sec
      // but here's a scenario
      // we go to sleep as the leader
      // and wake back up as a follower/candidate
      time.Sleep(time.Duration(180 * time.Millisecond))
    }
  }
}

/*
  - retry indefinetly
  - decrement index on retry
  - receive an entry
  - determine when to commit an entry (entries)
*/
