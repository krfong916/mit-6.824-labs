Candidate 2 is elected at[6]
Establishes Authority
Receives Append Entries
Our network is made unreliable, messages that Leader 2 sends are re-ordered, not delivered etc.
A peer's election timeout passes, and a new candidate steps forth
The candidate increments their term and strikes an election
Leader 2 acknowledges the candidate's higher term and steps down

- We didn't have a safety check that prevented the client from submitting requests to assumed leaders. The bug: in an unreliable network, if a new leader come to term, it takes some time to propagate the new leader's information to the client. The following may occur: from the Raft cluster's POV, we have an old leader A, and a new leader B. B has a higher term than A. From the client's POV, leader A is the correct leader and leader B does not exist because the information has not been propagated to the client.
  The client sends requests to A, because the client believes A is the leader. In this scenario, we found a bug in our protocol - we did not implement a safety check for the client request handler to ensure only the leader can APPEND ENTRIES.
  The original code looked like this:

```go
func (rf *Raft) Start(command interface{}) (int, int, bool) {
  term, isLeader := rf.GetState()
  if !isLeader {
    return -1, term, isLeader
  }
  rf.mu.Lock()

  // append entry to the log

  rf.mu.Unlock()

  /* unprotected critical section, we cannot ensure the peer that received the request
  is still a leader when we start this thread */
  go rf.attemptCommitEntry()

  return index, term, isLeader
}
```

And the change:

```go
func (rf *Raft) Start(command interface{}) (int, int, bool) {
  term, isLeader := rf.GetState()
  if !isLeader {
    return -1, term, isLeader
  }
  rf.mu.Lock()
  defer rf.mu.Unlock()
  // append entry to the log

  /* unprotected critical section, we cannot ensure the peer that received the request
  is still a leader when we start this thread */
  if rf.state == LEADER {
    go rf.attemptCommitEntry()
    return index, term, isLeader
  } else {
    return -1, term, isLeader
  }
}
```

Leader 3 at Term 2 receives client request 5534
CReq (LEADER)[3][2]: log: [{0 <nil>} {2 4250} {2 7961} {2 4152} {2 5534}] <- index[4]
2021/06/18 15:12:50 apply error: commit index=4 server=3 5534 != server=4 1826

Peer 2 comes to candidacy and wins the election
Receives and sends client requests at Term 3
APPL (FOLLOWER)[1][3]: log: [{0 <nil>} {2 4250} {2 7961} {2 4152} {3 1826}] <- index[4], commitIndex[4], lastApplied[4]
Successfully replicates and updates the commit index and applies to the client
APPL (LEADER)[2][3]: log: [{0 <nil>} {2 4250} {2 7961} {2 4152} {3 1826}] <- index[4], commitIndex[4], lastApplied[4]
Receives and sends more requests which are replicated
Followers acknowledge the delivery
At a later time, the followers also update their commitIndex and lastApplied
APPL (FOLLOWER)[0][3]: log: [{0 <nil>} {2 4250} {2 7961} {2 4152} {3 1826} {3 9938}] <- index[4], commitIndex[4], lastApplied[4]
APPL (FOLLOWER)[4][3]: log: [{0 <nil>} {2 4250} {2 7961} {2 4152} {3 1826} {3 9938}] <- index[4], commitIndex[4], lastApplied[4]

Leader 2 gets disconnected
Peer 0 comes to candidacy and wins the election

(leader)[3][2]: received a larger term
(leader)[3][2]: peer

The commit index is mistakenly updated without

Our tests are taking too long - longer than the testing suites benchmarks

- timing and availability
  We had an issue of too short of AppendEntry RPC timeouts that resulted in sending too many RPCs and holding onto locks for too long.
  This resulted in availabilty issues because a candidate could not step forth and become leader in a timely manner.
  This issue of leader election and chatty network communication was further compounded when we introduced network unreliability to our cluster.
  AppendEntry RPCs would be sent, but they would be delayed and re-ordered. Many AE RPCs would eventually be delivered and the receiver and sender would have to process stale requests (acquiring and releasing the lock) - this takes time.

Our tests fail because the peer's within our cluster have incorrect logs when we model an unreliable network

- There were two bugs in our implementation of our protocol
- 1: We made a distinction between heartbeats and AppendEntries in our mental model. This is wrong, and as a result, we incorrectly implemented the sending and receiving methods. First, we assumed heartbeats contained no entries, therefore, we sent AppendEntries with no entries. Second, if a follower accepted a heartbeat and resolved conflicting entries, the leader did not update their commit index. In order to correct our protocol, heartbeats sent entries for followers lagging behind, and we updated the leader's commit index if they received a successful reply
  - The bugs in our protocol presented itself when we tested under unreliable network conditions. Under normal operation, all AppendEntries were sent, delivered, and replicated amongst peers with no issue. However, under failure, we had logs that were incorrect. Our testing failures were further compounded by the second bug in our Raft protocol - our implementation of the accelerate log optimization protocol
- 2: The issue: we would incorrectly accept an AppendEntry request (Heartbeats)
  - We introduce a network partition between Leader A and the cluster
  - Follower B comes to candidacy and is granted leadership
  - Leader B makes progress
  - The partition is resolved, Leader A rejoins
  - Leader A receives an RPC with a term higher and steps down to Follower
  - Follower A has an outdated log
  - Follower A receives a heartbeat message. Here is where the source of our headaches occured - our log did not satisfy the log matching protocol: either the entry at PrevLogIndex did not have a matching term, or an entry at PrevLogIndex did not exist at all and we would incorrectly reply OK. Secondly, we would perform step 5 and update our commit index.
  - In order to performs the safety checks outlined in Figure 2, we needed to reply false and reply with the conflicting index and term WITHOUT
- We corrected our implementation of the accelerated log optimization protocol
