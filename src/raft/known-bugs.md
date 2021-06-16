## Basic Agreement

- Efficacy: Done 98/100; 98 ok, 2 failed
- Bug: We will fail the basic agreement test if our election timeout is too long
- **5/26/2021 FIXED: adjusted election timeout**

```
=== RUN   TestBasicAgree2B
Test (2B): basic agreement ...
    config.go:480: one(100) failed to reach agreement
--- FAIL: TestBasicAgree2B (3.28s)
```

## Agreement Despite Follower Disconnection

- Efficacy: Done 95/100; 95 ok, 5 failed
- **SOLUTION: not a bug.** TL;DR: if the entry is not replicated on a majority of machines (and therefore not committed on the leader), if a new leader steps forth then the uncommitted entries may be overwritten. The tests fail due to the testing harness, and not the program itself.
  - Steps to reproduce:
    - Submit a client request
    - Immediately strike a new election before the leader delivers AppendEntry RPCs to followers. The candidate will have a higher term
    - Send RequestVote RPCs, upon the leader receiving a RequestVote RPC, the term will be higher and will grant the vote to the candidate and step down
    - The candidate will receive the reply and ascend to leadership
    - --> The testing suite will fail for this reason because no 'agreement' has been made on the committed client request.

```
=== RUN   TestFailAgree2B
Test (2B): agreement despite follower disconnection ...
    config.go:480: one(101) failed to reach agreement
--- FAIL: TestFailAgree2B (2.47s)
```

## No Agreement If Too Many Followers

- Efficacy: Done 100/100; 93 ok, 7 failed

```
=== RUN   TestFailNoAgree2B
Test (2B): no agreement if too many followers disconnect ...
    config.go:480: one(10) failed to reach agreement
--- FAIL: TestFailNoAgree2B (2.47s)
FAIL
```

## Rejoin of Partitioned Leader

```
truncate log before: [{0 <nil>} {1 101} {2 103} {6 103}]
truncate log after: [{0 <nil>} {1 101}]
2021/05/25 11:28:41 apply error: commit index=2 server=1 102 != server=2 103
exit status 1
FAIL  _/Users/kyle/go/6.824/src/raft  6.584s

truncate log before: [{0 <nil>} {1 101} {5 103}]
truncate log after: [{0 <nil>} {1 101}]
2021/05/25 11:28:50 apply error: commit index=2 server=1 102 != server=2 103
exit status 1
FAIL  _/Users/kyle/go/6.824/src/raft  6.080s
```

### Findings

- We need to break from attempting to commit entries once we have committed on all servers, we only break when we are no longer leader
- We have two leaders present and the leader with the smaller term is not stepping down

## Leader Backs Up Quickly Over Incorrect Follower Logs

```
race: limit on 8128 simultaneously alive goroutines is exceeded, dying
```

// 2A
// 350.300 and 50 - 38/49.11
// 350.300 and 30 - 42/49.7
// 350.250 and 30 -
// 350.225 and 30 - 85/100.15
// 350.200 and 30 - 45/49.4
// 350.150 and 30 - 41/48.8
// 0.400 and 30 - 88/100.12
// 0.350 and 30 - 94/100.6
// 0.350 and 30 - 88/100.13 term changes too mcuh

// 2B Pt.1
// 0.350 - 93/100.7
// 0.500 and 30 - 35/49.14
// 0.350 and 100 - 19/27.8
// 300.170 and 20 - 99/100.1!! wow - still, cannot reach agreement

// 2B Pt.2
// 0.500 - 100/100!! wow

// 2B Pt.3
// 0.500 and 30 - 5/11.6

// 2B Altogether
// 0.500 -

- [x] RULES FOR ALL SERVERS (Rule 1) create an applyEntry handler. if commitIndex > lastApplied at anytime, apply a particular log entry. First, we must lock on the applies - Why? - some other routine doesn't also detect that entries need to be applied and also tries to apply
  - when do we check? after commitIndex is updated (like after matchIndex is updated).
  - Warning, if we check commitIndex @thesame time as sending out AppendEntries to peers, we'll have to wait until the next entry is appended to the log before applying the entry you just sent out and got acknowledged
- [x] Raft leader's can't be sure an entry is actually committed on other servers, so, we'll need to specifically check that log[N].term == currentTerm - where?

- [x] truncate the log ONLY when we find a conflict in append entries RPC
- [x] If an AE is rejected and not because of log inconsistency - then immediately step down (we know this) and NOT update nextIndex. But why? If we do, we could race with the resetting of nextIndex if we are re-elected immediately

Difference between nextIndex and matchIndex

- matchIndex == nextIndex-1
- matchIndex is used for safety
- nextIndex is a guess about the log prefix that the peer shares with the leader
  - optimistic and only moves back on negative responses
- [x] when a leader has been elected, nextIndex is set to be the index at the end of the log
- [x] matchIndex is never set too high, it is a conservative measurement of what log prefix the peer shares with the leader. It is initially set to -1 and only updated when a follower positively acknowledges an AppendEntries RPC --> it's the highest value known to be replicated on the peer's server, we must update with prevLogIndex + len(entries[]) with the args that we sent in the RPC originally

While we send an RPC, our state can change, therefore, we must handle our assumptions about our data with care
