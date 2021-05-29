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
