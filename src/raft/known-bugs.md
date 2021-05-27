## Basic Agreement

- Efficacy: Done 100/100; 98 ok, 2 failed
- Bug: We will fail the basic agreement test if our election timeout is too long
- Fix: adjust election timeout

```
=== RUN   TestBasicAgree2B
Test (2B): basic agreement ...
    config.go:480: one(100) failed to reach agreement
--- FAIL: TestBasicAgree2B (3.28s)
```

## Agreement Despite Follower Disconnection

- Efficacy: Done 100/100; 95 ok, 5 failed

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

## Leader Backs Up Quickly Over Incorrect Follower Logs

```
race: limit on 8128 simultaneously alive goroutines is exceeded, dying
```
