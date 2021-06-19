We win leadership and receive client requests
- CReq (LEADER)[3][2]: received client request: 5534


AESender Leader[3][2]: peer: 4 has a term 5 that's larger than ours: 2, stepping down!
RVReceiver (CANDIDATE)[3][15]: received a larger term: 32, stepping down from CANDIDATE to Follower
HB Leader[1][32]: sent a heartbeat message to 3 with log: 0, term: 31, and index: 83
AEReceiver (FOLLOWER)[3][32]: received an append entry request from [1][32]
AEReceiver (FOLLOWER)[3][32]: commitIndex=1, lastApplied=1
AEReceiver (FOLLOWER)[3][32]: Leader[1][32] sent args.PrevLogIndex=83, args.PrevLogTerm=31, args.LeaderCommit=83
AEReceiver (FOLLOWER)[3][32]: PrevLogIndex DNE, Leader[1][32]: index: 83, term: 31
HB Leader[1][32]: sent a heartbeat message to 3 with log: 0, term: 4, and index: 35
AEReceiver (FOLLOWER)[3][32]: received an append entry request from [1][32]
AEReceiver (FOLLOWER)[3][32]: commitIndex=1, lastApplied=1
AEReceiver (FOLLOWER)[3][32]: Leader[1][32] sent args.PrevLogIndex=35, args.PrevLogTerm=4, args.LeaderCommit=83
AEReceiver (FOLLOWER)[3][32]: Terms Conflict, ConflictIndex 35 Leader[1][32]: index: 35, term: 4
AEReceiver (FOLLOWER)[3][32]: received an append entry request from [1][32]
AEReceiver (FOLLOWER)[3][32]: commitIndex=1, lastApplied=1
AEReceiver (FOLLOWER)[3][32]: Leader[1][32] sent args.PrevLogIndex=34, args.PrevLogTerm=4, args.LeaderCommit=83
AEReceiver (FOLLOWER)[3][32]: Terms Conflict, ConflictIndex 1 Leader[1][32]: index: 34, term: 4
HB Leader[1][32]: sent a heartbeat message to 3 with log: 0, term: 2, and index: 3
AEReceiver (FOLLOWER)[3][32]: received an append entry request from [1][32]
AEReceiver (FOLLOWER)[3][32]: commitIndex=1, lastApplied=1
AEReceiver (FOLLOWER)[3][32]: Leader[1][32] sent args.PrevLogIndex=3, args.PrevLogTerm=2, args.LeaderCommit=83
HB (FOLLOWER)[3][32]: recognizes heartbeat from Leader[1][32]
AEReceiver (FOLLOWER)[3][32]: received AE from leader: 1, term: 32
AEReceiver (FOLLOWER)[3][32]: received an append entry request from [1][32]
AEReceiver (FOLLOWER)[3][32]: commitIndex=35, lastApplied=1
AEReceiver (FOLLOWER)[3][32]: Leader[1][32] sent args.PrevLogIndex=34, args.PrevLogTerm=4, args.LeaderCommit=83
AEReceiver (FOLLOWER)[3][32]: Terms Conflict, ConflictIndex 1 Leader[1][32]: index: 34, term: 4
AEReceiver (FOLLOWER)[3][32]: received an append entry request from [1][32]
AEReceiver (FOLLOWER)[3][32]: commitIndex=35, lastApplied=1
AEReceiver (FOLLOWER)[3][32]: Leader[1][32] sent args.PrevLogIndex=34, args.PrevLogTerm=4, args.LeaderCommit=83
AEReceiver (FOLLOWER)[3][32]: Terms Conflict, ConflictIndex 1 Leader[1][32]: index: 34, term: 4
APPL (FOLLOWER)[3][32]: updated lastapplied: 2, commitIndex: 35
APPL (FOLLOWER)[3][32]: updated lastapplied: 3, commitIndex: 35
APPL (FOLLOWER)[3][32]: updated lastapplied: 4, commitIndex: 35
APPL (FOLLOWER)[3][32]: updated lastapplied: 5, commitIndex: 35
2021/06/18 15:12:50 apply error: commit index=4 server=3 5534 != server=4 1826
exit status 1
FAIL  _/Users/kyle/go/6.824/src/raft  86.017s

real  1m30.279s
user  1m16.926s
sys 0m11.466s
MacBook-Pro:raft kyle$ time go test -race -run 2C
