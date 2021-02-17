- Implement Raft leader election and heartbeats (AppendEntries w/no log entries), the leader asserting control, and leader election assuming failures and packets lost

- How do I initialize a Raft cluster of servers?
  - how do i define a leader and followers? What decides the initial leader?
  - how do i implement a timer for each peer? how do i reset the timer during AppendEntries?
  - Where, when, and how do we invoke AppendEntries?

Questions:

- What's a channel? Why is it important how do I send/recieve lock on it?
- get the RPC working - which one? Do I need to create an AppendEntries RPC?
- figure out how the service uses the Raft instance
- figure out how a term is started and leader is initially elected when the service creates a Raft cluster
- send messages between raft instances, when do we reply back to the client? When the entry has been acknowledged? or committed?

initial thoughts:

- make a raft instance
- reach consensus on log entry
- get current term and identity of leader
- commit a new entry to the log... each raft peer must send an applymsg to the leader, or service?
