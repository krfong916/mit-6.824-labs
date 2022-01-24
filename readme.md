# Description

This repo contains work from MIT's Graduate Course on Distributed Systems taught by Professor Robert Morris, Spring 2020.

- Distributed key, value store that supports serializability
- Raft consensus algorithm

Future work:

- Modify the k,v store to support transactions with high-performance using a log-structured merge-tree
- Implement a client application to demonstrate proof-of-concept
- Draft experiential post of learnings

# Snapshotting Thoughts

time to time, the kv server persists snapshot

- need a long-running go-routine to detect the time to snapshot - log exceeds size?
- call persister api to persist
- need to calculate the correct sized snapshot and metadata to persist
- how to tell Raft to discard old log entries? and how to actually free that memory? we cannot have any references to those discarded log entries
- how to detect when a server restarts or falls behind the leader and deem it appropriate to send InstallSnapshot RPC?
- the interface to discard raft log entries, or take a snapshot should be rf.Snapshot()
- Raft will internally call persister.SaveRaftState()
