## Initialization of the Raft cluster

Raft peers will be initialized and each will connect to the network

Each Raft peer will be initialized with a timeout - is this randomized? Or is the sleep and wakeup to check the randomized bit?

A peer will wakeup and start a leader election

## Leader election

how often will our leader send heartbeats
we don't know what our election timeout should be

we want to replicate election timeouts
election timeout
a node ascends to a candidate state and starts an election
conditions:

- the node keeps track of the last time it heard from the leader: variable
- also has a predefined interval of time that permits waiting for communication from the leader: variable
- 1: thread sleeps and does the election timeout computation
- we sleep and periodically wake up
- when we wakeup we check - what's the current time
- current time and when's the last time that we heard from the leader: value >= predefined interval of time then start the election
- 2: thread listens for the updates from the leader and updates the tolh variable

variables: tolh, predefined interval of time
actors: leader, follower

- leader can be a thread that periodically sends messages: infinite running thread
  - use a random function to send a message (how to send a message?)
- follower has 2 threads
  - 1: sleeping and waking up, calculating the election timeout (locks)
  - 2: listen for messages from the leader and update the tolh variable
