in our single process, we want read and write safety
we are accessing and modifying memory at the same time, therefore
we cannot establish a happens before relationship between two events, a and b
meaning, we cannot establish an order between a and b
We are going to modify our code so that we can establish a happens-before relationship, and achieve an ordering of events for our process

in other words, we are demonstrating how to handle concurrency with shared variables

in multiple processes, we need to use a consensus algorithm for replication and fault tolerance
raft is a consensus algorithm that provides guarantees ordering of events, we can use the algorithm as a module that we can layer service code on top of
