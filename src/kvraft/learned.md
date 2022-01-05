In our single process, we want read and write safety.
We are accessing and modifying memory at the same time, therefore
We cannot establish a happens before relationship between two events, a and b
meaning, we cannot establish an order between a and b
We are going to modify our code so that we can establish a happens-before relationship, and achieve an ordering of events for our process.

In other words, we are demonstrating how to handle concurrency of shared memory.

We use channels for synchronizing threads of execution via message passing, and mutexes for synchronizing state and caching.

We are building a key value service that demonstrates linearizability - the strongest model of consistency for single objects. Linearizability implies operations take place atomically, and in order. We demonstrate that if an operation A completes before oepration B, operation B observes the result of operation A. With respect to this definition of linearizability, we can describe how we handle concurrent requests. If we say x is concurrent with y, that doesn't mean x and y are simultaneous, it means that we cannot determine what happened first - x or y. Our server code must determine how to order x and y. It may happen that y comes before x, or the other way around. Whatever the result, as long as we can establish a single order of execution amongst concurrent requests - then we can produce a linearizable history.

Linearizability can help simplify the programming model and complexity for application programmers if there only exists one state of an object at one time.
Though we may use a number of servers in order to increase the number of read ops., with multiple processes, we need to use a consensus algorithm for replication and fault tolerance to maintain the appearance of a single process.
Raft is a consensus algorithm that provides guarantees ordering of events, we can use the algorithm as a module that we can layer service code on top of.
