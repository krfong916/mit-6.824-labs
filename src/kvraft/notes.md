We are creating a key value store that implements linearizability
The key, value store supports the following operations
put(key, value)
get(key)
append(key, arg)

The clerk will implement these operations and interact with the Raft servers on our behalf. 
If the user calls put on a key that already exists, the value is appended
If the user calls append on a key that doesn't exist, the value is put

Linearizability
- As written by Peter Bailis 'Linearizability is a guarantee about single operations on single objects'
- Linearizability is the ordering of requests where their operation happens sometime between their invocation and their its response
- single object semantics: specify effects of an operation and the order that they occur
- In production deployments, coordination makes linearizability possible

If a single object is linearizable, a

Think about a single thread of execution - executing a single operation
locks as a synchronization point of concurrent operations (threads) - requests are made visible to other threads
and each request has its own linearization point

"Operations that must wait for some other thread to establish a precondition" - concurrent data structures


https://www.cs.rochester.edu/u/scott/papers/2004_DISC_dual_DS.pdf
http://www.bailis.org/blog/linearizability-versus-serializability/

# Protocol Definition
## Name
Raft Consensus Algorithm
## Messages

## Roles
### Client
- sends requests to the Clerk

### Clerk
- makes requests to the Server on behalf of the Client
- reveals interface to interact with the server
  + get()
  + put()
- responsible for responding to the Client with result of the request
- responsible for sending request to the Server that is established as the current Raft Leader

### Server
- communicates with the Raft module and performs operations on the log
- implements linearizable semantics, handles failures under the hood
- in-memory key, value store


### Raft Module
- The big boy

## Challenges
### Overall
- Where is the point of serialization? Is that the way to think about this problem?
- What are the concurrent operations?
  + What makes this program a challenge is defining a single, total order of executions across replicas. aqzzaRequests from clients to servers can be made concurrently, 
- How should we expect our servers and clients to handle failures
  + what states can we put them in?
  + More importantly what can we do to prove the protocol and behavior is correct?

### Clerk
- retry on different servers
- field requests on another thread 
- respond to servers

### Server
- How do we implement linearizable semantics?
- How do we recover from failure?