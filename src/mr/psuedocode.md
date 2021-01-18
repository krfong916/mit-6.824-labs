# psuedocode

Roles:

- master
- responsibility: coordination
- structure to maintain task status
  - the status of the task
  - status: un_processed, map, reduce
- every so often, the master pings workers to check liveness. If there is no response, then the worker is declared dead
- if the worker reports its liveness/completion of a task, the master verifies their message with it's state. If the task and master's log do not match, the master disregards their message

```
we are given a set of files to apply MapReduce computation to
- we want to know how many map tasks we have left and which ones
- we want to know how many reduce tasks we have left and which ones

O(m*r)


two sets of data structures?

```

- worker: map

  - responsibility: apply the map function
  - the worker reads a file and applies a map function to it
  - every so often, the worker writes the in-memory data to a file in chunks
  - when the worker is completed with the task, the file is made permanent
  - the worker notifies the master via RPC that the region has been written and location of the file so a reduce worker can fetch its contents

- worker: reduce
  - a reduce worker writes to the distributed file system
  - reduce workers are told the location of the file. The reduce worker _can_ read the file in chunks over the network.

When each region is written, the master is informed. This allows the master to assign tasks to idle threads that can be used to execute a reduce task

The more Map tasks there are, the more effort is made by the Master to keep track of the state of computation. Since the Master needs to manage each Map and each Reduce, it must make O(M+R) schedulign decisions. And, since there is state associated with each Map-Reduce pair, it must maintain O(M\*R) state information.

### 10/15

- [x] write a common file
- [x] send intermediate files and the map_task_id to master
      have master
      remove map_task_id from the slice
      insert reduce task in the list, along with the list of files
- [x] check code into github

synchronous reduce task
after all map have been completed
test reduce... use test shell script to see what it does instead of manually having to reduce
given the list of files to reduce
open file
read contents and sort
put contents in the reduce file

Still confused about the temp file!

then race conditions?
locking on shared data

then crashes
profile how long a map task will take
need to assign each RPC request a unique identifier and timestamp
this will help with crashing

Make it fast/efficient now, and cleanup

Write post-mortem

when you are at scale, the goal is to reconstruct what happened
need great logs that are descriptive, provide sufficient information
tracing - assign each request a unique identifier and timestamp
when something goes wrong - reconcile the request with the logs

Nice to have:
write intermediate file to temp folder - is this a race condition? check if folder exists... parallel threads? explore further
