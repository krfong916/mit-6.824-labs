package main

import "sync"

// This is a bad example for sending RPCs in parallel
// in a for loop, we spawn a number of goroutines
// we create an closure
// but we do not explicitly pass the value of i

// i refers to the outer scope
// we mutate the identifier i

// by the time we send an rpc and here back from its call
// the for loop has already incremented i
// This will result in non-determinism

// What's the meaning of go func() ?
// it spawns a bunch of threads in a loop
// it sends rpc in parallel - clean way to express this idea

// for our Raft lab - i may be the index of the follower we're trying to send an rpc to
func main() {
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			sendRPC(i)
			wg.Done()
		}()
	}
	wg.Wait()
}

func sendRPC(i int) {
	println(i)
}
