package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func minOf(args ...int) int {
	min := args[0]
	for _, i := range args {
		if i < min {
			min = i
		}
	}
	return min
}
