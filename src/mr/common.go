package mr

import "log"

type taskType string
const (
	MapTask taskType = "MAP"
	ReduceTask = "REDUCE"
	ExitTask = "EXIT"
)

func check(err error) {
  if err != nil {
	  log.Fatalf("error: %v", err)
	}
}