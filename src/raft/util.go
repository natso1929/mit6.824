package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func (rf *Raft) dlog(format string, args ...interface{}) {
	if !Debug {
		format = fmt.Sprintf("[%d] ", rf.me) + format
		log.Printf(format, args...)
	}
}
