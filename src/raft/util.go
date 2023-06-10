package raft

import "log"

// Debugging
const Debug_level = 10000

func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	if Debug_level <= level {
		log.Printf(format, a...)
	}
	return
}
