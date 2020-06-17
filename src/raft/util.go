package raft

import "log"

// Debugging
// 0: not print
// 1: print debug info
// 2: print debug info and lock change info
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrint(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Print(a...)
	}
	return
}
