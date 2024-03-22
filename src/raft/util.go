package raft

import (
	"fmt"
	"log"
	"time"
)

// Debug Debug mode switch.
const Debug = true

// DPrintf a log printer.
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		currentTime := time.Now().Format("2006-01-02 15:04:05.000")
		log.Printf(currentTime+" "+format, a...)
	}
	return 0, nil
}

// FirstFifteenChars Return first 15 string the given string if applied.
func FirstFifteenChars(v interface{}) string {
	s := fmt.Sprintf("%s", v)
	if len(s) < 15 {
		return s
	}
	return s[:15] + "..."
}
