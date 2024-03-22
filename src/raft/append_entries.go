package raft

import (
	"fmt"
	"time"
)

//
// Definition RPC Append Entries.
// Includes struct of Args and Reply, and the handle function.
//

// AppendEntriesArgs struct of AppendEntriesArgs.
type AppendEntriesArgs struct {
	Term         int           // Leader's votedFor
	LeaderId     int           // Leader's id
	PrevLogIndex int           // Index of LogEntry immediately preceding new ones
	PrevLogTerm  int           // Term of PrevLogIndex entry
	Entries      []interface{} // Logs to store, empty for heartbeat
	LeaderCommit int           // Leader’s CommitIndex
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("AppendEntriesArgs: Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, "+
		"LeaderCommit: %d, len(E): %d, LogsSlice: %s", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
		args.LeaderCommit, len(args.Entries), "") //args.Entries) Will not print entries by default.
}

// AppendEntriesReply struct of AppendEntriesReply.
type AppendEntriesReply struct {
	Term          int  // Current votedFor
	Success       bool // True if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int  // Return conflict term when PrevLogTerm is not match,  only applied when Success is false.
	ConflictIndex int  // Return conflict idx when PrevLogIndex is not found, only applied when Success is false.
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("AppendEntriesReply: Term: %d, Success: %t, ConflictTerm: %d, ConflictIndex: %d.",
		reply.Term, reply.Success, reply.ConflictTerm, reply.ConflictIndex)
}

// AppendEntries RPCServer handler.
// Locked method.
// 1 Switch to follower when received a newer votedFor.
// 2 Reply false when args.VoteFor is outdated.
// 3 Transfer to the handler of each mode.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Raft-%d, RPC AppendEntries Start, args: %s, %s", rf.me, args, rf)
	reply.ConflictTerm = IntNil
	reply.ConflictIndex = IntNil
	if args.Term < rf.currentTerm { // “Reply false if votedFor < currentTerm”
		DPrintf("Raft-%d, RPC AppendEntries, Return outdated term msg. args: %s, reply: %s", rf.me, args, reply)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// “If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)”
	if args.Term > rf.currentTerm { // Re-run start follower when meet greater votedFor.
		rf.startFollower(args.Term, IntNil, true) // Unlocked method
	}
	reply.Term = rf.currentTerm
	switch rf.mode {
	case leader:
		reply.Success = false // The leader will not accept a message from other leader.
	case candidate: // If AppendEntries RPC received from new leader: convert to follower
		rf.startFollower(args.Term, IntNil, true) // Unlocked method
		fallthrough
	case follower: // “Respond to RPCs from candidates and leaders”
		rf.followerPerformAppendEntries(args, reply) // Unlocked method
	}
	DPrintf("Raft-%d, RPC AppendEntries, replied. args: %s, reply: %s, %s", rf.me, args, reply, rf)
}

// sendAppendEntries a helper function to send AppendEntries to the given peer.
// Unlocked method.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	done := make(chan bool, 1)
	go func() {
		done <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(HeartBeatInterval):
		return false
	}
}
