package raft

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

//
// A ticker to perform background tasks as a raft peer.
//

const (
	TickerInterval     time.Duration = 5 * time.Millisecond   // The interval of ApplyCh.
	HeartBeatInterval                = 100 * time.Millisecond // The interval of HeartBeats.
	MinElectionTimeout               = 250 * time.Millisecond // The minimal timeout of a election.
	MaxElectionTimeout               = 400 * time.Millisecond // The maximum timeout of a election.
)

// ApplyMsg the message that transferred from a Raft peer to status machine.
// Set CommandValid to true to deliver a log.
// Set to false for other uses such as snapshots.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// For snapshots:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (am ApplyMsg) String() string {
	return fmt.Sprintf("CommandValid: %t, Id: %s, CommandIdx: %d, SnapValid: %t, SnapTerm: %d, SnapIdx: %d",
		am.CommandValid, FirstFifteenChars(am.Command), am.CommandIndex,
		am.SnapshotValid, am.SnapshotTerm, am.SnapshotIndex)
}

// ticker background tasks for all server.
func (rf *Raft) ticker() {
	go rf.tickerElections()
	go rf.tickerApplyCh()
}

// tickerLeader background tasks for leader.
func (rf *Raft) tickerLeader() {
	go rf.tickerHeartBeats()
	go rf.tickerLeaderCommit()
	go rf.tickerSendLogs()
}

// tickerApplyCh send logs to ApplyCh.
// Unlocked method. but may call RLocked method.
// Non-concurrency sending to ApplyCh to keep linearizability without lock.
func (rf *Raft) tickerApplyCh() {
	for !rf.killed() {
		messages := rf.generateApplyChLogs() // RLocked method.
		for _, msg := range messages {
			rf.applyCh <- msg
		}
		time.Sleep(TickerInterval)
	}
}

// generateApplyChLogs return the slice of ApplyMsg that need to be applied to status machine.
// RLocked method. Can NOT concurrency call this method
// since updating the lastApplied & snapshotApplyStatus is without Lock.
// Non-concurrency sending to ApplyCh to keep linearizability without lock.
func (rf *Raft) generateApplyChLogs() []ApplyMsg {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	messages := make([]ApplyMsg, 0)
	if rf.snapshotApplyStatus == Pending {
		rf.generateSnapShotApplyChLogs(&messages)
	}
	if rf.snapshotApplyStatus == Done && rf.commitIndex > rf.lastApplied {
		DPrintf("Raft-%d, ApplyToStatusMachine, Start, %s.", rf.me, rf)
		// "If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)"
		rf.generateLogsApplyChLogs(&messages)
	}
	return messages
}

// generateSnapShotApplyChLogs check and add snapshot to message to ApplyCh.
// Unlocked method, so the caller should hold at least the RLock.
func (rf *Raft) generateSnapShotApplyChLogs(messages *[]ApplyMsg) {
	DPrintf("Raft-%d, ApplySnapshotToStatusMachine, Start, %s.", rf.me, rf)
	msg := ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot,
		SnapshotTerm: rf.logs.SnapshotTerm, SnapshotIndex: rf.logs.SnapshotIndex}
	*messages = append(*messages, msg)
	rf.snapshotApplyStatus = Done
	if rf.lastApplied < rf.logs.SnapshotIndex {
		rf.lastApplied = rf.logs.SnapshotIndex
	}
	DPrintf("Raft-%d, ApplySnapshotToStatusMachine, Done, %s.", rf.me, rf)
}

// generateLogsApplyChLogs check and add Logs to message to ApplyCh.
// Unlocked method, so the caller should hold at least the RLock.
func (rf *Raft) generateLogsApplyChLogs(messages *[]ApplyMsg) {
	for i := rf.lastApplied; i < rf.commitIndex; i++ {
		if i >= rf.logs.SnapshotIndex {
			log, _ := rf.logs.Get(i + 1)
			DPrintf("Raft-%d, ApplyToStatusMachine, log is: %s, %s.", rf.me, log, rf)
			applyMsg := ApplyMsg{CommandValid: true, Command: log.Command, CommandIndex: i + 1}
			*messages = append(*messages, applyMsg)
		}
	}
	rf.lastApplied = rf.commitIndex
}

// tickerElections launch election when timeout.
// Unlocked Method, but may call Locked method.
// "If election timeout elapses without receiving AppendEntries RPC
// from current leader or granting vote to candidate: convert to candidate"
// "If election timeout elapses: start new election"
func (rf *Raft) tickerElections() {
	for !rf.killed() {
		if rf.mode != leader {
			timeSinceHeartBeat := time.Since(rf.lastHeartBeat)
			DPrintf("Raft-%d, Ticker Election, timeSinceHB: %s, %s", rf.me, timeSinceHeartBeat, rf)
			if timeSinceHeartBeat > MinElectionTimeout { // Election timeout detector.
				DPrintf("Raft-%d, Ticker, Election timeout, timeSinceHB: %s, %s", rf.me, timeSinceHeartBeat, rf)
				go rf.startCandidate() // Locked method
			}
		}
		time.Sleep(generateRandTimeout())
	}
}

// tickerHeartBeats runs the background Log tasks for every TickerInterval.
// Unlocked method.
func (rf *Raft) tickerHeartBeats() {
	for !rf.killed() && rf.mode == leader {
		DPrintf("Raft-%d, Ticker, Heartbeat sender task. %s", rf.me, rf)
		go rf.broadcastHeartBeat() // Unlocked method
		time.Sleep(HeartBeatInterval)
	}
}

// tickerSendLogs set committed index for leader.
func (rf *Raft) tickerSendLogs() {
	for !rf.killed() && rf.mode == leader {
		rf.sendLogs()
		time.Sleep(TickerInterval)
	}
}

// sendLogs send logs to peer when necessary.
// Unlocked method.
func (rf *Raft) sendLogs() {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	for i, nextIdx := range rf.nextIndex {
		if i != rf.me && rf.logs.Len() >= nextIdx {
			go rf.sendLogHelper(i, false)
		}
	}
}

// tickerLeaderCommit set committed index for leader.
func (rf *Raft) tickerLeaderCommit() {
	for !rf.killed() && rf.mode == leader {
		rf.setCommittedIdx()
		time.Sleep(TickerInterval)
	}
}

// setCommittedIdx set committed index by matchIndex.
// RLocked method.
// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
func (rf *Raft) setCommittedIdx() {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	majority := findMajorityIdx(rf.matchIndex)
	DPrintf("Raft-%d, Ticker, Committed idx, get majority, %d. %s", rf.me, majority, rf)
	committedLog, _ := rf.logs.Get(majority)
	if rf.commitIndex < majority && (majority == rf.logs.SnapshotIndex || committedLog.Term == rf.currentTerm) {
		rf.commitIndex = majority
		DPrintf("Raft-%d, Ticker, Committed idx set. %s", rf.me, rf)
	}
}

// findMajorityIdx return the majority number of a list.
func findMajorityIdx(list []int) int {
	cp := make([]int, len(list))
	copy(cp, list)
	sort.Ints(cp)
	DPrintf("Ticker, Committed idx, get majority, list: %v, sorted: %v", list, cp)
	return cp[len(cp)/2]
}

// generateRandTimeout return a random time.duration between MinElectionTimeout and MaxElectionTimeout.
func generateRandTimeout() time.Duration {
	return time.Duration(rand.New(rand.NewSource(time.Now().UnixNano())).
		Int63n(int64(MaxElectionTimeout-MinElectionTimeout+1))) + MinElectionTimeout
}
