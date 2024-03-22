package raft

import (
	"log"
	"time"
)

//
// Definition the Raft methods related to role Leader.
// Includes startLeader and sending heartBeats.
//

const MaxRetry int = 3

// Start the client use this function to send request to server.
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// votedFor. the third return value is true if this server believes it is
// the leader.
// "If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)"
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("Raft-%d, Start, cmd: %s, %s", rf.me, FirstFifteenChars(command), rf)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := false
	if rf.mode == leader {
		rf.logs.AppendCommand(rf.currentTerm, command)
		rf.persist()
		index = rf.logs.Len()
		term = rf.currentTerm
		isLeader = true
		rf.matchIndex[rf.me] = rf.logs.Len()
	}
	DPrintf("Raft-%d, Start, Finished, index: %d, term: %d, isLeader: %t, %s", rf.me, index, term, isLeader, rf)
	return index, term, isLeader
}

// startLeader Unlocked start or switch the Raft to leader mode.
// Unlocked method, so the called should be locked.
func (rf *Raft) startLeader() {
	DPrintf("Raft-%d, StartLeader start %s.", rf.me, rf)
	rf.mode = leader
	rf.lastHeartBeat = time.Now()
	if rf.voteFor != IntNil {
		rf.voteFor = IntNil
		rf.persist()
	}
	rf.voted = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ { // Set nextIndex to len(LogsSlice) + 1 because the startIndex of log is 1.
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.logs.Len() + 1
	}
	rf.tickerLeader()
	DPrintf("Raft-%d, StartLeader finished, %s.", rf.me, rf)
}

// broadcastHeartBeat Send heartbeat query to peers as a leader.
// Locked method.
func (rf *Raft) broadcastHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.mode == leader {
		DPrintf("Raft-%d, Broadcast Heartbeat start, %s.", rf.me, rf)
		for i := 0; i < len(rf.peers); i++ {
			idx := i
			if idx == rf.me { // skip self.
				continue
			}
			go rf.sendLogHelper(idx, true) // Use new routine to avoid block.
		}
		DPrintf("Raft-%d, Broadcast Heartbeat finished, %s.", rf.me, rf)
	}
}

// sendLogHelper help to send log.
// RLocked method.
func (rf *Raft) sendLogHelper(idx int, isHeartBeat bool) {
	DPrintf("Raft-%d, SendLog Helper, start, receiver: %d, %s.", rf.me, idx, rf)
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if rf.mode == leader {
		DPrintf("Raft-%d, SendLog Helper, is Leader, receiver: %d, isHB: %t, %s.", rf.me, idx, isHeartBeat, rf)
		if rf.nextIndex[idx] <= rf.logs.SnapshotIndex {
			go rf.leaderSendSnapShot(idx)
			return
		}
		nextIdx := rf.nextIndex[idx]
		prevLogIdx := nextIdx - 1
		var thisLogs []interface{}
		// "If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex"
		if !isHeartBeat { // Entries are not apply for heartbeat.
			rf.assembleLogs(&thisLogs, nextIdx, idx, isHeartBeat)
		}
		prevLogTerm, _ := rf.logs.GetTerm(prevLogIdx)
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIdx,
			PrevLogTerm: prevLogTerm, Entries: thisLogs, LeaderCommit: rf.commitIndex}
		reply := AppendEntriesReply{}
		go rf.sendLog(idx, isHeartBeat, &args, &reply) // Unlocked method.
	}
}

// assembleLogs assemble Logs for sending log to peers.
// Unlocked method, so the caller should hold at least RLock.
func (rf *Raft) assembleLogs(thisLogs *[]interface{}, nextIdx int, idx int, isHeartBeat bool) {
	if rf.logs.Len() < nextIdx {
		DPrintf("Raft-%d, SendLog Helper, stop for empty entries, receiver: %d, isHB: %t, %s.",
			rf.me, idx, isHeartBeat, rf)
		return
	}
	cutLogs, _ := rf.logs.GetMany(nextIdx)
	for _, logItem := range cutLogs {
		*thisLogs = append(*thisLogs, logItem)
	}
}

// sendLog send the log and deal with the return value.
// Will be locked when perform the return value. Cannot lock before received reply to avoid blocking.
func (rf *Raft) sendLog(idx int, isHeartBeat bool, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Raft-%d, Send Log, Start, to %d, Args: %s, %s.", rf.me, idx, args, rf)
	ok := rf.sendAppendEntries(idx, args, reply)
	DPrintf("Raft-%d, Send Log, Received from Raft-%d, %s, %s", rf.me, idx, reply, rf)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		DPrintf("Raft-%d, Send Log, Received OK from Raft-%d, %s.", rf.me, idx, rf)
		rf.leaderPerformReturnRpc(idx, isHeartBeat, args, reply)
	}
}

// leaderPerformReturnRpc deal with the return value of Log RPC.
// Unlocked method, so the caller should be a locked caller.
// 1 If votedFor is newer, switch to follower. otherwise:
// 2 Update the heartbeat.
// 3 Update the matchIndex and nextIndex if success.
// 4 Decrease the nextIndex and re-send the log recursively.
func (rf *Raft) leaderPerformReturnRpc(idx int, isHeartBeat bool, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term < rf.currentTerm {
		DPrintf("Raft-%d, PerformReturnLogRpc, From Raft-%d, Drop outdate msg, %s, %s", rf.me, idx, reply, rf)
		return
	}
	if reply.Term > rf.currentTerm {
		DPrintf("Raft-%d, PerformReturnLogRpc, From Raft-%d, Switch to follower, %s, %s", rf.me, idx, reply, rf)
		rf.startFollower(reply.Term, IntNil, false) // Switch to follower when votedFor is newer.
		return
	}
	if reply.Success {
		rf.leaderPerformSuccessReturnLogRpc(idx, args)
	} else {
		rf.leaderPerformFailedReturnLogRpc(idx, isHeartBeat, args, reply)
	}
}

// leaderPerformSuccessReturnLogPrc perform return a successful rpc message.
// Unlocked method, so the caller should be a locked caller.
func (rf *Raft) leaderPerformSuccessReturnLogRpc(idx int, args *AppendEntriesArgs) {
	// “If successful: update nextIndex and matchIndex for follower (§5.3)”
	lastIdx := args.PrevLogIndex + len(args.Entries)
	if rf.matchIndex[idx] < lastIdx {
		rf.matchIndex[idx] = lastIdx
		rf.nextIndex[idx] = rf.matchIndex[idx] + 1
	}
	DPrintf("Raft-%d, LeaderPerformReturnLogRpc from Raft-%d, finished. args:%s, %s.", rf.me, idx, args, rf)
}

// leaderPerformFailedReturnLogPrc perform return a failed rpc message.
// Unlocked method, so the caller should be a locked caller.
func (rf *Raft) leaderPerformFailedReturnLogRpc(idx int, isHeartBeat bool,
	args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// “If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)”
	if rf.nextIndex[idx] == args.PrevLogIndex+1 {
		if reply.ConflictTerm == IntNil && reply.ConflictIndex > IntNil { // for index not matched.
			rf.nextIndex[idx] = reply.ConflictIndex
			DPrintf("Raft-%d, LeaderPerformFailedReturnLogRpc, Idx not matched from Raft-%d, Updated. "+
				"args: %s, %s.", rf.me, idx, args, rf)
		} else if reply.ConflictTerm > IntNil { // for term not matched.
			rf.leaderPerformUnmatchedTerm(idx, args, reply)
		} else {
			return
		}
		if rf.nextIndex[idx] < 1 {
			log.Fatal("nextIdx is < 1")
		}
		DPrintf("Raft-%d, LeaderPerformFailedReturnLogRpc, Refused from Raft-%d, Updated. args: %s, %s.",
			rf.me, idx, args, rf)
	} else {
		DPrintf("Raft-%d, LeaderPerformFailedReturnLogRpc, Outdated batchid from Raft-%d, "+
			"Updated. args: %s, %s.", rf.me, idx, args, rf)
	}
}

// leaderPerformUnmatchedTerm when term not matched.
// Unlocked Method, so the caller should hold the Lock.
func (rf *Raft) leaderPerformUnmatchedTerm(idx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	matchingTermLogIdx, err := rf.logs.FindLatestMatchingTermLog(args.PrevLogIndex, reply.ConflictTerm)
	if err == nil && matchingTermLogIdx > 1 {
		rf.nextIndex[idx] = matchingTermLogIdx
		DPrintf("Raft-%d, LeaderPerformFailedReturnLogRpc, Found Term not matched from Raft-%d, "+
			"Updated. args: %s, %s.", rf.me, idx, args, rf)
	} else {
		rf.nextIndex[idx] = reply.ConflictIndex
		DPrintf("Raft-%d, LeaderPerformFailedReturnLogRpc, Not Found Term not matched from Raft-%d, "+
			"Updated. args: %s, %s.", rf.me, idx, args, rf)
	}
}
