package raft

import (
	"time"
)

//
// Definition the Raft methods related to role Follower.
//

// startFollower start or switch the Raft to follower mode.
// Unlocked mode, so the caller should be locked.
func (rf *Raft) startFollower(term int, voteFor int, updateHb bool) {
	DPrintf("Raft-%d, StartFollower, start %s.", rf.me, rf)
	rf.mode = follower
	if term != rf.currentTerm || voteFor != rf.voteFor {
		rf.currentTerm = term
		rf.voteFor = voteFor
		rf.persist()
	}
	if updateHb {
		rf.lastHeartBeat = time.Now()
	}
	rf.voted = 0
	DPrintf("Raft-%d, StartFollower End, %s.", rf.me, rf)
}

// followerPerformAppendEntries perform append entries from leader as a follower.
// Unlocked method, so the caller should be locked.
func (rf *Raft) followerPerformAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Raft-%d, Follower Perform AppendEntries, Start, %s, %s", rf.me, args, rf)
	rf.lastHeartBeat = time.Now()
	matchedLogTerm, err := rf.logs.GetTerm(args.PrevLogIndex) // Get will return nil when PrevLogIndex is outbound.
	if err != nil {                                           // if matchedLog is not found
		rf.performAppendEntriesUnmatchedIdx(args, reply)
	} else if matchedLogTerm != args.PrevLogTerm { // if matchedLog's term is not matched
		rf.performAppendEntriesUnmatchedTerm(args, reply, matchedLogTerm)
	} else {
		rf.performAppendEntriesTrue(args, reply)
	}
}

// performAppendEntriesUnmatchedIdx set ConflictIndex if matchedLog is not found.
// Unlocked method.
// “Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)”
// *** Optimization of returning conflictIdx and conflictTerm.
func (rf *Raft) performAppendEntriesUnmatchedIdx(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	reply.ConflictIndex = rf.logs.Len() + 1 // set ConflictIndex to last term of LogsSlice
	DPrintf("Raft-%d, Follower Perform AppendEntries, Not Matched Idx, %s, %s, %s", rf.me, args, reply, rf)
}

// performAppendEntriesUnmatchedTerm set ConflictIndex and ConflictTerm if term is not matched.
// Unlocked method, but will call RLocked method.
// “Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)”
// *** Optimization of returning conflictIdx and conflictTerm.
func (rf *Raft) performAppendEntriesUnmatchedTerm(
	args *AppendEntriesArgs, reply *AppendEntriesReply, matchedLogTerm int) {
	reply.Success = false
	reply.ConflictTerm = matchedLogTerm
	reply.ConflictIndex, _ = rf.logs.FindLatestMatchingTermLog(args.PrevLogIndex, reply.ConflictTerm)
	DPrintf("Raft-%d, Follower Perform AppendEntries, Not Matched Term, %s, %s, %s", rf.me, args, reply, rf)
}

// performAppendEntriesTrue reply true.
// Unlocked method, so the caller should be locked.
func (rf *Raft) performAppendEntriesTrue(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Raft-%d, Follower Perform AppendEntries, checked, %s, %s", rf.me, args, rf)
	if len(args.Entries) > 0 { // For Logs
		// "3. If an existing entry conflicts with a new one (same index but different terms), delete
		// the existing entry and all that follow it (§5.3)"
		// "4. Append any new entries not already in the log"
		rf.logs.AppendLogs(args.PrevLogIndex+1, &args.Entries)
		rf.persist()
	}
	rf.followerPerformCommit(args)
	reply.Success = true
	DPrintf("Raft-%d, Follower Perform AppendEntries, Return true, %s, %s", rf.me, args, rf)
}

// followerPerformCommit perform commit.
// Unlocked method, so the caller should be locked.
func (rf *Raft) followerPerformCommit(args *AppendEntriesArgs) {
	if args.LeaderCommit > rf.commitIndex {
		// "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)"
		lastNewIdx := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit > lastNewIdx {
			rf.commitIndex = lastNewIdx
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		DPrintf("Raft-%d, FollowerPerformCommit, finished, %s, %s.", rf.me, args, rf)
	}
}
