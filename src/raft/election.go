package raft

import (
	"time"
)

//
// Definition Raft methods related to Election.
// Includes struct of Args, Reply of RequestVote PRC, and it's handler function.
// And the logic about candidate, includes starting candidate, sending request vote messages.
//

// RequestVoteArgs RequestVote RPCServer arguments structure.
type RequestVoteArgs struct {
	Term         int // candidate’s votedFor
	CandidateId  int // candidate requesting vote
	LastLogIndex int // Index of candidate’s last log entry
	LastLogTerm  int // votedFor of candidate’s last log entry
}

func (args *RequestVoteArgs) String() string {
	return "" /* fmt.Sprintf("RequestVoteArgs: Term: %d, CandidateId: %d, LastLogIndex: %d, LastLogTerm: %d",
	args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm) */
}

// RequestVoteReply RequestVote RPCServer reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm
	VoteGranted bool // true means candidate received vote
}

func (reply RequestVoteReply) String() string {
	return "" /* fmt.Sprintf("RequestVoteReply: Term: %d, VoteGranted: %t", reply.Term, reply.VoteGranted) */
}

// RequestVote RPCServer handler.
// Locked method.
// 1 Reply false when term is outdated.
// 2 Turn to follower mode when the term is newer.
// 3 Set reply to true meet certain conditions, set to false for other situations.
// 4 Update lastHeartBeat and term of reply.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Raft-%d, RequestVote RPC, Start, %s, %s", rf.me, args, rf)
	if args.Term < rf.currentTerm { // "Reply false if term < currentTerm (§5.1)"
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// "If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)"
	if args.Term > rf.currentTerm {
		DPrintf("Raft-%d, RequestVote RPC, Turn to follower for newer term, %s, %s, %s.", rf.me, args, reply, rf)
		rf.startFollower(args.Term, IntNil, false) // Unlocked method.
	}
	rf.assembleRvReply(args, reply)
	DPrintf("Raft-%d, RequestVote, Finished, %s, %s.", rf.me, reply, rf)
}

// assembleRvReply assemble RequestVote reply after term check.
// Unlocked method, so the caller should hold the Lock.
func (rf *Raft) assembleRvReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	// "Respond to RPCs from candidates and leaders"
	if rf.isTrueRequestVote(args) { // Unlocked method.
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.lastHeartBeat = time.Now()
		rf.persist() // Unlocked method
	} else {
		reply.VoteGranted = false
	}
}

// isTrueRequestVote help determining if the reply of RequestVote should be true.
// Unlocked method but the caller should be locked.
// Checkpoints includes: 1 same terms, 2 has not voted, 3 mode is not leader,
// 4 lastTerm is smaller || same term but lastIdx is not greater.
func (rf *Raft) isTrueRequestVote(args *RequestVoteArgs) bool {
	lastTerm, lastIdx := rf.logs.GetLastLogInfo()
	// "If votedFor is null or candidateId, and candidate’s log is
	// at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)"
	return args.Term == rf.currentTerm &&
		(rf.voteFor == IntNil || rf.voteFor == args.CandidateId) &&
		rf.mode != leader &&
		(lastTerm < args.LastLogTerm ||
			(lastTerm == args.LastLogTerm &&
				lastIdx <= args.LastLogIndex))
}

// startCandidate start Candidate mode.
// Locked method.
// "On conversion to candidate, start election: Increment currentTerm, Vote for self, Reset election timer,
// Send RequestVote RPCs to all other servers"
func (rf *Raft) startCandidate() {
	DPrintf("Raft-%d, Start Candidate, Start, %s.", rf.me, rf)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.mode = candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.persist()
	rf.voted = 1
	rf.lastHeartBeat = time.Now()
	term := rf.currentTerm
	lastTerm, lastIdx := rf.logs.GetLastLogInfo()
	go rf.broadcastRequestVotes(term, lastTerm, lastIdx) // Unlocked method.
	DPrintf("Raft-%d, Start Candidate, Finished, %s.", rf.me, rf)
}

// broadcastRequestVotes send request votes to all peers.
// Unlocked method. But will call Locked method when process the return value from RPC.
func (rf *Raft) broadcastRequestVotes(term int, lastTerm int, lastIdx int) {
	DPrintf("Raft-%d, Send Request Votes, Start, lastTerm: %d, lastIdx: %d, %s.", rf.me, lastTerm, lastIdx, rf)
	args := RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogTerm: lastTerm, LastLogIndex: lastIdx}
	for i := 0; i < len(rf.peers); i++ { // send RequestVote RPC to everyone except itself.
		idx := i
		if idx == rf.me {
			continue
		}
		go rf.sendRequestVoteHelper(&args, idx)
	}
	DPrintf("Raft-%d, Send Request Votes Finished, %s.", rf.me, rf)
}

// sendRequestVoteHelper send and perform the return value of request vote.
// Unlocked method. But will call Locked method when process the return value from RPC.
func (rf *Raft) sendRequestVoteHelper(args *RequestVoteArgs, idx int) {
	DPrintf("Raft-%d, Send RVH, Start, to %d, %s, %s.", rf.me, idx, args, rf)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(idx, args, &reply)
	DPrintf("Raft-%d, Send RVH, Received from Raft-%d, %s, %s.", rf.me, idx, reply, rf)
	if ok {
		rf.performRequestVoteReply(&reply)
	}
}

// performRequestVoteReply perform the return value of request vote.
// Locked method.
// 1. Start follower when received a newer votedFor.
// 2. When a.terms are same, and b.in candidate mode, and c.replay is true:
// 2.1 Update the count.
// 2.2 Switch to leader when the agree rate > 50%.
func (rf *Raft) performRequestVoteReply(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Raft-%d, PerformRequestVoteReply, start, %s, %s.", rf.me, reply, rf)
	if reply.Term > rf.currentTerm {
		DPrintf("Raft-%d, PerformRequestVoteReply, start follower for newer term, %s.", rf.me, rf)
		rf.startFollower(reply.Term, IntNil, false) // Unlocked method
	}
	if reply.VoteGranted && reply.Term == rf.currentTerm && rf.mode == candidate {
		rf.voted++
		DPrintf("Raft-%d, PerformRequestVoteReply, valid vote, %s.", rf.me, rf)
		if len(rf.peers) < rf.voted*2 { // “If votes received from majority of servers: become leader”
			DPrintf("Raft-%d, PerformRequestVoteReply, start leader, %s.", rf.me, rf)
			rf.startLeader() // Unlocked method
		}
	}
}

// sendRequestVote send RequestVote RPC message.
// Unlocked method. Cannot be locked to avoid blocking.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
