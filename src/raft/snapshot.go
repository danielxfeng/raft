package raft

import "fmt"

//
// Definition of methods of Snapshot.
//

type SnapshotApplyStatus int

const (
	Done = iota
	Pending
)

var snapshotApplyStatusArray = []string{"Done", "Pending"}

func (sas SnapshotApplyStatus) String() string {
	return snapshotApplyStatusArray[sas]
}

// InstallSnapShotArgs the struct of InstallSnapshot RPC arguments.
type InstallSnapShotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

func (isa *InstallSnapShotArgs) String() string {
	return fmt.Sprintf("Args: Term: %d, Leaderid: %d, LastIndex: %d, LastTerm: %d",
		isa.Term, isa.LeaderId, isa.LastIncludedIndex, isa.LastIncludedTerm)
}

// InstallSnapShotReply the struct of InstallSnapshot RPC reply.
type InstallSnapShotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (isr *InstallSnapShotReply) String() string {
	return fmt.Sprintf("Reply: Term: %d", isr.Term)
}

// Snapshot leader installs snapshot from client.
// Locked method.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := rf.logs.ApplySnapshot(index, IntNil, rf.me)
	if ok {
		rf.snapshot = snapshot
		rf.persist()
		DPrintf("Raft-%d, Snap OK, end, %s", rf.me, rf)
	}
	DPrintf("Raft-%d, Snap Failed, end, %s", rf.me, rf)
}

// InstallSnapshot RPC of InstallSnapshot.
// Locked method.
func (rf *Raft) InstallSnapshot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { // "Reply immediately if term < currentTerm"
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.startFollower(args.Term, IntNil, true)
	}
	reply.Term = rf.currentTerm
	// Ignored the rules of
	// "Create new snapshot file if first chunk (offset is 0),
	// Reply and wait for more data chunks if done is false,"
	// because "Send the entire snapshot in a single InstallSnapshot RPC.
	// Don't implement Figure 13's offset mechanism for splitting up the snapshot."
	ok := rf.logs.ApplySnapshot(args.LastIncludedIndex, args.LastIncludedTerm, rf.me)
	if ok {
		rf.snapshot = args.Data // "Save snapshot file, discard any existing or partial snapshot with a smaller index"
		rf.persist()
		// "Reset state machine using snapshot contents (and load snapshot’s cluster configuration)"
		rf.snapshotApplyStatus = Pending
		if args.LastIncludedIndex > rf.commitIndex {
			rf.commitIndex = args.LastIncludedIndex
		}
		DPrintf("Raft-%d, InstallSnap OK, end, %s", rf.me, rf)
	}
	DPrintf("Raft-%d, InstallSnap Failed, end, %s", rf.me, rf)
}

// leaderSendSnapShot Unlocked method. So the caller should be Locked.
func (rf *Raft) leaderSendSnapShot(idx int) {
	args := rf.assembleInstallSnapShotArgs()
	reply := InstallSnapShotReply{}
	DPrintf("Raft-%d, leaderSendSnapShot to %d, args: %v, %s ", rf.me, idx, args, rf)
	ok := rf.peers[idx].Call("Raft.InstallSnapshot", args, &reply)
	if ok {
		DPrintf("Raft-%d, leaderSendSnapShot received from %d, args: %s, reply: %s, %s ",
			rf.me, idx, args, &reply, rf)
		rf.leaderPerformSnapShotReturnRPC(idx, args, &reply)
	}
}

// assembleInstallSnapShotArgs RLocked method.
func (rf *Raft) assembleInstallSnapShotArgs() *InstallSnapShotArgs {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return &InstallSnapShotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.logs.SnapshotIndex,
		LastIncludedTerm: rf.logs.SnapshotTerm, Data: rf.snapshot}
}

// leaderPerformSnapShotReturnPRC Locked method.
func (rf *Raft) leaderPerformSnapShotReturnRPC(idx int, args *InstallSnapShotArgs, isr *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if isr.Term > rf.currentTerm {
		rf.startFollower(isr.Term, IntNil, true)
	} else if isr.Term == rf.currentTerm {
		rf.matchIndex[idx] = args.LastIncludedIndex
		rf.nextIndex[idx] = rf.matchIndex[idx] + 1
	}
}
