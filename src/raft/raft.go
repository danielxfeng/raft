package raft

//
// Definition of Raft peer. Main entry point of this package.
//

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const IntNil = -1 // respects nil

// ServerMode the working mode of a server. // stage: 2A
type ServerMode int

// stage:
const (
	follower ServerMode = iota
	leader
	candidate
)

var serverModeArray = []string{"follower", "leader", "candidate"}

func (sm ServerMode) String() string {
	return serverModeArray[sm]
}

// Raft a Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPCServer end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Persistent state on all servers:
	currentTerm int    // Initialized to 0, and the 1st votedFor is 1.
	logs        *Logs  // Logs.
	voteFor     int    // The voted leader of this term. Valid only in follower mode.
	snapshot    []byte // the snapshot of the server.
	// Volatile state on all servers:
	applyCh             chan ApplyMsg       // The channel of ApplyMsg.
	lastHeartBeat       time.Time           // The timestamp of the last valid heart beat.
	commitIndex         int                 // Index of highest log entry known to be committed.
	lastApplied         int                 // Index of highest log entry applied to state machine
	voted               int                 // The count of peers who agreed with vote. Valid only in candidate mode.
	nextIndex           []int               // Index of the next entry for each peer. Valid only in leader mode.
	matchIndex          []int               // Index of the matched entry for each peer., Valid only in leader mode.
	mode                ServerMode          // The ServerMode, Initialized to follower.
	snapshotApplyStatus SnapshotApplyStatus // Initialized to Done. Valid only in follower mode.
}

func (rf *Raft) String() string {
	return fmt.Sprintf("Raft: mode: %s, len(p): %d, currentTerm: %d, lastHB: %s, voted: %d, voteFor: %d, "+
		"commitIdx: %d, lastApplied: %d, len(LogsSlice): %d, LogsSlice: %s.", rf.mode, len(rf.peers), rf.currentTerm,
		rf.lastHeartBeat, rf.voted, rf.voteFor, rf.commitIndex, rf.lastApplied, rf.logs.Len(), rf.logs)
}

// GetState RLocked return currentTerm and whether this server
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	DPrintf("Raft-%d, GetState: %s", rf.me, rf)
	return rf.currentTerm, rf.mode == leader
}

// Make Constructor of a Raft peer.
// Raft peer has several roles to ensure all the commands Linearization and Idempotent:
// 1 Try to win the election when it's a candidate.
// 2 Perform the command from KVServer to get majority when it's a leader.
// 3 Perform leader's command when it's a follower.
// 4 Apply all commands to KVServer(state machine) after the commands got majority.
// 5 Also includes the logic of Snapshot for log compression.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("Raft-%d, Making, Start.", me)
	rf := &Raft{}
	rf.startServer(peers, me, persister, applyCh)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.ticker()
	DPrintf("Raft-%d, Make, Finished, %s", rf.me, rf)
	return rf
}

// Kill call this method to kill this Raft peer.
func (rf *Raft) Kill() {
	DPrintf("Raft-%d, Killed, %s", rf.me, rf)
	atomic.StoreInt32(&rf.dead, 1)
}

// killed return if the Raft peer is killed.
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// startServer Locked init the server and start.
func (rf *Raft) startServer(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.snapshotApplyStatus = Done
	rf.logs = NewLogEntries()
	if rf.readPersist(rf.persister.ReadRaftState()) {
		rf.mode = follower
		rf.lastHeartBeat = time.Now()
		rf.voted = 0
	} else {
		rf.startFollower(0, IntNil, true)
	}
}
