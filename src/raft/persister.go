package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//

import (
	"6.5840/labgob"
	"bytes"
	"sync"
)

// persist save state to persist.
// Includes: currentTerm, voteFor, logs, Snapshot Term and last index.
// Unlocked method, so the caller should be at least RLocked.
func (rf *Raft) persist() {
	DPrintf("Raft-%d, Persist, Write start, %s", rf.me, rf)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs.SnapshotTerm)
	e.Encode(rf.logs.SnapshotIndex)
	e.Encode(rf.logs.LogsSlice)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
	DPrintf("Raft-%d, Persist, Write Finished, %s", rf.me, rf)
}

// readPersist restore previously persisted state.
// Includes: currentTerm, voteFor, logs, SnapshotTerm, SnapshotIndex.
// Unlocked method, so the caller should be Locked.
func (rf *Raft) readPersist(data []byte) bool {
	DPrintf("Raft-%d, Persist, Read start, %s", rf.me, rf)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return false
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logSlice []LogItem
	var snapshotTerm int
	var snapshotIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&snapshotTerm) != nil ||
		d.Decode(&snapshotIndex) != nil || d.Decode(&logSlice) != nil {
		DPrintf("Raft-%d, Persist, Read error, maybe no persist yet, %s", rf.me, rf)
		return false
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs.LogsSlice = logSlice
		rf.logs.SnapshotTerm = snapshotTerm
		rf.logs.SnapshotIndex = snapshotIndex
	}
	DPrintf("Raft-%d, Persist, Read Finished, %s", rf.me, rf)
	return true
}

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
