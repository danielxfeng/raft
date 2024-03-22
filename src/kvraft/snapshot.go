package kvraft

import (
	"6.5840/labgob"
	"6.5840/raft"
	"bytes"
	"fmt"
)

//
// Definition of Logic of Snapshot.
//

// Snapshot the struct of Snapshot.
type Snapshot struct {
	Kv          map[string]string // The KV store
	PrevMap     map[int64]Reply   // The prev Map
	LastApplied int               // The index of LastApplied
}

// MakeSnapshot the constructor of Snapshot.
// Make a Snapshot from ApplyCh.
func MakeSnapshot(data []byte) (*Snapshot, bool) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("Read Snapshot, Read MSG error, data not exists")
		return nil, false
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kv map[string]string
	var prevMap map[int64]Reply
	var lastIdx int
	if d.Decode(&lastIdx) != nil || d.Decode(&kv) != nil || d.Decode(&prevMap) != nil {
		DPrintf("Read Snapshot, Decode error.")
		return nil, false
	} else {
		snapshot := Snapshot{Kv: kv, PrevMap: prevMap, LastApplied: lastIdx}
		return &snapshot, true
	}
}

func (s *Snapshot) String() string {
	return fmt.Sprintf("Snapshot idx: %d", s.LastApplied)
}

// Export return exported Snapshot to byte.
func (s *Snapshot) Export() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(s.LastApplied)
	e.Encode(s.Kv)
	e.Encode(s.PrevMap)
	return w.Bytes()
}

// generateSnapshot generate a Snapshot when Raft's persister is oversize.
// RLocked method.
// Do a deep copy because the map is reference.
func (kv *KVServer) generateSnapshot() (*Snapshot, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if kv.maxraftstate != -1 && kv.maxraftstate < kv.persister.RaftStateSize() {
		DPrintf("Server-%d, Make Snapshot, Request received.", kv.me)
		clonedKv := make(map[string]string)
		for k, v := range kv.kv {
			clonedKv[k] = v
		}
		clonedPrevMap := make(map[int64]Reply)
		for k, v := range kv.cachedTasks {
			clonedPrevMap[k] = v
		}
		snapShot := Snapshot{Kv: clonedKv, PrevMap: clonedPrevMap, LastApplied: kv.lastApplied}
		DPrintf("Server-%d, Make Snapshot, Created, %s.", kv.me, &snapShot)
		return &snapShot, true
	}
	return nil, false
}

// installSnapshotFromApplyCh install snapshot from ApplyCh.
// Unlocked method. But will call Locked methods.
func (kv *KVServer) installSnapshotFromApplyCh(msg *raft.ApplyMsg) {
	kv.installSnapshot(msg.Snapshot) // Locked method.
}

// installSnapshotFromApplyCh install snapshot from persist.
// Unlocked method. But will call Locked methods.
func (kv *KVServer) installSnapshotFromPersist() {
	kv.installSnapshot(kv.persister.ReadSnapshot()) // Locked method.
}

// installSnapshot install snapshot to KVServer.
// Locked method.
func (kv *KVServer) installSnapshot(snapshotData []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Server-%d, Install Snapshot", kv.me)
	snapshot, ok := MakeSnapshot(snapshotData)
	if ok && snapshot.LastApplied > kv.lastApplied { // discard outdated snapshot.
		kv.kv = snapshot.Kv
		kv.cachedTasks = snapshot.PrevMap
		kv.lastApplied = snapshot.LastApplied
		DPrintf("Server-%d, Install Snapshot, msg: %s", kv.me, snapshot)
	}
}

// checkAndSendSnapshot check the persist size of Raft peer, generate and send a Snapshot if over the limit.
// Unlocked method. But will call RLocked methods.
func (kv *KVServer) checkAndSendSnapshot() {
	snapShot, ok := kv.generateSnapshot() // RLocked method.
	if ok {
		DPrintf("Server-%d, Send Snapshot, snapShot: %s", kv.me, snapShot)
		kv.rf.Snapshot(snapShot.LastApplied, snapShot.Export())
	}
	DPrintf("Server-%d, checkAndSendSnapshot, Done.", kv.me)
}
