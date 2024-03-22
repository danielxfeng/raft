package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
)

//
// Definition of the Server in the Raft KV store.
// Main entry point of this package.
//

const Timeout = 500 * time.Millisecond // The timeout for Raft peer to get majority.

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int               // snapshot if log grows this big
	persister    *raft.Persister   // raft's persister
	lastApplied  int               // lastApplied index of a Raft peer.
	kv           map[string]string // kv storage.

	// map: Clerk: {chan, Reply} includes the processing task as a leader.
	// There is at most ONE processing task for a Clerk since Clerk is synchronized.
	leaderTasks map[int64]*OpTracker
	// map: Clerk: Reply includes every Clerk's latest processed task.
	// Only cache ONE processed task for each Clerk because Clerk is synchronized so the task Id is increment.
	cachedTasks map[int64]Reply
}

// StartKVServer constructor of KVServer.
// KVServer is the state machine in a Raft System. The main logic is:
// 1. Clerk calls the KVServer to send command.
// 2. KVServer forwards the command to Raft peer to get majority when it is a leader.
// 3. KVServer receives commands after getting majority by ApplyCh from Raft peer, then apply to KVStore.
// Then return to Clerk when it is a leader.
// 4. Also includes logic about: Timeout detection, Snapshot for log compression.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})
	labgob.Register(Reply{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.leaderTasks = make(map[int64]*OpTracker)
	kv.cachedTasks = make(map[int64]Reply)
	kv.kv = make(map[string]string)
	kv.installSnapshotFromPersist()
	go kv.ticker()
	DPrintf("Server-%d, Started.", kv.me)
	return kv
}

// Kill the tester calls Kill() when a KVServer instance won't be needed again.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

// killed return if the server is killed.
func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// performKV perform KV store
// UnLocked method, so the caller should hold the Locker.
func (kv *KVServer) performKv(op *Op, tracker *OpTracker) {
	DPrintf("Server-%d, PK, ready for KV: %s.", kv.me, tracker)
	switch op.OpType {
	case TypeGet:
		v, ok := kv.kv[op.Key]
		if ok {
			tracker.OpReply.Value = v
		} else {
			tracker.OpReply.Result = ErrNoKey
			tracker.OpReply.Value = ""
		}
	case TypePut:
		kv.kv[op.Key] = op.Value
	case TypeAppend:
		kv.kv[op.Key] += op.Value
	}
	DPrintf("Server-%d, PK, Done: %s, %s.", kv.me, op, tracker)
}
