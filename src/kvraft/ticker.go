package kvraft

import "6.5840/raft"

//
// Definition of ticker, the consumer of ApplyCh.
//

// ticker consumer of the ApplyCh.
// Unlocked method. But will call Locked methods.
// Deal with the msg from ApplyCh.
func (kv *KVServer) ticker() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op, isValidLog := kv.isValidLog(&msg) // RLocked method
			if isValidLog {
				kv.performLog(op)         // RLocked method.
				kv.checkAndSendSnapshot() // RLocked method. send snapshot to Raft peer if applied.
			}
		} else if msg.SnapshotValid {
			kv.installSnapshotFromApplyCh(&msg) // Locked method
		}
	}
}

// isValidLog check if the msg from ApplyCh is a valid log.
// RLocked method.
// 1 Discard outdated Log.
// 2 Discard duplicated Log.
func (kv *KVServer) isValidLog(msg *raft.ApplyMsg) (*Op, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	DPrintf("Server-%d, Perform Log, ApplyMsg in, %s.", kv.me, msg)
	if msg.CommandIndex <= kv.lastApplied { // 1 Discard outdated Log.
		DPrintf("Server-%d, Perform Log, Discard outdated log, %s.", kv.me, msg)
		return nil, false
	}
	kv.lastApplied = msg.CommandIndex
	opMsg, _ := msg.Command.(Op)
	if kv.isDuplicatedMsg(&opMsg) { // 2 Discard duplicated Log.
		DPrintf("Server-%d, Perform Log, Discard duplicated log, %s.", kv.me, msg)
		return nil, false
	}
	return &opMsg, true
}

// isDuplicatedMsg return if a msg is a duplicated message.
// Unlocked method, so the caller should be locked.
// Return false if the op does not exist or does NOT have a smaller Id in the cachedTasks.
func (kv *KVServer) isDuplicatedMsg(op *Op) bool {
	prevRes, exists := kv.cachedTasks[op.Client]
	if !exists || prevRes.Id < op.Id {
		return false
	}
	DPrintf("Server-%d, Duplicated Msg Check, true, %s, prevres: %s.", kv.me, op, &prevRes)
	return true
}

// performLog perform the Majority Log from ApplyCh.
// Locked method.
// 3 Determine if it's a leader's task.
// 4 Apply to KV store.
// 5 Notify the RPC.
func (kv *KVServer) performLog(opMsg *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	opStatus, exists := kv.isLeaderTask(opMsg) // 3 Determine if it's a leader's task.
	if !exists {                               // performs the follower's log
		newStatus := OpTracker{OpReply: &Reply{Result: Running, Value: "", Id: opMsg.Id}}
		opStatus = &newStatus
	}
	isNotify := true
	if opStatus.OpReply.Result == ErrTimeOut { // Cancel 4 notification for an ErrTimeout log.
		isNotify = false
	}
	opStatus.OpReply.Result = OK
	kv.performKv(opMsg, opStatus)                    // Unlocked Method. 4 Apply to KV store.
	kv.cachedTasks[opMsg.Client] = *opStatus.OpReply // AddForLeader to cachedTasks for keep Idempotent.
	if exists {                                      // 5 Notify the RPC if it's the leader.
		delete(kv.leaderTasks, opMsg.Client) // Delete completed task from leaderTasks.
		if isNotify {
			DPrintf("Server-%d, Perform Log, Send to ch, %s.", kv.me, opStatus)
			opStatus.Ch <- true
		}
	}
	DPrintf("Server-%d, Perform Log, Done, %s.", kv.me, opStatus)
}

// isLeaderTask return OpTracker when the task is a leader task.
// Unlocked method, so the caller should be locked.
// Return false if the op does not exist or does NOT have a same Id in the leaderTasks.
func (kv *KVServer) isLeaderTask(op *Op) (*OpTracker, bool) {
	opStatus, exists := kv.leaderTasks[op.Client]
	if !exists || opStatus.OpReply.Id != op.Id {
		return nil, false
	}
	DPrintf("Server-%d, Leader Task Check, true, %s, currTask: %s.", kv.me, op, opStatus)
	return opStatus, true
}
