package kvraft

import "time"

//
// Definition of ProcessOP RPC.
//

// ProcessOp RPC handler.
// UnLocked method, but may call Locked methods.
// 1 Return immediately for duplication Op. LINEARZATION Guarantee.
// 2 Send to Raft peer.
// 3 Return immediately if the Raft peer is not leader.
// 4 Wait for majority.
// 5 Return when ErrTimeOut or OK.
func (kv *KVServer) ProcessOp(op *Op, reply *Reply) {
	DPrintf("Server-%d, PO task in, %s.", kv.me, op)
	reply.Id = op.Id
	if kv.duplicationProcess(op, reply) { // RLocked method.
		DPrintf("Server-%d, PO, Return duplicated, %s, %s.", kv.me, op, reply)
		return
	}
	opStatus := kv.callStart(op, reply) // Send to Raft peer. Locked Method.
	if reply.Result == ErrWrongLeader {
		DPrintf("Server-%d, PO, end for ErrWrongLeader, %s.", kv.me, op)
		return
	}
	DPrintf("Server-%d, PO task is running, %s.", kv.me, opStatus)
	select {
	case <-opStatus.Ch:
		DPrintf("Server-%d, PO, Got OK Result-%s.", kv.me, opStatus)
	case <-time.After(Timeout):
		opStatus.OpReply.Result = ErrTimeOut
		DPrintf("Server-%d, PO, Got Timeout Result-%s.", kv.me, opStatus)
	}
	go kv.closeCh(opStatus) // Locked method. Have to start a go routine for a corner case.
	DPrintf("Server-%d, PO task Finnished, %s.", kv.me, opStatus)
}

// duplicationProcess return true if the Op is duplicated, also assemble the reply from cachedTasks.
// RLocked Method.
// Return false when Op of this Clerk does not exist in previous map or existed, but has an outdated Id.
// Return ture when Op of theis Clerk existed, and has a same or greater Id, reply cached value for equal.
func (kv *KVServer) duplicationProcess(op *Op, reply *Reply) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	prevReply, exists := kv.cachedTasks[op.Client]
	if !exists || prevReply.Id < op.Id {
		DPrintf("Server-%d, Duplication RPC check false, %s.", kv.me, op)
		return false
	}
	if prevReply.Id == op.Id {
		reply.Result = prevReply.Result
		reply.Value = prevReply.Value
	} else if prevReply.Id > op.Id {
		reply.Result = ErrDuplicated
	}
	DPrintf("Server-%d, Duplication RPC check true, %s, %s, prev: %s.", kv.me, op, reply, &prevReply)
	return true
}

// callStart call raft peer and return the result.
// Partly locked method.
func (kv *KVServer) callStart(op *Op, reply *Reply) *OpTracker {
	opStatus := OpTracker{OpReply: reply}
	kv.startCommand(op, &opStatus) // Unlocked Method.
	if opStatus.OpReply.Result == Running {
		opStatus.Ch = make(chan bool)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.leaderTasks[op.Client] = &opStatus // Will overwrite the value.
	}
	DPrintf("Server-%d, callStart, finished status: %s.", kv.me, &opStatus)
	return &opStatus
}

// startCommand send commands to Raft peer.
// Unlocked Method.
// return if the raft peer is leader.
func (kv *KVServer) startCommand(op *Op, tracker *OpTracker) {
	index, _, isLeader := kv.rf.Start(*op)
	DPrintf("Server-%d, startCommand, Index: %d, isLeader: %t,  %s.", kv.me, index, isLeader, tracker)
	if isLeader {
		tracker.OpReply.Result = Running
	} else {
		tracker.OpReply.Result = ErrWrongLeader
	}
}

// closeCh close the channel when received a message.
// Locked Method.
// Wait for 5 ms for a corner case that the timeout and result come consistently.
func (kv *KVServer) closeCh(opStatus *OpTracker) {
	time.Sleep(5 * time.Millisecond)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(opStatus.Ch)
}
