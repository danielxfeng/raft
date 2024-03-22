package kvraft

import (
	"6.5840/labrpc"
	"crypto/rand"
	"fmt"
	"math/big"
	"time"
)

//
// Definition of the Clerk in the Raft KV store.
// The clerk runs synchronized.
//

// Retry time interval of RPC retry.
const Retry time.Duration = 10 * time.Millisecond

// Clerk struct of Clerk
type Clerk struct {
	servers []*labrpc.ClientEnd // Servers of Raft.
	me      int64               // The unique tag of this Clerk.
	leader  int                 // Leader of Raft.
	id      int                 // current id of command.
}

func (ck *Clerk) String() string {
	return fmt.Sprintf("CID-%d, leader: %d, id: %d.", ck.me, ck.leader, ck.id)
}

// MakeClerk constructor of Clerk.
// Clerk receives request, then sends to KVServer who has a leader Raft peer.
// Clerk will auto retry the command until it returns the result from KVServer.
// All the transactions are linearization and idempotent.
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.me = nrand()
	DPrintf("%s, Clerk Started.", ck)
	return ck
}

// Get fetch the current value for a key.
// Returns "" if the key does not exist.
func (ck *Clerk) Get(key string) string {
	return ck.processCommand(key, "", TypeGet)
}

// Put to the kv store.
func (ck *Clerk) Put(key string, value string) {
	ck.processCommand(key, value, TypePut)
}

// Append to the kv store.
func (ck *Clerk) Append(key string, value string) {
	ck.processCommand(key, value, TypeAppend)
}

// processCommand process the command.
// Try another server when the server is not leader.
// Retry continuously when Timeout.
func (ck *Clerk) processCommand(key string, value string, opType OpType) string {
	args := Op{Key: key, Value: value, OpType: opType, Client: ck.me, Id: ck.newId()}
	reply := Reply{}
	servers := ck.getOrderedServers()
	for {
		for _, i := range servers {
			DPrintf("%s, PC, Try send to %d, args: %s.", ck, i, &args)
			ok := ck.servers[i].Call("KVServer.ProcessOp", &args, &reply)
			if ok {
				DPrintf("%s, PC, Replied, args: %s, reply: %s.", ck, &args, &reply)
				if reply.Result == OK || reply.Result == ErrNoKey || reply.Result == ErrDuplicated { // Final res.
					ck.leader = i
					return reply.Value
				} else {
					continue
				}
			} else {
				DPrintf("%s, PC, Replied False, args: %s.", ck, &args)
				continue
			}
		}
		time.Sleep(Retry)
	}
}

// newId register a new id of the command.
func (ck *Clerk) newId() int {
	ck.id++
	return ck.id
}

// getOrderedServers return an ordered server slice that the previous leader will always at the first.
func (ck *Clerk) getOrderedServers() []int {
	servers := make([]int, len(ck.servers))
	servers[0] = ck.leader
	index := 1
	for i := 0; i < len(ck.servers); i++ {
		if i != ck.leader {
			servers[index] = i
			index++
		}
	}
	return servers
}

// nrand return a random int.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
