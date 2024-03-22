package kvraft

import (
	"fmt"
	"log"
	"time"
)

//
// Definitions of struct Op, and its relative structs: OpTracker, OpType, Reply
// Also includes the log print function DPrinter.
//

const Debug = true // Set to true will output the log.

const (
	NoResult = iota
	Running
	OK
	ErrNoKey
	ErrWrongLeader
	ErrTimeOut
	ErrDuplicated
)

type Result int

var ResultArr = []string{"NoResult", "Running", "OK", "NoKey", "WrongLeader", "TimeOut", "Duplicated"}

func (res *Result) String() string {
	return ResultArr[*res]
}

const (
	TypeGet = iota
	TypePut
	TypeAppend
)

type OpType int

var opTypeArr = []string{"Get", "Put", "Append"}

func (ot *OpType) String() string {
	return opTypeArr[*ot]
}

// Op strut of operation, also respects the args of RPC.
type Op struct {
	Key    string // the request Key.
	Value  string // the Value in case OpType is either TypePut or TypeAppend.
	OpType OpType // the OpType.
	Client int64  // id of Clerk
	Id     int    // id of command
}

func (op *Op) String() string {
	return fmt.Sprintf("Op-%d-%d, type: %s, key: %s, value: %s", op.Client, op.Id, &op.OpType, op.Key, op.Value)
}

// Reply the reply of Op RPC.
type Reply struct {
	Result Result // result of op.
	Value  string // the Value of KV store when OpType is TypeGet.
	Id     int    // the index of command.
}

func (reply *Reply) String() string {
	return fmt.Sprintf("Reply: id: %d, status: %s, value: %s", reply.Id, &reply.Result, reply.Value)
}

// OpTracker the status of Op.
type OpTracker struct {
	Ch      chan bool // the channel for receiving OpReply.
	OpReply *Reply    // the result of Op.
}

func (ot *OpTracker) String() string {
	return fmt.Sprintf("%s", ot.OpReply)
}

// DPrintf a log print system.
func DPrintf(format string, a ...interface{}) (n int, err error) {
	currentTime := time.Now().Format("2006-01-02 15:04:05.000")
	if Debug {
		log.Printf(currentTime+" "+format, a...)
	}
	return
}
