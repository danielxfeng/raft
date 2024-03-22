package mr

//
// Definitions of RPCServer.
// Includes RPCNewTask, RPCReturnTask, NewTaskArgs, NewTaskReply, ReturnTaskArgs, ReturnTaskReply
//

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)
import "strconv"

// RPC name
const (
	RPCNameNewTask    = "Coordinator.RPCNewTask"
	RPCNameReturnTask = "Coordinator.RPCReturnTask"
)

// NewTaskArgs is the arguments of RPC function RPCNewTask.
type NewTaskArgs struct {
	WorkerId WorkerId
}

// str return a string of NewTaskArgs
func (nta *NewTaskArgs) str() string {
	return fmt.Sprintf("NewTaskArgs: WID: %s", nta.WorkerId)
}

// NewTaskReply is the reply of RPC function RPCNewTask.
type NewTaskReply struct {
	Task Task
}

// str return a string of NewTaskReply
func (ntp *NewTaskReply) str() string {
	return fmt.Sprintf("NewTaskReply: %s", ntp.Task.str())
}

// ReturnTaskArgs is the arguments of RPC function RPCReturnTask.
type ReturnTaskArgs struct {
	Task Task
}

// str return a string of ReturnTaskArgs
func (rta *ReturnTaskArgs) str() string {
	return fmt.Sprintf("ReturnTaskArgs: %s", rta.Task.str())
}

// ReturnTaskReply is the reply of RPC function RPCReturnTask.
type ReturnTaskReply struct {
	WorkerId WorkerId
}

// coordinatorSock: cook up a unique-ish UNIX-domain socket name in /var/tmp, for the coordinator.
// Can't use the current directory since Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// RPCNewTask new task function of RPCServer server
// nta is the Args, which is the Worker's id.
// ntr is the Reply, which is the Worker's id, TaskMode, Task.
// Communication with worker threads.
func (c *Coordinator) RPCNewTask(nta *NewTaskArgs, ntr *NewTaskReply) error {
	DPrintf("C, RPCNewTask, request coming, %s.", nta.str())
	if c.isDone { // Return quit when all task are done.
		DPrintf("C, RPCNewTask, Return Quit because isDone, %s.", nta.str())
		ntr.Task.Mode = Quit
		return nil
	}
	select {
	case task := <-c.chTaskAssign: // If there is any new task available.
		ntr.Task = *task // Task copied here.
		ntr.Task.Status = InProgress
		ntr.Task.WorkerId = nta.WorkerId
		ntr.Task.STime = time.Now()
		c.chTaskAssigned <- &ntr.Task
		DPrintf("C, RPCNewTask, Return New Task, %s", ntr.str())
	default: // If there is NO new task available.
		DPrintf("C, RPCNewTask, Return Wait because ch is empty %s.", ntr.str())
		ntr.Task.Mode = Wait
	}
	DPrintf("C, RPCNewTask, Query finished %s.", ntr.str())
	return nil
}

// RPCReturnTask return task Status function of RPCServer server
// rta is the Args, which is the Worker's id, TaskID, TaskStatus.
// ntr is the Reply, which is the Worker's id.
// Communication with worker threads.
func (c *Coordinator) RPCReturnTask(rta *ReturnTaskArgs, rtr *ReturnTaskReply) error {
	DPrintf("C, RPCReturnTask, Request coming, %s.", rta.str())
	if c.isDone { // Return quit when all task are done.
		DPrintf("C, RPCReturnTask, Ignore because Done, %s.", rta.str())
	} else {
		DPrintf("C, RPCReturnTask, Query forword to statusUpdateProducer, %s.", rta.str())
		c.chResult <- &rta.Task
	}
	DPrintf("C, RPCReturnTask, Query finished, %s.", rta.str())
	return nil
}

// call to call the RPC Server and get return value.
// Send a request to the RPCServer, wait for the response.
// Usually returns true. Returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}

// server start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	if err := rpc.Register(c); err != nil {
		log.Fatal("RPC registration failed:", err)
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	DPrintf("C, RPC Server started.")
}
