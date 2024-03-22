package mr

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

//
// Definition of Worker of Mapreduce.
//

type WorkerId string

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Worker main entrance of the Worker.
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerId := GenerateWorkerId()
	DPrintf("W, Worker, A Worker starting, WID: %s.", workerId)
	nta := NewTaskArgs{workerId}
	for { // Infinity loop to ask for a new task when available.
		ntr := NewTaskReply{}
		DPrintf("W, Worker, Ask for task, %s.", nta.str())
		ok := call(RPCNameNewTask, &nta, &ntr)
		if !ok {
			DPrintf("W, Worker, Wait for RPC error, %s.", nta.str())
			ntr.Task.Mode = Wait
		}
		DPrintf("W, Worker, Got task, %s.", ntr.str())
		switch ntr.Task.Mode { // Switch by reply from RPCServer
		case Mapper: // Go to mapper logic.
			mapper(mapf, &ntr.Task)
		case Reducer: // Go to reducer logic.
			reducer(reducef, &ntr.Task)
		case Wait:
			DPrintf("W, Worker, Wait, %s.", nta.str())
			time.Sleep(time.Second) // Sleep for 1 sec
		case Quit:
			DPrintf("W, Worker, Quit, %s.", nta.str())
			return
		default:
			continue
		}
	}
}

// reportRes a helper function for worker.
// Send Q and taskID to RPCServer server when the current task is finished.
func reportRes(task *Task, status TaskStatus) {
	task.Status = status
	rta := ReturnTaskArgs{*task}
	rtr := ReturnTaskReply{}
	call(RPCNameReturnTask, &rta, &rtr)
	DPrintf("W, Report Res send worker result. %s", task.str())
}

// atomicRename rename the tmpFile to filename.
// Remove tmpFile if the file already exists.
func atomicRename(tmpFile *os.File, filename string, task *Task) bool {
	DPrintf("W, Atomic Rename start, %s, file name: %s", task.str(), filename)
	err := os.Rename(tmpFile.Name(), filename)
	if err != nil {
		DPrintf("W, Atomic Rename, error when rename the temp file, %s, "+
			"file name: %s, error is %v", task.str(), filename, err)
		if os.IsExist(err) { // If target file exists, means that the task is duplicated.
			DPrintf("W, Atomic Rename skip because file existed, %s, file name: %s",
				task.str(), filename)
			os.Remove(tmpFile.Name())
			return true
		}
		return false
	}
	DPrintf("W, Atomic Rename finished, %s, file name: %s", task.str(), filename)
	return true
}

// GenerateWorkerId return an Unique value as worker id.
func GenerateWorkerId() WorkerId {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	uniqueValue := fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(1000))
	return WorkerId(uniqueValue)
}
