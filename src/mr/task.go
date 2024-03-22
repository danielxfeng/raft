package mr

import (
	"fmt"
	"time"
)

//
// Definition of Task, TaskStatus, TaskMode, TaskId for the MapReduce System.
//

// Task defines the properties of a task
// For TaskMode Mapper, the FileName is the pending file name.
// For TaskMode Reducer, the generateFileNames() is the pending file segments.
type Task struct {
	Mode      TaskMode   // mapper, reducer, wait, quit
	Serial    int        // index of tasks []Task
	WorkerId  WorkerId   // the worker's id
	Status    TaskStatus // current Status of task
	STime     time.Time  // the start time of task, for timeout detection
	Retry     int        // Retry times
	FileName  string     // input file for mapper
	FileNames []string   // input files for reducer
	NReducer  int        // number of reducer workers
}

// taskId return the taskId which is Mode-Id
func (t *Task) taskId() string {
	return fmt.Sprintf("%s-%d", t.Mode.str(), t.Serial)
}

// str return the string of a task.
func (t *Task) str() string {
	return fmt.Sprintf("WID: %s, Task: id: %s, Retry: %d, Status: %s, NReducer: %d",
		t.WorkerId, t.taskId(), t.Retry, t.Status.str(), t.NReducer)
}

// generateFileNames return the pending file segments.
// Only for TaskMode Reducer.
// Each segment file is named using the format "<FileName>-<originalFile>"
func (t *Task) generateFileNames(nMapTasks int) {
	fns := make([]string, nMapTasks)
	if t.Mode == Reducer {
		for i := 0; i < nMapTasks; i++ {
			fns[i] = fmt.Sprintf("mr-%d-%d", i, t.Serial)
		}
	}
	t.FileNames = fns
}

// TaskStatus defines the TaskStatus of a task.
type TaskStatus int

// Enums for TaskStatus
const (
	Pending = iota
	Success
	Fail
	InProgress
)

var taskStatusArray = []string{"pending", "success", "failed", "in-progress"}

// str return the TaskStatus of a task.
func (ts *TaskStatus) str() string {
	return taskStatusArray[*ts]
}

// TaskMode defines the Mode of tasks.
type TaskMode int

// Enums for TaskMode
const (
	Mapper TaskMode = iota
	Reducer
	Quit
	Wait
)

var taskModes = []string{"mapper", "reducer", "quit", "wait"}

// str returns the string of task Mode.
func (r *TaskMode) str() string {
	return taskModes[*r]
}
