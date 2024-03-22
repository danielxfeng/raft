package mr

//
// Definition of Coordinator of MapReduce.
//

const (
	MaxRetry = 5  // A Failed task would Retry MaxRetry times.
	TimeOut  = 10 // The seconds of timeout.
)

type Coordinator struct {
	isDone         bool       // Whether all tasks are done, everything will quit.
	mode           TaskMode   // Mode of the coordinator.
	nReducer       int        // Number of reducer workers.
	mapperFiles    []string   // Files that need to be MapReduce.
	tasks          []*Task    // Slice of all tasks.
	chTaskAssign   chan *Task // Channel of Pending task.
	chTaskAssigned chan *Task // Channel of Assigned task.
	chResult       chan *Task // Channel of StatusUpdate.
}

// MakeCoordinator constructor of Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// will write to c.tasks only when init.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	DPrintf("Coordinator starting. There are %d pending files and %d reducer workers.",
		len(files), nReduce)
	c := Coordinator{}
	c.server()
	DPrintf("C, MakeCoordinator, RPC Server starting.")
	c.isDone = false
	c.mapperFiles = files
	c.nReducer = nReduce
	c.mapperInit()
	go c.watchDog()
	DPrintf("C, MakeCoordinator finished.")
	return &c
}

// Done Return if the coordinator is done.
// main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	DPrintf("C, Done, Returned %t", c.isDone)
	return c.isDone
}

// mapperInit init the mapper mode.
func (c *Coordinator) mapperInit() {
	DPrintf("C, mapperInit initing.")
	c.modeInitHelper(Mapper, len(c.mapperFiles))
	DPrintf("C, mapperInit, Channel started.")
	// Iterate the pending files, 1 build task, 2 insert to tasks, 3 insert to channel.
	for i, v := range c.mapperFiles {
		task := Task{Mode: Mapper, Serial: i, FileName: v, Retry: 0, Status: Pending, NReducer: c.nReducer}
		c.tasks = append(c.tasks, &task)
		c.chTaskAssign <- &task
	}
	DPrintf("C, mapperInit, Adding task.")
	for _, task := range c.tasks {
		DPrintf(task.str())
	}
	DPrintf("C, mapperInit, finished.")
}

// reducerInit do the init job when switch from mapper to reducer.
// Will write to c.tasks only when init.
func (c *Coordinator) reducerInit() {
	DPrintf("C, reducerInit initing")
	c.modeInitHelper(Reducer, c.nReducer)
	DPrintf("C, reducerInit, Channel started.")
	newSlice := make([]*Task, c.nReducer)
	for i := range newSlice {
		newSlice[i] = &Task{Mode: Reducer, Status: Pending, Serial: i, Retry: 0}
		newSlice[i].generateFileNames(len(c.mapperFiles))
		c.chTaskAssign <- newSlice[i]
	}
	c.tasks = newSlice
	for _, task := range c.tasks {
		DPrintf(task.str())
	}
	DPrintf("C, ReducerInit finished.")
}

// modeInitHelper a helper function for init the coordinator.
// 1 set the mode 2 init the channels.
func (c *Coordinator) modeInitHelper(mode TaskMode, n int) {
	c.mode = mode
	c.chTaskAssign = make(chan *Task, n)
	c.chTaskAssigned = make(chan *Task, n)
	c.chResult = make(chan *Task, n)
}
