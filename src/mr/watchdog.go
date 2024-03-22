package mr

import "time"

//
// Definition of watchDog for Coordinator and it's helper functions.
//

// watchDog monitor the Status of tasks
// 1 Consume the assigned task update first, and restart when available.
// 2 Consume the task Status update then and restart when available.
// 3 Do the periodicTasks and sleep for a while.
func (c *Coordinator) watchDog() {
	DPrintf("C, WatchDog started.")
	for !c.isDone {
		DPrintf("C, WatchDog awaked.")
		select { // Update Assigned Tasks. - Priority Channel
		case assignedTask := <-c.chTaskAssigned:
			if assignedTask.Mode == c.tasks[assignedTask.Serial].Mode {
				DPrintf("C, WatchDog assigned task update start, %s", assignedTask.str())
				c.tasks[assignedTask.Serial] = assignedTask
				DPrintf("C, WatchDog, task assigned updated finished, %s",
					c.tasks[assignedTask.Serial].str())
			} else {
				DPrintf("C, WatchDog assigned task update find outdated report, %s",
					assignedTask.str())
			}
		default:
			c.taskStatusUpdate() // Status Update.
		}
	}
}

// taskStatusUpdate to consume the channel chResult to update the task Status.
func (c *Coordinator) taskStatusUpdate() {
	select { // Status Update.
	case updatedTask := <-c.chResult:
		DPrintf("C, WatchDog taskStatusUpdate start, %s", updatedTask.str())
		originalTask := c.tasks[updatedTask.Serial]
		if updatedTask.Mode == originalTask.Mode {
			if originalTask.Status == InProgress {
				c.taskStatusUpdateHelper(originalTask, updatedTask)
			} else {
				DPrintf("C, WatchDog taskStatusUpdate find outdated report, %s", updatedTask.str())
			}
		} else {
			DPrintf("C, WatchDog taskStatusUpdate, ignore to update a finished task: %s, %s",
				originalTask.str(), updatedTask.str())
		}
		DPrintf("C, Watchdog taskStatusUpdate finished.")
	default:
		c.periodicTasks()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) taskStatusUpdateHelper(originalTask *Task, updatedTask *Task) {
	if updatedTask.Status == Success ||
		(updatedTask.Status == Fail && originalTask.Retry >= MaxRetry) {
		c.tasks[updatedTask.Serial] = updatedTask
		DPrintf("C, WatchDog taskStatusUpdate, task Status final updated, %s",
			c.tasks[updatedTask.Serial].str())
	} else if updatedTask.Status == Fail && originalTask.Retry < MaxRetry {
		// Retry when fail but no exceeds the max Retry.
		c.tasks[updatedTask.Serial].Retry++
		c.tasks[updatedTask.Serial].Status = Pending
		c.chTaskAssign <- c.tasks[updatedTask.Serial]
		DPrintf("C, WatchDog taskStatusUpdate, task update Retry, %s, %s",
			c.tasks[updatedTask.Serial].str(), updatedTask.str())
	} else {
		DPrintf("C, WatchDog taskStatusUpdate, ERROR task update unhandled, %s, %s",
			c.tasks[updatedTask.Serial].str(), updatedTask.str())
	}
}

// periodicTasks to execute the periodic tasks.
// 1 timeout tasks check.
// 2 task progress check. Switch to Reducer mode or Set isDone if applicable.
func (c *Coordinator) periodicTasks() {
	currTime := time.Now()
	finalTasks := 0
	for _, taskPtr := range c.tasks {
		task := *taskPtr
		if task.Status == InProgress && isTimeOut(currTime, task.STime) { // Timeout
			task.Status = Fail
			go func() {
				c.chResult <- &task // Cannot block for avoiding deadlock.
			}()
			DPrintf("C, WatchDog periodicTasks, find timeout task, %s", task.str())
		} else if task.Status == Success || task.Status == Fail {
			finalTasks++
			DPrintf("C, Watchdog periodicTasks, find final task: id %d. There are %d final tasks",
				task.Serial, finalTasks)
		}
	}
	if finalTasks == len(c.tasks) { // All tasks finished.
		DPrintf("C, WatchDog periodicTasks, Tasks done, Final tasks: %d, All tasks: %d",
			finalTasks, len(c.tasks))
		if c.mode == Mapper { // Switch to Reducer Mode when mapper is done.
			DPrintf("C, Watchdog periodicTasks, will switch Mode to Reducer.")
			c.reducerInit()
		} else {
			c.isDone = true // Job is finished.
			DPrintf("C, Watchdog periodicTasks, set isDone.")
		}
	}
	DPrintf("C, Watchdog periodicTasks finished.")
}

// isTimeOut A helper function to determine if the time of task has exceeded the timeout.
func isTimeOut(currTime time.Time, startTime time.Time) bool {
	return currTime.Sub(startTime) > time.Duration(TimeOut)*time.Second
}
