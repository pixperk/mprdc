package main

import (
	"sync"
	"time"
)

// master is the coordinator that:
// - holds all map and reduce tasks
// - assigns tasks to workers on request
// - tracks task state: idle -> in_progress -> completed
// - detects timed-out tasks and resets them for retry
type Master struct {
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	mu          sync.Mutex
	done        chan struct{} // closed when all tasks complete, signals workers to exit
	stopChecker chan struct{} // closed to stop the timeout checker goroutine
}

// NewMaster creates a master with M map tasks (one per file) and R reduce tasks
// e.g. NewMaster(["a.txt", "b.txt"], 3, 10s) creates:
//   - 2 map tasks (M=2): process a.txt and b.txt
//   - 3 reduce tasks (R=3): partitions 0, 1, 2
//   - timeout checker that runs every 5s (timeout/2)
func NewMaster(files []string, nReduce int, timeoutDuration time.Duration) *Master {
	m := &Master{
		nReduce:     nReduce,
		done:        make(chan struct{}),
		stopChecker: make(chan struct{}),
	}

	// create one map task per input file
	// each map task will read its file, apply mapFunc, and write intermediate files
	for i, file := range files {
		m.mapTasks = append(m.mapTasks, Task{
			ID:       i,
			Type:     MapTask,
			State:    Idle,
			Filename: file,
		})
	}

	// create R reduce tasks (one per partition)
	// each reduce task will read intermediate files for its partition and apply reduceFunc
	for i := 0; i < nReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, Task{
			ID:    i,
			Type:  ReduceTask,
			State: Idle,
		})
	}

	// start background goroutine to detect timed-out tasks
	m.startTimeoutChecker(timeoutDuration)
	return m
}

// AssignTask is called by workers to get work
// returns nil if no work available (worker should wait and retry)
// priority: map tasks first, then reduce tasks (only after ALL maps complete)
func (m *Master) AssignTask() *Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	// phase 1: assign map tasks first
	// workers process input files and produce intermediate files
	for i := range m.mapTasks {
		if m.mapTasks[i].State == Idle {
			m.mapTasks[i].State = InProgress
			m.mapTasks[i].StartTime = time.Now() // for timeout detection
			return &m.mapTasks[i]
		}
	}

	// phase 2: check if all map tasks are completed
	// reduce tasks can only start after ALL maps are done
	// because reduce needs intermediate files from every map task
	if !m.allMapsDone() {
		return nil // maps still running, wait
	}

	// phase 3: assign reduce tasks
	// workers read intermediate files and produce final output
	for i := range m.reduceTasks {
		if m.reduceTasks[i].State == Idle {
			m.reduceTasks[i].State = InProgress
			m.reduceTasks[i].StartTime = time.Now()
			return &m.reduceTasks[i]
		}
	}

	return nil // all tasks assigned or completed
}

// allMapsDone returns true if every map task is completed
// called while holding the lock
func (m *Master) allMapsDone() bool {
	for i := range m.mapTasks {
		if m.mapTasks[i].State != Completed {
			return false
		}
	}
	return true
}

// ReportDone is called by workers when they successfully complete a task
// marks the task as completed and checks if all work is done
func (m *Master) ReportDone(taskID int, taskType TaskType) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch taskType {
	case MapTask:
		for i := range m.mapTasks {
			if m.mapTasks[i].ID == taskID {
				m.mapTasks[i].State = Completed
				return
			}
		}

	case ReduceTask:
		for i := range m.reduceTasks {
			if m.reduceTasks[i].ID == taskID {
				m.reduceTasks[i].State = Completed
				// check if this was the last reduce task
				// if so, signal all workers to exit
				go m.checkAndClose()
				return
			}
		}
	}
}

// ReportFailed is called by workers when a task fails (e.g. file not found)
// resets task to idle so another worker can retry it
func (m *Master) ReportFailed(taskID int, taskType TaskType) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch taskType {
	case MapTask:
		for i := range m.mapTasks {
			if m.mapTasks[i].ID == taskID {
				m.mapTasks[i].State = Idle
				return
			}
		}
	case ReduceTask:
		for i := range m.reduceTasks {
			if m.reduceTasks[i].ID == taskID {
				m.reduceTasks[i].State = Idle
				return
			}
		}
	}
}

func (m *Master) NReduce() int {
	return m.nReduce
}

func (m *Master) NumMapTasks() int {
	return len(m.mapTasks)
}

// Done returns a channel that's closed when all tasks complete
// workers select on this to know when to exit
func (m *Master) Done() <-chan struct{} {
	return m.done
}

// checkAndClose checks if all reduce tasks are completed
// if so, closes done channel (signals workers to exit) and stops timeout checker
func (m *Master) checkAndClose() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.reduceTasks {
		if m.reduceTasks[i].State != Completed {
			return // not all done yet
		}
	}

	// all reduce tasks completed - job is done
	close(m.done)        // signal workers to exit
	close(m.stopChecker) // stop timeout checker goroutine
}

// startTimeoutChecker runs a background goroutine that periodically
// checks for tasks stuck in IN_PROGRESS state (worker crashed or hung)
// if a task exceeds the timeout, it's reset to IDLE for another worker
func (m *Master) startTimeoutChecker(timeoutDuration time.Duration) {
	go func() {
		for {
			select {
			case <-m.stopChecker:
				return // job done, stop checking
			case <-time.After(timeoutDuration / 2):
				// check twice per timeout period for responsiveness
				m.checkForTimeouts(timeoutDuration)
			}
		}
	}()
}

// checkForTimeouts finds tasks that have been IN_PROGRESS too long
// and resets them to IDLE so another worker can pick them up
// this handles worker crashes/hangs without blocking the job
func (m *Master) checkForTimeouts(timeoutDuration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	// check map tasks for timeout
	for i := range m.mapTasks {
		if m.mapTasks[i].State == InProgress {
			if now.Sub(m.mapTasks[i].StartTime) > timeoutDuration {
				// task timed out - assume worker is dead, reset to idle
				m.mapTasks[i].State = Idle
			}
		}
	}

	// check reduce tasks for timeout
	for i := range m.reduceTasks {
		if m.reduceTasks[i].State == InProgress {
			if now.Sub(m.reduceTasks[i].StartTime) > timeoutDuration {
				m.reduceTasks[i].State = Idle
			}
		}
	}
}
