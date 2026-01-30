package main

import (
	"math/rand"
	"os"
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

// NewMaster creates a master with M map tasks and R reduce tasks
// files are split into chunks of chunkSize bytes (e.g. 64MB)
// e.g. a 150MB file with chunkSize=64MB creates 3 map tasks:
//   - task 0: bytes 0-64MB
//   - task 1: bytes 64MB-128MB
//   - task 2: bytes 128MB-150MB
// if chunkSize <= 0, entire file is one task (no splitting)
func NewMaster(files []string, nReduce int, chunkSize int64, timeoutDuration time.Duration) *Master {
	m := &Master{
		nReduce:     nReduce,
		done:        make(chan struct{}),
		stopChecker: make(chan struct{}),
	}

	// create map tasks by splitting each file into chunks
	// each chunk becomes a separate map task
	// this allows large files to be processed in parallel
	taskID := 0
	for _, file := range files {
		fileInfo, err := os.Stat(file)
		if err != nil {
			// file doesn't exist yet or error - create single task for whole file
			// worker will handle the error when it tries to read
			m.mapTasks = append(m.mapTasks, Task{
				ID:       taskID,
				Type:     MapTask,
				State:    Idle,
				Filename: file,
				Offset:   0,
				Size:     0, // 0 means read entire file
			})
			taskID++
			continue
		}

		fileSize := fileInfo.Size()

		// if chunkSize <= 0 or file is smaller than chunk, don't split
		if chunkSize <= 0 || fileSize <= chunkSize {
			m.mapTasks = append(m.mapTasks, Task{
				ID:       taskID,
				Type:     MapTask,
				State:    Idle,
				Filename: file,
				Offset:   0,
				Size:     fileSize,
			})
			taskID++
			continue
		}

		// split file into chunks
		// e.g. 150MB file with 64MB chunks:
		//   chunk 0: offset=0, size=64MB
		//   chunk 1: offset=64MB, size=64MB
		//   chunk 2: offset=128MB, size=22MB (remaining)
		for offset := int64(0); offset < fileSize; offset += chunkSize {
			size := chunkSize
			if offset+size > fileSize {
				size = fileSize - offset // last chunk may be smaller
			}
			m.mapTasks = append(m.mapTasks, Task{
				ID:       taskID,
				Type:     MapTask,
				State:    Idle,
				Filename: file,
				Offset:   offset,
				Size:     size,
			})
			taskID++
		}
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
// respects backoff: tasks with RetryAfter in the future are skipped
func (m *Master) AssignTask() *Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	// phase 1: assign map tasks first
	// workers process input files and produce intermediate files
	for i := range m.mapTasks {
		if m.mapTasks[i].State == Idle {
			// check backoff: skip if task is in cooldown period after failure
			// this prevents rapid retries on persistent failures
			if !now.After(m.mapTasks[i].RetryAfter) && m.mapTasks[i].RetryCount > 0 {
				continue // still in backoff, try next task
			}
			m.mapTasks[i].State = InProgress
			m.mapTasks[i].StartTime = now // for timeout detection
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
			// check backoff for reduce tasks too
			if !now.After(m.reduceTasks[i].RetryAfter) && m.reduceTasks[i].RetryCount > 0 {
				continue
			}
			m.reduceTasks[i].State = InProgress
			m.reduceTasks[i].StartTime = now
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
// resets task to idle with exponential backoff so another worker can retry later
// backoff prevents rapid retry loops that waste resources on persistent failures
func (m *Master) ReportFailed(taskID int, taskType TaskType) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// backoff constants
	baseDelay := 1 * time.Second  // first retry after ~1s
	maxDelay := 30 * time.Second  // cap at 30s to avoid excessive waits

	switch taskType {
	case MapTask:
		for i := range m.mapTasks {
			if m.mapTasks[i].ID == taskID {
				m.mapTasks[i].State = Idle
				m.mapTasks[i].RetryCount++
				// set backoff: task won't be assigned until after this time
				m.mapTasks[i].RetryAfter = time.Now().Add(
					backoffWithJitter(m.mapTasks[i].RetryCount, baseDelay, maxDelay),
				)
				return
			}
		}
	case ReduceTask:
		for i := range m.reduceTasks {
			if m.reduceTasks[i].ID == taskID {
				m.reduceTasks[i].State = Idle
				m.reduceTasks[i].RetryCount++
				m.reduceTasks[i].RetryAfter = time.Now().Add(
					backoffWithJitter(m.reduceTasks[i].RetryCount, baseDelay, maxDelay),
				)
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

// backoffWithJitter calculates exponential backoff with random jitter
// formula: baseDelay * 2^retryCount + random jitter (0-50% of delay)
// e.g. retry 0: 1s + jitter, retry 1: 2s + jitter, retry 2: 4s + jitter
// jitter prevents thundering herd when multiple tasks fail simultaneously
// capped at maxDelay to prevent infinite waits
func backoffWithJitter(retryCount int, baseDelay, maxDelay time.Duration) time.Duration {
	// exponential: 1s, 2s, 4s, 8s, ...
	delay := baseDelay * (1 << retryCount) // 2^retryCount

	// cap at max
	if delay > maxDelay {
		delay = maxDelay
	}

	// add jitter: 0-50% of delay
	// this spreads out retries to avoid all failed tasks retrying at once
	jitter := time.Duration(rand.Int63n(int64(delay / 2)))
	return delay + jitter
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
