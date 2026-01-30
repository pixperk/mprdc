package main

import "sync"

type Master struct {
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	mu          sync.Mutex
	done        chan struct{} // to signal completion
}

func NewMaster(files []string, nReduce int) *Master {
	m := &Master{
		nReduce: nReduce,
		done:    make(chan struct{}),
	}

	for i, file := range files {
		m.mapTasks = append(m.mapTasks, Task{
			ID:       i,
			Type:     MapTask,
			State:    Idle,
			Filename: file,
		})
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, Task{
			ID:    i,
			Type:  ReduceTask,
			State: Idle,
		})
	}

	return m
}

func (m *Master) AssignTask() *Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	//assign map tasks first
	for i := range m.mapTasks {
		if m.mapTasks[i].State == Idle {
			m.mapTasks[i].State = InProgress
			return &m.mapTasks[i]
		}
	}

	//check if all map tasks are completed
	if !m.allMapsDone() {
		return nil
	}

	//assign reduce tasks
	for i := range m.reduceTasks {
		if m.reduceTasks[i].State == Idle {
			m.reduceTasks[i].State = InProgress
			return &m.reduceTasks[i]
		}
	}

	return nil
}

func (m *Master) allMapsDone() bool {
	for i := range m.mapTasks {
		if m.mapTasks[i].State != Completed {
			return false
		}
	}
	return true
}

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
				go m.checkAndClose()
				return
			}
		}

	default:
		return
	}
}

func (m *Master) NReduce() int {
	return m.nReduce
}

func (m *Master) NumMapTasks() int {
	return len(m.mapTasks)
}

func (m *Master) Done() <-chan struct{} {
	return m.done
}

func (m *Master) checkAndClose() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.reduceTasks {
		if m.reduceTasks[i].State != Completed {
			return
		}
	}

	close(m.done)
}
