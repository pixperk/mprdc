package main

import "time"

// mapfunc takes an input of type In and produces a slice of KeyValue pairs with key of type K and value of type V
type MapFunc[In, K comparable, V any] func(In) []KeyValue[K, V]

// reducefunc takes a key of type K and a slice of values of type V and reduces them to a single value of type V
type ReduceFunc[K comparable, V any] func(K, []V) V

type KeyValue[K comparable, V any] struct {
	Key   K
	Value V
}

type TaskType string

const (
	MapTask    TaskType = "MAP"
	ReduceTask TaskType = "REDUCE"
)

type TaskState string

const (
	Idle       TaskState = "IDLE"
	InProgress TaskState = "IN_PROGRESS"
	Completed  TaskState = "COMPLETED"
)

type Task struct {
	ID        int
	Type      TaskType
	State     TaskState
	Filename  string    // for input file (map tasks only)
	StartTime time.Time // to track task start time, we use time.Time for monotonic clock

}
