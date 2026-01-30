package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"time"
)

type Worker struct {
	id         int
	mapFunc    MapFunc[string, string, string]
	reduceFunc ReduceFunc[string, string]
	master     *Master
	nReduce    int
}

func NewWorker(id int, mapF MapFunc[string, string, string], reduceF ReduceFunc[string, string], master *Master) *Worker {
	return &Worker{
		id:         id,
		mapFunc:    mapF,
		reduceFunc: reduceF,
		master:     master,
		nReduce:    master.NReduce(),
	}
}

func (w *Worker) Run() {
	for {
		select {
		case <-w.master.Done():
			return
		default:
			task := w.master.AssignTask()
			if task == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			var err error
			switch task.Type {
			case MapTask:
				err = w.handleMapTask(task)
			case ReduceTask:
				err = w.handleReduceTask(task)
			}

			if err != nil {
				log.Printf("worker %d: task %d (%v) failed: %v", w.id, task.ID, task.Type, err)
				w.master.ReportFailed(task.ID, task.Type)
				continue
			}

			w.master.ReportDone(task.ID, task.Type)
		}
	}
}

func (w *Worker) handleMapTask(task *Task) error {
	// step 1: read input file
	// e.g. task.Filename = "doc1.txt" containing "the quick brown fox"
	inputData, err := os.ReadFile(task.Filename)
	if err != nil {
		return err
	}

	// step 2: apply user's map function to produce key-value pairs
	// e.g. "the quick brown fox" -> [{"the","1"}, {"quick","1"}, {"brown","1"}, {"fox","1"}]
	kvs := w.mapFunc(string(inputData))

	// step 3: partition kvs into nReduce buckets using hash(key) % nReduce
	// this ensures same key always goes to same reduce task
	// e.g. hash("the") % 3 = 0, so "the" goes to partition 0
	partitions := make(map[int][]KeyValue[string, string])
	for _, kv := range kvs {
		partition := ihash(kv.Key) % w.nReduce
		partitions[partition] = append(partitions[partition], kv)
	}

	// step 4: write each partition to intermediate file
	// filename format: mr-{mapTaskID}-{partition}
	// e.g. map task 0 writes: mr-0-0, mr-0-1, mr-0-2 (one per partition)
	// this is writing ROWS of the intermediate file matrix
	for partition, pkvs := range partitions {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", task.ID, partition)
		file, err := os.Create(intermediateFilename)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(file)
		for _, kv := range pkvs {
			if err := encoder.Encode(&kv); err != nil {
				file.Close()
				return err
			}
		}
		file.Close()
	}

	return nil
}

// ihash returns a deterministic hash for a key
// used to assign keys to reduce partitions: hash(key) % nReduce
// same key always produces same hash -> same partition -> same reduce task
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *Worker) handleReduceTask(task *Task) error {
	// step 1: collect all key-value pairs for this partition
	// task.ID is the partition number (0, 1, 2, ...)
	intermediateKVs := make(map[string][]string)

	// step 2: read intermediate files from ALL map tasks for this partition
	// filename format: mr-{mapTaskID}-{partition}
	// e.g. reduce task 0 reads: mr-0-0, mr-1-0, mr-2-0 (COLUMN of the matrix)
	// this gathers all keys that hashed to partition 0 from every map task
	for i := 0; i < w.master.NumMapTasks(); i++ {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", i, task.ID)
		file, err := os.Open(intermediateFilename)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue[string, string]
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			// step 3: group values by key
			// e.g. {"the": ["1","1","1"], "fox": ["1","1"]}
			intermediateKVs[kv.Key] = append(intermediateKVs[kv.Key], kv.Value)
		}
		file.Close()
	}

	// step 4: apply user's reduce function to each key
	// e.g. reduceF("the", ["1","1","1"]) -> "3"
	outputFilename := fmt.Sprintf("mr-out-%d", task.ID)
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	for key, values := range intermediateKVs {
		reducedValue := w.reduceFunc(key, values)
		fmt.Fprintf(outputFile, "%s %s\n", key, reducedValue)
	}

	return nil
}
