package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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

			switch task.Type {
			case MapTask:
				w.handleMapTask(task)
			case ReduceTask:
				w.handleReduceTask(task)
			}

			w.master.ReportDone(task.ID, task.Type)
		}
	}
}

func (w *Worker) handleMapTask(task *Task) error {
	//read input file
	inputData, err := os.ReadFile(task.Filename)
	if err != nil {
		return err
	}

	kvs := w.mapFunc(string(inputData))

	//partition and write intermediate files
	partitions := make(map[int][]KeyValue[string, string])
	for _, kv := range kvs {
		partition := ihash(kv.Key) % w.nReduce
		partitions[partition] = append(partitions[partition], kv)
	}

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

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *Worker) handleReduceTask(task *Task) error {
	intermediateKVs := make(map[string][]string)

	//read intermediate files
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
			intermediateKVs[kv.Key] = append(intermediateKVs[kv.Key], kv.Value)
		}
		file.Close()
	}

	//apply reduce function and write output file
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
