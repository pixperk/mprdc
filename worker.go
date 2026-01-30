package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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
	// step 1: read the chunk from input file
	// task.Offset = where to start reading
	// task.Size = how many bytes to read (0 means entire file)
	inputData, err := w.readChunk(task.Filename, task.Offset, task.Size)
	if err != nil {
		return err
	}

	// step 2: apply user's map function to produce key-value pairs
	// e.g. "the quick brown fox" -> [{"the","1"}, {"quick","1"}, {"brown","1"}, {"fox","1"}]
	kvs := w.mapFunc(inputData)

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

// readChunk reads a portion of a file from offset to offset+size
// if size is 0, reads the entire file
// for chunks in the middle of a file, we:
//   - skip partial line at the start (previous chunk handles it)
//   - extend to include complete line at the end
// this ensures we don't split words/lines across chunks
func (w *Worker) readChunk(filename string, offset, size int64) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// if size is 0, read entire file (no chunking)
	if size == 0 {
		data, err := io.ReadAll(file)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	// seek to the offset
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)

	// if not at start of file, skip to next newline
	// this avoids reading partial line from previous chunk
	// e.g. if previous chunk ended mid-word, we skip that partial word
	if offset > 0 {
		_, err = reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return "", err
		}
	}

	// read up to 'size' bytes, then extend to end of line
	// this ensures we get complete lines/words
	var result []byte
	bytesRead := int64(0)

	for bytesRead < size {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return "", err
		}
		result = append(result, line...)
		bytesRead += int64(len(line))
		if err == io.EOF {
			break
		}
	}

	return string(result), nil
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
