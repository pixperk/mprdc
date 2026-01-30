package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	createTestFiles()

	files := []string{"data/doc1.txt", "data/doc2.txt", "data/doc3.txt"}

	// Word count map function
	mapF := func(content string) []KeyValue[string, string] {
		words := strings.Fields(content)
		kvs := make([]KeyValue[string, string], 0, len(words))
		for _, word := range words {
			kvs = append(kvs, KeyValue[string, string]{
				Key:   strings.ToLower(word),
				Value: "1",
			})
		}
		return kvs
	}

	// Word count reduce function
	reduceF := func(key string, values []string) string {
		return strconv.Itoa(len(values))
	}

	// Run MapReduce and get results
	results := RunMapReduce(files, 3, 4, mapF, reduceF)

	fmt.Println("Word counts:")
	for word, count := range results {
		fmt.Printf("  %s: %s\n", word, count)
	}
}

// RunMapReduce orchestrates the entire job and returns results
func RunMapReduce(
	files []string,
	nReduce int,
	nWorkers int,
	mapF MapFunc[string, string, string],
	reduceF ReduceFunc[string, string],
) map[string]string {
	// chunkSize: 64MB (set to 0 to disable splitting)
	// for small test files, this means 1 task per file
	// for large files (>64MB), they get split into multiple tasks
	chunkSize := int64(64 * 1024 * 1024) // 64MB

	master := NewMaster(files, nReduce, chunkSize, 10*time.Second)

	// Spawn workers
	var wg sync.WaitGroup
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker := NewWorker(id, mapF, reduceF, master)
			worker.Run()
		}(i)
	}

	wg.Wait()

	// Collect results
	results := collectResults(nReduce)

	// Clean up - use NumMapTasks() because splitting may create more tasks than files
	cleanupIntermediateFiles(master.NumMapTasks(), nReduce)
	cleanupOutputFiles(nReduce)

	return results
}

func cleanupOutputFiles(nReduce int) {
	for r := 0; r < nReduce; r++ {
		os.Remove(fmt.Sprintf("mr-out-%d", r))
	}
}

func collectResults(nReduce int) map[string]string {
	results := make(map[string]string)

	for r := 0; r < nReduce; r++ {
		filename := fmt.Sprintf("mr-out-%d", r)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), " ", 2)
			if len(parts) == 2 {
				results[parts[0]] = parts[1]
			}
		}
		file.Close()
	}

	return results
}

func cleanupIntermediateFiles(nMap, nReduce int) {
	for m := 0; m < nMap; m++ {
		for r := 0; r < nReduce; r++ {
			os.Remove(fmt.Sprintf("mr-%d-%d", m, r))
		}
	}
}

func createTestFiles() {
	os.MkdirAll("data", 0755)

	files := map[string]string{
		"data/doc1.txt": "the quick brown fox jumps over the lazy dog",
		"data/doc2.txt": "the fox is quick and the dog is lazy",
		"data/doc3.txt": "hello world the world is beautiful",
	}

	for path, content := range files {
		os.WriteFile(path, []byte(content), 0644)
	}
}
