# mprdc

a simple mapreduce implementation in go, built for learning.

based on the [google mapreduce paper](https://research.google/pubs/pub62/) by jeffrey dean and sanjay ghemawat (2004).

## what is mapreduce?

mapreduce is a programming model for processing large datasets in parallel across a cluster. the idea is simple:

1. **map**: transform input data into key-value pairs
2. **shuffle**: group all values by key
3. **reduce**: combine values for each key into a final result

```mermaid
flowchart LR
    subgraph map phase
        I1[input 1] --> M1[map]
        I2[input 2] --> M2[map]
        I3[input 3] --> M3[map]
    end

    subgraph shuffle
        M1 --> S{partition<br/>by key}
        M2 --> S
        M3 --> S
    end

    subgraph reduce phase
        S --> R1[reduce 0]
        S --> R2[reduce 1]
        S --> R3[reduce 2]
    end

    R1 --> O1[output 0]
    R2 --> O2[output 1]
    R3 --> O3[output 2]
```

## architecture

the system uses a master-worker pattern:

```mermaid
flowchart TB
    subgraph master
        TQ[task queue]
        TS[task state tracking]
        TC[timeout checker]
    end

    subgraph workers
        W1[worker 1]
        W2[worker 2]
        W3[worker 3]
        W4[worker 4]
    end

    W1 <-->|assign task<br/>report done| master
    W2 <-->|assign task<br/>report done| master
    W3 <-->|assign task<br/>report done| master
    W4 <-->|assign task<br/>report done| master
```

- **master**: holds all tasks, assigns them to workers, tracks state
- **workers**: request tasks, execute map/reduce, report completion

## task lifecycle

each task goes through these states:

```mermaid
stateDiagram-v2
    [*] --> idle
    idle --> in_progress: assigned to worker
    in_progress --> completed: worker reports done
    in_progress --> idle: timeout or failure
    completed --> [*]
```

## data flow (word count example)

```mermaid
flowchart TB
    subgraph input files
        F1["doc1.txt<br/>'the quick brown fox'"]
        F2["doc2.txt<br/>'the fox is quick'"]
    end

    subgraph map phase
        F1 --> M1["map task 0"]
        F2 --> M2["map task 1"]
        M1 --> KV1["(the,1) (quick,1)<br/>(brown,1) (fox,1)"]
        M2 --> KV2["(the,1) (fox,1)<br/>(is,1) (quick,1)"]
    end

    subgraph intermediate files
        KV1 --> |"hash(key) % R"| IF
        KV2 --> |"hash(key) % R"| IF
        IF["mr-0-0, mr-0-1, mr-0-2<br/>mr-1-0, mr-1-1, mr-1-2"]
    end

    subgraph reduce phase
        IF --> R0["reduce 0<br/>reads mr-*-0"]
        IF --> R1["reduce 1<br/>reads mr-*-1"]
        IF --> R2["reduce 2<br/>reads mr-*-2"]
    end

    subgraph output
        R0 --> O0["mr-out-0<br/>the 2"]
        R1 --> O1["mr-out-1<br/>fox 2, quick 2"]
        R2 --> O2["mr-out-2<br/>brown 1, is 1"]
    end
```

## intermediate file naming

the key insight is how intermediate files are organized:

```
         reduce partitions
              0   1   2
           ┌───┬───┬───┐
map task 0 │0-0│0-1│0-2│  ← map writes ROWS
map task 1 │1-0│1-1│1-2│
map task 2 │2-0│2-1│2-2│
           └───┴───┴───┘
             ↓   ↓   ↓
           reduce reads COLUMNS
```

- map task `m` writes to `mr-m-0`, `mr-m-1`, `mr-m-2` (one per partition)
- reduce task `r` reads `mr-0-r`, `mr-1-r`, `mr-2-r` (from all map tasks)

## features implemented

### from the paper

| feature | status | description |
|---------|--------|-------------|
| map/reduce | ✅ | core paradigm |
| partitioning | ✅ | `hash(key) % nReduce` |
| master-worker | ✅ | coordinator pattern |
| task timeouts | ✅ | detect crashed workers |
| task retry | ✅ | reschedule failed tasks |
| file splitting | ✅ | large files → multiple map tasks |

### additional

| feature | description |
|---------|-------------|
| exponential backoff | failed tasks wait before retry (1s → 2s → 4s...) |
| jitter | randomized delays to prevent thundering herd |
| line alignment | chunks don't split words/lines |

## fault tolerance

```mermaid
sequenceDiagram
    participant M as master
    participant W1 as worker 1
    participant W2 as worker 2

    M->>W1: assign task 0
    Note over W1: starts working...
    Note over W1: crashes!

    loop timeout checker
        M->>M: task 0 exceeded timeout
        M->>M: reset to idle
    end

    W2->>M: request task
    M->>W2: assign task 0
    W2->>M: report done
    M->>M: mark completed
```

## usage

```go
// define your map function
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

// define your reduce function
reduceF := func(key string, values []string) string {
    return strconv.Itoa(len(values))
}

// run mapreduce
files := []string{"data/doc1.txt", "data/doc2.txt"}
results := RunMapReduce(files, 3, 4, mapF, reduceF)
//                      ↑      ↑  ↑
//                      │      │  └── 4 workers
//                      │      └───── 3 reduce partitions
//                      └──────────── input files
```

## run

```bash
go run .
```

output:
```
Word counts:
  the: 5
  fox: 2
  quick: 2
  ...
```

## project structure

```
mprdc/
├── main.go      # orchestration, word count example
├── master.go    # task assignment, timeout detection, backoff
├── worker.go    # map/reduce execution, file i/o
├── types.go     # Task, KeyValue, MapFunc, ReduceFunc
└── README.md
```

## references

- [mapreduce: simplified data processing on large clusters](https://research.google/pubs/pub62/) - the original paper
- [mit 6.824](https://pdos.csail.mit.edu/6.824/) - distributed systems course with mapreduce lab
