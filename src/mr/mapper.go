package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
)

//
// Definition of Mapper Worker of Mapreduce.
//

// mapper output an intermediate file by taking an input file and a map function.
// 1 read file from disk, 2 perform map function, 3 shuffle and output
func mapper(mapf func(string, string) []KeyValue, task *Task) {
	DPrintf("W, Mapper, job starting, %s.", task.str())
	// Read the file
	content, err := readFile(task.FileName, task)
	if err != nil {
		DPrintf("W, Mapper, ERROR, file read error to report Fail, %s.", task.str())
		reportRes(task, Fail) // Report fail
		return
	}
	// Map
	kva := mapf(task.FileName, string(content))
	DPrintf("W, Mapper, Map finished, %s.", task.str())
	// Shuffle
	intermediates := make([][]KeyValue, task.NReducer) // Init a [][] for NReducer partitions
	for _, kv := range kva {
		reducer := ihash(kv.Key) % task.NReducer
		intermediates[reducer] = append(intermediates[reducer], kv) // Dispatch the kv by reducer
	}
	DPrintf("W, Mapper, Partition finished, %s.", task.str())
	for i, kva := range intermediates { // Iterate to save partitioned intermediate files to disk.
		ok := kvToFile(fmt.Sprintf("mr-%d-%d", task.Serial, i), kva, task)
		if !ok {
			DPrintf("W, Mapper, ERROR when writing to disk, to Report fail, %s.", task.str())
			reportRes(task, Fail) // Return fail
			return
		}
	}
	DPrintf("W, Mapper, Intermedia files finished, %s.", task.str())
	// End
	DPrintf("W, Mapper, Mapper done to report done, %s.", task.str())
	reportRes(task, Success)
}

// readFile a helper function for mapper.
// reads files and returns contents and error.
func readFile(filename string, task *Task) ([]byte, error) {
	DPrintf("W, Read file start, %s.", task.str())
	file, err := os.Open(filename)
	if err != nil {
		DPrintf("W, Read file, ERROR when open file, %s, filename : %s, error is %v",
			task.str(), filename, err)
		return nil, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		DPrintf("W, Read file, ERROR when read file content, %s, file name: %s, error is %v",
			task.str(), filename, err)
		return nil, err
	}
	defer file.Close()
	DPrintf("W, Read file Finished, %s.", task.str())
	return content, nil
}

// kvToFile a helper function for mapper.
// write kv pairs to file, return the result.
// 1 create a temp file to save the kv pairs, 2 try to rename the file
// Use these method to avoid data race because io.rename is an atomic operation.
func kvToFile(filename string, kva []KeyValue, task *Task) bool {
	tmpFile, err := os.CreateTemp("", "mapper-temp-")
	if err != nil {
		DPrintf("W, KvToFile, ERROR when create the temp file, %s, file: %s, error is %v",
			task.str(), filename, err)
		return false
	}
	defer tmpFile.Close()
	enc := json.NewEncoder(tmpFile)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			DPrintf("W, KvToFile, ERROR when write the temp file, %s, file: %s, error is %v",
				task.str(), filename, err)
			return false
		}
	}
	return atomicRename(tmpFile, filename, task)
}

// ihash a support function for mapper to identify the correct reducer worker.
// use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
