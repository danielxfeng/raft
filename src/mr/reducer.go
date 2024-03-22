package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

//
// Definition of Reducer Worker of Mapreduce.
//

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// MaxInt return the lager value of a, b.
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// reducer the worker of reducer
// 1 shuffle: Merge and Sort 2 reduce
func reducer(reducef func(string, []string) string, task *Task) {
	DPrintf("W, Reducer, job starting, %s", task.str())
	// Merge
	kva := []KeyValue{}
	for _, filename := range task.FileNames { // Iterate each partition files to merge the kv.
		ok := fileToKV(filename, &kva, task)
		if !ok {
			DPrintf("W, Reducer, Error when parsing files to Report fail, %s.", task.str())
			reportRes(task, Fail)
			return
		}
	}
	// Sort
	sort.Sort(ByKey(kva))
	// Reduce and Output
	ok := reducerHelper(reducef, kva, task)
	if !ok {
		DPrintf("W, Reducer, ERROR when ReduceHelper to Report fail, %s.", task.str())
		reportRes(task, Fail)
		return
	}
	DPrintf("W, Reducer Finished to Report success, %s.", task.str())
	reportRes(task, Success)
}

// fileToKV a helper function for reducer.
// Read kv pairs to intermediate file, return the result.
func fileToKV(filename string, kva *[]KeyValue, task *Task) bool {
	DPrintf("W, FileToKV, open file. %s, file name: %s", task.str(), filename)
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return true
		}
		DPrintf("W, FileToKV, ERROR, open file error, %s, file name is %s, error is %v",
			task.str(), filename, err)
		return false
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			DPrintf("W, FileToKV, ERROR, read kv from file error, %s, file name is %s, error is %v",
				task.str(), filename, err)
			return false
		}
		*kva = append(*kva, kv)
	}
	return true
}

// reducerHelper a helper function for reducer.
// 1 perform the reducer function 2 write to a temp file 3 atomic rename
func reducerHelper(reducef func(string, []string) string, kva []KeyValue, task *Task) bool {
	filename := fmt.Sprintf("mr-out-%d", task.Serial)
	tmpFile, err := os.CreateTemp("", "reduce-temp-")
	if err != nil {
		DPrintf("W, ReduceHelper, ERROR when create the temp file, %s, file name: %s, err: %s",
			task, filename, err)
		return false
	}
	defer tmpFile.Close()
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)
		if err != nil {
			DPrintf("W, ReduceHelper, ERROR when write to the temp file, %s, "+
				"file name: %s, err: %s", task.str(), filename, err)
			return false
		}
		i = j
	}
	return atomicRename(tmpFile, filename, task)
}
