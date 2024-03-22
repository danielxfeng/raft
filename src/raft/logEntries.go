package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
)

//
// Definition of Logs.
//

var ErrIndexOutOfBounds = errors.New("index out of bounds")
var ErrLogInSnapShot = errors.New("log is in snapshot")

// LogItem a single log
type LogItem struct {
	Term    int         // The votedFor of a log
	Command interface{} // The command of a log
}

// String return the string of LogItem
func (li LogItem) String() string {
	return fmt.Sprintf("t-%d, c-%s", li.Term, FirstFifteenChars(li.Command))
}

// init Register the struct.
func init() {
	gob.Register(LogItem{})
	gob.Register([]LogItem{})
}

// Logs The struct of Logs.
type Logs struct {
	LogsSlice     []LogItem // The slice of LogsSlice.
	SnapshotTerm  int       // The term of the last log in Snapshot.
	SnapshotIndex int       // The index of the last log in Snapshot.
}

// String return empty string by default for shorter the log.
func (ls *Logs) String() string {
	return fmt.Sprintf("SnapTerm: %d, SnapIdx: %d, l: %d", ls.SnapshotTerm, ls.SnapshotIndex, len(ls.LogsSlice))
}

// NewLogEntries the constructor of Logs.
func NewLogEntries() *Logs {
	logs := make([]LogItem, 0)
	return &Logs{LogsSlice: logs, SnapshotIndex: 0, SnapshotTerm: 0}
}

// Len return the length of Logs
func (ls *Logs) Len() int {
	return len(ls.LogsSlice) + ls.SnapshotIndex
}

// Get return the log by given index.
// Transferred the 1-based index in raft to 0-based index in Golang Slice.
func (ls *Logs) Get(idx int) (*LogItem, error) {
	if idx > ls.Len() {
		return nil, ErrIndexOutOfBounds
	}
	if idx < 1 {
		return &LogItem{0, ""}, nil
	}
	if idx <= ls.SnapshotIndex {
		return nil, ErrLogInSnapShot
	}
	return &ls.LogsSlice[idx-ls.SnapshotIndex-1], nil
}

// GetTerm return the term of log by given index.
// Transferred the 1-based index in raft to 0-based index in Golang Slice.
func (ls *Logs) GetTerm(idx int) (int, error) {
	if idx > ls.Len() {
		return IntNil, ErrIndexOutOfBounds
	}
	if idx < 1 {
		return 0, nil
	}
	if idx < ls.SnapshotIndex {
		return IntNil, ErrLogInSnapShot
	}
	if idx == ls.SnapshotIndex {
		return ls.SnapshotTerm, nil
	}
	logEntry, err := ls.Get(idx)
	if errors.Is(err, ErrLogInSnapShot) {
		return ls.SnapshotTerm, nil
	}
	return logEntry.Term, nil
}

// GetMany return the LogsSlice by given index.
// Transferred the 1-based index in raft to 0-based index in Golang Slice.
func (ls *Logs) GetMany(startIdx int) ([]LogItem, error) {
	if startIdx < 1 || startIdx > ls.Len() {
		return nil, ErrIndexOutOfBounds
	}
	if startIdx <= ls.SnapshotIndex {
		return nil, ErrLogInSnapShot
	}
	return ls.LogsSlice[startIdx-ls.SnapshotIndex-1:], nil
}

// GetLastLogInfo return the VoteFor and Index of the last log.
func (ls *Logs) GetLastLogInfo() (int, int) {
	idx := ls.Len()
	if idx == ls.SnapshotIndex {
		return ls.SnapshotTerm, ls.SnapshotIndex
	}
	last, _ := ls.Get(idx)
	return last.Term, idx
}

// AppendCommand Append a new command by given votedFor and command, for leader appending client request only.
func (ls *Logs) AppendCommand(term int, cmd interface{}) {
	ls.LogsSlice = append(ls.LogsSlice, LogItem{Term: term, Command: cmd})
}

// AppendLogs Append a slice of LogItem to given idx.
func (ls *Logs) AppendLogs(startIdx int, pendingLogs *[]interface{}) {
	lastIdx := ls.Len()
	if startIdx < 1 {
		return
	}
	if startIdx > lastIdx+1 || startIdx <= ls.SnapshotIndex {
		log.Fatal("Append log error, start position is out of index.")
	}
	truncated := false
	var existingLog *LogItem
	var err error
	for i, le := range *pendingLogs { // Assemble the LogEntry
		pendingLog := le.(LogItem)
		if !truncated {
			existingLog, err = ls.Get(i + startIdx)
			// "3. If an existing entry conflicts with a new one (same index but different terms), delete
			// the existing entry and all that follow it (§5.3)"
			if err == nil && existingLog.Term != pendingLog.Term {
				ls.LogsSlice = ls.LogsSlice[:startIdx+i-ls.SnapshotIndex-1]
				truncated = true
			}
		}
		if truncated || errors.Is(err, ErrIndexOutOfBounds) { // "4. Append any new entries not already in the log"
			ls.LogsSlice = append(ls.LogsSlice, pendingLog) // Transfer from interface to LogEntry.
		}
	}
}

// FindLatestMatchingTermLog iterate to find out the latest matching term log.
func (ls *Logs) FindLatestMatchingTermLog(unMatchedIdx int, term int) (int, error) {
	founded := false
	for idx := unMatchedIdx - 1; idx > ls.SnapshotIndex; idx-- {
		matchedTerm, _ := ls.GetTerm(idx)
		if founded && matchedTerm != term {
			return idx + 2, nil
		} else if !founded && matchedTerm == term {
			founded = true
		}
	}
	if ls.SnapshotIndex > 0 {
		return 1, ErrLogInSnapShot
	}
	return ls.SnapshotIndex + 1, nil
}

// ApplySnapshot update last index term and last index of the snapshot, also trim the logs.
// "Discard the entire log"
func (ls *Logs) ApplySnapshot(index int, term int, me int) bool {
	if index <= ls.SnapshotIndex {
		DPrintf("Raft-%d, Apply snap, duplicated/outdated index: %d, snapindex: %d", me, index, ls.SnapshotIndex)
		return false
	}
	matchedLog, _ := ls.Get(index)
	if index >= ls.Len() || term != IntNil && term != matchedLog.Term {
		ls.LogsSlice = make([]LogItem, 0)
	} else {
		ls.LogsSlice = ls.LogsSlice[index-ls.SnapshotIndex:]
	}
	if term != IntNil {
		ls.SnapshotTerm = term
	} else {
		ls.SnapshotTerm = matchedLog.Term
	}
	ls.SnapshotIndex = index
	DPrintf("Raft-%d, Snap Failed, unmatched term, index: %d, term: %d", me, index, term)
	return true
}
