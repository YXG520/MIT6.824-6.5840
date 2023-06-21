package raft

import (
	"fmt"
)

// import "6.824/labgob"
type Entry struct {
	Term    int
	Command interface{}
}
type Log struct {
	Entries       []Entry
	FirstLogIndex int
	LastLogIndex  int
}

func NewLog() *Log {
	return &Log{
		Entries:       make([]Entry, 0),
		FirstLogIndex: 1,
		LastLogIndex:  0,
	}
}
func (log *Log) getRealIndex(index int) int {
	return index - log.FirstLogIndex
}
func (log *Log) getOneEntry(index int) *Entry {

	return &log.Entries[log.getRealIndex(index)]
}

func (log *Log) appendL(newEntries ...Entry) {
	log.Entries = append(log.Entries[:log.getRealIndex(log.LastLogIndex)+1], newEntries...)
	log.LastLogIndex += len(newEntries)

}

// 这是按照相对FirstLogIndex的偏移量来取日志的
func (log *Log) getAppendEntries(start int) []Entry {
	ret := append([]Entry{}, log.Entries[log.getRealIndex(start):log.getRealIndex(log.LastLogIndex)+1]...)
	return ret
}
func (log *Log) String() string {
	if log.empty() {
		return "logempty"
	}
	return fmt.Sprintf("%v", log.getAppendEntries(log.FirstLogIndex))
}
func (log *Log) empty() bool {
	return log.FirstLogIndex > log.LastLogIndex
}
func (rf *Raft) GetLogEntries() []Entry {
	return rf.log.Entries
}
func (rf *Raft) getEntryTerm(index int) int {
	if index == 0 {
		return 0
	}
	if index == rf.log.FirstLogIndex-1 {
		return rf.snapshotLastIncludeTerm
	}
	if rf.log.FirstLogIndex <= rf.log.LastLogIndex {
		return rf.log.getOneEntry(index).Term
	}

	//DPrintf(999, "invalid index=%v in getEntryTerm rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v\n", index, rf.log.FirstLogIndex, rf.log.LastLogIndex)
	return -1
}

//func (rf *Raft) getEntryTerm(index int) int {
//	if index == 0 {
//		return 0
//	}
//	if index == rf.log.FirstLogIndex-1 {
//		return rf.snapshotLastIncludeTerm
//	}
//	if rf.log.FirstLogIndex <= rf.log.LastLogIndex {
//		return rf.log.Entry(index).Term
//	}
//
//	DPrintf(999, "invalid index=%v in getEntryTerm rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v\n", index, rf.log.FirstLogIndex, rf.log.LastLogIndex)
//	return -1
//}

// func main() {
// 	log := NewLog()
// 	log.appendL(Entry{233, 10001})

// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)
// 	// e.Encode(rf.xxx)
// 	// e.Encode(rf.yyy)
// 	e.Encode(log)
// 	data := w.Bytes()
// 	r := bytes.NewBuffer(data)
// 	d := labgob.NewDecoder(r)
// 	var log2 *Log
// 	if d.Decode(&log2) != nil {
// 		//   error...
// 		fmt.Printf("error\n")
// 	} else {
// 		fmt.Println(log2)
// 	}
// }
