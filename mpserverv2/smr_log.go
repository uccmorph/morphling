package mpserverv2

import (
	"fmt"
	"morphling/mplogger"
)

type Entry struct {
	Term  uint64
	Index uint64
	Data  Command
}

type RaftLog struct {
	committed uint64

	applied uint64

	stabled uint64

	entries []Entry

	logStartAt uint64 // first index in `storage`
	debuglog   *mplogger.RaftLogger

	oldTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog() *RaftLog {
	// Your Code Here (2A).
	l := &RaftLog{}

	l.entries = []Entry{}

	l.logStartAt = 0

	return l
}

func (l *RaftLog) firstIndex() uint64 {
	return l.logStartAt
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

//
// l.storage: |-5-|-6-|-7-|
// l.entries: ....|.6.|.7.|-8-|-9-|
// In this example, 7 is l.stabled, 9 is entryIdx
// Then `entryIdx` should be in l.entries[3], that is (9 - 6)
// l.storage: |-5-|-6-|-7-|
// l.entries: ............|-8-|-9-|
// In this example, `entryIdx` should be in l.entries[1], that is (9 - 8)
// If entryIdx <= l.stable, such case should be handled outside this function

// There are times that storage is not matched with l.entries
// This may happen after truncation, like TestFollowerAppendEntries2AB
// So entries in storage are just replicated, not committed.
func (l *RaftLog) positionInRaftLog(entryIdx uint64) uint64 {

	if len(l.entries) == 0 {
		panic(fmt.Sprintf("index %v is not in l.entries", entryIdx))
	}

	return entryIdx - l.entries[0].Index
}

// unstableEntries return all the unstable entries
// don't return nil, or TestFollowerAppendEntries2AB may fail
func (l *RaftLog) unstableEntries() []Entry {
	// Your Code Here (2A).

	if l.stabled+1 > l.LastIndex() {
		return []Entry{}
	}

	return l.entries[l.positionInRaftLog(l.stabled+1):]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []Entry) {
	// Your Code Here (2A).

	// todo: what if l.applied is in storage?
	l.debuglog.Info("nextEnts: applied: %v, committed: %v", l.applied, l.committed)

	if l.applied+1 > l.LastIndex() {
		return nil
	}
	return l.entries[l.positionInRaftLog(l.applied+1):l.positionInRaftLog(l.committed+1)]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	return l.lastEntry().Index
}

// Term return the term of the entry in the given index
// Never return error
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0, nil
	}

	if i == 0 {
		return 0, nil
	}

	return l.entries[l.positionInRaftLog(i)].Term, nil
}

func (l *RaftLog) lastEntry() Entry {
	if len(l.entries) == 0 {
		return Entry{
			Term:  l.oldTerm,
			Index: l.stabled,
		}
	}

	return l.entries[len(l.entries)-1]
}

func (l *RaftLog) uncommittedEntries() []*Entry {
	return l.entriesFrom(l.committed + 1)
}

func (l *RaftLog) appendCmd(term uint64, cmd Command) uint64 {
	e := Entry{
		Term:  term,
		Index: l.LastIndex() + 1,
		Data:  cmd,
	}
	l.entries = append(l.entries, e)

	return e.Index
}

func (l *RaftLog) appendNoop(term uint64) uint64 {
	e := Entry{
		Term:  term,
		Index: l.LastIndex() + 1,
		Data:  Command{},
	}
	l.entries = append(l.entries, e)

	return e.Index
}

// It's safe to call multiple times, since commit index only increment.
func (l *RaftLog) commitLogTo(idx uint64) bool {
	nextCommitIdx := min(l.lastEntry().Index, idx)
	if l.committed < nextCommitIdx {
		l.debuglog.InfoCommit("RaftLog commit %v", nextCommitIdx)
		l.committed = nextCommitIdx
		return true
	}

	return false
}

func (l *RaftLog) commitAt() uint64 {
	return l.committed
}

func (l *RaftLog) showAllEntries() string {
	res := ""
	for i := range l.entries {
		res += fmt.Sprintf("(Term: %v, Index: %v)", l.entries[i].Term, l.entries[i].Index)
	}
	return res
}

func (l *RaftLog) showEntriesProfile() string {
	res := ""
	if len(l.entries) == 0 {
		return res
	}
	res += fmt.Sprintf("First: (Term: %v, Index: %v)|", l.entries[0].Term, l.entries[0].Index)
	res += fmt.Sprintf("Last: (Term: %v, Index: %v)", l.lastEntry().Term, l.lastEntry().Index)

	return res
}

// truncate include `truncateAt` entry
func (l *RaftLog) truncateAndAppendEntries(truncateAt uint64, entries []*Entry) {
	// if index <= l.stabled, then some place must be wrong
	if truncateAt <= l.stabled {
		l.debuglog.Error("truncate to %v, but stabled index is %v. Decide what to do next...", truncateAt, l.stabled)
		l.stabled = truncateAt - 1
	}
	if truncateAt <= l.committed {
		panic(fmt.Sprintf("truncate to %v, but has committed to %v", truncateAt, l.committed))
	}
	l.entries = l.entries[:l.positionInRaftLog(truncateAt)]

	l.debuglog.Info("after truncate, entries: %v", l.showEntriesProfile())

	for _, e := range entries {
		residx := l.appendCmd(e.Term, e.Data)
		if residx != e.Index {
			panic(fmt.Sprintf("after truncate, local entry and msg entry should have same idx. %v -> %v", residx, e.Index))
		}
	}
}

// return nil if idx > l.LastIndex(), or inner error
// Since l.entries preload storage, we only need to get entries from l.entries
func (l *RaftLog) entriesFrom(idx uint64) []*Entry {
	l.debuglog.InfoDeep("retrive entry from: %v, curr stable: %v, last: %v", idx, l.stabled, l.LastIndex())
	if idx > l.LastIndex() {
		return nil
	}

	if len(l.entries) == 0 {
		return nil
	}

	totalNums := l.LastIndex() - idx + 1

	res := make([]*Entry, 0, totalNums)

	// if idx <= l.stabled {
	// 	part1, err := l.storage.Entries(idx, l.stabled+1)
	// 	if err != nil {
	// 		l.debuglog.Error("get entries form storage error: %v", err)
	// 		return nil
	// 	}
	// 	for i := range part1 {
	// 		res = append(res, &part1[i])
	// 	}
	// 	for i := range l.entries {
	// 		res = append(res, &l.entries[i])
	// 	}
	// 	return res
	// }

	for i := range l.entries[l.positionInRaftLog(idx):] {
		l.debuglog.Info("i = %v, e = %+v", i, l.entries[l.positionInRaftLog(idx)+uint64(i)])
		res = append(res, &l.entries[l.positionInRaftLog(idx)+uint64(i)])
	}
	return res
}

func (l *RaftLog) replaceEntry(entry *Entry) uint64 {
	if entry.Term == 0 {
		l.entries = make([]Entry, 1)
		l.entries[0] = *entry
		return entry.Index
	}
	return l.appendCmd(entry.Term, entry.Data)
}

func (l *RaftLog) updateApply(entries []Entry) {
	if len(entries) == 0 {
		return
	}
	l.applied = entries[len(entries)-1].Index
}

func (l *RaftLog) updateStable(entries []Entry) {
	if len(entries) == 0 {
		return
	}
	l.stabled = entries[len(entries)-1].Index
}
