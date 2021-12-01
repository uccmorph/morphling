package mpserverv2

import (
	"fmt"
	"morphling/mplogger"
	"strconv"
)

type ReplicaStatus struct {
	Alive       bool
	StartKeyPos uint64
	EndKeyPos   uint64
}

func (p *ReplicaStatus) KeyisIn(pos uint64) bool {
	if !p.Alive {
		return false
	}
	if p.StartKeyPos > p.EndKeyPos {
		if p.StartKeyPos <= pos && pos < defaultKeySpace {
			return true
		} else if 0 <= pos && pos < p.EndKeyPos {
			return true
		}
	} else {
		if p.StartKeyPos <= pos && pos < p.EndKeyPos {
			return true
		}
	}
	return false
}

func (p *Guidance) triDirectionCompare(other *Guidance, equal, lhsnewer, lhsolder func()) {
	if p.Epoch == other.Epoch && equal != nil {
		equal()
	} else if p.Epoch < other.Epoch && lhsolder != nil {
		lhsolder()
	} else if p.Epoch > other.Epoch && lhsnewer != nil {
		lhsnewer()
	}
}

func (p *Guidance) triDirectionCompareV2(other *Guidance, equal, lhsnewer, lhsolder func(lhs, rhs *Guidance)) {
	if p.Epoch == other.Epoch && equal != nil {
		equal(p, other)
	} else if p.Epoch < other.Epoch && lhsolder != nil {
		lhsolder(p, other)
	} else if p.Epoch > other.Epoch && lhsnewer != nil {
		lhsnewer(p, other)
	}
}

type logEntry struct {
	Epoch uint64
	Index uint64
	Cmd   interface{}
}

// This can be regarded as a sub-replica
type groupLogger struct {
	/* if KeyPos == groupLogger.position, then this key's command should be stored here.
	position is equal to the index of this groupLogger.*/
	position uint64
	commitTo uint64
	glog     []logEntry
	nextIdx  []uint64
	matchIdx []uint64
}

type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

type RaftLog struct {
	committed uint64

	applied uint64

	entries []Entry

	logStartAt uint64 // first index in `entries`
	debuglog   *mplogger.RaftLogger
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

func (l *RaftLog) showInitResult() {
	l.debuglog.Info("entries: %+v", l.entries)
}

func positionInLog(raftidx uint64) uint64 {
	return raftidx - 1
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.entries
	}

	return l.entries[positionInLog(l.applied+1):positionInLog(l.committed+1)]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
// Never return error
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// if i is verflowed, then some place must be wrong
	if i == 0 {
		return 0, nil
	}

	return l.entries[positionInLog(i)].Term, nil
}

func (l *RaftLog) lastEntry() Entry {
	if len(l.entries) == 0 {
		return Entry{}
	}

	return l.entries[len(l.entries)-1]
}

func (l *RaftLog) uncommittedEntries() []*Entry {
	return l.entriesFrom(l.committed + 1)
}

func (l *RaftLog) appendCmd(term uint64, cmd []byte) uint64 {
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
		Data:  nil,
	}
	l.entries = append(l.entries, e)

	return e.Index
}

// It's safe to call multiple times, since commit index only increment.
func (l *RaftLog) commitLogTo(idx uint64) bool {
	nextCommitIdx := min(l.lastEntry().Index, idx)
	if l.committed < nextCommitIdx {
		l.debuglog.InfoCommit("commit %v", nextCommitIdx)
		l.committed = nextCommitIdx
		return true
	}

	return false
}

func (l *RaftLog) commitAt() uint64 {
	return l.committed
}

func (l *RaftLog) truncateAndAppendCmd(term, index uint64, cmd []byte) {
	// if index is 0, then some place must be wrong
	l.entries = l.entries[:positionInLog(index)]
	residx := l.appendCmd(term, cmd)
	if residx != index {
		panic(fmt.Sprintf("after truncate, old entry and new entry should have same idx. %v -> %v", index, residx))
	}
}

func (l *RaftLog) truncateAndAppendEntries(truncateAt uint64, entries []*Entry) {
	l.entries = l.entries[:positionInLog(truncateAt)]
	for _, e := range entries {
		residx := l.appendCmd(e.Term, e.Data)
		if residx != e.Index {
			panic(fmt.Sprintf("after truncate, local entry and msg entry should have same idx. %v -> %v", residx, e.Index))
		}
	}
}

func (l *RaftLog) entriesFrom(idx uint64) []*Entry {
	if idx == 0 {
		return []*Entry{
			{},
		}
	}

	l.debuglog.Info("retrive entry from: %v, curr max: %v", idx, l.LastIndex())
	res := make([]*Entry, len(l.entries[positionInLog(idx):]))
	for i, _ := range l.entries[positionInLog(idx):] {
		res[i] = &l.entries[positionInLog(idx)+uint64(i)]
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

type Replica struct {
	localGuidance Guidance
	log           []RaftLog
	msgCh         chan *HandlerInfo
	me            int
	peers         []int
	storage       *MemStorage
	clientPending map[string]*HandlerInfo
}

func CreateReplica(g *Guidance, s *MemStorage) *Replica {
	p := &Replica{}
	p.localGuidance = *g
	p.storage = s
	go p.mainLoop()
	return p
}

func (p *Replica) mainLoop() {
	for {
		select {
		case msg := <-p.msgCh:
			if msg.IsClient {
				p.HandleClientMsg(msg)
			} else {
				p.HandleMsg(msg)
			}
		}
	}
}

func (p *Replica) GetMsgCh() chan *HandlerInfo {
	return p.msgCh
}

func (p *Replica) HandleClientMsg(msg *HandlerInfo) {
	args := msg.Cargs
	reply := msg.Creply
	guide := p.localGuidance
	// pos := calcKeyPos(args.KeyHash, guide.GroupMask, guide.GroupSize)
	switch args.Type {
	case MsgTypeGetGuidance:
		reply.Guide = &guide
		reply.Type = args.Type
		msg.Res <- msg

	case MsgTypeClientRead:
		if guide.Epoch == msg.Cargs.Guide.Epoch {
			keyStr := strconv.FormatUint(args.KeyHash, 10)
			value := p.storage.Get(CfDefault, []byte(keyStr))
			reply.Data = value
			reply.KeyHash = args.KeyHash
			reply.Guide = &guide
			reply.Type = args.Type
		} else {
			reply.Guide = &guide
		}
		msg.Res <- msg

	case MsgTypeClientProposal:
		ok := p.processClientProposal(msg.Cargs, msg.Creply)
		if ok {
			// save the context
			reqTag := GenClientTag(msg.Cargs.ClientID, msg.Cargs.Seq)
			if _, ok := p.clientPending[reqTag]; ok {
				panic(fmt.Sprintf("tag %v has already in client context saver", reqTag))
			}
			p.clientPending[reqTag] = msg
		} else {
			msg.Creply.Guide = &p.localGuidance
			msg.Res <- msg
		}
	}
}

// after send Append, should give out control to event loop
func (p *Replica) processClientProposal(args, reply *ClientMsg) bool {
	guide := &p.localGuidance
	if guide.Epoch == args.Guide.Epoch {
		pos := CalcKeyPos(args.KeyHash, guide.GroupMask, guide.GroupSize)
		log := p.log[pos]
		log.appendCmd(guide.Epoch, args.Data)

		appendMsg := &ReplicaMsg{
			Type:      MsgTypeAppend,
			To:        guide.ReplicaID(pos),
			From:      p.me,
			KeyHash:   args.KeyHash,
			Guide:     guide,
			CommitTo:  log.committed,
			PrevIdx:   log.lastEntry().Index,
			PrevEpoch: log.lastEntry().Term,
			Command:   args.Data,
		}
		p.sendAppendMsg(appendMsg)

		return true
	}
	return false
}

func (p *Replica) HandleMsg(msg *HandlerInfo) {

	switch msg.Args.Type {
	case MsgTypeAppend:
		p.processAppendEntries(msg.Args, msg.Reply)
		msg.Res <- msg
	case MsgTypeAppendReply:
		if msg.Reply.Success != replyStatusSuccess {

		}

	case MsgTypeGossip:
	}
}

func (p *Replica) processAppendEntries(args, reply *ReplicaMsg) {
	guide := &p.localGuidance
	reply.Type = MsgTypeAppendReply
	if args.Guide.Epoch != guide.Epoch {
		reply.Guide = guide
		reply.Success = replyStatusStaleGuidance
		return
	}
	pos := CalcKeyPos(args.KeyHash, guide.GroupMask, guide.GroupSize)
	log := p.log[pos]

	if log.lastEntry().Index == args.PrevIdx && log.lastEntry().Term == args.PrevEpoch {
		log.appendCmd(args.Guide.Epoch, args.Command)
		reply.Success = replyStatusSuccess
		return
	}
}

func (p *Replica) commitFromTo(log *groupLogger, start, end uint64) {}

func (p *Replica) prepareGuidanceTransfer(new *Guidance) {}

func (p *Replica) sendAppendMsg(msg *ReplicaMsg) {
	for _, peerID := range p.peers {
	}
}
