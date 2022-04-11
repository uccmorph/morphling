// raft_core.go only contains replication logic.
// replica role and election is removed.
package mpserverv2

import (
	"fmt"
	"morphling/mplogger"
)

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

func (p *Progress) String() string {
	return fmt.Sprintf("{Match: %v, Next: %v}", p.Match, p.Next)
}

type SMR struct {
	id int

	Term uint64
	Vote uint64

	// the log
	SMRLog *SMRLog

	// log replication progress of each peers
	Prs map[int]*Progress

	// msgs need to send
	msgs []ReplicaMsg

	// Other replicas. Self is not included
	peers  []int
	quorum int

	entriesVotes map[uint64]map[int]bool // map[index]{map[replica]{voted}}
	debuglog     *mplogger.RaftLogger

	replyClient func()
}

// newSMR return a raft peer with the given config
func newSMR(id int, peers []int, debugLogger *mplogger.RaftLogger) *SMR {

	r := &SMR{}
	r.id = id

	r.msgs = make([]ReplicaMsg, 0)
	r.Term = 0

	r.SMRLog = newLog()
	r.Prs = make(map[int]*Progress)

	if len(peers) == 0 {
		panic("no peers")
	} else {
		r.peers = make([]int, 0, len(peers)-1)
		for _, id := range peers {
			if id != r.id {
				r.peers = append(r.peers, id)
			}
		}
	}
	for _, id := range r.peers {
		r.Prs[id] = &Progress{}
	}
	r.Prs[r.id] = &Progress{}
	r.quorum = (len(r.peers)+1)/2 + 1
	r.entriesVotes = make(map[uint64]map[int]bool)

	r.debuglog = debugLogger
	r.debuglog.SetContext("", r.Term, r.id)
	r.SMRLog.debuglog = r.debuglog

	return r
}

func (r *SMR) ChangeTerm(term uint64) {
	r.Term = term
}

func (r *SMR) sendAppend(to int) bool {

	// todo: send entries according to r.Prs
	if r.Prs[to].Next == r.SMRLog.firstIndex() {
		r.Prs[to].Next += 1
	}
	entries := r.SMRLog.entriesFrom(r.Prs[to].Next)
	if entries == nil || len(entries) == 0 {
		return false
	}
	r.Prs[to].Next = entries[len(entries)-1].Index
	var logterm uint64
	if entries[0].Term == 0 {
		logterm = 0
	} else {
		logterm, _ = r.SMRLog.Term(entries[0].Index - 1)

	}
	msg := ReplicaMsg{
		Type:      MsgTypeAppend,
		To:        to,
		From:      r.id,
		CommitTo:  r.SMRLog.commitAt(),
		PrevEpoch: logterm,
		PrevIdx:   entries[0].Index - 1,
		Entries:   entries,
	}

	r.msgs = append(r.msgs, msg)

	r.debuglog.DebugSendAppend("send msg: %+v", msg)
	return false
}

func (r *SMR) forAllPeers(do func(id int)) {
	for _, id := range r.peers {
		do(id)
	}
}

func (r *SMR) readNextMsg() []ReplicaMsg {
	msgs := r.msgs
	r.msgs = make([]ReplicaMsg, 0)
	return msgs
}

func (r *SMR) Step(m ReplicaMsg) error {
	switch m.Type {
	case MsgTypeAppend:
		r.handleAppendEntries(m)
	case MsgTypeAppendReply:
		r.msgs = r.leaderStepAppendResp(m)

	case MsgTypeClientProposal:
		for _, entry := range m.Entries {
			r.leaderRecordLocal(entry)
		}

		r.debuglog.Info("all progress: %+v", r.Prs)
		r.Prs[r.id].Match = r.SMRLog.LastIndex()
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
		r.forAllPeers(func(id int) {
			r.Prs[id].Match = r.Prs[r.id].Match - 1
			r.Prs[id].Next = r.Prs[r.id].Next - 1
			r.sendAppend(id)
		})

	}
	return nil

}

func (r SMR) leaderStepAppendResp(m ReplicaMsg) []ReplicaMsg {
	r.debuglog.Info("Prs of server %v: %+v", m.From, r.Prs[m.From])
	r.debuglog.DebugVote("server %v vote [%v] for entry %v", m.From, m.Success, m.PrevIdx)
	if m.Success == ReplyStatusSuccess && m.PrevIdx > r.SMRLog.commitAt() {
		// if index.Term != r.Term, then index.entry is not belong to current leader.
		indexTerm, _ := r.SMRLog.Term(m.PrevIdx)
		if indexTerm == r.Term {
			r.debuglog.Info("entriesVotes %v: %+v", m.PrevIdx, r.entriesVotes[m.PrevIdx])
			idxVotes := r.entriesVotes[m.PrevIdx]
			idxVotes[m.From] = true
			r.entriesVotes[m.PrevIdx] = idxVotes
			r.debuglog.DebugVote("entry %v: count: %v", m.PrevIdx, len(r.entriesVotes[m.PrevIdx]))
		}
	}
	// when transmission time is larger than processing time, then multiple same reject may come in.
	// many same heartbeat will be generated, causing a congestion
	// This situation may happen in any AppendEntries RPC.
	// Let's control that within a single tick, only 2~5 same AppendEntries RPC can be sent.
	if m.Success != ReplyStatusSuccess {
		if m.PrevIdx <= r.Prs[m.From].Next {
			r.Prs[m.From].Next = m.PrevIdx - 1
		}
		r.Prs[m.From].Match = 0
		// optimistic guessing this heartbeat still rejected.
		r.Prs[m.From].Next -= 1
	} else {
		if r.Prs[m.From].Match == 0 {
			if m.PrevIdx == 0 {
				// maybe useless, see heartbeat tick for detail
				r.Prs[m.From].Next = r.SMRLog.firstIndex() + 1
			} else {
				r.Prs[m.From].Next = m.PrevIdx + 1
			}
			r.sendAppend(m.From)
		} else {
			if m.PrevIdx >= r.Prs[m.From].Next {
				r.Prs[m.From].Next = m.PrevIdx + 1
			}
		}
		if m.PrevIdx > r.Prs[m.From].Match {
			r.Prs[m.From].Match = m.PrevIdx
		}
	}
	r.leaderCommitTo(m.PrevIdx)

	return r.msgs
}

// make sure only commit once
func (r *SMR) leaderCommitTo(idx uint64) {
	if len(r.entriesVotes[idx]) == r.quorum {
		r.debuglog.InfoCommit("gather enough votes for entry %v", idx)
		r.SMRLog.commitLogTo(idx)
	}
}

// only when idx:term match current term, entriesVotes will have corresponding voting place.
func (r *SMR) leaderRecordLocal(entry *Entry) {
	// lastIdx := r.SMRLog.LastIndex()
	// entry.Index = lastIdx + 1
	entry.Term = r.Term
	// idx := r.SMRLog.replaceEntry(entry)
	idx := r.SMRLog.appendCmd(entry.Term, entry.Data)
	idxVotes := make(map[int]bool)
	idxVotes[r.id] = true
	r.entriesVotes[idx] = idxVotes
	if len(r.entriesVotes[idx]) >= r.quorum {
		r.SMRLog.commitLogTo(idx)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *SMR) handleAppendEntries(m ReplicaMsg) {

	reply := ReplicaMsg{
		Type:    MsgTypeAppendReply,
		To:      m.From,
		From:    r.id,
		Success: ReplyStatusSuccess,
		PrevIdx: m.Entries[0].Index,
	}

	// if accept, stale := r.safetyCheck(m.PrevEpoch, m.PrevIdx); !accept {
	// 	r.debuglog.Error("mismatch entry at: %v", m.PrevIdx)
	// 	reply.Success = replyStatusMissingEntries
	// 	reply.PrevIdx = m.PrevIdx
	// 	goto SEND_REPLY
	// } else {
	// 	// should truncate when m.entries are not fully matched
	// 	if stale {
	// 		if len(m.Entries) > 0 {
	// 			r.HandleStaleEntries(m)
	// 			reply.PrevIdx = r.SMRLog.LastIndex()
	// 		} else {
	// 			reply.PrevIdx = m.PrevIdx
	// 			// reply.Index = r.SMRLog.LastIndex()
	// 		}
	// 	} else {
	// 		for _, e := range m.Entries {
	// 			r.SMRLog.appendCmd(e.Term, e.Data)
	// 		}
	// 		reply.PrevIdx = r.SMRLog.LastIndex()
	// 	}
	// }
	// r.debuglog.Info("last index: %v, msg commit: %v", r.SMRLog.LastIndex(), m.CommitTo)
	// // Should commit and apply in some place
	// if reply.Success == ReplyStatusSuccess && m.CommitTo > r.SMRLog.commitAt() {
	// 	idx := min(reply.PrevIdx, m.CommitTo)
	// 	r.SMRLog.commitLogTo(idx)
	// }

	// SEND_REPLY:
	r.debuglog.DebugReply("reply append: %+v", reply)
	r.msgs = append(r.msgs, reply)
}

func (r *SMR) HandleStaleEntries(m ReplicaMsg) {
	lastIndex := r.SMRLog.LastIndex()
	for i, mentry := range m.Entries {
		if mentry.Index > lastIndex {
			// msg has more entries that local doesn't have
			r.SMRLog.truncateAndAppendEntries(mentry.Index, m.Entries[i:])
			break
		}
		localTerm, _ := r.SMRLog.Term(mentry.Index)
		if localTerm != mentry.Term {
			// a previous entry is mismatched
			r.SMRLog.truncateAndAppendEntries(mentry.Index, m.Entries[i:])
			break
		}
	}
}

// If lastIndex and lastTerm match a previous local entry, then `stale` is true
func (r *SMR) safetyCheck(msgTerm, msgIndex uint64) (accept, stale bool) {

	lastEntry := r.SMRLog.lastEntry()
	if msgIndex < lastEntry.Index {
		targetTerm, _ := r.SMRLog.Term(msgIndex)
		if msgTerm != targetTerm {
			return false, false
		} else {
			return true, true
		}
	} else if msgIndex == lastEntry.Index {
		if msgTerm != lastEntry.Term {
			return false, false
		} else {
			return true, false
		}
	} else {
		return false, false
	}

	return true, false
}
