package mpserverv2

import (
	"strconv"
	"strings"
)

type msgType uint64

const (
	MsgTypeClient msgType = iota
	MsgTypeClientReply
	MsgTypeGetGuidance
	MsgTypeClientProposal
	MsgTypeClientRead
	MsgTypeClientWrite
	MsgTypeAppend
	MsgTypeAppendReply
	MsgTypeGossip
)

type ReplicaMsg struct {
	Type          msgType
	To            int
	From          int
	Success       replyStatus
	KeyHash       uint64
	Guide         *Guidance
	CommitTo      uint64
	PrevIdx       uint64
	PrevEpoch     uint64
	Entries       []*Entry
	FastRewindIdx uint64
}

type ClientMsg struct {
	Type     msgType
	Guide    *Guidance
	Success  replyStatus
	ClientID int
	Seq      int
	KeyHash  uint64
	Data     []byte  // only for reply
	Command  Command // only for request
}

const (
	CommandTypeNoop = iota
	CommandTypeRead
	CommandTypeWrite
)

type Command struct {
	Type  int
	Key   uint64
	Value string
}

func GenClientTag(pos, idx uint64) string {
	part1 := strconv.FormatUint(pos, 10)
	part2 := strconv.FormatUint(idx, 10)

	return part1 + "." + part2
}

func ParseClientTag(tag string) (int, uint64) {
	s := strings.Split(tag, ".")
	part1, _ := strconv.ParseInt(s[0], 10, 64)
	part2, _ := strconv.ParseUint(s[1], 10, 64)

	return int(part1), part2
}

type replyStatus uint64

const (
	ReplyStatusSuccess replyStatus = iota
	replyStatusWrongGuidance
	replyStatusMissingEntries
	replyStatusMismatchEntry
	replyStatusTimeout
	ReplyStatusNoValue
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

type Guidance struct {
	Epoch     uint64
	GroupMask uint64 // KeyPos = KeyHash & GroupMask >> GroupSize. Like, 0x5498 & 0xff00 >> 8 = 0x54, then this key is in part 0x54
	GroupSize uint64 // The number of  1 digits in KeyMask, however it can be deduced from GroupMask
	Cluster   []ReplicaStatus
}

var defaultKeySpace uint64 = 256

func (p *Guidance) InitDefault(size uint64) {
	p.Epoch = 1
	p.GroupMask = 0xff000000
	p.GroupSize = 24
	p.Cluster = make([]ReplicaStatus, size)
	for i := range p.Cluster {
		p.Cluster[i].Alive = true
		p.Cluster[i].StartKeyPos = uint64(i) * defaultKeySpace / size
		p.Cluster[i].EndKeyPos = uint64(i+1) * defaultKeySpace / size
	}
}

func (p *Guidance) ReplicaID(pos uint64) int {
	for i := range p.Cluster {
		if p.Cluster[i].KeyisIn(pos) {
			return i
		}
	}
	return -1
}

func CalcKeyPos(key uint64, mask uint64, bits uint64) uint64 {
	return key & mask >> bits
}

type HandlerInfo struct {
	IsClient bool
	RMsg     *ReplicaMsg
	CMsg     *ClientMsg
	Res      chan *HandlerInfo
	UUID     uint64
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// stat-server
type StatisticsArgs struct {
	AvgLatencyNs   int
	ThroughputKops float64
	VClientNums    int
	MachineID      int
}

type StatisticsReply struct{}

type ClientRegisterArgs struct{}

type ClientRegisterReply struct {
	MachineID int
}

// sent from stat-server to clients
type StartTestArgs struct {
	TestCount  int
	ClientNums int
	Finish     bool
}

type StartTestReply struct{}
