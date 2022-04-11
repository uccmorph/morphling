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
	MsgTypeClientCURPProposal
	MsgTypeClientRead
	MsgTypeClientWrite
	MsgTypeAppend
	MsgTypeAppendReply
	MsgTypeGossip
	MsgTypeGossipEmptyReply
)

type ReplicaMsg struct {
	Type      msgType
	To        int
	From      int
	Success   replyStatus
	KeyHash   uint64
	Guide     *Guidance
	CommitTo  uint64
	PrevIdx   uint64
	PrevEpoch uint64
	Entries   []*Entry

	Load uint64
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
	ReplyStatusInterfere
)

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
	ClientNums int
	Finish     bool
}

type StartTestReply struct{}
