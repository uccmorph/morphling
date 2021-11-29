package mpserverv2

type msgType uint64
type serverID uint64

const (
	MsgTypeClient msgType = iota
	MsgTypeClientReply
	MsgTypeGetGuidance
	MsgTypeClientProposal
	MsgTypeAppend
	MsgTypeAppendReply
	MsgTypeGossip
)

type ReplicaMsg struct {
	Type          msgType
	From          serverID
	Success       replyStatus
	KeyHash       uint64
	Guide         *Guidance
	CommitTo      uint64
	PrevIdx       uint64
	PrevEpoch     uint64
	Command       interface{}
	FastRewindIdx uint64
}

type ClientMsg struct {
	Type  msgType
	Guide *Guidance
}

type replyStatus uint64

const (
	replyStatusSuccess replyStatus = iota
	replyStatusStaleGuidance
	replyStatusMissingEntries
	replyStatusMismatchEntry
)
