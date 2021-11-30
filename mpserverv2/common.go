package mpserverv2

type msgType uint64
type serverID int

const (
	MsgTypeClient msgType = iota
	MsgTypeClientReply
	MsgTypeGetGuidance
	MsgTypeClientProposal
	MsgTypeClientRead
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
	Type    msgType
	Guide   *Guidance
	KeyHash uint64
	Data    []byte
}

type replyStatus uint64

const (
	replyStatusSuccess replyStatus = iota
	replyStatusStaleGuidance
	replyStatusMissingEntries
	replyStatusMismatchEntry
)

type Guidance struct {
	Epoch     uint64
	AliveNum  uint64
	GroupMask uint64 // KeyPos = KeyHash & GroupMask >> GroupSize. Like, 0x5498 & 0xff00 >> 8 = 0x54, then this key is in slot 84
	GroupSize uint64 // The number of  1 digits in KeyMask, however it can be deduced from GroupMask
	Cluster   []ReplicaStatus
}

var defaultKeySpace uint64 = 256

func (p *Guidance) InitDefault(size uint64) {
	p.Epoch = 1
	p.AliveNum = size
	p.GroupMask = 0xff00
	p.GroupSize = 8
	p.Cluster = make([]ReplicaStatus, p.AliveNum)
	for i := range p.Cluster {
		p.Cluster[i].Alive = true
		p.Cluster[i].StartKeyPos = uint64(i) * defaultKeySpace / p.AliveNum
		p.Cluster[i].EndKeyPos = uint64(i+1) * defaultKeySpace / p.AliveNum
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

type HandlerInfo struct {
	IsClient bool
	Args     *ReplicaMsg
	Reply    *ReplicaMsg
	Cargs    *ClientMsg
	Creply   *ClientMsg
	Res      chan *HandlerInfo
}
