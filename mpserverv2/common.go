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
	Command       []byte
	FastRewindIdx uint64
}

type ClientMsg struct {
	Type     msgType
	Guide    *Guidance
	ClientID int
	Seq      int
	KeyHash  uint64
	Data     []byte
}

func GenClientTag(id, seq int) string {
	part1 := strconv.FormatInt(int64(id), 10)
	part2 := strconv.FormatInt(int64(seq), 10)

	return part1 + "." + part2
}

func ParseClientTag(tag string) (int, int) {
	s := strings.Split(tag, ".")
	part1, _ := strconv.ParseInt(s[0], 10, 64)
	part2, _ := strconv.ParseInt(s[1], 10, 64)

	return int(part1), int(part2)
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

func CalcKeyPos(key uint64, mask uint64, bits uint64) uint64 {
	return key & mask >> bits
}

type HandlerInfo struct {
	IsClient bool
	Args     *ReplicaMsg
	Reply    *ReplicaMsg
	Cargs    *ClientMsg
	Creply   *ClientMsg
	Res      chan *HandlerInfo
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
