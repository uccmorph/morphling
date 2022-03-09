package main

import (
	"fmt"
	"sync"
	"time"
)

type NodeStatus struct {
	alive bool
	start int
	end   int
}

type Guidance struct {
	epoch       int
	clusterSize int
	cluster     []NodeStatus
}

func (p *Guidance) InitDefault(size int) {
	p.epoch = 1
	p.clusterSize = size
	p.cluster = make([]NodeStatus, size)
	for i := 0; i < size; i++ {
		p.cluster[i].alive = true
		p.cluster[i].start = (i * 256 / size) % 256
	}
	for i := 0; i < size; i++ {
		nextIdx := (i + 1) % size
		p.cluster[i].end = (p.cluster[nextIdx].start + 255) % 256
	}
}

func (p *Guidance) Clone() *Guidance {
	guide := &Guidance{}
	guide.epoch = p.epoch
	guide.clusterSize = p.clusterSize
	guide.cluster = make([]NodeStatus, len(p.cluster))
	copy(guide.cluster, p.cluster)
	return guide
}

func (p *Guidance) IncEpoch(base int) {
	epoch := (p.epoch/p.clusterSize*p.clusterSize + base) + p.clusterSize
	if epoch < p.epoch {
		epoch += p.clusterSize
	}
	p.epoch = epoch
}

func (p *Guidance) findPrevIdx(id int) int {
	prevIdx := (id + p.clusterSize - 1) % p.clusterSize
	startIdx := prevIdx
	for !p.cluster[prevIdx].alive {
		prevIdx = (prevIdx + p.clusterSize - 1) % p.clusterSize
		if prevIdx == startIdx {
			panic("no alive node")
		}
	}

	return prevIdx
}

func (p *Guidance) findNextIdx(id int) int {
	nextIdx := (id + 1) % p.clusterSize
	startIdx := nextIdx
	for !p.cluster[nextIdx].alive {
		nextIdx = (nextIdx + 1) % p.clusterSize
		if nextIdx == startIdx {
			panic("no alive node")
		}
	}

	return nextIdx
}

func (p *Guidance) SetDead(id int) {
	deadRange := p.calcRange(id) + 1
	if deadRange < 0 {
		deadRange = 0 - deadRange
	}
	prevPart := deadRange / 2
	nextPart := deadRange - prevPart
	p.cluster[id].alive = false
	p.cluster[id].start = 0
	p.cluster[id].end = 0

	prevIdx := p.findPrevIdx(id)
	nextIdx := p.findNextIdx(id)

	p.cluster[prevIdx].end = (p.cluster[prevIdx].end + prevPart) % 256
	p.cluster[nextIdx].start = (p.cluster[nextIdx].start - nextPart + 256) % 256
}

func calcRange(start, end int) int {
	return (end - start + 256) % 256
}

func (p *Guidance) calcRange(id int) int {
	return calcRange(p.cluster[id].start, p.cluster[id].end)
}

func (p *Guidance) SetAlive(id int) {
	p.cluster[id].alive = true

	prevIdx := p.findPrevIdx(id)
	nextIdx := p.findNextIdx(id)

	prevRange := p.calcRange(prevIdx)
	nextRange := p.calcRange(nextIdx)

	prevAddRange := prevRange / 3
	nextAddRange := nextRange / 3

	p.cluster[prevIdx].end = (p.cluster[prevIdx].end - prevAddRange + 256) % 256
	p.cluster[nextIdx].start = (p.cluster[nextIdx].start + nextAddRange) % 256

	p.cluster[id].start = (p.cluster[prevIdx].end + 1) % 256
	p.cluster[id].end = (p.cluster[nextIdx].start - 1 + 256) % 256
}

type Server struct {
	mu        sync.Mutex
	debug     *Logger
	net       *network
	id        int
	deadCount map[int]int
	guide     Guidance

	onwaitingGuide *Guidance
	proposalGuide  *Guidance
	proposalVotes  int
	quorum         int
}

const timeoutCount = 3

var debugPrefix []string = []string{
	"\033[48;2;255;188;188m%s\033[0m",
	"\033[48;2;231;188;255m%s\033[0m",
	"\033[48;2;188;240;255m%s\033[0m",
}

func NewServer(net *network, size int) *Server {
	s := &Server{}
	s.net = net
	s.id = net.Self
	s.quorum = size/2 + 1
	prefix := fmt.Sprintf("[Server %d]", s.id)
	s.debug = NewLogger(prefix, debugPrefix[s.id])
	s.deadCount = make(map[int]int)
	for i := 0; i < 3; i++ {
		s.deadCount[i] = timeoutCount
	}

	s.guide.InitDefault(size)
	s.guide.IncEpoch(s.id)
	s.debug.Info("%d guide: %+v", s.id, s.guide)

	return s
}

func (p *Server) Start() {

	t := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-t.C:
				p.mu.Lock()

				deadID := []int{}
				for id := 0; id < p.guide.clusterSize; id++ {
					if id == p.id {
						continue
					}
					if p.deadCount[id] == 0 {
						p.debug.Timeout("%d Warning: server %d is timeout", p.id, id)
						deadID = append(deadID, id)
					}
					p.debug.Info("gossip with %d", id)
					p.deadCount[id] -= 1
					msg := &GossipeMessage{
						msgType: 10,
					}
					if p.onwaitingGuide != nil {
						msg.guide = *p.onwaitingGuide
					} else {
						msg.guide = p.guide
					}
					p.net.Send(id, msg)
				}
				if len(deadID) > 0 {
					if len(deadID) >= p.quorum {
						p.debug.Info("I'm in a network partition that the majority can't find me")
					} else {
						p.debug.Info("shifting since servers %d dead", deadID)
						p.proposalGuide = p.guide.Clone()
						p.proposalGuide.IncEpoch(p.id)
						for _, id := range deadID {
							p.proposalGuide.SetDead(id)
						}
						p.onwaitingGuide = p.proposalGuide
						p.proposalVotes = 1
						p.shifting(p.proposalGuide, false)
					}
				}

				p.mu.Unlock()
			}
		}
	}()

	go func() {
		for {
			from, msg := p.net.RecvNew()
			p.mu.Lock()
			p.debug.Info("%d recv new msg from %d, content: %v", p.id, from, msg)
			if p.deadCount[from] <= 0 {
				if p.guide.cluster[from].alive {
					panic(fmt.Sprintf("server %d is dead but guidance doesn't think so", from))
				}
				p.join(from)
				p.deadCount[from] = timeoutCount
				p.mu.Unlock()
				continue // ignore msg type, regard it stale
			}
			p.deadCount[from] = timeoutCount // reset counter
			if mmsg, ok := msg.(Message); ok {
				p.debug.Info("message type: %d", mmsg.Type())
				if mmsg.Type() == 100 {
					shift := mmsg.(*ShiftingMessage)
					reply := &ShiftingMessage{
						msgType:  101,
						proposal: shift.proposal,
						vote:     false,
					}
					if shift.proposal.epoch > p.guide.epoch &&
						shift.proposal.cluster[p.id].alive {
						if p.onwaitingGuide == nil ||
							shift.proposal.epoch > p.onwaitingGuide.epoch {
							reply.vote = true
							p.onwaitingGuide = shift.proposal.Clone()
						}
					}
					p.debug.Vote("vote (%v) for proposal %d", reply.vote, shift.proposal.epoch)
					p.net.Send(from, reply)
				} else if mmsg.Type() == 101 {
					shift := mmsg.(*ShiftingMessage)
					if p.proposalGuide != nil && shift.proposal.epoch == p.proposalGuide.epoch {
						p.debug.GatherVote("server %d vote (%v) to proposal %d",
							from, shift.vote, shift.proposal.epoch)
						p.proposalVotes += 1
						if p.proposalVotes >= p.quorum {
							p.guide = *p.proposalGuide.Clone()
							p.debug.GatherVote("change guidance to %v", p.guide)
							p.shifting(&p.guide, true)
							p.proposalGuide = nil
							p.onwaitingGuide = nil
							p.proposalVotes = 0
						}
					}
				} else if mmsg.Type() == 110 {
					shift := mmsg.(*ShiftingMessage)
					if p.onwaitingGuide != nil &&
						shift.proposal.epoch == p.onwaitingGuide.epoch {
						p.guide = *shift.proposal.Clone()
						p.debug.Info("change guidance to %v", p.guide)

						p.proposalGuide = nil
						p.onwaitingGuide = nil
					}
				} else if mmsg.Type() == 10 {
					gossip := mmsg.(*GossipeMessage)
					if gossip.guide.epoch > p.guide.epoch {
						p.guide = *gossip.guide.Clone()
						p.debug.Info("change guidance to %v", p.guide)
						if !p.guide.cluster[p.id].alive {
							p.debug.Info("I'm dead in epoch %d", p.guide.epoch)
						}
					}
				}
			}

			p.mu.Unlock()
		}
	}()
}

func (p *Server) join(id int) {
	p.debug.Info("server %d become online again", id)
	if p.onwaitingGuide == nil {
		if !p.guide.cluster[id].alive {
			p.proposalGuide = p.guide.Clone()
			p.proposalGuide.IncEpoch(p.id)
			p.proposalGuide.SetAlive(id)
			p.onwaitingGuide = p.proposalGuide
			p.proposalVotes = 1
			p.shifting(p.proposalGuide, false)
		}
	} else {
		p.proposalGuide = p.onwaitingGuide.Clone()
		p.proposalGuide.IncEpoch(p.id)
		p.proposalGuide.SetAlive(id)
		p.onwaitingGuide = p.proposalGuide
		p.proposalVotes = 1
		p.shifting(p.proposalGuide, false)
	}
}

type Message interface {
	Type() int
}

type GossipeMessage struct {
	msgType int // 10
	guide   Guidance
}

func (p *GossipeMessage) Type() int {
	return p.msgType
}

type ShiftingMessage struct {
	msgType  int // prepare 100,  prepare reply 101, commit 110
	proposal Guidance
	vote     bool
}

func (p *ShiftingMessage) Type() int {
	return p.msgType
}

func (p *Server) shifting(proposal *Guidance, commit bool) {

	msg := &ShiftingMessage{
		msgType:  100,
		proposal: *proposal,
	}
	if commit {
		msg.msgType = 110
	}
	for i := 0; i < p.guide.clusterSize; i++ {
		if i == p.id {
			continue
		}
		p.net.Send(i, msg)
	}
}
