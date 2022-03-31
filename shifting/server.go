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

func posInRange(pos int, start, end int) bool {
	// end is over 255
	if start > end {
		if start <= pos && pos <= 255 {
			return true
		}
		if 0 <= pos && pos <= end {
			return true
		}
	} else {
		if start <= pos && pos <= end {
			return true
		}
	}
	return false
}

// return: 1 for p is subset of target, 0 for same, -1 for other case
func (p *Guidance) compareRange(target *Guidance, id int) int {
	if p.cluster[id].start == target.cluster[id].start &&
		p.cluster[id].end == target.cluster[id].end {
		return 0
	}

	if p.calcRange(id) < target.calcRange(id) {
		if posInRange(p.cluster[id].start, target.cluster[id].start, target.cluster[id].end) &&
			posInRange(p.cluster[id].end, target.cluster[id].start, target.cluster[id].end) {
			return 1
		}
	}

	return -1
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

type State int

const Shifting State = 0
const Serving State = 1

type Server struct {
	mu        sync.Mutex
	debug     *Logger
	net       *network
	id        int
	deadCount map[int]int
	guide     Guidance
	prevGuide *Guidance

	onwaitingGuide *Guidance
	proposalGuide  *Guidance
	proposalVotes  int
	quorum         int
	state          State
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
	s.state = Shifting

	s.guide.InitDefault(size)
	s.guide.IncEpoch(s.id)
	s.prevGuide = s.guide.Clone()
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
				p.debug.Info("My local guidance: %v", p.guide)
				deadID := []int{}
				for id := 0; id < p.guide.clusterSize; id++ {
					if id == p.id {
						continue
					}
					if p.deadCount[id] <= 0 {
						p.debug.Timeout("%d Warning: server %d is timeout", p.id, id)
						deadID = append(deadID, id)
					} else {
						p.deadCount[id] -= 1
					}
				}
				changed := false
				if len(deadID) > 0 {
					if len(deadID) >= p.quorum {
						p.debug.Info("I'm in a network partition that can't find majority." +
							" Just stay.")
					} else {
						p.prevGuide = p.guide.Clone()
						for _, id := range deadID {
							if !p.guide.cluster[id].alive {
								continue
							}
							changed = true
							p.debug.Info("change guidance since servers %d dead", deadID)
							p.guide.SetDead(id)
						}
						if changed {
							p.guide.IncEpoch(p.id)
						}
					}
				}
				p.gossip()
				if changed {
					p.tryRecovery()
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
				if mmsg.Type() == 10 {
					gossip := mmsg.(*GossipMessage)
					if gossip.guide.epoch > p.guide.epoch {
						p.guide = *gossip.guide.Clone()
						p.debug.Info("change guidance to %v", p.guide)
						if !p.guide.cluster[p.id].alive {
							p.debug.Info("I'm dead in epoch %d", p.guide.epoch)
						}
						// make sure local log in range guide.cluster[p.id] is up-to-date
						p.tryRecovery()
					}
				}
			}

			p.mu.Unlock()
		}
	}()
}

func (p *Server) join(id int) {
	p.debug.Timeout("server %d become online again", id)
	p.guide.SetAlive(id)
	p.guide.IncEpoch(p.id)
	p.gossip()
}

func (p *Server) tryRecovery() {
	p.debug.Recovery("previous guide: %v", p.prevGuide)
	if p.guide.compareRange(p.prevGuide, p.id) == -1 {
		p.debug.Recovery("need recovery.")
	}
}

type Message interface {
	Type() int
}

type GossipMessage struct {
	msgType int // 10
	guide   Guidance
}

func (p *GossipMessage) Type() int {
	return p.msgType
}

func (p *Server) gossip() {
	for id := 0; id < p.guide.clusterSize; id++ {
		if id == p.id {
			continue
		}

		p.debug.Info("gossip with %d", id)
		msg := &GossipMessage{
			msgType: 10,
		}
		if p.onwaitingGuide != nil {
			msg.guide = *p.onwaitingGuide
		} else {
			msg.guide = p.guide
		}
		p.net.Send(id, msg)
	}
}
