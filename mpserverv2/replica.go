package mpserverv2

import "fmt"

type msgType uint64
type serverID uint64

const (
	msgTypeClient msgType = iota
	msgTypeClientReply
	msgTypeAppend
	msgTypeAppendReply
	msgTypeGossip
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

type replyStatus uint64

const (
	replyStatusSuccess replyStatus = iota
	replyStatusStaleGuidance
	replyStatusMissingEntries
	replyStatusMismatchEntry
)

type ReplicaStatus struct {
	Alive       bool
	StartKeyPos uint64
	EndKeyPos   uint64
}

type Guidance struct {
	Epoch     uint64
	AliveNum  uint64
	GroupMask uint64 // KeyPos = KeyHash & GroupMask >> GroupSize.
	GroupSize uint64 // The number of  1 digits in KeyMask, however it can be deduced from GroupMask
	Cluster   []ReplicaStatus
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
	nextIdx  []serverID
	matchIdx []serverID
}

type Replica struct {
	localGuidance Guidance
	log           []groupLogger
	msgCh         chan *ReplicaMsg
	me            serverID
	peers         []serverID
	sendCh        chan *sendInfo
}

type sendInfo struct {
	dest     serverID
	toClient bool
	msg      *ReplicaMsg
}

func CreateReplica() *Replica {
	p := &Replica{}

	go p.mainLoop()
	return p
}

func (p *Replica) mainLoop() {
	for {
		select {
		case msg := <-p.msgCh:
			p.handleMsg(msg)

		}
	}
}

func (p *Replica) handleMsg(msg *ReplicaMsg) {
	guide := p.localGuidance
	pos := calcKeyPos(msg.KeyHash, guide.GroupMask, guide.GroupSize)
	switch msg.Type {
	case msgTypeClient:
		guide.triDirectionCompare(msg.Guide,
			func() {

				log := p.log[pos]
				prevEntry := log.glog[len(log.glog)-1]
				entry := logEntry{
					Epoch: guide.Epoch,
					Index: uint64(len(log.glog)),
					Cmd:   msg.Command,
				}
				log.glog = append(log.glog, entry)

				appendMsg := &ReplicaMsg{
					Type:      msgTypeAppend,
					From:      p.me,
					KeyHash:   msg.KeyHash,
					Guide:     &guide,
					CommitTo:  log.commitTo,
					PrevIdx:   prevEntry.Index,
					PrevEpoch: prevEntry.Epoch,
					Command:   msg.Command,
				}
				p.sendAppendMsg(appendMsg)
			},
			func() {
				reply := &ReplicaMsg{
					Type:    msgTypeClientReply,
					From:    p.me,
					Success: replyStatusStaleGuidance,
					Guide:   &guide,
				}
				p.asyncSendToClient(msg.From, reply)
			},
			func() {
				panic(fmt.Sprintf("local guidance is older than client's? local: %+v, client's: %+v",
					guide, msg.Guide))
			})

	case msgTypeAppend:
		guide.triDirectionCompare(msg.Guide,
			func() {
				log := p.log[pos]
				prevEntry := log.glog[len(log.glog)-1]
				if prevEntry.Index < msg.PrevIdx {
					// have some holes, ask for missing entries
					reply := &ReplicaMsg{
						Type:    msgTypeAppendReply,
						From:    p.me,
						Success: replyStatusMissingEntries,
						Guide:   &guide,
						KeyHash: msg.KeyHash,
					}
					p.asyncSend(msg.From, reply)
					return
				}
				for prevEntry.Index > msg.PrevIdx {
					if prevEntry.Epoch < msg.PrevEpoch {
						// last entry is stale, truncate until index match
						log.glog = log.glog[:len(log.glog)-1] // minus one entry
						prevEntry = log.glog[len(log.glog)-1] // get last entry
					} else {
						// a bug in algorithm.
						// repre-leader should make sure it has newest per-group log
						panic(fmt.Sprintf("Too many advanced entries. "+
							"local last entry: %+v, msg: %+v", prevEntry, msg))
					}
				}
				// now prevEntry.Index == msg.PrevIdx
				if prevEntry.Epoch < msg.PrevEpoch {
					// still not match, notify leader
					reply := &ReplicaMsg{
						Type:    msgTypeAppendReply,
						From:    p.me,
						Success: replyStatusMismatchEntry,
						Guide:   &guide,
						KeyHash: msg.KeyHash,
					}
					p.asyncSend(msg.From, reply)
					return
				}
				if prevEntry.Epoch > msg.PrevEpoch {
					// panic, since it's a more advanced entry
					panic(fmt.Sprintf("last entry is advanced. "+
						"local last entry: %+v, msg: %+v", prevEntry, msg))
				}
				// now local last entry is consitent with leader's msg, we can do safe replication
				entry := logEntry{
					Epoch: guide.Epoch,
					Index: uint64(len(log.glog)),
					Cmd:   msg.Command,
				}
				log.glog = append(log.glog, entry)

				// now check if there are any uncommited commands
				if msg.CommitTo > log.commitTo {
					p.commitFromTo(&log, log.commitTo+1, msg.CommitTo)
				}
				reply := &ReplicaMsg{
					Type:    msgTypeAppendReply,
					From:    p.me,
					Success: replyStatusSuccess,
					Guide:   &guide,
				}
				p.asyncSend(msg.From, reply)
			},
			func() {
				reply := &ReplicaMsg{
					Type:    msgTypeAppendReply,
					From:    p.me,
					Success: replyStatusStaleGuidance,
					Guide:   &guide,
					KeyHash: msg.KeyHash,
				}
				p.asyncSend(msg.From, reply)
			},
			func() {
				// This replica missed some guidance transformation,
				// since such transfer doesn't require all alive replicas participating
				p.prepareGuidanceTransfer(msg.Guide)
			})

	case msgTypeAppendReply:
		if msg.Success == replyStatusSuccess {
			guide.triDirectionCompare(msg.Guide,
				func() {

				},
				func() {

				},
				func() {

				})
		}
	case msgTypeGossip:
	}
}

func (p *Replica) processAppendEntries()

func (p *Replica) commitFromTo(log *groupLogger, start, end uint64) {}

func (p *Replica) prepareGuidanceTransfer(new *Guidance) {}

func (p *Replica) sendAppendMsg(msg *ReplicaMsg) {
	for _, peerID := range p.peers {
		p.asyncSend(peerID, msg)
	}
}

func (p *Replica) asyncSendToClient(id serverID, msg *ReplicaMsg) {
	go func() {
		p.sendCh <- &sendInfo{
			dest:     id,
			toClient: true,
			msg:      msg,
		}
	}()
}

func (p *Replica) asyncSend(peerID serverID, msg *ReplicaMsg) {
	go func() {
		p.sendCh <- &sendInfo{
			dest:     peerID,
			toClient: false,
			msg:      msg,
		}
	}()
}

func (p *Replica) sendLoop() {
	for range p.sendCh {

	}
}

func calcKeyPos(key uint64, mask uint64, bits uint64) uint64 {
	return key & mask >> bits
}
