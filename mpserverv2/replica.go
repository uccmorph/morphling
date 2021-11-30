package mpserverv2

import (
	"fmt"
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
	msgCh         chan *HandlerInfo
	me            serverID
	peers         []serverID
	sendCh        chan *sendInfo
	storage       *MemStorage
}

type sendInfo struct {
	dest     serverID
	toClient bool
	msg      *ReplicaMsg
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
				p.HandleMsg(msg.Args)
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
		keyStr := strconv.FormatUint(args.KeyHash, 10)
		value := p.storage.Get(CfDefault, []byte(keyStr))
		reply.Data = value
		reply.KeyHash = args.KeyHash
		reply.Guide = &guide
		reply.Type = args.Type
		msg.Res <- msg
	}
}

func (p *Replica) HandleMsg(msg *ReplicaMsg) {
	guide := p.localGuidance
	pos := CalcKeyPos(msg.KeyHash, guide.GroupMask, guide.GroupSize)
	switch msg.Type {
	case MsgTypeAppend:
		guide.triDirectionCompare(msg.Guide,
			func() {
				log := p.log[pos]
				prevEntry := log.glog[len(log.glog)-1]
				if prevEntry.Index < msg.PrevIdx {
					// have some holes, ask for missing entries
					reply := &ReplicaMsg{
						Type:    MsgTypeAppendReply,
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
						Type:    MsgTypeAppendReply,
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
					Type:    MsgTypeAppendReply,
					From:    p.me,
					Success: replyStatusSuccess,
					Guide:   &guide,
				}
				p.asyncSend(msg.From, reply)
			},
			func() {
				reply := &ReplicaMsg{
					Type:    MsgTypeAppendReply,
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
	case MsgTypeAppendReply:
		if msg.Success == replyStatusSuccess {
			guide.triDirectionCompare(msg.Guide,
				func() {

				},
				func() {

				},
				func() {

				})
		}
	case MsgTypeGossip:
	}
}

func (p *Replica) processAppendEntries() {}

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

func CalcKeyPos(key uint64, mask uint64, bits uint64) uint64 {
	return key & mask >> bits
}
