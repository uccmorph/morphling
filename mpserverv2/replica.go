package mpserverv2

import (
	"fmt"
	"morphling/mplogger"
	"net/rpc"
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

func (p *Guidance) triDirectionCompareV2(other *Guidance, equal, lhsnewer, lhsolder func(lhs, rhs *Guidance)) {
	if p.Epoch == other.Epoch && equal != nil {
		equal(p, other)
	} else if p.Epoch < other.Epoch && lhsolder != nil {
		lhsolder(p, other)
	} else if p.Epoch > other.Epoch && lhsnewer != nil {
		lhsnewer(p, other)
	}
}

type Config struct {
	Guide    *Guidance
	Store    *MemStorage
	Peers    map[int]*rpc.Client
	Ch       chan *HandlerInfo
	Me       int
	RaftLike bool
}

type Replica struct {
	localGuidance  Guidance
	raftCore       []*Raft
	msgCh          chan *HandlerInfo
	me             int
	storage        *MemStorage
	clientPending  map[uint64]*HandlerInfo // uuid to client ctx
	clientCmdIndex map[string]uint64       // entry info to uuid

	peersStub map[int]*rpc.Client
	debugLog  *mplogger.RaftLogger
}

func CreateReplica(config *Config) *Replica {
	p := &Replica{}
	p.me = config.Me
	p.localGuidance = *config.Guide
	p.storage = config.Store
	p.msgCh = config.Ch
	p.peersStub = config.Peers
	p.clientPending = make(map[uint64]*HandlerInfo)
	p.clientCmdIndex = make(map[string]uint64)

	peers := []int{}
	for key := range config.Peers {
		peers = append(peers, key)
	}

	p.debugLog = mplogger.NewRaftDebugLogger()
	p.raftCore = make([]*Raft, defaultKeySpace)
	for i := range p.raftCore {
		p.raftCore[i] = newRaft(config.Me, peers, p.debugLog)
	}

	go p.mainLoop()
	return p
}

func (p *Replica) mainLoop() {
	for {
		// p.debugLog.Info("waiting new msg...")
		guide := p.localGuidance
		select {
		case msg := <-p.msgCh:
			if msg.IsClient {
				// p.debugLog.Info("get client msg: %+v", msg.CMsg)
				p.HandleClientMsg(msg)

			} else {
				args := msg.RMsg
				pos := CalcKeyPos(args.KeyHash, guide.GroupMask, guide.GroupSize)
				rlog := p.raftCore[pos]
				p.debugLog.Info("process log group %v", pos)
				rlog.Step(*args)
				commitEntries := rlog.RaftLog.nextEnts()
				p.processCommit(pos, commitEntries)
				rlog.RaftLog.updateApply(commitEntries)
				msgs := rlog.readNextMsg()
				for i := range msgs {
					msgs[i].KeyHash = args.KeyHash
				}
				p.sendReplicaMsgs(msgs)
			}
		}
	}
}

func (p *Replica) processCommit(pos uint64, entries []Entry) {
	for i := range entries {
		var value []byte
		if entries[i].Data.Type == CommandTypeWrite {
			keyStr := strconv.FormatUint(entries[i].Data.Key, 10)
			p.storage.Set(CfDefault, []byte(keyStr), []byte(entries[i].Data.Value))
		} else if entries[i].Data.Type == CommandTypeRead {
			keyStr := strconv.FormatUint(entries[i].Data.Key, 10)
			value = p.storage.Get(CfDefault, []byte(keyStr))
		}

		entryTag := GenClientTag(pos, entries[i].Index)
		uuid := p.clientCmdIndex[entryTag]
		hinfo := p.clientPending[uuid]
		reply := &ClientMsg{
			Type:    MsgTypeClientReply,
			Guide:   &p.localGuidance,
			KeyHash: entries[i].Data.Key,
			Data:    value,
		}
		hinfo.CMsg = reply
		hinfo.Res <- hinfo
	}
}

func (p *Replica) HandleClientMsg(msg *HandlerInfo) {
	args := msg.CMsg
	reply := ClientMsg{}
	guide := p.localGuidance
	replyInfo := &HandlerInfo{
		IsClient: true,
		CMsg:     &reply,
		Res:      msg.Res,
	}
	// pos := calcKeyPos(args.KeyHash, guide.GroupMask, guide.GroupSize)
	switch args.Type {
	case MsgTypeGetGuidance:

		reply.Guide = &guide
		reply.Type = args.Type

		replyInfo.Res <- replyInfo

	case MsgTypeClientRead:
		if guide.Epoch == args.Guide.Epoch {
			keyStr := strconv.FormatUint(args.KeyHash, 10)
			value := p.storage.Get(CfDefault, []byte(keyStr))
			// p.debugLog.Printf("key = %v, value = %v", keyStr, value)
			reply.Data = value
			reply.KeyHash = args.KeyHash
			reply.Guide = &guide
			reply.Type = args.Type
		} else {
			reply.Guide = &guide
		}
		replyInfo.Res <- replyInfo
	case MsgTypeClientProposal:
		pos := CalcKeyPos(args.KeyHash, guide.GroupMask, guide.GroupSize)
		rlog := p.raftCore[pos]
		replicaMsg := ReplicaMsg{
			Type:    MsgTypeClientProposal,
			KeyHash: args.KeyHash,
			Entries: []*Entry{
				{
					Data: args.Command,
				},
			},
		}
		if msg.UUID == 0 {
			panic("handler info can have 0 uuid")
		}
		rlog.Step(replicaMsg)
		entries := rlog.RaftLog.unstableEntries()
		if len(entries) != 1 {
			panic(fmt.Sprintf("should have only 1 entry, but now have %v", len(entries)))
		}
		p.clientPending[msg.UUID] = msg
		entryTag := GenClientTag(pos, entries[0].Index)
		p.clientCmdIndex[entryTag] = msg.UUID

		msgs := rlog.readNextMsg()
		for i := range msgs {
			msgs[i].KeyHash = args.KeyHash
		}
		p.sendReplicaMsgs(msgs)

		rlog.RaftLog.updateStable(entries)
	}
}

func (p *Replica) prepareGuidanceTransfer(new *Guidance) {}

func (p *Replica) sendReplicaMsgs(msgs []ReplicaMsg) {
	for i := range msgs {
		peer := p.peersStub[msgs[i].To]
		go func(peer *rpc.Client, msg ReplicaMsg) {
			reply := &ReplicaMsg{}
			err := peer.Call("RPCEndpoint.ReplicaCall", msg, reply)
			if err != nil {
				p.debugLog.Error("call RPCEndpoint.ReplicaCall error: %v", err)
			}
			info := &HandlerInfo{
				IsClient: false,
				RMsg:     reply,
			}
			// log.Printf("finish sendAppendMsg, reply: %+v", info.Reply)
			p.msgCh <- info
		}(peer, msgs[i])
	}
}
