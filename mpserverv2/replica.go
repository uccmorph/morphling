package mpserverv2

import (
	"fmt"
	"log"
	"math/rand"
	"morphling/mplogger"
	"net/rpc"
	"strconv"
	"time"
)

type Config struct {
	Guide      *Guidance
	Store      *MemStorage
	Peers      map[int]*rpc.Client
	Ch         chan *HandlerInfo
	GuidanceCh chan *HandlerInfo
	Me         int
	RaftLike   bool
	CURPLike   bool
}

type Replica struct {
	localGuidance  Guidance
	raftCore       []*SMR
	msgCh          chan *HandlerInfo
	gCh            chan *HandlerInfo
	me             int
	storage        *MemStorage
	clientPending  map[uint64]*HandlerInfo // uuid to client ctx
	clientCmdIndex map[string]uint64       // entry info to uuid
	enableRaft     bool

	peersStub           map[int]*rpc.Client
	debugLog            *mplogger.RaftLogger
	weakFailureDetector map[int]int // replica id -> liveness counts
}

func CreateReplica(config *Config) *Replica {
	p := &Replica{}
	p.me = config.Me
	p.localGuidance = *config.Guide
	p.storage = config.Store
	p.msgCh = config.Ch
	p.gCh = config.GuidanceCh
	p.peersStub = config.Peers
	p.clientPending = make(map[uint64]*HandlerInfo)
	p.clientCmdIndex = make(map[string]uint64)

	peers := []int{}
	for id := range config.Peers {
		peers = append(peers, id)
	}

	p.debugLog = mplogger.NewRaftDebugLogger()
	p.raftCore = make([]*SMR, DefaultKeySpace)
	for i := range p.raftCore {
		p.raftCore[i] = newSMR(config.Me, peers, p.debugLog)
		p.raftCore[i].ChangeTerm(p.localGuidance.Epoch)
	}

	if config.CURPLike {
		log.Fatalf("can not use CURP in generic replica")
	}
	p.enableRaft = config.RaftLike

	p.weakFailureDetector = make(map[int]int)
	for id := range config.Peers {
		p.weakFailureDetector[id] = 0
	}

	go p.mainLoop()
	go p.guidanceLoop()
	return p
}

func (p *Replica) mainLoop() {
	ticker := time.NewTimer(time.Millisecond * time.Duration(500+rand.Intn(500)))
	for {
		// p.debugLog.Info("waiting new msg...")

		select {
		case msg := <-p.msgCh:
			// time.Sleep(time.Millisecond * 1)
			if msg.IsClient {
				// p.debugLog.Info("get client msg: %+v", msg.CMsg)
				p.HandleClientMsg(msg)
			} else {
				p.HandleReplicaMsg(msg)
			}
		case <-ticker.C:
			p.bcastGossip()
			for id := range p.peersStub {
				p.weakFailureDetector[id] += 1
			}
			ticker = time.NewTimer(time.Millisecond * time.Duration(500+rand.Intn(500)))
		}
	}
}

func (p *Replica) guidanceLoop() {
	for {
		select {
		case msg := <-p.gCh:
			if !msg.IsClient {
				log.Fatalf("msg is not from client. %+v", msg)
			}
			if msg.CMsg.Type != MsgTypeGetGuidance {
				log.Fatalf("msg type is not MsgTypeGetGuidance. %v", msg.CMsg.Type)
			}
			// log.Printf("msg %+v", msg.CMsg)
			p.HandleClientMsg(msg)
		}
	}
}

func (p *Replica) processCommit(pos uint64, entries []Entry) {
	for i := range entries {
		var value []byte
		reply := &ClientMsg{
			Type:    MsgTypeClientReply,
			Guide:   &p.localGuidance,
			KeyHash: entries[i].Data.Key,
		}
		if entries[i].Data.Type == CommandTypeWrite {
			keyStr := strconv.FormatUint(entries[i].Data.Key, 10)
			p.storage.Set([]byte(keyStr), []byte(entries[i].Data.Value))
		} else if entries[i].Data.Type == CommandTypeRead {
			keyStr := strconv.FormatUint(entries[i].Data.Key, 10)
			value = p.storage.Get([]byte(keyStr))
			if len(value) == 0 {
				reply.Success = ReplyStatusNoValue
			}
		}
		reply.Data = value

		entryTag := GenClientTag(pos, entries[i].Index)
		uuid := p.clientCmdIndex[entryTag]
		hinfo := p.clientPending[uuid]
		reply.Seq = hinfo.CMsg.Seq
		hinfo.CMsg = reply
		hinfo.Res <- hinfo
	}
}

func (p *Replica) HandleReplicaMsg(info *HandlerInfo) {
	args := info.RMsg

	if !p.enableRaft {
		if args.Guide.Epoch < p.localGuidance.Epoch {
			log.Printf("peer %d is stale %+v. local %+v", args.From, args.Guide, p.localGuidance)
		}
		if args.Guide.Epoch > p.localGuidance.Epoch {
			log.Printf("local is stale %+v. peer %+v", p.localGuidance, args.Guide)
			log.Printf("shift guidance to %+v", args.Guide)
			p.localGuidance.CopyFrom(args.Guide)
		}
	}
	switch args.Type {
	case MsgTypeAppend, MsgTypeAppendReply:
		pos := CalcKeyPos(args.KeyHash)
		rlog := p.raftCore[pos]
		p.debugLog.Info("process smr part %v", pos)
		rlog.Step(*args)
		commitEntries := rlog.SMRLog.nextEnts()
		p.processCommit(pos, commitEntries)
		rlog.SMRLog.updateApply(commitEntries)
		msgs := rlog.readNextMsg()
		for i := range msgs {
			msgs[i].KeyHash = args.KeyHash
			switch msgs[i].Type {
			case MsgTypeAppendReply:
				p.replyReplicaMsgs(&msgs[i], info.Res)
			case MsgTypeAppend:
				p.sendReplicaMsgs(&msgs[i])
			}

		}
	case MsgTypeGossip:
		log.Printf("receive gossip msg from %v, guidance: %+v, load: %v",
			args.From, args.Guide, args.Load)
		p.weakFailureDetector[args.From] = 0
		info.RMsg.Type = MsgTypeGossipEmptyReply
		info.Res <- info
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
		reply.Seq = args.Seq

		replyInfo.Res <- replyInfo

	// for morphling read and unreplicated read
	case MsgTypeClientRead:
		if guide.Epoch == args.Guide.Epoch {
			keyStr := strconv.FormatUint(args.KeyHash, 10)
			value := p.storage.Get([]byte(keyStr))
			// p.debugLog.Printf("key = %v, value = %v", keyStr, value)
			reply.Data = value
			reply.KeyHash = args.KeyHash
			reply.Guide = &guide
			reply.Type = args.Type
		} else {
			reply.Guide = &guide
			reply.Success = replyStatusWrongGuidance
		}
		replyInfo.Res <- replyInfo

	// for replicated write
	case MsgTypeClientWrite:
		keyStr := strconv.FormatUint(args.KeyHash, 10)
		p.storage.Set([]byte(keyStr), []byte(args.Command.Value))

		replyInfo.Res <- replyInfo

	// for morphling write and raft write
	case MsgTypeClientProposal:
		pos := CalcKeyPos(args.KeyHash)
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
		entries := rlog.SMRLog.unstableEntries()
		if len(entries) != 1 {
			panic(fmt.Sprintf("should have only 1 entry, but now have %v", len(entries)))
		}
		p.clientPending[msg.UUID] = msg
		entryTag := GenClientTag(pos, entries[0].Index)
		p.clientCmdIndex[entryTag] = msg.UUID

		msgs := rlog.readNextMsg()
		for i := range msgs {
			msgs[i].KeyHash = args.KeyHash
			p.sendReplicaMsgs(&msgs[i])
		}

		rlog.SMRLog.updateStable(entries)
	}
}

// `to` replica should be set in msg.To
func (p *Replica) sendReplicaMsgs(msg *ReplicaMsg) {
	peer := p.peersStub[msg.To]
	go func(peer *rpc.Client, msg *ReplicaMsg) {
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
	}(peer, msg)
}

func (p *Replica) replyReplicaMsgs(reply *ReplicaMsg, replyCh chan *HandlerInfo) {
	replyInfo := &HandlerInfo{
		IsClient: false,
		RMsg:     reply,
	}
	replyCh <- replyInfo
}

func (p *Replica) gossip(to int) {
	msg := &ReplicaMsg{
		Type:  MsgTypeGossip,
		From:  p.me,
		To:    to,
		Guide: &p.localGuidance,
	}
	go func(msg *ReplicaMsg) {
		reply := &ReplicaMsg{}
		err := p.peersStub[to].Call("RPCEndpoint.ReplicaCall", msg, reply)
		if err != nil {
			p.debugLog.Error("call RPCEndpoint.ReplicaCall error: %v", err)
		}
		// don't use p.msgCh
	}(msg)
}

func (p *Replica) bcastGossip() {
	for i := range p.peersStub {
		if i == p.me {
			continue
		}
		p.gossip(i)
	}
}
