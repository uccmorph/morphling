package mpserverv2

import (
	"log"
	"net/rpc"
	"strconv"
)

type Witness struct {
	commutSpace map[uint64]Entry
}

func newWitness() *Witness {
	p := &Witness{}
	p.commutSpace = make(map[uint64]Entry)

	return p
}

func (p *Witness) TryRecord(key uint64, e Entry) bool {
	if _, ok := p.commutSpace[key]; !ok {
		p.commutSpace[key] = e
		return true
	}
	return false
}

func (p *Witness) Remove(key uint64) {
	delete(p.commutSpace, key)
}

type CURPReplica struct {
	me       int
	isLeader bool
	smrLog   *SMRLog
	witness  *Witness
	storage  *MemStorage

	msgCh     chan *HandlerInfo
	peersStub map[int]*rpc.Client
}

func CreateCURPReplica(config Config) *CURPReplica {
	p := &CURPReplica{}
	p.me = config.Me
	if config.Me == 0 {
		p.isLeader = true
	}

	p.peersStub = config.Peers
	peers := []int{}
	for id := range config.Peers {
		peers = append(peers, id)
	}
	p.smrLog = newLog()
	p.witness = newWitness()
	p.storage = config.Store

	p.msgCh = config.Ch

	go p.mainLoop()

	return p
}

func (p *CURPReplica) mainLoop() {
	for {
		select {
		case msg := <-p.msgCh:
			if msg.IsClient {
				p.HandleClient(msg)
			} else {
				p.HandleReplicaMsg(msg)
			}

		}
	}
}

func (p *CURPReplica) HandleClient(info *HandlerInfo) {
	cmsg := info.CMsg
	// log.Printf("new client msg: %+v", cmsg)
	reply := ClientMsg{}
	replyInfo := &HandlerInfo{
		IsClient: true,
		CMsg:     &reply,
		Res:      info.Res,
	}
	reply.Type = cmsg.Type
	reply.Seq = cmsg.Seq
	reply.ClientID = cmsg.ClientID

	e := Entry{
		Data: cmsg.Command,
	}
	success := p.witness.TryRecord(cmsg.Command.Key, e)

	if p.isLeader {
		p.smrLog.appendCmd(1, cmsg.Command)
		entries := p.smrLog.unstableEntries()
		if !success { // some unstable entries are non-commute
			p.Replicate(entries)
		} else { // if there are enough unstable entries, replicate them
			if cmsg.Command.Type == CommandTypeRead {
				value := p.predictExecuteRead(cmsg.Command)
				reply.Data = value
			} else {
				p.predictExecuteWrite(cmsg.Command)
			}
			if len(entries) >= 100 {
				p.Replicate(entries)
			}
		}
	}

	if success {
		log.Printf("replica %d, there are non-commute cmds in witness", p.me)
		reply.Success = ReplyStatusSuccess
	} else {
		reply.Success = ReplyStatusInterfere
	}

	info.Res <- replyInfo
}

func (p *CURPReplica) HandleReplicaMsg(info *HandlerInfo) {
	rmsg := info.RMsg

	for i := range rmsg.Entries {
		idx := p.smrLog.appendCmd(rmsg.Entries[i].Term, rmsg.Entries[i].Data)
		if idx != rmsg.Entries[i].Index {
			log.Fatalf("append entry %+v at a different index %v", rmsg.Entries[i], idx)
		}
	}

	reply := &ReplicaMsg{
		Type:    MsgTypeAppendReply,
		To:      rmsg.From,
		From:    p.me,
		Success: ReplyStatusSuccess,
	}
	replyInfo := &HandlerInfo{
		IsClient: false,
		RMsg:     reply,
	}

	info.Res <- replyInfo
}

func (p *CURPReplica) predictExecuteWrite(cmd Command) {
	keyStr := strconv.FormatUint(cmd.Key, 10)
	p.storage.Set([]byte(keyStr), []byte(cmd.Value))
}

func (p *CURPReplica) predictExecuteRead(cmd Command) []byte {
	keyStr := strconv.FormatUint(cmd.Key, 10)
	value := p.storage.Get([]byte(keyStr))

	return value
}

func (p *CURPReplica) Replicate(entries []Entry) {
	pentries := make([]*Entry, len(entries))
	for i := range entries {
		pentries[i] = &entries[i]
	}
	for i := range p.peersStub {
		if i == p.me {
			continue
		}
		msg := ReplicaMsg{
			Type:      MsgTypeAppend,
			To:        i,
			From:      p.me,
			CommitTo:  p.smrLog.commitAt(),
			PrevEpoch: pentries[0].Term - 1,
			PrevIdx:   pentries[0].Index - 1,
			Entries:   pentries,
		}
		go func(peer *rpc.Client, msg ReplicaMsg) {
			reply := &ReplicaMsg{}
			err := peer.Call("RPCEndpoint.ReplicaCall", msg, reply)
			if err != nil {
				log.Printf("call RPCEndpoint.ReplicaCall error: %v", err)
			}
			info := &HandlerInfo{
				IsClient: false,
				RMsg:     reply,
			}
			p.msgCh <- info
		}(p.peersStub[i], msg)
	}
}
