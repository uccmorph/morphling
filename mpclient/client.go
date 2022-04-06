package mpclient

import (
	"log"
	"morphling/mpserverv2"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type MPClient struct {
	replicaAddr   []string
	replicas      []*rpc.Client
	guidanceSrvs  []*rpc.Client
	guide         mpserverv2.Guidance
	quorum        int
	id            int
	seq           int
	guidancePorts []string
	curp          bool

	chs      []chan *mpserverv2.ClientMsg
	replyChs []chan *mpserverv2.ClientMsg
}

func NewMPClient(rAddr []string, id int) *MPClient {
	p := &MPClient{}
	p.replicaAddr = rAddr
	p.replicas = make([]*rpc.Client, len(rAddr))
	p.guidanceSrvs = make([]*rpc.Client, len(rAddr))
	p.quorum = len(rAddr)/2 + 1
	p.id = id

	// each virtual client maintains 3 channels, one channel to one replica
	p.chs = make([]chan *mpserverv2.ClientMsg, 3)
	for i := range p.chs {
		p.chs[i] = make(chan *mpserverv2.ClientMsg)
	}
	p.replyChs = make([]chan *mpserverv2.ClientMsg, 3)
	for i := range p.replyChs {
		p.replyChs[i] = make(chan *mpserverv2.ClientMsg)
	}
	return p
}

func (p *MPClient) SetGuidancePorts(ports []string) {
	p.guidancePorts = ports
}

func (p *MPClient) SetCURPMode(curp bool) {
	p.curp = curp
}

func (p *MPClient) Disconnect() {
	for i := range p.replicas {
		p.replicas[i].Close()
	}
	// log.Printf("disconnect client %v", p.id)
}

func (p *MPClient) Connet() {
	wg := sync.WaitGroup{}
	for i := range p.replicaAddr {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				end, err := rpc.DialHTTP("tcp", p.replicaAddr[i])
				if err != nil {
					log.Printf("dial %v error: %v", p.replicaAddr[i], err)
					time.Sleep(time.Second)
					continue
				}
				p.replicas[i] = end
				if p.curp {
					return
				}
				var guidanceSrv string
				if len(p.guidancePorts) != 0 {
					guidanceSrv = strings.Split(p.replicaAddr[i], ":")[0] + ":" + p.guidancePorts[i]
				} else {
					// hard code guidanceSrv port to 9996
					guidanceSrv = strings.Split(p.replicaAddr[i], ":")[0] + ":" + "9996"
				}
				gEnd, err := rpc.DialHTTP("tcp", guidanceSrv)
				if err != nil {
					log.Fatalf("cannot connect to guidance service: %s, %v", guidanceSrv, err)
				}
				p.guidanceSrvs[i] = gEnd
				// log.Printf("replica %v connected", p.replicaAddr[i])
				return
			}
		}(i)
	}
	wg.Wait()
}

func (p *MPClient) GetGuidance() {
	if p.curp {
		p.RunReplicaLoop()
		return
	}
	ch := make(chan *mpserverv2.Guidance)
	count := 0
	total := 0

	for i := range p.replicas {
		total += 1
		go func(i int) {
			args := &mpserverv2.ClientMsg{
				Type: mpserverv2.MsgTypeGetGuidance,
			}
			reply := &mpserverv2.ClientMsg{}
			// log.Printf("ready to call %v", p.replicaAddr[i])
			err := p.replicas[i].Call("RPCEndpoint.ClientCall", args, reply)
			if err != nil {
				log.Printf("call %v GetGuidance error: %v", i, err)
				ch <- nil
				return
			}
			// log.Printf("guidance: %+v", reply.Guide)
			ch <- reply.Guide
		}(i)
	}

	for guide := range ch {
		count += 1
		if guide == nil {
			continue
		}

		if count == total {
			p.guide = *guide
			close(ch)
			// log.Printf("close ch")
		}
	}
	// log.Printf("finish GetGuidance")

	p.RunReplicaLoop()
}

func (p *MPClient) RunReplicaLoop() {
	for i := range p.chs {
		go func(i int) {
			log.Printf("start client [%d] loop to replica %d", p.id, i)
			for cmsg := range p.chs[i] {
				reply := &mpserverv2.ClientMsg{}
				log.Printf("ready to call %v", p.replicaAddr[i])
				if cmsg.Type == mpserverv2.MsgTypeGetGuidance {
					err := p.replicas[i].Call("GuidanceEndpoint.Call", cmsg, reply)
					if err != nil {
						log.Fatalf("call %v msg type %v error: %v", i, cmsg.Type, err)
					}
				} else {
					err := p.replicas[i].Call("RPCEndpoint.ClientCall", cmsg, reply)
					if err != nil {
						log.Fatalf("call %v msg type %v error: %v", i, cmsg.Type, err)
					}
				}
				p.replyChs[i] <- reply
			}
		}(i)
	}
}

func (p *MPClient) UnreplicateReadKV(key uint64) (string, error) {
	p.seq += 1
	args := &mpserverv2.ClientMsg{
		Type:     mpserverv2.MsgTypeClientRead,
		Guide:    &p.guide,
		KeyHash:  key,
		Seq:      p.seq,
		ClientID: p.id,
		Command: mpserverv2.Command{
			Type: mpserverv2.CommandTypeRead,
		},
	}

	sendTo := 0
	reply := &mpserverv2.ClientMsg{}
	// log.Printf("ready to call %v", p.replicaAddr[sendTo])
	err := p.replicas[sendTo].Call("RPCEndpoint.ClientCall", args, reply)
	if err != nil {
		log.Printf("call %v MsgTypeClientRead error: %v", sendTo, err)
		return "", err
	}
	return string(reply.Data), nil
}

func (p *MPClient) RaftReadKV(key uint64) (string, error) {
	p.seq += 1
	args := &mpserverv2.ClientMsg{
		Type:     mpserverv2.MsgTypeClientProposal,
		Guide:    &p.guide,
		KeyHash:  key,
		Seq:      p.seq,
		ClientID: p.id,
		Command: mpserverv2.Command{
			Type: mpserverv2.CommandTypeRead,
			Key:  key,
		},
	}

	sendTo := 0
	reply := &mpserverv2.ClientMsg{}
	err := p.replicas[sendTo].Call("RPCEndpoint.ClientCall", args, reply)
	if err != nil {
		log.Printf("call %v MsgTypeClientProposal error: %v", sendTo, err)
		return "", err
	}
	return string(reply.Data), nil
}

func (p *MPClient) ReadKVFast(key uint64) (string, error) {
	p.seq += 1
	seq := p.seq
	readArgs := &mpserverv2.ClientMsg{
		Type:     mpserverv2.MsgTypeClientRead,
		Guide:    &p.guide,
		KeyHash:  key,
		Seq:      p.seq,
		ClientID: p.id,
		Command: mpserverv2.Command{
			Type: mpserverv2.CommandTypeRead,
		},
	}
	gdArgs := &mpserverv2.ClientMsg{
		Type: mpserverv2.MsgTypeGetGuidance,
		Seq:  p.seq,
	}

	keyPos := mpserverv2.CalcKeyPos(key, p.guide.GroupMask, p.guide.GroupSize)
	sendTo := p.guide.ReplicaID(keyPos)
	// log.Printf("key 0x%08x pos 0x%x send to replica %d", key, keyPos, sendTo)

	for i := range p.chs {
		if i == sendTo {
			p.chs[i] <- readArgs
		} else {
			p.chs[i] <- gdArgs
		}
	}
	var res string

	var quorum uint32
	reachQuorum := make(chan bool, 1)
	var prlReplied uint32 = 0
	for i := range p.replyChs {
		go func(chi int) {
			resp := <-p.replyChs[chi]
			if chi == sendTo {
				if resp.Success != mpserverv2.ReplyStatusSuccess {
					log.Fatalf("client read fail code: %d", resp.Success)
				}
				res = string(resp.Data)
				atomic.AddUint32(&quorum, 1)
				atomic.StoreUint32(&prlReplied, 1)
			} else {
				if resp.Seq != seq {
					log.Fatalf("stale GetGuidance resp seq %d", resp.Seq)
				}
				atomic.AddUint32(&quorum, 1)
			}
			if atomic.LoadUint32(&quorum) >= uint32(p.quorum) && atomic.LoadUint32(&prlReplied) == 1 {
				select {
				case reachQuorum <- true:
				default:
					// log.Printf("resp %+v no need for quorum", resp)
				}
			}
		}(i)
	}

	<-reachQuorum
	return res, nil
}

func (p *MPClient) CURPReadKV(key uint64) (string, error) {
	p.seq += 1
	for i := range p.chs {
		args := &mpserverv2.ClientMsg{
			Type:     mpserverv2.MsgTypeClientCURPProposal,
			Seq:      p.seq,
			ClientID: p.id,
			Command: mpserverv2.Command{
				Type: mpserverv2.CommandTypeRead,
				Key:  key,
			},
		}
		p.chs[i] <- args
	}

	var value string
	var q quorum
	q.target = 2
	quorumWait := make(chan bool, 1)
	for i := range p.replyChs {
		go func(chi int) {
			reply := <-p.replyChs[chi]
			if chi == 0 {
				if reply.Success == mpserverv2.ReplyStatusInterfere {
					log.Printf("replica %d, this cmd is non-commute %v", chi, reply.Seq)
					q.Reject()
				} else {
					value = string(reply.Data)
					q.Accept()
				}
			} else {
				if reply.Success == mpserverv2.ReplyStatusInterfere {
					log.Printf("replica %d, this cmd is non-commute %v", chi, reply.Seq)
					q.Reject()
				} else {
					q.Accept()
				}
			}

			if q.EnoughAccept() {
				quorumWait <- true
			}
			if q.EnoughReject() {
				quorumWait <- false
			}
		}(i)
	}

	wait := <-quorumWait
	if !wait {
		args := &mpserverv2.ClientMsg{
			Type:     mpserverv2.MsgTypeClientProposal,
			Seq:      p.seq,
			ClientID: p.id,
			Command: mpserverv2.Command{
				Type: mpserverv2.CommandTypeRead,
				Key:  key,
			},
		}
		sendTo := 0
		reply := &mpserverv2.ClientMsg{}
		err := p.replicas[sendTo].Call("RPCEndpoint.ClientCall", args, reply)
		if err != nil {
			log.Printf("call %v MsgTypeClientProposal error: %v", sendTo, err)
			return "", err
		}
		value = string(args.Data)
	}

	return value, nil
}

func (p *MPClient) WriteKV(key uint64, value string) error {
	p.seq += 1
	args := &mpserverv2.ClientMsg{
		Type:     mpserverv2.MsgTypeClientProposal,
		Guide:    &p.guide,
		KeyHash:  key,
		ClientID: p.id,
		Seq:      p.seq,
		Command: mpserverv2.Command{
			Type:  mpserverv2.CommandTypeWrite,
			Key:   key,
			Value: value,
		},
	}
	keyPos := mpserverv2.CalcKeyPos(key, p.guide.GroupMask, p.guide.GroupSize)
	sendTo := p.guide.ReplicaID(keyPos)

	reply := &mpserverv2.ClientMsg{}
	err := p.replicas[sendTo].Call("RPCEndpoint.ClientCall", args, reply)
	if err != nil {
		log.Printf("call %v MsgTypeClientProposal error: %v", sendTo, err)
		return err
	}

	return nil
}

func (p *MPClient) UnreplicatedWriteKV(key uint64, value string) error {
	p.seq += 1
	args := &mpserverv2.ClientMsg{
		Type:     mpserverv2.MsgTypeClientWrite,
		Guide:    &p.guide,
		KeyHash:  key,
		ClientID: p.id,
		Seq:      p.seq,
		Command: mpserverv2.Command{
			Type:  mpserverv2.CommandTypeWrite,
			Key:   key,
			Value: value,
		},
	}

	sendTo := 0
	reply := &mpserverv2.ClientMsg{}
	err := p.replicas[sendTo].Call("RPCEndpoint.ClientCall", args, reply)
	if err != nil {
		log.Printf("call %v MsgTypeClientWrite error: %v", sendTo, err)
		return err
	}

	return nil
}

func (p *MPClient) RaftWriteKV(key uint64, value string) error {
	p.seq += 1
	args := &mpserverv2.ClientMsg{
		Type:     mpserverv2.MsgTypeClientProposal,
		Guide:    &p.guide,
		KeyHash:  key,
		ClientID: p.id,
		Seq:      p.seq,
		Command: mpserverv2.Command{
			Type:  mpserverv2.CommandTypeWrite,
			Key:   key,
			Value: value,
		},
	}

	sendTo := 0
	reply := &mpserverv2.ClientMsg{}
	err := p.replicas[sendTo].Call("RPCEndpoint.ClientCall", args, reply)
	if err != nil {
		log.Printf("call %v MsgTypeClientProposal error: %v", sendTo, err)
		return err
	}

	return nil
}
