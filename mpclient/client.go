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
	replicaAddr  []string
	replicas     []*rpc.Client
	guidanceSrvs []*rpc.Client
	guide        mpserverv2.Guidance
	quorum       int
	id           int
	seq          int

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
				// hard code guidanceSrv port to 9996
				guidanceSrv := strings.Split(p.replicaAddr[i], ":")[0] + ":" + "9996"
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
			// log.Printf("start client [%d] loop to replica %d", p.id, i)
			for cmsg := range p.chs[i] {
				reply := &mpserverv2.ClientMsg{}
				// log.Printf("ready to call %v", p.replicaAddr[i])
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
	// keyPos := mpserverv2.CalcKeyPos(key, p.guide.GroupMask, p.guide.GroupSize)
	// sendTo := p.guide.ReplicaID(keyPos)
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
	// log.Printf("client %v send seq %v", p.id, p.seq)
	// keyPos := mpserverv2.CalcKeyPos(key, p.guide.GroupMask, p.guide.GroupSize)
	// sendTo := p.guide.ReplicaID(keyPos)
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

	// gdchs := make([]chan *mpserverv2.ClientMsg, 0)
	// for i := range p.replyChs {
	// 	if i == sendTo {
	// 		continue
	// 	}
	// 	gdchs = append(gdchs, p.replyChs[i])
	// }

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

	// select {
	// case re := <-p.replyChs[sendTo]:
	// 	res = string(re.Data)
	// }
	// for i := range gdchs {
	// 	<-gdchs[i]
	// }

	<-reachQuorum
	return res, nil
}

func (p *MPClient) ReadKV(key uint64) (string, error) {

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
	keyPos := mpserverv2.CalcKeyPos(key, p.guide.GroupMask, p.guide.GroupSize)
	sendTo := p.guide.ReplicaID(keyPos)
	replyCh := make(chan *mpserverv2.ClientMsg)
	mainCh := make(chan *mpserverv2.ClientMsg)
	resCh := make(chan string)

	// log.Printf("read kv in replica %v", sendTo)
	go func() {

		go func() {
			reply := &mpserverv2.ClientMsg{}
			// log.Printf("ready to call %v", p.replicaAddr[sendTo])
			err := p.replicas[sendTo].Call("RPCEndpoint.ClientCall", args, reply)
			if err != nil {
				log.Printf("call %v MsgTypeClientRead error: %v", sendTo, err)
				mainCh <- nil
				return
			}
			// log.Printf("replica %v read result: %v", sendTo, string(reply.Data))
			mainCh <- reply
		}()

		for i := range p.replicas {
			if i == sendTo {
				continue
			}
			go func(i int) {
				args := &mpserverv2.ClientMsg{
					Type: mpserverv2.MsgTypeGetGuidance,
				}
				reply := &mpserverv2.ClientMsg{}
				// log.Printf("ready to call %v", p.replicaAddr[i])
				err := p.replicas[i].Call("RPCEndpoint.ClientCall", args, reply)
				if err != nil {
					log.Printf("call %v GetGuidance error: %v", i, err)
					return
				}
				// log.Printf("replica %v guidance: %+v", i, reply.Guide)
				replyCh <- reply
			}(i)
		}

		var res string
		var targetEpoch uint64
		gm := make(map[uint64]int)
		for {
			select {
			case mainRes := <-mainCh:
				res = string(mainRes.Data)
				targetEpoch = mainRes.Guide.Epoch
				gm[targetEpoch] += 1

			case guiRes := <-replyCh:
				gm[guiRes.Guide.Epoch] += 1
			}
			// log.Printf("gm: %+v", gm)
			if gm[targetEpoch] >= p.quorum {
				select {
				case resCh <- res:
					// log.Printf("send res")
				default:
				}
			}
		}
	}()

	resultStr := <-resCh
	// log.Printf("finish ReadKV")
	return resultStr, nil
}

func (p *MPClient) WriteKV(key uint64, value string) error {
	p.seq += 1
	args := &mpserverv2.ClientMsg{
		Type:     mpserverv2.MsgTypeClientProposal,
		Guide:    &p.guide,
		KeyHash:  key,
		Data:     []byte(value),
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
