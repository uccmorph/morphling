package mpclient

import (
	"log"
	"morphling/mpserverv2"
	"net/rpc"
	"sync"
	"time"
)

type MPClient struct {
	replicaAddr []string
	replicas    []*rpc.Client
	guide       mpserverv2.Guidance
	quorum      int
	id          int
	seq         int
}

func NewMPClient(rAddr []string, id int) *MPClient {
	p := &MPClient{}
	p.replicaAddr = rAddr
	p.replicas = make([]*rpc.Client, len(rAddr))
	p.quorum = len(rAddr)/2 + 1
	p.id = id
	return p
}

func (p *MPClient) Connet() {
	wg := sync.WaitGroup{}
	for i := range p.replicaAddr {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				client, err := rpc.DialHTTP("tcp", p.replicaAddr[i])
				if err != nil {
					log.Printf("dial %v error: %v", p.replicaAddr[i], err)
					time.Sleep(time.Second)
					continue
				}
				p.replicas[i] = client
				log.Printf("replica %v connected", p.replicaAddr[i])
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
			log.Printf("ready to call %v", p.replicaAddr[i])
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
			log.Printf("close ch")
		}
	}
	log.Printf("finish GetGuidance")
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
	keyPos := mpserverv2.CalcKeyPos(key, p.guide.GroupMask, p.guide.GroupSize)
	sendTo := p.guide.ReplicaID(keyPos)
	reply := &mpserverv2.ClientMsg{}
	err := p.replicas[sendTo].Call("RPCEndpoint.ClientCall", args, reply)
	if err != nil {
		log.Printf("call %v MsgTypeClientProposal error: %v", sendTo, err)
		return "", err
	}
	return string(reply.Data), nil
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
