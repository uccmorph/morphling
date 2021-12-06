package mpclient

import (
	"errors"
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
	}
	keyPos := mpserverv2.CalcKeyPos(key, p.guide.GroupMask, p.guide.GroupSize)
	sendTo := p.guide.ReplicaID(keyPos)
	replyCh := make(chan *mpserverv2.ClientMsg)

	// log.Printf("read kv in replica %v", sendTo)
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			reply := &mpserverv2.ClientMsg{}
			// log.Printf("ready to call %v", p.replicaAddr[sendTo])
			err := p.replicas[sendTo].Call("RPCEndpoint.ClientCall", args, reply)
			if err != nil {
				log.Printf("call %v MsgTypeClientRead error: %v", sendTo, err)
				replyCh <- nil
				return
			}
			replyCh <- reply
			// log.Printf("replica %v read result: %v", sendTo, string(reply.Data))
		}()

		for i := range p.replicas {
			if i == sendTo {
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
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

		wg.Wait()
		close(replyCh)
	}()

	var resultStr string
	var targetGuidance mpserverv2.Guidance
	gm := make(map[uint64]int)
	for res := range replyCh {
		if res.Type == mpserverv2.MsgTypeClientRead {
			resultStr = string(res.Data)
			targetGuidance = *res.Guide
		}
		if _, ok := gm[res.Guide.Epoch]; !ok {
			gm[res.Guide.Epoch] = 1
		} else {
			gm[res.Guide.Epoch] += 1
		}
	}

	if gm[targetGuidance.Epoch] < p.quorum {
		return "", errors.New("invalid quorum")
	}
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
