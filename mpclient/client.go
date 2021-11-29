package mpclient

import (
	"log"
	"morphling/mpserverv2"
	"net/rpc"
	"sync"
	"time"
)

type ClientEndPoint struct {
	replicaAddr []string
	replicas    []*rpc.Client
}

func NewClientEntPoint(rAddr []string) *ClientEndPoint {
	p := &ClientEndPoint{}
	p.replicaAddr = rAddr
	p.replicas = make([]*rpc.Client, len(rAddr))
	return p
}

func (p *ClientEndPoint) Connet() {
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

func (p *ClientEndPoint) GetGuidance() {
	for i := range p.replicas {
		go func(i int) {
			args := &mpserverv2.ClientMsg{
				Type: mpserverv2.MsgTypeGetGuidance,
			}
			reply := &mpserverv2.ClientMsg{}
			log.Printf("ready to call %v", p.replicaAddr[i])
			err := p.replicas[i].Call("RPCEndpoint.ClientCall", args, reply)
			if err != nil {
				log.Printf("call %v GetGuidance error: %v", i, err)
				return
			}
			log.Printf("guidance: %+v", reply.Guide)
		}(i)
	}
}

func (p *ClientEndPoint) InsertKV() {}
