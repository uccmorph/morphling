package main

import (
	"log"
	"sync"
)

type networkMsg struct {
	sendBy int
	sendTo int
	msg    interface{}
}

type network struct {
	Self   int
	sendCh chan networkMsg
	recvCh chan networkMsg
}

func (p *network) Send(endPoint int, msg interface{}) {
	p.sendCh <- networkMsg{
		sendBy: p.Self,
		sendTo: endPoint,
		msg:    msg,
	}
}

func (p *network) RecvNew() (from int, msg interface{}) {
	nm := <-p.recvCh
	return nm.sendBy, nm.msg
}

type networkContext struct {
	mu         sync.Mutex
	servers    map[int]*network
	disconnect map[int]map[int]bool // server pair in this number cannot communication
}

func newNetworkContext(serverNums int) *networkContext {
	nc := &networkContext{}
	nc.mu = sync.Mutex{}
	nc.servers = make(map[int]*network)
	nc.disconnect = make(map[int]map[int]bool)
	for i := 0; i < serverNums; i++ {
		nc.servers[i] = &network{
			Self:   i,
			sendCh: make(chan networkMsg),
			recvCh: make(chan networkMsg),
		}
		nc.disconnect[i] = make(map[int]bool)
		for j := 0; j < serverNums; j++ {
			nc.disconnect[i][j] = false
		}
	}

	return nc
}

func (p *networkContext) getNetwork(id int) *network {
	return p.servers[id]
}

func (p *networkContext) Start() {
	for id, _ := range p.servers {
		go p.forwarding(p.servers[id].sendCh)
	}
}

func (p *networkContext) SetDisconn(from, to int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	log.Printf("network ctrl: disconnect %d -> %d", from, to)
	p.disconnect[from][to] = true
}

func (p *networkContext) SetConn(from, to int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	log.Printf("network ctrl: connect %d -> %d", from, to)
	p.disconnect[from][to] = false
}

func (p *networkContext) checkDisconnect(from, to int) bool {
	return p.disconnect[from][to]
}

func (p *networkContext) forwarding(ch chan networkMsg) {
	for nm := range ch {
		p.mu.Lock()
		if p.checkDisconnect(nm.sendBy, nm.sendTo) {
			p.mu.Unlock()
			continue
		}
		go func(nm networkMsg) {
			p.servers[nm.sendTo].recvCh <- nm
		}(nm)
		p.mu.Unlock()
	}
}
