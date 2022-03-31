package mpserverv2

import (
	"sync/atomic"
	"time"
)

type RPCEndpoint struct {
	MsgChan chan *HandlerInfo
	Replica *Replica

	uuid uint64
}

func (p *RPCEndpoint) ReplicaCall(args *ReplicaMsg, reply *ReplicaMsg) error {
	res := make(chan *HandlerInfo)
	// log.Printf("ReplicaCall: %+v", args)
	p.MsgChan <- &HandlerInfo{
		IsClient: false,
		Res:      res,
		RMsg:     args,
	}

	timer := time.NewTimer(3 * time.Second)
	// close(res) may cause panic. take care when we send msg to res.
	select {
	case <-timer.C:
		close(res)
		reply.Success = replyStatusTimeout
	case info := <-res:
		if !timer.Stop() {
			<-timer.C
		}
		*reply = *info.RMsg
	}

	// log.Printf("reply replica: %+v", reply)
	return nil
}

func (p *RPCEndpoint) ClientCall(args *ClientMsg, reply *ClientMsg) error {
	// log.Printf("receive client msg: %+v", args)
	uuid := atomic.AddUint64(&p.uuid, 1)
	res := make(chan *HandlerInfo)
	p.MsgChan <- &HandlerInfo{
		IsClient: true,
		Res:      res,
		CMsg:     args,
		UUID:     uuid,
	}
	timer := time.NewTimer(3 * time.Second)
	select {
	case <-timer.C:
		close(res)
		reply.Success = replyStatusTimeout
	case info := <-res:
		if !timer.Stop() {
			<-timer.C
		}
		*reply = *info.CMsg
	}
	// log.Printf("get reply: %+v", info.CMsg)

	return nil
}

type GuidanceEndpoint struct {
	MsgChan chan *HandlerInfo
	uuid    uint64
}

func (p *GuidanceEndpoint) Call(args *ClientMsg, reply *ClientMsg) error {
	res := make(chan *HandlerInfo)
	p.MsgChan <- &HandlerInfo{
		IsClient: true,
		Res:      res,
		CMsg:     args,
	}
	timer := time.NewTimer(3 * time.Second)
	select {
	case <-timer.C:
		close(res)
		reply.Success = replyStatusTimeout
	case info := <-res:
		if !timer.Stop() {
			<-timer.C
		}
		*reply = *info.CMsg
	}
	// log.Printf("get reply: %+v", info.CMsg)

	return nil
}
