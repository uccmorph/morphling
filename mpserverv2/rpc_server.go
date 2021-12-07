package mpserverv2

import (
	"sync/atomic"
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

	info := <-res
	*reply = *info.RMsg
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
	info := <-res
	// log.Printf("get reply: %+v", info.CMsg)
	*reply = *info.CMsg
	return nil
}
