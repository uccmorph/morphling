package mpserverv2

import "log"

type HandlerInfo struct {
	Type   msgType
	Args   *ReplicaMsg
	Reply  *ReplicaMsg
	Cargs  *ClientMsg
	Creply *ClientMsg
	Res    chan *HandlerInfo
}

type RPCEndpoint struct {
	MsgChan chan *HandlerInfo
}

func (p *RPCEndpoint) ReplicaCall(args *ReplicaMsg, reply *ReplicaMsg) error {
	return nil
}

func (p *RPCEndpoint) ClientCall(args *ClientMsg, reply *ClientMsg) error {
	log.Printf("receive client msg: %+v", args)
	res := make(chan *HandlerInfo)
	p.MsgChan <- &HandlerInfo{
		Res:    res,
		Cargs:  args,
		Creply: reply,
	}
	info := <-res
	*reply = *info.Creply
	close(res)
	return nil
}
