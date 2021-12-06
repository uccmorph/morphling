package mpserverv2

type RPCEndpoint struct {
	MsgChan chan *HandlerInfo
	Replica *Replica
}

func (p *RPCEndpoint) ReplicaCall(args *ReplicaMsg, reply *ReplicaMsg) error {
	res := make(chan *HandlerInfo)
	// log.Printf("ReplicaCall: %+v", args)
	p.MsgChan <- &HandlerInfo{
		IsClient: false,
		Res:      res,
		Args:     args,
		Reply:    reply,
	}

	info := <-res
	*reply = *info.Reply
	// log.Printf("reply replica: %+v", reply)
	return nil
}

func (p *RPCEndpoint) ClientCall(args *ClientMsg, reply *ClientMsg) error {
	// log.Printf("receive client msg: %+v", args)
	res := make(chan *HandlerInfo)
	p.MsgChan <- &HandlerInfo{
		IsClient: true,
		Res:      res,
		Cargs:    args,
		Creply:   reply,
	}
	info := <-res
	*reply = *info.Creply
	return nil
}
