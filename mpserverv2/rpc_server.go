package mpserverv2

type RPCEndpoint struct {
	MsgChan chan *HandlerInfo
	Replica *Replica
}

func (p *RPCEndpoint) Init() {

	p.Replica.msgCh = p.MsgChan
}

func (p *RPCEndpoint) ReplicaCall(args *ReplicaMsg, reply *ReplicaMsg) error {
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
	close(res)
	return nil
}
