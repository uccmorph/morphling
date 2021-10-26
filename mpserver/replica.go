package mpserver

type Replica struct {
	clusterMethod
	clientMethod
}

type clusterMethod interface {
	SendStatus()
	Connect()
}

type clientMethod interface {
	WaitClient()
}

type LocalNetwork struct {
	clusterMethod
	clientMethod
}

func (p *LocalNetwork) SendStatus() {}

func (p *LocalNetwork) Connect() {}

func (p *LocalNetwork) WaitClient() {}

func CreateReplica() *Replica {
	n := new(LocalNetwork)
	p := &Replica{
		clusterMethod: n,
		clientMethod:  n,
	}

	return p
}
