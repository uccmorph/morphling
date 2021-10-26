package mpserver

type ReplicaMsg struct {
	Type int
	Data interface{}
}

type SMRServer struct {
	CheckSafety func() bool
	BcastMsg    func(msg *ReplicaMsg)
	WaitQuorum  func() int
	GiveUp      func()
	StoreLocal  func()
	CommitLocal func()
}

func (p *SMRServer) Replicate(entry interface{}) {
	if !p.CheckSafety() {
		p.GiveUp()
		return
	}
	success := false
	for !success {
		p.BcastMsg(&ReplicaMsg{
			Type: 0,
			Data: entry,
		})
		if res := p.WaitQuorum(); res != 0 {
			if res == 1 {
				continue
			} else if res == 2 {
				p.GiveUp()
			}
		} else {
			success = true
		}
	}
}

func (p *SMRServer) HandleNewEntry(entry interface{}) {
	p.StoreLocal()
	p.Replicate(entry)
	p.CommitLocal()
}
