package mpserverv2

const DefaultKeySpace uint64 = 256
const DefaultKeyMask uint64 = 0xff000000
const DefaultKeyShiftBits uint64 = 6 * 4
const DefaultClusterSize int = 3

type ReplicaStatus struct {
	Alive       bool
	StartKeyPos uint64
	EndKeyPos   uint64
}

func (p *ReplicaStatus) KeyisIn(pos uint64) bool {
	if !p.Alive {
		return false
	}
	if p.StartKeyPos > p.EndKeyPos {
		if p.StartKeyPos <= pos && pos < DefaultKeySpace {
			return true
		} else if 0 <= pos && pos < p.EndKeyPos {
			return true
		}
	} else {
		if p.StartKeyPos <= pos && pos < p.EndKeyPos {
			return true
		}
	}
	return false
}

type Guidance struct {
	Epoch   uint64
	Cluster []ReplicaStatus
}

func (p *Guidance) InitDefault(size uint64) {
	p.Epoch = 1
	p.Cluster = make([]ReplicaStatus, size)
	for i := range p.Cluster {
		p.Cluster[i].Alive = true
		p.Cluster[i].StartKeyPos = uint64(i) * DefaultKeySpace / size
		p.Cluster[i].EndKeyPos = uint64(i+1) * DefaultKeySpace / size
	}
}

func (p *Guidance) ReplicaID(pos uint64) int {
	for i := range p.Cluster {
		if p.Cluster[i].KeyisIn(pos) {
			return i
		}
	}
	return -1
}

func (p *Guidance) Clone() *Guidance {
	guide := &Guidance{}
	guide.Epoch = p.Epoch
	guide.Cluster = make([]ReplicaStatus, len(p.Cluster))
	copy(guide.Cluster, p.Cluster)

	return guide
}

func (p *Guidance) CopyFrom(g *Guidance) {
	p.Epoch = g.Epoch
	if len(p.Cluster) != len(g.Cluster) {
		panic("copy guidance, but with different cluster size")
	}
	copy(p.Cluster, g.Cluster)
}

func CalcKeyPos(key uint64) uint64 {
	return key & DefaultKeyMask >> DefaultKeyShiftBits
}
