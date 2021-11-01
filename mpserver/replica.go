package mpserver

type Guidance struct {
	Epoch     uint64
	AliveNum  uint64
	GroupMask uint64 // KeyPos = KeyHash & GroupMask >> GroupSize.
	GroupSize uint64 // The number of  1 digits in KeyMask, however it can be deduced from GroupMask
	Cluster   []ReplicaStatus
}

type ReplicaStatus struct {
	Alive       bool
	StartKeyPos uint64
	EndKeyPos   uint64
}

type logEntry struct {
	Epoch uint64
	Index uint64
	Cmd   interface{}
}

type groupLogger struct {
	position uint64 // if KeyPos == groupLogger.position, then this key's command should be stored here. position is also equal to the index of this groupLogger.
	glog     []logEntry
}

type Replica struct {
	localGuidance Guidance
	log           []groupLogger
}

func CreateReplica() *Replica {
	p := &Replica{}

	return p
}

func mainLoop() {

}
