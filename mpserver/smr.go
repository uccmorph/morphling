// The core of SMR(State Machine Replication)
package mpserver

import (
	"context"
	"fmt"
	"time"
)

type ReplicaMsg struct {
	Type int
	From uint64
	Meta interface{}
	Data interface{}
}

func CreateReplicationMsg(self uint64, meta, data interface{}) *ReplicaMsg {
	return &ReplicaMsg{
		Type: 0,
		From: self,
		Meta: meta,
		Data: data,
	}
}

func CreateSyncMsg(self uint64, meta interface{}) *ReplicaMsg {
	return &ReplicaMsg{
		Type: 2,
		From: self,
		Meta: meta,
	}
}

type entryContext struct {
	entry        interface{}
	emeta        interface{}
	retryCount   uint64
	peerResultCh chan *voteResult
	quorumCh     chan int
	agreeCount   uint64
	rejectCount  uint64
	peerVotes    []bool
	done         context.Context
	cancelFn     context.CancelFunc
}

type SMRServer struct {
	log     SMRStorage
	network SMRNetwork
	// CheckSafety is for follower to check validity, racing and safety
	CheckSafety func(ectx *entryContext) bool

	quorum      uint64
	clusterSize uint64 // ID is numbered from 0, it may bring difficulties when reconfig
	me          uint64
	peersID     []uint64
}

type SMRStorage interface {
	// Store() return entry meta, so SMRServer can keep it and used for commit or apply
	Store(entry interface{}) interface{}
	Exist(entry interface{}) bool
	CheckSafety(emeta interface{}) bool
	Commit(meta interface{})
}

type SMRNetwork interface {
	Send(target uint64, msg interface{})
	Recv(target uint64) interface{}
}

func NewSMRServer(nums uint64, me uint64, s SMRStorage, n SMRNetwork) *SMRServer {
	p := &SMRServer{}
	p.clusterSize = nums
	p.quorum = (nums + 1) / 2
	p.me = me
	p.log = s
	p.network = n
	p.peersID = make([]uint64, 0, nums)
	// generate default peer id
	for i := 0; i < int(nums); i++ {
		if uint64(i) == me {
			continue
		}
		p.peersID = append(p.peersID, uint64(i))
	}

	return p
}

func (p *SMRServer) HandleNewEntry(entry interface{}) {
	ctx, cancel := context.WithCancel(context.TODO())
	ectx := &entryContext{
		entry:        entry,
		agreeCount:   1,
		peerResultCh: make(chan *voteResult),
		quorumCh:     make(chan int),
		peerVotes:    make([]bool, p.clusterSize),
		done:         ctx,
		cancelFn:     cancel,
	}
	defer func() {
		close(ectx.peerResultCh)
		close(ectx.quorumCh)
	}()
	ectx.peerVotes[p.me] = true

	p.storeLocal(ectx)
	p.replicate(ectx)
	p.commitLocal(ectx)
}

func (p *SMRServer) HandleReplicaMsg(msg *ReplicaMsg) {
	switch msg.Type {
	case 0:
		// replicate msg
		p.processPeerReplication(msg.Meta, msg.Data)
	case 1:
		// recovery msg
	case 2:
		// sync commit
	}
}

func (p *SMRServer) processPeerReplication(emeta, entry interface{}) {
	if !p.log.CheckSafety(emeta) {
		return
	}
	p.log.Store(entry)
}

func (p *SMRServer) PullMissingEntry(meta interface{}) {

}

func (p *SMRServer) Sync(emeta interface{}) {

}

func (p *SMRServer) SyncWithEntry(emeta, entry interface{}) {

}

func (p *SMRServer) replicate(ectx *entryContext) {
	p.asyncGatherVotes(ectx)

RETRY:
	p.bcastMsg(ectx, CreateReplicationMsg(p.me, ectx.emeta, ectx.entry))
	res := p.waitQuorum(ectx)
	if res != 0 {
		if ectx.retryCount == 10 {
			p.giveUp(ectx)
			return
		}
		if res == 1 {
			ectx.retryCount += 1
			goto RETRY
		} else if res == 2 {
			p.giveUp(ectx)
		}
	}
}

func (p *SMRServer) bcastMsg(ectx *entryContext, msg *ReplicaMsg) {
	for id := 0; id < int(p.clusterSize); id++ {
		if id == int(p.me) {
			continue
		}
		if ectx.peerVotes[id] {
			continue
		}
		p.asyncSendMsg(uint64(id), ectx, msg)
	}
}

/*
result:
	0 for success;
	1 for need retry this entry;
	2 for need sync and give up this entry;
	3 for notify staleness
*/
type voteResult struct {
	ID     uint64
	result uint64
}

func (p *SMRServer) asyncGatherVotes(ectx *entryContext) {
	go func() {
		for res := range ectx.peerResultCh {
			if ectx.peerVotes[res.ID] {
				continue
			}
			if res.result == 0 {
				// An agree vote
				ectx.peerVotes[res.ID] = true
				ectx.agreeCount += 1
				if ectx.agreeCount == p.quorum {
					ectx.quorumCh <- 0
				}
			} else if res.result == 1 {
				// todo: handle executor timeout, or just ignore
				p.asyncSendMsg(res.ID, ectx, CreateReplicationMsg(p.me, ectx.emeta, ectx.entry))
			} else if res.result == 2 {
				// A reject vote
				ectx.peerVotes[res.ID] = true
				ectx.rejectCount += 1
				if ectx.rejectCount == p.quorum {
					ectx.quorumCh <- 2
				}

			}
		}
	}()
}

func (p *SMRServer) waitQuorum(ectx *entryContext) int {
	timeout := time.NewTimer(time.Millisecond * 1000)
	select {
	case res := <-ectx.quorumCh:
		if !timeout.Stop() {
			<-timeout.C
		}
		return res
	case <-timeout.C:
		return 1
	}
}

func (p *SMRServer) asyncSendMsg(id uint64, ectx *entryContext, msg *ReplicaMsg) {
	go func() {
		p.network.Send(id, msg)
		timeout := time.NewTimer(time.Millisecond * 1000)
		select {
		case <-ectx.done.Done():
			if !timeout.Stop() {
				<-timeout.C
			}
			return
		case <-timeout.C:
			ectx.peerResultCh <- &voteResult{
				ID:     id,
				result: 1,
			}
		}
		res := p.network.Recv(id)
		voteRes, ok := res.(*voteResult)
		if !ok {
			panic(fmt.Sprintf("cast reply (%+v) to voteResult failed", res))
		}
		ectx.peerResultCh <- voteRes
	}()
}

func (p *SMRServer) giveUp(ectx *entryContext) {
	ectx.cancelFn()
}

// should store to a proper slot of the log
func (p *SMRServer) storeLocal(ectx *entryContext) {
	ectx.emeta = p.log.Store(ectx.entry)
}

func (p *SMRServer) commitLocal(ectx *entryContext) {
	p.log.Commit(ectx.emeta)
}
