package mpclient

import "sync/atomic"

type quorum struct {
	accepts uint64
	reject  uint64

	target uint64
}

func (p *quorum) Accept() {
	atomic.AddUint64(&p.accepts, 1)
}

func (p *quorum) Reject() {
	atomic.AddUint64(&p.reject, 1)
}

func (p *quorum) EnoughAccept() bool {
	return atomic.LoadUint64(&p.accepts) == p.target
}

func (p *quorum) EnoughReject() bool {
	return atomic.LoadUint64(&p.reject) == p.target
}
