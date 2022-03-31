package main

import (
	"time"
)

var clusterSize = 3

func main() {

	nc := newNetworkContext(clusterSize)

	nc.Start()

	for i := 0; i < clusterSize; i++ {
		s := NewServer(nc.getNetwork(i), clusterSize)
		s.Start()
	}

	disconnT := time.NewTimer(time.Second * 5)
	connT := time.NewTimer(time.Second * 15)
	finishT := time.NewTimer(time.Second * 20)
	finish := false
	for !finish {
		select {
		case <-disconnT.C:
			nc.SetDisconn(2, 1)
			nc.SetDisconn(2, 0)

		case <-connT.C:
			nc.SetConn(2, 1)
			nc.SetConn(2, 0)

		case <-finishT.C:
			finish = true
		}
	}
}
