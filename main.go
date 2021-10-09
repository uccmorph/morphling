package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var maxKeyHash uint64 = 400

type Guidance struct {
	Epoch       uint64
	ClusterSize uint64
	LiveR       []ReplicaStatus
}

type ReplicaStatus struct {
	Alive    bool
	StartKey uint64
	EndKey   uint64
}

func (p *Guidance) createDefault() {
	p.LiveR = make([]ReplicaStatus, p.ClusterSize)
	for i := range p.LiveR {
		p.LiveR[i].Alive = true
		p.LiveR[i].StartKey = uint64(i) * maxKeyHash / p.ClusterSize
		p.LiveR[i].EndKey = uint64(i+1) * maxKeyHash / p.ClusterSize
	}
}

func (p *Guidance) exclude(ri uint64) {
	p.Epoch += uint64(clusterSize)
	p.LiveR[ri].Alive = false
	p.LiveR[ri].StartKey = 0
	p.LiveR[ri].EndKey = 0
	p.ClusterSize -= 1
	for i := range p.LiveR {
		if !p.LiveR[i].Alive {
			continue
		}
		p.LiveR[i].StartKey = uint64(i) * maxKeyHash / p.ClusterSize
		p.LiveR[i].EndKey = uint64(i+1) * maxKeyHash / p.ClusterSize
	}
}

func (p *Guidance) include(ri uint64) {

}

type RPCStub int

type GossipRPCArgs struct {
	Guide    Guidance
	Workload uint64
	SenderID uint64
}

type RPCEndpoint struct {
	address   string
	ID        uint64
	client    *rpc.Client
	connectch chan bool
	connected bool
}

func (p *RPCEndpoint) connect() {
	go func() {
		for {
			client, err := rpc.DialHTTP("tcp", p.address)
			if err != nil {
				log.Printf("dialing: %v", err)
				time.Sleep(time.Millisecond * 1000)
				continue
			}
			p.client = client
			p.connectch <- true
			return
		}
	}()
}

func (p *RPCEndpoint) callGossip(args *GossipRPCArgs) bool {
	select {
	case <-p.connectch:
		p.connected = true
	default:
		if !p.connected {
			return false
		}
	}

	err := p.client.Call("RPCEndpoint.GossipRPC", args, nil)
	if err == rpc.ErrShutdown {
		// problem implementation
		p.connected = false
		p.connect()
	}
	if err != nil {
		log.Printf("rpc call error: %v", err)
		return false
	}
	return true
}

func (p *RPCEndpoint) GossipRPC(args *GossipRPCArgs, reply *int) error {
	log.Printf("calling gossip, args: %+v", args)
	return nil
}

type Replica struct {
	peers   []*RPCEndpoint
	ID      uint64
	address string
	guide   *Guidance
}

func newAddress(old string, offset uint64) string {
	as := strings.Split(old, ":")
	if len(as) != 2 {
		log.Fatalf("address format is not correct. addr: %v", old)
	}
	port, err := strconv.ParseUint(as[1], 10, 64)
	if err != nil {
		log.Fatalf("address format is not correct. addr: %v. %v", old, err)
	}
	port += offset
	as[1] = strconv.FormatUint(port, 10)
	return strings.Join(as, ":")
}

func (p *Replica) init() {

	// new port = p.address + p.ID
	p.address = newAddress(defaultAddress, p.ID)

	// init guidance
	p.guide = &Guidance{
		Epoch:       p.ID,
		ClusterSize: uint64(clusterSize),
	}
	p.guide.createDefault()

	// init rpc endpoints
	p.peers = make([]*RPCEndpoint, clusterSize)
	for i, _ := range p.peers {
		if uint64(i) == p.ID {
			continue
		}
		log.Printf("i = %v", i)
		p.peers[i] = &RPCEndpoint{}
		p.peers[i].connectch = make(chan bool)
		p.peers[i].address = newAddress(defaultAddress, uint64(i))
		p.peers[i].ID = uint64(i)
	}

	// start rpc server stub
	stub := new(RPCEndpoint)
	rpc.Register(stub)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", p.address)
	if err != nil {
		log.Fatalf("cannot listen on %v, %v", p.address, err)
	}
	go http.Serve(l, nil)

	// connect peers
	for i, _ := range p.peers {
		if uint64(i) == p.ID {
			continue
		}
		p.peers[i].connect()
	}

	log.Printf("replica running")

	go p.gossip()
}

func (p *Replica) gossip() {
	failureCount := make([]uint64, clusterSize)
	for {
		args := &GossipRPCArgs{
			SenderID: p.ID,
			Guide:    *p.guide,
		}
		for i, peer := range p.peers {
			if uint64(i) == p.ID {
				continue
			}
			go func(peer *RPCEndpoint, i int) {
				ok := peer.callGossip(args)
				if !ok {
					atomic.AddUint64(&failureCount[i], 1)
				} else {
					atomic.StoreUint64(&failureCount[i], 0)
				}
			}(peer, i)
		}
		time.Sleep(time.Millisecond * 1000)
		for i, _ := range p.peers {
			if uint64(i) == p.ID {
				continue
			}
			fc := atomic.LoadUint64(&failureCount[i])
			log.Printf("peer %d failure count: %v", i, fc)
			if fc == 3 {
				p.guide.exclude(uint64(i))
			} else if fc == 0 {

			}
		}
	}
}

var r Replica
var clusterSize int
var defaultAddress string

func main() {
	flag.Uint64Var(&r.ID, "id", 0, "my id")
	flag.StringVar(&defaultAddress, "addr", "localhost:22330", "ip:port for peer rpc")
	flag.IntVar(&clusterSize, "num", 3, "number of replicas")

	flag.Parse()

	if len(os.Args) < 2 {
		flag.Usage()
	}

	r.init()

	select {}
}
