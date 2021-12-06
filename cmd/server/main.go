package main

import (
	"flag"
	"log"
	"morphling/mpserverv2"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var clientPort string = "9990"
var peerPort string = "4567"
var replicaID int

var replicaAddr []string
var randS []byte
var randSLen int = 1000

func init() {
	replicaAddr = []string{
		"127.0.0.1:4567",
		"127.0.0.1:4568",
		"127.0.0.1:4569",
	}
	randS = make([]byte, randSLen)
	for i := range randS {
		randS[i] = 'a'
	}
}

func randomString(length int) []byte {
	str := make([]byte, length)
	copy(str, randS)

	return str
}

func main() {
	flag.IntVar(&replicaID, "id", -1, "replica unique id")
	flag.Parse()

	if replicaID == -1 {
		flag.Usage()
		os.Exit(1)
	}
	p, _ := strconv.ParseInt(clientPort, 10, 64)
	p += int64(replicaID)
	clientPort = strconv.FormatInt(p, 10)
	p, _ = strconv.ParseInt(peerPort, 10, 64)
	p += int64(replicaID)
	peerPort = strconv.FormatInt(p, 10)
	log.Printf("clientPort: %v, peerPort: %v", clientPort, peerPort)

	defaultGuidance := &mpserverv2.Guidance{}
	defaultGuidance.InitDefault(3)
	storage := mpserverv2.NewMemStorage()

	for i := 0; i < 0xffff; i += 8 {
		mod := []mpserverv2.Modify{
			{
				Data: mpserverv2.Put{
					Key:   []byte(strconv.FormatUint(uint64(i), 10)),
					Value: randomString(randSLen),
					Cf:    mpserverv2.CfDefault,
				},
			},
		}
		storage.Write(mod)
	}

	serverEndpoint := &mpserverv2.RPCEndpoint{}
	clientEndpoint := &mpserverv2.RPCEndpoint{}
	rpc.Register(serverEndpoint)
	rpc.Register(clientEndpoint)
	rpc.HandleHTTP()

	go func() {
		clientService := ":" + clientPort
		l, err := net.Listen("tcp", clientService)
		if err != nil {
			log.Fatalf("cannot listen on %v, %v", clientService, err)
		}
		err = http.Serve(l, nil)
		if err != nil {
			log.Printf("start http failed: %v", err)
		}
	}()

	go func() {
		peerService := ":" + peerPort
		pl, err := net.Listen("tcp", peerService)
		if err != nil {
			log.Fatalf("cannot listen on %v, %v", peerService, err)
		}
		err = http.Serve(pl, nil)
		if err != nil {
			log.Printf("start http failed: %v", err)
		}
	}()

	peersStub := ConnectPeers(replicaAddr, replicaID)

	msgCh := make(chan *mpserverv2.HandlerInfo)

	config := mpserverv2.Config{
		Guide:    defaultGuidance,
		Store:    storage,
		Peers:    peersStub,
		Ch:       msgCh,
		Me:       replicaID,
		RaftLike: true,
	}

	replica := mpserverv2.CreateReplica(&config)

	serverEndpoint.MsgChan = msgCh
	serverEndpoint.Replica = replica

	clientEndpoint.MsgChan = serverEndpoint.MsgChan
	clientEndpoint.Replica = serverEndpoint.Replica

	select {}
}

func ConnectPeers(peersAddr []string, me int) map[int]*rpc.Client {
	res := make(map[int]*rpc.Client)
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	for id, addr := range peersAddr {
		if id == me {
			continue
		}
		wg.Add(1)
		go func(id int, addr string) {
			defer wg.Done()
			stub := Connect(addr)
			mu.Lock()
			res[id] = stub
			mu.Unlock()
		}(id, addr)
	}
	wg.Wait()

	log.Printf("connect all peers")
	return res
}

func Connect(addr string) *rpc.Client {
	for {
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Printf("dialing: %v", err)
			time.Sleep(time.Millisecond * 1000)
			continue
		}
		return client
	}
}
