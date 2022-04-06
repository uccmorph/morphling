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
	"strings"
	"sync"
	"time"
)

var clientPort string = "9990"
var guidancePort string = "9996"
var replicaID int
var serveraddrs string
var curp bool

var randS []byte
var randSLen int = 1000

func init() {
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

// ./server -id 0 -saddr 'localhost:4567,localhost:4568,localhost:4569' -cport 9990
func main() {
	flag.IntVar(&replicaID, "id", -1, "replica unique id")
	flag.StringVar(&serveraddrs, "saddr", "", "server addrs, separated by ;")
	flag.StringVar(&clientPort, "cport", "9990", "client port for operational requests")
	flag.StringVar(&guidancePort, "gport", "9996", "client port for GetGuidance")
	flag.BoolVar(&curp, "curp", false, "enable curp replica")
	flag.Parse()

	if replicaID == -1 || len(serveraddrs) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	replicaAddr := strings.Split(serveraddrs, ",")
	log.Printf("clientPort: %v, guidancePort: %v", clientPort, guidancePort)
	log.Printf("all server: %+v", replicaAddr)

	defaultGuidance := &mpserverv2.Guidance{}
	defaultGuidance.InitDefault(3)

	// init storage, mask 0xfffff000
	storage := mpserverv2.NewMemStorage()
	kvCount := 0
	for i := 0; i < 0xffffffff; i += 0x1000 {
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
		kvCount += 1
	}
	log.Printf("total %d kv pairs", kvCount)

	serverEndpoint := &mpserverv2.RPCEndpoint{}
	clientEndpoint := &mpserverv2.RPCEndpoint{}
	rpc.Register(serverEndpoint)
	rpc.Register(clientEndpoint)

	startSrv := func(addr string, title string) {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("cannot listen on %v, %v", addr, err)
		}
		log.Printf("start %s service at: %s", title, addr)
		err = http.Serve(l, nil)
		if err != nil {
			log.Printf("start http failed: %v", err)
		}
	}

	if curp {
		log.Printf("start curp replica")
		rpc.HandleHTTP()
		go startSrv(":"+clientPort, "client op")
		go startSrv(replicaAddr[replicaID], "peer")
		peersStub := ConnectPeers(replicaAddr, replicaID)

		msgCh := make(chan *mpserverv2.HandlerInfo)
		serverEndpoint.MsgChan = msgCh
		clientEndpoint.MsgChan = msgCh

		config := mpserverv2.Config{
			Guide:    defaultGuidance,
			Store:    storage,
			Peers:    peersStub,
			Ch:       msgCh,
			Me:       replicaID,
			RaftLike: true,
		}

		mpserverv2.CreateCURPReplica(config)
	} else {
		log.Printf("start normal replica")
		guidanceEndpoint := &mpserverv2.GuidanceEndpoint{}
		rpc.Register(guidanceEndpoint)
		rpc.HandleHTTP()

		go startSrv(":"+clientPort, "client op")
		go startSrv(":"+guidancePort, "guidance")
		go startSrv(replicaAddr[replicaID], "peer")
		peersStub := ConnectPeers(replicaAddr, replicaID)

		msgCh := make(chan *mpserverv2.HandlerInfo)
		guidanceCh := make(chan *mpserverv2.HandlerInfo)

		config := mpserverv2.Config{
			Guide:      defaultGuidance,
			Store:      storage,
			Peers:      peersStub,
			Ch:         msgCh,
			GuidanceCh: guidanceCh,
			Me:         replicaID,
			RaftLike:   true,
		}

		mpserverv2.CreateReplica(&config)

		serverEndpoint.MsgChan = msgCh
		clientEndpoint.MsgChan = msgCh
		guidanceEndpoint.MsgChan = guidanceCh
	}

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
