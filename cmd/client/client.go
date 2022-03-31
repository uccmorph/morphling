package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"morphling/mpclient"
	"morphling/mpserverv2"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	// _ "net/http/pprof"
)

var testCount int
var clientNum int
var serveraddrs string
var writeMode bool
var controlMode bool

// var raftRead bool
// var unreplicateRead bool
var readMode string

type requestCase struct {
	key uint64
	id  int
}

var newTest chan bool
var statServer *rpc.Client
var machineID int

type StatControl struct {
}

func (p *StatControl) Start(args *mpserverv2.StartTestArgs, reply *mpserverv2.StartTestReply) error {
	testCount = args.TestCount
	clientNum = args.ClientNums

	if args.Finish {
		close(newTest)
	} else {
		newTest <- true
	}
	return nil
}

// ./client -count 10 -cn 2 -saddr 'localhost:9990;localhost:9991;localhost:9992'
func main() {
	flag.IntVar(&testCount, "count", 100, "test count")
	flag.IntVar(&clientNum, "cn", 10, "client number")
	flag.StringVar(&serveraddrs, "saddr", "", "server addrs, separated by ;")
	flag.BoolVar(&writeMode, "write", false, "all operation is write")
	flag.StringVar(&readMode, "rmode", "m", "mode: m for morphling, u for unreplicated, r for raft, c for curp")
	flag.BoolVar(&controlMode, "ctrl", false, "waiting stat-server control")
	flag.Parse()

	replicaAddr := strings.Split(serveraddrs, ",")
	log.Printf("readMode: %v, writeMode: %v, controlMode: %v", readMode, writeMode, controlMode)
	log.Printf("servers: %+v", replicaAddr)

	// go func() {
	// 	log.Println(http.ListenAndServe(":6060", nil))
	// }()

	if controlMode {
		runControlMode(replicaAddr)
	} else {
		clients := make([]*mpclient.MPClient, clientNum)
		for i := 0; i < clientNum; i++ {
			client := mpclient.NewMPClient(replicaAddr, i)
			client.Connet()
			client.GetGuidance()
			clients[i] = client
		}
		oneTest(clients)

		for i := range clients {
			clients[i].Disconnect()
		}
	}

}

func runControlMode(replicaAddr []string) {

	control := &StatControl{}
	rpc.Register(control)
	rpc.HandleHTTP()

	// hard code to max 100 clients
	clients := make([]*mpclient.MPClient, 100)
	for i := 0; i < 100; i++ {
		client := mpclient.NewMPClient(replicaAddr, i)
		client.Connet()
		client.GetGuidance()
		clients[i] = client
	}

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
	go startSrv(":10111", "statistics")

	newTest = make(chan bool)
	go func() {
		stub, err := rpc.DialHTTP("tcp", "10.1.6.236:10110")
		if err != nil {
			log.Fatalf("connect stat server error: %v", err)
		}
		args := &mpserverv2.ClientRegisterArgs{}
		reply := &mpserverv2.ClientRegisterReply{}
		stub.Call("Server.ClientRegister", args, reply)
		machineID = reply.MachineID
		statServer = stub
	}()

	for range newTest {
		oneTest(clients)
	}

	for i := range clients {
		clients[i].Disconnect()
	}
}

func oneTest(clients []*mpclient.MPClient) {

	// handleSignal(clients)
	log.Printf("%v clients connection complete, will send %v total request", clientNum, testCount)

	start := time.Now()
	requests := make(chan requestCase, clientNum)
	wg := sync.WaitGroup{}
	value := randomString(1000)
	latencyMonitor := make([]int64, testCount)
	for i := 0; i < clientNum; i++ {
		wg.Add(1)
		go func(client *mpclient.MPClient, i int) {
			defer wg.Done()
			for req := range requests {
				// log.Printf("client %v 's turn, key: %x", i, req.key)
				var err error
				reqstart := time.Now()
				if writeMode {
					err = client.WriteKV(req.key, string(value))
				} else {
					if strings.HasPrefix(readMode, "r") {
						_, err = client.RaftReadKV(req.key)
					} else if strings.HasPrefix(readMode, "u") {
						_, err = client.UnreplicateReadKV(req.key)
					} else {
						_, err = client.ReadKVFast(req.key)
					}
				}
				latencyMonitor[req.id] = int64(time.Since(reqstart))
				// log.Printf("req %d latency: %v", req.id, latencyMonitor[req.id])
				if err != nil {
					panic(fmt.Sprintf("operation error: %v", err))
				}
				// log.Printf("res len: %v", len(v))
				// log.Printf("res: %v", v)
			}
		}(clients[i], i)
	}

	// generate request keys
	go func() {
		for i := 0; i < testCount; i++ {
			rNum := rand.Int31n(0xfffff)
			// log.Printf("random key: 0x%08x", rNum)
			requests <- requestCase{uint64(rNum) << 12, i}
		}
		close(requests)
	}()

	wg.Wait()

	dur := time.Since(start)
	log.Printf("total %v s, ops: %v", dur.Seconds(), float64(testCount)/dur.Seconds())

	var latencyTotal int64
	for i := 0; i < testCount; i++ {
		latencyTotal += latencyMonitor[i]
	}
	log.Printf("total latency %v, average latency: %v ns", latencyTotal, latencyTotal/int64(testCount))

	if controlMode {
		reportArgs := &mpserverv2.StatisticsArgs{
			AvgLatencyNs:   int(latencyTotal) / testCount,
			ThroughputKops: float64(testCount) / dur.Seconds(),
			VClientNums:    clientNum,
			MachineID:      machineID,
		}
		reportReply := &mpserverv2.StatisticsReply{}
		statServer.Call("Server.Report", reportArgs, reportReply)
	}
}

func randomString(length int) []byte {
	randS := make([]byte, length)
	for i := range randS {
		randS[i] = 'a'
	}
	return randS
}

func handleSignal(clients []*mpclient.MPClient) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sigCh
		for i := range clients {
			clients[i].Disconnect()
		}
	}()
}
