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

var cfgTestCount int
var cfgClientNum int
var cfgServeraddrs string
var cfgWriteRatio int
var cfgControlMode bool
var cfgSingleKey bool
var cfgReadMode string
var cfgSkewness float64

type requestCase struct {
	key     uint64
	id      int
	isWrite bool
}

var newTest chan bool
var statServer *rpc.Client
var machineID int

type StatControl struct {
}

func (p *StatControl) Start(args *mpserverv2.StartTestArgs, reply *mpserverv2.StartTestReply) error {
	cfgTestCount = args.TestCount
	cfgClientNum = args.ClientNums

	if args.Finish {
		close(newTest)
	} else {
		newTest <- true
	}
	return nil
}

var randomValue [][]byte
var zipf *Zipfian

// ./client -count 10 -cn 2 -saddr 'localhost:9990;localhost:9991;localhost:9992'
func main() {
	flag.IntVar(&cfgTestCount, "count", 100, "test count")
	flag.IntVar(&cfgClientNum, "cn", 10, "client number")
	flag.StringVar(&cfgServeraddrs, "saddr", "", "server addrs, separated by ;")
	flag.IntVar(&cfgWriteRatio, "write", 0, "writes per 100 requests")
	flag.StringVar(&cfgReadMode, "rmode", "m", "mode: m for morphling, u for unreplicated, r for raft, c for curp")
	flag.BoolVar(&cfgControlMode, "ctrl", false, "waiting stat-server control")
	flag.BoolVar(&cfgSingleKey, "sk", false, "request only a single key")
	flag.Float64Var(&cfgSkewness, "skewness", 0.0, "thera of zipf distribution")
	flag.Parse()

	replicaAddr := strings.Split(cfgServeraddrs, ",")
	log.Printf("cfgReadMode: %v, cfgWriteRatio: %v, cfgControlMode: %v", cfgReadMode, cfgWriteRatio, cfgControlMode)
	log.Printf("servers: %+v", replicaAddr)

	initRandomValue()
	if cfgSkewness > 0 {
		zipf = NewZipfianWithItems(0x1000, cfgSkewness)
	}
	// go func() {
	// 	log.Println(http.ListenAndServe(":6060", nil))
	// }()

	if cfgControlMode {
		runControlMode(replicaAddr)
	} else {
		clients := make([]*mpclient.MPClient, cfgClientNum)
		for i := 0; i < cfgClientNum; i++ {
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
	log.Printf("%v clients connection complete, will send %v total request", cfgClientNum, cfgTestCount)

	start := time.Now()
	requests := make(chan requestCase, cfgClientNum)
	wg := sync.WaitGroup{}

	latencyMonitor := make([]int64, cfgTestCount)
	for i := 0; i < cfgClientNum; i++ {
		wg.Add(1)
		go func(client *mpclient.MPClient, i int) {
			defer wg.Done()
			for req := range requests {
				// log.Printf("client %v 's turn, key: %x, isWrite: %v", i, req.key, req.isWrite)
				var err error
				// var v string
				reqstart := time.Now()
				if req.isWrite {
					value := randomString()
					if strings.HasPrefix(cfgReadMode, "r") {
						err = client.RaftWriteKV(req.key, string(value))
					} else if strings.HasPrefix(cfgReadMode, "u") {
						err = client.UnreplicatedWriteKV(req.key, string(value))
					} else {
						err = client.WriteKV(req.key, string(value))
					}
				} else {
					if strings.HasPrefix(cfgReadMode, "r") {
						_, err = client.RaftReadKV(req.key)
					} else if strings.HasPrefix(cfgReadMode, "u") {
						_, err = client.UnreplicateReadKV(req.key)
					} else {
						_, err = client.ReadKVFast(req.key)
					}
				}
				latencyMonitor[req.id] = int64(time.Since(reqstart))

				if err != nil {
					panic(fmt.Sprintf("operation error: %v", err))
				}

				// log.Printf("key 0x%08x, res: %v", req.key, v)
			}
		}(clients[i], i)
	}

	// generate request keys
	go func() {
		randGen := rand.New(rand.NewSource(0))
		writeGen := rand.New(rand.NewSource(1))
		for i := 0; i < cfgTestCount; i++ {
			isWrite := (100-writeGen.Intn(100) <= cfgWriteRatio)
			if cfgSingleKey {
				requests <- requestCase{
					key:     0x140000,
					id:      i,
					isWrite: isWrite,
				}
			} else {
				rNum := randGen.Int31n(0x1000)
				requests <- requestCase{
					key:     uint64(rNum) << 20,
					id:      i,
					isWrite: isWrite,
				}
			}

			// requests <- requestCase{0x14000000, i}
			// switch i % 3 {
			// case 0:
			// 	requests <- requestCase{0x14000000, i}
			// case 1:
			// 	requests <- requestCase{0x88000000, i}
			// case 2:
			// 	requests <- requestCase{0xb2000000, i}
			// }
		}
		close(requests)
	}()

	wg.Wait()

	dur := time.Since(start)
	log.Printf("total %v s, ops: %v", dur.Seconds(), float64(cfgTestCount)/dur.Seconds())

	var latencyTotal int64
	for i := 0; i < cfgTestCount; i++ {
		latencyTotal += latencyMonitor[i]
	}
	log.Printf("total latency %v, average latency: %v ns", latencyTotal, latencyTotal/int64(cfgTestCount))

	if cfgControlMode {
		reportArgs := &mpserverv2.StatisticsArgs{
			AvgLatencyNs:   int(latencyTotal) / cfgTestCount,
			ThroughputKops: float64(cfgTestCount) / dur.Seconds(),
			VClientNums:    cfgClientNum,
			MachineID:      machineID,
		}
		reportReply := &mpserverv2.StatisticsReply{}
		statServer.Call("Server.Report", reportArgs, reportReply)
	}
}

func initRandomValue() {
	randomValue = make([][]byte, 10)
	for i := range randomValue {
		randS := make([]byte, 1000)
		for k := range randS {
			randS[k] = 'b' + byte(i)
		}
		randomValue[i] = randS
	}
}

var getCount int = 0
var rsMtx sync.Mutex

func randomString() []byte {
	rsMtx.Lock()
	defer rsMtx.Unlock()
	getCount += 1
	if getCount == 10 {
		getCount = 0
	}

	return randomValue[getCount]
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
