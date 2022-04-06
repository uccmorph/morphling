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
var cfgGuidancePorts string
var cfgWriteRatio int
var cfgControlMode bool
var cfgSingleKey bool
var cfgReplicaMode string
var cfgSkewness float64
var cfgStatServerAddr string = "localhost:10110"

const cfgAccessItems int = 0x1000
const cfgAccessKeyShift int = 20

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
	cfgClientNum = args.ClientNums
	cfgTestCount = cfgClientNum * 10000
	if cfgTestCount > 200000 {
		cfgTestCount = 200000
	}

	if args.Finish {
		close(newTest)
	} else {
		newTest <- true
	}
	return nil
}

var randomValue [][]byte

// ./client -count 10 -cn 1 -saddr 'localhost:9990,localhost:9991,localhost:9992' -gport '9996,9997,9998'
func main() {
	flag.IntVar(&cfgTestCount, "count", 100, "test count for non-control or zipfian")
	flag.IntVar(&cfgClientNum, "cn", 10, "client number")
	flag.StringVar(&cfgServeraddrs, "saddr", "", "server addrs, separated by ,")
	flag.StringVar(&cfgGuidancePorts, "gport", "", "server addrs, separated by ,")
	flag.IntVar(&cfgWriteRatio, "write", 0, "writes per 100 requests")
	flag.StringVar(&cfgReplicaMode, "rmode", "m", "mode: m for morphling, u for unreplicated, r for raft, c for curp")
	flag.BoolVar(&cfgControlMode, "ctrl", false, "waiting stat-server control")
	flag.BoolVar(&cfgSingleKey, "sk", false, "request only a single key")
	flag.Float64Var(&cfgSkewness, "skewness", 0.0, "thera of zipf distribution")
	flag.Parse()

	replicaAddr := strings.Split(cfgServeraddrs, ",")
	guidancePorts := strings.Split(cfgGuidancePorts, ",")
	log.Printf("cfgReplicaMode: %v, cfgWriteRatio: %v, cfgControlMode: %v", cfgReplicaMode, cfgWriteRatio, cfgControlMode)
	log.Printf("servers: %+v", replicaAddr)
	log.Printf("zipf skewness: %+v", cfgSkewness)

	initRandomValue()
	if cfgSkewness > 0 {
		initZipf()
	}
	// go func() {
	// 	log.Println(http.ListenAndServe(":6060", nil))
	// }()

	if cfgControlMode {
		runControlMode(replicaAddr, guidancePorts)
	} else {
		clients := make([]*mpclient.MPClient, cfgClientNum)
		for i := 0; i < cfgClientNum; i++ {
			client := mpclient.NewMPClient(replicaAddr, i)
			client.SetGuidancePorts(guidancePorts)
			client.SetCURPMode(strings.HasPrefix(cfgReplicaMode, "c"))
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

func runControlMode(replicaAddr, guidancePorts []string) {

	control := &StatControl{}
	rpc.Register(control)
	rpc.HandleHTTP()

	// hard code to max 100 clients
	clients := make([]*mpclient.MPClient, 100)
	for i := 0; i < 100; i++ {
		client := mpclient.NewMPClient(replicaAddr, i)
		client.SetGuidancePorts(guidancePorts)
		client.SetCURPMode(strings.HasPrefix(cfgReplicaMode, "c"))
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
		stub, err := rpc.DialHTTP("tcp", cfgStatServerAddr)
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
				log.Printf("client %v 's turn, key: %x, isWrite: %v", i, req.key, req.isWrite)
				var err error
				// var v string
				reqstart := time.Now()
				if req.isWrite {
					value := randomString()
					if strings.HasPrefix(cfgReplicaMode, "r") {
						err = client.RaftWriteKV(req.key, string(value))
					} else if strings.HasPrefix(cfgReplicaMode, "u") {
						err = client.UnreplicatedWriteKV(req.key, string(value))
					} else {
						err = client.WriteKV(req.key, string(value))
					}
				} else {
					if strings.HasPrefix(cfgReplicaMode, "r") {
						_, err = client.RaftReadKV(req.key)
					} else if strings.HasPrefix(cfgReplicaMode, "u") {
						_, err = client.UnreplicateReadKV(req.key)
					} else if strings.HasPrefix(cfgReplicaMode, "c") {
						_, err = client.CURPReadKV(req.key)
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
			} else if cfgSkewness > 0 {
				key := zipfSeq[i%zipfBase]
				requests <- requestCase{
					key:     uint64(key) << uint64(cfgAccessKeyShift),
					id:      i,
					isWrite: isWrite,
				}
			} else {
				rNum := randGen.Intn(cfgAccessItems)
				requests <- requestCase{
					key:     uint64(rNum) << uint64(cfgAccessKeyShift),
					id:      i,
					isWrite: isWrite,
				}
			}
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
	log.Printf("total time %v, average latency: %v ns", latencyTotal, latencyTotal/int64(cfgTestCount))

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

var zipfSeq []int
var zipfBase int = 10000

func initZipf() {
	zipfGen := NewZipfianWithItems(int64(cfgAccessItems), cfgSkewness)
	zipfSeq = make([]int, zipfBase)
	ran := rand.New(rand.NewSource(0))

	ranAsso := make(map[int]int) // mapping from 0~cfgItems to random keys
	occRan := rand.New(rand.NewSource(time.Now().UnixNano()))
	newIdx := 0
	for {
		tryIdx := occRan.Intn(cfgAccessItems)
		if len(ranAsso) == cfgAccessItems {
			break
		}
		if _, ok := ranAsso[tryIdx]; ok {
			continue
		}
		ranAsso[tryIdx] = newIdx
		newIdx += 1
	}

	for i := 0; i < zipfBase; i++ {
		num := zipfGen.Next(ran)
		zipfSeq[i] = ranAsso[int(num)]
	}
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
