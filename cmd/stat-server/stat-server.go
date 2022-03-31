package main

import (
	"fmt"
	"log"
	"morphling/mpserverv2"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

var cfgClientMachineNum int = 2
var cfgClientControlAddr [2]string = [2]string{"10.1.6.236:10111", "10.1.6.232:10111"}

var clientCtrlStubs []*rpc.Client
var finishOneTest chan bool

type statistics struct {
	throughput float64
	latency    float64
}

var gstat []statistics

type Server struct {
	mtx     sync.Mutex
	clients map[int]*mpserverv2.StatisticsArgs
	allocID int
}

func newServer() *Server {
	p := &Server{}

	p.clients = make(map[int]*mpserverv2.StatisticsArgs, cfgClientMachineNum)
	p.allocID = 0
	p.mtx = sync.Mutex{}

	return p
}

func (p *Server) Report(args *mpserverv2.StatisticsArgs, reply *mpserverv2.StatisticsReply) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.clients[args.MachineID] = args
	if len(p.clients) == cfgClientMachineNum {
		latency := 0.0
		throughput := 0.0
		clientNums := 0
		for _, stat := range p.clients {
			latency += float64(stat.AvgLatencyNs)
			throughput += stat.ThroughputKops
			clientNums += stat.VClientNums
		}

		for k := range p.clients {
			delete(p.clients, k)
		}
		latency = latency / 1e3 / float64(cfgClientMachineNum)
		log.Printf("clients %d, throughput ops, avg latency us: (%.2f %.2f)", clientNums, throughput, latency)
		gstat = append(gstat, statistics{
			throughput: throughput,
			latency:    latency,
		})
		select {
		case finishOneTest <- true:
		default:
		}
	}
	return nil
}

func (p *Server) ClientRegister(args *mpserverv2.ClientRegisterArgs, reply *mpserverv2.ClientRegisterReply) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	reply.MachineID = p.allocID
	p.allocID += 1

	if p.allocID == cfgClientMachineNum {
		p.allocID = 0
	}

	return nil
}

func ConnectClientControl() {
	for i := 0; i < cfgClientMachineNum; i++ {
		for {
			stub, err := rpc.DialHTTP("tcp", cfgClientControlAddr[i])
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			clientCtrlStubs = append(clientCtrlStubs, stub)
			log.Printf("connect to client %v control", cfgClientControlAddr[i])
			break
		}
	}
	log.Printf("finish ConnectClientControl")
}

func main() {
	server := newServer()
	rpc.Register(server)

	rpc.HandleHTTP()
	finishOneTest = make(chan bool)
	gstat = make([]statistics, 0)

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

	go startSrv(":10110", "statistics")

	ConnectClientControl()
	countBase := 10000
	cnums := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 18, 20, 25, 30, 40, 50, 60}
	// cnums := []int{1, 2}
	for _, cn := range cnums {
		runTest(cn*countBase, cn)
		time.Sleep(time.Millisecond * 100)
	}

	for i := 0; i < cfgClientMachineNum; i++ {
		args := &mpserverv2.StartTestArgs{
			Finish: true,
		}
		reply := &mpserverv2.StartTestReply{}
		clientCtrlStubs[i].Call("StatControl.Start", args, reply)
	}

	for _, stat := range gstat {
		fmt.Printf("%.2f %.2f\n", stat.throughput, stat.latency)
	}
}

func runTest(counts, cnums int) {
	for i := 0; i < cfgClientMachineNum; i++ {
		go func(i int) {
			if cnums >= 20 {
				counts = 100000
			}
			args := &mpserverv2.StartTestArgs{
				TestCount:  counts,
				ClientNums: cnums,
			}
			reply := &mpserverv2.StartTestReply{}
			log.Printf("ctrl client %d, with %+v", i, args)
			clientCtrlStubs[i].Call("StatControl.Start", args, reply)
		}(i)
	}

	<-finishOneTest
}
