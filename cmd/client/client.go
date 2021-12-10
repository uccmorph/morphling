package main

import (
	"flag"
	"fmt"
	"log"
	"morphling/mpclient"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var testCount int
var clientNum int
var keys int
var raftRead bool
var serveraddrs string
var writeMode bool
var unreplicateRead bool

// ./client -count 10 -cn 2 -saddr 'localhost:9990;localhost:9991;localhost:9992'
func main() {
	flag.IntVar(&testCount, "count", 100, "test count")
	flag.IntVar(&clientNum, "cn", 10, "client number")
	flag.IntVar(&keys, "keys", 1, "number of keys")
	flag.BoolVar(&raftRead, "rr", false, "use raft like read")
	flag.StringVar(&serveraddrs, "saddr", "", "server addrs, separated by ;")
	flag.BoolVar(&writeMode, "write", false, "all operation is write")
	flag.BoolVar(&unreplicateRead, "ur", false, "unreplicate read, only one server")
	flag.Parse()

	replicaAddr := strings.Split(serveraddrs, ";")
	log.Printf("raftRead: %v", raftRead)
	log.Printf("servers: %+v", replicaAddr)

	clients := make([]*mpclient.MPClient, clientNum)
	for i := 0; i < clientNum; i++ {
		client := mpclient.NewMPClient(replicaAddr, i)
		client.Connet()
		client.GetGuidance()
		clients[i] = client
	}
	handleSignal(clients)
	log.Printf("%v clients connection complete, will send %v total request", clientNum, testCount)

	start := time.Now()
	requests := make(chan uint64, clientNum)
	wg := sync.WaitGroup{}
	value := randomString(1000)
	for i := 0; i < clientNum; i++ {
		wg.Add(1)
		go func(client *mpclient.MPClient, i int) {
			defer wg.Done()
			for k := range requests {
				// log.Printf("client %v 's turn", i)
				var err error
				if writeMode {
					err = client.WriteKV(k, string(value))
				} else {
					if raftRead {
						_, err = client.RaftReadKV(k)
					} else if unreplicateRead {
						_, err = client.UnreplicateReadKV(k)
					} else {
						_, err = client.ReadKVFast(k)
					}
				}
				if err != nil {
					panic(fmt.Sprintf("operation error: %v", err))
				}
				// log.Printf("res len: %v", len(v))
				// log.Printf("res: %v", v)
			}
		}(clients[i], i)
	}

	go func() {
		for i := 0; i < testCount; i++ {
			switch i % keys {
			case 0:
				requests <- 0x5480
			case 1:
				requests <- 0x6090
			case 2:
				requests <- 0xd290
			}
		}
		close(requests)
	}()

	wg.Wait()

	dur := time.Since(start)
	log.Printf("total %v s, ops: %v", dur.Seconds(), float64(testCount)/dur.Seconds())

	time.Sleep(time.Second * 1)
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
