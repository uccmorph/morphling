package main

import (
	"flag"
	"fmt"
	"log"
	"morphling/mpclient"
	"sync"
	"time"
)

var replicaAddr []string

func init() {
	replicaAddr = []string{
		"127.0.0.1:9990",
		"127.0.0.1:9991",
		"127.0.0.1:9992",
	}
}

var testCount int
var clientNum int
var keys int
var raftRead bool

func main() {
	flag.IntVar(&testCount, "count", 100, "test count")
	flag.IntVar(&clientNum, "cn", 10, "client number")
	flag.IntVar(&keys, "keys", 1, "number of keys")
	flag.BoolVar(&raftRead, "rr", false, "use raft like read")
	flag.Parse()

	log.Printf("raftRead: %v", raftRead)

	clients := make([]*mpclient.MPClient, clientNum)
	for i := 0; i < clientNum; i++ {
		client := mpclient.NewMPClient(replicaAddr, i)
		client.Connet()
		client.GetGuidance()
		clients[i] = client
	}

	start := time.Now()
	requests := make(chan uint64, clientNum)
	wg := sync.WaitGroup{}
	for i := 0; i < clientNum; i++ {
		wg.Add(1)
		go func(client *mpclient.MPClient, i int) {
			defer wg.Done()
			for k := range requests {
				// log.Printf("client %v 's turn", i)
				var err error
				if raftRead {
					_, err = client.RaftReadKV(k)
				} else {
					_, err = client.ReadKV(k)
				}
				if err != nil {
					panic(fmt.Sprintf("read error: %v", err))
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

	time.Sleep(time.Second * 3)
}
