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

func main() {
	flag.IntVar(&testCount, "count", 100, "test count")
	flag.IntVar(&clientNum, "cn", 10, "client number")
	flag.IntVar(&keys, "keys", 1, "number of keys")
	flag.Parse()

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
		go func(client *mpclient.MPClient) {
			defer wg.Done()
			for op := range requests {
				// resStr := client.ReadKV(op)
				_, err := client.ReadKV(op)
				if err != nil {
					panic(fmt.Sprintf("read error: %v", err))
				}

				// log.Printf("res: %v", resStr)
			}
		}(clients[i])
	}

	go func() {
		for i := 0; i < testCount; i++ {
			switch i % keys {
			case 0:
				requests <- 0x5489
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
