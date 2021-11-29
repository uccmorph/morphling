package main

import (
	"morphling/mpclient"
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

func main() {
	client := mpclient.NewClientEntPoint(replicaAddr)
	client.Connet()

	client.GetGuidance()

	time.Sleep(time.Second * 10)
}
