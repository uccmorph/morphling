package main

import (
	"flag"
	"log"
	"morphling/mpserverv2"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)

var clientPort string

func randomString(length int) []byte {
	str := make([]byte, length)
	for i := range str {
		str[i] = 'a'
	}

	return str
}

func main() {
	flag.StringVar(&clientPort, "cport", "9990", "client connect to this port")
	flag.Parse()

	defaultGuidance := &mpserverv2.Guidance{}
	defaultGuidance.InitDefault(3)
	storage := mpserverv2.NewMemStorage()
	storage.Write([]mpserverv2.Modify{
		{
			Data: mpserverv2.Put{
				Key:   []byte(strconv.FormatUint(0x5489, 10)),
				Value: randomString(20000),
				Cf:    mpserverv2.CfDefault,
			},
		},
		{
			Data: mpserverv2.Put{
				Key:   []byte(strconv.FormatUint(0x6090, 10)),
				Value: randomString(20000),
				Cf:    mpserverv2.CfDefault,
			},
		},
		{
			Data: mpserverv2.Put{
				Key:   []byte(strconv.FormatUint(0xd290, 10)),
				Value: randomString(20000),
				Cf:    mpserverv2.CfDefault,
			},
		},
	})

	serverEndpoint := &mpserverv2.RPCEndpoint{
		MsgChan: make(chan *mpserverv2.HandlerInfo),
		Replica: mpserverv2.CreateReplica(defaultGuidance, storage),
	}
	serverEndpoint.Init()
	clientEndpoint := &mpserverv2.RPCEndpoint{}
	clientEndpoint.MsgChan = serverEndpoint.MsgChan
	clientEndpoint.Replica = serverEndpoint.Replica
	rpc.Register(serverEndpoint)
	rpc.Register(clientEndpoint)
	rpc.HandleHTTP()

	clientService := ":" + clientPort
	l, err := net.Listen("tcp", clientService)
	if err != nil {
		log.Fatalf("cannot listen on %v, %v", clientService, err)
	}
	peerService := ":4567"
	pl, err := net.Listen("tcp", peerService)
	if err != nil {
		log.Fatalf("cannot listen on %v, %v", clientService, err)
	}
	go http.Serve(l, nil)
	http.Serve(pl, nil)

}
