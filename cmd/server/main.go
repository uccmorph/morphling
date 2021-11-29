package main

import (
	"flag"
	"log"
	"morphling/mpserverv2"
	"net"
	"net/http"
	"net/rpc"
)

var clientPort string

func main() {
	flag.StringVar(&clientPort, "cport", "9990", "client connect to this port")
	flag.Parse()

	rpc.Register(&mpserverv2.RPCEndpoint{})
	rpc.HandleHTTP()

	clientService := ":" + clientPort
	l, err := net.Listen("tcp", clientService)
	if err != nil {
		log.Fatalf("cannot listen on %v, %v", clientService, err)
	}
	http.Serve(l, nil)

}
