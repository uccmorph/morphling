package main

import "morphling/mpserver"

func main() {
	r := mpserver.CreateReplica()
	r.SendStatus()
	r.WaitClient()
}
