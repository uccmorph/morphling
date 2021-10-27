package mpserver_test

import (
	"morphling/mpserver"
	"testing"
)

func TestNewSMR(t *testing.T) {
	server := mpserver.NewSMRServer(3, 0, nil, nil)
	server.HandleNewEntry(1)
}
