package internal

import (
	"bufio"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCleanupConnectionResources_DoubleClose_SRV_C2 verifies that calling
// cleanupConnectionResources multiple times does not panic due to double-close
// of the heartbeatStop channel. This is reachable when, e.g., a graceful close
// triggers cleanup and then the read error path also triggers cleanup.
func TestCleanupConnectionResources_DoubleClose_SRV_C2(t *testing.T) {
	IsTerminal.Store(true)

	// Create a minimal server and connection with heartbeat enabled
	s := NewServer()
	s.AddVHost("/")

	// Use a pipe as a fake net.Conn
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	c := &connection{
		conn:              serverConn,
		reader:            bufio.NewReader(serverConn),
		writer:            bufio.NewWriter(serverConn),
		channels:          make(map[uint16]*channel),
		server:            s,
		vhost:             s.vhosts["/"],
		heartbeatTimeout:  make(chan struct{}, 1),
		heartbeatInterval: 60,
	}

	// Simulate heartbeat having been started (channel allocated)
	c.heartbeatStop = make(chan struct{})

	// First call should work fine
	assert.NotPanics(t, func() {
		c.cleanupConnectionResources()
	}, "first call to cleanupConnectionResources should not panic")

	// Second call should also not panic (this is the bug: double-close)
	assert.NotPanics(t, func() {
		c.cleanupConnectionResources()
	}, "second call to cleanupConnectionResources should not panic")
}
