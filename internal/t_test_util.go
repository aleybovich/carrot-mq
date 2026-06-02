package internal

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testRand = rand.New(rand.NewSource(time.Now().UnixNano())) // For unique names

// getNextTestPort asks the OS for a free ephemeral port by binding ":0" and
// immediately releasing it. Replaces the previous monotonic counter scheme,
// which collided when the chosen port was already taken (TIME_WAIT from a
// prior run, another process, etc.). A tiny race remains between the probe
// close and the server's rebind, but ephemeral-range collisions are rare
// enough that this eliminates the flake in practice.
func getNextTestPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Failed to probe a free port")
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close(), "Failed to release probed port %d", port)
	return fmt.Sprintf(":%d", port)
}

// Helper to generate unique names for exchanges, queues, etc.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), testRand.Intn(10000))
}

// Helper to start a server and return its address and a cleanup function
func setupAndReturnTestServer(t *testing.T, opts ...ServerOption) (s *server, addr string, cleanup func()) {
	IsTerminal.Store(true) // Force colorized output for server logs during tests
	addr = getNextTestPort(t)
	s = NewServer(opts...) // Uses default internal logger

	// Channel to signal when server goroutine exits
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		if err := s.Start(addr); err != nil {
			// Only log if it's not the expected closed listener error
			if !errors.Is(err, net.ErrClosed) {
				t.Logf("Test server failed to start on %s: %v", addr, err)
			}
		}
	}()

	// Wait a bit for server to start
	time.Sleep(200 * time.Millisecond)

	cleanup = func() {
		if ln := s.getListener(); ln != nil {
			err := ln.Close()
			if err != nil {
				t.Logf("Error closing test server listener on %s: %v", addr, err)
			}
		}

		// Wait for server goroutine to exit with timeout
		select {
		case <-serverDone:
			// Server exited cleanly
		case <-time.After(1 * time.Second):
			t.Logf("Warning: Server goroutine did not exit within timeout for %s", addr)
		}
	}

	return s, addr, cleanup
}
