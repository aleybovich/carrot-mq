package internal

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

var testRand = rand.New(rand.NewSource(time.Now().UnixNano())) // For unique names
// testServerPortCounter is a global counter to assign unique ports to test servers.
var testServerPortCounter = 5800 // Starting port number, different from previous example
var portCounterMutex sync.Mutex

func getNextTestPort() string {
	portCounterMutex.Lock()
	defer portCounterMutex.Unlock()
	port := testServerPortCounter
	testServerPortCounter++
	return fmt.Sprintf(":%d", port)
}

// Helper to generate unique names for exchanges, queues, etc.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), testRand.Intn(10000))
}

// Helper to start a server and return its address and a cleanup function
func setupAndReturnTestServer(t *testing.T, opts ...ServerOption) (s *server, addr string, cleanup func()) {
	IsTerminal = true // Force colorized output for server logs during tests
	addr = getNextTestPort()
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
		if s.listener != nil {
			err := s.listener.Close()
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
