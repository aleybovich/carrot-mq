// Package testutil holds helpers shared by carrot-mq's test suites in both the
// root package and the internal package. It lives under internal/ so it is not
// importable by external consumers.
package testutil

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// GetNextTestPort asks the OS for a free ephemeral port by binding ":0" and
// immediately releasing it. Replaces a previous monotonic counter scheme that
// collided when the chosen port was held by another process or in TIME_WAIT.
// A tiny race remains between the probe close and the server's rebind, but
// ephemeral-range collisions are rare enough that this eliminates the flake
// in practice.
func GetNextTestPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Failed to probe a free port")
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close(), "Failed to release probed port %d", port)
	return fmt.Sprintf(":%d", port)
}
