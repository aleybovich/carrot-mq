package internal

import (
	"encoding/binary"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestFrameReaderGoroutineLeak_SRV_C1 verifies that the frame-reader goroutine
// does not leak when the main connection loop exits while the reader is blocked
// on sending to frameChan/frameErrChan.
//
// The scenario: the server reads a valid frame and tries to send it on the
// unbuffered frameChan, but the main loop has already exited (e.g., due to a
// graceful close from a prior frame). The reader goroutine blocks forever.
func TestFrameReaderGoroutineLeak_SRV_C1(t *testing.T) {
	s, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	// Record baseline goroutine count
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baselineGoroutines := runtime.NumGoroutine()

	const numConns = 5
	conns := make([]net.Conn, numConns)

	for i := range numConns {
		conn, err := net.Dial("tcp", addr)
		if !assert.NoError(t, err, "dial %d", i) {
			return
		}
		conns[i] = conn

		// Send AMQP protocol header
		_, err = conn.Write([]byte("AMQP\x00\x00\x09\x01"))
		assert.NoError(t, err)

		// Wait for Connection.Start from server
		time.Sleep(100 * time.Millisecond)

		// Now send a valid Connection.Close method frame (class=10, method=50)
		// to trigger the graceful close path in handleConnection.
		// The server main loop will return after processing this, but the
		// frame-reader goroutine may still be trying to read the next frame
		// from the conn. We then write another frame into the pipe so the
		// reader successfully reads it and tries to send on frameChan — which
		// nobody is draining. This causes the leak.
		sendConnectionClose(t, conn)

		// Give server time to process the close
		time.Sleep(100 * time.Millisecond)
	}

	// Close all connections from client side to unblock any io.ReadFull
	for _, conn := range conns {
		if conn != nil {
			conn.Close()
		}
	}

	// Allow goroutines to wind down
	_ = s // keep reference
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()

	// We expect no leaked goroutines. Allow a small delta for runtime jitter,
	// but certainly not numConns extra goroutines.
	leaked := finalGoroutines - baselineGoroutines
	t.Logf("baseline goroutines: %d, final: %d, leaked: %d", baselineGoroutines, finalGoroutines, leaked)
	assert.LessOrEqual(t, leaked, 2,
		"expected no goroutine leak, but %d goroutines remain after closing %d connections", leaked, numConns)
}

// sendConnectionClose writes a minimal AMQP Connection.Close frame to conn.
// Connection.Close is class=10, method=50.
func sendConnectionClose(t *testing.T, conn net.Conn) {
	t.Helper()

	// Method frame payload: class(2) + method(2) + replyCode(2) + replyText(shortstr) + classId(2) + methodId(2)
	// replyText = "" (length 0)
	payload := make([]byte, 0, 10)
	payload = binary.BigEndian.AppendUint16(payload, 10) // class: connection
	payload = binary.BigEndian.AppendUint16(payload, 50) // method: close
	payload = binary.BigEndian.AppendUint16(payload, 200) // reply-code: normal
	payload = append(payload, 0)                          // reply-text length: 0
	payload = binary.BigEndian.AppendUint16(payload, 0)   // failing class-id
	payload = binary.BigEndian.AppendUint16(payload, 0)   // failing method-id

	// Frame: type(1) + channel(2) + size(4) + payload + frame-end(1)
	frame := make([]byte, 0, 7+len(payload)+1)
	frame = append(frame, 1) // type: METHOD
	frame = binary.BigEndian.AppendUint16(frame, 0) // channel 0
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = append(frame, 0xCE) // frame-end

	_, err := conn.Write(frame)
	assert.NoError(t, err)
}
