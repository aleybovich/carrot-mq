package internal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeMethodFrame builds a raw AMQP method frame with the given payload size.
// The payload is filled with zeros (content doesn't matter for size checks).
func makeMethodFrame(channel uint16, payloadSize int) []byte {
	buf := make([]byte, 0, 7+payloadSize+1)
	buf = append(buf, FrameMethod)
	buf = binary.BigEndian.AppendUint16(buf, channel)
	buf = binary.BigEndian.AppendUint32(buf, uint32(payloadSize))
	buf = append(buf, make([]byte, payloadSize)...)
	buf = append(buf, FrameEnd)
	return buf
}

// performAMQPHandshake sends the protocol header and reads Connection.Start,
// returning the raw conn positioned after start for further test interaction.
func performAMQPHandshake(t *testing.T, conn net.Conn) {
	t.Helper()
	_, err := conn.Write([]byte("AMQP\x00\x00\x09\x01"))
	require.NoError(t, err)
	// Wait for Connection.Start from server
	time.Sleep(150 * time.Millisecond)
}

// sendConnectionStartOk sends a minimal connection.start-ok frame.
func sendConnectionStartOk(t *testing.T, conn net.Conn) {
	t.Helper()
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
	binary.Write(payload, binary.BigEndian, uint16(MethodConnectionStartOk))

	// client-properties (empty table: 4 bytes of 0 length)
	binary.Write(payload, binary.BigEndian, uint32(0))
	// mechanism: "PLAIN" (short string)
	payload.WriteByte(5)
	payload.WriteString("PLAIN")
	// response (long string): "\x00guest\x00guest"
	response := "\x00guest\x00guest"
	binary.Write(payload, binary.BigEndian, uint32(len(response)))
	payload.WriteString(response)
	// locale: "en_US"
	payload.WriteByte(5)
	payload.WriteString("en_US")

	frame := makeMethodFrame(0, payload.Len())
	// Overwrite payload content with actual start-ok data
	copy(frame[7:], payload.Bytes())

	_, err := conn.Write(frame)
	require.NoError(t, err)
}

// sendTuneOk sends a connection.tune-ok with specified parameters.
func sendTuneOk(t *testing.T, conn net.Conn, channelMax uint16, frameMax uint32, heartbeat uint16) {
	t.Helper()
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
	binary.Write(payload, binary.BigEndian, uint16(MethodConnectionTuneOk))
	binary.Write(payload, binary.BigEndian, channelMax)
	binary.Write(payload, binary.BigEndian, frameMax)
	binary.Write(payload, binary.BigEndian, heartbeat)

	frame := makeMethodFrame(0, payload.Len())
	copy(frame[7:], payload.Bytes())

	_, err := conn.Write(frame)
	require.NoError(t, err)
}

// readResponseFrame reads one AMQP frame from the connection.
// Returns frame type, channel, and payload.
func readResponseFrame(t *testing.T, conn net.Conn) (frameType byte, channel uint16, payload []byte) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	header := make([]byte, 7)
	_, err := io.ReadFull(conn, header)
	require.NoError(t, err, "reading response frame header")

	frameType = header[0]
	channel = binary.BigEndian.Uint16(header[1:3])
	size := binary.BigEndian.Uint32(header[3:7])

	payload = make([]byte, size)
	_, err = io.ReadFull(conn, payload)
	require.NoError(t, err, "reading response frame payload")

	end := make([]byte, 1)
	_, err = io.ReadFull(conn, end)
	require.NoError(t, err, "reading frame end")
	assert.Equal(t, byte(FrameEnd), end[0])

	return frameType, channel, payload
}

// drainUntilConnectionClose reads frames until it finds a Connection.Close,
// returning the reply code from that frame.
func drainUntilConnectionClose(t *testing.T, conn net.Conn) (replyCode uint16, found bool) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	for i := 0; i < 10; i++ { // safety limit
		header := make([]byte, 7)
		_, err := io.ReadFull(conn, header)
		if err != nil {
			return 0, false
		}

		size := binary.BigEndian.Uint32(header[3:7])
		payload := make([]byte, size)
		_, err = io.ReadFull(conn, payload)
		if err != nil {
			return 0, false
		}

		// Read frame-end
		end := make([]byte, 1)
		io.ReadFull(conn, end)

		// Check if this is a method frame on channel 0
		if header[0] == FrameMethod && size >= 4 {
			reader := bytes.NewReader(payload)
			var classId, methodId uint16
			binary.Read(reader, binary.BigEndian, &classId)
			binary.Read(reader, binary.BigEndian, &methodId)
			if classId == ClassConnection && methodId == MethodConnectionClose {
				binary.Read(reader, binary.BigEndian, &replyCode)
				return replyCode, true
			}
		}
	}
	return 0, false
}

// === Tests for readFrame pre-negotiation size enforcement ===

func TestReadFrame_PreNegotiation_AcceptsFrameWithinMinSize(t *testing.T) {
	_, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	// Send AMQP header — after this, frameMax is still 0 (pre-negotiation)
	performAMQPHandshake(t, conn)

	// Server sent Connection.Start. Now send a start-ok frame that is
	// well within 4096 bytes — this should be accepted.
	sendConnectionStartOk(t, conn)

	// If accepted, server should respond with Connection.Tune
	time.Sleep(200 * time.Millisecond)

	_, _, _ = readResponseFrame(t, conn) // Connection.Start (already buffered)
	// The start frame was already consumed by performAMQPHandshake's sleep,
	// so the next frame should be Connection.Tune after our start-ok.
	// But we need to consume Connection.Start first if it hasn't been read yet.

	// If we get here without error, the frame was accepted.
	// The test passes if the connection is still alive.
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	peek := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err = conn.Read(peek)
	// We expect either data (Tune frame) or timeout — NOT connection reset
	if err != nil {
		assert.ErrorIs(t, err, netTimeoutErr{}, "connection should still be alive, not reset")
	}
}

// netTimeoutErr implements the net.Error interface check
type netTimeoutErr struct{}

func (netTimeoutErr) Error() string   { return "timeout" }
func (netTimeoutErr) Timeout() bool   { return true }
func (netTimeoutErr) Temporary() bool { return true }

func TestReadFrame_PreNegotiation_RejectsOversizedFrame(t *testing.T) {
	_, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	performAMQPHandshake(t, conn)

	// Read and discard Connection.Start
	readResponseFrame(t, conn)

	// Send a frame that exceeds frameMinSize (4096).
	// The declared payload size is 5000 bytes — the server should reject
	// this before even reading the payload, and close the connection.
	oversizedFrame := makeMethodFrame(0, 5000)
	_, err = conn.Write(oversizedFrame)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Server should have closed the connection (possibly after sending Connection.Close)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 1024)
	_, err = conn.Read(buf)
	// Connection should be closed or we get a Connection.Close frame
	// Either way, subsequent reads should fail
	if err == nil {
		// We read something — it should be a Connection.Close frame
		// Try reading more; connection should die
		time.Sleep(100 * time.Millisecond)
		_, err = conn.Read(buf)
	}
	// Eventually the connection must be dead
	assert.Error(t, err, "server should have closed the connection after oversized frame")
}

func TestReadFrame_PreNegotiation_AcceptsExactlyMinSizeFrame(t *testing.T) {
	_, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	performAMQPHandshake(t, conn)

	// Read and discard Connection.Start
	readResponseFrame(t, conn)

	// Send a frame with payload exactly frameMinSize (4096) bytes.
	// This should be accepted per spec.
	exactFrame := makeMethodFrame(0, frameMinSize)
	_, err = conn.Write(exactFrame)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// The server may reject this as an invalid method (payload is zeros),
	// but it should NOT reject it for size. Check that the connection is
	// still alive or we got a protocol-level error (not frame-error for size).
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err == nil && n > 0 {
		// Parse any connection.close we get — reply code should NOT be frame-error (501)
		reader := bufio.NewReader(bytes.NewReader(buf[:n]))
		header := make([]byte, 7)
		if _, readErr := io.ReadFull(reader, header); readErr == nil {
			if header[0] == FrameMethod {
				size := binary.BigEndian.Uint32(header[3:7])
				payload := make([]byte, size)
				if _, readErr = io.ReadFull(reader, payload); readErr == nil && size >= 6 {
					pr := bytes.NewReader(payload)
					var classId, methodId, replyCode uint16
					binary.Read(pr, binary.BigEndian, &classId)
					binary.Read(pr, binary.BigEndian, &methodId)
					binary.Read(pr, binary.BigEndian, &replyCode)
					if classId == ClassConnection && methodId == MethodConnectionClose {
						// Frame was accepted (not rejected for size). Server may close
						// for other reasons (invalid method content) — that's fine.
						assert.NotEqual(t, uint16(501), replyCode,
							"server should not reject a frame-min-size frame with FRAME_ERROR")
					}
				}
			}
		}
	}
}

// === Tests for tune-ok frame-max validation ===

func TestTuneOk_AcceptsFrameMaxEqualToServerProposal(t *testing.T) {
	s, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()
	_ = s

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	performAMQPHandshake(t, conn)
	// Read Connection.Start
	readResponseFrame(t, conn)
	// Send start-ok
	sendConnectionStartOk(t, conn)
	time.Sleep(200 * time.Millisecond)
	// Read Connection.Tune
	readResponseFrame(t, conn)

	// Send tune-ok with frame-max = suggestedFrameMaxSize (131072) — should be accepted
	sendTuneOk(t, conn, 0, suggestedFrameMaxSize, 0)
	time.Sleep(200 * time.Millisecond)

	// Connection should still be alive — server waits for connection.open
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	// Expect timeout (connection alive, waiting for more data)
	if err != nil {
		var netErr net.Error
		if assert.ErrorAs(t, err, &netErr) {
			assert.True(t, netErr.Timeout(), "expected timeout, got: %v", err)
		}
	}
}

func TestTuneOk_AcceptsFrameMaxLowerThanServerProposal(t *testing.T) {
	s, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()
	_ = s

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	performAMQPHandshake(t, conn)
	readResponseFrame(t, conn)
	sendConnectionStartOk(t, conn)
	time.Sleep(200 * time.Millisecond)
	readResponseFrame(t, conn)

	// Send tune-ok with frame-max smaller than server's — should be accepted
	sendTuneOk(t, conn, 0, 65536, 0)
	time.Sleep(200 * time.Millisecond)

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err != nil {
		var netErr net.Error
		if assert.ErrorAs(t, err, &netErr) {
			assert.True(t, netErr.Timeout(), "expected timeout (alive), got: %v", err)
		}
	}
}

func TestTuneOk_AcceptsFrameMaxZero_TreatsAsServerDefault(t *testing.T) {
	s, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()
	_ = s

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	performAMQPHandshake(t, conn)
	readResponseFrame(t, conn)
	sendConnectionStartOk(t, conn)
	time.Sleep(200 * time.Millisecond)
	readResponseFrame(t, conn)

	// frame-max=0 means "no limit from client" — server should accept and
	// use its own proposed value
	sendTuneOk(t, conn, 0, 0, 0)
	time.Sleep(200 * time.Millisecond)

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err != nil {
		var netErr net.Error
		if assert.ErrorAs(t, err, &netErr) {
			assert.True(t, netErr.Timeout(), "expected timeout (alive), got: %v", err)
		}
	}
}

func TestTuneOk_RejectsFrameMaxExceedingServerProposal(t *testing.T) {
	_, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	performAMQPHandshake(t, conn)
	readResponseFrame(t, conn)
	sendConnectionStartOk(t, conn)
	time.Sleep(200 * time.Millisecond)
	readResponseFrame(t, conn)

	// Send tune-ok with frame-max > suggestedFrameMaxSize — server MUST close
	sendTuneOk(t, conn, 0, suggestedFrameMaxSize+1, 0)
	time.Sleep(300 * time.Millisecond)

	// Server should send Connection.Close with NOT_ALLOWED (530)
	replyCode, found := drainUntilConnectionClose(t, conn)
	if found {
		assert.Equal(t, uint16(530), replyCode,
			"expected NOT_ALLOWED (530) for frame-max exceeding server proposal")
	} else {
		// Connection may have been torn down directly
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		assert.Error(t, err, "connection should be closed after invalid frame-max")
	}
}

func TestTuneOk_RejectsFrameMaxWayAboveServerProposal(t *testing.T) {
	_, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	performAMQPHandshake(t, conn)
	readResponseFrame(t, conn)
	sendConnectionStartOk(t, conn)
	time.Sleep(200 * time.Millisecond)
	readResponseFrame(t, conn)

	// Send a very large frame-max (10MB) — must be rejected
	sendTuneOk(t, conn, 0, 10*1024*1024, 0)
	time.Sleep(300 * time.Millisecond)

	replyCode, found := drainUntilConnectionClose(t, conn)
	if found {
		assert.Equal(t, uint16(530), replyCode)
	} else {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		assert.Error(t, err, "connection should be closed after invalid frame-max")
	}
}

// === Post-negotiation frame size enforcement ===

func TestReadFrame_PostNegotiation_AcceptsFrameWithinNegotiatedMax(t *testing.T) {
	_, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	performAMQPHandshake(t, conn)
	readResponseFrame(t, conn)
	sendConnectionStartOk(t, conn)
	time.Sleep(200 * time.Millisecond)
	readResponseFrame(t, conn)

	// Negotiate frame-max = 8192
	sendTuneOk(t, conn, 0, 8192, 0)
	time.Sleep(200 * time.Millisecond)

	// Send a frame with payload of 5000 bytes — within negotiated max
	frame := makeMethodFrame(0, 5000)
	_, err = conn.Write(frame)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Connection should still be alive (server may send error for invalid method
	// but NOT for frame size)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err == nil && n > 0 {
		// If we got a Connection.Close, verify it's not FRAME_ERROR
		reader := bytes.NewReader(buf[:n])
		header := make([]byte, 7)
		if _, readErr := io.ReadFull(reader, header); readErr == nil {
			if header[0] == FrameMethod {
				size := binary.BigEndian.Uint32(header[3:7])
				payload := make([]byte, size)
				if _, readErr = io.ReadFull(reader, payload); readErr == nil && size >= 6 {
					pr := bytes.NewReader(payload)
					var classId, methodId, replyCode uint16
					binary.Read(pr, binary.BigEndian, &classId)
					binary.Read(pr, binary.BigEndian, &methodId)
					binary.Read(pr, binary.BigEndian, &replyCode)
					if classId == ClassConnection && methodId == MethodConnectionClose {
						assert.NotEqual(t, uint16(501), replyCode,
							"frame within negotiated max should not get FRAME_ERROR")
					}
				}
			}
		}
	}
}

func TestReadFrame_PostNegotiation_RejectsFrameExceedingNegotiatedMax(t *testing.T) {
	_, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	performAMQPHandshake(t, conn)
	readResponseFrame(t, conn)
	sendConnectionStartOk(t, conn)
	time.Sleep(200 * time.Millisecond)
	readResponseFrame(t, conn)

	// Negotiate a small frame-max
	sendTuneOk(t, conn, 0, 8192, 0)
	time.Sleep(200 * time.Millisecond)

	// Send a frame exceeding the negotiated max
	frame := makeMethodFrame(0, 9000)
	_, err = conn.Write(frame)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	// Server should close the connection
	replyCode, found := drainUntilConnectionClose(t, conn)
	if found {
		assert.Equal(t, uint16(501), replyCode,
			"expected FRAME_ERROR (501) for oversized post-negotiation frame")
	} else {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		assert.Error(t, err, "connection should be closed after oversized frame")
	}
}
