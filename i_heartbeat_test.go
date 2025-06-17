package carrotmq

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// TestServerHeartbeatsKeepConnectionAlive demonstrates that server heartbeats are essential for keeping idle connections alive
// With server heartbeats enabled (current state), the connection should remain open during idle periods
func TestServerHeartbeatsKeepConnectionAlive(t *testing.T) {
	// Setup server (with heartbeats enabled by default)
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect with short heartbeat interval
	config := amqp.Config{
		Heartbeat: 2 * time.Second,
	}

	conn, err := amqp.DialConfig("amqp://"+addr, config)
	require.NoError(t, err)
	defer conn.Close()

	// Set up connection close notification
	connCloseChan := make(chan *amqp.Error, 1)
	conn.NotifyClose(connCloseChan)

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create a queue
	qName := uniqueName("q-heartbeat-alive-test")
	_, err = ch.QueueDeclare(
		qName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	require.NoError(t, err)

	// Start a consumer
	msgs, err := ch.Consume(
		qName,
		"test-consumer",
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	require.NoError(t, err)

	// Track when consumer stops
	consumerStopped := make(chan bool, 1)
	go func() {
		for range msgs {
			// Just consume messages
		}
		consumerStopped <- true
	}()

	// Wait for longer than the heartbeat timeout would be
	// With server heartbeats, connection should stay alive
	idleTime := config.Heartbeat * 3
	t.Logf("Keeping connection idle for %v (heartbeat interval is %v)", idleTime, config.Heartbeat)
	t.Log("With server heartbeats enabled, connection should remain open")

	select {
	case closeErr := <-connCloseChan:
		// This would indicate a problem - connection shouldn't close with heartbeats
		t.Fatalf("Connection closed unexpectedly during idle period: %v", closeErr)
	case <-time.After(idleTime):
		// Good - connection stayed alive
		t.Log("✓ Connection remained open during idle period (server heartbeats working)")
	}

	// Verify we can still use the connection
	err = ch.Publish(
		"",    // exchange
		qName, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("test message after idle"),
		},
	)
	require.NoError(t, err, "Should be able to publish after idle period")

	t.Log("✓ Successfully published message after idle period")
}

// TestHeartbeatWithActiveTraffic verifies that regular traffic doesn't interfere with heartbeats
func TestHeartbeatWithActiveTraffic(t *testing.T) {
	// Setup server
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect with short heartbeat
	config := amqp.Config{
		Heartbeat: 1 * time.Second, // Very short for testing
	}

	conn, err := amqp.DialConfig("amqp://"+addr, config)
	require.NoError(t, err)
	defer conn.Close()

	// Set up connection close notification
	connCloseChan := make(chan *amqp.Error, 1)
	conn.NotifyClose(connCloseChan)

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create a queue
	qName := uniqueName("q-heartbeat-traffic-test")
	_, err = ch.QueueDeclare(
		qName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	require.NoError(t, err)

	// Keep publishing messages at intervals less than heartbeat
	stopPublishing := make(chan bool)
	var publishWg sync.WaitGroup
	publishWg.Add(1)

	go func() {
		defer publishWg.Done()
		ticker := time.NewTicker(500 * time.Millisecond) // Publish every 0.5s
		defer ticker.Stop()

		msgCount := 0
		for {
			select {
			case <-ticker.C:
				msgCount++
				err := ch.Publish(
					"",    // exchange
					qName, // routing key
					false, // mandatory
					false, // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(time.Now().Format(time.RFC3339)),
					},
				)
				if err != nil {
					t.Logf("Publish error after %d messages: %v", msgCount, err)
					return
				}
			case <-stopPublishing:
				t.Logf("Published %d messages total", msgCount)
				return
			}
		}
	}()

	// Run for several heartbeat intervals
	testDuration := config.Heartbeat * 5
	t.Logf("Running test for %v with heartbeat interval %v", testDuration, config.Heartbeat)

	select {
	case closeErr := <-connCloseChan:
		t.Fatalf("Connection closed unexpectedly: %v", closeErr)
	case <-time.After(testDuration):
		// Good - connection stayed alive with active traffic
		t.Log("✓ Connection remained open with active traffic and heartbeats")
	}

	// Stop publishing
	close(stopPublishing)
	publishWg.Wait()
}

// TestConfigurableHeartbeatInterval verifies that the server can be configured with a custom heartbeat interval
func TestConfigurableHeartbeatInterval(t *testing.T) {
	// Test with different heartbeat intervals
	testCases := []struct {
		name               string
		serverHeartbeat    uint16
		clientHeartbeat    time.Duration
		expectedNegotiated uint16
	}{
		{
			name:               "Server suggests 30s, client requests 30s",
			serverHeartbeat:    30,
			clientHeartbeat:    30 * time.Second,
			expectedNegotiated: 30,
		},
		{
			name:               "Server suggests 10s, client requests 20s",
			serverHeartbeat:    10,
			clientHeartbeat:    20 * time.Second,
			expectedNegotiated: 10, // Should use the smaller value
		},
		{
			name:               "Server suggests 120s, client requests 60s",
			serverHeartbeat:    120,
			clientHeartbeat:    60 * time.Second,
			expectedNegotiated: 60, // Should use the smaller value
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock logger to capture server logs
			mockLogger := &mockLogger{
				logs: make([]string, 0),
				mu:   sync.Mutex{},
			}

			// Setup server with custom heartbeat interval and mock logger
			addr, cleanup := setupTestServer(t,
				WithInMemoryStorage(),
				WithHeartbeatInterval(tc.serverHeartbeat),
				WithLogger(mockLogger),
			)
			defer cleanup()

			// Connect with client-requested heartbeat
			config := amqp.Config{
				Heartbeat: tc.clientHeartbeat,
			}

			conn, err := amqp.DialConfig("amqp://"+addr, config)
			require.NoError(t, err)
			defer conn.Close()

			// Give a moment for connection negotiation to complete
			time.Sleep(100 * time.Millisecond)

			// Check the logs for the negotiated heartbeat value
			mockLogger.mu.Lock()
			logs := mockLogger.logs
			mockLogger.mu.Unlock()

			// Look for the log entry that contains the negotiated heartbeat
			foundNegotiated := false
			for _, log := range logs {
				// Look for log entries that mention connection parameters negotiated
				if strings.Contains(log, "Connection parameters negotiated:") {
					// Extract the heartbeat value from the log
					// Format: "Connection parameters negotiated: channelMax=%d, frameMax=%d, heartbeat=%d"
					var channelMax, heartbeat uint16
					var frameMax uint32
					_, err := fmt.Sscanf(log, "Connection parameters negotiated: channelMax=%d, frameMax=%d, heartbeat=%d",
						&channelMax, &frameMax, &heartbeat)
					if err == nil {
						require.Equal(t, tc.expectedNegotiated, heartbeat,
							"Negotiated heartbeat should match expected value")
						foundNegotiated = true
						t.Logf("✓ Found negotiated heartbeat: %d seconds", heartbeat)
						break
					}
				}
			}

			require.True(t, foundNegotiated, "Should find negotiated heartbeat in logs")
		})
	}
}

// mockLogger implements logger.Logger interface for testing
type mockLogger struct {
	logs []string
	mu   sync.Mutex
}

func (m *mockLogger) log(_, format string, a ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msg := fmt.Sprintf(format, a...)
	m.logs = append(m.logs, msg)
}

func (m *mockLogger) Fatal(format string, a ...any) {
	m.log("FATAL", format, a...)
	panic(fmt.Sprintf(format, a...))
}

func (m *mockLogger) Err(format string, a ...any) {
	m.log("ERROR", format, a...)
}

func (m *mockLogger) Warn(format string, a ...any) {
	m.log("WARN", format, a...)
}

func (m *mockLogger) Info(format string, a ...any) {
	m.log("INFO", format, a...)
}

func (m *mockLogger) Debug(format string, a ...any) {
	m.log("DEBUG", format, a...)
}
