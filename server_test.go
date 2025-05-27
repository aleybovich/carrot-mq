// server_test.go
package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	// "os" // No longer needed directly in this file for AMQP_DEBUG

	amqp "github.com/rabbitmq/amqp091-go" // Official RabbitMQ client
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// Helper to start a server and return its address and a cleanup function
func setupTestServer(t *testing.T, opts ...ServerOption) (addr string, cleanup func()) {
	isTerminal = true // Force colorized output for server logs during tests
	addr = getNextTestPort()
	server := NewServer(opts...) // Uses default internal logger

	// Channel to signal when server goroutine exits
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		if err := server.Start(addr); err != nil {
			// Only log if it's not the expected closed listener error
			if !errors.Is(err, net.ErrClosed) {
				t.Logf("Test server failed to start on %s: %v", addr, err)
			}
		}
	}()

	// Wait a bit for server to start
	time.Sleep(200 * time.Millisecond)

	cleanup = func() {
		if server.listener != nil {
			err := server.listener.Close()
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

	return addr, cleanup
}

// Basic connection test
func TestServerConnection(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Should connect successfully")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Should open channel successfully")
	defer ch.Close()
}

func TestConnection_ClientInitiatedClose(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Should connect successfully")

	err = conn.Close()
	require.NoError(t, err, "Client should close connection without error")

	_, chErr := conn.Channel()
	require.Error(t, chErr, "Operations on closed connection should fail")
	assert.Contains(t, chErr.Error(), amqp.ErrClosed.Error(), "Error should indicate connection is closed")
}

// --- Authentication Tests ---

func TestAuth_NoAuthMode(t *testing.T) {
	// Server with no authentication
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect with any credentials - should succeed
	conn, err := amqp.Dial("amqp://anyuser:anypass@" + addr)
	require.NoError(t, err, "Should connect successfully without authentication")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Should open channel successfully")
	defer ch.Close()
}

func TestAuth_PlainMode_ValidCredentials(t *testing.T) {
	// Server with authentication enabled
	addr, cleanup := setupTestServer(t, WithAuth(map[string]string{
		"guest": "guest123",
		"admin": "secretpass",
	}))
	defer cleanup()

	// Test valid credentials
	conn, err := amqp.Dial("amqp://guest:guest123@" + addr)
	require.NoError(t, err, "Should connect successfully with valid credentials")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Should open channel successfully")
	defer ch.Close()
}

func TestAuth_PlainMode_InvalidCredentials(t *testing.T) {
	// Server with authentication enabled
	addr, cleanup := setupTestServer(t, WithAuth(map[string]string{
		"guest": "guest123",
		"admin": "secretpass",
	}))
	defer cleanup()

	// Test invalid password
	_, err := amqp.Dial("amqp://guest:wrongpass@" + addr)
	require.Error(t, err, "Should fail to connect with invalid password")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Error should be of type *amqp.Error")
	assert.Equal(t, amqp.AccessRefused, amqpErr.Code, "AMQP error code should be 403 (ACCESS_REFUSED)")

	// Test invalid username
	conn2, err := amqp.Dial("amqp://unknownuser:anypass@" + addr)
	require.Error(t, err, "Should fail to connect with invalid username")
	if conn2 != nil {
		conn2.Close()
	}

	amqpErr2, ok := err.(*amqp.Error)
	require.True(t, ok, "Error should be of type *amqp.Error")
	assert.Equal(t, amqp.AccessRefused, amqpErr2.Code, "AMQP error code should be 403 (ACCESS_REFUSED)")
}

func TestAuth_PlainMode_EmptyCredentials(t *testing.T) {
	// Server with authentication enabled
	addr, cleanup := setupTestServer(t, WithAuth(map[string]string{
		"guest": "guest123",
	}))
	defer cleanup()

	// Test empty credentials
	conn, err := amqp.Dial("amqp://" + addr)
	require.Error(t, err, "Should fail to connect without credentials when auth is enabled")
	if conn != nil {
		conn.Close()
	}
}

// --- Exchange Type Tests ---

func TestExchangeType_Direct(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Declare direct exchange
	exchangeName := "test-direct-ex"
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	// Create two queues with different routing keys
	q1, err := ch.QueueDeclare("direct-q1", false, false, false, false, nil)
	require.NoError(t, err)
	q2, err := ch.QueueDeclare("direct-q2", false, false, false, false, nil)
	require.NoError(t, err)

	// Bind queues with different routing keys
	err = ch.QueueBind(q1.Name, "key1", exchangeName, false, nil)
	require.NoError(t, err)
	err = ch.QueueBind(q2.Name, "key2", exchangeName, false, nil)
	require.NoError(t, err)

	// Start consumers
	msgs1, err := ch.Consume(q1.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	msgs2, err := ch.Consume(q2.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Publish to key1
	body1 := "Message for key1"
	err = ch.Publish(exchangeName, "key1", false, false, amqp.Publishing{Body: []byte(body1)})
	require.NoError(t, err)

	// Publish to key2
	body2 := "Message for key2"
	err = ch.Publish(exchangeName, "key2", false, false, amqp.Publishing{Body: []byte(body2)})
	require.NoError(t, err)

	// Verify routing
	select {
	case msg := <-msgs1:
		assert.Equal(t, body1, string(msg.Body), "Queue1 should receive message for key1")
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for message in queue1")
	}

	select {
	case msg := <-msgs2:
		assert.Equal(t, body2, string(msg.Body), "Queue2 should receive message for key2")
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for message in queue2")
	}

	// Ensure no cross-delivery
	select {
	case <-msgs1:
		assert.Fail(t, "Queue1 should not receive message for key2")
	case <-time.After(200 * time.Millisecond):
		// Expected timeout
	}
}

func TestExchangeType_Fanout(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Declare fanout exchange
	exchangeName := "test-fanout-ex"
	err = ch.ExchangeDeclare(exchangeName, "fanout", false, false, false, false, nil)
	require.NoError(t, err)

	// Create three queues
	q1, err := ch.QueueDeclare("fanout-q1", false, false, false, false, nil)
	require.NoError(t, err)
	q2, err := ch.QueueDeclare("fanout-q2", false, false, false, false, nil)
	require.NoError(t, err)
	q3, err := ch.QueueDeclare("fanout-q3", false, false, false, false, nil)
	require.NoError(t, err)

	// Bind all queues to fanout exchange (routing key is ignored)
	err = ch.QueueBind(q1.Name, "ignored1", exchangeName, false, nil)
	require.NoError(t, err)
	err = ch.QueueBind(q2.Name, "ignored2", exchangeName, false, nil)
	require.NoError(t, err)
	err = ch.QueueBind(q3.Name, "ignored3", exchangeName, false, nil)
	require.NoError(t, err)

	// Start consumers
	msgs1, err := ch.Consume(q1.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	msgs2, err := ch.Consume(q2.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	msgs3, err := ch.Consume(q3.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Publish one message
	body := "Fanout message to all"
	err = ch.Publish(exchangeName, "any-routing-key", false, false, amqp.Publishing{Body: []byte(body)})
	require.NoError(t, err)

	// All queues should receive the message
	received := 0
	timeout := time.After(2 * time.Second)

	for received < 3 {
		select {
		case msg := <-msgs1:
			assert.Equal(t, body, string(msg.Body))
			received++
		case msg := <-msgs2:
			assert.Equal(t, body, string(msg.Body))
			received++
		case msg := <-msgs3:
			assert.Equal(t, body, string(msg.Body))
			received++
		case <-timeout:
			assert.Fail(t, fmt.Sprintf("Timeout: only received %d/3 messages", received))
			return
		}
	}

	assert.Equal(t, 3, received, "All three queues should receive the fanout message")
}

func TestExchangeType_Topic(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Declare topic exchange
	exchangeName := "test-topic-ex"
	err = ch.ExchangeDeclare(exchangeName, "topic", false, false, false, false, nil)
	require.NoError(t, err)

	// Create queues for different patterns
	qAll, err := ch.QueueDeclare("topic-all", false, false, false, false, nil)
	require.NoError(t, err)
	qErrors, err := ch.QueueDeclare("topic-errors", false, false, false, false, nil)
	require.NoError(t, err)
	qWarnings, err := ch.QueueDeclare("topic-warnings", false, false, false, false, nil)
	require.NoError(t, err)
	qKernel, err := ch.QueueDeclare("topic-kernel", false, false, false, false, nil)
	require.NoError(t, err)

	// Bind with topic patterns
	err = ch.QueueBind(qAll.Name, "#", exchangeName, false, nil) // Receives everything
	require.NoError(t, err)
	err = ch.QueueBind(qErrors.Name, "*.error", exchangeName, false, nil) // Any error
	require.NoError(t, err)
	err = ch.QueueBind(qWarnings.Name, "*.warning", exchangeName, false, nil) // Any warning
	require.NoError(t, err)
	err = ch.QueueBind(qKernel.Name, "kernel.*", exchangeName, false, nil) // Kernel messages
	require.NoError(t, err)

	// Start consumers
	msgsAll, err := ch.Consume(qAll.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	msgsErrors, err := ch.Consume(qErrors.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	msgsWarnings, err := ch.Consume(qWarnings.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	msgsKernel, err := ch.Consume(qKernel.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Test messages
	testCases := []struct {
		routingKey     string
		body           string
		expectedQueues map[string]bool
	}{
		{
			routingKey: "kernel.error",
			body:       "Kernel error message",
			expectedQueues: map[string]bool{
				"all":    true,
				"errors": true,
				"kernel": true,
			},
		},
		{
			routingKey: "app.warning",
			body:       "App warning message",
			expectedQueues: map[string]bool{
				"all":      true,
				"warnings": true,
			},
		},
		{
			routingKey: "kernel.info",
			body:       "Kernel info message",
			expectedQueues: map[string]bool{
				"all":    true,
				"kernel": true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.routingKey, func(t *testing.T) {
			err := ch.Publish(exchangeName, tc.routingKey, false, false, amqp.Publishing{Body: []byte(tc.body)})
			require.NoError(t, err)

			// Give messages time to be delivered
			time.Sleep(100 * time.Millisecond)

			// Check each queue
			checkQueue := func(name string, msgs <-chan amqp.Delivery, shouldReceive bool) {
				select {
				case msg := <-msgs:
					if shouldReceive {
						assert.Equal(t, tc.body, string(msg.Body), "%s queue should receive correct message", name)
					} else {
						assert.Fail(t, "%s queue should not receive message for routing key %s", name, tc.routingKey)
					}
				case <-time.After(200 * time.Millisecond):
					if shouldReceive {
						assert.Fail(t, "%s queue should receive message for routing key %s", name, tc.routingKey)
					}
				}
			}

			checkQueue("all", msgsAll, tc.expectedQueues["all"])
			checkQueue("errors", msgsErrors, tc.expectedQueues["errors"])
			checkQueue("warnings", msgsWarnings, tc.expectedQueues["warnings"])
			checkQueue("kernel", msgsKernel, tc.expectedQueues["kernel"])
		})
	}
}

func TestExchangeType_Topic_ComplexPatterns(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "test-topic-complex"
	err = ch.ExchangeDeclare(exchangeName, "topic", false, false, false, false, nil)
	require.NoError(t, err)

	// Test # matching zero words
	q1, err := ch.QueueDeclare("", false, false, false, false, nil)
	require.NoError(t, err)
	err = ch.QueueBind(q1.Name, "logs.#", exchangeName, false, nil)
	require.NoError(t, err)

	msgs1, err := ch.Consume(q1.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Should match "logs" (# matches zero words)
	err = ch.Publish(exchangeName, "logs", false, false, amqp.Publishing{Body: []byte("test")})
	require.NoError(t, err)

	select {
	case <-msgs1:
		// Expected
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Pattern 'logs.#' should match 'logs'")
	}

	// Test # in middle of pattern
	q2, err := ch.QueueDeclare("", false, false, false, false, nil)
	require.NoError(t, err)
	err = ch.QueueBind(q2.Name, "*.#.error", exchangeName, false, nil)
	require.NoError(t, err)

	msgs2, err := ch.Consume(q2.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Should match "app.module.submodule.error"
	err = ch.Publish(exchangeName, "app.module.submodule.error", false, false, amqp.Publishing{Body: []byte("test")})
	require.NoError(t, err)

	select {
	case <-msgs2:
		// Expected
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Pattern '*.#.error' should match 'app.module.submodule.error'")
	}
}

// --- Exchange.Declare Tests ---
func TestExchangeDeclare_Passive_NotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	err = ch.ExchangeDeclarePassive("non-existent-exchange-passive-nf", "direct", true, false, false, false, nil)
	require.Error(t, err, "Passive declare of non-existent exchange should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Error should be of type *amqp.Error")
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "AMQP error code should be 404 (NOT_FOUND)")
}

func TestExchangeDeclare_Passive_TypeMismatch(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "passive-type-mismatch-ex"
	err = ch.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil)
	require.NoError(t, err, "Initial exchange declaration failed")

	err = ch.ExchangeDeclarePassive(exchangeName, "fanout", true, false, false, false, nil)
	require.Error(t, err, "Passive declare with type mismatch should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Error should be of type *amqp.Error")
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "AMQP error code should be 406 (PRECONDITION_FAILED)")
}

func TestExchangeDeclare_Active_RedeclareDifferentProperties(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "redeclare-prop-diff-ex"
	err = ch.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil) // durable=true
	require.NoError(t, err, "Initial exchange declaration failed")

	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil) // durable=false
	require.Error(t, err, "Redeclare exchange with different 'durable' property should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Error should be of type *amqp.Error")
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "AMQP error code should be 406 (PRECONDITION_FAILED)")
}

func TestExchangeDeclare_Active_RedeclareSameProperties(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "redeclare-same-ex"
	err = ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
	require.NoError(t, err, "First declare should succeed")

	err = ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
	require.NoError(t, err, "Redeclare exchange with same properties should succeed")
}

func TestExchangeDeclare_InvalidType(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	err = ch.ExchangeDeclare("invalid-type-ex", "invalid-exchange-type-value", false, false, false, false, nil)
	require.Error(t, err, "Declare exchange with invalid type should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Error should be of type *amqp.Error")
	assert.Equal(t, amqp.NotImplemented, amqpErr.Code, "AMQP error code should be 540 (NOT_IMPLEMENTED)")
}

func TestExchangeDeclare_NoWait(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "nowait-declare-ex"
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, true, nil) // noWait = true
	require.NoError(t, err, "ExchangeDeclare with noWait should not return an error on valid declaration")

	err = ch.ExchangeDeclarePassive(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err, "Passive declare of noWait-declared exchange should succeed")
}

// --- Queue.Declare Tests ---

func TestQueueDeclare_ServerGeneratedName(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	q, err := ch.QueueDeclare("", false, false, false, false, nil)
	require.NoError(t, err, "QueueDeclare with empty name should succeed")
	assert.NotEmpty(t, q.Name, "Server should generate a queue name")
	t.Logf("Server generated queue name: %s", q.Name)
}

func TestQueueDeclare_ServerGeneratedName_PassiveError(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	_, err = ch.QueueDeclarePassive("", false, false, false, false, nil)
	require.Error(t, err, "Passive QueueDeclare with empty name should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.AccessRefused, amqpErr.Code, "AMQP error code should be 403 (ACCESS_REFUSED)")
}

func TestQueueDeclare_Passive_NotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	_, err = ch.QueueDeclarePassive("non-existent-q-passive-nf", false, false, false, false, nil)
	require.Error(t, err, "Passive declare of non-existent queue should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "AMQP error code should be 404 (NOT_FOUND)")
}

func TestQueueDeclare_Passive_ExclusiveError(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn1, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn1.Close()
	ch1, err := conn1.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	queueName := "my-exclusive-q-passive-test"
	_, err = ch1.QueueDeclare(queueName, false, true, true, false, nil)
	require.NoError(t, err, "Declaring exclusive queue on conn1/ch1 failed")

	conn2, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Failed to establish second connection")
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err, "Failed to open second channel")
	defer ch2.Close()

	_, err = ch2.QueueDeclarePassive(queueName, false, true, true, false, nil)
	require.Error(t, err, "Passive declare of an exclusive queue from another connection should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Error was not an amqp.Error")
	assert.Equal(t, amqp.ResourceLocked, amqpErr.Code, "AMQP error code should be 405 (RESOURCE_LOCKED)")
}

func TestQueueDeclare_Active_RedeclareDifferentProperties(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := "q-redec-props-active"
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	require.NoError(t, err, "Initial queue declaration failed")

	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.Error(t, err, "Redeclare queue with different 'durable' property should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "AMQP error code should be 406 (PRECONDITION_FAILED)")
}

func TestQueueDeclare_Active_RedeclareExclusiveMismatch(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := "q-redec-exclusive-active"
	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Initial queue declaration failed")

	_, err = ch.QueueDeclare(queueName, false, true, false, false, nil)
	require.Error(t, err, "Redeclare queue with different 'exclusive' property should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "AMQP error code should be 406 (PRECONDITION_FAILED)")
}

func TestQueueDeclare_NoWait(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := "q-nowait-declare"
	_, err = ch.QueueDeclare(queueName, false, false, false, true, nil)
	require.NoError(t, err, "QueueDeclare with noWait should not return an error on valid declaration")

	_, err = ch.QueueDeclarePassive(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Passive declare of noWait-declared queue should succeed")
}

// --- Queue.Bind Tests ---

func TestQueueBind_QueueNotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "bind-test-ex-qnf"
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err, "Exchange declaration failed")

	err = ch.QueueBind("non-existent-q-for-bind", "key", exchangeName, false, nil)
	require.Error(t, err, "QueueBind with non-existent queue should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "AMQP error code should be 404 (NOT_FOUND)")
}

func TestQueueBind_ExchangeNotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := "bind-test-q-enf"
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Queue declaration failed")

	err = ch.QueueBind(q.Name, "key", "non-existent-ex-for-bind", false, nil)
	require.Error(t, err, "QueueBind with non-existent exchange should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "AMQP error code should be 404 (NOT_FOUND)")
}

func TestQueueBind_NoWait(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "bind-nowait-ex2"
	queueName := "bind-nowait-q2"
	routingKey := "bind-key-nw"

	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	err = ch.QueueBind(q.Name, routingKey, exchangeName, true, nil)
	require.NoError(t, err, "QueueBind with noWait should not return an error")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err, "Consume failed")
	time.Sleep(50 * time.Millisecond)

	body := "message for noWait bind test"
	err = ch.Publish(exchangeName, routingKey, false, false, amqp.Publishing{Body: []byte(body)})
	require.NoError(t, err, "Publish failed")

	select {
	case d := <-msgs:
		assert.Equal(t, body, string(d.Body), "Message content mismatch after noWait bind")
	case <-time.After(300 * time.Millisecond):
		assert.Fail(t, "Timeout waiting for message after noWait bind")
	}
}

// --- Basic.Publish Tests ---

func TestBasicPublish_ExchangeNotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	t.Log("Testing publish to non-existent exchange...")

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)

	err = ch.Publish("non-existent-ex-for-publish", "key", false, false, amqp.Publishing{Body: []byte("test")})

	var amqpErr *amqp.Error
	if err != nil {
		var ok bool
		amqpErr, ok = err.(*amqp.Error)
		if !ok {
			assert.Contains(t, err.Error(), "channel/connection is not open", "Error should indicate channel closed if not AMQP error")
		}
	} else {
		_, errAfter := ch.QueueDeclare("probeq-after-publish-fail", false, false, false, false, nil)
		require.Error(t, errAfter, "Subsequent operation should fail if channel was closed")
		var ok bool
		amqpErr, ok = errAfter.(*amqp.Error)
		if !ok {
			assert.Contains(t, errAfter.Error(), "channel/connection is not open", "Error should indicate channel closed if not AMQP error")
		}
	}

	if amqpErr != nil {
		assert.Equal(t, amqp.NotFound, amqpErr.Code, "AMQP error code should be 404 (NOT_FOUND) from server")
	} else {
		t.Log("Publish to non-existent exchange resulted in client-detected channel closure.")
	}

	time.Sleep(200 * time.Millisecond) // Allow time for server to process the error

	// Instead of expecting an error from ch.Close(), verify the channel is actually closed
	// by attempting another operation
	err = ch.ExchangeDeclare("test-exchange", "direct", false, false, false, false, nil)
	if err != nil {
		// Channel is closed, which is what we expect
		assert.Contains(t, err.Error(), "channel/connection is not open", "Channel should be closed after server sent Channel.Close")
	} else {
		t.Error("Channel operations should fail after server-initiated close")
	}
}

// --- Basic.Consume Tests ---

func TestBasicConsume_QueueNotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	_, err = ch.Consume("non-existent-q-for-consume", "", false, false, false, false, nil)
	require.Error(t, err, "Consume from non-existent queue should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "AMQP error code should be 404 (NOT_FOUND)")
}

func TestBasicConsume_ConsumerTagInUseOnChannel(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := "q-consumetag-channel-test"
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Queue declaration failed")

	consumerTag := "my-custom-tag-channel"
	_, err = ch.Consume(q.Name, consumerTag, false, false, false, false, nil)
	require.NoError(t, err, "First consume with custom tag should succeed")

	_, err = ch.Consume(q.Name, consumerTag, false, false, false, false, nil)
	require.Error(t, err, "Consume with already used tag on channel should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotAllowed, amqpErr.Code, "AMQP error code should be 530 (NOT_ALLOWED)")
}

func TestBasicConsume_Exclusive_ResourceLocked(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn1, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn1.Close()
	ch1, err := conn1.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	queueName := "q-exclusive-consume-rltest"
	q, err := ch1.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Queue declaration on conn1/ch1 failed")

	// First consumer (non-exclusive)
	_, err = ch1.Consume(q.Name, "consumer1-rl", true, false, false, false, nil)
	require.NoError(t, err, "First consumer (non-exclusive) failed")

	conn2, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Second consumer trying to be exclusive
	// Parameters: queue, consumer, autoAck, exclusive, noLocal, noWait, args
	_, err = ch2.Consume(q.Name, "consumer2-exclusive-rl", false, true, false, false, nil)
	//                                                       ^^^^^ ^^^^ - autoAck=false, exclusive=true
	require.Error(t, err, "Exclusive consume on queue with existing non-exclusive consumer should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.ResourceLocked, amqpErr.Code, "AMQP error code should be 405 (RESOURCE_LOCKED)")
}

func TestBasicConsume_NoWait(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := "q-consume-nowait-test"
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Queue declaration failed")

	// For noWait consume, the client library's Consume method still returns a channel (<-chan amqp.Delivery),
	// but no ConsumeOk is waited for from the server.
	// The consumerTag can be provided or left empty for server generation.
	consumerTag := "consumer-nowait-tag"
	_, err = ch.Consume(
		q.Name,      // queue
		consumerTag, // consumer
		true,        // autoAck (noAck)
		false,       // exclusive
		false,       // noLocal
		true,        // noWait
		nil,         // args
	)
	require.NoError(t, err, "Consume with noWait should not return an error if arguments are valid")

	// Indirect verification: Publish a message.
	// If the server processed the noWait Consume correctly, the message should be routed.
	// However, since the client doesn't get a ConsumeOk, it doesn't "know" for sure the consumer is active
	// from the server's perspective immediately.
	// This test primarily ensures the noWait call itself is accepted by the server.
	body := "message for noWait consume (indirect verification)"
	err = ch.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte(body)})
	require.NoError(t, err, "Publish failed for noWait consume test")

	// We cannot reliably receive the message here because the 'msgs' channel from a noWait Consume
	// might not be set up in time or behave as expected without ConsumeOk.
	// The primary check is that the Consume call with noWait=true didn't cause an immediate protocol error.
	t.Log("NOTE: Basic.Consume with noWait=true called. Server acceptance verified by lack of error. Message delivery not directly asserted in this specific noWait test.")
}

// --- Default Exchange Routing Test ---
func TestDefaultExchangeRouting(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := "my-direct-q-default-ex"
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Should declare queue successfully")

	err = ch.QueueBind(q.Name, q.Name, "", false, nil)
	require.NoError(t, err, "Should bind queue to default exchange with queue name as key")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err, "Should consume from queue")
	time.Sleep(100 * time.Millisecond)

	body := "Message for default exchange, routed by queue name"
	err = ch.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte(body)})
	require.NoError(t, err, "Should publish to default exchange")

	select {
	case d := <-msgs:
		assert.Equal(t, body, string(d.Body), "Message should be routed via default exchange")
	case <-time.After(300 * time.Millisecond):
		assert.Fail(t, "Timeout waiting for message from default exchange")
	}
}

// --- Publish/Consume ---
func TestServerPublishConsume(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Should connect successfully")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Should open channel successfully")
	defer ch.Close()

	exchangeName := "test-exchange-pubcons"
	queueName := "test-queue-pubcons"
	routingKey := "test-key-pubcons"

	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err, "Should declare exchange successfully")

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Should declare queue successfully")

	err = ch.QueueBind(q.Name, routingKey, exchangeName, false, nil)
	require.NoError(t, err, "Should bind queue successfully")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err, "Should consume successfully")
	time.Sleep(50 * time.Millisecond)

	body := "Hello, PubCons!"
	err = ch.Publish(exchangeName, routingKey, false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte(body)},
	)
	require.NoError(t, err, "Should publish message successfully")

	select {
	case msg := <-msgs:
		assert.Equal(t, body, string(msg.Body), "Message content should match")
	case <-time.After(2 * time.Second):
		assert.Fail(t, "Timeout waiting for message in PubCons test")
	}
}

func TestTopicMatching_Comprehensive(t *testing.T) {
	testCases := []struct {
		name        string
		pattern     string
		routingKey  string
		shouldMatch bool
	}{
		// Exact Matches
		{"ExactMatch_Simple", "stock.usd.nyse", "stock.usd.nyse", true},
		{"ExactMatch_SingleWord", "logs", "logs", true},
		{"ExactMismatch_Simple", "stock.usd.nyse", "stock.eur.nyse", false},
		{"ExactMismatch_Length_PatternShorter", "stock.usd", "stock.usd.nyse", false}, // Corrected based on previous failure
		{"ExactMismatch_Length_KeyShorter", "stock.usd.nyse", "stock.usd", false},

		// Star (*) Wildcard
		{"Star_MatchMiddle", "stock.*.nyse", "stock.usd.nyse", true},
		{"Star_MatchEnd", "stock.usd.*", "stock.usd.nyse", true},
		{"Star_MatchStart", "*.usd.nyse", "stock.usd.nyse", true},
		{"Star_Multiple", "*.usd.*", "stock.usd.nyse", true},
		{"Star_MismatchMiddle", "stock.*.nyse", "stock.eur.nasdaq", false},
		{"Star_MismatchTooFewPartsForStar_PatternNeeds3_KeyHas2", "stock.*.nyse", "stock.nyse", false},
		{"Star_MismatchTooManyPartsForStar_PatternNeeds3_KeyHas4", "stock.*.nyse", "stock.usd.nyse.fast", false}, // Corrected
		{"Star_OnlyStar_MatchOneWord", "*", "word", true},
		{"Star_OnlyStar_MismatchTwoWords", "*", "word.another", false}, // Corrected
		{"Star_OnlyStar_MismatchEmptyWordFromSplit", "*", "", false},   // routingKey="" -> routingParts=[]
		{"Star_AdjacentStars", "a.*.*.d", "a.b.c.d", true},
		{"Star_AdjacentStars_MismatchNotEnoughParts", "a.*.*.d", "a.b.d", false},

		// Hash (#) Wildcard
		{"Hash_MatchEnd_ZeroWords_AfterDot", "stock.usd.#", "stock.usd", true},
		{"Hash_MatchEnd_OneWord_AfterDot", "stock.usd.#", "stock.usd.nyse", true},
		{"Hash_MatchEnd_MultipleWords_AfterDot", "stock.usd.#", "stock.usd.nyse.fast.quotes", true},
		{"Hash_MatchStart_ZeroWords_BeforeDot", "#.nyse", "nyse", true},
		{"Hash_MatchStart_OneWord_BeforeDot", "#.usd.nyse", "stock.usd.nyse", true},
		{"Hash_MatchStart_MultipleWords_BeforeDot", "#.nyse.fast.quotes", "stock.usd.nyse.fast.quotes", true},
		{"Hash_MatchOnlyHash_AnyWords", "#", "anything.goes.here", true},
		{"Hash_MatchOnlyHash_SingleWord", "#", "word", true},
		{"Hash_MatchOnlyHash_EmptyRoutingKey", "#", "", true},
		{"Hash_NoMatch_HashRequiresPrefixMatch", "stock.#", "other.usd", false},
		{"Hash_NoMatch_HashRequiresSuffixMatch", "#.quotes", "stock.usd.nyse", false},
		{"Hash_PatternIsJustWord_KeyIsEmpty", "word", "", false},

		// Hash (#) in the Middle
		{"Hash_Middle_MultipleWords", "a.#.d", "a.b.c.d", true},
		{"Hash_Middle_OneWord", "a.#.d", "a.b.d", true},
		{"Hash_Middle_ZeroWords", "a.#.d", "a.d", true},
		{"Hash_Middle_NoMatchPrefix", "a.#.d", "x.b.c.d", false},
		{"Hash_Middle_NoMatchSuffix", "a.#.d", "a.b.c.y", false},
		{"Hash_Middle_NoMatchTooShort_KeyIsAPrefix", "a.#.d", "a", false},
		{"Hash_Middle_NoMatchTooShort_KeyIsEmpty", "a.#.d", "", false},

		// Combinations of Star and Hash
		{"Combo_StarHash_KeyHasMoreForHash", "*.usd.#", "stock.usd.nyse.fast", true},
		{"Combo_StarHash_HashMatchesZero", "*.usd.#", "stock.usd", true},
		{"Combo_StarHash_PatternMoreSpecific", "stock.*.#", "stock.usd.nyse", true},
		{"Combo_StarHash_PatternMoreSpecific_HashMatchesZero", "stock.*.#", "stock.usd", true},
		{"Combo_HashStar_HashMatchesMultiple", "#.usd.*", "stock.eur.usd.nyse", true},
		{"Combo_HashStar_HashMatchesOne", "#.usd.*", "stock.usd.nyse", true},
		{"Combo_HashStar_HashMatchesZero", "#.usd.*", "usd.nyse", true},
		{"Combo_Complex_HashStarHash", "a.#.c.*.e.#.g", "a.b.c.d.e.f.g", true},
		{"Combo_Complex_HashStarHash_Zeroes", "a.#.c.*.e.#.g", "a.c.d.e.g", true},
		{"Combo_StarHashStar", "*.*.#", "a.b.c.d", true},
		{"Combo_StarHashStar_HashMatchesZero", "*.*.#", "a.b", true},
		{"Combo_NoMatch_StarHash_StarFails", "*.eur.#", "stock.usd.nyse", false},

		// Edge Cases with Empty Parts due to Dots
		{"Edge_EmptyPattern_EmptyKey", "", "", true},
		{"Edge_EmptyPattern_NonEmptyKey", "", "a.b", false},
		{"Edge_NonEmptyPattern_EmptyKey", "a.b", "", false},
		{"Edge_PatternWithDot_KeyWithout", "a.b", "ab", false},

		{"Edge_KeyEndsWithDot_PatternShorter", "a.b", "a.b.", false}, // Corrected
		{"Edge_PatternEndsWithDot_KeyMatchesExactly", "a.b.", "a.b.", true},
		{"Edge_PatternEndsWithDot_KeyShorter", "a.b.", "a.b", false},

		{"Edge_PatternStartsWithDot_Star_NoMatch", ".*.b", "a.b", false},
		{"Edge_PatternStartsWithDot_Star_Match", ".*.b", ".a.b", true},

		{"Edge_PatternStartsWithDot_Hash_NoMatch", ".#.b", "a.b", false},
		{"Edge_PatternStartsWithDot_Hash_Match", ".#.b", ".any.words.b", true},

		{"Edge_PatternMultipleEmptyParts_Star_Match", "a.*.b", "a..b", true},
		{"Edge_PatternMultipleEmptyParts_Star_Mismatch", "a.*.c", "a..b", false},

		{"Edge_PatternMultipleDots_Star_ExplicitEmptyInPattern", "a..*.b", "a..c.b", true},
		{"Edge_PatternMultipleDots_Hash_ExplicitEmptyInPattern", "a..#.b", "a..anything.here.b", true},

		// Specific cases from your existing tests (re-verified)
		{"Existing_logs.#_vs_logs", "logs.#", "logs", true},
		{"Existing_*.#.error_vs_app.module.submodule.error", "*.#.error", "app.module.submodule.error", true},
		{"Existing_*.#.error_vs_app.error", "*.#.error", "app.error", true},

		// More complex cases
		{"Complex_1", "data.asset.*.compute.#.result", "data.asset.id123.compute.analysis.final.result", true},
		{"Complex_1_NoMatch", "data.asset.*.compute.#.result", "data.asset.id123.compute.analysis.final.wrong", false},
		{"Complex_2_HashAtStart", "#.compute.asset.*.result", "raw.transformed.compute.asset.id456.result", true},
		{"Complex_2_HashAtStart_NoMatch_MiddlePartFails", "#.compute.asset.*.result", "raw.transformed.asset.id456.result", false},
		{"Complex_HashAndStarInterleaved", "v1.*.events.#.processed.*", "v1.user.events.login.success.processed.byAgentX", true},
		{"Complex_HashAndStarInterleaved_NoMatch_MissingLastStarPart", "v1.*.events.#.processed.*", "v1.user.events.login.success.processed", false},

		// --- NEW Deeper Edge Cases with Dots and Empty Segments ---
		{"Edge_DotOnly_Pattern_DotOnly_Key", ".", ".", true},       // P["",""], K["",""]
		{"Edge_DotOnly_Pattern_EmptyKey", ".", "", false},          // P["",""], K[]
		{"Edge_EmptyPattern_DotOnly_Key", "", ".", false},          // P[] (or [""] if not handled by `pattern==""` check), K["",""]
		{"Edge_DoubleDot_Pattern_DoubleDot_Key", "..", "..", true}, // P["","",""], K["","",""]
		{"Edge_Pattern_a_dot_Key_a", "a.", "a", false},             // P["a",""], K["a"]
		{"Edge_Pattern_dot_a_Key_a", ".a", "a", false},             // P["","a"], K["a"]
		{"Edge_Pattern_a_Key_a_dot", "a", "a.", false},             // P["a"], K["a",""]
		{"Edge_Pattern_a_Key_dot_a", "a", ".a", false},             // P["a"], K["","a"]

		// --- Wildcards with Leading/Trailing/Multiple Dots ---
		{"Edge_Star_With_LeadingDotPattern_MatchingKey", ".*", ".a", true},                           // P["","*"], K["","a"] -> * matches "a"
		{"Edge_Star_With_LeadingDotPattern_NonMatchingKey", ".*", "a", false},                        // P["","*"], K["a"] -> "" != "a"
		{"Edge_Star_With_TrailingDotPattern_MatchingKey", "*.", "a.", true},                          // P["*",""], K["a",""] -> * matches "a"
		{"Edge_Star_With_TrailingDotPattern_NonMatchingKey", "*.", "a", false},                       // P["*",""], K["a"] -> pattern has trailing "" part, key does not.
		{"Edge_Star_With_DoubleDotPattern_Key_a_empty_b", "*..*", "a..b", true},                      // P["*","","*"], K["a","","b"] -> 1st * matches a, 2nd * matches b
		{"Edge_Star_With_DoubleDotPattern_Key_empty_empty_empty_IterativeMatch", "*..*", "..", true}, // P["*","","*"], K["","",""] -> * matches "", then "" literal, then * matches "".

		{"Edge_Hash_With_LeadingDotPattern_MatchingKey", ".#", ".a.b", true},          // P["","#"], K["","a","b"] -> # matches "a.b"
		{"Edge_Hash_With_LeadingDotPattern_MatchingEmptySuffix", ".#", ".", true},     // P["","#"], K["",""] -> # matches ""
		{"Edge_Hash_With_LeadingDotPattern_NonMatchingKey", ".#", "a.b", false},       // P["","#"], K["a","b"] -> "" != "a"
		{"Edge_Hash_With_TrailingDotPattern_MatchingKey", "#.", "a.b.", true},         // P["#",""], K["a","b",""] -> # matches "a.b"
		{"Edge_Hash_With_TrailingDotPattern_MatchingEmptyPrefix", "#.", ".", true},    // P["#",""], K["",""] -> # matches ""
		{"Edge_Hash_With_TrailingDotPattern_NonMatchingKey", "#.", "a.b", false},      // P["#",""], K["a","b"] -> pattern has trailing "" part, key does not.
		{"Edge_Hash_With_DoubleDotPattern_Key_a_empty_b", "#..#", "a..b", true},       // P["#","","#"], K["a","","b"] -> 1st # matches a, 2nd # matches b
		{"Edge_Hash_With_DoubleDotPattern_Key_empty_empty_empty", "#..#", "..", true}, // P["#","","#"], K["","",""] -> #s match empty parts

		// --- Patterns consisting only of wildcards or mixed with dots ---
		{"Edge_StarStar_Pattern_Key_ab", "*.*", "a.b", true},
		{"Edge_StarStar_Pattern_Key_a_empty", "*.*", "a.", true}, // P["*","*"], K["a",""]
		{"Edge_StarStar_Pattern_Key_empty_b", "*.*", ".b", true}, // P["*","*"], K["","b"]
		{"Edge_StarStar_Pattern_Key_dot", "*.*", ".", true},      // P["*","*"], K["",""]
		{"Edge_StarStar_Pattern_Key_a", "*.*", "a", false},       // Needs two words
		{"Edge_StarStar_Pattern_Key_empty", "*.*", "", false},    // Needs two words

		{"Edge_HashHash_Pattern_Key_ab", "#.#", "a.b", true},
		{"Edge_HashHash_Pattern_Key_a_empty", "#.#", "a.", true}, // P["#","#"], K["a",""]
		{"Edge_HashHash_Pattern_Key_empty_b", "#.#", ".b", true}, // P["#","#"], K["","b"]
		{"Edge_HashHash_Pattern_Key_dot", "#.#", ".", true},      // P["#","#"], K["",""]
		{"Edge_HashHash_Pattern_Key_a", "#.#", "a", true},        // First # matches "a", second # matches empty
		{"Edge_HashHash_Pattern_Key_empty", "#.#", "", true},     // Both # match empty

		{"Edge_StarHash_Pattern_Key_a", "*.#", "a", true},            // * matches "a", # matches empty
		{"Edge_StarHash_Pattern_Key_ab", "*.#", "a.b", true},         // * matches "a", # matches "b"
		{"Edge_StarHash_Pattern_Key_a_empty_c", "*.#", "a..c", true}, // * matches "a", # matches ".c" (empty string and c)
		{"Edge_StarHash_Pattern_Key_empty", "*.#", "", false},        // * needs one word

		{"Edge_HashStar_Pattern_Key_a", "#.*", "a", true},            // # matches empty, * matches "a"
		{"Edge_HashStar_Pattern_Key_ab", "#.*", "a.b", true},         // # matches "a", * matches "b"
		{"Edge_HashStar_Pattern_Key_a_empty_c", "#.*", "a..c", true}, // # matches "a.", * matches "c" (a then empty, then c)
		{"Edge_HashStar_Pattern_Key_empty", "#.*", "", false},        // * needs one word

		// --- More complex interactions ---
		{"Edge_ComplexDotsAndStar_Pattern1", "a.*.b.*.c", "a..b..c", true}, // P[a,*,b,*,c], K[a,"",b,"",c] -> *s match ""
		{"Edge_ComplexDotsAndHash_Pattern1", "a.#.b.#.c", "a..b..c", true}, // P[a,#,b,#,c], K[a,"",b,"",c] -> #s match ""
		{"Edge_ComplexDotsAndHash_Pattern2", "a.#.b.#.c", "a.x.y.b.z.c", true},
		{"Edge_ComplexDotsAndHash_Pattern3", "a.#.b.#.c", "a.b.c", true}, // Inner #s match zero words

		{"Edge_Key_Is_SingleDot_Pattern_a", "a", ".", false},
		{"Edge_Pattern_Is_SingleDot_Key_Is_Word", ".", "a", false},
		{"Edge_Pattern_Is_Star_Key_Is_Single_EmptySegment_From_Split_Dot", "*", ".", false}, // // * (1 segment) != . (2 empty segments)
		// Corrected expectation: false
		{"Edge_Pattern_Is_Hash_Key_Is_SingleDot", "#", ".", true}, // P["#"], K["",""] -> # matches anything

		// Recheck previously failing tests with corrected expectations
		{"Recheck_ExactMismatch_Length_PatternShorter", "stock.usd", "stock.usd.nyse", false},
		{"Recheck_Star_MismatchTooManyPartsForStar", "stock.*.nyse", "stock.usd.nyse.fast", false},
		{"Recheck_Star_OnlyStar_MismatchTwoWords", "*", "word.another", false},
		{"Recheck_Edge_KeyEndsWithDot_PatternShorter", "a.b", "a.b.", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var routingKeyParts []string
			if tc.routingKey == "" {
				routingKeyParts = []string{}
			} else {
				routingKeyParts = strings.Split(tc.routingKey, ".")
			}

			// The iterative topicMatch function handles splitting pattern internally
			actualMatch := topicMatch(tc.pattern, tc.routingKey) // Use the entry topicMatch
			assert.Equal(t, tc.shouldMatch, actualMatch, "Pattern: '%s', RoutingKey: '%s' (KeyParts for debug: %v)", tc.pattern, tc.routingKey, routingKeyParts)
		})
	}
}

// --- Basic.Return Tests ---

// Helper function to setup a channel and a return notification channel
func setupChannelWithReturn(t *testing.T, conn *amqp.Connection) (*amqp.Channel, <-chan amqp.Return) {
	t.Helper()
	ch, err := conn.Channel()
	require.NoError(t, err, "Should open channel successfully")

	returnNotify := make(chan amqp.Return, 1) // Buffered to prevent blocking publisher
	ch.NotifyReturn(returnNotify)

	return ch, returnNotify
}

// Helper to assert common amqp.Return fields for NO_ROUTE
func assertReturnNoRoute(t *testing.T, ret amqp.Return, expectedExchange, expectedRoutingKey string) {
	t.Helper()
	assert.Equal(t, uint16(amqp.NoRoute), ret.ReplyCode, "Return ReplyCode should be 312 (NO_ROUTE)")
	assert.Equal(t, "NO_ROUTE", ret.ReplyText, "Return ReplyText should be NO_ROUTE")
	assert.Equal(t, expectedExchange, ret.Exchange, "Return Exchange name mismatch")
	assert.Equal(t, expectedRoutingKey, ret.RoutingKey, "Return RoutingKey mismatch")
}

func TestBasicReturn_Mandatory_NoRoute_DefaultExchange(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, returnNotify := setupChannelWithReturn(t, conn)
	defer ch.Close()

	routingKey := "non_existent_queue_for_default_return"
	body := "message for default exchange return"
	messageId := "default-return-msg-id"

	err = ch.Publish(
		"",         // Default exchange
		routingKey, // Routing key (non-existent queue)
		true,       // Mandatory
		false,      // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			MessageId:   messageId,
			Body:        []byte(body),
		},
	)
	require.NoError(t, err, "Publish call should succeed")

	select {
	case ret := <-returnNotify:
		assertReturnNoRoute(t, ret, "", routingKey)
		assert.Equal(t, "text/plain", ret.ContentType, "Returned ContentType mismatch")
		assert.Equal(t, messageId, ret.MessageId, "Returned MessageId mismatch")
		assert.Equal(t, []byte(body), ret.Body, "Returned Body mismatch")
	case <-time.After(2 * time.Second):
		assert.Fail(t, "Timeout waiting for basic.return for default exchange")
	}
}

func TestBasicReturn_Mandatory_NoRoute_DirectExchange(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, returnNotify := setupChannelWithReturn(t, conn)
	defer ch.Close()

	exchangeName := "direct_exchange_for_return"
	routingKey := "unbound_key_for_direct_return"
	body := "message for direct exchange return"
	correlationId := "direct-return-corr-id"

	err = ch.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare direct exchange")

	err = ch.Publish(
		exchangeName,
		routingKey,
		true,  // Mandatory
		false, // Immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: correlationId,
			Body:          []byte(body),
		},
	)
	require.NoError(t, err, "Publish call should succeed")

	select {
	case ret := <-returnNotify:
		assertReturnNoRoute(t, ret, exchangeName, routingKey)
		assert.Equal(t, "application/json", ret.ContentType, "Returned ContentType mismatch")
		assert.Equal(t, correlationId, ret.CorrelationId, "Returned CorrelationId mismatch")
		assert.Equal(t, []byte(body), ret.Body, "Returned Body mismatch")
	case <-time.After(2 * time.Second):
		assert.Fail(t, "Timeout waiting for basic.return for direct exchange")
	}
}

func TestBasicReturn_Mandatory_NoRoute_TopicExchange(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, returnNotify := setupChannelWithReturn(t, conn)
	defer ch.Close()

	exchangeName := "topic_exchange_for_return"
	routingKey := "topic.key.unmatched"
	body := "message for topic exchange return"
	appId := "topic-return-app-id"

	err = ch.ExchangeDeclare(exchangeName, amqp.ExchangeTopic, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare topic exchange")

	// Declare a queue and bind it with a non-matching pattern to ensure the target key has no route
	_, err = ch.QueueDeclare("some_other_topic_queue", false, false, false, false, nil)
	require.NoError(t, err)
	err = ch.QueueBind("some_other_topic_queue", "topic.key.other.*", exchangeName, false, nil)
	require.NoError(t, err)

	err = ch.Publish(
		exchangeName,
		routingKey,
		true,  // Mandatory
		false, // Immediate
		amqp.Publishing{
			AppId: appId,
			Body:  []byte(body),
		},
	)
	require.NoError(t, err, "Publish call should succeed")

	select {
	case ret := <-returnNotify:
		assertReturnNoRoute(t, ret, exchangeName, routingKey)
		assert.Equal(t, appId, ret.AppId, "Returned AppId mismatch")
		assert.Equal(t, []byte(body), ret.Body, "Returned Body mismatch")
	case <-time.After(2 * time.Second):
		assert.Fail(t, "Timeout waiting for basic.return for topic exchange")
	}
}

func TestBasicReturn_Mandatory_RouteExists_NoReturn(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, returnNotify := setupChannelWithReturn(t, conn)
	defer ch.Close()

	exchangeName := "direct_exchange_route_exists"
	queueName := "queue_for_route_exists"
	routingKey := "key_for_route_exists"
	body := "message that should be routed, not returned"

	err = ch.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, false, false, false, false, nil)
	require.NoError(t, err)
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)
	err = ch.QueueBind(q.Name, routingKey, exchangeName, false, nil)
	require.NoError(t, err)

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	err = ch.Publish(
		exchangeName,
		routingKey,
		true,  // Mandatory
		false, // Immediate
		amqp.Publishing{Body: []byte(body)},
	)
	require.NoError(t, err, "Publish call should succeed")

	// Check if message is consumed (meaning it was routed)
	select {
	case consumedMsg := <-msgs:
		assert.Equal(t, []byte(body), consumedMsg.Body, "Consumed message body mismatch")
	case <-time.After(2 * time.Second):
		assert.Fail(t, "Timeout waiting for message to be consumed")
	}

	// Check that no message was returned
	select {
	case ret := <-returnNotify:
		assert.Fail(t, "basic.return received unexpectedly", "Return data: %+v", ret)
	case <-time.After(200 * time.Millisecond):
		// Expected: no return
	}
}

func TestBasicReturn_NotMandatory_NoRoute_NoReturn(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, returnNotify := setupChannelWithReturn(t, conn)
	defer ch.Close()

	exchangeName := "direct_exchange_not_mandatory"
	routingKey := "unbound_key_not_mandatory"
	body := "message not mandatory, no route"

	err = ch.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare direct exchange")

	err = ch.Publish(
		exchangeName,
		routingKey,
		false, // Mandatory = false
		false, // Immediate
		amqp.Publishing{Body: []byte(body)},
	)
	require.NoError(t, err, "Publish call should succeed")

	// Check that no message was returned
	select {
	case ret := <-returnNotify:
		assert.Fail(t, "basic.return received unexpectedly for non-mandatory message", "Return data: %+v", ret)
	case <-time.After(500 * time.Millisecond): // Longer timeout to be reasonably sure
		// Expected: no return, message is silently dropped
	}
}

func TestBasicPublish_Immediate_Rejected(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	// No defer ch.Close() here, as we expect the channel to be closed by the server.

	exchangeName := "exchange_for_immediate_test"
	routingKey := "key_for_immediate"

	err = ch.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil { // If channel is already closed due to setup, this might fail.
		require.Contains(t, err.Error(), "channel/connection is not open", "ExchangeDeclare failed unexpectedly before immediate publish")
		return
	}

	// Attempt to publish with immediate = true
	// The client library might not return an error directly from Publish if it's asynchronous.
	// The error will manifest when the server closes the channel.
	publishErr := ch.Publish(
		exchangeName,
		routingKey,
		false, // Mandatory
		true,  // Immediate = true
		amqp.Publishing{Body: []byte("test immediate")},
	)

	// We expect the server to close the channel.
	// Wait for a channel close notification or for a subsequent operation to fail.
	closedCh := make(chan *amqp.Error, 1)
	ch.NotifyClose(closedCh)

	select {
	case amqpErr := <-closedCh:
		require.NotNil(t, amqpErr, "Should receive a channel close error")
		assert.Equal(t, amqp.NotImplemented, amqpErr.Code, "AMQP error code should be 540 (NOT_IMPLEMENTED) for immediate flag")
		assert.Contains(t, amqpErr.Reason, "The 'immediate' flag is deprecated", "Error reason mismatch")
	case <-time.After(2 * time.Second):
		// If NotifyClose didn't fire, check if publishErr already indicated an issue
		// or try another operation.
		if publishErr != nil {
			amqpErr, ok := publishErr.(*amqp.Error)
			if ok {
				assert.Equal(t, amqp.NotImplemented, amqpErr.Code, "AMQP error code from Publish should be 540")
				return
			}
		}
		// Try another operation to confirm channel state
		errAfter := ch.ExchangeDeclare("another-ex", "direct", false, false, false, false, nil)
		require.Error(t, errAfter, "Channel should be closed after publishing with immediate=true")
		if amqpErr, ok := errAfter.(*amqp.Error); ok {
			assert.Equal(t, amqp.ChannelError, amqpErr.Code, "Subsequent operation should fail with ChannelError or similar")
		} else {
			assert.Contains(t, errAfter.Error(), "channel/connection is not open", "Error should indicate channel is closed")
		}
		assert.Fail(t, "Timeout waiting for channel close notification after publishing with immediate=true")
	}

	// Ensure publishErr itself isn't a success code if the channel was closed.
	// This depends on client library behavior; sometimes Publish is fire-and-forget.
	if publishErr == nil {
		t.Log("Publish call itself did not return an error, channel closure was detected asynchronously.")
	}
}
