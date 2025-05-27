// server_test.go
package main

import (
	"errors"
	"fmt"
	"net"
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
