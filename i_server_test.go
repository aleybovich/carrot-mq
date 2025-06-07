package carrotmq

import (
	"fmt"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestAuth_PlainMode_InvalidCredentials_SpecCompliant(t *testing.T) {
	// Server with authentication enabled
	addr, cleanup := setupTestServer(t, WithAuth(map[string]string{
		"guest": "guest123",
		"admin": "secretpass",
	}))
	defer cleanup()

	testCases := []struct {
		name     string
		username string
		password string
		desc     string
	}{
		{
			name:     "InvalidPassword",
			username: "guest",
			password: "wrongpass",
			desc:     "Should fail with invalid password",
		},
		{
			name:     "InvalidUsername",
			username: "unknownuser",
			password: "anypass",
			desc:     "Should fail with invalid username",
		},
		{
			name:     "EmptyCredentials",
			username: "",
			password: "",
			desc:     "Should fail with empty credentials",
		},
		{
			name:     "EmptyPassword",
			username: "guest",
			password: "",
			desc:     "Should fail with empty password",
		},
		{
			name:     "EmptyUsername",
			username: "",
			password: "guest123",
			desc:     "Should fail with empty username",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()

			// Build connection URL
			var connURL string
			if tc.username == "" && tc.password == "" {
				connURL = "amqp://" + addr
			} else {
				connURL = fmt.Sprintf("amqp://%s:%s@%s", tc.username, tc.password, addr)
			}

			// Try to connect
			conn, err := amqp.Dial(connURL)

			// Clean up if connection somehow succeeded
			if conn != nil {
				conn.Close()
			}

			// Connection should fail
			require.Error(t, err, tc.desc)

			// Measure elapsed time
			elapsed := time.Since(start)

			// Per spec 4.10.2, server should pause for 1 second before closing
			// Allow some margin for network/processing time
			const failedAuthThrottle = 1 * time.Second
			assert.GreaterOrEqual(t, elapsed, failedAuthThrottle,
				"Server should pause for at least ~1 second before closing connection on auth failure")
			assert.LessOrEqual(t, elapsed, failedAuthThrottle*2,
				"Server pause should not be much longer than 1 second")

			// The RabbitMQ Go client library may generate an AMQP error locally when
			// it detects the connection was closed during authentication. This is a
			// client-side behavior, not a protocol violation. The important thing is
			// that the server didn't send a Connection.Close frame.

			// Check if we got an AMQP error
			amqpErr, isAMQPError := err.(*amqp.Error)

			if isAMQPError {
				// If the client generated an AMQP error, it should be an authentication error
				// The RabbitMQ Go client uses code 403 (ACCESS_REFUSED) for auth failures
				assert.Equal(t, amqp.AccessRefused, amqpErr.Code,
					"AMQP error code should be 403 (ACCESS_REFUSED) for authentication failure")

				// The error message should indicate authentication failure
				assert.True(t,
					strings.Contains(amqpErr.Reason, "username") ||
						strings.Contains(amqpErr.Reason, "password") ||
						strings.Contains(amqpErr.Reason, "auth"),
					"AMQP error should indicate authentication failure, got: %s", amqpErr.Reason)
			} else {
				// If not an AMQP error, it should be a raw connection error
				errStr := err.Error()
				assert.True(t,
					strings.Contains(errStr, "EOF") ||
						strings.Contains(errStr, "connection reset") ||
						strings.Contains(errStr, "closed") ||
						strings.Contains(errStr, "broken pipe"),
					"Non-AMQP error should indicate connection was closed/reset/EOF, got: %s", errStr)
			}

			// Log the actual error for debugging
			t.Logf("Authentication failure resulted in error: %v (type: %T)", err, err)
		})
	}
}

func TestAuth_PlainMode_ValidCredentials_Timing(t *testing.T) {
	// Server with authentication enabled
	addr, cleanup := setupTestServer(t, WithAuth(map[string]string{
		"guest": "guest123",
		"admin": "secretpass",
	}))
	defer cleanup()

	testCases := []struct {
		name     string
		username string
		password string
	}{
		{
			name:     "ValidGuest",
			username: "guest",
			password: "guest123",
		},
		{
			name:     "ValidAdmin",
			username: "admin",
			password: "secretpass",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()

			conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s", tc.username, tc.password, addr))
			require.NoError(t, err, "Should connect successfully with valid credentials")
			defer conn.Close()

			elapsed := time.Since(start)

			// Valid credentials should connect quickly (no artificial delay)
			const failedAuthThrottle = 1 * time.Second
			assert.Less(t, elapsed, failedAuthThrottle,
				"Valid authentication should complete quickly without delay")

			// Verify connection is functional
			ch, err := conn.Channel()
			require.NoError(t, err, "Should open channel successfully")
			defer ch.Close()

			// Do a simple operation to verify connection is fully established
			q, err := ch.QueueDeclare("test-queue-"+tc.name, false, false, false, false, nil)
			require.NoError(t, err, "Should declare queue successfully")
			assert.NotEmpty(t, q.Name, "Queue should have a name")
		})
	}
}

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

	// Exchange declare with no-wait
	exchangeName := "test-nowait-ex"
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, true, nil)
	require.NoError(t, err, "No-wait declare should succeed without error")

	// Wait a bit for server to process
	time.Sleep(50 * time.Millisecond)

	// Verify exchange exists by declaring again (should succeed)
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err, "Exchange should exist after no-wait declare")
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

	_, err = ch.QueueDeclarePassive("non-existent-queue-passive-nf", false, false, false, false, nil)
	require.Error(t, err, "Passive declare of non-existent queue should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Error should be of type *amqp.Error")
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "AMQP error code should be 404 (NOT_FOUND)")
}

func TestQueueDeclare_Passive_TypeMismatch(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := "passive-type-mismatch-q"
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil) // durable=true
	require.NoError(t, err, "Initial queue declaration failed")

	_, err = ch.QueueDeclarePassive(queueName, false, false, false, false, nil) // durable=false
	require.Error(t, err, "Passive declare with property mismatch should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Error should be of type *amqp.Error")
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "AMQP error code should be 406 (PRECONDITION_FAILED)")
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

	queueName := "redeclare-prop-diff-q"
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil) // durable=true
	require.NoError(t, err, "Initial queue declaration failed")

	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil) // durable=false
	require.Error(t, err, "Redeclare queue with different 'durable' property should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Error should be of type *amqp.Error")
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "AMQP error code should be 406 (PRECONDITION_FAILED)")
}

func TestQueueDeclare_Active_RedeclareSameProperties(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := "redeclare-same-q"
	q, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	require.NoError(t, err, "First declare should succeed")

	q2, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	require.NoError(t, err, "Redeclare queue with same properties should succeed")
	assert.Equal(t, q.Name, q2.Name, "Queue names should match")
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

	// Queue declare with no-wait
	queueName := "test-nowait-q"
	q, err := ch.QueueDeclare(queueName, false, false, false, true, nil)
	require.NoError(t, err, "No-wait declare should succeed without error")
	// With no-wait, some fields might be empty
	assert.NotEmpty(t, q.Name, "Queue name should be returned even with no-wait")

	// Wait a bit for server to process
	time.Sleep(50 * time.Millisecond)

	// Verify queue exists by declaring again (should succeed)
	q2, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Queue should exist after no-wait declare")
	assert.Equal(t, queueName, q2.Name)
}

func TestQueueBind_Basic(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create exchange and queue
	exchangeName := "bind-test-ex"
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	q, err := ch.QueueDeclare("bind-test-q", false, false, false, false, nil)
	require.NoError(t, err)

	// Bind queue to exchange
	err = ch.QueueBind(q.Name, "routing-key", exchangeName, false, nil)
	require.NoError(t, err, "Queue bind should succeed")

	// Verify binding works by publishing and consuming
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	body := "test message"
	err = ch.Publish(exchangeName, "routing-key", false, false, amqp.Publishing{Body: []byte(body)})
	require.NoError(t, err)

	select {
	case msg := <-msgs:
		assert.Equal(t, body, string(msg.Body))
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for message")
	}
}

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
	require.NoError(t, err)

	// Try to bind non-existent queue
	err = ch.QueueBind("non-existent-queue", "key", exchangeName, false, nil)
	require.Error(t, err, "Bind non-existent queue should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "Should return 404 NOT_FOUND")
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

	q, err := ch.QueueDeclare("bind-test-q-enf", false, false, false, false, nil)
	require.NoError(t, err)

	// Try to bind to non-existent exchange
	err = ch.QueueBind(q.Name, "key", "non-existent-exchange", false, nil)
	require.Error(t, err, "Bind to non-existent exchange should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "Should return 404 NOT_FOUND")
}

func TestBasicConsume_QueueNotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	_, err = ch.Consume("non-existent-queue-consume", "", true, false, false, false, nil)
	require.Error(t, err, "Consume from non-existent queue should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code)
}

func TestBasicPublish_ExchangeNotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Set up channel close notification
	closeCh := make(chan *amqp.Error, 1)
	ch.NotifyClose(closeCh)

	// Publish to non-existent exchange - this won't return an error immediately
	err = ch.Publish("non-existent-exchange-pub", "key", false, false, amqp.Publishing{
		Body: []byte("test"),
	})
	// The publish itself doesn't return an error in AMQP
	require.NoError(t, err, "Publish operation itself should not fail")

	// But the channel should be closed by the server
	select {
	case amqpErr := <-closeCh:
		require.NotNil(t, amqpErr, "Should receive channel close error")
		assert.Equal(t, amqp.NotFound, amqpErr.Code, "Should receive NOT_FOUND error")
		assert.Contains(t, amqpErr.Reason, "non-existent-exchange-pub", "Error should mention the exchange name")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for channel close notification")
	}

	// Verify channel is closed by trying another operation
	err = ch.ExchangeDeclare("test-ex", "direct", false, false, false, false, nil)
	require.Error(t, err, "Channel should be closed after publishing to non-existent exchange")
}

func TestBasicPublish_MandatoryNoRoute(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Enable returns
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))

	// Create exchange with no bindings
	exchangeName := "mandatory-test-ex"
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	// Publish with mandatory flag
	body := "unroutable message"
	err = ch.Publish(exchangeName, "no-route-key", true, false, amqp.Publishing{
		Body: []byte(body),
	})
	require.NoError(t, err, "Publish with mandatory should succeed")

	// Should receive a return
	select {
	case ret := <-returns:
		assert.Equal(t, body, string(ret.Body))
		assert.Equal(t, uint16(amqp.NoRoute), ret.ReplyCode)
		assert.Contains(t, ret.ReplyText, "NO_ROUTE")
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Should receive return for unroutable mandatory message")
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
	defer ch.Close()

	// Create exchange and queue with binding
	exchangeName := "immediate-test-ex"
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	q, err := ch.QueueDeclare("immediate-test-q", false, false, false, false, nil)
	require.NoError(t, err)

	err = ch.QueueBind(q.Name, "test-key", exchangeName, false, nil)
	require.NoError(t, err)

	// Try to publish with immediate flag (no consumers)
	err = ch.Publish(exchangeName, "test-key", false, true, amqp.Publishing{
		Body: []byte("immediate message"),
	})

	// Most implementations reject immediate flag
	if err != nil {
		amqpErr, ok := err.(*amqp.Error)
		if ok {
			// Server might reject with NOT_IMPLEMENTED
			assert.Contains(t, []int{amqp.NotImplemented, amqp.NotAllowed}, amqpErr.Code,
				"Immediate flag might be rejected with NOT_IMPLEMENTED or similar")
		}
	}
}

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
	// This test tests the topic matching functionality using the client
	// It creates topic exchanges and verifies routing patterns
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "test-topic-comprehensive"
	err = ch.ExchangeDeclare(exchangeName, "topic", false, false, false, false, nil)
	require.NoError(t, err)

	// Test various topic patterns
	patterns := []struct {
		pattern    string
		routingKey string
		shouldGet  bool
	}{
		{"#", "anything", true},
		{"#", "anything.else", true},
		{"*", "oneword", true},
		{"*", "two.words", false},
		{"*.#", "one.two.three", true},
		{"#.end", "anything.end", true},
		{"#.end", "end", true},
		{"*.*.third", "first.second.third", true},
		{"*.*.third", "first.second.notthird", false},
		{"logs.#", "logs", true},
		{"logs.#", "logs.info", true},
		{"logs.#", "logs.info.verbose", true},
	}

	for i, test := range patterns {
		// Create unique queue for each test
		qName := fmt.Sprintf("test-topic-q-%d", i)
		q, err := ch.QueueDeclare(qName, false, false, false, false, nil)
		require.NoError(t, err)

		// Bind with pattern
		err = ch.QueueBind(q.Name, test.pattern, exchangeName, false, nil)
		require.NoError(t, err)

		// Publish message
		msgBody := fmt.Sprintf("Message for pattern %s", test.pattern)
		err = ch.Publish(exchangeName, test.routingKey, false, false, amqp.Publishing{
			Body: []byte(msgBody),
		})
		require.NoError(t, err)

		// Try to get message
		msg, ok, err := ch.Get(q.Name, true)
		require.NoError(t, err)

		if test.shouldGet {
			assert.True(t, ok, "Pattern '%s' should match routing key '%s'", test.pattern, test.routingKey)
			if ok {
				assert.Equal(t, msgBody, string(msg.Body))
			}
		} else {
			assert.False(t, ok, "Pattern '%s' should NOT match routing key '%s'", test.pattern, test.routingKey)
		}
	}
}

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
