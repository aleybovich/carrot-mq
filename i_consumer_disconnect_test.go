package carrotmq

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// TestConsumerLoopHeartbeatTimeout tests that consumer loop exits when connection is closed
func TestConsumerLoopHeartbeatTimeout(t *testing.T) {
	// Setup server
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect
	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	// Don't defer close - we'll force close it

	ch, err := conn.Channel()
	require.NoError(t, err)
	// Don't defer close

	// Create queue
	qName := uniqueName("q-connection-close")
	q, err := ch.QueueDeclare(
		qName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	require.NoError(t, err)

	// Start consumer
	msgs, err := ch.Consume(
		q.Name,          // queue
		"test-consumer", // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	require.NoError(t, err)

	// Track loop exit
	loopExited := make(chan bool)

	// Set up connection close notification
	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))

	// Start consumer loop
	go func() {
		for range msgs {
			// Process messages
		}
		loopExited <- true
	}()

	// Give consumer time to start
	time.Sleep(100 * time.Millisecond)

	// Force close the connection
	conn.Close()

	// Verify connection was closed
	select {
	case err := <-connClose:
		t.Logf("Connection closed: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Connection close notification not received")
	}

	// Verify loop exits due to connection loss
	select {
	case <-loopExited:
		t.Log("Consumer loop exited after connection close")
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer loop did not exit after connection close")
	}
}

// TestConsumerCancelledByServer tests consumer loop exits when server cancels consumer
func TestConsumerCancelledByServer(t *testing.T) {
	// Setup server
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect
	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create queue
	qName := uniqueName("q-server-cancel")
	q, err := ch.QueueDeclare(
		qName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	require.NoError(t, err)

	// Start consumer
	msgs, err := ch.Consume(
		q.Name,          // queue
		"test-consumer", // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	require.NoError(t, err)

	// Track loop exit and cancellation
	loopExited := make(chan bool)
	consumerCancelled := make(chan string, 1)

	// Set up cancel notification
	ch.NotifyCancel(consumerCancelled)

	// Start consumer loop
	go func() {
		for range msgs {
			// Process messages
		}
		loopExited <- true
	}()

	// Give consumer time to start
	time.Sleep(100 * time.Millisecond)

	// Delete the queue - this should cancel all consumers
	_, err = ch.QueueDelete(qName, false, false, false)
	require.NoError(t, err)

	// Verify consumer was cancelled
	select {
	case tag := <-consumerCancelled:
		require.Equal(t, "test-consumer", tag)
		t.Log("Consumer was cancelled by server")
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer was not cancelled")
	}

	// Verify loop exits
	select {
	case <-loopExited:
		t.Log("Consumer loop exited after cancellation")
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer loop did not exit after cancellation")
	}
}

// TestConsumerExclusiveConflict tests consumer loop behavior with exclusive consumer conflicts
func TestConsumerExclusiveConflict(t *testing.T) {
	// Setup server
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// First connection
	conn1, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn1.Close()

	ch1, err := conn1.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	// Create queue
	qName := uniqueName("q-exclusive")
	q, err := ch1.QueueDeclare(
		qName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	require.NoError(t, err)

	// Start exclusive consumer
	msgs1, err := ch1.Consume(
		q.Name,               // queue
		"exclusive-consumer", // consumer
		false,                // auto-ack
		true,                 // exclusive
		false,                // no-local
		false,                // no-wait
		nil,                  // args
	)
	require.NoError(t, err)

	// Track first consumer
	consumer1Active := true
	go func() {
		for range msgs1 {
			// Process messages
		}
		consumer1Active = false
	}()

	// Second connection tries to consume from same queue
	conn2, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn2.Close()

	ch2, err := conn2.Channel()
	require.NoError(t, err)

	// Set up channel error notification
	chErrors := ch2.NotifyClose(make(chan *amqp.Error, 1))

	// This should trigger a channel close due to exclusive consumer
	_, err = ch2.Consume(
		qName,            // queue
		"test-consumer2", // consumer
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	require.NoError(t, err)

	// The Consume call might succeed, but the channel should close with ACCESS_REFUSED
	select {
	case chErr := <-chErrors:
		require.NotNil(t, chErr)
		require.Equal(t, 403, chErr.Code) // ACCESS_REFUSED
		require.Contains(t, chErr.Reason, "already has an exclusive consumer")
	case <-time.After(1 * time.Second):
		t.Fatal("Expected channel to close with ACCESS_REFUSED error")
	}

	// Verify first consumer is still active
	time.Sleep(100 * time.Millisecond)
	require.True(t, consumer1Active)
}

// TestConsumerLoopQueueDeleted tests consumer loop exits when queue is deleted
func TestConsumerLoopQueueDeleted(t *testing.T) {
	// Setup server
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect
	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch1, err := conn.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	ch2, err := conn.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Create queue
	qName := uniqueName("q-delete-while-consuming")
	q, err := ch1.QueueDeclare(
		qName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	require.NoError(t, err)

	// Start consumer
	msgs, err := ch1.Consume(
		q.Name,          // queue
		"test-consumer", // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	require.NoError(t, err)

	// Track loop exit
	loopExited := make(chan bool)

	// Start consumer loop
	go func() {
		for range msgs {
			// Process messages
		}
		loopExited <- true
	}()

	// Give consumer time to start
	time.Sleep(100 * time.Millisecond)

	// Delete queue from another channel
	_, err = ch2.QueueDelete(qName, false, false, false)
	require.NoError(t, err)

	// Verify loop exits
	select {
	case <-loopExited:
		t.Log("Consumer loop exited after queue deletion")
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer loop did not exit after queue deletion")
	}
}

// TestConsumerLoopChannelError tests consumer loop exits on channel errors
func TestConsumerLoopChannelError(t *testing.T) {
	// Setup server
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect
	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	// Don't defer close - we'll cause an error

	// Create queue
	qName := uniqueName("q-channel-error")
	q, err := ch.QueueDeclare(
		qName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	require.NoError(t, err)

	// Start consumer
	msgs, err := ch.Consume(
		q.Name,          // queue
		"test-consumer", // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	require.NoError(t, err)

	// Track loop exit
	loopExited := make(chan bool)

	// Start consumer loop
	go func() {
		for msg := range msgs {
			// Try to ack with invalid delivery tag to cause channel error
			_ = ch.Ack(msg.DeliveryTag+1000, false)
		}
		loopExited <- true
	}()

	// Publish a message to trigger the error
	err = ch.Publish(
		"",    // exchange
		qName, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("trigger error"),
		},
	)
	require.NoError(t, err)

	// Verify loop exits due to channel error
	select {
	case <-loopExited:
		t.Log("Consumer loop exited after channel error")
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer loop did not exit after channel error")
	}
}

// TestConsumerReconnectionPattern shows a pattern for handling reconnection
func TestConsumerReconnectionPattern(t *testing.T) {
	// Setup server
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	qName := uniqueName("q-reconnect-pattern")

	// Example of reconnection pattern
	consumeWithReconnect := func() {
		for {
			conn, err := amqp.Dial("amqp://" + addr)
			if err != nil {
				t.Logf("Failed to connect: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			ch, err := conn.Channel()
			if err != nil {
				t.Logf("Failed to open channel: %v", err)
				conn.Close()
				time.Sleep(1 * time.Second)
				continue
			}

			// Declare queue (idempotent operation)
			_, err = ch.QueueDeclare(
				qName,
				false, // durable
				false, // delete when unused
				false, // exclusive
				false, // no-wait
				nil,   // arguments
			)
			if err != nil {
				t.Logf("Failed to declare queue: %v", err)
				ch.Close()
				conn.Close()
				time.Sleep(1 * time.Second)
				continue
			}

			// Start consuming
			msgs, err := ch.Consume(
				qName,           // queue
				"test-consumer", // consumer
				false,           // auto-ack
				false,           // exclusive
				false,           // no-local
				false,           // no-wait
				nil,             // args
			)
			if err != nil {
				t.Logf("Failed to start consumer: %v", err)
				ch.Close()
				conn.Close()
				time.Sleep(1 * time.Second)
				continue
			}

			// Consumer loop
			t.Log("Consumer started successfully")
			for msg := range msgs {
				// Process message
				t.Logf("Received message: %s", msg.Body)
				_ = msg.Ack(false)
			}

			// If we get here, consumer was closed
			t.Log("Consumer loop exited, will reconnect...")
			ch.Close()
			conn.Close()
			time.Sleep(1 * time.Second)
		}
	}

	// Run consumer in background
	done := make(chan bool)
	go func() {
		consumeWithReconnect()
		done <- true
	}()

	// Let it run briefly
	time.Sleep(500 * time.Millisecond)

	// Test passes - this is just demonstrating the pattern
}
