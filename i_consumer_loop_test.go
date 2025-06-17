package carrotmq

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// shortHash generates a short hash of the message body for tracing
func shortHash(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])[:8]
}

// TestConsumerLoopEmptyQueue tests that the consumer loop continues when no messages are available
func TestConsumerLoopEmptyQueue(t *testing.T) {
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
	qName := uniqueName("q-consumer-loop-empty")
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
		q.Name,            // queue
		"test-consumer",   // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	require.NoError(t, err)

	// Track loop status
	loopExited := make(chan bool)
	messagesReceived := 0
	var mu sync.Mutex

	// Start consumer loop in goroutine
	go func() {
		for msg := range msgs {
			mu.Lock()
			messagesReceived++
			mu.Unlock()
			
			// Ack the message
			_ = msg.Ack(false)
		}
		loopExited <- true
	}()

	// Wait a bit to ensure consumer is ready
	time.Sleep(100 * time.Millisecond)

	// Verify loop is still running (hasn't exited)
	select {
	case <-loopExited:
		t.Fatal("Consumer loop exited prematurely when no messages were available")
	case <-time.After(500 * time.Millisecond):
		// Good - loop is still running
	}

	// Now publish a message to verify consumer is still active
	msgBody := []byte("test message after wait")
	err = ch.Publish(
		"",    // exchange
		qName, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msgBody,
		},
	)
	require.NoError(t, err)

	// Wait for message to be consumed
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	require.Equal(t, 1, messagesReceived, "Expected 1 message to be received")
	mu.Unlock()

	// Verify loop is still running
	select {
	case <-loopExited:
		t.Fatal("Consumer loop exited after processing message")
	case <-time.After(300 * time.Millisecond):
		// Good - loop is still running
	}
}

// TestConsumerLoopWithMessageHandler tests consumer loop with a message handler pattern
func TestConsumerLoopWithMessageHandler(t *testing.T) {
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
	qName := uniqueName("q-consumer-loop-handler")
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
		q.Name,            // queue
		"test-consumer",   // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	require.NoError(t, err)

	// Track processing
	processedMessages := make([]string, 0)
	rejectedMessages := make([]string, 0)
	loopExited := make(chan bool)
	messageCount := 0
	var mu sync.Mutex

	// Message handler that rejects every other message
	handleMessage := func(body []byte) error {
		mu.Lock()
		defer mu.Unlock()
		
		messageCount++
		if messageCount%2 == 1 {
			// Accept odd-numbered messages (1st, 3rd, etc)
			return nil
		}
		// Reject even-numbered messages (2nd, 4th, etc)
		return amqp.Error{Code: 500, Reason: "simulated error"}
	}

	// Start consumer loop
	go func() {
		for msg := range msgs {
			if err := handleMessage(msg.Body); err != nil {
				if err := msg.Reject(false); err == nil {
					mu.Lock()
					rejectedMessages = append(rejectedMessages, string(msg.Body))
					mu.Unlock()
				}
			} else {
				if err := msg.Ack(false); err == nil {
					mu.Lock()
					processedMessages = append(processedMessages, string(msg.Body))
					mu.Unlock()
				}
			}
		}
		loopExited <- true
	}()

	// Publish multiple messages
	messages := []string{"msg1", "msg2", "msg3", "msg4"}
	for _, msg := range messages {
		err = ch.Publish(
			"",    // exchange
			qName, // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg),
			},
		)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Verify processing results
	mu.Lock()
	require.Equal(t, 2, len(processedMessages), "Expected 2 messages to be processed")
	require.Equal(t, 2, len(rejectedMessages), "Expected 2 messages to be rejected")
	require.Contains(t, processedMessages, "msg1")
	require.Contains(t, processedMessages, "msg3")
	require.Contains(t, rejectedMessages, "msg2")
	require.Contains(t, rejectedMessages, "msg4")
	mu.Unlock()

	// Verify loop is still running
	select {
	case <-loopExited:
		t.Fatal("Consumer loop exited prematurely")
	case <-time.After(200 * time.Millisecond):
		// Good - loop is still running
	}
}

// TestConsumerLoopChannelClose tests that consumer loop exits when channel closes
func TestConsumerLoopChannelClose(t *testing.T) {
	// Setup server
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect
	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	// Don't defer close - we'll close it manually

	// Create queue
	qName := uniqueName("q-consumer-loop-close")
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
		q.Name,            // queue
		"test-consumer",   // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	require.NoError(t, err)

	// Track loop exit
	loopExited := make(chan bool)

	// Start consumer loop
	go func() {
		for msg := range msgs {
			_ = msg.Ack(false)
		}
		loopExited <- true
	}()

	// Verify loop is running
	select {
	case <-loopExited:
		t.Fatal("Consumer loop exited before channel close")
	case <-time.After(200 * time.Millisecond):
		// Good - loop is running
	}

	// Close the channel
	err = ch.Close()
	require.NoError(t, err)

	// Verify loop exits after channel close
	select {
	case <-loopExited:
		// Good - loop exited as expected
	case <-time.After(1 * time.Second):
		t.Fatal("Consumer loop did not exit after channel close")
	}
}

// TestConsumerLoopConnectionClose tests that consumer loop exits when connection closes
func TestConsumerLoopConnectionClose(t *testing.T) {
	// Setup server
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect
	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	// Don't defer close - we'll close it manually

	ch, err := conn.Channel()
	require.NoError(t, err)

	// Create queue
	qName := uniqueName("q-consumer-loop-conn-close")
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
		q.Name,            // queue
		"test-consumer",   // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	require.NoError(t, err)

	// Track loop exit
	loopExited := make(chan bool)

	// Start consumer loop
	go func() {
		for msg := range msgs {
			_ = msg.Ack(false)
		}
		loopExited <- true
	}()

	// Verify loop is running
	select {
	case <-loopExited:
		t.Fatal("Consumer loop exited before connection close")
	case <-time.After(200 * time.Millisecond):
		// Good - loop is running
	}

	// Close the connection
	err = conn.Close()
	require.NoError(t, err)

	// Verify loop exits after connection close
	select {
	case <-loopExited:
		// Good - loop exited as expected
	case <-time.After(1 * time.Second):
		t.Fatal("Consumer loop did not exit after connection close")
	}
}