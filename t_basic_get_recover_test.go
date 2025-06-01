package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Basic.Get Tests ---

func TestBasicGet_Success(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue
	queueName := uniqueName("basic-get-success")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish messages
	messages := []string{"msg1", "msg2", "msg3"}
	for i, msg := range messages {
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body:      []byte(msg),
			MessageId: fmt.Sprintf("id-%d", i),
		})
		require.NoError(t, err)
	}

	// Get messages one by one
	for i, expectedMsg := range messages {
		msg, ok, err := ch.Get(q.Name, true) // autoAck = true
		require.NoError(t, err)
		require.True(t, ok, "Should have message available")
		assert.Equal(t, expectedMsg, string(msg.Body))
		assert.Equal(t, fmt.Sprintf("id-%d", i), msg.MessageId)
		assert.False(t, msg.Redelivered)
	}

	// Queue should be empty now
	_, ok, err := ch.Get(q.Name, true)
	require.NoError(t, err)
	require.False(t, ok, "Queue should be empty")
}

func TestBasicGet_Empty(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup empty queue
	queueName := uniqueName("basic-get-empty")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Get from empty queue
	msg, ok, err := ch.Get(q.Name, true)
	require.NoError(t, err)
	require.False(t, ok, "Should return false for empty queue")
	assert.Empty(t, msg.Body, "Message should be empty")
}

func TestBasicGet_QueueNotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Get from non-existent queue
	_, _, err = ch.Get("non-existent-queue-get", true)
	require.Error(t, err)

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code)
}

func TestBasicGet_WithAck(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue
	queueName := uniqueName("basic-get-ack")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish message
	body := "message to be acked"
	err = ch.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte(body)})
	require.NoError(t, err)

	// Get with noAck=false
	msg, ok, err := ch.Get(q.Name, false)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, body, string(msg.Body))
	assert.Greater(t, msg.DeliveryTag, uint64(0))

	// Message should still be unacked
	// Try to get again - should be empty (message is unacked, not available)
	_, ok, err = ch.Get(q.Name, false)
	require.NoError(t, err)
	require.False(t, ok, "Queue should appear empty while message is unacked")

	// Ack the message
	err = ch.Ack(msg.DeliveryTag, false)
	require.NoError(t, err)

	// Queue should still be empty after ack
	_, ok, err = ch.Get(q.Name, false)
	require.NoError(t, err)
	require.False(t, ok, "Queue should be empty after ack")
}

func TestBasicGet_WithReject(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue
	queueName := uniqueName("basic-get-reject")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish message
	body := "message to be rejected"
	err = ch.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte(body)})
	require.NoError(t, err)

	// Get with noAck=false
	msg1, ok, err := ch.Get(q.Name, false)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, body, string(msg1.Body))

	// Reject with requeue
	err = ch.Reject(msg1.DeliveryTag, true)
	require.NoError(t, err)

	// Get again - should get the same message but redelivered
	msg2, ok, err := ch.Get(q.Name, false)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, body, string(msg2.Body))
	assert.True(t, msg2.Redelivered)

	// Reject without requeue
	err = ch.Reject(msg2.DeliveryTag, false)
	require.NoError(t, err)

	// Queue should be empty
	_, ok, err = ch.Get(q.Name, false)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestBasicGet_MessageProperties(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue
	queueName := uniqueName("basic-get-props")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish message with properties
	headers := amqp.Table{
		"x-custom": "value",
		"x-number": int32(42),
	}

	pub := amqp.Publishing{
		Headers:         headers,
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		DeliveryMode:    2,
		Priority:        5,
		CorrelationId:   "corr-123",
		ReplyTo:         "reply-queue",
		Expiration:      "60000",
		MessageId:       "msg-123",
		Timestamp:       time.Now(),
		Type:            "test.message",
		UserId:          "",
		AppId:           "test-app",
	}
	pub.Body = []byte(`{"test": "data"}`)

	err = ch.Publish("", q.Name, false, false, pub)
	require.NoError(t, err)

	// Get message
	msg, ok, err := ch.Get(q.Name, true)
	require.NoError(t, err)
	require.True(t, ok)

	// Verify properties
	assert.Equal(t, pub.Body, msg.Body)
	assert.Equal(t, pub.ContentType, msg.ContentType)
	assert.Equal(t, pub.ContentEncoding, msg.ContentEncoding)
	assert.Equal(t, pub.DeliveryMode, msg.DeliveryMode)
	assert.Equal(t, pub.Priority, msg.Priority)
	assert.Equal(t, pub.CorrelationId, msg.CorrelationId)
	assert.Equal(t, pub.ReplyTo, msg.ReplyTo)
	assert.Equal(t, pub.Expiration, msg.Expiration)
	assert.Equal(t, pub.MessageId, msg.MessageId)
	assert.Equal(t, pub.Type, msg.Type)
	assert.Equal(t, pub.AppId, msg.AppId)
	assert.Equal(t, "value", msg.Headers["x-custom"])
	assert.Equal(t, int32(42), msg.Headers["x-number"])
}

func TestBasicGet_MultipleChannels(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create two channels
	ch1, err := conn.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	ch2, err := conn.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Setup queue
	queueName := uniqueName("basic-get-multi-ch")
	q, err := ch1.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish messages
	for i := 0; i < 4; i++ {
		err = ch1.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("msg%d", i)),
		})
		require.NoError(t, err)
	}

	// Get from different channels
	msg1, ok, err := ch1.Get(q.Name, false)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "msg0", string(msg1.Body))

	msg2, ok, err := ch2.Get(q.Name, false)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "msg1", string(msg2.Body))

	// Ack from respective channels
	err = ch1.Ack(msg1.DeliveryTag, false)
	require.NoError(t, err)

	err = ch2.Ack(msg2.DeliveryTag, false)
	require.NoError(t, err)

	// Get remaining messages
	msg3, ok, err := ch1.Get(q.Name, true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "msg2", string(msg3.Body))

	msg4, ok, err := ch2.Get(q.Name, true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "msg3", string(msg4.Body))
}

// --- Basic.Recover Tests (now using Basic.Nack) ---

func TestBasicRecover_SimpleRequeue(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue and consumer
	queueName := uniqueName("basic-recover-simple")
	q, deliveries, _ := t_setupQueueAndConsumer(t, ch, queueName, false) // autoAck=false

	// Publish messages
	messages := []string{"msg1", "msg2", "msg3"}
	for _, msg := range messages {
		t_publishMessage(t, ch, "", q.Name, msg, false, amqp.Publishing{})
	}

	// Receive but don't ack
	for i := 0; i < 3; i++ {
		select {
		case d := <-deliveries:
			// The 'd' variable is used for assertions below, so it's not unused in this scope.
			assert.False(t, d.Redelivered)
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i+1)
		}
	}

	// Call Nack to requeue all unacknowledged messages on the channel
	// deliveryTag=0, multiple=true, requeue=true
	err = ch.Nack(0, true, true)
	require.NoError(t, err)

	// Messages should be redelivered
	for i := 0; i < 3; i++ {
		select {
		case d := <-deliveries:
			assert.True(t, d.Redelivered)
			assert.Contains(t, []string{"msg1", "msg2", "msg3"}, string(d.Body))
			// Ack this time
			err = d.Ack(false)
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for redelivered message %d", i+1)
		}
	}

	// No more messages
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)
}

func TestBasicRecover_NoRequeue(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue and consumer
	queueName := uniqueName("basic-recover-no-requeue")
	q, deliveries, _ := t_setupQueueAndConsumer(t, ch, queueName, false)

	// Publish messages
	for i := 0; i < 3; i++ {
		t_publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
	}

	// Receive but don't ack
	for i := 0; i < 3; i++ {
		select {
		case <-deliveries:
			// Just consume, don't ack. The act of receiving makes it unacknowledged.
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i+1)
		}
	}

	// Call Nack to discard all unacknowledged messages on the channel
	// deliveryTag=0, multiple=true, requeue=false
	err = ch.Nack(0, true, false)
	require.NoError(t, err)

	// With requeue=false, messages are discarded (not redelivered)
	t_expectNoMessage(t, deliveries, 500*time.Millisecond)
}

func TestBasicRecover_MultipleConsumers(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue
	queueName := uniqueName("basic-recover-multi-consumer")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Create two consumers on the same channel
	deliveries1, tag1 := t_consumeMessage(t, ch, q.Name, "consumer1", false)
	deliveries2, tag2 := t_consumeMessage(t, ch, q.Name, "consumer2", false)
	require.NotEqual(t, tag1, tag2)

	// Publish more messages to increase distribution likelihood
	messageCount := 8
	expectedMessages := make(map[string]bool)
	for i := 0; i < messageCount; i++ {
		msgBody := fmt.Sprintf("msg%d", i)
		expectedMessages[msgBody] = true
		t_publishMessage(t, ch, "", q.Name, msgBody, false, amqp.Publishing{})
	}

	// Collect messages from both consumers with longer timeout
	// These messages are not acked, so they become unacknowledged on 'ch'.
	consumer1ReceiveCount := 0
	consumer2ReceiveCount := 0
	receivedMessages := make(map[string]bool)

	timeout := time.After(3 * time.Second) // Increased timeout
	totalReceived := 0

	// First phase: collect all initial messages
	for totalReceived < messageCount {
		select {
		case d := <-deliveries1:
			receivedMessages[string(d.Body)] = true
			consumer1ReceiveCount++
			totalReceived++
		case d := <-deliveries2:
			receivedMessages[string(d.Body)] = true
			consumer2ReceiveCount++
			totalReceived++
		case <-timeout:
			t.Fatalf("Timeout: only received %d/%d messages", totalReceived, messageCount)
		}
	}

	// Verify we got all expected messages
	for expectedMsg := range expectedMessages {
		assert.True(t, receivedMessages[expectedMsg], "Missing expected message: %s", expectedMsg)
	}

	t.Logf("Initial distribution: consumer1=%d, consumer2=%d", consumer1ReceiveCount, consumer2ReceiveCount)

	// Don't acknowledge any messages - they should all be unacked on this channel 'ch'

	// Call Nack to requeue all unacknowledged messages on channel 'ch'
	// deliveryTag=0, multiple=true, requeue=true
	err = ch.Nack(0, true, true)
	require.NoError(t, err)

	// Clear previous message tracking for redelivery
	consumer1RedeliveredCount := 0
	consumer2RedeliveredCount := 0
	redeliveredCount := 0
	redeliveredMessages := make(map[string]int) // Count how many times each message is redelivered

	// Second phase: collect redelivered messages with longer timeout
	timeout = time.After(3 * time.Second)

	for redeliveredCount < messageCount {
		select {
		case d := <-deliveries1:
			assert.True(t, d.Redelivered, "Message should be marked as redelivered")
			consumer1RedeliveredCount++
			redeliveredMessages[string(d.Body)]++
			redeliveredCount++
			err = d.Ack(false) // Ack this time to clean up
			require.NoError(t, err)
		case d := <-deliveries2:
			assert.True(t, d.Redelivered, "Message should be marked as redelivered")
			consumer2RedeliveredCount++
			redeliveredMessages[string(d.Body)]++
			redeliveredCount++
			err = d.Ack(false) // Ack this time to clean up
			require.NoError(t, err)
		case <-timeout:
			t.Fatalf("Timeout: only received %d/%d redelivered messages", redeliveredCount, messageCount)
		}
	}

	// Verify all original messages were redelivered exactly once
	for expectedMsg := range expectedMessages {
		count, found := redeliveredMessages[expectedMsg]
		assert.True(t, found, "Message %s was not redelivered", expectedMsg)
		assert.Equal(t, 1, count, "Message %s was redelivered %d times, expected 1", expectedMsg, count)
	}

	t.Logf("Redelivery distribution: consumer1=%d, consumer2=%d", consumer1RedeliveredCount, consumer2RedeliveredCount)
	assert.Equal(t, messageCount, redeliveredCount, "Should have received all messages as redelivered")
	assert.Equal(t, messageCount, len(redeliveredMessages), "Should have unique redelivered messages")

	t_expectNoMessage(t, deliveries1, 200*time.Millisecond)
	t_expectNoMessage(t, deliveries2, 200*time.Millisecond)
}

func TestBasicRecover_PartialAck(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue and consumer
	queueName := uniqueName("basic-recover-partial")
	q, deliveries, _ := t_setupQueueAndConsumer(t, ch, queueName, false)

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		t_publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
	}

	// Receive all messages, ack only some
	ackedMsgs := make(map[string]bool)
	unackedMsgs := make(map[string]bool)

	for i := 0; i < 5; i++ {
		select {
		case d := <-deliveries:
			if i%2 == 0 { // Ack even-indexed messages
				err = d.Ack(false)
				require.NoError(t, err)
				ackedMsgs[string(d.Body)] = true
			} else {
				// This message is received but not acked, making it unacknowledged.
				unackedMsgs[string(d.Body)] = true
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i)
		}
	}

	// Call Nack to requeue all unacknowledged messages on the channel
	// deliveryTag=0, multiple=true, requeue=true
	err = ch.Nack(0, true, true)
	require.NoError(t, err)

	// Only unacked messages should be redelivered
	redeliveredCount := 0
	timeout := time.After(1 * time.Second)

	for redeliveredCount < len(unackedMsgs) {
		select {
		case d := <-deliveries:
			assert.True(t, d.Redelivered)
			assert.True(t, unackedMsgs[string(d.Body)], "Redelivered message '%s' should have been unacked", string(d.Body))
			assert.False(t, ackedMsgs[string(d.Body)], "Acked message '%s' should not be redelivered", string(d.Body))
			d.Ack(false)
			redeliveredCount++
		case <-timeout:
			t.Fatalf("Timeout: only received %d/%d expected redelivered messages", redeliveredCount, len(unackedMsgs))
		}
	}

	// No more messages
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)
}

func TestBasicRecover_EmptyUnackedList(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue and consumer
	queueName := uniqueName("basic-recover-empty")
	q, deliveries, _ := t_setupQueueAndConsumer(t, ch, queueName, false)

	// Publish and immediately ack messages
	for i := 0; i < 3; i++ {
		t_publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
		d := t_expectMessage(t, deliveries, fmt.Sprintf("msg%d", i), nil, 1*time.Second)
		err = d.Ack(false)
		require.NoError(t, err)
	}

	// Call Nack with no unacked messages
	// deliveryTag=0, multiple=true, requeue=true
	err = ch.Nack(0, true, true)
	require.NoError(t, err)

	// No messages should be redelivered
	t_expectNoMessage(t, deliveries, 500*time.Millisecond)
}

func TestBasicRecover_ConcurrentWithGet(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue
	queueName := uniqueName("basic-recover-concurrent-get")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish messages
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		t_publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
	}

	// Get some messages without acking to make them unacknowledged on the channel.
	for i := 0; i < 5; i++ {
		_, ok, err := ch.Get(q.Name, false) // autoAck=false
		require.NoError(t, err)
		require.True(t, ok)
	}

	// Start a consumer for remaining messages
	deliveries, _ := t_consumeMessage(t, ch, q.Name, "consumer1", false) // autoAck=false

	// Consume remaining messages (these will also be unacked initially on the channel)
	consumedCount := 0
	timeout := time.After(1 * time.Second)

	for consumedCount < 5 {
		select {
		case d := <-deliveries:
			assert.False(t, d.Redelivered)
			// Don't ack these yet
			consumedCount++
		case <-timeout:
			t.Fatalf("Timeout: only consumed %d/5 messages", consumedCount)
		}
	}

	// Call Nack to requeue all 10 unacknowledged messages on the channel
	// deliveryTag=0, multiple=true, requeue=true
	err = ch.Nack(0, true, true)
	require.NoError(t, err)

	// All 10 unacked messages should be available for redelivery to the consumer
	redeliveredCount := 0
	timeout = time.After(2 * time.Second)

	for redeliveredCount < 10 {
		select {
		case d := <-deliveries:
			assert.True(t, d.Redelivered)
			d.Ack(false) // Ack them now
			redeliveredCount++
		case <-timeout:
			t.Fatalf("Timeout: only received %d/10 redelivered messages", redeliveredCount)
		}
	}
}

func TestBasicRecover_MultipleChannels(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create two channels
	ch1, err := conn.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	ch2, err := conn.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Setup separate queues for each channel to ensure isolation
	queueName1 := uniqueName("basic-recover-multi-ch1")
	q1, err := ch1.QueueDeclare(queueName1, false, false, false, false, nil)
	require.NoError(t, err)

	queueName2 := uniqueName("basic-recover-multi-ch2")
	q2, err := ch2.QueueDeclare(queueName2, false, false, false, false, nil)
	require.NoError(t, err)

	// Create consumers on separate queues, on their respective channels
	deliveries1, _ := t_consumeMessage(t, ch1, q1.Name, "consumer-ch1", false)
	deliveries2, _ := t_consumeMessage(t, ch2, q2.Name, "consumer-ch2", false)

	messageCount := 3

	// Publish to queue 1 via channel 1
	for i := 0; i < messageCount; i++ {
		t_publishMessage(t, ch1, "", q1.Name, fmt.Sprintf("ch1-msg%d", i), false, amqp.Publishing{})
	}

	// Publish to queue 2 via channel 2
	for i := 0; i < messageCount; i++ {
		t_publishMessage(t, ch2, "", q2.Name, fmt.Sprintf("ch2-msg%d", i), false, amqp.Publishing{})
	}

	// Collect messages from both channels (don't ack)
	// The act of receiving makes them unacknowledged on their respective channels.
	ch1ReceiveCount := 0
	ch2ReceiveCount := 0
	timeout := time.After(2 * time.Second)
	totalReceived := 0
	expectedTotal := messageCount * 2

	for totalReceived < expectedTotal {
		select {
		case <-deliveries1:
			ch1ReceiveCount++
			totalReceived++
		case <-deliveries2:
			ch2ReceiveCount++
			totalReceived++
		case <-timeout:
			t.Fatalf("Timeout: only received %d/%d messages", totalReceived, expectedTotal)
		}
	}
	require.Equal(t, messageCount, ch1ReceiveCount, "Channel 1 should have received %d messages", messageCount)
	require.Equal(t, messageCount, ch2ReceiveCount, "Channel 2 should have received %d messages", messageCount)

	// Test 1: Nack all on channel 1 (requeue=true)
	err = ch1.Nack(0, true, true)
	require.NoError(t, err)

	ch1RedeliveredCount := 0
	timeout = time.After(2 * time.Second)
	for ch1RedeliveredCount < messageCount {
		select {
		case d := <-deliveries1:
			assert.True(t, d.Redelivered, "Message on ch1 should be redelivered")
			assert.Contains(t, string(d.Body), "ch1-", "Should be ch1 message")
			ch1RedeliveredCount++
			d.Ack(false)
		case <-deliveries2:
			t.Fatalf("Channel 2 should not receive messages from channel 1's Nack")
		case <-timeout:
			t.Fatalf("Timeout: only %d/%d redelivered on ch1", ch1RedeliveredCount, messageCount)
		}
	}
	t_expectNoMessage(t, deliveries1, 200*time.Millisecond) // ch1 should be clear now

	// Test 2: Nack all on channel 2 (requeue=true)
	err = ch2.Nack(0, true, true)
	require.NoError(t, err)

	ch2RedeliveredCount := 0
	timeout = time.After(2 * time.Second)
	for ch2RedeliveredCount < messageCount {
		select {
		case d := <-deliveries2:
			assert.True(t, d.Redelivered, "Message on ch2 should be redelivered")
			assert.Contains(t, string(d.Body), "ch2-", "Should be ch2 message")
			ch2RedeliveredCount++
			d.Ack(false)
		case <-deliveries1:
			t.Fatalf("Channel 1 should not receive messages from channel 2's Nack")
		case <-timeout:
			t.Fatalf("Timeout: only %d/%d redelivered on ch2", ch2RedeliveredCount, messageCount)
		}
	}
	t_expectNoMessage(t, deliveries2, 200*time.Millisecond) // ch2 should be clear now

	// Test 3: Nack with no unacked messages does nothing
	err = ch1.Nack(0, true, true)
	require.NoError(t, err)
	err = ch2.Nack(0, true, true)
	require.NoError(t, err)
	t_expectNoMessage(t, deliveries1, 200*time.Millisecond)
	t_expectNoMessage(t, deliveries2, 200*time.Millisecond)
}

func TestBasicRecover_RaceConditionMultiChannel(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create multiple channels to avoid conflicts
	ch1, err := conn.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	ch2, err := conn.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Setup queue
	queueName := uniqueName("basic-recover-race-multi")
	q, err := ch1.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Create consumers on both channels
	deliveries1, _ := t_consumeMessage(t, ch1, q.Name, "consumer1", false)
	deliveries2, _ := t_consumeMessage(t, ch2, q.Name, "consumer2", false)

	// Publish messages
	messageCount := 50
	for i := 0; i < messageCount; i++ {
		t_publishMessage(t, ch1, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
	}

	var wg sync.WaitGroup
	ch1Messages := int32(0)
	ch2Messages := int32(0)
	ch1Acked := int32(0)
	ch2Acked := int32(0)

	wg.Add(3)

	// Goroutine 1: Consume on ch1 and ack some messages
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			select {
			case d := <-deliveries1:
				atomic.AddInt32(&ch1Messages, 1)
				if i%3 == 0 {
					d.Ack(false)
					atomic.AddInt32(&ch1Acked, 1)
				}
			case <-time.After(100 * time.Millisecond):
				return
			}
		}
	}()

	// Goroutine 2: Periodically nack all on ch1
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			time.Sleep(50 * time.Millisecond)
			ch1.Nack(0, true, true) // This only affects ch1's unacked messages
		}
	}()

	// Goroutine 3: Consume on ch2 and ack all messages
	go func() {
		defer wg.Done()
		timeout := time.After(1 * time.Second)
		for {
			select {
			case d := <-deliveries2:
				atomic.AddInt32(&ch2Messages, 1)
				d.Ack(false)
				atomic.AddInt32(&ch2Acked, 1)
			case <-timeout:
				return
			}
		}
	}()

	wg.Wait()

	// Log results
	t.Logf("Channel 1: Received %d messages, Acked %d", ch1Messages, ch1Acked)
	t.Logf("Channel 2: Received %d messages, Acked %d", ch2Messages, ch2Acked)

	// Verify both channels got messages
	assert.Greater(t, ch1Messages, int32(0), "Channel 1 should have received messages")
	assert.Greater(t, ch2Messages, int32(0), "Channel 2 should have received messages")
}

func TestBasicGet_Recover_Integration(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue
	queueName := uniqueName("get-recover-integration")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish messages
	for i := 0; i < 5; i++ {
		t_publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
	}

	// Get messages without acking (these are unacked on the channel)
	for i := 0; i < 3; i++ {
		_, ok, err := ch.Get(q.Name, false) // autoAck=false
		require.NoError(t, err)
		require.True(t, ok)
		// The act of getting makes them unacknowledged.
	}

	// Start a consumer (will also have its messages unacked on the same channel)
	deliveries, _ := t_consumeMessage(t, ch, q.Name, "consumer1", false) // autoAck=false

	// Get remaining messages via consumer (don't ack yet)
	for i := 0; i < 2; i++ {
		select {
		case d := <-deliveries:
			assert.False(t, d.Redelivered)
			// The act of receiving makes them unacknowledged.
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for consumed message %d", i)
		}
	}

	// At this point, all 5 messages are unacknowledged on channel 'ch'.
	// 3 from Get, 2 from Consume.

	// Nack to requeue all unacknowledged messages on the channel
	// deliveryTag=0, multiple=true, requeue=true
	err = ch.Nack(0, true, true)
	require.NoError(t, err)

	// All 5 messages should be redelivered to the active consumer
	redeliveredCount := 0
	timeout := time.After(2 * time.Second)

	for redeliveredCount < 5 {
		select {
		case d := <-deliveries:
			assert.True(t, d.Redelivered)
			d.Ack(false) // Ack them now
			redeliveredCount++
		case <-timeout:
			t.Fatalf("Timeout: only received %d/5 redelivered messages", redeliveredCount)
		}
	}

	// Queue should be empty
	_, ok, err := ch.Get(q.Name, true) // autoAck=true
	require.NoError(t, err)
	require.False(t, ok, "Queue should be empty after Get/Recover integration test")
}
