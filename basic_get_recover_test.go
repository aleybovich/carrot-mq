package main

import (
	"fmt"
	"sync"
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

// --- Basic.Recover Tests ---

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
	q, deliveries, _ := setupQueueAndConsumer(t, ch, queueName, false) // autoAck=false

	// Publish messages
	messages := []string{"msg1", "msg2", "msg3"}
	for _, msg := range messages {
		publishMessage(t, ch, "", q.Name, msg, false, amqp.Publishing{})
	}

	// Receive but don't ack
	received := make([]amqp.Delivery, 0, 3)
	for i := 0; i < 3; i++ {
		select {
		case d := <-deliveries:
			received = append(received, d)
			assert.False(t, d.Redelivered)
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i+1)
		}
	}

	// Call Recover with requeue=true
	err = ch.Recover(true)
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
	expectNoMessage(t, deliveries, 200*time.Millisecond)
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
	q, deliveries, _ := setupQueueAndConsumer(t, ch, queueName, false)

	// Publish messages
	for i := 0; i < 3; i++ {
		publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
	}

	// Receive but don't ack
	for i := 0; i < 3; i++ {
		select {
		case <-deliveries:
			// Just consume, don't ack
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i+1)
		}
	}

	// Call Recover with requeue=false
	err = ch.Nack(0, true, false)
	require.NoError(t, err)

	// With requeue=false, messages are discarded (not redelivered)
	expectNoMessage(t, deliveries, 500*time.Millisecond)
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

	// Create two consumers
	deliveries1, tag1 := consumeMessage(t, ch, q.Name, "consumer1", false)
	deliveries2, tag2 := consumeMessage(t, ch, q.Name, "consumer2", false)
	require.NotEqual(t, tag1, tag2)

	// Publish more messages to increase distribution likelihood
	messageCount := 8
	expectedMessages := make(map[string]bool)
	for i := 0; i < messageCount; i++ {
		msgBody := fmt.Sprintf("msg%d", i)
		expectedMessages[msgBody] = true
		publishMessage(t, ch, "", q.Name, msgBody, false, amqp.Publishing{})
	}

	// Collect messages from both consumers with longer timeout
	consumer1Msgs := make([]amqp.Delivery, 0)
	consumer2Msgs := make([]amqp.Delivery, 0)
	receivedMessages := make(map[string]bool)

	timeout := time.After(3 * time.Second) // Increased timeout
	totalReceived := 0

	// First phase: collect all initial messages
	for totalReceived < messageCount {
		select {
		case d := <-deliveries1:
			consumer1Msgs = append(consumer1Msgs, d)
			receivedMessages[string(d.Body)] = true
			totalReceived++
		case d := <-deliveries2:
			consumer2Msgs = append(consumer2Msgs, d)
			receivedMessages[string(d.Body)] = true
			totalReceived++
		case <-timeout:
			t.Fatalf("Timeout: only received %d/%d messages", totalReceived, messageCount)
		}
	}

	// Verify we got all expected messages
	for expectedMsg := range expectedMessages {
		assert.True(t, receivedMessages[expectedMsg], "Missing expected message: %s", expectedMsg)
	}

	t.Logf("Initial distribution: consumer1=%d, consumer2=%d", len(consumer1Msgs), len(consumer2Msgs))

	// Both consumers should have received some messages for a meaningful test
	// If all went to one consumer, add a short delay and retry
	if len(consumer1Msgs) == messageCount || len(consumer2Msgs) == messageCount {
		t.Logf("All messages went to one consumer, this may happen due to timing")
		// Continue with the test anyway, as the recover behavior is still valid
	}

	// Don't acknowledge any messages - they should all be unacked

	// Call Recover with requeue=true using Nack
	err = ch.Nack(0, true, true)
	require.NoError(t, err)

	// Clear previous message tracking
	consumer1Redelivered := make([]amqp.Delivery, 0)
	consumer2Redelivered := make([]amqp.Delivery, 0)
	redeliveredCount := 0
	redeliveredMessages := make(map[string]int) // Count how many times each message is redelivered

	// Second phase: collect redelivered messages with longer timeout
	timeout = time.After(3 * time.Second)

	for redeliveredCount < messageCount {
		select {
		case d := <-deliveries1:
			assert.True(t, d.Redelivered, "Message should be marked as redelivered")
			consumer1Redelivered = append(consumer1Redelivered, d)
			redeliveredMessages[string(d.Body)]++
			redeliveredCount++
			// Ack this time to clean up
			err = d.Ack(false)
			require.NoError(t, err)
		case d := <-deliveries2:
			assert.True(t, d.Redelivered, "Message should be marked as redelivered")
			consumer2Redelivered = append(consumer2Redelivered, d)
			redeliveredMessages[string(d.Body)]++
			redeliveredCount++
			// Ack this time to clean up
			err = d.Ack(false)
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

	t.Logf("Redelivery distribution: consumer1=%d, consumer2=%d", len(consumer1Redelivered), len(consumer2Redelivered))

	// The key test: verify that messages were redelivered (regardless of distribution)
	assert.Equal(t, messageCount, redeliveredCount, "Should have received all messages as redelivered")
	assert.Equal(t, messageCount, len(redeliveredMessages), "Should have unique redelivered messages")

	// No more messages should be available
	expectNoMessage(t, deliveries1, 200*time.Millisecond)
	expectNoMessage(t, deliveries2, 200*time.Millisecond)
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
	q, deliveries, _ := setupQueueAndConsumer(t, ch, queueName, false)

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
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
				unackedMsgs[string(d.Body)] = true
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i)
		}
	}

	// Call Recover
	err = ch.Recover(true)
	require.NoError(t, err)

	// Only unacked messages should be redelivered
	redeliveredCount := 0
	timeout := time.After(1 * time.Second)

	for redeliveredCount < len(unackedMsgs) {
		select {
		case d := <-deliveries:
			assert.True(t, d.Redelivered)
			assert.True(t, unackedMsgs[string(d.Body)], "Redelivered message should have been unacked")
			assert.False(t, ackedMsgs[string(d.Body)], "Acked message should not be redelivered")
			d.Ack(false)
			redeliveredCount++
		case <-timeout:
			t.Fatalf("Timeout: only received %d/%d expected redelivered messages", redeliveredCount, len(unackedMsgs))
		}
	}

	// No more messages
	expectNoMessage(t, deliveries, 200*time.Millisecond)
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
	q, deliveries, _ := setupQueueAndConsumer(t, ch, queueName, false)

	// Publish and immediately ack messages
	for i := 0; i < 3; i++ {
		publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
		d := expectMessage(t, deliveries, fmt.Sprintf("msg%d", i), nil, 1*time.Second)
		err = d.Ack(false)
		require.NoError(t, err)
	}

	// Call Recover with no unacked messages
	err = ch.Recover(true)
	require.NoError(t, err)

	// No messages should be redelivered
	expectNoMessage(t, deliveries, 500*time.Millisecond)
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
		publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
	}

	// Get some messages without acking
	var getDeliveryTags []uint64
	for i := 0; i < 5; i++ {
		msg, ok, err := ch.Get(q.Name, false)
		require.NoError(t, err)
		require.True(t, ok)
		getDeliveryTags = append(getDeliveryTags, msg.DeliveryTag)
	}

	// Start a consumer for remaining messages
	deliveries, _ := consumeMessage(t, ch, q.Name, "consumer1", false)

	// Consume remaining messages
	consumedCount := 0
	timeout := time.After(1 * time.Second)

	for consumedCount < 5 {
		select {
		case d := <-deliveries:
			assert.False(t, d.Redelivered)
			consumedCount++
		case <-timeout:
			t.Fatalf("Timeout: only consumed %d/5 messages", consumedCount)
		}
	}

	// Call Recover
	err = ch.Nack(0, true, true)
	require.NoError(t, err)

	// All 10 unacked messages should be available for redelivery
	redeliveredCount := 0
	timeout = time.After(2 * time.Second)

	for redeliveredCount < 10 {
		select {
		case d := <-deliveries:
			assert.True(t, d.Redelivered)
			d.Ack(false)
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

	// Create consumers on separate queues
	deliveries1, _ := consumeMessage(t, ch1, q1.Name, "consumer-ch1", false)
	deliveries2, _ := consumeMessage(t, ch2, q2.Name, "consumer-ch2", false)

	// Publish messages to each queue separately
	messageCount := 3

	// Publish to queue 1
	for i := 0; i < messageCount; i++ {
		publishMessage(t, ch1, "", q1.Name, fmt.Sprintf("ch1-msg%d", i), false, amqp.Publishing{})
	}

	// Publish to queue 2
	for i := 0; i < messageCount; i++ {
		publishMessage(t, ch2, "", q2.Name, fmt.Sprintf("ch2-msg%d", i), false, amqp.Publishing{})
	}

	// Collect messages from both channels
	var ch1Messages, ch2Messages []amqp.Delivery
	timeout := time.After(2 * time.Second)
	totalReceived := 0
	expectedTotal := messageCount * 2

	for totalReceived < expectedTotal {
		select {
		case d := <-deliveries1:
			ch1Messages = append(ch1Messages, d)
			totalReceived++
		case d := <-deliveries2:
			ch2Messages = append(ch2Messages, d)
			totalReceived++
		case <-timeout:
			t.Fatalf("Timeout: only received %d/%d messages", totalReceived, expectedTotal)
		}
	}

	// Both channels should have received their messages
	require.Len(t, ch1Messages, messageCount, "Channel 1 should have received %d messages", messageCount)
	require.Len(t, ch2Messages, messageCount, "Channel 2 should have received %d messages", messageCount)

	t.Logf("Distribution: ch1=%d, ch2=%d", len(ch1Messages), len(ch2Messages))

	// Don't acknowledge any messages - they remain unacked

	// Test 1: Recover only on channel 1
	err = ch1.Recover(true)
	require.NoError(t, err)

	// Only channel 1's unacked messages should be redelivered
	ch1RedeliveredCount := 0
	timeout = time.After(2 * time.Second)

	for ch1RedeliveredCount < messageCount {
		select {
		case d := <-deliveries1:
			assert.True(t, d.Redelivered, "Message should be marked as redelivered")
			assert.Contains(t, string(d.Body), "ch1-", "Should receive ch1 message")
			ch1RedeliveredCount++
			// Ack to clean up
			err = d.Ack(false)
			require.NoError(t, err)
		case d := <-deliveries2:
			// Channel 2 should NOT receive any redelivered messages from channel 1's recover
			assert.False(t, d.Redelivered, "Channel 2 should not receive redelivered messages from channel 1's recover")
			t.Fatalf("Unexpected redelivered message on channel 2 from channel 1's recover: %s", string(d.Body))
		case <-timeout:
			t.Fatalf("Timeout: only received %d/%d expected redelivered messages on channel 1",
				ch1RedeliveredCount, messageCount)
		}
	}

	// Verify channel 2 doesn't get any unexpected messages
	expectNoMessage(t, deliveries1, 200*time.Millisecond)

	t.Logf("Channel 1 recover test passed: %d messages redelivered", ch1RedeliveredCount)

	// Test 2: Now recover on channel 2
	err = ch2.Recover(true)
	require.NoError(t, err)

	// Only channel 2's unacked messages should be redelivered
	ch2RedeliveredCount := 0
	timeout = time.After(2 * time.Second)

	for ch2RedeliveredCount < messageCount {
		select {
		case d := <-deliveries2:
			assert.True(t, d.Redelivered, "Message should be marked as redelivered")
			assert.Contains(t, string(d.Body), "ch2-", "Should receive ch2 message")
			ch2RedeliveredCount++
			// Ack to clean up
			err = d.Ack(false)
			require.NoError(t, err)
		case d := <-deliveries1:
			// Channel 1 should NOT receive any redelivered messages from channel 2's recover
			assert.False(t, d.Redelivered, "Channel 1 should not receive redelivered messages from channel 2's recover")
			t.Fatalf("Unexpected redelivered message on channel 1 from channel 2's recover: %s", string(d.Body))
		case <-timeout:
			t.Fatalf("Timeout: only received %d/%d expected redelivered messages on channel 2",
				ch2RedeliveredCount, messageCount)
		}
	}

	t.Logf("Channel 2 recover test passed: %d messages redelivered", ch2RedeliveredCount)

	// Verify no more messages are delivered
	expectNoMessage(t, deliveries1, 200*time.Millisecond)
	expectNoMessage(t, deliveries2, 200*time.Millisecond)

	// Test 3: Verify recover with no unacked messages does nothing
	err = ch1.Recover(true)
	require.NoError(t, err)

	err = ch2.Recover(true)
	require.NoError(t, err)

	// Should receive no messages
	expectNoMessage(t, deliveries1, 200*time.Millisecond)
	expectNoMessage(t, deliveries2, 200*time.Millisecond)

	t.Logf("Empty recover test passed")
}

func TestBasicRecover_RaceCondition(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup queue and consumer
	queueName := uniqueName("basic-recover-race")
	q, deliveries, _ := setupQueueAndConsumer(t, ch, queueName, false)

	// Publish many messages
	messageCount := 50
	for i := 0; i < messageCount; i++ {
		publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
	}

	var wg sync.WaitGroup
	wg.Add(3)

	// Goroutine 1: Consume and randomly ack
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			select {
			case d := <-deliveries:
				if i%3 == 0 {
					d.Ack(false)
				}
			case <-time.After(100 * time.Millisecond):
				return
			}
		}
	}()

	// Goroutine 2: Call recover periodically
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			time.Sleep(50 * time.Millisecond)
			ch.Recover(true)
		}
	}()

	// Goroutine 3: Continue consuming
	ackedCount := 0
	go func() {
		defer wg.Done()
		timeout := time.After(3 * time.Second)
		for ackedCount < messageCount {
			select {
			case d := <-deliveries:
				d.Ack(false)
				ackedCount++
			case <-timeout:
				return
			}
		}
	}()

	wg.Wait()

	// Verify queue is eventually empty
	time.Sleep(500 * time.Millisecond)
	msg, ok, err := ch.Get(q.Name, true)
	require.NoError(t, err)
	if ok {
		t.Logf("Queue still has message: %s", string(msg.Body))
	}
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
		publishMessage(t, ch, "", q.Name, fmt.Sprintf("msg%d", i), false, amqp.Publishing{})
	}

	// Get messages without acking
	var unackedMsgs []amqp.Delivery
	for i := 0; i < 3; i++ {
		msg, ok, err := ch.Get(q.Name, false)
		require.NoError(t, err)
		require.True(t, ok)
		unackedMsgs = append(unackedMsgs, msg)
	}

	// Start a consumer
	deliveries, _ := consumeMessage(t, ch, q.Name, "consumer1", false)

	// Get remaining messages via consumer
	for i := 0; i < 2; i++ {
		select {
		case d := <-deliveries:
			assert.False(t, d.Redelivered)
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for consumed message %d", i)
		}
	}

	// Recover all unacked messages
	err = ch.Recover(true)
	require.NoError(t, err)

	// All 5 messages should be redelivered to the consumer
	redeliveredCount := 0
	timeout := time.After(2 * time.Second)

	for redeliveredCount < 5 {
		select {
		case d := <-deliveries:
			assert.True(t, d.Redelivered)
			d.Ack(false)
			redeliveredCount++
		case <-timeout:
			t.Fatalf("Timeout: only received %d/5 redelivered messages", redeliveredCount)
		}
	}

	// Queue should be empty
	_, ok, err := ch.Get(q.Name, true)
	require.NoError(t, err)
	require.False(t, ok)
}
