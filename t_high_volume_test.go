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

func TestHighVolumeSingleConsumerDelivery(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Failed to connect to AMQP server")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open a channel")
	defer ch.Close()

	queueName := uniqueName("high-volume-queue")
	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	require.NoError(t, err, "Failed to declare queue")

	// Bind to default exchange for simplicity
	err = ch.QueueBind(q.Name, q.Name, "", false, nil)
	require.NoError(t, err, "Failed to bind queue %s", q.Name)

	messageCount := 1000
	publishedBodies := make(map[string]bool)

	t.Logf("Publishing %d messages to queue '%s'...", messageCount, q.Name)
	for i := 0; i < messageCount; i++ {
		body := fmt.Sprintf("message-%d", i)
		publishedBodies[body] = true
		err = ch.Publish(
			"",     // exchange (default)
			q.Name, // routing key (queue name)
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
				MessageId:   fmt.Sprintf("msg-id-%d", i),
			})
		require.NoError(t, err, "Failed to publish message %d", i)
	}
	t.Logf("Successfully published %d messages.", messageCount)

	consumerTag := "single-consumer"
	// Using autoAck=true for simplicity, as we're testing delivery volume, not ack mechanics here.
	deliveries, actualConsumerTag := t_consumeMessage(t, ch, q.Name, consumerTag, true)
	require.Equal(t, consumerTag, actualConsumerTag, "Consumer tag mismatch")

	t.Logf("Consumer '%s' started. Waiting for %d messages...", consumerTag, messageCount)

	var receivedCount int32
	receivedMessages := make(map[string]int) // To count occurrences of each body
	var wg sync.WaitGroup
	wg.Add(1) // For the consumer goroutine

	go func() {
		defer wg.Done()
		timeout := time.After(20 * time.Second) // Generous timeout for 1000 messages

		for {
			select {
			case delivery, ok := <-deliveries:
				if !ok {
					t.Log("Deliveries channel closed by server.")
					// This might happen if the test is shutting down or an error occurred.
					// If receivedCount is not messageCount, the assertion below will fail.
					return
				}
				atomic.AddInt32(&receivedCount, 1)
				count := atomic.LoadInt32(&receivedCount)

				bodyStr := string(delivery.Body)
				receivedMessages[bodyStr]++

				if count%100 == 0 || count == int32(messageCount) { // Log progress
					t.Logf("Consumer received message %d/%d. Body: %s, MessageID: %s", count, messageCount, bodyStr, delivery.MessageId)
				}

				if count == int32(messageCount) {
					t.Logf("Consumer received all %d expected messages.", messageCount)
					return
				}
			case <-timeout:
				t.Errorf("Timeout waiting for messages. Received %d out of %d.", atomic.LoadInt32(&receivedCount), messageCount)
				return
			}
		}
	}()

	wg.Wait() // Wait for the consumer goroutine to finish (either all messages received or timeout)

	finalReceivedCount := atomic.LoadInt32(&receivedCount)
	assert.Equal(t, int32(messageCount), finalReceivedCount, "Mismatch in the number of received messages")

	// Additional check: ensure all unique published messages were received at least once
	if int(finalReceivedCount) == messageCount { // Only do this check if we received the correct total number
		for body := range publishedBodies {
			assert.GreaterOrEqual(t, receivedMessages[body], 1, "Expected to receive message body '%s'", body)
		}
		assert.Len(t, receivedMessages, messageCount, "Number of unique message bodies received does not match number published")
	}

	t.Log("High volume delivery test completed.")
}
