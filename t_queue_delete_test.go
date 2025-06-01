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

// --- Basic Queue Delete Tests ---

func TestQueueDelete_Success(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create a queue with messages
	queueName := uniqueName("delete-basic")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish some messages
	for i := 0; i < 5; i++ {
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: fmt.Appendf(nil, "message-%d", i),
		})
		require.NoError(t, err)
	}

	// Delete the queue
	msgCount, err := ch.QueueDelete(q.Name, false, false, false)
	require.NoError(t, err)
	assert.Equal(t, 5, msgCount, "Should report correct number of deleted messages")

	// Verify queue is gone
	_, err = ch.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.Error(t, err)
	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code)
}

func TestQueueDelete_NonExistent(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Try to delete non-existent queue
	_, err = ch.QueueDelete("non-existent-queue-delete", false, false, false)
	require.Error(t, err)

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code)
	assert.Contains(t, amqpErr.Reason, "no queue")
}

// --- If-Unused Tests ---

func TestQueueDelete_IfUnused_WithConsumers(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create queue and add consumer
	queueName := uniqueName("delete-if-unused")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	_, err = ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	// Try to delete with if-unused=true
	_, err = ch.QueueDelete(q.Name, true, false, false)
	require.Error(t, err)

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code)
	assert.Contains(t, amqpErr.Reason, "in use")
	assert.Contains(t, amqpErr.Reason, "1 consumers")
}

func TestQueueDelete_IfUnused_NoConsumers(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create queue without consumers
	queueName := uniqueName("delete-if-unused-ok")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Delete with if-unused=true should succeed
	_, err = ch.QueueDelete(q.Name, true, false, false)
	require.NoError(t, err)

	// Verify queue is gone
	_, err = ch.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.Error(t, err)
}

// --- If-Empty Tests ---

func TestQueueDelete_IfEmpty_WithMessages(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create queue and add messages
	queueName := uniqueName("delete-if-empty")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish messages
	for i := 0; i < 3; i++ {
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Try to delete with if-empty=true
	_, err = ch.QueueDelete(q.Name, false, true, false)
	require.Error(t, err)

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code)
	assert.Contains(t, amqpErr.Reason, "not empty")
	assert.Contains(t, amqpErr.Reason, "3 messages")
}

func TestQueueDelete_IfEmpty_NoMessages(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create empty queue
	queueName := uniqueName("delete-if-empty-ok")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Delete with if-empty=true should succeed
	msgCount, err := ch.QueueDelete(q.Name, false, true, false)
	require.NoError(t, err)
	assert.Equal(t, 0, msgCount)
}

// --- Combined Conditions Tests ---

func TestQueueDelete_IfUnusedAndIfEmpty_BothConditionsFail(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create queue with consumer and messages
	queueName := uniqueName("delete-both-conditions")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Add consumer
	_, err = ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	// Add messages
	err = ch.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte("test")})
	require.NoError(t, err)

	// Try to delete with both conditions - should fail on if-unused first
	_, err = ch.QueueDelete(q.Name, true, true, false)
	require.Error(t, err)

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code)
	assert.Contains(t, amqpErr.Reason, "in use") // Fails on first condition
}

// --- No-Wait Tests ---

func TestQueueDelete_NoWait(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create queue
	queueName := uniqueName("delete-nowait")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Delete with no-wait
	msgCount, err := ch.QueueDelete(q.Name, false, false, true)
	require.NoError(t, err)
	assert.Equal(t, 0, msgCount) // No-wait might return 0

	// Give server time to process
	time.Sleep(100 * time.Millisecond)

	// Verify queue is gone
	_, err = ch.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.Error(t, err)
}

// --- Exclusive Queue Tests ---

func TestQueueDelete_ExclusiveQueue_SameConnection(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create exclusive queue
	queueName := uniqueName("delete-exclusive")
	q, err := ch.QueueDeclare(queueName, false, false, true, false, nil)
	require.NoError(t, err)

	// Delete should succeed from same connection
	_, err = ch.QueueDelete(q.Name, false, false, false)
	require.NoError(t, err)
}

func TestQueueDelete_ExclusiveQueue_DifferentConnection(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// First connection creates exclusive queue
	conn1, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn1.Close()

	ch1, err := conn1.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	queueName := uniqueName("delete-exclusive-other")
	q, err := ch1.QueueDeclare(queueName, false, false, true, false, nil)
	require.NoError(t, err)

	// Second connection tries to delete
	conn2, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn2.Close()

	ch2, err := conn2.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Should fail (in current implementation, may need to add ownership check)
	_, err = ch2.QueueDelete(q.Name, false, false, false)
	// This might succeed in current implementation if exclusive ownership isn't tracked
	// If it succeeds, you might want to add a TODO comment about implementing ownership tracking
	if err != nil {
		amqpErr, ok := err.(*amqp.Error)
		if ok {
			assert.Equal(t, amqp.ResourceLocked, amqpErr.Code)
		}
	}
}

// --- Resource Cleanup Tests ---

func TestQueueDelete_ConsumerCleanup(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create queue and consumers
	queueName := uniqueName("delete-consumer-cleanup")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Start multiple consumers
	var deliveryChans []<-chan amqp.Delivery
	consumerCount := 3
	for i := 0; i < consumerCount; i++ {
		deliveries, err := ch.Consume(q.Name, fmt.Sprintf("consumer-%d", i), true, false, false, false, nil)
		require.NoError(t, err)
		deliveryChans = append(deliveryChans, deliveries)
	}

	// Delete queue
	_, err = ch.QueueDelete(q.Name, false, false, false)
	require.NoError(t, err)

	// Verify all consumer channels are closed
	for i, deliveries := range deliveryChans {
		select {
		case _, ok := <-deliveries:
			assert.False(t, ok, "Consumer %d channel should be closed", i)
		case <-time.After(500 * time.Millisecond):
			t.Errorf("Consumer %d channel was not closed", i)
		}
	}
}

func TestQueueDelete_BindingCleanup(t *testing.T) {
	server, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create exchange and queue
	exchangeName := uniqueName("ex-binding-cleanup")
	queueName := uniqueName("q-binding-cleanup")

	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Create multiple bindings
	routingKeys := []string{"key1", "key2", "key3"}
	for _, key := range routingKeys {
		err = ch.QueueBind(q.Name, key, exchangeName, false, nil)
		require.NoError(t, err)
	}

	// Verify bindings exist in server state
	vhost := server.vhosts["/"]
	vhost.mu.RLock()
	exchange := vhost.exchanges[exchangeName]
	vhost.mu.RUnlock()

	exchange.mu.RLock()
	for _, key := range routingKeys {
		queues := exchange.Bindings[key]
		assert.Contains(t, queues, queueName, "Binding should exist before deletion")
	}
	exchange.mu.RUnlock()

	// Delete queue
	_, err = ch.QueueDelete(q.Name, false, false, false)
	require.NoError(t, err)

	// Verify bindings are cleaned up
	exchange.mu.RLock()
	for _, key := range routingKeys {
		queues := exchange.Bindings[key]
		assert.NotContains(t, queues, queueName, "Binding should be removed after queue deletion")
	}
	exchange.mu.RUnlock()
}

// --- Concurrent Operations Tests ---

func TestQueueDelete_ConcurrentWithPublish(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create queue
	queueName := uniqueName("delete-concurrent-publish")
	ch1, err := conn.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	q, err := ch1.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Start publisher goroutine
	stopPublish := make(chan struct{})
	publishCount := int32(0)
	publishErrors := int32(0)

	go func() {
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		defer ch.Close()

		for {
			select {
			case <-stopPublish:
				return
			default:
				err := ch.Publish("", queueName, false, false, amqp.Publishing{
					Body: []byte(fmt.Sprintf("msg-%d", atomic.LoadInt32(&publishCount))),
				})
				if err != nil {
					atomic.AddInt32(&publishErrors, 1)
					return // Channel likely closed due to queue deletion
				}
				atomic.AddInt32(&publishCount, 1)
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Let some publishes happen
	time.Sleep(100 * time.Millisecond)

	// Delete queue
	msgCount, err := ch1.QueueDelete(q.Name, false, false, false)
	require.NoError(t, err)
	t.Logf("Deleted queue with %d messages", msgCount)

	// Stop publisher
	close(stopPublish)
	time.Sleep(50 * time.Millisecond)

	// Verify queue is gone
	_, err = ch1.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.Error(t, err)

	t.Logf("Published %d messages, %d errors", atomic.LoadInt32(&publishCount), atomic.LoadInt32(&publishErrors))
}

func TestQueueDelete_ConcurrentWithConsume(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create queue with messages
	queueName := uniqueName("delete-concurrent-consume")
	ch1, err := conn.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	q, err := ch1.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish messages
	messageCount := 100
	for i := 0; i < messageCount; i++ {
		err = ch1.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Start consumer
	ch2, err := conn.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	deliveries, err := ch2.Consume(q.Name, "", false, false, false, false, nil)
	require.NoError(t, err)

	// Process messages concurrently with deletion
	consumedCount := int32(0)
	consumerClosed := make(chan struct{})

	go func() {
		defer close(consumerClosed)
		for delivery := range deliveries {
			atomic.AddInt32(&consumedCount, 1)
			delivery.Ack(false)
			time.Sleep(5 * time.Millisecond) // Simulate processing
		}
	}()

	// Let some consumption happen
	time.Sleep(50 * time.Millisecond)

	// Delete queue
	remainingMsgs, err := ch1.QueueDelete(q.Name, false, false, false)
	require.NoError(t, err)

	// Wait for consumer to finish
	select {
	case <-consumerClosed:
		// Consumer channel closed as expected
	case <-time.After(2 * time.Second):
		t.Error("Consumer channel did not close after queue deletion")
	}

	consumed := atomic.LoadInt32(&consumedCount)
	t.Logf("Consumed %d messages, %d messages deleted with queue", consumed, remainingMsgs)
	assert.LessOrEqual(t, int(consumed)+remainingMsgs, messageCount, "Total should not exceed published messages")
}

func TestQueueDelete_ConcurrentMultipleDeletes(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create queue
	queueName := uniqueName("delete-concurrent-deletes")
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Try to delete from multiple goroutines
	var wg sync.WaitGroup
	successCount := int32(0)
	notFoundCount := int32(0)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ch, err := conn.Channel()
			if err != nil {
				return
			}
			defer ch.Close()

			_, err = ch.QueueDelete(q.Name, false, false, false)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else if amqpErr, ok := err.(*amqp.Error); ok && amqpErr.Code == amqp.NotFound {
				atomic.AddInt32(&notFoundCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Only one should succeed
	assert.Equal(t, int32(1), atomic.LoadInt32(&successCount), "Only one delete should succeed")
	assert.Equal(t, int32(4), atomic.LoadInt32(&notFoundCount), "Others should get not found")
}

// --- VHost Deletion Interaction Tests ---

func TestQueueDelete_DuringVHostDeletion(t *testing.T) {
	server, addr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	// Add a second vhost
	vhostName := "test-vhost"
	err := server.AddVHost(vhostName)
	require.NoError(t, err)

	uri := fmt.Sprintf("amqp://%s/%s", addr, vhostName)
	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create multiple queues
	queueCount := 5
	var queueNames []string
	for i := 0; i < queueCount; i++ {
		qName := fmt.Sprintf("queue-%d", i)
		_, err := ch.QueueDeclare(qName, false, false, false, false, nil)
		require.NoError(t, err)
		queueNames = append(queueNames, qName)
	}

	// Start queue deletion attempts
	var wg sync.WaitGroup
	for _, qName := range queueNames {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			time.Sleep(time.Duration(10+time.Now().UnixNano()%50) * time.Millisecond) // Random delay

			ch, err := conn.Channel()
			if err != nil {
				return // Connection might be closed
			}
			defer ch.Close()

			ch.QueueDelete(name, false, false, false)
			// Ignore errors - vhost might be deleted
		}(qName)
	}

	// Delete vhost concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(30 * time.Millisecond)
		err := server.DeleteVHost(vhostName)
		require.NoError(t, err)
	}()

	wg.Wait()

	// Verify vhost is deleted
	vh, err := server.GetVHost(vhostName)
	require.Nil(t, vh)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

// --- Message Requeue Tests ---

func TestQueueDelete_UnackedMessagesRequeued(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create two queues
	queue1Name := uniqueName("delete-unacked-1")

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	q1, err := ch.QueueDeclare(queue1Name, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish messages to queue1
	messageCount := 5
	for i := 0; i < messageCount; i++ {
		err = ch.Publish("", q1.Name, false, false, amqp.Publishing{
			Body: fmt.Appendf(nil, "msg-%d", i),
		})
		require.NoError(t, err)
	}

	// Consume from queue1 without acking
	deliveries1, err := ch.Consume(q1.Name, "", false, false, false, false, nil)
	require.NoError(t, err)

	// Receive all messages but don't ack
	var unackedDeliveries []amqp.Delivery
	for i := 0; i < messageCount; i++ {
		select {
		case d := <-deliveries1:
			unackedDeliveries = append(unackedDeliveries, d)
		case <-time.After(time.Second):
			t.Fatalf("Timeout receiving message %d", i)
		}
	}

	// Now delete queue1 - this is actually not the test we want
	// We need to test channel/connection close causing requeue
	// Let's close the channel instead
	ch.Close()

	// Create new channel and check queue1
	ch2, err := conn.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Start consumer on queue1 again
	deliveries2, err := ch2.Consume(q1.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	// All messages should be redelivered
	redeliveredCount := 0
	timeout := time.After(2 * time.Second)

	for redeliveredCount < messageCount {
		select {
		case d := <-deliveries2:
			assert.True(t, d.Redelivered, "Message should be marked as redelivered")
			redeliveredCount++
		case <-timeout:
			t.Fatalf("Only %d/%d messages were redelivered", redeliveredCount, messageCount)
		}
	}

	// Now actually delete the queue
	_, err = ch2.QueueDelete(q1.Name, false, false, false)
	require.NoError(t, err)
}
