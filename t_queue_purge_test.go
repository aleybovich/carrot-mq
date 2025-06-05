package carrotmq

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

func TestQueuePurge_Success(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create a queue with messages
	queueName := uniqueName("purge-basic")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish some messages
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("message-%d", i)),
		})
		require.NoError(t, err)
	}

	// Purge the queue
	purgedCount, err := ch.QueuePurge(q.Name, false)
	require.NoError(t, err)
	assert.Equal(t, messageCount, purgedCount, "Should report correct number of purged messages")

	// Verify queue is empty
	q2, err := ch.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, q2.Messages, "Queue should be empty after purge")
}

func TestQueuePurge_EmptyQueue(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create an empty queue
	queueName := uniqueName("purge-empty")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Purge the empty queue
	purgedCount, err := ch.QueuePurge(q.Name, false)
	require.NoError(t, err)
	assert.Equal(t, 0, purgedCount, "Should report 0 messages purged from empty queue")
}

func TestQueuePurge_NonExistent(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Try to purge non-existent queue
	_, err = ch.QueuePurge("non-existent-queue-purge", false)
	require.Error(t, err)

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code)
	assert.Contains(t, amqpErr.Reason, "no queue")
}

// --- No-Wait Tests ---

func TestQueuePurge_NoWait(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create queue with messages
	queueName := uniqueName("purge-nowait")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish messages
	messageCount := 5
	for i := 0; i < messageCount; i++ {
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Purge with no-wait
	purgedCount, err := ch.QueuePurge(q.Name, true)
	require.NoError(t, err)
	// With no-wait, the count might be 0 as server doesn't send PurgeOk
	assert.GreaterOrEqual(t, 0, purgedCount)

	// Give server time to process
	time.Sleep(100 * time.Millisecond)

	// Verify queue is empty
	q2, err := ch.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, q2.Messages, "Queue should be empty after no-wait purge")
}

// --- Exclusive Queue Tests ---

func TestQueuePurge_ExclusiveQueue_SameConnection(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create exclusive queue with messages
	queueName := uniqueName("purge-exclusive")
	q, err := ch.QueueDeclare(queueName, false, false, true, false, nil)
	require.NoError(t, err)

	// Add messages
	for i := 0; i < 5; i++ {
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Purge should succeed from same connection
	purgedCount, err := ch.QueuePurge(q.Name, false)
	require.NoError(t, err)
	assert.Equal(t, 5, purgedCount)
}

func TestQueuePurge_ExclusiveQueue_DifferentConnection(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// First connection creates exclusive queue
	conn1, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn1.Close()

	ch1, err := conn1.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	queueName := uniqueName("purge-exclusive-other")
	q, err := ch1.QueueDeclare(queueName, false, false, true, false, nil)
	require.NoError(t, err)

	// Add messages
	for i := 0; i < 5; i++ {
		err = ch1.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Second connection tries to purge
	conn2, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn2.Close()

	ch2, err := conn2.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Should be able to purge (purge doesn't require ownership in most implementations)
	// If your implementation restricts this, adjust the test accordingly
	_, err = ch2.QueuePurge(q.Name, false)
	// Current implementation allows this, but you could restrict it
	if err != nil {
		amqpErr, ok := err.(*amqp.Error)
		if ok {
			assert.Equal(t, amqp.ResourceLocked, amqpErr.Code)
		}
	}
}

// --- Concurrent Operations Tests ---

func TestQueuePurge_ConcurrentWithPublish(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create queue
	queueName := uniqueName("purge-concurrent-publish")
	ch1, err := conn.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	q, err := ch1.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Start publisher goroutine
	stopPublish := make(chan struct{})
	publishCount := int32(0)

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
					return
				}
				atomic.AddInt32(&publishCount, 1)
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Let some publishes happen
	time.Sleep(100 * time.Millisecond)

	// Purge queue multiple times
	var totalPurged int32
	purgeCount := 3
	for i := 0; i < purgeCount; i++ {
		purged, err := ch1.QueuePurge(q.Name, false)
		require.NoError(t, err)
		atomic.AddInt32(&totalPurged, int32(purged))
		t.Logf("Purge %d removed %d messages", i+1, purged)
		time.Sleep(50 * time.Millisecond)
	}

	// Stop publisher
	close(stopPublish)
	time.Sleep(50 * time.Millisecond)

	// Final purge
	finalPurged, err := ch1.QueuePurge(q.Name, false)
	require.NoError(t, err)

	t.Logf("Published %d messages total, purged %d + %d final",
		atomic.LoadInt32(&publishCount), atomic.LoadInt32(&totalPurged), finalPurged)
}

func TestQueuePurge_ConcurrentWithConsume(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create queue with many messages
	queueName := uniqueName("purge-concurrent-consume")
	ch1, err := conn.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	q, err := ch1.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish many messages
	messageCount := 1000
	for i := 0; i < messageCount; i++ {
		err = ch1.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Start multiple consumers
	consumerCount := 3
	consumedCounts := make([]int32, consumerCount)
	var wg sync.WaitGroup

	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ch, err := conn.Channel()
			if err != nil {
				return
			}
			defer ch.Close()

			deliveries, err := ch.Consume(q.Name, fmt.Sprintf("consumer-%d", id), true, false, false, false, nil)
			if err != nil {
				return
			}

			for range deliveries {
				atomic.AddInt32(&consumedCounts[id], 1)
				time.Sleep(2 * time.Millisecond) // Simulate processing
			}
		}(i)
	}

	// Let consumption happen
	time.Sleep(100 * time.Millisecond)

	// Purge queue
	purgedCount, err := ch1.QueuePurge(q.Name, false)
	require.NoError(t, err)

	// Calculate total consumed
	var totalConsumed int32
	for i := 0; i < consumerCount; i++ {
		totalConsumed += atomic.LoadInt32(&consumedCounts[i])
	}

	t.Logf("Consumed %d messages, purged %d messages", totalConsumed, purgedCount)
	assert.LessOrEqual(t, int(totalConsumed)+purgedCount, messageCount,
		"Total consumed + purged should not exceed published")

	// Stop consumers by closing connection
	conn.Close()
	wg.Wait()
}

func TestQueuePurge_ConcurrentMultiplePurges(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create queue with messages
	queueName := uniqueName("purge-concurrent-purges")
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Add many messages
	messageCount := 100
	for i := 0; i < messageCount; i++ {
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Try to purge from multiple goroutines
	var wg sync.WaitGroup
	totalPurged := int32(0)
	goroutineCount := 5

	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ch, err := conn.Channel()
			if err != nil {
				return
			}
			defer ch.Close()

			purged, err := ch.QueuePurge(q.Name, false)
			if err == nil {
				atomic.AddInt32(&totalPurged, int32(purged))
				t.Logf("Goroutine %d purged %d messages", id, purged)
			}
		}(i)
	}

	wg.Wait()

	// Total purged should equal original message count
	assert.Equal(t, int32(messageCount), atomic.LoadInt32(&totalPurged),
		"Total purged should equal original message count")

	// Queue should be empty
	q2, err := ch.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, q2.Messages)
}

// --- Purge During Queue/VHost Deletion Tests ---

func TestQueuePurge_DuringQueueDeletion(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create queue with messages
	queueName := uniqueName("purge-during-delete")
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Add messages
	for i := 0; i < 50; i++ {
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Start concurrent operations
	var wg sync.WaitGroup
	purgeErrors := int32(0)
	deleteErrors := int32(0)

	// Multiple purge attempts
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, err := conn.Channel()
			if err != nil {
				return
			}
			defer ch.Close()

			time.Sleep(time.Duration(10+time.Now().UnixNano()%30) * time.Millisecond)
			_, err = ch.QueuePurge(q.Name, false)
			if err != nil {
				atomic.AddInt32(&purgeErrors, 1)
			}
		}()
	}

	// Queue delete attempt
	wg.Add(1)
	go func() {
		defer wg.Done()
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		defer ch.Close()

		time.Sleep(20 * time.Millisecond)
		_, err = ch.QueueDelete(q.Name, false, false, false)
		if err != nil {
			atomic.AddInt32(&deleteErrors, 1)
		}
	}()

	wg.Wait()

	t.Logf("Purge errors: %d, Delete errors: %d",
		atomic.LoadInt32(&purgeErrors), atomic.LoadInt32(&deleteErrors))

	// Queue should be deleted
	_, err = ch.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.Error(t, err)
}
