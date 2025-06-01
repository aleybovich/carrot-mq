package main

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueUnbind_Success(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create exchange and queue
	exchangeName := "unbind-test-ex"
	queueName := "unbind-test-q"
	routingKey := "unbind-key"

	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Bind the queue
	err = ch.QueueBind(q.Name, routingKey, exchangeName, false, nil)
	require.NoError(t, err)

	// Start consumer to verify messages are routed
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Publish a message - should be routed
	body1 := "message before unbind"
	err = ch.Publish(exchangeName, routingKey, false, false, amqp.Publishing{Body: []byte(body1)})
	require.NoError(t, err)

	// Verify message is received
	select {
	case msg := <-msgs:
		assert.Equal(t, body1, string(msg.Body))
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for message before unbind")
	}

	// Unbind the queue
	err = ch.QueueUnbind(q.Name, routingKey, exchangeName, nil)
	require.NoError(t, err, "Queue unbind should succeed")

	// Publish another message - should NOT be routed
	body2 := "message after unbind"
	err = ch.Publish(exchangeName, routingKey, false, false, amqp.Publishing{Body: []byte(body2)})
	require.NoError(t, err)

	// Verify message is NOT received
	select {
	case msg := <-msgs:
		assert.Fail(t, "Should not receive message after unbind", "Body: %s", string(msg.Body))
	case <-time.After(200 * time.Millisecond):
		// Expected: no message
	}
}

func TestQueueUnbind_QueueNotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "unbind-test-ex-qnf"
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	// Try to unbind non-existent queue
	err = ch.QueueUnbind("non-existent-queue", "key", exchangeName, nil)
	require.Error(t, err, "Unbind non-existent queue should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "Should return 404 NOT_FOUND")
}

func TestQueueUnbind_ExchangeNotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	q, err := ch.QueueDeclare("unbind-test-q-enf", false, false, false, false, nil)
	require.NoError(t, err)

	// Try to unbind from non-existent exchange
	err = ch.QueueUnbind(q.Name, "key", "non-existent-exchange", nil)
	require.Error(t, err, "Unbind from non-existent exchange should fail")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "Should return 404 NOT_FOUND")
}

func TestQueueUnbind_NonExistentBinding(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create exchange and queue
	exchangeName := "unbind-test-ex-neb"
	queueName := "unbind-test-q-neb"

	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Try to unbind a non-existent binding - should succeed (idempotent)
	err = ch.QueueUnbind(q.Name, "non-existent-key", exchangeName, nil)
	require.NoError(t, err, "Unbind non-existent binding should succeed (idempotent)")
}

func TestQueueUnbind_MultipleBindings(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create exchange and queue
	exchangeName := "unbind-test-ex-mb"
	queueName := "unbind-test-q-mb"
	routingKey1 := "key1"
	routingKey2 := "key2"

	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Bind with two different routing keys
	err = ch.QueueBind(q.Name, routingKey1, exchangeName, false, nil)
	require.NoError(t, err)
	err = ch.QueueBind(q.Name, routingKey2, exchangeName, false, nil)
	require.NoError(t, err)

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Unbind only the first key
	err = ch.QueueUnbind(q.Name, routingKey1, exchangeName, nil)
	require.NoError(t, err)

	// Message to key1 should NOT be routed
	err = ch.Publish(exchangeName, routingKey1, false, false, amqp.Publishing{Body: []byte("msg1")})
	require.NoError(t, err)

	// Message to key2 should still be routed
	err = ch.Publish(exchangeName, routingKey2, false, false, amqp.Publishing{Body: []byte("msg2")})
	require.NoError(t, err)

	// Should only receive message for key2
	select {
	case msg := <-msgs:
		assert.Equal(t, "msg2", string(msg.Body), "Should receive message for key2")
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for message")
	}

	// Should not receive any more messages
	select {
	case msg := <-msgs:
		assert.Fail(t, "Should not receive message for key1", "Body: %s", string(msg.Body))
	case <-time.After(200 * time.Millisecond):
		// Expected
	}
}

// -------- RACE CONDITION TESTS --------
func TestQueueBind_RaceWithQueueDelete_Protected(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Create channels for different operations
	ch1, err := conn.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	ch2, err := conn.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Setup
	exchangeName := "race-protected-ex"
	queueName := "race-protected-q"

	err = ch1.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	_, err = ch1.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	var wg sync.WaitGroup
	bindErrors := make([]error, 0)
	deleteErrors := make([]error, 0)
	var mu sync.Mutex

	// Goroutine 1: Try to bind
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ch1.QueueBind(queueName, "key1", exchangeName, false, nil)
		mu.Lock()
		bindErrors = append(bindErrors, err)
		mu.Unlock()
	}()

	// Goroutine 2: Try to delete queue concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Small delay to increase chance of concurrent execution
		time.Sleep(5 * time.Millisecond)
		_, err := ch2.QueueDelete(queueName, false, false, false)
		mu.Lock()
		deleteErrors = append(deleteErrors, err)
		mu.Unlock()
	}()

	wg.Wait()

	// Analyze results - with proper locking, one must succeed and one must fail
	// OR both could succeed if bind completed before delete started
	mu.Lock()
	defer mu.Unlock()

	// If bind succeeded, delete might have succeeded or failed (409)
	// If bind failed (404), delete must have succeeded
	// They cannot both fail with race-related errors

	bindSucceeded := len(bindErrors) == 0 || bindErrors[0] == nil
	deleteSucceeded := len(deleteErrors) == 0 || deleteErrors[0] == nil

	if !bindSucceeded && !deleteSucceeded {
		t.Error("Both operations failed - this suggests a problem")
	}

	if !bindSucceeded {
		// Bind failed - should be 404 (queue not found) or 409 (queue being deleted)
		amqpErr, ok := bindErrors[0].(*amqp.Error)
		require.True(t, ok, "Bind error should be AMQP error")
		assert.True(t, amqpErr.Code == 404 || amqpErr.Code == 409,
			"Bind error should be 404 or 409, got %d: %s", amqpErr.Code, amqpErr.Reason)
	}

	if !deleteSucceeded {
		// Delete failed - might be 404 if bind created binding that prevents deletion
		// or other valid AMQP errors
		amqpErr, ok := deleteErrors[0].(*amqp.Error)
		require.True(t, ok, "Delete error should be AMQP error")
		t.Logf("Delete failed with code %d: %s", amqpErr.Code, amqpErr.Reason)
	}
}

func TestQueueUnbind_RaceWithQueueDelete_Protected(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch1, err := conn.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	ch2, err := conn.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Setup with existing binding
	exchangeName := "unbind-race-ex"
	queueName := "unbind-race-q"
	routingKey := "test-key"

	err = ch1.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	_, err = ch1.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	err = ch1.QueueBind(queueName, routingKey, exchangeName, false, nil)
	require.NoError(t, err)

	var wg sync.WaitGroup
	var unbindErr, deleteErr error

	// Race: unbind vs delete
	wg.Add(2)

	go func() {
		defer wg.Done()
		unbindErr = ch1.QueueUnbind(queueName, routingKey, exchangeName, nil)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_, deleteErr = ch2.QueueDelete(queueName, false, false, false)
	}()

	wg.Wait()

	// One should succeed, or both could succeed if unbind finished first
	unbindSucceeded := unbindErr == nil
	deleteSucceeded := deleteErr == nil

	assert.True(t, unbindSucceeded || deleteSucceeded,
		"At least one operation should succeed")

	if !unbindSucceeded {
		amqpErr, ok := unbindErr.(*amqp.Error)
		require.True(t, ok)
		assert.True(t, amqpErr.Code == 404 || amqpErr.Code == 409,
			"Unbind should fail with 404 or 409, got %d", amqpErr.Code)
	}
}

func TestQueueBind_ConcurrentSameBinding_Protected(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Setup
	exchangeName := "concurrent-bind-protected-ex"
	queueName := "concurrent-bind-protected-q"
	routingKey := "test-key"

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Create multiple channels
	const numGoroutines = 20
	channels := make([]*amqp.Channel, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		channels[i], err = conn.Channel()
		require.NoError(t, err)
		defer channels[i].Close()
	}

	// All try to create the same binding concurrently
	var wg sync.WaitGroup
	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errors[idx] = channels[idx].QueueBind(queueName, routingKey, exchangeName, false, nil)
		}(i)
	}

	wg.Wait()

	// All operations should succeed (binding is idempotent)
	for i, err := range errors {
		assert.NoError(t, err, "Goroutine %d should successfully bind", i)
	}

	// Verify binding works and exists only once
	msgs, err := ch.Consume(queueName, "", true, false, false, false, nil)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// Publish one message
	body := "test message"
	err = ch.Publish(exchangeName, routingKey, false, false, amqp.Publishing{Body: []byte(body)})
	require.NoError(t, err)

	// Should receive exactly one message (not duplicates)
	select {
	case msg := <-msgs:
		assert.Equal(t, body, string(msg.Body))
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for message")
	}

	// Ensure no duplicates
	select {
	case <-msgs:
		assert.Fail(t, "Received duplicate message - binding was created multiple times")
	case <-time.After(200 * time.Millisecond):
		// Good - no duplicates
	}
}

// func TestQueueBindUnbind_RaceWithExchangeDelete(t *testing.T) {
// 	addr, cleanup := setupTestServer(t)
// 	defer cleanup()

// 	conn, err := amqp.Dial("amqp://" + addr)
// 	require.NoError(t, err)
// 	defer conn.Close()

// 	ch1, err := conn.Channel()
// 	require.NoError(t, err)
// 	defer ch1.Close()

// 	ch2, err := conn.Channel()
// 	require.NoError(t, err)
// 	defer ch2.Close()

// 	// Note: Exchange deletion is not implemented in the server yet,
// 	// but this test structure shows what would be tested
// 	t.Skip("Exchange deletion not implemented yet")

// 	exchangeName := "delete-race-ex"
// 	queueName := "delete-race-q"

// 	err = ch1.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
// 	require.NoError(t, err)

// 	_, err = ch1.QueueDeclare(queueName, false, false, false, false, nil)
// 	require.NoError(t, err)

// 	var wg sync.WaitGroup
// 	var bindErr, deleteErr error

// 	wg.Add(2)

// 	go func() {
// 		defer wg.Done()
// 		bindErr = ch1.QueueBind(queueName, "key", exchangeName, false, nil)
// 	}()

// 	go func() {
// 		defer wg.Done()
// 		time.Sleep(5 * time.Millisecond)
// 		// deleteErr = ch2.ExchangeDelete(exchangeName, false, false)
// 		// When implemented, should return proper error codes
// 	}()

// 	wg.Wait()

// 	// Similar assertions as queue delete test
// }

func TestQueueBind_StressTest_NoDeadlock(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	const (
		numQueues     = 10
		numExchanges  = 5
		numOperations = 100
		numWorkers    = 20
	)

	// Create exchanges
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	for i := 0; i < numExchanges; i++ {
		err = ch.ExchangeDeclare(fmt.Sprintf("stress-ex-%d", i), "direct", false, false, false, false, nil)
		require.NoError(t, err)
	}

	// Create queues
	for i := 0; i < numQueues; i++ {
		_, err = ch.QueueDeclare(fmt.Sprintf("stress-q-%d", i), false, false, false, false, nil)
		require.NoError(t, err)
	}

	// Create worker channels
	workers := make([]*amqp.Channel, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i], err = conn.Channel()
		require.NoError(t, err)
		defer workers[i].Close()
	}

	// Stress test with random operations
	var wg sync.WaitGroup
	start := time.Now()
	timeout := time.After(10 * time.Second)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			ch := workers[workerIdx]

			for j := 0; j < numOperations; j++ {
				select {
				case <-timeout:
					return
				default:
				}

				queueIdx := rand.Intn(numQueues)
				exchangeIdx := rand.Intn(numExchanges)
				queueName := fmt.Sprintf("stress-q-%d", queueIdx)
				exchangeName := fmt.Sprintf("stress-ex-%d", exchangeIdx)
				routingKey := fmt.Sprintf("key-%d-%d", queueIdx, exchangeIdx)

				operation := rand.Intn(4)
				switch operation {
				case 0, 1: // Bind (more common)
					ch.QueueBind(queueName, routingKey, exchangeName, false, nil)
				case 2: // Unbind
					ch.QueueUnbind(queueName, routingKey, exchangeName, nil)
				case 3: // Queue delete and recreate
					ch.QueueDelete(queueName, false, false, false)
					ch.QueueDeclare(queueName, false, false, false, false, nil)
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		t.Logf("Stress test completed successfully in %v", elapsed)
	case <-timeout:
		t.Fatal("Stress test timed out - possible deadlock")
	}
}
