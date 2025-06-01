package main

import (
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Add these tests to t_server_test.go

func TestExchangeDelete_Success(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "test-delete-ex"

	// Create exchange
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare exchange")

	// Delete it
	err = ch.ExchangeDelete(exchangeName, false, false)
	require.NoError(t, err, "Failed to delete exchange")

	// Verify it's gone by trying to declare passively
	err = ch.ExchangeDeclarePassive(exchangeName, "direct", false, false, false, false, nil)
	require.Error(t, err, "Exchange should not exist after deletion")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "Should get 404 NOT_FOUND")
}

func TestExchangeDelete_DefaultExchange(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Try to delete the default exchange
	err = ch.ExchangeDelete("", false, false)
	require.Error(t, err, "Should not be able to delete default exchange")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.AccessRefused, amqpErr.Code, "Should get 403 ACCESS_REFUSED")
	assert.Contains(t, amqpErr.Reason, "cannot delete default exchange")
}

func TestExchangeDelete_NotFound(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Try to delete non-existent exchange
	err = ch.ExchangeDelete("non-existent-exchange", false, false)
	require.Error(t, err, "Should fail to delete non-existent exchange")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "Should get 404 NOT_FOUND")
}

func TestExchangeDelete_IfUnused_WithBindings(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "test-if-unused-ex"
	queueName := "test-if-unused-q"

	// Create exchange and queue
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Bind queue to exchange
	err = ch.QueueBind(q.Name, "test-key", exchangeName, false, nil)
	require.NoError(t, err)

	// Try to delete with if-unused=true
	err = ch.ExchangeDelete(exchangeName, true, false) // if-unused = true
	require.Error(t, err, "Should fail to delete exchange with bindings when if-unused=true")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok)
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "Should get 406 PRECONDITION_FAILED")
	assert.Contains(t, amqpErr.Reason, "in use")
}

func TestExchangeDelete_IfUnused_NoBindings(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "test-if-unused-nobind-ex"

	// Create exchange without any bindings
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	// Delete with if-unused=true should succeed
	err = ch.ExchangeDelete(exchangeName, true, false) // if-unused = true
	require.NoError(t, err, "Should succeed deleting unused exchange")

	// Verify it's gone
	err = ch.ExchangeDeclarePassive(exchangeName, "direct", false, false, false, false, nil)
	require.Error(t, err)
}

func TestExchangeDelete_NoWait(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "test-nowait-delete-ex"

	// Create exchange
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	// Delete with no-wait
	err = ch.ExchangeDelete(exchangeName, false, true) // no-wait = true
	require.NoError(t, err, "No-wait delete should not return error")

	// Give server time to process
	time.Sleep(100 * time.Millisecond)

	// Verify it's gone
	err = ch.ExchangeDeclarePassive(exchangeName, "direct", false, false, false, false, nil)
	require.Error(t, err, "Exchange should be deleted")
}

func TestExchangeDelete_CleansUpQueueBindings(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := "test-cleanup-ex"
	queueName := "test-cleanup-q"
	routingKey := "test-key"

	// Create exchange and queue
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Bind queue to exchange
	err = ch.QueueBind(q.Name, routingKey, exchangeName, false, nil)
	require.NoError(t, err)

	// Start consumer to verify messages are routed
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Verify binding works
	body := "test message"
	err = ch.Publish(exchangeName, routingKey, false, false, amqp.Publishing{Body: []byte(body)})
	require.NoError(t, err)

	select {
	case msg := <-msgs:
		assert.Equal(t, body, string(msg.Body))
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Should receive message before exchange deletion")
	}

	// Delete the exchange (without if-unused, so it should succeed despite bindings)
	err = ch.ExchangeDelete(exchangeName, false, false)
	require.NoError(t, err)

	// Set up channel close notification before publishing
	closeCh := make(chan *amqp.Error, 1)
	ch.NotifyClose(closeCh)

	// Try to publish to deleted exchange - the publish call itself may succeed
	// but the server will close the channel with 404
	err = ch.Publish(exchangeName, routingKey, false, false, amqp.Publishing{Body: []byte("should fail")})

	// Either the publish immediately fails (unlikely with async publish)
	// or the channel gets closed by the server
	if err == nil {
		// Wait for channel close notification
		select {
		case amqpErr := <-closeCh:
			require.NotNil(t, amqpErr, "Channel should be closed by server")
			assert.Equal(t, amqp.NotFound, amqpErr.Code, "Should get 404 NOT_FOUND")
			assert.Contains(t, amqpErr.Reason, exchangeName, "Error should mention the exchange")
		case <-time.After(1 * time.Second):
			// If no close notification, try another operation to check channel state
			err = ch.ExchangeDeclare("probe-exchange", "direct", false, false, false, false, nil)
			require.Error(t, err, "Channel should be closed after publishing to deleted exchange")
		}
	} else {
		// If publish returned error immediately, verify it's the right error
		if amqpErr, ok := err.(*amqp.Error); ok {
			assert.Equal(t, amqp.NotFound, amqpErr.Code, "Should get 404 NOT_FOUND")
		}
	}

	// The binding should have been cleaned up from the queue
	// We need a new channel since the previous one was closed
	ch2, err := conn.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Try to unbind - should fail with 404 since exchange no longer exists
	err = ch2.QueueUnbind(q.Name, routingKey, exchangeName, nil)
	require.Error(t, err, "Unbind should fail when exchange doesn't exist")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Error should be AMQP error")
	assert.Equal(t, amqp.NotFound, amqpErr.Code, "Should get 404 NOT_FOUND for non-existent exchange")
}

func TestExchangeDelete_RaceWithQueueBind(t *testing.T) {
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

	exchangeName := "race-delete-ex"
	queueName := "race-delete-q"

	// Create exchange and queue
	err = ch1.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	_, err = ch1.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	var wg sync.WaitGroup
	var bindErr, deleteErr error

	// Race: bind vs delete
	wg.Add(2)

	go func() {
		defer wg.Done()
		bindErr = ch1.QueueBind(queueName, "key", exchangeName, false, nil)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond) // Small delay to increase race likelihood
		deleteErr = ch2.ExchangeDelete(exchangeName, false, false)
	}()

	wg.Wait()

	// One should succeed, or both could succeed if bind completed first
	bindSucceeded := bindErr == nil
	deleteSucceeded := deleteErr == nil

	assert.True(t, bindSucceeded || deleteSucceeded,
		"At least one operation should succeed")

	if !bindSucceeded {
		amqpErr, ok := bindErr.(*amqp.Error)
		require.True(t, ok)
		assert.True(t, amqpErr.Code == 404 || amqpErr.Code == 409,
			"Bind should fail with 404 or 409, got %d: %s", amqpErr.Code, amqpErr.Reason)
	}

	if !deleteSucceeded {
		// Delete might fail if bind created a binding and if-unused wasn't set
		t.Logf("Delete failed (possibly due to race): %v", deleteErr)
	}
}

func TestExchangeDelete_MultipleDeletions(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	exchangeName := "multi-delete-ex"

	// Create exchange
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	// Create multiple channels for concurrent deletion
	channels := make([]*amqp.Channel, 5)
	for i := 0; i < 5; i++ {
		channels[i], err = conn.Channel()
		require.NoError(t, err)
		defer channels[i].Close()
	}

	// Try to delete from multiple goroutines
	var wg sync.WaitGroup
	errors := make([]error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errors[idx] = channels[idx].ExchangeDelete(exchangeName, false, false)
		}(i)
	}

	wg.Wait()

	// Exactly one should succeed, others should get 404 or 409
	successCount := 0
	for i, err := range errors {
		if err == nil {
			successCount++
		} else {
			amqpErr, ok := err.(*amqp.Error)
			require.True(t, ok, "Error %d should be AMQP error", i)
			assert.True(t, amqpErr.Code == 404 || amqpErr.Code == 409,
				"Error %d should be 404 or 409, got %d: %s", i, amqpErr.Code, amqpErr.Reason)
		}
	}

	assert.Equal(t, 1, successCount, "Exactly one deletion should succeed")
}
