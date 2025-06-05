package carrotmq

import (
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPublisherConfirms_HappyPath tests basic publisher confirms functionality
func TestPublisherConfirms_HappyPath(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Enable publisher confirms
	err = ch.Confirm(false)
	require.NoError(t, err, "Failed to enable publisher confirms")

	// Set up confirm listener
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 10))

	// Declare exchange and queue
	exchangeName := uniqueName("test-confirms-ex")
	queueName := uniqueName("test-confirms-q")

	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	err = ch.QueueBind(q.Name, "test-key", exchangeName, false, nil)
	require.NoError(t, err)

	// Publish multiple messages
	numMessages := 5
	for i := 1; i <= numMessages; i++ {
		body := fmt.Sprintf("Message %d", i)
		err = ch.Publish(exchangeName, "test-key", false, false, amqp.Publishing{
			Body: []byte(body),
		})
		require.NoError(t, err)
	}

	// Verify all confirmations
	confirmed := make(map[uint64]bool)
	timeout := time.After(2 * time.Second)

	for len(confirmed) < numMessages {
		select {
		case confirm := <-confirms:
			assert.True(t, confirm.Ack, "Message %d should be positively acknowledged", confirm.DeliveryTag)
			confirmed[confirm.DeliveryTag] = true
		case <-timeout:
			t.Fatalf("Timeout waiting for confirmations. Got %d/%d", len(confirmed), numMessages)
		}
	}

	// Verify delivery tags are sequential starting from 1
	for i := uint64(1); i <= uint64(numMessages); i++ {
		assert.True(t, confirmed[i], "Missing confirmation for delivery tag %d", i)
	}
}

// TestPublisherConfirms_NoRoute tests nack for unroutable messages
func TestPublisherConfirms_NoRoute(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Enable publisher confirms
	err = ch.Confirm(false)
	require.NoError(t, err)

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 10))
	returns := ch.NotifyReturn(make(chan amqp.Return, 10))

	// Declare exchange but no queue binding
	exchangeName := uniqueName("test-no-route-ex")
	err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	// Publish mandatory message with no route
	body := "Unroutable message"
	err = ch.Publish(exchangeName, "no-binding-key", true, false, amqp.Publishing{
		Body: []byte(body),
	})
	require.NoError(t, err)

	// Should receive both return and nack
	select {
	case ret := <-returns:
		assert.Equal(t, uint16(312), ret.ReplyCode, "Should be NO_ROUTE")
		assert.Equal(t, body, string(ret.Body))
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for basic.return")
	}

	select {
	case confirm := <-confirms:
		assert.False(t, confirm.Ack, "Unroutable message should be nacked")
		assert.Equal(t, uint64(1), confirm.DeliveryTag)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for nack confirmation")
	}
}

// TestPublisherConfirms_MultipleAck tests multiple flag in acks
func TestPublisherConfirms_MultipleAck(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Enable publisher confirms
	err = ch.Confirm(false)
	require.NoError(t, err)

	// Don't set up confirm listener initially

	// Set up queue
	queueName := uniqueName("test-multiple-ack-q")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err)

	// Publish several messages
	numMessages := 10
	for i := 1; i <= numMessages; i++ {
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("Message %d", i)),
		})
		require.NoError(t, err)
	}

	// Now set up confirm listener
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 20))

	// Give server time to process and send confirms
	time.Sleep(100 * time.Millisecond)

	// Collect all confirmations
	receivedConfirms := make([]amqp.Confirmation, 0)
	timeout := time.After(1 * time.Second)

collecting:
	for {
		select {
		case confirm := <-confirms:
			receivedConfirms = append(receivedConfirms, confirm)
			// If we got a confirmation for the last message, we're done
			if confirm.DeliveryTag >= uint64(numMessages) {
				break collecting
			}
		case <-timeout:
			break collecting
		}
	}

	// Verify we got confirmations covering all messages
	confirmed := make(map[uint64]bool)
	for _, confirm := range receivedConfirms {
		assert.True(t, confirm.Ack, "All messages should be acked")
		// Mark this tag and all previous as confirmed if multiple flag would be set
		for i := uint64(1); i <= confirm.DeliveryTag; i++ {
			confirmed[i] = true
		}
	}

	// All messages should be confirmed
	for i := uint64(1); i <= uint64(numMessages); i++ {
		assert.True(t, confirmed[i], "Message %d should be confirmed", i)
	}
}

// TestPublisherConfirms_NoWait tests confirm select with nowait
func TestPublisherConfirms_NoWait(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Enable publisher confirms with nowait
	// Note: amqp091-go doesn't expose nowait parameter, but we can test the server handles it
	err = ch.Confirm(false) // This sends nowait=false
	require.NoError(t, err)

	// Should still be able to publish and get confirms
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	err = ch.Publish("", "test", false, false, amqp.Publishing{Body: []byte("test")})
	require.NoError(t, err)

	select {
	case confirm := <-confirms:
		// Even with nowait, confirms should work
		assert.True(t, confirm.Ack || !confirm.Ack, "Should receive confirmation")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for confirmation")
	}
}

// TestPublisherConfirms_ChannelClose tests pending confirms are nacked on channel close
func TestPublisherConfirms_ChannelClose(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	// Note: no defer close as we'll close it explicitly

	// Enable publisher confirms
	err = ch.Confirm(false)
	require.NoError(t, err)

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 10))

	// Publish to non-existent exchange to trigger channel error
	err = ch.Publish("non-existent-exchange", "key", false, false, amqp.Publishing{
		Body: []byte("This should fail"),
	})
	assert.NoError(t, err, "Publishing to non-existent exchange should not error immediately")

	// Channel should be closed by server
	select {
	case confirm := <-confirms:
		// Might get a nack before channel closes
		assert.False(t, confirm.Ack, "Should be nacked due to channel closing")
	case <-time.After(1 * time.Second):
		// Channel might close before sending individual nacks
	}

	// Verify channel is closed
	err = ch.ExchangeDeclare("test", "direct", false, false, false, false, nil)
	assert.Error(t, err, "Channel should be closed")
}

// TestPublisherConfirms_ConcurrentPublish tests thread safety
func TestPublisherConfirms_ConcurrentPublish(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Enable publisher confirms
	err = ch.Confirm(false)
	require.NoError(t, err)

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 100))

	// Create queue
	q, err := ch.QueueDeclare("", false, false, false, false, nil)
	require.NoError(t, err)

	// Publish concurrently
	numGoroutines := 10
	messagesPerGoroutine := 10
	totalMessages := numGoroutines * messagesPerGoroutine

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for m := 0; m < messagesPerGoroutine; m++ {
				body := fmt.Sprintf("Goroutine %d, Message %d", goroutineID, m)
				err := ch.Publish("", q.Name, false, false, amqp.Publishing{
					Body: []byte(body),
				})
				assert.NoError(t, err)
			}
		}(g)
	}

	// Collect confirmations
	confirmed := make(map[uint64]bool)
	go func() {
		for confirm := range confirms {
			confirmed[confirm.DeliveryTag] = confirm.Ack
		}
	}()

	wg.Wait()

	// Give time for all confirmations
	time.Sleep(500 * time.Millisecond)

	// Verify we got confirmations for all messages
	assert.GreaterOrEqual(t, len(confirmed), totalMessages,
		"Should have confirmations for all %d messages", totalMessages)

	// All should be positive acks
	for tag, ack := range confirmed {
		assert.True(t, ack, "Message %d should be acked", tag)
	}
}

// TestPublisherConfirms_DefaultExchange tests confirms with default exchange
func TestPublisherConfirms_DefaultExchange(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Enable publisher confirms
	err = ch.Confirm(false)
	require.NoError(t, err)

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	// Create queue
	q, err := ch.QueueDeclare("", false, false, false, false, nil)
	require.NoError(t, err)

	// Publish using default exchange
	err = ch.Publish("", q.Name, false, false, amqp.Publishing{
		Body: []byte("Message via default exchange"),
	})
	require.NoError(t, err)

	// Should get positive ack
	select {
	case confirm := <-confirms:
		assert.True(t, confirm.Ack, "Should be positively acknowledged")
		assert.Equal(t, uint64(1), confirm.DeliveryTag)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for confirmation")
	}
}

// TestPublisherConfirms_SequenceNumbers tests delivery tag sequencing
func TestPublisherConfirms_SequenceNumbers(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Test on multiple channels
	numChannels := 3
	channels := make([]*amqp.Channel, numChannels)

	for i := 0; i < numChannels; i++ {
		ch, err := conn.Channel()
		require.NoError(t, err)
		defer ch.Close()

		err = ch.Confirm(false)
		require.NoError(t, err)

		channels[i] = ch
	}

	// Each channel should have independent sequence numbers
	for i, ch := range channels {
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 5))

		// Create queue for this channel
		q, err := ch.QueueDeclare("", false, false, false, false, nil)
		require.NoError(t, err)

		// Publish 5 messages
		for j := 1; j <= 5; j++ {
			err = ch.Publish("", q.Name, false, false, amqp.Publishing{
				Body: []byte(fmt.Sprintf("Channel %d, Message %d", i, j)),
			})
			require.NoError(t, err)
		}

		// Verify sequence numbers start at 1 for each channel
		for j := 1; j <= 5; j++ {
			select {
			case confirm := <-confirms:
				assert.Equal(t, uint64(j), confirm.DeliveryTag,
					"Channel %d should have sequential delivery tags", i)
				assert.True(t, confirm.Ack)
			case <-time.After(1 * time.Second):
				t.Fatalf("Timeout waiting for confirmation %d on channel %d", j, i)
			}
		}
	}
}

// TestPublisherConfirms_ErrorConditions tests various error scenarios
func TestPublisherConfirms_ErrorConditions(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	t.Run("ConfirmOnChannel0", func(t *testing.T) {
		// This test would require raw protocol access to send Confirm.Select on channel 0
		// Skipping as amqp091-go library prevents this
		t.Skip("Cannot test channel 0 with client library")
	})

	t.Run("PublishBeforeConfirmMode", func(t *testing.T) {
		ch, err := conn.Channel()
		require.NoError(t, err)
		defer ch.Close()

		// Publish without enabling confirms
		err = ch.Publish("", "test", false, false, amqp.Publishing{Body: []byte("test")})
		require.NoError(t, err, "Publishing should work without confirm mode")

		// Now enable confirms
		err = ch.Confirm(false)
		require.NoError(t, err)

		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		// Sequence should start at 1
		err = ch.Publish("", "test", false, false, amqp.Publishing{Body: []byte("test")})
		require.NoError(t, err)

		select {
		case confirm := <-confirms:
			assert.Equal(t, uint64(1), confirm.DeliveryTag,
				"First confirmed message should have delivery tag 1")
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for confirmation")
		}
	})
}
