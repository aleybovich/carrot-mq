package carrotmq

import (
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// TestServerHeartbeatsKeepConnectionAlive demonstrates that server heartbeats are essential for keeping idle connections alive
// With server heartbeats enabled (current state), the connection should remain open during idle periods
func TestServerHeartbeatsKeepConnectionAlive(t *testing.T) {
	// Setup server (with heartbeats enabled by default)
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect with short heartbeat interval
	config := amqp.Config{
		Heartbeat: 2 * time.Second,
	}

	conn, err := amqp.DialConfig("amqp://"+addr, config)
	require.NoError(t, err)
	defer conn.Close()

	// Set up connection close notification
	connCloseChan := make(chan *amqp.Error, 1)
	conn.NotifyClose(connCloseChan)

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create a queue
	qName := uniqueName("q-heartbeat-alive-test")
	_, err = ch.QueueDeclare(
		qName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	require.NoError(t, err)

	// Start a consumer
	msgs, err := ch.Consume(
		qName,
		"test-consumer",
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	require.NoError(t, err)

	// Track when consumer stops
	consumerStopped := make(chan bool, 1)
	go func() {
		for range msgs {
			// Just consume messages
		}
		consumerStopped <- true
	}()

	// Wait for longer than the heartbeat timeout would be
	// With server heartbeats, connection should stay alive
	idleTime := config.Heartbeat * 3
	t.Logf("Keeping connection idle for %v (heartbeat interval is %v)", idleTime, config.Heartbeat)
	t.Log("With server heartbeats enabled, connection should remain open")

	select {
	case closeErr := <-connCloseChan:
		// This would indicate a problem - connection shouldn't close with heartbeats
		t.Fatalf("Connection closed unexpectedly during idle period: %v", closeErr)
	case <-time.After(idleTime):
		// Good - connection stayed alive
		t.Log("✓ Connection remained open during idle period (server heartbeats working)")
	}

	// Verify we can still use the connection
	err = ch.Publish(
		"",    // exchange
		qName, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("test message after idle"),
		},
	)
	require.NoError(t, err, "Should be able to publish after idle period")

	t.Log("✓ Successfully published message after idle period")
}

// TestHeartbeatWithActiveTraffic verifies that regular traffic doesn't interfere with heartbeats
func TestHeartbeatWithActiveTraffic(t *testing.T) {
	// Setup server
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect with short heartbeat
	config := amqp.Config{
		Heartbeat: 1 * time.Second, // Very short for testing
	}

	conn, err := amqp.DialConfig("amqp://"+addr, config)
	require.NoError(t, err)
	defer conn.Close()

	// Set up connection close notification
	connCloseChan := make(chan *amqp.Error, 1)
	conn.NotifyClose(connCloseChan)

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Create a queue
	qName := uniqueName("q-heartbeat-traffic-test")
	_, err = ch.QueueDeclare(
		qName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	require.NoError(t, err)

	// Keep publishing messages at intervals less than heartbeat
	stopPublishing := make(chan bool)
	var publishWg sync.WaitGroup
	publishWg.Add(1)

	go func() {
		defer publishWg.Done()
		ticker := time.NewTicker(500 * time.Millisecond) // Publish every 0.5s
		defer ticker.Stop()

		msgCount := 0
		for {
			select {
			case <-ticker.C:
				msgCount++
				err := ch.Publish(
					"",    // exchange
					qName, // routing key
					false, // mandatory
					false, // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(time.Now().Format(time.RFC3339)),
					},
				)
				if err != nil {
					t.Logf("Publish error after %d messages: %v", msgCount, err)
					return
				}
			case <-stopPublishing:
				t.Logf("Published %d messages total", msgCount)
				return
			}
		}
	}()

	// Run for several heartbeat intervals
	testDuration := config.Heartbeat * 5
	t.Logf("Running test for %v with heartbeat interval %v", testDuration, config.Heartbeat)

	select {
	case closeErr := <-connCloseChan:
		t.Fatalf("Connection closed unexpectedly: %v", closeErr)
	case <-time.After(testDuration):
		// Good - connection stayed alive with active traffic
		t.Log("✓ Connection remained open with active traffic and heartbeats")
	}

	// Stop publishing
	close(stopPublishing)
	publishWg.Wait()
}
