package carrotmq

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wakeupDeadline is the upper bound an idle peer consumer should take to receive
// a requeued message. The server's fallback delivery poll runs every ~100 ms,
// so anything well below that proves the wake() path fired.
const wakeupDeadline = 40 * time.Millisecond

// TestCleanupConnectionResources_WakesPeerConsumer asserts that when a publisher
// connection dies with an unacked message, an idle consumer on a peer connection
// is woken immediately rather than waiting out the 100 ms fallback poll.
//
// Exercises the wake() call in cleanupConnectionResources().
func TestCleanupConnectionResources_WakesPeerConsumer(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	qName := uniqueName("q-cleanup-wake")
	msgBody := "CleanupWakeMsg"

	// Connection A: will receive the message, then die without acking.
	connA, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	chA, err := connA.Channel()
	require.NoError(t, err)
	_, deliveriesA, _ := t_setupQueueAndConsumer(t, chA, qName, false) // autoAck=false

	t_publishMessage(t, chA, "", qName, msgBody, false, amqp.Publishing{})
	t_expectMessage(t, deliveriesA, msgBody, ptr(false), 1*time.Second)
	// Do not ack — connection close will requeue.

	// Connection B: idle peer consumer, attached BEFORE the requeue happens,
	// so it must be woken by the cleanup path (not by a fresh consume poll).
	connB, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer connB.Close()
	chB, err := connB.Channel()
	require.NoError(t, err)
	defer chB.Close()
	deliveriesB, _ := t_consumeMessage(t, chB, qName, uniqueName("consumer-B"), false)

	// Give B a moment to register its consumer with the server.
	time.Sleep(50 * time.Millisecond)

	// Kill A; server should requeue and wake B.
	start := time.Now()
	require.NoError(t, connA.Close())

	msg := t_expectMessage(t, deliveriesB, msgBody, ptr(true), 1*time.Second)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, wakeupDeadline,
		"peer consumer should be woken within %s of connection-cleanup requeue, took %s",
		wakeupDeadline, elapsed)
	require.NoError(t, chB.Ack(msg.DeliveryTag, false))
}

// TestForceRemoveChannel_WakesPeerConsumer asserts that closing a channel
// (not the whole connection) with an unacked message wakes an idle peer
// consumer on the same connection immediately.
//
// Exercises the wake() call in forceRemoveChannel().
func TestForceRemoveChannel_WakesPeerConsumer(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	qName := uniqueName("q-chclose-wake")
	msgBody := "ChannelCloseWakeMsg"

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	// Channel A: receives the message, then closes without acking.
	chA, err := conn.Channel()
	require.NoError(t, err)
	_, deliveriesA, _ := t_setupQueueAndConsumer(t, chA, qName, false) // autoAck=false

	t_publishMessage(t, chA, "", qName, msgBody, false, amqp.Publishing{})
	t_expectMessage(t, deliveriesA, msgBody, ptr(false), 1*time.Second)

	// Channel B: idle peer consumer on the same connection, attached BEFORE
	// the requeue. Must be woken by forceRemoveChannel, not by a poll.
	chB, err := conn.Channel()
	require.NoError(t, err)
	defer chB.Close()
	deliveriesB, _ := t_consumeMessage(t, chB, qName, uniqueName("consumer-B"), false)

	time.Sleep(50 * time.Millisecond)

	// Close channel A; server should requeue and wake B.
	start := time.Now()
	require.NoError(t, chA.Close())

	msg := t_expectMessage(t, deliveriesB, msgBody, ptr(true), 1*time.Second)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, wakeupDeadline,
		"peer consumer should be woken within %s of channel-close requeue, took %s",
		wakeupDeadline, elapsed)
	require.NoError(t, chB.Ack(msg.DeliveryTag, false))
}
