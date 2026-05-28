package carrotmq

// Integration tests for exclusive queue auto-deletion on connection close.
//
// Per AMQP 0-9-1 spec section 3.1.3, exclusive queues MUST be deleted when
// the connection that declared them closes.

import (
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExclusiveQueue_DeletedOnConnectionClose verifies that an exclusive queue
// is automatically deleted when its declaring connection closes.  After the
// close, a second connection must be able to re-declare the same name without
// any conflict.
func TestExclusiveQueue_DeletedOnConnectionClose(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connection A declares an exclusive queue.
	connA, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)

	chA, err := connA.Channel()
	require.NoError(t, err)

	queueName := uniqueName("excl-auto-del")
	_, err = chA.QueueDeclare(queueName, false, false, true, false, nil)
	require.NoError(t, err, "connection A should declare exclusive queue successfully")

	// Close connection A — the server must now auto-delete the exclusive queue.
	err = connA.Close()
	require.NoError(t, err)

	// Allow the server time to process the TCP close and run cleanup.
	time.Sleep(200 * time.Millisecond)

	// Connection B re-declares the same name as a non-exclusive queue.
	// This succeeds only if the queue was truly deleted on connection A's close.
	connB, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer connB.Close()

	chB, err := connB.Channel()
	require.NoError(t, err)
	defer chB.Close()

	_, err = chB.QueueDeclare(queueName, false, false, false, false, nil)
	assert.NoError(t, err, "connection B should be able to declare the queue name after connection A's exclusive queue was auto-deleted")
}

// TestExclusiveQueue_DeletedOnConnectionClose_MessagesLost verifies that
// messages published to an exclusive queue are lost when the declaring
// connection closes (the queue is deleted, not merely emptied).  A second
// connection re-declares the same name and should see zero messages.
func TestExclusiveQueue_DeletedOnConnectionClose_MessagesLost(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connection A declares an exclusive queue and publishes messages.
	connA, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)

	chA, err := connA.Channel()
	require.NoError(t, err)

	queueName := uniqueName("excl-msgs-lost")
	_, err = chA.QueueDeclare(queueName, false, false, true, false, nil)
	require.NoError(t, err, "connection A should declare exclusive queue successfully")

	const messageCount = 5
	for i := range messageCount {
		err = chA.Publish("", queueName, false, false, amqp.Publishing{
			Body: fmt.Appendf(nil, "message-%d", i),
		})
		require.NoError(t, err)
	}

	// Close connection A — queue and all messages must be discarded.
	err = connA.Close()
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Connection B re-declares the same name.  If the queue was deleted the
	// declaration succeeds and the returned message count is 0.
	connB, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer connB.Close()

	chB, err := connB.Channel()
	require.NoError(t, err)
	defer chB.Close()

	q, err := chB.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "connection B should be able to declare the queue after auto-deletion")
	assert.Equal(t, 0, q.Messages, "re-declared queue should contain 0 messages — old messages were discarded with the deleted queue")
}

// TestExclusiveQueue_NotDeletedWhileConnectionAlive is a baseline sanity check.
// While the declaring connection is still open, the exclusive queue must exist
// and its messages must remain accessible.
func TestExclusiveQueue_NotDeletedWhileConnectionAlive(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := uniqueName("excl-alive")
	_, err = ch.QueueDeclare(queueName, false, false, true, false, nil)
	require.NoError(t, err, "should declare exclusive queue successfully")

	const messageCount = 3
	for i := range messageCount {
		err = ch.Publish("", queueName, false, false, amqp.Publishing{
			Body: fmt.Appendf(nil, "msg-%d", i),
		})
		require.NoError(t, err)
	}

	// Passive declare on the same connection must succeed and report the messages.
	q, err := ch.QueueDeclarePassive(queueName, false, false, true, false, nil)
	require.NoError(t, err, "passive declare on same connection should succeed")
	assert.Equal(t, messageCount, q.Messages, "queue should still contain all published messages while connection is alive")
}

// TestExclusiveQueue_MultipleExclusiveQueues_AllDeleted verifies that ALL
// exclusive queues declared on a connection are deleted when that connection
// closes, not just a subset of them.
func TestExclusiveQueue_MultipleExclusiveQueues_AllDeleted(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connection A declares three exclusive queues.
	connA, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)

	chA, err := connA.Channel()
	require.NoError(t, err)

	names := [3]string{
		uniqueName("excl-multi-0"),
		uniqueName("excl-multi-1"),
		uniqueName("excl-multi-2"),
	}
	for _, name := range names {
		_, err = chA.QueueDeclare(name, false, false, true, false, nil)
		require.NoError(t, err, "connection A should declare exclusive queue %s", name)
	}

	// Close connection A — all three queues must be auto-deleted.
	err = connA.Close()
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Connection B must be able to re-declare every one of the three names.
	connB, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer connB.Close()

	chB, err := connB.Channel()
	require.NoError(t, err)
	defer chB.Close()

	for _, name := range names {
		_, err = chB.QueueDeclare(name, false, false, false, false, nil)
		assert.NoError(t, err, "connection B should be able to declare %q after auto-deletion", name)
	}
}

// TestExclusiveQueue_NonExclusiveQueue_NotDeletedOnConnectionClose is the
// control test.  A NON-exclusive queue must survive the close of the
// connection that declared it; its messages must remain intact for other
// connections.
func TestExclusiveQueue_NonExclusiveQueue_NotDeletedOnConnectionClose(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connection A declares a non-exclusive queue and publishes messages.
	connA, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)

	chA, err := connA.Channel()
	require.NoError(t, err)

	queueName := uniqueName("non-excl-survives")
	_, err = chA.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "connection A should declare non-exclusive queue successfully")

	const messageCount = 4
	for i := range messageCount {
		err = chA.Publish("", queueName, false, false, amqp.Publishing{
			Body: fmt.Appendf(nil, "msg-%d", i),
		})
		require.NoError(t, err)
	}

	// Close connection A — the non-exclusive queue must NOT be deleted.
	err = connA.Close()
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Connection B passive-declares the queue and verifies the messages are still there.
	connB, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer connB.Close()

	chB, err := connB.Channel()
	require.NoError(t, err)
	defer chB.Close()

	q, err := chB.QueueDeclarePassive(queueName, false, false, false, false, nil)
	require.NoError(t, err, "non-exclusive queue should still exist after connection A closed")
	assert.Equal(t, messageCount, q.Messages, "non-exclusive queue should retain all messages after connection A closed")
}

// TestExclusiveQueue_WithActiveConsumer_DeletedOnConnectionClose verifies that
// an exclusive queue with an active consumer is properly cleaned up (consumer
// stopped and queue deleted) when the owning connection closes.
func TestExclusiveQueue_WithActiveConsumer_DeletedOnConnectionClose(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connection A declares an exclusive queue, publishes messages, and starts consuming.
	connA, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)

	chA, err := connA.Channel()
	require.NoError(t, err)

	queueName := uniqueName("excl-consumer")
	_, err = chA.QueueDeclare(queueName, false, false, true, false, nil)
	require.NoError(t, err)

	// Publish a few messages
	for i := range 3 {
		err = chA.Publish("", queueName, false, false, amqp.Publishing{
			Body: fmt.Appendf(nil, "msg-%d", i),
		})
		require.NoError(t, err)
	}

	// Start a consumer — this exercises the stopCh close path during cleanup.
	msgs, err := chA.Consume(queueName, "", true, false, false, false, nil)
	require.NoError(t, err)

	// Drain at least one message to confirm consumer is active.
	select {
	case m := <-msgs:
		assert.NotNil(t, m.Body)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message on consumer")
	}

	// Close connection A — queue and consumer must be cleaned up without panic.
	err = connA.Close()
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Connection B re-declares the same name to prove the queue was deleted.
	connB, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer connB.Close()

	chB, err := connB.Channel()
	require.NoError(t, err)
	defer chB.Close()

	_, err = chB.QueueDeclare(queueName, false, false, false, false, nil)
	assert.NoError(t, err, "queue should be re-declarable after exclusive queue with consumer was auto-deleted")
}

// TestExclusiveQueue_WithBindings_DeletedOnConnectionClose verifies that when
// an exclusive queue bound to an exchange is auto-deleted on connection close,
// the exchange bindings referencing that queue are also removed.
func TestExclusiveQueue_WithBindings_DeletedOnConnectionClose(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Connection A sets up exchange, exclusive queue, and binding.
	connA, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)

	chA, err := connA.Channel()
	require.NoError(t, err)

	exchangeName := uniqueName("excl-bind-ex")
	queueName := uniqueName("excl-bind-q")
	routingKey := "test.key"

	err = chA.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	require.NoError(t, err)

	_, err = chA.QueueDeclare(queueName, false, false, true, false, nil)
	require.NoError(t, err)

	err = chA.QueueBind(queueName, routingKey, exchangeName, false, nil)
	require.NoError(t, err)

	// Publish a message via the exchange to prove the binding works.
	err = chA.Publish(exchangeName, routingKey, false, false, amqp.Publishing{
		Body: []byte("routed-message"),
	})
	require.NoError(t, err)

	// Verify message arrived in the queue.
	q, err := chA.QueueDeclarePassive(queueName, false, false, true, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, q.Messages)

	// Close connection A — exclusive queue and its bindings must be removed.
	err = connA.Close()
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Connection B: publish via the same exchange+routing key with mandatory=true.
	// Since the exclusive queue (the only binding target) was deleted, the message
	// should be returned as unroutable.
	connB, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer connB.Close()

	chB, err := connB.Channel()
	require.NoError(t, err)
	defer chB.Close()

	// Enable return notifications
	returns := chB.NotifyReturn(make(chan amqp.Return, 1))

	err = chB.Publish(exchangeName, routingKey, true, false, amqp.Publishing{
		Body: []byte("should-be-returned"),
	})
	require.NoError(t, err)

	// Expect a basic.return since no queue is bound anymore.
	select {
	case ret := <-returns:
		assert.Equal(t, []byte("should-be-returned"), ret.Body,
			"returned message body should match what was published")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for basic.return — binding was not cleaned up")
	}
}
