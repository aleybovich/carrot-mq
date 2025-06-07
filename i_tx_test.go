package carrotmq

import (
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTxSelect tests the tx.select method
func TestTxSelect(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	defer ch.Close()

	// Enable transaction mode
	err = ch.Tx()
	assert.NoError(t, err, "Failed to select transaction mode")
}

// TestTxSelectWithConfirmMode tests that tx.select fails when confirm mode is enabled
func TestTxSelectWithConfirmMode(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")

	// Enable confirm mode first
	err = ch.Confirm(false)
	require.NoError(t, err, "Failed to enable confirm mode")

	// Try to enable tx mode - should fail
	err = ch.Tx()
	require.Error(t, err, "Expected error when enabling tx mode with confirm mode active")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Expected AMQP error")
	assert.Equal(t, 406, amqpErr.Code, "Expected PRECONDITION_FAILED error code")
	assert.Contains(t, amqpErr.Reason, "cannot use transactions with confirm mode")
}

// TestTxCommitEmpty tests committing an empty transaction
func TestTxCommitEmpty(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	defer ch.Close()

	// Enable tx mode
	err = ch.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Commit empty transaction
	err = ch.TxCommit()
	assert.NoError(t, err, "Failed to commit empty transaction")
}

// TestTxRollback tests rolling back a transaction
func TestTxRollback(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	defer ch.Close()

	// Enable tx mode
	err = ch.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Rollback empty transaction
	err = ch.TxRollback()
	assert.NoError(t, err, "Failed to rollback transaction")
}

// TestTxPublishCommit tests publishing messages within a transaction
func TestTxPublishCommit(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	defer ch.Close()

	// Declare a queue
	queueName := uniqueName("test-tx-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Enable tx mode
	err = ch.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Publish 3 messages
	for i := range 3 {
		msg := amqp.Publishing{
			Body: []byte("Message " + string(rune('A'+i))),
		}
		err = ch.Publish("", q.Name, false, false, msg)
		require.NoError(t, err, "Failed to publish message")
	}

	// Start a consumer to check messages
	deliveries, _ := t_consumeMessage(t, ch, q.Name, uniqueName("consumer"), true)

	// Messages should not be delivered yet
	t_expectNoMessage(t, deliveries, 100*time.Millisecond)

	// Commit transaction
	err = ch.TxCommit()
	require.NoError(t, err, "Failed to commit transaction")

	// Now messages should be delivered
	for i := range 3 {
		expectedBody := "Message " + string(rune('A'+i))
		t_expectMessage(t, deliveries, expectedBody, nil, 2*time.Second)
	}
}

// TestTxPublishRollback tests rolling back published messages
func TestTxPublishRollback(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	defer ch.Close()

	// Declare a queue
	queueName := uniqueName("test-tx-rollback-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Enable tx mode
	err = ch.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Publish 3 messages
	for i := range 3 {
		msg := amqp.Publishing{
			Body: []byte("Message " + string(rune('A'+i))),
		}
		err = ch.Publish("", q.Name, false, false, msg)
		require.NoError(t, err, "Failed to publish message")
	}

	// Rollback transaction
	err = ch.TxRollback()
	require.NoError(t, err, "Failed to rollback transaction")

	// Start a consumer to verify no messages
	deliveries, _ := t_consumeMessage(t, ch, q.Name, uniqueName("consumer"), true)

	// No messages should be delivered
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)
}

// TestTxAckCommit tests acknowledging messages within a transaction
func TestTxAckCommit(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	// Use two channels - one for publishing, one for consuming with tx
	pubCh, err := conn.Channel()
	require.NoError(t, err, "Failed to open publishing channel")
	defer pubCh.Close()

	consCh, err := conn.Channel()
	require.NoError(t, err, "Failed to open consuming channel")
	defer consCh.Close()

	// Declare a queue
	queueName := uniqueName("test-tx-ack-queue")
	q, err := pubCh.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Publish 3 messages (outside transaction)
	expectedMessages := []string{"Message A", "Message B", "Message C"}
	for _, msg := range expectedMessages {
		err = pubCh.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err, "Failed to publish message")
	}

	// Start a consumer with manual ack
	deliveries, _ := t_consumeMessage(t, consCh, q.Name, uniqueName("consumer"), false)

	// Receive 3 messages
	var messages []amqp.Delivery
	for i := 0; i < 3; i++ {
		select {
		case msg := <-deliveries:
			messages = append(messages, msg)
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for message")
		}
	}

	// Verify we got all expected messages (order doesn't matter)
	receivedBodies := make([]string, len(messages))
	for i, msg := range messages {
		receivedBodies[i] = string(msg.Body)
	}
	assert.ElementsMatch(t, expectedMessages, receivedBodies, "Should receive all expected messages")

	// Enable tx mode on consumer channel
	err = consCh.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Ack all messages within transaction
	for _, msg := range messages {
		err = msg.Ack(false)
		require.NoError(t, err, "Failed to ack message")
	}

	// Close and reopen consumer channel to check unacked messages
	consCh.Close()
	consCh, err = conn.Channel()
	require.NoError(t, err, "Failed to reopen channel")

	// Messages should still be in queue (unacked)
	deliveries2, _ := t_consumeMessage(t, consCh, q.Name, uniqueName("consumer2"), false)

	// Collect all redelivered messages
	var redeliveredBodies []string
	for i := 0; i < 3; i++ {
		select {
		case msg := <-deliveries2:
			assert.True(t, msg.Redelivered, "Message should be marked as redelivered")
			redeliveredBodies = append(redeliveredBodies, string(msg.Body))
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for redelivered message")
		}
	}

	// Verify we got all messages back (order doesn't matter per AMQP 0-9-1 spec)
	assert.ElementsMatch(t, expectedMessages, redeliveredBodies,
		"Should receive all messages after uncommitted tx (order not guaranteed by AMQP 0-9-1)")
}

// TestTxAckRollback tests rolling back acknowledgments
func TestTxAckRollback(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	defer ch.Close()

	// Declare a queue
	queueName := uniqueName("test-tx-ack-rollback-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Publish a message
	msg := amqp.Publishing{
		Body: []byte("Test message"),
	}
	err = ch.Publish("", q.Name, false, false, msg)
	require.NoError(t, err, "Failed to publish message")

	// Start a consumer with manual ack
	deliveries, _ := t_consumeMessage(t, ch, q.Name, uniqueName("consumer"), false)

	// Receive the message
	delivery := t_expectMessage(t, deliveries, "Test message", nil, 2*time.Second)

	// Enable tx mode
	err = ch.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Ack the message
	err = delivery.Ack(false)
	require.NoError(t, err, "Failed to ack message")

	// Rollback transaction
	err = ch.TxRollback()
	require.NoError(t, err, "Failed to rollback transaction")

	// Close and reopen channel
	ch.Close()
	ch, err = conn.Channel()
	require.NoError(t, err, "Failed to reopen channel")

	// Message should still be unacked and redelivered
	deliveries2, _ := t_consumeMessage(t, ch, q.Name, uniqueName("consumer2"), false)
	redelivered := true
	t_expectMessage(t, deliveries2, "Test message", &redelivered, 2*time.Second)
}

// TestTxNackRequeue tests nacking messages with requeue in a transaction
func TestTxNackRequeue(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	defer ch.Close()

	// Declare a queue
	queueName := uniqueName("test-tx-nack-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Publish a message
	msg := amqp.Publishing{
		Body: []byte("Test nack message"),
	}
	err = ch.Publish("", q.Name, false, false, msg)
	require.NoError(t, err, "Failed to publish message")

	// Start a consumer with manual ack
	deliveries, _ := t_consumeMessage(t, ch, q.Name, uniqueName("consumer"), false)

	// Receive the message
	delivery := t_expectMessage(t, deliveries, "Test nack message", nil, 2*time.Second)

	// Enable tx mode
	err = ch.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Nack with requeue
	err = delivery.Nack(false, true)
	require.NoError(t, err, "Failed to nack message")

	// Commit transaction
	err = ch.TxCommit()
	require.NoError(t, err, "Failed to commit transaction")

	// Message should be redelivered
	redelivered := true
	t_expectMessage(t, deliveries, "Test nack message", &redelivered, 2*time.Second)
}

// TestTxReject tests rejecting messages within a transaction
func TestTxReject(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	defer ch.Close()

	// Declare a queue
	queueName := uniqueName("test-tx-reject-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Publish a message
	msg := amqp.Publishing{
		Body: []byte("Test reject message"),
	}
	err = ch.Publish("", q.Name, false, false, msg)
	require.NoError(t, err, "Failed to publish message")

	// Start a consumer with manual ack
	deliveries, _ := t_consumeMessage(t, ch, q.Name, uniqueName("consumer"), false)

	// Receive the message
	delivery := t_expectMessage(t, deliveries, "Test reject message", nil, 2*time.Second)

	// Enable tx mode
	err = ch.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Reject without requeue
	err = delivery.Reject(false)
	require.NoError(t, err, "Failed to reject message")

	// Commit transaction
	err = ch.TxCommit()
	require.NoError(t, err, "Failed to commit transaction")

	// Message should not be redelivered
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)
}

// TestTxMultipleCommits tests multiple commits on same transaction
func TestTxMultipleCommits(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	defer ch.Close()

	// Declare a queue
	queueName := uniqueName("test-tx-multi-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Enable tx mode
	err = ch.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// First transaction
	msg1 := amqp.Publishing{Body: []byte("Message 1")}
	err = ch.Publish("", q.Name, false, false, msg1)
	require.NoError(t, err, "Failed to publish message 1")

	err = ch.TxCommit()
	require.NoError(t, err, "Failed to commit first transaction")

	// Second transaction
	msg2 := amqp.Publishing{Body: []byte("Message 2")}
	err = ch.Publish("", q.Name, false, false, msg2)
	require.NoError(t, err, "Failed to publish message 2")

	msg3 := amqp.Publishing{Body: []byte("Message 3")}
	err = ch.Publish("", q.Name, false, false, msg3)
	require.NoError(t, err, "Failed to publish message 3")

	err = ch.TxCommit()
	require.NoError(t, err, "Failed to commit second transaction")

	// Verify all messages are in queue
	deliveries, _ := t_consumeMessage(t, ch, q.Name, uniqueName("consumer"), true)
	t_expectMessage(t, deliveries, "Message 1", nil, 2*time.Second)
	t_expectMessage(t, deliveries, "Message 2", nil, 2*time.Second)
	t_expectMessage(t, deliveries, "Message 3", nil, 2*time.Second)
}

// TestTxChannelClose tests that closing channel discards uncommitted transaction
func TestTxChannelClose(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")

	// Declare a queue
	queueName := uniqueName("test-tx-close-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Enable tx mode
	err = ch.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Publish messages in transaction
	msg1 := amqp.Publishing{Body: []byte("Message 1")}
	err = ch.Publish("", q.Name, false, false, msg1)
	require.NoError(t, err, "Failed to publish message 1")

	msg2 := amqp.Publishing{Body: []byte("Message 2")}
	err = ch.Publish("", q.Name, false, false, msg2)
	require.NoError(t, err, "Failed to publish message 2")

	// Close channel without committing
	ch.Close()

	// Open new channel to check queue
	ch2, err := conn.Channel()
	require.NoError(t, err, "Failed to open new channel")
	defer ch2.Close()

	// Messages should not be in queue
	deliveries, _ := t_consumeMessage(t, ch2, q.Name, uniqueName("consumer"), true)
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)
}

// TestTxCommitWithoutSelect tests committing without selecting tx mode first
func TestTxCommitWithoutSelect(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")

	// Try to commit without tx mode - should fail
	err = ch.TxCommit()
	require.Error(t, err, "Expected error when committing without tx mode")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Expected AMQP error")
	assert.Equal(t, 406, amqpErr.Code, "Expected PRECONDITION_FAILED error code")
}

// TestTxRollbackWithoutSelect tests rolling back without selecting tx mode first
func TestTxRollbackWithoutSelect(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")

	// Try to rollback without tx mode - should fail
	err = ch.TxRollback()
	require.Error(t, err, "Expected error when rolling back without tx mode")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "Expected AMQP error")
	assert.Equal(t, 406, amqpErr.Code, "Expected PRECONDITION_FAILED error code")
}

// TestTxMixedOperations tests a complex transaction with mixed operations
func TestTxMixedOperations(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	// Setup channels
	pubCh, err := conn.Channel()
	require.NoError(t, err, "Failed to open publishing channel")
	defer pubCh.Close()

	txCh, err := conn.Channel()
	require.NoError(t, err, "Failed to open transaction channel")
	defer txCh.Close()

	// Declare queues
	queue1 := uniqueName("test-tx-mixed-queue1")
	q1, err := pubCh.QueueDeclare(queue1, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue 1")

	queue2 := uniqueName("test-tx-mixed-queue2")
	q2, err := pubCh.QueueDeclare(queue2, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue 2")

	// Publish initial messages
	for i := 0; i < 3; i++ {
		msg := amqp.Publishing{Body: []byte("Initial " + string(rune('A'+i)))}
		err = pubCh.Publish("", q1.Name, false, false, msg)
		require.NoError(t, err, "Failed to publish initial message")
	}

	// Start consumer on queue 1
	deliveries1, _ := t_consumeMessage(t, txCh, q1.Name, uniqueName("consumer1"), false)

	// Receive messages
	var toAck []amqp.Delivery
	for i := 0; i < 3; i++ {
		msg := t_expectMessage(t, deliveries1, "Initial "+string(rune('A'+i)), nil, 2*time.Second)
		toAck = append(toAck, msg)
	}

	// Enable tx mode
	err = txCh.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Mix of operations in transaction:
	// 1. Ack first message
	err = toAck[0].Ack(false)
	require.NoError(t, err, "Failed to ack first message")

	// 2. Nack second message with requeue
	err = toAck[1].Nack(false, true)
	require.NoError(t, err, "Failed to nack second message")

	// 3. Reject third message without requeue
	err = toAck[2].Reject(false)
	require.NoError(t, err, "Failed to reject third message")

	// 4. Publish new messages to queue 2
	for i := 0; i < 2; i++ {
		msg := amqp.Publishing{Body: []byte("New " + string(rune('X'+i)))}
		err = txCh.Publish("", q2.Name, false, false, msg)
		require.NoError(t, err, "Failed to publish new message")
	}

	// Commit transaction
	err = txCh.TxCommit()
	require.NoError(t, err, "Failed to commit transaction")

	// Verify results:
	// Queue 1 should have only the nacked message (requeued)
	redelivered := true
	t_expectMessage(t, deliveries1, "Initial B", &redelivered, 2*time.Second)
	t_expectNoMessage(t, deliveries1, 200*time.Millisecond)

	// Queue 2 should have the new messages
	deliveries2, _ := t_consumeMessage(t, txCh, q2.Name, uniqueName("consumer2"), true)
	t_expectMessage(t, deliveries2, "New X", nil, 2*time.Second)
	t_expectMessage(t, deliveries2, "New Y", nil, 2*time.Second)
}

// TestTxConnectionClose tests that closing connection discards uncommitted transactions
func TestTxConnectionClose(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")

	// Declare a queue
	queueName := uniqueName("test-tx-conn-close-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Enable tx mode
	err = ch.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Publish messages in transaction
	for i := 0; i < 3; i++ {
		msg := amqp.Publishing{Body: []byte("Message " + string(rune('A'+i)))}
		err = ch.Publish("", q.Name, false, false, msg)
		require.NoError(t, err, "Failed to publish message")
	}

	// Close connection without committing
	conn.Close()

	// Connect again to check queue
	conn2, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to reconnect")
	defer conn2.Close()

	ch2, err := conn2.Channel()
	require.NoError(t, err, "Failed to open new channel")
	defer ch2.Close()

	// Messages should not be in queue
	deliveries, _ := t_consumeMessage(t, ch2, q.Name, uniqueName("consumer"), true)
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)
}

// TestTxChannelOrdering verifies that messages published on a regular channel
// are delivered before messages published on a transactional channel (before commit)
func TestTxChannelOrdering(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	// Create three channels
	regularCh, err := conn.Channel()
	require.NoError(t, err, "Failed to open regular channel")
	defer regularCh.Close()

	txCh, err := conn.Channel()
	require.NoError(t, err, "Failed to open tx channel")
	defer txCh.Close()

	consumerCh, err := conn.Channel()
	require.NoError(t, err, "Failed to open consumer channel")
	defer consumerCh.Close()

	// Declare a queue
	queueName := uniqueName("test-tx-ordering-queue")
	q, err := regularCh.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Enable tx mode on the transaction channel
	err = txCh.Tx()
	require.NoError(t, err, "Failed to enable tx mode")

	// Publish messages alternating between regular and tx channels
	// Pattern: R1, T1, R2, T2, R3, T3
	for i := 1; i <= 3; i++ {
		// Publish on regular channel
		regularMsg := amqp.Publishing{
			Body: []byte(fmt.Sprintf("Regular %d", i)),
		}
		err = regularCh.Publish("", q.Name, false, false, regularMsg)
		require.NoError(t, err, "Failed to publish regular message")

		// Publish on tx channel
		txMsg := amqp.Publishing{
			Body: []byte(fmt.Sprintf("Tx %d", i)),
		}
		err = txCh.Publish("", q.Name, false, false, txMsg)
		require.NoError(t, err, "Failed to publish tx message")
	}

	// Start consumer
	deliveries, _ := t_consumeMessage(t, consumerCh, q.Name, uniqueName("consumer"), true)

	// First, we should receive only the regular messages (R1, R2, R3)
	for i := 1; i <= 3; i++ {
		expectedBody := fmt.Sprintf("Regular %d", i)
		msg := t_expectMessage(t, deliveries, expectedBody, nil, 2*time.Second)
		assert.Equal(t, expectedBody, string(msg.Body), "Should receive regular messages first")
	}

	// Tx messages should not be delivered yet
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)

	// Now commit the transaction
	err = txCh.TxCommit()
	require.NoError(t, err, "Failed to commit transaction")

	// Now we should receive the tx messages (T1, T2, T3)
	for i := 1; i <= 3; i++ {
		expectedBody := fmt.Sprintf("Tx %d", i)
		msg := t_expectMessage(t, deliveries, expectedBody, nil, 2*time.Second)
		assert.Equal(t, expectedBody, string(msg.Body), "Should receive tx messages after commit")
	}

	// No more messages
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)
}

// TestTxMultipleChannelInterleaving tests complex interleaving of regular and tx messages
func TestTxMultipleChannelInterleaving(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	// Create channels
	regularCh, err := conn.Channel()
	require.NoError(t, err, "Failed to open regular channel")
	defer regularCh.Close()

	txCh1, err := conn.Channel()
	require.NoError(t, err, "Failed to open first tx channel")
	defer txCh1.Close()

	txCh2, err := conn.Channel()
	require.NoError(t, err, "Failed to open second tx channel")
	defer txCh2.Close()

	consumerCh, err := conn.Channel()
	require.NoError(t, err, "Failed to open consumer channel")
	defer consumerCh.Close()

	// Declare a queue
	queueName := uniqueName("test-multi-tx-queue")
	q, err := regularCh.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Enable tx mode on both tx channels
	err = txCh1.Tx()
	require.NoError(t, err, "Failed to enable tx mode on channel 1")
	err = txCh2.Tx()
	require.NoError(t, err, "Failed to enable tx mode on channel 2")

	// Publish messages: R1, T1-1, T2-1, R2, T1-2, T2-2
	err = regularCh.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte("Regular 1")})
	require.NoError(t, err)

	err = txCh1.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte("Tx1-1")})
	require.NoError(t, err)

	err = txCh2.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte("Tx2-1")})
	require.NoError(t, err)

	err = regularCh.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte("Regular 2")})
	require.NoError(t, err)

	err = txCh1.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte("Tx1-2")})
	require.NoError(t, err)

	err = txCh2.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte("Tx2-2")})
	require.NoError(t, err)

	// Start consumer
	deliveries, _ := t_consumeMessage(t, consumerCh, q.Name, uniqueName("consumer"), true)

	// Should receive only regular messages first
	t_expectMessage(t, deliveries, "Regular 1", nil, 2*time.Second)
	t_expectMessage(t, deliveries, "Regular 2", nil, 2*time.Second)
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)

	// Commit tx channel 2 first
	err = txCh2.TxCommit()
	require.NoError(t, err, "Failed to commit tx channel 2")

	// Should receive tx2 messages
	t_expectMessage(t, deliveries, "Tx2-1", nil, 2*time.Second)
	t_expectMessage(t, deliveries, "Tx2-2", nil, 2*time.Second)
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)

	// Commit tx channel 1
	err = txCh1.TxCommit()
	require.NoError(t, err, "Failed to commit tx channel 1")

	// Should receive tx1 messages
	t_expectMessage(t, deliveries, "Tx1-1", nil, 2*time.Second)
	t_expectMessage(t, deliveries, "Tx1-2", nil, 2*time.Second)
	t_expectNoMessage(t, deliveries, 200*time.Millisecond)
}