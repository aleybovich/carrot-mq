package carrotmq

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiFrameBody_LargeMessage verifies that messages larger than frameMax
// are correctly assembled from multiple body frames and delivered intact.
func TestMultiFrameBody_LargeMessage(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")

	queueName := uniqueName("multi-frame-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Generate a message body larger than the default frameMax (131072).
	// The AMQP frame payload max is frameMax - 8 (frame header + end byte).
	// Use 256KB to ensure multiple body frames are needed.
	largeBody := make([]byte, 256*1024)
	_, err = rand.Read(largeBody)
	require.NoError(t, err, "Failed to generate random body")

	err = ch.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "application/octet-stream",
		Body:        largeBody,
	})
	require.NoError(t, err, "Failed to publish large message")

	deliveries, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err, "Failed to start consumer")

	select {
	case msg, ok := <-deliveries:
		require.True(t, ok, "Delivery channel closed unexpectedly")
		assert.Equal(t, len(largeBody), len(msg.Body), "Message body length mismatch")
		assert.True(t, bytes.Equal(largeBody, msg.Body), "Message body content mismatch")
		assert.Equal(t, "application/octet-stream", msg.ContentType)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for large message delivery")
	}
}

// TestMultiFrameBody_ExactlyFrameMax tests a message body exactly at frameMax boundary.
func TestMultiFrameBody_ExactlyFrameMax(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")

	queueName := uniqueName("frame-boundary-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// Body exactly at the frame payload limit (frameMax - 8 bytes for frame overhead)
	// With frameMax=131072, max body payload per frame is 131072 - 8 = 131064
	// A body of exactly 131064 fits in one frame; 131065 requires two frames.
	bodySize := 131065 // Just over one frame
	body := make([]byte, bodySize)
	for i := range body {
		body[i] = byte(i % 256)
	}

	err = ch.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "application/octet-stream",
		Body:        body,
	})
	require.NoError(t, err, "Failed to publish message at frame boundary")

	deliveries, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err, "Failed to start consumer")

	select {
	case msg, ok := <-deliveries:
		require.True(t, ok, "Delivery channel closed unexpectedly")
		assert.Equal(t, len(body), len(msg.Body), "Message body length mismatch")
		assert.True(t, bytes.Equal(body, msg.Body), "Message body content mismatch")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for boundary message delivery")
	}
}

// TestMultiFrameBody_EmptyBody verifies that a zero-length body (bodySize=0) is handled correctly.
func TestMultiFrameBody_EmptyBody(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")

	queueName := uniqueName("empty-body-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	err = ch.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte{},
	})
	require.NoError(t, err, "Failed to publish empty message")

	deliveries, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err, "Failed to start consumer")

	select {
	case msg, ok := <-deliveries:
		require.True(t, ok, "Delivery channel closed unexpectedly")
		assert.Equal(t, 0, len(msg.Body), "Empty message body should have length 0")
		assert.Equal(t, "text/plain", msg.ContentType)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for empty body message delivery")
	}
}

// TestMultiFrameBody_VeryLargeMessage tests a 1MB message requiring many frames.
func TestMultiFrameBody_VeryLargeMessage(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")

	queueName := uniqueName("very-large-queue")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue")

	// 1MB message - requires ~8 body frames with default frameMax
	largeBody := make([]byte, 1024*1024)
	_, err = rand.Read(largeBody)
	require.NoError(t, err, "Failed to generate random body")

	err = ch.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "application/octet-stream",
		Body:        largeBody,
	})
	require.NoError(t, err, "Failed to publish very large message")

	deliveries, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	require.NoError(t, err, "Failed to start consumer")

	select {
	case msg, ok := <-deliveries:
		require.True(t, ok, "Delivery channel closed unexpectedly")
		assert.Equal(t, len(largeBody), len(msg.Body), "Message body length mismatch for 1MB message")
		assert.True(t, bytes.Equal(largeBody, msg.Body), "Message body content mismatch for 1MB message")
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for very large message delivery")
	}
}
