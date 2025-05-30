package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryDispatch_ConsumerChannelFull_MessageRemains(t *testing.T) {
	_, conn, _, q := setupTestEnvForDispatch(t)
	msg1 := t_makeTestMessage("m_full", "body_full", q.Name)
	t_addMessageToQueue(t, q, msg1)
	consumerChan := t_addConsumerToQueue(t, q, "c1_full", 0) // Unbuffered

	conn.tryDispatchFromSingleQueue(q.Name) // Attempt dispatch

	q.mu.RLock()
	require.Len(t, q.Messages, 1, "Message should remain in queue")
	// With the fix, the Dispatching flag should be reset
	assert.False(t, q.Messages[0].Dispatching, "Dispatching flag should be false after failed attempt")
	q.mu.RUnlock()

	// Ensure consumer didn't get it
	_, received := t_getMessageFromChan(t, consumerChan, 50*time.Millisecond)
	assert.False(t, received, "Consumer should not have received message from full channel")
}

// --- MODIFIED TEST for Race Condition ---
func TestTryDispatch_RaceBetweenDispatchers_FixVerified_NoDuplicateSend(t *testing.T) {
	_, conn, _, q := setupTestEnvForDispatch(t)

	msg1 := t_makeTestMessage("m1_race_fixed", "body_race_fixed", q.Name)
	t_addMessageToQueue(t, q, msg1)

	c1Chan := t_addConsumerToQueue(t, q, "c1_race_fixed", 1)
	c2Chan := t_addConsumerToQueue(t, q, "c2_race_fixed", 1)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		conn.tryDispatchFromSingleQueue(q.Name)
	}()
	go func() {
		defer wg.Done()
		conn.tryDispatchFromSingleQueue(q.Name)
	}()

	wg.Wait()

	receivedCount := 0
	var receivedMsg Message // To store the one message that was received

	c1Msg, c1Rx := t_getMessageFromChan(t, c1Chan, 200*time.Millisecond)
	if c1Rx {
		receivedCount++
		receivedMsg = c1Msg
		t.Logf("C1 received message ID: %s", c1Msg.Properties.MessageId)
	}

	c2Msg, c2Rx := t_getMessageFromChan(t, c2Chan, 200*time.Millisecond)
	if c2Rx {
		receivedCount++
		// If C1 also received, this would be a failure of the fix.
		if c1Rx {
			t.Errorf("C2 also received message ID: %s. C1 had ID: %s. This indicates a duplicate send despite the fix.", c2Msg.Properties.MessageId, c1Msg.Properties.MessageId)
		}
		receivedMsg = c2Msg
		t.Logf("C2 received message ID: %s", c2Msg.Properties.MessageId)
	}

	q.mu.RLock()
	assert.Empty(t, q.Messages, "Queue should be empty after dispatch attempt(s)")
	q.mu.RUnlock()

	assert.Equal(t, 1, receivedCount, "Exactly one consumer should have received the message")
	if receivedCount == 1 {
		assert.Equal(t, msg1.Properties.MessageId, receivedMsg.Properties.MessageId, "The correct message ID was received")
	}
}

// --- NEW TEST for Dispatching Flag Lifecycle ---
func TestTryDispatch_DispatchingFlag_ResetIfNoConsumerAvailable(t *testing.T) {
	_, conn, _, q := setupTestEnvForDispatch(t)
	msg1 := t_makeTestMessage("m_flag_test", "body_flag", q.Name)
	t_addMessageToQueue(t, q, msg1)
	// NO consumers added to the queue

	conn.tryDispatchFromSingleQueue(q.Name) // Attempt dispatch

	// Message should remain, and its Dispatching flag should be false
	foundMsg, exists := t_findMessageInQueue(q, msg1.Properties.MessageId)
	require.True(t, exists, "Message should still be in the queue")
	assert.False(t, foundMsg.Dispatching, "Dispatching flag should be false as no consumer was available")

	q.mu.RLock()
	assert.Len(t, q.Messages, 1, "Queue should still contain the message")
	q.mu.RUnlock()
}

// --- MODIFIED TEST (Conceptual - hard to time perfectly) ---
func TestTryDispatch_ConsumerCancels_MessageRemains_FlagReset(t *testing.T) {
	// This test aims to check behavior when a consumer vanishes mid-dispatch.
	// Your fix resets the Dispatching flag and keeps the message.
	// Perfect timing is hard. This test simplifies by:
	// 1. Have a message and a consumer.
	// 2. Let dispatch occur (message sent to channel, removed from queue *by the successful dispatcher*).
	// 3. Add a *new* message.
	// 4. Before dispatching the new message, simulate its intended consumer vanishing.
	// 5. Call dispatch, expect new message to remain with flag reset.
	// This isn't testing the *exact* "consumer vanished after send but before removal of *that specific* message"
	// but rather the general case of "dispatch attempt fails because consumer is gone".

	_, conn, _, q := setupTestEnvForDispatch(t)
	log := conn.server.customLogger

	// Scenario: A message is in queue, consumer exists but will "vanish" before dispatch can complete for it.
	msgToRemain := t_makeTestMessage("m_vanish", "body_vanish", q.Name)
	t_addMessageToQueue(t, q, msgToRemain)

	consumerTag := "c_vanish"
	// Add a consumer, but we'll remove it to simulate it vanishing
	consumerChan := t_addConsumerToQueue(t, q, consumerTag, 1)
	log.Info("Added consumer %s", consumerTag)

	// Simulate consumer vanishing by removing it from the queue's map
	// *before* calling tryDispatchFromSingleQueue for msgToRemain
	q.mu.Lock()
	delete(q.Consumers, consumerTag)
	close(consumerChan) // also close its channel
	log.Info("Manually removed consumer %s and closed its channel", consumerTag)
	q.mu.Unlock()

	conn.tryDispatchFromSingleQueue(q.Name) // Attempt to dispatch msgToRemain

	// Check msgToRemain's state
	foundMsg, exists := t_findMessageInQueue(q, msgToRemain.Properties.MessageId)
	require.True(t, exists, "Message 'msgToRemain' should still be in the queue")
	assert.False(t, foundMsg.Dispatching, "Dispatching flag for 'msgToRemain' should be false")

	log.Info("Test 'TestTryDispatch_ConsumerCancels_MessageRemains_FlagReset' assumes that if a dispatch attempt is made and the target consumer is found to be gone (or send fails), the message's Dispatching flag is reset.")
}
