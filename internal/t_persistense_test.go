package internal

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aleybovich/carrot-mq/storage"

	"github.com/stretchr/testify/require"
)

// TestStorageWrapper wraps a storage provider and prevents it from being closed
type TestStorageWrapper struct {
	storage.StorageProvider
	initialized bool
}

func (w *TestStorageWrapper) Initialize() error {
	if !w.initialized {
		w.initialized = true
		return w.StorageProvider.Initialize()
	}
	return nil
}

func (w *TestStorageWrapper) Close() error {
	// Don't actually close during tests
	return nil
}

// Helper to create a wrapped in-memory storage
func createTestStorage() *TestStorageWrapper {
	return &TestStorageWrapper{
		StorageProvider: storage.NewBuntDBProvider(":memory:"),
	}
}

// Helper to create a test server with the given storage
func createServerWithStorage(storage *TestStorageWrapper) *server {
	return NewServer(
		WithStorageProvider(storage),
	)
}

// =============================================================================
// VHOST PERSISTENCE TESTS
// =============================================================================

func TestVHostPersistence(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	// Create a single storage instance that will be reused
	storage := createTestStorage()
	defer storage.StorageProvider.Close() // Clean up at the end

	// First server instance
	server := createServerWithStorage(storage)

	// Create vhosts
	err := server.AddVHost("test-vhost")
	if err != nil {
		t.Fatalf("Failed to add vhost: %v", err)
	}

	err = server.AddVHost("another-vhost")
	if err != nil {
		t.Fatalf("Failed to add another vhost: %v", err)
	}

	// Verify vhosts exist
	_, err = server.GetVHost("test-vhost")
	if err != nil {
		t.Errorf("Failed to get test-vhost: %v", err)
	}

	// Shutdown server (storage stays open due to wrapper)
	server.Shutdown(context.TODO())

	// Create new server with same storage
	newServer := createServerWithStorage(storage)

	// Verify vhosts were recovered
	vhost, err := newServer.GetVHost("test-vhost")
	if err != nil {
		t.Errorf("Failed to get test-vhost after restart: %v", err)
	}
	if vhost == nil {
		t.Error("test-vhost was not recovered after restart")
	}

	vhost2, err := newServer.GetVHost("another-vhost")
	if err != nil {
		t.Errorf("Failed to get another-vhost after restart: %v", err)
	}
	if vhost2 == nil {
		t.Error("another-vhost was not recovered after restart")
	}

	// Test vhost deletion persistence
	err = newServer.DeleteVHost("another-vhost")
	if err != nil {
		t.Fatalf("Failed to delete vhost: %v", err)
	}

	// Restart again
	newServer.Shutdown(context.TODO())
	finalServer := createServerWithStorage(storage)
	defer finalServer.Shutdown(context.TODO())

	// Verify deleted vhost is gone
	_, err = finalServer.GetVHost("another-vhost")
	if err == nil {
		t.Error("Deleted vhost should not exist after restart")
	}

	// Verify other vhost still exists
	_, err = finalServer.GetVHost("test-vhost")
	if err != nil {
		t.Error("test-vhost should still exist after restart")
	}
}

// =============================================================================
// EXCHANGE PERSISTENCE TESTS
// =============================================================================

func TestExchangePersistence(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	server := createServerWithStorage(storage)
	vhost, _ := server.GetVHost("/")

	// Create durable exchanges
	durableExchange := &exchange{
		Name:       "test.durable",
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		Bindings:   make(map[string][]string),
	}

	nonDurableExchange := &exchange{
		Name:       "test.transient",
		Type:       "fanout",
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		Bindings:   make(map[string][]string),
	}

	// Add exchanges
	vhost.mu.Lock()
	vhost.exchanges[durableExchange.Name] = durableExchange
	vhost.exchanges[nonDurableExchange.Name] = nonDurableExchange
	vhost.mu.Unlock()

	// Persist durable exchange
	if server.persistenceManager != nil {
		record := ExchangeToRecord(durableExchange)
		err := server.persistenceManager.SaveExchange(vhost.name, record)
		if err != nil {
			t.Fatalf("Failed to persist exchange: %v", err)
		}
	}

	// Restart server
	server.Shutdown(context.TODO())
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	newVhost, _ := newServer.GetVHost("/")

	// Check durable exchange was recovered
	newVhost.mu.RLock()
	recoveredExchange, exists := newVhost.exchanges["test.durable"]
	newVhost.mu.RUnlock()

	if !exists {
		t.Error("Durable exchange was not recovered after restart")
	} else {
		if recoveredExchange.Type != "direct" {
			t.Errorf("Exchange type mismatch: expected direct, got %s", recoveredExchange.Type)
		}
		if !recoveredExchange.Durable {
			t.Error("Exchange should be durable")
		}
	}

	// Check non-durable exchange was NOT recovered
	newVhost.mu.RLock()
	_, exists = newVhost.exchanges["test.transient"]
	newVhost.mu.RUnlock()

	if exists {
		t.Error("Non-durable exchange should not be recovered after restart")
	}
}

// =============================================================================
// QUEUE PERSISTENCE TESTS
// =============================================================================

func TestQueuePersistence(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	server := createServerWithStorage(storage)
	vhost, _ := server.GetVHost("/")

	// Create queues
	durableQueue := &queue{
		Name:       "test.durable.queue",
		Messages:   []message{},
		Bindings:   make(map[string]bool),
		Consumers:  make(map[string]*consumer),
		Durable:    true,
		Exclusive:  false,
		AutoDelete: false,
	}

	exclusiveQueue := &queue{
		Name:       "test.exclusive.queue",
		Messages:   []message{},
		Bindings:   make(map[string]bool),
		Consumers:  make(map[string]*consumer),
		Durable:    true,
		Exclusive:  true, // Should not be recovered
		AutoDelete: false,
	}

	transientQueue := &queue{
		Name:       "test.transient.queue",
		Messages:   []message{},
		Bindings:   make(map[string]bool),
		Consumers:  make(map[string]*consumer),
		Durable:    false, // Should not be recovered
		Exclusive:  false,
		AutoDelete: false,
	}

	// Add queues
	vhost.mu.Lock()
	vhost.queues[durableQueue.Name] = durableQueue
	vhost.queues[exclusiveQueue.Name] = exclusiveQueue
	vhost.queues[transientQueue.Name] = transientQueue
	vhost.mu.Unlock()

	// Persist durable queues
	if server.persistenceManager != nil {
		record1 := QueueToRecord(durableQueue)
		server.persistenceManager.SaveQueue(vhost.name, record1)

		record2 := QueueToRecord(exclusiveQueue)
		server.persistenceManager.SaveQueue(vhost.name, record2)
	}

	// Restart server
	server.Shutdown(context.TODO())
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	newVhost, _ := newServer.GetVHost("/")

	// Check durable non-exclusive queue was recovered
	newVhost.mu.RLock()
	_, exists := newVhost.queues["test.durable.queue"]
	newVhost.mu.RUnlock()
	if !exists {
		t.Error("Durable queue was not recovered after restart")
	}

	// Check exclusive queue was NOT recovered
	newVhost.mu.RLock()
	_, exists = newVhost.queues["test.exclusive.queue"]
	newVhost.mu.RUnlock()
	if exists {
		t.Error("Exclusive queue should not be recovered after restart")
	}

	// Check transient queue was NOT recovered
	newVhost.mu.RLock()
	_, exists = newVhost.queues["test.transient.queue"]
	newVhost.mu.RUnlock()
	if exists {
		t.Error("Non-durable queue should not be recovered after restart")
	}
}

// =============================================================================
// BINDING PERSISTENCE TESTS
// =============================================================================

func TestBindingPersistence(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	server := createServerWithStorage(storage)
	vhost, _ := server.GetVHost("/")

	// Create durable exchange and queue
	exchange := &exchange{
		Name:     "test.exchange",
		Type:     "topic",
		Durable:  true,
		Bindings: make(map[string][]string),
	}

	q := &queue{
		Name:      "test.queue",
		Durable:   true,
		Bindings:  make(map[string]bool),
		Consumers: make(map[string]*consumer),
		Messages:  []message{},
	}

	transientQueue := &queue{
		Name:      "transient.queue",
		Durable:   false,
		Bindings:  make(map[string]bool),
		Consumers: make(map[string]*consumer),
		Messages:  []message{},
	}

	// Add entities
	vhost.mu.Lock()
	vhost.exchanges[exchange.Name] = exchange
	vhost.queues[q.Name] = q
	vhost.queues[transientQueue.Name] = transientQueue
	vhost.mu.Unlock()

	// Create bindings
	exchange.mu.Lock()
	exchange.Bindings["test.#"] = []string{q.Name}
	exchange.Bindings["temp.#"] = []string{transientQueue.Name}
	exchange.mu.Unlock()

	q.mu.Lock()
	q.Bindings["test.exchange:test.#"] = true
	q.mu.Unlock()

	transientQueue.mu.Lock()
	transientQueue.Bindings["test.exchange:temp.#"] = true
	transientQueue.mu.Unlock()

	// Persist entities and bindings
	if server.persistenceManager != nil {
		// Save exchange and durable queue
		server.persistenceManager.SaveExchange(vhost.name, ExchangeToRecord(exchange))
		server.persistenceManager.SaveQueue(vhost.name, QueueToRecord(q))

		// Save binding between durable entities
		binding := &BindingRecord{
			Exchange:   exchange.Name,
			Queue:      q.Name,
			RoutingKey: "test.#",
			CreatedAt:  time.Now(),
		}
		server.persistenceManager.SaveBinding(vhost.name, binding)
	}

	// Restart server
	server.Shutdown(context.TODO())
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	newVhost, _ := newServer.GetVHost("/")

	// Verify binding was recovered
	newVhost.mu.RLock()
	newExchange := newVhost.exchanges["test.exchange"]
	newQueue := newVhost.queues["test.queue"]
	newVhost.mu.RUnlock()

	if newExchange == nil || newQueue == nil {
		t.Fatal("Exchange or queue not recovered")
	}

	// Check exchange binding
	newExchange.mu.RLock()
	boundQueues, exists := newExchange.Bindings["test.#"]
	newExchange.mu.RUnlock()
	if !exists || len(boundQueues) != 1 || boundQueues[0] != "test.queue" {
		t.Error("Exchange binding not recovered correctly")
	}

	// Check queue binding
	newQueue.mu.RLock()
	_, exists = newQueue.Bindings["test.exchange:test.#"]
	newQueue.mu.RUnlock()
	if !exists {
		t.Error("Queue binding not recovered correctly")
	}

	// Verify binding to transient queue was NOT recovered
	newExchange.mu.RLock()
	_, exists = newExchange.Bindings["temp.#"]
	newExchange.mu.RUnlock()
	if exists {
		t.Error("Binding to transient queue should not be recovered")
	}
}

// =============================================================================
// MESSAGE PERSISTENCE TESTS
// =============================================================================

func TestMessagePersistence(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	server := createServerWithStorage(storage)
	vhost, _ := server.GetVHost("/")

	// Create durable queue
	queue := &queue{
		Name:      "persistent.queue",
		Durable:   true,
		Messages:  []message{},
		Bindings:  make(map[string]bool),
		Consumers: make(map[string]*consumer),
	}

	vhost.mu.Lock()
	vhost.queues[queue.Name] = queue
	vhost.mu.Unlock()

	// Save queue
	if server.persistenceManager != nil {
		server.persistenceManager.SaveQueue(vhost.name, QueueToRecord(queue))
	}

	// Create messages
	persistentMsg := &message{
		Exchange:   "",
		RoutingKey: "persistent.queue",
		Properties: properties{
			DeliveryMode: 2, // Persistent
			MessageId:    "msg-001",
			Timestamp:    uint64(time.Now().Unix()),
		},
		Body: []byte("This is a persistent message"),
	}

	transientMsg := &message{
		Exchange:   "",
		RoutingKey: "persistent.queue",
		Properties: properties{
			DeliveryMode: 1, // Transient
			MessageId:    "msg-002",
		},
		Body: []byte("This is a transient message"),
	}

	// Add messages to queue
	queue.mu.Lock()
	queue.Messages = append(queue.Messages, *persistentMsg, *transientMsg)
	queue.mu.Unlock()

	// Persist only persistent message
	if server.persistenceManager != nil {
		msgId := GetMessageIdentifier(persistentMsg)
		record := MessageToRecord(persistentMsg, msgId, 1)
		server.persistenceManager.SaveMessage(vhost.name, queue.Name, record)
	}

	// Restart server
	server.Shutdown(context.TODO())
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	newVhost, _ := newServer.GetVHost("/")

	// Check queue and messages
	newVhost.mu.RLock()
	newQueue := newVhost.queues["persistent.queue"]
	newVhost.mu.RUnlock()

	if newQueue == nil {
		t.Fatal("Queue not recovered")
	}

	newQueue.mu.RLock()
	messageCount := len(newQueue.Messages)
	newQueue.mu.RUnlock()

	if messageCount != 1 {
		t.Errorf("Expected 1 persistent message, got %d", messageCount)
	}

	// Verify it's the persistent message
	newQueue.mu.RLock()
	if len(newQueue.Messages) > 0 {
		msg := newQueue.Messages[0]
		if msg.Properties.DeliveryMode != 2 {
			t.Error("Recovered message should be persistent")
		}
		if !msg.Redelivered {
			t.Error("Recovered message should be marked as redelivered")
		}
		if string(msg.Body) != "This is a persistent message" {
			t.Error("Message body mismatch")
		}
	}
	newQueue.mu.RUnlock()
}

// =============================================================================
// MESSAGE ACKNOWLEDGMENT PERSISTENCE TESTS
// =============================================================================

func TestMessageAcknowledgmentPersistence(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	server := createServerWithStorage(storage)
	vhost, _ := server.GetVHost("/")

	// Create durable queue
	queue := &queue{
		Name:      "ack.test.queue",
		Durable:   true,
		Messages:  []message{},
		Bindings:  make(map[string]bool),
		Consumers: make(map[string]*consumer),
	}

	vhost.mu.Lock()
	vhost.queues[queue.Name] = queue
	vhost.mu.Unlock()

	// Save queue
	if server.persistenceManager != nil {
		server.persistenceManager.SaveQueue(vhost.name, QueueToRecord(queue))
	}

	// Create multiple persistent messages
	for i := 0; i < 5; i++ {
		msg := &message{
			Exchange:   "",
			RoutingKey: queue.Name,
			Properties: properties{
				DeliveryMode: 2,
				MessageId:    fmt.Sprintf("msg-%03d", i),
			},
			Body: []byte(fmt.Sprintf("Message %d", i)),
		}

		queue.mu.Lock()
		queue.Messages = append(queue.Messages, *msg)
		queue.mu.Unlock()

		// Persist message
		if server.persistenceManager != nil {
			msgId := GetMessageIdentifier(msg)
			record := MessageToRecord(msg, msgId, int64(i))
			server.persistenceManager.SaveMessage(vhost.name, queue.Name, record)
		}
	}

	// Simulate acknowledging some messages
	// Delete messages 1 and 3 (simulating they were acknowledged)
	if server.persistenceManager != nil {
		queue.mu.RLock()
		msg1 := &queue.Messages[1]
		msg3 := &queue.Messages[3]
		queue.mu.RUnlock()

		server.persistenceManager.DeleteMessage(vhost.name, queue.Name, GetMessageIdentifier(msg1))
		server.persistenceManager.DeleteMessage(vhost.name, queue.Name, GetMessageIdentifier(msg3))
	}

	// Restart server
	server.Shutdown(context.TODO())
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	newVhost, _ := newServer.GetVHost("/")
	newVhost.mu.RLock()
	newQueue := newVhost.queues[queue.Name]
	newVhost.mu.RUnlock()

	// Verify only unacknowledged messages were recovered
	newQueue.mu.RLock()
	messageCount := len(newQueue.Messages)
	newQueue.mu.RUnlock()

	if messageCount != 3 {
		t.Errorf("Expected 3 unacknowledged messages, got %d", messageCount)
	}

	// Verify the correct messages were recovered (0, 2, 4)
	expectedBodies := []string{"Message 0", "Message 2", "Message 4"}
	newQueue.mu.RLock()
	for i, msg := range newQueue.Messages {
		if i < len(expectedBodies) && string(msg.Body) != expectedBodies[i] {
			t.Errorf("Message %d body mismatch: expected %s, got %s",
				i, expectedBodies[i], string(msg.Body))
		}
	}
	newQueue.mu.RUnlock()
}

// =============================================================================
// TRANSACTION PERSISTENCE TESTS
// =============================================================================

func TestTransactionMessagePersistence(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	server := createServerWithStorage(storage)
	vhost, _ := server.GetVHost("/")

	// Create durable queue
	queue := &queue{
		Name:      "tx.queue",
		Durable:   true,
		Messages:  []message{},
		Bindings:  make(map[string]bool),
		Consumers: make(map[string]*consumer),
	}

	vhost.mu.Lock()
	vhost.queues[queue.Name] = queue
	vhost.mu.Unlock()

	if server.persistenceManager != nil {
		server.persistenceManager.SaveQueue(vhost.name, QueueToRecord(queue))
	}

	// Simulate a transaction with messages
	// Add messages directly (simulating committed transaction)
	committedMsg := &message{
		Exchange:   "",
		RoutingKey: queue.Name,
		Properties: properties{
			DeliveryMode: 2,
			MessageId:    "tx-committed",
		},
		Body: []byte("Committed transaction message"),
	}

	queue.mu.Lock()
	queue.Messages = append(queue.Messages, *committedMsg)
	queue.mu.Unlock()

	// Persist the committed message
	if server.persistenceManager != nil {
		msgId := GetMessageIdentifier(committedMsg)
		record := MessageToRecord(committedMsg, msgId, 1)
		server.persistenceManager.SaveMessage(vhost.name, queue.Name, record)
	}

	// Note: Uncommitted transaction messages should NOT be persisted
	// This is handled by the transaction logic - messages are only persisted
	// after tx.commit succeeds

	// Restart server
	server.Shutdown(context.TODO())
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	newVhost, _ := newServer.GetVHost("/")
	newVhost.mu.RLock()
	newQueue := newVhost.queues[queue.Name]
	newVhost.mu.RUnlock()

	// Verify only committed message was recovered
	newQueue.mu.RLock()
	if len(newQueue.Messages) != 1 {
		t.Errorf("Expected 1 committed message, got %d", len(newQueue.Messages))
	}
	if len(newQueue.Messages) > 0 && string(newQueue.Messages[0].Body) != "Committed transaction message" {
		t.Error("Recovered message mismatch")
	}
	newQueue.mu.RUnlock()
}

// =============================================================================
// COMPLETE SCENARIO TEST
// =============================================================================

func TestCompleteScenario(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	// This test simulates a complete scenario:
	// 1. Create vhost, exchanges, queues, bindings
	// 2. Publish messages
	// 3. Consume and acknowledge some messages
	// 4. Restart server
	// 5. Verify everything is correctly recovered

	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	server := createServerWithStorage(storage)

	// Step 1: Create infrastructure
	server.AddVHost("test-app")
	vhost, _ := server.GetVHost("test-app")

	// Create exchanges
	directExchange := &exchange{
		Name:     "app.direct",
		Type:     "direct",
		Durable:  true,
		Bindings: make(map[string][]string),
	}

	topicExchange := &exchange{
		Name:     "app.events",
		Type:     "topic",
		Durable:  true,
		Bindings: make(map[string][]string),
	}

	vhost.mu.Lock()
	vhost.exchanges[directExchange.Name] = directExchange
	vhost.exchanges[topicExchange.Name] = topicExchange
	vhost.mu.Unlock()

	// Persist exchanges
	if server.persistenceManager != nil {
		server.persistenceManager.SaveExchange(vhost.name, ExchangeToRecord(directExchange))
		server.persistenceManager.SaveExchange(vhost.name, ExchangeToRecord(topicExchange))
	}

	// Create queues
	ordersQueue := &queue{
		Name:      "orders",
		Durable:   true,
		Messages:  []message{},
		Bindings:  make(map[string]bool),
		Consumers: make(map[string]*consumer),
	}

	eventsQueue := &queue{
		Name:      "events",
		Durable:   true,
		Messages:  []message{},
		Bindings:  make(map[string]bool),
		Consumers: make(map[string]*consumer),
	}

	vhost.mu.Lock()
	vhost.queues[ordersQueue.Name] = ordersQueue
	vhost.queues[eventsQueue.Name] = eventsQueue
	vhost.mu.Unlock()

	// Persist queues
	if server.persistenceManager != nil {
		server.persistenceManager.SaveQueue(vhost.name, QueueToRecord(ordersQueue))
		server.persistenceManager.SaveQueue(vhost.name, QueueToRecord(eventsQueue))
	}

	// Create bindings
	directExchange.mu.Lock()
	directExchange.Bindings["order.create"] = []string{"orders"}
	directExchange.mu.Unlock()

	ordersQueue.mu.Lock()
	ordersQueue.Bindings["app.direct:order.create"] = true
	ordersQueue.mu.Unlock()

	topicExchange.mu.Lock()
	topicExchange.Bindings["events.#"] = []string{"events"}
	topicExchange.mu.Unlock()

	eventsQueue.mu.Lock()
	eventsQueue.Bindings["app.events:events.#"] = true
	eventsQueue.mu.Unlock()

	// Persist bindings
	if server.persistenceManager != nil {
		server.persistenceManager.SaveBinding(vhost.name, &BindingRecord{
			Exchange:   "app.direct",
			Queue:      "orders",
			RoutingKey: "order.create",
			CreatedAt:  time.Now(),
		})

		server.persistenceManager.SaveBinding(vhost.name, &BindingRecord{
			Exchange:   "app.events",
			Queue:      "events",
			RoutingKey: "events.#",
			CreatedAt:  time.Now(),
		})
	}

	// Step 2: Publish messages
	orderMessages := []message{
		{
			Exchange:   "app.direct",
			RoutingKey: "order.create",
			Properties: properties{DeliveryMode: 2, MessageId: "order-001"},
			Body:       []byte(`{"orderId": "001", "amount": 100}`),
		},
		{
			Exchange:   "app.direct",
			RoutingKey: "order.create",
			Properties: properties{DeliveryMode: 2, MessageId: "order-002"},
			Body:       []byte(`{"orderId": "002", "amount": 200}`),
		},
		{
			Exchange:   "app.direct",
			RoutingKey: "order.create",
			Properties: properties{DeliveryMode: 1, MessageId: "order-003"}, // Transient
			Body:       []byte(`{"orderId": "003", "amount": 300}`),
		},
	}

	eventMessages := []message{
		{
			Exchange:   "app.events",
			RoutingKey: "events.user.login",
			Properties: properties{DeliveryMode: 2, MessageId: "event-001"},
			Body:       []byte(`{"event": "login", "userId": "user123"}`),
		},
		{
			Exchange:   "app.events",
			RoutingKey: "events.order.placed",
			Properties: properties{DeliveryMode: 2, MessageId: "event-002"},
			Body:       []byte(`{"event": "orderPlaced", "orderId": "001"}`),
		},
	}

	// Add messages to queues
	ordersQueue.mu.Lock()
	for _, msg := range orderMessages {
		ordersQueue.Messages = append(ordersQueue.Messages, msg)
		if server.persistenceManager != nil && msg.Properties.DeliveryMode == 2 {
			msgId := GetMessageIdentifier(&msg)
			record := MessageToRecord(&msg, msgId, 0)
			server.persistenceManager.SaveMessage(vhost.name, ordersQueue.Name, record)
		}
	}
	ordersQueue.mu.Unlock()

	eventsQueue.mu.Lock()
	for _, msg := range eventMessages {
		eventsQueue.Messages = append(eventsQueue.Messages, msg)
		if server.persistenceManager != nil && msg.Properties.DeliveryMode == 2 {
			msgId := GetMessageIdentifier(&msg)
			record := MessageToRecord(&msg, msgId, 0)
			server.persistenceManager.SaveMessage(vhost.name, eventsQueue.Name, record)
		}
	}
	eventsQueue.mu.Unlock()

	// Step 3: Simulate acknowledging first order message
	if server.persistenceManager != nil {
		firstOrderMsg := &orderMessages[0]
		server.persistenceManager.DeleteMessage(vhost.name, "orders", GetMessageIdentifier(firstOrderMsg))
	}

	// Step 4: Restart server
	server.Shutdown(context.TODO())
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	// Step 5: Verify recovery
	newVhost, err := newServer.GetVHost("test-app")
	if err != nil {
		t.Fatalf("Failed to get vhost after restart: %v", err)
	}

	// Check exchanges
	newVhost.mu.RLock()
	if _, exists := newVhost.exchanges["app.direct"]; !exists {
		t.Error("Direct exchange not recovered")
	}
	if _, exists := newVhost.exchanges["app.events"]; !exists {
		t.Error("Topic exchange not recovered")
	}
	newVhost.mu.RUnlock()

	// Check queues
	newVhost.mu.RLock()
	newOrdersQueue := newVhost.queues["orders"]
	newEventsQueue := newVhost.queues["events"]
	newVhost.mu.RUnlock()

	if newOrdersQueue == nil {
		t.Fatal("Orders queue not recovered")
	}
	if newEventsQueue == nil {
		t.Fatal("Events queue not recovered")
	}

	// Check messages in orders queue (should have 1 persistent message)
	newOrdersQueue.mu.RLock()
	orderMsgCount := len(newOrdersQueue.Messages)
	newOrdersQueue.mu.RUnlock()
	if orderMsgCount != 1 {
		t.Errorf("Expected 1 message in orders queue, got %d", orderMsgCount)
	}

	// Check messages in events queue (should have 2 persistent messages)
	newEventsQueue.mu.RLock()
	eventMsgCount := len(newEventsQueue.Messages)
	newEventsQueue.mu.RUnlock()
	if eventMsgCount != 2 {
		t.Errorf("Expected 2 messages in events queue, got %d", eventMsgCount)
	}

	// Verify bindings
	newVhost.mu.RLock()
	newDirectEx := newVhost.exchanges["app.direct"]
	newTopicEx := newVhost.exchanges["app.events"]
	newVhost.mu.RUnlock()

	newDirectEx.mu.RLock()
	if queues, exists := newDirectEx.Bindings["order.create"]; !exists || len(queues) != 1 {
		t.Error("Direct exchange binding not recovered correctly")
	}
	newDirectEx.mu.RUnlock()

	newTopicEx.mu.RLock()
	if queues, exists := newTopicEx.Bindings["events.#"]; !exists || len(queues) != 1 {
		t.Error("Topic exchange binding not recovered correctly")
	}
	newTopicEx.mu.RUnlock()

	// Verify all recovered messages are marked as redelivered
	newOrdersQueue.mu.RLock()
	for _, msg := range newOrdersQueue.Messages {
		if !msg.Redelivered {
			t.Error("Recovered message should be marked as redelivered")
		}
	}
	newOrdersQueue.mu.RUnlock()

	newEventsQueue.mu.RLock()
	for _, msg := range newEventsQueue.Messages {
		if !msg.Redelivered {
			t.Error("Recovered message should be marked as redelivered")
		}
	}
	newEventsQueue.mu.RUnlock()
}

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

func TestPersistenceErrorHandling(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	// Test that server continues to work even if persistence fails

	// Create a storage provider that fails
	failingStorage := &FailingStorageProvider{
		failAfter: 5, // Fail after 5 operations
		inner:     storage.NewBuntDBProvider(":memory:"),
	}

	server := NewServer(
		WithStorageProvider(failingStorage),
	)
	defer server.Shutdown(context.TODO())

	// Operations should work even when persistence fails
	err := server.AddVHost("test-vhost")
	if err != nil {
		t.Errorf("AddVHost should succeed even if persistence fails: %v", err)
	}

	vhost, err := server.GetVHost("test-vhost")
	if err != nil || vhost == nil {
		t.Error("VHost should exist in memory even if persistence failed")
	}
}

// FailingStorageProvider simulates storage failures for testing
type FailingStorageProvider struct {
	failAfter int
	count     int
	inner     storage.StorageProvider
}

func (f *FailingStorageProvider) Initialize() error {
	return f.inner.Initialize()
}

func (f *FailingStorageProvider) Close() error {
	return f.inner.Close()
}

func (f *FailingStorageProvider) Set(key string, value []byte) error {
	f.count++
	if f.count > f.failAfter {
		return fmt.Errorf("simulated storage failure")
	}
	return f.inner.Set(key, value)
}

func (f *FailingStorageProvider) Get(key string) ([]byte, error) {
	return f.inner.Get(key)
}

func (f *FailingStorageProvider) Delete(key string) error {
	f.count++
	if f.count > f.failAfter {
		return fmt.Errorf("simulated storage failure")
	}
	return f.inner.Delete(key)
}

func (f *FailingStorageProvider) Exists(key string) (bool, error) {
	return f.inner.Exists(key)
}

func (f *FailingStorageProvider) SetBatch(items map[string][]byte) error {
	f.count++
	if f.count > f.failAfter {
		return fmt.Errorf("simulated storage failure")
	}
	return f.inner.SetBatch(items)
}

func (f *FailingStorageProvider) GetBatch(keys []string) (map[string][]byte, error) {
	return f.inner.GetBatch(keys)
}

func (f *FailingStorageProvider) DeleteBatch(keys []string) error {
	f.count++
	if f.count > f.failAfter {
		return fmt.Errorf("simulated storage failure")
	}
	return f.inner.DeleteBatch(keys)
}

func (f *FailingStorageProvider) Keys(prefix string) ([]string, error) {
	return f.inner.Keys(prefix)
}

func (f *FailingStorageProvider) Scan(prefix string, fn func(key string, value []byte) error) error {
	return f.inner.Scan(prefix, fn)
}

func (f *FailingStorageProvider) BeginTx() (storage.StorageTransaction, error) {
	return f.inner.BeginTx()
}

// =============================================================================
// SEQUENCE COUNTER PERSISTENCE TESTS
// =============================================================================

func TestSequenceCounterPersistence(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	// First server instance
	server := createServerWithStorage(storage)
	vhost, _ := server.GetVHost("/")

	// Create a durable queue
	queue := &queue{
		Name:      "seq.test.queue",
		Durable:   true,
		Messages:  []message{},
		Bindings:  make(map[string]bool),
		Consumers: make(map[string]*consumer),
	}

	vhost.mu.Lock()
	vhost.queues[queue.Name] = queue
	vhost.mu.Unlock()

	if server.persistenceManager != nil {
		server.persistenceManager.SaveQueue(vhost.name, QueueToRecord(queue))
	}

	// Add messages and track sequence numbers
	var lastSequence int64
	for i := 0; i < 5; i++ {
		msg := &message{
			Exchange:   "",
			RoutingKey: queue.Name,
			Properties: properties{
				DeliveryMode: 2,
				MessageId:    fmt.Sprintf("seq-msg-%d", i),
			},
			Body: []byte(fmt.Sprintf("Message %d", i)),
		}

		queue.mu.Lock()
		queue.Messages = append(queue.Messages, *msg)
		queue.mu.Unlock()

		if server.persistenceManager != nil {
			msgId := GetMessageIdentifier(msg)
			record := MessageToRecord(msg, msgId, 0)
			server.persistenceManager.SaveMessage(vhost.name, queue.Name, record)
			// The sequence number is assigned inside SaveMessage
			lastSequence = server.persistenceManager.messageSeq.Load()
		}
	}

	t.Logf("Last sequence number before restart: %d", lastSequence)

	// Shutdown server
	server.Shutdown(context.TODO())

	// Create new server with same storage
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	// Get the current sequence number after recovery
	recoveredSequence := newServer.persistenceManager.messageSeq.Load()
	t.Logf("Recovered sequence number: %d", recoveredSequence)

	// Verify sequence was recovered
	if recoveredSequence != lastSequence {
		t.Errorf("Sequence counter not recovered correctly. Expected %d, got %d", lastSequence, recoveredSequence)
	}

	// Add more messages and verify sequence continues from where it left off
	newVhost, _ := newServer.GetVHost("/")
	newVhost.mu.RLock()
	newQueue := newVhost.queues[queue.Name]
	newVhost.mu.RUnlock()

	for i := 5; i < 8; i++ {
		msg := &message{
			Exchange:   "",
			RoutingKey: queue.Name,
			Properties: properties{
				DeliveryMode: 2,
				MessageId:    fmt.Sprintf("seq-msg-%d", i),
			},
			Body: []byte(fmt.Sprintf("Message %d", i)),
		}

		newQueue.mu.Lock()
		newQueue.Messages = append(newQueue.Messages, *msg)
		newQueue.mu.Unlock()

		if newServer.persistenceManager != nil {
			msgId := GetMessageIdentifier(msg)
			record := MessageToRecord(msg, msgId, 0)
			newServer.persistenceManager.SaveMessage(newVhost.name, queue.Name, record)
		}
	}

	// Check that sequence numbers continued incrementing
	newSequence := newServer.persistenceManager.messageSeq.Load()
	expectedSequence := lastSequence + 3 // We added 3 more messages
	if newSequence != expectedSequence {
		t.Errorf("Sequence counter not incrementing correctly. Expected %d, got %d", expectedSequence, newSequence)
	}
	t.Logf("Final sequence number: %d", newSequence)
}

func TestSequenceCounterInTransaction(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	server := createServerWithStorage(storage)
	vhost, _ := server.GetVHost("/")

	// Create a durable queue
	queue := &queue{
		Name:      "tx.seq.queue",
		Durable:   true,
		Messages:  []message{},
		Bindings:  make(map[string]bool),
		Consumers: make(map[string]*consumer),
	}

	vhost.mu.Lock()
	vhost.queues[queue.Name] = queue
	vhost.mu.Unlock()

	if server.persistenceManager != nil {
		server.persistenceManager.SaveQueue(vhost.name, QueueToRecord(queue))
	}

	// Use a transaction to save messages
	if server.persistenceManager != nil {
		tx, err := server.persistenceManager.BeginTransaction()
		require.NoError(t, err)

		// Save multiple messages in transaction
		for i := 0; i < 3; i++ {
			msg := &message{
				Exchange:   "",
				RoutingKey: queue.Name,
				Properties: properties{
					DeliveryMode: 2,
					MessageId:    fmt.Sprintf("tx-seq-msg-%d", i),
				},
				Body: []byte(fmt.Sprintf("Tx Message %d", i)),
			}

			msgId := GetMessageIdentifier(msg)
			record := MessageToRecord(msg, msgId, 0)
			err = tx.SaveMessage(vhost.name, queue.Name, record)
			require.NoError(t, err)
		}

		// Commit transaction
		err = tx.Commit()
		require.NoError(t, err)
	}

	lastSequence := server.persistenceManager.messageSeq.Load()
	t.Logf("Sequence after transaction: %d", lastSequence)

	// Restart server
	server.Shutdown(context.TODO())
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	// Verify sequence was recovered
	recoveredSequence := newServer.persistenceManager.messageSeq.Load()
	if recoveredSequence != lastSequence {
		t.Errorf("Sequence counter not recovered after transaction. Expected %d, got %d", lastSequence, recoveredSequence)
	}
}

func TestSequenceCounterConcurrent(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	server := createServerWithStorage(storage)
	vhost, _ := server.GetVHost("/")

	// Create multiple queues
	numQueues := 5
	for i := 0; i < numQueues; i++ {
		queue := &queue{
			Name:      fmt.Sprintf("seq.concurrent.%d", i),
			Durable:   true,
			Messages:  []message{},
			Bindings:  make(map[string]bool),
			Consumers: make(map[string]*consumer),
		}

		vhost.mu.Lock()
		vhost.queues[queue.Name] = queue
		vhost.mu.Unlock()

		if server.persistenceManager != nil {
			server.persistenceManager.SaveQueue(vhost.name, QueueToRecord(queue))
		}
	}

	// Concurrently save messages
	var wg sync.WaitGroup
	messagesPerQueue := 20

	for i := 0; i < numQueues; i++ {
		wg.Add(1)
		go func(queueIndex int) {
			defer wg.Done()

			queueName := fmt.Sprintf("seq.concurrent.%d", queueIndex)
			vhost.mu.RLock()
			queue := vhost.queues[queueName]
			vhost.mu.RUnlock()

			for j := 0; j < messagesPerQueue; j++ {
				msg := &message{
					Exchange:   "",
					RoutingKey: queueName,
					Properties: properties{
						DeliveryMode: 2,
						MessageId:    fmt.Sprintf("q%d-msg%d", queueIndex, j),
					},
					Body: []byte(fmt.Sprintf("Queue %d Message %d", queueIndex, j)),
				}

				queue.mu.Lock()
				queue.Messages = append(queue.Messages, *msg)
				queue.mu.Unlock()

				if server.persistenceManager != nil {
					msgId := GetMessageIdentifier(msg)
					record := MessageToRecord(msg, msgId, 0)
					server.persistenceManager.SaveMessage(vhost.name, queueName, record)
				}
			}
		}(i)
	}

	wg.Wait()

	// Total messages saved
	totalMessages := numQueues * messagesPerQueue
	finalSequence := server.persistenceManager.messageSeq.Load()
	t.Logf("Final sequence after concurrent saves: %d (total messages: %d)", finalSequence, totalMessages)

	// Sequence should be at least totalMessages
	if finalSequence < int64(totalMessages) {
		t.Errorf("Sequence counter too low. Expected at least %d, got %d", totalMessages, finalSequence)
	}

	// Restart and verify
	server.Shutdown(context.TODO())
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	recoveredSequence := newServer.persistenceManager.messageSeq.Load()
	if recoveredSequence != finalSequence {
		t.Errorf("Sequence counter not recovered correctly. Expected %d, got %d", finalSequence, recoveredSequence)
	}
}

// =============================================================================
// Concurrent Persistence Operations
// =============================================================================
func TestConcurrentPersistenceOperations(t *testing.T) {
	IsTerminal = true // Force colorized output for server logs during tests
	storage := createTestStorage()
	defer storage.StorageProvider.Close()

	server := createServerWithStorage(storage)
	vhost, _ := server.GetVHost("/")

	// Create multiple queues
	numQueues := 10
	for i := 0; i < numQueues; i++ {
		queue := &queue{
			Name:      fmt.Sprintf("concurrent.queue.%d", i),
			Durable:   true,
			Messages:  []message{},
			Bindings:  make(map[string]bool),
			Consumers: make(map[string]*consumer),
		}

		vhost.mu.Lock()
		vhost.queues[queue.Name] = queue
		vhost.mu.Unlock()

		if server.persistenceManager != nil {
			server.persistenceManager.SaveQueue(vhost.name, QueueToRecord(queue))
		}
	}

	// Concurrently add messages to different queues
	var wg sync.WaitGroup
	messagesPerQueue := 100

	for i := 0; i < numQueues; i++ {
		wg.Add(1)
		go func(queueIndex int) {
			defer wg.Done()

			queueName := fmt.Sprintf("concurrent.queue.%d", queueIndex)
			vhost.mu.RLock()
			queue := vhost.queues[queueName]
			vhost.mu.RUnlock()

			for j := 0; j < messagesPerQueue; j++ {
				msg := &message{
					Exchange:   "",
					RoutingKey: queueName,
					Properties: properties{
						DeliveryMode: 2,
						MessageId:    fmt.Sprintf("q%d-msg%d", queueIndex, j),
					},
					Body: []byte(fmt.Sprintf("Queue %d Message %d", queueIndex, j)),
				}

				queue.mu.Lock()
				queue.Messages = append(queue.Messages, *msg)
				queue.mu.Unlock()

				if server.persistenceManager != nil {
					msgId := GetMessageIdentifier(msg)
					record := MessageToRecord(msg, msgId, int64(j))
					server.persistenceManager.SaveMessage(vhost.name, queueName, record)
				}
			}
		}(i)
	}

	wg.Wait()

	// Restart server
	server.Shutdown(context.TODO())
	newServer := createServerWithStorage(storage)
	defer newServer.Shutdown(context.TODO())

	newVhost, _ := newServer.GetVHost("/")

	// Verify all messages were persisted correctly
	for i := 0; i < numQueues; i++ {
		queueName := fmt.Sprintf("concurrent.queue.%d", i)

		newVhost.mu.RLock()
		queue := newVhost.queues[queueName]
		newVhost.mu.RUnlock()

		require.NotNil(t, queue, "Queue %s not found after recovery", queueName)

		queue.mu.RLock()
		if len(queue.Messages) != messagesPerQueue {
			t.Errorf("Queue %s: expected %d messages, got %d",
				queueName, messagesPerQueue, len(queue.Messages))
		}
		queue.mu.RUnlock()
	}
}
