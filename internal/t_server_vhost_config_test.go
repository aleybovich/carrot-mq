package internal

import (
	"testing"

	"github.com/aleybovich/carrot-mq/config"

	"github.com/stretchr/testify/assert"
)

func TestWithVHosts_HappyPath(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/production",
			Exchanges: []config.ExchangeConfig{
				{Name: "orders", Type: "direct", Durable: true},
				{Name: "events", Type: "fanout", Durable: true},
				{Name: "notifications", Type: "topic", Durable: false},
			},
			Queues: []config.QueueConfig{
				{
					Name:    "order-processing",
					Durable: true,
					Bindings: map[string]bool{
						"orders:new":    true,
						"orders:update": true,
					},
				},
				{
					Name:    "all-events",
					Durable: true,
					Bindings: map[string]bool{
						"events:": true, // fanout binding
					},
				},
				{
					Name:    "email-notifications",
					Durable: false,
					Bindings: map[string]bool{
						"notifications:email.*": true,
					},
				},
			},
		},
		{
			Name: "/staging",
			Exchanges: []config.ExchangeConfig{
				{Name: "test-exchange", Type: "direct", Durable: false},
			},
			Queues: []config.QueueConfig{
				{
					Name:      "test-queue",
					Durable:   false,
					Exclusive: true,
					Bindings: map[string]bool{
						"test-exchange:test": true,
					},
				},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	// Verify vhosts were created
	vhost1, err := server.GetVHost("/production")
	assert.NoError(t, err, "vhost /production should exist")
	assert.NotNil(t, vhost1)

	vhost2, err := server.GetVHost("/staging")
	assert.NoError(t, err, "vhost /staging should exist")
	assert.NotNil(t, vhost2)

	// Verify exchanges in /production
	vhost1.mu.RLock()
	assert.Len(t, vhost1.exchanges, 4, "should have 4 exchanges (3 configured + 1 default)")

	// Check specific exchanges
	ordersEx, exists := vhost1.exchanges["orders"]
	assert.True(t, exists, "orders exchange should exist")
	if assert.NotNil(t, ordersEx) {
		assert.Equal(t, "direct", ordersEx.Type)
		assert.True(t, ordersEx.Durable)
	}

	eventsEx, exists := vhost1.exchanges["events"]
	assert.True(t, exists, "events exchange should exist")
	if assert.NotNil(t, eventsEx) {
		assert.Equal(t, "fanout", eventsEx.Type)
		assert.True(t, eventsEx.Durable)
	}

	notifEx, exists := vhost1.exchanges["notifications"]
	assert.True(t, exists, "notifications exchange should exist")
	if assert.NotNil(t, notifEx) {
		assert.Equal(t, "topic", notifEx.Type)
		assert.False(t, notifEx.Durable)
	}

	// Verify queues in /production
	assert.Len(t, vhost1.queues, 3, "should have 3 queues")

	// Check order-processing queue and its bindings
	orderQueue, exists := vhost1.queues["order-processing"]
	assert.True(t, exists, "order-processing queue should exist")
	if assert.NotNil(t, orderQueue) {
		assert.True(t, orderQueue.Durable)
		assert.Len(t, orderQueue.Bindings, 2, "should have 2 bindings")
		assert.Contains(t, orderQueue.Bindings, "orders:new")
		assert.Contains(t, orderQueue.Bindings, "orders:update")
	}

	// Check all-events queue
	eventsQueue, exists := vhost1.queues["all-events"]
	assert.True(t, exists, "all-events queue should exist")
	if assert.NotNil(t, eventsQueue) {
		assert.True(t, eventsQueue.Durable)
		assert.Contains(t, eventsQueue.Bindings, "events:")
	}

	// Check email-notifications queue
	emailQueue, exists := vhost1.queues["email-notifications"]
	assert.True(t, exists, "email-notifications queue should exist")
	if assert.NotNil(t, emailQueue) {
		assert.False(t, emailQueue.Durable)
		assert.Contains(t, emailQueue.Bindings, "notifications:email.*")
	}

	// Verify exchange bindings were created
	if ordersEx != nil {
		ordersEx.mu.RLock()
		assert.Len(t, ordersEx.Bindings["new"], 1, "should have one binding for 'new' routing key")
		assert.Equal(t, "order-processing", ordersEx.Bindings["new"][0])
		assert.Len(t, ordersEx.Bindings["update"], 1, "should have one binding for 'update' routing key")
		assert.Equal(t, "order-processing", ordersEx.Bindings["update"][0])
		ordersEx.mu.RUnlock()
	}

	if eventsEx != nil {
		eventsEx.mu.RLock()
		assert.Len(t, eventsEx.Bindings[""], 1, "should have one binding for empty routing key")
		assert.Equal(t, "all-events", eventsEx.Bindings[""][0])
		eventsEx.mu.RUnlock()
	}
	vhost1.mu.RUnlock()

	// Verify /staging vhost
	vhost2.mu.RLock()
	assert.Len(t, vhost2.exchanges, 2, "should have 2 exchanges (1 configured + 1 default)")
	assert.Len(t, vhost2.queues, 1, "should have 1 queue")

	testQueue, exists := vhost2.queues["test-queue"]
	assert.True(t, exists, "test-queue should exist")
	if assert.NotNil(t, testQueue) {
		assert.False(t, testQueue.Durable)
		assert.True(t, testQueue.Exclusive)
		assert.Contains(t, testQueue.Bindings, "test-exchange:test")
	}
	vhost2.mu.RUnlock()
}

func TestWithVHosts_DefaultVHost(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/", // Default vhost
			Exchanges: []config.ExchangeConfig{
				{Name: "custom-exchange", Type: "direct", Durable: true},
			},
			Queues: []config.QueueConfig{
				{
					Name:    "custom-queue",
					Durable: true,
					Bindings: map[string]bool{
						"custom-exchange:test": true,
					},
				},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	// Verify default vhost still exists and has our custom additions
	vhost, err := server.GetVHost("/")
	assert.NoError(t, err, "default vhost should exist")
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	// Should have default "" exchange + our custom exchange
	assert.Len(t, vhost.exchanges, 2, "should have 2 exchanges")
	assert.Contains(t, vhost.exchanges, "custom-exchange")

	customEx := vhost.exchanges["custom-exchange"]
	assert.Equal(t, "direct", customEx.Type)
	assert.True(t, customEx.Durable)

	assert.Len(t, vhost.queues, 1, "should have 1 queue")
	assert.Contains(t, vhost.queues, "custom-queue")

	customQueue := vhost.queues["custom-queue"]
	assert.True(t, customQueue.Durable)
	assert.Contains(t, customQueue.Bindings, "custom-exchange:test")
}

func TestWithVHosts_EmptyConfiguration(t *testing.T) {
	IsTerminal = true
	// Test with empty vhost list
	server := NewServer(WithVHosts([]config.VHostConfig{}))

	// Should still have default vhost
	vhost, err := server.GetVHost("/")
	assert.NoError(t, err, "default vhost should exist even with empty config")
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	assert.Len(t, vhost.exchanges, 1, "should have 1 exchange (default)")
	assert.Contains(t, vhost.exchanges, "")
	assert.Len(t, vhost.queues, 0, "should have 0 queues")
}

func TestWithVHosts_ExistingExchangesAndQueues(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/test",
			Exchanges: []config.ExchangeConfig{
				{Name: "", Type: "direct", Durable: true}, // Try to recreate default exchange
				{Name: "new-exchange", Type: "fanout", Durable: false},
			},
			Queues: []config.QueueConfig{
				{Name: "new-queue", Durable: true},
				{Name: "duplicate-queue", Durable: false},
				{Name: "duplicate-queue", Durable: true}, // Duplicate in config
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	vhost, err := server.GetVHost("/test")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	// Should skip the default "" exchange and create only the new one
	assert.Len(t, vhost.exchanges, 2, "should have 2 exchanges (default + new-exchange)")
	assert.Contains(t, vhost.exchanges, "")
	assert.Contains(t, vhost.exchanges, "new-exchange")

	newEx := vhost.exchanges["new-exchange"]
	assert.Equal(t, "fanout", newEx.Type)
	assert.False(t, newEx.Durable)

	// Should create only one instance of duplicate-queue (first one wins)
	assert.Len(t, vhost.queues, 2, "should have 2 queues (new-queue + first duplicate-queue)")
	assert.Contains(t, vhost.queues, "new-queue")
	assert.Contains(t, vhost.queues, "duplicate-queue")

	duplicateQueue := vhost.queues["duplicate-queue"]
	assert.False(t, duplicateQueue.Durable, "first duplicate-queue config should win (Durable=false)")

	newQueue := vhost.queues["new-queue"]
	assert.True(t, newQueue.Durable)
}

func TestWithVHosts_InvalidBindings(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/test-bindings",
			Exchanges: []config.ExchangeConfig{
				{Name: "valid-exchange", Type: "direct", Durable: false},
			},
			Queues: []config.QueueConfig{
				{
					Name: "test-queue",
					Bindings: map[string]bool{
						"invalid-format":       true, // Missing ':'
						"nonexistent:key":      true, // Exchange doesn't exist
						"valid-exchange:valid": true, // Valid binding
						"":                     true, // Empty binding
						"too:many:colons:here": true, // Too many colons (should use first split)
					},
				},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	vhost, err := server.GetVHost("/test-bindings")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	queue := vhost.queues["test-queue"]
	assert.NotNil(t, queue, "test-queue should exist")

	// Should only have valid bindings in queue.Bindings
	validBindings := []string{"valid-exchange:valid", "too:many:colons:here"}
	actualValidBindings := 0
	for binding := range queue.Bindings {
		for _, validBinding := range validBindings {
			if binding == validBinding {
				actualValidBindings++
				break
			}
		}
	}
	assert.Equal(t, 1, actualValidBindings, "should have 1 valid binding")

	// Check exchange bindings
	validEx, exists := vhost.exchanges["valid-exchange"]
	assert.True(t, exists, "valid-exchange should exist")
	if validEx != nil {
		validEx.mu.RLock()
		assert.Len(t, validEx.Bindings["valid"], 1, "should have binding for 'valid' routing key")
		assert.Equal(t, "test-queue", validEx.Bindings["valid"][0])
		validEx.mu.RUnlock()
	}
}

func TestWithVHosts_OnlyExchanges(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/exchanges-only",
			Exchanges: []config.ExchangeConfig{
				{Name: "exchange1", Type: "direct", Durable: true},
				{Name: "exchange2", Type: "fanout", Durable: false},
			},
			// No queues
		},
	}

	server := NewServer(WithVHosts(vhosts))

	vhost, err := server.GetVHost("/exchanges-only")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	assert.Len(t, vhost.exchanges, 3, "should have 3 exchanges (2 configured + 1 default)")
	assert.Contains(t, vhost.exchanges, "exchange1")
	assert.Contains(t, vhost.exchanges, "exchange2")
	assert.Contains(t, vhost.exchanges, "")

	assert.Len(t, vhost.queues, 0, "should have 0 queues")

	ex1 := vhost.exchanges["exchange1"]
	assert.Equal(t, "direct", ex1.Type)
	assert.True(t, ex1.Durable)

	ex2 := vhost.exchanges["exchange2"]
	assert.Equal(t, "fanout", ex2.Type)
	assert.False(t, ex2.Durable)
}

func TestWithVHosts_OnlyQueues(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/queues-only",
			// No exchanges
			Queues: []config.QueueConfig{
				{Name: "queue1", Durable: true},
				{Name: "queue2", Durable: false, Exclusive: true},
				{
					Name: "queue-with-default-binding",
					Bindings: map[string]bool{
						":some-key": true, // Binding to default exchange
					},
				},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	vhost, err := server.GetVHost("/queues-only")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	assert.Len(t, vhost.exchanges, 1, "should have 1 exchange (default only)")
	assert.Contains(t, vhost.exchanges, "")

	assert.Len(t, vhost.queues, 3, "should have 3 queues")
	assert.Contains(t, vhost.queues, "queue1")
	assert.Contains(t, vhost.queues, "queue2")
	assert.Contains(t, vhost.queues, "queue-with-default-binding")

	queue1 := vhost.queues["queue1"]
	assert.True(t, queue1.Durable)
	assert.False(t, queue1.Exclusive)

	queue2 := vhost.queues["queue2"]
	assert.False(t, queue2.Durable)
	assert.True(t, queue2.Exclusive)

	// Check binding to default exchange
	defaultEx := vhost.exchanges[""]
	assert.NotNil(t, defaultEx)
	defaultEx.mu.RLock()
	assert.Len(t, defaultEx.Bindings["some-key"], 1, "should have binding to default exchange")
	assert.Equal(t, "queue-with-default-binding", defaultEx.Bindings["some-key"][0])
	defaultEx.mu.RUnlock()
}

func TestWithVHosts_MultipleVHostsWithSameNames(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/duplicate",
			Exchanges: []config.ExchangeConfig{
				{Name: "exchange1", Type: "direct", Durable: true},
			},
		},
		{
			Name: "/duplicate", // Same name
			Exchanges: []config.ExchangeConfig{
				{Name: "exchange2", Type: "fanout", Durable: false},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	vhost, err := server.GetVHost("/duplicate")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	// Should have both exchanges since the vhost already exists when processing the second config
	assert.Len(t, vhost.exchanges, 2, "should have 2 exchanges (default + exchange1)")
	assert.Contains(t, vhost.exchanges, "")
	assert.Contains(t, vhost.exchanges, "exchange1")

	ex1 := vhost.exchanges["exchange1"]
	assert.Equal(t, "direct", ex1.Type)
	assert.True(t, ex1.Durable)
}

func TestWithVHosts_CombinedWithOtherOptions(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/combined-test",
			Exchanges: []config.ExchangeConfig{
				{Name: "test-exchange", Type: "direct", Durable: true},
			},
		},
	}

	credentials := map[string]string{"user": "pass"}

	server := NewServer(
		WithVHosts(vhosts),
		WithAuth(credentials),
	)

	// Verify vhost configuration worked
	vhost, err := server.GetVHost("/combined-test")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	assert.Contains(t, vhost.exchanges, "test-exchange")
	testEx := vhost.exchanges["test-exchange"]
	assert.Equal(t, "direct", testEx.Type)
	assert.True(t, testEx.Durable)
	vhost.mu.RUnlock()

	// Verify auth configuration worked
	assert.Equal(t, config.AuthModePlain, server.authMode)
	assert.Len(t, server.credentials, 1)
	assert.Contains(t, server.credentials, "user")
	assert.Equal(t, "pass", server.credentials["user"])
}

func TestWithVHosts_ComplexBindingScenarios(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/complex-bindings",
			Exchanges: []config.ExchangeConfig{
				{Name: "direct-ex", Type: "direct", Durable: true},
				{Name: "topic-ex", Type: "topic", Durable: true},
				{Name: "fanout-ex", Type: "fanout", Durable: true},
			},
			Queues: []config.QueueConfig{
				{
					Name: "multi-bound-queue",
					Bindings: map[string]bool{
						"direct-ex:exact":      true,
						"topic-ex:user.#":      true,
						"topic-ex:*.important": true,
						"fanout-ex:":           true, // Empty routing key for fanout
					},
				},
				{
					Name: "single-bound-queue",
					Bindings: map[string]bool{
						"direct-ex:single": true,
					},
				},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	vhost, err := server.GetVHost("/complex-bindings")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	// Verify multi-bound-queue has all bindings
	multiQueue := vhost.queues["multi-bound-queue"]
	assert.NotNil(t, multiQueue)
	assert.Len(t, multiQueue.Bindings, 4, "multi-bound-queue should have 4 bindings")
	assert.Contains(t, multiQueue.Bindings, "direct-ex:exact")
	assert.Contains(t, multiQueue.Bindings, "topic-ex:user.#")
	assert.Contains(t, multiQueue.Bindings, "topic-ex:*.important")
	assert.Contains(t, multiQueue.Bindings, "fanout-ex:")

	// Verify single-bound-queue
	singleQueue := vhost.queues["single-bound-queue"]
	assert.NotNil(t, singleQueue)
	assert.Len(t, singleQueue.Bindings, 1, "single-bound-queue should have 1 binding")
	assert.Contains(t, singleQueue.Bindings, "direct-ex:single")

	// Verify exchange bindings
	directEx := vhost.exchanges["direct-ex"]
	assert.NotNil(t, directEx)
	directEx.mu.RLock()
	assert.Len(t, directEx.Bindings["exact"], 1)
	assert.Equal(t, "multi-bound-queue", directEx.Bindings["exact"][0])
	assert.Len(t, directEx.Bindings["single"], 1)
	assert.Equal(t, "single-bound-queue", directEx.Bindings["single"][0])
	directEx.mu.RUnlock()

	topicEx := vhost.exchanges["topic-ex"]
	assert.NotNil(t, topicEx)
	topicEx.mu.RLock()
	assert.Len(t, topicEx.Bindings["user.#"], 1)
	assert.Equal(t, "multi-bound-queue", topicEx.Bindings["user.#"][0])
	assert.Len(t, topicEx.Bindings["*.important"], 1)
	assert.Equal(t, "multi-bound-queue", topicEx.Bindings["*.important"][0])
	topicEx.mu.RUnlock()

	fanoutEx := vhost.exchanges["fanout-ex"]
	assert.NotNil(t, fanoutEx)
	fanoutEx.mu.RLock()
	assert.Len(t, fanoutEx.Bindings[""], 1) // Empty routing key
	assert.Equal(t, "multi-bound-queue", fanoutEx.Bindings[""][0])
	fanoutEx.mu.RUnlock()
}

func TestWithVHosts_ErrorRecovery(t *testing.T) {
	IsTerminal = true
	// Test that errors in one vhost don't prevent processing others
	vhosts := []config.VHostConfig{
		{
			Name: "", // Invalid empty vhost name
			Exchanges: []config.ExchangeConfig{
				{Name: "should-not-exist", Type: "direct"},
			},
		},
		{
			Name: "/valid-vhost",
			Exchanges: []config.ExchangeConfig{
				{Name: "should-exist", Type: "direct"},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	// First vhost should fail to create due to empty name
	_, err := server.GetVHost("")
	assert.Error(t, err, "empty vhost name should cause error")

	// Second vhost should still be created successfully
	vhost, err := server.GetVHost("/valid-vhost")
	assert.NoError(t, err, "valid vhost should exist despite earlier error")
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	assert.Contains(t, vhost.exchanges, "should-exist")
	shouldExistEx := vhost.exchanges["should-exist"]
	assert.Equal(t, "direct", shouldExistEx.Type)
}

func TestWithVHosts_QueuePropertiesValidation(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/queue-props-test",
			Queues: []config.QueueConfig{
				{
					Name:       "durable-queue",
					Durable:    true,
					Exclusive:  false,
					AutoDelete: false,
				},
				{
					Name:       "exclusive-queue",
					Durable:    false,
					Exclusive:  true,
					AutoDelete: false,
				},
				{
					Name:       "autodelete-queue",
					Durable:    false,
					Exclusive:  false,
					AutoDelete: true,
				},
				{
					Name:       "all-flags-queue",
					Durable:    true,
					Exclusive:  true,
					AutoDelete: true,
				},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	vhost, err := server.GetVHost("/queue-props-test")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	assert.Len(t, vhost.queues, 4)

	// Test durable queue
	durableQueue := vhost.queues["durable-queue"]
	assert.NotNil(t, durableQueue)
	assert.True(t, durableQueue.Durable)
	assert.False(t, durableQueue.Exclusive)
	assert.False(t, durableQueue.AutoDelete)

	// Test exclusive queue
	exclusiveQueue := vhost.queues["exclusive-queue"]
	assert.NotNil(t, exclusiveQueue)
	assert.False(t, exclusiveQueue.Durable)
	assert.True(t, exclusiveQueue.Exclusive)
	assert.False(t, exclusiveQueue.AutoDelete)

	// Test autodelete queue
	autodeleteQueue := vhost.queues["autodelete-queue"]
	assert.NotNil(t, autodeleteQueue)
	assert.False(t, autodeleteQueue.Durable)
	assert.False(t, autodeleteQueue.Exclusive)
	assert.True(t, autodeleteQueue.AutoDelete)

	// Test all flags queue
	allFlagsQueue := vhost.queues["all-flags-queue"]
	assert.NotNil(t, allFlagsQueue)
	assert.True(t, allFlagsQueue.Durable)
	assert.True(t, allFlagsQueue.Exclusive)
	assert.True(t, allFlagsQueue.AutoDelete)
}

func TestWithVHosts_ExchangePropertiesValidation(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/exchange-props-test",
			Exchanges: []config.ExchangeConfig{
				{
					Name:       "durable-direct",
					Type:       "direct",
					Durable:    true,
					AutoDelete: false,
					Internal:   false,
				},
				{
					Name:       "temp-fanout",
					Type:       "fanout",
					Durable:    false,
					AutoDelete: true,
					Internal:   false,
				},
				{
					Name:       "internal-topic",
					Type:       "topic",
					Durable:    true,
					AutoDelete: false,
					Internal:   true,
				},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	vhost, err := server.GetVHost("/exchange-props-test")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	assert.Len(t, vhost.exchanges, 4) // 3 configured + 1 default

	// Test durable direct exchange
	durableEx := vhost.exchanges["durable-direct"]
	assert.NotNil(t, durableEx)
	assert.Equal(t, "direct", durableEx.Type)
	assert.True(t, durableEx.Durable)
	assert.False(t, durableEx.AutoDelete)
	assert.False(t, durableEx.Internal)

	// Test temporary fanout exchange
	tempEx := vhost.exchanges["temp-fanout"]
	assert.NotNil(t, tempEx)
	assert.Equal(t, "fanout", tempEx.Type)
	assert.False(t, tempEx.Durable)
	assert.True(t, tempEx.AutoDelete)
	assert.False(t, tempEx.Internal)

	// Test internal topic exchange
	internalEx := vhost.exchanges["internal-topic"]
	assert.NotNil(t, internalEx)
	assert.Equal(t, "topic", internalEx.Type)
	assert.True(t, internalEx.Durable)
	assert.False(t, internalEx.AutoDelete)
	assert.True(t, internalEx.Internal)
}

func TestWithVHosts_EmptyBindingsMap(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/empty-bindings-test",
			Exchanges: []config.ExchangeConfig{
				{Name: "test-exchange", Type: "direct", Durable: false},
			},
			Queues: []config.QueueConfig{
				{
					Name:     "queue-no-bindings",
					Durable:  false,
					Bindings: map[string]bool{}, // Empty bindings map
				},
				{
					Name:    "queue-nil-bindings",
					Durable: false,
					// Bindings: nil (implicitly)
				},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	vhost, err := server.GetVHost("/empty-bindings-test")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	assert.Len(t, vhost.queues, 2)

	// Queue with empty bindings map
	queue1 := vhost.queues["queue-no-bindings"]
	assert.NotNil(t, queue1)
	assert.NotNil(t, queue1.Bindings)
	assert.Len(t, queue1.Bindings, 0)

	// Queue with nil bindings (should be initialized)
	queue2 := vhost.queues["queue-nil-bindings"]
	assert.NotNil(t, queue2)
	assert.NotNil(t, queue2.Bindings)
	assert.Len(t, queue2.Bindings, 0)
}

// Benchmark test for large configurations
// func BenchmarkWithVHosts_LargeConfig(b *testing.B) {
// 	isTerminal = true
// 	// Create a large configuration
// 	var vhosts []VHostConfig
// 	for i := 0; i < 10; i++ {
// 		var exchanges []config.ExchangeConfig
// 		var queues []config.QueueConfig

// 		for j := 0; j < 10; j++ {
// 			exchanges = append(exchanges, config.ExchangeConfig{
// 				Name:    fmt.Sprintf("exchange-%d-%d", i, j),
// 				Type:    "direct",
// 				Durable: true,
// 			})
// 		}

// 		for j := 0; j < 20; j++ {
// 			bindings := make(map[string]bool)
// 			for k := 0; k < 5; k++ {
// 				bindings[fmt.Sprintf("exchange-%d-%d:key-%d", i, k, j)] = true
// 			}
// 			queues = append(queues, config.QueueConfig{
// 				Name:     fmt.Sprintf("queue-%d-%d", i, j),
// 				Durable:  true,
// 				Bindings: bindings,
// 			})
// 		}

// 		vhosts = append(vhosts, VHostConfig{
// 			Name:      fmt.Sprintf("/vhost-%d", i),
// 			exchanges: exchanges,
// 			queues:    queues,
// 		})
// 	}

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		server := NewServer(WithVHosts(vhosts))
// 		_ = server
// 	}
// }

func TestWithVHosts_SpecialCharactersInNames(t *testing.T) {
	IsTerminal = true
	vhosts := []config.VHostConfig{
		{
			Name: "/special-chars-test",
			Exchanges: []config.ExchangeConfig{
				{Name: "exchange.with.dots", Type: "direct", Durable: false},
				{Name: "exchange-with-dashes", Type: "fanout", Durable: false},
				{Name: "exchange_with_underscores", Type: "topic", Durable: false},
			},
			Queues: []config.QueueConfig{
				{
					Name: "queue.with.dots",
					Bindings: map[string]bool{
						"exchange.with.dots:key.with.dots": true,
					},
				},
				{
					Name: "queue-with-dashes",
					Bindings: map[string]bool{
						"exchange-with-dashes:": true, // Fanout binding
					},
				},
				{
					Name: "queue_with_underscores",
					Bindings: map[string]bool{
						"exchange_with_underscores:topic.#": true,
					},
				},
			},
		},
	}

	server := NewServer(WithVHosts(vhosts))

	vhost, err := server.GetVHost("/special-chars-test")
	assert.NoError(t, err)
	assert.NotNil(t, vhost)

	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	// Verify all exchanges and queues were created with special characters
	assert.Contains(t, vhost.exchanges, "exchange.with.dots")
	assert.Contains(t, vhost.exchanges, "exchange-with-dashes")
	assert.Contains(t, vhost.exchanges, "exchange_with_underscores")

	assert.Contains(t, vhost.queues, "queue.with.dots")
	assert.Contains(t, vhost.queues, "queue-with-dashes")
	assert.Contains(t, vhost.queues, "queue_with_underscores")

	// Verify bindings work with special characters
	dotsEx := vhost.exchanges["exchange.with.dots"]
	dotsEx.mu.RLock()
	assert.Contains(t, dotsEx.Bindings, "key.with.dots")
	assert.Equal(t, "queue.with.dots", dotsEx.Bindings["key.with.dots"][0])
	dotsEx.mu.RUnlock()

	dashesEx := vhost.exchanges["exchange-with-dashes"]
	dashesEx.mu.RLock()
	assert.Contains(t, dashesEx.Bindings, "")
	assert.Equal(t, "queue-with-dashes", dashesEx.Bindings[""][0])
	dashesEx.mu.RUnlock()

	underscoresEx := vhost.exchanges["exchange_with_underscores"]
	underscoresEx.mu.RLock()
	assert.Contains(t, underscoresEx.Bindings, "topic.#")
	assert.Equal(t, "queue_with_underscores", underscoresEx.Bindings["topic.#"][0])
	underscoresEx.mu.RUnlock()
}
