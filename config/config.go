package config

// VHostConfig defines configuration for a virtual host
// Used with WithVHosts option for initial server setup
type VHostConfig struct {
	Name      string
	Exchanges []ExchangeConfig
	Queues    []QueueConfig
}

// ExchangeConfig defines configuration for an exchange
type ExchangeConfig struct {
	Name       string
	Type       string // "direct", "fanout", "topic", "headers"
	Durable    bool
	AutoDelete bool
	Internal   bool
}

// QueueConfig defines configuration for a queue
type QueueConfig struct {
	Name       string
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	Bindings   map[string]bool // Exchange bindings: "exchangeName:routingKey" -> true
}
