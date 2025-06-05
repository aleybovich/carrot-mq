# AMQP-Go Server

A lightweight, pure Go implementation of the AMQP 0-9-1 protocol, designed for simulations, testing, and development environments.

## Overview

This project implements core functionality of the AMQP 0-9-1 protocol (the protocol used by RabbitMQ and other message brokers) in Go. It provides a server that speaks the AMQP protocol, allowing client applications to connect and use standard AMQP libraries such as `amqp091-go` to interact with it.

## Features

- **Protocol Compliance**: Implements key parts of the AMQP 0-9-1 protocol.
- **Exchange Types**: Supports `direct`, `fanout`, and `topic` exchange types for message routing.
- **Queue Operations**: Create queues, bind to exchanges, and consume messages.
- **Messaging**: Publish and consume messages with routing keys, including support for `basic.get`.
- **Publisher Confirms**: Server-side support for publisher acknowledgments (`confirm.select`, `basic.ack`/`basic.nack` sent to publisher).
- **Consumer Acknowledgements**: Handles `basic.ack`, `basic.nack`, and `basic.reject` from consumers.
- **Message Requeuing**: Supports requeuing of messages on `basic.nack`/`basic.reject` (if requested by client) and on connection drops for unacknowledged messages. Also supports `basic.recover`.
- **Connection Management**: Handles AMQP connections, channels, and heartbeats.
- **Virtual Hosts**: Supports multiple virtual hosts, configurable at startup using `WithVHosts` (see "Advanced Server Configuration"). Each vhost automatically gets a default direct exchange (`""`).
- **Authentication**: Supports PLAIN authentication mechanism via the `WithAuth` server option.
- **Colorized Logging**: Detailed, readable logs with syntax highlighting (enable debug logs with `AMQP_DEBUG=1` environment variable).
- **Custom Logging**: Pluggable logger interface for integration with external logging systems.
- **Connection Cleanup**: Proper resource cleanup when connections are closed, including requeuing of unacknowledged messages.
- **Heartbeat Support**: Handles client heartbeats for connection health monitoring.
- **Graceful Shutdown**: Supports standard AMQP connection and channel closure protocols.

## Implemented AMQP Operations

- **Connection**: `start`, `start-ok`, `tune`, `tune-ok`, `open`, `open-ok`, `close`, `close-ok`
- **Channel**: `open`, `open-ok`, `close`, `close-ok`
- **Exchange**: `declare`, `declare-ok` (for types: direct, fanout, topic)
- **Queue**: `declare`, `declare-ok`, `bind`, `bind-ok`
- **Basic**: `publish`, `consume`, `consume-ok`, `deliver`, `get`, `get-ok`, `get-empty`, `ack` (consumer ack), `nack` (consumer nack), `reject`, `cancel`, `cancel-ok`, `recover`, `recover-ok`, `return` (sent by server for unroutable mandatory messages)
- **Confirm**: `select`, `select-ok` (server also sends `basic.ack`/`basic.nack` to publisher if confirm mode is active on the channel)

## Usage

### Running the Server

```go
package main

import (
    "log"
    "os"
    // If NewServer and other types are in a different package, adjust imports.
    // For this example, assume they are in the 'main' package or accessible.
)

func main() {
    server := NewServer() // Or NewServer(options...)
    if err := server.Start(":5672"); err != nil {
        log.Fatalf("Failed to start server: %v", err)
        os.Exit(1)
    }
}
```

### Connecting with an AMQP Client

```go
package main // Or your client package

import (
    "log"
    amqp "github.com/rabbitmq/amqp091-go" // Standard RabbitMQ Go client
)

func ConnectAndDeclare() {
    // Connect to the server
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/") // Default guest/guest if no auth configured
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    // Create a channel
    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Failed to open channel: %v", err)
    }
    defer ch.Close()

    // Declare an exchange (direct, fanout, or topic)
    err = ch.ExchangeDeclare(
        "my-exchange", // name
        "direct",      // type (can also be "fanout", "topic")
        true,          // durable (server respects flag, but no persistence)
        false,         // auto-deleted
        false,         // internal
        false,         // no-wait
        nil,           // arguments
    )
    if err != nil {
        log.Fatalf("Failed to declare exchange: %v", err)
    }
    log.Println("Exchange 'my-exchange' declared")

    // Declare a queue
    q, err := ch.QueueDeclare(
        "my-queue",    // name
        true,          // durable
        false,         // delete when unused
        false,         // exclusive
        false,         // no-wait
        nil,           // arguments
    )
    if err != nil {
        log.Fatalf("Failed to declare queue: %v", err)
    }
    log.Printf("Queue '%s' declared", q.Name)

    // Bind queue to exchange
    err = ch.QueueBind(
        q.Name,        // queue name
        "my_routing_key", // routing key
        "my-exchange", // exchange
        false,
        nil,
    )
    if err != nil {
        log.Fatalf("Failed to bind queue: %v", err)
    }
    log.Printf("Queue '%s' bound to exchange 'my-exchange' with key 'my_routing_key'", q.Name)
}
```

## Storage and Persistence

The server supports pluggable storage backends for message and metadata persistence. By default, the server runs without persistence (all data is in-memory), but you can enable various storage options.

### Built-in Storage Providers

#### In-Memory Storage
Fast, no persistence across restarts. Useful for development and testing.

```go
server := NewServer(
    WithInMemoryStorage(),
)
```

#### BuntDB Storage
File-based embedded database providing persistence with good performance.

```go
// Persistent storage to file
server := NewServer(
    WithBuntDBStorage("./amqp-data.db"),
)

// Or use the full configuration
server := NewServer(
    WithStorage(StorageConfig{
        Type: StorageTypeBuntDB,
        BuntDB: &BuntDBConfig{
            Path: "/var/lib/amqp/data.db",
        },
    }),
)
```

#### No Storage (Default)
Explicitly disable persistence (default behavior, not necessary to provide).

```go
server := NewServer(
    WithNoStorage(),
)
```

### What Gets Persisted?

When persistence is enabled, the following entities are saved:
- **Virtual Hosts**: All vhost configurations
- **Exchanges**: Exchange declarations (name, type, durability flags)
- **Queues**: Queue declarations (name, durability flags) - excluding exclusive queues
- **Bindings**: Queue-to-exchange binding relationships
- **Messages**: Messages with delivery mode 2 (persistent) sent to durable queues
- **Message Order**: Message sequence is preserved within each queue

### Recovery on Restart

When the server starts with persistence enabled:
1. All durable entities (vhosts, exchanges, queues, bindings) are restored
2. Persistent messages in durable queues are recovered in order
3. Exclusive queues are not recovered (per AMQP specification)
4. All recovered messages are marked as redelivered

### Custom Storage Providers

You can implement custom storage backends by implementing the `StorageProvider` interface. This is useful for testing or integrating with existing infrastructure.

```go
// Use a custom storage provider
customProvider := NewCustomStorageProvider()
server := NewServer(
    WithStorageProvider(customProvider),
)
```

The `StorageProvider` interface includes methods for basic CRUD operations, batch operations, key scanning, and transaction support.

### Storage Configuration Examples

```go
// Testing setup - in-memory with inspection
testServer := NewServer(
    WithInMemoryStorage(),
)
```

The storage abstraction ensures that switching between providers requires only a configuration change, not code modifications.

## Advanced Server Configuration

The server supports several options for customization at startup, passed to the `NewServer` function.

### Configuring Authentication

Enable PLAIN authentication by providing a map of credentials:
```go
// main.go (or your server setup file)
package main

import (
	"log"
	"os"
)
// Assuming NewServer, WithAuth are in the current package
func main() {
    credentials := map[string]string{
        "user1": "pass1",
        "admin": "secret",
    }
    server := NewServer(WithAuth(credentials))
    // ... start server
    if err := server.Start(":5672"); err != nil {
        log.Fatalf("Failed to start server: %v", err)
        os.Exit(1)
    }
}
```
Clients would then connect using `amqp://user1:pass1@localhost:5672/`.

### Configuring Virtual Hosts, Exchanges, and Queues at Startup

The server supports pre-configuring virtual hosts, exchanges, queues, and bindings at startup using the `WithVHosts` server option. This is useful for setting up a predefined topology when the server starts.

The `WithVHosts` option takes a slice of `VHostConfig` structs. Each `VHostConfig` can define:
- The vhost name.
- A slice of `ExchangeConfig` for exchanges to be created within that vhost.
- A slice of `QueueConfig` for queues to be created within that vhost.

**`ExchangeConfig` fields:**
- `Name`: Name of the exchange.
- `Type`: Type of the exchange (e.g., "direct", "fanout", "topic").
- `Durable`: bool
- `AutoDelete`: bool
- `Internal`: bool

**`QueueConfig` fields:**
- `Name`: Name of the queue.
- `Durable`: bool
- `Exclusive`: bool
- `AutoDelete`: bool
- `Bindings`: A `map[string]bool` where the key is a string in the format `"exchangeName:routingKey"` to bind the queue to. The boolean value is currently unused but must be present. For the default exchange, use `"": "routingKey"`. For fanout exchanges, the routing key part is often empty or ignored by convention, so you might use `"exchangeName:"`.

#### Example:

```go
// main.go (or your server setup file)
package main

import (
	"log"
	"os"
)
// Assuming VHostConfig, ExchangeConfig, QueueConfig, NewServer, WithVHosts
// are in the current package.

func main() {
	vhostConfigs := []VHostConfig{
		{
			name: "/", // Configure the default vhost
			exchanges: []ExchangeConfig{
				{Name: "app_direct_logs", Type: "direct", Durable: true},
				{Name: "app_topic_events", Type: "topic", Durable: true},
			},
			queues: []QueueConfig{
				{
					Name:    "tasks",
					Durable: true,
					Bindings: map[string]bool{
						"": "tasks", // Bind to default exchange with routing key "tasks"
					},
				},
				{
					Name:    "errors_log_queue",
					Durable: false,
					Bindings: map[string]bool{
						"app_direct_logs":  "error",          // Bind to app_direct_logs with routing key "error"
						"app_topic_events": "events.error.#", // Bind to app_topic_events with pattern "events.error.#"
					},
				},
			},
		},
		{
			name: "finance_vhost", // Define a custom virtual host
			exchanges: []ExchangeConfig{
				{Name: "transactions", Type: "fanout", Durable: true},
			},
			queues: []QueueConfig{
				{
					Name:    "audit_trail_queue",
					Durable: true,
					Bindings: map[string]bool{
						// Binding to a fanout exchange. Routing key part of "exchangeName:routingKey" is usually ignored.
						"transactions": "",
					},
				},
			},
		},
	}

	server := NewServer(
		WithVHosts(vhostConfigs),
		// WithAuth(map[string]string{"user": "password"}), // Optionally add auth
	)

	if err := server.Start(":5672"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
		os.Exit(1)
	}
}
```

## Quality of Service (QoS)

The server implements basic QoS functionality to control message delivery flow to consumers.

### Prefetch Limits

QoS allows you to limit how many unacknowledged messages a consumer can have at any given time. This prevents fast producers from overwhelming slow consumers.

```go
// Set QoS on a channel
err = channel.Qos(
    1,     // prefetch count - max unacknowledged messages
    0,     // prefetch size - max unacknowledged message size in bytes (0 = unlimited)
    false, // global - false means per-consumer, true means per-channel
)
```

### How It Works

- **Prefetch Count**: Limits the number of unacknowledged messages. If set to 10, the server will stop sending new messages to a consumer once it has 10 unacknowledged messages.
- **Prefetch Size**: Limits the total size of unacknowledged message bodies in bytes. If set to 1048576 (1MB), the server stops sending when the total size of unacked messages reaches this limit.
- **Global Flag**: When false (default), limits apply per consumer. When true, limits apply to all consumers on the channel combined.

### Example Usage

```go
// Connect and create channel
conn, _ := amqp.Dial("amqp://localhost:5672/")
channel, _ := conn.Channel()

// Set prefetch to process one message at a time
channel.Qos(1, 0, false)

// Start consuming
messages, _ := channel.Consume("my-queue", "", false, false, false, false, nil)

for msg := range messages {
    // Process message
    processMessage(msg)
    
    // Acknowledge when done - this allows the next message to be delivered
    msg.Ack(false)
}
```

### Important Notes

- QoS only applies when auto-acknowledgment is **disabled** (autoAck = false when consuming)
- With autoAck = true, messages are acknowledged immediately upon delivery and QoS has no effect
- The server tracks unacknowledged messages per consumer
- When a connection is closed, all unacknowledged messages are automatically requeued


## Transactions

The server implements AMQP 0-9-1 transaction support, allowing clients to group multiple operations that will be executed atomically.

### Supported Operations

Transactions can include:
- Message publishing (`basic.publish`)
- Message acknowledgments (`basic.ack`)
- Message negative acknowledgments (`basic.nack`)
- Message rejection (`basic.reject`)

All operations are buffered until either committed (executed atomically) or rolled back (discarded). If a channel or connection is closed before committing, all pending operations are automatically discarded.

### Client Usage Example

```go
// Enable transaction mode on a channel
err = channel.Tx()
if err != nil {
    // Handle error
}

// Perform operations within transaction
// These operations won't take effect until commit
err = channel.Publish("exchange", "routing.key", false, false, 
    amqp.Publishing{Body: []byte("message")})
    
// Acknowledge a received message
err = delivery.Ack(false)

// Commit all operations atomically
err = channel.TxCommit()
if err != nil {
    // Handle error
}

// Alternatively, roll back all operations
// err = channel.TxRollback()
```

## Custom Logging Integration

The server supports pluggable logging through the `Logger` interface:

```go
// Create a server with custom logger
myLogger := NewCustomLogger() // You need to implement the Logger interface
server := NewServer(WithLogger(myLogger))
```

### Logger Interface

```go
type Logger interface {
    Fatal(format string, a ...any)
    Err(format string, a ...any)
    Warn(format string, a ...any)
    Info(format string, a ...any)
    Debug(format string, a ...any)
}
```
To see detailed debug logs from the server, set the environment variable `AMQP_DEBUG=1`.

## Default Logging

The server includes detailed logging with color support to make it easier to track AMQP operations:
- 🟢 `[INFO]` - Standard operational messages
- 🟡 `[WARN]` - Warning conditions
- 🔴 `[ERROR]` - Error conditions
- 🟣 `[DEBUG]` - Debug messages (if `AMQP_DEBUG=1` is set)

## Testing

NOTE: due to the number of unit tests, increase `go.testTimeout` to `1m` to avoid intermittent unit test timeouts. This can be done in VSCode settings (`go.testTimeout`) or by running tests with the flag: `go test -v -timeout 1m`.

The project includes comprehensive tests demonstrating the server capabilities:

```bash
# Run all tests
go test -v -timeout 1m

# Run a specific test
go test -v -timeout 1m -run TestServerPublishConsume
```

## Limitations

This implementation is designed for simulation and development purposes:
- **Persistence**: No persistence; all data (messages, queues, exchanges) is in-memory and lost on server restart.
- **Exchange Types**: While `direct`, `fanout`, and `topic` exchanges are supported for routing, `headers` exchanges are not implemented.
- **Security**: Supports PLAIN authentication if configured with credentials. Does not yet implement more advanced security features (e.g., other SASL mechanisms, TLS).
- **Clustering/HA**: No clustering or high-availability features.
- **Durability**: `durable` flags for queues and exchanges are respected for declaration compatibility but do not result in persistence of these entities across server restarts. Similarly, persistent message delivery mode is parsed but messages are not saved to disk.

## Development Status

This is an educational project/work in progress. Not intended for production use.

# Missing Features

This list tracks major AMQP functionalities not yet implemented or only partially implemented.

## Core Messaging
- [ ] **Quality of Service (QoS)**
  - `basic.qos` - prefetch count/size limits.
  - Per-consumer and per-channel flow control (beyond basic consumer buffering).
- [ ] **Headers Exchanges**: Routing based on message headers.

## Queue & Exchange Management
- [ ] **Queue Operations**
  - Full auto-delete queue logic (e.g., delete when last consumer disconnects and other specific conditions are met).
  - Full exclusive queue logic (e.g., strict per-connection isolation and behavior according to spec).
- [ ] **Exchange Operations**
  - `exchange.bind` / `exchange.unbind` - for creating and removing exchange-to-exchange bindings.

## Reliability & Error Handling
- [ ] **Dead Letter Exchanges (DLX)**: Mechanism to route messages that are rejected, nacked (with requeue=false), or expire to an alternate exchange.
- [ ] **Time-To-Live (TTL)**: Support for message and queue TTL, allowing them to expire after a certain period.
- [ ] **Heartbeat Management**: Robust server-side connection timeout detection and automatic closure based on missed heartbeats from the client. Option to configure heartbeat intervals via server settings.

## Persistence & Durability
- [ ] **Message Persistence**:
  - True durable queues and exchanges that survive server restarts.
  - Persistent messages (delivery mode 2) actually saved to disk.
  - Recovery of durable entities (queues, exchanges, messages) on server startup.


## Advanced Features
- [ ] **Connection/Channel Flow Control**: Implementation of `connection.blocked` / `connection.unblocked` and `channel.flow` / `channel.flow-ok` for pausing/resuming message delivery to prevent overwhelming clients or server.
- [ ] **Alternate Exchanges (AE)**: For unroutable messages (distinct from DLX, configured on exchange declaration).

## Monitoring & Management
- [ ] **Server Statistics**: Exposure of detailed metrics (e.g., queue depths, message rates, consumer counts, connection statistics, memory usage).
- [ ] **Management Interface**: An HTTP API or similar interface for inspecting server entities (queues, exchanges, connections) and performing administrative operations (e.g., purging queues, closing connections).

---
