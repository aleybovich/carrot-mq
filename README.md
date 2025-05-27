# AMQP-Go Server

A lightweight, pure Go implementation of the AMQP 0-9-1 protocol, designed for simulations, testing, and development environments.

## Overview

This project implements the core functionality of the AMQP 0-9-1 protocol (the protocol used by RabbitMQ and other message brokers) in Go. It provides a server that speaks the AMQP protocol, allowing client applications to connect and use standard AMQP libraries such as `amqp091-go` to interact with it.

## Features

- **Protocol Compliance**: Implements key parts of the AMQP 0-9-1 protocol
- **Exchange Types**: Supports direct exchange type
- **Queue Operations**: Create queues, bind to exchanges, and consume messages
- **Messaging**: Publish and consume messages with routing keys
- **Connection Management**: Handles AMQP connections, channels, and heartbeats
- **Colorized Logging**: Detailed, readable logs with syntax highlighting
- **Custom Logging**: Pluggable logger interface for integration with external logging systems
- **Connection Cleanup**: Proper resource cleanup when connections are closed
- **Heartbeat Support**: Handles client heartbeats for connection health monitoring
- **Graceful Shutdown**: Supports standard AMQP connection closure protocol

## Implemented AMQP Operations

- **Connection**: `start`, `start-ok`, `tune`, `tune-ok`, `open`, `open-ok`, `close`, `close-ok`
- **Channel**: `open`, `open-ok`, `close`, `close-ok`
- **Exchange**: `declare`, `declare-ok`
- **Queue**: `declare`, `declare-ok`, `bind`, `bind-ok`
- **Basic**: `publish`, `consume`, `consume-ok`, `deliver`

## Usage

### Running the Server

```go
package main

import (
    "log"
    "os"
)

func main() {
    server := NewServer()
    if err := server.Start(":5672"); err != nil {
        log.Fatalf("Failed to start server: %v", err)
        os.Exit(1)
    }
}
```

### Connecting with an AMQP Client

```go
import (
    amqp "github.com/rabbitmq/amqp091-go"
)

// Connect to the server
conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
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

// Declare an exchange
err = ch.ExchangeDeclare(
    "my-exchange", // name
    "direct",      // type
    false,         // durable
    false,         // auto-deleted
    false,         // internal
    false,         // no-wait
    nil,           // arguments
)
```

## Custom Logging Integration

The server supports pluggable logging through the `Logger` interface:

```go
// Create a server with custom logger
myLogger := NewCustomLogger() // Implementing the Logger interface
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

## Default Logging

The server includes detailed logging with color support to make it easier to track AMQP operations:
- 🟢 [INFO] - Standard operational messages
- 🟡 [WARN] - Warning conditions
- 🔴 [ERROR] - Error conditions

## Testing

The project includes comprehensive tests demonstrating the server capabilities:

```bash
# Run all tests
go test -v

# Run a specific test
go test -v -run TestServerPublishConsume
```

## Limitations

This implementation is designed for simulation and development purposes:
- No persistence (all data is in-memory)
- Limited exchange types (currently only direct exchanges are supported)
- Basic security (default credentials only)
- No clustering or high availability features

## Development Status

This is an educational project/work in progress. Not intended for production use.

# Missing Features

## Core Messaging
- [ ] **Message Acknowledgments**
  - `basic.ack` - acknowledge message delivery
  - `basic.nack` - negative acknowledgment with requeue option
  - `basic.reject` - reject single message
  - Unacknowledged message tracking and redelivery
- [ ] **Quality of Service (QoS)**
  - `basic.qos` - prefetch count/size limits
  - Per-consumer and per-channel flow control
- [ ] **Additional Exchange Types**
  - Fanout exchanges (broadcast to all bound queues)
  - Topic exchanges (wildcard pattern matching with `*` and `#`)
  - Headers exchanges (routing based on message headers)

## Queue & Exchange Management
- [ ] **Queue Operations**
  - `queue.delete` - remove queues
  - `queue.purge` - clear queue contents
  - `queue.unbind` - remove queue bindings
  - Auto-delete queues when last consumer disconnects
  - Proper exclusive queue handling (per-connection isolation)
- [ ] **Exchange Operations**
  - `exchange.delete` - remove exchanges
  - `exchange.bind` / `exchange.unbind` - exchange-to-exchange bindings

## Reliability & Error Handling
- [ ] **AMQP Error Codes**
  - Proper error responses with standard AMQP reply codes
  - Channel exceptions (close channel on recoverable errors)
  - Connection exceptions (close connection on fatal errors)
- [ ] **Dead Letter Exchanges**
  - Route failed/rejected messages to alternate exchanges
  - TTL (Time-To-Live) support for messages and queues
- [ ] **Heartbeat Management**
  - Connection timeout detection and automatic closure
  - Configurable heartbeat intervals

## Persistence & Durability
- [ ] **Message Persistence**
  - Durable queues and exchanges (survive server restart)
  - Persistent messages (delivery mode 2)
  - Recovery of durable entities on startup
- [ ] **Transaction Support**
  - `tx.select` / `tx.commit` / `tx.rollback`
  - Atomic message publishing and acknowledgments

## Advanced Features
- [ ] **Publisher Confirms**
  - `confirm.select` - enable publisher acknowledgments
  - `basic.ack` for published messages
  - Message sequence numbering
- [ ] **Consumer Cancel Notifications**
  - `basic.cancel` - stop consuming from queue
  - Server-initiated consumer cancellation
- [ ] **Connection/Channel Flow Control**
  - `connection.blocked` / `connection.unblocked`
  - `channel.flow` - pause/resume message delivery

## Monitoring & Management
- [ ] **Server Statistics**
  - Queue depths, consumer counts
  - Message rates, connection counts
  - Memory usage monitoring
- [ ] **Management Interface**
  - HTTP API for queue/exchange inspection
  - Administrative operations (queue purge, connection close)

---

**Priority Recommendation:** Start with message acknowledgments and QoS as they're fundamental to reliable messaging, followed by additional exchange types for routing flexibility.