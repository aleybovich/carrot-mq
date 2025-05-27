# Changelog

## [0.0.1] 2025-05-27

### Added
- Basic AMQP 0-9-1 protocol support
- Connection handling with PLAIN authentication mechanism
- Channel management with proper error handling
- Exchange types support:
  - Direct exchange
  - Fanout exchange
  - Topic exchange with pattern matching
  - Headers exchange (basic structure only)
- Queue management:
  - Queue declaration with durable, exclusive, and auto-delete flags
  - Queue binding to exchanges
- Message handling:
  - Basic.Publish
  - Basic.Consume
  - Message delivery to consumers
  - Support for mandatory and immediate flags (structure only)
- Topic exchange pattern matching with # and * wildcards
- Colored logging with different log levels (DEBUG, INFO, WARN, ERROR, FATAL)
- Connection and channel cleanup on close
- Channel.Close and Connection.Close with proper AMQP error codes
- Timeout handling for Channel.CloseOk responses
- Support for server-generated queue names
- Custom logger interface for external logging integration

### Not Implemented
- Publisher confirms
- Transaction support
- Exchange-to-exchange bindings
- Queue deletion
- Queue purge
- Basic.Get
- Basic.Ack/Nack/Reject
- Basic.Recover
- Basic.Return for unroutable messages
- Channel flow control
- Heartbeat monitoring
- SSL/TLS support
- Full headers exchange functionality
- Virtual hosts support
- Full implementation of immediate flag
- Auto-delete functionality for exchanges and queues
- Connection.Blocked/Unblocked
- Consumer priorities
- Per-consumer QoS
- Message persistence
