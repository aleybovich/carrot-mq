# Changelog

## [unreleased]

### Added 
- Basic.Return for unroutable messages
- Basic.Cancel
- Basic.Ack/Nack/Reject
- Basic.Get/GetOk
- Basic.Recover
- Max Frame Size validation
- Publisher confirms
- Improved error handling for frame reading - now returns 502 SYNTAX_ERROR when fails to read

## [0.0.1] 2025-05-27

### Added
- Basic AMQP 0-9-1 protocol support
- Connection handling with PLAIN authentication mechanism
- Channel management with proper error handling
- Exchange types support:
  - Direct exchange
  - Fanout exchange
  - Topic exchange with pattern matching
  - Headers exchange not supported 
- Queue management:
  - Queue declaration with durable, exclusive, and auto-delete flags
  - Queue binding to exchanges
- Message handling:
  - Basic.Publish
  - Basic.Consume
  - Basic.Return for unroutable messages
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
- Transaction support
- Exchange-to-exchange bindings
- Queue deletion
- Queue purge
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
