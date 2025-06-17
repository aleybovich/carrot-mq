# Changelog

## [0.2.3]

### Added
- Add exclusive consumer checks
- Configurable logging with `LoggingConfig` struct via `WithLoggingConfig` server option
- Ability to disable all logging with `DisableLogging` flag
- Ability to provide custom logger implementations via `CustomLogger` field
- Control over heartbeat message logging with `HeartbeatLogging` flag (disabled by default)
- Implemented connection closure on client heartbeat timeout as per amqp091 protocol specification
- Configurable heartbeat interval via `WithHeartbeatInterval` server option (default: 60 seconds)

### Changed
- Consolidated all logging configuration into a single `WithLoggingConfig` option
- Removed separate `WithLogger` function in favor of unified configuration approach
- Refactored exclusive consumer validation to perform all checks before modifying state
- Heartbeat interval is now configurable instead of hardcoded constant

### Fixed
- Test for exclusive consumer access now properly uses channel error notifications instead of expecting immediate errors


## [0.2.2]

### Fixed
- Server now properly sends heartbeats to clients, preventing connection timeouts during idle periods
- Implemented bidirectional heartbeat mechanism as required by AMQP 0-9-1 specification

### Added
- Comprehensive heartbeat integration tests demonstrating the necessity of server-side heartbeats

## [0.2.1]

### Added
- `IsReady()` method to Server interface and public API for checking if server is ready to accept connections

## [0.2.0]

### Changed
- Refactored the code to extract the public api and move internal impl to ./internal
- Separated unit and integration tests

## [0.1.0]

### Added
- Queue deletion
- Queue purge
- Queue unbind
- ExchangeDelete
- Transaction support
- Message persistence (BuntDB)
- Per-consumer QoS
- Message delivery refactored, consumer buffer removed

## [0.0.2]

### Added 
- Basic.Return for unroutable messages
- Basic.Cancel
- Basic.Ack/Nack/Reject
- Basic.Get/GetOk
- Basic.Recover
- Max Frame Size validation
- Publisher confirms
- Virtual hosts support
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
- ExchangeBind, ExchangeUnbind (Exchange-to-exchange bindings) - not a priority as it's AMQP 091 extension, not core func
- Channel flow control
- Heartbeat monitoring
- SSL/TLS support
- Full headers exchange functionality
- Full implementation of immediate flag
- Auto-delete functionality for exchanges and queues
- Connection.Blocked/Unblocked
- Consumer priorities

