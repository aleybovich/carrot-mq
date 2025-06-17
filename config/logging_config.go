package config

import "github.com/aleybovich/carrot-mq/logger"

// LoggingConfig defines configuration for logging behavior
type LoggingConfig struct {
	// HeartbeatLogging controls whether heartbeat messages are logged
	// Default is false to reduce log noise
	HeartbeatLogging bool

	// DisableLogging completely disables all logging when true
	// Default is false
	DisableLogging bool

	// CustomLogger allows providing a custom logger implementation
	// Cannot be used together with DisableLogging
	CustomLogger logger.Logger
}
