// package carrotmq provides the public API for creating and managing a CarrotMQ AMQP server.
// It offers a simple and configurable way to embed an AMQP 0.9.1 server into any Go application.
package carrotmq

import (
	"context"

	"github.com/aleybovich/carrot-mq/config"
	"github.com/aleybovich/carrot-mq/internal"
	"github.com/aleybovich/carrot-mq/logger"
	"github.com/aleybovich/carrot-mq/storage"
)

// Server represents a CarrotMQ server instance.
// It wraps the internal server implementation to provide a clean public API.
type Server struct {
	srv internal.Server
}

// ServerOption is a function that configures a Server during initialization.
// Use the provided With* functions to create ServerOptions.
type ServerOption func(*serverOptions)

// serverOptions holds the configuration that will be passed to the internal server
type serverOptions struct {
	internalOpts []internal.ServerOption
}

// Start begins listening for AMQP connections on the specified address.
// The address should be in the format "host:port", e.g., ":5672" or "localhost:5672".
// This method is blocking and will run until the server is shut down via the Shutdown method.
// It is recommended to run this in a separate goroutine.
//
// Example:
//
//	go func() {
//	    if err := server.Start(":5672"); err != nil {
//	        log.Printf("Server returned an error: %v", err)
//	    }
//	}()
func (s *Server) Start(addr string) error {
	return s.srv.Start(addr)
}

// Shutdown gracefully stops the server. It stops accepting new connections and attempts
// to close all active client connections by sending a connection.close frame.
// The provided context can be used to set a deadline for the shutdown process.
// If the context is canceled before all connections are closed, the shutdown
// will terminate, potentially leaving some connections open.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// Logger returns the server's configured logger instance, which conforms to the
// logger.Logger interface.
func (s *Server) Logger() logger.Logger {
	return s.srv.Logger()
}

// IsReady returns true when the server has started and is ready to accept connections.
// This can be useful for health checks or ensuring the server is fully initialized
// before attempting to connect clients.
func (s *Server) IsReady() bool {
	return s.srv.IsReady()
}

// NewServer creates a new CarrotMQ server with the provided options.
func NewServer(opts ...ServerOption) *Server {
	options := &serverOptions{}

	// Apply all public options to build internal options
	for _, opt := range opts {
		opt(options)
	}

	// Create the internal server with the collected options
	internalServer := internal.NewServer(options.internalOpts...)

	return &Server{
		srv: internalServer,
	}
}

// WithLogger sets a custom logger that implements the logger.Logger interface.
// If not used, a default logger that writes to stdout will be used.
func WithLogger(l logger.Logger) ServerOption {
	return func(opts *serverOptions) {
		opts.internalOpts = append(opts.internalOpts, internal.WithLoggingConfig(config.LoggingConfig{CustomLogger: l}))
	}
}

// WithAuth enables PLAIN authentication with the provided credentials.
// The credentials map should contain username-to-password mappings.
func WithAuth(credentials map[string]string) ServerOption {
	return func(opts *serverOptions) {
		opts.internalOpts = append(opts.internalOpts, internal.WithAuth(credentials))
	}
}

// WithVHosts configures the server with a set of predefined virtual hosts,
// including their exchanges, queues, and bindings. This is intended for
// initial server setup; runtime management should be done via the AMQP protocol.
func WithVHosts(vhosts []config.VHostConfig) ServerOption {
	return func(opts *serverOptions) {
		opts.internalOpts = append(opts.internalOpts, internal.WithVHosts(vhosts))
	}
}

// WithStorage configures the persistence storage provider for the server
// based on the provided StorageConfig.
func WithStorage(cfg config.StorageConfig) ServerOption {
	return func(opts *serverOptions) {
		opts.internalOpts = append(opts.internalOpts, internal.WithStorage(cfg))
	}
}

// WithInMemoryStorage is a convenience option that configures in-memory storage,
// which is volatile and will be lost on server shutdown.
func WithInMemoryStorage() ServerOption {
	return func(opts *serverOptions) {
		opts.internalOpts = append(opts.internalOpts, internal.WithStorage(config.StorageConfig{
			Type: config.StorageTypeMemory,
		}))
	}
}

// WithBuntDBStorage is a convenience option that configures persistent storage
// using BuntDB at the specified file path.
func WithBuntDBStorage(path string) ServerOption {
	return func(opts *serverOptions) {
		opts.internalOpts = append(opts.internalOpts, internal.WithStorage(config.StorageConfig{
			Type: config.StorageTypeBuntDB,
			BuntDB: &config.BuntDBConfig{
				Path: path,
			},
		}))
	}
}

// WithNoStorage is a convenience option that explicitly disables persistence.
// This is the default behavior if no storage option is provided.
func WithNoStorage() ServerOption {
	return func(opts *serverOptions) {
		opts.internalOpts = append(opts.internalOpts, internal.WithStorage(config.StorageConfig{
			Type: config.StorageTypeNone,
		}))
	}
}

// WithStorageProvider allows for the injection of a custom storage implementation
// that conforms to the storage.StorageProvider interface.
func WithStorageProvider(provider storage.StorageProvider) ServerOption {
	return func(opts *serverOptions) {
		opts.internalOpts = append(opts.internalOpts, internal.WithStorageProvider(provider))
	}
}

// WithHeartbeatInterval configures the suggested heartbeat interval in seconds.
// The default is 60 seconds if not specified. The client and server will negotiate
// the actual heartbeat interval during connection establishment.
func WithHeartbeatInterval(interval uint16) ServerOption {
	return func(opts *serverOptions) {
		opts.internalOpts = append(opts.internalOpts, internal.WithHeartbeatInterval(interval))
	}
}
