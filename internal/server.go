package internal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqpError "github.com/aleybovich/carrot-mq/amqperror"
	"github.com/aleybovich/carrot-mq/config"
	"github.com/aleybovich/carrot-mq/logger"
	"github.com/aleybovich/carrot-mq/storage"
)

// How log to wait until cleanup if channel Close Ok not received from client
// TODO: make server config value
const channelCloseOkTimeout = 100 * time.Millisecond

const (
	suggestedHeartbeatInterval = 60 // Suggested heartbeat interval for AMQP 0-9-1
	suggestedFrameMaxSize      = 131072
)

const failedAuthThrottle = 1 * time.Second // Throttle failed auth attempts to prevent abuse

// Flag to determine if we're logging to a terminal (with colors) or a file
var IsTerminal bool

func init() {
	// Check if stdout is a terminal
	fileInfo, _ := os.Stdout.Stat()
	IsTerminal = (fileInfo.Mode() & os.ModeCharDevice) != 0
}

type Server interface {
	Start(addr string) error
	Shutdown(ctx context.Context) error
	Logger() logger.Logger
}

type frame struct {
	Type     byte
	Channel  uint16
	Payload  []byte
	FrameEnd byte
}

type channel struct {
	id                            uint16
	conn                          *connection
	consumers                     map[string]string // consumerTag -> queueName
	pendingMessages               []message         // For assembling messages from Publish -> Header -> Body
	deliveryTag                   uint64
	mu                            sync.Mutex
	closingByServer               bool        // True if server sent Channel.Close and is waiting for CloseOk
	closeOkTimer                  *time.Timer // Timer for waiting for Channel.CloseOk
	serverInitiatedCloseReplyCode uint16      // Store the reply code for logging if CloseOk times out
	serverInitiatedCloseReplyText string      // Store the reply text for logging if CloseOk times out

	unackedMessages map[uint64]*unackedMessage // deliveryTag -> UnackedMessage

	// Publisher confirms fields
	confirmMode      bool            // Whether confirm mode is enabled
	nextPublishSeqNo uint64          // Next sequence number for published messages
	pendingConfirms  map[uint64]bool // Map of unconfirmed delivery tags

	// Transaction fields
	txMode     bool                   // Whether transaction mode is enabled
	txMessages []transactionalMessage // Messages published within current transaction
	txAcks     []uint64               // Delivery tags acknowledged within current transaction
	txNacks    []transactionalNack    // Delivery tags rejected within current transaction

	// QoS settings
	prefetchCount uint16 // 0 means unlimited
	prefetchSize  uint32 // ignored for simplicity
	qosGlobal     bool   // ignored for simplicity
}

type transactionalMessage struct {
	Message    *message
	RoutingKey string
	Exchange   string
	Mandatory  bool
	Immediate  bool
}

type transactionalNack struct {
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

type properties struct {
	ContentType     string
	ContentEncoding string
	Headers         map[string]interface{}
	DeliveryMode    uint8
	Priority        uint8
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	MessageId       string
	Timestamp       uint64
	Type            string
	UserId          string
	AppId           string
	ClusterId       string
}

type message struct {
	Exchange    string
	RoutingKey  string
	Mandatory   bool
	Immediate   bool
	Properties  properties
	Body        []byte
	Redelivered bool
}

func (m *message) DeepCopy() *message {
	if m == nil {
		return nil
	}

	// Deep copy properties
	propsCopy := properties{
		ContentType:     m.Properties.ContentType,
		ContentEncoding: m.Properties.ContentEncoding,
		DeliveryMode:    m.Properties.DeliveryMode,
		Priority:        m.Properties.Priority,
		CorrelationId:   m.Properties.CorrelationId,
		ReplyTo:         m.Properties.ReplyTo,
		Expiration:      m.Properties.Expiration,
		MessageId:       m.Properties.MessageId,
		Timestamp:       m.Properties.Timestamp,
		Type:            m.Properties.Type,
		UserId:          m.Properties.UserId,
		AppId:           m.Properties.AppId,
		ClusterId:       m.Properties.ClusterId,
	}

	if m.Properties.Headers != nil {
		propsCopy.Headers = make(map[string]interface{})
		if len(m.Properties.Headers) > 0 {
			maps.Copy(propsCopy.Headers, m.Properties.Headers)
		}
	}

	// Deep copy body
	var bodyCopy []byte
	if m.Body != nil {
		bodyCopy = make([]byte, len(m.Body))
		copy(bodyCopy, m.Body)
	}

	return &message{
		Exchange:    m.Exchange,
		RoutingKey:  m.RoutingKey,
		Mandatory:   m.Mandatory,
		Immediate:   m.Immediate,
		Properties:  propsCopy,
		Body:        bodyCopy,
		Redelivered: m.Redelivered,
	}
}

type queue struct {
	Name       string
	Messages   []message
	Bindings   map[string]bool
	Consumers  map[string]*consumer
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	mu         sync.RWMutex

	deleting atomic.Bool
}

type exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	Bindings   map[string][]string
	mu         sync.RWMutex

	deleted atomic.Bool
}

type unackedMessage struct {
	Message     message
	ConsumerTag string
	QueueName   string
	DeliveryTag uint64
	Delivered   time.Time
}

type connection struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	channels map[uint16]*channel
	server   *server
	vhost    *vHost
	mu       sync.RWMutex
	writeMu  sync.Mutex

	// negotiated values
	channelMax        uint16
	frameMax          uint32
	heartbeatInterval uint16

	username string // Store authenticated username
}

type consumer struct {
	Tag       string
	ChannelId uint16
	NoAck     bool
	Queue     *queue
	stopCh    chan struct{} // Signal to stop this consumer
}

type credentials struct {
	Username string
	Password string
}

type server struct {
	listener       net.Listener
	vhosts         map[string]*vHost
	internalLogger *log.Logger   // Internal logger for formatting output
	customLogger   logger.Logger // External logger interface, if provided
	mu             sync.RWMutex

	// Authentication fields
	authMode    config.AuthMode
	credentials map[string]string // username -> password

	// track active connections
	connections   map[*connection]struct{}
	connectionsMu sync.RWMutex

	// Persistence fields
	persistenceManager *PersistenceManager
}

type amqpDecimal struct {
	Scale uint8
	Value int32
}

// ServerOption defines functional options for configuring the AMQP server
type ServerOption func(*server)

// WithLogger sets a custom logger that implements the Logger interface
func WithLogger(logger logger.Logger) ServerOption {
	return func(s *server) {
		s.customLogger = logger
	}
}

func WithAuth(credentials map[string]string) ServerOption {
	return func(s *server) {
		if len(credentials) > 0 {
			s.authMode = config.AuthModePlain
			s.credentials = make(map[string]string)
			for user, pass := range credentials {
				s.credentials[user] = pass
			}
			s.Info("Authentication enabled with %d users", len(credentials))
		}
	}
}

func WithVHosts(vhosts []config.VHostConfig) ServerOption {
	return func(s *server) {
		for _, vhostConfig := range vhosts {
			// Create vhost if it doesn't exist (skip default "/" if already created)
			if vhostConfig.Name != "/" {
				if err := s.AddVHost(vhostConfig.Name); err != nil {
					s.Warn("Failed to create vhost '%s': %v", vhostConfig.Name, err)
					continue
				}
			}

			// Get the vhost
			vhost, err := s.GetVHost(vhostConfig.Name)
			if err != nil {
				s.Warn("Failed to get vhost '%s': %v", vhostConfig.Name, err)
				continue
			}

			// Add exchanges to vhost
			vhost.mu.Lock()
			for _, exchConfig := range vhostConfig.Exchanges {
				// Skip if exchange already exists (like default "" exchange)
				if _, exists := vhost.exchanges[exchConfig.Name]; exists {
					s.Info("Exchange '%s' already exists in vhost '%s', skipping", exchConfig.Name, vhostConfig.Name)
					continue
				}

				vhost.exchanges[exchConfig.Name] = &exchange{
					Name:       exchConfig.Name,
					Type:       exchConfig.Type,
					Durable:    exchConfig.Durable,
					AutoDelete: exchConfig.AutoDelete,
					Internal:   exchConfig.Internal,
					Bindings:   make(map[string][]string),
				}
				s.Info("Created exchange '%s' (type: %s) in vhost '%s'", exchConfig.Name, exchConfig.Type, vhostConfig.Name)
			}

			// Add queues to vhost
			for _, queueConfig := range vhostConfig.Queues {
				// Skip if queue already exists
				if _, exists := vhost.queues[queueConfig.Name]; exists {
					s.Info("Queue '%s' already exists in vhost '%s', skipping", queueConfig.Name, vhostConfig.Name)
					continue
				}

				// Create the queue
				vhost.queues[queueConfig.Name] = &queue{
					Name:       queueConfig.Name,
					Messages:   []message{},
					Bindings:   make(map[string]bool),
					Consumers:  make(map[string]*consumer),
					Durable:    queueConfig.Durable,
					Exclusive:  queueConfig.Exclusive,
					AutoDelete: queueConfig.AutoDelete,
				}
				s.Info("Created queue '%s' in vhost '%s' (durable: %v, exclusive: %v)",
					queueConfig.Name, vhostConfig.Name, queueConfig.Durable, queueConfig.Exclusive)

				// Handle queue bindings
				for bindingKey := range queueConfig.Bindings {
					// Parse binding: "exchangeName:routingKey"
					parts := strings.SplitN(bindingKey, ":", 2)
					if len(parts) != 2 {
						s.Warn("Invalid binding format '%s' for queue '%s', expected 'exchange:routingKey'",
							bindingKey, queueConfig.Name)
						continue
					}

					exchangeName := parts[0]
					routingKey := parts[1]

					// Check if exchange exists
					exchange, exchangeExists := vhost.exchanges[exchangeName]
					if !exchangeExists {
						s.Warn("Exchange '%s' not found for binding queue '%s', skipping binding '%s'",
							exchangeName, queueConfig.Name, bindingKey)
						continue
					}

					// Add binding to exchange
					exchange.mu.Lock()
					exchange.Bindings[routingKey] = append(exchange.Bindings[routingKey], queueConfig.Name)
					exchange.mu.Unlock()

					// Add binding info to queue
					vhost.queues[queueConfig.Name].Bindings[bindingKey] = true

					s.Info("Bound queue '%s' to exchange '%s' with routing key '%s' in vhost '%s'",
						queueConfig.Name, exchangeName, routingKey, vhostConfig.Name)
				}
			}
			vhost.mu.Unlock()
		}
	}
}

// WithStorage configures the storage provider for the server
func WithStorage(cfg config.StorageConfig) ServerOption {
	return func(s *server) {
		// Validate config
		if err := cfg.Validate(); err != nil {
			s.Warn("Invalid storage config: %v, persistence disabled", err)
			return
		}

		var provider storage.StorageProvider

		switch cfg.Type {
		case config.StorageTypeNone:
			// No persistence
			s.Info("Persistence disabled")
			return

		case config.StorageTypeMemory:
			provider = storage.NewBuntDBProvider(":memory:")
			s.Info("Using in-memory storage (BuntDB)")

		case config.StorageTypeBuntDB:
			path := cfg.BuntDB.Path
			if path == "" {
				path = ":memory:"
			}
			provider = storage.NewBuntDBProvider(path)
			if path == ":memory:" {
				s.Info("Using in-memory BuntDB storage")
			} else {
				s.Info("Using persistent BuntDB storage at: %s", path)
			}

			// Future providers
			// case StorageTypeBoltDB:
			//     provider = storage.NewBoltDBProvider(config.BoltDB)
			//     s.Info("Using BoltDB storage at: %s", config.BoltDB.Path)
			//
			// case StorageTypeRedis:
			//     provider = storage.NewRedisProvider(config.Redis)
			//     s.Info("Using Redis storage at: %s", config.Redis.Address)
		}

		if provider != nil {
			s.persistenceManager = NewPersistenceManager(provider, s)
		}
	}
}

// Convenience functions for common configurations

// WithInMemoryStorage configures in-memory storage using BuntDB
func WithInMemoryStorage() ServerOption {
	return WithStorage(config.StorageConfig{
		Type: config.StorageTypeMemory,
	})
}

// WithBuntDBStorage configures persistent BuntDB storage
func WithBuntDBStorage(path string) ServerOption {
	return WithStorage(config.StorageConfig{
		Type: config.StorageTypeBuntDB,
		BuntDB: &config.BuntDBConfig{
			Path: path,
		},
	})
}

// Optional WithNoStorage explicitly disables persistence
func WithNoStorage() ServerOption {
	return WithStorage(config.StorageConfig{
		Type: config.StorageTypeNone,
	})
}

// Using  custom storage provider directly
func WithStorageProvider(provider storage.StorageProvider) ServerOption {
	return func(s *server) {
		if provider != nil {
			s.persistenceManager = NewPersistenceManager(provider, s)
		}
	}
}

// Get caller function name for logging
func getCallerName() string {
	pc, _, _, _ := runtime.Caller(2) // Use depth 2 to get the actual caller, not the logging function
	caller := runtime.FuncForPC(pc).Name()
	parts := strings.Split(caller, ".")
	return parts[len(parts)-1]
}

// Fatal logs a message with Fatal level and exits with code 1
func (s *server) Fatal(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Fatal(format, args...)
		return
	}

	funcName := getCallerName()

	if IsTerminal {
		prefix := fmt.Sprintf("%s[FATAL]%s %s%s%s: ", colorBoldRed, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[FATAL] %s: "+format, append([]interface{}{funcName}, args...)...)
	}

	os.Exit(1) // Exit with error code 1
}

// Err logs a message with Error level
func (s *server) Err(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Err(format, args...)
		return
	}

	funcName := getCallerName()

	if IsTerminal {
		prefix := fmt.Sprintf("%s[ERROR]%s %s%s%s: ", colorBoldRed, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[ERROR] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

// Warn logs a message with Warning level
func (s *server) Warn(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Warn(format, args...)
		return
	}

	funcName := getCallerName()

	if IsTerminal {
		prefix := fmt.Sprintf("%s[WARN]%s %s%s%s: ", colorYellow, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[WARN] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

// Info logs a message with Info level
func (s *server) Info(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Info(format, args...)
		return
	}

	funcName := getCallerName()

	if IsTerminal {
		prefix := fmt.Sprintf("%s[INFO]%s %s%s%s: ", colorGreen, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[INFO] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

// Debug logs a message with Debug level
func (s *server) Debug(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Debug(format, args...)
		return
	}

	// Only log debug messages if AMQP_DEBUG environment variable is set
	if os.Getenv("AMQP_DEBUG") != "1" {
		return
	}

	funcName := getCallerName()

	if IsTerminal {
		prefix := fmt.Sprintf("%s[DEBUG]%s %s%s%s: ", colorPurple, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[DEBUG] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

func (s *server) Logger() logger.Logger {
	// If using a custom logger, return it
	if s.customLogger != nil {
		return s.customLogger
	}
	// Otherwise return the internal logger (server itself implements Logger interface)
	return s
}

func NewServer(opts ...ServerOption) *server {
	var logPrefix string
	if IsTerminal {
		logPrefix = fmt.Sprintf("%s[AMQP]%s ", colorBlue, colorReset)
	} else {
		logPrefix = "[AMQP] "
	}

	s := &server{
		vhosts:         make(map[string]*vHost),
		internalLogger: log.New(os.Stdout, logPrefix, log.LstdFlags|log.Lmicroseconds),
		connections:    make(map[*connection]struct{}),
	}

	// Create default vhost
	s.AddVHost("/")

	// Apply all provided options
	for _, opt := range opts {
		opt(s)
	}

	// If no custom logger is provided, use the server itself as the logger
	if s.customLogger == nil {
		s.customLogger = s
	}

	// Initialize persistence if configured
	if s.persistenceManager != nil {
		if err := s.persistenceManager.Initialize(); err != nil {
			s.Err("Failed to initialize persistence: %v", err)
			s.persistenceManager = nil
		} else {
			// Server owns the recovery logic
			if err := s.recoverPersistedState(); err != nil {
				s.Err("Failed to recover persisted state: %v", err)
			}
			s.Info("Persistence enabled")
		}
	} else {
		s.Info("Running without persistence")
	}

	s.Info("AMQP server created with default direct exchange")
	return s
}

func (s *server) Start(addr string) error {
	var err error
	s.Info("Starting AMQP server on %s", addr)
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		s.Err("Error starting server: %v", err)
		return err
	}
	s.Info("Server listening on %s", addr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				s.Info("Server listener on %s closed. Stopping accept loop.", addr)
				return nil // Or a specific "shutdown" error if you prefer
			}

			s.Err("Error accepting connection: %v", err)
			continue
		}
		s.Info("New connection from %s", conn.RemoteAddr())
		go s.handleConnection(conn)
	}
}

func (s *server) Shutdown(ctx context.Context) error {
	s.Info("Shutting down AMQP server...")

	// 1. Stop accepting new connections
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.Warn("Error closing network listener: %v", err)
		}
	}

	// 2. Initiate graceful close on all active connections
	var wg sync.WaitGroup
	s.connectionsMu.RLock()
	s.Info("Closing %d active connections...", len(s.connections))
	for conn := range s.connections {
		wg.Add(1)
		go func(c *connection) {
			defer wg.Done()
			// Send Connection.Close to the client to initiate graceful shutdown
			// This is a normal shutdown, not an error state.
			err := c.sendConnectionClose(amqpError.ConnectionForced.Code(), "server shutdown", 0, 0)
			if err != nil {
				s.Warn("Failed to send Connection.Close to %s, closing connection forcefully: %v", c.conn.RemoteAddr(), err)
				c.conn.Close() // Force close if sending fails
			}
			// The connection's read loop will eventually detect the close and clean up resources.
		}(conn)
	}
	s.connectionsMu.RUnlock()

	// 3. Wait for all connections to close or for the context to be done
	shutdownComplete := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		s.Info("All active connections notified.")
	case <-ctx.Done():
		s.Warn("Shutdown context canceled. Some connections may not have closed gracefully: %v", ctx.Err())
		// Force close any remaining connections
		s.connectionsMu.RLock()
		for conn := range s.connections {
			conn.conn.Close()
		}
		s.connectionsMu.RUnlock()
	}

	// 4. Close persistence layer
	if s.persistenceManager != nil {
		if err := s.persistenceManager.Close(); err != nil {
			s.Err("Error closing persistence manager: %v", err)
			return err // Return this error as it might indicate data loss
		}
	}

	s.Info("Server shutdown complete.")
	return nil
}

func (s *server) recoverPersistedState() error {
	s.Info("Starting state recovery from persistence")

	// Recover vhosts
	vhostRecords, err := s.persistenceManager.LoadAllVHosts()
	if err != nil {
		return fmt.Errorf("loading vhosts: %w", err)
	}

	// Always recover entities for default vhost if it exists in persistence
	if err := s.recoverVHostEntities("/"); err != nil {
		s.Warn("Failed to recover entities for default vhost: %v", err)
	}

	for _, vhostRec := range vhostRecords {
		// Skip default vhost as it's already created
		if vhostRec.Name == "/" {
			continue
		}

		// Create vhost without persisting it again
		if err := s.addVHostInternal(vhostRec.Name, false); err != nil {
			s.Warn("Failed to recover vhost %s: %v", vhostRec.Name, err)
			continue
		}

		// Recover all entities in this vhost
		if err := s.recoverVHostEntities(vhostRec.Name); err != nil {
			s.Warn("Failed to recover entities for vhost %s: %v", vhostRec.Name, err)
		}
	}

	s.Info("State recovery completed")
	return nil
}

// Recover all entities within a vhost
func (s *server) recoverVHostEntities(vhostName string) error {
	vhost, err := s.GetVHost(vhostName)
	if err != nil {
		return err
	}

	// Recover exchanges
	exchangeRecords, err := s.persistenceManager.LoadAllExchanges(vhostName)
	if err != nil {
		s.Warn("Failed to load exchanges for vhost %s: %v", vhostName, err)
	} else {
		for _, exchRec := range exchangeRecords {
			// Skip default exchange
			if exchRec.Name == "" {
				continue
			}

			vhost.mu.Lock()
			vhost.exchanges[exchRec.Name] = RecordToExchange(exchRec)
			vhost.mu.Unlock()

			s.Info("Recovered exchange %s in vhost %s", exchRec.Name, vhostName)
		}
	}

	// Recover queues
	queueRecords, err := s.persistenceManager.LoadAllQueues(vhostName)
	if err != nil {
		s.Warn("Failed to load queues for vhost %s: %v", vhostName, err)
	} else {
		for _, queueRec := range queueRecords {
			// Skip exclusive queues on recovery
			if queueRec.Exclusive {
				s.Info("Skipping exclusive queue %s on recovery", queueRec.Name)
				continue
			}

			vhost.mu.Lock()
			vhost.queues[queueRec.Name] = RecordToQueue(queueRec)
			vhost.mu.Unlock()

			s.Info("Recovered queue %s in vhost %s", queueRec.Name, vhostName)
		}
	}

	// Recover bindings
	bindingRecords, err := s.persistenceManager.LoadAllBindings(vhostName)
	if err != nil {
		s.Warn("Failed to load bindings for vhost %s: %v", vhostName, err)
	} else {
		for _, bindRec := range bindingRecords {
			vhost.mu.RLock()
			exchange, exExists := vhost.exchanges[bindRec.Exchange]
			queue, qExists := vhost.queues[bindRec.Queue]
			vhost.mu.RUnlock()

			if exExists && qExists {
				// Restore binding in both directions
				exchange.mu.Lock()
				exchange.Bindings[bindRec.RoutingKey] = append(exchange.Bindings[bindRec.RoutingKey], bindRec.Queue)
				exchange.mu.Unlock()

				queue.mu.Lock()
				queue.Bindings[bindRec.Exchange+":"+bindRec.RoutingKey] = true
				queue.mu.Unlock()

				s.Info("Recovered binding %s:%s -> %s in vhost %s",
					bindRec.Exchange, bindRec.RoutingKey, bindRec.Queue, vhostName)
			}
		}
	}

	// Recover messages for each queue
	vhost.mu.RLock()
	queueNames := make([]string, 0, len(vhost.queues))
	for name := range vhost.queues {
		queueNames = append(queueNames, name)
	}
	vhost.mu.RUnlock()

	for _, queueName := range queueNames {
		messageRecords, err := s.persistenceManager.LoadQueueMessages(vhostName, queueName)
		if err != nil {
			s.Warn("Failed to load messages for queue %s in vhost %s: %v",
				queueName, vhostName, err)
			continue
		}

		if len(messageRecords) > 0 {
			vhost.mu.RLock()
			queue, exists := vhost.queues[queueName]
			vhost.mu.RUnlock()

			if exists {
				queue.mu.Lock()
				for _, msgRec := range messageRecords {
					message := RecordToMessage(msgRec)
					queue.Messages = append(queue.Messages, *message)
				}
				queue.mu.Unlock()

				s.Info("Recovered %d messages for queue %s in vhost %s",
					len(messageRecords), queueName, vhostName)
			}
		}
	}

	return nil
}

// Add new connection to the active server connections map
func (s *server) addConnection(c *connection) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	s.connections[c] = struct{}{}
	s.Info("Connection %s added to active list. Total: %d", c.conn.RemoteAddr(), len(s.connections))
}

// Remove a connection from the active server connections map.
func (s *server) removeConnection(c *connection) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	if _, ok := s.connections[c]; ok {
		delete(s.connections, c)
		s.Info("Connection %s removed from active list. Total remaining: %d", c.conn.RemoteAddr(), len(s.connections))
	} else {
		s.Warn("Attempted to remove connection %s from active list, but it was not found.", c.conn.RemoteAddr())
	}
}

func (s *server) handleConnection(conn net.Conn) {

	s.Info("Handling connection from %s", conn.RemoteAddr())

	c := &connection{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
		channels: make(map[uint16]*channel),
		server:   s,
	}

	protocol := make([]byte, 8)
	// Use io.ReadFull for precise byte reading
	if _, err := io.ReadFull(c.reader, protocol); err != nil {
		s.Err("Error reading protocol header from %s: %v", conn.RemoteAddr(), err)
		conn.Close() // Ensure connection is closed on early error
		return
	}

	if !bytes.Equal(protocol, []byte("AMQP\x00\x00\x09\x01")) {
		s.Warn("Invalid protocol header from %s: %v", conn.RemoteAddr(), protocol)
		conn.Close() // Ensure connection is closed
		return
	}
	s.Info("Protocol header validated from %s", conn.RemoteAddr())

	if err := c.sendConnectionStart(); err != nil {
		s.Err("Failed to send connection.start to %s: %v. Closing connection.", conn.RemoteAddr(), err)
		// No AMQP error to send here as connection.start itself failed.
		c.cleanupConnectionResources() // Attempt cleanup
		conn.Close()
		return
	}
	s.Info("Sent connection.start to %s", conn.RemoteAddr())

	s.addConnection(c) // add new active connection to server connection map

	for {
		frame, err := c.readFrame()
		if err != nil {
			// Enhanced error checking for readFrame
			errMsg := err.Error()
			isNetClosedError := errors.Is(err, io.EOF) ||
				strings.Contains(errMsg, "use of closed network connection") ||
				strings.Contains(errMsg, "connection reset by peer") ||
				errors.Is(err, net.ErrClosed)

			if isNetClosedError {
				s.Info("Connection %s closed: %v", conn.RemoteAddr(), err)
			} else {
				s.Err("Error reading frame from %s: %v", conn.RemoteAddr(), err)
			}
			c.cleanupConnectionResources() // Clean up channels and consumers
			conn.Close()                   // Ensure underlying connection is closed
			s.removeConnection(c)
			return // Exit handleConnection goroutine
		}

		var handlerError error
		switch frame.Type {
		case FrameMethod:
			handlerError = c.handleMethod(frame)
		case FrameHeader:
			handlerError = c.handleHeader(frame)
		case FrameBody:
			c.handleBody(frame)
		case FrameHeartbeat:
			// Heartbeats are typically high-volume and low importance for logging
			// Only log them at detailed level or in debug mode
			s.Debug("Received %s from %s on channel %d",
				colorize("HEARTBEAT", colorGray),
				colorize(conn.RemoteAddr().String(), colorCyan),
				frame.Channel)
		default:
			s.Warn("Received unhandled frame type %d from %s on channel %d", frame.Type, conn.RemoteAddr(), frame.Channel)
			handlerError = c.sendConnectionClose(amqpError.UnexpectedFrame.Code(), fmt.Sprintf("unhandled frame type %d", frame.Type), 0, 0)
		}

		if handlerError != nil {
			if errors.Is(handlerError, errConnectionClosedGracefully) {
				s.Info("AMQP connection gracefully closed with %s.", conn.RemoteAddr())
				// Connection is already closed by handleConnectionMethod.
				// Resources should be cleaned by cleanupConnectionResources, which is called
				// when readFrame eventually fails due to the closed connection, or we can call it here too.
				// Since conn.Close() was called in the handler, the next readFrame() will fail and trigger cleanup.
				// For explicitness, we can ensure cleanup and return.
				c.cleanupConnectionResources()
				s.removeConnection(c)
				// conn.Close() was already called by the method handler that returned errConnectionClosedGracefully
				return // Exit the loop and goroutine
			}

			if errors.Is(handlerError, errConnectionCloseSentByServer) {
				s.Info("Server initiated Connection.Close with %s. Waiting for CloseOk or client disconnect.", conn.RemoteAddr())
				// Continue read loop to get CloseOk or detect disconnect.
				// TODO: A timeout for CloseOk could be implemented here.
			} else if errors.Is(handlerError, errChannelClosedByServer) {
				s.Info("AMQP Channel.Close was sent successfully for channel %d due to protocol error. Connection remains active.", frame.Channel)
				// Channel specific error handled, connection continues.
			} else {
				// For any other error (I/O, unhandled AMQP issues that didn't send a specific close frame).
				s.Err("Critical error from handler for %s: %v. Closing connection.", conn.RemoteAddr(), handlerError)
				c.cleanupConnectionResources()
				conn.Close()
				s.removeConnection(c)
				return
			}
		}
	}
}

func (c *connection) readFrame() (*frame, error) {
	header := make([]byte, 7)
	_, err := io.ReadFull(c.reader, header)
	if err != nil {
		return nil, fmt.Errorf("error reading frame header: %w", err)
	}

	frame := &frame{
		Type:    header[0],
		Channel: binary.BigEndian.Uint16(header[1:3]),
	}

	size := binary.BigEndian.Uint32(header[3:7])
	if c.frameMax > 0 && size > uint32(c.frameMax) {
		// Send connection.close with frame-error
		c.sendConnectionClose(amqpError.UnexpectedFrame.Code(), "frame too large", 0, 0)
		return nil, fmt.Errorf("frame size %d exceeds negotiated max %d", size, c.frameMax)
	}

	frame.Payload = make([]byte, size)
	_, err = io.ReadFull(c.reader, frame.Payload)
	if err != nil {
		return nil, fmt.Errorf("error reading frame payload: %w", err)
	}

	frameEnd := make([]byte, 1)
	_, err = io.ReadFull(c.reader, frameEnd)
	if err != nil {
		return nil, fmt.Errorf("error reading frame end: %w", err)
	}

	if frameEnd[0] != FrameEnd {
		c.sendConnectionClose(amqpError.FrameError.Code(), "frame-end octet missing", 0, 0)
		return nil, fmt.Errorf("invalid frame-end octet: %x", frameEnd[0])
	}

	frameTypeName := getFrameTypeName(frame.Type)
	c.server.Info("Read frame: type=%s, channel=%d, size=%d",
		colorize(frameTypeName, colorYellow),
		frame.Channel,
		size)
	return frame, nil
}

// writeFrameInternal writes a frame to the connection's buffered writer.
// It does NOT acquire a lock and does NOT flush the writer.
// This is intended to be used when multiple frames need to be written
// atomically under an external lock, followed by a single flush.
func (c *connection) writeFrameInternal(frameType byte, channelID uint16, payload []byte) error {
	header := make([]byte, 7)
	header[0] = frameType
	binary.BigEndian.PutUint16(header[1:3], channelID)
	binary.BigEndian.PutUint32(header[3:7], uint32(len(payload)))

	frameTypeName := getFrameTypeName(frameType)
	c.server.Info("Buffering frame: type=%s, channel=%d, size=%d",
		colorize(frameTypeName, colorYellow),
		channelID,
		len(payload))

	if _, err := c.writer.Write(header); err != nil {
		return fmt.Errorf("error writing frame header to buffer: %w", err)
	}
	if _, err := c.writer.Write(payload); err != nil {
		return fmt.Errorf("error writing frame payload to buffer: %w", err)
	}
	if err := c.writer.WriteByte(FrameEnd); err != nil { // Assuming FrameEnd is defined
		return fmt.Errorf("error writing frame end to buffer: %w", err)
	}
	return nil
}

// writeFrame writes a single frame to the connection.
// It acquires a lock and flushes the writer, suitable for individual frame transmissions.
func (c *connection) writeFrame(frame *frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	header := make([]byte, 7)
	header[0] = frame.Type
	binary.BigEndian.PutUint16(header[1:3], frame.Channel)
	binary.BigEndian.PutUint32(header[3:7], uint32(len(frame.Payload)))

	frameTypeName := getFrameTypeName(frame.Type)
	c.server.Info("Writing frame: type=%s (%d), channel=%d, size=%d", frameTypeName, frame.Type, frame.Channel, len(frame.Payload))

	if _, err := c.writer.Write(header); err != nil {
		return err
	}
	if _, err := c.writer.Write(frame.Payload); err != nil {
		return err
	}
	if err := c.writer.WriteByte(FrameEnd); err != nil {
		return err
	}
	return c.writer.Flush()
}

func (c *connection) sendConnectionStart() error { // MODIFIED: to return error
	payload := &bytes.Buffer{}
	// Protocol Class ID and Method ID for connection.start
	if err := binary.Write(payload, binary.BigEndian, uint16(ClassConnection)); err != nil {
		return fmt.Errorf("writing classId for connection.start: %w", err)
	}
	if err := binary.Write(payload, binary.BigEndian, uint16(MethodConnectionStart)); err != nil {
		return fmt.Errorf("writing methodId for connection.start: %w", err)
	}

	// Version (major, minor)
	payload.WriteByte(0) // AMQP 0-9-1 major version
	payload.WriteByte(9) // AMQP 0-9-1 minor version (actually 0-9, then 1 for revision)
	// The protocol bytes sent earlier are AMQP<0><0><9><1>
	// The connection.start method arguments include server-properties, mechanisms, locales.
	// Version here is for the protocol version the method arguments adhere to.

	// Server properties (field table)
	serverProperties := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"publisher_confirms":           true,
			"exchange_exchange_bindings":   true, // If you support this extension
			"basic.nack":                   true,
			"consumer_cancel_notify":       true,
			"connection.blocked":           true, // If you support this extension
			"consumer_priorities":          true,
			"authentication_failure_close": true,
			"per_consumer_qos":             true,
			// "direct_reply_to": true, // Another common capability
		},
		"product":   "GoAMQPServer (Basic)", // Example product name
		"version":   VERSION,                // Example version
		"platform":  "Go " + runtime.Version(),
		"copyright": "Your Server Copyright",
		// "information": "More info at http://...",
	}
	if err := writeTable(payload, serverProperties); err != nil {
		// This is a server-side error during connection negotiation.
		// The connection cannot properly start.
		c.server.Err("Error writing server_properties for connection.start: %v", err)
		return fmt.Errorf("writing server_properties for connection.start: %w", err)
	}

	// Security mechanisms (long string)
	mechanisms := "PLAIN" // Example: only PLAIN supported
	// writeLongString equivalent:
	payload.Write(binary.BigEndian.AppendUint32(nil, uint32(len(mechanisms))))
	payload.WriteString(mechanisms)

	// Locales (long string)
	locales := "en_US"
	// writeLongString equivalent:
	payload.Write(binary.BigEndian.AppendUint32(nil, uint32(len(locales))))
	payload.WriteString(locales)

	err := c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: 0, // Connection methods are on channel 0
		Payload: payload.Bytes(),
	})
	if err != nil {
		c.server.Err("Error sending connection.start frame: %v", err)
		return fmt.Errorf("sending connection.start frame: %w", err)
	}
	c.server.Info("Sent connection.start to %s", c.conn.RemoteAddr())
	return nil
}

func (c *connection) handleMethod(frame *frame) error { // Return type error
	reader := bytes.NewReader(frame.Payload)
	var classId, methodId uint16
	binary.Read(reader, binary.BigEndian, &classId)
	binary.Read(reader, binary.BigEndian, &methodId)

	if frame.Channel == 0 && classId != ClassConnection {
		c.server.Err("Received non-Connection class %d on channel 0", classId)
		return c.sendConnectionClose(amqpError.CommandInvalid.Code(), "command invalid - channel 0 is for Connection class only", classId, methodId)
	}

	methodName := getFullMethodName(classId, methodId)

	var err error
	switch classId {
	case ClassConfirm:
		err = c.handleClassConfirmMethod(methodId, reader, frame.Channel)
	case ClassConnection:
		err = c.handleClassConnectionMethod(methodId, reader) // Assumes handleConnectionMethod also returns error
	case ClassChannel:
		err = c.handleClassChannelMethod(methodId, reader, frame.Channel)
	case ClassExchange:
		err = c.handleClassExchangeMethod(methodId, reader, frame.Channel)
	case ClassQueue:
		err = c.handleClassQueueMethod(methodId, reader, frame.Channel)
	case ClassBasic:
		err = c.handleClassBasicMethod(methodId, reader, frame.Channel)
	case ClassTx:
		err = c.handleClassTxMethod(methodId, reader, frame.Channel)
	default:
		errMsgBase := "received method for unknown class ID"
		c.server.Warn("%s: %d on channel %d", errMsgBase, classId, frame.Channel)
		// AMQP: Connection.Close(amqpError.CommandInvalid.Code(), "command invalid - unknown class", classId, methodId)
		err = fmt.Errorf("%s: %d", errMsgBase, classId)
	}

	// If err is not nil, it means one of the handlers returned an error.
	// This error will be propagated to the main handleConnection loop.
	if err != nil {
		// Specific logging of the error source might have already happened in the handler.
		// Wrapping it provides context up the call stack.
		return fmt.Errorf("error processing method %s (class: %d, method: %d): %w", methodName, classId, methodId, err)
	}

	return nil // Success
}

func (c *connection) handleClassConfirmMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassConfirm, methodId)

	ch, channelExists, isClosing := c.getChannel(channelId)

	if !channelExists && channelId != 0 {
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for confirm operation", channelId)
		c.server.Err("Confirm method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		return c.sendConnectionClose(amqpError.CommandInvalid.Code(), replyText, uint16(ClassConfirm), methodId)
	}

	if ch == nil && channelId != 0 {
		c.server.Err("Internal error: channel %d object not found for confirm method %s", channelId, methodName)
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR - channel state inconsistency", uint16(ClassConfirm), methodId)
	}

	if channelId == 0 {
		replyText := "COMMAND_INVALID - confirm methods cannot be used on channel 0"
		c.server.Err("Confirm method %s on channel 0. Sending Connection.Close.", methodName)
		return c.sendConnectionClose(amqpError.CommandInvalid.Code(), replyText, uint16(ClassConfirm), methodId)
	}

	if isClosing {
		c.server.Debug("Ignoring confirm method %s on channel %d that is being closed", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodConfirmSelect:
		return c.handleMethodConfirmSelect(reader, channelId, ch)

	default:
		replyText := fmt.Sprintf("unknown or not implemented confirm method id %d", methodId)
		c.server.Err("Unhandled confirm method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		return c.sendChannelClose(channelId, amqpError.NotImplemented.Code(), replyText, uint16(ClassConfirm), methodId)
	}
}

func (c *connection) sendConnectionTune() error {
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
	binary.Write(payload, binary.BigEndian, uint16(MethodConnectionTune))

	// channelMax: 0 means no specific limit from server
	binary.Write(payload, binary.BigEndian, uint16(0))

	// frameMax: suggested max frame size (including header and end byte)
	binary.Write(payload, binary.BigEndian, uint32(suggestedFrameMaxSize))

	// heartbeat: suggested heartbeat interval in seconds
	binary.Write(payload, binary.BigEndian, uint16(suggestedHeartbeatInterval))

	if err := c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: 0,
		Payload: payload.Bytes(),
	}); err != nil {
		c.server.Err("Error sending connection.tune frame: %v", err)
		return fmt.Errorf("sending connection.tune frame: %w", err)
	}
	c.server.Info("Sent connection.tune")
	return nil
}

func (c *connection) sendBasicGetEmpty(channelId uint16) error {
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
	binary.Write(payload, binary.BigEndian, uint16(MethodBasicGetEmpty))
	writeShortString(payload, "") // cluster-id (reserved, must be empty)

	return c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	})
}

func (c *connection) handleHeader(frame *frame) error {
	if frame.Channel == 0 {
		c.server.Err("Received content header frame on channel 0")
		return c.sendConnectionClose(amqpError.ChannelError.Code(), "channel error - content frames cannot use channel 0", 0, 0)
	}

	ch, exists, isClosing := c.getChannel(frame.Channel)

	if !exists {
		// Header frame for a non-existent channel (or channel 0 which shouldn't get headers like this)
		c.server.Warn("Received header frame on non-existent or invalid channel %d", frame.Channel)
		return c.sendConnectionClose(amqpError.ChannelError.Code(), fmt.Sprintf("header frame on invalid channel %d", frame.Channel), 0, 0)
	}

	if isClosing {
		c.server.Debug("Ignoring header frame on channel %d that is being closed by server", frame.Channel)
		return nil
	}

	ch.mu.Lock()
	defer ch.mu.Unlock() // Ensure unlock even on error paths

	// Check if channel is being closed by server
	if ch.closingByServer {
		c.server.Debug("Ignoring header frame on channel %d that is being closed by server", frame.Channel)
		return nil // Just ignore it
	}

	if len(ch.pendingMessages) == 0 {
		c.server.Warn("Received header frame with no pending message on channel %d", frame.Channel)
		return c.sendChannelClose(frame.Channel, amqpError.UnexpectedFrame.Code(), "header frame received without pending basic.publish", uint16(ClassBasic), 0) // 0 for methodId as it's not a direct method response
	}

	pendingMessage := &ch.pendingMessages[0] // Get, don't remove yet

	reader := bytes.NewReader(frame.Payload)
	var classId, weight uint16 // weight is deprecated
	var bodySize uint64

	if err := binary.Read(reader, binary.BigEndian, &classId); err != nil {
		return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "malformed header: could not read class-id", 0, 0)
	}
	if err := binary.Read(reader, binary.BigEndian, &weight); err != nil {
		return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "malformed header: could not read weight", classId, 0)
	}
	if err := binary.Read(reader, binary.BigEndian, &bodySize); err != nil {
		return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "malformed header: could not read body-size", classId, 0)
	}

	c.server.Info("Processing basic header frame: channel=%d, classId=%d, bodySize=%d", frame.Channel, classId, bodySize)

	if classId != ClassBasic { // Only Basic class messages have properties like this
		c.server.Warn("Received header frame for non-Basic class %d on channel %d", classId, frame.Channel)
		// AMQP code 503 (COMMAND_INVALID) or 505 (UNEXPECTED_FRAME)
		return c.sendChannelClose(frame.Channel, amqpError.CommandInvalid.Code(), fmt.Sprintf("header frame for unexpected class %d", classId), classId, 0)
	}

	var flags uint16
	if err := binary.Read(reader, binary.BigEndian, &flags); err != nil {
		return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "malformed header: could not read property-flags", classId, 0)
	}

	// Parse properties based on flags
	var errProp error // To catch errors from reading properties
	var err error
	if flags&0x8000 != 0 { // content-type
		pendingMessage.Properties.ContentType, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed content-type", classId, 0)
		}
	}
	if flags&0x4000 != 0 { // content-encoding
		pendingMessage.Properties.ContentEncoding, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed content-encoding", classId, 0)
		}
	}
	if flags&0x2000 != 0 { // headers
		var headersMap map[string]interface{}
		headersMap, errProp = readTable(reader) // Use error-returning readTable
		if errProp != nil {
			c.server.Err("Error parsing headers table in handleHeader for channel %d: %v", frame.Channel, errProp)
			// AMQP code 502 (SYNTAX_ERROR) for malformed table
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "malformed headers table", classId, 0)
		}
		pendingMessage.Properties.Headers = headersMap
	}
	if flags&0x1000 != 0 { // delivery-mode
		if errProp = binary.Read(reader, binary.BigEndian, &pendingMessage.Properties.DeliveryMode); errProp != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "malformed delivery-mode", classId, 0)
		}
	}
	if flags&0x0800 != 0 { // priority
		if errProp = binary.Read(reader, binary.BigEndian, &pendingMessage.Properties.Priority); errProp != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "malformed priority", classId, 0)
		}
	}
	if flags&0x0400 != 0 { // correlation-id
		pendingMessage.Properties.CorrelationId, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed correlation-id", classId, 0)
		}
	}
	if flags&0x0200 != 0 { // reply-to
		pendingMessage.Properties.ReplyTo, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed reply-to", classId, 0)
		}
	}
	if flags&0x0100 != 0 { // expiration
		pendingMessage.Properties.Expiration, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed expiration", classId, 0)
		}
	}
	if flags&0x0080 != 0 { // message-id
		pendingMessage.Properties.MessageId, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed message-id", classId, 0)
		}
	}
	if flags&0x0040 != 0 { // timestamp
		if errProp = binary.Read(reader, binary.BigEndian, &pendingMessage.Properties.Timestamp); errProp != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "malformed timestamp", classId, 0)
		}
	}
	if flags&0x0020 != 0 { // type
		pendingMessage.Properties.Type, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed type", classId, 0)
		}
	}
	if flags&0x0010 != 0 { // user-id
		pendingMessage.Properties.UserId, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed user-id", classId, 0)
		}
	}
	if flags&0x0008 != 0 { // app-id
		pendingMessage.Properties.AppId, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed app-id", classId, 0)
		}
	}
	if flags&0x0004 != 0 { // cluster-id (Reserved, usually not used by clients)
		pendingMessage.Properties.ClusterId, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed cluster-id", classId, 0)
		}
	}

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of header frame payload on channel %d", frame.Channel)
	}

	return nil // Successfully processed header
}

func (c *connection) handleMethodConfirmSelect(reader *bytes.Reader, channelId uint16, ch *channel) error {
	c.server.Info("Processing confirm.%select for channel %d", channelId)

	// Read nowait bit
	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(),
			"SYNTAX_ERROR - malformed confirm.select (bits)", uint16(ClassConfirm), MethodConfirmSelect)
	}

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of confirm.select payload for channel %d.", channelId)
	}

	noWait := (bits & 0x01) != 0

	// Enable confirm mode for this channel
	ch.mu.Lock()
	ch.confirmMode = true
	ch.mu.Unlock()

	c.server.Info("Enabled publisher confirms on channel %d, noWait=%v", channelId, noWait)

	if !noWait {
		// Send Confirm.SelectOk
		payloadOk := &bytes.Buffer{}
		binary.Write(payloadOk, binary.BigEndian, uint16(ClassConfirm))
		binary.Write(payloadOk, binary.BigEndian, uint16(MethodConfirmSelectOk))

		if err := c.writeFrame(&frame{
			Type:    FrameMethod,
			Channel: channelId,
			Payload: payloadOk.Bytes(),
		}); err != nil {
			c.server.Err("Error sending confirm.select-ok: %v", err)
			return err
		}
		c.server.Info("Sent confirm.select-ok for channel %d", channelId)
	}

	return nil
}

func (c *connection) handleBody(frame *frame) {
	if frame.Channel == 0 {
		c.server.Err("Received content body frame on channel 0")
		// For body frames, we might already be in an inconsistent state
		c.conn.Close()
		return
	}

	// Get the channel and its pendingMessage
	ch, exists, isClosing := c.getChannel(frame.Channel)

	if !exists {
		c.server.Warn("Received body frame on non-existent channel %d", frame.Channel)
		return
	}

	if isClosing {
		c.server.Debug("Ignoring body frame on channel %d that is being closed by server", frame.Channel)
		return
	}

	ch.mu.Lock()

	if len(ch.pendingMessages) > 0 {
		// Take the FIRST pending message (FIFO)
		pendingMessage := ch.pendingMessages[0]
		ch.pendingMessages = ch.pendingMessages[1:] // Remove it

		c.server.Info("Processing body frame: channel=%d, size=%d, exchange=%s, routingKey=%s",
			frame.Channel, len(frame.Payload), pendingMessage.Exchange, pendingMessage.RoutingKey)

		// Create a DEEP copy of Properties
		propertiesCopy := properties{
			ContentType:     pendingMessage.Properties.ContentType,
			ContentEncoding: pendingMessage.Properties.ContentEncoding,
			DeliveryMode:    pendingMessage.Properties.DeliveryMode,
			Priority:        pendingMessage.Properties.Priority,
			CorrelationId:   pendingMessage.Properties.CorrelationId,
			ReplyTo:         pendingMessage.Properties.ReplyTo,
			Expiration:      pendingMessage.Properties.Expiration,
			MessageId:       pendingMessage.Properties.MessageId,
			Timestamp:       pendingMessage.Properties.Timestamp,
			Type:            pendingMessage.Properties.Type,
			UserId:          pendingMessage.Properties.UserId,
			AppId:           pendingMessage.Properties.AppId,
			ClusterId:       pendingMessage.Properties.ClusterId,
		}

		// Deep copy the Headers map if it exists
		if pendingMessage.Properties.Headers != nil {
			propertiesCopy.Headers = make(map[string]interface{})
			for k, v := range pendingMessage.Properties.Headers {
				propertiesCopy.Headers[k] = v
			}
		}

		// Create the complete message with deep-copied properties
		messageToDeliver := message{
			Exchange:   pendingMessage.Exchange,
			RoutingKey: pendingMessage.RoutingKey,
			Mandatory:  pendingMessage.Mandatory,
			Immediate:  pendingMessage.Immediate,
			Properties: propertiesCopy,
			Body:       make([]byte, len(frame.Payload)),
		}
		copy(messageToDeliver.Body, frame.Payload)

		ch.mu.Unlock()

		// Deliver the message
		c.deliverMessage(&messageToDeliver, frame.Channel)
	} else {
		ch.mu.Unlock()
		c.server.Warn("Received body frame with no pending message on channel %d", frame.Channel)
	}
}

// RouteMessage handles all exchange type routing logic
func (c *connection) routeMessage(msg *message) ([]string, error) {
	vhost := c.vhost

	if msg.Exchange == "" { // Default exchange routes directly to queue by name
		vhost.mu.RLock()
		_, exists := vhost.queues[msg.RoutingKey]
		vhost.mu.RUnlock()

		if exists {
			return []string{msg.RoutingKey}, nil
		}
		return nil, nil
	}

	vhost.mu.RLock()
	exchange := vhost.exchanges[msg.Exchange]
	vhost.mu.RUnlock()

	if exchange == nil {
		return nil, fmt.Errorf("exchange '%s' not found", msg.Exchange)
	}

	exchange.mu.RLock()
	defer exchange.mu.RUnlock()

	switch exchange.Type {
	case "direct":
		return c.routeDirect(exchange, msg.RoutingKey), nil
	case "fanout":
		return c.routeFanout(exchange), nil
	case "topic":
		return c.routeTopic(exchange, msg.RoutingKey), nil
	// case "headers": // Not implemented yet
	default:
		return nil, fmt.Errorf("unknown exchange type: %s", exchange.Type)
	}
}

// routeDirect returns queues bound with exact routing key match
func (c *connection) routeDirect(exchange *exchange, routingKey string) []string {
	return exchange.Bindings[routingKey]
}

// routeFanout returns all queues bound to the exchange
func (c *connection) routeFanout(exchange *exchange) []string {
	queues := make([]string, 0)
	queueSet := make(map[string]bool)

	for _, boundQueues := range exchange.Bindings {
		for _, queueName := range boundQueues {
			if !queueSet[queueName] {
				queueSet[queueName] = true
				queues = append(queues, queueName)
			}
		}
	}

	return queues
}

// routeTopic returns queues with topic pattern matching
func (c *connection) routeTopic(exchange *exchange, routingKey string) []string {
	queues := make([]string, 0)
	queueSet := make(map[string]bool)

	for pattern, boundQueues := range exchange.Bindings {
		if topicMatch(pattern, routingKey) {
			for _, queueName := range boundQueues {
				if !queueSet[queueName] {
					queueSet[queueName] = true
					queues = append(queues, queueName)
				}
			}
		}
	}

	return queues
}

func (c *connection) deliverMessage(msg *message, channelId uint16) {
	c.deliverMessageInternal(msg, channelId, false)
}

func (c *connection) deliverMessageInternal(msg *message, channelId uint16, isCommit bool) {
	// Check if channel is in transaction mode (but skip this check during commit)
	if !isCommit {
		ch, exists, _ := c.getChannel(channelId)
		if exists && ch != nil && channelId != 0 {
			ch.mu.Lock()
			if ch.txMode {
				// Store message in transaction instead of delivering immediately
				txMsg := transactionalMessage{
					Message:    msg.DeepCopy(),
					RoutingKey: msg.RoutingKey,
					Exchange:   msg.Exchange,
					Mandatory:  msg.Mandatory,
					Immediate:  msg.Immediate,
				}
				ch.txMessages = append(ch.txMessages, txMsg)
				ch.mu.Unlock()

				c.server.Info("Stored message in transaction on channel %d: exchange=%s, routingKey=%s",
					channelId, msg.Exchange, msg.RoutingKey)
				return
			}
			ch.mu.Unlock()
		}
	}

	c.server.Info("Message: exchange=%s, routingKey=%s, mandatory=%v", msg.Exchange, msg.RoutingKey, msg.Mandatory)

	// Get channel for confirm handling
	var deliveryTag uint64
	var confirmMode bool

	ch, exists, _ := c.getChannel(channelId)

	if exists && ch != nil && channelId != 0 {
		ch.mu.Lock()
		if ch.confirmMode {
			confirmMode = true
			deliveryTag = ch.nextPublishSeqNo
			ch.nextPublishSeqNo++
			ch.pendingConfirms[deliveryTag] = true
		}
		ch.mu.Unlock()
	}

	// Route the message
	queueNames, err := c.routeMessage(msg)
	if err != nil {
		c.server.Err("Error routing message: %v", err)
		if msg.Mandatory && channelId != 0 {
			if returnErr := c.sendBasicReturn(channelId, amqpError.NoRoute.Code(), "NO_ROUTE", msg.Exchange, msg.RoutingKey); returnErr != nil {
				c.server.Err("Failed to send basic.return: %v", returnErr)
			}
			c.sendReturnedMessage(channelId, msg)
		}
		if confirmMode {
			c.sendBasicNack(channelId, deliveryTag, false, false)
		}
		return
	}

	if len(queueNames) == 0 && msg.Mandatory && channelId != 0 {
		c.server.Warn("No queues for mandatory message on exchange '%s' with routing key '%s'", msg.Exchange, msg.RoutingKey)

		if returnErr := c.sendBasicReturn(channelId, amqpError.NoRoute.Code(), "NO_ROUTE", msg.Exchange, msg.RoutingKey); returnErr != nil {
			c.server.Err("Failed to send basic.return: %v", returnErr)
			if confirmMode {
				c.sendBasicNack(channelId, deliveryTag, false, false)
			}
			return
		}

		c.sendReturnedMessage(channelId, msg)
		if confirmMode {
			c.sendBasicNack(channelId, deliveryTag, false, false)
		}
		return
	}

	// Deliver to each queue
	for _, queueName := range queueNames {
		c.deliverToQueue(queueName, msg)
	}

	if confirmMode {
		c.sendBasicAck(channelId, deliveryTag, false)
	}
}

func (c *connection) sendBasicAck(channelId uint16, deliveryTag uint64, multiple bool) error {
	ch, exists, _ := c.getChannel(channelId)
	if !exists || ch == nil {
		return fmt.Errorf("channel %d not found for sending basic.ack", channelId)
	}

	ch.mu.Lock()
	if !ch.confirmMode {
		ch.mu.Unlock()
		return nil // Not in confirm mode, nothing to do
	}

	// Remove from pending confirms
	if multiple {
		for tag := range ch.pendingConfirms {
			if tag <= deliveryTag {
				delete(ch.pendingConfirms, tag)
			}
		}
	} else {
		delete(ch.pendingConfirms, deliveryTag)
	}
	ch.mu.Unlock()

	c.server.Info("Sending basic.ack for publisher confirm on channel %d: deliveryTag=%d, multiple=%v",
		channelId, deliveryTag, multiple)

	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
	binary.Write(payload, binary.BigEndian, uint16(MethodBasicAck))
	binary.Write(payload, binary.BigEndian, deliveryTag)

	if multiple {
		payload.WriteByte(1)
	} else {
		payload.WriteByte(0)
	}

	return c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	})
}

func (c *connection) sendBasicNack(channelId uint16, deliveryTag uint64, multiple bool, requeue bool) error {
	ch, exists, _ := c.getChannel(channelId)
	if !exists || ch == nil {
		return fmt.Errorf("channel %d not found for sending basic.nack", channelId)
	}

	ch.mu.Lock()
	if !ch.confirmMode {
		ch.mu.Unlock()
		return nil // Not in confirm mode, nothing to do
	}

	// Remove from pending confirms
	if multiple {
		for tag := range ch.pendingConfirms {
			if tag <= deliveryTag {
				delete(ch.pendingConfirms, tag)
			}
		}
	} else {
		delete(ch.pendingConfirms, deliveryTag)
	}
	ch.mu.Unlock()

	c.server.Info("Sending basic.nack for publisher confirm on channel %d: deliveryTag=%d, multiple=%v, requeue=%v",
		channelId, deliveryTag, multiple, requeue)

	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
	binary.Write(payload, binary.BigEndian, uint16(MethodBasicNack))
	binary.Write(payload, binary.BigEndian, deliveryTag)

	bits := byte(0)
	if multiple {
		bits |= 0x01
	}
	if requeue {
		bits |= 0x02
	}
	payload.WriteByte(bits)

	return c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	})
}

// Helper method to send the returned message's header and body
func (c *connection) sendReturnedMessage(channelId uint16, msg *message) {
	// Send header frame
	headerPayload := &bytes.Buffer{}
	binary.Write(headerPayload, binary.BigEndian, uint16(ClassBasic))
	binary.Write(headerPayload, binary.BigEndian, uint16(0)) // weight
	binary.Write(headerPayload, binary.BigEndian, uint64(len(msg.Body)))

	// Calculate and write property flags
	flags := uint16(0)
	if msg.Properties.ContentType != "" {
		flags |= 0x8000
	}
	if msg.Properties.ContentEncoding != "" {
		flags |= 0x4000
	}
	if len(msg.Properties.Headers) > 0 {
		flags |= 0x2000
	}
	if msg.Properties.DeliveryMode != 0 {
		flags |= 0x1000
	}
	if msg.Properties.Priority != 0 {
		flags |= 0x0800
	}
	if msg.Properties.CorrelationId != "" {
		flags |= 0x0400
	}
	if msg.Properties.ReplyTo != "" {
		flags |= 0x0200
	}
	if msg.Properties.Expiration != "" {
		flags |= 0x0100
	}
	if msg.Properties.MessageId != "" {
		flags |= 0x0080
	}
	if msg.Properties.Timestamp != 0 {
		flags |= 0x0040
	}
	if msg.Properties.Type != "" {
		flags |= 0x0020
	}
	if msg.Properties.UserId != "" {
		flags |= 0x0010
	}
	if msg.Properties.AppId != "" {
		flags |= 0x0008
	}
	if msg.Properties.ClusterId != "" {
		flags |= 0x0004
	}

	binary.Write(headerPayload, binary.BigEndian, flags)

	// Write properties based on flags
	if flags&0x8000 != 0 {
		writeShortString(headerPayload, msg.Properties.ContentType)
	}
	if flags&0x4000 != 0 {
		writeShortString(headerPayload, msg.Properties.ContentEncoding)
	}
	if flags&0x2000 != 0 {
		writeTable(headerPayload, msg.Properties.Headers)
	}
	if flags&0x1000 != 0 {
		binary.Write(headerPayload, binary.BigEndian, msg.Properties.DeliveryMode)
	}
	if flags&0x0800 != 0 {
		binary.Write(headerPayload, binary.BigEndian, msg.Properties.Priority)
	}
	if flags&0x0400 != 0 {
		writeShortString(headerPayload, msg.Properties.CorrelationId)
	}
	if flags&0x0200 != 0 {
		writeShortString(headerPayload, msg.Properties.ReplyTo)
	}
	if flags&0x0100 != 0 {
		writeShortString(headerPayload, msg.Properties.Expiration)
	}
	if flags&0x0080 != 0 {
		writeShortString(headerPayload, msg.Properties.MessageId)
	}
	if flags&0x0040 != 0 {
		binary.Write(headerPayload, binary.BigEndian, msg.Properties.Timestamp)
	}
	if flags&0x0020 != 0 {
		writeShortString(headerPayload, msg.Properties.Type)
	}
	if flags&0x0010 != 0 {
		writeShortString(headerPayload, msg.Properties.UserId)
	}
	if flags&0x0008 != 0 {
		writeShortString(headerPayload, msg.Properties.AppId)
	}
	if flags&0x0004 != 0 {
		writeShortString(headerPayload, msg.Properties.ClusterId)
	}

	// Send header frame
	if err := c.writeFrame(&frame{
		Type:    FrameHeader,
		Channel: channelId,
		Payload: headerPayload.Bytes(),
	}); err != nil {
		c.server.Err("Failed to send header frame for returned message: %v", err)
		return
	}

	// Send body frame
	if err := c.writeFrame(&frame{
		Type:    FrameBody,
		Channel: channelId,
		Payload: msg.Body,
	}); err != nil {
		c.server.Err("Failed to send body frame for returned message: %v", err)
	}
}

// sendBasicCancelFromServer sends a Basic.Cancel method frame to the client.
// This informs the client that a consumer has been cancelled by the server.
func (c *connection) sendBasicCancelFromServer(channelId uint16, consumerTag string) error {
	c.server.Debug("Sending Basic.Cancel to client for consumer '%s' on channel %d", consumerTag, channelId)

	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
	binary.Write(payload, binary.BigEndian, uint16(MethodBasicCancel))
	writeShortString(payload, consumerTag)
	binary.Write(payload, binary.BigEndian, byte(0x00)) // no-wait bit (always false for server-sent)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.writeFrameInternal(FrameMethod, channelId, payload.Bytes()); err != nil {
		return fmt.Errorf("error writing basic.cancel frame: %w", err)
	}
	return c.writer.Flush()
}

func (c *connection) sendBasicCancelToConsumers(consumers map[string]*consumer, queueName string) {
	c.server.Info("Sending Basic.Cancel for %d consumers on queue '%s'", len(consumers), queueName)
	for _, consumer := range consumers {
		if err := c.sendBasicCancelFromServer(consumer.ChannelId, consumer.Tag); err != nil {
			c.server.Err("Failed to send Basic.Cancel for consumer '%s' on channel %d: %v",
				consumer.Tag, consumer.ChannelId, err)
		} else {
			c.server.Info("Sent Basic.Cancel for consumer '%s' on channel %d",
				consumer.Tag, consumer.ChannelId)
		}
	}
}

func (c *connection) sendPurgeOk(channelId uint16, messageCount uint32) error {
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassQueue))
	binary.Write(payload, binary.BigEndian, uint16(MethodQueuePurgeOk))
	binary.Write(payload, binary.BigEndian, messageCount)

	return c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	})
}

// deliverToQueue handles delivery to a single queue
func (c *connection) deliverToQueue(queueName string, msg *message) error {
	vhost := c.vhost
	if vhost == nil || vhost.IsDeleting() {
		return fmt.Errorf("vhost is nil or being deleted")
	}

	vhost.mu.RLock()
	queue, exists := vhost.queues[queueName]
	vhost.mu.RUnlock()

	if !exists || queue == nil {
		return fmt.Errorf("queue %s not found", queueName)
	}

	msgCopy := msg.DeepCopy()

	// For persistent messages to durable queues, use transactions
	if c.server.persistenceManager != nil &&
		msg.Properties.DeliveryMode == 2 &&
		queue.Durable {

		// Start transaction
		tx, err := c.server.persistenceManager.BeginTransaction()
		if err != nil {
			return fmt.Errorf("beginning transaction: %w", err)
		}

		// Prepare message record
		messageId := GetMessageIdentifier(msgCopy)
		record := MessageToRecord(msgCopy, messageId, 0)

		// Save to transaction
		if err := tx.SaveMessage(vhost.name, queueName, record); err != nil {
			tx.Rollback()
			return fmt.Errorf("saving message to transaction: %w", err)
		}

		// Commit persistence
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("committing message: %w", err)
		}

		// Only add to memory after successful persistence
		queue.mu.Lock()
		queue.Messages = append(queue.Messages, *msgCopy)
		queue.mu.Unlock()
	} else {
		// Non-persistent or non-durable: just add to memory
		queue.mu.Lock()
		queue.Messages = append(queue.Messages, *msgCopy)
		queue.mu.Unlock()
	}

	c.server.Info("Message enqueued to queue '%s'. Queue now has %d messages.",
		queueName, len(queue.Messages))

	return nil
}

func (c *connection) deliverMessages(channelId uint16, consumerTag string, consumer *consumer) {
	queue := consumer.Queue
	noAck := consumer.NoAck

	for {
		var messageAvailable bool

		// Quick check if we have messages
		queue.mu.Lock()
		messageAvailable = len(queue.Messages) > 0
		queue.mu.Unlock()

		if !messageAvailable {
			// Only use select with timer when no messages
			select {
			case <-consumer.stopCh:
				c.server.Info("Consumer %s stopped on channel %d", consumerTag, channelId)
				return
			case <-time.After(100 * time.Millisecond):
				// Check again for messages
				continue
			}
		} else {
			// We have messages, just check stopCh without blocking
			select {
			case <-consumer.stopCh:
				c.server.Info("Consumer %s stopped on channel %d", consumerTag, channelId)
				return
			default:
				// Fall through immediately to process message
			}
		}

		ch, exists, isClosing := c.getChannel(channelId)
		if !exists || isClosing {
			c.server.Info("Channel %d no longer valid, stopping delivery for consumer %s", channelId, consumerTag)
			return
		}

		// Check QoS prefetch limit BEFORE taking a message
		// Check if we've hit the prefetch limit
		ch.mu.Lock()
		prefetchCount := ch.prefetchCount
		prefetchSize := ch.prefetchSize
		unackedCount := uint16(0)
		unackedSize := uint32(0)

		// Count unacked messages and size for this consumer
		if !noAck && (prefetchCount > 0 || prefetchSize > 0) {
			for _, unacked := range ch.unackedMessages {
				if unacked.ConsumerTag == consumerTag {
					unackedCount++
					// Calculate message size: body + properties overhead
					messageSize := uint32(len(unacked.Message.Body))
					// Add estimated overhead for properties (rough estimate)
					unackedSize += messageSize
				}
			}

			// Check count limit
			if prefetchCount > 0 && unackedCount >= prefetchCount {
				ch.mu.Unlock()
				c.server.Debug("Consumer %s has %d unacked messages (limit %d), waiting",
					consumerTag, unackedCount, prefetchCount)
				time.Sleep(50 * time.Millisecond)
				continue
			}

			// Check size limit
			if prefetchSize > 0 && unackedSize >= prefetchSize {
				ch.mu.Unlock()
				c.server.Debug("Consumer %s has %d bytes unacked (limit %d), waiting",
					consumerTag, unackedSize, prefetchSize)
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
		ch.mu.Unlock()

		// Try to get and process a message
		queue.mu.Lock()

		// Double-check we still have messages and consumer exists
		if len(queue.Messages) == 0 {
			queue.mu.Unlock()
			continue
		}

		if _, exists := queue.Consumers[consumerTag]; !exists {
			queue.mu.Unlock()
			c.server.Info("Consumer %s no longer registered, stopping delivery", consumerTag)
			return
		}

		// Peek at the first message to check if it would exceed size limit
		msg := queue.Messages[0]

		// Calculate the size of this message
		nextMessageSize := uint32(len(msg.Body))

		// Check if adding this message would exceed the size limit
		if !noAck && prefetchSize > 0 && (unackedSize+nextMessageSize) > prefetchSize {
			queue.mu.Unlock()
			c.server.Debug("Next message would exceed size limit for consumer %s (current: %d, next: %d, limit: %d)",
				consumerTag, unackedSize, nextMessageSize, prefetchSize)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Remove the message from queue
		queue.Messages = queue.Messages[1:]
		queue.mu.Unlock()

		// Check if vhost is being deleted
		if c.vhost != nil && c.vhost.IsDeleting() {
			// Put message back at front of queue
			queue.mu.Lock()
			queue.Messages = append([]message{msg}, queue.Messages...)
			queue.mu.Unlock()
			c.server.Info("VHost deletion detected, stopping delivery for consumer %s", consumerTag)
			return
		}

		// Get channel and check if it's still valid
		ch, exists, isClosing = c.getChannel(channelId)
		if !exists || isClosing {
			// Put message back at front of queue
			queue.mu.Lock()
			queue.Messages = append([]message{msg}, queue.Messages...)
			queue.mu.Unlock()
			c.server.Info("Channel %d no longer valid, stopping delivery for consumer %s", channelId, consumerTag)
			return
		}

		// Assign delivery tag and track if not noAck
		ch.mu.Lock()
		ch.deliveryTag++
		deliveryTag := ch.deliveryTag

		if !noAck {
			unacked := &unackedMessage{
				Message:     msg,
				ConsumerTag: consumerTag,
				QueueName:   queue.Name,
				DeliveryTag: deliveryTag,
				Delivered:   time.Now(),
			}
			ch.unackedMessages[deliveryTag] = unacked
		}
		ch.mu.Unlock()

		// Create a deep copy of the message to avoid data races
		msgCopy := msg.DeepCopy()

		c.server.Info("Delivering message: channel=%d, consumerTag=%s, deliveryTag=%d, exchange=%s, routingKey=%s, bodySize=%d, noAck=%v, redelivered=%v",
			channelId, consumerTag, deliveryTag, msgCopy.Exchange, msgCopy.RoutingKey, len(msgCopy.Body), noAck, msgCopy.Redelivered)

		// Construct Method Payload (Basic.Deliver)
		methodPayload := &bytes.Buffer{}
		binary.Write(methodPayload, binary.BigEndian, uint16(ClassBasic))
		binary.Write(methodPayload, binary.BigEndian, uint16(MethodBasicDeliver))
		writeShortString(methodPayload, consumerTag)
		binary.Write(methodPayload, binary.BigEndian, deliveryTag)

		if msgCopy.Redelivered {
			methodPayload.WriteByte(1)
		} else {
			methodPayload.WriteByte(0)
		}

		writeShortString(methodPayload, msgCopy.Exchange)
		writeShortString(methodPayload, msgCopy.RoutingKey)

		// Construct Header Payload
		headerPayload := &bytes.Buffer{}
		binary.Write(headerPayload, binary.BigEndian, uint16(ClassBasic))
		binary.Write(headerPayload, binary.BigEndian, uint16(0))
		binary.Write(headerPayload, binary.BigEndian, uint64(len(msgCopy.Body)))

		flags := uint16(0)
		if msgCopy.Properties.ContentType != "" {
			flags |= 0x8000
		}
		if msgCopy.Properties.ContentEncoding != "" {
			flags |= 0x4000
		}
		if len(msgCopy.Properties.Headers) > 0 {
			flags |= 0x2000
		}
		if msgCopy.Properties.DeliveryMode != 0 {
			flags |= 0x1000
		}
		if msgCopy.Properties.Priority != 0 {
			flags |= 0x0800
		}
		if msgCopy.Properties.CorrelationId != "" {
			flags |= 0x0400
		}
		if msgCopy.Properties.ReplyTo != "" {
			flags |= 0x0200
		}
		if msgCopy.Properties.Expiration != "" {
			flags |= 0x0100
		}
		if msgCopy.Properties.MessageId != "" {
			flags |= 0x0080
		}
		if msgCopy.Properties.Timestamp != 0 {
			flags |= 0x0040
		}
		if msgCopy.Properties.Type != "" {
			flags |= 0x0020
		}
		if msgCopy.Properties.UserId != "" {
			flags |= 0x0010
		}
		if msgCopy.Properties.AppId != "" {
			flags |= 0x0008
		}
		if msgCopy.Properties.ClusterId != "" {
			flags |= 0x0004
		}

		binary.Write(headerPayload, binary.BigEndian, flags)

		// Write properties based on flags
		if flags&0x8000 != 0 {
			writeShortString(headerPayload, msgCopy.Properties.ContentType)
		}
		if flags&0x4000 != 0 {
			writeShortString(headerPayload, msgCopy.Properties.ContentEncoding)
		}
		if flags&0x2000 != 0 {
			writeTable(headerPayload, msgCopy.Properties.Headers)
		}
		if flags&0x1000 != 0 {
			binary.Write(headerPayload, binary.BigEndian, msgCopy.Properties.DeliveryMode)
		}
		if flags&0x0800 != 0 {
			binary.Write(headerPayload, binary.BigEndian, msgCopy.Properties.Priority)
		}
		if flags&0x0400 != 0 {
			writeShortString(headerPayload, msgCopy.Properties.CorrelationId)
		}
		if flags&0x0200 != 0 {
			writeShortString(headerPayload, msgCopy.Properties.ReplyTo)
		}
		if flags&0x0100 != 0 {
			writeShortString(headerPayload, msgCopy.Properties.Expiration)
		}
		if flags&0x0080 != 0 {
			writeShortString(headerPayload, msgCopy.Properties.MessageId)
		}
		if flags&0x0040 != 0 {
			binary.Write(headerPayload, binary.BigEndian, msgCopy.Properties.Timestamp)
		}
		if flags&0x0020 != 0 {
			writeShortString(headerPayload, msgCopy.Properties.Type)
		}
		if flags&0x0010 != 0 {
			writeShortString(headerPayload, msgCopy.Properties.UserId)
		}
		if flags&0x0008 != 0 {
			writeShortString(headerPayload, msgCopy.Properties.AppId)
		}
		if flags&0x0004 != 0 {
			writeShortString(headerPayload, msgCopy.Properties.ClusterId)
		}

		// Lock, Buffer all frames, Flush once, Unlock
		c.writeMu.Lock()

		var deliveryErr error
		deliveryErr = c.writeFrameInternal(FrameMethod, channelId, methodPayload.Bytes())
		if deliveryErr != nil {
			c.server.Err("Error buffering basic.deliver frame for tag %d: %v", deliveryTag, deliveryErr)
			c.writeMu.Unlock()

			// Put message back and clean up unacked tracking
			queue.mu.Lock()
			queue.Messages = append([]message{msg}, queue.Messages...)
			queue.mu.Unlock()

			if !noAck {
				ch.mu.Lock()
				delete(ch.unackedMessages, deliveryTag)
				ch.mu.Unlock()
			}
			continue
		}

		deliveryErr = c.writeFrameInternal(FrameHeader, channelId, headerPayload.Bytes())
		if deliveryErr != nil {
			c.server.Err("Error buffering header frame for tag %d: %v", deliveryTag, deliveryErr)
		}

		if deliveryErr == nil {
			deliveryErr = c.writeFrameInternal(FrameBody, channelId, msgCopy.Body)
			if deliveryErr != nil {
				c.server.Err("Error buffering body frame for tag %d: %v", deliveryTag, deliveryErr)
			}
		}

		if deliveryErr == nil {
			flushErr := c.writer.Flush()
			if flushErr != nil {
				c.server.Err("Error flushing writer after message delivery for tag %d: %v", deliveryTag, flushErr)
				deliveryErr = flushErr
			}
		}

		c.writeMu.Unlock()

		if deliveryErr == nil {
			c.server.Info("Successfully delivered message (method, header, body) for deliveryTag=%d", deliveryTag)

			// PERSISTENCE: Delete auto-acked persistent messages
			if noAck && c.server.persistenceManager != nil &&
				msgCopy.Properties.DeliveryMode == 2 && queue.Durable {
				messageId := GetMessageIdentifier(msgCopy)
				if err := c.server.persistenceManager.DeleteMessage(c.vhost.name, queue.Name, messageId); err != nil {
					c.server.Err("Failed to delete auto-acked message %s from persistence: %v", messageId, err)
				}
			}
		} else {
			c.server.Err("Failed to fully deliver message for deliveryTag=%d", deliveryTag)

			// Put message back and clean up unacked tracking
			queue.mu.Lock()
			queue.Messages = append([]message{msg}, queue.Messages...)
			queue.mu.Unlock()

			if !noAck {
				ch.mu.Lock()
				delete(ch.unackedMessages, deliveryTag)
				ch.mu.Unlock()
			}
		}
	}
}

func (c *connection) cleanupConnectionResources() {
	vhost := c.vhost

	c.server.Info("Cleaning up resources for connection %s", c.conn.RemoteAddr())
	c.mu.Lock() // Lock the connection to safely iterate over channels

	// NEW: Collect queue names that will need dispatch attempts
	affectedQueuesForDispatch := make(map[string]bool)

	for chanId, ch := range c.channels {
		c.server.Info("Cleaning up resources for channel %d on connection %s", chanId, c.conn.RemoteAddr())
		ch.mu.Lock() // Lock the specific channel

		// Handle unacked messages: requeue them
		if len(ch.unackedMessages) > 0 {
			c.server.Info("Channel %d has %d unacked messages during connection cleanup, preparing for requeue", chanId, len(ch.unackedMessages))
			for deliveryTag, unacked := range ch.unackedMessages {
				vhost.mu.RLock() // RLock server to access s.queues
				queue, qExists := vhost.queues[unacked.QueueName]
				vhost.mu.RUnlock()

				if qExists && queue != nil {
					unacked.Message.Redelivered = true // Mark as redelivered

					queue.mu.Lock() // Lock the specific queue
					// Prepend to queue (requeued messages go to front)
					queue.Messages = append([]message{unacked.Message}, queue.Messages...)
					c.server.Debug("Prepared message (tag %d from channel %d) for requeue to queue '%s'", deliveryTag, chanId, unacked.QueueName)
					affectedQueuesForDispatch[unacked.QueueName] = true // Mark queue for dispatch
					queue.mu.Unlock()                                   // Unlock queue
				} else {
					c.server.Warn("Queue '%s' not found for requeuing unacked message (tag %d from channel %d) during connection cleanup", unacked.QueueName, deliveryTag, chanId)
				}
			}
			// Clear unacked messages for the channel
			ch.unackedMessages = make(map[uint64]*unackedMessage)
		}

		// Clean up consumers associated with this channel
		for consumerTag, queueName := range ch.consumers {
			vhost.mu.RLock() // RLock server to access s.queues
			if queue, ok := vhost.queues[queueName]; ok {
				queue.mu.Lock() // Lock the specific queue
				if consumer, consumerExists := queue.Consumers[consumerTag]; consumerExists {
					c.server.Info("Closing consumer message channel for tag '%s' on queue '%s' (channel %d)", consumerTag, queueName, chanId)
					close(consumer.stopCh) // This will make the deliverMessages goroutine exit
					delete(queue.Consumers, consumerTag)
					// If this queue had messages and now has one less consumer, it might affect dispatch.
					// The generic dispatch attempt later will cover this.
				}
				queue.mu.Unlock() // Unlock queue
			}
			vhost.mu.RUnlock()
		}
		ch.consumers = map[string]string{} // Clear consumers map for the channel
		ch.pendingMessages = nil           // Clear any partial messages from publish
		// Ensure timer is stopped if channel was being closed by server
		if ch.closeOkTimer != nil {
			ch.closeOkTimer.Stop()
			ch.closeOkTimer = nil
		}
		ch.mu.Unlock() // Unlock the channel
	}
	c.channels = map[uint16]*channel{} // Clear the channels map on the connection
	c.mu.Unlock()                      // Unlock the connection

	c.server.Info("Finished cleaning up resources for connection %s", c.conn.RemoteAddr())
	// The underlying c.conn will be closed by the caller of cleanupConnectionResources or was already closed.
}

// Helper method to clean up queue bindings when an exchange is deleted
func (c *connection) cleanupQueueBindingsForExchange(vhost *vHost, exchangeName string) {
	vhost.mu.RLock()
	// Make a copy of queue names to avoid holding the lock too long
	queueNames := make([]string, 0, len(vhost.queues))
	for name := range vhost.queues {
		queueNames = append(queueNames, name)
	}
	vhost.mu.RUnlock()

	// Clean up bindings from each queue
	for _, queueName := range queueNames {
		vhost.mu.RLock()
		queue, exists := vhost.queues[queueName]
		vhost.mu.RUnlock()

		if !exists {
			continue
		}

		queue.mu.Lock()
		// Remove any bindings that reference the deleted exchange
		for binding := range queue.Bindings {
			if strings.HasPrefix(binding, exchangeName+":") {
				delete(queue.Bindings, binding)
				c.server.Debug("Removed binding '%s' from queue '%s' due to exchange deletion", binding, queueName)
			}
		}
		queue.mu.Unlock()
	}
}

// sendChannelClose sends a Channel.Close method to the client and cleans up the channel.
func (c *connection) sendChannelClose(channelId uint16, replyCode uint16, replyText string, offendingClassId uint16, offendingMethodId uint16) error {
	c.server.Warn("Sending Channel.Close on channel %d: code=%d, text='%s', class=%d, method=%d",
		channelId, replyCode, replyText, offendingClassId, offendingMethodId)

	payload := &bytes.Buffer{}
	// Ensure binary.Write errors are checked and handled ---
	if err := binary.Write(payload, binary.BigEndian, uint16(ClassChannel)); err != nil {
		// This is a server-side serialization error, very unlikely but good to be aware of
		c.server.Err("Internal error serializing ClassChannel for Channel.Close: %v", err)
		// Fallback to just closing connection if we can't even form the message
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR", 0, 0)
	}
	if err := binary.Write(payload, binary.BigEndian, uint16(MethodChannelClose)); err != nil {
		c.server.Err("Internal error serializing MethodChannelClose for Channel.Close: %v", err)
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR", 0, 0)
	}
	if err := binary.Write(payload, binary.BigEndian, replyCode); err != nil {
		c.server.Err("Internal error serializing replyCode for Channel.Close: %v", err)
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR", 0, 0)
	}
	writeShortString(payload, replyText) // Assuming writeShortString doesn't error or handles internally
	if err := binary.Write(payload, binary.BigEndian, offendingClassId); err != nil {
		c.server.Err("Internal error serializing offendingClassId for Channel.Close: %v", err)
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR", 0, 0)
	}
	if err := binary.Write(payload, binary.BigEndian, offendingMethodId); err != nil {
		c.server.Err("Internal error serializing offendingMethodId for Channel.Close: %v", err)
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR", 0, 0)
	}

	frame := &frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	}

	if err := c.writeFrame(frame); err != nil {
		c.server.Err("Error sending Channel.Close frame for channel %d: %v. Connection will likely be dropped.", channelId, err)
		// Call forceRemoveChannel if sending Channel.Close fails ---
		c.forceRemoveChannel(channelId, fmt.Sprintf("failed to send Channel.Close (underlying error: %v)", err))
		return fmt.Errorf("failed to send Channel.Close on channel %d: %w", channelId, err)
	}

	// If Channel.Close was sent successfully, mark channel and start CloseOk timer.
	c.mu.Lock() // Lock connection to safely access c.channels
	ch, exists := c.channels[channelId]
	if exists && ch != nil {
		ch.mu.Lock() // Lock the specific channel

		// Check if already closing to prevent re-arming timer unnecessarily ---
		if ch.closingByServer {
			ch.mu.Unlock()
			c.mu.Unlock()
			c.server.Warn("Channel.Close already initiated for channel %d, not re-arming timer.", channelId)
			return errChannelClosedByServer // Already in this state
		}

		c.server.Info("Marking channel %d for closure after sending Channel.Close. Awaiting CloseOk.", channelId)
		ch.closingByServer = true
		// Store replyCode and replyText for potential timeout logging
		ch.serverInitiatedCloseReplyCode = replyCode
		ch.serverInitiatedCloseReplyText = replyText

		// Clean up consumers and pending messages now, as the channel is logically closed to new work.
		if len(ch.consumers) > 0 {
			vhost := c.vhost
			for consumerTag, queueName := range ch.consumers {
				vhost.mu.RLock() // RLock server to access s.queues
				if queue, qExists := vhost.queues[queueName]; qExists {
					queue.mu.Lock()
					if consumer, consumerExistsOnQueue := queue.Consumers[consumerTag]; consumerExistsOnQueue {
						close(consumer.stopCh) // Signal deliverMessages goroutine to stop
						delete(queue.Consumers, consumerTag)
						c.server.Info("Closed consumer %s on queue %s for channel %d during server-initiated close", consumerTag, queueName, channelId)
					}
					queue.mu.Unlock()
				}
				vhost.mu.RUnlock()
			}
			ch.consumers = make(map[string]string)
		}
		ch.pendingMessages = nil // Clear any partial messages

		// Stop previous timer if any, before starting a new one
		if ch.closeOkTimer != nil {
			ch.closeOkTimer.Stop()
		}

		// Start a timer to wait for Channel.CloseOk
		c.server.Info("Starting %.0fs timer for Channel.CloseOk on channel %d.", channelCloseOkTimeout.Seconds(), channelId)
		ch.closeOkTimer = time.AfterFunc(channelCloseOkTimeout, func() {
			// Use stored reply code/text in timeout log ---
			c.server.Warn("Timeout waiting for Channel.CloseOk on channel %d (originally closed for code %d: '%s'). Force closing.",
				channelId, ch.serverInitiatedCloseReplyCode, ch.serverInitiatedCloseReplyText)
			c.forceRemoveChannel(channelId, "timeout waiting for Channel.CloseOk")
		})
		ch.mu.Unlock()
	} else {
		// Channel was already removed or ch was nil (shouldn't happen if exists is true).
		c.server.Warn("Attempted to send Channel.Close for channel %d, but it was not found in map or was nil.", channelId)
	}
	c.mu.Unlock()

	return errChannelClosedByServer
}

// forceRemoveChannel is an internal helper to clean up a channel from the connection
// typically called after a timeout, unrecoverable error, or when client initiates close.
func (c *connection) forceRemoveChannel(channelId uint16, reason string) {
	c.server.Info("Forcibly removing channel %d from connection %s. Reason: %s", channelId, c.conn.RemoteAddr(), reason)

	vhost := c.vhost

	c.mu.Lock()
	ch, exists := c.channels[channelId]
	if !exists || ch == nil {
		c.mu.Unlock()
		c.server.Info("Attempted to forcibly remove channel %d, but it was already gone or nil. Reason: %s", channelId, reason)
		return
	}

	ch.mu.Lock()
	if ch.closingByServer {
		ch.mu.Unlock()
		c.mu.Unlock()
		c.server.Debug("Channel %d already being cleaned up, skipping", channelId)
		return
	}
	ch.closingByServer = true
	ch.mu.Unlock()
	c.mu.Unlock()

	ch.mu.Lock()

	// Clear any pending transaction data
	if ch.txMode {
		messagesCount := len(ch.txMessages)
		acksCount := len(ch.txAcks)
		nacksCount := len(ch.txNacks)

		if messagesCount > 0 || acksCount > 0 || nacksCount > 0 {
			c.server.Info("Discarding uncommitted transaction on channel %d: %d messages, %d acks, %d nacks",
				channelId, messagesCount, acksCount, nacksCount)
		}

		ch.txMessages = nil
		ch.txAcks = nil
		ch.txNacks = nil
		ch.txMode = false
	}

	// Rest of the existing cleanup logic...
	if ch.closeOkTimer != nil {
		if !ch.closeOkTimer.Stop() {
			c.server.Debug("CloseOkTimer for channel %d had already fired", channelId)
		}
		ch.closeOkTimer = nil
	}

	affectedQueuesForDispatch := make(map[string]bool)

	if len(ch.unackedMessages) > 0 {
		c.server.Info("Channel %d has %d unacked messages, requeuing", channelId, len(ch.unackedMessages))
		for deliveryTag, unacked := range ch.unackedMessages {
			vhost.mu.RLock()
			queue, qExists := vhost.queues[unacked.QueueName]
			vhost.mu.RUnlock()

			if qExists && queue != nil {
				unacked.Message.Redelivered = true
				queue.mu.Lock()
				queue.Messages = append([]message{unacked.Message}, queue.Messages...)
				c.server.Debug("Prepared message (tag %d from channel %d) for requeue to queue '%s'",
					deliveryTag, channelId, unacked.QueueName)
				affectedQueuesForDispatch[unacked.QueueName] = true
				queue.mu.Unlock()
			} else {
				c.server.Warn("Queue '%s' not found for requeuing unacked message (tag %d from channel %d)",
					unacked.QueueName, deliveryTag, channelId)
			}
		}
		ch.unackedMessages = make(map[uint64]*unackedMessage)
	}

	if len(ch.pendingConfirms) > 0 {
		c.server.Info("Channel %d has %d pending confirms, sending nacks", channelId, len(ch.pendingConfirms))
		var maxTag uint64
		for tag := range ch.pendingConfirms {
			if tag > maxTag {
				maxTag = tag
			}
		}
		if maxTag > 0 {
			c.sendBasicNack(channelId, maxTag, true, false)
		}
		ch.pendingConfirms = make(map[uint64]bool)
	}

	if len(ch.consumers) > 0 {
		c.server.Info("Channel %d cleaning up %d consumers", channelId, len(ch.consumers))
		for consumerTag, queueName := range ch.consumers {
			vhost.mu.RLock()
			if q, qExists := vhost.queues[queueName]; qExists {
				q.mu.Lock()
				if consumer, consumerExists := q.Consumers[consumerTag]; consumerExists {
					close(consumer.stopCh)
					delete(q.Consumers, consumerTag)
					c.server.Info("Closed consumer %s on queue %s for channel %d",
						consumerTag, queueName, channelId)
				}
				q.mu.Unlock()
			}
			vhost.mu.RUnlock()
		}
		ch.consumers = map[string]string{}
	}
	ch.pendingMessages = nil
	ch.mu.Unlock()

	c.mu.Lock()
	delete(c.channels, channelId)
	c.mu.Unlock()

	c.server.Info("Finished forcibly removing channel %d. Reason: %s", channelId, reason)
}

// sendConnectionClose sends a Connection.Close method to the client.
// It does NOT close the underlying net.Conn immediately.
// The server should then expect a Connection.CloseOk from the client.
func (c *connection) sendConnectionClose(replyCode uint16, replyText string, offendingClassId uint16, offendingMethodId uint16) error {
	c.server.Warn("Sending Connection.Close: code=%d, text='%s', class=%d, method=%d",
		replyCode, replyText, offendingClassId, offendingMethodId)

	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
	binary.Write(payload, binary.BigEndian, uint16(MethodConnectionClose))
	binary.Write(payload, binary.BigEndian, replyCode)
	writeShortString(payload, replyText)
	binary.Write(payload, binary.BigEndian, offendingClassId)
	binary.Write(payload, binary.BigEndian, offendingMethodId)

	frame := &frame{
		Type:    FrameMethod,
		Channel: 0, // Connection.Close is always on channel 0
		Payload: payload.Bytes(),
	}

	if err := c.writeFrame(frame); err != nil {
		c.server.Err("Error sending Connection.Close frame: %v. Dropping connection.", err)
		// If we can't send Connection.Close, the connection is effectively dead.
		return fmt.Errorf("failed to send Connection.Close: %w", err)
	}

	// Signal that Connection.Close was sent. The main loop should handle waiting for CloseOk or timeout.
	return errConnectionCloseSentByServer
}

func (c *connection) sendBasicReturn(channelId uint16, replyCode uint16, replyText string, exchange string, routingKey string) error {
	c.server.Info("Sending basic.return on channel %d: code=%d, text='%s', exchange='%s', routingKey='%s'",
		channelId, replyCode, replyText, exchange, routingKey)

	payload := &bytes.Buffer{}
	if err := binary.Write(payload, binary.BigEndian, uint16(ClassBasic)); err != nil {
		return fmt.Errorf("writing class id for basic.return: %w", err)
	}
	if err := binary.Write(payload, binary.BigEndian, uint16(MethodBasicReturn)); err != nil {
		return fmt.Errorf("writing method id for basic.return: %w", err)
	}

	// Write reply-code
	if err := binary.Write(payload, binary.BigEndian, replyCode); err != nil {
		return fmt.Errorf("writing reply code for basic.return: %w", err)
	}

	// Write reply-text
	writeShortString(payload, replyText)

	// Write exchange
	writeShortString(payload, exchange)

	// Write routing-key
	writeShortString(payload, routingKey)

	return c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	})
}

// Helper method to clean up exchange bindings
func (c *connection) cleanupExchangeBindings(queueName string, bindings []string) {
	if len(bindings) == 0 {
		return
	}

	// Extract unique exchange names from bindings
	exchangeNames := make(map[string]bool)
	for _, binding := range bindings {
		parts := strings.SplitN(binding, ":", 2)
		if len(parts) >= 1 {
			exchangeNames[parts[0]] = true
		}
	}

	vhost := c.vhost
	for exchangeName := range exchangeNames {
		vhost.mu.RLock()
		exchange, exists := vhost.exchanges[exchangeName]
		vhost.mu.RUnlock()

		if !exists {
			continue
		}

		exchange.mu.Lock()
		modified := false
		for routingKey, boundQueues := range exchange.Bindings {
			newQueues := make([]string, 0, len(boundQueues))
			for _, boundQueue := range boundQueues {
				if boundQueue != queueName {
					newQueues = append(newQueues, boundQueue)
				} else {
					modified = true
				}
			}
			if len(newQueues) > 0 {
				exchange.Bindings[routingKey] = newQueues
			} else if len(boundQueues) > 0 {
				delete(exchange.Bindings, routingKey)
			}
		}
		exchange.mu.Unlock()

		if modified {
			c.server.Debug("Removed bindings for queue '%s' from exchange '%s'", queueName, exchangeName)
		}
	}
}

// getChannel safely retrieves a channel and checks if it's closing
// Returns (channel, exists, isClosing)
func (c *connection) getChannel(channelId uint16) (*channel, bool, bool) {
	c.mu.RLock()
	ch, exists := c.channels[channelId]
	c.mu.RUnlock()

	if !exists || ch == nil {
		return nil, false, false
	}

	// Check if closing while we have the channel reference
	ch.mu.Lock()
	isClosing := ch.closingByServer
	ch.mu.Unlock()

	return ch, true, isClosing
}
