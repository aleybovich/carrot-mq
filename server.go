package main

import (
	"bufio"
	"bytes"
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
)

const VERSION = "0.0.1"

// How log to wait until cleanup if channel Close Ok not received from client
// TODO: make server config value
const channelCloseOkTimeout = 100 * time.Millisecond

const (
	suggestedHeartbeatInterval = 60 // Suggested heartbeat interval for AMQP 0-9-1
	suggestedFrameMaxSize      = 131072
)

const failedAuthThrottle = 1 * time.Second // Throttle failed auth attempts to prevent abuse

// Flag to determine if we're logging to a terminal (with colors) or a file
var isTerminal bool

func init() {
	// Check if stdout is a terminal
	fileInfo, _ := os.Stdout.Stat()
	isTerminal = (fileInfo.Mode() & os.ModeCharDevice) != 0
}

type Frame struct {
	Type     byte
	Channel  uint16
	Payload  []byte
	FrameEnd byte
}

type Method struct {
	ClassId  uint16
	MethodId uint16
	Args     interface{}
}

type Properties struct {
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

type Message struct {
	Exchange    string
	RoutingKey  string
	Mandatory   bool
	Immediate   bool
	Properties  Properties
	Body        []byte
	Redelivered bool
}

func (m *Message) DeepCopy() *Message {
	if m == nil {
		return nil
	}

	// Deep copy properties
	propsCopy := Properties{
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

	return &Message{
		Exchange:    m.Exchange,
		RoutingKey:  m.RoutingKey,
		Mandatory:   m.Mandatory,
		Immediate:   m.Immediate,
		Properties:  propsCopy,
		Body:        bodyCopy,
		Redelivered: m.Redelivered,
	}
}

type Queue struct {
	Name       string
	Messages   []Message
	Bindings   map[string]bool
	Consumers  map[string]*Consumer
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	mu         sync.RWMutex

	deleting atomic.Bool
}

type QueueConfig struct {
	Name       string
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	Bindings   map[string]bool // Exchange bindings for the queue
}

type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	Bindings   map[string][]string
	mu         sync.RWMutex

	deleted atomic.Bool
}

type ExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
}

type Channel struct {
	id                            uint16
	conn                          *Connection
	consumers                     map[string]string // consumerTag -> queueName
	pendingMessages               []Message         // For assembling messages from Publish -> Header -> Body
	deliveryTag                   uint64
	mu                            sync.Mutex
	closingByServer               bool        // True if server sent Channel.Close and is waiting for CloseOk
	closeOkTimer                  *time.Timer // Timer for waiting for Channel.CloseOk
	serverInitiatedCloseReplyCode uint16      // Store the reply code for logging if CloseOk times out
	serverInitiatedCloseReplyText string      // Store the reply text for logging if CloseOk times out

	unackedMessages map[uint64]*UnackedMessage // deliveryTag -> UnackedMessage

	// Publisher confirms fields
	confirmMode      bool            // Whether confirm mode is enabled
	nextPublishSeqNo uint64          // Next sequence number for published messages
	pendingConfirms  map[uint64]bool // Map of unconfirmed delivery tags
}

type UnackedMessage struct {
	Message     Message
	ConsumerTag string
	QueueName   string
	DeliveryTag uint64
	Delivered   time.Time
}

type Connection struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	channels map[uint16]*Channel
	server   *Server
	vhost    *VHost
	mu       sync.RWMutex
	writeMu  sync.Mutex

	// negotiated values
	channelMax        uint16
	frameMax          uint32
	heartbeatInterval uint16

	username string // Store authenticated username
}

type Consumer struct {
	Tag       string
	ChannelId uint16
	NoAck     bool
	Queue     *Queue
	stopCh    chan struct{} // Signal to stop this consumer
}

type AuthMode int

const (
	AuthModeNone AuthMode = iota
	AuthModePlain
)

type Credentials struct {
	Username string
	Password string
}

type Server struct {
	listener       net.Listener
	vhosts         map[string]*VHost
	internalLogger *log.Logger // Internal logger for formatting output
	customLogger   Logger      // External logger interface, if provided
	mu             sync.RWMutex

	// Authentication fields
	authMode    AuthMode
	credentials map[string]string // username -> password

	// track active connections
	connections   map[*Connection]struct{}
	connectionsMu sync.RWMutex
}

type AmqpDecimal struct {
	Scale uint8
	Value int32
}

// ServerOption defines functional options for configuring the AMQP server
type ServerOption func(*Server)

// WithLogger sets a custom logger that implements the Logger interface
func WithLogger(logger Logger) ServerOption {
	return func(s *Server) {
		s.customLogger = logger
	}
}

func WithAuth(credentials map[string]string) ServerOption {
	return func(s *Server) {
		if len(credentials) > 0 {
			s.authMode = AuthModePlain
			s.credentials = make(map[string]string)
			for user, pass := range credentials {
				s.credentials[user] = pass
			}
			s.Info("Authentication enabled with %d users", len(credentials))
		}
	}
}

func WithVHosts(vhosts []VHostConfig) ServerOption {
	return func(s *Server) {
		for _, vhostConfig := range vhosts {
			// Create vhost if it doesn't exist (skip default "/" if already created)
			if vhostConfig.name != "/" {
				if err := s.AddVHost(vhostConfig.name); err != nil {
					s.Warn("Failed to create vhost '%s': %v", vhostConfig.name, err)
					continue
				}
			}

			// Get the vhost
			vhost, err := s.GetVHost(vhostConfig.name)
			if err != nil {
				s.Warn("Failed to get vhost '%s': %v", vhostConfig.name, err)
				continue
			}

			// Add exchanges to vhost
			vhost.mu.Lock()
			for _, exchConfig := range vhostConfig.exchanges {
				// Skip if exchange already exists (like default "" exchange)
				if _, exists := vhost.exchanges[exchConfig.Name]; exists {
					s.Info("Exchange '%s' already exists in vhost '%s', skipping", exchConfig.Name, vhostConfig.name)
					continue
				}

				vhost.exchanges[exchConfig.Name] = &Exchange{
					Name:       exchConfig.Name,
					Type:       exchConfig.Type,
					Durable:    exchConfig.Durable,
					AutoDelete: exchConfig.AutoDelete,
					Internal:   exchConfig.Internal,
					Bindings:   make(map[string][]string),
				}
				s.Info("Created exchange '%s' (type: %s) in vhost '%s'", exchConfig.Name, exchConfig.Type, vhostConfig.name)
			}

			// Add queues to vhost
			for _, queueConfig := range vhostConfig.queues {
				// Skip if queue already exists
				if _, exists := vhost.queues[queueConfig.Name]; exists {
					s.Info("Queue '%s' already exists in vhost '%s', skipping", queueConfig.Name, vhostConfig.name)
					continue
				}

				// Create the queue
				vhost.queues[queueConfig.Name] = &Queue{
					Name:       queueConfig.Name,
					Messages:   []Message{},
					Bindings:   make(map[string]bool),
					Consumers:  make(map[string]*Consumer),
					Durable:    queueConfig.Durable,
					Exclusive:  queueConfig.Exclusive,
					AutoDelete: queueConfig.AutoDelete,
				}
				s.Info("Created queue '%s' in vhost '%s' (durable: %v, exclusive: %v)",
					queueConfig.Name, vhostConfig.name, queueConfig.Durable, queueConfig.Exclusive)

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
						queueConfig.Name, exchangeName, routingKey, vhostConfig.name)
				}
			}
			vhost.mu.Unlock()
		}
	}
}

// Logger interface definition
type Logger interface {
	Fatal(format string, a ...any)
	Err(format string, a ...any)
	Warn(format string, a ...any)
	Info(format string, a ...any)
	Debug(format string, a ...any)
}

// Get caller function name for logging
func getCallerName() string {
	pc, _, _, _ := runtime.Caller(2) // Use depth 2 to get the actual caller, not the logging function
	caller := runtime.FuncForPC(pc).Name()
	parts := strings.Split(caller, ".")
	return parts[len(parts)-1]
}

// Fatal logs a message with Fatal level and exits with code 1
func (s *Server) Fatal(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Fatal(format, args...)
		return
	}

	funcName := getCallerName()

	if isTerminal {
		prefix := fmt.Sprintf("%s[FATAL]%s %s%s%s: ", colorBoldRed, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[FATAL] %s: "+format, append([]interface{}{funcName}, args...)...)
	}

	os.Exit(1) // Exit with error code 1
}

// Err logs a message with Error level
func (s *Server) Err(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Err(format, args...)
		return
	}

	funcName := getCallerName()

	if isTerminal {
		prefix := fmt.Sprintf("%s[ERROR]%s %s%s%s: ", colorBoldRed, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[ERROR] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

// Warn logs a message with Warning level
func (s *Server) Warn(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Warn(format, args...)
		return
	}

	funcName := getCallerName()

	if isTerminal {
		prefix := fmt.Sprintf("%s[WARN]%s %s%s%s: ", colorYellow, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[WARN] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

// Info logs a message with Info level
func (s *Server) Info(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Info(format, args...)
		return
	}

	funcName := getCallerName()

	if isTerminal {
		prefix := fmt.Sprintf("%s[INFO]%s %s%s%s: ", colorGreen, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[INFO] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

// Debug logs a message with Debug level
func (s *Server) Debug(format string, args ...interface{}) {
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

	if isTerminal {
		prefix := fmt.Sprintf("%s[DEBUG]%s %s%s%s: ", colorPurple, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[DEBUG] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

func NewServer(opts ...ServerOption) *Server {
	var logPrefix string
	if isTerminal {
		logPrefix = fmt.Sprintf("%s[AMQP]%s ", colorBlue, colorReset)
	} else {
		logPrefix = "[AMQP] "
	}

	s := &Server{
		vhosts:         make(map[string]*VHost),
		internalLogger: log.New(os.Stdout, logPrefix, log.LstdFlags|log.Lmicroseconds),
		connections:    make(map[*Connection]struct{}),
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

	s.Info("AMQP server created with default direct exchange")
	return s
}

func (s *Server) Start(addr string) error {
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

// Add new connection to the active server connections map
func (s *Server) addConnection(c *Connection) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	s.connections[c] = struct{}{}
	s.Info("Connection %s added to active list. Total: %d", c.conn.RemoteAddr(), len(s.connections))
}

// Remove a connection from the active server connections map.
func (s *Server) removeConnection(c *Connection) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	if _, ok := s.connections[c]; ok {
		delete(s.connections, c)
		s.Info("Connection %s removed from active list. Total remaining: %d", c.conn.RemoteAddr(), len(s.connections))
	} else {
		s.Warn("Attempted to remove connection %s from active list, but it was not found.", c.conn.RemoteAddr())
	}
}

func (s *Server) handleConnection(conn net.Conn) {

	s.Info("Handling connection from %s", conn.RemoteAddr())

	c := &Connection{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
		channels: make(map[uint16]*Channel),
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
			handlerError = c.handleMethod(frame) // MODIFIED: Assign returned error
		case FrameHeader:
			// handleHeader might also need to return an error if table parsing fails
			// For now, assuming it logs and continues or sets empty properties.
			// To be fully compliant, it should also return error to be handled here.
			// Let's modify handleHeader to return error for this exercise.
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

			// Optionally, send a heartbeat frame back. AMQP 0-9-1 spec says server SHOULD respond to peer's heartbeat.
			// For simplicity, we are just logging. If heartbeat timeout is implemented, this is where it would be reset.
			// Example: if err := c.writeFrame(&Frame{Type: FrameHeartbeat, Channel: 0, Payload: []byte{}}); err != nil { ... }
		default:
			s.Warn("Received unhandled frame type %d from %s on channel %d", frame.Type, conn.RemoteAddr(), frame.Channel)
			// AMQP code 505 (UNEXPECTED_FRAME)
			handlerError = c.sendConnectionClose(505, fmt.Sprintf("unhandled frame type %d", frame.Type), 0, 0)
		}

		// MODIFIED: Check handlerError
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

func (c *Connection) readFrame() (*Frame, error) {
	header := make([]byte, 7)
	_, err := io.ReadFull(c.reader, header)
	if err != nil {
		return nil, fmt.Errorf("error reading frame header: %w", err)
	}

	frame := &Frame{
		Type:    header[0],
		Channel: binary.BigEndian.Uint16(header[1:3]),
	}

	size := binary.BigEndian.Uint32(header[3:7])
	if c.frameMax > 0 && size > uint32(c.frameMax) {
		// Send connection.close with frame-error
		c.sendConnectionClose(505, "frame too large", 0, 0)
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
		c.sendConnectionClose(501, "frame-end octet missing", 0, 0)
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
func (c *Connection) writeFrameInternal(frameType byte, channelID uint16, payload []byte) error {
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
func (c *Connection) writeFrame(frame *Frame) error {
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

func (c *Connection) sendConnectionStart() error { // MODIFIED: to return error
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

	err := c.writeFrame(&Frame{
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

func (c *Connection) handleMethod(frame *Frame) error { // Return type error
	reader := bytes.NewReader(frame.Payload)
	var classId, methodId uint16
	binary.Read(reader, binary.BigEndian, &classId)
	binary.Read(reader, binary.BigEndian, &methodId)

	if frame.Channel == 0 && classId != ClassConnection {
		c.server.Err("Received non-Connection class %d on channel 0", classId)
		return c.sendConnectionClose(503, "command invalid - channel 0 is for Connection class only", classId, methodId)
	}

	methodName := getFullMethodName(classId, methodId)

	var err error
	switch classId {
	case ClassConfirm:
		err = c.handleConfirmMethod(methodId, reader, frame.Channel)
	case ClassConnection:
		err = c.handleConnectionMethod(methodId, reader) // Assumes handleConnectionMethod also returns error
	case ClassChannel:
		err = c.handleChannelMethod(methodId, reader, frame.Channel)
	case ClassExchange:
		err = c.handleExchangeMethod(methodId, reader, frame.Channel)
	case ClassQueue:
		err = c.handleQueueMethod(methodId, reader, frame.Channel)
	case ClassBasic:
		err = c.handleBasicMethod(methodId, reader, frame.Channel)
	default:
		errMsgBase := "received method for unknown class ID"
		c.server.Warn("%s: %d on channel %d", errMsgBase, classId, frame.Channel)
		// AMQP: Connection.Close(503, "command invalid - unknown class", classId, methodId)
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

func (c *Connection) handleConfirmMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassConfirm, methodId)

	ch, channelExists, isClosing := c.getChannel(channelId)

	if !channelExists && channelId != 0 {
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for confirm operation", channelId)
		c.server.Err("Confirm method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		return c.sendConnectionClose(503, replyText, uint16(ClassConfirm), methodId)
	}

	if ch == nil && channelId != 0 {
		c.server.Err("Internal error: channel %d object not found for confirm method %s", channelId, methodName)
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR - channel state inconsistency", uint16(ClassConfirm), methodId)
	}

	if channelId == 0 {
		replyText := "COMMAND_INVALID - confirm methods cannot be used on channel 0"
		c.server.Err("Confirm method %s on channel 0. Sending Connection.Close.", methodName)
		return c.sendConnectionClose(503, replyText, uint16(ClassConfirm), methodId)
	}

	if isClosing {
		c.server.Debug("Ignoring confirm method %s on channel %d that is being closed", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodConfirmSelect:
		c.server.Info("Processing confirm.%s for channel %d", methodName, channelId)

		// Read nowait bit
		bits, err := reader.ReadByte()
		if err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed confirm.select (bits)", uint16(ClassConfirm), MethodConfirmSelect)
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

			if err := c.writeFrame(&Frame{
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

	default:
		replyText := fmt.Sprintf("unknown or not implemented confirm method id %d", methodId)
		c.server.Err("Unhandled confirm method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		return c.sendChannelClose(channelId, 540, replyText, uint16(ClassConfirm), methodId)
	}
}

func (c *Connection) handleConnectionMethod(methodId uint16, reader *bytes.Reader) error { // MODIFIED: Return type error
	methodName := getMethodName(ClassConnection, methodId)

	switch methodId {
	case MethodConnectionStartOk:
		c.server.Info("Processing connection.%s", methodName)
		// Arguments: client-properties (table), mechanism (shortstr), response (longstr), locale (shortstr)
		var clientProperties map[string]interface{}
		clientProperties, err := readTable(reader) // Use error-returning readTable
		if err != nil {
			c.server.Err("Error reading client_properties in connection.start-ok: %v", err)
			// AMQP code 502 (SYNTAX_ERROR) for malformed table
			return c.sendConnectionClose(502, "malformed client_properties table", uint16(ClassConnection), MethodConnectionStartOk)
		}
		_ = clientProperties // Use clientProperties if needed, e.g., logging or capabilities check

		mechanism, err := readShortString(reader) // Auth mechanism
		if err != nil {
			c.server.Err("Error reading mechanism in connection.start-ok: %v", err)
			return c.sendConnectionClose(502, "SYNTAX_ERROR - malformed mechanism string", uint16(ClassConnection), MethodConnectionStartOk)
		}

		response, err := readLongString(reader) // Auth credentials
		if err != nil {
			c.server.Err("Error reading response in connection.start-ok: %v", err)
			return c.sendConnectionClose(502, "SYNTAX_ERROR - malformed response string", uint16(ClassConnection), MethodConnectionStartOk)
		}

		_, err = readShortString(reader) // locale
		if err != nil {
			c.server.Err("Error reading locale in connection.start-ok: %v", err)
			return c.sendConnectionClose(502, "SYNTAX_ERROR - malformed locale string", uint16(ClassConnection), MethodConnectionStartOk)
		}

		// Check for remaining bytes, indicates malformed arguments
		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of connection.start-ok payload.")
		}

		// Handle authentication based on server auth mode
		if c.server.authMode == AuthModePlain {
			if mechanism != "PLAIN" {
				c.server.Err("Unsupported authentication mechanism: %s", mechanism)
				// During negotiation phase - pause and close socket per spec 4.10.2
				time.Sleep(failedAuthThrottle)
				c.conn.Close()
				return fmt.Errorf("unsupported mechanism '%s'", mechanism)
			}

			// Parse PLAIN authentication response
			if len(response) < 2 {
				c.server.Err("Invalid PLAIN authentication response length")
				// During negotiation phase - pause and close socket per spec 4.10.2
				time.Sleep(failedAuthThrottle)
				c.conn.Close()
				return fmt.Errorf("invalid authentication response")
			}

			parts := bytes.Split([]byte(response), []byte{0})
			if len(parts) != 3 {
				c.server.Err("Invalid PLAIN authentication response format")
				// During negotiation phase - pause and close socket per spec 4.10.2
				time.Sleep(failedAuthThrottle)
				c.conn.Close()
				return fmt.Errorf("invalid authentication response format")
			}

			username := string(parts[1])
			password := string(parts[2])

			// Validate credentials
			if storedPass, ok := c.server.credentials[username]; !ok || storedPass != password {
				c.server.Err("Authentication failed for user: %s from %s", username, c.conn.RemoteAddr())

				// Per spec 4.10.2: pause before closing to prevent DoS attacks
				// Use a goroutine to avoid blocking the handler

				time.Sleep(failedAuthThrottle)
				c.conn.Close()

				// Return error without sending any protocol frames
				return fmt.Errorf("authentication failed for user %s", username)
			}

			c.username = username
			c.server.Info("Authentication successful for user: %s", username)
		} else {
			// No auth mode - accept any mechanism/credentials
			c.server.Info("No authentication configured, accepting connection")
		}

		if errTune := c.sendConnectionTune(); errTune != nil {
			// Error already logged by sendConnectionTune if writeFrame failed.
			// This is a server-side issue or I/O error.
			return errTune // Propagate error from sendConnectionTune
		}
		return nil // Successfully processed

	case MethodConnectionTuneOk:
		c.server.Info("Processing connection.%s", methodName)
		if err := binary.Read(reader, binary.BigEndian, &c.channelMax); err != nil {
			return c.sendConnectionClose(502, "malformed connection.tune-ok (channel-max)", uint16(ClassConnection), MethodConnectionTuneOk)
		}
		if err := binary.Read(reader, binary.BigEndian, &c.frameMax); err != nil {
			return c.sendConnectionClose(502, "malformed connection.tune-ok (frame-max)", uint16(ClassConnection), MethodConnectionTuneOk)
		}
		if err := binary.Read(reader, binary.BigEndian, &c.heartbeatInterval); err != nil {
			return c.sendConnectionClose(502, "malformed connection.tune-ok (heartbeat)", uint16(ClassConnection), MethodConnectionTuneOk)
		}
		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of connection.tune-ok payload.")
		}

		c.server.Info("Connection parameters negotiated: channelMax=%d, frameMax=%d, heartbeat=%d",
			c.channelMax, c.frameMax, c.heartbeatInterval)
		return nil

	case MethodConnectionOpen:
		vhostName, err := readShortString(reader)
		if err != nil {
			c.server.Err("Error reading vhost in connection.open: %v", err)
			return c.sendConnectionClose(502, "SYNTAX_ERROR - malformed vhost string", uint16(ClassConnection), MethodConnectionStartOk)
		}

		_, _ = readShortString(reader) // capabilities

		_, errReadByte := reader.ReadByte() // insist bit
		if errReadByte != nil {
			return c.sendConnectionClose(502, "malformed connection.open (insist bit)", uint16(ClassConnection), MethodConnectionOpen)
		}
		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of connection.open payload.")
		}
		c.server.Info("Processing connection.%s for vhost: '%s'", methodName, vhostName)

		// Validate vhost exists
		if vhost, err := c.server.GetVHost(vhostName); err != nil {
			c.server.Err("Connection.Open: vhost '%s' does not exist", vhostName)
			return c.sendConnectionClose(403, fmt.Sprintf("ACCESS_REFUSED - vhost '%s' does not exist", vhostName), uint16(ClassConnection), MethodConnectionOpen)
		} else {
			// Store vhost on connection
			c.vhost = vhost
		}

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
		binary.Write(payload, binary.BigEndian, uint16(MethodConnectionOpenOk))
		// AMQP 0-9-1 Connection.OpenOk has one field: known-hosts (shortstr), which "MUST be zero length".
		writeShortString(payload, "") // known-hosts

		err = c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: 0,
			Payload: payload.Bytes(),
		})
		if err != nil {
			c.server.Err("Error sending connection.open-ok: %v", err)
			return fmt.Errorf("sending connection.open-ok frame: %w", err)
		}
		c.server.Info("Sent connection.open-ok")

	case MethodConnectionClose:
		c.server.Info("Processing connection.%s (client initiated close)", methodName)
		var replyCode uint16
		binary.Read(reader, binary.BigEndian, &replyCode)
		replyText, err := readShortString(reader)
		if err != nil {
			c.server.Err("Error reading replyText in connection.close: %v", err)
			// Server is closing anyway, but good to note if client sent malformed close
			// For simplicity, we won't send another error, just proceed with CloseOk
		}

		var classIdField, methodIdField uint16
		binary.Read(reader, binary.BigEndian, &classIdField)
		binary.Read(reader, binary.BigEndian, &methodIdField)

		if reader.Len() > 0 {
			c.server.Warn("Extra data in client's connection.close method.")
		}

		c.server.Info("Client requests connection close: replyCode=%d, replyText='%s', classId=%d, methodId=%d",
			replyCode, replyText, classIdField, methodIdField)

		payloadCloseOk := &bytes.Buffer{}
		binary.Write(payloadCloseOk, binary.BigEndian, uint16(ClassConnection))
		binary.Write(payloadCloseOk, binary.BigEndian, uint16(MethodConnectionCloseOk))

		// Attempt to send CloseOk, but proceed to close connection regardless of write error
		if errWrite := c.writeFrame(&Frame{Type: FrameMethod, Channel: 0, Payload: payloadCloseOk.Bytes()}); errWrite != nil {
			c.server.Err("Error sending connection.close-ok in response to client's close: %v", errWrite)
		} else {
			c.server.Info("Sent connection.close-ok")
		}
		c.conn.Close()
		return errConnectionClosedGracefully

	case MethodConnectionCloseOk: // Server receives this if it initiated a Connection.Close
		c.server.Info("Received connection.close-ok. Client acknowledged connection closure.")
		if reader.Len() > 0 {
			c.server.Warn("Extra data in client's connection.close-ok method.")
		}
		c.conn.Close()
		return errConnectionClosedGracefully

	default:
		errMsg := fmt.Sprintf("unhandled connection method ID: %d", methodId)
		c.server.Err(errMsg)
		// According to AMQP, for an unknown method on channel 0 (connection),
		// the server should respond with Connection.Close(503, "command invalid", classId, methodId).
		// For simplicity here, we'll return an error that leads to generic connection closure.
		return errors.New(errMsg)
	}
	return nil
}

func (c *Connection) sendConnectionTune() error {
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
	binary.Write(payload, binary.BigEndian, uint16(MethodConnectionTune))

	// channelMax: 0 means no specific limit from server
	binary.Write(payload, binary.BigEndian, uint16(0))

	// frameMax: suggested max frame size (including header and end byte)
	binary.Write(payload, binary.BigEndian, uint32(suggestedFrameMaxSize))

	// heartbeat: suggested heartbeat interval in seconds
	binary.Write(payload, binary.BigEndian, uint16(suggestedHeartbeatInterval))

	if err := c.writeFrame(&Frame{
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

func (c *Connection) handleChannelMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassChannel, methodId)

	if methodId == MethodChannelOpen {
		c.server.Info("Processing channel.%s for channel %d", methodName, channelId)
		// AMQP 0-9-1: reserved-1 (shortstr), "out-of-band, MUST be zero length string"
		// Read and discard "out-of-band" argument.
		_, err := readShortString(reader)
		if err != nil {
			c.server.Err("Error reading out-of-band string for channel.open on channel %d: %v", channelId, err)
			return c.sendConnectionClose(502, "SYNTAX_ERROR - malformed out-of-band string in channel.open", uint16(ClassChannel), MethodChannelOpen)
		}

		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of channel.open payload for channel %d.", channelId)
		}

		// SPECIAL CASE: Need write lock for channel creation
		c.mu.Lock()
		if _, alreadyOpen := c.channels[channelId]; alreadyOpen {
			c.mu.Unlock()
			errMsg := fmt.Sprintf("channel %d already open", channelId)
			c.server.Err("Channel.Open error: %s", errMsg)
			// AMQP code 504 (CHANNEL_ERROR) - Channel ID already in use.
			// Server should send Channel.Close, not Connection.Close here.
			return c.sendChannelClose(channelId, 504, "CHANNEL_ERROR - channel ID already in use", uint16(ClassChannel), MethodChannelOpen)
		}

		newCh := &Channel{
			id:               channelId,
			conn:             c,
			consumers:        make(map[string]string),
			pendingMessages:  make([]Message, 0),
			unackedMessages:  make(map[uint64]*UnackedMessage),
			confirmMode:      false,
			nextPublishSeqNo: 1,
			pendingConfirms:  make(map[uint64]bool),
		}
		c.channels[channelId] = newCh
		c.mu.Unlock()

		// Send Channel.OpenOk
		payloadOpenOk := &bytes.Buffer{}

		if err := binary.Write(payloadOpenOk, binary.BigEndian, uint16(ClassChannel)); err != nil {
			c.server.Err("Internal error serializing ClassChannel for Channel.OpenOk: %v", err)
			c.forceRemoveChannel(channelId, "internal error sending channel.open-ok")
			return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
		}
		if err := binary.Write(payloadOpenOk, binary.BigEndian, uint16(MethodChannelOpenOk)); err != nil {
			c.server.Err("Internal error serializing MethodChannelOpenOk for Channel.OpenOk: %v", err)
			c.forceRemoveChannel(channelId, "internal error sending channel.open-ok")
			return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
		}
		// AMQP 0-9-1 Channel.OpenOk has one field: channel-id (longstr), which is reserved and "MUST be empty".
		// An empty longstr is represented by its length (uint32) being 0.
		if err := binary.Write(payloadOpenOk, binary.BigEndian, uint32(0)); err != nil {
			c.server.Err("Internal error serializing empty longstr for Channel.OpenOk: %v", err)
			c.forceRemoveChannel(channelId, "internal error sending channel.open-ok")
			return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
		}

		if err := c.writeFrame(&Frame{Type: FrameMethod, Channel: channelId, Payload: payloadOpenOk.Bytes()}); err != nil {
			c.server.Err("Error sending channel.open-ok for channel %d: %v", channelId, err)
			c.forceRemoveChannel(channelId, "failed to send channel.open-ok")
			return err
		}
		c.server.Info("Sent channel.open-ok for channel %d", channelId)
		return nil
	}

	// For all other methods, use getChannel
	ch, exists, isClosing := c.getChannel(channelId)
	if !exists {
		errMsg := fmt.Sprintf("received method %s for non-existent channel %d", methodName, channelId)
		c.server.Err(errMsg)
		// If client sends a command on a channel server doesn't know (or is broken), it's a connection error.
		// AMQP 504 CHANNEL_ERROR is appropriate.
		return c.sendConnectionClose(504, "CHANNEL_ERROR - "+errMsg, uint16(ClassChannel), methodId)
	}

	// Check if channel is closing
	if isClosing && methodId != MethodChannelCloseOk {
		c.server.Warn("Received method %s on channel %d that is already being closed by server. Ignoring.", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodChannelClose:
		c.server.Info("Processing channel.%s for channel %d (client initiated)", methodName, channelId)
		var replyCode uint16
		if err := binary.Read(reader, binary.BigEndian, &replyCode); err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed channel.close (reply-code)", uint16(ClassChannel), MethodChannelClose)
		}
		replyText, err := readShortString(reader)
		if err != nil {
			c.server.Err("Error reading replyText for channel.close on channel %d: %v", channelId, err)
		}

		var classIdField, methodIdField uint16
		if err := binary.Read(reader, binary.BigEndian, &classIdField); err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed channel.close (class-id)", uint16(ClassChannel), MethodChannelClose)
		}
		if err := binary.Read(reader, binary.BigEndian, &methodIdField); err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed channel.close (method-id)", uint16(ClassChannel), MethodChannelClose)
		}

		if reader.Len() > 0 {
			c.server.Warn("Extra data in client's channel.close payload for channel %d.", channelId)
		}

		c.server.Info("Client requests channel close for channel %d: replyCode=%d, replyText='%s', classId=%d, methodId=%d",
			channelId, replyCode, replyText, classIdField, methodIdField)

		// Clean up and send CloseOk
		c.forceRemoveChannel(channelId, "client initiated Channel.Close")

		payloadCloseOk := &bytes.Buffer{}
		if err := binary.Write(payloadCloseOk, binary.BigEndian, uint16(ClassChannel)); err != nil {
			c.server.Err("Internal error serializing ClassChannel for Channel.CloseOk: %v", err)
			return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
		}
		if err := binary.Write(payloadCloseOk, binary.BigEndian, uint16(MethodChannelCloseOk)); err != nil {
			c.server.Err("Internal error serializing MethodChannelCloseOk: %v", err)
			return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
		}

		if errWrite := c.writeFrame(&Frame{Type: FrameMethod, Channel: channelId, Payload: payloadCloseOk.Bytes()}); errWrite != nil {
			c.server.Err("Error sending channel.close-ok for client-initiated close on channel %d: %v", channelId, errWrite)
			return errWrite
		}
		c.server.Info("Sent channel.close-ok for channel %d (client initiated)", channelId)
		return nil

	case MethodChannelCloseOk:
		c.server.Info("Received channel.close-ok for channel %d.", channelId)
		if reader.Len() > 0 {
			c.server.Warn("Extra data in channel.close-ok payload for channel %d.", channelId)
		}

		// Need to access timer, so lock the channel
		ch.mu.Lock()
		timerStopped := false
		if ch.closeOkTimer != nil {
			timerStopped = ch.closeOkTimer.Stop()
			ch.closeOkTimer = nil
		}
		wasServerInitiated := ch.closingByServer
		ch.mu.Unlock()

		if wasServerInitiated {
			if timerStopped {
				c.server.Info("Client acknowledged server-initiated close for channel %d within timeout. Finalizing.", channelId)
			} else {
				c.server.Info("Client acknowledged server-initiated close for channel %d (timer might have already fired). Finalizing.", channelId)
			}
		} else {
			c.server.Warn("Received unsolicited or late channel.close-ok for channel %d. Finalizing.", channelId)
		}

		c.forceRemoveChannel(channelId, "received Channel.CloseOk")
		return nil

	default:
		replyText := fmt.Sprintf("channel method %s (id %d) not implemented or invalid in this state", methodName, methodId)
		c.server.Err("Unhandled channel method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		return c.sendChannelClose(channelId, 540, replyText, uint16(ClassChannel), methodId)
	}
}

func (c *Connection) handleExchangeMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassExchange, methodId)

	_, channelExists, isClosing := c.getChannel(channelId)

	if !channelExists && channelId != 0 { // Channel 0 is special and doesn't need this check here
		// This is a critical error: operation on a non-existent channel.
		// AMQP spec is somewhat open, could be Channel.Close or Connection.Close.
		// Given the channel context is invalid, a Connection.Close is safer.
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for exchange operation", channelId)
		c.server.Err("Exchange method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		// AMQP code 503 (COMMAND_INVALID) or 504 (CHANNEL_ERROR)
		return c.sendConnectionClose(503, replyText, uint16(ClassExchange), methodId)
	}

	if isClosing {
		c.server.Debug("Ignoring exchange method %s on channel %d that is being closed", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodExchangeDeclare:
		// AMQP 0-9-1: ticket (short, reserved), exchange (shortstr), type (shortstr), passive (bit),
		// durable (bit), auto-delete (bit), internal (bit), no-wait (bit), arguments (table)
		var ticket uint16
		if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
			return fmt.Errorf("reading ticket for exchange.declare: %w", err)
		}
		exchangeName, err := readShortString(reader)
		if err != nil {
			c.server.Err("Error reading exchangeName for exchange.declare: %v", err)
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed exchange.declare (exchangeName)", uint16(ClassExchange), MethodExchangeDeclare)
		}

		exchangeType, err := readShortString(reader)
		if err != nil {
			c.server.Err("Error reading exchangeType for exchange.declare: %v", err)
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed exchange.declare (exchangeType)", uint16(ClassExchange), MethodExchangeDeclare)
		}

		bits, errReadByte := reader.ReadByte()
		if errReadByte != nil {
			return c.sendChannelClose(channelId, 502, "malformed exchange.declare (bits)", uint16(ClassExchange), MethodExchangeDeclare)
		}

		args, errArgs := readTable(reader)
		if errArgs != nil {
			c.server.Err("Error reading arguments for exchange.declare ('%s'): %v", exchangeName, errArgs)
			// AMQP code 502 (SYNTAX_ERROR)
			return c.sendChannelClose(channelId, 502, "malformed arguments table for exchange.declare", uint16(ClassExchange), MethodExchangeDeclare)
		}
		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of exchange.declare payload.")
		}

		passive := (bits & 0x01) != 0
		durable := (bits & 0x02) != 0
		autoDelete := (bits & 0x04) != 0 // Server doesn't fully implement auto-delete logic for now
		internal := (bits & 0x08) != 0   // Server doesn't fully implement 'internal' exchange logic for now
		noWait := (bits & 0x10) != 0

		c.server.Info("Processing exchange.%s: name='%s', type='%s', passive=%v, durable=%v, autoDelete=%v, internal=%v, noWait=%v, args=%v on channel %d",
			methodName, exchangeName, exchangeType, passive, durable, autoDelete, internal, noWait, args, channelId)

		// Validate exchange type - essential for server operation. Headers exchange is not implemented for now
		validTypes := map[string]bool{"direct": true, "fanout": true, "topic": true, "headers": false}
		if _, isValidType := validTypes[exchangeType]; !isValidType {
			replyText := fmt.Sprintf("exchange type '%s' not implemented", exchangeType)
			c.server.Warn("Exchange.Declare: %s for exchange '%s'. Sending Channel.Close.", replyText, exchangeName)
			// AMQP code 540 (NOT_IMPLEMENTED)
			return c.sendChannelClose(channelId, 540, replyText, uint16(ClassExchange), MethodExchangeDeclare)
		}

		vhost := c.vhost
		vhost.mu.Lock()
		ex, exists := vhost.exchanges[exchangeName]

		if passive {
			// Passive declare: check if exchange exists and is compatible.
			if !exists {
				vhost.mu.Unlock()
				replyText := fmt.Sprintf("no exchange '%s' in vhost '/'", exchangeName) // Standard AMQP reply text format
				c.server.Info("Passive Exchange.Declare: Exchange '%s' not found. Sending Channel.Close.", exchangeName)
				// AMQP code 404 (NOT_FOUND)
				// This is an expected outcome for a client checking existence passively.
				return c.sendChannelClose(channelId, 404, replyText, uint16(ClassExchange), MethodExchangeDeclare)
			}
			// Passive and exists: Check compatibility (primarily type according to spec for passive).
			// AMQP spec: "If the exchange already exists and is of the same type... then it does nothing and replies with DeclareOk."
			// "If the exchange exists but is of a different type, the server closes the channel with a 406 (PRECONDITION_FAILED) status."
			if ex.Type != exchangeType {
				vhost.mu.Unlock()
				replyText := fmt.Sprintf("exchange '%s' of type '%s' already declared, cannot passively declare with type '%s'", ex.Name, ex.Type, exchangeType)
				c.server.Warn("Passive Exchange.Declare: Type mismatch for '%s'. Sending Channel.Close.", exchangeName)
				// AMQP code 406 (PRECONDITION_FAILED)
				return c.sendChannelClose(channelId, 406, replyText, uint16(ClassExchange), MethodExchangeDeclare)
			}
			// Could also check durable, autoDelete, internal, args for stricter compliance if needed,
			// though spec for passive focuses on type.
			c.server.Info("Exchange '%s' already exists and matches type (passive declare). Durable: %v", ex.Name, ex.Durable)
		} else { // Not passive: declare or re-declare.
			if exists {
				// Exchange exists, this is a re-declaration attempt.
				// AMQP spec: "If the exchange already exists, is of the same type, and the arguments are the same,
				// then it does nothing and replies with DeclareOk."
				// "If the exchange exists but with different properties (type, arguments, etc.), the server
				// closes the channel with a 406 (PRECONDITION_FAILED) status."
				c.server.Info("Exchange '%s' already exists. Current: type='%s', durable=%v. Requested: type='%s', durable=%v. Checking for conflict.",
					exchangeName, ex.Type, ex.Durable, exchangeType, durable)

				// Strict checks for re-declaration (as per AMQP spec for non-passive declare)
				// Note: Argument comparison (areTablesEqual) is not implemented here for brevity.
				if ex.Type != exchangeType || ex.Durable != durable || ex.AutoDelete != autoDelete || ex.Internal != internal /* || !areTablesEqual(ex.Arguments, args) */ {
					vhost.mu.Unlock()
					replyText := fmt.Sprintf("cannot redeclare exchange '%s' with different properties", exchangeName)
					c.server.Warn("Exchange.Declare: %s. Sending Channel.Close. Existing: type=%s, dur=%v. Req: type=%s, dur=%v",
						replyText, ex.Type, ex.Durable, exchangeType, durable)
					// AMQP code 406 (PRECONDITION_FAILED)
					return c.sendChannelClose(channelId, 406, replyText, uint16(ClassExchange), MethodExchangeDeclare)
				}
				c.server.Info("Exchange '%s' re-declared with matching properties.", exchangeName)
			} else { // Not passive and not exists: create it.
				vhost.exchanges[exchangeName] = &Exchange{
					Name:       exchangeName,
					Type:       exchangeType,
					Durable:    durable,
					AutoDelete: autoDelete,
					Internal:   internal,
					Bindings:   make(map[string][]string),
					// Arguments: args, // If you decide to store and use exchange arguments
				}
				c.server.Info("Created new exchange: '%s' type '%s', durable: %v", exchangeName, exchangeType, durable)
			}
		}
		vhost.mu.Unlock()

		if !noWait {
			// If noWait is false, server MUST reply with Exchange.DeclareOk.
			payloadOk := &bytes.Buffer{}
			binary.Write(payloadOk, binary.BigEndian, uint16(ClassExchange))
			binary.Write(payloadOk, binary.BigEndian, uint16(MethodExchangeDeclareOk))

			// Attempt to send DeclareOk
			writeErr := c.writeFrame(&Frame{
				Type:    FrameMethod,
				Channel: channelId,
				Payload: payloadOk.Bytes(),
			})
			if writeErr != nil {
				c.server.Err("Error sending exchange.declare-ok for exchange '%s': %v", exchangeName, writeErr)
				// This is a connection I/O error, not an AMQP protocol error from the client.
				// The original error from writeFrame should be returned to signal connection failure.
				return writeErr // Propagate the I/O error
			}
			c.server.Info("Sent exchange.declare-ok for exchange '%s'", exchangeName)
		} else {
			// If noWait is true, server MUST NOT reply with Exchange.DeclareOk.
			// If an error occurs (e.g., precondition failed), server closes the channel or connection.
			// That error handling is already done above before this noWait check.
			c.server.Info("Exchange.Declare with noWait=true processed for '%s'. No DeclareOk sent.", exchangeName)
		}
		return nil // Successfully processed Exchange.Declare

	case MethodExchangeDelete:
		return c.handleExchangeDelete(reader, channelId)

	default:
		replyText := fmt.Sprintf("unknown or not implemented exchange method id %d", methodId)
		c.server.Err("Unhandled exchange method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		// AMQP code 540 (NOT_IMPLEMENTED) or 503 (COMMAND_INVALID)
		return c.sendChannelClose(channelId, 540, replyText, uint16(ClassExchange), methodId)
	}
}

func (c *Connection) handleQueueMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassQueue, methodId)

	_, channelExists, isClosing := c.getChannel(channelId)
	if !channelExists && channelId != 0 {
		// This is a connection-level error as the channel context is invalid for the operation.
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for queue operation", channelId)
		c.server.Err("Queue method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		// AMQP code 503 (COMMAND_INVALID) or 504 (CHANNEL_ERROR) - 503 is general for bad command sequence.
		return c.sendConnectionClose(503, replyText, uint16(ClassQueue), methodId)
	}

	if isClosing {
		c.server.Debug("Ignoring queue method %s on channel %d that is being closed", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodQueueDeclare:
		return c.handleQueueDeclare(reader, channelId)

	case MethodQueueBind:
		return c.handleQueueBind(reader, channelId)

	case MethodQueueUnbind:
		return c.handleQueueUnbind(reader, channelId)

	case MethodQueuePurge:
		return c.handleQueuePurge(reader, channelId)

	case MethodQueueDelete:
		return c.handleQueueDelete(reader, channelId)

	default:
		replyText := fmt.Sprintf("unknown or not implemented queue method id %d", methodId)
		c.server.Err("Unhandled queue method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		// AMQP code 540 (NOT_IMPLEMENTED) or 503 (COMMAND_INVALID)
		return c.sendChannelClose(channelId, 540, replyText, uint16(ClassQueue), methodId)
	}
}

func (c *Connection) handleBasicMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassBasic, methodId)

	ch, channelExists, isClosing := c.getChannel(channelId)

	if !channelExists && channelId != 0 { // Channel 0 is special and should not receive Basic methods
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for basic operation", channelId)
		c.server.Err("Basic method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		// AMQP code 503 (COMMAND_INVALID) or 504 (CHANNEL_ERROR)
		return c.sendConnectionClose(503, replyText, uint16(ClassBasic), methodId)
	}
	// ch can be nil if channelId is 0, but basic methods are not for channel 0.
	// This case should be caught by the above, but as a safeguard:
	if ch == nil && channelId != 0 { // Should have been caught by channelExists check
		// This indicates an internal server inconsistency if channelId was non-zero but ch is nil.
		c.server.Err("Internal error: channel %d object not found for basic method %s, though channel map indicated existence.", channelId, methodName)
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR - channel state inconsistency", uint16(ClassBasic), methodId)
	}
	if channelId == 0 { // Basic methods are not valid on channel 0
		replyText := "COMMAND_INVALID - basic methods cannot be used on channel 0"
		c.server.Err("Basic method %s on channel 0. Sending Connection.Close.", methodName)
		return c.sendConnectionClose(503, replyText, uint16(ClassBasic), methodId)
	}

	if isClosing {
		c.server.Debug("Ignoring basic method %s on channel %d that is being closed", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodBasicGet:
		return c.handleBasicGet(reader, channelId)

	case MethodBasicRecover:
		return c.handleBasicRecover(reader, channelId)
	case MethodBasicPublish:
		var ticket uint16
		if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.publish (ticket)", uint16(ClassBasic), MethodBasicPublish)
		}
		exchangeName, err := readShortString(reader)
		if err != nil {
			c.server.Err("Error reading exchangeName for basic.publish: %v", err)
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.publish (exchangeName)", uint16(ClassBasic), MethodBasicPublish)
		}

		routingKey, err := readShortString(reader)
		if err != nil {
			c.server.Err("Error reading routingKey for basic.publish: %v", err)
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.publish (routingKey)", uint16(ClassBasic), MethodBasicPublish)
		}

		bits, errReadByte := reader.ReadByte()
		if errReadByte != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.publish (bits)", uint16(ClassBasic), MethodBasicPublish)
		}

		// Check for extra data after all arguments are read.
		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of basic.publish payload for exchange '%s', rkey '%s'.", exchangeName, routingKey)
		}

		mandatory := (bits & 0x01) != 0
		immediate := (bits & 0x02) != 0 // Immediate is deprecated in AMQP 0-9-1, server should reject

		if immediate {
			// AMQP 0-9-1 spec: "servers SHOULD reject messages published with the 'immediate' flag set."
			replyText := "The 'immediate' flag is deprecated and not supported by this server."
			c.server.Warn("Client on channel %d published with 'immediate' flag. Rejecting. Exchange: %s, RK: %s", channelId, exchangeName, routingKey)
			return c.sendChannelClose(channelId, 540, replyText, uint16(ClassBasic), MethodBasicPublish) // 540 NOT_IMPLEMENTED
		}

		c.server.Info("Processing basic.%s: exchange='%s', routingKey='%s', mandatory=%v, immediate=%v on channel %d",
			methodName, exchangeName, routingKey, mandatory, immediate, channelId)

		// Server-side validation for exchange existence (unless it's the default "" exchange which always exists)
		if exchangeName != "" {
			vhost := c.vhost
			vhost.mu.RLock()
			_, exExists := vhost.exchanges[exchangeName]
			vhost.mu.RUnlock()
			if !exExists {
				replyText := fmt.Sprintf("no exchange '%s' in vhost '/'", exchangeName)
				c.server.Warn("Basic.Publish: %s. Sending Channel.Close.", replyText)
				// AMQP code 404 (NOT_FOUND)
				// Note: If 'mandatory' is true, a Basic.Return should be sent if no queue is bound.
				// However, a non-existent exchange is a more fundamental error leading to Channel.Close.
				return c.sendChannelClose(channelId, 404, replyText, uint16(ClassBasic), MethodBasicPublish)
			}
		}

		ch.mu.Lock()
		// Add to pending messages for this channel. Header and Body frames will follow.
		newMessage := Message{
			Exchange:   exchangeName,
			RoutingKey: routingKey,
			Mandatory:  mandatory, // Server needs to implement Basic.Return if unroutable & mandatory
			Immediate:  immediate, // Server should ideally reject if immediate=true and not supported,
			// or implement Basic.Return if no consumer ready (deprecated behavior)
		}
		ch.pendingMessages = append(ch.pendingMessages, newMessage)
		ch.mu.Unlock()
		c.server.Info("Pending message added for basic.publish on channel %d. Waiting for header/body.", channelId)
		// No direct response for Basic.Publish. Confirmation is via Publisher Confirms (Basic.Ack/Nack) if enabled (not implemented here).
		return nil

	case MethodBasicConsume:
		var ticket uint16
		if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.consume (ticket)", uint16(ClassBasic), MethodBasicConsume)
		}
		queueName, err := readShortString(reader)
		if err != nil {
			c.server.Err("Error reading queueName for basic.consume: %v", err)
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.consume (queueName)", uint16(ClassBasic), MethodBasicConsume)
		}
		consumerTagIn, err := readShortString(reader) // Client can specify, or server generates if empty
		if err != nil {
			c.server.Err("Error reading consumerTagIn for basic.consume: %v", err)
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.consume (consumerTagIn)", uint16(ClassBasic), MethodBasicConsume)
		}

		bits, errReadByte := reader.ReadByte()
		if errReadByte != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.consume (bits)", uint16(ClassBasic), MethodBasicConsume)
		}

		args, errReadTable := readTable(reader) // Consumer arguments
		if errReadTable != nil {
			c.server.Err("Error reading arguments table for basic.consume (queue: '%s'): %v", queueName, errReadTable)
			// AMQP code 502 (SYNTAX_ERROR) for malformed table.
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed arguments table for basic.consume", uint16(ClassBasic), MethodBasicConsume)
		}

		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of basic.consume payload for queue '%s'.", queueName)
		}

		noLocal := (bits & 0x01) != 0 // Server doesn't fully implement no-local yet
		noAck := (bits & 0x02) != 0
		exclusive := (bits & 0x04) != 0
		noWait := (bits & 0x08) != 0

		c.server.Info("Processing basic.%s: queue='%s', consumerTag='%s', noLocal=%v, noAck=%v, exclusive=%v, noWait=%v, args=%v on channel %d",
			methodName, queueName, consumerTagIn, noLocal, noAck, exclusive, noWait, args, channelId)

		actualConsumerTag := consumerTagIn
		if actualConsumerTag == "" {
			// Generate a unique consumer tag for the channel
			ch.mu.Lock()     // Lock channel to safely generate a unique tag part
			ch.deliveryTag++ // Using deliveryTag as a simple counter for uniqueness on this channel
			actualConsumerTag = fmt.Sprintf("ctag-%d.%d", channelId, ch.deliveryTag)
			ch.mu.Unlock()
			c.server.Info("Server generated consumerTag: %s for basic.consume on queue '%s'", actualConsumerTag, queueName)
		}

		vhost := c.vhost
		vhost.mu.RLock() // RLock server to access queues map
		q, qExists := vhost.queues[queueName]
		vhost.mu.RUnlock()

		if !qExists {
			replyText := fmt.Sprintf("no queue '%s' in vhost '/'", queueName)
			c.server.Warn("Basic.Consume: %s. Sending Channel.Close.", replyText)
			// AMQP code 404 (NOT_FOUND)
			return c.sendChannelClose(channelId, 404, replyText, uint16(ClassBasic), MethodBasicConsume)
		}

		// Add consumer to channel and queue
		ch.mu.Lock() // Lock channel to modify its consumers map
		if _, tagExistsOnChannel := ch.consumers[actualConsumerTag]; tagExistsOnChannel {
			ch.mu.Unlock()
			replyText := fmt.Sprintf("consumer tag '%s' already in use on channel %d", actualConsumerTag, channelId)
			c.server.Err("Basic.Consume: %s. Sending Channel.Close.", replyText)
			// AMQP code 530 (NOT_ALLOWED) - AMQP spec says "consumer tag already in use on this channel"
			return c.sendChannelClose(channelId, 530, replyText, uint16(ClassBasic), MethodBasicConsume)
		}
		ch.consumers[actualConsumerTag] = queueName // Store mapping of consumer tag to queue name on this channel
		ch.mu.Unlock()

		q.mu.Lock() // Lock queue to modify its consumers map
		if exclusive {
			if len(q.Consumers) > 0 {
				q.mu.Unlock()
				// Rollback channel's consumer addition because queue is exclusively locked by another consumer
				ch.mu.Lock()
				delete(ch.consumers, actualConsumerTag)
				ch.mu.Unlock()
				replyText := fmt.Sprintf("queue '%s' is exclusive and already has consumer(s)", queueName)
				c.server.Warn("Basic.Consume: %s. Sending Channel.Close.", replyText)
				// AMQP code 405 (RESOURCE_LOCKED)
				return c.sendChannelClose(channelId, 405, replyText, uint16(ClassBasic), MethodBasicConsume)
			}
			// If exclusive and no consumers, this consumer gets exclusive access.
			// Your Queue struct has an `Exclusive` field set at Queue.Declare.
			// This `exclusive` bit in Basic.Consume refers to this consumer wanting exclusive access.
		}
		// Check if consumerTag is already in use on the queue (globally for the queue, across all channels)
		if _, consumerExistsOnQueue := q.Consumers[actualConsumerTag]; consumerExistsOnQueue {
			q.mu.Unlock()
			ch.mu.Lock()
			delete(ch.consumers, actualConsumerTag) // Rollback channel's consumer addition
			ch.mu.Unlock()
			replyText := fmt.Sprintf("consumer tag '%s' is already active on queue '%s'", actualConsumerTag, queueName)
			c.server.Err("Basic.Consume: %s. Sending Channel.Close.", replyText)
			// AMQP code 530 (NOT_ALLOWED) - "consumer tag not unique"
			return c.sendChannelClose(channelId, 530, replyText, uint16(ClassBasic), MethodBasicConsume)
		}

		consumer := &Consumer{
			Tag:       actualConsumerTag,
			ChannelId: channelId,
			NoAck:     noAck,
			Queue:     q,
			stopCh:    make(chan struct{}),
		}
		q.Consumers[actualConsumerTag] = consumer
		q.mu.Unlock()

		if !noWait {
			payloadOk := &bytes.Buffer{}
			binary.Write(payloadOk, binary.BigEndian, uint16(ClassBasic))
			binary.Write(payloadOk, binary.BigEndian, uint16(MethodBasicConsumeOk))
			writeShortString(payloadOk, actualConsumerTag)

			if errWrite := c.writeFrame(&Frame{Type: FrameMethod, Channel: channelId, Payload: payloadOk.Bytes()}); errWrite != nil {
				c.server.Err("Error sending basic.consume-ok for consumer '%s' on queue '%s': %v", actualConsumerTag, queueName, errWrite)
				// Rollback consumer registration if ConsumeOk fails to send
				ch.mu.Lock()
				delete(ch.consumers, actualConsumerTag)
				ch.mu.Unlock()
				q.mu.Lock()
				close(consumer.stopCh) // Close the message channel we created
				delete(q.Consumers, actualConsumerTag)
				q.mu.Unlock()
				return errWrite // Propagate I/O error
			}
			c.server.Info("Sent basic.consume-ok for consumer '%s' on queue '%s'", actualConsumerTag, queueName)
		}

		// Start the message delivery goroutine for this consumer
		go c.deliverMessages(channelId, actualConsumerTag, consumer) // Pass noAck to delivery function
		c.server.Info("Started message delivery goroutine for consumer '%s' on queue '%s' (noAck=%v)", actualConsumerTag, queueName, noAck)

		return nil

	case MethodBasicAck:
		return c.handleBasicAck(reader, channelId)

	case MethodBasicNack:
		return c.handleBasicNack(reader, channelId)

	case MethodBasicReject:
		return c.handleBasicReject(reader, channelId)

	case MethodBasicCancel:
		return c.handleBasicCancel(reader, channelId)

	default:
		replyText := fmt.Sprintf("unknown or not implemented basic method id %d", methodId)
		c.server.Err("Unhandled basic method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		// AMQP code 540 (NOT_IMPLEMENTED) or 503 (COMMAND_INVALID)
		return c.sendChannelClose(channelId, 540, replyText, uint16(ClassBasic), methodId)
	}
}

func (c *Connection) handleBasicGet(reader *bytes.Reader, channelId uint16) error {
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.get (ticket)", uint16(ClassBasic), MethodBasicGet)
	}

	queueName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueName for basic.get: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.get (queueName)", uint16(ClassBasic), MethodBasicGet)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.get (noAck bit)", uint16(ClassBasic), MethodBasicGet)
	}
	noAck := (bits & 0x01) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of basic.get payload.")
	}

	c.server.Info("Processing basic.get: queue='%s', noAck=%v on channel %d", queueName, noAck, channelId)

	// Get the queue
	vhost := c.vhost
	vhost.mu.RLock()
	queue, qExists := vhost.queues[queueName]
	vhost.mu.RUnlock()

	if !qExists {
		replyText := fmt.Sprintf("no queue '%s' in vhost '/'", queueName)
		c.server.Warn("Basic.Get: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, 404, replyText, uint16(ClassBasic), MethodBasicGet)
	}

	// Try to get a message from the queue
	queue.mu.Lock()

	if len(queue.Messages) == 0 {
		queue.mu.Unlock()
		// Send Basic.GetEmpty
		c.server.Info("No messages available in queue '%s' for basic.get", queueName)
		return c.sendBasicGetEmpty(channelId)
	}

	// Get the first message
	msg := queue.Messages[0]
	queue.Messages = queue.Messages[1:]
	messageCount := uint32(len(queue.Messages))
	queue.mu.Unlock()

	// Get channel for delivery tag management
	ch, exists, isClosing := c.getChannel(channelId)
	if !exists {
		// Put message back since we can't deliver it
		queue.mu.Lock()
		queue.Messages = append([]Message{msg}, queue.Messages...)
		queue.mu.Unlock()
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR - channel object nil", uint16(ClassBasic), MethodBasicGet)
	}

	if isClosing {
		// Put message back since channel is closing
		queue.mu.Lock()
		queue.Messages = append([]Message{msg}, queue.Messages...)
		queue.mu.Unlock()
		c.server.Debug("Ignoring basic.get on channel %d that is being closed", channelId)
		return nil
	}

	// Assign delivery tag and track if not noAck
	ch.mu.Lock()
	ch.deliveryTag++
	deliveryTag := ch.deliveryTag

	if !noAck {
		unacked := &UnackedMessage{
			Message:     msg,
			ConsumerTag: "", // No consumer tag for basic.get
			QueueName:   queueName,
			DeliveryTag: deliveryTag,
			Delivered:   time.Now(),
		}
		ch.unackedMessages[deliveryTag] = unacked
	}
	ch.mu.Unlock()

	c.server.Info("Delivering message via basic.get: deliveryTag=%d, exchange=%s, routingKey=%s, bodySize=%d, noAck=%v, redelivered=%v",
		deliveryTag, msg.Exchange, msg.RoutingKey, len(msg.Body), noAck, msg.Redelivered)

	// Send Basic.GetOk method frame
	methodPayload := &bytes.Buffer{}
	binary.Write(methodPayload, binary.BigEndian, uint16(ClassBasic))
	binary.Write(methodPayload, binary.BigEndian, uint16(MethodBasicGetOk))
	binary.Write(methodPayload, binary.BigEndian, deliveryTag)

	if msg.Redelivered {
		methodPayload.WriteByte(1) // redelivered = true
	} else {
		methodPayload.WriteByte(0) // redelivered = false
	}

	writeShortString(methodPayload, msg.Exchange)
	writeShortString(methodPayload, msg.RoutingKey)
	binary.Write(methodPayload, binary.BigEndian, messageCount)

	// Prepare header frame
	headerPayload := &bytes.Buffer{}
	binary.Write(headerPayload, binary.BigEndian, uint16(ClassBasic))
	binary.Write(headerPayload, binary.BigEndian, uint16(0)) // weight
	binary.Write(headerPayload, binary.BigEndian, uint64(len(msg.Body)))

	// Calculate property flags
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

	// Send all three frames atomically
	c.writeMu.Lock()

	// Buffer GetOk frame
	if err := c.writeFrameInternal(FrameMethod, channelId, methodPayload.Bytes()); err != nil {
		c.writeMu.Unlock()
		c.server.Err("Error buffering basic.get-ok frame: %v", err)
		// Put message back in queue if we failed to send
		queue.mu.Lock()
		queue.Messages = append([]Message{msg}, queue.Messages...)
		queue.mu.Unlock()
		// Remove from unacked if we tracked it
		if !noAck {
			ch.mu.Lock()
			delete(ch.unackedMessages, deliveryTag)
			ch.mu.Unlock()
		}
		return err
	}

	// Buffer header frame
	if err := c.writeFrameInternal(FrameHeader, channelId, headerPayload.Bytes()); err != nil {
		c.writeMu.Unlock()
		c.server.Err("Error buffering header frame for basic.get: %v", err)
		return err
	}

	// Buffer body frame
	if err := c.writeFrameInternal(FrameBody, channelId, msg.Body); err != nil {
		c.writeMu.Unlock()
		c.server.Err("Error buffering body frame for basic.get: %v", err)
		return err
	}

	// Flush all frames
	if err := c.writer.Flush(); err != nil {
		c.writeMu.Unlock()
		c.server.Err("Error flushing frames for basic.get: %v", err)
		return err
	}

	c.writeMu.Unlock()

	c.server.Info("Successfully delivered message via basic.get (deliveryTag=%d)", deliveryTag)
	return nil
}

func (c *Connection) sendBasicGetEmpty(channelId uint16) error {
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
	binary.Write(payload, binary.BigEndian, uint16(MethodBasicGetEmpty))
	writeShortString(payload, "") // cluster-id (reserved, must be empty)

	return c.writeFrame(&Frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	})
}

func (c *Connection) handleBasicCancel(reader *bytes.Reader, channelId uint16) error {
	consumerTag, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading consumerTag for basic.cancel: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.cancel (consumerTag)", uint16(ClassBasic), MethodBasicCancel)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.cancel (noWait bit)", uint16(ClassBasic), MethodBasicCancel)
	}
	noWait := (bits & 0x01) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of basic.cancel payload.")
	}

	c.server.Info("Processing basic.cancel: consumerTag='%s', noWait=%v on channel %d", consumerTag, noWait, channelId)

	c.mu.RLock()
	ch := c.channels[channelId]
	c.mu.RUnlock()

	if ch == nil {
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR - channel object nil", uint16(ClassBasic), MethodBasicCancel)
	}

	ch.mu.Lock()
	queueName, exists := ch.consumers[consumerTag]
	if !exists {
		ch.mu.Unlock()
		return c.sendChannelClose(channelId, 404, fmt.Sprintf("NOT_FOUND - no consumer '%s'", consumerTag), uint16(ClassBasic), MethodBasicCancel)
	}
	// Remove from channel's consumer map
	delete(ch.consumers, consumerTag)
	ch.mu.Unlock()

	// Remove from queue's consumer map
	vhost := c.vhost
	vhost.mu.RLock()
	queue, qExists := vhost.queues[queueName]
	vhost.mu.RUnlock()

	if qExists && queue != nil {
		queue.mu.Lock()
		if consumer, consumerExists := queue.Consumers[consumerTag]; consumerExists {
			close(consumer.stopCh) // This will cause deliverMessages goroutine to exit
			delete(queue.Consumers, consumerTag)
			c.server.Info("Cancelled consumer '%s' on queue '%s' for channel %d", consumerTag, queueName, channelId)
		}
		queue.mu.Unlock()
	}

	if !noWait {
		// Send Basic.CancelOk
		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
		binary.Write(payload, binary.BigEndian, uint16(MethodBasicCancelOk))
		writeShortString(payload, consumerTag)

		if err := c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channelId,
			Payload: payload.Bytes(),
		}); err != nil {
			c.server.Err("Error sending basic.cancel-ok for consumer '%s': %v", consumerTag, err)
			return err
		}
		c.server.Info("Sent basic.cancel-ok for consumer '%s'", consumerTag)
	}

	return nil
}

func (c *Connection) handleBasicAck(reader *bytes.Reader, channelId uint16) error {
	var deliveryTag uint64
	if err := binary.Read(reader, binary.BigEndian, &deliveryTag); err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.ack (delivery-tag)", uint16(ClassBasic), MethodBasicAck)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.ack (bits)", uint16(ClassBasic), MethodBasicAck)
	}

	multiple := (bits & 0x01) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of basic.ack payload.")
	}

	c.server.Info("Processing basic.ack: deliveryTag=%d, multiple=%v on channel %d", deliveryTag, multiple, channelId)

	ch, exists, isClosing := c.getChannel(channelId)

	if !exists {
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR - channel object nil", uint16(ClassBasic), MethodBasicAck)
	}

	if isClosing {
		c.server.Debug("Ignoring basic.ack on channel %d that is being closed", channelId)
		return nil
	}

	ch.mu.Lock()

	if deliveryTag == 0 {
		// delivery-tag of 0 means "all messages delivered so far"
		if !multiple {
			ch.mu.Unlock()
			// If multiple is false and delivery-tag is 0, it's a protocol error
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - delivery-tag 0 with multiple=false", uint16(ClassBasic), MethodBasicAck)
		}
		// Acknowledge all messages
		for tag := range ch.unackedMessages {
			delete(ch.unackedMessages, tag)
		}
		c.server.Info("Acknowledged all messages on channel %d", channelId)
	} else {
		if multiple {
			// Acknowledge all messages up to and including deliveryTag
			for tag := range ch.unackedMessages {
				if tag <= deliveryTag {
					delete(ch.unackedMessages, tag)
					c.server.Debug("Acknowledged message with delivery tag %d on channel %d", tag, channelId)
				}
			}
			c.server.Info("Acknowledged messages up to delivery tag %d on channel %d", deliveryTag, channelId)
		} else {
			// Acknowledge only the specific message
			if _, exists := ch.unackedMessages[deliveryTag]; !exists {
				ch.mu.Unlock()
				// AMQP spec: unknown delivery tag is a channel error
				return c.sendChannelClose(channelId, 406, fmt.Sprintf("PRECONDITION_FAILED - unknown delivery-tag %d", deliveryTag), uint16(ClassBasic), MethodBasicAck)
			}
			delete(ch.unackedMessages, deliveryTag)
			c.server.Info("Acknowledged message with delivery tag %d on channel %d", deliveryTag, channelId)
		}
	}

	ch.mu.Unlock()
	return nil
}

func (c *Connection) handleBasicNack(reader *bytes.Reader, channelId uint16) error {
	var deliveryTag uint64
	if err := binary.Read(reader, binary.BigEndian, &deliveryTag); err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.nack (delivery-tag)", uint16(ClassBasic), MethodBasicNack)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.nack (bits)", uint16(ClassBasic), MethodBasicNack)
	}

	multiple := (bits & 0x01) != 0
	requeue := (bits & 0x02) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of basic.nack payload.")
	}

	c.server.Info("Processing basic.nack: deliveryTag=%d, multiple=%v, requeue=%v on channel %d",
		deliveryTag, multiple, requeue, channelId)

	ch, exists, isClosing := c.getChannel(channelId)

	if !exists {
		// This should ideally not happen if channel existence is checked before calling handlers for specific classes.
		// However, if it does, it's a server-side issue with channel management.
		c.server.Err("Internal error: channel object nil for basic.nack on channel %d", channelId)
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR - channel object nil for nack", uint16(ClassBasic), MethodBasicNack)
	}

	if isClosing {
		c.server.Debug("Ignoring basic.nack on channel %d that is being closed", channelId)
		return nil
	}

	ch.mu.Lock() // Lock channel to safely access unackedMessages

	var messagesToProcess []*UnackedMessage
	var affectedQueues = make(map[string]bool) // To track queues that need dispatch attempt

	if deliveryTag == 0 {
		if !multiple {
			ch.mu.Unlock()
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - delivery-tag 0 with multiple=false for nack", uint16(ClassBasic), MethodBasicNack)
		}
		for _, unacked := range ch.unackedMessages {
			messagesToProcess = append(messagesToProcess, unacked)
		}
		// Clear all unacked messages after collecting them
		ch.unackedMessages = make(map[uint64]*UnackedMessage)
	} else {
		if multiple {
			for tag, unacked := range ch.unackedMessages {
				if tag <= deliveryTag {
					messagesToProcess = append(messagesToProcess, unacked)
					delete(ch.unackedMessages, tag)
				}
			}
		} else {
			unacked, exists := ch.unackedMessages[deliveryTag]
			if !exists {
				ch.mu.Unlock()
				return c.sendChannelClose(channelId, 406, fmt.Sprintf("PRECONDITION_FAILED - unknown delivery-tag %d for nack", deliveryTag), uint16(ClassBasic), MethodBasicNack)
			}
			messagesToProcess = append(messagesToProcess, unacked)
			delete(ch.unackedMessages, deliveryTag)
		}
	}
	ch.mu.Unlock() // Unlock channel as soon as unackedMessages modification is done

	// Process the nacked messages
	if requeue {
		vhost := c.vhost
		for _, unacked := range messagesToProcess {
			vhost.mu.RLock()
			queue, qExists := vhost.queues[unacked.QueueName]
			vhost.mu.RUnlock()

			if qExists && queue != nil {
				unacked.Message.Redelivered = true // Mark message as redelivered

				queue.mu.Lock() // Lock the specific queue
				// Prepend to queue (requeued messages go to front)
				queue.Messages = append([]Message{unacked.Message}, queue.Messages...)
				c.server.Info("Requeued message to queue '%s' (original delivery tag %d on channel %d)", unacked.QueueName, unacked.DeliveryTag, channelId)
				affectedQueues[unacked.QueueName] = true
				queue.mu.Unlock() // Unlock the queue
			} else {
				c.server.Warn("Queue '%s' not found for requeuing nacked message (tag %d on channel %d)", unacked.QueueName, unacked.DeliveryTag, channelId)
			}
		}
	} else {
		// Messages are discarded (potentially dead-lettered in a more advanced server)
		c.server.Info("Discarded %d nacked messages (requeue=false) from channel %d", len(messagesToProcess), channelId)
	}

	return nil
}

func (c *Connection) handleBasicReject(reader *bytes.Reader, channelId uint16) error {
	var deliveryTag uint64
	if err := binary.Read(reader, binary.BigEndian, &deliveryTag); err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.reject (delivery-tag)", uint16(ClassBasic), MethodBasicReject)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.reject (requeue bit)", uint16(ClassBasic), MethodBasicReject)
	}
	requeue := (bits & 0x01) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of basic.reject payload.")
	}

	c.server.Info("Processing basic.reject: deliveryTag=%d, requeue=%v on channel %d", deliveryTag, requeue, channelId)

	ch, exists, isClosing := c.getChannel(channelId)
	if !exists {
		c.server.Err("Internal error: channel object nil for basic.reject on channel %d", channelId)
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR - channel object nil for reject", uint16(ClassBasic), MethodBasicReject)
	}

	if isClosing {
		c.server.Debug("Ignoring basic.reject on channel %d that is being closed", channelId)
		return nil
	}

	ch.mu.Lock() // Lock channel
	if deliveryTag == 0 {
		ch.mu.Unlock()
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - delivery-tag cannot be 0 for basic.reject", uint16(ClassBasic), MethodBasicReject)
	}

	unacked, exists := ch.unackedMessages[deliveryTag]
	if !exists {
		ch.mu.Unlock()
		return c.sendChannelClose(channelId, 406, fmt.Sprintf("PRECONDITION_FAILED - unknown delivery-tag %d for reject", deliveryTag), uint16(ClassBasic), MethodBasicReject)
	}
	delete(ch.unackedMessages, deliveryTag) // Remove from unacked
	ch.mu.Unlock()                          // Unlock channel

	if requeue {
		vhost := c.vhost
		vhost.mu.RLock()
		queue, qExists := vhost.queues[unacked.QueueName]
		vhost.mu.RUnlock()

		if qExists && queue != nil {
			unacked.Message.Redelivered = true // Mark message as redelivered

			queue.mu.Lock() // Lock the specific queue
			queue.Messages = append([]Message{unacked.Message}, queue.Messages...)
			c.server.Info("Requeued rejected message to queue '%s' (delivery tag %d on channel %d)", unacked.QueueName, deliveryTag, channelId)
			queue.mu.Unlock() // Unlock the queue
		} else {
			c.server.Warn("Queue '%s' not found for requeuing rejected message (tag %d on channel %d)", unacked.QueueName, deliveryTag, channelId)
		}
	} else {
		// Message is discarded
		c.server.Info("Discarded rejected message (requeue=false) from channel %d (delivery tag %d)", channelId, deliveryTag)
	}

	return nil
}

func (c *Connection) handleBasicRecover(reader *bytes.Reader, channelId uint16) error {
	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.recover (requeue bit)", uint16(ClassBasic), MethodBasicRecover)
	}
	requeue := (bits & 0x01) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of basic.recover payload.")
	}

	c.server.Info("Processing basic.recover: requeue=%v on channel %d", requeue, channelId)

	ch, exists, isClosing := c.getChannel(channelId)
	if !exists {
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR - channel object nil", uint16(ClassBasic), MethodBasicRecover)
	}

	if isClosing {
		c.server.Debug("Ignoring basic.recover on channel %d that is being closed", channelId)
		return nil
	}

	ch.mu.Lock()

	// Collect all unacked messages
	var messagesToProcess []*UnackedMessage
	affectedQueues := make(map[string]bool)

	for _, unacked := range ch.unackedMessages {
		messagesToProcess = append(messagesToProcess, unacked)
		affectedQueues[unacked.QueueName] = true
	}

	// Clear all unacked messages from this channel
	ch.unackedMessages = make(map[uint64]*UnackedMessage)
	ch.mu.Unlock()

	c.server.Info("Basic.Recover: Processing %d unacked messages with requeue=%v", len(messagesToProcess), requeue)

	if requeue {
		vhost := c.vhost
		// Requeue messages to their original queues
		for _, unacked := range messagesToProcess {
			vhost.mu.RLock()
			queue, qExists := vhost.queues[unacked.QueueName]
			vhost.mu.RUnlock()

			if qExists && queue != nil {
				unacked.Message.Redelivered = true

				queue.mu.Lock()
				// Prepend to queue (recovered messages go to front)
				queue.Messages = append([]Message{unacked.Message}, queue.Messages...)
				c.server.Info("Requeued recovered message to queue '%s' (original delivery tag %d on channel %d)",
					unacked.QueueName, unacked.DeliveryTag, channelId)
				queue.mu.Unlock()
			} else {
				c.server.Warn("Queue '%s' not found for requeuing recovered message (tag %d on channel %d)",
					unacked.QueueName, unacked.DeliveryTag, channelId)
			}
		}
	} else {
		// If requeue is false, messages should be redelivered to the original consumer
		// Since we don't track which consumer received each message, and the AMQP spec
		// is vague about this case, we'll treat it as discarding the messages
		// (similar to nack with requeue=false)
		c.server.Info("Discarded %d recovered messages (requeue=false) from channel %d",
			len(messagesToProcess), channelId)
	}

	// Send Basic.RecoverOk
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
	binary.Write(payload, binary.BigEndian, uint16(MethodBasicRecoverOk))

	if err := c.writeFrame(&Frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	}); err != nil {
		c.server.Err("Error sending basic.recover-ok: %v", err)
		return err
	}

	c.server.Info("Sent basic.recover-ok for channel %d", channelId)
	return nil
}

func (c *Connection) handleHeader(frame *Frame) error {
	if frame.Channel == 0 {
		c.server.Err("Received content header frame on channel 0")
		return c.sendConnectionClose(504, "channel error - content frames cannot use channel 0", 0, 0)
	}

	ch, exists, isClosing := c.getChannel(frame.Channel)

	if !exists {
		// Header frame for a non-existent channel (or channel 0 which shouldn't get headers like this)
		c.server.Warn("Received header frame on non-existent or invalid channel %d", frame.Channel)
		// AMQP code 504 (CHANNEL_ERROR)
		// This is a connection error because the channel context is wrong.
		return c.sendConnectionClose(504, fmt.Sprintf("header frame on invalid channel %d", frame.Channel), 0, 0)
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
		// This is a protocol violation: unexpected frame.
		// AMQP code 505 (UNEXPECTED_FRAME)
		return c.sendChannelClose(frame.Channel, 505, "header frame received without pending basic.publish", uint16(ClassBasic), 0) // 0 for methodId as it's not a direct method response
	}

	pendingMessage := &ch.pendingMessages[0] // Get, don't remove yet

	reader := bytes.NewReader(frame.Payload)
	var classId, weight uint16 // weight is deprecated
	var bodySize uint64

	if err := binary.Read(reader, binary.BigEndian, &classId); err != nil {
		return c.sendChannelClose(frame.Channel, 502, "malformed header: could not read class-id", 0, 0)
	}
	if err := binary.Read(reader, binary.BigEndian, &weight); err != nil {
		return c.sendChannelClose(frame.Channel, 502, "malformed header: could not read weight", classId, 0)
	}
	if err := binary.Read(reader, binary.BigEndian, &bodySize); err != nil {
		return c.sendChannelClose(frame.Channel, 502, "malformed header: could not read body-size", classId, 0)
	}

	c.server.Info("Processing basic header frame: channel=%d, classId=%d, bodySize=%d", frame.Channel, classId, bodySize)

	if classId != ClassBasic { // Only Basic class messages have properties like this
		c.server.Warn("Received header frame for non-Basic class %d on channel %d", classId, frame.Channel)
		// AMQP code 503 (COMMAND_INVALID) or 505 (UNEXPECTED_FRAME)
		return c.sendChannelClose(frame.Channel, 503, fmt.Sprintf("header frame for unexpected class %d", classId), classId, 0)
	}

	var flags uint16
	if err := binary.Read(reader, binary.BigEndian, &flags); err != nil {
		return c.sendChannelClose(frame.Channel, 502, "malformed header: could not read property-flags", classId, 0)
	}

	// Parse properties based on flags
	var errProp error // To catch errors from reading properties
	var err error
	if flags&0x8000 != 0 { // content-type
		pendingMessage.Properties.ContentType, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, 502, "SYNTAX_ERROR - malformed content-type", classId, 0)
		}
	}
	if flags&0x4000 != 0 { // content-encoding
		pendingMessage.Properties.ContentEncoding, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, 502, "SYNTAX_ERROR - malformed content-encoding", classId, 0)
		}
	}
	if flags&0x2000 != 0 { // headers
		var headersMap map[string]interface{}
		headersMap, errProp = readTable(reader) // Use error-returning readTable
		if errProp != nil {
			c.server.Err("Error parsing headers table in handleHeader for channel %d: %v", frame.Channel, errProp)
			// AMQP code 502 (SYNTAX_ERROR) for malformed table
			return c.sendChannelClose(frame.Channel, 502, "malformed headers table", classId, 0)
		}
		pendingMessage.Properties.Headers = headersMap
	}
	if flags&0x1000 != 0 { // delivery-mode
		if errProp = binary.Read(reader, binary.BigEndian, &pendingMessage.Properties.DeliveryMode); errProp != nil {
			return c.sendChannelClose(frame.Channel, 502, "malformed delivery-mode", classId, 0)
		}
	}
	if flags&0x0800 != 0 { // priority
		if errProp = binary.Read(reader, binary.BigEndian, &pendingMessage.Properties.Priority); errProp != nil {
			return c.sendChannelClose(frame.Channel, 502, "malformed priority", classId, 0)
		}
	}
	if flags&0x0400 != 0 { // correlation-id
		pendingMessage.Properties.CorrelationId, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, 502, "SYNTAX_ERROR - malformed correlation-id", classId, 0)
		}
	}
	if flags&0x0200 != 0 { // reply-to
		pendingMessage.Properties.ReplyTo, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, 502, "SYNTAX_ERROR - malformed reply-to", classId, 0)
		}
	}
	if flags&0x0100 != 0 { // expiration
		pendingMessage.Properties.Expiration, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, 502, "SYNTAX_ERROR - malformed expiration", classId, 0)
		}
	}
	if flags&0x0080 != 0 { // message-id
		pendingMessage.Properties.MessageId, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, 502, "SYNTAX_ERROR - malformed message-id", classId, 0)
		}
	}
	if flags&0x0040 != 0 { // timestamp
		if errProp = binary.Read(reader, binary.BigEndian, &pendingMessage.Properties.Timestamp); errProp != nil {
			return c.sendChannelClose(frame.Channel, 502, "malformed timestamp", classId, 0)
		}
	}
	if flags&0x0020 != 0 { // type
		pendingMessage.Properties.Type, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, 502, "SYNTAX_ERROR - malformed type", classId, 0)
		}
	}
	if flags&0x0010 != 0 { // user-id
		pendingMessage.Properties.UserId, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, 502, "SYNTAX_ERROR - malformed user-id", classId, 0)
		}
	}
	if flags&0x0008 != 0 { // app-id
		pendingMessage.Properties.AppId, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, 502, "SYNTAX_ERROR - malformed app-id", classId, 0)
		}
	}
	if flags&0x0004 != 0 { // cluster-id (Reserved, usually not used by clients)
		pendingMessage.Properties.ClusterId, err = readShortString(reader)
		if err != nil {
			return c.sendChannelClose(frame.Channel, 502, "SYNTAX_ERROR - malformed cluster-id", classId, 0)
		}
	}

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of header frame payload on channel %d", frame.Channel)
	}

	return nil // Successfully processed header
}

func (c *Connection) handleBody(frame *Frame) {
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
		propertiesCopy := Properties{
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
		messageToDeliver := Message{
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
		c.deliverMessage(messageToDeliver, frame.Channel)
	} else {
		ch.mu.Unlock()
		c.server.Warn("Received body frame with no pending message on channel %d", frame.Channel)
	}
}

// Add routing functions to server.go

// RouteMessage handles all exchange type routing logic
func (c *Connection) routeMessage(msg Message) ([]string, error) {
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
func (c *Connection) routeDirect(exchange *Exchange, routingKey string) []string {
	return exchange.Bindings[routingKey]
}

// routeFanout returns all queues bound to the exchange
func (c *Connection) routeFanout(exchange *Exchange) []string {
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
func (c *Connection) routeTopic(exchange *Exchange, routingKey string) []string {
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

func (c *Connection) deliverMessage(msg Message, channelId uint16) {
	c.server.Info("Message: exchange=%s, routingKey=%s, mandatory=%v", msg.Exchange, msg.RoutingKey, msg.Mandatory)

	// Get channel for confirm handling
	ch, exists, _ := c.getChannel(channelId)
	var deliveryTag uint64
	var confirmMode bool

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
			// Send basic.return for unroutable mandatory message due to error
			if returnErr := c.sendBasicReturn(channelId, 312, "NO_ROUTE", msg.Exchange, msg.RoutingKey); returnErr != nil {
				c.server.Err("Failed to send basic.return: %v", returnErr)
			}
			// Also send the header and body frames after basic.return
			c.sendReturnedMessage(channelId, msg)
		}
		// Send negative acknowledgment if in confirm mode
		if confirmMode {
			c.sendBasicNack(channelId, deliveryTag, false, false)
		}
		return
	}

	if len(queueNames) == 0 && msg.Mandatory && channelId != 0 {
		// No queues found for mandatory message
		c.server.Warn("No queues for mandatory message on exchange '%s' with routing key '%s'", msg.Exchange, msg.RoutingKey)

		// Send basic.return
		if returnErr := c.sendBasicReturn(channelId, 312, "NO_ROUTE", msg.Exchange, msg.RoutingKey); returnErr != nil {
			c.server.Err("Failed to send basic.return: %v", returnErr)
			// Send negative acknowledgment if in confirm mode
			if confirmMode {
				c.sendBasicNack(channelId, deliveryTag, false, false)
			}
			return
		}

		// Send the returned message content
		c.sendReturnedMessage(channelId, msg)
		// Send negative acknowledgment if in confirm mode
		if confirmMode {
			c.sendBasicNack(channelId, deliveryTag, false, false)
		}
		return
	}

	// Deliver to each queue
	for _, queueName := range queueNames {
		c.deliverToQueue(queueName, msg)
	}

	// Send positive acknowledgment if in confirm mode
	if confirmMode {
		c.sendBasicAck(channelId, deliveryTag, false)
	}
}

func (c *Connection) sendBasicAck(channelId uint16, deliveryTag uint64, multiple bool) error {
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

	return c.writeFrame(&Frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	})
}

func (c *Connection) sendBasicNack(channelId uint16, deliveryTag uint64, multiple bool, requeue bool) error {
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

	return c.writeFrame(&Frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	})
}

// Helper method to send the returned message's header and body
func (c *Connection) sendReturnedMessage(channelId uint16, msg Message) {
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
	if err := c.writeFrame(&Frame{
		Type:    FrameHeader,
		Channel: channelId,
		Payload: headerPayload.Bytes(),
	}); err != nil {
		c.server.Err("Failed to send header frame for returned message: %v", err)
		return
	}

	// Send body frame
	if err := c.writeFrame(&Frame{
		Type:    FrameBody,
		Channel: channelId,
		Payload: msg.Body,
	}); err != nil {
		c.server.Err("Failed to send body frame for returned message: %v", err)
	}
}

// sendBasicCancelFromServer sends a Basic.Cancel method frame to the client.
// This informs the client that a consumer has been cancelled by the server.
func (c *Connection) sendBasicCancelFromServer(channelId uint16, consumerTag string) error {
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

func (c *Connection) sendBasicCancelToConsumers(consumers map[string]*Consumer, queueName string) {
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

func (c *Connection) handleExchangeDelete(reader *bytes.Reader, channelId uint16) error {
	// AMQP 0-9-1: ticket (short), exchange (shortstr), if-unused (bit), no-wait (bit)
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed exchange.delete (ticket)", uint16(ClassExchange), MethodExchangeDelete)
	}

	exchangeName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading exchangeName for exchange.delete: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed exchange.delete (exchangeName)", uint16(ClassExchange), MethodExchangeDelete)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed exchange.delete (bits)", uint16(ClassExchange), MethodExchangeDelete)
	}

	ifUnused := (bits & 0x01) != 0
	noWait := (bits & 0x02) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of exchange.delete payload.")
	}

	c.server.Info("Processing exchange.delete: exchange='%s', ifUnused=%v, noWait=%v on channel %d",
		exchangeName, ifUnused, noWait, channelId)

	// Cannot delete the default exchange
	if exchangeName == "" {
		replyText := "ACCESS_REFUSED - cannot delete default exchange"
		c.server.Warn("Attempt to delete default exchange. Sending Channel.Close.")
		return c.sendChannelClose(channelId, 403, replyText, uint16(ClassExchange), MethodExchangeDelete)
	}

	vhost := c.vhost

	// Hold read lock to safely check and mark for deletion
	vhost.mu.RLock()

	if vhost.IsDeleting() {
		vhost.mu.RUnlock()
		c.server.Info("Exchange.Delete: VHost is being deleted")
		return c.sendConnectionClose(320, "VHost deleted", uint16(ClassExchange), MethodExchangeDelete)
	}

	exchange, exists := vhost.exchanges[exchangeName]
	if !exists {
		vhost.mu.RUnlock()
		replyText := fmt.Sprintf("NOT_FOUND - no exchange '%s' in vhost '/'", exchangeName)
		c.server.Warn("Exchange.Delete: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, 404, replyText, uint16(ClassExchange), MethodExchangeDelete)
	}

	// Check if-unused condition
	if ifUnused {
		exchange.mu.RLock()
		hasBindings := len(exchange.Bindings) > 0
		exchange.mu.RUnlock()

		if hasBindings {
			vhost.mu.RUnlock()
			replyText := fmt.Sprintf("PRECONDITION_FAILED - exchange '%s' in use (has bindings)", exchangeName)
			c.server.Warn("Exchange.Delete: %s. Sending Channel.Close.", replyText)
			return c.sendChannelClose(channelId, 406, replyText, uint16(ClassExchange), MethodExchangeDelete)
		}
	}

	// Mark exchange for deletion
	if !exchange.deleted.CompareAndSwap(false, true) {
		vhost.mu.RUnlock()
		replyText := fmt.Sprintf("exchange '%s' is already being deleted", exchangeName)
		return c.sendChannelClose(channelId, 409, replyText, uint16(ClassExchange), MethodExchangeDelete)
	}

	vhost.mu.RUnlock()

	// Now perform the actual deletion
	// First, clean up any queue bindings that reference this exchange
	c.cleanupQueueBindingsForExchange(vhost, exchangeName)

	// Remove from vhost
	vhost.mu.Lock()
	delete(vhost.exchanges, exchangeName)
	vhost.mu.Unlock()

	c.server.Info("Successfully deleted exchange '%s'", exchangeName)

	// Send Exchange.DeleteOk if not no-wait
	if !noWait {
		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassExchange))
		binary.Write(payload, binary.BigEndian, uint16(MethodExchangeDeleteOk))

		if err := c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channelId,
			Payload: payload.Bytes(),
		}); err != nil {
			c.server.Err("Error sending exchange.delete-ok: %v", err)
			return err
		}
		c.server.Info("Sent exchange.delete-ok for exchange '%s'", exchangeName)
	}

	return nil
}

func (c *Connection) handleQueueDeclare(reader *bytes.Reader, channelId uint16) error {
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		// Malformed frame if basic fields cannot be read.
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.declare (ticket)", uint16(ClassQueue), MethodQueueDeclare)
	}
	queueNameIn, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueNameIn for queue.declare: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.declare (queueNameIn)", uint16(ClassQueue), MethodQueueDeclare)
	}

	bits, errReadByte := reader.ReadByte()
	if errReadByte != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.declare (bits)", uint16(ClassQueue), MethodQueueDeclare)
	}

	args, errReadTable := readTable(reader) // readTable now returns (map, error)
	if errReadTable != nil {
		c.server.Err("Error reading arguments table for queue.declare (queue: '%s'): %v", queueNameIn, errReadTable)
		// AMQP code 502 (SYNTAX_ERROR) for malformed table.
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed arguments table for queue.declare", uint16(ClassQueue), MethodQueueDeclare)
	}

	// Check for extra data after all arguments are read.
	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of queue.declare payload for queue '%s'.", queueNameIn)
	}

	passive := (bits & 0x01) != 0
	durable := (bits & 0x02) != 0
	exclusive := (bits & 0x04) != 0
	autoDelete := (bits & 0x08) != 0 // Server doesn't fully implement auto-delete logic yet
	noWait := (bits & 0x10) != 0

	c.server.Info("Processing queue.declare: name='%s', passive=%v, durable=%v, exclusive=%v, autoDelete=%v, noWait=%v, args=%v on channel %d",
		queueNameIn, passive, durable, exclusive, autoDelete, noWait, args, channelId)

	var actualQueueName = queueNameIn
	var messageCount uint32 = 0
	var consumerCount uint32 = 0

	vhost := c.vhost
	vhost.mu.Lock()

	if queueNameIn == "" { // Client requests server-generated name
		if passive {
			vhost.mu.Unlock()
			errMsg := "passive declare of server-named queue not allowed"
			c.server.Err("Queue.Declare passive error: %s", errMsg)
			// AMQP code 403 (ACCESS_REFUSED)
			return c.sendChannelClose(channelId, 403, errMsg, uint16(ClassQueue), MethodQueueDeclare)
		}
		// Generate a unique name
		actualQueueName = fmt.Sprintf("amq.gen-%s-%d-%d-q", c.conn.LocalAddr().String(), channelId, c.server.listener.Addr().(*net.TCPAddr).Port)
		tempCounter := 0
		baseName := actualQueueName
		// Ensure unique name (simple approach)
		for _, existsGen := vhost.queues[actualQueueName]; existsGen; _, existsGen = vhost.queues[actualQueueName] {
			tempCounter++
			actualQueueName = fmt.Sprintf("%s-%d", baseName, tempCounter)
		}
		c.server.Info("Server generated queue name: %s", actualQueueName)
	}

	q, exists := vhost.queues[actualQueueName]

	if passive {
		if !exists {
			vhost.mu.Unlock()
			replyText := fmt.Sprintf("no queue '%s' in vhost '/'", actualQueueName)
			c.server.Info("Passive Queue.Declare: Queue '%s' not found. Sending Channel.Close.", actualQueueName)
			// AMQP code 404 (NOT_FOUND)
			return c.sendChannelClose(channelId, 404, replyText, uint16(ClassQueue), MethodQueueDeclare)
		}
		// Passive and exists: check compatibility.
		q.mu.RLock()
		// Per your original logic: if it's exclusive, it's a 405.
		// This implies an attempt to use/check an exclusive queue owned by another connection.
		// If your server tracked ownerChannel, a more nuanced check could be done here.
		if q.Exclusive {
			q.mu.RUnlock()
			vhost.mu.Unlock()
			replyText := fmt.Sprintf("queue '%s' is exclusive", actualQueueName)
			c.server.Warn("Passive Queue.Declare: %s. Sending Channel.Close.", replyText)
			// AMQP code 405 (RESOURCE_LOCKED)
			return c.sendChannelClose(channelId, 405, replyText, uint16(ClassQueue), MethodQueueDeclare)
		}
		messageCount = uint32(len(q.Messages))
		consumerCount = uint32(len(q.Consumers))
		q.mu.RUnlock()
		c.server.Info("Queue '%s' already exists (passive declare). Messages: %d, Consumers: %d. Durable: %v, Exclusive: %v",
			actualQueueName, messageCount, consumerCount, q.Durable, q.Exclusive)
		// For passive declare, if it exists and isn't an immediate RESOURCE_LOCKED,
		// the server should check if other properties like durable, auto-delete, arguments match.
		// If they don't, it's a 406 PRECONDITION_FAILED.
		// Your original code didn't have this extra check for passive, so I'll stick to that for now,
		// but a fully compliant server would add it. The main thing for passive is type match for exchanges.
		// For queues, it's existence and non-exclusive access.

	} else { // Not passive: declare or re-declare.
		if exists {
			q.mu.RLock()
			// Per your original logic:
			// 1. Check if it's an exclusive queue (implies owned by another if we don't track owner) -> 405
			if q.Exclusive { // This implies an attempt to re-declare an existing exclusive queue.
				// If this connection *is* the owner, this check might be too strict without ownerChannel tracking.
				// However, if it *is* the owner, and tries to change 'exclusive' from true to false, that's a 406.
				q.mu.RUnlock()
				vhost.mu.Unlock()
				replyText := fmt.Sprintf("queue '%s' is exclusive and cannot be redeclared by this connection or with changed exclusive status", actualQueueName)
				c.server.Warn("Queue.Declare: %s. Sending Channel.Close.", replyText)
				// AMQP code 405 (RESOURCE_LOCKED) if trying to access another's exclusive queue.
				// AMQP code 406 (PRECONDITION_FAILED) if owner tries to change 'exclusive' flag.
				// Given no owner tracking, 405 is a safe bet if q.Exclusive is true.
				return c.sendChannelClose(channelId, 405, replyText, uint16(ClassQueue), MethodQueueDeclare)
			}

			// 2. If not q.Exclusive, check for other property mismatches -> 406
			// Your original check: q.Durable != durable || q.Exclusive != exclusive
			// This covers changing durable, or changing exclusive from false to true, or true to false (if it passed the first q.Exclusive check).
			// Also consider autoDelete and arguments for full compliance.
			propertiesMatch := (q.Durable == durable &&
				q.Exclusive == exclusive &&
				q.AutoDelete == autoDelete) // && areTablesEqual(q.Arguments, args)

			if !propertiesMatch {
				q.mu.RUnlock()
				vhost.mu.Unlock()
				replyText := fmt.Sprintf("properties mismatch for existing queue '%s' on redeclare", actualQueueName)
				c.server.Warn("Queue.Declare: %s. Sending Channel.Close. Existing(dur:%v, excl:%v, ad:%v), Req(dur:%v, excl:%v, ad:%v)",
					replyText, q.Durable, q.Exclusive, q.AutoDelete, durable, exclusive, autoDelete)
				// AMQP code 406 (PRECONDITION_FAILED)
				return c.sendChannelClose(channelId, 406, replyText, uint16(ClassQueue), MethodQueueDeclare)
			}
			// If properties match, it's a valid re-declaration.
			c.server.Info("Queue '%s' re-declared with matching properties.", actualQueueName)
			messageCount = uint32(len(q.Messages))
			consumerCount = uint32(len(q.Consumers))
			q.mu.RUnlock()
		} else { // Not passive and not exists: create it.
			vhost.queues[actualQueueName] = &Queue{
				Name:       actualQueueName,
				Messages:   []Message{},
				Bindings:   make(map[string]bool),
				Consumers:  make(map[string]*Consumer),
				Durable:    durable,
				Exclusive:  exclusive, // If exclusive, ideally track ownerChannelId = channelId
				AutoDelete: autoDelete,
				// Arguments: args, // If you store and use queue arguments
			}
			c.server.Info("Created new queue: '%s', durable=%v, exclusive=%v, autoDelete=%v",
				actualQueueName, durable, exclusive, autoDelete)
			messageCount = 0
			consumerCount = 0
		}
	}
	vhost.mu.Unlock()

	if !noWait {
		// If noWait is false, server MUST reply with Queue.DeclareOk.
		payloadOk := &bytes.Buffer{}
		binary.Write(payloadOk, binary.BigEndian, uint16(ClassQueue))
		binary.Write(payloadOk, binary.BigEndian, uint16(MethodQueueDeclareOk))
		writeShortString(payloadOk, actualQueueName)
		binary.Write(payloadOk, binary.BigEndian, messageCount)
		binary.Write(payloadOk, binary.BigEndian, consumerCount)

		if errWrite := c.writeFrame(&Frame{Type: FrameMethod, Channel: channelId, Payload: payloadOk.Bytes()}); errWrite != nil {
			c.server.Err("Error sending queue.declare-ok for queue '%s': %v", actualQueueName, errWrite)
			// This is an I/O error. The queue might have been created/state changed.
			return errWrite // Propagate I/O error
		}
		c.server.Info("Sent queue.declare-ok for queue '%s'", actualQueueName)
	} else {
		c.server.Info("Queue.Declare with noWait=true processed for '%s'. No DeclareOk sent.", actualQueueName)
	}
	return nil // Successfully processed queue.declare
}

func (c *Connection) handleQueueBind(reader *bytes.Reader, channelId uint16) error {
	// Parse all arguments first
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.bind (ticket)", uint16(ClassQueue), MethodQueueBind)
	}

	queueName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueName for queue.bind: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.bind (queueName)", uint16(ClassQueue), MethodQueueBind)
	}

	exchangeName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading exchangeName for queue.bind: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.bind (exchangeName)", uint16(ClassQueue), MethodQueueBind)
	}

	routingKey, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading routingKey for queue.bind: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.bind (routingKey)", uint16(ClassQueue), MethodQueueBind)
	}

	bits, errReadByte := reader.ReadByte()
	if errReadByte != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.bind (bits)", uint16(ClassQueue), MethodQueueBind)
	}

	argsBind, errReadTableBind := readTable(reader)
	if errReadTableBind != nil {
		c.server.Err("Error reading arguments table for queue.bind (queue: '%s', exchange: '%s'): %v", queueName, exchangeName, errReadTableBind)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed arguments table for queue.bind", uint16(ClassQueue), MethodQueueBind)
	}

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of queue.bind payload for queue '%s'.", queueName)
	}

	noWait := (bits & 0x01) != 0

	c.server.Info("Processing queue.bind: queue='%s', exchange='%s', routingKey='%s', noWait=%v, args=%v on channel %d",
		queueName, exchangeName, routingKey, noWait, argsBind, channelId)

	vhost := c.vhost

	// Hold read lock for entire operation to prevent TOCTOU race
	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	// Check if vhost is being deleted
	if vhost.IsDeleting() {
		c.server.Info("Queue.Bind: VHost is being deleted, cannot bind queue '%s'", queueName)
		return c.sendConnectionClose(320, "VHost deleted", uint16(ClassQueue), MethodQueueBind)
	}

	ex, exExists := vhost.exchanges[exchangeName]
	q, qExists := vhost.queues[queueName]

	if !exExists {
		replyText := fmt.Sprintf("no exchange '%s' in vhost '/'", exchangeName)
		c.server.Warn("Queue.Bind failed: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, 404, replyText, uint16(ClassQueue), MethodQueueBind)
	}

	if !qExists {
		replyText := fmt.Sprintf("no queue '%s' in vhost '/'", queueName)
		c.server.Warn("Queue.Bind failed: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, 404, replyText, uint16(ClassQueue), MethodQueueBind)
	}

	// Check if queue is being deleted
	if q.deleting.Load() {
		replyText := fmt.Sprintf("queue '%s' is being deleted", queueName)
		return c.sendChannelClose(channelId, 409, replyText, uint16(ClassQueue), MethodQueueBind)
	}

	// Use consistent lock ordering: always exchange first, then queue
	ex.mu.Lock()
	q.mu.Lock()

	// Double-check queue deletion status while holding locks
	if q.deleting.Load() {
		q.mu.Unlock()
		ex.mu.Unlock()
		replyText := fmt.Sprintf("queue '%s' is being deleted", queueName)
		return c.sendChannelClose(channelId, 409, replyText, uint16(ClassQueue), MethodQueueBind)
	}

	// Perform binding atomically on both structures
	alreadyBound := false
	for _, existingQueueName := range ex.Bindings[routingKey] {
		if existingQueueName == queueName {
			alreadyBound = true
			break
		}
	}

	if !alreadyBound {
		ex.Bindings[routingKey] = append(ex.Bindings[routingKey], queueName)
		q.Bindings[exchangeName+":"+routingKey] = true
		c.server.Info("Bound exchange '%s' (type: %s) to queue '%s' with routing key '%s'",
			exchangeName, ex.Type, queueName, routingKey)
	} else {
		c.server.Info("Binding already exists for exchange '%s' to queue '%s' with routing key '%s'",
			exchangeName, queueName, routingKey)
	}

	q.mu.Unlock()
	ex.mu.Unlock()

	if !noWait {
		payloadBindOk := &bytes.Buffer{}
		binary.Write(payloadBindOk, binary.BigEndian, uint16(ClassQueue))
		binary.Write(payloadBindOk, binary.BigEndian, uint16(MethodQueueBindOk))

		if errWrite := c.writeFrame(&Frame{Type: FrameMethod, Channel: channelId, Payload: payloadBindOk.Bytes()}); errWrite != nil {
			c.server.Err("Error sending queue.bind-ok for queue '%s': %v", queueName, errWrite)
			return errWrite
		}
		c.server.Info("Sent queue.bind-ok for queue '%s'", queueName)
	}

	return nil
}

func (c *Connection) handleQueueDelete(reader *bytes.Reader, channelId uint16) error {
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.delete (ticket)", uint16(ClassQueue), MethodQueueDelete)
	}

	queueName, err := readShortString(reader)
	if err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.delete (queueName)", uint16(ClassQueue), MethodQueueDelete)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.delete (bits)", uint16(ClassQueue), MethodQueueDelete)
	}

	ifUnused := (bits & 0x01) != 0
	ifEmpty := (bits & 0x02) != 0
	noWait := (bits & 0x04) != 0

	if reader.Len() > 0 {
	}

	c.server.Info("Processing queue.delete: queue='%s', ifUnused=%v, ifEmpty=%v, noWait=%v on channel %d",
		queueName, ifUnused, ifEmpty, noWait, channelId)

	// Phase 1: Atomic check-and-mark for deletion
	vhost := c.vhost
	vhost.mu.RLock()
	queue, exists := vhost.queues[queueName]
	vhost.mu.RUnlock()

	if !exists {
		replyText := fmt.Sprintf("no queue '%s' in vhost '/'", queueName)
		return c.sendChannelClose(channelId, 404, replyText, uint16(ClassQueue), MethodQueueDelete)
	}

	// Atomically mark queue for deletion
	if !queue.deleting.CompareAndSwap(false, true) {
		// Queue is already being deleted by another operation
		replyText := fmt.Sprintf("queue '%s' is already being deleted", queueName)
		return c.sendChannelClose(channelId, 409, replyText, uint16(ClassQueue), MethodQueueDelete)
	}

	// From this point, we own the deletion process
	defer func() {
		// Reset deletion flag if we don't complete successfully
		if queue.deleting.Load() {
			queue.deleting.Store(false)
		}
	}()

	// Phase 2: Validate deletion conditions under lock
	queue.mu.Lock()

	consumerCount := len(queue.Consumers)
	messageCount := uint32(len(queue.Messages))

	if ifUnused && consumerCount > 0 {
		queue.mu.Unlock()
		queue.deleting.Store(false)
		replyText := fmt.Sprintf("queue '%s' in use (has %d consumers)", queueName, consumerCount)
		return c.sendChannelClose(channelId, 406, replyText, uint16(ClassQueue), MethodQueueDelete)
	}

	if ifEmpty && messageCount > 0 {
		queue.mu.Unlock()
		queue.deleting.Store(false)
		replyText := fmt.Sprintf("queue '%s' not empty (has %d messages)", queueName, messageCount)
		return c.sendChannelClose(channelId, 406, replyText, uint16(ClassQueue), MethodQueueDelete)
	}

	// --- SNAPSHOT consumers and bindings BEFORE clearing ---
	queueBindings := make([]string, 0, len(queue.Bindings))
	for binding := range queue.Bindings {
		queueBindings = append(queueBindings, binding)
	}

	consumersSnapshot := make(map[string]*Consumer, len(queue.Consumers))
	for tag, consumer := range queue.Consumers {
		consumersSnapshot[tag] = consumer
	}

	// --- ALSO snapshot all channel->consumerTag->queueName BEFORE clearing for channel cleanup ---
	c.mu.RLock()
	channelConsumersSnapshot := make(map[uint16]map[string]string)
	for channelId, ch := range c.channels {
		ch.mu.Lock()
		m := make(map[string]string, len(ch.consumers))
		for tag, qName := range ch.consumers {
			m[tag] = qName
		}
		ch.mu.Unlock()
		channelConsumersSnapshot[channelId] = m
	}
	c.mu.RUnlock()

	deletedMessageCount := messageCount

	// Clear queue state immediately
	queue.Messages = nil
	queue.Consumers = make(map[string]*Consumer)
	queue.Bindings = make(map[string]bool)

	queue.mu.Unlock()

	// Phase 3: Coordinated cleanup without holding queue lock
	var cleanupWG sync.WaitGroup

	// Send Basic.Cancel messages first
	cleanupWG.Add(1)
	go func() {
		defer cleanupWG.Done()
		c.sendBasicCancelToConsumers(consumersSnapshot, queueName)
	}()

	// Clean up channel consumer mappings
	cleanupWG.Add(1)
	go func() {
		defer cleanupWG.Done()
		// Use the snapshot for channel consumer cleanup
		for channelId, tagMap := range channelConsumersSnapshot {
			ch := c.channels[channelId]
			if ch == nil {
				continue
			}
			ch.mu.Lock()
			modified := false
			for consumerTag, qName := range tagMap {
				if qName == queueName {
					delete(ch.consumers, consumerTag)
					modified = true
				}
			}
			ch.mu.Unlock()
			if modified {
				c.server.Debug("Removed consumers from channel %d for deleted queue '%s'", channelId, queueName)
			}
		}
	}()

	// Clean up exchange bindings
	cleanupWG.Add(1)
	go func() {
		defer cleanupWG.Done()
		c.cleanupExchangeBindings(queueName, queueBindings)
	}()

	// Wait for all cleanup operations with timeout
	done := make(chan struct{})
	go func() {
		cleanupWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.server.Info("All cleanup operations completed for queue '%s'", queueName)
	case <-time.After(5 * time.Second):
		c.server.Warn("Timeout during cleanup operations for queue '%s' - proceeding anyway", queueName)
	}

	// Phase 4: Remove from vhost and finalize
	vhost.mu.Lock()
	delete(vhost.queues, queueName)
	vhost.mu.Unlock()

	// Mark deletion as complete
	queue.deleting.Store(false)

	c.server.Info("Successfully deleted queue '%s' with %d messages", queueName, deletedMessageCount)

	if !noWait {
		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassQueue))
		binary.Write(payload, binary.BigEndian, uint16(MethodQueueDeleteOk))
		binary.Write(payload, binary.BigEndian, deletedMessageCount)

		if err := c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channelId,
			Payload: payload.Bytes(),
		}); err != nil {
			c.server.Err("Error sending queue.delete-ok for queue '%s': %v", queueName, err)
			return err
		}
	}

	return nil
}

func (c *Connection) handleQueueUnbind(reader *bytes.Reader, channelId uint16) error {
	// Parse all arguments first
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.unbind (ticket)", uint16(ClassQueue), MethodQueueUnbind)
	}

	queueName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueName for queue.unbind: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.unbind (queueName)", uint16(ClassQueue), MethodQueueUnbind)
	}

	exchangeName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading exchangeName for queue.unbind: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.unbind (exchangeName)", uint16(ClassQueue), MethodQueueUnbind)
	}

	routingKey, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading routingKey for queue.unbind: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.unbind (routingKey)", uint16(ClassQueue), MethodQueueUnbind)
	}

	argsUnbind, errReadTableUnbind := readTable(reader)
	if errReadTableUnbind != nil {
		c.server.Err("Error reading arguments table for queue.unbind (queue: '%s', exchange: '%s'): %v", queueName, exchangeName, errReadTableUnbind)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed arguments table for queue.unbind", uint16(ClassQueue), MethodQueueUnbind)
	}

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of queue.unbind payload for queue '%s'.", queueName)
	}

	c.server.Info("Processing queue.unbind: queue='%s', exchange='%s', routingKey='%s', args=%v on channel %d",
		queueName, exchangeName, routingKey, argsUnbind, channelId)

	vhost := c.vhost

	// Hold read lock for entire operation to prevent TOCTOU race
	vhost.mu.RLock()
	defer vhost.mu.RUnlock()

	// Check if vhost is being deleted
	if vhost.IsDeleting() {
		c.server.Info("Queue.Unbind: VHost is being deleted, cannot unbind queue '%s'", queueName)
		return c.sendConnectionClose(320, "VHost deleted", uint16(ClassQueue), MethodQueueUnbind)
	}

	ex, exExists := vhost.exchanges[exchangeName]
	q, qExists := vhost.queues[queueName]

	if !exExists {
		replyText := fmt.Sprintf("no exchange '%s' in vhost '/'", exchangeName)
		c.server.Warn("Queue.Unbind failed: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, 404, replyText, uint16(ClassQueue), MethodQueueUnbind)
	}

	if !qExists {
		replyText := fmt.Sprintf("no queue '%s' in vhost '/'", queueName)
		c.server.Warn("Queue.Unbind failed: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, 404, replyText, uint16(ClassQueue), MethodQueueUnbind)
	}

	// Check if queue is being deleted
	if q.deleting.Load() {
		replyText := fmt.Sprintf("queue '%s' is being deleted", queueName)
		return c.sendChannelClose(channelId, 409, replyText, uint16(ClassQueue), MethodQueueUnbind)
	}

	// Use consistent lock ordering: always exchange first, then queue
	ex.mu.Lock()
	q.mu.Lock()

	// Double-check queue deletion status while holding locks
	if q.deleting.Load() {
		q.mu.Unlock()
		ex.mu.Unlock()
		replyText := fmt.Sprintf("queue '%s' is being deleted", queueName)
		return c.sendChannelClose(channelId, 409, replyText, uint16(ClassQueue), MethodQueueUnbind)
	}

	// Remove binding atomically from both structures
	bindingKey := exchangeName + ":" + routingKey
	bindingRemoved := false

	// Remove from exchange bindings
	if boundQueues, exists := ex.Bindings[routingKey]; exists {
		newQueues := make([]string, 0, len(boundQueues))
		for _, boundQueue := range boundQueues {
			if boundQueue != queueName {
				newQueues = append(newQueues, boundQueue)
			} else {
				bindingRemoved = true
			}
		}
		if len(newQueues) > 0 {
			ex.Bindings[routingKey] = newQueues
		} else {
			delete(ex.Bindings, routingKey)
		}
	}

	// Remove from queue bindings
	if q.Bindings[bindingKey] {
		delete(q.Bindings, bindingKey)
		bindingRemoved = true
	}

	q.mu.Unlock()
	ex.mu.Unlock()

	if bindingRemoved {
		c.server.Info("Unbound queue '%s' from exchange '%s' with routing key '%s'", queueName, exchangeName, routingKey)
	} else {
		c.server.Info("No binding found to remove for queue '%s', exchange '%s', routing key '%s'", queueName, exchangeName, routingKey)
	}

	// Send Queue.UnbindOk
	payloadUnbindOk := &bytes.Buffer{}
	binary.Write(payloadUnbindOk, binary.BigEndian, uint16(ClassQueue))
	binary.Write(payloadUnbindOk, binary.BigEndian, uint16(MethodQueueUnbindOk))

	if errWrite := c.writeFrame(&Frame{Type: FrameMethod, Channel: channelId, Payload: payloadUnbindOk.Bytes()}); errWrite != nil {
		c.server.Err("Error sending queue.unbind-ok for queue '%s': %v", queueName, errWrite)
		return errWrite
	}
	c.server.Info("Sent queue.unbind-ok for queue '%s'", queueName)

	return nil
}

func (c *Connection) handleQueuePurge(reader *bytes.Reader, channelId uint16) error {
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.purge (ticket)", uint16(ClassQueue), MethodQueuePurge)
	}

	queueName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueName for queue.purge: %v", err)
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.purge (queueName)", uint16(ClassQueue), MethodQueuePurge)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.purge (bits)", uint16(ClassQueue), MethodQueuePurge)
	}

	noWait := (bits & 0x01) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of queue.purge payload.")
	}

	c.server.Info("Processing queue.purge: queue='%s', noWait=%v on channel %d", queueName, noWait, channelId)

	// Get the vhost and queue
	vhost := c.vhost
	vhost.mu.RLock()
	queue, exists := vhost.queues[queueName]
	vhost.mu.RUnlock()

	if !exists {
		replyText := fmt.Sprintf("no queue '%s' in vhost '/'", queueName)
		c.server.Warn("Queue.Purge: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, 404, replyText, uint16(ClassQueue), MethodQueuePurge)
	}

	// Atomically check if queue is being deleted
	if queue.deleting.Load() {
		replyText := fmt.Sprintf("queue '%s' is being deleted", queueName)
		return c.sendChannelClose(channelId, 409, replyText, uint16(ClassQueue), MethodQueuePurge)
	}

	// Purge the queue
	queue.mu.Lock()

	// Double-check deletion status while holding lock
	if queue.deleting.Load() {
		queue.mu.Unlock()
		replyText := fmt.Sprintf("queue '%s' is being deleted", queueName)
		return c.sendChannelClose(channelId, 409, replyText, uint16(ClassQueue), MethodQueuePurge)
	}

	// Check if vhost is being deleted
	if vhost.IsDeleting() {
		queue.mu.Unlock()
		c.server.Info("Queue.Purge: VHost is being deleted, cannot purge queue '%s'", queueName)
		return c.sendConnectionClose(320, "VHost deleted", uint16(ClassQueue), MethodQueuePurge)
	}

	messageCount := uint32(len(queue.Messages))

	// Clear all messages
	if messageCount > 0 {
		queue.Messages = []Message{}
		c.server.Info("Purged %d messages from queue '%s'", messageCount, queueName)
	} else {
		c.server.Info("Queue '%s' was already empty (purge had no effect)", queueName)
	}

	queue.mu.Unlock()

	// Send PurgeOk if not no-wait
	if !noWait {

		if err := c.sendPurgeOk(channelId, messageCount); err != nil {
			c.server.Err("Error sending queue.purge-ok for queue '%s': %v", queueName, err)
			return err
		}
		c.server.Info("Sent queue.purge-ok for queue '%s' (purged %d messages)", queueName, messageCount)
	}

	return nil
}

func (c *Connection) sendPurgeOk(channelId uint16, messageCount uint32) error {
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassQueue))
	binary.Write(payload, binary.BigEndian, uint16(MethodQueuePurgeOk))
	binary.Write(payload, binary.BigEndian, messageCount)

	return c.writeFrame(&Frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	})
}

// deliverToQueue handles delivery to a single queue
func (c *Connection) deliverToQueue(queueName string, msg Message) {
	vhost := c.vhost
	if vhost == nil || vhost.IsDeleting() {
		return
	}

	vhost.mu.RLock()
	queue, exists := vhost.queues[queueName]
	vhost.mu.RUnlock()

	if !exists || queue == nil {
		return
	}

	queue.mu.Lock()
	msgCopy := msg.DeepCopy()
	queue.Messages = append(queue.Messages, *msgCopy)
	queue.mu.Unlock()

	c.server.Info("Message enqueued to queue '%s'. Queue now has %d messages.",
		queueName, len(queue.Messages))
}

func (c *Connection) deliverMessages(channelId uint16, consumerTag string, consumer *Consumer) {
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

		// Get the first message
		msg := queue.Messages[0]
		queue.Messages = queue.Messages[1:]
		queue.mu.Unlock()

		// Check if vhost is being deleted
		if c.vhost != nil && c.vhost.IsDeleting() {
			// Put message back at front of queue
			queue.mu.Lock()
			queue.Messages = append([]Message{msg}, queue.Messages...)
			queue.mu.Unlock()
			c.server.Info("VHost deletion detected, stopping delivery for consumer %s", consumerTag)
			return
		}

		// Get channel and check if it's still valid
		ch, exists, isClosing := c.getChannel(channelId)
		if !exists || isClosing {
			// Put message back at front of queue
			queue.mu.Lock()
			queue.Messages = append([]Message{msg}, queue.Messages...)
			queue.mu.Unlock()
			c.server.Info("Channel %d no longer valid, stopping delivery for consumer %s", channelId, consumerTag)
			return
		}

		// Assign delivery tag and track if not noAck
		ch.mu.Lock()
		ch.deliveryTag++
		deliveryTag := ch.deliveryTag

		if !noAck {
			unacked := &UnackedMessage{
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
			queue.Messages = append([]Message{msg}, queue.Messages...)
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
		} else {
			c.server.Err("Failed to fully deliver message for deliveryTag=%d", deliveryTag)

			// Put message back and clean up unacked tracking
			queue.mu.Lock()
			queue.Messages = append([]Message{msg}, queue.Messages...)
			queue.mu.Unlock()

			if !noAck {
				ch.mu.Lock()
				delete(ch.unackedMessages, deliveryTag)
				ch.mu.Unlock()
			}
		}
	}
}

func (c *Connection) cleanupConnectionResources() {
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
					queue.Messages = append([]Message{unacked.Message}, queue.Messages...)
					c.server.Debug("Prepared message (tag %d from channel %d) for requeue to queue '%s'", deliveryTag, chanId, unacked.QueueName)
					affectedQueuesForDispatch[unacked.QueueName] = true // Mark queue for dispatch
					queue.mu.Unlock()                                   // Unlock queue
				} else {
					c.server.Warn("Queue '%s' not found for requeuing unacked message (tag %d from channel %d) during connection cleanup", unacked.QueueName, deliveryTag, chanId)
				}
			}
			// Clear unacked messages for the channel
			ch.unackedMessages = make(map[uint64]*UnackedMessage)
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
	c.channels = map[uint16]*Channel{} // Clear the channels map on the connection
	c.mu.Unlock()                      // Unlock the connection

	c.server.Info("Finished cleaning up resources for connection %s", c.conn.RemoteAddr())
	// The underlying c.conn will be closed by the caller of cleanupConnectionResources or was already closed.
}

// Helper method to clean up queue bindings when an exchange is deleted
func (c *Connection) cleanupQueueBindingsForExchange(vhost *VHost, exchangeName string) {
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
func (c *Connection) sendChannelClose(channelId uint16, replyCode uint16, replyText string, offendingClassId uint16, offendingMethodId uint16) error {
	c.server.Warn("Sending Channel.Close on channel %d: code=%d, text='%s', class=%d, method=%d",
		channelId, replyCode, replyText, offendingClassId, offendingMethodId)

	payload := &bytes.Buffer{}
	// Ensure binary.Write errors are checked and handled ---
	if err := binary.Write(payload, binary.BigEndian, uint16(ClassChannel)); err != nil {
		// This is a server-side serialization error, very unlikely but good to be aware of
		c.server.Err("Internal error serializing ClassChannel for Channel.Close: %v", err)
		// Fallback to just closing connection if we can't even form the message
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
	}
	if err := binary.Write(payload, binary.BigEndian, uint16(MethodChannelClose)); err != nil {
		c.server.Err("Internal error serializing MethodChannelClose for Channel.Close: %v", err)
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
	}
	if err := binary.Write(payload, binary.BigEndian, replyCode); err != nil {
		c.server.Err("Internal error serializing replyCode for Channel.Close: %v", err)
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
	}
	writeShortString(payload, replyText) // Assuming writeShortString doesn't error or handles internally
	if err := binary.Write(payload, binary.BigEndian, offendingClassId); err != nil {
		c.server.Err("Internal error serializing offendingClassId for Channel.Close: %v", err)
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
	}
	if err := binary.Write(payload, binary.BigEndian, offendingMethodId); err != nil {
		c.server.Err("Internal error serializing offendingMethodId for Channel.Close: %v", err)
		return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
	}

	frame := &Frame{
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
func (c *Connection) forceRemoveChannel(channelId uint16, reason string) {
	c.server.Info("Forcibly removing channel %d from connection %s. Reason: %s", channelId, c.conn.RemoteAddr(), reason)

	vhost := c.vhost

	// Phase 1: Get channel reference and mark as closing
	c.mu.Lock()
	ch, exists := c.channels[channelId]
	if !exists || ch == nil {
		c.mu.Unlock()
		c.server.Info("Attempted to forcibly remove channel %d, but it was already gone or nil. Reason: %s", channelId, reason)
		return
	}

	// Don't remove from map yet - just mark as closing
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

	// Phase 2: Clean up channel resources (channel still in map)
	ch.mu.Lock()

	// Stop timer first
	if ch.closeOkTimer != nil {
		if !ch.closeOkTimer.Stop() {
			c.server.Debug("CloseOkTimer for channel %d had already fired", channelId)
		}
		ch.closeOkTimer = nil
	}

	// Collect queue names that will need dispatch attempts
	affectedQueuesForDispatch := make(map[string]bool)

	// Cleanup unacked messages
	if len(ch.unackedMessages) > 0 {
		c.server.Info("Channel %d has %d unacked messages, requeuing", channelId, len(ch.unackedMessages))
		for deliveryTag, unacked := range ch.unackedMessages {
			vhost.mu.RLock()
			queue, qExists := vhost.queues[unacked.QueueName]
			vhost.mu.RUnlock()

			if qExists && queue != nil {
				unacked.Message.Redelivered = true
				queue.mu.Lock()
				queue.Messages = append([]Message{unacked.Message}, queue.Messages...)
				c.server.Debug("Prepared message (tag %d from channel %d) for requeue to queue '%s'",
					deliveryTag, channelId, unacked.QueueName)
				affectedQueuesForDispatch[unacked.QueueName] = true
				queue.mu.Unlock()
			} else {
				c.server.Warn("Queue '%s' not found for requeuing unacked message (tag %d from channel %d)",
					unacked.QueueName, deliveryTag, channelId)
			}
		}
		ch.unackedMessages = make(map[uint64]*UnackedMessage)
	}

	// Handle pending confirms
	if len(ch.pendingConfirms) > 0 {
		c.server.Info("Channel %d has %d pending confirms, sending nacks", channelId, len(ch.pendingConfirms))
		// Send nack for all pending confirms
		var maxTag uint64
		for tag := range ch.pendingConfirms {
			if tag > maxTag {
				maxTag = tag
			}
		}
		if maxTag > 0 {
			// Send a single nack with multiple=true to cover all pending
			c.sendBasicNack(channelId, maxTag, true, false)
		}
		ch.pendingConfirms = make(map[uint64]bool)
	}

	// Clean up consumers
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

	// Phase 3: Remove from connection map after cleanup is complete
	c.mu.Lock()
	delete(c.channels, channelId)
	c.mu.Unlock()

	c.server.Info("Finished forcibly removing channel %d. Reason: %s", channelId, reason)
}

// sendConnectionClose sends a Connection.Close method to the client.
// It does NOT close the underlying net.Conn immediately.
// The server should then expect a Connection.CloseOk from the client.
func (c *Connection) sendConnectionClose(replyCode uint16, replyText string, offendingClassId uint16, offendingMethodId uint16) error {
	c.server.Warn("Sending Connection.Close: code=%d, text='%s', class=%d, method=%d",
		replyCode, replyText, offendingClassId, offendingMethodId)

	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
	binary.Write(payload, binary.BigEndian, uint16(MethodConnectionClose))
	binary.Write(payload, binary.BigEndian, replyCode)
	writeShortString(payload, replyText)
	binary.Write(payload, binary.BigEndian, offendingClassId)
	binary.Write(payload, binary.BigEndian, offendingMethodId)

	frame := &Frame{
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

func (c *Connection) sendBasicReturn(channelId uint16, replyCode uint16, replyText string, exchange string, routingKey string) error {
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

	return c.writeFrame(&Frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	})
}

// Helper method to clean up exchange bindings
func (c *Connection) cleanupExchangeBindings(queueName string, bindings []string) {
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
func (c *Connection) getChannel(channelId uint16) (*Channel, bool, bool) {
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

func main() {
	server := NewServer()
	server.Info("Starting AMQP server")
	if err := server.Start(":5672"); err != nil {
		server.Err("Failed to start server: %v", err)
		os.Exit(1)
	}
}
