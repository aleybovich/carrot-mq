package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const VERSION = "0.0.1"

// How log to wait until cleanup if channel Close Ok not received from client
// TODO: make server config value
const channelCloseOkTimeout = 100 * time.Millisecond

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
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
	Properties Properties
	Body       []byte
}

type Queue struct {
	Name       string
	Messages   []Message
	Bindings   map[string]bool
	Consumers  map[string]chan Message
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	mu         sync.RWMutex
}

type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	Bindings   map[string][]string
	mu         sync.RWMutex
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
}

type Connection struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	channels map[uint16]*Channel
	server   *Server
	mu       sync.RWMutex
	writeMu  sync.Mutex

	username string // Store authenticated username
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
	listener     net.Listener
	exchanges    map[string]*Exchange
	queues       map[string]*Queue
	logger       *log.Logger // Internal logger for formatting output
	customLogger Logger      // External logger interface, if provided
	mu           sync.RWMutex

	// Authentication fields
	authMode    AuthMode
	credentials map[string]string // username -> password
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
		s.logger.Printf(prefix+format, args...)
	} else {
		s.logger.Printf("[FATAL] %s: "+format, append([]interface{}{funcName}, args...)...)
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
		s.logger.Printf(prefix+format, args...)
	} else {
		s.logger.Printf("[ERROR] %s: "+format, append([]interface{}{funcName}, args...)...)
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
		s.logger.Printf(prefix+format, args...)
	} else {
		s.logger.Printf("[WARN] %s: "+format, append([]interface{}{funcName}, args...)...)
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
		s.logger.Printf(prefix+format, args...)
	} else {
		s.logger.Printf("[INFO] %s: "+format, append([]interface{}{funcName}, args...)...)
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
		s.logger.Printf(prefix+format, args...)
	} else {
		s.logger.Printf("[DEBUG] %s: "+format, append([]interface{}{funcName}, args...)...)
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
		exchanges: make(map[string]*Exchange),
		queues:    make(map[string]*Queue),
		logger:    log.New(os.Stdout, logPrefix, log.LstdFlags|log.Lmicroseconds),
	}

	// Apply all provided options
	for _, opt := range opts {
		opt(s)
	}

	// If no custom logger is provided, use the server itself as the logger
	if s.customLogger == nil {
		s.customLogger = s
	}

	// Create default direct exchange
	s.exchanges[""] = &Exchange{
		Name:     "",
		Type:     "direct",
		Bindings: make(map[string][]string),
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

func (s *Server) handleConnection(conn net.Conn) {
	// defer conn.Close() // Explicitly closed now

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
			return                         // Exit handleConnection goroutine
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
		return nil, fmt.Errorf("invalid frame end byte: %v", frameEnd[0])
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

	methodName := getFullMethodName(classId, methodId)
	if isTerminal {
		highlightedMethod := fmt.Sprintf("%s%s%s", colorYellow, methodName, colorReset)
		c.server.Info("Handling AMQP method: %s on channel=%d", highlightedMethod, frame.Channel)
	} else {
		c.server.Info("Handling AMQP method: %s, channel=%d", methodName, frame.Channel)
	}

	var err error
	switch classId {
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

		mechanism := readShortString(reader) // Auth mechanism
		response := readLongString(reader)   // Auth credentials
		_ = readShortString(reader)          // locale

		// Check for remaining bytes, indicates malformed arguments
		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of connection.start-ok payload.")
			return c.sendConnectionClose(502, "extra data in connection.start-ok", uint16(ClassConnection), MethodConnectionStartOk)
		}

		// Handle authentication based on server auth mode
		if c.server.authMode == AuthModePlain {
			if mechanism != "PLAIN" {
				c.server.Err("Unsupported authentication mechanism: %s", mechanism)
				return c.sendConnectionClose(506, fmt.Sprintf("unsupported mechanism '%s'", mechanism), uint16(ClassConnection), MethodConnectionStartOk)
			}

			// Parse PLAIN authentication response
			if len(response) < 2 {
				c.server.Err("Invalid PLAIN authentication response length")
				return c.sendConnectionClose(501, "invalid authentication response", uint16(ClassConnection), MethodConnectionStartOk)
			}

			parts := bytes.Split([]byte(response), []byte{0})
			if len(parts) != 3 {
				c.server.Err("Invalid PLAIN authentication response format")
				return c.sendConnectionClose(501, "invalid authentication response format", uint16(ClassConnection), MethodConnectionStartOk)
			}

			username := string(parts[1])
			password := string(parts[2])

			// Validate credentials
			if storedPass, ok := c.server.credentials[username]; !ok || storedPass != password {
				c.server.Err("Authentication failed for user: %s", username)
				return c.sendConnectionClose(403, "authentication failed", uint16(ClassConnection), MethodConnectionStartOk)
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
		var channelMax uint16
		var frameMax uint32
		var heartbeatInterval uint16 // Renamed from heartbeat to avoid confusion
		if err := binary.Read(reader, binary.BigEndian, &channelMax); err != nil {
			return c.sendConnectionClose(502, "malformed connection.tune-ok (channel-max)", uint16(ClassConnection), MethodConnectionTuneOk)
		}
		if err := binary.Read(reader, binary.BigEndian, &frameMax); err != nil {
			return c.sendConnectionClose(502, "malformed connection.tune-ok (frame-max)", uint16(ClassConnection), MethodConnectionTuneOk)
		}
		if err := binary.Read(reader, binary.BigEndian, &heartbeatInterval); err != nil {
			return c.sendConnectionClose(502, "malformed connection.tune-ok (heartbeat)", uint16(ClassConnection), MethodConnectionTuneOk)
		}
		if reader.Len() > 0 {
			return c.sendConnectionClose(502, "extra data in connection.tune-ok", uint16(ClassConnection), MethodConnectionTuneOk)
		}
		c.server.Info("Connection parameters negotiated: channelMax=%d, frameMax=%d, heartbeat=%d",
			channelMax, frameMax, heartbeatInterval)
		// Store these if your server needs to enforce them (e.g., c.channelMax = channelMax)
		return nil

	case MethodConnectionOpen:
		vhost := readShortString(reader)
		_ = readShortString(reader)         // capabilities
		_, errReadByte := reader.ReadByte() // insist bit
		if errReadByte != nil {
			return c.sendConnectionClose(502, "malformed connection.open (insist bit)", uint16(ClassConnection), MethodConnectionOpen)
		}
		if reader.Len() > 0 {
			return c.sendConnectionClose(502, "extra data in connection.open", uint16(ClassConnection), MethodConnectionOpen)
		}
		c.server.Info("Processing connection.%s for vhost: '%s'", methodName, vhost)

		// TODO: Implement actual vhost authorization/handling if necessary.
		// For now, we accept any vhost.

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
		binary.Write(payload, binary.BigEndian, uint16(MethodConnectionOpenOk))
		// AMQP 0-9-1 Connection.OpenOk has one field: known-hosts (shortstr), which "MUST be zero length".
		writeShortString(payload, "") // known-hosts

		err := c.writeFrame(&Frame{
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
		replyText := readShortString(reader)
		var classIdField, methodIdField uint16
		binary.Read(reader, binary.BigEndian, &classIdField)
		binary.Read(reader, binary.BigEndian, &methodIdField)

		if reader.Len() > 0 {
			// Client sent extra data, but we are closing anyway. Log it.
			c.server.Warn("Extra data in client's connection.close method.")
			// Don't send an error for this, just proceed with close.
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
	binary.Write(payload, binary.BigEndian, uint32(131072))

	// heartbeat: suggested heartbeat interval in seconds
	binary.Write(payload, binary.BigEndian, uint16(60))

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
	methodName := getMethodName(ClassChannel, methodId) // Ensure getMethodName is available

	if methodId == MethodChannelOpen {
		c.server.Info("Processing channel.%s for channel %d", methodName, channelId)
		// AMQP 0-9-1: reserved-1 (shortstr), "out-of-band, MUST be zero length string"
		// Read and discard "out-of-band" argument.
		_ = readShortString(reader) // Assuming readShortString correctly consumes the length byte even for empty string.
		// If not, this needs to be more careful.

		// Check for extra data after the single reserved argument.
		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of channel.open payload for channel %d.", channelId)
			// Channel.Open is fundamental; syntax error here often implies connection-level issue or badly formed client.
			return c.sendConnectionClose(502, "SYNTAX_ERROR - extra data in channel.open payload", uint16(ClassChannel), MethodChannelOpen)
		}

		c.mu.Lock() // Lock connection to modify c.channels
		if _, alreadyOpen := c.channels[channelId]; alreadyOpen {
			c.mu.Unlock()
			errMsg := fmt.Sprintf("channel %d already open", channelId)
			c.server.Err("Channel.Open error: %s", errMsg)
			// AMQP code 504 (CHANNEL_ERROR) - Channel ID already in use.
			// Server should send Channel.Close, not Connection.Close here.
			return c.sendChannelClose(channelId, 504, "CHANNEL_ERROR - channel ID already in use", uint16(ClassChannel), MethodChannelOpen)
		}
		// Create and add the new channel
		newCh := &Channel{
			id:              channelId,
			conn:            c,
			consumers:       make(map[string]string),
			pendingMessages: make([]Message, 0),
			// closingByServer, closeOkTimer will be zero/nil initially
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
		if err := binary.Write(payloadOpenOk, binary.BigEndian, uint32(0)); err != nil { // For the empty long string
			c.server.Err("Internal error serializing empty longstr for Channel.OpenOk: %v", err)
			c.forceRemoveChannel(channelId, "internal error sending channel.open-ok")
			return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
		}

		if err := c.writeFrame(&Frame{Type: FrameMethod, Channel: channelId, Payload: payloadOpenOk.Bytes()}); err != nil {
			c.server.Err("Error sending channel.open-ok for channel %d: %v", channelId, err)
			c.forceRemoveChannel(channelId, "failed to send channel.open-ok") // Clean up if OpenOk fails
			return err                                                        // Propagate I/O error
		}
		c.server.Info("Sent channel.open-ok for channel %d", channelId)
		return nil
	}

	// For all other methods, the channel MUST exist.
	c.mu.RLock() // RLock to check existence
	ch, existsInMap := c.channels[channelId]
	c.mu.RUnlock()

	if !existsInMap || ch == nil {
		errMsg := ""
		if !existsInMap {
			errMsg = fmt.Sprintf("received method %s for non-existent channel %d in map", methodName, channelId)
		} else { // ch == nil but existsInMap was true (internal inconsistency)
			errMsg = fmt.Sprintf("internal server error: channel %d object is nil in map for method %s", channelId, methodName)
		}
		c.server.Err(errMsg)
		// If client sends a command on a channel server doesn't know (or is broken), it's a connection error.
		// AMQP 504 CHANNEL_ERROR is appropriate.
		return c.sendConnectionClose(504, "CHANNEL_ERROR - "+errMsg, uint16(ClassChannel), methodId)
	}

	// Channel exists, proceed with method handling
	switch methodId {
	case MethodChannelClose: // Client initiates Channel.Close
		c.server.Info("Processing channel.%s for channel %d (client initiated)", methodName, channelId)
		var replyCode uint16

		if err := binary.Read(reader, binary.BigEndian, &replyCode); err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed channel.close (reply-code)", uint16(ClassChannel), MethodChannelClose)
		}
		replyText := readShortString(reader) // Assuming robust
		var classIdField, methodIdField uint16
		if err := binary.Read(reader, binary.BigEndian, &classIdField); err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed channel.close (class-id)", uint16(ClassChannel), MethodChannelClose)
		}
		if err := binary.Read(reader, binary.BigEndian, &methodIdField); err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed channel.close (method-id)", uint16(ClassChannel), MethodChannelClose)
		}

		if reader.Len() > 0 {
			c.server.Warn("Extra data in client's channel.close payload for channel %d.", channelId)
			// Server should still process the close but can send its own Channel.Close for syntax error if strict.
			// For simplicity here, we proceed with their close request.
		}

		c.server.Info("Client requests channel close for channel %d: replyCode=%d, replyText='%s', classId=%d, methodId=%d",
			channelId, replyCode, replyText, classIdField, methodIdField)

		// Server cleans up and sends Channel.CloseOk
		c.forceRemoveChannel(channelId, "client initiated Channel.Close")

		payloadCloseOk := &bytes.Buffer{}
		if err := binary.Write(payloadCloseOk, binary.BigEndian, uint16(ClassChannel)); err != nil {
			c.server.Err("Internal error serializing ClassChannel for Channel.CloseOk: %v", err)
			return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0) // Connection error if we can't form response
		}

		if err := binary.Write(payloadCloseOk, binary.BigEndian, uint16(MethodChannelCloseOk)); err != nil {
			c.server.Err("Internal error serializing MethodChannelCloseOk: %v", err)
			return c.sendConnectionClose(500, "INTERNAL_SERVER_ERROR", 0, 0)
		}

		if errWrite := c.writeFrame(&Frame{Type: FrameMethod, Channel: channelId, Payload: payloadCloseOk.Bytes()}); errWrite != nil {
			c.server.Err("Error sending channel.close-ok for client-initiated close on channel %d: %v", channelId, errWrite)
			// Channel already removed by forceRemoveChannel, propagate I/O error
			return errWrite
		}
		c.server.Info("Sent channel.close-ok for channel %d (client initiated)", channelId)
		return nil // Channel closed gracefully by client

	case MethodChannelCloseOk:
		c.server.Info("Received channel.close-ok for channel %d.", channelId)
		if reader.Len() > 0 {
			c.server.Warn("Extra data in channel.close-ok payload for channel %d.", channelId)
			// AMQP spec says the peer receiving an invalid frame MAY close the connection.
			// For a simple CloseOk with extra data, just logging might be acceptable.
		}

		// Logic to handle stopping the timer and checking channel state ---
		ch.mu.Lock() // Lock the specific channel
		timerStopped := false
		if ch.closeOkTimer != nil {
			timerStopped = ch.closeOkTimer.Stop() // Stop the timeout timer
			ch.closeOkTimer = nil                 // Clear the timer field
		}
		wasServerInitiated := ch.closingByServer
		ch.mu.Unlock() // Unlock the channel before calling forceRemoveChannel (which also locks connection)
		// --- END CHANGE ---

		if wasServerInitiated {
			if timerStopped {
				c.server.Info("Client acknowledged server-initiated close for channel %d within timeout. Finalizing.", channelId)
			} else {
				// This case implies the timer might have already fired OR CloseOk arrived for a channel not server-closed.
				c.server.Info("Client acknowledged server-initiated close for channel %d (timer might have already fired or was not set). Finalizing.", channelId)
			}
		} else {
			c.server.Warn("Received unsolicited or late channel.close-ok for channel %d (was not marked as server-initiated). Finalizing.", channelId)
		}
		// Final removal from map
		c.forceRemoveChannel(channelId, "received Channel.CloseOk")
		return nil

	default:
		// For other methods, check if channel is already being closed by server.
		ch.mu.Lock()
		isClosing := ch.closingByServer
		ch.mu.Unlock()

		if isClosing {
			c.server.Warn("Received method %s on channel %d that is already being closed by server. Ignoring.", methodName, channelId)
			// Client should not send further commands (except CloseOk) on a channel it received Channel.Close for.
			// If it does, server can ignore or send another Channel.Close. Ignoring is simpler.
			return nil // nil means the command is just dropped, aligned to AMQP 0-9-1 spec
		}

		// CHANGE: Fallback to a generic "not implemented" for other channel methods ---
		replyText := fmt.Sprintf("channel method %s (id %d) not implemented or invalid in this state", methodName, methodId)
		c.server.Err("Unhandled channel method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		return c.sendChannelClose(channelId, 540, replyText, uint16(ClassChannel), methodId) // 540 NOT_IMPLEMENTED
	}
}

func (c *Connection) handleExchangeMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassExchange, methodId)

	c.mu.RLock() // Check if channel exists
	_, channelExists := c.channels[channelId]
	c.mu.RUnlock()
	if !channelExists && channelId != 0 { // Channel 0 is special and doesn't need this check here
		// This is a critical error: operation on a non-existent channel.
		// AMQP spec is somewhat open, could be Channel.Close or Connection.Close.
		// Given the channel context is invalid, a Connection.Close is safer.
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for exchange operation", channelId)
		c.server.Err("Exchange method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		// AMQP code 503 (COMMAND_INVALID) or 504 (CHANNEL_ERROR)
		return c.sendConnectionClose(503, replyText, uint16(ClassExchange), methodId)
	}

	switch methodId {
	case MethodExchangeDeclare:
		// AMQP 0-9-1: ticket (short, reserved), exchange (shortstr), type (shortstr), passive (bit),
		// durable (bit), auto-delete (bit), internal (bit), no-wait (bit), arguments (table)
		var ticket uint16
		if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
			return fmt.Errorf("reading ticket for exchange.declare: %w", err)
		}
		exchangeName := readShortString(reader)
		exchangeType := readShortString(reader)
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
			return c.sendChannelClose(channelId, 502, "extra data in exchange.declare", uint16(ClassExchange), MethodExchangeDeclare)
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

		c.server.mu.Lock()
		ex, exists := c.server.exchanges[exchangeName]

		if passive {
			// Passive declare: check if exchange exists and is compatible.
			if !exists {
				c.server.mu.Unlock()
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
				c.server.mu.Unlock()
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
					c.server.mu.Unlock()
					replyText := fmt.Sprintf("cannot redeclare exchange '%s' with different properties", exchangeName)
					c.server.Warn("Exchange.Declare: %s. Sending Channel.Close. Existing: type=%s, dur=%v. Req: type=%s, dur=%v",
						replyText, ex.Type, ex.Durable, exchangeType, durable)
					// AMQP code 406 (PRECONDITION_FAILED)
					return c.sendChannelClose(channelId, 406, replyText, uint16(ClassExchange), MethodExchangeDeclare)
				}
				c.server.Info("Exchange '%s' re-declared with matching properties.", exchangeName)
			} else { // Not passive and not exists: create it.
				c.server.exchanges[exchangeName] = &Exchange{
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
		c.server.mu.Unlock()

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

	// case MethodExchangeDelete:
	//    // Future implementation: remember to handle 'if-unused' and 'noWait' bits.
	//    // AMQP: Channel.Close(540, "NOT_IMPLEMENTED", ...)
	//    return c.sendChannelClose(channelId, 540, "Exchange.Delete not implemented", uint16(ClassExchange), methodId)

	// case MethodExchangeBind:   // AMQP 0-9-1 Extension: exchange_exchange_bindings capability
	//    // Future implementation
	//    return c.sendChannelClose(channelId, 540, "Exchange.Bind (exchange-to-exchange) not implemented", uint16(ClassExchange), methodId)

	// case MethodExchangeUnbind: // AMQP 0-9-1 Extension: exchange_exchange_bindings capability
	//    // Future implementation
	//    return c.sendChannelClose(channelId, 540, "Exchange.Unbind (exchange-to-exchange) not implemented", uint16(ClassExchange), methodId)

	default:
		replyText := fmt.Sprintf("unknown or not implemented exchange method id %d", methodId)
		c.server.Err("Unhandled exchange method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		// AMQP code 540 (NOT_IMPLEMENTED) or 503 (COMMAND_INVALID)
		return c.sendChannelClose(channelId, 540, replyText, uint16(ClassExchange), methodId)
	}
}

func (c *Connection) handleQueueMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassQueue, methodId)

	c.mu.RLock() // Check if channel exists
	_, channelExists := c.channels[channelId]
	c.mu.RUnlock()
	if !channelExists && channelId != 0 {
		// This is a connection-level error as the channel context is invalid for the operation.
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for queue operation", channelId)
		c.server.Err("Queue method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		// AMQP code 503 (COMMAND_INVALID) or 504 (CHANNEL_ERROR) - 503 is general for bad command sequence.
		return c.sendConnectionClose(503, replyText, uint16(ClassQueue), methodId)
	}

	switch methodId {
	case MethodQueueDeclare:
		var ticket uint16
		if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
			// Malformed frame if basic fields cannot be read.
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.declare (ticket)", uint16(ClassQueue), MethodQueueDeclare)
		}
		queueNameIn := readShortString(reader) // Assuming readShortString is robust for now.
		// If it could fail mid-read, it would need to return an error.

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
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - extra data in queue.declare payload", uint16(ClassQueue), MethodQueueDeclare)
		}

		passive := (bits & 0x01) != 0
		durable := (bits & 0x02) != 0
		exclusive := (bits & 0x04) != 0
		autoDelete := (bits & 0x08) != 0 // Server doesn't fully implement auto-delete logic yet
		noWait := (bits & 0x10) != 0

		c.server.Info("Processing queue.%s: name='%s', passive=%v, durable=%v, exclusive=%v, autoDelete=%v, noWait=%v, args=%v on channel %d",
			methodName, queueNameIn, passive, durable, exclusive, autoDelete, noWait, args, channelId)

		var actualQueueName = queueNameIn
		var messageCount uint32 = 0
		var consumerCount uint32 = 0

		c.server.mu.Lock() // Lock server for queue operations

		if queueNameIn == "" { // Client requests server-generated name
			if passive {
				c.server.mu.Unlock()
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
			for _, existsGen := c.server.queues[actualQueueName]; existsGen; _, existsGen = c.server.queues[actualQueueName] {
				tempCounter++
				actualQueueName = fmt.Sprintf("%s-%d", baseName, tempCounter)
			}
			c.server.Info("Server generated queue name: %s", actualQueueName)
		}

		q, exists := c.server.queues[actualQueueName]

		if passive {
			if !exists {
				c.server.mu.Unlock()
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
				c.server.mu.Unlock()
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
					c.server.mu.Unlock()
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
					c.server.mu.Unlock()
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
				c.server.queues[actualQueueName] = &Queue{
					Name:       actualQueueName,
					Messages:   []Message{},
					Bindings:   make(map[string]bool),
					Consumers:  make(map[string]chan Message),
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
		c.server.mu.Unlock() // This was in your original code, ensuring it's unlocked.

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
		return nil // Successfully processed Queue.Declare

	case MethodQueueBind:
		var ticket uint16
		if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.bind (ticket)", uint16(ClassQueue), MethodQueueBind)
		}
		queueName := readShortString(reader)
		exchangeName := readShortString(reader)
		routingKey := readShortString(reader)

		bits, errReadByte := reader.ReadByte()
		if errReadByte != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed queue.bind (bits)", uint16(ClassQueue), MethodQueueBind)
		}

		argsBind, errReadTableBind := readTable(reader) // readTable now returns (map, error)
		if errReadTableBind != nil {
			c.server.Err("Error reading arguments table for queue.bind (queue: '%s', exchange: '%s'): %v", queueName, exchangeName, errReadTableBind)
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed arguments table for queue.bind", uint16(ClassQueue), MethodQueueBind)
		}

		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of queue.bind payload for queue '%s'.", queueName)
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - extra data in queue.bind payload", uint16(ClassQueue), MethodQueueBind)
		}

		noWait := (bits & 0x01) != 0

		c.server.Info("Processing queue.%s: queue='%s', exchange='%s', routingKey='%s', noWait=%v, args=%v on channel %d",
			methodName, queueName, exchangeName, routingKey, noWait, argsBind, channelId)

		// Your original bindError logic structure is fine, just adapting to sendChannelClose
		c.server.mu.RLock() // Use RLock for reading exchanges and queues initially
		ex, exExists := c.server.exchanges[exchangeName]
		q, qExists := c.server.queues[queueName]
		c.server.mu.RUnlock()

		if !exExists {
			replyText := fmt.Sprintf("no exchange '%s' in vhost '/'", exchangeName)
			c.server.Warn("Queue.Bind failed: %s. Sending Channel.Close.", replyText)
			// AMQP code 404 (NOT_FOUND)
			return c.sendChannelClose(channelId, 404, replyText, uint16(ClassQueue), MethodQueueBind)
		}
		if !qExists {
			replyText := fmt.Sprintf("no queue '%s' in vhost '/'", queueName)
			c.server.Warn("Queue.Bind failed: %s. Sending Channel.Close.", replyText)
			// AMQP code 404 (NOT_FOUND)
			return c.sendChannelClose(channelId, 404, replyText, uint16(ClassQueue), MethodQueueBind)
		}

		// Add binding (locking specific exchange and queue)
		ex.mu.Lock()
		alreadyBound := false
		for _, existingQueueName := range ex.Bindings[routingKey] {
			if existingQueueName == queueName {
				alreadyBound = true
				break
			}
		}
		if !alreadyBound {
			ex.Bindings[routingKey] = append(ex.Bindings[routingKey], queueName)
		}
		ex.mu.Unlock()

		q.mu.Lock()
		q.Bindings[exchangeName+":"+routingKey] = true // Store more specific binding info
		q.mu.Unlock()
		c.server.Info("Bound exchange '%s' (type: %s) to queue '%s' with routing key '%s'", exchangeName, ex.Type, queueName, routingKey)

		if !noWait {
			payloadBindOk := &bytes.Buffer{}
			binary.Write(payloadBindOk, binary.BigEndian, uint16(ClassQueue))
			binary.Write(payloadBindOk, binary.BigEndian, uint16(MethodQueueBindOk))

			if errWrite := c.writeFrame(&Frame{Type: FrameMethod, Channel: channelId, Payload: payloadBindOk.Bytes()}); errWrite != nil {
				c.server.Err("Error sending queue.bind-ok for queue '%s': %v", queueName, errWrite)
				return errWrite // Propagate I/O error
			}
			c.server.Info("Sent queue.bind-ok for queue '%s'", queueName)
		}
		return nil // Successfully processed Queue.Bind

	// Add cases for MethodQueueUnbind, MethodQueuePurge, MethodQueueDelete if you implement them.
	// Example for a missing one:
	// case MethodQueueDelete:
	//  return c.sendChannelClose(channelId, 540, "Queue.Delete not implemented", uint16(ClassQueue), MethodQueueDelete)

	default:
		replyText := fmt.Sprintf("unknown or not implemented queue method id %d", methodId)
		c.server.Err("Unhandled queue method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		// AMQP code 540 (NOT_IMPLEMENTED) or 503 (COMMAND_INVALID)
		return c.sendChannelClose(channelId, 540, replyText, uint16(ClassQueue), methodId)
	}
}

func (c *Connection) handleBasicMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassBasic, methodId)

	c.mu.RLock() // Lock connection to access its channels map
	ch, channelExists := c.channels[channelId]
	c.mu.RUnlock()

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

	switch methodId {
	case MethodBasicPublish:
		var ticket uint16
		if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.publish (ticket)", uint16(ClassBasic), MethodBasicPublish)
		}
		exchangeName := readShortString(reader)
		routingKey := readShortString(reader)

		bits, errReadByte := reader.ReadByte()
		if errReadByte != nil {
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - malformed basic.publish (bits)", uint16(ClassBasic), MethodBasicPublish)
		}

		// Check for extra data after all arguments are read.
		if reader.Len() > 0 {
			c.server.Warn("Extra data at end of basic.publish payload for exchange '%s', rkey '%s'.", exchangeName, routingKey)
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - extra data in basic.publish payload", uint16(ClassBasic), MethodBasicPublish)
		}

		mandatory := (bits & 0x01) != 0
		immediate := (bits & 0x02) != 0 // Immediate is deprecated in AMQP 0-9-1, server should ignore or reject.
		// For now, we'll accept it but not implement Basic.Return for immediate.

		c.server.Info("Processing basic.%s: exchange='%s', routingKey='%s', mandatory=%v, immediate=%v on channel %d",
			methodName, exchangeName, routingKey, mandatory, immediate, channelId)

		// Server-side validation for exchange existence (unless it's the default "" exchange which always exists)
		if exchangeName != "" {
			c.server.mu.RLock()
			_, exExists := c.server.exchanges[exchangeName]
			c.server.mu.RUnlock()
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
		queueName := readShortString(reader)
		consumerTagIn := readShortString(reader) // Client can specify, or server generates if empty

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
			return c.sendChannelClose(channelId, 502, "SYNTAX_ERROR - extra data in basic.consume payload", uint16(ClassBasic), MethodBasicConsume)
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

		c.server.mu.RLock() // RLock server to access queues map
		q, qExists := c.server.queues[queueName]
		c.server.mu.RUnlock()

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

		msgChan := make(chan Message, 100) // Buffered channel for this consumer

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
		q.Consumers[actualConsumerTag] = msgChan
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
				delete(q.Consumers, actualConsumerTag)
				close(msgChan) // Close the message channel we created
				q.mu.Unlock()
				return errWrite // Propagate I/O error
			}
			c.server.Info("Sent basic.consume-ok for consumer '%s' on queue '%s'", actualConsumerTag, queueName)
		}

		// Start the message delivery goroutine for this consumer
		go c.deliverMessages(channelId, actualConsumerTag, msgChan) // Pass noAck if deliverMessages needs it
		c.server.Info("Started message delivery goroutine for consumer '%s' on queue '%s' (noAck=%v)", actualConsumerTag, queueName, noAck)
		return nil

	// Add cases for MethodBasicAck, MethodBasicNack, MethodBasicReject, MethodBasicCancel, etc.
	// Example:
	// case MethodBasicAck:
	//  return c.sendChannelClose(channelId, 540, "Basic.Ack not implemented", uint16(ClassBasic), MethodBasicAck)

	default:
		replyText := fmt.Sprintf("unknown or not implemented basic method id %d", methodId)
		c.server.Err("Unhandled basic method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		// AMQP code 540 (NOT_IMPLEMENTED) or 503 (COMMAND_INVALID)
		return c.sendChannelClose(channelId, 540, replyText, uint16(ClassBasic), methodId)
	}
}

func (c *Connection) handleHeader(frame *Frame) error {
	c.mu.RLock()
	ch := c.channels[frame.Channel]
	c.mu.RUnlock()

	if ch == nil {
		// Header frame for a non-existent channel (or channel 0 which shouldn't get headers like this)
		c.server.Warn("Received header frame on non-existent or invalid channel %d", frame.Channel)
		// AMQP code 504 (CHANNEL_ERROR)
		// This is a connection error because the channel context is wrong.
		return c.sendConnectionClose(504, fmt.Sprintf("header frame on invalid channel %d", frame.Channel), 0, 0)
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
	var errProp error      // To catch errors from reading properties
	if flags&0x8000 != 0 { // content-type
		pendingMessage.Properties.ContentType = readShortString(reader)
	}
	if flags&0x4000 != 0 { // content-encoding
		pendingMessage.Properties.ContentEncoding = readShortString(reader)
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
		pendingMessage.Properties.CorrelationId = readShortString(reader)
	}
	if flags&0x0200 != 0 { // reply-to
		pendingMessage.Properties.ReplyTo = readShortString(reader)
	}
	if flags&0x0100 != 0 { // expiration
		pendingMessage.Properties.Expiration = readShortString(reader)
	}
	if flags&0x0080 != 0 { // message-id
		pendingMessage.Properties.MessageId = readShortString(reader)
	}
	if flags&0x0040 != 0 { // timestamp
		if errProp = binary.Read(reader, binary.BigEndian, &pendingMessage.Properties.Timestamp); errProp != nil {
			return c.sendChannelClose(frame.Channel, 502, "malformed timestamp", classId, 0)
		}
	}
	if flags&0x0020 != 0 { // type
		pendingMessage.Properties.Type = readShortString(reader)
	}
	if flags&0x0010 != 0 { // user-id
		pendingMessage.Properties.UserId = readShortString(reader)
	}
	if flags&0x0008 != 0 { // app-id
		pendingMessage.Properties.AppId = readShortString(reader)
	}
	if flags&0x0004 != 0 { // cluster-id (Reserved, usually not used by clients)
		pendingMessage.Properties.ClusterId = readShortString(reader)
	}

	// Check if readShortString (if it could indicate error) or binary.Read caused an issue
	// For now, assuming readShortString is problematic if it returns "" when reader isn't empty.
	// A more robust solution requires readShortString to return an error.
	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of header frame payload on channel %d", frame.Channel)
		// AMQP code 502 (SYNTAX_ERROR)
		return c.sendChannelClose(frame.Channel, 502, "extra data in header payload", classId, 0)
	}

	return nil // Successfully processed header
}

func (c *Connection) handleBody(frame *Frame) {
	// Get the channel and its pendingMessage
	c.mu.RLock()
	ch := c.channels[frame.Channel]
	c.mu.RUnlock()

	if ch != nil {
		ch.mu.Lock()

		// Check if channel is being closed by server
		if ch.closingByServer {
			ch.mu.Unlock()
			c.server.Debug("Ignoring body frame on channel %d that is being closed by server", frame.Channel)
			return // Just ignore it
		}

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
			c.deliverMessage(messageToDeliver)
		} else {
			ch.mu.Unlock()
			c.server.Warn("Received body frame with no pending message on channel %d", frame.Channel)
		}
	}
}

// Add routing functions to server.go

// RouteMessage handles all exchange type routing logic
func (c *Connection) routeMessage(msg Message) ([]string, error) {
	if msg.Exchange == "" {
		// Default exchange routes directly to queue by name
		if _, exists := c.server.queues[msg.RoutingKey]; exists {
			return []string{msg.RoutingKey}, nil
		}
		return nil, nil
	}

	c.server.mu.RLock()
	exchange := c.server.exchanges[msg.Exchange]
	c.server.mu.RUnlock()

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

	routingParts := strings.Split(routingKey, ".")

	for pattern, boundQueues := range exchange.Bindings {
		if topicMatch(pattern, routingParts) {
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

// topicMatch checks if a topic pattern matches routing key parts
func topicMatch(pattern string, routingParts []string) bool {
	patternParts := strings.Split(pattern, ".")

	i, j := 0, 0
	for i < len(patternParts) && j < len(routingParts) {
		if patternParts[i] == "#" {
			// # matches zero or more words
			if i == len(patternParts)-1 {
				// # at the end matches everything remaining
				return true
			}
			// # in the middle - need to try matching rest of pattern
			// from various positions in the routing key
			for k := j; k <= len(routingParts); k++ {
				if topicMatchHelper(patternParts[i+1:], routingParts[k:]) {
					return true
				}
			}
			return false
		} else if patternParts[i] == "*" {
			// * matches exactly one word
			i++
			j++
		} else if patternParts[i] == routingParts[j] {
			// Exact match
			i++
			j++
		} else {
			// No match
			return false
		}
	}

	// Check if we've consumed both patterns completely
	// OR if remaining pattern is just "#" (which matches zero words)
	return (i == len(patternParts) && j == len(routingParts)) ||
		(i == len(patternParts)-1 && patternParts[i] == "#" && j == len(routingParts))
}

// Refactored deliverMessage to use routing functions
func (c *Connection) deliverMessage(msg Message) {
	c.server.Info("Message: exchange=%s, routingKey=%s", msg.Exchange, msg.RoutingKey)

	// Route the message
	queueNames, err := c.routeMessage(msg)
	if err != nil {
		c.server.Err("Error routing message: %v", err)
		if msg.Mandatory {
			// TODO: Send basic.return
			c.server.Warn("Failed to route mandatory message: %v", err)
		}
		return
	}

	if len(queueNames) == 0 && msg.Mandatory {
		// TODO: Send basic.return
		c.server.Warn("No queues for mandatory message on exchange '%s' with routing key '%s'", msg.Exchange, msg.RoutingKey)
		return
	}

	// Deliver to each queue
	for _, queueName := range queueNames {
		c.deliverToQueue(queueName, msg)
	}

}

// deliverToQueue handles delivery to a single queue
func (c *Connection) deliverToQueue(queueName string, msg Message) {
	c.server.mu.RLock()
	queue := c.server.queues[queueName]
	c.server.mu.RUnlock()

	if queue == nil {
		c.server.Warn("Queue %s disappeared during delivery", queueName)
		return
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	// Create a deep copy of the message
	msgCopy := Message{
		Exchange:   msg.Exchange,
		RoutingKey: msg.RoutingKey,
		Mandatory:  msg.Mandatory,
		Immediate:  msg.Immediate,
		Properties: Properties{
			ContentType:     msg.Properties.ContentType,
			ContentEncoding: msg.Properties.ContentEncoding,
			DeliveryMode:    msg.Properties.DeliveryMode,
			Priority:        msg.Properties.Priority,
			CorrelationId:   msg.Properties.CorrelationId,
			ReplyTo:         msg.Properties.ReplyTo,
			Expiration:      msg.Properties.Expiration,
			MessageId:       msg.Properties.MessageId,
			Timestamp:       msg.Properties.Timestamp,
			Type:            msg.Properties.Type,
			UserId:          msg.Properties.UserId,
			AppId:           msg.Properties.AppId,
			ClusterId:       msg.Properties.ClusterId,
		},
		Body: make([]byte, len(msg.Body)),
	}
	copy(msgCopy.Body, msg.Body)

	// Deep copy headers if they exist
	if msg.Properties.Headers != nil {
		msgCopy.Properties.Headers = make(map[string]interface{})
		for k, v := range msg.Properties.Headers {
			msgCopy.Properties.Headers[k] = v
		}
	}

	queue.Messages = append(queue.Messages, msgCopy)
	c.server.Info("Queue %s now has %d messages", queueName, len(queue.Messages))

	// Send to all consumers for this queue
	for consumerTag, msgChan := range queue.Consumers {
		c.server.Info("Sending message to consumer %s in queue %s", consumerTag, queueName)
		select {
		case msgChan <- msgCopy:
			c.server.Info("Message queued for consumer %s", consumerTag)
		default:
			c.server.Warn("Consumer %s channel full, message dropped", consumerTag)
		}
	}
}

func (c *Connection) deliverMessages(channelId uint16, consumerTag string, msgChan chan Message) {
	for msg := range msgChan { // Loop as long as the message channel is open
		c.mu.RLock()
		ch := c.channels[channelId]
		c.mu.RUnlock()

		if ch == nil {
			c.server.Info("Channel %d no longer exists, stopping delivery for consumer %s", channelId, consumerTag)
			return // Exit goroutine if channel is gone
		}

		// Atomically increment delivery tag for this channel
		ch.mu.Lock()
		ch.deliveryTag++
		deliveryTag := ch.deliveryTag
		ch.mu.Unlock() // Unlock immediately after getting the tag

		// Create a deep copy of the message to avoid data races if msg is modified elsewhere
		// or if the same msg object is sent to multiple consumers (though deliverMessage already makes copies for queues)
		msgCopy := Message{
			Exchange:   msg.Exchange,
			RoutingKey: msg.RoutingKey,
			Mandatory:  msg.Mandatory,
			Immediate:  msg.Immediate,
			Properties: Properties{
				ContentType:     msg.Properties.ContentType,
				ContentEncoding: msg.Properties.ContentEncoding,
				DeliveryMode:    msg.Properties.DeliveryMode,
				Priority:        msg.Properties.Priority,
				CorrelationId:   msg.Properties.CorrelationId,
				ReplyTo:         msg.Properties.ReplyTo,
				Expiration:      msg.Properties.Expiration,
				MessageId:       msg.Properties.MessageId,
				Timestamp:       msg.Properties.Timestamp,
				Type:            msg.Properties.Type,
				UserId:          msg.Properties.UserId,
				AppId:           msg.Properties.AppId,
				ClusterId:       msg.Properties.ClusterId,
			},
			Body: make([]byte, len(msg.Body)),
		}
		copy(msgCopy.Body, msg.Body)
		if msg.Properties.Headers != nil {
			msgCopy.Properties.Headers = make(map[string]interface{})
			for k, v := range msg.Properties.Headers {
				msgCopy.Properties.Headers[k] = v // Assuming v is also safe or deep copied if necessary
			}
		}

		c.server.Info("Delivering message: channel=%d, consumerTag=%s, deliveryTag=%d, exchange=%s, routingKey=%s, bodySize=%d",
			channelId, consumerTag, deliveryTag, msgCopy.Exchange, msgCopy.RoutingKey, len(msgCopy.Body))

		// --- Construct Method Payload (Basic.Deliver) ---
		methodPayload := &bytes.Buffer{}
		binary.Write(methodPayload, binary.BigEndian, uint16(ClassBasic))         // ClassID
		binary.Write(methodPayload, binary.BigEndian, uint16(MethodBasicDeliver)) // MethodID
		writeShortString(methodPayload, consumerTag)
		binary.Write(methodPayload, binary.BigEndian, deliveryTag)
		methodPayload.WriteByte(0) // redelivered (false)
		writeShortString(methodPayload, msgCopy.Exchange)
		writeShortString(methodPayload, msgCopy.RoutingKey)

		// --- Construct Header Payload ---
		headerPayload := &bytes.Buffer{}
		binary.Write(headerPayload, binary.BigEndian, uint16(ClassBasic)) // ClassID
		binary.Write(headerPayload, binary.BigEndian, uint16(0))          // weight (deprecated, must be 0)
		binary.Write(headerPayload, binary.BigEndian, uint64(len(msgCopy.Body)))

		flags := uint16(0)
		if msgCopy.Properties.ContentType != "" {
			flags |= (1 << 15)
		} // 0x8000
		if msgCopy.Properties.ContentEncoding != "" {
			flags |= (1 << 14)
		} // 0x4000
		if len(msgCopy.Properties.Headers) > 0 {
			flags |= (1 << 13)
		} // 0x2000
		if msgCopy.Properties.DeliveryMode != 0 {
			flags |= (1 << 12)
		} // 0x1000
		if msgCopy.Properties.Priority != 0 {
			flags |= (1 << 11)
		} // 0x0800
		if msgCopy.Properties.CorrelationId != "" {
			flags |= (1 << 10)
		} // 0x0400
		if msgCopy.Properties.ReplyTo != "" {
			flags |= (1 << 9)
		} // 0x0200
		if msgCopy.Properties.Expiration != "" {
			flags |= (1 << 8)
		} // 0x0100
		if msgCopy.Properties.MessageId != "" {
			flags |= (1 << 7)
		} // 0x0080
		if msgCopy.Properties.Timestamp != 0 {
			flags |= (1 << 6)
		} // 0x0040
		if msgCopy.Properties.Type != "" {
			flags |= (1 << 5)
		} // 0x0020
		if msgCopy.Properties.UserId != "" {
			flags |= (1 << 4)
		} // 0x0010
		if msgCopy.Properties.AppId != "" {
			flags |= (1 << 3)
		} // 0x0008
		if msgCopy.Properties.ClusterId != "" {
			flags |= (1 << 2)
		} // 0x0004
		// Note: AMQP spec says remaining bits of property flags are reserved.

		binary.Write(headerPayload, binary.BigEndian, flags)

		if (flags & (1 << 15)) != 0 {
			writeShortString(headerPayload, msgCopy.Properties.ContentType)
		}
		if (flags & (1 << 14)) != 0 {
			writeShortString(headerPayload, msgCopy.Properties.ContentEncoding)
		}
		if (flags & (1 << 13)) != 0 {
			writeTable(headerPayload, msgCopy.Properties.Headers)
		} // Assuming writeTable is defined
		if (flags & (1 << 12)) != 0 {
			binary.Write(headerPayload, binary.BigEndian, msgCopy.Properties.DeliveryMode)
		}
		if (flags & (1 << 11)) != 0 {
			binary.Write(headerPayload, binary.BigEndian, msgCopy.Properties.Priority)
		}
		if (flags & (1 << 10)) != 0 {
			writeShortString(headerPayload, msgCopy.Properties.CorrelationId)
		}
		if (flags & (1 << 9)) != 0 {
			writeShortString(headerPayload, msgCopy.Properties.ReplyTo)
		}
		if (flags & (1 << 8)) != 0 {
			writeShortString(headerPayload, msgCopy.Properties.Expiration)
		}
		if (flags & (1 << 7)) != 0 {
			writeShortString(headerPayload, msgCopy.Properties.MessageId)
		}
		if (flags & (1 << 6)) != 0 {
			binary.Write(headerPayload, binary.BigEndian, msgCopy.Properties.Timestamp)
		}
		if (flags & (1 << 5)) != 0 {
			writeShortString(headerPayload, msgCopy.Properties.Type)
		}
		if (flags & (1 << 4)) != 0 {
			writeShortString(headerPayload, msgCopy.Properties.UserId)
		}
		if (flags & (1 << 3)) != 0 {
			writeShortString(headerPayload, msgCopy.Properties.AppId)
		}
		if (flags & (1 << 2)) != 0 {
			writeShortString(headerPayload, msgCopy.Properties.ClusterId)
		}

		// --- Lock, Buffer all frames, Flush once, Unlock ---
		c.writeMu.Lock() // Acquire the connection's write lock for the entire message delivery

		var deliveryErr error
		// Buffer Method Frame
		deliveryErr = c.writeFrameInternal(FrameMethod, channelId, methodPayload.Bytes())
		if deliveryErr != nil {
			c.server.Err("Error buffering basic.deliver frame for tag %d: %v", deliveryTag, deliveryErr)
			c.writeMu.Unlock() // Release lock on error
			// Decide if to continue to next message or return/break from loop
			// If connection is broken, subsequent writes will likely fail anyway.
			continue // or return, depending on desired error handling
		}

		// Buffer Header Frame

		deliveryErr = c.writeFrameInternal(FrameHeader, channelId, headerPayload.Bytes())
		if deliveryErr != nil {
			c.server.Err("Error buffering header frame for tag %d: %v", deliveryTag, deliveryErr)
		}

		// Buffer Body Frame
		if deliveryErr == nil {
			deliveryErr = c.writeFrameInternal(FrameBody, channelId, msgCopy.Body)
			if deliveryErr != nil {
				c.server.Err("Error buffering body frame for tag %d: %v", deliveryTag, deliveryErr)
			}
		}

		// Flush all buffered frames for this message if no buffering errors occurred
		if deliveryErr == nil {
			flushErr := c.writer.Flush()
			if flushErr != nil {
				c.server.Err("Error flushing writer after message delivery for tag %d: %v", deliveryTag, flushErr)
				deliveryErr = flushErr // Report flush error as the overall delivery error
			}
		}

		c.writeMu.Unlock() // Release the lock

		if deliveryErr == nil {
			c.server.Info("Successfully delivered message (method, header, body) for deliveryTag=%d", deliveryTag)
		} else {
			// Handle the case where delivery failed.
			// The client might not have received the message or received a partial one.
			// Depending on noAck, redelivery logic might be needed (more complex).
			// For now, just logging.
			c.server.Err("Failed to fully deliver message for deliveryTag=%d", deliveryTag)
		}
	}
	c.server.Info("Message channel closed for consumer %s on channel %d. Exiting deliverMessages goroutine.", consumerTag, channelId)
}

func (c *Connection) cleanupConnectionResources() {
	c.server.Info("Cleaning up resources for connection %s", c.conn.RemoteAddr())
	c.mu.Lock() // Lock the connection to safely iterate over channels
	defer c.mu.Unlock()

	for chanId, ch := range c.channels {
		c.server.Info("Closing resources for channel %d on connection %s", chanId, c.conn.RemoteAddr())
		ch.mu.Lock()
		for consumerTag, queueName := range ch.consumers {
			c.server.mu.RLock()
			if queue, ok := c.server.queues[queueName]; ok {
				queue.mu.Lock()
				if msgChan, consumerExists := queue.Consumers[consumerTag]; consumerExists {
					c.server.Info("Closing consumer channel for tag %s on queue %s", consumerTag, queueName)
					close(msgChan) // This will make the deliverMessages goroutine exit
					delete(queue.Consumers, consumerTag)
				}
				queue.mu.Unlock()
			}
			c.server.mu.RUnlock()
		}
		ch.consumers = make(map[string]string) // Clear consumers map
		ch.pendingMessages = nil               // Clear pending messages
		ch.mu.Unlock()
		// We don't delete from c.channels here because we are iterating over it.
		// The channel map will be discarded when the Connection object is GC'd.
	}
	c.channels = make(map[uint16]*Channel) // Clear the channels map
	// The underlying c.conn will be closed by the caller of cleanupConnectionResources or already closed.
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
			for consumerTag, queueName := range ch.consumers {
				c.server.mu.RLock() // RLock server to access s.queues
				if queue, qExists := c.server.queues[queueName]; qExists {
					queue.mu.Lock()
					if msgChan, consumerExistsOnQueue := queue.Consumers[consumerTag]; consumerExistsOnQueue {
						close(msgChan) // Signal deliverMessages goroutine to stop
						delete(queue.Consumers, consumerTag)
						c.server.Info("Closed consumer %s on queue %s for channel %d during server-initiated close", consumerTag, queueName, channelId)
					}
					queue.mu.Unlock()
				}
				c.server.mu.RUnlock()
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
	c.mu.Lock() // Lock connection to modify c.channels
	defer c.mu.Unlock()

	ch, exists := c.channels[channelId]
	if !exists || ch == nil {
		c.server.Info("Attempted to forcibly remove channel %d, but it was already gone or nil. Reason: %s", channelId, reason)
		return
	}

	ch.mu.Lock()
	// Ensure timer is stopped if it hasn't fired and exists
	if ch.closeOkTimer != nil {
		if !ch.closeOkTimer.Stop() {
			// Timer already fired or was stopped, allow its goroutine to potentially run if it just fired
			// Or, it might be nil if never started (e.g. client initiated close)
			c.server.Debug("CloseOkTimer for channel %d had already fired or was stopped when forceRemoveChannel called.", channelId)
		}
		ch.closeOkTimer = nil // Clear the timer
	}

	// Defensive cleanup of consumers and pending messages if not already done
	if len(ch.consumers) > 0 {
		c.server.Warn("Channel %d (reason: %s) had remaining consumers during forceRemove: %v", channelId, reason, ch.consumers)
		for consumerTag, queueName := range ch.consumers {
			c.server.mu.RLock()
			if q, qExists := c.server.queues[queueName]; qExists {
				q.mu.Lock()
				if msgChan, consumerExistsOnQueue := q.Consumers[consumerTag]; consumerExistsOnQueue {
					close(msgChan)
					delete(q.Consumers, consumerTag)
				}
				q.mu.Unlock()
			}
			c.server.mu.RUnlock()
		}
		ch.consumers = make(map[string]string)
	}
	ch.pendingMessages = nil

	ch.mu.Unlock() // Unlock the channel

	delete(c.channels, channelId)
	c.server.Info("Forcibly removed channel %d from connection %s. Reason: %s", channelId, c.conn.RemoteAddr(), reason)
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

func readTable(reader *bytes.Reader) (map[string]interface{}, error) {
	var tablePayloadLength uint32
	if err := binary.Read(reader, binary.BigEndian, &tablePayloadLength); err != nil {
		return nil, fmt.Errorf("reading table payload length: %w", err)
	}

	if tablePayloadLength == 0 {
		return make(map[string]interface{}), nil
	}

	tablePayloadBytes := make([]byte, tablePayloadLength)
	n, err := io.ReadFull(reader, tablePayloadBytes)
	if err != nil {
		// err will be io.ErrUnexpectedEOF if fewer than tablePayloadLength bytes were read
		return nil, fmt.Errorf("reading table payload bytes (expected %d, read %d): %w", tablePayloadLength, n, err)
	}
	// This check is redundant if io.ReadFull is used correctly, as it returns an error for short reads.
	// if n != int(tablePayloadLength) {
	// 	return nil, fmt.Errorf("short read for table payload: got %d, expected %d", n, tablePayloadLength)
	// }

	tableReader := bytes.NewReader(tablePayloadBytes)
	table := make(map[string]interface{})

	for tableReader.Len() > 0 {
		// Assuming readShortString does not return an error itself.
		// If readShortString could fail (e.g. EOF mid-string), it should return an error.
		key := readShortString(tableReader)

		// If after reading a key, there's no more data for its value type, it's malformed.
		if tableReader.Len() == 0 {
			if key != "" { // We read a key but no value followed.
				return table, fmt.Errorf("malformed table: key '%s' read but no value type followed", key)
			}
			// If key is "" and tableReader.Len() is 0, it might be a valid empty key at the end,
			// or readShortString failed silently. This depends on readShortString's contract.
			// For now, if key is "" and no more data, we assume loop termination is fine.
			break
		}

		valueType, err := tableReader.ReadByte()
		if err != nil {
			return table, fmt.Errorf("reading value type for key '%s' (or after last key): %w", key, err)
		}

		value, err := readFieldValue(tableReader, valueType)
		if err != nil {
			return table, fmt.Errorf("reading value for key '%s' (type %c): %w", key, valueType, err)
		}
		table[key] = value
	}

	if tableReader.Len() > 0 {
		// This means not all bytes of the declared table payload were consumed.
		return table, fmt.Errorf("%d unread bytes remaining in table payload after parsing; table may be malformed or contain extra data", tableReader.Len())
	}

	return table, nil
}

func writeTable(writer *bytes.Buffer, table map[string]interface{}) error {
	tablePayloadBuffer := &bytes.Buffer{}

	for key, value := range table {
		// Assuming writeShortString does not return an error.
		// If it could fail, its error would need to be handled here.
		writeShortString(tablePayloadBuffer, key)

		if err := writeFieldValue(tablePayloadBuffer, value); err != nil {
			return fmt.Errorf("serializing value for key '%s' (type %T): %w", key, value, err)
		}
	}

	// Write the total length of the table payload first
	if err := binary.Write(writer, binary.BigEndian, uint32(tablePayloadBuffer.Len())); err != nil {
		return fmt.Errorf("writing table payload length: %w", err)
	}
	// Then write the actual table payload
	if _, err := writer.Write(tablePayloadBuffer.Bytes()); err != nil {
		return fmt.Errorf("writing table payload bytes: %w", err)
	}
	return nil
}

func main() {
	server := NewServer()
	server.Info("Starting AMQP server")
	if err := server.Start(":5672"); err != nil {
		server.Err("Failed to start server: %v", err)
		os.Exit(1)
	}
}
