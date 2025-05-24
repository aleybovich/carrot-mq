// server.go
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
)

// ANSI color codes for terminal output
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[37m"
	colorWhite  = "\033[97m"

	colorBoldRed    = "\033[1;31m"
	colorBoldGreen  = "\033[1;32m"
	colorBoldYellow = "\033[1;33m"
	colorBoldBlue   = "\033[1;34m"
)

// Flag to determine if we're logging to a terminal (with colors) or a file
var isTerminal bool

func init() {
	// Check if stdout is a terminal
	fileInfo, _ := os.Stdout.Stat()
	isTerminal = (fileInfo.Mode() & os.ModeCharDevice) != 0
}

const (
	FrameMethod    = 1
	FrameHeader    = 2
	FrameBody      = 3
	FrameHeartbeat = 8
	FrameEnd       = 206
)

const (
	ClassConnection = 10
	ClassChannel    = 20
	ClassExchange   = 40
	ClassQueue      = 50
	ClassBasic      = 60
)

const (
	MethodConnectionStart   = 10
	MethodConnectionStartOk = 11
	MethodConnectionTune    = 30
	MethodConnectionTuneOk  = 31
	MethodConnectionOpen    = 40
	MethodConnectionOpenOk  = 41
	MethodConnectionClose   = 50
	MethodConnectionCloseOk = 51
	MethodChannelOpen       = 10
	MethodChannelOpenOk     = 11
	MethodChannelClose      = 40
	MethodChannelCloseOk    = 41
	MethodExchangeDeclare   = 10
	MethodExchangeDeclareOk = 11
	MethodQueueDeclare      = 10
	MethodQueueDeclareOk    = 11
	MethodQueueBind         = 20
	MethodQueueBindOk       = 21
	MethodBasicConsume      = 20
	MethodBasicConsumeOk    = 21
	MethodBasicPublish      = 40
	MethodBasicDeliver      = 60
)

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
	id              uint16
	conn            *Connection
	consumers       map[string]string
	pendingMessages []Message
	deliveryTag     uint64
	mu              sync.Mutex
}

type Connection struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	channels map[uint16]*Channel
	server   *Server
	mu       sync.RWMutex
	writeMu  sync.Mutex
}

type Server struct {
	listener     net.Listener
	exchanges    map[string]*Exchange
	queues       map[string]*Queue
	logger       *log.Logger // Internal logger for formatting output
	customLogger Logger      // External logger interface, if provided
	mu           sync.RWMutex
}

var errConnectionClosedGracefully = errors.New("connection closed gracefully by AMQP protocol")

// ServerOption defines functional options for configuring the AMQP server
type ServerOption func(*Server)

// WithLogger sets a custom logger that implements the Logger interface
func WithLogger(logger Logger) ServerOption {
	return func(s *Server) {
		s.customLogger = logger
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

	c.sendConnectionStart()
	s.Info("Sent connection.start to %s", conn.RemoteAddr())

	for {
		frame, err := c.readFrame()
		if err != nil {
			// Enhanced error checking for readFrame
			errMsg := err.Error()
			if errors.Is(err, io.EOF) || strings.Contains(errMsg, "use of closed network connection") || strings.Contains(errMsg, "connection reset by peer") || errors.Is(err, net.ErrClosed) {
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
			c.handleHeader(frame)
		case FrameBody:
			c.handleBody(frame)
		case FrameHeartbeat:
			// Heartbeats are typically high-volume and low importance for logging
			// Only log them at detailed level or in debug mode
			if os.Getenv("AMQP_DEBUG") == "1" {
				s.Debug("Received %s from %s on channel %d",
					colorize("HEARTBEAT", colorGray),
					colorize(conn.RemoteAddr().String(), colorCyan),
					frame.Channel)
			}
			// Optionally, send a heartbeat frame back. AMQP 0-9-1 spec says server SHOULD respond to peer's heartbeat.
			// For simplicity, we are just logging. If heartbeat timeout is implemented, this is where it would be reset.
			// Example: if err := c.writeFrame(&Frame{Type: FrameHeartbeat, Channel: 0, Payload: []byte{}}); err != nil { ... }
		default:
			s.Warn("Received unhandled frame type %d from %s on channel %d", frame.Type, conn.RemoteAddr(), frame.Channel)
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
			// Handle other potential errors from method handlers if they are critical
			s.Err("Error from method handler for %s: %v. Closing connection.", conn.RemoteAddr(), handlerError)
			c.cleanupConnectionResources()
			conn.Close()
			return
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

func (c *Connection) sendConnectionStart() {
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
	binary.Write(payload, binary.BigEndian, uint16(MethodConnectionStart))
	payload.WriteByte(0)
	payload.WriteByte(9)

	writeTable(payload, map[string]interface{}{
		"capabilities": map[string]interface{}{
			"publisher_confirms":           true,
			"exchange_exchange_bindings":   true,
			"basic.nack":                   true,
			"consumer_cancel_notify":       true,
			"connection.blocked":           true,
			"consumer_priorities":          true,
			"authentication_failure_close": true,
			"per_consumer_qos":             true,
		},
	})

	mechanisms := "PLAIN"
	binary.Write(payload, binary.BigEndian, uint32(len(mechanisms)))
	payload.WriteString(mechanisms)

	locales := "en_US"
	binary.Write(payload, binary.BigEndian, uint32(len(locales)))
	payload.WriteString(locales)

	err := c.writeFrame(&Frame{
		Type:    FrameMethod,
		Channel: 0,
		Payload: payload.Bytes(),
	})
	if err != nil {
		c.server.Err("Error sending connection.start: %v", err)
	}
}

//var currentMessage *Message

func (c *Connection) handleMethod(frame *Frame) error { // MODIFIED: Added return type error
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

	switch classId {
	case ClassConnection:
		return c.handleConnectionMethod(methodId, reader)
	case ClassChannel:
		c.handleChannelMethod(methodId, reader, frame.Channel)
		// If handleChannelMethod could also close the connection, it should also return an error.
	case ClassExchange:
		c.handleExchangeMethod(methodId, reader, frame.Channel)
	case ClassQueue:
		c.handleQueueMethod(methodId, reader, frame.Channel)
	case ClassBasic:
		c.handleBasicMethod(methodId, reader, frame.Channel)
	default:
		c.server.Warn("Received method for unknown class ID: %d on channel %d", classId, frame.Channel)
	}
	return nil
}

func (c *Connection) handleConnectionMethod(methodId uint16, reader *bytes.Reader) error {
	methodName := getMethodName(ClassConnection, methodId)

	switch methodId {
	case MethodConnectionStartOk:
		c.server.Info("Processing connection.%s", methodName)
		readTable(reader)
		readShortString(reader)
		readLongString(reader)
		readShortString(reader)

		c.sendConnectionTune()

	case MethodConnectionTuneOk:
		c.server.Info("Processing connection.%s", methodName)
		var channelMax uint16
		var frameMax uint32
		var heartbeat uint16
		binary.Read(reader, binary.BigEndian, &channelMax)
		binary.Read(reader, binary.BigEndian, &frameMax)
		binary.Read(reader, binary.BigEndian, &heartbeat)
		c.server.Info("Connection parameters: channelMax=%d, frameMax=%d, heartbeat=%d",
			channelMax, frameMax, heartbeat)

	case MethodConnectionOpen:
		vhost := readShortString(reader)
		c.server.Info("Processing connection.%s for vhost: %s", methodName, vhost)
		reserved1, _ := reader.ReadByte()
		reserved2, _ := reader.ReadByte()

		// Use vhost if needed
		_ = vhost

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
		binary.Write(payload, binary.BigEndian, uint16(MethodConnectionOpenOk))
		payload.WriteByte(reserved1 & reserved2) // Using both reserved bytes

		err := c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: 0,
			Payload: payload.Bytes(),
		})
		if err != nil {
			c.server.Err("Error sending connection.open-ok: %v", err)
		} else {
			c.server.Info("Sent connection.open-ok")
		}
	case MethodConnectionCloseOk: // Server receives this if it initiated a Connection.Close
		c.server.Info("Received connection.close-ok. Client acknowledged connection closure.")
		c.conn.Close()
		return errConnectionClosedGracefully
	case MethodConnectionClose:
		c.server.Info("Processing connection.%s", methodName)

		// Read the close method parameters
		var replyCode uint16
		binary.Read(reader, binary.BigEndian, &replyCode)
		replyText := readShortString(reader)
		var classId, methodId uint16
		binary.Read(reader, binary.BigEndian, &classId)
		binary.Read(reader, binary.BigEndian, &methodId)

		c.server.Info("Connection close: replyCode=%d, replyText=%s, classId=%d, methodId=%d",
			replyCode, replyText, classId, methodId)

		// Send connection.close-ok
		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
		binary.Write(payload, binary.BigEndian, uint16(MethodConnectionCloseOk))

		err := c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: 0,
			Payload: payload.Bytes(),
		})
		if err != nil {
			c.server.Err("Error sending connection.close-ok: %v", err)
		} else {
			c.server.Info("Sent connection.close-ok")
		}

		// Close the connection after sending the response
		c.conn.Close()
		return errConnectionClosedGracefully
	default:
		c.server.Err("Received unhandled connection method ID: %d", methodId)
		// return fmt.Errorf("unhandled connection method: %d", methodId) // Optional: make it an error
	}
	return nil
}

func (c *Connection) sendConnectionTune() {
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
	binary.Write(payload, binary.BigEndian, uint16(MethodConnectionTune))
	binary.Write(payload, binary.BigEndian, uint16(0))
	binary.Write(payload, binary.BigEndian, uint32(131072))
	binary.Write(payload, binary.BigEndian, uint16(60))

	err := c.writeFrame(&Frame{
		Type:    FrameMethod,
		Channel: 0,
		Payload: payload.Bytes(),
	})
	if err != nil {
		c.server.Err("Error sending connection.tune: %v", err)
	} else {
		c.server.Info("Sent connection.tune")
	}
}

func (c *Connection) handleChannelMethod(methodId uint16, reader *bytes.Reader, channelId uint16) {
	methodName := getMethodName(ClassChannel, methodId)

	switch methodId {
	case MethodChannelOpen:
		c.server.Info("Processing channel.%s for channel %d", methodName, channelId)
		reserved := readShortString(reader)
		_ = reserved

		c.mu.Lock()
		c.channels[channelId] = &Channel{
			id:              channelId,
			conn:            c,
			consumers:       make(map[string]string),
			pendingMessages: make([]Message, 0),
		}
		c.mu.Unlock()

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassChannel))
		binary.Write(payload, binary.BigEndian, uint16(MethodChannelOpenOk))
		binary.Write(payload, binary.BigEndian, uint32(0))

		err := c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channelId,
			Payload: payload.Bytes(),
		})
		if err != nil {
			c.server.Err("Error sending channel.open-ok for channel %d: %v", channelId, err)
		} else {
			c.server.Info("Sent channel.open-ok for channel %d", channelId)
		}
	case MethodChannelClose:
		c.server.Info("Processing channel.%s for channel %d", methodName, channelId)

		// Read the close method parameters
		var replyCode uint16
		binary.Read(reader, binary.BigEndian, &replyCode)
		replyText := readShortString(reader)
		var classId, methodId uint16
		binary.Read(reader, binary.BigEndian, &classId)
		binary.Read(reader, binary.BigEndian, &methodId)

		c.server.Info("Channel close: replyCode=%d, replyText=%s, classId=%d, methodId=%d",
			replyCode, replyText, classId, methodId)

		// Clean up the channel
		c.mu.Lock()
		if ch, exists := c.channels[channelId]; exists {
			// Close any consumer channels for this channel
			ch.mu.Lock()
			for consumerTag, queueName := range ch.consumers {
				c.server.mu.RLock()
				if queue, ok := c.server.queues[queueName]; ok {
					queue.mu.Lock()
					if msgChan, ok := queue.Consumers[consumerTag]; ok {
						close(msgChan)
						delete(queue.Consumers, consumerTag)
					}
					queue.mu.Unlock()
				}
				c.server.mu.RUnlock()
			}
			ch.mu.Unlock()
			delete(c.channels, channelId)
		}
		c.mu.Unlock()

		// Send channel.close-ok
		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassChannel))
		binary.Write(payload, binary.BigEndian, uint16(MethodChannelCloseOk))

		err := c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channelId,
			Payload: payload.Bytes(),
		})
		if err != nil {
			c.server.Err("Error sending channel.close-ok for channel %d: %v", channelId, err)
		} else {
			c.server.Info("Sent channel.close-ok for channel %d", channelId)
		}
	}
}

func (c *Connection) handleExchangeMethod(methodId uint16, reader *bytes.Reader, channel uint16) {
	methodName := getMethodName(ClassExchange, methodId)

	switch methodId {
	case MethodExchangeDeclare:
		var reserved1 uint16
		binary.Read(reader, binary.BigEndian, &reserved1) // Read as uint16, not byte
		exchange := readShortString(reader)
		exchangeType := readShortString(reader)
		bits, _ := reader.ReadByte()
		args := readTable(reader)

		// Parse bits
		passive := bits&0x01 != 0
		durable := bits&0x02 != 0
		autoDelete := bits&0x04 != 0
		internal := bits&0x08 != 0
		noWait := bits&0x10 != 0

		c.server.Info("Processing exchange.%s: name=%s, type=%s, passive=%v, durable=%v, autoDelete=%v, internal=%v, noWait=%v",
			methodName, exchange, exchangeType, passive, durable, autoDelete, internal, noWait)

		_ = passive
		_ = args

		c.server.mu.Lock()
		if _, exists := c.server.exchanges[exchange]; !exists {
			c.server.exchanges[exchange] = &Exchange{
				Name:       exchange,
				Type:       exchangeType,
				Durable:    durable,
				AutoDelete: autoDelete,
				Internal:   internal,
				Bindings:   make(map[string][]string),
			}
			c.server.Info("Created new exchange: %s", exchange)
		} else {
			c.server.Info("Exchange already exists: %s", exchange)
		}
		c.server.mu.Unlock()

		// Don't send response if noWait is true
		if !noWait {
			payload := &bytes.Buffer{}
			binary.Write(payload, binary.BigEndian, uint16(ClassExchange))
			binary.Write(payload, binary.BigEndian, uint16(MethodExchangeDeclareOk))

			err := c.writeFrame(&Frame{
				Type:    FrameMethod,
				Channel: channel,
				Payload: payload.Bytes(),
			})
			if err != nil {
				c.server.Err("Error sending exchange.declare-ok for exchange %s: %v", exchange, err)
			} else {
				c.server.Info("Sent exchange.declare-ok for exchange %s", exchange)
			}
		}
	}
}

func (c *Connection) handleQueueMethod(methodId uint16, reader *bytes.Reader, channel uint16) {
	methodName := getMethodName(ClassQueue, methodId)

	switch methodId {
	case MethodQueueDeclare:
		var reserved1 uint16
		binary.Read(reader, binary.BigEndian, &reserved1) // Read as uint16, not byte
		queue := readShortString(reader)
		bits, _ := reader.ReadByte()
		args := readTable(reader)

		// Parse bits
		passive := bits&0x01 != 0
		durable := bits&0x02 != 0
		exclusive := bits&0x04 != 0
		autoDelete := bits&0x08 != 0
		noWait := bits&0x10 != 0

		c.server.Info("Processing queue.%s: name=%s, passive=%v, durable=%v, exclusive=%v, autoDelete=%v, noWait=%v",
			methodName, queue, passive, durable, exclusive, autoDelete, noWait)

		_ = passive
		_ = args
		_ = noWait

		c.server.mu.Lock()
		if _, exists := c.server.queues[queue]; !exists {
			c.server.queues[queue] = &Queue{
				Name:       queue,
				Messages:   []Message{},
				Bindings:   make(map[string]bool),
				Consumers:  make(map[string]chan Message),
				Durable:    durable,
				Exclusive:  exclusive,
				AutoDelete: autoDelete,
			}
			c.server.Info("Created new queue: %s", queue)
		} else {
			c.server.Info("Queue already exists: %s", queue)
		}
		c.server.mu.Unlock()

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassQueue))
		binary.Write(payload, binary.BigEndian, uint16(MethodQueueDeclareOk))
		writeShortString(payload, queue)
		binary.Write(payload, binary.BigEndian, uint32(0))
		binary.Write(payload, binary.BigEndian, uint32(0))

		err := c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channel,
			Payload: payload.Bytes(),
		})
		if err != nil {
			c.server.Err("Error sending queue.declare-ok for queue %s: %v", queue, err)
		} else {
			c.server.Info("Sent queue.declare-ok for queue %s", queue)
		}

	case MethodQueueBind:
		var reserved1 uint16
		binary.Read(reader, binary.BigEndian, &reserved1) // Read as uint16, not byte
		queue := readShortString(reader)
		exchange := readShortString(reader)
		routingKey := readShortString(reader)
		bits, _ := reader.ReadByte()
		args := readTable(reader)

		// Parse bits
		noWait := bits&0x01 != 0

		c.server.Info("Processing queue.%s: queue=%s, exchange=%s, routingKey=%s, noWait=%v",
			methodName, queue, exchange, routingKey, noWait)

		_ = noWait
		_ = args

		c.server.mu.Lock()
		if ex, ok := c.server.exchanges[exchange]; ok {
			ex.mu.Lock()
			ex.Bindings[routingKey] = append(ex.Bindings[routingKey], queue)
			ex.mu.Unlock()
			c.server.Info("Bound exchange %s to queue %s with routing key %s", exchange, queue, routingKey)
		} else {
			c.server.Warn("Exchange %s not found for binding", exchange)
		}
		if q, ok := c.server.queues[queue]; ok {
			q.mu.Lock()
			q.Bindings[routingKey] = true
			q.mu.Unlock()
		} else {
			c.server.Warn("Queue %s not found for binding", queue)
		}
		c.server.mu.Unlock()

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassQueue))
		binary.Write(payload, binary.BigEndian, uint16(MethodQueueBindOk))

		err := c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channel,
			Payload: payload.Bytes(),
		})
		if err != nil {
			c.server.Err("Error sending queue.bind-ok for queue %s: %v", queue, err)
		} else {
			c.server.Info("Sent queue.bind-ok for queue %s", queue)
		}
	}
}

func (c *Connection) handleBasicMethod(methodId uint16, reader *bytes.Reader, channelId uint16) {
	methodName := getMethodName(ClassBasic, methodId)

	switch methodId {
	case MethodBasicPublish:
		var reserved1 uint16
		binary.Read(reader, binary.BigEndian, &reserved1)
		exchange := readShortString(reader)
		routingKey := readShortString(reader)
		bits, _ := reader.ReadByte()

		// Parse bits
		mandatory := bits&0x01 != 0
		immediate := bits&0x02 != 0

		c.server.Info("Processing basic.%s: exchange=%s, routingKey=%s, mandatory=%v, immediate=%v",
			methodName, exchange, routingKey, mandatory, immediate)

		// Get the channel and set its pendingMessage
		c.mu.RLock()
		ch := c.channels[channelId]
		c.mu.RUnlock()

		if ch != nil {
			ch.mu.Lock()
			// Add to queue instead of overwriting
			newMessage := Message{
				Exchange:   exchange,
				RoutingKey: routingKey,
				Mandatory:  mandatory,
				Immediate:  immediate,
			}
			ch.pendingMessages = append(ch.pendingMessages, newMessage)
			ch.mu.Unlock()
		}

	case MethodBasicConsume:
		var reserved1 uint16
		binary.Read(reader, binary.BigEndian, &reserved1)
		queue := readShortString(reader)
		consumerTag := readShortString(reader)
		bits, _ := reader.ReadByte()
		args := readTable(reader)

		// Parse bits
		noLocal := bits&0x01 != 0
		noAck := bits&0x02 != 0
		exclusive := bits&0x04 != 0
		noWait := bits&0x08 != 0

		c.server.Info("Processing basic.%s: queue=%s, consumerTag=%s, noLocal=%v, noAck=%v, exclusive=%v, noWait=%v",
			methodName, queue, consumerTag, noLocal, noAck, exclusive, noWait)

		_ = noLocal
		_ = noAck
		_ = exclusive
		_ = args

		c.mu.RLock()
		ch := c.channels[channelId]
		c.mu.RUnlock()

		c.server.mu.RLock()
		q := c.server.queues[queue]
		c.server.mu.RUnlock()

		if ch != nil && q != nil {
			ch.mu.Lock()
			ch.consumers[consumerTag] = queue
			ch.mu.Unlock()

			msgChan := make(chan Message, 100)
			q.mu.Lock()
			q.Consumers[consumerTag] = msgChan
			q.mu.Unlock()

			// Always send consume-ok, regardless of noWait
			payload := &bytes.Buffer{}
			binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
			binary.Write(payload, binary.BigEndian, uint16(MethodBasicConsumeOk))
			writeShortString(payload, consumerTag)

			err := c.writeFrame(&Frame{
				Type:    FrameMethod,
				Channel: channelId,
				Payload: payload.Bytes(),
			})
			if err != nil {
				c.server.Err("Error sending basic.consume-ok: %v", err)
			} else {
				c.server.Info("Sent basic.consume-ok for consumer %s", consumerTag)
			}

			// Start delivery goroutine
			go c.deliverMessages(channelId, consumerTag, msgChan)
		}
	}
}

func (c *Connection) handleHeader(frame *Frame) {
	c.mu.RLock()
	ch := c.channels[frame.Channel]
	c.mu.RUnlock()

	if ch != nil {
		ch.mu.Lock()
		if len(ch.pendingMessages) > 0 {
			// Get the FIRST pending message (but don't remove it yet - body will do that)
			pendingMessage := &ch.pendingMessages[0]

			reader := bytes.NewReader(frame.Payload)
			var classId, weight uint16
			var bodySize uint64

			binary.Read(reader, binary.BigEndian, &classId)
			binary.Read(reader, binary.BigEndian, &weight)
			binary.Read(reader, binary.BigEndian, &bodySize)

			c.server.Info("Processing basic header frame: channel=%d, bodySize=%d", frame.Channel, bodySize)

			// Read property flags
			var flags uint16
			binary.Read(reader, binary.BigEndian, &flags)

			// Parse properties based on flags
			if flags&0x8000 != 0 { // content-type
				pendingMessage.Properties.ContentType = readShortString(reader)
			}
			if flags&0x4000 != 0 { // content-encoding
				pendingMessage.Properties.ContentEncoding = readShortString(reader)
			}
			if flags&0x2000 != 0 { // headers
				pendingMessage.Properties.Headers = readTable(reader)
			}
			if flags&0x1000 != 0 { // delivery-mode
				binary.Read(reader, binary.BigEndian, &pendingMessage.Properties.DeliveryMode)
			}
			if flags&0x0800 != 0 { // priority
				binary.Read(reader, binary.BigEndian, &pendingMessage.Properties.Priority)
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
				binary.Read(reader, binary.BigEndian, &pendingMessage.Properties.Timestamp)
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
			if flags&0x0004 != 0 { // cluster-id
				pendingMessage.Properties.ClusterId = readShortString(reader)
			}

			ch.mu.Unlock()
		} else {
			ch.mu.Unlock()
			c.server.Warn("Received header frame with no pending message on channel %d", frame.Channel)
		}
	}
}

func (c *Connection) handleBody(frame *Frame) {
	// Get the channel and its pendingMessage
	c.mu.RLock()
	ch := c.channels[frame.Channel]
	c.mu.RUnlock()

	if ch != nil {
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
			c.deliverMessage(messageToDeliver)
		} else {
			ch.mu.Unlock()
			c.server.Warn("Received body frame with no pending message on channel %d", frame.Channel)
		}
	}
}

// New method to handle message delivery
func (c *Connection) deliverMessage(msg Message) {
	c.server.Info("=== DELIVER MESSAGE DEBUG ===")
	c.server.Info("Message: exchange=%s, routingKey=%s", msg.Exchange, msg.RoutingKey)

	c.server.mu.RLock()
	exchange := c.server.exchanges[msg.Exchange]
	c.server.mu.RUnlock()

	if exchange != nil {
		exchange.mu.RLock()
		queues := exchange.Bindings[msg.RoutingKey]
		c.server.Info("Found %d queues for routing key %s: %v", len(queues), msg.RoutingKey, queues)
		exchange.mu.RUnlock()

		for _, queueName := range queues {
			c.server.Info("Delivering to queue: %s", queueName)
			c.server.mu.RLock()
			queue := c.server.queues[queueName]
			c.server.mu.RUnlock()

			if queue != nil {
				queue.mu.Lock()
				// Create a deep copy of the message for this queue
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
				queue.mu.Unlock()
			}
		}
	}
	c.server.Info("=== END DELIVER MESSAGE DEBUG ===")
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

// colorize adds ANSI color to a string if the output is a terminal
func colorize(s string, color string) string {
	if isTerminal {
		return fmt.Sprintf("%s%s%s", color, s, colorReset)
	}
	return s
}

// Helper functions for better logging

// getFrameTypeName returns a string representation of a frame type
func getFrameTypeName(frameType byte) string {
	switch frameType {
	case FrameMethod:
		return "METHOD"
	case FrameHeader:
		return "HEADER"
	case FrameBody:
		return "BODY"
	case FrameHeartbeat:
		return "HEARTBEAT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", frameType)
	}
}

// getClassName returns a string representation of a class ID
func getClassName(classId uint16) string {
	switch classId {
	case ClassConnection:
		return "connection"
	case ClassChannel:
		return "channel"
	case ClassExchange:
		return "exchange"
	case ClassQueue:
		return "queue"
	case ClassBasic:
		return "basic"
	default:
		return fmt.Sprintf("unknown(%d)", classId)
	}
}

// getMethodName returns a string representation of a method ID within a class
func getMethodName(classId uint16, methodId uint16) string {
	switch classId {
	case ClassConnection:
		switch methodId {
		case MethodConnectionStart:
			return "start"
		case MethodConnectionStartOk:
			return "start-ok"
		case MethodConnectionTune:
			return "tune"
		case MethodConnectionTuneOk:
			return "tune-ok"
		case MethodConnectionOpen:
			return "open"
		case MethodConnectionOpenOk:
			return "open-ok"
		case MethodConnectionClose:
			return "close"
		case MethodConnectionCloseOk:
			return "close-ok"
		}
	case ClassChannel:
		switch methodId {
		case MethodChannelOpen:
			return "open"
		case MethodChannelOpenOk:
			return "open-ok"
		case MethodChannelClose:
			return "close"
		case MethodChannelCloseOk:
			return "close-ok"
		}
	case ClassExchange:
		switch methodId {
		case MethodExchangeDeclare:
			return "declare"
		case MethodExchangeDeclareOk:
			return "declare-ok"
		}
	case ClassQueue:
		switch methodId {
		case MethodQueueDeclare:
			return "declare"
		case MethodQueueDeclareOk:
			return "declare-ok"
		case MethodQueueBind:
			return "bind"
		case MethodQueueBindOk:
			return "bind-ok"
		}
	case ClassBasic:
		switch methodId {
		case MethodBasicConsume:
			return "consume"
		case MethodBasicConsumeOk:
			return "consume-ok"
		case MethodBasicPublish:
			return "publish"
		case MethodBasicDeliver:
			return "deliver"
		}
	}
	return fmt.Sprintf("unknown(%d)", methodId)
}

// getFullMethodName returns the complete method name as class.method
func getFullMethodName(classId uint16, methodId uint16) string {
	return fmt.Sprintf("%s.%s", getClassName(classId), getMethodName(classId, methodId))
}

func readShortString(reader *bytes.Reader) string {
	var length uint8
	err := binary.Read(reader, binary.BigEndian, &length)
	if err != nil {
		return ""
	}
	if length == 0 {
		return ""
	}
	data := make([]byte, length)
	n, err := reader.Read(data)
	if err != nil || n != int(length) {
		return ""
	}
	return string(data)
}

func readLongString(reader *bytes.Reader) string {
	var length uint32
	err := binary.Read(reader, binary.BigEndian, &length)
	if err != nil {
		return ""
	}
	if length == 0 {
		return ""
	}
	data := make([]byte, length)
	n, err := reader.Read(data)
	if err != nil || n != int(length) {
		return ""
	}
	return string(data)
}

func writeShortString(writer *bytes.Buffer, s string) {
	writer.WriteByte(uint8(len(s)))
	writer.WriteString(s)
}

func readTable(reader *bytes.Reader) map[string]interface{} {
	var length uint32
	err := binary.Read(reader, binary.BigEndian, &length)
	if err != nil {
		return make(map[string]interface{})
	}

	// If length is 0, return empty table
	if length == 0 {
		return make(map[string]interface{})
	}

	table := make(map[string]interface{})
	startLen := reader.Len()

	for startLen-reader.Len() < int(length) {
		key := readShortString(reader)
		valueType, err := reader.ReadByte()
		if err != nil {
			break
		}

		switch valueType {
		case 'S':
			table[key] = readLongString(reader)
		case 'I':
			var val int32
			binary.Read(reader, binary.BigEndian, &val)
			table[key] = val
		case 'F':
			table[key] = readTable(reader)
		case 't':
			b, _ := reader.ReadByte()
			table[key] = b != 0
		}
	}

	return table
}

func writeTable(writer *bytes.Buffer, table map[string]interface{}) {
	tableBuffer := &bytes.Buffer{}

	for key, value := range table {
		writeShortString(tableBuffer, key)

		switch v := value.(type) {
		case string:
			tableBuffer.WriteByte('S')
			binary.Write(tableBuffer, binary.BigEndian, uint32(len(v)))
			tableBuffer.WriteString(v)
		case int32:
			tableBuffer.WriteByte('I')
			binary.Write(tableBuffer, binary.BigEndian, v)
		case map[string]interface{}:
			tableBuffer.WriteByte('F')
			writeTable(tableBuffer, v)
		case bool:
			tableBuffer.WriteByte('t')
			if v {
				tableBuffer.WriteByte(1)
			} else {
				tableBuffer.WriteByte(0)
			}
		}
	}

	binary.Write(writer, binary.BigEndian, uint32(tableBuffer.Len()))
	writer.Write(tableBuffer.Bytes())
}

func main() {
	server := NewServer()
	server.Info("Starting AMQP server")
	if err := server.Start(":5672"); err != nil {
		server.Err("Failed to start server: %v", err)
		os.Exit(1)
	}
}
