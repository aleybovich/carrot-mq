// server.go
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

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
	Timestamp       time.Time
	Type            string
	UserId          string
	AppId           string
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
	id        uint16
	conn      *Connection
	consumers map[string]string
	mu        sync.Mutex
}

type Connection struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	channels map[uint16]*Channel
	server   *Server
	mu       sync.RWMutex
}

type Server struct {
	listener  net.Listener
	exchanges map[string]*Exchange
	queues    map[string]*Queue
	mu        sync.RWMutex
}

func NewServer() *Server {
	s := &Server{
		exchanges: make(map[string]*Exchange),
		queues:    make(map[string]*Queue),
	}

	// Create default direct exchange
	s.exchanges[""] = &Exchange{
		Name:     "",
		Type:     "direct",
		Bindings: make(map[string][]string),
	}

	return s
}

func (s *Server) Start(addr string) error {
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	c := &Connection{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
		channels: make(map[uint16]*Channel),
		server:   s,
	}

	protocol := make([]byte, 8)
	_, err := io.ReadFull(c.reader, protocol)
	if err != nil {
		return
	}

	if !bytes.Equal(protocol, []byte("AMQP\x00\x00\x09\x01")) {
		return
	}

	c.sendConnectionStart()

	for {
		frame, err := c.readFrame()
		if err != nil {
			return
		}

		if frame.Type == FrameMethod {
			c.handleMethod(frame)
		} else if frame.Type == FrameHeader {
			c.handleHeader(frame)
		} else if frame.Type == FrameBody {
			c.handleBody(frame)
		}
	}
}

func (c *Connection) readFrame() (*Frame, error) {
	header := make([]byte, 7)
	_, err := io.ReadFull(c.reader, header)
	if err != nil {
		return nil, err
	}

	frame := &Frame{
		Type:    header[0],
		Channel: binary.BigEndian.Uint16(header[1:3]),
	}

	size := binary.BigEndian.Uint32(header[3:7])
	frame.Payload = make([]byte, size)
	_, err = io.ReadFull(c.reader, frame.Payload)
	if err != nil {
		return nil, err
	}

	frameEnd := make([]byte, 1)
	_, err = io.ReadFull(c.reader, frameEnd)
	if err != nil {
		return nil, err
	}

	if frameEnd[0] != FrameEnd {
		return nil, fmt.Errorf("invalid frame end")
	}

	return frame, nil
}

func (c *Connection) writeFrame(frame *Frame) error {
	header := make([]byte, 7)
	header[0] = frame.Type
	binary.BigEndian.PutUint16(header[1:3], frame.Channel)
	binary.BigEndian.PutUint32(header[3:7], uint32(len(frame.Payload)))

	c.writer.Write(header)
	c.writer.Write(frame.Payload)
	c.writer.WriteByte(FrameEnd)
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

	c.writeFrame(&Frame{
		Type:    FrameMethod,
		Channel: 0,
		Payload: payload.Bytes(),
	})
}

var currentMessage *Message

func (c *Connection) handleMethod(frame *Frame) {
	reader := bytes.NewReader(frame.Payload)
	var classId, methodId uint16
	binary.Read(reader, binary.BigEndian, &classId)
	binary.Read(reader, binary.BigEndian, &methodId)

	switch classId {
	case ClassConnection:
		c.handleConnectionMethod(methodId, reader)
	case ClassChannel:
		c.handleChannelMethod(methodId, reader, frame.Channel)
	case ClassExchange:
		c.handleExchangeMethod(methodId, reader, frame.Channel)
	case ClassQueue:
		c.handleQueueMethod(methodId, reader, frame.Channel)
	case ClassBasic:
		c.handleBasicMethod(methodId, reader, frame.Channel)
	}
}

func (c *Connection) handleConnectionMethod(methodId uint16, reader *bytes.Reader) {
	switch methodId {
	case MethodConnectionStartOk:
		readTable(reader)
		readShortString(reader)
		readLongString(reader)
		readShortString(reader)

		c.sendConnectionTune()

	case MethodConnectionTuneOk:
		var channelMax uint16
		var frameMax uint32
		var heartbeat uint16
		binary.Read(reader, binary.BigEndian, &channelMax)
		binary.Read(reader, binary.BigEndian, &frameMax)
		binary.Read(reader, binary.BigEndian, &heartbeat)

	case MethodConnectionOpen:
		vhost := readShortString(reader)
		reserved1, _ := reader.ReadByte()
		reserved2, _ := reader.ReadByte()

		// Use vhost if needed
		_ = vhost

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
		binary.Write(payload, binary.BigEndian, uint16(MethodConnectionOpenOk))
		payload.WriteByte(reserved1 & reserved2) // Using both reserved bytes

		c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: 0,
			Payload: payload.Bytes(),
		})
	}
}

func (c *Connection) sendConnectionTune() {
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
	binary.Write(payload, binary.BigEndian, uint16(MethodConnectionTune))
	binary.Write(payload, binary.BigEndian, uint16(0))
	binary.Write(payload, binary.BigEndian, uint32(131072))
	binary.Write(payload, binary.BigEndian, uint16(60))

	c.writeFrame(&Frame{
		Type:    FrameMethod,
		Channel: 0,
		Payload: payload.Bytes(),
	})
}

func (c *Connection) handleChannelMethod(methodId uint16, reader *bytes.Reader, channelId uint16) {
	switch methodId {
	case MethodChannelOpen:
		reserved := readShortString(reader)
		_ = reserved

		c.mu.Lock()
		c.channels[channelId] = &Channel{
			id:        channelId,
			conn:      c,
			consumers: make(map[string]string),
		}
		c.mu.Unlock()

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassChannel))
		binary.Write(payload, binary.BigEndian, uint16(MethodChannelOpenOk))
		binary.Write(payload, binary.BigEndian, uint32(0))

		c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channelId,
			Payload: payload.Bytes(),
		})
	}
}

func (c *Connection) handleExchangeMethod(methodId uint16, reader *bytes.Reader, channel uint16) {
	switch methodId {
	case MethodExchangeDeclare:
		reserved1, _ := reader.ReadByte()
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

		_ = passive
		_ = args
		_ = noWait
		_ = reserved1

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
		}
		c.server.mu.Unlock()

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassExchange))
		binary.Write(payload, binary.BigEndian, uint16(MethodExchangeDeclareOk))

		c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channel,
			Payload: payload.Bytes(),
		})
	}
}

func (c *Connection) handleQueueMethod(methodId uint16, reader *bytes.Reader, channel uint16) {
	switch methodId {
	case MethodQueueDeclare:
		reserved1, _ := reader.ReadByte()
		queue := readShortString(reader)
		bits, _ := reader.ReadByte()
		args := readTable(reader)

		// Parse bits
		passive := bits&0x01 != 0
		durable := bits&0x02 != 0
		exclusive := bits&0x04 != 0
		autoDelete := bits&0x08 != 0
		noWait := bits&0x10 != 0

		_ = passive
		_ = args
		_ = noWait
		_ = reserved1

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
		}
		c.server.mu.Unlock()

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassQueue))
		binary.Write(payload, binary.BigEndian, uint16(MethodQueueDeclareOk))
		writeShortString(payload, queue)
		binary.Write(payload, binary.BigEndian, uint32(0))
		binary.Write(payload, binary.BigEndian, uint32(0))

		c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channel,
			Payload: payload.Bytes(),
		})

	case MethodQueueBind:
		reserved1, _ := reader.ReadByte()
		queue := readShortString(reader)
		exchange := readShortString(reader)
		routingKey := readShortString(reader)
		bits, _ := reader.ReadByte()
		args := readTable(reader)

		// Parse bits
		noWait := bits&0x01 != 0

		_ = noWait
		_ = args
		_ = reserved1

		c.server.mu.Lock()
		if ex, ok := c.server.exchanges[exchange]; ok {
			ex.mu.Lock()
			ex.Bindings[routingKey] = append(ex.Bindings[routingKey], queue)
			ex.mu.Unlock()
		}
		if q, ok := c.server.queues[queue]; ok {
			q.mu.Lock()
			q.Bindings[routingKey] = true
			q.mu.Unlock()
		}
		c.server.mu.Unlock()

		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassQueue))
		binary.Write(payload, binary.BigEndian, uint16(MethodQueueBindOk))

		c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channel,
			Payload: payload.Bytes(),
		})
	}
}

func (c *Connection) handleBasicMethod(methodId uint16, reader *bytes.Reader, channel uint16) {
	switch methodId {
	case MethodBasicPublish:
		reserved1, _ := reader.ReadByte()
		exchange := readShortString(reader)
		routingKey := readShortString(reader)
		bits, _ := reader.ReadByte()

		// Parse bits
		mandatory := bits&0x01 != 0
		immediate := bits&0x02 != 0

		_ = reserved1

		currentMessage = &Message{
			Exchange:   exchange,
			RoutingKey: routingKey,
			Mandatory:  mandatory,
			Immediate:  immediate,
		}

	case MethodBasicConsume:
		reserved1, _ := reader.ReadByte()
		queue := readShortString(reader)
		consumerTag := readShortString(reader)
		bits, _ := reader.ReadByte()
		args := readTable(reader)

		// Parse bits
		noLocal := bits&0x01 != 0
		noAck := bits&0x02 != 0
		exclusive := bits&0x04 != 0
		noWait := bits&0x08 != 0

		_ = noLocal
		_ = noAck
		_ = exclusive
		_ = noWait
		_ = args
		_ = reserved1

		c.mu.RLock()
		ch := c.channels[channel]
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

			payload := &bytes.Buffer{}
			binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
			binary.Write(payload, binary.BigEndian, uint16(MethodBasicConsumeOk))
			writeShortString(payload, consumerTag)

			c.writeFrame(&Frame{
				Type:    FrameMethod,
				Channel: channel,
				Payload: payload.Bytes(),
			})

			go c.deliverMessages(channel, consumerTag, msgChan)
		}
	}
}

func (c *Connection) handleHeader(frame *Frame) {
	reader := bytes.NewReader(frame.Payload)
	var classId uint16
	var weight uint16
	var bodySize uint64

	binary.Read(reader, binary.BigEndian, &classId)
	binary.Read(reader, binary.BigEndian, &weight)
	binary.Read(reader, binary.BigEndian, &bodySize)

	var flags uint16
	binary.Read(reader, binary.BigEndian, &flags)

	if currentMessage != nil {
		props := &Properties{}

		if flags&0x8000 != 0 {
			props.ContentType = readShortString(reader)
		}
		if flags&0x4000 != 0 {
			props.ContentEncoding = readShortString(reader)
		}
		if flags&0x2000 != 0 {
			props.Headers = readTable(reader)
		}
		if flags&0x1000 != 0 {
			binary.Read(reader, binary.BigEndian, &props.DeliveryMode)
		}
		if flags&0x0800 != 0 {
			binary.Read(reader, binary.BigEndian, &props.Priority)
		}
		if flags&0x0400 != 0 {
			props.CorrelationId = readShortString(reader)
		}
		if flags&0x0200 != 0 {
			props.ReplyTo = readShortString(reader)
		}
		if flags&0x0100 != 0 {
			props.Expiration = readShortString(reader)
		}
		if flags&0x0080 != 0 {
			props.MessageId = readShortString(reader)
		}
		if flags&0x0040 != 0 {
			var timestamp uint64
			binary.Read(reader, binary.BigEndian, &timestamp)
			props.Timestamp = time.Unix(int64(timestamp), 0)
		}
		if flags&0x0020 != 0 {
			props.Type = readShortString(reader)
		}
		if flags&0x0010 != 0 {
			props.UserId = readShortString(reader)
		}
		if flags&0x0008 != 0 {
			props.AppId = readShortString(reader)
		}

		currentMessage.Properties = *props
	}
}

func (c *Connection) handleBody(frame *Frame) {
	if currentMessage != nil {
		currentMessage.Body = frame.Payload

		c.server.mu.RLock()
		exchange := c.server.exchanges[currentMessage.Exchange]
		c.server.mu.RUnlock()

		if exchange != nil {
			exchange.mu.RLock()
			queues := exchange.Bindings[currentMessage.RoutingKey]
			exchange.mu.RUnlock()

			for _, queueName := range queues {
				c.server.mu.RLock()
				queue := c.server.queues[queueName]
				c.server.mu.RUnlock()

				if queue != nil {
					queue.mu.Lock()
					queue.Messages = append(queue.Messages, *currentMessage)

					for _, ch := range queue.Consumers {
						select {
						case ch <- *currentMessage:
						default:
						}
					}
					queue.mu.Unlock()
				}
			}
		}

		currentMessage = nil
	}
}

func (c *Connection) deliverMessages(channel uint16, consumerTag string, msgChan chan Message) {
	deliveryTag := uint64(1)

	for msg := range msgChan {
		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
		binary.Write(payload, binary.BigEndian, uint16(MethodBasicDeliver))
		writeShortString(payload, consumerTag)
		binary.Write(payload, binary.BigEndian, deliveryTag)
		payload.WriteByte(0)
		writeShortString(payload, msg.Exchange)
		writeShortString(payload, msg.RoutingKey)

		c.writeFrame(&Frame{
			Type:    FrameMethod,
			Channel: channel,
			Payload: payload.Bytes(),
		})

		headerPayload := &bytes.Buffer{}
		binary.Write(headerPayload, binary.BigEndian, uint16(ClassBasic))
		binary.Write(headerPayload, binary.BigEndian, uint16(0))
		binary.Write(headerPayload, binary.BigEndian, uint64(len(msg.Body)))

		flags := uint16(0)
		if msg.Properties.ContentType != "" {
			flags |= 0x8000
		}
		if msg.Properties.DeliveryMode != 0 {
			flags |= 0x1000
		}

		binary.Write(headerPayload, binary.BigEndian, flags)

		if msg.Properties.ContentType != "" {
			writeShortString(headerPayload, msg.Properties.ContentType)
		}
		if msg.Properties.DeliveryMode != 0 {
			binary.Write(headerPayload, binary.BigEndian, msg.Properties.DeliveryMode)
		}

		c.writeFrame(&Frame{
			Type:    FrameHeader,
			Channel: channel,
			Payload: headerPayload.Bytes(),
		})

		c.writeFrame(&Frame{
			Type:    FrameBody,
			Channel: channel,
			Payload: msg.Body,
		})

		deliveryTag++
	}
}

func readShortString(reader *bytes.Reader) string {
	var length uint8
	binary.Read(reader, binary.BigEndian, &length)
	data := make([]byte, length)
	reader.Read(data)
	return string(data)
}

func readLongString(reader *bytes.Reader) string {
	var length uint32
	binary.Read(reader, binary.BigEndian, &length)
	data := make([]byte, length)
	reader.Read(data)
	return string(data)
}

func writeShortString(writer *bytes.Buffer, s string) {
	writer.WriteByte(uint8(len(s)))
	writer.WriteString(s)
}

func readTable(reader *bytes.Reader) map[string]interface{} {
	var length uint32
	binary.Read(reader, binary.BigEndian, &length)

	table := make(map[string]interface{})
	end := reader.Len() - int(length)

	for reader.Len() > end {
		key := readShortString(reader)
		valueType, _ := reader.ReadByte()

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
	go server.Start(":5672")
	select {}
}
