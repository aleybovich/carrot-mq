package carrotmq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// mockConn implements net.Conn for testing
type mockConn struct {
	readBuffer  *bytes.Buffer
	writeBuffer *bytes.Buffer
	closed      bool
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return m.readBuffer.Read(b)
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	return m.writeBuffer.Write(b)
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// TestBasicQosMethod tests the Basic.Qos method handling
func TestBasicQosMethod(t *testing.T) {
	server := NewServer()

	// Create mock connection
	mockNetConn := &mockConn{
		readBuffer:  bytes.NewBuffer([]byte{}),
		writeBuffer: bytes.NewBuffer([]byte{}),
	}

	conn := &Connection{
		conn:     mockNetConn,
		reader:   bufio.NewReader(mockNetConn),
		writer:   bufio.NewWriter(mockNetConn),
		server:   server,
		channels: make(map[uint16]*Channel),
		vhost:    &VHost{name: "/"},
	}

	// Create a test channel
	ch := &Channel{
		id:              1,
		conn:            conn,
		unackedMessages: make(map[uint64]*UnackedMessage),
		consumers:       make(map[string]string),
	}
	conn.channels[1] = ch

	tests := []struct {
		name          string
		prefetchSize  uint32
		prefetchCount uint16
		global        bool
		expectError   bool
	}{
		{
			name:          "valid qos with prefetch count",
			prefetchSize:  0,
			prefetchCount: 10,
			global:        false,
			expectError:   false,
		},
		{
			name:          "valid qos unlimited",
			prefetchSize:  0,
			prefetchCount: 0,
			global:        false,
			expectError:   false,
		},
		{
			name:          "valid qos with size and count",
			prefetchSize:  1048576, // 1MB
			prefetchCount: 5,
			global:        true,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear write buffer
			mockNetConn.writeBuffer.Reset()

			// Create method payload
			payload := &bytes.Buffer{}
			binary.Write(payload, binary.BigEndian, tt.prefetchSize)
			binary.Write(payload, binary.BigEndian, tt.prefetchCount)
			if tt.global {
				payload.WriteByte(1)
			} else {
				payload.WriteByte(0)
			}

			reader := bytes.NewReader(payload.Bytes())
			err := conn.handleMethodBasicQos(reader, 1)

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Verify QoS was set on channel
			if !tt.expectError {
				ch.mu.Lock()
				prefetchCount := ch.prefetchCount
				prefetchSize := ch.prefetchSize
				qosGlobal := ch.qosGlobal
				ch.mu.Unlock()

				if prefetchCount != tt.prefetchCount {
					t.Errorf("Expected prefetchCount %d, got %d", tt.prefetchCount, prefetchCount)
				}
				if prefetchSize != tt.prefetchSize {
					t.Errorf("Expected prefetchSize %d, got %d", tt.prefetchSize, prefetchSize)
				}
				if qosGlobal != tt.global {
					t.Errorf("Expected global %v, got %v", tt.global, qosGlobal)
				}

				// Verify QosOk was sent
				responseData := mockNetConn.writeBuffer.Bytes()
				if len(responseData) == 0 {
					t.Error("Expected Basic.QosOk frame to be sent")
				}
			}
		})
	}
}

// TestQosPrefetchLimit tests that deliverMessages respects prefetch limits
func TestQosPrefetchLimit(t *testing.T) {
	server := NewServer()
	vhost := &VHost{
		name:      "/",
		queues:    make(map[string]*Queue),
		exchanges: make(map[string]*Exchange),
	}
	server.vhosts["/"] = vhost

	// Create a queue with messages
	queue := &Queue{
		Name:      "test-queue",
		Messages:  []Message{},
		Consumers: make(map[string]*Consumer),
		Bindings:  make(map[string]bool),
	}

	// Add 10 messages to the queue
	for i := 0; i < 10; i++ {
		queue.Messages = append(queue.Messages, Message{
			Body:       []byte("test message"),
			RoutingKey: "test",
			Properties: Properties{},
		})
	}
	vhost.queues["test-queue"] = queue

	// Create mock connection
	mockNetConn := &mockConn{
		readBuffer:  bytes.NewBuffer([]byte{}),
		writeBuffer: bytes.NewBuffer([]byte{}),
	}

	// Create connection and channel
	conn := &Connection{
		conn:     mockNetConn,
		reader:   bufio.NewReader(mockNetConn),
		writer:   bufio.NewWriter(mockNetConn),
		server:   server,
		channels: make(map[uint16]*Channel),
		vhost:    vhost,
	}

	ch := &Channel{
		id:              1,
		conn:            conn,
		consumers:       make(map[string]string),
		unackedMessages: make(map[uint64]*UnackedMessage),
		prefetchCount:   2, // Set prefetch limit to 2
	}
	conn.channels[1] = ch

	// Create consumer
	consumer := &Consumer{
		Tag:       "test-consumer",
		ChannelId: 1,
		NoAck:     false, // Important: noAck must be false for QoS to apply
		Queue:     queue,
		stopCh:    make(chan struct{}),
	}

	ch.consumers["test-consumer"] = "test-queue"
	queue.Consumers["test-consumer"] = consumer

	// Start delivery in a goroutine
	done := make(chan bool)
	go func() {
		conn.deliverMessages(1, "test-consumer", consumer)
		done <- true
	}()

	// Give it time to deliver messages
	time.Sleep(200 * time.Millisecond)

	// Check unacked messages count
	ch.mu.Lock()
	unackedCount := 0
	for _, unacked := range ch.unackedMessages {
		if unacked.ConsumerTag == "test-consumer" {
			unackedCount++
		}
	}
	ch.mu.Unlock()

	// Should have exactly 2 unacked messages (prefetch limit)
	if unackedCount != 2 {
		t.Errorf("Expected 2 unacked messages (prefetch limit), got %d", unackedCount)
	}

	// Check queue still has 8 messages
	queue.mu.Lock()
	remainingMessages := len(queue.Messages)
	queue.mu.Unlock()

	if remainingMessages != 8 {
		t.Errorf("Expected 8 messages remaining in queue, got %d", remainingMessages)
	}

	// Stop consumer
	close(consumer.stopCh)
	<-done
}

// TestQosWithAck tests that acknowledging messages allows more to be delivered
func TestQosWithAck(t *testing.T) {
	server := NewServer()
	vhost := &VHost{
		name:      "/",
		queues:    make(map[string]*Queue),
		exchanges: make(map[string]*Exchange),
	}
	server.vhosts["/"] = vhost

	// Create a queue with messages
	queue := &Queue{
		Name:      "test-queue",
		Messages:  []Message{},
		Consumers: make(map[string]*Consumer),
		Bindings:  make(map[string]bool),
	}

	// Add 5 messages
	for i := 0; i < 5; i++ {
		queue.Messages = append(queue.Messages, Message{
			Body:       []byte("test message"),
			RoutingKey: "test",
			Properties: Properties{},
		})
	}
	vhost.queues["test-queue"] = queue

	// Create mock connection
	mockNetConn := &mockConn{
		readBuffer:  bytes.NewBuffer([]byte{}),
		writeBuffer: bytes.NewBuffer([]byte{}),
	}

	// Create connection and channel
	conn := &Connection{
		conn:     mockNetConn,
		reader:   bufio.NewReader(mockNetConn),
		writer:   bufio.NewWriter(mockNetConn),
		server:   server,
		channels: make(map[uint16]*Channel),
		vhost:    vhost,
	}

	ch := &Channel{
		id:              1,
		conn:            conn,
		consumers:       make(map[string]string),
		unackedMessages: make(map[uint64]*UnackedMessage),
		prefetchCount:   1, // Prefetch limit of 1
		deliveryTag:     0,
	}
	conn.channels[1] = ch

	// Create consumer
	consumer := &Consumer{
		Tag:       "test-consumer",
		ChannelId: 1,
		NoAck:     false,
		Queue:     queue,
		stopCh:    make(chan struct{}),
	}

	ch.consumers["test-consumer"] = "test-queue"
	queue.Consumers["test-consumer"] = consumer

	// Start delivery
	done := make(chan bool)
	go func() {
		conn.deliverMessages(1, "test-consumer", consumer)
		done <- true
	}()

	// Wait for first message
	time.Sleep(100 * time.Millisecond)

	// Check we have 1 unacked message
	ch.mu.Lock()
	unackedBefore := len(ch.unackedMessages)
	var firstDeliveryTag uint64
	for tag := range ch.unackedMessages {
		firstDeliveryTag = tag
		break
	}
	ch.mu.Unlock()

	if unackedBefore != 1 {
		t.Fatalf("Expected 1 unacked message initially, got %d", unackedBefore)
	}

	// Acknowledge the first message
	ackPayload := &bytes.Buffer{}
	binary.Write(ackPayload, binary.BigEndian, firstDeliveryTag)
	ackPayload.WriteByte(0) // multiple = false

	ackReader := bytes.NewReader(ackPayload.Bytes())
	err := conn.handleMethodBasicAck(ackReader, 1)
	if err != nil {
		t.Fatalf("Error acknowledging message: %v", err)
	}

	// Wait for next message to be delivered
	time.Sleep(100 * time.Millisecond)

	// Check we still have 1 unacked message (the new one)
	ch.mu.Lock()
	unackedAfter := len(ch.unackedMessages)
	ch.mu.Unlock()

	if unackedAfter != 1 {
		t.Errorf("Expected 1 unacked message after ack, got %d", unackedAfter)
	}

	// Check queue has fewer messages
	queue.mu.Lock()
	remainingMessages := len(queue.Messages)
	queue.mu.Unlock()

	if remainingMessages != 3 { // Started with 5, delivered 2
		t.Errorf("Expected 3 messages remaining in queue, got %d", remainingMessages)
	}

	// Stop consumer
	close(consumer.stopCh)
	<-done
}

// TestQosNoAckMode tests that QoS doesn't apply when noAck is true
func TestQosNoAckMode(t *testing.T) {
	server := NewServer()
	vhost := &VHost{
		name:      "/",
		queues:    make(map[string]*Queue),
		exchanges: make(map[string]*Exchange),
	}
	server.vhosts["/"] = vhost

	// Create a queue with messages
	queue := &Queue{
		Name:      "test-queue",
		Messages:  []Message{},
		Consumers: make(map[string]*Consumer),
		Bindings:  make(map[string]bool),
	}

	// Add 5 messages
	for i := 0; i < 5; i++ {
		queue.Messages = append(queue.Messages, Message{
			Body:       []byte("test message"),
			RoutingKey: "test",
			Properties: Properties{},
		})
	}
	vhost.queues["test-queue"] = queue

	// Create mock connection
	mockNetConn := &mockConn{
		readBuffer:  bytes.NewBuffer([]byte{}),
		writeBuffer: bytes.NewBuffer([]byte{}),
	}

	// Create connection and channel
	conn := &Connection{
		conn:     mockNetConn,
		reader:   bufio.NewReader(mockNetConn),
		writer:   bufio.NewWriter(mockNetConn),
		server:   server,
		channels: make(map[uint16]*Channel),
		vhost:    vhost,
	}

	ch := &Channel{
		id:              1,
		conn:            conn,
		consumers:       make(map[string]string),
		unackedMessages: make(map[uint64]*UnackedMessage),
		prefetchCount:   1, // This should be ignored for noAck
	}
	conn.channels[1] = ch

	// Create consumer with noAck = true
	consumer := &Consumer{
		Tag:       "test-consumer",
		ChannelId: 1,
		NoAck:     true, // QoS should not apply
		Queue:     queue,
		stopCh:    make(chan struct{}),
	}

	ch.consumers["test-consumer"] = "test-queue"
	queue.Consumers["test-consumer"] = consumer

	// Start delivery
	done := make(chan bool)
	go func() {
		conn.deliverMessages(1, "test-consumer", consumer)
		done <- true
	}()

	// Give enough time for all messages to be delivered
	time.Sleep(300 * time.Millisecond)

	// Check no unacked messages (noAck mode)
	ch.mu.Lock()
	unackedCount := len(ch.unackedMessages)
	ch.mu.Unlock()

	if unackedCount != 0 {
		t.Errorf("Expected 0 unacked messages in noAck mode, got %d", unackedCount)
	}

	// Check queue is empty
	queue.mu.Lock()
	remainingMessages := len(queue.Messages)
	queue.mu.Unlock()

	if remainingMessages != 0 {
		t.Errorf("Expected 0 messages remaining in queue (noAck mode), got %d", remainingMessages)
	}

	// Stop consumer
	close(consumer.stopCh)
	<-done
}

// TestQosPerChannel tests that QoS settings are per-channel
func TestQosPerChannel(t *testing.T) {
	server := NewServer()

	// Create mock connection
	mockNetConn := &mockConn{
		readBuffer:  bytes.NewBuffer([]byte{}),
		writeBuffer: bytes.NewBuffer([]byte{}),
	}

	conn := &Connection{
		conn:     mockNetConn,
		reader:   bufio.NewReader(mockNetConn),
		writer:   bufio.NewWriter(mockNetConn),
		server:   server,
		channels: make(map[uint16]*Channel),
		vhost:    &VHost{name: "/"},
	}

	// Create two channels with different QoS settings
	ch1 := &Channel{
		id:              1,
		conn:            conn,
		unackedMessages: make(map[uint64]*UnackedMessage),
		consumers:       make(map[string]string),
	}
	ch2 := &Channel{
		id:              2,
		conn:            conn,
		unackedMessages: make(map[uint64]*UnackedMessage),
		consumers:       make(map[string]string),
	}

	conn.channels[1] = ch1
	conn.channels[2] = ch2

	// Set different QoS on each channel
	qos1Payload := &bytes.Buffer{}
	binary.Write(qos1Payload, binary.BigEndian, uint32(0))
	binary.Write(qos1Payload, binary.BigEndian, uint16(5))
	qos1Payload.WriteByte(0)

	qos2Payload := &bytes.Buffer{}
	binary.Write(qos2Payload, binary.BigEndian, uint32(0))
	binary.Write(qos2Payload, binary.BigEndian, uint16(10))
	qos2Payload.WriteByte(0)

	// Apply QoS to channel 1
	reader1 := bytes.NewReader(qos1Payload.Bytes())
	err := conn.handleMethodBasicQos(reader1, 1)
	if err != nil {
		t.Fatalf("Error setting QoS on channel 1: %v", err)
	}

	// Apply QoS to channel 2
	reader2 := bytes.NewReader(qos2Payload.Bytes())
	err = conn.handleMethodBasicQos(reader2, 2)
	if err != nil {
		t.Fatalf("Error setting QoS on channel 2: %v", err)
	}

	// Verify each channel has its own QoS settings
	ch1.mu.Lock()
	ch1PrefetchCount := ch1.prefetchCount
	ch1.mu.Unlock()

	ch2.mu.Lock()
	ch2PrefetchCount := ch2.prefetchCount
	ch2.mu.Unlock()

	if ch1PrefetchCount != 5 {
		t.Errorf("Channel 1 prefetchCount = %d, expected 5", ch1PrefetchCount)
	}
	if ch2PrefetchCount != 10 {
		t.Errorf("Channel 2 prefetchCount = %d, expected 10", ch2PrefetchCount)
	}
}

// TestQosPrefetchSize tests that deliverMessages respects prefetch size limits
func TestQosPrefetchSize(t *testing.T) {
	server := NewServer()
	vhost := &VHost{
		name:      "/",
		queues:    make(map[string]*Queue),
		exchanges: make(map[string]*Exchange),
	}
	server.vhosts["/"] = vhost

	// Create a queue with messages of different sizes
	queue := &Queue{
		Name:      "test-queue",
		Messages:  []Message{},
		Consumers: make(map[string]*Consumer),
		Bindings:  make(map[string]bool),
	}

	// Add messages with different sizes
	// Small message (100 bytes)
	queue.Messages = append(queue.Messages, Message{
		Body:       make([]byte, 100),
		RoutingKey: "test",
		Properties: Properties{},
	})

	// Medium message (500 bytes)
	queue.Messages = append(queue.Messages, Message{
		Body:       make([]byte, 500),
		RoutingKey: "test",
		Properties: Properties{},
	})

	// Large message (1000 bytes)
	queue.Messages = append(queue.Messages, Message{
		Body:       make([]byte, 1000),
		RoutingKey: "test",
		Properties: Properties{},
	})

	vhost.queues["test-queue"] = queue

	// Create mock connection
	mockNetConn := &mockConn{
		readBuffer:  bytes.NewBuffer([]byte{}),
		writeBuffer: bytes.NewBuffer([]byte{}),
	}

	// Create connection and channel
	conn := &Connection{
		conn:     mockNetConn,
		reader:   bufio.NewReader(mockNetConn),
		writer:   bufio.NewWriter(mockNetConn),
		server:   server,
		channels: make(map[uint16]*Channel),
		vhost:    vhost,
	}

	ch := &Channel{
		id:              1,
		conn:            conn,
		consumers:       make(map[string]string),
		unackedMessages: make(map[uint64]*UnackedMessage),
		prefetchSize:    1024, // Set size limit to ~1KB (accounting for overhead)
		prefetchCount:   0,    // No count limit
	}
	conn.channels[1] = ch

	// Create consumer
	consumer := &Consumer{
		Tag:       "test-consumer",
		ChannelId: 1,
		NoAck:     false,
		Queue:     queue,
		stopCh:    make(chan struct{}),
	}

	ch.consumers["test-consumer"] = "test-queue"
	queue.Consumers["test-consumer"] = consumer

	// Start delivery in a goroutine
	done := make(chan bool)
	go func() {
		conn.deliverMessages(1, "test-consumer", consumer)
		done <- true
	}()

	// Give it time to deliver messages
	time.Sleep(200 * time.Millisecond)

	// Check unacked messages
	ch.mu.Lock()
	unackedCount := 0
	unackedSize := uint32(0)
	for _, unacked := range ch.unackedMessages {
		if unacked.ConsumerTag == "test-consumer" {
			unackedCount++
			unackedSize += uint32(len(unacked.Message.Body))
		}
	}
	ch.mu.Unlock()

	// Should have delivered 2 messages (100 + 500 bytes + overhead < 1024)
	// But not the third (would exceed limit)
	if unackedCount != 2 {
		t.Errorf("Expected 2 unacked messages (size limit), got %d", unackedCount)
	}

	if unackedSize > ch.prefetchSize {
		t.Errorf("Unacked size %d exceeds prefetch size %d", unackedSize, ch.prefetchSize)
	}

	// Check queue still has 1 message
	queue.mu.Lock()
	remainingMessages := len(queue.Messages)
	queue.mu.Unlock()

	if remainingMessages != 1 {
		t.Errorf("Expected 1 message remaining in queue, got %d", remainingMessages)
	}

	// Stop consumer
	close(consumer.stopCh)
	<-done
}

// TestQosSizeAndCount tests that both size and count limits are respected
func TestQosSizeAndCount(t *testing.T) {
	server := NewServer()
	vhost := &VHost{
		name:      "/",
		queues:    make(map[string]*Queue),
		exchanges: make(map[string]*Exchange),
	}
	server.vhosts["/"] = vhost

	// Create a queue with small messages
	queue := &Queue{
		Name:      "test-queue",
		Messages:  []Message{},
		Consumers: make(map[string]*Consumer),
		Bindings:  make(map[string]bool),
	}

	// Add 10 small messages (50 bytes each)
	for i := 0; i < 10; i++ {
		queue.Messages = append(queue.Messages, Message{
			Body:       make([]byte, 50),
			RoutingKey: "test",
			Properties: Properties{},
		})
	}

	vhost.queues["test-queue"] = queue

	// Create mock connection
	mockNetConn := &mockConn{
		readBuffer:  bytes.NewBuffer([]byte{}),
		writeBuffer: bytes.NewBuffer([]byte{}),
	}

	// Create connection and channel
	conn := &Connection{
		conn:     mockNetConn,
		reader:   bufio.NewReader(mockNetConn),
		writer:   bufio.NewWriter(mockNetConn),
		server:   server,
		channels: make(map[uint16]*Channel),
		vhost:    vhost,
	}

	ch := &Channel{
		id:              1,
		conn:            conn,
		consumers:       make(map[string]string),
		unackedMessages: make(map[uint64]*UnackedMessage),
		prefetchSize:    2048, // Size limit that would allow ~6-7 messages
		prefetchCount:   3,    // But count limit is only 3
	}
	conn.channels[1] = ch

	// Create consumer
	consumer := &Consumer{
		Tag:       "test-consumer",
		ChannelId: 1,
		NoAck:     false,
		Queue:     queue,
		stopCh:    make(chan struct{}),
	}

	ch.consumers["test-consumer"] = "test-queue"
	queue.Consumers["test-consumer"] = consumer

	// Start delivery
	done := make(chan bool)
	go func() {
		conn.deliverMessages(1, "test-consumer", consumer)
		done <- true
	}()

	// Give it time to deliver messages
	time.Sleep(200 * time.Millisecond)

	// Check unacked messages
	ch.mu.Lock()
	unackedCount := 0
	for _, unacked := range ch.unackedMessages {
		if unacked.ConsumerTag == "test-consumer" {
			unackedCount++
		}
	}
	ch.mu.Unlock()

	// Should have delivered exactly 3 messages (count limit reached first)
	if unackedCount != 3 {
		t.Errorf("Expected 3 unacked messages (count limit), got %d", unackedCount)
	}

	// Stop consumer
	close(consumer.stopCh)
	<-done
}
