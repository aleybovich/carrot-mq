package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
)

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
	case ClassConfirm:
		return "confirm"
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
	case ClassConfirm:
		switch methodId {
		case MethodConfirmSelect:
			return "select"
		case MethodConfirmSelectOk:
			return "select-ok"
		}
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
		case MethodExchangeDelete:
			return "delete"
		case MethodExchangeDeleteOk:
			return "delete-ok"
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
		case MethodQueueUnbind:
			return "unbind"
		case MethodQueueUnbindOk:
			return "unbind-ok"
		case MethodQueuePurge:
			return "purge"
		case MethodQueuePurgeOk:
			return "purge-ok"
		case MethodQueueDelete:
			return "delete"
		case MethodQueueDeleteOk:
			return "delete-ok"
		}
	case ClassBasic:
		switch methodId {
		case MethodBasicConsume:
			return "consume"
		case MethodBasicConsumeOk:
			return "consume-ok"
		case MethodBasicCancel:
			return "cancel"
		case MethodBasicCancelOk:
			return "cancel-ok"
		case MethodBasicPublish:
			return "publish"
		case MethodBasicDeliver:
			return "deliver"
		case MethodBasicReturn:
			return "return"
		case MethodBasicAck:
			return "ack"
		case MethodBasicReject:
			return "reject"
		case MethodBasicNack:
			return "nack"
		case MethodBasicGet:
			return "get"
		case MethodBasicGetOk:
			return "get-ok"
		case MethodBasicGetEmpty:
			return "get-empty"
		case MethodBasicRecover:
			return "recover"
		case MethodBasicRecoverOk:
			return "recover-ok"
		}
	}
	return fmt.Sprintf("unknown(%d)", methodId)
}

// getFullMethodName returns the complete method name as class.method
func getFullMethodName(classId uint16, methodId uint16) string {
	return fmt.Sprintf("%s.%s", getClassName(classId), getMethodName(classId, methodId))
}

func readShortString(reader *bytes.Reader) (string, error) {
	var length uint8
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return "", fmt.Errorf("reading short string length: %w", err)
	}

	if length == 0 {
		return "", nil // Successfully read an empty string
	}

	// Check if enough bytes are available before attempting to read.
	// This is particularly useful if the reader is not guaranteed to block.
	if int(length) > reader.Len() {
		return "", fmt.Errorf("not enough data for short string: expected %d, available %d", length, reader.Len())
	}

	data := make([]byte, length)
	// Use io.ReadFull to ensure all 'length' bytes are read, or an error is returned.
	if _, err := io.ReadFull(reader, data); err != nil {
		return "", fmt.Errorf("reading short string data (expected %d bytes): %w", length, err)
	}
	return string(data), nil
}

func readLongString(reader *bytes.Reader) (string, error) {
	var length uint32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return "", fmt.Errorf("reading long string length: %w", err)
	}

	if length == 0 {
		return "", nil // Successfully read an empty string
	}

	// Check if enough bytes are available.
	if int(length) > reader.Len() {
		return "", fmt.Errorf("not enough data for long string: expected %d, available %d", length, reader.Len())
	}

	data := make([]byte, length)
	// Use io.ReadFull to ensure all 'length' bytes are read.
	if _, err := io.ReadFull(reader, data); err != nil {
		return "", fmt.Errorf("reading long string data (expected %d bytes): %w", length, err)
	}
	return string(data), nil
}

func writeShortString(writer *bytes.Buffer, s string) {
	writer.WriteByte(uint8(len(s)))
	writer.WriteString(s)
}

func readFieldValue(reader *bytes.Reader, valueType byte) (interface{}, error) {
	switch valueType {
	case 't': // boolean
		b, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("reading boolean value: %w", err)
		}
		return b != 0, nil
	case 'b': // octet / byte (signed int8)
		var val int8
		err := binary.Read(reader, binary.BigEndian, &val)
		if err != nil {
			return nil, fmt.Errorf("reading int8 value: %w", err)
		}
		return val, nil
	case 's': // short / short-int (signed int16)
		var val int16
		err := binary.Read(reader, binary.BigEndian, &val)
		if err != nil {
			return nil, fmt.Errorf("reading int16 value: %w", err)
		}
		return val, nil
	case 'I': // long / long-int (signed int32)
		var val int32
		err := binary.Read(reader, binary.BigEndian, &val)
		if err != nil {
			return nil, fmt.Errorf("reading int32 value: %w", err)
		}
		return val, nil
	case 'l': // long-long / long-long-int (signed int64)
		var val int64
		err := binary.Read(reader, binary.BigEndian, &val)
		if err != nil {
			return nil, fmt.Errorf("reading int64 value: %w", err)
		}
		return val, nil
	case 'f': // float (single-precision float32)
		var val float32
		err := binary.Read(reader, binary.BigEndian, &val)
		if err != nil {
			return nil, fmt.Errorf("reading float32 value: %w", err)
		}
		return val, nil
	case 'd': // double (double-precision float64)
		var val float64
		err := binary.Read(reader, binary.BigEndian, &val)
		if err != nil {
			return nil, fmt.Errorf("reading float64 value: %w", err)
		}
		return val, nil
	case 'D': // decimal-value
		var scale uint8
		if err := binary.Read(reader, binary.BigEndian, &scale); err != nil {
			return nil, fmt.Errorf("reading decimal scale: %w", err)
		}
		var val int32
		if err := binary.Read(reader, binary.BigEndian, &val); err != nil {
			return nil, fmt.Errorf("reading decimal value: %w", err)
		}
		return AmqpDecimal{Scale: scale, Value: val}, nil // AmqpDecimal must be defined or imported
	case 'S': // long-string
		strVal, err := readLongString(reader)
		if err != nil {
			return nil, fmt.Errorf("reading long string field value: %w", err)
		}
		return strVal, nil
	case 'A': // field-array
		var arrayPayloadLength uint32
		if err := binary.Read(reader, binary.BigEndian, &arrayPayloadLength); err != nil {
			return nil, fmt.Errorf("reading field array payload length: %w", err)
		}
		if arrayPayloadLength == 0 {
			return []interface{}{}, nil
		}

		// Check if enough bytes are available for the array payload.
		if int(arrayPayloadLength) > reader.Len() {
			return nil, fmt.Errorf("not enough data for field array payload: expected %d, available %d", arrayPayloadLength, reader.Len())
		}

		arrayPayloadBytes := make([]byte, arrayPayloadLength)
		if _, err := io.ReadFull(reader, arrayPayloadBytes); err != nil { // Use io.ReadFull
			return nil, fmt.Errorf("reading field array payload bytes (expected %d): %w", arrayPayloadLength, err)
		}

		arrayDataReader := bytes.NewReader(arrayPayloadBytes)
		arr := make([]interface{}, 0)

		for arrayDataReader.Len() > 0 {
			valueTypeInArray, err := arrayDataReader.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("reading type in field array: %w", err)
			}
			val, errVal := readFieldValue(arrayDataReader, valueTypeInArray) // Recursive call
			if errVal != nil {
				return nil, fmt.Errorf("reading value in field array (type %c): %w", valueTypeInArray, errVal)
			}
			arr = append(arr, val)
		}
		// This check might be redundant if arrayPayloadLength was honored correctly.
		// if arrayDataReader.Len() > 0 {
		// 	return arr, fmt.Errorf("field array parsing finished with %d unconsumed bytes in array payload", arrayDataReader.Len())
		// }
		return arr, nil
	case 'T': // timestamp (uint64 - seconds since Epoch)
		var val uint64
		err := binary.Read(reader, binary.BigEndian, &val)
		if err != nil {
			return nil, fmt.Errorf("reading uint64 timestamp value: %w", err)
		}
		return val, nil
	case 'F': // nested field-table
		nestedTable, err := readTable(reader) // Recursive call, readTable itself needs to be in scope
		if err != nil {
			return nil, fmt.Errorf("reading nested field table: %w", err)
		}
		return nestedTable, nil
	case 'x': // byte-array
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return nil, fmt.Errorf("reading byte array length: %w", err)
		}
		if length == 0 {
			return []byte{}, nil
		}

		if int(length) > reader.Len() {
			return nil, fmt.Errorf("not enough data for byte array: expected %d, available %d", length, reader.Len())
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil { // Use io.ReadFull
			return nil, fmt.Errorf("reading byte array data (expected %d): %w", length, err)
		}
		return data, nil
	case 'V': // void
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported field table value type: %c (%d)", valueType, valueType)
	}
}

// writeFieldValue writes a single AMQP field value to the writer, including its type indicator.
func writeFieldValue(writer *bytes.Buffer, value interface{}) error {
	switch v := value.(type) {
	case bool:
		writer.WriteByte('t')
		if v {
			writer.WriteByte(1)
		} else {
			writer.WriteByte(0)
		}
	case int8:
		writer.WriteByte('b')
		binary.Write(writer, binary.BigEndian, v)
	case uint8:
		writer.WriteByte('b')
		binary.Write(writer, binary.BigEndian, v)
	case int16:
		writer.WriteByte('s')
		binary.Write(writer, binary.BigEndian, v)
	case int32:
		writer.WriteByte('I')
		binary.Write(writer, binary.BigEndian, v)
	case int64:
		writer.WriteByte('l')
		binary.Write(writer, binary.BigEndian, v)
	case uint64: // Assuming AMQP Timestamp 'T'
		writer.WriteByte('T')
		binary.Write(writer, binary.BigEndian, v)
	case float32:
		writer.WriteByte('f')
		binary.Write(writer, binary.BigEndian, v)
	case float64:
		writer.WriteByte('d')
		binary.Write(writer, binary.BigEndian, v)
	case AmqpDecimal:
		writer.WriteByte('D')
		binary.Write(writer, binary.BigEndian, v.Scale)
		binary.Write(writer, binary.BigEndian, v.Value)
	case string:
		writer.WriteByte('S')
		strBytes := []byte(v)
		binary.Write(writer, binary.BigEndian, uint32(len(strBytes)))
		writer.Write(strBytes)
	case []byte: // AMQP 'x' - byte-array
		writer.WriteByte('x')
		binary.Write(writer, binary.BigEndian, uint32(len(v)))
		writer.Write(v)
	case []interface{}: // AMQP 'A' - field-array
		writer.WriteByte('A')
		arrayPayloadBuffer := &bytes.Buffer{}
		for _, item := range v {
			if err := writeFieldValue(arrayPayloadBuffer, item); err != nil { // Recursive call
				return fmt.Errorf("writing item of type %T in field array: %w", item, err)
			}
		}
		binary.Write(writer, binary.BigEndian, uint32(arrayPayloadBuffer.Len()))
		writer.Write(arrayPayloadBuffer.Bytes())
	case map[string]interface{}: // AMQP 'F' - nested field-table
		writer.WriteByte('F')
		// writeTable will now return an error
		if err := writeTable(writer, v); err != nil { // Recursive call
			return fmt.Errorf("writing nested field table: %w", err)
		}
	case nil: // AMQP 'V' - void
		writer.WriteByte('V')
	default:
		return fmt.Errorf("unsupported type for field table serialization: %T", v)
	}
	return nil
}

// topicMatch checks if a topic pattern matches a routing key
// Supports AMQP wildcards: * (exactly one word) and # (zero or more words)
func topicMatch(pattern string, routingKey string) bool {
	// Handle empty pattern case - only matches empty routing key
	if pattern == "" {
		return routingKey == ""
	}

	// Handle single # pattern - matches everything
	if pattern == "#" {
		return true
	}

	// Split into parts
	patternParts := strings.Split(pattern, ".")
	routingParts := strings.Split(routingKey, ".")

	// Handle empty routing key after split
	// strings.Split("", ".") returns [""], but we want []
	if routingKey == "" {
		routingParts = []string{}
	}

	return matchParts(patternParts, routingParts)
}

// matchParts performs iterative matching with backtracking for #
func matchParts(patternParts, routingParts []string) bool {
	// Stack for backtracking: stores (patternIndex, routingIndex)
	type state struct {
		pi, ri int
	}
	stack := []state{{0, 0}}

	for len(stack) > 0 {
		// Pop from stack
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		pi, ri := current.pi, current.ri

		// Success condition: consumed all parts
		if pi >= len(patternParts) && ri >= len(routingParts) {
			return true
		}

		// If pattern is exhausted but routing key isn't
		if pi >= len(patternParts) {
			continue // This path didn't match
		}

		// If routing key is exhausted but pattern isn't
		if ri >= len(routingParts) {
			// Check if remaining pattern parts are all #
			allHash := true
			for i := pi; i < len(patternParts); i++ {
				if patternParts[i] != "#" {
					allHash = false
					break
				}
			}
			if allHash {
				return true
			}
			continue
		}

		pattern := patternParts[pi]

		switch pattern {
		case "#":
			// Try matching # with different number of words (0 to remaining)
			// Add states in reverse order so we try matching MORE words first
			// This is just a search strategy - either order works for correctness
			for i := len(routingParts); i >= ri; i-- {
				stack = append(stack, state{pi + 1, i})
			}

		case "*":
			// * matches exactly one word (even if empty)
			// Following common AMQP implementations where * can match empty segments
			stack = append(stack, state{pi + 1, ri + 1})

		default:
			// Literal match required (including empty strings)
			if pattern == routingParts[ri] {
				stack = append(stack, state{pi + 1, ri + 1})
			}
		}
	}

	return false
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
		key, err := readShortString(tableReader)
		if err != nil {
			// This error means reading the key string itself failed (e.g., length byte read, but data was short).
			return table, fmt.Errorf("malformed table: error reading field key: %w", err)
		}

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

func FindMessageInQueueNonLocking(msgIdentifier string, queue *Queue) (int, bool) {
	for i, msg := range queue.Messages {
		if GetMessageIdentifier(&msg) == msgIdentifier {
			return i, true
		}
	}
	return -1, false
}

// GetMessageIdentifier creates a unique hash for a message based on its key properties
// Note: For production code, consider adding "crypto/sha256" to your imports
func GetMessageIdentifier(msg *Message) string {
	if msg == nil {
		return ""
	}

	// Create a buffer to concatenate all fields
	var buffer bytes.Buffer

	// Add message ID (or empty string if not set)
	buffer.WriteString(msg.Properties.MessageId)

	// Add timestamp as bytes
	binary.Write(&buffer, binary.BigEndian, msg.Properties.Timestamp)

	// Add routing key
	buffer.WriteString(msg.RoutingKey)

	// Add message body
	buffer.Write(msg.Body)

	// Create a simple hash using built-in hash/fnv package
	h := fnv.New64a()
	h.Write(buffer.Bytes())

	// Return hex representation of the hash
	return fmt.Sprintf("%x", h.Sum64())
}
