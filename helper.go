package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
		return AmqpDecimal{Scale: scale, Value: val}, nil
	case 'S': // long-string
		// Assuming readLongString does not return an error itself but returns "" on failure.
		// For robust error handling, readLongString should ideally also return an error.
		// If readLongString could fail and corrupt the reader's state, that's a deeper issue.
		strVal := readLongString(reader)
		// If readLongString's contract is that it might return "" for an actual empty string
		// or "" on error, we can't distinguish here without it returning an error.
		// We'll assume it consumes correctly or the caller of readTable handles this.
		return strVal, nil
	case 'A': // field-array
		var arrayPayloadLength uint32
		if err := binary.Read(reader, binary.BigEndian, &arrayPayloadLength); err != nil {
			return nil, fmt.Errorf("reading field array payload length: %w", err)
		}
		if arrayPayloadLength == 0 {
			return []interface{}{}, nil
		}

		arrayPayloadBytes := make([]byte, arrayPayloadLength)
		n, err := io.ReadFull(reader, arrayPayloadBytes)
		if err != nil {
			return nil, fmt.Errorf("reading field array payload bytes (expected %d): %w", arrayPayloadLength, err)
		}
		if n != int(arrayPayloadLength) { // Should be caught by io.ReadFull returning io.ErrUnexpectedEOF
			return nil, fmt.Errorf("short read for field array payload: got %d, expected %d", n, arrayPayloadLength)
		}

		arrayDataReader := bytes.NewReader(arrayPayloadBytes)
		arr := make([]interface{}, 0)

		for arrayDataReader.Len() > 0 {
			valueTypeInArray, err := arrayDataReader.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("reading type in field array: %w", err)
			}
			val, err := readFieldValue(arrayDataReader, valueTypeInArray) // Recursive call
			if err != nil {
				return nil, fmt.Errorf("reading value in field array (type %c): %w", valueTypeInArray, err)
			}
			arr = append(arr, val)
		}
		if arrayDataReader.Len() > 0 { // Should not happen if parsing is correct
			return arr, fmt.Errorf("field array parsing finished with %d unconsumed bytes in array payload", arrayDataReader.Len())
		}
		return arr, nil
	case 'T': // timestamp (uint64 - seconds since Epoch)
		var val uint64
		err := binary.Read(reader, binary.BigEndian, &val)
		if err != nil {
			return nil, fmt.Errorf("reading uint64 timestamp value: %w", err)
		}
		return val, nil
	case 'F': // nested field-table
		// readTable will now return (map[string]interface{}, error)
		nestedTable, err := readTable(reader) // Recursive call
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
		data := make([]byte, length)
		n, err := io.ReadFull(reader, data)
		if err != nil {
			return nil, fmt.Errorf("reading byte array data (expected %d): %w", length, err)
		}
		if n != int(length) { // Should be caught by io.ReadFull
			return nil, fmt.Errorf("short read for byte array data: got %d, expected %d", n, length)
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

func topicMatchHelper(patternParts []string, routingParts []string) bool {
	if len(patternParts) == 0 {
		return len(routingParts) == 0
	}

	i, j := 0, 0
	for i < len(patternParts) && j < len(routingParts) {
		if patternParts[i] == "#" {
			if i == len(patternParts)-1 {
				return true
			}
			for k := j; k <= len(routingParts); k++ {
				if topicMatchHelper(patternParts[i+1:], routingParts[k:]) {
					return true
				}
			}
			return false
		} else if patternParts[i] == "*" {
			i++
			j++
		} else if patternParts[i] == routingParts[j] {
			i++
			j++
		} else {
			return false
		}
	}

	return (i == len(patternParts) && j == len(routingParts)) ||
		(i == len(patternParts)-1 && patternParts[i] == "#" && j == len(routingParts))
}
