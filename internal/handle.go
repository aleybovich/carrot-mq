package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	amqpError "github.com/aleybovich/carrot-mq/amqperror"
	"github.com/aleybovich/carrot-mq/config"
)

func (c *connection) handleMethodConnectionStartOk(reader *bytes.Reader) error {
	c.server.Info("Processing connection.start-ok")
	// Arguments: client-properties (table), mechanism (shortstr), response (longstr), locale (shortstr)
	var clientProperties map[string]interface{}
	clientProperties, err := readTable(reader) // Use error-returning readTable
	if err != nil {
		c.server.Err("Error reading client_properties in connection.start-ok: %v", err)
		// AMQP code 502 (SYNTAX_ERROR) for malformed table
		return c.sendConnectionClose(amqpError.SyntaxError.Code(), "malformed client_properties table", uint16(ClassConnection), MethodConnectionStartOk)
	}
	_ = clientProperties // Use clientProperties if needed, e.g., logging or capabilities check

	mechanism, err := readShortString(reader) // Auth mechanism
	if err != nil {
		c.server.Err("Error reading mechanism in connection.start-ok: %v", err)
		return c.sendConnectionClose(amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed mechanism string", uint16(ClassConnection), MethodConnectionStartOk)
	}

	response, err := readLongString(reader) // Auth credentials
	if err != nil {
		c.server.Err("Error reading response in connection.start-ok: %v", err)
		return c.sendConnectionClose(amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed response string", uint16(ClassConnection), MethodConnectionStartOk)
	}

	_, err = readShortString(reader) // locale
	if err != nil {
		c.server.Err("Error reading locale in connection.start-ok: %v", err)
		return c.sendConnectionClose(amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed locale string", uint16(ClassConnection), MethodConnectionStartOk)
	}

	// Check for remaining bytes, indicates malformed arguments
	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of connection.start-ok payload.")
	}

	// Handle authentication based on server auth mode
	if c.server.authMode == config.AuthModePlain {
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
}

func (c *connection) handleMethodConnectionTuneOk(reader *bytes.Reader) error {
	c.server.Info("Processing connection.tune-ok")
	if err := binary.Read(reader, binary.BigEndian, &c.channelMax); err != nil {
		return c.sendConnectionClose(amqpError.SyntaxError.Code(), "malformed connection.tune-ok (channel-max)", uint16(ClassConnection), MethodConnectionTuneOk)
	}
	if err := binary.Read(reader, binary.BigEndian, &c.frameMax); err != nil {
		return c.sendConnectionClose(amqpError.SyntaxError.Code(), "malformed connection.tune-ok (frame-max)", uint16(ClassConnection), MethodConnectionTuneOk)
	}
	if err := binary.Read(reader, binary.BigEndian, &c.heartbeatInterval); err != nil {
		return c.sendConnectionClose(amqpError.SyntaxError.Code(), "malformed connection.tune-ok (heartbeat)", uint16(ClassConnection), MethodConnectionTuneOk)
	}
	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of connection.tune-ok payload.")
	}

	c.server.Info("Connection parameters negotiated: channelMax=%d, frameMax=%d, heartbeat=%d",
		c.channelMax, c.frameMax, c.heartbeatInterval)
	return nil
}

func (c *connection) handleMethodConnectionOpen(reader *bytes.Reader) error {
	vhostName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading vhost in connection.open: %v", err)
		return c.sendConnectionClose(amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed vhost string", uint16(ClassConnection), MethodConnectionOpen)
	}

	_, _ = readShortString(reader) // capabilities

	_, errReadByte := reader.ReadByte() // insist bit
	if errReadByte != nil {
		return c.sendConnectionClose(amqpError.SyntaxError.Code(), "malformed connection.open (insist bit)", uint16(ClassConnection), MethodConnectionOpen)
	}
	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of connection.open payload.")
	}
	c.server.Info("Processing connection.open for vhost: '%s'", vhostName)

	// Validate vhost exists
	if vhost, err := c.server.GetVHost(vhostName); err != nil {
		c.server.Err("Connection.Open: vhost '%s' does not exist", vhostName)
		return c.sendConnectionClose(amqpError.AccessRefused.Code(), fmt.Sprintf("ACCESS_REFUSED - vhost '%s' does not exist", vhostName), uint16(ClassConnection), MethodConnectionOpen)
	} else {
		// Store vhost on connection
		c.vhost = vhost
	}

	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassConnection))
	binary.Write(payload, binary.BigEndian, uint16(MethodConnectionOpenOk))
	// AMQP 0-9-1 Connection.OpenOk has one field: known-hosts (shortstr), which "MUST be zero length".
	writeShortString(payload, "") // known-hosts

	err = c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: 0,
		Payload: payload.Bytes(),
	})
	if err != nil {
		c.server.Err("Error sending connection.open-ok: %v", err)
		return fmt.Errorf("sending connection.open-ok frame: %w", err)
	}
	c.server.Info("Sent connection.open-ok")
	return nil
}

func (c *connection) handleMethodConnectionClose(reader *bytes.Reader) error {
	c.server.Info("Processing connection.close (client initiated close)")
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
	if errWrite := c.writeFrame(&frame{Type: FrameMethod, Channel: 0, Payload: payloadCloseOk.Bytes()}); errWrite != nil {
		c.server.Err("Error sending connection.close-ok in response to client's close: %v", errWrite)
	} else {
		c.server.Info("Sent connection.close-ok")
	}
	c.conn.Close()
	return errConnectionClosedGracefully
}

func (c *connection) handleMethodConnectionCloseOk(reader *bytes.Reader) error {
	c.server.Info("Received connection.close-ok. Client acknowledged connection closure.")
	if reader.Len() > 0 {
		c.server.Warn("Extra data in client's connection.close-ok method.")
	}
	c.conn.Close()
	return errConnectionClosedGracefully
}

// Updated handleConnectionMethod to use the new handler functions:
func (c *connection) handleClassConnectionMethod(methodId uint16, reader *bytes.Reader) error {

	switch methodId {
	case MethodConnectionStartOk:
		return c.handleMethodConnectionStartOk(reader)

	case MethodConnectionTuneOk:
		return c.handleMethodConnectionTuneOk(reader)

	case MethodConnectionOpen:
		return c.handleMethodConnectionOpen(reader)

	case MethodConnectionClose:
		return c.handleMethodConnectionClose(reader)

	case MethodConnectionCloseOk:
		return c.handleMethodConnectionCloseOk(reader)

	default:
		errMsg := fmt.Sprintf("unhandled connection method ID: %d", methodId)
		c.server.Err(errMsg)
		// According to AMQP, for an unknown method on channel 0 (connection),
		// the server should respond with Connection.Close(amqpError.CommandInvalid.Code(), "command invalid", classId, methodId).
		return c.sendConnectionClose(amqpError.CommandInvalid.Code(), errMsg, uint16(ClassConnection), methodId)
	}
}

// ---- Channel Method Handlers ------

func (c *connection) handleMethodChannelOpen(reader *bytes.Reader, channelId uint16) error {
	c.server.Info("Processing channel.open for channel %d", channelId)
	// AMQP 0-9-1: reserved-1 (shortstr), "out-of-band, MUST be zero length string"
	// Read and discard "out-of-band" argument.
	_, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading out-of-band string for channel.open on channel %d: %v", channelId, err)
		return c.sendConnectionClose(amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed out-of-band string in channel.open", uint16(ClassChannel), MethodChannelOpen)
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
		return c.sendChannelClose(channelId, amqpError.ChannelError.Code(), "CHANNEL_ERROR - channel ID already in use", uint16(ClassChannel), MethodChannelOpen)
	}

	newCh := &channel{
		id:               channelId,
		conn:             c,
		consumers:        make(map[string]string),
		pendingMessages:  make([]message, 0),
		unackedMessages:  make(map[uint64]*unackedMessage),
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
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR", 0, 0)
	}
	if err := binary.Write(payloadOpenOk, binary.BigEndian, uint16(MethodChannelOpenOk)); err != nil {
		c.server.Err("Internal error serializing MethodChannelOpenOk for Channel.OpenOk: %v", err)
		c.forceRemoveChannel(channelId, "internal error sending channel.open-ok")
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR", 0, 0)
	}
	// AMQP 0-9-1 Channel.OpenOk has one field: channel-id (longstr), which is reserved and "MUST be empty".
	// An empty longstr is represented by its length (uint32) being 0.
	if err := binary.Write(payloadOpenOk, binary.BigEndian, uint32(0)); err != nil {
		c.server.Err("Internal error serializing empty longstr for Channel.OpenOk: %v", err)
		c.forceRemoveChannel(channelId, "internal error sending channel.open-ok")
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR", 0, 0)
	}

	if err := c.writeFrame(&frame{Type: FrameMethod, Channel: channelId, Payload: payloadOpenOk.Bytes()}); err != nil {
		c.server.Err("Error sending channel.open-ok for channel %d: %v", channelId, err)
		c.forceRemoveChannel(channelId, "failed to send channel.open-ok")
		return err
	}
	c.server.Info("Sent channel.open-ok for channel %d", channelId)
	return nil
}

func (c *connection) handleMethodChannelClose(reader *bytes.Reader, channelId uint16, ch *channel) error {
	c.server.Info("Processing channel.close for channel %d (client initiated)", channelId)
	var replyCode uint16
	if err := binary.Read(reader, binary.BigEndian, &replyCode); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed channel.close (reply-code)", uint16(ClassChannel), MethodChannelClose)
	}
	replyText, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading replyText for channel.close on channel %d: %v", channelId, err)
	}

	var classIdField, methodIdField uint16
	if err := binary.Read(reader, binary.BigEndian, &classIdField); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed channel.close (class-id)", uint16(ClassChannel), MethodChannelClose)
	}
	if err := binary.Read(reader, binary.BigEndian, &methodIdField); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed channel.close (method-id)", uint16(ClassChannel), MethodChannelClose)
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
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR", 0, 0)
	}
	if err := binary.Write(payloadCloseOk, binary.BigEndian, uint16(MethodChannelCloseOk)); err != nil {
		c.server.Err("Internal error serializing MethodChannelCloseOk: %v", err)
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR", 0, 0)
	}

	if errWrite := c.writeFrame(&frame{Type: FrameMethod, Channel: channelId, Payload: payloadCloseOk.Bytes()}); errWrite != nil {
		c.server.Err("Error sending channel.close-ok for client-initiated close on channel %d: %v", channelId, errWrite)
		return errWrite
	}
	c.server.Info("Sent channel.close-ok for channel %d (client initiated)", channelId)
	return nil
}

func (c *connection) handleMethodChannelCloseOk(reader *bytes.Reader, channelId uint16, ch *channel) error {
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
}

func (c *connection) handleClassChannelMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassChannel, methodId)

	if methodId == MethodChannelOpen {
		return c.handleMethodChannelOpen(reader, channelId)
	}

	// For all other methods, use getChannel
	ch, exists, isClosing := c.getChannel(channelId)
	if !exists {
		errMsg := fmt.Sprintf("received method %s for non-existent channel %d", methodName, channelId)
		c.server.Err(errMsg)
		return c.sendConnectionClose(amqpError.ChannelError.Code(), "CHANNEL_ERROR - "+errMsg, uint16(ClassChannel), methodId)
	}

	// Check if channel is closing
	if isClosing && methodId != MethodChannelCloseOk {
		c.server.Warn("Received method %s on channel %d that is already being closed by server. Ignoring.", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodChannelClose:
		return c.handleMethodChannelClose(reader, channelId, ch)

	case MethodChannelCloseOk:
		return c.handleMethodChannelCloseOk(reader, channelId, ch)

	default:
		replyText := fmt.Sprintf("channel method %s (id %d) not implemented or invalid in this state", methodName, methodId)
		c.server.Err("Unhandled channel method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		return c.sendChannelClose(channelId, amqpError.NotImplemented.Code(), replyText, uint16(ClassChannel), methodId)
	}
}

// ----- Exchange Method Handlers -----

func (c *connection) handleMethodExchangeDeclare(reader *bytes.Reader, channelId uint16) error {
	// AMQP 0-9-1: ticket (short, reserved), exchange (shortstr), type (shortstr), passive (bit),
	// durable (bit), auto-delete (bit), internal (bit), no-wait (bit), arguments (table)
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return fmt.Errorf("reading ticket for exchange.declare: %w", err)
	}
	exchangeName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading exchangeName for exchange.declare: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed exchange.declare (exchangeName)", uint16(ClassExchange), MethodExchangeDeclare)
	}

	exchangeType, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading exchangeType for exchange.declare: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed exchange.declare (exchangeType)", uint16(ClassExchange), MethodExchangeDeclare)
	}

	bits, errReadByte := reader.ReadByte()
	if errReadByte != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "malformed exchange.declare (bits)", uint16(ClassExchange), MethodExchangeDeclare)
	}

	args, errArgs := readTable(reader)
	if errArgs != nil {
		c.server.Err("Error reading arguments for exchange.declare ('%s'): %v", exchangeName, errArgs)
		// AMQP code 502 (SYNTAX_ERROR)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "malformed arguments table for exchange.declare", uint16(ClassExchange), MethodExchangeDeclare)
	}
	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of exchange.declare payload.")
	}

	passive := (bits & 0x01) != 0
	durable := (bits & 0x02) != 0
	autoDelete := (bits & 0x04) != 0 // Server doesn't fully implement auto-delete logic for now
	internal := (bits & 0x08) != 0   // Server doesn't fully implement 'internal' exchange logic for now
	noWait := (bits & 0x10) != 0

	c.server.Info("Processing exchange.declare: name='%s', type='%s', passive=%v, durable=%v, autoDelete=%v, internal=%v, noWait=%v, args=%v on channel %d",
		exchangeName, exchangeType, passive, durable, autoDelete, internal, noWait, args, channelId)

	// Validate exchange type - essential for server operation. Headers exchange is not implemented for now
	validTypes := map[string]bool{"direct": true, "fanout": true, "topic": true, "headers": false}
	if _, isValidType := validTypes[exchangeType]; !isValidType {
		replyText := fmt.Sprintf("exchange type '%s' not implemented", exchangeType)
		c.server.Warn("Exchange.Declare: %s for exchange '%s'. Sending Channel.Close.", replyText, exchangeName)
		// AMQP code 540 (NOT_IMPLEMENTED)
		return c.sendChannelClose(channelId, amqpError.NotImplemented.Code(), replyText, uint16(ClassExchange), MethodExchangeDeclare)
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
			// This is an expected outcome for a client checking existence passively.
			return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassExchange), MethodExchangeDeclare)
		}
		// Passive and exists: Check compatibility (primarily type according to spec for passive).
		// AMQP spec: "If the exchange already exists and is of the same type... then it does nothing and replies with DeclareOk."
		// "If the exchange exists but is of a different type, the server closes the channel with a 406 (PRECONDITION_FAILED) status."
		if ex.Type != exchangeType {
			vhost.mu.Unlock()
			replyText := fmt.Sprintf("exchange '%s' of type '%s' already declared, cannot passively declare with type '%s'", ex.Name, ex.Type, exchangeType)
			c.server.Warn("Passive Exchange.Declare: Type mismatch for '%s'. Sending Channel.Close.", exchangeName)
			// AMQP code 406 (PRECONDITION_FAILED)
			return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(), replyText, uint16(ClassExchange), MethodExchangeDeclare)
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
				return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(), replyText, uint16(ClassExchange), MethodExchangeDeclare)
			}
			c.server.Info("Exchange '%s' re-declared with matching properties.", exchangeName)
		} else { // Not passive and not exists: create it.

			newExchange := &exchange{
				Name:       exchangeName,
				Type:       exchangeType,
				Durable:    durable,
				AutoDelete: autoDelete,
				Internal:   internal,
				Bindings:   make(map[string][]string),
			}

			// PERSISTENCE: Save durable exchange after successful creation
			if c.server.persistenceManager != nil && durable {
				vhost.mu.Unlock() // Unlock before persistence operation

				record := ExchangeToRecord(newExchange)
				if err := c.server.persistenceManager.SaveExchange(vhost.name, record); err != nil {
					c.server.Err("Failed to persist exchange %s: %v", exchangeName, err)
				}

				vhost.mu.Lock() // Re-lock for consistency
			}

			vhost.exchanges[exchangeName] = newExchange

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
		writeErr := c.writeFrame(&frame{
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
}

func (c *connection) handleMethodExchangeDelete(reader *bytes.Reader, channelId uint16) error {
	// AMQP 0-9-1: ticket (short), exchange (shortstr), if-unused (bit), no-wait (bit)
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed exchange.delete (ticket)", uint16(ClassExchange), MethodExchangeDelete)
	}

	exchangeName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading exchangeName for exchange.delete: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed exchange.delete (exchangeName)", uint16(ClassExchange), MethodExchangeDelete)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed exchange.delete (bits)", uint16(ClassExchange), MethodExchangeDelete)
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
		return c.sendChannelClose(channelId, amqpError.AccessRefused.Code(), replyText, uint16(ClassExchange), MethodExchangeDelete)
	}

	vhost := c.vhost

	// Hold read lock to safely check and mark for deletion
	vhost.mu.RLock()

	if vhost.IsDeleting() {
		vhost.mu.RUnlock()
		c.server.Info("Exchange.Delete: VHost is being deleted")
		return c.sendConnectionClose(amqpError.ConnectionForced.Code(), "VHost deleted", uint16(ClassExchange), MethodExchangeDelete)
	}

	exchange, exists := vhost.exchanges[exchangeName]
	if !exists {
		vhost.mu.RUnlock()
		replyText := fmt.Sprintf("NOT_FOUND - no exchange '%s' in vhost '/'", exchangeName)
		c.server.Warn("Exchange.Delete: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassExchange), MethodExchangeDelete)
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
			return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(), replyText, uint16(ClassExchange), MethodExchangeDelete)
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

	// PERSISTENCE: Delete exchange after successful memory deletion
	if c.server.persistenceManager != nil {
		if err := c.server.persistenceManager.DeleteExchange(vhost.name, exchangeName); err != nil {
			c.server.Err("Failed to delete exchange %s from persistence: %v", exchangeName, err)
		}
	}

	// Send Exchange.DeleteOk if not no-wait
	if !noWait {
		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassExchange))
		binary.Write(payload, binary.BigEndian, uint16(MethodExchangeDeleteOk))

		if err := c.writeFrame(&frame{
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

func (c *connection) handleClassExchangeMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassExchange, methodId)

	_, channelExists, isClosing := c.getChannel(channelId)

	if !channelExists && channelId != 0 { // Channel 0 is special and doesn't need this check here
		// This is a critical error: operation on a non-existent channel.
		// AMQP spec is somewhat open, could be Channel.Close or Connection.Close.
		// Given the channel context is invalid, a Connection.Close is safer.
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for exchange operation", channelId)
		c.server.Err("Exchange method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		// AMQP code 503 (COMMAND_INVALID) or 504 (CHANNEL_ERROR)
		return c.sendConnectionClose(amqpError.CommandInvalid.Code(), replyText, uint16(ClassExchange), methodId)
	}

	if isClosing {
		c.server.Debug("Ignoring exchange method %s on channel %d that is being closed", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodExchangeDeclare:
		return c.handleMethodExchangeDeclare(reader, channelId)

	case MethodExchangeDelete:
		return c.handleMethodExchangeDelete(reader, channelId)

	default:
		replyText := fmt.Sprintf("unknown or not implemented exchange method id %d", methodId)
		c.server.Err("Unhandled exchange method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		// AMQP code 540 (NOT_IMPLEMENTED) or 503 (COMMAND_INVALID)
		return c.sendChannelClose(channelId, amqpError.NotImplemented.Code(), replyText, uint16(ClassExchange), methodId)
	}
}

// ----- Queue Method Handlers -----

func (c *connection) handleMethodQueueDeclare(reader *bytes.Reader, channelId uint16) error {
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		// Malformed frame if basic fields cannot be read.
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.declare (ticket)", uint16(ClassQueue), MethodQueueDeclare)
	}
	queueNameIn, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueNameIn for queue.declare: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.declare (queueNameIn)", uint16(ClassQueue), MethodQueueDeclare)
	}

	bits, errReadByte := reader.ReadByte()
	if errReadByte != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.declare (bits)", uint16(ClassQueue), MethodQueueDeclare)
	}

	args, errReadTable := readTable(reader) // readTable now returns (map, error)
	if errReadTable != nil {
		c.server.Err("Error reading arguments table for queue.declare (queue: '%s'): %v", queueNameIn, errReadTable)
		// AMQP code 502 (SYNTAX_ERROR) for malformed table.
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed arguments table for queue.declare", uint16(ClassQueue), MethodQueueDeclare)
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
			return c.sendChannelClose(channelId, amqpError.AccessRefused.Code(), errMsg, uint16(ClassQueue), MethodQueueDeclare)
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
			return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassQueue), MethodQueueDeclare)
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
			return c.sendChannelClose(channelId, amqpError.ResourceLocked.Code(), replyText, uint16(ClassQueue), MethodQueueDeclare)
		}

		// Check property compatibility for passive declaration
		// According to AMQP 0-9-1 spec: "If not set and the queue exists, the server MUST check that
		// the existing queue has the same values for durable, exclusive, auto-delete, and arguments fields"
		if q.Durable != durable || q.Exclusive != exclusive || q.AutoDelete != autoDelete {
			q.mu.RUnlock()
			vhost.mu.Unlock()
			replyText := fmt.Sprintf("PRECONDITION_FAILED - queue '%s' declared with different durable, exclusive or auto-delete values", actualQueueName)
			c.server.Warn("Passive Queue.Declare: %s. Existing(dur:%v, excl:%v, ad:%v), Req(dur:%v, excl:%v, ad:%v). Sending Channel.Close.",
				replyText, q.Durable, q.Exclusive, q.AutoDelete, durable, exclusive, autoDelete)
			return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(), replyText, uint16(ClassQueue), MethodQueueDeclare)
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
				return c.sendChannelClose(channelId, amqpError.ResourceLocked.Code(), replyText, uint16(ClassQueue), MethodQueueDeclare)
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
				return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(), replyText, uint16(ClassQueue), MethodQueueDeclare)
			}
			// If properties match, it's a valid re-declaration.
			c.server.Info("Queue '%s' re-declared with matching properties.", actualQueueName)
			messageCount = uint32(len(q.Messages))
			consumerCount = uint32(len(q.Consumers))
			q.mu.RUnlock()
		} else { // Not passive and not exists: create it.
			newQueue := &queue{
				Name:       actualQueueName,
				Messages:   []message{},
				Bindings:   make(map[string]bool),
				Consumers:  make(map[string]*consumer),
				Durable:    durable,
				Exclusive:  exclusive,
				AutoDelete: autoDelete,
			}
			vhost.queues[actualQueueName] = newQueue

			c.server.Info("Created new queue: '%s', durable=%v, exclusive=%v, autoDelete=%v",
				actualQueueName, durable, exclusive, autoDelete)

			// PERSISTENCE: Save durable queue after successful creation
			if c.server.persistenceManager != nil && durable {
				vhost.mu.Unlock() // Unlock before persistence

				record := QueueToRecord(newQueue)
				if err := c.server.persistenceManager.SaveQueue(vhost.name, record); err != nil {
					c.server.Err("Failed to persist queue %s: %v", actualQueueName, err)
				}

				vhost.mu.Lock() // Re-lock
			}

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

		if errWrite := c.writeFrame(&frame{Type: FrameMethod, Channel: channelId, Payload: payloadOk.Bytes()}); errWrite != nil {
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

func (c *connection) handleMethodQueueBind(reader *bytes.Reader, channelId uint16) error {
	// Parse all arguments first
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.bind (ticket)", uint16(ClassQueue), MethodQueueBind)
	}

	queueName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueName for queue.bind: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.bind (queueName)", uint16(ClassQueue), MethodQueueBind)
	}

	exchangeName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading exchangeName for queue.bind: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.bind (exchangeName)", uint16(ClassQueue), MethodQueueBind)
	}

	routingKey, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading routingKey for queue.bind: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.bind (routingKey)", uint16(ClassQueue), MethodQueueBind)
	}

	bits, errReadByte := reader.ReadByte()
	if errReadByte != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.bind (bits)", uint16(ClassQueue), MethodQueueBind)
	}

	argsBind, errReadTableBind := readTable(reader)
	if errReadTableBind != nil {
		c.server.Err("Error reading arguments table for queue.bind (queue: '%s', exchange: '%s'): %v", queueName, exchangeName, errReadTableBind)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed arguments table for queue.bind", uint16(ClassQueue), MethodQueueBind)
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
		return c.sendConnectionClose(amqpError.ConnectionForced.Code(), "VHost deleted", uint16(ClassQueue), MethodQueueBind)
	}

	ex, exExists := vhost.exchanges[exchangeName]
	q, qExists := vhost.queues[queueName]

	if !exExists {
		replyText := fmt.Sprintf("no exchange '%s' in vhost '/'", exchangeName)
		c.server.Warn("Queue.Bind failed: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassQueue), MethodQueueBind)
	}

	if !qExists {
		replyText := fmt.Sprintf("no queue '%s' in vhost '/'", queueName)
		c.server.Warn("Queue.Bind failed: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassQueue), MethodQueueBind)
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
	alreadyBound := slices.Contains(ex.Bindings[routingKey], queueName)

	if !alreadyBound {
		ex.Bindings[routingKey] = append(ex.Bindings[routingKey], queueName)
		q.Bindings[exchangeName+":"+routingKey] = true
		c.server.Info("Bound exchange '%s' (type: %s) to queue '%s' with routing key '%s'",
			exchangeName, ex.Type, queueName, routingKey)

		q.mu.Unlock()
		ex.mu.Unlock()

		// PERSISTENCE: Save binding if both exchange and queue are durable
		if c.server.persistenceManager != nil && ex.Durable && q.Durable {
			// Need to unlock before persistence operation

			bindingRecord := &BindingRecord{
				Exchange:   exchangeName,
				Queue:      queueName,
				RoutingKey: routingKey,
				Arguments:  argsBind,
				CreatedAt:  time.Now(),
			}

			if err := c.server.persistenceManager.SaveBinding(vhost.name, bindingRecord); err != nil {
				c.server.Err("Failed to persist binding %s:%s->%s: %v",
					exchangeName, routingKey, queueName, err)
			}
		}
	} else {
		q.mu.Unlock()
		ex.mu.Unlock()

		c.server.Info("Binding already exists for exchange '%s' to queue '%s' with routing key '%s'",
			exchangeName, queueName, routingKey)
	}

	if !noWait {
		payloadBindOk := &bytes.Buffer{}
		binary.Write(payloadBindOk, binary.BigEndian, uint16(ClassQueue))
		binary.Write(payloadBindOk, binary.BigEndian, uint16(MethodQueueBindOk))

		if errWrite := c.writeFrame(&frame{Type: FrameMethod, Channel: channelId, Payload: payloadBindOk.Bytes()}); errWrite != nil {
			c.server.Err("Error sending queue.bind-ok for queue '%s': %v", queueName, errWrite)
			return errWrite
		}
		c.server.Info("Sent queue.bind-ok for queue '%s'", queueName)
	}

	return nil
}

func (c *connection) handleMethodQueueDelete(reader *bytes.Reader, channelId uint16) error {
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.delete (ticket)", uint16(ClassQueue), MethodQueueDelete)
	}

	queueName, err := readShortString(reader)
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.delete (queueName)", uint16(ClassQueue), MethodQueueDelete)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.delete (bits)", uint16(ClassQueue), MethodQueueDelete)
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
		return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassQueue), MethodQueueDelete)
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
		return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(), replyText, uint16(ClassQueue), MethodQueueDelete)
	}

	if ifEmpty && messageCount > 0 {
		queue.mu.Unlock()
		queue.deleting.Store(false)
		replyText := fmt.Sprintf("queue '%s' not empty (has %d messages)", queueName, messageCount)
		return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(), replyText, uint16(ClassQueue), MethodQueueDelete)
	}

	// --- SNAPSHOT consumers and bindings BEFORE clearing ---
	queueBindings := make([]string, 0, len(queue.Bindings))
	for binding := range queue.Bindings {
		queueBindings = append(queueBindings, binding)
	}

	consumersSnapshot := make(map[string]*consumer, len(queue.Consumers))
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
	queue.Consumers = make(map[string]*consumer)
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

	// PERSISTENCE: Delete queue and all its messages
	if c.server.persistenceManager != nil {
		// Delete all messages in the queue
		if err := c.server.persistenceManager.DeleteAllQueueMessages(vhost.name, queueName); err != nil {
			c.server.Err("Failed to delete messages for queue %s from persistence: %v", queueName, err)
		}

		// Delete the queue itself
		if err := c.server.persistenceManager.DeleteQueue(vhost.name, queueName); err != nil {
			c.server.Err("Failed to delete queue %s from persistence: %v", queueName, err)
		}
	}

	if !noWait {
		payload := &bytes.Buffer{}
		binary.Write(payload, binary.BigEndian, uint16(ClassQueue))
		binary.Write(payload, binary.BigEndian, uint16(MethodQueueDeleteOk))
		binary.Write(payload, binary.BigEndian, deletedMessageCount)

		if err := c.writeFrame(&frame{
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

func (c *connection) handleMethodQueueUnbind(reader *bytes.Reader, channelId uint16) error {
	// Parse all arguments first
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.unbind (ticket)", uint16(ClassQueue), MethodQueueUnbind)
	}

	queueName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueName for queue.unbind: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.unbind (queueName)", uint16(ClassQueue), MethodQueueUnbind)
	}

	exchangeName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading exchangeName for queue.unbind: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.unbind (exchangeName)", uint16(ClassQueue), MethodQueueUnbind)
	}

	routingKey, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading routingKey for queue.unbind: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.unbind (routingKey)", uint16(ClassQueue), MethodQueueUnbind)
	}

	argsUnbind, errReadTableUnbind := readTable(reader)
	if errReadTableUnbind != nil {
		c.server.Err("Error reading arguments table for queue.unbind (queue: '%s', exchange: '%s'): %v", queueName, exchangeName, errReadTableUnbind)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed arguments table for queue.unbind", uint16(ClassQueue), MethodQueueUnbind)
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
		return c.sendConnectionClose(amqpError.ConnectionForced.Code(), "VHost deleted", uint16(ClassQueue), MethodQueueUnbind)
	}

	ex, exExists := vhost.exchanges[exchangeName]
	q, qExists := vhost.queues[queueName]

	if !exExists {
		replyText := fmt.Sprintf("no exchange '%s' in vhost '/'", exchangeName)
		c.server.Warn("Queue.Unbind failed: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassQueue), MethodQueueUnbind)
	}

	if !qExists {
		replyText := fmt.Sprintf("no queue '%s' in vhost '/'", queueName)
		c.server.Warn("Queue.Unbind failed: %s. Sending Channel.Close.", replyText)
		return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassQueue), MethodQueueUnbind)
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

		// PERSISTENCE: Delete binding
		if c.server.persistenceManager != nil {
			if err := c.server.persistenceManager.DeleteBinding(vhost.name, exchangeName, queueName, routingKey); err != nil {
				c.server.Err("Failed to delete binding from persistence: %v", err)
			}
		}
	} else {
		c.server.Info("No binding found to remove for queue '%s', exchange '%s', routing key '%s'", queueName, exchangeName, routingKey)
	}

	// Send Queue.UnbindOk
	payloadUnbindOk := &bytes.Buffer{}
	binary.Write(payloadUnbindOk, binary.BigEndian, uint16(ClassQueue))
	binary.Write(payloadUnbindOk, binary.BigEndian, uint16(MethodQueueUnbindOk))

	if errWrite := c.writeFrame(&frame{Type: FrameMethod, Channel: channelId, Payload: payloadUnbindOk.Bytes()}); errWrite != nil {
		c.server.Err("Error sending queue.unbind-ok for queue '%s': %v", queueName, errWrite)
		return errWrite
	}
	c.server.Info("Sent queue.unbind-ok for queue '%s'", queueName)

	return nil
}

func (c *connection) handleMethodQueuePurge(reader *bytes.Reader, channelId uint16) error {
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.purge (ticket)", uint16(ClassQueue), MethodQueuePurge)
	}

	queueName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueName for queue.purge: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.purge (queueName)", uint16(ClassQueue), MethodQueuePurge)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed queue.purge (bits)", uint16(ClassQueue), MethodQueuePurge)
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
		return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassQueue), MethodQueuePurge)
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
		return c.sendConnectionClose(amqpError.ConnectionForced.Code(), "VHost deleted", uint16(ClassQueue), MethodQueuePurge)
	}

	messageCount := uint32(len(queue.Messages))

	// Collect IDs of persistent messages before clearing
	var persistentMessageIds []string
	if c.server.persistenceManager != nil && queue.Durable {
		for _, msg := range queue.Messages {
			if msg.Properties.DeliveryMode == 2 { // Persistent
				persistentMessageIds = append(persistentMessageIds, GetMessageIdentifier(&msg))
			}
		}
	}

	// Clear all messages
	if messageCount > 0 {
		queue.Messages = []message{}
		c.server.Info("Purged %d messages from queue '%s'", messageCount, queueName)
	} else {
		c.server.Info("Queue '%s' was already empty (purge had no effect)", queueName)
	}

	queue.mu.Unlock()

	// PERSISTENCE: Delete purged messages
	if c.server.persistenceManager != nil && len(persistentMessageIds) > 0 {
		for _, messageId := range persistentMessageIds {
			if err := c.server.persistenceManager.DeleteMessage(vhost.name, queueName, messageId); err != nil {
				c.server.Err("Failed to delete purged message %s from persistence: %v", messageId, err)
			}
		}
	}

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

func (c *connection) handleClassQueueMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassQueue, methodId)

	_, channelExists, isClosing := c.getChannel(channelId)
	if !channelExists && channelId != 0 {
		// This is a connection-level error as the channel context is invalid for the operation.
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for queue operation", channelId)
		c.server.Err("Queue method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		// AMQP code 503 (COMMAND_INVALID) or 504 (CHANNEL_ERROR) - 503 is general for bad command sequence.
		return c.sendConnectionClose(amqpError.CommandInvalid.Code(), replyText, uint16(ClassQueue), methodId)
	}

	if isClosing {
		c.server.Debug("Ignoring queue method %s on channel %d that is being closed", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodQueueDeclare:
		return c.handleMethodQueueDeclare(reader, channelId)

	case MethodQueueBind:
		return c.handleMethodQueueBind(reader, channelId)

	case MethodQueueUnbind:
		return c.handleMethodQueueUnbind(reader, channelId)

	case MethodQueuePurge:
		return c.handleMethodQueuePurge(reader, channelId)

	case MethodQueueDelete:
		return c.handleMethodQueueDelete(reader, channelId)

	default:
		replyText := fmt.Sprintf("unknown or not implemented queue method id %d", methodId)
		c.server.Err("Unhandled queue method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		// AMQP code 540 (NOT_IMPLEMENTED) or 503 (COMMAND_INVALID)
		return c.sendChannelClose(channelId, amqpError.NotImplemented.Code(), replyText, uint16(ClassQueue), methodId)
	}
}

// ----- Basic Method Handlers -----

func (c *connection) handleMethodBasicGet(reader *bytes.Reader, channelId uint16) error {
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.get (ticket)", uint16(ClassBasic), MethodBasicGet)
	}

	queueName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueName for basic.get: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.get (queueName)", uint16(ClassBasic), MethodBasicGet)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.get (noAck bit)", uint16(ClassBasic), MethodBasicGet)
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
		return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassBasic), MethodBasicGet)
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
		queue.Messages = append([]message{msg}, queue.Messages...)
		queue.mu.Unlock()
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR - channel object nil", uint16(ClassBasic), MethodBasicGet)
	}

	if isClosing {
		// Put message back since channel is closing
		queue.mu.Lock()
		queue.Messages = append([]message{msg}, queue.Messages...)
		queue.mu.Unlock()
		c.server.Debug("Ignoring basic.get on channel %d that is being closed", channelId)
		return nil
	}

	// Assign delivery tag and track if not noAck
	ch.mu.Lock()
	ch.deliveryTag++
	deliveryTag := ch.deliveryTag

	if !noAck {
		unacked := &unackedMessage{
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
		queue.Messages = append([]message{msg}, queue.Messages...)
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

func (c *connection) handleMethodBasicCancel(reader *bytes.Reader, channelId uint16) error {
	consumerTag, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading consumerTag for basic.cancel: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.cancel (consumerTag)", uint16(ClassBasic), MethodBasicCancel)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.cancel (noWait bit)", uint16(ClassBasic), MethodBasicCancel)
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
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR - channel object nil", uint16(ClassBasic), MethodBasicCancel)
	}

	ch.mu.Lock()
	queueName, exists := ch.consumers[consumerTag]
	if !exists {
		ch.mu.Unlock()
		return c.sendChannelClose(channelId, amqpError.NotFound.Code(), fmt.Sprintf("NOT_FOUND - no consumer '%s'", consumerTag), uint16(ClassBasic), MethodBasicCancel)
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

		if err := c.writeFrame(&frame{
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

func (c *connection) handleMethodBasicPublish(reader *bytes.Reader, channelId uint16, ch *channel) error {
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.publish (ticket)", uint16(ClassBasic), MethodBasicPublish)
	}
	exchangeName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading exchangeName for basic.publish: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.publish (exchangeName)", uint16(ClassBasic), MethodBasicPublish)
	}

	routingKey, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading routingKey for basic.publish: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.publish (routingKey)", uint16(ClassBasic), MethodBasicPublish)
	}

	bits, errReadByte := reader.ReadByte()
	if errReadByte != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.publish (bits)", uint16(ClassBasic), MethodBasicPublish)
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
		return c.sendChannelClose(channelId, amqpError.NotImplemented.Code(), replyText, uint16(ClassBasic), MethodBasicPublish) // 540 NOT_IMPLEMENTED
	}

	c.server.Info("Processing basic.publish: exchange='%s', routingKey='%s', mandatory=%v, immediate=%v on channel %d",
		exchangeName, routingKey, mandatory, immediate, channelId)

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
			return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassBasic), MethodBasicPublish)
		}
	}

	ch.mu.Lock()
	// Add to pending messages for this channel. Header and Body frames will follow.
	newMessage := message{
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
}

func (c *connection) handleMethodBasicConsume(reader *bytes.Reader, channelId uint16, ch *channel) error {
	var ticket uint16
	if err := binary.Read(reader, binary.BigEndian, &ticket); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.consume (ticket)", uint16(ClassBasic), MethodBasicConsume)
	}
	queueName, err := readShortString(reader)
	if err != nil {
		c.server.Err("Error reading queueName for basic.consume: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.consume (queueName)", uint16(ClassBasic), MethodBasicConsume)
	}
	consumerTagIn, err := readShortString(reader) // Client can specify, or server generates if empty
	if err != nil {
		c.server.Err("Error reading consumerTagIn for basic.consume: %v", err)
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.consume (consumerTagIn)", uint16(ClassBasic), MethodBasicConsume)
	}

	bits, errReadByte := reader.ReadByte()
	if errReadByte != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.consume (bits)", uint16(ClassBasic), MethodBasicConsume)
	}

	args, errReadTable := readTable(reader) // Consumer arguments
	if errReadTable != nil {
		c.server.Err("Error reading arguments table for basic.consume (queue: '%s'): %v", queueName, errReadTable)
		// AMQP code 502 (SYNTAX_ERROR) for malformed table.
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed arguments table for basic.consume", uint16(ClassBasic), MethodBasicConsume)
	}

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of basic.consume payload for queue '%s'.", queueName)
	}

	noLocal := (bits & 0x01) != 0 // Server doesn't fully implement no-local yet
	noAck := (bits & 0x02) != 0
	exclusive := (bits & 0x04) != 0
	noWait := (bits & 0x08) != 0

	c.server.Info("Processing basic.consume: queue='%s', consumerTag='%s', noLocal=%v, noAck=%v, exclusive=%v, noWait=%v, args=%v on channel %d",
		queueName, consumerTagIn, noLocal, noAck, exclusive, noWait, args, channelId)

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
		return c.sendChannelClose(channelId, amqpError.NotFound.Code(), replyText, uint16(ClassBasic), MethodBasicConsume)
	}

	// Add consumer to channel and queue
	ch.mu.Lock() // Lock channel to modify its consumers map
	if _, tagExistsOnChannel := ch.consumers[actualConsumerTag]; tagExistsOnChannel {
		ch.mu.Unlock()
		replyText := fmt.Sprintf("consumer tag '%s' already in use on channel %d", actualConsumerTag, channelId)
		c.server.Err("Basic.Consume: %s. Sending Channel.Close.", replyText)
		// AMQP code 530 (NOT_ALLOWED) - AMQP spec says "consumer tag already in use on this channel"
		return c.sendChannelClose(channelId, amqpError.NotAllowed.Code(), replyText, uint16(ClassBasic), MethodBasicConsume)
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
			return c.sendChannelClose(channelId, amqpError.ResourceLocked.Code(), replyText, uint16(ClassBasic), MethodBasicConsume)
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
		return c.sendChannelClose(channelId, amqpError.NotAllowed.Code(), replyText, uint16(ClassBasic), MethodBasicConsume)
	}

	consumer := &consumer{
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

		if errWrite := c.writeFrame(&frame{Type: FrameMethod, Channel: channelId, Payload: payloadOk.Bytes()}); errWrite != nil {
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
}

func (c *connection) handleMethodBasicAck(reader *bytes.Reader, channelId uint16) error {
	var deliveryTag uint64
	if err := binary.Read(reader, binary.BigEndian, &deliveryTag); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.ack (delivery-tag)", uint16(ClassBasic), MethodBasicAck)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.ack (bits)", uint16(ClassBasic), MethodBasicAck)
	}

	multiple := (bits & 0x01) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of basic.ack payload.")
	}

	c.server.Info("Processing basic.ack: deliveryTag=%d, multiple=%v on channel %d", deliveryTag, multiple, channelId)

	ch, exists, isClosing := c.getChannel(channelId)

	if !exists {
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR - channel object nil", uint16(ClassBasic), MethodBasicAck)
	}

	if isClosing {
		c.server.Debug("Ignoring basic.ack on channel %d that is being closed", channelId)
		return nil
	}

	// Collect messages to delete from persistence
	var messagesToDelete []AckedMessageInfo

	ch.mu.Lock()

	// Check if in transaction mode
	if ch.txMode {
		// Store acks for transaction
		if deliveryTag == 0 {
			if !multiple {
				ch.mu.Unlock()
				return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - delivery-tag 0 with multiple=false", uint16(ClassBasic), MethodBasicAck)
			}
			// Acknowledge all messages in transaction
			for tag := range ch.unackedMessages {
				ch.txAcks = append(ch.txAcks, tag)
			}
			c.server.Info("Stored ack for all messages in transaction on channel %d", channelId)
		} else {
			if multiple {
				// Acknowledge all messages up to and including deliveryTag
				for tag := range ch.unackedMessages {
					if tag <= deliveryTag {
						ch.txAcks = append(ch.txAcks, tag)
					}
				}
				c.server.Info("Stored ack for messages up to tag %d in transaction on channel %d", deliveryTag, channelId)
			} else {
				// Acknowledge only the specific message
				if _, exists := ch.unackedMessages[deliveryTag]; !exists {
					ch.mu.Unlock()
					return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(), fmt.Sprintf("PRECONDITION_FAILED - unknown delivery-tag %d", deliveryTag), uint16(ClassBasic), MethodBasicAck)
				}
				ch.txAcks = append(ch.txAcks, deliveryTag)
				c.server.Info("Stored ack for tag %d in transaction on channel %d", deliveryTag, channelId)
			}
		}
		ch.mu.Unlock()
		return nil
	}

	// Original non-transactional ack logic
	if deliveryTag == 0 {
		if !multiple {
			ch.mu.Unlock()
			return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - delivery-tag 0 with multiple=false", uint16(ClassBasic), MethodBasicAck)
		}
		for tag, unacked := range ch.unackedMessages {
			if unacked.Message.Properties.DeliveryMode == 2 { // Persistent
				messagesToDelete = append(messagesToDelete, AckedMessageInfo{
					MessageId: GetMessageIdentifier(&unacked.Message),
					QueueName: unacked.QueueName,
					VHostName: c.vhost.name,
				})
			}
			delete(ch.unackedMessages, tag)
		}
		c.server.Info("Acknowledged all messages on channel %d", channelId)
	} else {
		if multiple {
			for tag, unacked := range ch.unackedMessages {
				if tag <= deliveryTag {
					if unacked.Message.Properties.DeliveryMode == 2 { // Persistent
						messagesToDelete = append(messagesToDelete, AckedMessageInfo{
							MessageId: GetMessageIdentifier(&unacked.Message),
							QueueName: unacked.QueueName,
							VHostName: c.vhost.name,
						})
					}
					delete(ch.unackedMessages, tag)
					c.server.Debug("Acknowledged message with delivery tag %d on channel %d", tag, channelId)
				}
			}
			c.server.Info("Acknowledged messages up to delivery tag %d on channel %d", deliveryTag, channelId)
		} else {
			if unacked, exists := ch.unackedMessages[deliveryTag]; exists {
				if unacked.Message.Properties.DeliveryMode == 2 { // Persistent
					messagesToDelete = append(messagesToDelete, AckedMessageInfo{
						MessageId: GetMessageIdentifier(&unacked.Message),
						QueueName: unacked.QueueName,
						VHostName: c.vhost.name,
					})
				}
				delete(ch.unackedMessages, deliveryTag)
				c.server.Info("Acknowledged message with delivery tag %d on channel %d", deliveryTag, channelId)
			} else {
				ch.mu.Unlock()
				return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(),
					fmt.Sprintf("PRECONDITION_FAILED - unknown delivery-tag %d", deliveryTag), uint16(ClassBasic), MethodBasicAck)
			}
		}
	}

	ch.mu.Unlock()

	// PERSISTENCE: Delete acknowledged messages
	if c.server.persistenceManager != nil && len(messagesToDelete) > 0 {
		// Group messages by queue for batch operations
		messagesByQueue := make(map[string][]string)

		for _, msgInfo := range messagesToDelete {
			key := msgInfo.VHostName + ":" + msgInfo.QueueName
			messagesByQueue[key] = append(messagesByQueue[key], msgInfo.MessageId)
		}

		// Delete in batches by queue
		for queueKey, messageIds := range messagesByQueue {
			parts := strings.Split(queueKey, ":")
			vhostName, queueName := parts[0], parts[1]

			if err := c.server.persistenceManager.DeleteMessagesBatch(vhostName, queueName, messageIds); err != nil {
				c.server.Err("Failed to delete nacked messages from queue %s: %v", queueName, err)
			}
		}
	}

	return nil
}

func (c *connection) handleMethodBasicNack(reader *bytes.Reader, channelId uint16) error {
	var deliveryTag uint64
	if err := binary.Read(reader, binary.BigEndian, &deliveryTag); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.nack (delivery-tag)", uint16(ClassBasic), MethodBasicNack)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.nack (bits)", uint16(ClassBasic), MethodBasicNack)
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
		c.server.Err("Internal error: channel object nil for basic.nack on channel %d", channelId)
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR - channel object nil for nack", uint16(ClassBasic), MethodBasicNack)
	}

	if isClosing {
		c.server.Debug("Ignoring basic.nack on channel %d that is being closed", channelId)
		return nil
	}

	ch.mu.Lock()

	// Check if in transaction mode
	if ch.txMode {
		// Store nacks for transaction
		nack := transactionalNack{
			DeliveryTag: deliveryTag,
			Multiple:    multiple,
			Requeue:     requeue,
		}
		ch.txNacks = append(ch.txNacks, nack)
		ch.mu.Unlock()

		c.server.Info("Stored nack in transaction on channel %d: tag=%d, multiple=%v, requeue=%v",
			channelId, deliveryTag, multiple, requeue)
		return nil
	}

	// Original non-transactional nack logic continues...
	var messagesToProcess []*unackedMessage
	var affectedQueues = make(map[string]bool)

	if deliveryTag == 0 {
		if !multiple {
			ch.mu.Unlock()
			return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - delivery-tag 0 with multiple=false for nack", uint16(ClassBasic), MethodBasicNack)
		}
		for _, unacked := range ch.unackedMessages {
			messagesToProcess = append(messagesToProcess, unacked)
		}
		ch.unackedMessages = make(map[uint64]*unackedMessage)
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
				return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(), fmt.Sprintf("PRECONDITION_FAILED - unknown delivery-tag %d for nack", deliveryTag), uint16(ClassBasic), MethodBasicNack)
			}
			messagesToProcess = append(messagesToProcess, unacked)
			delete(ch.unackedMessages, deliveryTag)
		}
	}
	ch.mu.Unlock()

	var messagesToDelete []AckedMessageInfo

	// Process the nacked messages
	if requeue {
		vhost := c.vhost
		for _, unacked := range messagesToProcess {
			vhost.mu.RLock()
			queue, qExists := vhost.queues[unacked.QueueName]
			vhost.mu.RUnlock()

			if qExists && queue != nil {
				unacked.Message.Redelivered = true

				queue.mu.Lock()
				queue.Messages = append([]message{unacked.Message}, queue.Messages...)
				c.server.Info("Requeued message to queue '%s' (original delivery tag %d on channel %d)", unacked.QueueName, unacked.DeliveryTag, channelId)
				affectedQueues[unacked.QueueName] = true
				queue.mu.Unlock()
			} else {
				c.server.Warn("Queue '%s' not found for requeuing nacked message (tag %d on channel %d)", unacked.QueueName, unacked.DeliveryTag, channelId)
			}
		}
	} else {
		// Collect persistent messages to delete
		for _, unacked := range messagesToProcess {
			if unacked.Message.Properties.DeliveryMode == 2 {
				messagesToDelete = append(messagesToDelete, AckedMessageInfo{
					MessageId: GetMessageIdentifier(&unacked.Message),
					QueueName: unacked.QueueName,
					VHostName: c.vhost.name,
				})
			}
		}

		c.server.Info("Discarded %d nacked messages (requeue=false) from channel %d", len(messagesToProcess), channelId)

		// PERSISTENCE: Delete nacked messages
		if c.server.persistenceManager != nil && len(messagesToDelete) > 0 {
			for _, msgInfo := range messagesToDelete {
				if err := c.server.persistenceManager.DeleteMessage(msgInfo.VHostName,
					msgInfo.QueueName, msgInfo.MessageId); err != nil {
					c.server.Err("Failed to delete nacked message %s from persistence: %v", msgInfo.MessageId, err)
				}
			}
		}
	}

	return nil
}

func (c *connection) handleMethodBasicReject(reader *bytes.Reader, channelId uint16) error {
	var deliveryTag uint64
	if err := binary.Read(reader, binary.BigEndian, &deliveryTag); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.reject (delivery-tag)", uint16(ClassBasic), MethodBasicReject)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.reject (requeue bit)", uint16(ClassBasic), MethodBasicReject)
	}
	requeue := (bits & 0x01) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of basic.reject payload.")
	}

	c.server.Info("Processing basic.reject: deliveryTag=%d, requeue=%v on channel %d", deliveryTag, requeue, channelId)

	ch, exists, isClosing := c.getChannel(channelId)
	if !exists {
		c.server.Err("Internal error: channel object nil for basic.reject on channel %d", channelId)
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR - channel object nil for reject", uint16(ClassBasic), MethodBasicReject)
	}

	if isClosing {
		c.server.Debug("Ignoring basic.reject on channel %d that is being closed", channelId)
		return nil
	}

	ch.mu.Lock()

	// Check if in transaction mode
	if ch.txMode {
		// Store reject as nack for transaction (reject is equivalent to nack with multiple=false)
		nack := transactionalNack{
			DeliveryTag: deliveryTag,
			Multiple:    false,
			Requeue:     requeue,
		}
		ch.txNacks = append(ch.txNacks, nack)
		ch.mu.Unlock()

		c.server.Info("Stored reject as nack in transaction on channel %d: tag=%d, requeue=%v",
			channelId, deliveryTag, requeue)
		return nil
	}

	// Original non-transactional reject logic
	if deliveryTag == 0 {
		ch.mu.Unlock()
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - delivery-tag cannot be 0 for basic.reject", uint16(ClassBasic), MethodBasicReject)
	}

	unacked, exists := ch.unackedMessages[deliveryTag]
	if !exists {
		ch.mu.Unlock()
		return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(), fmt.Sprintf("PRECONDITION_FAILED - unknown delivery-tag %d for reject", deliveryTag), uint16(ClassBasic), MethodBasicReject)
	}

	// Store info for persistence deletion if not requeuing
	var messageToDelete *AckedMessageInfo
	if !requeue && unacked.Message.Properties.DeliveryMode == 2 {
		messageToDelete = &AckedMessageInfo{
			MessageId: GetMessageIdentifier(&unacked.Message),
			QueueName: unacked.QueueName,
			VHostName: c.vhost.name,
		}
	}

	delete(ch.unackedMessages, deliveryTag)
	ch.mu.Unlock()

	if requeue {
		vhost := c.vhost
		vhost.mu.RLock()
		queue, qExists := vhost.queues[unacked.QueueName]
		vhost.mu.RUnlock()

		if qExists && queue != nil {
			unacked.Message.Redelivered = true

			queue.mu.Lock()
			queue.Messages = append([]message{unacked.Message}, queue.Messages...)
			c.server.Info("Requeued rejected message to queue '%s' (delivery tag %d on channel %d)", unacked.QueueName, deliveryTag, channelId)
			queue.mu.Unlock()
		} else {
			c.server.Warn("Queue '%s' not found for requeuing rejected message (tag %d on channel %d)", unacked.QueueName, deliveryTag, channelId)
		}
	} else {
		c.server.Info("Discarded rejected message (requeue=false) from channel %d (delivery tag %d)", channelId, deliveryTag)

		// PERSISTENCE: Delete rejected message if not requeued
		if c.server.persistenceManager != nil && messageToDelete != nil {
			if err := c.server.persistenceManager.DeleteMessage(messageToDelete.VHostName,
				messageToDelete.QueueName, messageToDelete.MessageId); err != nil {
				c.server.Err("Failed to delete rejected message %s from persistence: %v", messageToDelete.MessageId, err)
			}
		}
	}

	return nil
}

func (c *connection) handleMethodBasicQos(reader *bytes.Reader, channelId uint16) error {
	var prefetchSize uint32
	var prefetchCount uint16

	if err := binary.Read(reader, binary.BigEndian, &prefetchSize); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.qos (prefetch-size)", uint16(ClassBasic), MethodBasicQos)
	}

	if err := binary.Read(reader, binary.BigEndian, &prefetchCount); err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.qos (prefetch-count)", uint16(ClassBasic), MethodBasicQos)
	}

	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.qos (global bit)", uint16(ClassBasic), MethodBasicQos)
	}
	global := (bits & 0x01) != 0

	c.server.Info("Processing basic.qos: prefetchSize=%d, prefetchCount=%d, global=%v on channel %d",
		prefetchSize, prefetchCount, global, channelId)

	// Get the channel
	ch, exists, isClosing := c.getChannel(channelId)
	if !exists {
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR - channel object nil", uint16(ClassBasic), MethodBasicQos)
	}

	if isClosing {
		c.server.Debug("Ignoring basic.qos on channel %d that is being closed", channelId)
		return nil
	}

	// Set QoS parameters (ignoring size and global for simplicity)
	ch.mu.Lock()
	ch.prefetchCount = prefetchCount
	ch.prefetchSize = prefetchSize
	ch.qosGlobal = global
	ch.mu.Unlock()

	c.server.Info("Set QoS on channel %d: prefetchCount=%d", channelId, prefetchCount)

	// Send Basic.QosOk
	payload := &bytes.Buffer{}
	binary.Write(payload, binary.BigEndian, uint16(ClassBasic))
	binary.Write(payload, binary.BigEndian, uint16(MethodBasicQosOk))

	if err := c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payload.Bytes(),
	}); err != nil {
		c.server.Err("Error sending basic.qos-ok: %v", err)
		return err
	}

	c.server.Info("Sent basic.qos-ok for channel %d", channelId)
	return nil
}

func (c *connection) handleMethodBasicRecover(reader *bytes.Reader, channelId uint16) error {
	bits, err := reader.ReadByte()
	if err != nil {
		return c.sendChannelClose(channelId, amqpError.SyntaxError.Code(), "SYNTAX_ERROR - malformed basic.recover (requeue bit)", uint16(ClassBasic), MethodBasicRecover)
	}
	requeue := (bits & 0x01) != 0

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of basic.recover payload.")
	}

	c.server.Info("Processing basic.recover: requeue=%v on channel %d", requeue, channelId)

	ch, exists, isClosing := c.getChannel(channelId)
	if !exists {
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR - channel object nil", uint16(ClassBasic), MethodBasicRecover)
	}

	if isClosing {
		c.server.Debug("Ignoring basic.recover on channel %d that is being closed", channelId)
		return nil
	}

	ch.mu.Lock()

	// Collect all unacked messages
	var messagesToProcess []*unackedMessage
	affectedQueues := make(map[string]bool)

	for _, unacked := range ch.unackedMessages {
		messagesToProcess = append(messagesToProcess, unacked)
		affectedQueues[unacked.QueueName] = true
	}

	// Clear all unacked messages from this channel
	ch.unackedMessages = make(map[uint64]*unackedMessage)
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
				queue.Messages = append([]message{unacked.Message}, queue.Messages...)
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

	if err := c.writeFrame(&frame{
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

func (c *connection) handleClassBasicMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassBasic, methodId)

	ch, channelExists, isClosing := c.getChannel(channelId)

	if !channelExists && channelId != 0 { // Channel 0 is special and should not receive Basic methods
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for basic operation", channelId)
		c.server.Err("Basic method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		// AMQP code 503 (COMMAND_INVALID) or 504 (CHANNEL_ERROR)
		return c.sendConnectionClose(amqpError.CommandInvalid.Code(), replyText, uint16(ClassBasic), methodId)
	}
	// ch can be nil if channelId is 0, but basic methods are not for channel 0.
	// This case should be caught by the above, but as a safeguard:
	if ch == nil && channelId != 0 { // Should have been caught by channelExists check
		// This indicates an internal server inconsistency if channelId was non-zero but ch is nil.
		c.server.Err("Internal error: channel %d object not found for basic method %s, though channel map indicated existence.", channelId, methodName)
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR - channel state inconsistency", uint16(ClassBasic), methodId)
	}
	if channelId == 0 { // Basic methods are not valid on channel 0
		replyText := "COMMAND_INVALID - basic methods cannot be used on channel 0"
		c.server.Err("Basic method %s on channel 0. Sending Connection.Close.", methodName)
		return c.sendConnectionClose(amqpError.CommandInvalid.Code(), replyText, uint16(ClassBasic), methodId)
	}

	if isClosing {
		c.server.Debug("Ignoring basic method %s on channel %d that is being closed", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodBasicGet:
		return c.handleMethodBasicGet(reader, channelId)
	case MethodBasicRecover:
		return c.handleMethodBasicRecover(reader, channelId)
	case MethodBasicPublish:
		return c.handleMethodBasicPublish(reader, channelId, ch)
	case MethodBasicConsume:
		return c.handleMethodBasicConsume(reader, channelId, ch)
	case MethodBasicAck:
		return c.handleMethodBasicAck(reader, channelId)
	case MethodBasicNack:
		return c.handleMethodBasicNack(reader, channelId)
	case MethodBasicReject:
		return c.handleMethodBasicReject(reader, channelId)
	case MethodBasicCancel:
		return c.handleMethodBasicCancel(reader, channelId)
	case MethodBasicQos:
		return c.handleMethodBasicQos(reader, channelId)
	default:
		replyText := fmt.Sprintf("unknown or not implemented basic method id %d", methodId)
		c.server.Err("Unhandled basic method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		// AMQP code 540 (NOT_IMPLEMENTED) or 503 (COMMAND_INVALID)
		return c.sendChannelClose(channelId, amqpError.NotImplemented.Code(), replyText, uint16(ClassBasic), methodId)
	}
}

// ------ Helper functions for Tx methods ------

func (c *connection) handleMethodTxSelect(reader *bytes.Reader, channelId uint16, ch *channel) error {
	c.server.Info("Processing tx.select for channel %d", channelId)

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of tx.select payload for channel %d.", channelId)
	}

	ch.mu.Lock()

	// Check if already in confirm mode
	if ch.confirmMode {
		ch.mu.Unlock()
		return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(),
			"PRECONDITION_FAILED - cannot use transactions with confirm mode", uint16(ClassTx), MethodTxSelect)
	}

	// Enable transaction mode
	ch.txMode = true
	// Initialize transaction storage if needed
	if ch.txMessages == nil {
		ch.txMessages = make([]transactionalMessage, 0)
	}
	if ch.txAcks == nil {
		ch.txAcks = make([]uint64, 0)
	}
	if ch.txNacks == nil {
		ch.txNacks = make([]transactionalNack, 0)
	}

	ch.mu.Unlock()

	c.server.Info("Enabled transaction mode on channel %d", channelId)

	// Send Tx.SelectOk
	payloadOk := &bytes.Buffer{}
	binary.Write(payloadOk, binary.BigEndian, uint16(ClassTx))
	binary.Write(payloadOk, binary.BigEndian, uint16(MethodTxSelectOk))

	if err := c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payloadOk.Bytes(),
	}); err != nil {
		c.server.Err("Error sending tx.select-ok: %v", err)
		return err
	}

	c.server.Info("Sent tx.select-ok for channel %d", channelId)
	return nil
}

func (c *connection) handleMethodTxCommit(reader *bytes.Reader, channelId uint16, ch *channel) error {
	c.server.Info("Processing tx.commit for channel %d", channelId)

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of tx.commit payload for channel %d.", channelId)
	}

	ch.mu.Lock()

	if !ch.txMode {
		ch.mu.Unlock()
		return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(),
			"PRECONDITION_FAILED - channel not in transaction mode", uint16(ClassTx), MethodTxCommit)
	}

	// Capture transaction data and clear it
	messagesToPublish := make([]transactionalMessage, len(ch.txMessages))
	copy(messagesToPublish, ch.txMessages)
	ch.txMessages = ch.txMessages[:0] // Clear but keep capacity

	acksToProcess := make([]uint64, len(ch.txAcks))
	copy(acksToProcess, ch.txAcks)
	ch.txAcks = ch.txAcks[:0]

	nacksToProcess := make([]transactionalNack, len(ch.txNacks))
	copy(nacksToProcess, ch.txNacks)
	ch.txNacks = ch.txNacks[:0]

	ch.mu.Unlock()

	c.server.Info("Committing transaction on channel %d: %d messages, %d acks, %d nacks",
		channelId, len(messagesToPublish), len(acksToProcess), len(nacksToProcess))

	// Process all published messages
	for _, txMsg := range messagesToPublish {
		// Create a deep copy of the message for delivery
		msgToDeliver := txMsg.Message.DeepCopy()
		msgToDeliver.Exchange = txMsg.Exchange
		msgToDeliver.RoutingKey = txMsg.RoutingKey
		msgToDeliver.Mandatory = txMsg.Mandatory
		msgToDeliver.Immediate = txMsg.Immediate

		c.deliverMessageInternal(msgToDeliver, channelId, true)
	}

	// Process all acknowledgements and negative acknowledgements together under lock
	ch.mu.Lock()

	// Collect messages to delete from persistence
	var messagesToDelete []AckedMessageInfo

	// Process acknowledgements
	for _, deliveryTag := range acksToProcess {
		if unacked, exists := ch.unackedMessages[deliveryTag]; exists {
			if unacked.Message.Properties.DeliveryMode == 2 { // Persistent message
				messagesToDelete = append(messagesToDelete, AckedMessageInfo{
					MessageId: GetMessageIdentifier(&unacked.Message),
					QueueName: unacked.QueueName,
					VHostName: c.vhost.name,
				})
			}
		}
		delete(ch.unackedMessages, deliveryTag)
		c.server.Debug("Committed ack for delivery tag %d on channel %d", deliveryTag, channelId)
	}

	// Collect messages that need to be requeued from nacks
	var messagesToRequeue []struct {
		Message   message
		QueueName string
	}

	// Process negative acknowledgements
	for _, nack := range nacksToProcess {
		if nack.Multiple {
			// Handle multiple nacks
			for tag, unacked := range ch.unackedMessages {
				if tag <= nack.DeliveryTag {
					if nack.Requeue {
						messagesToRequeue = append(messagesToRequeue, struct {
							Message   message
							QueueName string
						}{
							Message:   unacked.Message,
							QueueName: unacked.QueueName,
						})
					} else if unacked.Message.Properties.DeliveryMode == 2 {
						// Not requeuing and message is persistent - delete from storage
						messagesToDelete = append(messagesToDelete, AckedMessageInfo{
							MessageId: GetMessageIdentifier(&unacked.Message),
							QueueName: unacked.QueueName,
							VHostName: c.vhost.name,
						})
					}
					delete(ch.unackedMessages, tag)
					c.server.Debug("Processed nack for tag %d in transaction on channel %d", tag, channelId)
				}
			}
		} else {
			// Handle single nack
			if unacked, exists := ch.unackedMessages[nack.DeliveryTag]; exists {
				if nack.Requeue {
					messagesToRequeue = append(messagesToRequeue, struct {
						Message   message
						QueueName string
					}{
						Message:   unacked.Message,
						QueueName: unacked.QueueName,
					})
				} else if unacked.Message.Properties.DeliveryMode == 2 {
					// Not requeuing and message is persistent - delete from storage
					messagesToDelete = append(messagesToDelete, AckedMessageInfo{
						MessageId: GetMessageIdentifier(&unacked.Message),
						QueueName: unacked.QueueName,
						VHostName: c.vhost.name,
					})
				}
				delete(ch.unackedMessages, nack.DeliveryTag)
				c.server.Debug("Processed nack for tag %d in transaction on channel %d", nack.DeliveryTag, channelId)
			}
		}
	}

	ch.mu.Unlock()

	// Now requeue messages outside the channel lock to avoid potential deadlocks
	if len(messagesToRequeue) > 0 {
		vhost := c.vhost
		for _, item := range messagesToRequeue {
			vhost.mu.RLock()
			queue, exists := vhost.queues[item.QueueName]
			vhost.mu.RUnlock()

			if exists && queue != nil {
				item.Message.Redelivered = true
				queue.mu.Lock()
				queue.Messages = append([]message{item.Message}, queue.Messages...)
				queue.mu.Unlock()
				c.server.Debug("Requeued message to queue '%s' via transaction commit", item.QueueName)
			}
		}
	}

	// PERSISTENCE: Delete all acknowledged/nacked messages in the transaction
	if c.server.persistenceManager != nil && len(messagesToDelete) > 0 {
		for _, msgInfo := range messagesToDelete {
			if err := c.server.persistenceManager.DeleteMessage(msgInfo.VHostName,
				msgInfo.QueueName, msgInfo.MessageId); err != nil {
				c.server.Err("Failed to delete message %s from persistence in transaction: %v", msgInfo.MessageId, err)
			}
		}
	}

	// Send Tx.CommitOk
	payloadOk := &bytes.Buffer{}
	binary.Write(payloadOk, binary.BigEndian, uint16(ClassTx))
	binary.Write(payloadOk, binary.BigEndian, uint16(MethodTxCommitOk))

	if err := c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payloadOk.Bytes(),
	}); err != nil {
		c.server.Err("Error sending tx.commit-ok: %v", err)
		return err
	}

	c.server.Info("Sent tx.commit-ok for channel %d", channelId)
	return nil
}

func (c *connection) handleMethodTxRollback(reader *bytes.Reader, channelId uint16, ch *channel) error {
	c.server.Info("Processing tx.rollback for channel %d", channelId)

	if reader.Len() > 0 {
		c.server.Warn("Extra data at end of tx.rollback payload for channel %d.", channelId)
	}

	ch.mu.Lock()

	if !ch.txMode {
		ch.mu.Unlock()
		return c.sendChannelClose(channelId, amqpError.PreconditionFailed.Code(),
			"PRECONDITION_FAILED - channel not in transaction mode", uint16(ClassTx), MethodTxRollback)
	}

	// Clear all transaction data
	messagesCount := len(ch.txMessages)
	acksCount := len(ch.txAcks)
	nacksCount := len(ch.txNacks)

	ch.txMessages = ch.txMessages[:0]
	ch.txAcks = ch.txAcks[:0]
	ch.txNacks = ch.txNacks[:0]

	ch.mu.Unlock()

	c.server.Info("Rolled back transaction on channel %d: discarded %d messages, %d acks, %d nacks",
		channelId, messagesCount, acksCount, nacksCount)

	// Send Tx.RollbackOk
	payloadOk := &bytes.Buffer{}
	binary.Write(payloadOk, binary.BigEndian, uint16(ClassTx))
	binary.Write(payloadOk, binary.BigEndian, uint16(MethodTxRollbackOk))

	if err := c.writeFrame(&frame{
		Type:    FrameMethod,
		Channel: channelId,
		Payload: payloadOk.Bytes(),
	}); err != nil {
		c.server.Err("Error sending tx.rollback-ok: %v", err)
		return err
	}

	c.server.Info("Sent tx.rollback-ok for channel %d", channelId)
	return nil
}

func (c *connection) handleClassTxMethod(methodId uint16, reader *bytes.Reader, channelId uint16) error {
	methodName := getMethodName(ClassTx, methodId)

	ch, channelExists, isClosing := c.getChannel(channelId)

	if !channelExists && channelId != 0 {
		replyText := fmt.Sprintf("COMMAND_INVALID - unknown channel id %d for tx operation", channelId)
		c.server.Err("Tx method %s on non-existent channel %d. Sending Connection.Close.", methodName, channelId)
		return c.sendConnectionClose(amqpError.CommandInvalid.Code(), replyText, uint16(ClassTx), methodId)
	}

	if ch == nil && channelId != 0 {
		c.server.Err("Internal error: channel %d object not found for tx method %s", channelId, methodName)
		return c.sendConnectionClose(amqpError.InternalError.Code(), "INTERNAL_SERVER_ERROR - channel state inconsistency", uint16(ClassTx), methodId)
	}

	if channelId == 0 {
		replyText := "COMMAND_INVALID - tx methods cannot be used on channel 0"
		c.server.Err("Tx method %s on channel 0. Sending Connection.Close.", methodName)
		return c.sendConnectionClose(amqpError.CommandInvalid.Code(), replyText, uint16(ClassTx), methodId)
	}

	if isClosing {
		c.server.Debug("Ignoring tx method %s on channel %d that is being closed", methodName, channelId)
		return nil
	}

	switch methodId {
	case MethodTxSelect:
		return c.handleMethodTxSelect(reader, channelId, ch)
	case MethodTxCommit:
		return c.handleMethodTxCommit(reader, channelId, ch)
	case MethodTxRollback:
		return c.handleMethodTxRollback(reader, channelId, ch)
	default:
		replyText := fmt.Sprintf("unknown or not implemented tx method id %d", methodId)
		c.server.Err("Unhandled tx method on channel %d: %s. Sending Channel.Close.", channelId, replyText)
		return c.sendChannelClose(channelId, amqpError.NotImplemented.Code(), replyText, uint16(ClassTx), methodId)
	}
}
