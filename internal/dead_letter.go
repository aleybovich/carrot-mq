package internal

// Dead-lettering, per-queue/per-message TTL, and queue length limits
// (RabbitMQ-compatible AMQP 0-9-1 extension).
//
// Queue arguments understood here:
//   x-dead-letter-exchange    (string)  exchange dead messages are republished to
//   x-dead-letter-routing-key (string)  overrides the routing key on republish
//   x-message-ttl             (int, ms) per-queue message TTL
//   x-max-length              (int)     max ready messages; overflow drops from head
//
// Death reasons: "rejected" (basic.reject/nack requeue=false), "expired"
// (per-queue or per-message TTL), "maxlen" (x-max-length overflow).

import (
	"fmt"
	"strconv"
	"time"
)

const (
	deathReasonRejected = "rejected"
	deathReasonExpired  = "expired"
	deathReasonMaxlen   = "maxlen"

	headerXDeath              = "x-death"
	headerFirstDeathReason    = "x-first-death-reason"
	headerFirstDeathQueue     = "x-first-death-queue"
	headerFirstDeathExchange  = "x-first-death-exchange"
	headerOriginalExpiration  = "original-expiration"
	argDeadLetterExchange     = "x-dead-letter-exchange"
	argDeadLetterRoutingKey   = "x-dead-letter-routing-key"
	argMessageTTL             = "x-message-ttl"
	argMaxLength              = "x-max-length"
)

// queueArgs is the parsed, normalized form of the dead-letter/TTL/length
// queue arguments. Unset numeric values are -1.
type queueArgs struct {
	deadLetterExchange   string
	hasDLX               bool
	deadLetterRoutingKey string
	hasDLRoutingKey      bool
	messageTTL           int64 // milliseconds, -1 = unset
	maxLength            int64 // -1 = unset
}

// asAMQPInt normalizes any AMQP integer field value (and JSON-recovered
// float64 holding an integral value) to int64.
func asAMQPInt(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int8:
		return int64(n), true
	case uint8:
		return int64(n), true
	case int16:
		return int64(n), true
	case uint16:
		return int64(n), true
	case int32:
		return int64(n), true
	case uint32:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64: // persisted arguments round-trip through JSON as float64
		if n == float64(int64(n)) {
			return int64(n), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// parseQueueArgs validates and normalizes the dead-letter/TTL/length queue
// arguments. Unknown arguments are ignored. An invalid type or a negative
// numeric value is an error (queue.declare must fail with 406).
func parseQueueArgs(args map[string]interface{}) (queueArgs, error) {
	qa := queueArgs{messageTTL: -1, maxLength: -1}
	if args == nil {
		return qa, nil
	}

	if v, ok := args[argDeadLetterExchange]; ok {
		s, isStr := v.(string)
		if !isStr {
			return qa, fmt.Errorf("invalid arg '%s': expected string, got %T", argDeadLetterExchange, v)
		}
		qa.deadLetterExchange = s
		qa.hasDLX = true
	}
	if v, ok := args[argDeadLetterRoutingKey]; ok {
		s, isStr := v.(string)
		if !isStr {
			return qa, fmt.Errorf("invalid arg '%s': expected string, got %T", argDeadLetterRoutingKey, v)
		}
		qa.deadLetterRoutingKey = s
		qa.hasDLRoutingKey = true
	}
	if v, ok := args[argMessageTTL]; ok {
		n, isInt := asAMQPInt(v)
		if !isInt || n < 0 {
			return qa, fmt.Errorf("invalid arg '%s': expected non-negative integer, got %v (%T)", argMessageTTL, v, v)
		}
		qa.messageTTL = n
	}
	if v, ok := args[argMaxLength]; ok {
		n, isInt := asAMQPInt(v)
		if !isInt || n < 0 {
			return qa, fmt.Errorf("invalid arg '%s': expected non-negative integer, got %v (%T)", argMaxLength, v, v)
		}
		qa.maxLength = n
	}
	return qa, nil
}

// equivalent reports whether two parsed argument sets are the same for the
// purposes of queue.declare equivalence checking. Comparing the normalized
// form makes int32-vs-int64 (and JSON float64) representations equivalent.
func (qa queueArgs) equivalent(other queueArgs) bool {
	return qa == other
}

// messageDeadline computes when msg expires in a queue with these arguments.
// Returns the zero time when the message never expires. The per-message
// expiration property and the per-queue TTL are combined with min().
// An unparseable expiration property is ignored.
func (qa queueArgs) messageDeadline(msg *message, now time.Time) time.Time {
	var deadline time.Time
	if qa.messageTTL >= 0 {
		deadline = now.Add(time.Duration(qa.messageTTL) * time.Millisecond)
	}
	if exp := msg.Properties.Expiration; exp != "" {
		if ms, err := strconv.ParseInt(exp, 10, 64); err == nil && ms >= 0 {
			perMsg := now.Add(time.Duration(ms) * time.Millisecond)
			if deadline.IsZero() || perMsg.Before(deadline) {
				deadline = perMsg
			}
		}
	}
	return deadline
}

// isExpired reports whether the message's TTL deadline has passed.
func (m *message) isExpired(now time.Time) bool {
	return !m.expiresAt.IsZero() && !m.expiresAt.After(now)
}

// recordDeath adds a death event to the message's x-death header, following
// RabbitMQ semantics: a repeated death from the same queue with the same
// reason increments the existing entry's count (moving it to the front)
// instead of appending a duplicate. The x-first-death-* headers are set on
// the first death only. Must be called BEFORE the message's Exchange and
// RoutingKey are rewritten for republishing.
//
// All modified maps/slices are freshly allocated: message deep copies share
// nested header values, so entries are never mutated in place.
func recordDeath(msg *message, reason, queueName, originalExpiration string) {
	headers := make(map[string]interface{}, len(msg.Properties.Headers)+4)
	for k, v := range msg.Properties.Headers {
		headers[k] = v
	}

	newEntry := map[string]interface{}{
		"count":        int64(1),
		"reason":       reason,
		"queue":        queueName,
		"time":         uint64(time.Now().Unix()), // encoded as AMQP timestamp 'T'
		"exchange":     msg.Exchange,
		"routing-keys": []interface{}{msg.RoutingKey},
	}
	if originalExpiration != "" {
		newEntry[headerOriginalExpiration] = originalExpiration
	}

	var rebuilt []interface{}
	if existing, ok := headers[headerXDeath].([]interface{}); ok {
		for _, e := range existing {
			entry, isTable := e.(map[string]interface{})
			if !isTable {
				rebuilt = append(rebuilt, e)
				continue
			}
			if entry["queue"] == queueName && entry["reason"] == reason {
				// Same queue+reason: carry the accumulated count over.
				if prev, isInt := asAMQPInt(entry["count"]); isInt {
					newEntry["count"] = prev + 1
				}
				continue // replaced by newEntry at the front
			}
			rebuilt = append(rebuilt, e)
		}
	}
	headers[headerXDeath] = append([]interface{}{newEntry}, rebuilt...)

	if _, ok := headers[headerFirstDeathReason]; !ok {
		headers[headerFirstDeathReason] = reason
		headers[headerFirstDeathQueue] = queueName
		headers[headerFirstDeathExchange] = msg.Exchange
	}

	msg.Properties.Headers = headers
}

// routeInVHost resolves the queue names an (exchange, routingKey) pair routes
// to, independent of any client connection. The default exchange ("") routes
// directly to the queue named by the routing key.
func routeInVHost(vh *vHost, exchangeName, routingKey string) ([]string, error) {
	if exchangeName == "" {
		vh.mu.RLock()
		_, exists := vh.queues[routingKey]
		vh.mu.RUnlock()
		if exists {
			return []string{routingKey}, nil
		}
		return nil, nil
	}

	vh.mu.RLock()
	ex := vh.exchanges[exchangeName]
	vh.mu.RUnlock()
	if ex == nil {
		return nil, fmt.Errorf("exchange '%s' not found", exchangeName)
	}

	ex.mu.RLock()
	defer ex.mu.RUnlock()

	switch ex.Type {
	case "direct":
		return ex.Bindings[routingKey], nil
	case "fanout":
		queues := make([]string, 0)
		seen := make(map[string]bool)
		for _, bound := range ex.Bindings {
			for _, q := range bound {
				if !seen[q] {
					seen[q] = true
					queues = append(queues, q)
				}
			}
		}
		return queues, nil
	case "topic":
		queues := make([]string, 0)
		seen := make(map[string]bool)
		for pattern, bound := range ex.Bindings {
			if topicMatch(pattern, routingKey) {
				for _, q := range bound {
					if !seen[q] {
						seen[q] = true
						queues = append(queues, q)
					}
				}
			}
		}
		return queues, nil
	default:
		return nil, fmt.Errorf("unknown exchange type: %s", ex.Type)
	}
}

// deadLetterMessage republishes msg — already removed from sourceQueue — to
// the source queue's dead-letter exchange with the appropriate x-death
// metadata. If the queue has no DLX, or the DLX is missing or routes
// nowhere, the message is dropped. The caller owns source-side persistence
// cleanup and must NOT hold sourceQueue.mu (a queue may dead-letter into
// itself).
func (s *server) deadLetterMessage(vh *vHost, sourceQueue *queue, msg *message, reason string) {
	sourceQueue.mu.RLock()
	qa := sourceQueue.args
	sourceQueue.mu.RUnlock()

	if !qa.hasDLX {
		return
	}

	dead := msg.DeepCopy()
	dead.Redelivered = false

	originalExpiration := ""
	if reason == deathReasonExpired && dead.Properties.Expiration != "" {
		// Clear per-message TTL so the message doesn't expire again in the
		// dead-letter queue; the original value is recorded in x-death.
		originalExpiration = dead.Properties.Expiration
		dead.Properties.Expiration = ""
	}

	// Record the death against the ORIGINAL exchange/routing key, then
	// rewrite them for republishing through the DLX.
	recordDeath(dead, reason, sourceQueue.Name, originalExpiration)
	dead.Exchange = qa.deadLetterExchange
	if qa.hasDLRoutingKey {
		dead.RoutingKey = qa.deadLetterRoutingKey
	}
	dead.Mandatory = false
	dead.Immediate = false

	queueNames, err := routeInVHost(vh, dead.Exchange, dead.RoutingKey)
	if err != nil {
		s.Warn("Dead-lettering from queue '%s': %v; message dropped", sourceQueue.Name, err)
		return
	}
	if len(queueNames) == 0 {
		s.Info("Dead-lettering from queue '%s': no route for exchange '%s' key '%s'; message dropped",
			sourceQueue.Name, dead.Exchange, dead.RoutingKey)
		return
	}

	s.Info("Dead-lettering message from queue '%s' (reason: %s) to exchange '%s' key '%s' -> %v",
		sourceQueue.Name, reason, dead.Exchange, dead.RoutingKey, queueNames)

	for _, name := range queueNames {
		vh.mu.RLock()
		target := vh.queues[name]
		vh.mu.RUnlock()
		if target == nil {
			continue
		}
		if err := s.enqueueMessage(vh, target, dead); err != nil {
			s.Err("Dead-lettering to queue '%s' failed: %v", name, err)
		}
	}
}

// enqueueMessage places msg at the tail of q, enforcing x-max-length
// (overflow drops from the head, dead-lettered with reason "maxlen"),
// stamping the message's TTL deadline, persisting persistent messages to
// durable queues, and waking an idle consumer. Callers must NOT hold q.mu.
func (s *server) enqueueMessage(vh *vHost, q *queue, msg *message) error {
	msgCopy := msg.DeepCopy()

	q.mu.RLock()
	qa := q.args
	q.mu.RUnlock()

	msgCopy.expiresAt = qa.messageDeadline(msgCopy, time.Now())

	// A zero-length queue dead-letters every incoming message immediately.
	if qa.maxLength == 0 {
		s.deadLetterMessage(vh, q, msgCopy, deathReasonMaxlen)
		return nil
	}

	// Persist before exposing in memory (mirrors the original publish path).
	persisted := s.persistenceManager != nil && msgCopy.Properties.DeliveryMode == 2 && q.Durable
	if persisted {
		tx, err := s.persistenceManager.BeginTransaction()
		if err != nil {
			return fmt.Errorf("beginning transaction: %w", err)
		}
		messageId := GetMessageIdentifier(msgCopy)
		record := MessageToRecord(msgCopy, messageId, 0)
		if err := tx.SaveMessage(vh.name, q.Name, record); err != nil {
			tx.Rollback()
			return fmt.Errorf("saving message to transaction: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("committing message: %w", err)
		}
	}

	var overflow []message
	q.mu.Lock()
	if qa.maxLength > 0 {
		for int64(len(q.Messages)) >= qa.maxLength {
			overflow = append(overflow, q.Messages[0])
			q.Messages = q.Messages[1:]
		}
	}
	q.Messages = append(q.Messages, *msgCopy)
	s.scheduleExpiryLocked(vh, q)
	q.mu.Unlock()
	q.wake()

	for i := range overflow {
		dropped := overflow[i]
		s.dropPersistedMessage(vh, q, &dropped)
		s.deadLetterMessage(vh, q, &dropped, deathReasonMaxlen)
	}
	return nil
}

// dropPersistedMessage removes a message that left the queue without being
// acked (expired or maxlen-dropped) from persistent storage.
func (s *server) dropPersistedMessage(vh *vHost, q *queue, msg *message) {
	if s.persistenceManager == nil || msg.Properties.DeliveryMode != 2 || !q.Durable {
		return
	}
	if err := s.persistenceManager.DeleteMessage(vh.name, q.Name, GetMessageIdentifier(msg)); err != nil {
		s.Err("Failed to delete dead-lettered message from persistence (queue '%s'): %v", q.Name, err)
	}
}

// scheduleExpiryLocked (re)arms the queue's expiry timer for the earliest
// message deadline. Caller must hold q.mu. The timer goroutine is owned by
// the queue; it is disarmed by stopExpiryTimerLocked on queue deletion,
// vhost cleanup, and server shutdown, and every firing re-checks those
// states before touching the queue.
func (s *server) scheduleExpiryLocked(vh *vHost, q *queue) {
	var earliest time.Time
	for i := range q.Messages {
		exp := q.Messages[i].expiresAt
		if exp.IsZero() {
			continue
		}
		if earliest.IsZero() || exp.Before(earliest) {
			earliest = exp
		}
	}

	if earliest.IsZero() {
		if q.expiryTimer != nil {
			q.expiryTimer.Stop()
		}
		return
	}

	delay := time.Until(earliest)
	if delay < 0 {
		delay = 0
	}
	if q.expiryTimer == nil {
		q.expiryTimer = time.AfterFunc(delay, func() { s.expireMessages(vh, q) })
	} else {
		q.expiryTimer.Stop()
		q.expiryTimer.Reset(delay)
	}
}

// stopExpiryTimerLocked disarms the queue's expiry timer. Caller must hold
// q.mu (or otherwise have exclusive access, e.g. during shutdown).
func (q *queue) stopExpiryTimerLocked() {
	if q.expiryTimer != nil {
		q.expiryTimer.Stop()
		q.expiryTimer = nil
	}
}

// expireMessages removes every expired message from the queue (preserving
// order), dead-letters them with reason "expired", and re-arms the timer for
// the next deadline. Runs on the queue's expiry timer goroutine.
func (s *server) expireMessages(vh *vHost, q *queue) {
	if s.shuttingDown.Load() || vh.IsDeleting() || q.deleting.Load() {
		return
	}

	now := time.Now()
	var expired []message

	q.mu.Lock()
	if len(q.Messages) > 0 {
		kept := q.Messages[:0]
		for i := range q.Messages {
			if q.Messages[i].isExpired(now) {
				expired = append(expired, q.Messages[i])
			} else {
				kept = append(kept, q.Messages[i])
			}
		}
		// Zero the tail so dropped messages don't linger in the backing array.
		for i := len(kept); i < len(q.Messages); i++ {
			q.Messages[i] = message{}
		}
		q.Messages = kept
	}
	s.scheduleExpiryLocked(vh, q)
	q.mu.Unlock()

	for i := range expired {
		msg := expired[i]
		s.dropPersistedMessage(vh, q, &msg)
		s.deadLetterMessage(vh, q, &msg, deathReasonExpired)
	}
}

// takeExpiredHeadLocked checks whether the message at the head of the queue
// has expired; if so it removes and returns it. Caller must hold q.mu and,
// when a message is returned, must dead-letter it (and clean up persistence)
// after releasing the lock. Used by the delivery paths so consumers never
// receive an expired message the timer hasn't collected yet.
func takeExpiredHeadLocked(q *queue, now time.Time) (message, bool) {
	if len(q.Messages) == 0 || !q.Messages[0].isExpired(now) {
		return message{}, false
	}
	msg := q.Messages[0]
	q.Messages = q.Messages[1:]
	return msg, true
}

// deadLetterExpiredFromQueue disposes of a single expired message found by a
// delivery path: persistence cleanup plus dead-lettering with reason
// "expired". Caller must NOT hold q.mu.
func (s *server) deadLetterExpiredFromQueue(vh *vHost, q *queue, msg *message) {
	s.dropPersistedMessage(vh, q, msg)
	s.deadLetterMessage(vh, q, msg, deathReasonExpired)
}

// rearmExpiry re-arms the expiry timer after messages re-enter the queue
// outside the central enqueue path (reject/nack/recover requeues, tx-commit
// requeues, recovery from persistence). Safe to call without q.mu held.
func (s *server) rearmExpiry(vh *vHost, q *queue) {
	q.mu.Lock()
	s.scheduleExpiryLocked(vh, q)
	q.mu.Unlock()
}
