package carrotmq

// Golden tests for dead-lettering, per-queue/per-message TTL, and queue length
// limits — the RabbitMQ-compatible extension of the AMQP 0-9-1 model.
//
// SPECIFICATION (the tests below are the executable contract):
//
// Queue arguments (validated at queue.declare):
//   - "x-dead-letter-exchange" (string): exchange to republish dead messages to.
//   - "x-dead-letter-routing-key" (string): overrides the routing key used when
//     dead-lettering. When absent, the message's original routing key is used.
//   - "x-message-ttl" (integer >= 0, milliseconds): messages resident in the
//     queue longer than this are dead-lettered with reason "expired". Expiry
//     must happen server-side even when the queue has no consumers and the
//     publishing connection is gone.
//   - "x-max-length" (integer >= 0): max number of ready messages. When a new
//     message would exceed the limit, the message at the HEAD of the queue
//     (the oldest) is dropped, dead-lettered with reason "maxlen" if a DLX is
//     configured.
//   Per-message TTL: the basic.publish "expiration" property (string, ms)
//   expires an individual message; on dead-lettering due to per-message TTL,
//   the expiration property is REMOVED from the republished message and the
//   original value is recorded in x-death as "original-expiration".
//
// Validation rules:
//   - x-dead-letter-exchange / x-dead-letter-routing-key must be strings;
//     x-message-ttl / x-max-length must be non-negative integers (any AMQP
//     signed integer type). A violation fails queue.declare with a channel
//     error 406 (PRECONDITION_FAILED).
//   - Non-passive redeclare of an existing queue with different values for
//     these arguments fails with 406. Redeclare with identical arguments
//     succeeds. Passive declare does NOT check argument equivalence
//     (amqp091-go QueueInspect sends passive declare with no arguments and
//     must keep working).
//   - Durable queues persist their arguments: after a server restart the
//     recovered queue still enforces TTL/DLX and still rejects a conflicting
//     redeclare with 406.
//
// Dead-lettering triggers:
//   - basic.reject / basic.nack with requeue=false (reason "rejected"),
//     including nack multiple=true, and including nacks buffered in a
//     tx-mode channel (dead-lettering happens at tx.commit).
//   - per-queue or per-message TTL expiry (reason "expired").
//   - x-max-length overflow (reason "maxlen").
//   Without a DLX configured, dead messages are silently dropped (existing
//   behavior).
//
// Dead-lettered message contents:
//   - Body and all basic properties are preserved (content-type, custom
//     headers, delivery-mode, etc.), except "expiration" which is cleared.
//   - The message is republished to the DLX with the original routing key,
//     or the x-dead-letter-routing-key override if set. Republishing goes
//     through normal exchange routing (direct/fanout/topic). The redelivered
//     flag on the dead-letter delivery is false.
//
// x-death header (RabbitMQ-compatible):
//   - Headers gain "x-death": a field-array of field-tables, most recent
//     death first. Each entry has:
//       "count" (int64), "reason" (string), "queue" (string, queue the
//       message died in), "time" (AMQP timestamp), "exchange" (string,
//       exchange the message was published to), "routing-keys" (array of
//       strings — the message's routing key(s) at death),
//       and "original-expiration" (string) only for per-message-TTL deaths.
//   - If a message is dead-lettered again from the same queue with the same
//     reason, the existing entry's count is incremented (no duplicate entry).
//   - On first death, "x-first-death-reason", "x-first-death-queue" and
//     "x-first-death-exchange" headers are set; they are never modified by
//     subsequent deaths.
//   - Dead-letter cycles caused by explicit consumer rejection are allowed
//     (a queue may dead-letter back into itself via its DLX).

import (
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- helpers ----------

func dlxDial(t *testing.T, addr string) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Failed to dial test server")
	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	return conn, ch
}

// dlxTopology declares a direct DLX exchange and a DLQ bound to it with
// bindKey, and returns their names.
func dlxTopology(t *testing.T, ch *amqp.Channel, bindKey string) (dlxName, dlqName string) {
	t.Helper()
	dlxName = uniqueName("dlx-ex")
	dlqName = uniqueName("dlq")
	require.NoError(t, ch.ExchangeDeclare(dlxName, "direct", false, false, false, false, nil))
	_, err := ch.QueueDeclare(dlqName, false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(dlqName, bindKey, dlxName, false, nil))
	return dlxName, dlqName
}

func dlxWaitDelivery(t *testing.T, deliveries <-chan amqp.Delivery, timeout time.Duration, what string) amqp.Delivery {
	t.Helper()
	select {
	case d, ok := <-deliveries:
		require.True(t, ok, "Delivery channel closed while waiting for %s", what)
		return d
	case <-time.After(timeout):
		t.Fatalf("Timed out after %v waiting for %s", timeout, what)
		return amqp.Delivery{}
	}
}

func dlxExpectNoDelivery(t *testing.T, deliveries <-chan amqp.Delivery, wait time.Duration, what string) {
	t.Helper()
	select {
	case d, ok := <-deliveries:
		if ok {
			t.Fatalf("Unexpected delivery (%s): body=%q", what, d.Body)
		}
	case <-time.After(wait):
	}
}

func dlxAsInt64(t *testing.T, v any, what string) int64 {
	t.Helper()
	switch n := v.(type) {
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	default:
		t.Fatalf("%s: expected integer, got %T (%v)", what, v, v)
		return 0
	}
}

// dlxXDeath extracts and type-checks the x-death header array.
func dlxXDeath(t *testing.T, headers amqp.Table) []amqp.Table {
	t.Helper()
	raw, ok := headers["x-death"]
	require.True(t, ok, "x-death header missing; headers: %v", headers)
	arr, ok := raw.([]interface{})
	require.True(t, ok, "x-death should be a field array, got %T", raw)
	require.NotEmpty(t, arr, "x-death array should not be empty")
	entries := make([]amqp.Table, 0, len(arr))
	for i, e := range arr {
		tbl, ok := e.(amqp.Table)
		require.True(t, ok, "x-death[%d] should be a field table, got %T", i, e)
		entries = append(entries, tbl)
	}
	return entries
}

// dlxAssertDeathEntry checks the common fields of an x-death entry.
func dlxAssertDeathEntry(t *testing.T, entry amqp.Table, count int64, reason, queueName, exchangeName, routingKey string) {
	t.Helper()
	assert.Equal(t, count, dlxAsInt64(t, entry["count"], "x-death count"), "x-death count")
	assert.Equal(t, reason, entry["reason"], "x-death reason")
	assert.Equal(t, queueName, entry["queue"], "x-death queue")
	assert.Equal(t, exchangeName, entry["exchange"], "x-death exchange")

	deathTime, ok := entry["time"].(time.Time)
	require.True(t, ok, "x-death time should be an AMQP timestamp, got %T", entry["time"])
	assert.WithinDuration(t, time.Now(), deathTime, time.Minute, "x-death time should be recent")

	rks, ok := entry["routing-keys"].([]interface{})
	require.True(t, ok, "x-death routing-keys should be an array, got %T", entry["routing-keys"])
	require.Len(t, rks, 1, "x-death routing-keys")
	assert.Equal(t, routingKey, rks[0], "x-death routing-keys[0]")
}

func dlxAssertFirstDeath(t *testing.T, headers amqp.Table, reason, queueName, exchangeName string) {
	t.Helper()
	assert.Equal(t, reason, headers["x-first-death-reason"], "x-first-death-reason")
	assert.Equal(t, queueName, headers["x-first-death-queue"], "x-first-death-queue")
	assert.Equal(t, exchangeName, headers["x-first-death-exchange"], "x-first-death-exchange")
}

// ---------- tests ----------

// A rejected (requeue=false) message is republished to the queue's DLX with
// body/properties intact and correct x-death / x-first-death metadata.
func TestDLX_RejectRoutesToDeadLetterExchange(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()
	conn, ch := dlxDial(t, addr)
	defer conn.Close()

	routingKey := "work"
	dlxName, dlqName := dlxTopology(t, ch, routingKey)

	origEx := uniqueName("orig-ex")
	require.NoError(t, ch.ExchangeDeclare(origEx, "direct", false, false, false, false, nil))

	qName := uniqueName("primary")
	_, err := ch.QueueDeclare(qName, false, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	})
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(qName, routingKey, origEx, false, nil))

	dlqDeliveries, _ := t_consumeMessage(t, ch, dlqName, "dlq-consumer", true)

	err = ch.Publish(origEx, routingKey, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Headers:     amqp.Table{"app-header": "app-value"},
		Body:        []byte("payload-1"),
	})
	require.NoError(t, err)

	primaryDeliveries, _ := t_consumeMessage(t, ch, qName, "primary-consumer", false)
	d := dlxWaitDelivery(t, primaryDeliveries, 3*time.Second, "delivery on primary queue")
	require.NoError(t, d.Reject(false))

	dead := dlxWaitDelivery(t, dlqDeliveries, 5*time.Second, "dead-lettered message on DLQ")
	assert.Equal(t, []byte("payload-1"), dead.Body, "body must be preserved")
	assert.Equal(t, "text/plain", dead.ContentType, "content-type must be preserved")
	assert.Equal(t, "app-value", dead.Headers["app-header"], "custom headers must be preserved")
	assert.Equal(t, dlxName, dead.Exchange, "dead-letter delivery arrives via the DLX")
	assert.Equal(t, routingKey, dead.RoutingKey, "original routing key used when no override configured")
	assert.False(t, dead.Redelivered, "dead-letter delivery is a fresh publish, not a redelivery")

	deaths := dlxXDeath(t, dead.Headers)
	require.Len(t, deaths, 1, "exactly one x-death entry after first death")
	dlxAssertDeathEntry(t, deaths[0], 1, "rejected", qName, origEx, routingKey)
	dlxAssertFirstDeath(t, dead.Headers, "rejected", qName, origEx)
}

// basic.nack with multiple=true and requeue=false dead-letters every unacked
// message up to the delivery tag.
func TestDLX_NackMultipleDeadLettersAll(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()
	conn, ch := dlxDial(t, addr)
	defer conn.Close()

	routingKey := "bulk"
	dlxName, dlqName := dlxTopology(t, ch, routingKey)

	qName := uniqueName("primary")
	_, err := ch.QueueDeclare(qName, false, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	})
	require.NoError(t, err)

	dlqDeliveries, _ := t_consumeMessage(t, ch, dlqName, "dlq-consumer", true)

	// Bind primary to DLX? No — publish via default exchange to the queue.
	for i := 0; i < 3; i++ {
		err = ch.Publish("", qName, false, false, amqp.Publishing{Body: []byte(fmt.Sprintf("bulk-%d", i))})
		require.NoError(t, err)
	}
	// The default-exchange routing key is the queue name; rebind DLQ to match.
	require.NoError(t, ch.QueueBind(dlqName, qName, dlxName, false, nil))

	primaryDeliveries, _ := t_consumeMessage(t, ch, qName, "primary-consumer", false)
	var last amqp.Delivery
	for i := 0; i < 3; i++ {
		last = dlxWaitDelivery(t, primaryDeliveries, 3*time.Second, fmt.Sprintf("primary delivery %d", i))
	}
	require.NoError(t, last.Nack(true /* multiple */, false /* requeue */))

	got := map[string]bool{}
	for i := 0; i < 3; i++ {
		dead := dlxWaitDelivery(t, dlqDeliveries, 5*time.Second, fmt.Sprintf("dead-lettered message %d", i))
		got[string(dead.Body)] = true
		deaths := dlxXDeath(t, dead.Headers)
		require.Len(t, deaths, 1)
		dlxAssertDeathEntry(t, deaths[0], 1, "rejected", qName, "", qName)
	}
	assert.Equal(t, map[string]bool{"bulk-0": true, "bulk-1": true, "bulk-2": true}, got,
		"all nacked messages must be dead-lettered")
}

// x-dead-letter-routing-key overrides the routing key used for republishing,
// while x-death records the original routing key.
func TestDLX_DeadLetterRoutingKeyOverride(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()
	conn, ch := dlxDial(t, addr)
	defer conn.Close()

	dlxName, dlqName := dlxTopology(t, ch, "dead-key")

	qName := uniqueName("primary")
	_, err := ch.QueueDeclare(qName, false, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    dlxName,
		"x-dead-letter-routing-key": "dead-key",
	})
	require.NoError(t, err)

	dlqDeliveries, _ := t_consumeMessage(t, ch, dlqName, "dlq-consumer", true)

	require.NoError(t, ch.Publish("", qName, false, false, amqp.Publishing{Body: []byte("override-me")}))

	primaryDeliveries, _ := t_consumeMessage(t, ch, qName, "primary-consumer", false)
	d := dlxWaitDelivery(t, primaryDeliveries, 3*time.Second, "delivery on primary queue")
	require.NoError(t, d.Reject(false))

	dead := dlxWaitDelivery(t, dlqDeliveries, 5*time.Second, "dead-lettered message on DLQ")
	assert.Equal(t, "dead-key", dead.RoutingKey, "x-dead-letter-routing-key must override the routing key")

	deaths := dlxXDeath(t, dead.Headers)
	require.Len(t, deaths, 1)
	// routing-keys records the ORIGINAL key (default exchange => queue name).
	dlxAssertDeathEntry(t, deaths[0], 1, "rejected", qName, "", qName)
}

// Per-queue x-message-ttl expires messages server-side — with no consumer on
// the queue and the publishing connection closed — and dead-letters them in
// order with reason "expired".
func TestDLX_MessageTTLExpiration(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Topology + publisher on a connection we will close before expiry.
	pubConn, pubCh := dlxDial(t, addr)
	routingKey := "ttl"
	dlxName, dlqName := dlxTopology(t, pubCh, routingKey)

	origEx := uniqueName("orig-ex")
	require.NoError(t, pubCh.ExchangeDeclare(origEx, "direct", false, false, false, false, nil))

	qName := uniqueName("primary")
	_, err := pubCh.QueueDeclare(qName, false, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
		"x-message-ttl":          int32(300),
	})
	require.NoError(t, err)
	require.NoError(t, pubCh.QueueBind(qName, routingKey, origEx, false, nil))

	for i := 0; i < 2; i++ {
		require.NoError(t, pubCh.Publish(origEx, routingKey, false, false,
			amqp.Publishing{Body: []byte(fmt.Sprintf("ttl-%d", i))}))
	}
	// Close the publishing connection: expiry must be driven by the server.
	require.NoError(t, pubConn.Close())

	subConn, subCh := dlxDial(t, addr)
	defer subConn.Close()
	dlqDeliveries, _ := t_consumeMessage(t, subCh, dlqName, "dlq-consumer", true)

	first := dlxWaitDelivery(t, dlqDeliveries, 5*time.Second, "first expired message")
	second := dlxWaitDelivery(t, dlqDeliveries, 5*time.Second, "second expired message")
	assert.Equal(t, "ttl-0", string(first.Body), "expired messages arrive in queue order")
	assert.Equal(t, "ttl-1", string(second.Body), "expired messages arrive in queue order")

	for _, dead := range []amqp.Delivery{first, second} {
		deaths := dlxXDeath(t, dead.Headers)
		require.Len(t, deaths, 1)
		dlxAssertDeathEntry(t, deaths[0], 1, "expired", qName, origEx, routingKey)
		dlxAssertFirstDeath(t, dead.Headers, "expired", qName, origEx)
	}

	// The primary queue must now be empty.
	q, err := subCh.QueueInspect(qName)
	require.NoError(t, err, "passive declare (QueueInspect) must not check argument equivalence")
	assert.Equal(t, 0, q.Messages, "expired messages must be removed from the primary queue")
}

// The per-message expiration property expires an individual message; the
// republished message has its expiration cleared and records
// original-expiration in x-death. A message without expiration stays queued.
func TestDLX_PerMessageExpiration(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()
	conn, ch := dlxDial(t, addr)
	defer conn.Close()

	dlxName, dlqName := dlxTopology(t, ch, "ignored")

	qName := uniqueName("primary")
	_, err := ch.QueueDeclare(qName, false, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	})
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(dlqName, qName, dlxName, false, nil))

	dlqDeliveries, _ := t_consumeMessage(t, ch, dlqName, "dlq-consumer", true)

	require.NoError(t, ch.Publish("", qName, false, false, amqp.Publishing{
		Body:       []byte("short-lived"),
		Expiration: "200",
	}))
	require.NoError(t, ch.Publish("", qName, false, false, amqp.Publishing{
		Body: []byte("immortal"),
	}))

	dead := dlxWaitDelivery(t, dlqDeliveries, 5*time.Second, "expired message on DLQ")
	assert.Equal(t, "short-lived", string(dead.Body))
	assert.Equal(t, "", dead.Expiration, "expiration property must be cleared on TTL dead-lettering")

	deaths := dlxXDeath(t, dead.Headers)
	require.Len(t, deaths, 1)
	dlxAssertDeathEntry(t, deaths[0], 1, "expired", qName, "", qName)
	assert.Equal(t, "200", deaths[0]["original-expiration"], "x-death original-expiration")

	// The message without expiration must still be queued.
	dlxExpectNoDelivery(t, dlqDeliveries, 500*time.Millisecond, "immortal message must not be dead-lettered")
	q, err := ch.QueueInspect(qName)
	require.NoError(t, err)
	assert.Equal(t, 1, q.Messages, "non-expiring message stays in the queue")
}

// x-max-length drops from the HEAD (oldest first) when the limit is exceeded,
// dead-lettering dropped messages with reason "maxlen".
func TestDLX_MaxLengthDropHead(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()
	conn, ch := dlxDial(t, addr)
	defer conn.Close()

	require.NoError(t, ch.Confirm(false))
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 8))

	dlxName, dlqName := dlxTopology(t, ch, "ignored")

	qName := uniqueName("primary")
	_, err := ch.QueueDeclare(qName, false, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
		"x-max-length":           int32(3),
	})
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(dlqName, qName, dlxName, false, nil))

	dlqDeliveries, _ := t_consumeMessage(t, ch, dlqName, "dlq-consumer", true)

	for i := 0; i < 5; i++ {
		require.NoError(t, ch.Publish("", qName, false, false, amqp.Publishing{Body: []byte(fmt.Sprintf("m-%d", i))}))
		conf := <-confirms
		require.True(t, conf.Ack, "publish %d should be confirmed", i)
	}

	// Oldest two messages must be dead-lettered with reason "maxlen", in order.
	for i := 0; i < 2; i++ {
		dead := dlxWaitDelivery(t, dlqDeliveries, 5*time.Second, fmt.Sprintf("maxlen-dropped message %d", i))
		assert.Equal(t, fmt.Sprintf("m-%d", i), string(dead.Body), "head (oldest) messages are dropped first")
		deaths := dlxXDeath(t, dead.Headers)
		require.Len(t, deaths, 1)
		dlxAssertDeathEntry(t, deaths[0], 1, "maxlen", qName, "", qName)
	}

	q, err := ch.QueueInspect(qName)
	require.NoError(t, err)
	require.Equal(t, 3, q.Messages, "queue must hold exactly x-max-length messages")

	primaryDeliveries, _ := t_consumeMessage(t, ch, qName, "primary-consumer", true)
	for i := 2; i < 5; i++ {
		d := dlxWaitDelivery(t, primaryDeliveries, 3*time.Second, "remaining message")
		assert.Equal(t, fmt.Sprintf("m-%d", i), string(d.Body), "newest messages are retained in order")
	}
}

// Repeated rejection through a dead-letter cycle increments the count of the
// existing x-death entry instead of appending duplicates, and leaves the
// x-first-death-* headers untouched.
func TestDLX_XDeathCountIncrementsOnRepeatedRejection(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()
	conn, ch := dlxDial(t, addr)
	defer conn.Close()

	routingKey := "loop"
	dlxName := uniqueName("dlx-ex")
	require.NoError(t, ch.ExchangeDeclare(dlxName, "direct", false, false, false, false, nil))

	// The queue dead-letters into the same exchange+key it is bound to:
	// rejected messages cycle straight back into the queue.
	qName := uniqueName("cycle")
	_, err := ch.QueueDeclare(qName, false, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	})
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(qName, routingKey, dlxName, false, nil))

	require.NoError(t, ch.Publish(dlxName, routingKey, false, false, amqp.Publishing{Body: []byte("boomerang")}))

	deliveries, _ := t_consumeMessage(t, ch, qName, "cycle-consumer", false)

	// First delivery: no deaths yet.
	d := dlxWaitDelivery(t, deliveries, 3*time.Second, "initial delivery")
	assert.Nil(t, d.Headers["x-death"], "no x-death before first rejection")
	require.NoError(t, d.Reject(false))

	// Second delivery: count 1.
	d = dlxWaitDelivery(t, deliveries, 5*time.Second, "delivery after first rejection")
	deaths := dlxXDeath(t, d.Headers)
	require.Len(t, deaths, 1, "single x-death entry after first rejection")
	dlxAssertDeathEntry(t, deaths[0], 1, "rejected", qName, dlxName, routingKey)
	dlxAssertFirstDeath(t, d.Headers, "rejected", qName, dlxName)
	require.NoError(t, d.Reject(false))

	// Third delivery: same single entry, count incremented to 2.
	d = dlxWaitDelivery(t, deliveries, 5*time.Second, "delivery after second rejection")
	deaths = dlxXDeath(t, d.Headers)
	require.Len(t, deaths, 1, "repeated death from the same queue+reason must not append a new entry")
	dlxAssertDeathEntry(t, deaths[0], 2, "rejected", qName, dlxName, routingKey)
	dlxAssertFirstDeath(t, d.Headers, "rejected", qName, dlxName)
	require.NoError(t, d.Ack(false))
}

// A nack with requeue=false issued on a tx-mode channel is buffered and the
// message is dead-lettered only when the transaction commits.
func TestDLX_TxCommitNackDeadLetters(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()
	conn, ch := dlxDial(t, addr)
	defer conn.Close()

	dlxName, dlqName := dlxTopology(t, ch, "ignored")

	qName := uniqueName("primary")
	_, err := ch.QueueDeclare(qName, false, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	})
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(dlqName, qName, dlxName, false, nil))

	require.NoError(t, ch.Publish("", qName, false, false, amqp.Publishing{Body: []byte("tx-victim")}))

	// Separate channel for consuming the DLQ (tx channel is for the nack).
	dlqCh, err := conn.Channel()
	require.NoError(t, err)
	dlqDeliveries, _ := t_consumeMessage(t, dlqCh, dlqName, "dlq-consumer", true)

	txConn, txCh := dlxDial(t, addr)
	defer txConn.Close()
	require.NoError(t, txCh.Tx())

	primaryDeliveries, _ := t_consumeMessage(t, txCh, qName, "tx-consumer", false)
	d := dlxWaitDelivery(t, primaryDeliveries, 3*time.Second, "delivery on tx channel")
	require.NoError(t, d.Nack(false, false))

	dlxExpectNoDelivery(t, dlqDeliveries, 400*time.Millisecond, "nack must not dead-letter before tx.commit")

	require.NoError(t, txCh.TxCommit())

	dead := dlxWaitDelivery(t, dlqDeliveries, 5*time.Second, "dead-lettered message after tx.commit")
	assert.Equal(t, "tx-victim", string(dead.Body))
	deaths := dlxXDeath(t, dead.Headers)
	require.Len(t, deaths, 1)
	dlxAssertDeathEntry(t, deaths[0], 1, "rejected", qName, "", qName)
}

// Malformed dead-letter/TTL/length arguments fail queue.declare with 406.
func TestDLX_InvalidArgumentTypes(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()
	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err)
	defer conn.Close()

	cases := []struct {
		name string
		args amqp.Table
	}{
		{"ttl-not-integer", amqp.Table{"x-message-ttl": "soon"}},
		{"ttl-negative", amqp.Table{"x-message-ttl": int32(-5)}},
		{"max-length-negative", amqp.Table{"x-max-length": int32(-1)}},
		{"dlx-not-string", amqp.Table{"x-dead-letter-exchange": int32(7)}},
		{"dl-routing-key-not-string", amqp.Table{"x-dead-letter-routing-key": int32(7)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ch, err := conn.Channel()
			require.NoError(t, err)
			_, err = ch.QueueDeclare(uniqueName("bad-args"), false, false, false, false, tc.args)
			require.Error(t, err, "queue.declare with invalid %s must fail", tc.name)
			amqpErr, ok := err.(*amqp.Error)
			require.True(t, ok, "expected *amqp.Error, got %T: %v", err, err)
			assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "expected 406 PRECONDITION_FAILED")
		})
	}
}

// Redeclaring an existing queue with different dead-letter/TTL arguments is a
// 406; redeclaring with identical arguments succeeds.
func TestDLX_ArgumentEquivalenceOnRedeclare(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()
	conn, ch := dlxDial(t, addr)
	defer conn.Close()

	qName := uniqueName("equiv")
	args := amqp.Table{"x-message-ttl": int32(60000), "x-dead-letter-exchange": "some-dlx"}
	_, err := ch.QueueDeclare(qName, false, false, false, false, args)
	require.NoError(t, err)

	// Identical arguments: fine, same channel stays open.
	_, err = ch.QueueDeclare(qName, false, false, false, false, args)
	require.NoError(t, err, "redeclare with identical arguments must succeed")

	// Different TTL: 406.
	_, err = ch.QueueDeclare(qName, false, false, false, false, amqp.Table{
		"x-message-ttl": int32(1000), "x-dead-letter-exchange": "some-dlx",
	})
	require.Error(t, err, "redeclare with different x-message-ttl must fail")
	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "expected *amqp.Error, got %T: %v", err, err)
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code)

	// Missing the arguments entirely on a fresh channel: also 406.
	ch2, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare(qName, false, false, false, false, nil)
	require.Error(t, err, "redeclare without the original arguments must fail")
	amqpErr, ok = err.(*amqp.Error)
	require.True(t, ok, "expected *amqp.Error, got %T: %v", err, err)
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code)
}

// Durable queue arguments survive a server restart: the recovered queue still
// enforces TTL + DLX and still rejects conflicting redeclares.
func TestDLX_QueueArgumentsPersistAcrossRestart(t *testing.T) {
	dbPath := t.TempDir() + "/dlx-restart.db"

	qName := uniqueName("durable-ttl")
	dlxName := uniqueName("durable-dlx")
	dlqName := uniqueName("durable-dlq")
	args := amqp.Table{
		"x-dead-letter-exchange": dlxName,
		"x-message-ttl":          int32(300),
	}

	// First server: declare durable topology.
	addr1, cleanup1 := setupTestServer(t, WithBuntDBStorage(dbPath))
	conn1, ch1 := dlxDial(t, addr1)
	require.NoError(t, ch1.ExchangeDeclare(dlxName, "direct", true, false, false, false, nil))
	_, err := ch1.QueueDeclare(dlqName, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch1.QueueBind(dlqName, qName, dlxName, false, nil))
	_, err = ch1.QueueDeclare(qName, true, false, false, false, args)
	require.NoError(t, err)
	conn1.Close()
	cleanup1()

	// Second server on the same storage.
	addr2, cleanup2 := setupTestServer(t, WithBuntDBStorage(dbPath))
	defer cleanup2()
	conn2, ch2 := dlxDial(t, addr2)
	defer conn2.Close()

	// (a) Identical redeclare succeeds.
	_, err = ch2.QueueDeclare(qName, true, false, false, false, args)
	require.NoError(t, err, "recovered queue must accept an identical redeclare")

	// (b) Conflicting redeclare fails with 406 (fresh channel; failure closes it).
	chConflict, err := conn2.Channel()
	require.NoError(t, err)
	_, err = chConflict.QueueDeclare(qName, true, false, false, false, nil)
	require.Error(t, err, "recovered queue must reject redeclare without arguments")
	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "expected *amqp.Error, got %T: %v", err, err)
	assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code)

	// (c) TTL + DLX still enforced after recovery.
	dlqDeliveries, _ := t_consumeMessage(t, ch2, dlqName, "dlq-consumer", true)
	require.NoError(t, ch2.Publish("", qName, false, false, amqp.Publishing{
		Body:         []byte("post-restart"),
		DeliveryMode: amqp.Persistent,
	}))
	dead := dlxWaitDelivery(t, dlqDeliveries, 5*time.Second, "expired message after restart")
	assert.Equal(t, "post-restart", string(dead.Body))
	deaths := dlxXDeath(t, dead.Headers)
	require.Len(t, deaths, 1)
	dlxAssertDeathEntry(t, deaths[0], 1, "expired", qName, "", qName)
}

// Without a DLX, reject(requeue=false) still just drops the message —
// regression guard for existing behavior.
func TestDLX_NoDLXConfiguredRejectDrops(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()
	conn, ch := dlxDial(t, addr)
	defer conn.Close()

	qName := uniqueName("plain")
	_, err := ch.QueueDeclare(qName, false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Publish("", qName, false, false, amqp.Publishing{Body: []byte("doomed")}))

	deliveries, _ := t_consumeMessage(t, ch, qName, "consumer", false)
	d := dlxWaitDelivery(t, deliveries, 3*time.Second, "delivery")
	require.NoError(t, d.Reject(false))

	dlxExpectNoDelivery(t, deliveries, 400*time.Millisecond, "rejected message must not be redelivered")
	q, err := ch.QueueInspect(qName)
	require.NoError(t, err)
	assert.Equal(t, 0, q.Messages, "rejected message is dropped when no DLX is configured")
}
