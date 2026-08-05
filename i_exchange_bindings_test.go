// Integration tests for exchange-to-exchange bindings (exchange.bind / exchange.unbind),
// the "alternate-exchange" declare argument, and enforcement of the `internal` exchange
// flag. These tests are the executable specification for that feature; the semantics they
// assert are spelled out in full below.
//
// WIRE FORMAT
//
//	Class 40 (exchange) gains four methods, matching amqp091-go v1.10.0 spec091.go:
//	    bind = 30, bind-ok = 31, unbind = 40, unbind-ok = 51   (unbind-ok is 51, not 41)
//	Field order for both bind and unbind:
//	    reserved-1 (short), destination (shortstr), source (shortstr),
//	    routing-key (shortstr), no-wait (bit), arguments (table)
//	The server must interoperate with the amqp091-go client calls
//	    ExchangeBind(destination, key, source, noWait, args)
//	    ExchangeUnbind(destination, key, source, noWait, args)
//
// EXCHANGE.BIND
//
//	Source or destination exchange missing            -> channel close 404 NOT_FOUND.
//	Default exchange ("") as source or destination    -> channel close 403 ACCESS_REFUSED.
//	Duplicate bind (same source/destination/key)      -> idempotent no-op, still Bind-Ok.
//	no-wait = true                                    -> no Bind-Ok frame is sent.
//	The binding is persisted iff BOTH exchanges are durable, and is recovered on restart.
//
// EXCHANGE.UNBIND
//
//	Either exchange missing                           -> channel close 404 NOT_FOUND.
//	Unbind of a binding that never existed            -> silent success, Unbind-Ok.
//	Persistence is updated when the binding was persisted.
//
// ROUTING
//
//	Publishing to exchange X traverses the binding graph. At every exchange visited, that
//	exchange matches its OWN bindings using its OWN type (direct = exact routing key,
//	topic = pattern match, fanout = every binding) across BOTH its queue bindings and its
//	exchange bindings. The message's ORIGINAL routing key is used at every hop -- it is
//	never rewritten by an intermediate binding key.
//	Traversal is recursive. Each exchange is processed AT MOST ONCE per publish, so cycles
//	terminate. The resulting queue set is deduplicated: every matched queue receives
//	exactly one copy of the message no matter how many paths reach it.
//
// ALTERNATE EXCHANGE
//
//	Configured through the exchange.declare arguments table under the key
//	"alternate-exchange" (a string).
//	  - A non-string value                            -> channel close 406 PRECONDITION_FAILED.
//	  - Redeclaring an exchange with a DIFFERENT alternate-exchange value
//	                                                  -> channel close 406 PRECONDITION_FAILED.
//	  - Redeclaring with the identical value          -> OK. (Declare equivalence for other,
//	    unknown arguments is unchanged, i.e. still unchecked.)
//	  - Durable exchanges persist and recover their alternate-exchange.
//	Consultation rule: when an exchange's own bindings -- queue bindings and exchange
//	bindings combined -- yield zero targets for a message, and that exchange has an
//	alternate-exchange, the message is forwarded to the alternate exchange with the SAME
//	routing key. Alternate-exchange hops obey the same at-most-once-per-exchange traversal
//	rule, so alternate-exchange cycles terminate. A missing or nonexistent alternate
//	exchange simply leaves the message unroutable.
//	A message that reaches a queue only by way of an alternate exchange COUNTS AS ROUTED:
//	no basic.return is sent, and in confirm mode the publisher receives an ack.
//
// UNROUTABLE MESSAGES
//
//	Once traversal (including alternate-exchange forwarding) yields no queue, carrot-mq's
//	existing observable behaviour is preserved: a mandatory message is returned via
//	basic.return, and a channel in confirm mode receives a nack. (This deliberately differs
//	from RabbitMQ, which returns AND acks.)
//
// INTERNAL FLAG
//
//	basic.publish addressed directly at an exchange declared internal = true
//	                                                  -> channel close 403 ACCESS_REFUSED.
//	Internal exchanges remain legal as an exchange.bind source, an exchange.bind
//	destination, an alternate-exchange target, and a queue.bind target.
//
// EXCHANGE.DELETE
//
//	Deleting an exchange removes ALL exchange-to-exchange bindings in which it appears as
//	source OR as destination (as well as its queue bindings, as before).
//	With if-unused = true an exchange counts as "in use" iff it has at least one binding in
//	which it is the SOURCE -- to a queue or to another exchange -- in which case the server
//	replies 406 PRECONDITION_FAILED. Inbound exchange-to-exchange edges (the exchange as
//	destination) never block deletion; they are removed silently.
package carrotmq

import (
	"path/filepath"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// t_ebWait is the generous timeout used when a message IS expected.
	t_ebWait = 3 * time.Second
	// t_ebQuiet is how long we watch an idle channel to prove nothing else arrives.
	t_ebQuiet = 400 * time.Millisecond
)

// t_ebDial opens a connection to the test server and registers its teardown.
func t_ebDial(t *testing.T, addr string) *amqp.Connection {
	t.Helper()
	conn, err := amqp.Dial("amqp://" + addr)
	require.NoError(t, err, "Failed to dial test server")
	t.Cleanup(func() { conn.Close() })
	return conn
}

// t_ebChannel opens a fresh channel. Tests that expect a channel-level exception need one
// channel per case, since a closed channel cannot be reused.
func t_ebChannel(t *testing.T, conn *amqp.Connection) *amqp.Channel {
	t.Helper()
	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	return ch
}

// t_ebConsumeBound declares a transient queue, binds it to exchange/key and starts an
// auto-ack consumer on it. Returns the queue name and the delivery channel.
func t_ebConsumeBound(t *testing.T, ch *amqp.Channel, exchange, key, prefix string) (string, <-chan amqp.Delivery) {
	t.Helper()
	name := uniqueName(prefix)
	_, err := ch.QueueDeclare(name, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue %s", name)
	require.NoError(t, ch.QueueBind(name, key, exchange, false, nil),
		"Failed to bind queue %s to exchange %s with key %q", name, exchange, key)
	deliveries, err := ch.Consume(name, uniqueName("ctag"), true, false, false, false, nil)
	require.NoError(t, err, "Failed to consume from queue %s", name)
	return name, deliveries
}

// t_ebExpectExactlyOne asserts that exactly one message with the given body arrives and
// that nothing follows it. Delivery is asserted per queue as a count, never as an ordering
// relative to another queue.
func t_ebExpectExactlyOne(t *testing.T, deliveries <-chan amqp.Delivery, body string) {
	t.Helper()
	t_expectMessage(t, deliveries, body, nil, t_ebWait)
	t_expectNoMessage(t, deliveries, t_ebQuiet)
}

// -----------------------------------------------------------------------------
// 1. A direct source bound to a fanout destination delivers a single copy.
// -----------------------------------------------------------------------------

func TestExchangeBindings_DirectToFanoutChain(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)
	ch := t_ebChannel(t, conn)
	defer ch.Close()

	source := uniqueName("eb-src-direct")
	dest := uniqueName("eb-dst-fanout")
	require.NoError(t, ch.ExchangeDeclare(source, "direct", false, false, false, false, nil))
	require.NoError(t, ch.ExchangeDeclare(dest, "fanout", false, false, false, false, nil))

	_, deliveries := t_ebConsumeBound(t, ch, dest, "", "eb-q-dest")

	require.NoError(t, ch.ExchangeBind(dest, "chain-key", source, false, nil),
		"exchange.bind should succeed for two existing exchanges")

	body := "direct-to-fanout"
	t_publishMessage(t, ch, source, "chain-key", body, false, amqp.Publishing{})

	t_ebExpectExactlyOne(t, deliveries, body)

	// A routing key that the direct source does not match must not reach the destination.
	t_publishMessage(t, ch, source, "other-key", "must-not-arrive", false, amqp.Publishing{})
	t_expectNoMessage(t, deliveries, t_ebQuiet)
}

// -----------------------------------------------------------------------------
// 2. Each hop applies its own exchange type, against the original routing key.
// -----------------------------------------------------------------------------

func TestExchangeBindings_PerHopTypeSemantics(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)
	ch := t_ebChannel(t, conn)
	defer ch.Close()

	source := uniqueName("eb-src-topic")
	dest := uniqueName("eb-dst-direct")
	require.NoError(t, ch.ExchangeDeclare(source, "topic", false, false, false, false, nil))
	require.NoError(t, ch.ExchangeDeclare(dest, "direct", false, false, false, false, nil))

	// The destination is direct: only the exact key "logs.error" reaches the queue.
	_, deliveries := t_ebConsumeBound(t, ch, dest, "logs.error", "eb-q-logs")

	// The source is topic: the e2e binding key is a pattern.
	require.NoError(t, ch.ExchangeBind(dest, "logs.*", source, false, nil))

	// "logs.error" matches the topic pattern at hop 1 and the exact key at hop 2.
	body := "matched-at-both-hops"
	t_publishMessage(t, ch, source, "logs.error", body, false, amqp.Publishing{})
	t_ebExpectExactlyOne(t, deliveries, body)

	// "logs.warn" matches the topic pattern at hop 1, but the ORIGINAL routing key is
	// carried to hop 2 where the direct destination has no "logs.warn" binding.
	t_publishMessage(t, ch, source, "logs.warn", "dropped-at-hop-2", false, amqp.Publishing{})
	t_expectNoMessage(t, deliveries, t_ebQuiet)

	// "audit.error" does not even match at hop 1.
	t_publishMessage(t, ch, source, "audit.error", "dropped-at-hop-1", false, amqp.Publishing{})
	t_expectNoMessage(t, deliveries, t_ebQuiet)
}

// -----------------------------------------------------------------------------
// 3. A two-exchange cycle terminates and delivers one copy to each queue.
// -----------------------------------------------------------------------------

func TestExchangeBindings_CycleTerminates(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)
	ch := t_ebChannel(t, conn)
	defer ch.Close()

	exA := uniqueName("eb-cycle-a")
	exB := uniqueName("eb-cycle-b")
	require.NoError(t, ch.ExchangeDeclare(exA, "fanout", false, false, false, false, nil))
	require.NoError(t, ch.ExchangeDeclare(exB, "fanout", false, false, false, false, nil))

	_, deliveriesA := t_ebConsumeBound(t, ch, exA, "", "eb-q-cycle-a")
	_, deliveriesB := t_ebConsumeBound(t, ch, exB, "", "eb-q-cycle-b")

	require.NoError(t, ch.ExchangeBind(exB, "", exA, false, nil))
	require.NoError(t, ch.ExchangeBind(exA, "", exB, false, nil))

	body := "cycle-payload"
	t_publishMessage(t, ch, exA, "anything", body, false, amqp.Publishing{})

	// Each queue gets exactly one copy; the traversal must not loop forever.
	t_ebExpectExactlyOne(t, deliveriesA, body)
	t_ebExpectExactlyOne(t, deliveriesB, body)
}

// -----------------------------------------------------------------------------
// 4. A diamond delivers exactly one copy to the queue reachable by two paths.
// -----------------------------------------------------------------------------

func TestExchangeBindings_DiamondDeduplicates(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)
	ch := t_ebChannel(t, conn)
	defer ch.Close()

	exA := uniqueName("eb-diamond-a")
	exB := uniqueName("eb-diamond-b")
	exC := uniqueName("eb-diamond-c")
	exD := uniqueName("eb-diamond-d")
	for _, name := range []string{exA, exB, exC, exD} {
		require.NoError(t, ch.ExchangeDeclare(name, "fanout", false, false, false, false, nil))
	}

	// A -> B, A -> C, B -> D, C -> D
	require.NoError(t, ch.ExchangeBind(exB, "", exA, false, nil))
	require.NoError(t, ch.ExchangeBind(exC, "", exA, false, nil))
	require.NoError(t, ch.ExchangeBind(exD, "", exB, false, nil))
	require.NoError(t, ch.ExchangeBind(exD, "", exC, false, nil))

	queueName, deliveries := t_ebConsumeBound(t, ch, exD, "", "eb-q-diamond")

	// The same queue is ALSO reachable directly from B, so it is matched by two
	// independent bindings as well as by two graph paths.
	require.NoError(t, ch.QueueBind(queueName, "", exB, false, nil))

	body := "diamond-payload"
	t_publishMessage(t, ch, exA, "rk", body, false, amqp.Publishing{})

	t_ebExpectExactlyOne(t, deliveries, body)
}

// -----------------------------------------------------------------------------
// 5. Unbind severs routing; unbinding a pair that was never bound is not an error.
// -----------------------------------------------------------------------------

func TestExchangeBindings_UnbindSeversRouting(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)
	ch := t_ebChannel(t, conn)
	defer ch.Close()

	source := uniqueName("eb-unbind-src")
	dest := uniqueName("eb-unbind-dst")
	require.NoError(t, ch.ExchangeDeclare(source, "direct", false, false, false, false, nil))
	require.NoError(t, ch.ExchangeDeclare(dest, "fanout", false, false, false, false, nil))

	_, deliveries := t_ebConsumeBound(t, ch, dest, "", "eb-q-unbind")

	require.NoError(t, ch.ExchangeBind(dest, "k", source, false, nil))
	body := "before-unbind"
	t_publishMessage(t, ch, source, "k", body, false, amqp.Publishing{})
	t_ebExpectExactlyOne(t, deliveries, body)

	require.NoError(t, ch.ExchangeUnbind(dest, "k", source, false, nil),
		"exchange.unbind of an existing binding should succeed")

	t_publishMessage(t, ch, source, "k", "after-unbind", false, amqp.Publishing{})
	t_expectNoMessage(t, deliveries, t_ebQuiet)

	// Unbinding again, and unbinding a pair that never had a binding, are both silent
	// successes -- and leave the channel usable.
	assert.NoError(t, ch.ExchangeUnbind(dest, "k", source, false, nil),
		"repeat exchange.unbind should be a silent no-op")
	assert.NoError(t, ch.ExchangeUnbind(dest, "never-bound-key", source, false, nil),
		"exchange.unbind of a non-existent binding should be a silent no-op")
	t_expectNoChannelClose(t, ch, t_ebQuiet)
}

// -----------------------------------------------------------------------------
// 6. Bind error cases. Each uses a fresh channel because each closes the channel.
// -----------------------------------------------------------------------------

func TestExchangeBindings_BindErrors(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)

	setup := t_ebChannel(t, conn)
	existing := uniqueName("eb-err-existing")
	require.NoError(t, setup.ExchangeDeclare(existing, "direct", false, false, false, false, nil))
	require.NoError(t, setup.Close())

	missing := uniqueName("eb-err-missing")

	t.Run("MissingSource", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		err := ch.ExchangeBind(existing, "k", missing, false, nil)
		require.Error(t, err, "binding from a nonexistent source must fail")
		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "expected an AMQP error, got %T", err)
		assert.Equal(t, amqp.NotFound, amqpErr.Code, "missing source must be 404 NOT_FOUND")
	})

	t.Run("MissingDestination", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		err := ch.ExchangeBind(missing, "k", existing, false, nil)
		require.Error(t, err, "binding to a nonexistent destination must fail")
		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "expected an AMQP error, got %T", err)
		assert.Equal(t, amqp.NotFound, amqpErr.Code, "missing destination must be 404 NOT_FOUND")
	})

	t.Run("DefaultExchangeAsSource", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		err := ch.ExchangeBind(existing, "k", "", false, nil)
		require.Error(t, err, "the default exchange may not be a binding source")
		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "expected an AMQP error, got %T", err)
		assert.Equal(t, amqp.AccessRefused, amqpErr.Code, "default exchange as source must be 403 ACCESS_REFUSED")
	})

	t.Run("DefaultExchangeAsDestination", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		err := ch.ExchangeBind("", "k", existing, false, nil)
		require.Error(t, err, "the default exchange may not be a binding destination")
		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "expected an AMQP error, got %T", err)
		assert.Equal(t, amqp.AccessRefused, amqpErr.Code, "default exchange as destination must be 403 ACCESS_REFUSED")
	})

	t.Run("UnbindMissingSource", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		err := ch.ExchangeUnbind(existing, "k", missing, false, nil)
		require.Error(t, err, "unbinding from a nonexistent source must fail")
		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "expected an AMQP error, got %T", err)
		assert.Equal(t, amqp.NotFound, amqpErr.Code, "missing source must be 404 NOT_FOUND")
	})

	t.Run("UnbindMissingDestination", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		err := ch.ExchangeUnbind(missing, "k", existing, false, nil)
		require.Error(t, err, "unbinding a nonexistent destination must fail")
		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "expected an AMQP error, got %T", err)
		assert.Equal(t, amqp.NotFound, amqpErr.Code, "missing destination must be 404 NOT_FOUND")
	})
}

// -----------------------------------------------------------------------------
// 7. Duplicate binds are idempotent; no-wait suppresses the Bind-Ok frame.
// -----------------------------------------------------------------------------

func TestExchangeBindings_DuplicateBindIdempotent(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)
	ch := t_ebChannel(t, conn)
	defer ch.Close()

	source := uniqueName("eb-dup-src")
	dest := uniqueName("eb-dup-dst")
	require.NoError(t, ch.ExchangeDeclare(source, "direct", false, false, false, false, nil))
	require.NoError(t, ch.ExchangeDeclare(dest, "fanout", false, false, false, false, nil))

	_, deliveries := t_ebConsumeBound(t, ch, dest, "", "eb-q-dup")

	require.NoError(t, ch.ExchangeBind(dest, "dup", source, false, nil))
	require.NoError(t, ch.ExchangeBind(dest, "dup", source, false, nil),
		"a duplicate exchange.bind must still answer bind-ok")

	// no-wait must NOT produce a bind-ok frame. If the server sent one anyway, the
	// following synchronous call would consume the stray frame and misbehave.
	require.NoError(t, ch.ExchangeBind(dest, "dup", source, true, nil))
	require.NoError(t, ch.ExchangeDeclarePassive(dest, "fanout", false, false, false, false, nil),
		"channel must stay in sync after a no-wait exchange.bind")

	body := "single-copy"
	t_publishMessage(t, ch, source, "dup", body, false, amqp.Publishing{})
	t_ebExpectExactlyOne(t, deliveries, body)
}

// -----------------------------------------------------------------------------
// 8. The `internal` flag blocks direct publishing but nothing else.
// -----------------------------------------------------------------------------

func TestExchangeBindings_InternalExchange(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)

	t.Run("DirectPublishRefused", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		name := uniqueName("eb-internal-direct")
		require.NoError(t, ch.ExchangeDeclare(name, "fanout", false, false, true /* internal */, false, nil))

		// basic.publish is asynchronous; the refusal arrives as a channel exception.
		require.NoError(t, ch.Publish(name, "k", false, false, amqp.Publishing{Body: []byte("nope")}))
		t_expectChannelClose(t, ch, int(amqp.AccessRefused), "")
	})

	t.Run("ReachableViaExchangeBinding", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		defer ch.Close()

		public := uniqueName("eb-internal-public")
		private := uniqueName("eb-internal-private")
		require.NoError(t, ch.ExchangeDeclare(public, "direct", false, false, false, false, nil))
		require.NoError(t, ch.ExchangeDeclare(private, "fanout", false, false, true /* internal */, false, nil))

		// A queue may bind to an internal exchange.
		_, deliveries := t_ebConsumeBound(t, ch, private, "", "eb-q-internal")

		// An internal exchange may be an exchange.bind destination.
		require.NoError(t, ch.ExchangeBind(private, "secret", public, false, nil))

		body := "via-e2e-binding"
		t_publishMessage(t, ch, public, "secret", body, false, amqp.Publishing{})
		t_ebExpectExactlyOne(t, deliveries, body)
	})

	t.Run("UsableAsAlternateExchange", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		defer ch.Close()

		private := uniqueName("eb-internal-ae")
		primary := uniqueName("eb-internal-primary")
		require.NoError(t, ch.ExchangeDeclare(private, "fanout", false, false, true /* internal */, false, nil))
		require.NoError(t, ch.ExchangeDeclare(primary, "direct", false, false, false, false,
			amqp.Table{"alternate-exchange": private}))

		_, deliveries := t_ebConsumeBound(t, ch, private, "", "eb-q-internal-ae")

		body := "via-internal-ae"
		t_publishMessage(t, ch, primary, "unmatched", body, false, amqp.Publishing{})
		t_ebExpectExactlyOne(t, deliveries, body)
	})

	t.Run("UsableAsExchangeBindingSource", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		defer ch.Close()

		private := uniqueName("eb-internal-source")
		downstream := uniqueName("eb-internal-downstream")
		entry := uniqueName("eb-internal-entry")
		require.NoError(t, ch.ExchangeDeclare(private, "fanout", false, false, true /* internal */, false, nil))
		require.NoError(t, ch.ExchangeDeclare(downstream, "fanout", false, false, false, false, nil))
		require.NoError(t, ch.ExchangeDeclare(entry, "fanout", false, false, false, false, nil))

		// entry -> private (internal) -> downstream
		require.NoError(t, ch.ExchangeBind(private, "", entry, false, nil))
		require.NoError(t, ch.ExchangeBind(downstream, "", private, false, nil),
			"an internal exchange must be usable as an exchange.bind source")

		_, deliveries := t_ebConsumeBound(t, ch, downstream, "", "eb-q-internal-src")

		body := "through-internal"
		t_publishMessage(t, ch, entry, "rk", body, false, amqp.Publishing{})
		t_ebExpectExactlyOne(t, deliveries, body)
	})
}

// -----------------------------------------------------------------------------
// 9. Alternate-exchange delivery counts as routed: no return, and a confirm ack.
// -----------------------------------------------------------------------------

func TestExchangeBindings_AlternateExchangeRoutesAndConfirms(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)
	ch := t_ebChannel(t, conn)
	defer ch.Close()

	altName := uniqueName("eb-ae")
	primary := uniqueName("eb-ae-primary")
	require.NoError(t, ch.ExchangeDeclare(altName, "fanout", false, false, false, false, nil))
	require.NoError(t, ch.ExchangeDeclare(primary, "direct", false, false, false, false,
		amqp.Table{"alternate-exchange": altName}))

	_, altDeliveries := t_ebConsumeBound(t, ch, altName, "", "eb-q-ae")
	// A queue on the primary that the message will NOT match, so the primary really does
	// yield zero targets for this routing key.
	_, primaryDeliveries := t_ebConsumeBound(t, ch, primary, "matched", "eb-q-primary")

	require.NoError(t, ch.Confirm(false))
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 4))
	returns := ch.NotifyReturn(make(chan amqp.Return, 4))

	body := "routed-via-ae"
	require.NoError(t, ch.Publish(primary, "unmatched", true /* mandatory */, false,
		amqp.Publishing{Body: []byte(body)}))

	// The alternate exchange's queue receives it exactly once.
	t_ebExpectExactlyOne(t, altDeliveries, body)
	t_expectNoMessage(t, primaryDeliveries, t_ebQuiet)

	// Routed via AE => publisher is acked...
	select {
	case confirm := <-confirms:
		assert.True(t, confirm.Ack, "a message routed via the alternate exchange must be acked")
		assert.Equal(t, uint64(1), confirm.DeliveryTag)
	case <-time.After(t_ebWait):
		t.Fatal("Timeout waiting for publisher confirm")
	}

	// ...and NOT returned, even though it was published mandatory.
	select {
	case ret := <-returns:
		t.Fatalf("Unexpected basic.return for an AE-routed message: code=%d body=%q", ret.ReplyCode, string(ret.Body))
	default:
	}

	// A key the primary DOES match must go to the primary and not to the AE.
	direct := "matched-directly"
	require.NoError(t, ch.Publish(primary, "matched", true, false, amqp.Publishing{Body: []byte(direct)}))
	t_ebExpectExactlyOne(t, primaryDeliveries, direct)
	t_expectNoMessage(t, altDeliveries, t_ebQuiet)
}

// -----------------------------------------------------------------------------
// 10. Alternate-exchange chains resolve; alternate-exchange cycles terminate.
// -----------------------------------------------------------------------------

func TestExchangeBindings_AlternateExchangeChainAndCycle(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)

	t.Run("ChainOfTwoResolves", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		defer ch.Close()

		last := uniqueName("eb-ae-chain-last")
		middle := uniqueName("eb-ae-chain-middle")
		first := uniqueName("eb-ae-chain-first")

		require.NoError(t, ch.ExchangeDeclare(last, "fanout", false, false, false, false, nil))
		require.NoError(t, ch.ExchangeDeclare(middle, "direct", false, false, false, false,
			amqp.Table{"alternate-exchange": last}))
		require.NoError(t, ch.ExchangeDeclare(first, "direct", false, false, false, false,
			amqp.Table{"alternate-exchange": middle}))

		_, deliveries := t_ebConsumeBound(t, ch, last, "", "eb-q-ae-chain")

		body := "two-ae-hops"
		t_publishMessage(t, ch, first, "unmatched", body, false, amqp.Publishing{})
		t_ebExpectExactlyOne(t, deliveries, body)
	})

	t.Run("CycleTerminatesAsUnroutable", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		defer ch.Close()

		exA := uniqueName("eb-ae-cycle-a")
		exB := uniqueName("eb-ae-cycle-b")

		// Declare B first pointing at A, then redeclare it once A exists is unnecessary:
		// a nonexistent alternate-exchange target is accepted at declare time.
		require.NoError(t, ch.ExchangeDeclare(exA, "direct", false, false, false, false,
			amqp.Table{"alternate-exchange": exB}))
		require.NoError(t, ch.ExchangeDeclare(exB, "direct", false, false, false, false,
			amqp.Table{"alternate-exchange": exA}))

		returns := ch.NotifyReturn(make(chan amqp.Return, 4))

		body := "ae-cycle"
		require.NoError(t, ch.Publish(exA, "nowhere", true /* mandatory */, false,
			amqp.Publishing{Body: []byte(body)}))

		select {
		case ret := <-returns:
			assert.Equal(t, uint16(312), ret.ReplyCode, "unroutable AE cycle should return NO_ROUTE")
			assert.Equal(t, body, string(ret.Body))
		case <-time.After(t_ebWait):
			t.Fatal("Timeout waiting for basic.return; the alternate-exchange cycle did not terminate")
		}

		t_expectNoChannelClose(t, ch, t_ebQuiet)
	})

	t.Run("NonexistentAlternateExchangeIsUnroutable", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		defer ch.Close()

		primary := uniqueName("eb-ae-dangling")
		require.NoError(t, ch.ExchangeDeclare(primary, "direct", false, false, false, false,
			amqp.Table{"alternate-exchange": uniqueName("eb-ae-nonexistent")}))

		returns := ch.NotifyReturn(make(chan amqp.Return, 4))

		body := "dangling-ae"
		require.NoError(t, ch.Publish(primary, "nowhere", true, false, amqp.Publishing{Body: []byte(body)}))

		select {
		case ret := <-returns:
			assert.Equal(t, uint16(312), ret.ReplyCode)
			assert.Equal(t, body, string(ret.Body))
		case <-time.After(t_ebWait):
			t.Fatal("Timeout waiting for basic.return for a dangling alternate-exchange")
		}
	})
}

// -----------------------------------------------------------------------------
// 11. Validation of the alternate-exchange declare argument.
// -----------------------------------------------------------------------------

func TestExchangeBindings_AlternateExchangeDeclareValidation(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)

	t.Run("NonStringValueRejected", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		err := ch.ExchangeDeclare(uniqueName("eb-ae-badtype"), "direct", false, false, false, false,
			amqp.Table{"alternate-exchange": int32(42)})
		require.Error(t, err, "a non-string alternate-exchange must be rejected")
		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "expected an AMQP error, got %T", err)
		assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "expected 406 PRECONDITION_FAILED")
	})

	t.Run("RedeclareWithDifferentValueRejected", func(t *testing.T) {
		setupCh := t_ebChannel(t, conn)
		name := uniqueName("eb-ae-redeclare")
		require.NoError(t, setupCh.ExchangeDeclare(name, "direct", false, false, false, false,
			amqp.Table{"alternate-exchange": "ae-one"}))
		require.NoError(t, setupCh.Close())

		ch := t_ebChannel(t, conn)
		err := ch.ExchangeDeclare(name, "direct", false, false, false, false,
			amqp.Table{"alternate-exchange": "ae-two"})
		require.Error(t, err, "redeclaring with a different alternate-exchange must be rejected")
		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "expected an AMQP error, got %T", err)
		assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "expected 406 PRECONDITION_FAILED")
	})

	t.Run("RedeclareWithIdenticalValueAccepted", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		defer ch.Close()

		name := uniqueName("eb-ae-same")
		args := amqp.Table{"alternate-exchange": "ae-same"}
		require.NoError(t, ch.ExchangeDeclare(name, "direct", false, false, false, false, args))
		require.NoError(t, ch.ExchangeDeclare(name, "direct", false, false, false, false, args),
			"redeclaring with an identical alternate-exchange must succeed")

		// Declare equivalence for other, unknown arguments stays unchecked.
		require.NoError(t, ch.ExchangeDeclare(name, "direct", false, false, false, false,
			amqp.Table{"alternate-exchange": "ae-same", "x-unknown": "whatever"}),
			"unknown arguments must remain unchecked on redeclare")
	})
}

// -----------------------------------------------------------------------------
// 12. P2P GUARD: with no alternate exchange, an unroutable mandatory message is
//     still returned and, in confirm mode, nacked. This test documents behaviour
//     that already exists on the base commit and must not regress.
// -----------------------------------------------------------------------------

func TestExchangeBindings_NoAlternateExchangeUnroutable(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)
	ch := t_ebChannel(t, conn)
	defer ch.Close()

	primary := uniqueName("eb-no-ae")
	require.NoError(t, ch.ExchangeDeclare(primary, "direct", false, false, false, false, nil))

	require.NoError(t, ch.Confirm(false))
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 4))
	returns := ch.NotifyReturn(make(chan amqp.Return, 4))

	body := "unroutable-no-ae"
	require.NoError(t, ch.Publish(primary, "nowhere", true /* mandatory */, false,
		amqp.Publishing{Body: []byte(body)}))

	select {
	case ret := <-returns:
		assert.Equal(t, uint16(312), ret.ReplyCode, "expected NO_ROUTE")
		assert.Equal(t, body, string(ret.Body))
	case <-time.After(t_ebWait):
		t.Fatal("Timeout waiting for basic.return")
	}

	select {
	case confirm := <-confirms:
		assert.False(t, confirm.Ack, "carrot-mq nacks unroutable messages in confirm mode")
		assert.Equal(t, uint64(1), confirm.DeliveryTag)
	case <-time.After(t_ebWait):
		t.Fatal("Timeout waiting for publisher confirm")
	}
}

// -----------------------------------------------------------------------------
// 13. Durable exchange bindings and alternate-exchange survive a restart.
// -----------------------------------------------------------------------------

func TestExchangeBindings_SurviveRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "eb-restart.db")

	source := uniqueName("eb-restart-src")
	dest := uniqueName("eb-restart-dst")
	altName := uniqueName("eb-restart-ae")
	destQueue := uniqueName("eb-restart-q-dest")
	altQueue := uniqueName("eb-restart-q-ae")
	transientDest := uniqueName("eb-restart-transient-dst")

	// --- First run: declare everything durably and verify routing works. ---
	func() {
		addr, cleanup := setupTestServer(t, WithBuntDBStorage(dbPath))
		defer cleanup()

		conn, err := amqp.Dial("amqp://" + addr)
		require.NoError(t, err)
		defer conn.Close()

		ch, err := conn.Channel()
		require.NoError(t, err)
		defer ch.Close()

		require.NoError(t, ch.ExchangeDeclare(altName, "fanout", true, false, false, false, nil))
		require.NoError(t, ch.ExchangeDeclare(dest, "fanout", true, false, false, false, nil))
		require.NoError(t, ch.ExchangeDeclare(source, "direct", true, false, false, false,
			amqp.Table{"alternate-exchange": altName}))
		// A transient destination: its binding must NOT survive the restart.
		require.NoError(t, ch.ExchangeDeclare(transientDest, "fanout", false, false, false, false, nil))

		for _, q := range []string{destQueue, altQueue} {
			_, err = ch.QueueDeclare(q, true, false, false, false, nil)
			require.NoError(t, err)
		}
		require.NoError(t, ch.QueueBind(destQueue, "", dest, false, nil))
		require.NoError(t, ch.QueueBind(altQueue, "", altName, false, nil))

		require.NoError(t, ch.ExchangeBind(dest, "k", source, false, nil))
		require.NoError(t, ch.ExchangeBind(transientDest, "k", source, false, nil))
	}()

	// --- Second run: same database, fresh server process state. ---
	addr, cleanup := setupTestServer(t, WithBuntDBStorage(dbPath))
	defer cleanup()

	conn := t_ebDial(t, addr)
	ch := t_ebChannel(t, conn)
	defer ch.Close()

	destDeliveries, err := ch.Consume(destQueue, uniqueName("ctag"), true, false, false, false, nil)
	require.NoError(t, err, "durable queue %s should have been recovered", destQueue)
	altDeliveries, err := ch.Consume(altQueue, uniqueName("ctag"), true, false, false, false, nil)
	require.NoError(t, err, "durable queue %s should have been recovered", altQueue)

	// The recovered exchange-to-exchange binding still routes.
	viaBinding := "recovered-binding"
	t_publishMessage(t, ch, source, "k", viaBinding, false, amqp.Publishing{})
	t_ebExpectExactlyOne(t, destDeliveries, viaBinding)
	t_expectNoMessage(t, altDeliveries, t_ebQuiet)

	// The recovered alternate-exchange still catches unmatched keys.
	viaAlt := "recovered-alternate-exchange"
	t_publishMessage(t, ch, source, "unmatched", viaAlt, false, amqp.Publishing{})
	t_ebExpectExactlyOne(t, altDeliveries, viaAlt)
	t_expectNoMessage(t, destDeliveries, t_ebQuiet)

	// The transient destination exchange is gone, so its binding is gone with it.
	err = ch.ExchangeDeclarePassive(transientDest, "fanout", false, false, false, false, nil)
	require.Error(t, err, "a transient exchange must not survive a restart")
}

// -----------------------------------------------------------------------------
// 14. exchange.delete removes both directions; if-unused only counts outbound edges.
// -----------------------------------------------------------------------------

func TestExchangeBindings_ExchangeDeleteSeversAndIfUnused(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn := t_ebDial(t, addr)

	t.Run("DeletingDestinationSeversSourceEdge", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		defer ch.Close()

		source := uniqueName("eb-del-src")
		dest := uniqueName("eb-del-dst")
		require.NoError(t, ch.ExchangeDeclare(source, "direct", false, false, false, false, nil))
		require.NoError(t, ch.ExchangeDeclare(dest, "fanout", false, false, false, false, nil))
		require.NoError(t, ch.ExchangeBind(dest, "k", source, false, nil))

		queueName, deliveries := t_ebConsumeBound(t, ch, dest, "", "eb-q-del")

		body := "before-delete"
		t_publishMessage(t, ch, source, "k", body, false, amqp.Publishing{})
		t_ebExpectExactlyOne(t, deliveries, body)

		// Delete and immediately recreate the destination under the same name. Since the
		// edge was removed from the source, the recreated exchange is NOT reachable.
		require.NoError(t, ch.ExchangeDelete(dest, false, false))
		require.NoError(t, ch.ExchangeDeclare(dest, "fanout", false, false, false, false, nil))
		require.NoError(t, ch.QueueBind(queueName, "", dest, false, nil))

		t_publishMessage(t, ch, source, "k", "after-delete", false, amqp.Publishing{})
		t_expectNoMessage(t, deliveries, t_ebQuiet)

		// The source itself is now unused, so if-unused deletion succeeds.
		assert.NoError(t, ch.ExchangeDelete(source, true, false),
			"a source with no remaining bindings should be deletable with if-unused")
	})

	t.Run("IfUnusedBlockedByOutboundEdge", func(t *testing.T) {
		ch := t_ebChannel(t, conn)

		source := uniqueName("eb-unused-src")
		dest := uniqueName("eb-unused-dst")
		require.NoError(t, ch.ExchangeDeclare(source, "direct", false, false, false, false, nil))
		require.NoError(t, ch.ExchangeDeclare(dest, "fanout", false, false, false, false, nil))
		require.NoError(t, ch.ExchangeBind(dest, "k", source, false, nil))

		err := ch.ExchangeDelete(source, true, false)
		require.Error(t, err, "an exchange with an outbound e2e binding is in use")
		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "expected an AMQP error, got %T", err)
		assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code, "expected 406 PRECONDITION_FAILED")
	})

	t.Run("IfUnusedAllowedWithOnlyInboundEdge", func(t *testing.T) {
		ch := t_ebChannel(t, conn)
		defer ch.Close()

		source := uniqueName("eb-inbound-src")
		dest := uniqueName("eb-inbound-dst")
		require.NoError(t, ch.ExchangeDeclare(source, "direct", false, false, false, false, nil))
		require.NoError(t, ch.ExchangeDeclare(dest, "fanout", false, false, false, false, nil))
		require.NoError(t, ch.ExchangeBind(dest, "k", source, false, nil))

		// dest only has an INBOUND edge, so it is not "in use".
		assert.NoError(t, ch.ExchangeDelete(dest, true, false),
			"an inbound e2e edge must not block if-unused deletion")

		// And the source's dangling edge was cleaned up, so it too is now unused.
		assert.NoError(t, ch.ExchangeDelete(source, true, false),
			"the source's edge should have been removed when the destination was deleted")
	})
}
