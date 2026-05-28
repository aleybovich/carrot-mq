package internal

import (
	"fmt"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExchangeDeclare_HeadersTypeRejected(t *testing.T) {
	_, serverAddr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s", serverAddr))
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Declaring a "headers" exchange should fail with NOT_IMPLEMENTED (540)
	err = ch.ExchangeDeclare("test-headers-exchange", "headers", false, false, false, false, nil)
	require.Error(t, err, "headers exchange type should be rejected")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "error should be an AMQP error")
	assert.Equal(t, 540, amqpErr.Code, "expected NOT_IMPLEMENTED (540)")
}

func TestExchangeDeclare_ValidTypesAccepted(t *testing.T) {
	_, serverAddr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s", serverAddr))
	require.NoError(t, err)
	defer conn.Close()

	for _, exType := range []string{"direct", "fanout", "topic"} {
		ch, err := conn.Channel()
		require.NoError(t, err)

		err = ch.ExchangeDeclare(fmt.Sprintf("test-%s-exchange", exType), exType, false, false, false, false, nil)
		assert.NoError(t, err, "exchange type %q should be accepted", exType)

		ch.Close()
	}
}

func TestExchangeDeclare_UnknownTypeRejected(t *testing.T) {
	_, serverAddr, cleanup := setupAndReturnTestServer(t)
	defer cleanup()

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s", serverAddr))
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Completely unknown type should also be rejected
	err = ch.ExchangeDeclare("test-bogus-exchange", "bogus", false, false, false, false, nil)
	require.Error(t, err, "unknown exchange type should be rejected")

	amqpErr, ok := err.(*amqp.Error)
	require.True(t, ok, "error should be an AMQP error")
	assert.Equal(t, 540, amqpErr.Code, "expected NOT_IMPLEMENTED (540)")
}
