package internal

import (
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxConnections_RejectsExcessConnections(t *testing.T) {
	const maxConns = 3

	_, serverAddr, serverCleanup := setupAndReturnTestServer(t, WithMaxConnections(maxConns))
	defer serverCleanup()

	// Establish maxConns connections — all should succeed
	conns := make([]*amqp.Connection, 0, maxConns)
	for i := range maxConns {
		conn, err := amqp.Dial(fmt.Sprintf("amqp://%s", serverAddr))
		require.NoError(t, err, "Connection %d should succeed", i+1)
		conns = append(conns, conn)
	}

	// The next connection should be rejected (TCP closed immediately)
	_, err := amqp.DialConfig(fmt.Sprintf("amqp://%s", serverAddr), amqp.Config{
		Dial: amqp.DefaultDial(2 * time.Second),
	})
	assert.Error(t, err, "Connection %d should be rejected when at max capacity", maxConns+1)

	// Close one existing connection
	require.NoError(t, conns[0].Close())
	conns = conns[1:]

	// Allow time for server to process the removal
	time.Sleep(200 * time.Millisecond)

	// Now a new connection should succeed again
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s", serverAddr))
	assert.NoError(t, err, "Connection should succeed after a slot is freed")
	if conn != nil {
		conns = append(conns, conn)
	}

	// Cleanup
	for _, c := range conns {
		c.Close()
	}
}
