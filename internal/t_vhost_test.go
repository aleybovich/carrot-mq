package internal

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connectToVHost establishes a connection to the server and attempts to open the specified vhost.
// It returns the connection, a cleanup function, and any error encountered.
func connectToVHost(t *testing.T, serverAddr, vhostName string) (*amqp.Connection, func(), error) {
	t.Helper()
	conn, err := amqp.DialConfig(fmt.Sprintf("amqp://%s", serverAddr), amqp.Config{
		Vhost: vhostName,
	})
	if err != nil {
		return nil, func() {}, err
	}
	cleanup := func() {
		conn.Close()
	}
	return conn, cleanup, nil
}

func TestVHost_Add_DefaultExists(t *testing.T) {
	server := NewServer() // Default vhost "/" should be created
	vhost, err := server.GetVHost("/")
	require.NoError(t, err, "Default vhost '/' should exist")
	require.NotNil(t, vhost, "Default vhost object should not be nil")
	assert.Equal(t, "/", vhost.name, "Default vhost name should be '/'")

	// Verify default exchange exists in default vhost
	vhost.mu.RLock()
	defaultEx, exExists := vhost.exchanges[""]
	vhost.mu.RUnlock()
	require.True(t, exExists, "Default exchange should exist in default vhost")
	require.NotNil(t, defaultEx, "Default exchange object should not be nil")
	assert.Equal(t, "direct", defaultEx.Type, "Default exchange type should be direct")
}

func TestVHost_Add_Named(t *testing.T) {
	IsTerminal = true
	server := NewServer()
	vhostName := "test-vhost-add"
	err := server.AddVHost(vhostName)
	require.NoError(t, err, "Should be able to add a named vhost")

	vhost, getErr := server.GetVHost(vhostName)
	require.NoError(t, getErr, "Should be able to retrieve the added vhost")
	require.NotNil(t, vhost, "Retrieved vhost object should not be nil")
	assert.Equal(t, vhostName, vhost.name, "VHost name mismatch")

	// Verify default exchange exists in the new vhost
	vhost.mu.RLock()
	defaultEx, exExists := vhost.exchanges[""]
	vhost.mu.RUnlock()
	require.True(t, exExists, "Default exchange should exist in named vhost")
	require.NotNil(t, defaultEx, "Default exchange object in named vhost should not be nil")
	assert.Equal(t, "direct", defaultEx.Type, "Default exchange type in named vhost should be direct")
}

func TestVHost_Add_ExistingNamed(t *testing.T) {
	IsTerminal = true
	server := NewServer()
	vhostName := "test-vhost-existing"
	err := server.AddVHost(vhostName)
	require.NoError(t, err, "First add should succeed")

	err = server.AddVHost(vhostName)
	require.Error(t, err, "Adding an existing vhost should fail")
	assert.Contains(t, err.Error(), "already exists", "Error message should indicate vhost already exists")
}

func TestVHost_Add_EmptyName(t *testing.T) {
	IsTerminal = true
	server := NewServer()
	err := server.AddVHost("")
	require.Error(t, err, "Adding a vhost with an empty name should fail")
	assert.Contains(t, err.Error(), "cannot be empty", "Error message should indicate name cannot be empty")
}

func TestVHost_Get_Existing(t *testing.T) {
	IsTerminal = true
	server := NewServer()
	vhostName := "test-vhost-get"
	err := server.AddVHost(vhostName)
	require.NoError(t, err)

	vhost, getErr := server.GetVHost(vhostName)
	require.NoError(t, getErr, "Should retrieve existing vhost")
	require.NotNil(t, vhost)
	assert.Equal(t, vhostName, vhost.name)
}

func TestVHost_Get_NonExistent(t *testing.T) {
	IsTerminal = true
	server := NewServer()
	_, err := server.GetVHost("non-existent-vhost")
	require.Error(t, err, "Should fail to retrieve non-existent vhost")
	assert.Contains(t, err.Error(), "does not exist", "Error message should indicate vhost does not exist")
}

func TestVHost_Delete_Named_NoResources(t *testing.T) {
	IsTerminal = true
	server, _, serverCleanup := setupAndReturnTestServer(t)
	defer serverCleanup()

	vhostName := "test-vhost-delete-empty"
	err := server.AddVHost(vhostName)
	require.NoError(t, err)

	// Verify it exists before delete
	_, getErr := server.GetVHost(vhostName)
	require.NoError(t, getErr)

	// Delete the vhost
	err = server.DeleteVHost(vhostName)
	require.NoError(t, err, "Should successfully delete an empty named vhost")

	// Verify it no longer exists
	_, getErr = server.GetVHost(vhostName)
	require.Error(t, getErr, "VHost should not exist after deletion")
	assert.Contains(t, getErr.Error(), "does not exist")
}

func TestVHost_Delete_Default(t *testing.T) {
	IsTerminal = true
	server := NewServer()
	err := server.DeleteVHost("/")
	require.Error(t, err, "Should fail to delete the default vhost '/'")
	assert.Contains(t, err.Error(), "cannot delete default vhost", "Error message mismatch")
}

func TestVHost_Delete_NonExistent(t *testing.T) {
	IsTerminal = true
	server := NewServer()
	err := server.DeleteVHost("non-existent-vhost-for-delete")
	require.Error(t, err, "Should fail to delete a non-existent vhost")
	assert.Contains(t, err.Error(), "does not exist", "Error message mismatch")
}

func TestVHost_Delete_AlreadyDeleting(t *testing.T) {
	IsTerminal = true
	server := NewServer()
	vhostName := "test-vhost-delete-twice"
	err := server.AddVHost(vhostName)
	require.NoError(t, err, "Setup: Failed to add vhost")

	var wg sync.WaitGroup
	errChan1 := make(chan error, 1)
	errChan2 := make(chan error, 1)

	wg.Add(2)

	// First deletion attempt in a goroutine
	go func() {
		defer wg.Done()
		errChan1 <- server.DeleteVHost(vhostName)
	}()

	// Second deletion attempt in another goroutine
	// (or you could run one in the main test goroutine, but two goroutines make the race more explicit)
	go func() {
		defer wg.Done()
		// Introduce a tiny artificial delay to slightly increase chances of different interleavings,
		// though the core idea is to handle whichever wins.
		// time.Sleep(1 * time.Millisecond) // Optional, for varying race outcomes
		errChan2 <- server.DeleteVHost(vhostName)
	}()

	wg.Wait() // Wait for both deletion attempts to complete

	close(errChan1)
	close(errChan2)

	err1 := <-errChan1
	err2 := <-errChan2

	t.Logf("Deletion attempt 1 returned: %v", err1)
	t.Logf("Deletion attempt 2 returned: %v", err2)

	alreadyDeletingSubstr := "is already in the process of deletion"
	// vhostDoesNotExistSubstr := "does not exist" // We are less concerned about this for this specific test's goal

	// Condition: One call successfully initiated the deletion (returning nil),
	// and the other call attempted to delete while it was in progress.
	err1IsAlreadyDeleting := err1 != nil && strings.Contains(err1.Error(), alreadyDeletingSubstr)
	err2IsAlreadyDeleting := err2 != nil && strings.Contains(err2.Error(), alreadyDeletingSubstr)

	// The logic should be: one of them successfully started the deletion (err is nil for that one),
	// and the other one found the 'deleting' flag set.
	testPassed := (err1IsAlreadyDeleting && err2 == nil) || (err2IsAlreadyDeleting && err1 == nil)

	if !testPassed {
		// Provide a more detailed failure message if the primary condition isn't met
		t.Errorf("Expected one DeleteVHost call to return nil (successful initiation) and the other to return an error containing '%s'. "+
			"Got err1: %v, err2: %v", alreadyDeletingSubstr, err1, err2)
	}

	assert.True(t, testPassed,
		fmt.Sprintf("Expected one deletion to succeed (nil error) and the other to report 'already in process of deletion'. "+
			"Actual errors - err1: [%v], err2: [%v]", err1, err2))
}

func TestVHost_Delete_WithConnections(t *testing.T) {
	IsTerminal = true
	server, serverAddr, serverCleanup := setupAndReturnTestServer(t)
	defer serverCleanup()

	vhostName := "vhost-with-conn"
	err := server.AddVHost(vhostName)
	require.NoError(t, err, "Failed to add vhost")

	// Establish a connection to this vhost
	conn, clientCleanup, err := connectToVHost(t, serverAddr, vhostName)
	require.NoError(t, err, "Failed to connect to vhost %s", vhostName)
	defer clientCleanup()

	// Channel to listen for connection close
	connClosedCh := make(chan *amqp.Error, 1)
	conn.NotifyClose(connClosedCh)

	// Delete the vhost
	deleteErr := server.DeleteVHost(vhostName)
	require.NoError(t, deleteErr, "Failed to delete vhost %s", vhostName)

	// Verify the connection was closed
	select {
	case amqpErr := <-connClosedCh:
		require.NotNil(t, amqpErr, "Connection should be closed with an error")
		assert.Equal(t, 320, amqpErr.Code, "Connection close code should be 320 (CONNECTION_FORCED)")
		assert.Contains(t, amqpErr.Reason, "VHost deleted", "Connection close reason mismatch")
	case <-time.After(3 * time.Second): // Increased timeout
		t.Fatal("Timeout waiting for connection to close after vhost deletion")
	}

	// Verify vhost is gone
	_, getErr := server.GetVHost(vhostName)
	require.Error(t, getErr, "VHost should not exist after deletion")
}

func TestVHost_Delete_WithQueuesAndConsumers(t *testing.T) {
	IsTerminal = true
	server, serverAddr, serverCleanup := setupAndReturnTestServer(t)
	defer serverCleanup()

	vhostName := "vhost-with-queues"
	err := server.AddVHost(vhostName)
	require.NoError(t, err)

	// Connect to the vhost
	conn, clientCleanup, err := connectToVHost(t, serverAddr, vhostName)
	require.NoError(t, err)
	defer clientCleanup()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close() // This might error if channel is closed by server due to vhost deletion

	// Declare a queue
	queueName := uniqueName("q-in-vhost")
	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue in vhost")

	// Start a consumer
	consumerTag := uniqueName("consumer-in-vhost")
	deliveries, err := ch.Consume(queueName, consumerTag, true, false, false, false, nil)
	require.NoError(t, err, "Failed to start consumer")

	// Publish a message to ensure consumer is active (optional, but good check)
	err = ch.Publish("", queueName, false, false, amqp.Publishing{Body: []byte("test")})
	require.NoError(t, err)
	select {
	case <-deliveries:
		// Message received by consumer
	case <-time.After(1 * time.Second):
		t.Log("Warning: Consumer did not receive initial message, continuing with delete test.")
	}

	// Listen for channel and connection close
	chanClosedCh := make(chan *amqp.Error, 1)
	ch.NotifyClose(chanClosedCh)
	connClosedCh := make(chan *amqp.Error, 1)
	conn.NotifyClose(connClosedCh)

	// Delete the vhost
	deleteErr := server.DeleteVHost(vhostName)
	require.NoError(t, deleteErr, "Failed to delete vhost %s", vhostName)

	// Verify connection was closed (which implies channel was also handled)
	select {
	case <-connClosedCh:
		// Connection closed as expected
	case <-time.After(3 * time.Second): // Increased timeout
		t.Fatal("Timeout waiting for connection to close after vhost deletion with resources")
	}

	// Verify vhost is gone
	_, getErr := server.GetVHost(vhostName)
	require.Error(t, getErr, "VHost should not exist after deletion")

	// Check internal server state (optional, white-box verification)
	server.mu.RLock()
	_, vhostStillExists := server.vhosts[vhostName]
	server.mu.RUnlock()
	assert.False(t, vhostStillExists, "VHost map in server should not contain the deleted vhost")
}

func TestVHost_Isolation_Resources(t *testing.T) {
	IsTerminal = true
	server, serverAddr, serverCleanup := setupAndReturnTestServer(t)
	defer serverCleanup()

	vhostName1 := "vhost-iso-1"
	vhostName2 := "vhost-iso-2"
	err := server.AddVHost(vhostName1)
	require.NoError(t, err)
	err = server.AddVHost(vhostName2)
	require.NoError(t, err)

	resourceName := "shared-resource-name"

	// Connect to vhost1
	conn1, cleanup1, err := connectToVHost(t, serverAddr, vhostName1)
	require.NoError(t, err)
	defer cleanup1()
	ch1, err := conn1.Channel()
	require.NoError(t, err)
	defer ch1.Close()

	// Connect to vhost2
	conn2, cleanup2, err := connectToVHost(t, serverAddr, vhostName2)
	require.NoError(t, err)
	defer cleanup2()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	defer ch2.Close()

	// Declare queue with same name in vhost1
	q1, err := ch1.QueueDeclare(resourceName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue in vhost1")
	assert.Equal(t, resourceName, q1.Name)

	// Declare queue with same name in vhost2 - should succeed and be distinct
	q2, err := ch2.QueueDeclare(resourceName, false, false, false, false, nil)
	require.NoError(t, err, "Failed to declare queue in vhost2")
	assert.Equal(t, resourceName, q2.Name)

	// Publish to queue in vhost1
	err = ch1.Publish("", q1.Name, false, false, amqp.Publishing{Body: []byte("msg-for-vhost1")})
	require.NoError(t, err)

	// Try to get message from queue in vhost1
	msg1, ok1, err := ch1.Get(q1.Name, true)
	require.NoError(t, err)
	require.True(t, ok1, "Should get message from vhost1 queue")
	assert.Equal(t, "msg-for-vhost1", string(msg1.Body))

	// Try to get message from queue in vhost2 - should be empty
	_, ok2, err := ch2.Get(q2.Name, true)
	require.NoError(t, err)
	require.False(t, ok2, "Queue in vhost2 should be empty")

	// Check server's internal state for queues (conceptual)
	sVhost1, _ := server.GetVHost(vhostName1)
	sVhost2, _ := server.GetVHost(vhostName2)

	sVhost1.mu.RLock()
	_, q1ExistsInternal := sVhost1.queues[resourceName]
	sVhost1.mu.RUnlock()
	assert.True(t, q1ExistsInternal, "Queue should exist in server's vhost1 struct")

	sVhost2.mu.RLock()
	_, q2ExistsInternal := sVhost2.queues[resourceName]
	sVhost2.mu.RUnlock()
	assert.True(t, q2ExistsInternal, "Queue should exist in server's vhost2 struct")
}

func TestVHost_Get_WhileDeleting(t *testing.T) {
	IsTerminal = true
	server := NewServer()
	vhostName := "vhost-get-while-deleting"
	err := server.AddVHost(vhostName)
	require.NoError(t, err)

	// Start deletion in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This delete might take a moment due to connection closing logic,
		// even if no connections exist.
		_ = server.DeleteVHost(vhostName)
	}()

	// Immediately try to get the vhost. It might succeed if GetVHost runs before
	// the 'deleting' flag is set, or fail if after.
	// The key is that once 'deleting' is true, GetVHost should fail.
	// We'll try a few times.

	deletedOrDeleting := false
	for i := 0; i < 10; i++ {
		_, getErr := server.GetVHost(vhostName)
		if getErr != nil {
			if strings.Contains(getErr.Error(), "is currently being deleted") || strings.Contains(getErr.Error(), "does not exist") {
				deletedOrDeleting = true
			}
			break
		}
		time.Sleep(10 * time.Millisecond) // Give delete goroutine time to progress
	}

	require.True(t, deletedOrDeleting, "GetVHost should eventually fail for a vhost being deleted")

	wg.Wait() // Ensure deletion goroutine finishes

	// After deletion is complete, GetVHost should report "does not exist"
	_, getErr := server.GetVHost(vhostName)
	require.Error(t, getErr)
	assert.Contains(t, getErr.Error(), "does not exist")
}

func TestVHost_DefaultExchange_PerVHost(t *testing.T) {
	IsTerminal = true
	server := NewServer()

	// Check default vhost
	vhostDefault, _ := server.GetVHost("/")
	vhostDefault.mu.RLock()
	exDefault, okDefault := vhostDefault.exchanges[""]
	vhostDefault.mu.RUnlock()
	require.True(t, okDefault, "Default vhost should have a default exchange")
	assert.Equal(t, "direct", exDefault.Type)

	// Add a named vhost
	vhostName := "test-vhost-def-ex"
	err := server.AddVHost(vhostName)
	require.NoError(t, err)

	vhostNamed, _ := server.GetVHost(vhostName)
	vhostNamed.mu.RLock()
	exNamed, okNamed := vhostNamed.exchanges[""]
	vhostNamed.mu.RUnlock()
	require.True(t, okNamed, "Named vhost should have a default exchange")
	assert.Equal(t, "direct", exNamed.Type)

	// Ensure they are distinct instances (conceptual check, direct pointer comparison might be too fragile)
	// A functional check is better: operations on one don't affect the other.
	// This is partly covered by TestVHost_Isolation_Resources.
	// For this test, just ensuring presence and type is sufficient.
}
