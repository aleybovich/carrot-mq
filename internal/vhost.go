package internal

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type vHost struct {
	name      string
	exchanges map[string]*exchange
	queues    map[string]*queue
	mu        sync.RWMutex // Protects exchanges and queues maps within this vhost

	deleting atomic.Bool // Indicates if this vhost is undergoing deletion
}

// Helper method to add vhost with optional persistence
func (s *server) addVHostInternal(name string, persist bool) error {
	if name == "" {
		return errors.New("vhost name cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.vhosts[name]; exists {
		return fmt.Errorf("vhost '%s' already exists", name)
	}

	newVHost := &vHost{
		name:      name,
		exchanges: make(map[string]*exchange),
		queues:    make(map[string]*queue),
	}

	// Create default direct exchange for this vhost
	newVHost.exchanges[""] = &exchange{
		Name:     "",
		Type:     "direct",
		Bindings: make(map[string][]string),
	}

	s.vhosts[name] = newVHost

	// Persist if requested
	if persist && s.persistenceManager != nil {
		record := &VHostRecord{
			Name:      name,
			CreatedAt: time.Now(),
		}
		if err := s.persistenceManager.SaveVHost(record); err != nil {
			s.Err("Failed to persist vhost %s: %v", name, err)
		}
	}

	s.Info("Created new vhost: '%s'", name)
	return nil
}

// Update public AddVHost to persist
func (s *server) AddVHost(name string) error {
	return s.addVHostInternal(name, true)
}

func (s *server) DeleteVHost(name string) error {
	if name == "/" {
		return errors.New("cannot delete default vhost '/'")
	}

	s.mu.RLock() // Use RLock initially to find the vhost in the server's map
	vhostToDelete, exists := s.vhosts[name]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("vhost '%s' does not exist", name)
	}

	// Atomically check and set the deleting flag on the vhost object itself.
	// This ensures only one goroutine proceeds with the actual deletion logic.
	if !vhostToDelete.deleting.CompareAndSwap(false, true) {
		s.Info("VHost '%s' is already being deleted by another operation.", name)
		return fmt.Errorf("vhost '%s' is already in the process of deletion", name)
	}

	// At this point, vhostToDelete.deleting is true.
	// This instance of DeleteVHost is responsible for the full deletion.
	s.Info("Initiating deletion for vhost '%s'", name)

	// Step 1: Stop all consumers for this vhost.
	s.Info("Stopping all consumers for vhost '%s'", name)
	vhostToDelete.stopAllConsumers(s) // Pass server for logging

	// Step 2: Close active connections associated with this vhost.
	var connsToClose []*connection
	s.connectionsMu.RLock()
	for conn := range s.connections {
		conn.mu.RLock() // Lock individual connection to safely access its vhost field
		if conn.vhost == vhostToDelete {
			connsToClose = append(connsToClose, conn)
		}
		conn.mu.RUnlock()
	}
	s.connectionsMu.RUnlock()

	if len(connsToClose) > 0 {
		s.Info("Closing %d connections for vhost '%s' deletion", len(connsToClose), name)
		var wg sync.WaitGroup
		wg.Add(len(connsToClose))

		for _, conn := range connsToClose {
			go func(c *connection) {
				defer wg.Done()
				s.Info("Sending Connection.Close to %s for vhost '%s' deletion", c.conn.RemoteAddr(), name)
				if err := c.sendConnectionClose(320, "VHost deleted", 0, 0); err != nil {
					s.Debug("Failed to send Connection.Close to %s: %v. Will force close.", c.conn.RemoteAddr(), err)
				}
				// Small grace period for client
				time.Sleep(100 * time.Millisecond)

				// Force close the underlying net.Conn
				c.conn.Close()
			}(conn)
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			s.Info("All connections for vhost '%s' have been signaled for closure.", name)
		case <-time.After(5 * time.Second):
			s.Warn("Timeout waiting for connection closure signals for vhost '%s'.", name)
		}
	}

	// Step 3: Perform internal cleanup of the vhost's resources (queues, exchanges).
	s.Info("Cleaning up internal resources for vhost '%s'", name)
	vhostToDelete.cleanup(s) // Pass server for logging

	// Step 4: Finally, remove the vhost from the server's main map.
	s.mu.Lock()
	delete(s.vhosts, name)
	s.mu.Unlock()

	// PERSISTENCE: Delete all vhost data at the end
	if s.persistenceManager != nil {
		if err := s.persistenceManager.DeleteAllVHostData(name); err != nil {
			s.Err("Failed to delete vhost %s from persistence: %v", name, err)
		}
	}

	s.Info("Successfully deleted vhost: '%s'", name)
	return nil
}

// GetVHost retrieves a virtual host by name
func (s *server) GetVHost(name string) (*vHost, error) {
	s.mu.RLock() // RLock to access s.vhosts map
	vhost, exists := s.vhosts[name]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("vhost '%s' does not exist", name)
	}

	// Check the deleting flag on the vhost object itself.
	if vhost.IsDeleting() {
		return nil, fmt.Errorf("vhost '%s' is currently being deleted", name)
	}

	return vhost, nil
}

// IsDeleting returns true if the vhost is being deleted
func (vh *vHost) IsDeleting() bool {
	return vh.deleting.Load()
}

// stopAllConsumers stops all consumers in the vhost
func (vh *vHost) stopAllConsumers(s *server) { // s *Server is used for logging
	vh.mu.RLock() // RLock to iterate vhost's queues map
	// Create a snapshot of queues to avoid holding lock while operating on individual queues
	queuesSnapshot := make([]*queue, 0, len(vh.queues))
	for _, q := range vh.queues {
		queuesSnapshot = append(queuesSnapshot, q)
	}
	vh.mu.RUnlock()

	for _, queue := range queuesSnapshot {
		queue.mu.Lock() // Lock individual queue to modify its consumers
		if len(queue.Consumers) > 0 {
			s.Info("VHost '%s', Queue '%s': Closing %d consumer(s).", vh.name, queue.Name, len(queue.Consumers))
			for consumerTag, consumer := range queue.Consumers {
				s.Debug("VHost '%s', Queue '%s': Closing consumer channel for tag '%s'.", vh.name, queue.Name, consumerTag)
				close(consumer.stopCh) // This signals the deliverMessages goroutine to stop
				delete(queue.Consumers, consumerTag)
			}
		}
		queue.mu.Unlock()
	}
}

// cleanup performs final cleanup of vhost resources (queues and exchanges)
func (vh *vHost) cleanup(s *server) { // s *Server is used for logging
	vh.mu.Lock() // Full lock on vhost to modify its internal maps
	defer vh.mu.Unlock()

	s.Info("VHost '%s': Cleaning up %d queues.", vh.name, len(vh.queues))
	for queueName, q := range vh.queues {
		q.mu.Lock() // Lock individual queue
		s.Debug("VHost '%s': Clearing messages and bindings for queue '%s'.", vh.name, queueName)
		q.Messages = nil // Clear messages
		q.Bindings = make(map[string]bool)
		if len(q.Consumers) > 0 { // Should be empty if stopAllConsumers worked
			s.Warn("VHost '%s', Queue '%s': Consumers map not empty during final cleanup. Force clearing.", vh.name, queueName)
			for tag, consumer := range q.Consumers {
				close(consumer.stopCh)
				delete(q.Consumers, tag)
			}
		}
		q.mu.Unlock()
	}
	vh.queues = make(map[string]*queue) // Reset the map

	s.Info("VHost '%s': Cleaning up %d exchanges.", vh.name, len(vh.exchanges))
	for exchangeName, ex := range vh.exchanges {
		ex.mu.Lock()
		s.Debug("VHost '%s': Clearing bindings for exchange '%s'.", vh.name, exchangeName)
		ex.Bindings = make(map[string][]string) // Clear bindings
		ex.mu.Unlock()
	}
	vh.exchanges = make(map[string]*exchange) // Reset the map
	// Re-add the default "" exchange.
	vh.exchanges[""] = &exchange{
		Name:     "",
		Type:     "direct",
		Bindings: make(map[string][]string),
	}

	s.Info("VHost '%s': Internal resource cleanup complete.", vh.name)
}
