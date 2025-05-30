package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type VHost struct {
	name      string
	exchanges map[string]*Exchange
	queues    map[string]*Queue
	mu        sync.RWMutex // Protects exchanges and queues maps within this vhost

	deleting atomic.Bool // Indicates if this vhost is undergoing deletion
}

type VHostConfig struct {
	name      string
	exchanges []ExchangeConfig
	queues    []QueueConfig
}

func (s *Server) AddVHost(name string) error {
	if name == "" {
		return errors.New("vhost name cannot be empty")
	}

	s.mu.Lock() // Full lock to check and add
	defer s.mu.Unlock()

	if _, exists := s.vhosts[name]; exists {
		return fmt.Errorf("vhost '%s' already exists", name)
	}

	newVHost := &VHost{
		name:      name,
		exchanges: make(map[string]*Exchange),
		queues:    make(map[string]*Queue),
	}

	// Create default direct exchange for this vhost
	newVHost.exchanges[""] = &Exchange{
		Name:     "",
		Type:     "direct",
		Bindings: make(map[string][]string),
	}

	s.vhosts[name] = newVHost
	s.Info("Created new vhost: '%s'", name)
	return nil
}

func (s *Server) DeleteVHost(name string) error {
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
	var connsToClose []*Connection
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
			go func(c *Connection) {
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

	s.Info("Successfully deleted vhost: '%s'", name)
	return nil
}

// GetVHost retrieves a virtual host by name
func (s *Server) GetVHost(name string) (*VHost, error) {
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
func (vh *VHost) IsDeleting() bool {
	return vh.deleting.Load()
}

// stopAllConsumers stops all consumers in the vhost
func (vh *VHost) stopAllConsumers(s *Server) { // s *Server is used for logging
	vh.mu.RLock() // RLock to iterate vhost's queues map
	// Create a snapshot of queues to avoid holding lock while operating on individual queues
	queuesSnapshot := make([]*Queue, 0, len(vh.queues))
	for _, q := range vh.queues {
		queuesSnapshot = append(queuesSnapshot, q)
	}
	vh.mu.RUnlock()

	for _, queue := range queuesSnapshot {
		queue.mu.Lock() // Lock individual queue to modify its consumers
		if len(queue.Consumers) > 0 {
			s.Info("VHost '%s', Queue '%s': Closing %d consumer(s).", vh.name, queue.Name, len(queue.Consumers))
			for consumerTag, msgChan := range queue.Consumers {
				s.Debug("VHost '%s', Queue '%s': Closing consumer channel for tag '%s'.", vh.name, queue.Name, consumerTag)
				close(msgChan) // This signals the deliverMessages goroutine to stop
				delete(queue.Consumers, consumerTag)
			}
		}
		queue.mu.Unlock()
	}
}

// cleanup performs final cleanup of vhost resources (queues and exchanges)
func (vh *VHost) cleanup(s *Server) { // s *Server is used for logging
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
			for tag, ch := range q.Consumers {
				close(ch)
				delete(q.Consumers, tag)
			}
		}
		q.mu.Unlock()
	}
	vh.queues = make(map[string]*Queue) // Reset the map

	s.Info("VHost '%s': Cleaning up %d exchanges.", vh.name, len(vh.exchanges))
	for exchangeName, ex := range vh.exchanges {
		ex.mu.Lock()
		s.Debug("VHost '%s': Clearing bindings for exchange '%s'.", vh.name, exchangeName)
		ex.Bindings = make(map[string][]string) // Clear bindings
		ex.mu.Unlock()
	}
	vh.exchanges = make(map[string]*Exchange) // Reset the map
	// Re-add the default "" exchange.
	vh.exchanges[""] = &Exchange{
		Name:     "",
		Type:     "direct",
		Bindings: make(map[string][]string),
	}

	s.Info("VHost '%s': Internal resource cleanup complete.", vh.name)
}
