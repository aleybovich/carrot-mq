package internal

import (
	"carrot-mq/logger"
	"carrot-mq/storage"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Helper functions to construct storage keys
func VHostKey(name string) string {
	return storage.KeyPrefixVHost + name
}

func ExchangeKey(vhost, exchange string) string {
	return storage.KeyPrefixExchange + vhost + ":" + exchange
}

func QueueKey(vhost, queue string) string {
	return storage.KeyPrefixQueue + vhost + ":" + queue
}

func BindingKey(vhost, exchange, queue, routingKey string) string {
	return storage.KeyPrefixBinding + vhost + ":" + exchange + ":" + queue + ":" + routingKey
}

func MessageKey(vhost, queue, messageId string) string {
	return storage.KeyPrefixMessage + vhost + ":" + queue + ":" + messageId
}

func MessageIndexKey(vhost, queue string) string {
	return storage.KeyPrefixMsgIndex + vhost + ":" + queue
}

// Storage record types that map to our domain objects

type AckedMessageInfo struct {
	MessageId string
	QueueName string
	VHostName string
}

type VHostRecord struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type ExchangeRecord struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Durable    bool                   `json:"durable"`
	AutoDelete bool                   `json:"auto_delete"`
	Internal   bool                   `json:"internal"`
	Arguments  map[string]interface{} `json:"arguments,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
}

type QueueRecord struct {
	Name       string                 `json:"name"`
	Durable    bool                   `json:"durable"`
	Exclusive  bool                   `json:"exclusive"`
	AutoDelete bool                   `json:"auto_delete"`
	Arguments  map[string]interface{} `json:"arguments,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
}

type BindingRecord struct {
	Exchange   string                 `json:"exchange"`
	Queue      string                 `json:"queue"`
	RoutingKey string                 `json:"routing_key"`
	Arguments  map[string]interface{} `json:"arguments,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
}

type MessageRecord struct {
	ID          string                 `json:"id"`          // Unique message identifier
	Exchange    string                 `json:"exchange"`    // Original exchange
	RoutingKey  string                 `json:"routing_key"` // Original routing key
	Mandatory   bool                   `json:"mandatory"`
	Immediate   bool                   `json:"immediate"`
	Headers     map[string]interface{} `json:"headers,omitempty"`
	Properties  MessageProperties      `json:"properties"`
	Body        []byte                 `json:"body"`
	Redelivered bool                   `json:"redelivered"`
	Timestamp   time.Time              `json:"timestamp"` // When message was first received
	Sequence    int64                  `json:"sequence"`  // For maintaining order
}

type MessageProperties struct {
	ContentType     string `json:"content_type,omitempty"`
	ContentEncoding string `json:"content_encoding,omitempty"`
	DeliveryMode    uint8  `json:"delivery_mode"`
	Priority        uint8  `json:"priority"`
	CorrelationId   string `json:"correlation_id,omitempty"`
	ReplyTo         string `json:"reply_to,omitempty"`
	Expiration      string `json:"expiration,omitempty"`
	MessageId       string `json:"message_id,omitempty"`
	Timestamp       uint64 `json:"timestamp"`
	Type            string `json:"type,omitempty"`
	UserId          string `json:"user_id,omitempty"`
	AppId           string `json:"app_id,omitempty"`
	ClusterId       string `json:"cluster_id,omitempty"`
}

// MessageIndex maintains ordered list of message IDs for a queue
type MessageIndex struct {
	QueueName string   `json:"queue_name"`
	Messages  []string `json:"messages"` // Ordered list of message IDs
}

// Conversion helpers to map between domain objects and storage records

func ExchangeToRecord(e *exchange) *ExchangeRecord {
	return &ExchangeRecord{
		Name:       e.Name,
		Type:       e.Type,
		Durable:    e.Durable,
		AutoDelete: e.AutoDelete,
		Internal:   e.Internal,
		Arguments:  nil, // TODO: Add arguments field to Exchange struct
		CreatedAt:  time.Now(),
	}
}

func RecordToExchange(r *ExchangeRecord) *exchange {
	return &exchange{
		Name:       r.Name,
		Type:       r.Type,
		Durable:    r.Durable,
		AutoDelete: r.AutoDelete,
		Internal:   r.Internal,
		Bindings:   make(map[string][]string),
	}
}

func QueueToRecord(q *queue) *QueueRecord {
	return &QueueRecord{
		Name:       q.Name,
		Durable:    q.Durable,
		Exclusive:  q.Exclusive,
		AutoDelete: q.AutoDelete,
		Arguments:  nil, // TODO: Add arguments field to Queue struct
		CreatedAt:  time.Now(),
	}
}

func RecordToQueue(r *QueueRecord) *queue {
	return &queue{
		Name:       r.Name,
		Messages:   []message{},
		Bindings:   make(map[string]bool),
		Consumers:  make(map[string]*consumer),
		Durable:    r.Durable,
		Exclusive:  r.Exclusive,
		AutoDelete: r.AutoDelete,
	}
}

func MessageToRecord(m *message, id string, sequence int64) *MessageRecord {
	return &MessageRecord{
		ID:         id,
		Exchange:   m.Exchange,
		RoutingKey: m.RoutingKey,
		Mandatory:  m.Mandatory,
		Immediate:  m.Immediate,
		Headers:    m.Properties.Headers,
		Properties: MessageProperties{
			ContentType:     m.Properties.ContentType,
			ContentEncoding: m.Properties.ContentEncoding,
			DeliveryMode:    m.Properties.DeliveryMode,
			Priority:        m.Properties.Priority,
			CorrelationId:   m.Properties.CorrelationId,
			ReplyTo:         m.Properties.ReplyTo,
			Expiration:      m.Properties.Expiration,
			MessageId:       m.Properties.MessageId,
			Timestamp:       m.Properties.Timestamp,
			Type:            m.Properties.Type,
			UserId:          m.Properties.UserId,
			AppId:           m.Properties.AppId,
			ClusterId:       m.Properties.ClusterId,
		},
		Body:        m.Body,
		Redelivered: m.Redelivered,
		Timestamp:   time.Now(),
		Sequence:    sequence,
	}
}

func RecordToMessage(r *MessageRecord) *message {
	return &message{
		Exchange:   r.Exchange,
		RoutingKey: r.RoutingKey,
		Mandatory:  r.Mandatory,
		Immediate:  r.Immediate,
		Properties: properties{
			ContentType:     r.Properties.ContentType,
			ContentEncoding: r.Properties.ContentEncoding,
			Headers:         r.Headers,
			DeliveryMode:    r.Properties.DeliveryMode,
			Priority:        r.Properties.Priority,
			CorrelationId:   r.Properties.CorrelationId,
			ReplyTo:         r.Properties.ReplyTo,
			Expiration:      r.Properties.Expiration,
			MessageId:       r.Properties.MessageId,
			Timestamp:       r.Properties.Timestamp,
			Type:            r.Properties.Type,
			UserId:          r.Properties.UserId,
			AppId:           r.Properties.AppId,
			ClusterId:       r.Properties.ClusterId,
		},
		Body:        r.Body,
		Redelivered: true, // All recovered messages should be marked as redelivered
	}
}

// ------ PersistenceManager ------

// PersistenceManager is a thin coordinator for AMQP persistence operations
// It knows how to serialize/deserialize entities and manage storage keys,
// but has no knowledge of the Server or business logic
type PersistenceManager struct {
	storage    storage.StorageProvider
	logger     logger.Logger
	messageSeq atomic.Int64 // Global message sequence counter

	// per-queue mutexes for message operations
	queueMutexes sync.Map // map[string]*sync.Mutex where key is "vhost:queue"
}

func NewPersistenceManager(storage storage.StorageProvider, logger logger.Logger) *PersistenceManager {
	return &PersistenceManager{
		storage: storage,
		logger:  logger,
	}
}

func (pm *PersistenceManager) getQueueMutex(vhostName, queueName string) *sync.Mutex {
	key := vhostName + ":" + queueName
	if v, ok := pm.queueMutexes.Load(key); ok {
		return v.(*sync.Mutex)
	}

	mu := &sync.Mutex{}
	actual, _ := pm.queueMutexes.LoadOrStore(key, mu)
	return actual.(*sync.Mutex)
}

// Initialize prepares the persistence manager
func (pm *PersistenceManager) Initialize() error {
	if err := pm.storage.Initialize(); err != nil {
		return err
	}

	// Recover the sequence counter
	return pm.recoverSequenceCounter()
}

// Close shuts down the persistence manager
func (pm *PersistenceManager) Close() error {
	// Save the current sequence counter before closing
	if err := pm.saveSequenceCounter(); err != nil {
		pm.logger.Warn("Failed to save sequence counter on close: %v", err)
	}
	return pm.storage.Close()
}

// recoverSequenceCounter loads the saved sequence counter from storage
func (pm *PersistenceManager) recoverSequenceCounter() error {
	data, err := pm.storage.Get(storage.KeySeqCounter)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			// No saved counter, start from 0
			pm.messageSeq.Store(0)
			return nil
		}
		return fmt.Errorf("loading sequence counter: %w", err)
	}

	var seqNo int64
	if err := json.Unmarshal(data, &seqNo); err != nil {
		return fmt.Errorf("unmarshalling sequence counter: %w", err)
	}

	pm.messageSeq.Store(seqNo)
	pm.logger.Info("Recovered message sequence counter: %d", seqNo)
	return nil
}

// saveSequenceCounter persists the current sequence counter to storage
func (pm *PersistenceManager) saveSequenceCounter() error {
	seqNo := pm.messageSeq.Load()
	data, err := json.Marshal(seqNo)
	if err != nil {
		return fmt.Errorf("marshaling sequence counter: %w", err)
	}

	return pm.storage.Set(storage.KeySeqCounter, data)
}

// Transaction support
func (pm *PersistenceManager) BeginTransaction() (PersistenceTransaction, error) {
	tx, err := pm.storage.BeginTx()
	if err != nil {
		return nil, err
	}
	return &persistenceTransaction{
		tx:                  tx,
		pm:                  pm,
		messageIndexUpdates: make(map[string]*MessageIndex),
		messageIndexCache:   make(map[string]*MessageIndex),
	}, nil
}

// --- VHost Operations ---

func (pm *PersistenceManager) SaveVHost(record *VHostRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling vhost record: %w", err)
	}
	return pm.storage.Set(VHostKey(record.Name), data)
}

func (pm *PersistenceManager) DeleteVHost(name string) error {
	// Note: Caller is responsible for deleting all entities within the vhost
	return pm.storage.Delete(VHostKey(name))
}

func (pm *PersistenceManager) LoadVHost(name string) (*VHostRecord, error) {
	data, err := pm.storage.Get(VHostKey(name))
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	var record VHostRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("unmarshalling vhost record: %w", err)
	}
	return &record, nil
}

func (pm *PersistenceManager) LoadAllVHosts() ([]*VHostRecord, error) {
	keys, err := pm.storage.Keys(storage.KeyPrefixVHost)
	if err != nil {
		return nil, fmt.Errorf("listing vhost keys: %w", err)
	}

	vhosts := make([]*VHostRecord, 0, len(keys))
	for _, key := range keys {
		data, err := pm.storage.Get(key)
		if err != nil {
			if err == storage.ErrKeyNotFound {
				continue
			}
			return nil, fmt.Errorf("getting vhost %s: %w", key, err)
		}

		var record VHostRecord
		if err := json.Unmarshal(data, &record); err != nil {
			pm.logger.Warn("Failed to unmarshal vhost record %s: %v", key, err)
			continue
		}
		vhosts = append(vhosts, &record)
	}

	return vhosts, nil
}

// --- Exchange Operations ---

func (pm *PersistenceManager) SaveExchange(vhostName string, record *ExchangeRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling exchange record: %w", err)
	}
	return pm.storage.Set(ExchangeKey(vhostName, record.Name), data)
}

func (pm *PersistenceManager) DeleteExchange(vhostName, exchangeName string) error {
	return pm.storage.Delete(ExchangeKey(vhostName, exchangeName))
}

func (pm *PersistenceManager) LoadExchange(vhostName, exchangeName string) (*ExchangeRecord, error) {
	data, err := pm.storage.Get(ExchangeKey(vhostName, exchangeName))
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	var record ExchangeRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("unmarshalling exchange record: %w", err)
	}
	return &record, nil
}

func (pm *PersistenceManager) LoadAllExchanges(vhostName string) ([]*ExchangeRecord, error) {
	prefix := storage.KeyPrefixExchange + vhostName + ":"
	keys, err := pm.storage.Keys(prefix)
	if err != nil {
		return nil, fmt.Errorf("listing exchange keys: %w", err)
	}

	exchanges := make([]*ExchangeRecord, 0, len(keys))
	for _, key := range keys {
		data, err := pm.storage.Get(key)
		if err != nil {
			if err == storage.ErrKeyNotFound {
				continue
			}
			return nil, fmt.Errorf("getting exchange %s: %w", key, err)
		}

		var record ExchangeRecord
		if err := json.Unmarshal(data, &record); err != nil {
			pm.logger.Warn("Failed to unmarshal exchange record %s: %v", key, err)
			continue
		}
		exchanges = append(exchanges, &record)
	}

	return exchanges, nil
}

// --- Queue Operations ---

func (pm *PersistenceManager) SaveQueue(vhostName string, record *QueueRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling queue record: %w", err)
	}
	return pm.storage.Set(QueueKey(vhostName, record.Name), data)
}

func (pm *PersistenceManager) DeleteQueue(vhostName, queueName string) error {
	// Note: Caller is responsible for deleting messages and message index
	return pm.storage.Delete(QueueKey(vhostName, queueName))
}

func (pm *PersistenceManager) LoadQueue(vhostName, queueName string) (*QueueRecord, error) {
	data, err := pm.storage.Get(QueueKey(vhostName, queueName))
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	var record QueueRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("unmarshalling queue record: %w", err)
	}
	return &record, nil
}

func (pm *PersistenceManager) LoadAllQueues(vhostName string) ([]*QueueRecord, error) {
	prefix := storage.KeyPrefixQueue + vhostName + ":"
	keys, err := pm.storage.Keys(prefix)
	if err != nil {
		return nil, fmt.Errorf("listing queue keys: %w", err)
	}

	queues := make([]*QueueRecord, 0, len(keys))
	for _, key := range keys {
		data, err := pm.storage.Get(key)
		if err != nil {
			if err == storage.ErrKeyNotFound {
				continue
			}
			return nil, fmt.Errorf("getting queue %s: %w", key, err)
		}

		var record QueueRecord
		if err := json.Unmarshal(data, &record); err != nil {
			pm.logger.Warn("Failed to unmarshal queue record %s: %v", key, err)
			continue
		}
		queues = append(queues, &record)
	}

	return queues, nil
}

// --- Binding Operations ---

func (pm *PersistenceManager) SaveBinding(vhostName string, record *BindingRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling binding record: %w", err)
	}
	key := BindingKey(vhostName, record.Exchange, record.Queue, record.RoutingKey)
	return pm.storage.Set(key, data)
}

func (pm *PersistenceManager) DeleteBinding(vhostName, exchangeName, queueName, routingKey string) error {
	return pm.storage.Delete(BindingKey(vhostName, exchangeName, queueName, routingKey))
}

func (pm *PersistenceManager) LoadAllBindings(vhostName string) ([]*BindingRecord, error) {
	prefix := storage.KeyPrefixBinding + vhostName + ":"
	keys, err := pm.storage.Keys(prefix)
	if err != nil {
		return nil, fmt.Errorf("listing binding keys: %w", err)
	}

	bindings := make([]*BindingRecord, 0, len(keys))
	for _, key := range keys {
		data, err := pm.storage.Get(key)
		if err != nil {
			if err == storage.ErrKeyNotFound {
				continue
			}
			return nil, fmt.Errorf("getting binding %s: %w", key, err)
		}

		var record BindingRecord
		if err := json.Unmarshal(data, &record); err != nil {
			pm.logger.Warn("Failed to unmarshal binding record %s: %v", key, err)
			continue
		}
		bindings = append(bindings, &record)
	}

	return bindings, nil
}

// --- Message Operations ---

func (pm *PersistenceManager) SaveMessage(vhostName, queueName string, record *MessageRecord) error {
	// Serialize message saves per queue to prevent index corruption
	queueMutex := pm.getQueueMutex(vhostName, queueName)
	queueMutex.Lock()
	defer queueMutex.Unlock()

	// Assign sequence number if not set
	if record.Sequence == 0 {
		record.Sequence = pm.messageSeq.Add(1)
	}

	// First, load the current message index
	currentIndex, err := pm.loadMessageIndex(vhostName, queueName)
	if err != nil && err != storage.ErrKeyNotFound {
		return fmt.Errorf("loading message index: %w", err)
	}

	// If index doesn't exist, create new one
	if currentIndex == nil {
		currentIndex = &MessageIndex{
			QueueName: queueName,
			Messages:  []string{},
		}
	}

	// Serialize message
	messageData, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling message record: %w", err)
	}

	// Create a new index with the appended message
	updatedIndex := &MessageIndex{
		QueueName: currentIndex.QueueName,
		Messages:  make([]string, len(currentIndex.Messages)+1),
	}
	copy(updatedIndex.Messages, currentIndex.Messages)
	updatedIndex.Messages[len(currentIndex.Messages)] = record.ID

	// Serialize the updated index
	indexData, err := json.Marshal(updatedIndex)
	if err != nil {
		return fmt.Errorf("marshaling message index: %w", err)
	}

	// Prepare sequence counter data
	seqData, err := json.Marshal(pm.messageSeq.Load())
	if err != nil {
		return fmt.Errorf("marshaling sequence counter: %w", err)
	}

	// Now save message, index, and sequence counter in a single batch operation
	items := map[string][]byte{
		MessageKey(vhostName, queueName, record.ID): messageData,
		MessageIndexKey(vhostName, queueName):       indexData,
		storage.KeySeqCounter:                       seqData,
	}

	if err := pm.storage.SetBatch(items); err != nil {
		return fmt.Errorf("saving message, index, and sequence counter: %w", err)
	}

	return nil
}

func (pm *PersistenceManager) DeleteMessage(vhostName, queueName, messageId string) error {
	queueMutex := pm.getQueueMutex(vhostName, queueName)
	queueMutex.Lock()
	defer queueMutex.Unlock()

	tx, err := pm.BeginTransaction()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	if err := tx.DeleteMessage(vhostName, queueName, messageId); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (pm *PersistenceManager) DeleteAllQueueMessages(vhostName, queueName string) error {
	queueMutex := pm.getQueueMutex(vhostName, queueName)
	queueMutex.Lock()
	defer queueMutex.Unlock()

	tx, err := pm.BeginTransaction()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	// Get all message IDs from index
	index, err := pm.loadMessageIndex(vhostName, queueName)
	if err != nil && err != storage.ErrKeyNotFound {
		tx.Rollback()
		return fmt.Errorf("loading message index: %w", err)
	}

	if index != nil && len(index.Messages) > 0 {
		// Delete all messages in the transaction
		for _, msgId := range index.Messages {
			if err := tx.DeleteMessage(vhostName, queueName, msgId); err != nil {
				tx.Rollback()
				return fmt.Errorf("deleting message %s: %w", msgId, err)
			}
		}
	}

	// Delete the message index itself using the proper method
	if err := tx.DeleteMessageIndex(vhostName, queueName); err != nil {
		tx.Rollback()
		return fmt.Errorf("deleting message index: %w", err)
	}

	return tx.Commit()
}

func (pm *PersistenceManager) LoadQueueMessages(vhostName, queueName string) ([]*MessageRecord, error) {
	// Get message index
	index, err := pm.loadMessageIndex(vhostName, queueName)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return []*MessageRecord{}, nil
		}
		return nil, fmt.Errorf("loading message index: %w", err)
	}

	if index == nil || len(index.Messages) == 0 {
		return []*MessageRecord{}, nil
	}

	// Load messages in order
	messages := make([]*MessageRecord, 0, len(index.Messages))
	for _, messageId := range index.Messages {
		data, err := pm.storage.Get(MessageKey(vhostName, queueName, messageId))
		if err != nil {
			if err == storage.ErrKeyNotFound {
				pm.logger.Warn("Message %s in index but not found in storage", messageId)
				continue
			}
			return nil, fmt.Errorf("getting message %s: %w", messageId, err)
		}

		var record MessageRecord
		if err := json.Unmarshal(data, &record); err != nil {
			pm.logger.Warn("Failed to unmarshal message record %s: %v", messageId, err)
			continue
		}

		messages = append(messages, &record)
	}

	return messages, nil
}

// --- Bulk Operations ---

func (pm *PersistenceManager) DeleteAllVHostData(vhostName string) error {
	tx, err := pm.storage.BeginTx()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	// Delete vhost record
	if err := tx.Delete(VHostKey(vhostName)); err != nil {
		tx.Rollback()
		return fmt.Errorf("deleting vhost record: %w", err)
	}

	// Delete all exchanges
	exchangeKeys, err := pm.storage.Keys(storage.KeyPrefixExchange + vhostName + ":")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("listing exchanges: %w", err)
	}
	if err := tx.DeleteBatch(exchangeKeys); err != nil {
		tx.Rollback()
		return fmt.Errorf("deleting exchanges: %w", err)
	}

	// Delete all queues
	queueKeys, err := pm.storage.Keys(storage.KeyPrefixQueue + vhostName + ":")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("listing queues: %w", err)
	}
	if err := tx.DeleteBatch(queueKeys); err != nil {
		tx.Rollback()
		return fmt.Errorf("deleting queues: %w", err)
	}

	// Delete all bindings
	bindingKeys, err := pm.storage.Keys(storage.KeyPrefixBinding + vhostName + ":")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("listing bindings: %w", err)
	}
	if err := tx.DeleteBatch(bindingKeys); err != nil {
		tx.Rollback()
		return fmt.Errorf("deleting bindings: %w", err)
	}

	// Delete all messages
	messageKeys, err := pm.storage.Keys(storage.KeyPrefixMessage + vhostName + ":")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("listing messages: %w", err)
	}
	if err := tx.DeleteBatch(messageKeys); err != nil {
		tx.Rollback()
		return fmt.Errorf("deleting messages: %w", err)
	}

	// Delete all message indices
	indexKeys, err := pm.storage.Keys(storage.KeyPrefixMsgIndex + vhostName + ":")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("listing message indices: %w", err)
	}
	if err := tx.DeleteBatch(indexKeys); err != nil {
		tx.Rollback()
		return fmt.Errorf("deleting message indices: %w", err)
	}

	return tx.Commit()
}

func (pm *PersistenceManager) DeleteMessagesBatch(vhostName, queueName string, messageIds []string) error {
	if len(messageIds) == 0 {
		return nil
	}

	tx, err := pm.BeginTransaction()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	// Delete all messages in the transaction
	for _, msgId := range messageIds {
		if err := tx.DeleteMessage(vhostName, queueName, msgId); err != nil {
			tx.Rollback()
			return fmt.Errorf("deleting message %s: %w", msgId, err)
		}
	}

	return tx.Commit()
}

// --- Internal helper methods ---

func (pm *PersistenceManager) loadMessageIndex(vhostName, queueName string) (*MessageIndex, error) {
	data, err := pm.storage.Get(MessageIndexKey(vhostName, queueName))
	if err != nil {
		return nil, err
	}

	var index MessageIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, fmt.Errorf("unmarshalling message index: %w", err)
	}
	return &index, nil
}

// PersistenceTransaction wraps storage transaction with helper methods
type PersistenceTransaction interface {
	SaveExchange(vhostName string, record *ExchangeRecord) error
	DeleteExchange(vhostName, exchangeName string) error

	SaveQueue(vhostName string, record *QueueRecord) error
	DeleteQueue(vhostName, queueName string) error

	SaveBinding(vhostName string, record *BindingRecord) error
	DeleteBinding(vhostName, exchangeName, queueName, routingKey string) error

	SaveMessage(vhostName, queueName string, record *MessageRecord) error
	DeleteMessage(vhostName, queueName, messageId string) error

	DeleteMessageIndex(vhostName, queueName string) error

	Commit() error
	Rollback() error
}

//--- ---

type persistenceTransaction struct {
	tx storage.StorageTransaction
	pm *PersistenceManager

	messageIndexUpdates map[string]*MessageIndex // key: "vhost:queue"
	messageIndexCache   map[string]*MessageIndex // cache of loaded indices
}

func (pt *persistenceTransaction) SaveExchange(vhostName string, record *ExchangeRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling exchange record: %w", err)
	}
	return pt.tx.Set(ExchangeKey(vhostName, record.Name), data)
}

func (pt *persistenceTransaction) DeleteExchange(vhostName, exchangeName string) error {
	return pt.tx.Delete(ExchangeKey(vhostName, exchangeName))
}

func (pt *persistenceTransaction) SaveQueue(vhostName string, record *QueueRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling queue record: %w", err)
	}
	return pt.tx.Set(QueueKey(vhostName, record.Name), data)
}

func (pt *persistenceTransaction) DeleteQueue(vhostName, queueName string) error {
	return pt.tx.Delete(QueueKey(vhostName, queueName))
}

func (pt *persistenceTransaction) SaveBinding(vhostName string, record *BindingRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling binding record: %w", err)
	}
	return pt.tx.Set(BindingKey(vhostName, record.Exchange, record.Queue, record.RoutingKey), data)
}

func (pt *persistenceTransaction) DeleteBinding(vhostName, exchangeName, queueName, routingKey string) error {
	return pt.tx.Delete(BindingKey(vhostName, exchangeName, queueName, routingKey))
}

func (pt *persistenceTransaction) SaveMessage(vhostName, queueName string, record *MessageRecord) error {
	// Assign sequence number if not set
	if record.Sequence == 0 {
		record.Sequence = pt.pm.messageSeq.Add(1)
	}

	// Serialize message
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling message record: %w", err)
	}

	// Save message to transaction
	messageKey := MessageKey(vhostName, queueName, record.ID)
	if err := pt.tx.Set(messageKey, data); err != nil {
		return fmt.Errorf("saving message to transaction: %w", err)
	}

	// Update message index in transaction
	indexKey := vhostName + ":" + queueName
	index, err := pt.getOrLoadMessageIndex(vhostName, queueName)
	if err != nil {
		return fmt.Errorf("loading message index: %w", err)
	}

	// Add message ID to index
	index.Messages = append(index.Messages, record.ID)

	// Mark index as updated
	pt.messageIndexUpdates[indexKey] = index

	return nil
}

func (pt *persistenceTransaction) DeleteMessage(vhostName, queueName, messageId string) error {
	// Delete message from transaction
	messageKey := MessageKey(vhostName, queueName, messageId)
	if err := pt.tx.Delete(messageKey); err != nil {
		return fmt.Errorf("deleting message from transaction: %w", err)
	}

	// Update message index in transaction
	indexKey := vhostName + ":" + queueName
	index, err := pt.getOrLoadMessageIndex(vhostName, queueName)
	if err != nil {
		return fmt.Errorf("loading message index: %w", err)
	}

	// Remove message ID from index
	newMessages := make([]string, 0, len(index.Messages)-1)
	for _, id := range index.Messages {
		if id != messageId {
			newMessages = append(newMessages, id)
		}
	}
	index.Messages = newMessages

	// Mark index as updated
	pt.messageIndexUpdates[indexKey] = index

	return nil
}

func (pt *persistenceTransaction) Commit() error {
	// First, save all message index updates
	for key, index := range pt.messageIndexUpdates {
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid index key format: %s", key)
		}
		vhostName, queueName := parts[0], parts[1]

		// Serialize index
		data, err := json.Marshal(index)
		if err != nil {
			return fmt.Errorf("marshaling message index for %s: %w", key, err)
		}

		// Save to transaction
		indexKey := MessageIndexKey(vhostName, queueName)
		if err := pt.tx.Set(indexKey, data); err != nil {
			return fmt.Errorf("saving message index for %s: %w", key, err)
		}
	}

	// Always save the current sequence counter with the transaction
	seqData, err := json.Marshal(pt.pm.messageSeq.Load())
	if err != nil {
		return fmt.Errorf("marshaling sequence counter: %w", err)
	}
	if err := pt.tx.Set(storage.KeySeqCounter, seqData); err != nil {
		return fmt.Errorf("saving sequence counter: %w", err)
	}

	// Commit the underlying storage transaction
	if err := pt.tx.Commit(); err != nil {
		return fmt.Errorf("committing storage transaction: %w", err)
	}

	// Clear transaction state
	pt.messageIndexUpdates = nil
	pt.messageIndexCache = nil
	pt.tx = nil

	return nil
}

func (pt *persistenceTransaction) Rollback() error {
	err := pt.tx.Rollback()

	// Clear transaction state regardless of rollback result
	pt.messageIndexUpdates = nil
	pt.messageIndexCache = nil
	pt.tx = nil

	return err
}

// Helper method to get or load message index
func (pt *persistenceTransaction) getOrLoadMessageIndex(vhostName, queueName string) (*MessageIndex, error) {
	cacheKey := vhostName + ":" + queueName

	// Check if we already have it in updates
	if index, exists := pt.messageIndexUpdates[cacheKey]; exists {
		return index, nil
	}

	// Check cache
	if index, exists := pt.messageIndexCache[cacheKey]; exists {
		// Make a copy to avoid modifying the cached version
		indexCopy := &MessageIndex{
			QueueName: index.QueueName,
			Messages:  make([]string, len(index.Messages)),
		}
		copy(indexCopy.Messages, index.Messages)
		return indexCopy, nil
	}

	// Load from storage via transaction
	indexKey := MessageIndexKey(vhostName, queueName)
	data, err := pt.tx.Get(indexKey)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			// Create new index
			index := &MessageIndex{
				QueueName: queueName,
				Messages:  []string{},
			}
			pt.messageIndexCache[cacheKey] = index
			return index, nil
		}
		return nil, err
	}

	// Unmarshal existing index
	var index MessageIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, fmt.Errorf("unmarshalling message index: %w", err)
	}

	// Cache it
	pt.messageIndexCache[cacheKey] = &index

	// Return a copy
	indexCopy := &MessageIndex{
		QueueName: index.QueueName,
		Messages:  make([]string, len(index.Messages)),
	}
	copy(indexCopy.Messages, index.Messages)

	return indexCopy, nil
}

func (pt *persistenceTransaction) DeleteMessageIndex(vhostName, queueName string) error {
	indexKey := MessageIndexKey(vhostName, queueName)

	// Remove from updates if it exists
	cacheKey := vhostName + ":" + queueName
	delete(pt.messageIndexUpdates, cacheKey)
	delete(pt.messageIndexCache, cacheKey)

	// Delete from transaction
	return pt.tx.Delete(indexKey)
}
