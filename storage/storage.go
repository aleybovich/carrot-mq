package storage

import "errors"

// Common errors
var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrTxNotStarted  = errors.New("transaction not started")
	ErrTxAlreadyOpen = errors.New("transaction already open")
)

const (
	KeyPrefixVHost    = "vhost:"
	KeyPrefixExchange = "exchange:"
	KeyPrefixQueue    = "queue:"
	KeyPrefixBinding  = "binding:"
	KeyPrefixMessage  = "message:"
	KeyPrefixMsgIndex = "msgidx:" // Message index by queue
	KeySeqCounter     = "system:msgseqno" // Global message sequence counter
)

// StorageProvider is the low-level storage abstraction
// This is what different backends (BuntDB, BoltDB, Redis, etc.) implement
type StorageProvider interface {
	// Initialize prepares the storage backend
	Initialize() error

	// Close cleanly shuts down the storage backend
	Close() error

	// Basic operations
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	Exists(key string) (bool, error)

	// Batch operations
	SetBatch(items map[string][]byte) error
	GetBatch(keys []string) (map[string][]byte, error)
	DeleteBatch(keys []string) error

	// Scanning/iteration
	Keys(prefix string) ([]string, error)
	Scan(prefix string, fn func(key string, value []byte) error) error

	// Transaction support
	BeginTx() (StorageTransaction, error)
}

// StorageTransaction represents a storage transaction
type StorageTransaction interface {
	// Basic operations within transaction
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	Exists(key string) (bool, error)

	// Batch operations within transaction
	SetBatch(items map[string][]byte) error
	DeleteBatch(keys []string) error

	// Transaction control
	Commit() error
	Rollback() error
}
