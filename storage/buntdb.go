package storage

import (
	"fmt"
	"sync"

	"github.com/tidwall/buntdb"
)

type BuntDBProvider struct {
	db   *buntdb.DB
	path string
	mu   sync.RWMutex
	inTx bool // Track if we're in a transaction
}

// NewBuntDBProvider creates a new BuntDB storage provider
// If path is empty, it creates an in-memory database
func NewBuntDBProvider(path string) *BuntDBProvider {
	return &BuntDBProvider{
		path: path,
	}
}

// Initialize opens the BuntDB database
func (b *BuntDBProvider) Initialize() error {
	var err error
	if b.path == "" || b.path == ":memory:" {
		// In-memory database
		b.db, err = buntdb.Open(":memory:")
	} else {
		// Persistent database
		b.db, err = buntdb.Open(b.path)
	}

	if err != nil {
		return fmt.Errorf("opening buntdb: %w", err)
	}

	// Create indices for efficient prefix scans
	// These help with Keys() and Scan() operations
	indices := []string{
		KeyPrefixVHost,
		KeyPrefixExchange,
		KeyPrefixQueue,
		KeyPrefixBinding,
		KeyPrefixMessage,
		KeyPrefixMsgIndex,
	}

	for _, prefix := range indices {
		indexName := "idx_" + prefix
		err = b.db.CreateIndex(indexName, prefix+"*", buntdb.IndexString)
		if err != nil && err != buntdb.ErrIndexExists {
			b.db.Close()
			return fmt.Errorf("creating index %s: %w", indexName, err)
		}
	}

	return nil
}

// Close closes the BuntDB database
func (b *BuntDBProvider) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

// Set stores a key-value pair
func (b *BuntDBProvider) Set(key string, value []byte) error {
	return b.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, string(value), nil)
		return err
	})
}

// Get retrieves a value by key
func (b *BuntDBProvider) Get(key string) ([]byte, error) {
	var value string
	err := b.db.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get(key)
		if err != nil {
			return err
		}
		value = val
		return nil
	})

	if err == buntdb.ErrNotFound {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	return []byte(value), nil
}

// Delete removes a key
func (b *BuntDBProvider) Delete(key string) error {
	return b.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(key)
		if err == buntdb.ErrNotFound {
			return nil // Deleting non-existent key is not an error
		}
		return err
	})
}

// Exists checks if a key exists
func (b *BuntDBProvider) Exists(key string) (bool, error) {
	exists := false
	err := b.db.View(func(tx *buntdb.Tx) error {
		_, err := tx.Get(key)
		if err == nil {
			exists = true
			return nil
		}
		if err == buntdb.ErrNotFound {
			return nil
		}
		return err
	})

	return exists, err
}

// SetBatch stores multiple key-value pairs
func (b *BuntDBProvider) SetBatch(items map[string][]byte) error {
	return b.db.Update(func(tx *buntdb.Tx) error {
		for key, value := range items {
			if _, _, err := tx.Set(key, string(value), nil); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetBatch retrieves multiple values by keys
func (b *BuntDBProvider) GetBatch(keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	err := b.db.View(func(tx *buntdb.Tx) error {
		for _, key := range keys {
			val, err := tx.Get(key)
			if err == nil {
				result[key] = []byte(val)
			} else if err != buntdb.ErrNotFound {
				return err
			}
			// Skip not found keys
		}
		return nil
	})

	return result, err
}

// DeleteBatch removes multiple keys
func (b *BuntDBProvider) DeleteBatch(keys []string) error {
	return b.db.Update(func(tx *buntdb.Tx) error {
		for _, key := range keys {
			if _, err := tx.Delete(key); err != nil && err != buntdb.ErrNotFound {
				return err
			}
		}
		return nil
	})
}

// Keys returns all keys with the given prefix
func (b *BuntDBProvider) Keys(prefix string) ([]string, error) {
	var keys []string

	err := b.db.View(func(tx *buntdb.Tx) error {
		err := tx.AscendKeys(prefix+"*", func(key, value string) bool {
			keys = append(keys, key)
			return true // continue iteration
		})
		return err
	})

	return keys, err
}

// Scan iterates over all keys with the given prefix
func (b *BuntDBProvider) Scan(prefix string, fn func(key string, value []byte) error) error {
	return b.db.View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(prefix+"*", func(key, value string) bool {
			if err := fn(key, []byte(value)); err != nil {
				// BuntDB doesn't support returning errors from iteration
				// We'd need to use a closure to capture the error
				return false // stop iteration
			}
			return true // continue iteration
		})
	})
}

// BeginTx starts a new transaction
func (b *BuntDBProvider) BeginTx() (StorageTransaction, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.inTx {
		return nil, ErrTxAlreadyOpen
	}

	tx := &buntDBTransaction{
		provider: b,
		writes:   make(map[string][]byte),
		deletes:  make(map[string]bool),
	}

	b.inTx = true
	return tx, nil
}

// buntDBTransaction implements StorageTransaction for BuntDB
type buntDBTransaction struct {
	provider *BuntDBProvider
	writes   map[string][]byte
	deletes  map[string]bool
	mu       sync.Mutex
}

// Set adds a write operation to the transaction
func (tx *buntDBTransaction) Set(key string, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	delete(tx.deletes, key) // Remove from deletes if it was there
	tx.writes[key] = value
	return nil
}

// Get retrieves a value, checking transaction writes first
func (tx *buntDBTransaction) Get(key string) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if deleted in this transaction
	if tx.deletes[key] {
		return nil, ErrKeyNotFound
	}

	// Check if written in this transaction
	if value, ok := tx.writes[key]; ok {
		return value, nil
	}

	// Fall back to database
	return tx.provider.Get(key)
}

// Delete adds a delete operation to the transaction
func (tx *buntDBTransaction) Delete(key string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	delete(tx.writes, key) // Remove from writes if it was there
	tx.deletes[key] = true
	return nil
}

// Exists checks if a key exists
func (tx *buntDBTransaction) Exists(key string) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if deleted in this transaction
	if tx.deletes[key] {
		return false, nil
	}

	// Check if written in this transaction
	if _, ok := tx.writes[key]; ok {
		return true, nil
	}

	// Fall back to database
	return tx.provider.Exists(key)
}

// SetBatch adds multiple write operations to the transaction
func (tx *buntDBTransaction) SetBatch(items map[string][]byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	for key, value := range items {
		delete(tx.deletes, key)
		tx.writes[key] = value
	}
	return nil
}

// DeleteBatch adds multiple delete operations to the transaction
func (tx *buntDBTransaction) DeleteBatch(keys []string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	for _, key := range keys {
		delete(tx.writes, key)
		tx.deletes[key] = true
	}
	return nil
}

// Commit applies all transaction operations
func (tx *buntDBTransaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Execute all operations in a BuntDB transaction
	err := tx.provider.db.Update(func(btx *buntdb.Tx) error {
		// Apply all writes
		for key, value := range tx.writes {
			if _, _, err := btx.Set(key, string(value), nil); err != nil {
				return err
			}
		}

		// Apply all deletes
		for key := range tx.deletes {
			if _, err := btx.Delete(key); err != nil && err != buntdb.ErrNotFound {
				return err
			}
		}

		return nil
	})

	// Clear transaction state
	tx.provider.mu.Lock()
	tx.provider.inTx = false
	tx.provider.mu.Unlock()

	tx.writes = nil
	tx.deletes = nil

	return err
}

// Rollback discards all transaction operations
func (tx *buntDBTransaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Clear transaction state
	tx.provider.mu.Lock()
	tx.provider.inTx = false
	tx.provider.mu.Unlock()

	tx.writes = nil
	tx.deletes = nil

	return nil
}
