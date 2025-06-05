package main

import (
	"amqp-go/storage"
	"fmt"
)

type StorageType string

const (
	StorageTypeNone   StorageType = "none"   // No persistence
	StorageTypeMemory StorageType = "memory" // In-memory (using BuntDB)
	StorageTypeBuntDB StorageType = "buntdb" // Persistent BuntDB
	// Future providers
	// StorageTypeBoltDB StorageType = "boltdb"
	// StorageTypeRedis  StorageType = "redis"
	// StorageTypeEtcd   StorageType = "etcd"
)

type StorageConfig struct {
	Type StorageType

	// BuntDB specific config
	BuntDB *BuntDBConfig

	// Future providers
	// BoltDB *BoltDBConfig
	// Redis  *RedisConfig
	// Etcd   *EtcdConfig
}

type BuntDBConfig struct {
	Path string // empty or ":memory:" for in-memory

	// Future options
	// SyncPolicy   int  // 0: never, 1: everysec, 2: always
	// AutoShrink   bool // Auto shrink the database file
}

// Validate ensures the storage configuration is valid
func (sc StorageConfig) Validate() error {
	switch sc.Type {
	case StorageTypeNone, StorageTypeMemory:
		// No validation needed
		return nil

	case StorageTypeBuntDB:
		if sc.BuntDB == nil {
			return fmt.Errorf("BuntDB config is required for BuntDB storage type")
		}
		// Path can be empty (defaults to :memory:)
		return nil

	case "":
		return fmt.Errorf("storage type not specified")

	default:
		return fmt.Errorf("unknown storage type: %s", sc.Type)
	}
}

// WithStorage configures the storage provider for the server
func WithStorage(config StorageConfig) ServerOption {
	return func(s *Server) {
		// Validate config
		if err := config.Validate(); err != nil {
			s.Warn("Invalid storage config: %v, persistence disabled", err)
			return
		}

		var provider storage.StorageProvider

		switch config.Type {
		case StorageTypeNone:
			// No persistence
			s.Info("Persistence disabled")
			return

		case StorageTypeMemory:
			provider = storage.NewBuntDBProvider(":memory:")
			s.Info("Using in-memory storage (BuntDB)")

		case StorageTypeBuntDB:
			path := config.BuntDB.Path
			if path == "" {
				path = ":memory:"
			}
			provider = storage.NewBuntDBProvider(path)
			if path == ":memory:" {
				s.Info("Using in-memory BuntDB storage")
			} else {
				s.Info("Using persistent BuntDB storage at: %s", path)
			}

			// Future providers
			// case StorageTypeBoltDB:
			//     provider = storage.NewBoltDBProvider(config.BoltDB)
			//     s.Info("Using BoltDB storage at: %s", config.BoltDB.Path)
			//
			// case StorageTypeRedis:
			//     provider = storage.NewRedisProvider(config.Redis)
			//     s.Info("Using Redis storage at: %s", config.Redis.Address)
		}

		if provider != nil {
			s.persistenceManager = NewPersistenceManager(provider, s)
		}
	}
}

// Convenience functions for common configurations

// WithInMemoryStorage configures in-memory storage using BuntDB
func WithInMemoryStorage() ServerOption {
	return WithStorage(StorageConfig{
		Type: StorageTypeMemory,
	})
}

// WithBuntDBStorage configures persistent BuntDB storage
func WithBuntDBStorage(path string) ServerOption {
	return WithStorage(StorageConfig{
		Type: StorageTypeBuntDB,
		BuntDB: &BuntDBConfig{
			Path: path,
		},
	})
}

// Optional WithNoStorage explicitly disables persistence
func WithNoStorage() ServerOption {
	return WithStorage(StorageConfig{
		Type: StorageTypeNone,
	})
}

// Using  custom storage provider directly
func WithStorageProvider(provider storage.StorageProvider) ServerOption {
	return func(s *Server) {
		if provider != nil {
			s.persistenceManager = NewPersistenceManager(provider, s)
		}
	}
}
