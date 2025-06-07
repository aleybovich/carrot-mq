package config

import (
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
