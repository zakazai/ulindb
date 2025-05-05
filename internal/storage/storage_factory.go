package storage

import (
	"fmt"
	"path/filepath"
	"time"
	
	"github.com/zakazai/ulin-db/internal/types"
)

// StorageType represents different storage backends supported by the database.
type StorageType string

const (
	// InMemoryStorageType is a volatile in-memory storage (for testing).
	InMemoryStorageType StorageType = "memory"

	// JSONStorageType stores data in JSON files with one file per table.
	JSONStorageType StorageType = "json"

	// BTreeStorageType stores data in a B+tree structure for OLTP workloads.
	BTreeStorageType StorageType = "btree"

	// ParquetStorageType stores data in column-oriented Parquet files for OLAP workloads.
	ParquetStorageType StorageType = "parquet"
)

// StorageConfig provides configuration options for creating storage backends.
type StorageConfig struct {
	// Type specifies which storage implementation to use.
	Type StorageType

	// FilePath is the path to the BTree storage file.
	FilePath string

	// DataDir is the directory for JSON and Parquet storage files.
	DataDir string

	// FilePrefix is used as a prefix for JSON storage files.
	FilePrefix string

	// SyncFromBTree enables synchronizing Parquet storage from BTree.
	SyncFromBTree bool

	// SyncInterval controls how frequently Parquet syncs from BTree.
	SyncInterval time.Duration
	
	// LogLevel controls the verbosity of logging.
	LogLevel types.LogLevel
}

// NewStorage creates a new storage instance based on the provided configuration.
// It will instantiate the appropriate storage implementation based on the config.Type
// and initialize it with the relevant parameters from the config.
func NewStorage(config StorageConfig) (Storage, error) {
	// Set the global log level from config
	types.GlobalLogger.SetLevel(config.LogLevel)
	
	switch config.Type {
	case InMemoryStorageType:
		return NewInMemoryStorage(), nil
	case JSONStorageType:
		if config.DataDir == "" {
			// Default to the directory of FilePath if provided
			if config.FilePath != "" {
				config.DataDir = filepath.Dir(config.FilePath)
			} else {
				return nil, fmt.Errorf("data directory is required for JSON storage")
			}
		}

		if config.FilePrefix == "" {
			config.FilePrefix = "db_" // Default prefix
		}

		return NewJSONStorage(config.DataDir, config.FilePrefix)
	case BTreeStorageType:
		if config.FilePath == "" {
			return nil, fmt.Errorf("file path is required for B-tree storage")
		}
		return NewBTreeStorage(config.FilePath)
	case ParquetStorageType:
		if config.DataDir == "" {
			return nil, fmt.Errorf("data directory is required for Parquet storage")
		}

		parquetStorage, err := NewParquetStorage(config.DataDir)
		if err != nil {
			return nil, err
		}

		// Configure sync from BTree if enabled
		if config.SyncFromBTree {
			// Note: This requires the caller to later set the BTree source
			// using SetBTreeSource method after creating both storages

			// Set sync interval if specified
			if config.SyncInterval > 0 {
				parquetStorage.SetSyncInterval(config.SyncInterval)
			}
		}

		return parquetStorage, nil
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.Type)
	}
}

// CreateHybridStorage creates a hybrid storage system with BTree for OLTP and Parquet for OLAP.
// This provides a complete storage solution that automatically routes queries to the optimal
// storage engine based on the query patterns. It also sets up background synchronization to
// keep the OLAP storage updated with data from the OLTP storage.
func CreateHybridStorage(config StorageConfig) (*HybridStorage, error) {
	// Set the global log level from config
	types.GlobalLogger.SetLevel(config.LogLevel)
	
	if config.Type != BTreeStorageType {
		return nil, fmt.Errorf("hybrid storage requires BTree as the primary storage type")
	}

	// Create BTree storage
	bTreeStorage, err := NewBTreeStorage(config.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create BTree storage: %w", err)
	}

	// Create Parquet storage
	parquetStorage, err := NewParquetStorage(config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet storage: %w", err)
	}

	// Configure Parquet to sync from BTree
	parquetStorage.SetBTreeSource(bTreeStorage)

	// Set sync interval if specified
	if config.SyncInterval > 0 {
		parquetStorage.SetSyncInterval(config.SyncInterval)
	}

	// Start sync worker
	parquetStorage.StartSyncWorker()

	// Create hybrid storage
	return &HybridStorage{
		oltp: bTreeStorage,
		olap: parquetStorage,
	}, nil
}
