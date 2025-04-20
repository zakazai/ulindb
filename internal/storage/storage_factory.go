package storage

import (
	"fmt"
	"path/filepath"
)

type StorageType string

const (
	InMemoryStorageType StorageType = "memory"
	JSONStorageType     StorageType = "json"
	BTreeStorageType    StorageType = "btree"
	ParquetStorageType  StorageType = "parquet"
)

type StorageConfig struct {
	Type          StorageType
	FilePath      string        // Used for BTree storage
	DataDir       string        // Used for JSON and Parquet storage
	FilePrefix    string        // Used for JSON storage
	SyncFromBTree bool          // Used for Parquet storage to sync from BTree
	SyncInterval  time.Duration // Used for Parquet storage sync interval
}

// NewStorage creates a new storage instance based on the provided configuration
func NewStorage(config StorageConfig) (Storage, error) {
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

// CreateHybridStorage creates a hybrid storage system with BTree for OLTP and Parquet for OLAP
func CreateHybridStorage(config StorageConfig) (*HybridStorage, error) {
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
