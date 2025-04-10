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
)

type StorageConfig struct {
	Type       StorageType
	FilePath   string // Used for BTree storage
	DataDir    string // Used for JSON storage
	FilePrefix string // Used for JSON storage
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
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.Type)
	}
}
