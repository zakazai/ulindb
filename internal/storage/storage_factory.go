package ulindb

import "fmt"

type StorageType string

const (
	InMemoryStorageType StorageType = "memory"
	JSONStorageType     StorageType = "json"
	BTreeStorageType    StorageType = "btree"
)

type StorageConfig struct {
	Type     StorageType
	FilePath string // Used for JSON and BTree storage
}

// NewStorage creates a new storage instance based on the provided configuration
func NewStorage(config StorageConfig) (Storage, error) {
	switch config.Type {
	case InMemoryStorageType:
		return NewInMemoryStorage(), nil
	case JSONStorageType:
		if config.FilePath == "" {
			return nil, fmt.Errorf("file path is required for JSON storage")
		}
		return NewJSONStorage(config.FilePath)
	case BTreeStorageType:
		if config.FilePath == "" {
			return nil, fmt.Errorf("file path is required for B-tree storage")
		}
		return NewBTreeStorage(config.FilePath)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.Type)
	}
}
