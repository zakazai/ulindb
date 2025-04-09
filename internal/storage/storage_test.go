package storage_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zakazai/ulin-db/internal/storage"
	"github.com/zakazai/ulin-db/internal/types"
)

func TestInMemoryStorage(t *testing.T) {
	storage := storage.NewInMemoryStorage()

	// Test CreateTable
	table := &types.Table{
		Name: "test",
		Columns: []types.ColumnDefinition{
			{name: "id", typ: "INT"},
			{name: "name", typ: "TEXT"},
		},
	}
	err := storage.CreateTable(table)
	assert.NoError(t, err)

	// Test CreateTable duplicate
	err = storage.CreateTable(table)
	assert.Error(t, err)

	// Test Insert
	err = storage.Insert("test", map[string]interface{}{
		"id":   1,
		"name": "test1",
	})
	assert.NoError(t, err)

	// Test Insert into non-existent table
	err = storage.Insert("nonexistent", map[string]interface{}{})
	assert.Error(t, err)

	// Test Select
	rows, err := storage.Select("test", []string{"*"}, "")
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0]["id"])
	assert.Equal(t, "test1", rows[0]["name"])

	// Test Select with where
	rows, err = storage.Select("test", []string{"*"}, "id = 1")
	assert.NoError(t, err)
	assert.Len(t, rows, 1)

	// Test Update
	err = storage.Update("test", map[string]interface{}{
		"name": "test2",
	}, "id = 1")
	assert.NoError(t, err)

	rows, err = storage.Select("test", []string{"*"}, "")
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "test2", rows[0]["name"])

	// Test Delete
	err = storage.Delete("test", "id = 1")
	assert.NoError(t, err)

	rows, err = storage.Select("test", []string{"*"}, "")
	assert.NoError(t, err)
	assert.Len(t, rows, 0)
}

func TestJSONStorage(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "testdb.json")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Create storage
	storage, err := storage.NewJSONStorage(tmpFile.Name())
	assert.NoError(t, err)

	// Test CreateTable
	table := &types.Table{
		Name: "test",
		Columns: []types.ColumnDefinition{
			{name: "id", typ: "INT"},
			{name: "name", typ: "TEXT"},
		},
	}
	err = storage.CreateTable(table)
	assert.NoError(t, err)

	// Test Insert
	err = storage.Insert("test", map[string]interface{}{
		"id":   1,
		"name": "test1",
	})
	assert.NoError(t, err)

	// Close storage
	err = storage.Close()
	assert.NoError(t, err)

	// Create new storage instance to test persistence
	storage, err = storage.NewJSONStorage(tmpFile.Name())
	assert.NoError(t, err)

	// Test that data was persisted
	rows, err := storage.Select("test", []string{"*"}, "")
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0]["id"])
	assert.Equal(t, "test1", rows[0]["name"])

	// Test Update
	err = storage.Update("test", map[string]interface{}{
		"name": "test2",
	}, "id = 1")
	assert.NoError(t, err)

	// Close storage again
	err = storage.Close()
	assert.NoError(t, err)

	// Create another storage instance
	storage, err = storage.NewJSONStorage(tmpFile.Name())
	storage, err = NewJSONStorage(tmpFile.Name())
	assert.NoError(t, err)

	// Test that update was persisted
	rows, err = storage.Select("test", []string{"*"}, "")
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "test2", rows[0]["name"])

	// Test Delete
	err = storage.Delete("test", "id = 1")
	assert.NoError(t, err)

	// Close storage
	err = storage.Close()
	assert.NoError(t, err)

	// Create final storage instance
	storage, err = NewJSONStorage(tmpFile.Name())
	assert.NoError(t, err)

	// Test that delete was persisted
	rows, err = storage.Select("test", []string{"*"}, "")
	assert.NoError(t, err)
	assert.Len(t, rows, 0)
}

func TestStorageEdgeCases(t *testing.T) {
	storage := NewInMemoryStorage()

	// Test Select from non-existent table
	_, err := storage.Select("nonexistent", []string{"*"}, "")
	assert.Error(t, err)

	// Test Update non-existent table
	err = storage.Update("nonexistent", map[string]interface{}{}, "")
	assert.Error(t, err)

	// Test Delete from non-existent table
	err = storage.Delete("nonexistent", "")
	assert.Error(t, err)

	// Test Insert with missing columns
	table := &Table{
		Name: "test",
		Columns: []ColumnDefinition{
			{name: "id", typ: "INT", Nullable: true},
			{name: "name", typ: "TEXT", Nullable: false}, // name is required
		},
	}
	err = storage.CreateTable(table)
	assert.NoError(t, err)

	// Test inserting with missing required column
	err = storage.Insert("test", map[string]interface{}{
		"id": 1,
		// Missing required 'name' field
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required column name")

	// Test inserting with all required columns
	err = storage.Insert("test", map[string]interface{}{
		"id":   1,
		"name": "test",
	})
	assert.NoError(t, err)

	// Test Select with non-existent column
	rows, err := storage.Select("test", []string{"nonexistent"}, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid column name: nonexistent")

	// Test Update with non-existent column
	err = storage.Update("test", map[string]interface{}{
		"nonexistent": "value",
	}, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid column name: nonexistent")

	// Test complex where conditions
	rows, err = storage.Select("test", []string{"*"}, "id = 1 AND name = 'test'")
	assert.NoError(t, err)
	assert.Len(t, rows, 1) // Should find the row since we set name = "test"
}
