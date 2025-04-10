package storage_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zakazai/ulin-db/internal/storage"
	"github.com/zakazai/ulin-db/internal/types"
)

func TestInMemoryStorage(t *testing.T) {
	s := storage.NewInMemoryStorage()

	// Test CreateTable
	table := &types.Table{
		Name: "test",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	}
	err := s.CreateTable(table)
	assert.NoError(t, err)

	// Test CreateTable duplicate
	err = s.CreateTable(table)
	assert.Error(t, err)

	// Test Insert
	err = s.Insert("test", map[string]interface{}{
		"id":   1,
		"name": "test1",
	})
	assert.NoError(t, err)

	// Test Insert into non-existent table
	err = s.Insert("nonexistent", map[string]interface{}{})
	assert.Error(t, err)

	// Test Select
	rows, err := s.Select("test", []string{"*"}, nil)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0]["id"])
	assert.Equal(t, "test1", rows[0]["name"])

	// Test Select with where
	rows, err = s.Select("test", []string{"*"}, map[string]interface{}{"id": 1})
	assert.NoError(t, err)
	assert.Len(t, rows, 1)

	// Test Update
	err = s.Update("test", map[string]interface{}{
		"name": "test2",
	}, map[string]interface{}{"id": 1})
	assert.NoError(t, err)

	rows, err = s.Select("test", []string{"*"}, nil)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "test2", rows[0]["name"])

	// Test Delete
	err = s.Delete("test", map[string]interface{}{"id": 1})
	assert.NoError(t, err)

	rows, err = s.Select("test", []string{"*"}, nil)
	assert.NoError(t, err)
	assert.Len(t, rows, 0)
}

func TestJSONStorage(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "testdb")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create storage
	s, err := storage.NewJSONStorage(tmpDir, "test_")
	assert.NoError(t, err)

	// Test CreateTable
	table := &types.Table{
		Name: "test",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	}
	err = s.CreateTable(table)
	assert.NoError(t, err)

	// Test Insert
	err = s.Insert("test", map[string]interface{}{
		"id":   1,
		"name": "test1",
	})
	assert.NoError(t, err)

	// Close storage
	err = s.Close()
	assert.NoError(t, err)

	// Create new storage instance to test persistence
	s, err = storage.NewJSONStorage(tmpDir, "test_")
	assert.NoError(t, err)

	// Test that data was persisted
	rows, err := s.Select("test", []string{"*"}, nil)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	// Use type assertion to check the value type
	idValue := rows[0]["id"]
	switch v := idValue.(type) {
	case int:
		assert.Equal(t, 1, v)
	case float64:
		assert.Equal(t, float64(1), v)
	default:
		assert.Fail(t, "unexpected type for id")
	}
	assert.Equal(t, "test1", rows[0]["name"])

	// Test Update
	err = s.Update("test", map[string]interface{}{
		"name": "test2",
	}, map[string]interface{}{"id": 1})
	assert.NoError(t, err)

	// Close storage again
	err = s.Close()
	assert.NoError(t, err)

	// Create another storage instance
	s, err = storage.NewJSONStorage(tmpDir, "test_")
	assert.NoError(t, err)

	// Test that update was persisted
	rows, err = s.Select("test", []string{"*"}, nil)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "test2", rows[0]["name"])

	// Test Delete
	err = s.Delete("test", map[string]interface{}{"id": 1})
	assert.NoError(t, err)

	// Close storage
	err = s.Close()
	assert.NoError(t, err)

	// Create final storage instance
	s, err = storage.NewJSONStorage(tmpDir, "test_")
	assert.NoError(t, err)

	// Test that delete was persisted
	rows, err = s.Select("test", []string{"*"}, nil)
	assert.NoError(t, err)
	assert.Len(t, rows, 0)
}

func TestStorageEdgeCases(t *testing.T) {
	s := storage.NewInMemoryStorage()

	// Test Select from non-existent table
	_, err := s.Select("nonexistent", []string{"*"}, nil)
	assert.Error(t, err)

	// Test Update non-existent table
	err = s.Update("nonexistent", map[string]interface{}{}, nil)
	assert.Error(t, err)

	// Test Delete from non-existent table
	err = s.Delete("nonexistent", nil)
	assert.Error(t, err)

	// Test Insert with missing columns
	table := &types.Table{
		Name: "test",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INT", Nullable: true},
			{Name: "name", Type: "TEXT", Nullable: false}, // name is required
		},
	}
	err = s.CreateTable(table)
	assert.NoError(t, err)

	// Test inserting with missing required column
	err = s.Insert("test", map[string]interface{}{
		"id": 1,
		// Missing required 'name' field
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required column name")

	// Test inserting with all required columns
	err = s.Insert("test", map[string]interface{}{
		"id":   1,
		"name": "test",
	})
	assert.NoError(t, err)

	// Test Select with non-existent column
	rows, err := s.Select("test", []string{"nonexistent"}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid column name: nonexistent")

	// Test Update with non-existent column
	err = s.Update("test", map[string]interface{}{
		"nonexistent": "value",
	}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid column name: nonexistent")

	// Test complex where conditions
	rows, err = s.Select("test", []string{"*"}, map[string]interface{}{"id": 1, "name": "test"})
	assert.NoError(t, err)
	assert.Len(t, rows, 1) // Should find the row since we set name = "test"
}

func TestCreateTable(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "testdb_create")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := storage.NewJSONStorage(tmpDir, "test_")
	assert.NoError(t, err)

	table := &types.Table{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "STRING", Nullable: true},
		},
	}

	err = s.CreateTable(table)
	assert.NoError(t, err)

	// Verify table was created
	tableResult := s.GetTable("test_table")
	assert.NotNil(t, tableResult)
	assert.Equal(t, "test_table", tableResult.Name)
	assert.Len(t, tableResult.Columns, 2)
	assert.Equal(t, "id", tableResult.Columns[0].Name)
	assert.Equal(t, "INT", tableResult.Columns[0].Type)
	assert.Equal(t, "name", tableResult.Columns[1].Name)
	assert.Equal(t, "STRING", tableResult.Columns[1].Type)
}

func TestInsert(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "testdb_insert")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := storage.NewJSONStorage(tmpDir, "test_")
	assert.NoError(t, err)

	table := &types.Table{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "STRING", Nullable: true},
		},
	}

	err = s.CreateTable(table)
	assert.NoError(t, err)

	// Test inserting a row
	values := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	err = s.Insert("test_table", values)
	assert.NoError(t, err)

	// Verify row was inserted
	rows, err := s.Select("test_table", []string{"*"}, nil)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0]["id"])
	assert.Equal(t, "test", rows[0]["name"])
}

func TestUpdate(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "testdb_update")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := storage.NewJSONStorage(tmpDir, "test_")
	assert.NoError(t, err)

	table := &types.Table{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "STRING", Nullable: true},
		},
	}

	err = s.CreateTable(table)
	assert.NoError(t, err)

	// Insert initial data
	values := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	err = s.Insert("test_table", values)
	assert.NoError(t, err)

	// Update the row
	set := map[string]interface{}{
		"name": "updated",
	}
	where := map[string]interface{}{
		"id": 1,
	}

	err = s.Update("test_table", set, where)
	assert.NoError(t, err)

	// Verify update
	rows, err := s.Select("test_table", []string{"*"}, nil)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "updated", rows[0]["name"])
}

func TestDelete(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "testdb_delete")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := storage.NewJSONStorage(tmpDir, "test_")
	assert.NoError(t, err)

	table := &types.Table{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "STRING", Nullable: true},
		},
	}

	err = s.CreateTable(table)
	assert.NoError(t, err)

	// Insert initial data
	values := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	err = s.Insert("test_table", values)
	assert.NoError(t, err)

	// Delete the row
	where := map[string]interface{}{
		"id": 1,
	}

	err = s.Delete("test_table", where)
	assert.NoError(t, err)

	// Verify deletion
	rows, err := s.Select("test_table", []string{"*"}, nil)
	assert.NoError(t, err)
	assert.Len(t, rows, 0)
}
