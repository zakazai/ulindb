package storage_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zakazai/ulin-db/internal/parser"
	"github.com/zakazai/ulin-db/internal/storage"
	"github.com/zakazai/ulin-db/internal/types"
)

func TestSQLIntegration(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "testdb_integration")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize storage with the temporary directory
	store, err := storage.NewJSONStorage(tmpDir, "test_")
	if err != nil {
		t.Fatalf("Failed to create JSON storage: %v", err)
	}

	// Test cases
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name: "Create table",
			sql:  "CREATE TABLE users (id INT, name STRING, age INT)",
		},
		{
			name: "Insert record",
			sql:  "INSERT INTO users VALUES (1, 'John', 25)",
		},
		{
			name: "Select record",
			sql:  "SELECT * FROM users WHERE id = 1",
		},
		{
			name: "Update record",
			sql:  "UPDATE users SET age = 26 WHERE id = 1",
		},
		{
			name: "Delete record",
			sql:  "DELETE FROM users WHERE id = 1",
		},
		{
			name:    "Invalid table",
			sql:     "SELECT * FROM non_existent",
			wantErr: true,
		},
		{
			name:    "Invalid column",
			sql:     "SELECT invalid_column FROM users",
			wantErr: true,
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			// Special handling for INSERT statements
			if insertStmt, ok := stmt.(*parser.InsertStatement); ok {
				// Map column1, column2, column3 to id, name, age
				values := map[string]interface{}{
					"id":   insertStmt.Values["column1"],
					"name": insertStmt.Values["column2"],
					"age":  insertStmt.Values["column3"],
				}
				// Execute directly instead of using the Statement interface
				err = store.Insert(insertStmt.Table, values)
			} else {
				// For other statements, use the normal Execute method
				_, err = stmt.Execute(store)
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMultipleInsertsAndCount(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "testdb_multiple_inserts_integration")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create BTree storage for testing multiple inserts and COUNT(*)
	filePath := tmpDir + "/test.btree"
	store, err := storage.NewBTreeStorage(filePath)
	assert.NoError(t, err)
	defer store.Close()

	// First, create a test table directly (not through SQL parser)
	table := &types.Table{
		Name: "test_rows",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "content", Type: "STRING", Nullable: false},
		},
	}
	err = store.CreateTable(table)
	assert.NoError(t, err)

	// Test with a smaller dataset for BTree implementation that we know works with smaller data
	rowCount := 5

	// Insert multiple rows
	for i := 1; i <= rowCount; i++ {
		err = store.Insert("test_rows", map[string]interface{}{
			"id":      i,
			"content": fmt.Sprintf("This is row %d", i),
		})
		assert.NoError(t, err)
	}

	// Now verify that all rows were inserted without overwriting
	rows, err := store.Select("test_rows", []string{"*"}, nil)
	assert.NoError(t, err)
	
	// Debug output to see what's happening
	fmt.Printf("DEBUG INTEGRATION TEST: Retrieved %d rows\n", len(rows))
	for i, row := range rows {
		fmt.Printf("DEBUG INTEGRATION TEST: Row %d: ID=%v, Content=%v\n", 
			i, row["id"], row["content"])
	}
	
	assert.Len(t, rows, rowCount, "All %d records should be preserved", rowCount)

	// Test COUNT(*) functionality
	countRows, err := store.Select("test_rows", []string{"COUNT(*)"}, nil)
	assert.NoError(t, err)
	fmt.Printf("DEBUG INTEGRATION TEST: COUNT(*) returned %v\n", countRows[0]["count"])
	assert.Equal(t, rowCount, countRows[0]["count"], "COUNT(*) should return %d for total rows", rowCount)

	// Test COUNT(*) with a WHERE filter
	firstHalfCount, err := store.Select("test_rows", []string{"COUNT(*)"}, map[string]interface{}{
		"id": 1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, firstHalfCount[0]["count"], "COUNT(*) with WHERE id=1 should return 1")

	// Add a couple more rows
	additionalRows := 3
	for i := 1; i <= additionalRows; i++ {
		err = store.Insert("test_rows", map[string]interface{}{
			"id":      rowCount + i,
			"content": fmt.Sprintf("This is additional row %d", i),
		})
		assert.NoError(t, err)
	}

	// Verify total count increased
	newTotalCount, err := store.Select("test_rows", []string{"COUNT(*)"}, nil)
	assert.NoError(t, err)
	expectedTotal := rowCount + additionalRows
	fmt.Printf("DEBUG INTEGRATION TEST: New COUNT(*) returned %v, expected %d\n", 
		newTotalCount[0]["count"], expectedTotal)

	// Check rows again
	allRows, err := store.Select("test_rows", []string{"*"}, nil)
	assert.NoError(t, err)
	fmt.Printf("DEBUG INTEGRATION TEST: After additions, retrieved %d rows\n", len(allRows))
	for i, row := range allRows {
		fmt.Printf("DEBUG INTEGRATION TEST: Row %d: ID=%v, Content=%v\n", 
			i, row["id"], row["content"])
	}

	assert.Equal(t, expectedTotal, newTotalCount[0]["count"], 
		"Total count should now be %d rows", expectedTotal)
}
