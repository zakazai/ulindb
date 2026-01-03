package planner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zakazai/ulin-db/internal/parser"
	"github.com/zakazai/ulin-db/internal/storage"
	"github.com/zakazai/ulin-db/internal/types"
)

func TestCreatePlan(t *testing.T) {
	store := storage.NewInMemoryStorage()

	tests := []struct {
		name    string
		sql     string
		want    *Plan
		wantErr bool
	}{
		{
			name: "Select all plan",
			sql:  "SELECT * FROM users",
			want: &Plan{
				Type:    "SELECT",
				Table:   "users",
				Columns: []string{"*"},
				Storage: store,
			},
		},
		{
			name: "Select with where",
			sql:  "SELECT name FROM users WHERE id = 1",
			want: &Plan{
				Type:    "SELECT",
				Table:   "users",
				Columns: []string{"name"},
				Where: map[string]interface{}{
					"id": float64(1),
				},
				Storage: store,
			},
		},
		{
			name: "Create table plan",
			sql:  "CREATE TABLE users (id INT, name TEXT)",
			want: &Plan{
				Type:    "CREATE",
				Table:   "users",
				Columns: []string{"id INT", "name TEXT"},
				Storage: store,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.sql)
			assert.NoError(t, err)

			got, err := CreatePlan(stmt, store)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPlanExecution(t *testing.T) {
	store := storage.NewInMemoryStorage()

	// Test CREATE TABLE
	createSQL := "CREATE TABLE users (id INT, name TEXT)"
	stmt, err := parser.Parse(createSQL)
	assert.NoError(t, err)

	plan, err := CreatePlan(stmt, store)
	assert.NoError(t, err)
	assert.Equal(t, "CREATE", plan.Type)
	assert.Equal(t, "users", plan.Table)

	// Test INSERT
	insertSQL := "INSERT INTO users VALUES (1, 'test')"
	stmt, err = parser.Parse(insertSQL)
	assert.NoError(t, err)

	plan, err = CreatePlan(stmt, store)
	assert.NoError(t, err)
	assert.Equal(t, "INSERT", plan.Type)
	assert.Equal(t, "users", plan.Table)

	// Test SELECT
	selectSQL := "SELECT * FROM users WHERE id = 1"
	stmt, err = parser.Parse(selectSQL)
	assert.NoError(t, err)

	plan, err = CreatePlan(stmt, store)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT", plan.Type)
	assert.Equal(t, "users", plan.Table)
	assert.Equal(t, []string{"*"}, plan.Columns)
	assert.Equal(t, float64(1), plan.Where["id"])
}

func TestPlanOptimization(t *testing.T) {
	store := storage.NewInMemoryStorage()

	// Create a test table
	err := store.CreateTable(&types.Table{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
			{Name: "age", Type: "INT"},
		},
	})
	assert.NoError(t, err)

	// Insert test data
	err = store.Insert("users", map[string]interface{}{
		"id":   1,
		"name": "test",
		"age":  25,
	})
	assert.NoError(t, err)

	// Test column pruning
	selectSQL := "SELECT name FROM users WHERE id = 1"
	stmt, err := parser.Parse(selectSQL)
	assert.NoError(t, err)

	plan, err := CreatePlan(stmt, store)
	assert.NoError(t, err)
	assert.Equal(t, []string{"name"}, plan.Columns)

	// Test predicate pushdown
	selectSQL = "SELECT * FROM users WHERE id = 1 AND age > 20"
	stmt, err = parser.Parse(selectSQL)
	assert.NoError(t, err)

	plan, err = CreatePlan(stmt, store)
	assert.NoError(t, err)
	assert.NotNil(t, plan.Where)
	assert.Equal(t, float64(1), plan.Where["id"])
}
