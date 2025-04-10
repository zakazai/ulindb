package planner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zakazai/ulin-db/internal/parser"
)

func TestPlanSelect(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected *Plan
	}{
		{
			name:  "Select all from table",
			query: "SELECT * FROM tablex",
			expected: &Plan{
				Type:    "SELECT",
				Table:   "tablex",
				Columns: []string{"*"},
			},
		},
		{
			name:  "Select specific columns",
			query: "SELECT a, b FROM tablex",
			expected: &Plan{
				Type:    "SELECT",
				Table:   "tablex",
				Columns: []string{"a", "b"},
			},
		},
		{
			name:  "Select with where clause",
			query: "SELECT a FROM tablex WHERE a = 1",
			expected: &Plan{
				Type:    "SELECT",
				Table:   "tablex",
				Columns: []string{"a"},
				Where:   map[string]interface{}{"a": float64(1)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parser.Parse(test.query)
			assert.NoError(t, err)
			plan, err := CreatePlan(stmt, nil)
			assert.NoError(t, err)
			assert.Equal(t, test.expected.Type, plan.Type)
			assert.Equal(t, test.expected.Table, plan.Table)
			assert.Equal(t, test.expected.Columns, plan.Columns)
			assert.Equal(t, test.expected.Where, plan.Where)
		})
	}
}

func TestPlanInsert(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected *Plan
	}{
		{
			name:  "Insert into table",
			query: "INSERT INTO tablex VALUES (1, 'test')",
			expected: &Plan{
				Type:   "INSERT",
				Table:  "tablex",
				Values: map[string]interface{}{"column1": float64(1), "column2": "test"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parser.Parse(test.query)
			assert.NoError(t, err)
			plan, err := CreatePlan(stmt, nil)
			assert.NoError(t, err)
			assert.Equal(t, test.expected.Type, plan.Type)
			assert.Equal(t, test.expected.Table, plan.Table)
			assert.Equal(t, test.expected.Values, plan.Values)
		})
	}
}

func TestPlanUpdate(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected *Plan
	}{
		{
			name:  "Update table",
			query: "UPDATE tablex SET a = 1 WHERE b = 2",
			expected: &Plan{
				Type:  "UPDATE",
				Table: "tablex",
				Set: map[string]interface{}{
					"a": float64(1),
				},
				Where: map[string]interface{}{"b": float64(2)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parser.Parse(test.query)
			assert.NoError(t, err)
			plan, err := CreatePlan(stmt, nil)
			assert.NoError(t, err)
			assert.Equal(t, test.expected.Type, plan.Type)
			assert.Equal(t, test.expected.Table, plan.Table)
			assert.Equal(t, test.expected.Set, plan.Set)
			assert.Equal(t, test.expected.Where, plan.Where)
		})
	}
}

func TestPlanDelete(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected *Plan
	}{
		{
			name:  "Delete from table",
			query: "DELETE FROM tablex WHERE a = 1",
			expected: &Plan{
				Type:  "DELETE",
				Table: "tablex",
				Where: map[string]interface{}{"a": float64(1)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parser.Parse(test.query)
			assert.NoError(t, err)
			plan, err := CreatePlan(stmt, nil)
			assert.NoError(t, err)
			assert.Equal(t, test.expected.Type, plan.Type)
			assert.Equal(t, test.expected.Table, plan.Table)
			assert.Equal(t, test.expected.Where, plan.Where)
		})
	}
}

func TestPlanCreate(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected *Plan
	}{
		{
			name:  "Create table",
			query: "CREATE TABLE tablex (id INT, name TEXT)",
			expected: &Plan{
				Type:    "CREATE",
				Table:   "tablex",
				Columns: []string{"id INT", "name TEXT"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parser.Parse(test.query)
			assert.NoError(t, err)
			plan, err := CreatePlan(stmt, nil)
			assert.NoError(t, err)
			assert.Equal(t, test.expected.Type, plan.Type)
			assert.Equal(t, test.expected.Table, plan.Table)
			assert.Equal(t, test.expected.Columns, plan.Columns)
		})
	}
}
