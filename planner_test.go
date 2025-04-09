package ulindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
				Where:   "a = 1",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt := parse(test.query)
			plan, err := stmt.Plan()
			assert.NoError(t, err)
			assert.Equal(t, test.expected, plan)
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
				Values: []interface{}{nil},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt := parse(test.query)
			plan, err := stmt.Plan()
			assert.NoError(t, err)
			assert.Equal(t, test.expected, plan)
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
					"a": "1",
				},
				Where: "b = 2",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt := parse(test.query)
			plan, err := stmt.Plan()
			assert.NoError(t, err)
			assert.Equal(t, test.expected, plan)
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
				Where: "a = 1",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt := parse(test.query)
			plan, err := stmt.Plan()
			assert.NoError(t, err)
			assert.Equal(t, test.expected, plan)
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
			stmt := parse(test.query)
			plan, err := stmt.Plan()
			assert.NoError(t, err)
			assert.Equal(t, test.expected, plan)
		})
	}
}
