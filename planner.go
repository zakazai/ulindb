package ulindb

import (
	"errors"
	"fmt"
)

// Plan represents a query execution plan
type Plan struct {
	Type    string
	Table   string
	Columns []string
	Where   string
	Values  []interface{}
	Set     map[string]interface{}
}

// Planner converts a Statement into an execution Plan
func (s *Statement) Plan() (*Plan, error) {
	if s.err != nil {
		return nil, s.err
	}

	if s.selectStatement.items != nil {
		return planSelect(&s.selectStatement)
	} else if s.insertStatement.items != nil {
		return planInsert(&s.insertStatement)
	} else if s.updateStatement.table != "" {
		return planUpdate(&s.updateStatement)
	} else if s.deleteStatement.from.name != "" {
		return planDelete(&s.deleteStatement)
	} else if s.createStatement.tableName != "" {
		return planCreate(&s.createStatement)
	}

	return nil, errors.New("invalid statement type")
}

func planSelect(stmt *SelectStatement) (*Plan, error) {
	plan := &Plan{
		Type:    "SELECT",
		Table:   stmt.from.name,
		Columns: make([]string, 0),
		Where:   stmt.where,
	}

	for _, item := range stmt.items {
		if item.All {
			plan.Columns = append(plan.Columns, "*")
		} else if item.Column != "," {
			plan.Columns = append(plan.Columns, item.Column)
		}
	}

	return plan, nil
}

func planInsert(stmt *InsertStatement) (*Plan, error) {
	plan := &Plan{
		Type:   "INSERT",
		Table:  stmt.table,
		Values: make([]interface{}, 1),
	}

	// For now, we'll just create a single nil value
	// In a real implementation, we would parse the actual values from the statement
	plan.Values[0] = nil

	return plan, nil
}

func planUpdate(stmt *UpdateStatement) (*Plan, error) {
	plan := &Plan{
		Type:  "UPDATE",
		Table: stmt.table,
		Set:   make(map[string]interface{}),
		Where: stmt.where,
	}

	for k, v := range stmt.set {
		plan.Set[k] = v
	}

	return plan, nil
}

func planDelete(stmt *DeleteStatement) (*Plan, error) {
	plan := &Plan{
		Type:  "DELETE",
		Table: stmt.from.name,
		Where: stmt.where,
	}

	return plan, nil
}

func planCreate(stmt *CreateStatement) (*Plan, error) {
	plan := &Plan{
		Type:    "CREATE",
		Table:   stmt.tableName,
		Columns: make([]string, len(stmt.columns)),
	}

	for i, col := range stmt.columns {
		plan.Columns[i] = fmt.Sprintf("%s %s", col.name, col.typ)
	}

	return plan, nil
}
