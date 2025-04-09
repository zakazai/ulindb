package ulindb

import (
	"errors"
	"fmt"
)

// Plan represents a query execution plan
type Plan struct {
	Storage Storage
	Stmt    *Statement
	Type    string
	Table   string
	Columns []string
	Where   string
	Set     map[string]interface{}
	Values  []interface{}
}

// NewPlan creates a new query execution plan
func NewPlan(storage Storage, stmt *Statement) *Plan {
	return &Plan{
		Storage: storage,
		Stmt:    stmt,
	}
}

// Execute executes the query plan
func (p *Plan) Execute() ([]Row, error) {
	if p.Stmt.Err != nil {
		return nil, p.Stmt.Err
	}

	if p.Stmt.SelectStatement.From.Name != "" {
		return p.Storage.Select(p.Stmt.SelectStatement.From.Name, []string{"*"}, p.Stmt.SelectStatement.Where)
	} else if p.Stmt.InsertStatement.Table != "" {
		// TODO: Implement insert plan
		return nil, fmt.Errorf("insert not implemented")
	} else if p.Stmt.UpdateStatement.Table != "" {
		// TODO: Implement update plan
		return nil, fmt.Errorf("update not implemented")
	} else if p.Stmt.DeleteStatement.From.Name != "" {
		// TODO: Implement delete plan
		return nil, fmt.Errorf("delete not implemented")
	}

	return nil, fmt.Errorf("unsupported statement type")
}

// ExecuteStatement executes a SQL statement
func ExecuteStatement(s *Statement, storage Storage) error {
	if s.Err != nil {
		return s.Err
	}

	if s.SelectStatement.From.Name != "" {
		results, err := storage.Select(s.SelectStatement.From.Name, []string{"*"}, s.SelectStatement.Where)
		if err != nil {
			return err
		}
		fmt.Printf("Results: %v\n", results)
	} else if s.InsertStatement.Table != "" {
		// TODO: Implement insert
		return fmt.Errorf("insert not implemented")
	} else if s.UpdateStatement.Table != "" {
		// TODO: Implement update
		return fmt.Errorf("update not implemented")
	} else if s.DeleteStatement.From.Name != "" {
		// TODO: Implement delete
		return fmt.Errorf("delete not implemented")
	}

	return nil
}

// Planner converts a Statement into an execution Plan
func (s *Statement) Plan() (*Plan, error) {
	if s.Err != nil {
		return nil, s.Err
	}

	if s.SelectStatement.From.Name != "" {
		return planSelect(&s.SelectStatement)
	} else if s.InsertStatement.Table != "" {
		return planInsert(&s.InsertStatement)
	} else if s.UpdateStatement.Table != "" {
		return planUpdate(&s.UpdateStatement)
	} else if s.DeleteStatement.From.Name != "" {
		return planDelete(&s.DeleteStatement)
	} else if s.CreateStatement.TableName != "" {
		return planCreate(&s.CreateStatement)
	}

	return nil, errors.New("invalid statement type")
}

func planSelect(stmt *SelectStatement) (*Plan, error) {
	plan := &Plan{
		Type:    "SELECT",
		Table:   stmt.From.Name,
		Columns: make([]string, 0),
		Where:   stmt.Where,
	}

	for _, item := range stmt.Items {
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
		Table:  stmt.Table,
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
		Table: stmt.Table,
		Set:   make(map[string]interface{}),
		Where: stmt.Where,
	}

	for k, v := range stmt.Set {
		plan.Set[k] = v
	}

	return plan, nil
}

func planDelete(stmt *DeleteStatement) (*Plan, error) {
	plan := &Plan{
		Type:  "DELETE",
		Table: stmt.From.Name,
		Where: stmt.Where,
	}

	return plan, nil
}

func planCreate(stmt *CreateStatement) (*Plan, error) {
	plan := &Plan{
		Type:    "CREATE",
		Table:   stmt.TableName,
		Columns: make([]string, len(stmt.Columns)),
	}

	for i, col := range stmt.Columns {
		plan.Columns[i] = fmt.Sprintf("%s %s", col.Name, col.Type)
	}

	return plan, nil
}
