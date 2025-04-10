package planner

import (
	"errors"
	"fmt"

	"github.com/zakazai/ulin-db/internal/parser"
	"github.com/zakazai/ulin-db/internal/types"
)

// Plan represents a query execution plan
type Plan struct {
	Storage types.Storage
	Type    string
	Table   string
	Columns []string
	Where   map[string]interface{}
	Set     map[string]interface{}
	Values  map[string]interface{}
}

type Planner struct {
	storage types.Storage
}

// NewPlan creates a new query execution plan
func NewPlan(storage types.Storage, stmt parser.Statement) *Plan {
	return &Plan{
		Storage: storage,
	}
}

// NewPlanner creates a new planner
func NewPlanner(storage types.Storage) *Planner {
	return &Planner{
		storage: storage,
	}
}

// Execute executes the query plan
func (p *Plan) Execute() (interface{}, error) {
	switch p.Type {
	case "SELECT":
		return p.Storage.Select(p.Table, p.Columns, p.Where)
	case "INSERT":
		return nil, p.Storage.Insert(p.Table, p.Values)
	case "UPDATE":
		return nil, p.Storage.Update(p.Table, p.Set, p.Where)
	case "DELETE":
		return nil, p.Storage.Delete(p.Table, p.Where)
	case "CREATE":
		// TODO: Implement create table plan
		return nil, fmt.Errorf("create table not implemented in planner")
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", p.Type)
	}
}

// ExecuteStatement executes a SQL statement
func ExecuteStatement(stmt parser.Statement, storage types.Storage) (interface{}, error) {
	return stmt.Execute(storage)
}

// CreatePlan converts a Statement into an execution Plan
func CreatePlan(stmt parser.Statement, storage types.Storage) (*Plan, error) {
	plan := &Plan{
		Storage: storage,
	}

	switch s := stmt.(type) {
	case *parser.SelectStatement:
		plan.Type = "SELECT"
		plan.Table = s.Table
		plan.Columns = s.Columns
		plan.Where = s.Where
	case *parser.InsertStatement:
		plan.Type = "INSERT"
		plan.Table = s.Table
		plan.Values = s.Values
	case *parser.UpdateStatement:
		plan.Type = "UPDATE"
		plan.Table = s.Table
		plan.Set = s.Set
		plan.Where = s.Where
	case *parser.DeleteStatement:
		plan.Type = "DELETE"
		plan.Table = s.Table
		plan.Where = s.Where
	case *parser.CreateStatement:
		plan.Type = "CREATE"
		plan.Table = s.Table
		// Convert columns to string format
		plan.Columns = make([]string, len(s.Columns))
		for i, col := range s.Columns {
			plan.Columns[i] = fmt.Sprintf("%s %s", col.Name, col.Type)
		}
	default:
		return nil, errors.New("invalid statement type")
	}

	return plan, nil
}

func (p *Planner) Execute(stmt parser.Statement) (interface{}, error) {
	return stmt.Execute(p.storage)
}
