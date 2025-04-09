package types

// Storage interface defines the methods required for database storage
type Storage interface {
	CreateTable(table *Table) error
	Insert(tableName string, values map[string]interface{}) error
	Update(tableName string, set map[string]interface{}, where string) error
	Delete(tableName string, where string) error
	Select(tableName string, columns []string, where string) (interface{}, error)
	ShowTables() (interface{}, error)
}

// Table represents a database table
type Table struct {
	Name    string
	Columns []ColumnDefinition
	Rows    [][]interface{}
}

// ColumnDefinition represents a column in a table
type ColumnDefinition struct {
	Name     string
	Type     string
	Nullable bool
}
