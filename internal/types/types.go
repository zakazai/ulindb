package types

// Row represents a single row in a table
type Row map[string]interface{}

// Storage interface defines the methods required for database storage
type Storage interface {
	CreateTable(table *Table) error
	Insert(tableName string, values map[string]interface{}) error
	Update(tableName string, set map[string]interface{}, where map[string]interface{}) error
	Delete(tableName string, where map[string]interface{}) error
	Select(tableName string, columns []string, where map[string]interface{}) ([]Row, error)
	ShowTables() ([]string, error)
}

// Table represents a database table
type Table struct {
	Name    string
	Columns []ColumnDefinition
	Rows    []Row
}

// ColumnDefinition represents a column in a table
type ColumnDefinition struct {
	Name     string
	Type     string
	Nullable bool
}
