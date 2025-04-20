package types

// Row represents a single row in a table with column names as keys and column values as values.
type Row map[string]interface{}

// Storage interface defines the methods required for database storage implementations.
type Storage interface {
	// CreateTable creates a new table with the given table definition.
	CreateTable(table *Table) error

	// Insert adds a new row to the specified table with the given values.
	Insert(tableName string, values map[string]interface{}) error

	// Update modifies existing rows in the table that match the where condition.
	Update(tableName string, set map[string]interface{}, where map[string]interface{}) error

	// Delete removes rows from the table that match the where condition.
	Delete(tableName string, where map[string]interface{}) error

	// Select retrieves rows from the table, optionally filtered by where condition.
	Select(tableName string, columns []string, where map[string]interface{}) ([]Row, error)

	// ShowTables returns a list of all table names in the database.
	ShowTables() ([]string, error)
}

// Table represents a database table with its schema and data.
type Table struct {
	// Name is the identifier of the table.
	Name string

	// Columns defines the schema of the table.
	Columns []ColumnDefinition

	// Rows contains the data stored in the table.
	Rows []Row
}

// ColumnDefinition represents a column in a table schema.
type ColumnDefinition struct {
	// Name is the identifier of the column.
	Name string

	// Type is the data type of the column (e.g., "INT", "STRING").
	Type string

	// Nullable indicates whether the column can contain NULL values.
	Nullable bool
}
