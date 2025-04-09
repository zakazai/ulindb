package types

type Table struct {
	Name    string
	Columns []Column
	Rows    []map[string]interface{}
}

type Column struct {
	Name     string
	Type     string
	Required bool
}

type Storage interface {
	Init() error
	Select(table string, columns []string, where string) (interface{}, error)
	Insert(table string, values map[string]interface{}) error
	Update(table string, set map[string]interface{}, where string) error
	Delete(table string, where string) error
	CreateTable(table *Table) error
	ShowTables() ([]string, error)
}
