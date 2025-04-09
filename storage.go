package ulindb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Row represents a single row in a table
type Row map[string]interface{}

// Table represents a database table
type Table struct {
	Name    string
	Columns []ColumnDefinition
	Rows    []Row
}

// Database represents the entire database
type Database struct {
	Tables map[string]*Table
	mu     sync.RWMutex
}

// Storage interface defines the methods for database storage
type Storage interface {
	CreateTable(table *Table) error
	Insert(tableName string, values map[string]interface{}) error
	Select(tableName string, columns []string, where string) ([]Row, error)
	Update(tableName string, set map[string]interface{}, where string) error
	Delete(tableName string, where string) error
	Close() error
}

// InMemoryStorage implements Storage interface using in-memory storage
type InMemoryStorage struct {
	db *Database
}

// NewInMemoryStorage creates a new in-memory storage
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		db: &Database{
			Tables: make(map[string]*Table),
		},
	}
}

func (s *InMemoryStorage) CreateTable(table *Table) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	if _, exists := s.db.Tables[table.Name]; exists {
		return fmt.Errorf("table %s already exists", table.Name)
	}

	s.db.Tables[table.Name] = table
	return nil
}

func (s *InMemoryStorage) Insert(tableName string, values map[string]interface{}) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate all required columns are present
	row := make(Row)
	for _, col := range table.Columns {
		val, exists := values[col.name]
		if !exists && !col.Nullable {
			return fmt.Errorf("missing required column %s", col.name)
		}
		if exists {
			row[col.name] = val
		}
	}

	table.Rows = append(table.Rows, row)
	return nil
}

func (s *InMemoryStorage) Select(tableName string, columns []string, where string) ([]Row, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	var results []Row
	for _, row := range table.Rows {
		if where == "" || evaluateWhere(row, where) {
			result := make(Row)
			for _, col := range columns {
				if col == "*" {
					for k, v := range row {
						result[k] = v
					}
				} else if val, ok := row[col]; ok {
					result[col] = val
				}
			}
			results = append(results, result)
		}
	}

	return results, nil
}

func (s *InMemoryStorage) Update(tableName string, set map[string]interface{}, where string) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	for i, row := range table.Rows {
		if where == "" || evaluateWhere(row, where) {
			for k, v := range set {
				row[k] = v
			}
			table.Rows[i] = row
		}
	}

	return nil
}

func (s *InMemoryStorage) Delete(tableName string, where string) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	var newRows []Row
	for _, row := range table.Rows {
		if where == "" || !evaluateWhere(row, where) {
			newRows = append(newRows, row)
		}
	}

	table.Rows = newRows
	return nil
}

func (s *InMemoryStorage) Close() error {
	return nil
}

// JSONStorage implements Storage interface using JSON file storage
type JSONStorage struct {
	*InMemoryStorage
	filePath string
}

// NewJSONStorage creates a new JSON file storage
func NewJSONStorage(filePath string) (*JSONStorage, error) {
	storage := &JSONStorage{
		InMemoryStorage: NewInMemoryStorage(),
		filePath:        filePath,
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Create file if it doesn't exist
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		file, err := os.Create(filePath)
		if err != nil {
			return nil, err
		}
		file.Close()

		// Initialize with empty database
		if err := storage.save(); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	if err := storage.load(); err != nil {
		return nil, err
	}

	return storage, nil
}

func (s *JSONStorage) load() error {
	data, err := ioutil.ReadFile(s.filePath)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		// Empty file, initialize with empty database
		s.db = &Database{
			Tables: make(map[string]*Table),
		}
		return nil
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	var db struct {
		Tables map[string]*struct {
			Name    string
			Columns []ColumnDefinition
			Rows    []map[string]interface{}
		}
	}

	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&db); err != nil {
		return err
	}

	s.db.Tables = make(map[string]*Table)
	for name, t := range db.Tables {
		table := &Table{
			Name:    t.Name,
			Columns: t.Columns,
			Rows:    make([]Row, len(t.Rows)),
		}
		for i, row := range t.Rows {
			newRow := make(Row)
			for k, v := range row {
				if num, ok := v.(json.Number); ok {
					if i, err := num.Int64(); err == nil {
						newRow[k] = int(i)
					} else if f, err := num.Float64(); err == nil {
						newRow[k] = f
					} else {
						newRow[k] = v
					}
				} else {
					newRow[k] = v
				}
			}
			table.Rows[i] = newRow
		}
		s.db.Tables[name] = table
	}

	return nil
}

func (s *JSONStorage) save() error {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	data, err := json.MarshalIndent(s.db, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return ioutil.WriteFile(s.filePath, data, 0644)
}

func (s *JSONStorage) CreateTable(table *Table) error {
	if err := s.InMemoryStorage.CreateTable(table); err != nil {
		return err
	}
	return s.save()
}

func (s *JSONStorage) Insert(tableName string, values map[string]interface{}) error {
	// Convert any json.Number to int64 or float64
	for k, v := range values {
		if num, ok := v.(json.Number); ok {
			if i, err := num.Int64(); err == nil {
				values[k] = int(i)
			} else if f, err := num.Float64(); err == nil {
				values[k] = f
			}
		}
	}

	if err := s.InMemoryStorage.Insert(tableName, values); err != nil {
		return err
	}
	return s.save()
}

func (s *JSONStorage) Update(tableName string, set map[string]interface{}, where string) error {
	if err := s.InMemoryStorage.Update(tableName, set, where); err != nil {
		return err
	}
	return s.save()
}

func (s *JSONStorage) Delete(tableName string, where string) error {
	if err := s.InMemoryStorage.Delete(tableName, where); err != nil {
		return err
	}
	return s.save()
}

func (s *JSONStorage) Close() error {
	return s.save()
}

// Helper function to evaluate WHERE conditions
func evaluateWhere(row Row, where string) bool {
	if where == "" {
		return true
	}

	// Handle AND conditions
	if strings.Contains(where, " AND ") {
		conditions := strings.Split(where, " AND ")
		for _, condition := range conditions {
			if !evaluateSimpleCondition(row, strings.TrimSpace(condition)) {
				return false
			}
		}
		return true
	}

	return evaluateSimpleCondition(row, where)
}

// Helper function to evaluate a simple condition (no AND/OR)
func evaluateSimpleCondition(row Row, condition string) bool {
	// Simple equality check
	parts := strings.Split(condition, "=")
	if len(parts) != 2 {
		return false
	}

	column := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])
	// Remove quotes if present
	value = strings.Trim(value, "'\"")

	rowValue, exists := row[column]
	if !exists {
		return false
	}

	return fmt.Sprintf("%v", rowValue) == value
}
