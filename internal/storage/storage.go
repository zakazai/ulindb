package storage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/zakazai/ulin-db/internal/types"
)

// Row represents a single row in a table
type Row map[string]interface{}

// ColumnDefinition represents the definition of a column in a table
type ColumnDefinition struct {
	Name     string
	Type     string
	Nullable bool
}

// Table represents a database table
type Table struct {
	Name    string
	Columns []types.ColumnDefinition
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
	GetTable(tableName string) *Table
	Close() error
	ShowTables() ([]string, error)
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

	// Check for duplicate column names
	columnNames := make(map[string]bool)
	for _, col := range table.Columns {
		if columnNames[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		columnNames[col.Name] = true
	}

	s.db.Tables[table.Name] = table
	return nil
}

func (s *InMemoryStorage) validateDataType(value interface{}, columnType string) error {
	if value == nil {
		return nil // NULL values are allowed for any type
	}

	switch columnType {
	case "INT":
		switch v := value.(type) {
		case int, int32, int64:
			return nil
		case float64:
			if float64(int(v)) == v {
				return nil
			}
		case string:
			if _, err := strconv.Atoi(v); err == nil {
				return nil
			}
		}
		return fmt.Errorf("value %v is not an integer", value)
	case "STRING", "TEXT":
		switch value.(type) {
		case string:
			return nil
		case int, int32, int64, float64:
			// Convert numeric values to string
			return nil
		}
		return fmt.Errorf("value %v is not a string", value)
	}
	return nil
}

func (s *InMemoryStorage) validateColumns(table *Table, columns []string) error {
	if len(columns) == 1 && columns[0] == "*" {
		return nil
	}

	columnMap := make(map[string]bool)
	for _, col := range table.Columns {
		columnMap[col.Name] = true
	}

	for _, col := range columns {
		if !columnMap[col] {
			return fmt.Errorf("invalid column name: %s", col)
		}
	}
	return nil
}

func (s *InMemoryStorage) validateColumnNames(table *Table, values map[string]interface{}) error {
	columnMap := make(map[string]bool)
	for _, col := range table.Columns {
		columnMap[col.Name] = true
	}

	for colName := range values {
		if !columnMap[colName] {
			return fmt.Errorf("invalid column name: %s", colName)
		}
	}
	return nil
}

func (s *InMemoryStorage) validateWhereColumns(table *Table, where string) error {
	if where == "" {
		return nil
	}

	columnMap := make(map[string]bool)
	for _, col := range table.Columns {
		columnMap[col.Name] = true
	}

	// Split on AND but preserve the AND tokens
	parts := strings.Split(where, " AND ")
	for _, part := range parts {
		// Split on spaces to get the column name
		conditionParts := strings.Fields(strings.TrimSpace(part))
		if len(conditionParts) > 0 && !columnMap[conditionParts[0]] {
			return fmt.Errorf("invalid column name in WHERE clause: %s", conditionParts[0])
		}
	}
	return nil
}

func (s *InMemoryStorage) Insert(tableName string, values map[string]interface{}) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate column names
	if err := s.validateColumnNames(table, values); err != nil {
		return err
	}

	// Validate all required columns are present and check data types
	row := make(Row)
	for _, col := range table.Columns {
		val, exists := values[col.Name]
		if !exists && !col.Nullable {
			return fmt.Errorf("missing required column %s", col.Name)
		}
		if exists {
			if err := s.validateDataType(val, col.Type); err != nil {
				return fmt.Errorf("invalid data type for column %s: %v", col.Name, err)
			}
			// Convert float64 to int for INT columns
			if col.Type == "INT" {
				if f, ok := val.(float64); ok {
					val = int(f)
				}
			}
			row[col.Name] = val
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

	// Validate column names
	if err := s.validateColumns(table, columns); err != nil {
		return nil, err
	}

	// Validate column names in WHERE clause
	if err := s.validateWhereColumns(table, where); err != nil {
		return nil, err
	}

	var results []Row
	for _, row := range table.Rows {
		if where == "" || evaluateWhere(row, where) {
			result := make(Row)
			if len(columns) == 1 && columns[0] == "*" {
				// Copy all columns
				for k, v := range row {
					result[k] = v
				}
			} else {
				// Copy only requested columns
				for _, col := range columns {
					result[col] = row[col]
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

	// Validate column names in SET clause
	if err := s.validateColumnNames(table, set); err != nil {
		return err
	}

	// Validate column names in WHERE clause
	if err := s.validateWhereColumns(table, where); err != nil {
		return err
	}

	// Validate data types
	for colName, val := range set {
		for _, col := range table.Columns {
			if col.Name == colName {
				if err := s.validateDataType(val, col.Type); err != nil {
					return fmt.Errorf("invalid data type for column %s: %v", colName, err)
				}
				// Convert float64 to int for INT columns
				if col.Type == "INT" {
					if f, ok := val.(float64); ok {
						set[colName] = int(f)
					}
				}
				break
			}
		}
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
		return fmt.Errorf("table not found: %s", tableName)
	}

	if err := s.validateWhereColumns(table, where); err != nil {
		return err
	}

	var newRows []Row
	for _, row := range table.Rows {
		match := evaluateWhere(row, where)
		if !match {
			newRows = append(newRows, row)
		}
	}

	table.Rows = newRows
	return nil
}

func (s *InMemoryStorage) GetTable(tableName string) *Table {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	return s.db.Tables[tableName]
}

func (s *InMemoryStorage) Close() error {
	return nil
}

func (s *InMemoryStorage) ShowTables() ([]string, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	tables := make([]string, 0, len(s.db.Tables))
	for name := range s.db.Tables {
		tables = append(tables, name)
	}
	return tables, nil
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

type jsonTable struct {
	Name    string                   `json:"name"`
	Columns []jsonColumnDefinition   `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
}

type jsonColumnDefinition struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type jsonDatabase struct {
	Tables map[string]*jsonTable `json:"tables"`
}

func (s *JSONStorage) save() error {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	// Convert to JSON-friendly format
	jsonDB := &jsonDatabase{
		Tables: make(map[string]*jsonTable),
	}

	for name, table := range s.db.Tables {
		jsonTable := &jsonTable{
			Name:    table.Name,
			Columns: make([]jsonColumnDefinition, len(table.Columns)),
			Rows:    make([]map[string]interface{}, len(table.Rows)),
		}

		// Copy column definitions
		for i, col := range table.Columns {
			jsonTable.Columns[i] = jsonColumnDefinition{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			}
		}

		// Copy rows
		for i, row := range table.Rows {
			jsonTable.Rows[i] = row
		}
		jsonDB.Tables[name] = jsonTable
	}

	data, err := json.MarshalIndent(jsonDB, "", "  ")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(s.filePath, data, 0644)
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

	var jsonDB jsonDatabase
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&jsonDB); err != nil {
		return err
	}

	s.db = &Database{
		Tables: make(map[string]*Table),
	}

	for name, jsonTable := range jsonDB.Tables {
		table := &Table{
			Name:    jsonTable.Name,
			Columns: make([]types.ColumnDefinition, len(jsonTable.Columns)),
			Rows:    make([]Row, len(jsonTable.Rows)),
		}

		// Copy column definitions
		for i, col := range jsonTable.Columns {
			table.Columns[i] = types.ColumnDefinition{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			}
		}

		// Create column name map for validation
		columnMap := make(map[string]bool)
		for _, col := range table.Columns {
			columnMap[col.Name] = true
		}

		// Copy rows with validation
		for i, row := range jsonTable.Rows {
			newRow := make(Row)
			for k, v := range row {
				if !columnMap[k] {
					return fmt.Errorf("invalid column name: %s", k)
				}
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

func (s *JSONStorage) CreateTable(table *Table) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	if _, exists := s.db.Tables[table.Name]; exists {
		return fmt.Errorf("table %s already exists", table.Name)
	}

	// Check for duplicate column names
	columnNames := make(map[string]bool)
	for _, col := range table.Columns {
		if columnNames[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		columnNames[col.Name] = true
	}

	s.db.Tables[table.Name] = table
	return s.save()
}

func (s *JSONStorage) Insert(tableName string, values map[string]interface{}) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate column names
	if err := s.validateColumnNames(table, values); err != nil {
		return err
	}

	// Validate all required columns are present and check data types
	row := make(Row)
	for _, col := range table.Columns {
		val, exists := values[col.Name]
		if !exists && !col.Nullable {
			return fmt.Errorf("missing required column %s", col.Name)
		}
		if exists {
			if err := s.validateDataType(val, col.Type); err != nil {
				return fmt.Errorf("invalid data type for column %s: %v", col.Name, err)
			}
			// Convert float64 to int for INT columns
			if col.Type == "INT" {
				if f, ok := val.(float64); ok {
					val = int(f)
				}
			}
			row[col.Name] = val
		}
	}

	table.Rows = append(table.Rows, row)
	return s.save()
}

func (s *JSONStorage) Update(tableName string, set map[string]interface{}, where string) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate column names in SET clause
	if err := s.validateColumnNames(table, set); err != nil {
		return err
	}

	// Validate column names in WHERE clause
	if err := s.validateWhereColumns(table, where); err != nil {
		return err
	}

	// Validate data types
	for colName, val := range set {
		for _, col := range table.Columns {
			if col.Name == colName {
				if err := s.validateDataType(val, col.Type); err != nil {
					return fmt.Errorf("invalid data type for column %s: %v", colName, err)
				}
				// Convert float64 to int for INT columns
				if col.Type == "INT" {
					if f, ok := val.(float64); ok {
						set[colName] = int(f)
					}
				}
				break
			}
		}
	}

	for i, row := range table.Rows {
		if where == "" || evaluateWhere(row, where) {
			for k, v := range set {
				row[k] = v
			}
			table.Rows[i] = row
		}
	}

	return s.save()
}

func (s *JSONStorage) Delete(tableName string, where string) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table not found: %s", tableName)
	}

	if err := s.validateWhereColumns(table, where); err != nil {
		return err
	}

	fmt.Printf("Before delete (JSON): %d rows\n", len(table.Rows))
	for _, row := range table.Rows {
		fmt.Printf("Row (JSON): %v\n", row)
	}

	var newRows []Row
	for _, row := range table.Rows {
		match := evaluateWhere(row, where)
		fmt.Printf("Row (JSON): %v, Where: %s, Match: %v\n", row, where, match)
		if !match {
			newRows = append(newRows, row)
		}
	}

	fmt.Printf("After delete (JSON): %d rows\n", len(newRows))
	table.Rows = newRows
	return s.save()
}

func (s *JSONStorage) GetTable(tableName string) *Table {
	return s.InMemoryStorage.GetTable(tableName)
}

func (s *JSONStorage) Close() error {
	return s.save()
}

func (s *JSONStorage) ShowTables() ([]string, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	tables := make([]string, 0, len(s.db.Tables))
	for name := range s.db.Tables {
		tables = append(tables, name)
	}
	return tables, nil
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

	// Handle OR conditions
	if strings.Contains(where, " OR ") {
		conditions := strings.Split(where, " OR ")
		for _, condition := range conditions {
			if evaluateSimpleCondition(row, strings.TrimSpace(condition)) {
				return true
			}
		}
		return false
	}

	return evaluateSimpleCondition(row, where)
}

// Helper function to evaluate a simple condition (no AND/OR)
func evaluateSimpleCondition(row Row, condition string) bool {
	// Split on spaces to handle operators properly
	parts := strings.Fields(condition)
	if len(parts) < 3 {
		return false
	}

	column := parts[0]
	operator := parts[1]
	value := strings.Join(parts[2:], " ")
	value = strings.Trim(value, "'\"")

	rowValue, exists := row[column]
	if !exists {
		return false
	}

	// Convert row value to float64 for numeric comparisons
	var rowNum float64
	var isNumeric bool

	switch v := rowValue.(type) {
	case int:
		rowNum = float64(v)
		isNumeric = true
	case float64:
		rowNum = v
		isNumeric = true
	case json.Number:
		if n, err := v.Float64(); err == nil {
			rowNum = n
			isNumeric = true
		}
	}

	// Try numeric comparison first
	if isNumeric {
		if valueNum, err := strconv.ParseFloat(value, 64); err == nil {
			return compareNumbers(rowNum, valueNum, operator)
		}
	}

	// Fall back to string comparison
	rowStr := fmt.Sprintf("%v", rowValue)
	return compareStrings(rowStr, value, operator)
}

func compareNumbers(a, b float64, operator string) bool {
	switch operator {
	case "=":
		return a == b
	case "!=":
		return a != b
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	default:
		return false
	}
}

func compareStrings(a, b, operator string) bool {
	switch operator {
	case "=":
		return a == b
	case "!=":
		return a != b
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	default:
		return false
	}
}
