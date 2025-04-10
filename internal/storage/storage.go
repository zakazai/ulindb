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

// Database represents the entire database
type Database struct {
	Tables map[string]*types.Table
	mu     sync.RWMutex
}

// Storage interface defines the methods for database storage
type Storage interface {
	CreateTable(table *types.Table) error
	Insert(tableName string, values map[string]interface{}) error
	Select(tableName string, columns []string, where map[string]interface{}) ([]types.Row, error)
	Update(tableName string, set map[string]interface{}, where map[string]interface{}) error
	Delete(tableName string, where map[string]interface{}) error
	GetTable(tableName string) *types.Table
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
			Tables: make(map[string]*types.Table),
		},
	}
}

func (s *InMemoryStorage) CreateTable(table *types.Table) error {
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

func (s *InMemoryStorage) validateColumns(table *types.Table, columns []string) error {
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

func (s *InMemoryStorage) validateColumnNames(table *types.Table, values map[string]interface{}) error {
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

func (s *InMemoryStorage) validateWhereColumns(table *types.Table, where map[string]interface{}) error {
	if where == nil {
		return nil
	}

	columnMap := make(map[string]bool)
	for _, col := range table.Columns {
		columnMap[col.Name] = true
	}

	for colName := range where {
		if !columnMap[colName] {
			return fmt.Errorf("invalid column name in WHERE clause: %s", colName)
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
	row := make(types.Row)
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

func (s *InMemoryStorage) Select(tableName string, columns []string, where map[string]interface{}) ([]types.Row, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate columns
	if err := s.validateColumns(table, columns); err != nil {
		return nil, err
	}

	// Validate where columns
	if err := s.validateWhereColumns(table, where); err != nil {
		return nil, err
	}

	var result []types.Row
	for _, row := range table.Rows {
		if s.matchesWhere(row, where) {
			selectedRow := make(types.Row)
			if len(columns) == 1 && columns[0] == "*" {
				// Select all columns
				for k, v := range row {
					selectedRow[k] = v
				}
			} else {
				// Select specific columns
				for _, col := range columns {
					selectedRow[col] = row[col]
				}
			}
			result = append(result, selectedRow)
		}
	}
	return result, nil
}

func (s *InMemoryStorage) Update(tableName string, set map[string]interface{}, where map[string]interface{}) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate set columns
	if err := s.validateColumnNames(table, set); err != nil {
		return err
	}

	// Validate where columns
	if err := s.validateWhereColumns(table, where); err != nil {
		return err
	}

	// Validate data types for set values
	for colName, value := range set {
		for _, col := range table.Columns {
			if col.Name == colName {
				if err := s.validateDataType(value, col.Type); err != nil {
					return fmt.Errorf("invalid data type for column %s: %v", colName, err)
				}
				break
			}
		}
	}

	rowsAffected := 0
	for i := range table.Rows {
		if s.matchesWhere(table.Rows[i], where) {
			for colName, value := range set {
				table.Rows[i][colName] = value
			}
			rowsAffected++
		}
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no rows matched the WHERE clause")
	}
	return nil
}

func (s *InMemoryStorage) Delete(tableName string, where map[string]interface{}) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate where columns
	if err := s.validateWhereColumns(table, where); err != nil {
		return err
	}

	// Filter out rows that match the where clause
	var newRows []types.Row
	rowsAffected := 0
	for _, row := range table.Rows {
		if !s.matchesWhere(row, where) {
			newRows = append(newRows, row)
		} else {
			rowsAffected++
		}
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no rows matched the WHERE clause")
	}

	table.Rows = newRows
	return nil
}

func (s *InMemoryStorage) GetTable(tableName string) *types.Table {
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
	for tableName := range s.db.Tables {
		tables = append(tables, tableName)
	}
	return tables, nil
}

func (s *InMemoryStorage) matchesWhere(row types.Row, where map[string]interface{}) bool {
	if where == nil {
		return true
	}

	for col, val := range where {
		rowVal, ok := row[col]
		if !ok {
			return false
		}

		// Special handling for numeric comparisons
		switch v := val.(type) {
		case float64:
			switch rv := rowVal.(type) {
			case int:
				// Compare int to float64
				if float64(rv) == v {
					continue
				}
			case float64:
				// Compare float64 to float64
				if rv == v {
					continue
				}
			}
		case int:
			switch rv := rowVal.(type) {
			case int:
				// Compare int to int
				if rv == v {
					continue
				}
			case float64:
				// Compare float64 to int
				if rv == float64(v) {
					continue
				}
			}
		}

		// For non-numeric types or if numeric comparison didn't match
		if rowVal != val {
			return false
		}
	}
	return true
}

// JSONStorage implements Storage interface using JSON files
type JSONStorage struct {
	db         *Database
	dataDir    string
	filePrefix string
}

// NewJSONStorage creates a new JSON storage
func NewJSONStorage(dataDir, filePrefix string) (*JSONStorage, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	storage := &JSONStorage{
		db: &Database{
			Tables: make(map[string]*types.Table),
		},
		dataDir:    dataDir,
		filePrefix: filePrefix,
	}

	// Load existing tables
	if err := storage.loadTables(); err != nil {
		return nil, fmt.Errorf("failed to load tables: %v", err)
	}

	return storage, nil
}

// jsonTable is used for JSON serialization/deserialization
type jsonTable struct {
	Name    string                   `json:"name"`
	Columns []types.ColumnDefinition `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
}

func (s *JSONStorage) loadTables() error {
	files, err := filepath.Glob(filepath.Join(s.dataDir, s.filePrefix+"*.json"))
	if err != nil {
		return fmt.Errorf("failed to list table files: %v", err)
	}

	for _, file := range files {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read table file %s: %v", file, err)
		}

		var jsonTable jsonTable
		if err := json.Unmarshal(data, &jsonTable); err != nil {
			return fmt.Errorf("failed to unmarshal table data from %s: %v", file, err)
		}

		// Create column map for validation
		columnMap := make(map[string]bool)
		for _, col := range jsonTable.Columns {
			columnMap[col.Name] = true
		}

		table := &types.Table{
			Name:    jsonTable.Name,
			Columns: make([]types.ColumnDefinition, len(jsonTable.Columns)),
			Rows:    make([]types.Row, len(jsonTable.Rows)),
		}

		// Copy columns
		copy(table.Columns, jsonTable.Columns)

		// Copy rows with validation
		for i, row := range jsonTable.Rows {
			newRow := make(types.Row)
			for k, v := range row {
				if !columnMap[k] {
					return fmt.Errorf("invalid column %s in table %s", k, jsonTable.Name)
				}
				newRow[k] = v
			}
			table.Rows[i] = newRow
		}

		s.db.Tables[table.Name] = table
	}

	return nil
}

func (s *JSONStorage) saveTables() error {
	for tableName, table := range s.db.Tables {
		// Convert types.Row to map[string]interface{} for JSON serialization
		jsonRows := make([]map[string]interface{}, len(table.Rows))
		for i, row := range table.Rows {
			jsonRows[i] = row
		}

		jsonTable := jsonTable{
			Name:    tableName,
			Columns: table.Columns,
			Rows:    jsonRows,
		}

		data, err := json.MarshalIndent(jsonTable, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal table %s: %v", tableName, err)
		}

		filePath := filepath.Join(s.dataDir, s.filePrefix+tableName+".json")
		if err := ioutil.WriteFile(filePath, data, 0644); err != nil {
			return fmt.Errorf("failed to write table %s to file: %v", tableName, err)
		}
	}

	return nil
}

func (s *JSONStorage) CreateTable(table *types.Table) error {
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

	if err := s.saveTables(); err != nil {
		return fmt.Errorf("failed to save tables: %v", err)
	}

	return nil
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
	row := make(types.Row)
	for _, col := range table.Columns {
		val, exists := values[col.Name]
		if !exists && !col.Nullable {
			return fmt.Errorf("missing required column %s", col.Name)
		}
		if exists {
			if val == nil || val == "NULL" {
				if !col.Nullable {
					return fmt.Errorf("NULL value not allowed for non-nullable column %s", col.Name)
				}
				// Set default value based on column type
				if col.Type == "INT" {
					row[col.Name] = 0
				} else {
					row[col.Name] = ""
				}
			} else {
				if err := s.validateDataType(val, col.Type); err != nil {
					return fmt.Errorf("invalid data type for column %s: %v", col.Name, err)
				}
				row[col.Name] = val
			}
		}
	}

	table.Rows = append(table.Rows, row)

	if err := s.saveTables(); err != nil {
		return fmt.Errorf("failed to save tables: %v", err)
	}

	return nil
}

func (s *JSONStorage) Select(tableName string, columns []string, where map[string]interface{}) ([]types.Row, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate columns
	if err := s.validateColumns(table, columns); err != nil {
		return nil, err
	}

	// Validate where columns
	if err := s.validateWhereColumns(table, where); err != nil {
		return nil, err
	}

	var result []types.Row
	for _, row := range table.Rows {
		if s.matchesWhere(row, where) {
			selectedRow := make(types.Row)
			if len(columns) == 1 && columns[0] == "*" {
				// Select all columns
				for k, v := range row {
					selectedRow[k] = v
				}
			} else {
				// Select specific columns
				for _, col := range columns {
					selectedRow[col] = row[col]
				}
			}
			result = append(result, selectedRow)
		}
	}

	return result, nil
}

func (s *JSONStorage) Update(tableName string, set map[string]interface{}, where map[string]interface{}) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate set columns
	if err := s.validateColumnNames(table, set); err != nil {
		return err
	}

	// Validate where columns
	if err := s.validateWhereColumns(table, where); err != nil {
		return err
	}

	// Validate data types for set values
	for colName, value := range set {
		for _, col := range table.Columns {
			if col.Name == colName {
				if err := s.validateDataType(value, col.Type); err != nil {
					return fmt.Errorf("invalid data type for column %s: %v", colName, err)
				}
				break
			}
		}
	}

	rowsAffected := 0
	for i := range table.Rows {
		if s.matchesWhere(table.Rows[i], where) {
			for colName, value := range set {
				table.Rows[i][colName] = value
			}
			rowsAffected++
		}
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no rows matched the WHERE clause")
	}

	if err := s.saveTables(); err != nil {
		return fmt.Errorf("failed to save tables: %v", err)
	}

	return nil
}

func (s *JSONStorage) Delete(tableName string, where map[string]interface{}) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	table, exists := s.db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate where columns
	if err := s.validateWhereColumns(table, where); err != nil {
		return err
	}

	rowsAffected := 0
	var newRows []types.Row
	for _, row := range table.Rows {
		if !s.matchesWhere(row, where) {
			newRows = append(newRows, row)
		} else {
			rowsAffected++
		}
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no rows matched the WHERE clause")
	}

	table.Rows = newRows

	if err := s.saveTables(); err != nil {
		return fmt.Errorf("failed to save tables: %v", err)
	}

	return nil
}

func (s *JSONStorage) GetTable(tableName string) *types.Table {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	return s.db.Tables[tableName]
}

func (s *JSONStorage) Close() error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	return s.saveTables()
}

func (s *JSONStorage) ShowTables() ([]string, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	tables := make([]string, 0, len(s.db.Tables))
	for tableName := range s.db.Tables {
		tables = append(tables, tableName)
	}
	return tables, nil
}

func (s *JSONStorage) validateDataType(value interface{}, columnType string) error {
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

func (s *JSONStorage) validateColumns(table *types.Table, columns []string) error {
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

func (s *JSONStorage) validateColumnNames(table *types.Table, values map[string]interface{}) error {
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

func (s *JSONStorage) validateWhereColumns(table *types.Table, where map[string]interface{}) error {
	if where == nil {
		return nil
	}

	columnMap := make(map[string]bool)
	for _, col := range table.Columns {
		columnMap[col.Name] = true
	}

	for colName := range where {
		if !columnMap[colName] {
			return fmt.Errorf("invalid column name in WHERE clause: %s", colName)
		}
	}
	return nil
}

func (s *JSONStorage) matchesWhere(row types.Row, where map[string]interface{}) bool {
	if where == nil {
		return true
	}

	for col, val := range where {
		rowVal, ok := row[col]
		if !ok {
			return false
		}

		// Special handling for numeric comparisons
		switch v := val.(type) {
		case float64:
			switch rv := rowVal.(type) {
			case int:
				// Compare int to float64
				if float64(rv) == v {
					continue
				}
			case float64:
				// Compare float64 to float64
				if rv == v {
					continue
				}
			}
		case int:
			switch rv := rowVal.(type) {
			case int:
				// Compare int to int
				if rv == v {
					continue
				}
			case float64:
				// Compare float64 to int
				if rv == float64(v) {
					continue
				}
			}
		}

		// For non-numeric types or if numeric comparison didn't match
		if rowVal != val {
			return false
		}
	}
	return true
}

// Helper function to evaluate WHERE conditions
func evaluateWhere(row types.Row, where string) bool {
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
func evaluateSimpleCondition(row types.Row, condition string) bool {
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
