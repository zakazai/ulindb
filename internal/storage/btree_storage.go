package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/zakazai/ulin-db/internal/types"
)

const (
	// B-tree parameters
	maxKeys    = 4 // Maximum number of keys in a node
	minKeys    = maxKeys / 2
	pageSize   = 4096 // Size of a page in bytes
	headerSize = 16   // Size of page header in bytes
)

// BTreeNode represents a node in the B-tree
type BTreeNode struct {
	isLeaf   bool
	numKeys  int
	keys     []string
	values   [][]byte
	children []int64 // Page offsets for children
}

// BTreeStorage implements Storage interface using B-tree file storage
type BTreeStorage struct {
	file     *os.File
	root     int64 // Page offset of root node
	mu       sync.RWMutex
	tables   map[string]*types.Table
	pagePool sync.Pool
}

// NewBTreeStorage creates a new B-tree storage
func NewBTreeStorage(filePath string) (*BTreeStorage, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	storage := &BTreeStorage{
		file:   file,
		tables: make(map[string]*types.Table),
		pagePool: sync.Pool{
			New: func() interface{} {
				return make([]byte, pageSize)
			},
		},
	}

	// Initialize root node if file is empty
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if info.Size() == 0 {
		// Create root node
		node := &BTreeNode{
			isLeaf:   true,
			numKeys:  0,
			keys:     make([]string, maxKeys),
			values:   make([][]byte, maxKeys),
			children: make([]int64, maxKeys+1),
		}

		// Write root node to file
		offset, err := storage.writeNode(node)
		if err != nil {
			return nil, err
		}
		storage.root = offset
		
		// Write root offset to file header
		file.Seek(0, 0)
		if err := binary.Write(file, binary.BigEndian, offset); err != nil {
			return nil, err
		}
	} else {
		// Read root offset from file header
		file.Seek(0, 0)
		var rootOffset int64
		if err := binary.Read(file, binary.BigEndian, &rootOffset); err != nil {
			return nil, err
		}
		storage.root = rootOffset
		
		// Load table metadata from the B-tree
		if err := storage.loadTables(); err != nil {
			fmt.Printf("Warning: Error loading tables: %v\n", err)
			// Continue anyway, as this might be a new file
		}
	}

	return storage, nil
}

func (s *BTreeStorage) CreateTable(table *types.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[table.Name]; exists {
		return fmt.Errorf("table %s already exists", table.Name)
	}

	s.tables[table.Name] = table
	return s.writeTable(table)
}

func (s *BTreeStorage) Insert(tableName string, values map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	table, exists := s.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate all required columns are present
	row := make(types.Row)
	for _, col := range table.Columns {
		val, exists := values[col.Name]
		if !exists && !col.Nullable {
			return fmt.Errorf("missing required column %s", col.Name)
		}
		if exists {
			row[col.Name] = val
		}
	}

	// Insert the row
	return s.insertRow(tableName, row)
}

func (s *BTreeStorage) Select(tableName string, columns []string, where map[string]interface{}) ([]types.Row, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	table, exists := s.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// If no columns specified, use all columns from table
	if len(columns) == 0 {
		columns = make([]string, len(table.Columns))
		for i, col := range table.Columns {
			columns[i] = col.Name
		}
	} else {
		// Validate requested columns exist in table
		for _, col := range columns {
			found := false
			for _, tableCol := range table.Columns {
				if tableCol.Name == col {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column %s does not exist in table %s", col, tableName)
			}
		}
	}

	// Read all rows from B-tree
	allRows, err := s.readRows(tableName)
	if err != nil {
		return nil, err
	}

	// Filter rows based on where clause and select specified columns
	var results []types.Row
	for _, row := range allRows {
		if where == nil || s.matchesWhere(row, where) {
			result := make(types.Row)
			for _, col := range columns {
				if col == "*" {
					for k, v := range row {
						result[k] = v
					}
					break
				} else if val, ok := row[col]; ok {
					result[col] = val
				}
			}
			results = append(results, result)
		}
	}

	return results, nil
}

func (s *BTreeStorage) Update(tableName string, set map[string]interface{}, where map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[tableName]; !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Read all rows
	rows, err := s.readRows(tableName)
	if err != nil {
		return err
	}

	// Update matching rows
	rowsAffected := 0
	for i, row := range rows {
		if where == nil || s.matchesWhere(row, where) {
			for k, v := range set {
				rows[i][k] = v
			}
			rowsAffected++
		}
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no rows matched the WHERE clause")
	}

	// Write updated rows back to B-tree
	return s.writeRows(tableName, rows)
}

func (s *BTreeStorage) Delete(tableName string, where map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[tableName]; !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Read all rows
	rows, err := s.readRows(tableName)
	if err != nil {
		return err
	}

	// Filter out rows that match the where clause
	var newRows []types.Row
	rowsAffected := 0
	for _, row := range rows {
		if where == nil || !s.matchesWhere(row, where) {
			newRows = append(newRows, row)
		} else {
			rowsAffected++
		}
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no rows matched the WHERE clause")
	}

	// Write remaining rows back to B-tree
	return s.writeRows(tableName, newRows)
}

func (s *BTreeStorage) Close() error {
	return s.file.Close()
}

func (s *BTreeStorage) GetTable(tableName string) *types.Table {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tables[tableName]
}

// Helper functions for B-tree operations

func (s *BTreeStorage) writeNode(node *BTreeNode) (int64, error) {
	// Get a page buffer from the pool
	page := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(page)

	// Write node data to page buffer
	offset := int64(0)
	binary.BigEndian.PutUint64(page[offset:], uint64(node.numKeys))
	offset += 8
	if node.isLeaf {
		binary.BigEndian.PutUint64(page[offset:], 1)
	} else {
		binary.BigEndian.PutUint64(page[offset:], 0)
	}
	offset += 8

	// Write keys and values
	for i := 0; i < node.numKeys; i++ {
		// Write key length and key
		keyLen := len(node.keys[i])
		binary.BigEndian.PutUint32(page[offset:], uint32(keyLen))
		offset += 4
		copy(page[offset:], node.keys[i])
		offset += int64(keyLen)

		// Write value length and value
		valueLen := len(node.values[i])
		binary.BigEndian.PutUint32(page[offset:], uint32(valueLen))
		offset += 4
		copy(page[offset:], node.values[i])
		offset += int64(valueLen)
	}

	// Write child pointers
	if !node.isLeaf {
		for i := 0; i <= node.numKeys; i++ {
			binary.BigEndian.PutUint64(page[offset:], uint64(node.children[i]))
			offset += 8
		}
	}

	// Write page to file
	fileOffset, err := s.file.Seek(0, os.SEEK_END)
	if err != nil {
		return 0, err
	}

	if _, err := s.file.Write(page[:pageSize]); err != nil {
		return 0, err
	}

	return fileOffset, nil
}

func (s *BTreeStorage) readNode(offset int64) (*BTreeNode, error) {
	// Get a page buffer from the pool
	page := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(page)

	// Read page from file
	if _, err := s.file.ReadAt(page, offset); err != nil {
		return nil, err
	}

	// Read node data from page buffer
	node := &BTreeNode{
		keys:     make([]string, maxKeys),
		values:   make([][]byte, maxKeys),
		children: make([]int64, maxKeys+1),
	}

	bufOffset := int64(0)
	node.numKeys = int(binary.BigEndian.Uint64(page[bufOffset:]))
	bufOffset += 8
	node.isLeaf = binary.BigEndian.Uint64(page[bufOffset:]) == 1
	bufOffset += 8

	// Read keys and values
	for i := 0; i < node.numKeys; i++ {
		// Read key
		keyLen := binary.BigEndian.Uint32(page[bufOffset:])
		bufOffset += 4
		node.keys[i] = string(page[bufOffset : bufOffset+int64(keyLen)])
		bufOffset += int64(keyLen)

		// Read value
		valueLen := binary.BigEndian.Uint32(page[bufOffset:])
		bufOffset += 4
		node.values[i] = make([]byte, valueLen)
		copy(node.values[i], page[bufOffset:bufOffset+int64(valueLen)])
		bufOffset += int64(valueLen)
	}

	// Read child pointers
	if !node.isLeaf {
		for i := 0; i <= node.numKeys; i++ {
			node.children[i] = int64(binary.BigEndian.Uint64(page[bufOffset:]))
			bufOffset += 8
		}
	}

	return node, nil
}

func (s *BTreeStorage) writeTable(table *types.Table) error {
	// Serialize table metadata to JSON
	tableJSON, err := json.Marshal(table)
	if err != nil {
		return fmt.Errorf("failed to serialize table metadata: %v", err)
	}
	
	// Create a special key for table metadata
	key := fmt.Sprintf("__table__%s", table.Name)
	
	// Store in BTree
	return s.insert(key, tableJSON)
}

func (s *BTreeStorage) insertRow(tableName string, row types.Row) error {
	// Convert row to bytes
	key := fmt.Sprintf("%s:%d", tableName, len(row)) // Simple key format
	value, err := encodeRow(row)
	if err != nil {
		return err
	}

	// Insert into B-tree
	return s.insert(key, value)
}

func (s *BTreeStorage) insert(key string, value []byte) error {
	root, err := s.readNode(s.root)
	if err != nil {
		return err
	}

	if root.numKeys == maxKeys {
		// Split root
		newRoot := &BTreeNode{
			isLeaf:   false,
			numKeys:  0,
			keys:     make([]string, maxKeys),
			values:   make([][]byte, maxKeys),
			children: make([]int64, maxKeys+1),
		}

		// Write old root to new page
		oldRootOffset, err := s.writeNode(root)
		if err != nil {
			return err
		}

		newRoot.children[0] = oldRootOffset
		s.splitChild(newRoot, 0, root)

		// Write new root
		rootOffset, err := s.writeNode(newRoot)
		if err != nil {
			return err
		}
		s.root = rootOffset

		return s.insertNonFull(newRoot, key, value)
	}

	return s.insertNonFull(root, key, value)
}

func (s *BTreeStorage) insertNonFull(node *BTreeNode, key string, value []byte) error {
	i := node.numKeys - 1

	if node.isLeaf {
		// Insert into leaf node
		for i >= 0 && key < node.keys[i] {
			node.keys[i+1] = node.keys[i]
			node.values[i+1] = node.values[i]
			i--
		}
		i++
		node.keys[i] = key
		node.values[i] = value
		node.numKeys++

		// Write node back to disk
		offset, err := s.writeNode(node)
		if err != nil {
			return err
		}
		if offset == s.root {
			s.root = offset
		}
		return nil
	}

	// Find child to recurse to
	for i >= 0 && key < node.keys[i] {
		i--
	}
	i++

	child, err := s.readNode(node.children[i])
	if err != nil {
		return err
	}

	if child.numKeys == maxKeys {
		// Split child
		s.splitChild(node, i, child)
		if key > node.keys[i] {
			i++
		}
		child, err = s.readNode(node.children[i])
		if err != nil {
			return err
		}
	}

	return s.insertNonFull(child, key, value)
}

func (s *BTreeStorage) splitChild(parent *BTreeNode, i int, child *BTreeNode) error {
	newChild := &BTreeNode{
		isLeaf:   child.isLeaf,
		numKeys:  minKeys,
		keys:     make([]string, maxKeys),
		values:   make([][]byte, maxKeys),
		children: make([]int64, maxKeys+1),
	}

	// Copy keys and values to new child
	for j := 0; j < minKeys; j++ {
		newChild.keys[j] = child.keys[j+minKeys]
		newChild.values[j] = child.values[j+minKeys]
	}

	// Copy children if not leaf
	if !child.isLeaf {
		for j := 0; j <= minKeys; j++ {
			newChild.children[j] = child.children[j+minKeys]
		}
	}

	// Update child's key count
	child.numKeys = minKeys

	// Write new child to disk
	newChildOffset, err := s.writeNode(newChild)
	if err != nil {
		return err
	}

	// Write modified child to disk
	childOffset, err := s.writeNode(child)
	if err != nil {
		return err
	}

	// Update parent
	for j := parent.numKeys; j > i; j-- {
		parent.keys[j] = parent.keys[j-1]
		parent.values[j] = parent.values[j-1]
		parent.children[j+1] = parent.children[j]
	}

	parent.keys[i] = child.keys[minKeys-1]
	parent.values[i] = child.values[minKeys-1]
	parent.children[i] = childOffset
	parent.children[i+1] = newChildOffset
	parent.numKeys++

	// Write parent to disk
	parentOffset, err := s.writeNode(parent)
	if err != nil {
		return err
	}
	if parentOffset == s.root {
		s.root = parentOffset
	}

	return nil
}

func (s *BTreeStorage) readRows(tableName string) ([]types.Row, error) {
	var rows []types.Row
	root, err := s.readNode(s.root)
	if err != nil {
		return nil, err
	}

	return s.readRowsFromNode(root, tableName, rows)
}

func (s *BTreeStorage) readRowsFromNode(node *BTreeNode, tableName string, rows []types.Row) ([]types.Row, error) {
	for i := 0; i < node.numKeys; i++ {
		if !node.isLeaf {
			child, err := s.readNode(node.children[i])
			if err != nil {
				return nil, err
			}
			rows, err = s.readRowsFromNode(child, tableName, rows)
			if err != nil {
				return nil, err
			}
		}

		// Check if key belongs to this table
		if tableNameFromKey(node.keys[i]) == tableName {
			row, err := decodeRow(node.values[i])
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}

	if !node.isLeaf {
		child, err := s.readNode(node.children[node.numKeys])
		if err != nil {
			return nil, err
		}
		rows, err = s.readRowsFromNode(child, tableName, rows)
		if err != nil {
			return nil, err
		}
	}

	return rows, nil
}

func (s *BTreeStorage) writeRows(tableName string, rows []types.Row) error {
	// Delete all existing rows for this table
	if err := s.Delete(tableName, nil); err != nil {
		return err
	}

	// Insert new rows
	for _, row := range rows {
		if err := s.insertRow(tableName, row); err != nil {
			return err
		}
	}

	return nil
}

// Helper functions for encoding/decoding rows

func encodeRow(row types.Row) ([]byte, error) {
	return json.Marshal(row)
}

func decodeRow(data []byte) (types.Row, error) {
	var row types.Row
	err := json.Unmarshal(data, &row)
	return row, err
}

func tableNameFromKey(key string) string {
	parts := strings.Split(key, ":")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

func (s *BTreeStorage) validateDataType(value interface{}, columnType string) error {
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
		}
		return fmt.Errorf("value %v is not an integer", value)
	case "STRING", "TEXT":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("value %v is not a string", value)
		}
	}
	return nil
}

func (s *BTreeStorage) validateColumns(table *types.Table, columns []string) error {
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

func (s *BTreeStorage) validateColumnNames(table *types.Table, values map[string]interface{}) error {
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

func (s *BTreeStorage) validateWhereColumns(table *types.Table, where map[string]interface{}) error {
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

func (s *BTreeStorage) matchesWhere(row types.Row, where map[string]interface{}) bool {
	if where == nil {
		return true
	}

	for col, val := range where {
		rowVal, ok := row[col]
		if !ok || rowVal != val {
			return false
		}
	}
	return true
}

func (s *BTreeStorage) ShowTables() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables := make([]string, 0, len(s.tables))
	for name := range s.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

// loadTables scans the BTree for table metadata and loads it into memory
func (s *BTreeStorage) loadTables() error {
	root, err := s.readNode(s.root)
	if err != nil {
		return err
	}

	// Recursively scan all nodes for table metadata
	return s.loadTablesFromNode(root)
}

// loadTablesFromNode recursively scans a node and its children for table metadata
func (s *BTreeStorage) loadTablesFromNode(node *BTreeNode) error {
	for i := 0; i < node.numKeys; i++ {
		// Check each key to see if it's a table metadata key
		if strings.HasPrefix(node.keys[i], "__table__") {
			// Extract table name from key
			tableName := strings.TrimPrefix(node.keys[i], "__table__")
			
			// Deserialize table metadata
			var table types.Table
			if err := json.Unmarshal(node.values[i], &table); err != nil {
				return fmt.Errorf("failed to deserialize table metadata for %s: %v", tableName, err)
			}
			
			// Store table in memory
			s.tables[tableName] = &table
		}

		// Recursively check children if not a leaf node
		if !node.isLeaf {
			child, err := s.readNode(node.children[i])
			if err != nil {
				return err
			}
			if err := s.loadTablesFromNode(child); err != nil {
				return err
			}
		}
	}

	// Check the last child if not a leaf node
	if !node.isLeaf {
		child, err := s.readNode(node.children[node.numKeys])
		if err != nil {
			return err
		}
		if err := s.loadTablesFromNode(child); err != nil {
			return err
		}
	}

	return nil
}
