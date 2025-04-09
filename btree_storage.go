package ulindb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
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
	tables   map[string]*Table
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
		tables: make(map[string]*Table),
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
	} else {
		// Read root offset from file header
		var rootOffset int64
		if err := binary.Read(file, binary.BigEndian, &rootOffset); err != nil {
			return nil, err
		}
		storage.root = rootOffset
	}

	return storage, nil
}

func (s *BTreeStorage) CreateTable(table *Table) error {
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

	// Insert the row
	return s.insertRow(tableName, row)
}

func (s *BTreeStorage) Select(tableName string, columns []string, where string) ([]Row, error) {
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
			columns[i] = col.name
		}
	} else {
		// Validate requested columns exist in table
		for _, col := range columns {
			found := false
			for _, tableCol := range table.Columns {
				if tableCol.name == col {
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
	var results []Row
	for _, row := range allRows {
		if where == "" || evaluateWhere(row, where) {
			result := make(Row)
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

func (s *BTreeStorage) Update(tableName string, set map[string]interface{}, where string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	table, exists := s.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Read all rows
	rows, err := s.readRows(tableName)
	if err != nil {
		return err
	}

	// Update matching rows
	for i, row := range rows {
		if where == "" || evaluateWhere(row, where) {
			for k, v := range set {
				row[k] = v
			}
			rows[i] = row
		}
	}

	// Write back all rows
	return s.writeRows(tableName, rows)
}

func (s *BTreeStorage) Delete(tableName string, where string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	table, exists := s.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Read all rows
	rows, err := s.readRows(tableName)
	if err != nil {
		return err
	}

	// Filter out rows that match where clause
	var newRows []Row
	for _, row := range rows {
		if where == "" || !evaluateWhere(row, where) {
			newRows = append(newRows, row)
		}
	}

	// Write back remaining rows
	return s.writeRows(tableName, newRows)
}

func (s *BTreeStorage) Close() error {
	return s.file.Close()
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

func (s *BTreeStorage) writeTable(table *Table) error {
	// For simplicity, we'll store table metadata in memory
	// In a real implementation, you would want to persist this to disk
	return nil
}

func (s *BTreeStorage) insertRow(tableName string, row Row) error {
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

func (s *BTreeStorage) readRows(tableName string) ([]Row, error) {
	var rows []Row
	root, err := s.readNode(s.root)
	if err != nil {
		return nil, err
	}

	return s.readRowsFromNode(root, tableName, rows)
}

func (s *BTreeStorage) readRowsFromNode(node *BTreeNode, tableName string, rows []Row) ([]Row, error) {
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

func (s *BTreeStorage) writeRows(tableName string, rows []Row) error {
	// Delete all existing rows for this table
	if err := s.Delete(tableName, ""); err != nil {
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

func encodeRow(row Row) ([]byte, error) {
	return json.Marshal(row)
}

func decodeRow(data []byte) (Row, error) {
	var row Row
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
