package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	// Make sure the directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for BTree file: %v", err)
	}

	// Open file with direct I/O mode if possible for better performance
	fmt.Printf("DEBUG: Opening BTree file at %s\n", filePath)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open BTree file: %v", err)
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
		return nil, fmt.Errorf("failed to stat file: %v", err)
	}

	fmt.Printf("DEBUG: BTree file size: %d bytes\n", info.Size())
	if info.Size() == 0 {
		fmt.Println("DEBUG: Creating new BTree file with empty root")
		
		// Initialize the file with a root offset of 0 (no data yet)
		storage.root = 0
		file.Seek(0, 0)
		if err := binary.Write(file, binary.BigEndian, int64(0)); err != nil {
			return nil, fmt.Errorf("failed to write initial root offset: %v", err)
		}
		
		// Sync the file to ensure changes are written
		if err := file.Sync(); err != nil {
			fmt.Printf("DEBUG: Error syncing file: %v\n", err)
		}
		
		fmt.Printf("DEBUG: Initialized empty BTree file with root offset 0\n")
	} else {
		// Read root offset from file header
		fmt.Println("DEBUG: Reading root offset from existing file")
		file.Seek(0, 0)
		var rootOffset int64
		if err := binary.Read(file, binary.BigEndian, &rootOffset); err != nil {
			return nil, fmt.Errorf("failed to read root offset from header: %v", err)
		}
		storage.root = rootOffset
		fmt.Printf("DEBUG: Read root offset: %d\n", rootOffset)
		
		// Load table metadata from the B-tree
		fmt.Println("DEBUG: Loading tables from BTree")
		if err := storage.loadTables(); err != nil {
			fmt.Printf("Warning: Error loading tables: %v\n", err)
			// Continue anyway, as this might be a new file
		}
		
		fmt.Printf("DEBUG: Loaded %d tables from BTree\n", len(storage.tables))
		for tableName := range storage.tables {
			fmt.Printf("DEBUG: Found table: %s\n", tableName)
		}
	}

	return storage, nil
}

func (s *BTreeStorage) CreateTable(table *types.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Printf("DEBUG: BTreeStorage.CreateTable called for table '%s'\n", table.Name)
	
	// Initialize the tables map if it's nil
	if s.tables == nil {
		s.tables = make(map[string]*types.Table)
	}

	if _, exists := s.tables[table.Name]; exists {
		fmt.Printf("DEBUG: Table '%s' already exists in memory\n", table.Name)
		return fmt.Errorf("table %s already exists", table.Name)
	}

	// Store table in memory first
	s.tables[table.Name] = table
	fmt.Printf("DEBUG: Table '%s' added to in-memory tables map\n", table.Name)
	
	// Then persist to disk
	err := s.writeTable(table)
	if err != nil {
		// Remove from memory if write failed
		delete(s.tables, table.Name)
		fmt.Printf("DEBUG: Failed to write table '%s' to disk: %v\n", table.Name, err)
		return err
	}
	
	fmt.Printf("DEBUG: Successfully created table '%s' in BTree storage\n", table.Name)
	return nil
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

	fmt.Printf("DEBUG: BTreeStorage.Select called for table '%s', columns %v\n", tableName, columns)

	table, exists := s.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Handle * (select all columns) case
	allColumns := false
	if len(columns) == 1 && columns[0] == "*" {
		fmt.Println("DEBUG: Processing * (select all columns)")
		allColumns = true
		columns = make([]string, len(table.Columns))
		for i, col := range table.Columns {
			columns[i] = col.Name
		}
	} else if len(columns) == 0 {
		// If no columns specified, use all columns from table
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
			if allColumns {
				// For * just copy the whole row
				for k, v := range row {
					result[k] = v
				}
			} else {
				// Select only the requested columns
				for _, col := range columns {
					if val, ok := row[col]; ok {
						result[col] = val
					}
				}
			}
			results = append(results, result)
		}
	}

	fmt.Printf("DEBUG: BTreeStorage.Select returning %d rows\n", len(results))
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
	fmt.Printf("DEBUG: writeNode called, node has %d keys\n", node.numKeys)
	
	// Get a page buffer from the pool
	page := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(page)

	// Clear the page buffer first to avoid any old data
	for i := range page {
		page[i] = 0
	}

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
		
		fmt.Printf("DEBUG: Wrote key %d: '%s' (len=%d)\n", i, node.keys[i], keyLen)

		// Write value length and value
		valueLen := len(node.values[i])
		binary.BigEndian.PutUint32(page[offset:], uint32(valueLen))
		offset += 4
		copy(page[offset:], node.values[i])
		offset += int64(valueLen)
		
		fmt.Printf("DEBUG: Wrote value %d (len=%d)\n", i, valueLen)
	}

	// Write child pointers
	if !node.isLeaf {
		for i := 0; i <= node.numKeys; i++ {
			binary.BigEndian.PutUint64(page[offset:], uint64(node.children[i]))
			offset += 8
			fmt.Printf("DEBUG: Wrote child pointer %d: %d\n", i, node.children[i])
		}
	}

	// Write page to file
	fileOffset, err := s.file.Seek(0, os.SEEK_END)
	if err != nil {
		fmt.Printf("DEBUG: Error seeking to end of file: %v\n", err)
		return 0, err
	}
	
	fmt.Printf("DEBUG: Writing page at offset %d\n", fileOffset)
	if _, err := s.file.Write(page[:pageSize]); err != nil {
		fmt.Printf("DEBUG: Error writing page: %v\n", err)
		return 0, err
	}
	
	// Sync to ensure data is written to disk
	if err := s.file.Sync(); err != nil {
		fmt.Printf("DEBUG: Error syncing file: %v\n", err)
		// Continue anyway, as this might not be critical
	}

	return fileOffset, nil
}

func (s *BTreeStorage) readNode(offset int64) (*BTreeNode, error) {
	fmt.Printf("DEBUG: readNode called at offset %d\n", offset)
	
	// Get a page buffer from the pool
	page := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(page)

	// Read page from file
	bytesRead, err := s.file.ReadAt(page, offset)
	if err != nil {
		fmt.Printf("DEBUG: Error reading page: %v\n", err)
		return nil, err
	}
	fmt.Printf("DEBUG: Read %d bytes from file at offset %d\n", bytesRead, offset)

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
	
	fmt.Printf("DEBUG: Node has %d keys, isLeaf=%v\n", node.numKeys, node.isLeaf)

	// Read keys and values
	for i := 0; i < node.numKeys; i++ {
		// Read key
		keyLen := binary.BigEndian.Uint32(page[bufOffset:])
		bufOffset += 4
		if keyLen > 0 && bufOffset+int64(keyLen) <= pageSize {
			node.keys[i] = string(page[bufOffset : bufOffset+int64(keyLen)])
			fmt.Printf("DEBUG: Read key %d: '%s' (len=%d)\n", i, node.keys[i], keyLen)
		} else {
			fmt.Printf("DEBUG: Invalid key length %d at offset %d\n", keyLen, bufOffset)
			return nil, fmt.Errorf("invalid key length %d at offset %d", keyLen, bufOffset)
		}
		bufOffset += int64(keyLen)

		// Read value
		valueLen := binary.BigEndian.Uint32(page[bufOffset:])
		bufOffset += 4
		if valueLen > 0 && bufOffset+int64(valueLen) <= pageSize {
			node.values[i] = make([]byte, valueLen)
			copy(node.values[i], page[bufOffset:bufOffset+int64(valueLen)])
			fmt.Printf("DEBUG: Read value %d (len=%d)\n", i, valueLen)
		} else {
			fmt.Printf("DEBUG: Invalid value length %d at offset %d\n", valueLen, bufOffset)
			return nil, fmt.Errorf("invalid value length %d at offset %d", valueLen, bufOffset)
		}
		bufOffset += int64(valueLen)
	}

	// Read child pointers
	if !node.isLeaf {
		for i := 0; i <= node.numKeys; i++ {
			node.children[i] = int64(binary.BigEndian.Uint64(page[bufOffset:]))
			bufOffset += 8
			fmt.Printf("DEBUG: Read child pointer %d: %d\n", i, node.children[i])
		}
	}

	return node, nil
}

func (s *BTreeStorage) writeTable(table *types.Table) error {
	// Store the table in memory first
	s.tables[table.Name] = table
	
	// Serialize table metadata to JSON
	tableJSON, err := json.Marshal(table)
	if err != nil {
		return fmt.Errorf("failed to serialize table metadata: %v", err)
	}
	
	// Create a special key for table metadata
	key := fmt.Sprintf("__table__%s", table.Name)
	
	// Log debugging info
	fmt.Printf("DEBUG: Writing table metadata for '%s' with key '%s'\n", table.Name, key)
	fmt.Printf("DEBUG: Table schema: %v\n", table.Columns)
	
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
	fmt.Printf("DEBUG: Inserting key '%s' into BTree\n", key)
	
	// For simplicity, we'll maintain two distinct pages for different types of data:
	// - Page 1 (offset 8): for table metadata (keys with "__table__" prefix)
	// - Page 2 (offset 8 + pageSize): for actual data rows
	
	page := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(page)
	
	// Clear the page
	for i := range page {
		page[i] = 0
	}
	
	// Create a simple node with just our key/value
	node := &BTreeNode{
		isLeaf:   true,
		numKeys:  1,
		keys:     make([]string, maxKeys),
		values:   make([][]byte, maxKeys),
		children: make([]int64, maxKeys+1),
	}
	node.keys[0] = key
	node.values[0] = value
	
	// Serialize the node
	offset := int64(0)
	binary.BigEndian.PutUint64(page[offset:], uint64(node.numKeys))
	offset += 8
	binary.BigEndian.PutUint64(page[offset:], 1) // isLeaf = true
	offset += 8
	
	// Write key and value
	keyLen := len(node.keys[0])
	binary.BigEndian.PutUint32(page[offset:], uint32(keyLen))
	offset += 4
	copy(page[offset:], node.keys[0])
	offset += int64(keyLen)
	
	valueLen := len(node.values[0])
	binary.BigEndian.PutUint32(page[offset:], uint32(valueLen))
	offset += 4
	copy(page[offset:], node.values[0])
	
	// Determine the write offset based on key type
	var dataOffset int64
	if strings.HasPrefix(key, "__table__") {
		// Table metadata goes to page 1
		dataOffset = 8
	} else {
		// Data rows go to page 2
		dataOffset = 8 + pageSize
	}
	
	fmt.Printf("DEBUG: Writing node to offset %d based on key type\n", dataOffset)
	
	// Write to the determined offset
	if _, err := s.file.WriteAt(page[:pageSize], dataOffset); err != nil {
		fmt.Printf("DEBUG: Error writing node: %v\n", err)
		return err
	}
	
	// Set the root pointer to page 1 (metadata) so it's found on reload
	const metadataOffset = 8
	s.file.Seek(0, 0)
	if err := binary.Write(s.file, binary.BigEndian, int64(metadataOffset)); err != nil {
		fmt.Printf("DEBUG: Error writing root offset to header: %v\n", err)
		return err
	}
	s.root = metadataOffset
	
	fmt.Printf("DEBUG: Wrote node with key '%s' at offset %d\n", key, dataOffset)
	fmt.Printf("DEBUG: Root offset remains at %d (metadata page)\n", metadataOffset)
	
	// Force a sync to ensure data is written to disk
	if err := s.file.Sync(); err != nil {
		fmt.Printf("DEBUG: Error syncing file: %v\n", err)
	}
	
	return nil
}

func (s *BTreeStorage) insertNonFull(node *BTreeNode, key string, value []byte) error {
	fmt.Printf("DEBUG: insertNonFull for key '%s', node has %d keys\n", key, node.numKeys)
	i := node.numKeys - 1

	if node.isLeaf {
		// Insert into leaf node
		fmt.Println("DEBUG: Inserting into leaf node")
		for i >= 0 && key < node.keys[i] {
			node.keys[i+1] = node.keys[i]
			node.values[i+1] = node.values[i]
			i--
		}
		i++
		node.keys[i] = key
		node.values[i] = value
		node.numKeys++
		fmt.Printf("DEBUG: Leaf node now has %d keys\n", node.numKeys)

		// Write node back to disk
		offset, err := s.writeNode(node)
		if err != nil {
			fmt.Printf("DEBUG: Error writing node: %v\n", err)
			return err
		}
		
		fmt.Printf("DEBUG: Wrote node at offset %d\n", offset)
		
		// If this is the root node (or we're updating the root),
		// update the root pointer and file header
		if s.root == 0 || offset == 0 {
			s.root = offset
			fmt.Printf("DEBUG: Updated root offset to %d\n", s.root)
			
			// Update root offset in file header
			s.file.Seek(0, 0)
			if err := binary.Write(s.file, binary.BigEndian, offset); err != nil {
				fmt.Printf("DEBUG: Error writing root offset to header: %v\n", err)
				return err
			}
			fmt.Println("DEBUG: Updated root offset in file header")
		}
		return nil
	}

	// Find child to recurse to
	fmt.Println("DEBUG: Non-leaf node, finding child to recurse to")
	for i >= 0 && key < node.keys[i] {
		i--
	}
	i++
	fmt.Printf("DEBUG: Selected child %d\n", i)

	child, err := s.readNode(node.children[i])
	if err != nil {
		fmt.Printf("DEBUG: Error reading child node: %v\n", err)
		return err
	}

	if child.numKeys == maxKeys {
		// Split child
		fmt.Printf("DEBUG: Child node is full, splitting\n")
		err = s.splitChild(node, i, child)
		if err != nil {
			fmt.Printf("DEBUG: Error splitting child: %v\n", err)
			return err
		}
		
		if key > node.keys[i] {
			i++
			fmt.Printf("DEBUG: After split, moving to child %d\n", i)
		}
		
		child, err = s.readNode(node.children[i])
		if err != nil {
			fmt.Printf("DEBUG: Error reading child after split: %v\n", err)
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
	fmt.Printf("DEBUG: readRows called for table '%s'\n", tableName)
	
	// Create an empty result set
	var rows []types.Row
	
	// Read the data page (page 2)
	dataOffset := int64(8 + pageSize) // Data is stored in page 2
	
	// Read the page
	page := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(page)
	
	bytesRead, err := s.file.ReadAt(page, dataOffset)
	if err != nil && err != io.EOF {
		fmt.Printf("DEBUG: Error reading data page: %v\n", err)
		return nil, err
	}
	fmt.Printf("DEBUG: Read %d bytes from data page at offset %d\n", bytesRead, dataOffset)
	
	// Parse the node header
	bufOffset := int64(0)
	numKeys := int(binary.BigEndian.Uint64(page[bufOffset:]))
	bufOffset += 8
	isLeaf := binary.BigEndian.Uint64(page[bufOffset:]) == 1
	bufOffset += 8
	
	fmt.Printf("DEBUG: Data node has %d keys, isLeaf=%v\n", numKeys, isLeaf)
	
	// Process all keys in the node
	for i := 0; i < numKeys; i++ {
		// Read key
		keyLen := binary.BigEndian.Uint32(page[bufOffset:])
		bufOffset += 4
		if keyLen == 0 || bufOffset+int64(keyLen) > pageSize {
			fmt.Printf("DEBUG: Invalid key length %d at offset %d\n", keyLen, bufOffset)
			continue
		}
		
		key := string(page[bufOffset : bufOffset+int64(keyLen)])
		bufOffset += int64(keyLen)
		fmt.Printf("DEBUG: Found key '%s'\n", key)
		
		// Check if this key belongs to our target table
		rowTableName := tableNameFromKey(key)
		fmt.Printf("DEBUG: Key belongs to table '%s'\n", rowTableName)
		
		// Read value
		valueLen := binary.BigEndian.Uint32(page[bufOffset:])
		bufOffset += 4
		if valueLen == 0 || bufOffset+int64(valueLen) > pageSize {
			fmt.Printf("DEBUG: Invalid value length %d at offset %d\n", valueLen, bufOffset)
			continue
		}
		
		value := make([]byte, valueLen)
		copy(value, page[bufOffset:bufOffset+int64(valueLen)])
		bufOffset += int64(valueLen)
		
		// If the row belongs to our table, decode and add it
		if rowTableName == tableName {
			row, err := decodeRow(value)
			if err != nil {
				fmt.Printf("DEBUG: Error decoding row: %v\n", err)
				continue
			}
			
			fmt.Printf("DEBUG: Adding row: %v\n", row)
			rows = append(rows, row)
		}
	}
	
	fmt.Printf("DEBUG: Found %d rows for table '%s'\n", len(rows), tableName)
	return rows, nil
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
	fmt.Printf("DEBUG: tableNameFromKey called with key: %s\n", key)
	
	// Skip meta table keys
	if strings.HasPrefix(key, "__table__") {
		tableName := strings.TrimPrefix(key, "__table__")
		fmt.Printf("DEBUG: Extracted table name from metadata key: %s\n", tableName)
		return tableName
	}
	
	// Regular row keys
	parts := strings.Split(key, ":")
	if len(parts) > 0 {
		fmt.Printf("DEBUG: Extracted table name from row key: %s\n", parts[0])
		return parts[0]
	}
	
	fmt.Printf("DEBUG: Could not extract table name from key: %s\n", key)
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

	fmt.Printf("DEBUG: BTreeStorage.ShowTables called. In-memory tables: %v\n", s.tables)
	
	// If tables map is empty, try reloading from disk
	if len(s.tables) == 0 {
		fmt.Println("DEBUG: No tables in memory, reloading from disk...")
		if err := s.loadTables(); err != nil {
			fmt.Printf("DEBUG: Error loading tables from disk: %v\n", err)
		}
	}

	tables := make([]string, 0, len(s.tables))
	for name := range s.tables {
		tables = append(tables, name)
	}
	fmt.Printf("DEBUG: Returning tables: %v\n", tables)
	return tables, nil
}

// loadTables scans the BTree for table metadata and loads it into memory
func (s *BTreeStorage) loadTables() error {
	fmt.Println("DEBUG: Loading tables from BTree storage...")
	
	// Initialize tables map if needed
	if s.tables == nil {
		s.tables = make(map[string]*types.Table)
	}

	// Read the root offset
	var rootOffset int64
	s.file.Seek(0, 0)
	if err := binary.Read(s.file, binary.BigEndian, &rootOffset); err != nil {
		fmt.Printf("DEBUG: Error reading root offset: %v\n", err)
		return err
	}
	fmt.Printf("DEBUG: Root offset from header: %d\n", rootOffset)
	
	// If root offset is 0, file is empty
	if rootOffset == 0 {
		fmt.Println("DEBUG: Root offset is 0, no data in file")
		return nil
	}
	
	// Read the page at the root offset
	page := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(page)
	
	bytesRead, err := s.file.ReadAt(page, rootOffset)
	if err != nil {
		fmt.Printf("DEBUG: Error reading page at offset %d: %v\n", rootOffset, err)
		return err
	}
	fmt.Printf("DEBUG: Read %d bytes from offset %d\n", bytesRead, rootOffset)
	
	// Parse the page
	bufOffset := int64(0)
	numKeys := int(binary.BigEndian.Uint64(page[bufOffset:]))
	bufOffset += 8
	isLeaf := binary.BigEndian.Uint64(page[bufOffset:]) == 1
	bufOffset += 8
	
	fmt.Printf("DEBUG: Node has %d keys, isLeaf=%v\n", numKeys, isLeaf)
	
	// Read each key/value pair
	for i := 0; i < numKeys; i++ {
		// Read key
		keyLen := binary.BigEndian.Uint32(page[bufOffset:])
		bufOffset += 4
		if keyLen == 0 || bufOffset+int64(keyLen) > pageSize {
			fmt.Printf("DEBUG: Invalid key length %d at offset %d\n", keyLen, bufOffset)
			continue
		}
		
		key := string(page[bufOffset : bufOffset+int64(keyLen)])
		bufOffset += int64(keyLen)
		fmt.Printf("DEBUG: Key %d: '%s'\n", i, key)
		
		// Read value length
		valueLen := binary.BigEndian.Uint32(page[bufOffset:])
		bufOffset += 4
		if valueLen == 0 || bufOffset+int64(valueLen) > pageSize {
			fmt.Printf("DEBUG: Invalid value length %d at offset %d\n", valueLen, bufOffset)
			continue
		}
		
		// Make a copy of the value data
		value := make([]byte, valueLen)
		copy(value, page[bufOffset:bufOffset+int64(valueLen)])
		bufOffset += int64(valueLen)
		
		// If this is a table metadata key, deserialize it
		if strings.HasPrefix(key, "__table__") {
			tableName := strings.TrimPrefix(key, "__table__")
			fmt.Printf("DEBUG: Found table metadata for '%s'\n", tableName)
			
			var table types.Table
			if err := json.Unmarshal(value, &table); err != nil {
				fmt.Printf("DEBUG: Error deserializing table metadata: %v\n", err)
				continue
			}
			
			fmt.Printf("DEBUG: Successfully loaded table '%s' with %d columns\n", 
				table.Name, len(table.Columns))
			
			// Store in memory
			s.tables[tableName] = &table
		}
	}
	
	fmt.Printf("DEBUG: Loaded %d tables from BTree\n", len(s.tables))
	return nil
}

// loadTablesFromNode recursively scans a node and its children for table metadata
func (s *BTreeStorage) loadTablesFromNode(node *BTreeNode) error {
	for i := 0; i < node.numKeys; i++ {
		fmt.Printf("DEBUG: Checking key: %s\n", node.keys[i])
		
		// Check each key to see if it's a table metadata key
		if strings.HasPrefix(node.keys[i], "__table__") {
			// Extract table name from key
			tableName := strings.TrimPrefix(node.keys[i], "__table__")
			fmt.Printf("DEBUG: Found table metadata for '%s'\n", tableName)
			
			// Deserialize table metadata
			var table types.Table
			if err := json.Unmarshal(node.values[i], &table); err != nil {
				fmt.Printf("DEBUG: Failed to deserialize table metadata: %v\n", err)
				return fmt.Errorf("failed to deserialize table metadata for %s: %v", tableName, err)
			}
			
			fmt.Printf("DEBUG: Successfully deserialized table '%s' with %d columns\n", 
				table.Name, len(table.Columns))
			
			// Store table in memory
			s.tables[tableName] = &table
		}

		// Recursively check children if not a leaf node
		if !node.isLeaf {
			fmt.Printf("DEBUG: Checking child node %d\n", i)
			child, err := s.readNode(node.children[i])
			if err != nil {
				fmt.Printf("DEBUG: Error reading child node: %v\n", err)
				return err
			}
			if err := s.loadTablesFromNode(child); err != nil {
				return err
			}
		}
	}

	// Check the last child if not a leaf node
	if !node.isLeaf {
		fmt.Printf("DEBUG: Checking last child node %d\n", node.numKeys)
		child, err := s.readNode(node.children[node.numKeys])
		if err != nil {
			fmt.Printf("DEBUG: Error reading last child node: %v\n", err)
			return err
		}
		if err := s.loadTablesFromNode(child); err != nil {
			return err
		}
	}

	return nil
}
