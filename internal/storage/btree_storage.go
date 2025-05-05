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
	"time"

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
	types.GlobalLogger.Debug("Opening BTree file at %s", filePath)
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

	types.GlobalLogger.Debug("BTree file size: %d bytes", info.Size())
	if info.Size() == 0 {
		types.GlobalLogger.Debug("Creating new BTree file with empty root")
		
		// Initialize the file with a root offset of 0 (no data yet)
		storage.root = 0
		file.Seek(0, 0)
		if err := binary.Write(file, binary.BigEndian, int64(0)); err != nil {
			return nil, fmt.Errorf("failed to write initial root offset: %v", err)
		}
		
		// Sync the file to ensure changes are written
		if err := file.Sync(); err != nil {
			types.GlobalLogger.Debug("Error syncing file: %v", err)
		}
		
		types.GlobalLogger.Debug("Initialized empty BTree file with root offset 0")
	} else {
		// Read root offset from file header
		types.GlobalLogger.Debug("Reading root offset from existing file")
		file.Seek(0, 0)
		var rootOffset int64
		if err := binary.Read(file, binary.BigEndian, &rootOffset); err != nil {
			return nil, fmt.Errorf("failed to read root offset from header: %v", err)
		}
		storage.root = rootOffset
		types.GlobalLogger.Debug("Read root offset: %d", rootOffset)
		
		// Load table metadata from the B-tree
		types.GlobalLogger.Debug("Loading tables from BTree")
		if err := storage.loadTables(); err != nil {
			types.GlobalLogger.Warning("Error loading tables: %v", err)
			// Continue anyway, as this might be a new file
		}
		
		types.GlobalLogger.Debug("Loaded %d tables from BTree", len(storage.tables))
		for tableName := range storage.tables {
			types.GlobalLogger.Debug("Found table: %s", tableName)
		}
	}

	return storage, nil
}

func (s *BTreeStorage) CreateTable(table *types.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	types.GlobalLogger.Debug("BTreeStorage.CreateTable called for table '%s'", table.Name)
	
	// Initialize the tables map if it's nil
	if s.tables == nil {
		s.tables = make(map[string]*types.Table)
	}

	if _, exists := s.tables[table.Name]; exists {
		types.GlobalLogger.Debug("Table '%s' already exists in memory", table.Name)
		return fmt.Errorf("table %s already exists", table.Name)
	}

	// Store table in memory first
	s.tables[table.Name] = table
	types.GlobalLogger.Debug("Table '%s' added to in-memory tables map", table.Name)
	
	// Then persist to disk
	err := s.writeTable(table)
	if err != nil {
		// Remove from memory if write failed
		delete(s.tables, table.Name)
		types.GlobalLogger.Debug("Failed to write table '%s' to disk: %v", table.Name, err)
		return err
	}
	
	types.GlobalLogger.Debug("Successfully created table '%s' in BTree storage", table.Name)
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

	types.GlobalLogger.Debug("BTreeStorage.Select called for table '%s', columns %v", tableName, columns)

	table, exists := s.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Check for COUNT(*) query
	isCountQuery := false
	if len(columns) == 1 && strings.HasPrefix(strings.ToUpper(columns[0]), "COUNT(") {
		types.GlobalLogger.Debug("Processing COUNT query")
		isCountQuery = true
	}

	// Handle * (select all columns) case
	allColumns := false
	if len(columns) == 1 && columns[0] == "*" && !isCountQuery {
		types.GlobalLogger.Debug("Processing * (select all columns)")
		allColumns = true
		columns = make([]string, len(table.Columns))
		for i, col := range table.Columns {
			columns[i] = col.Name
		}
	} else if len(columns) == 0 && !isCountQuery {
		// If no columns specified, use all columns from table
		columns = make([]string, len(table.Columns))
		for i, col := range table.Columns {
			columns[i] = col.Name
		}
	} else if !isCountQuery {
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

	// Count matching rows for COUNT(*) query
	if isCountQuery {
		var matchingRows int
		for _, row := range allRows {
			if where == nil || s.matchesWhere(row, where) {
				matchingRows++
			}
		}
		
		// Create a result row with the count
		countResult := make(types.Row)
		countResult["count"] = matchingRows
		
		fmt.Printf("DEBUG: COUNT(*) query returning count = %d\n", matchingRows)
		return []types.Row{countResult}, nil
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
	// Generate a unique key with timestamp to avoid overwrites
	key := fmt.Sprintf("%s:%d:%d", tableName, len(row), time.Now().UnixNano())
	fmt.Printf("DEBUG: Generated unique row key: %s\n", key)
	
	// Convert row to bytes
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
	// - Page 2+ (offset 8 + pageSize*n): for actual data rows
	
	// Determine whether this is a metadata or data key
	isMetadata := strings.HasPrefix(key, "__table__")
	
	// If it's a metadata key, write to page 1
	if isMetadata {
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
		
		// Write to metadata page
		const metadataOffset = 8
		fmt.Printf("DEBUG: Writing metadata to offset %d\n", metadataOffset)
		
		if _, err := s.file.WriteAt(page[:pageSize], metadataOffset); err != nil {
			fmt.Printf("DEBUG: Error writing metadata: %v\n", err)
			return err
		}
		
		// Set the root pointer to page 1 (metadata) so it's found on reload
		s.file.Seek(0, 0)
		if err := binary.Write(s.file, binary.BigEndian, int64(metadataOffset)); err != nil {
			fmt.Printf("DEBUG: Error writing root offset to header: %v\n", err)
			return err
		}
		s.root = metadataOffset
		
		fmt.Printf("DEBUG: Wrote metadata key '%s' at offset %d\n", key, metadataOffset)
	} else {
		// For data rows, we'll use a different strategy to ensure we don't lose rows:
		// Find the tableName from the key and store rows in pages by table

		// Extract table name from key for organizing data
		tableName := tableNameFromKey(key)
		if tableName == "" {
			return fmt.Errorf("could not determine table name from key: %s", key)
		}

		// Use a different page offset for each table to avoid conflicts
		// We'll use a simple hash of the table name to determine the page
		tableHash := 0
		for _, c := range tableName {
			tableHash = tableHash*31 + int(c)
		}
		pageIndex := 1 + (tableHash % 100) // Distribute across 100 possible pages
		dataOffset := int64(8 + pageSize*pageIndex)
		
		// Read existing pages for this table
		dataPage := s.pagePool.Get().([]byte)
		defer s.pagePool.Put(dataPage)
		
		// Get current node info
		bytesRead, err := s.file.ReadAt(dataPage, dataOffset)
		if err != nil && err != io.EOF {
			fmt.Printf("DEBUG: Error reading data page: %v\n", err)
			return err
		}
		
		var node *BTreeNode
		var numKeys int
		
		// Check if we have an existing data page or need to create a new one
		if bytesRead > 0 {
			// Parse existing page
			bufOffset := int64(0)
			numKeys = int(binary.BigEndian.Uint64(dataPage[bufOffset:]))
			
			// Create node from existing data
			node = &BTreeNode{
				isLeaf:   true,
				numKeys:  numKeys,
				keys:     make([]string, maxKeys),
				values:   make([][]byte, maxKeys),
				children: make([]int64, maxKeys+1),
			}
			
			// Read existing keys/values
			bufOffset = 16 // Skip the numKeys and isLeaf fields
			for i := 0; i < numKeys; i++ {
				// Read key
				keyLen := binary.BigEndian.Uint32(dataPage[bufOffset:])
				bufOffset += 4
				if keyLen > 0 {
					node.keys[i] = string(dataPage[bufOffset : bufOffset+int64(keyLen)])
				}
				bufOffset += int64(keyLen)
				
				// Read value
				valueLen := binary.BigEndian.Uint32(dataPage[bufOffset:])
				bufOffset += 4
				if valueLen > 0 {
					node.values[i] = make([]byte, valueLen)
					copy(node.values[i], dataPage[bufOffset:bufOffset+int64(valueLen)])
				}
				bufOffset += int64(valueLen)
			}
			
			fmt.Printf("DEBUG: Read existing data page with %d keys for table '%s'\n", numKeys, tableName)
		} else {
			// Create a new node
			node = &BTreeNode{
				isLeaf:   true,
				numKeys:  0,
				keys:     make([]string, maxKeys),
				values:   make([][]byte, maxKeys),
				children: make([]int64, maxKeys+1),
			}
			fmt.Printf("DEBUG: Creating new data page for table '%s'\n", tableName)
		}
		
		// Check if we need to add the row to this page
		if node.numKeys < maxKeys {
			// We have space in the current page
			node.keys[node.numKeys] = key
			node.values[node.numKeys] = value
			node.numKeys++
			fmt.Printf("DEBUG: Added key %s as item %d in data page for table '%s'\n", key, node.numKeys-1, tableName)
		} else {
			// Current page is full, we need to append to a new page
			// Find the next available page for this table
			nextPageOffset := dataOffset + pageSize
			
			// Check if the next page exists and has data related to this table
			nextPage := s.pagePool.Get().([]byte)
			defer s.pagePool.Put(nextPage)
			
			bytesRead, err := s.file.ReadAt(nextPage, nextPageOffset)
			if err != nil && err != io.EOF {
				fmt.Printf("DEBUG: Error reading next page: %v\n", err)
				return err
			}
			
			if bytesRead > 0 {
				// Next page exists, decode it and see if it has space
				var nextNode *BTreeNode
				var nextNumKeys int
				
				// Parse existing next page
				bufOffset := int64(0)
				nextNumKeys = int(binary.BigEndian.Uint64(nextPage[bufOffset:]))
				bufOffset += 8
				isLeaf := binary.BigEndian.Uint64(nextPage[bufOffset:]) == 1
				bufOffset += 8
				
				// Create node from existing data
				nextNode = &BTreeNode{
					isLeaf:   isLeaf,
					numKeys:  nextNumKeys,
					keys:     make([]string, maxKeys),
					values:   make([][]byte, maxKeys),
					children: make([]int64, maxKeys+1),
				}
				
				// Read existing keys/values from the next page
				for i := 0; i < nextNumKeys; i++ {
					// Read key
					keyLen := binary.BigEndian.Uint32(nextPage[bufOffset:])
					bufOffset += 4
					if keyLen > 0 {
						nextNode.keys[i] = string(nextPage[bufOffset : bufOffset+int64(keyLen)])
					}
					bufOffset += int64(keyLen)
					
					// Read value
					valueLen := binary.BigEndian.Uint32(nextPage[bufOffset:])
					bufOffset += 4
					if valueLen > 0 {
						nextNode.values[i] = make([]byte, valueLen)
						copy(nextNode.values[i], nextPage[bufOffset:bufOffset+int64(valueLen)])
					}
					bufOffset += int64(valueLen)
				}
				
				fmt.Printf("DEBUG: Current page full, checking next page at offset %d (has %d keys)\n", 
					nextPageOffset, nextNumKeys)
				
				// If the next page has space, add the key/value
				if nextNode.numKeys < maxKeys {
					nextNode.keys[nextNode.numKeys] = key
					nextNode.values[nextNode.numKeys] = value
					nextNode.numKeys++
					fmt.Printf("DEBUG: Added key '%s' to existing overflow page at index %d\n", 
						key, nextNode.numKeys-1)
				} else {
					// Next page is full too, create a new overflow page
					fmt.Printf("DEBUG: Overflow page is also full, creating another overflow page\n")
					
					// Calculate the offset for another overflow page
					nextNextPageOffset := nextPageOffset + pageSize
					
					// Create a new overflow page
					newOverflowPage := s.pagePool.Get().([]byte)
					defer s.pagePool.Put(newOverflowPage)
					
					// Clear the new overflow page
					for i := range newOverflowPage {
						newOverflowPage[i] = 0
					}
					
					// Create a node for the new overflow page
					newOverflowNode := &BTreeNode{
						isLeaf:   true,
						numKeys:  1,
						keys:     make([]string, maxKeys),
						values:   make([][]byte, maxKeys),
						children: make([]int64, maxKeys+1),
					}
					newOverflowNode.keys[0] = key
					newOverflowNode.values[0] = value
					
					// Serialize the new overflow node
					newOffset := int64(0)
					binary.BigEndian.PutUint64(newOverflowPage[newOffset:], uint64(newOverflowNode.numKeys))
					newOffset += 8
					binary.BigEndian.PutUint64(newOverflowPage[newOffset:], 1) // isLeaf = true
					newOffset += 8
					
					// Write key and value
					keyLen := len(newOverflowNode.keys[0])
					binary.BigEndian.PutUint32(newOverflowPage[newOffset:], uint32(keyLen))
					newOffset += 4
					copy(newOverflowPage[newOffset:], newOverflowNode.keys[0])
					newOffset += int64(keyLen)
					
					valueLen := len(newOverflowNode.values[0])
					binary.BigEndian.PutUint32(newOverflowPage[newOffset:], uint32(valueLen))
					newOffset += 4
					copy(newOverflowPage[newOffset:], newOverflowNode.values[0])
					
					// Write the new overflow page
					fmt.Printf("DEBUG: Writing additional overflow page at offset %d\n", nextNextPageOffset)
					if _, err := s.file.WriteAt(newOverflowPage[:pageSize], nextNextPageOffset); err != nil {
						fmt.Printf("DEBUG: Error writing additional overflow page: %v\n", err)
						return err
					}
					
					fmt.Printf("DEBUG: Successfully wrote key '%s' to additional overflow page at offset %d\n", 
						key, nextNextPageOffset)
						
					// We've written the key to a new overflow page, no need to update the current page
					return nil
				}
				
				// Clear the page for writing
				for i := range nextPage {
					nextPage[i] = 0
				}
				
				// Serialize the updated node to the page
				offset := int64(0)
				binary.BigEndian.PutUint64(nextPage[offset:], uint64(nextNode.numKeys))
				offset += 8
				binary.BigEndian.PutUint64(nextPage[offset:], 1) // isLeaf = true
				offset += 8
				
				// Write all keys and values
				for i := 0; i < nextNode.numKeys; i++ {
					// Write key
					keyLen := len(nextNode.keys[i])
					binary.BigEndian.PutUint32(nextPage[offset:], uint32(keyLen))
					offset += 4
					copy(nextPage[offset:], nextNode.keys[i])
					offset += int64(keyLen)
					
					// Write value
					valueLen := len(nextNode.values[i])
					binary.BigEndian.PutUint32(nextPage[offset:], uint32(valueLen))
					offset += 4
					copy(nextPage[offset:], nextNode.values[i])
					offset += int64(valueLen)
				}
				
				// Write to the page
				fmt.Printf("DEBUG: Writing updated overflow page with %d keys at offset %d\n", 
					nextNode.numKeys, nextPageOffset)
				if _, err := s.file.WriteAt(nextPage[:pageSize], nextPageOffset); err != nil {
					fmt.Printf("DEBUG: Error writing updated overflow page: %v\n", err)
					return err
				}
				
				fmt.Printf("DEBUG: Successfully wrote updated overflow page containing key '%s'\n", key)
			} else {
				// Create a new overflow page
				fmt.Printf("DEBUG: Creating new overflow page for table '%s' at offset %d\n", 
					tableName, nextPageOffset)
				
				// Create a new node for the overflow page
				newNode := &BTreeNode{
					isLeaf:   true,
					numKeys:  1,
					keys:     make([]string, maxKeys),
					values:   make([][]byte, maxKeys),
					children: make([]int64, maxKeys+1),
				}
				newNode.keys[0] = key
				newNode.values[0] = value
				
				// Clear the page for writing
				for i := range nextPage {
					nextPage[i] = 0
				}
				
				// Serialize the node to the page
				offset := int64(0)
				binary.BigEndian.PutUint64(nextPage[offset:], uint64(newNode.numKeys))
				offset += 8
				binary.BigEndian.PutUint64(nextPage[offset:], 1) // isLeaf = true
				offset += 8
				
				// Write key and value
				keyLen := len(newNode.keys[0])
				binary.BigEndian.PutUint32(nextPage[offset:], uint32(keyLen))
				offset += 4
				copy(nextPage[offset:], newNode.keys[0])
				offset += int64(keyLen)
				
				valueLen := len(newNode.values[0])
				binary.BigEndian.PutUint32(nextPage[offset:], uint32(valueLen))
				offset += 4
				copy(nextPage[offset:], newNode.values[0])
				
				// Write to the new page
				fmt.Printf("DEBUG: Writing new overflow page at offset %d\n", nextPageOffset)
				if _, err := s.file.WriteAt(nextPage[:pageSize], nextPageOffset); err != nil {
					fmt.Printf("DEBUG: Error writing overflow page: %v\n", err)
					return err
				}
				
				fmt.Printf("DEBUG: Successfully wrote key '%s' to new overflow page at offset %d\n", 
					key, nextPageOffset)
			}
			
			// We've written to a different page, so we don't need to update the current page
			return nil
		}
		
		// Clear the page for writing
		for i := range dataPage {
			dataPage[i] = 0
		}
		
		// Serialize the node to the page
		offset := int64(0)
		binary.BigEndian.PutUint64(dataPage[offset:], uint64(node.numKeys))
		offset += 8
		binary.BigEndian.PutUint64(dataPage[offset:], 1) // isLeaf = true
		offset += 8
		
		// Write all keys and values
		for i := 0; i < node.numKeys; i++ {
			// Write key
			keyLen := len(node.keys[i])
			binary.BigEndian.PutUint32(dataPage[offset:], uint32(keyLen))
			offset += 4
			copy(dataPage[offset:], node.keys[i])
			offset += int64(keyLen)
			
			// Write value
			valueLen := len(node.values[i])
			binary.BigEndian.PutUint32(dataPage[offset:], uint32(valueLen))
			offset += 4
			copy(dataPage[offset:], node.values[i])
			offset += int64(valueLen)
		}
		
		// Write the page to disk
		fmt.Printf("DEBUG: Writing data page with %d keys to offset %d\n", node.numKeys, dataOffset)
		if _, err := s.file.WriteAt(dataPage[:pageSize], dataOffset); err != nil {
			fmt.Printf("DEBUG: Error writing data page: %v\n", err)
			return err
		}
		
		fmt.Printf("DEBUG: Successfully wrote data page containing key '%s'\n", key)
	}
	
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
	
	// Calculate the hash of the table name to find its pages
	tableHash := 0
	for _, c := range tableName {
		tableHash = tableHash*31 + int(c)
	}
	pageIndex := 1 + (tableHash % 100) // Same hash as in insert method
	baseOffset := int64(8 + pageSize*pageIndex)
	
	// We need to read potentially multiple pages for this table
	// Start with the first page and continue to additional overflow pages
	currentOffset := baseOffset
	maxOffset := baseOffset + (pageSize * 100) // Limit to 100 pages per table for safety
	
	for currentOffset <= maxOffset {
		// Read the current page
		page := s.pagePool.Get().([]byte)
		defer s.pagePool.Put(page)
		
		bytesRead, err := s.file.ReadAt(page, currentOffset)
		if err != nil && err != io.EOF {
			// Error other than EOF, return it
			fmt.Printf("DEBUG: Error reading data page at offset %d: %v\n", currentOffset, err)
			return nil, err
		}
		
		// Check if we reached the end of the file or an empty page
		if bytesRead == 0 {
			fmt.Printf("DEBUG: Reached end of file at offset %d\n", currentOffset)
			break
		}
		
		fmt.Printf("DEBUG: Read %d bytes from data page at offset %d\n", bytesRead, currentOffset)
		
		// Parse the node header
		bufOffset := int64(0)
		numKeys := int(binary.BigEndian.Uint64(page[bufOffset:]))
		bufOffset += 8
		isLeaf := binary.BigEndian.Uint64(page[bufOffset:]) == 1
		bufOffset += 8
		
		fmt.Printf("DEBUG: Data node has %d keys, isLeaf=%v\n", numKeys, isLeaf)
		
		// If the page is empty or invalid, skip to the next page
		if numKeys == 0 {
			fmt.Printf("DEBUG: Empty page at offset %d, checking next page\n", currentOffset)
			currentOffset += pageSize
			continue
		}
		
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
		
		// Move to the next page
		currentOffset += pageSize
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
		if !ok {
			return false
		}
		
		// Handle different types of value comparisons
		switch v := val.(type) {
		case string:
			// String comparison
			rowStr, isStr := rowVal.(string)
			if !isStr || rowStr != v {
				return false
			}
		case int:
			// Integer comparison with various possible types
			switch rv := rowVal.(type) {
			case int:
				if rv != v {
					return false
				}
			case int64:
				if int(rv) != v {
					return false
				}
			case float64:
				if int(rv) != v {
					return false
				}
			default:
				// If the types are incompatible, it's not a match
				return false
			}
		case float64:
			// Float comparison
			rowFloat, isFloat := rowVal.(float64)
			if !isFloat || rowFloat != v {
				return false
			}
		default:
			// For any other type, use direct equality
			if rowVal != val {
				return false
			}
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