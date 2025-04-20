package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/zakazai/ulin-db/internal/types"
)

// ParquetRow represents a row in Parquet format with dynamic columns
type ParquetRow struct {
	TableName string                 `parquet:"name=table_name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Data      map[string]interface{} `parquet:"-"` // This will be serialized to JSON
	DataJSON  string                 `parquet:"name=data_json, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// ParquetStorage implements Storage interface using Parquet file format
type ParquetStorage struct {
	dataDir       string
	tables        map[string]*types.Table
	mu            sync.RWMutex
	syncFromBTree Storage // Reference to the BTree storage for syncing
	lastSync      time.Time
	syncInterval  time.Duration
}

// NewParquetStorage creates a new Parquet storage
func NewParquetStorage(dataDir string) (*ParquetStorage, error) {
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	return &ParquetStorage{
		dataDir:      dataDir,
		tables:       make(map[string]*types.Table),
		syncInterval: 5 * time.Minute, // Default sync interval
	}, nil
}

// SetBTreeSource sets the BTree storage to sync from
func (s *ParquetStorage) SetBTreeSource(btree Storage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncFromBTree = btree
}

// SetSyncInterval sets the interval for automatic syncing
func (s *ParquetStorage) SetSyncInterval(interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncInterval = interval
}

// StartSyncWorker starts a background worker that periodically syncs data from BTree
func (s *ParquetStorage) StartSyncWorker() {
	go func() {
		for {
			time.Sleep(s.syncInterval)
			if err := s.SyncFromBTree(); err != nil {
				fmt.Printf("Error syncing from BTree: %v\n", err)
			}
		}
	}()
}

// SyncFromBTree synchronizes data from the BTree storage
func (s *ParquetStorage) SyncFromBTree() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.syncFromBTree == nil {
		return fmt.Errorf("no BTree source configured")
	}

	// Get all tables from BTree
	tables, err := s.syncFromBTree.ShowTables()
	if err != nil {
		return fmt.Errorf("failed to get tables from BTree: %w", err)
	}

	// Sync each table
	for _, tableName := range tables {
		// Get table definition
		table := s.syncFromBTree.GetTable(tableName)
		if table == nil {
			continue
		}

		// Store table definition
		s.tables[tableName] = table

		// Get all rows from BTree
		rows, err := s.syncFromBTree.Select(tableName, nil, nil)
		if err != nil {
			return fmt.Errorf("failed to select rows from BTree for table %s: %w", tableName, err)
		}

		// Write to Parquet file
		if err := s.writeRowsToParquet(tableName, rows); err != nil {
			return fmt.Errorf("failed to write rows to Parquet for table %s: %w", tableName, err)
		}
	}

	s.lastSync = time.Now()
	return nil
}

func (s *ParquetStorage) writeRowsToParquet(tableName string, rows []types.Row) error {
	if len(rows) == 0 {
		return nil
	}

	// Create Parquet file
	filePath := filepath.Join(s.dataDir, fmt.Sprintf("%s.parquet", tableName))
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return err
	}
	defer fw.Close()

	// Create Parquet writer
	pw, err := writer.NewParquetWriter(fw, new(ParquetRow), 4)
	if err != nil {
		return err
	}

	// Set compression
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write rows
	for _, row := range rows {
		jsonData, err := json.Marshal(row)
		if err != nil {
			return err
		}

		parquetRow := &ParquetRow{
			TableName: tableName,
			DataJSON:  string(jsonData),
		}

		if err := pw.Write(parquetRow); err != nil {
			return err
		}
	}

	// Flush and close writer
	if err := pw.WriteStop(); err != nil {
		return err
	}

	return nil
}

// CreateTable implements Storage.CreateTable
func (s *ParquetStorage) CreateTable(table *types.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[table.Name]; exists {
		return fmt.Errorf("table %s already exists", table.Name)
	}

	s.tables[table.Name] = table

	// Create empty Parquet file for this table
	return s.writeRowsToParquet(table.Name, []types.Row{})
}

// Insert implements Storage.Insert (but is read-only for Parquet)
func (s *ParquetStorage) Insert(tableName string, values map[string]interface{}) error {
	// Parquet storage is read-only
	return fmt.Errorf("Parquet storage is read-only; insertions must go through the primary storage")
}

// Select implements Storage.Select
func (s *ParquetStorage) Select(tableName string, columns []string, where map[string]interface{}) ([]types.Row, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if table exists
	if _, exists := s.tables[tableName]; !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Read data from Parquet file
	filePath := filepath.Join(s.dataDir, fmt.Sprintf("%s.parquet", tableName))
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// If file doesn't exist, return empty result
			return []types.Row{}, nil
		}
		return nil, err
	}
	defer fr.Close()

	// Get Parquet reader
	pr, err := reader.NewParquetReader(fr, new(ParquetRow), 4)
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()

	// Read all rows
	numRows := int(pr.GetNumRows())
	parquetRows := make([]ParquetRow, numRows)
	if err := pr.Read(&parquetRows); err != nil {
		return nil, err
	}

	// Convert to types.Row and apply filtering
	var results []types.Row
	for _, prow := range parquetRows {
		// Skip rows that don't belong to this table
		if prow.TableName != tableName {
			continue
		}

		// Parse JSON data
		var row types.Row
		if err := json.Unmarshal([]byte(prow.DataJSON), &row); err != nil {
			return nil, err
		}

		// Apply WHERE filter
		if where != nil && !s.matchesWhere(row, where) {
			continue
		}

		// Apply column projection
		if columns != nil && len(columns) > 0 {
			result := make(types.Row)
			for _, col := range columns {
				if val, ok := row[col]; ok {
					result[col] = val
				}
			}
			results = append(results, result)
		} else {
			results = append(results, row)
		}
	}

	return results, nil
}

// Update implements Storage.Update (but is read-only for Parquet)
func (s *ParquetStorage) Update(tableName string, set map[string]interface{}, where map[string]interface{}) error {
	// Parquet storage is read-only
	return fmt.Errorf("Parquet storage is read-only; updates must go through the primary storage")
}

// Delete implements Storage.Delete (but is read-only for Parquet)
func (s *ParquetStorage) Delete(tableName string, where map[string]interface{}) error {
	// Parquet storage is read-only
	return fmt.Errorf("Parquet storage is read-only; deletions must go through the primary storage")
}

// Close implements Storage.Close
func (s *ParquetStorage) Close() error {
	// Nothing to close
	return nil
}

// GetTable implements Storage.GetTable
func (s *ParquetStorage) GetTable(tableName string) *types.Table {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tables[tableName]
}

// ShowTables implements Storage.ShowTables
func (s *ParquetStorage) ShowTables() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables := make([]string, 0, len(s.tables))
	for name := range s.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

// Helper function to check if a row matches a WHERE clause
func (s *ParquetStorage) matchesWhere(row types.Row, where map[string]interface{}) bool {
	for col, val := range where {
		rowVal, ok := row[col]
		if !ok || rowVal != val {
			return false
		}
	}
	return true
}

// GetLastSyncTime returns the time of the last sync
func (s *ParquetStorage) GetLastSyncTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastSync
}

// Helper function to convert string column names to parquet schema
func parquetSchemaForTable(table *types.Table) string {
	var schema strings.Builder
	schema.WriteString("message schema {")

	for _, col := range table.Columns {
		var parquetType string
		switch col.Type {
		case "INT":
			parquetType = "INT64"
		case "STRING":
			parquetType = "BYTE_ARRAY"
		default:
			parquetType = "BYTE_ARRAY"
		}

		schema.WriteString(fmt.Sprintf(" optional %s %s;", parquetType, col.Name))
	}

	schema.WriteString(" }")
	return schema.String()
}
