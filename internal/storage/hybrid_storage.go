package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/zakazai/ulin-db/internal/types"
)

// HybridStorage implements a hybrid storage system that routes queries
// between OLTP (BTree) and OLAP (Parquet) storage engines based on the query type.
// This provides optimal performance for both transactional and analytical workloads.
type HybridStorage struct {
	// oltp is the primary storage engine for transactional (OLTP) operations.
	// It handles all writes and point queries efficiently.
	oltp Storage

	// olap is the secondary storage engine optimized for analytical (OLAP) operations.
	// It is read-only and provides columnar access patterns for efficient analytics.
	olap Storage

	// syncTime records when data was last synchronized from OLTP to OLAP storage.
	syncTime time.Time
}

// IsOLAPQuery determines if a query is OLAP-style and should be routed to Parquet
func IsOLAPQuery(columns []string, where map[string]interface{}) bool {
	// Heuristics to determine if this is an OLAP query:
	// 1. Query reads many columns (reporting/analytics)
	// 2. No specific key lookup (range scan or full table scan)
	// 3. Query contains aggregation (future enhancement)

	// If no WHERE clause or ID lookup, likely an analytical query
	if where == nil || len(where) == 0 {
		return true
	}

	// If we're selecting all columns, likely an analytical query
	if len(columns) == 0 || (len(columns) == 1 && columns[0] == "*") {
		return true
	}

	// Check if WHERE contains only ID fields (OLTP) or range conditions (OLAP)
	idFieldNames := []string{"id", "ID", "Id", "_id", "pk"}
	for col := range where {
		isIdField := false
		for _, idField := range idFieldNames {
			if strings.EqualFold(col, idField) {
				isIdField = true
				break
			}
		}

		if !isIdField {
			// Non-ID field in WHERE clause suggests OLAP
			return true
		}
	}

	// Default to OLTP for safety
	return false
}

// CreateTable implements Storage.CreateTable by delegating to both backends
func (s *HybridStorage) CreateTable(table *types.Table) error {
	// Always create in OLTP first
	if err := s.oltp.CreateTable(table); err != nil {
		return err
	}

	// Then propagate to OLAP
	if err := s.olap.CreateTable(table); err != nil {
		// This is not critical, so just log and continue
		fmt.Printf("Warning: Failed to create table in OLAP storage: %v\n", err)
	}

	return nil
}

// Insert implements Storage.Insert by delegating to OLTP
func (s *HybridStorage) Insert(tableName string, values map[string]interface{}) error {
	// Inserts always go to OLTP storage
	return s.oltp.Insert(tableName, values)
}

// Select implements Storage.Select with intelligent routing
func (s *HybridStorage) Select(tableName string, columns []string, where map[string]interface{}) ([]types.Row, error) {
	// Route query based on its characteristics
	if IsOLAPQuery(columns, where) {
		// Try OLAP storage first
		rows, err := s.olap.Select(tableName, columns, where)

		// If successful or error is not just "not found", return results
		if err == nil || (err != nil && !strings.Contains(err.Error(), "does not exist")) {
			return rows, err
		}

		// Fall back to OLTP if OLAP fails
		fmt.Printf("OLAP query failed, falling back to OLTP: %v\n", err)
	}

	// Use OLTP storage
	return s.oltp.Select(tableName, columns, where)
}

// Update implements Storage.Update by delegating to OLTP
func (s *HybridStorage) Update(tableName string, set map[string]interface{}, where map[string]interface{}) error {
	// Updates always go to OLTP storage
	return s.oltp.Update(tableName, set, where)
}

// Delete implements Storage.Delete by delegating to OLTP
func (s *HybridStorage) Delete(tableName string, where map[string]interface{}) error {
	// Deletes always go to OLTP storage
	return s.oltp.Delete(tableName, where)
}

// Close implements Storage.Close by closing both storages
func (s *HybridStorage) Close() error {
	var oltpErr, olapErr error

	// Close OLTP storage
	oltpErr = s.oltp.Close()

	// Close OLAP storage
	olapErr = s.olap.Close()

	// Return first error encountered
	if oltpErr != nil {
		return oltpErr
	}
	return olapErr
}

// GetTable implements Storage.GetTable preferring OLTP
func (s *HybridStorage) GetTable(tableName string) *types.Table {
	// Try OLTP first, then fall back to OLAP
	table := s.oltp.GetTable(tableName)
	if table != nil {
		return table
	}
	return s.olap.GetTable(tableName)
}

// ShowTables implements Storage.ShowTables from OLTP
func (s *HybridStorage) ShowTables() ([]string, error) {
	// Get tables from primary storage (OLTP)
	return s.oltp.ShowTables()
}

// SyncNow forces a synchronization from OLTP to OLAP
func (s *HybridStorage) SyncNow() error {
	// Cast to specific implementation
	if parquetStorage, ok := s.olap.(*ParquetStorage); ok {
		err := parquetStorage.SyncFromBTree()
		if err == nil {
			s.syncTime = time.Now()
		}
		return err
	}
	return fmt.Errorf("OLAP storage is not a ParquetStorage")
}

// GetLastSyncTime returns the time of the last synchronization
func (s *HybridStorage) GetLastSyncTime() time.Time {
	return s.syncTime
}

// GetOLTPStorage returns the underlying OLTP storage
func (s *HybridStorage) GetOLTPStorage() Storage {
	return s.oltp
}

// GetOLAPStorage returns the underlying OLAP storage
func (s *HybridStorage) GetOLAPStorage() Storage {
	return s.olap
}
