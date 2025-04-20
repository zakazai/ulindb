package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/zakazai/ulin-db/internal/types"
)

// ParquetReader provides utilities for reading from Parquet files
type ParquetReader struct {
	dataDir string
}

// NewParquetReader creates a new ParquetReader
func NewParquetReader(dataDir string) *ParquetReader {
	return &ParquetReader{
		dataDir: dataDir,
	}
}

// ReadTable reads all rows from a table's Parquet file
func (r *ParquetReader) ReadTable(tableName string) ([]types.Row, error) {
	filePath := filepath.Join(r.dataDir, fmt.Sprintf("%s.parquet", tableName))
	
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return []types.Row{}, nil
	}
	
	// Open file
	fr, err := source.NewLocalFileReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open Parquet file: %w", err)
	}
	defer fr.Close()
	
	// Create Parquet reader
	pr, err := reader.NewParquetReader(fr, new(ParquetRow), 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet reader: %w", err)
	}
	defer pr.ReadStop()
	
	// Get row count
	numRows := int(pr.GetNumRows())
	if numRows == 0 {
		return []types.Row{}, nil
	}
	
	// Read rows
	parquetRows := make([]ParquetRow, numRows)
	if err := pr.Read(&parquetRows); err != nil {
		return nil, fmt.Errorf("failed to read Parquet rows: %w", err)
	}
	
	// Convert to types.Row
	rows := make([]types.Row, 0, numRows)
	for _, prow := range parquetRows {
		if prow.TableName != tableName {
			continue
		}
		
		var row types.Row
		if err := json.Unmarshal([]byte(prow.DataJSON), &row); err != nil {
			return nil, fmt.Errorf("failed to unmarshal row data: %w", err)
		}
		
		rows = append(rows, row)
	}
	
	return rows, nil
}

// ApplyFilter filters rows based on WHERE conditions
func (r *ParquetReader) ApplyFilter(rows []types.Row, where map[string]interface{}) []types.Row {
	if where == nil || len(where) == 0 {
		return rows
	}
	
	filtered := make([]types.Row, 0, len(rows))
	for _, row := range rows {
		if matchesWhere(row, where) {
			filtered = append(filtered, row)
		}
	}
	
	return filtered
}

// ApplyProjection applies column selection to rows
func (r *ParquetReader) ApplyProjection(rows []types.Row, columns []string) []types.Row {
	if columns == nil || len(columns) == 0 {
		return rows
	}
	
	projected := make([]types.Row, len(rows))
	for i, row := range rows {
		projectedRow := make(types.Row)
		
		// Handle * projection
		if len(columns) == 1 && columns[0] == "*" {
			projected[i] = row
			continue
		}
		
		// Handle specific columns
		for _, col := range columns {
			if val, ok := row[col]; ok {
				projectedRow[col] = val
			}
		}
		
		projected[i] = projectedRow
	}
	
	return projected
}

// matchesWhere checks if a row matches WHERE conditions
func matchesWhere(row types.Row, where map[string]interface{}) bool {
	for col, val := range where {
		rowVal, ok := row[col]
		if !ok || rowVal != val {
			return false
		}
	}
	return true
}