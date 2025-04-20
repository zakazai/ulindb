package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/zakazai/ulin-db/internal/lexer"
	"github.com/zakazai/ulin-db/internal/parser"
	"github.com/zakazai/ulin-db/internal/storage"
)

func main() {
	fmt.Println("UlinDB SQL Server")
	fmt.Println("Type 'exit' to quit")

	// Initialize BTree storage
	config := storage.StorageConfig{
		Type:     storage.BTreeStorageType,
		FilePath: "data/ulindb.btree",
	}
	s, err := storage.NewStorage(config)
	if err != nil {
		fmt.Printf("Error initializing storage: %v\n", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)

	// Check if we're in interactive mode or piped input
	isInteractive := true
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		// Input is being piped in
		isInteractive = false
	}

	for {
		if isInteractive {
			fmt.Print("> ")
		}

		input, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// End of input, exit gracefully
				if isInteractive {
					fmt.Println("Goodbye!")
				}
				break
			}
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		// Trim whitespace and check for exit command
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		if strings.ToLower(input) == "exit" {
			fmt.Println("Goodbye!")
			break
		}

		// Parse the SQL statement
		l := lexer.New(input)
		p := parser.New(l)
		stmt, err := p.Parse()
		if err != nil {
			fmt.Printf("Error parsing statement: %v\n", err)
			continue
		}

		// Special handling for INSERT statements
		if insertStmt, ok := stmt.(*parser.InsertStatement); ok {
			// Get the table definition to map column names
			table := s.GetTable(insertStmt.Table)
			if table == nil {
				fmt.Printf("Error executing statement: table %s does not exist\n", insertStmt.Table)
				continue
			}

			// Map values to column names based on their position
			values := make(map[string]interface{})

			// The values in insertStmt.Values are stored with keys like "column1", "column2"
			// We need to extract them in order
			for i := 1; i <= len(table.Columns); i++ {
				columnKey := fmt.Sprintf("column%d", i)
				if val, ok := insertStmt.Values[columnKey]; ok && i-1 < len(table.Columns) {
					colName := table.Columns[i-1].Name
					values[colName] = val
				}
			}

			// Execute the INSERT directly
			err = s.Insert(insertStmt.Table, values)
			if err != nil {
				fmt.Printf("Error executing statement: %v\n", err)
			}
			continue
		}

		// Execute other statement types
		result, err := stmt.(parser.Statement).Execute(s)
		if err != nil {
			fmt.Printf("Error executing statement: %v\n", err)
			continue
		}

		// Print the result
		if result != nil {
			// Check if result is a []types.Row from SELECT
			if rows, ok := result.([]map[string]interface{}); ok {
				printFormattedResults(rows)
			} else {
				fmt.Println(result)
			}
		}
	}

	// Close storage to ensure all data is saved
	if err := s.Close(); err != nil {
		fmt.Printf("Error closing storage: %v\n", err)
	}
}

// printFormattedResults formats and prints the results of a SELECT query in a tabular format
func printFormattedResults(rows []map[string]interface{}) {
	if len(rows) == 0 {
		fmt.Println("Empty result set")
		return
	}

	// Get column names and calculate their max width
	columns := make([]string, 0)
	columnWidths := make(map[string]int)

	// First, gather all column names from all rows
	for _, row := range rows {
		for col := range row {
			if !contains(columns, col) {
				columns = append(columns, col)
				columnWidths[col] = len(col)
			}
		}
	}

	// Sort columns for consistent display
	sort.Strings(columns)

	// Calculate maximum width for each column
	for _, row := range rows {
		for _, col := range columns {
			if val, ok := row[col]; ok {
				valStr := fmt.Sprintf("%v", val)
				if len(valStr) > columnWidths[col] {
					columnWidths[col] = len(valStr)
				}
			}
		}
	}

	// Print header
	for i, col := range columns {
		if i > 0 {
			fmt.Print(" | ")
		}
		fmt.Printf("%-*s", columnWidths[col], col)
	}
	fmt.Println()

	// Print separator
	for i, col := range columns {
		if i > 0 {
			fmt.Print("-+-")
		}
		for j := 0; j < columnWidths[col]; j++ {
			fmt.Print("-")
		}
	}
	fmt.Println()

	// Print data rows
	for _, row := range rows {
		for i, col := range columns {
			if i > 0 {
				fmt.Print(" | ")
			}
			val, ok := row[col]
			if !ok {
				val = "NULL"
			}
			fmt.Printf("%-*v", columnWidths[col], val)
		}
		fmt.Println()
	}
}

// contains checks if a string slice contains a specific string
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
