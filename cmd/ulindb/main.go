package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/zakazai/ulin-db/internal/lexer"
	"github.com/zakazai/ulin-db/internal/parser"
	"github.com/zakazai/ulin-db/internal/storage"
)

func main() {
	fmt.Println("UlinDB SQL Server")
	fmt.Println("Type 'exit' to quit")

	// Initialize hybrid storage with BTree for OLTP and Parquet for OLAP
	config := storage.StorageConfig{
		Type:         storage.BTreeStorageType,
		FilePath:     "data/ulindb.btree",
		DataDir:      "data/parquet",
		SyncInterval: time.Minute * 5, // Sync every 5 minutes
	}

	// Create hybrid storage
	hybridStorage, err := storage.CreateHybridStorage(config)
	if err != nil {
		fmt.Printf("Error initializing hybrid storage: %v\n", err)
		return
	}

	// Force initial sync to ensure data is available in Parquet
	err = hybridStorage.SyncNow()
	if err != nil {
		fmt.Printf("Warning: Initial sync failed: %v\n", err)
		// Continue anyway as this is not critical
	}

	// Use hybrid storage for all operations
	s := hybridStorage

	// Check if we're in interactive mode or piped input
	isInteractive := true
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		// Input is being piped in
		isInteractive = false
	}

	if isInteractive {
		// Interactive mode with command history
		executeInteractiveMode(s)
	} else {
		// Non-interactive mode (piped input)
		executePipedMode(s)
	}

	// Close storage to ensure all data is saved
	if err := s.Close(); err != nil {
		fmt.Printf("Error closing storage: %v\n", err)
	}
}

// executeInteractiveMode handles interactive mode with command history
func executeInteractiveMode(s *storage.HybridStorage) {
	// Create history file path in user's home directory
	historyFile := getHistoryFilePath()

	// Initialize readline with history support
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "> ",
		HistoryFile:     historyFile,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		fmt.Printf("Error initializing readline: %v\n", err)
		return
	}
	defer rl.Close()

	// Process commands in a loop
	multilineBuffer := ""
	for {
		// Read a line of input
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				// Ctrl+C pressed, clear the current buffer and continue
				multilineBuffer = ""
				continue
			} else if err == io.EOF {
				// Ctrl+D or EOF, exit
				fmt.Println("Goodbye!")
				break
			}
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		// Trim whitespace but keep track of the line
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" {
			continue
		}

		// Handle exit command
		if strings.ToLower(trimmedLine) == "exit" {
			fmt.Println("Goodbye!")
			break
		}

		// Append the line to the multiline buffer
		if multilineBuffer != "" {
			multilineBuffer += "\n"
		}
		multilineBuffer += line

		// If the input doesn't end with semicolon, collect more lines
		if !strings.HasSuffix(trimmedLine, ";") && trimmedLine != "exit" {
			rl.SetPrompt("... ")
			continue
		}

		// Reset prompt for next command
		rl.SetPrompt("> ")

		// Process the completed command
		processCommand(s, multilineBuffer)

		// Clear the buffer for the next command
		multilineBuffer = ""
	}
}

// executePipedMode handles non-interactive mode with piped input
func executePipedMode(s *storage.HybridStorage) {
	reader := bufio.NewReader(os.Stdin)
	buffer := ""

	// Read input line by line
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// Process any remaining commands at EOF
				if strings.TrimSpace(buffer) != "" {
					processCommand(s, buffer)
				}
				break
			}
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		// Add the line to our buffer
		buffer += input

		// Check if the line contains a complete command (ends with semicolon)
		trimmedLine := strings.TrimSpace(input)
		if strings.HasSuffix(trimmedLine, ";") || strings.ToLower(trimmedLine) == "exit" {
			processCommand(s, buffer)
			buffer = ""
		}
	}
}

// processCommand handles a single complete SQL command
func processCommand(s *storage.HybridStorage, input string) {
	// Trim whitespace
	input = strings.TrimSpace(input)
	if input == "" {
		return
	}

	// Handle special commands
	if strings.ToLower(input) == "exit" {
		// Exit is handled in the calling function
		return
	}

	// Special command to force sync from BTree to Parquet
	if strings.ToUpper(input) == "FORCE_SYNC;" {
		fmt.Println("Forcing sync from BTree to Parquet storage...")
		startTime := time.Now()
		err := s.SyncNow()
		duration := time.Since(startTime)
		if err != nil {
			fmt.Printf("Error during sync: %v\n", err)
		} else {
			fmt.Printf("Sync completed in %v\n", duration)
		}
		return
	}

	// Handle SHOW TABLES command to list all tables
	if strings.ToUpper(input) == "SHOW TABLES;" {
		fmt.Println("Fetching all tables...")
		startTime := time.Now()
		tables, err := s.ShowTables()
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("Error getting tables: %v\n", err)
		} else {
			// Sort tables alphabetically for consistent output
			sort.Strings(tables)

			fmt.Println("Results:")
			fmt.Println("TABLE_NAME")
			fmt.Println("---------")
			for _, tableName := range tables {
				fmt.Println(tableName)
			}
			fmt.Printf("\nFound %d tables in %v\n", len(tables), duration)
		}
		return
	}

	// Handle EXPLAIN command for query execution plan
	if strings.HasPrefix(strings.ToUpper(input), "EXPLAIN ") {
		// Extract the actual query
		query := strings.TrimSpace(input[8:])
		fmt.Printf("Explaining query: %s\n", query)

		// Parse the query
		l := lexer.New(query)
		p := parser.New(l)
		stmt, err := p.Parse()
		if err != nil {
			fmt.Printf("Error parsing statement: %v\n", err)
			return
		}

		// Only support EXPLAIN for SELECT statements
		if selectStmt, ok := stmt.(*parser.SelectStatement); ok {
			isOLAP := storage.IsOLAPQuery(selectStmt.Columns, selectStmt.Where)
			fmt.Println("======= Query Execution Plan =======")
			fmt.Printf("Query Type: %s\n", map[bool]string{true: "OLAP (Analytical)", false: "OLTP (Transactional)"}[isOLAP])
			fmt.Printf("Storage Engine: %s\n", map[bool]string{true: "Parquet", false: "BTree"}[isOLAP])
			fmt.Printf("Table: %s\n", selectStmt.Table)
			fmt.Printf("Columns: %v\n", selectStmt.Columns)
			if len(selectStmt.Where) > 0 {
				fmt.Println("Filters:")
				for col, val := range selectStmt.Where {
					fmt.Printf("  %s = %v\n", col, val)
				}
			} else {
				fmt.Println("Filters: None (Full Table Scan)")
			}
			fmt.Println("===================================")
		} else {
			fmt.Println("EXPLAIN is currently only supported for SELECT statements")
		}
		return
	}

	// Parse the SQL statement
	l := lexer.New(input)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		fmt.Printf("Error parsing statement: %v\n", err)
		return
	}

	// Special handling for INSERT statements
	if insertStmt, ok := stmt.(*parser.InsertStatement); ok {
		// Get the table definition to map column names
		table := s.GetTable(insertStmt.Table)
		if table == nil {
			fmt.Printf("Error executing statement: table %s does not exist\n", insertStmt.Table)
			return
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

		// Execute the INSERT with timing
		fmt.Printf("Executing INSERT operation on BTree storage...\n")
		startTime := time.Now()
		err = s.Insert(insertStmt.Table, values)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("Error executing statement: %v\n", err)
		} else {
			fmt.Printf("Successfully inserted record in %v\n", duration)
		}
		return
	}

	// Execute other statement types with timing
	fmt.Printf("Executing statement...\n")
	startTime := time.Now()

	// For SELECT statements, determine if OLAP or OLTP
	if selectStmt, ok := stmt.(*parser.SelectStatement); ok {
		isOLAP := storage.IsOLAPQuery(selectStmt.Columns, selectStmt.Where)
		storageType := "BTree (OLTP)"
		if isOLAP {
			storageType = "Parquet (OLAP)"
		}
		fmt.Printf("Query classified as %s, using %s storage\n",
			map[bool]string{true: "analytical", false: "transactional"}[isOLAP],
			storageType)
	}

	result, err := stmt.(parser.Statement).Execute(s)
	duration := time.Since(startTime)

	if err != nil {
		fmt.Printf("Error executing statement: %v\n", err)
		return
	}

	// Print the result with timing information
	fmt.Printf("Execution completed in %v\n", duration)
	if result != nil {
		// Check if result is a []types.Row from SELECT
		if rows, ok := result.([]map[string]interface{}); ok {
			fmt.Printf("Retrieved %d rows\n", len(rows))
			printFormattedResults(rows)
		} else {
			fmt.Println(result)
		}
	}
}

// getHistoryFilePath returns the path to the history file
func getHistoryFilePath() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		// Fallback to current directory if home dir can't be determined
		return ".ulindb_history"
	}
	return filepath.Join(homeDir, ".ulindb_history")
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