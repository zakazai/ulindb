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
	"github.com/zakazai/ulin-db/internal/types"
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

	// Make sure the data directories exist
	os.MkdirAll("data", 0755)
	os.MkdirAll("data/parquet", 0755)

	// Create hybrid storage
	hybridStorage, err := storage.CreateHybridStorage(config)
	if err != nil {
		fmt.Printf("Error initializing hybrid storage: %v\n", err)
		return
	}

	// Debug storage status
	fmt.Println("Hybrid storage initialized successfully")
	fmt.Println("OLTP storage type:", fmt.Sprintf("%T", hybridStorage.GetOLTPStorage()))
	fmt.Println("OLAP storage type:", fmt.Sprintf("%T", hybridStorage.GetOLAPStorage()))

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
	
	// Handle SHOW TABLE command to display the schema of a specific table
	if strings.HasPrefix(strings.ToUpper(input), "SHOW TABLE ") {
		// Extract the table name
		parts := strings.Split(strings.TrimSpace(input), " ")
		if len(parts) < 3 {
			fmt.Println("Error: Invalid SHOW TABLE command. Usage: SHOW TABLE <table_name>;")
			return
		}
		
		tableName := strings.TrimSuffix(parts[2], ";")
		fmt.Printf("Fetching schema for table '%s'...\n", tableName)
		startTime := time.Now()
		
		// Get the table definition
		table := s.GetTable(tableName)
		duration := time.Since(startTime)
		
		if table == nil {
			fmt.Printf("Error: Table '%s' does not exist\n", tableName)
			return
		}
		
		// Print table schema
		fmt.Printf("Table: %s\n", table.Name)
		fmt.Println("\nCOLUMN_NAME  | TYPE    | NULLABLE")
		fmt.Println("-------------+---------+---------")
		
		for _, col := range table.Columns {
			nullable := "YES"
			if !col.Nullable {
				nullable = "NO"
			}
			fmt.Printf("%-12s | %-7s | %s\n", col.Name, col.Type, nullable)
		}
		
		fmt.Printf("\nSchema retrieved in %v\n", duration)
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
		// Debug
		fmt.Println("DEBUG: Executing INSERT Statement")
		fmt.Printf("DEBUG: Table name = %s\n", insertStmt.Table)
		fmt.Printf("DEBUG: Raw values = %v\n", insertStmt.Values)
		
		// Show all available tables
		tables, _ := s.ShowTables()
		fmt.Printf("DEBUG: Available tables = %v\n", tables)
		
		// Get the table definition to map column names
		table := s.GetTable(insertStmt.Table)
		if table == nil {
			fmt.Printf("Error executing statement: table %s does not exist\n", insertStmt.Table)
			// Try direct OLTP query to see if table exists there
			oltpTable := s.GetOLTPStorage().GetTable(insertStmt.Table)
			if oltpTable != nil {
				fmt.Printf("DEBUG: Table found in OLTP but not in hybrid - schema: %v\n", oltpTable.Columns)
			} else {
				fmt.Println("DEBUG: Table not found in OLTP storage either")
			}
			return
		}
		
		fmt.Printf("DEBUG: Table columns = %v\n", table.Columns)

		// Map values to column names based on their position
		values := make(map[string]interface{})

		// The values in insertStmt.Values are stored with keys like "column1", "column2"
		// We need to extract them in order
		for i := 1; i <= len(table.Columns); i++ {
			columnKey := fmt.Sprintf("column%d", i)
			if val, ok := insertStmt.Values[columnKey]; ok && i-1 < len(table.Columns) {
				colName := table.Columns[i-1].Name
				values[colName] = val
				fmt.Printf("DEBUG: Mapping %s -> %s = %v\n", columnKey, colName, val)
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
	
	// For SELECT statements, handle specially
	if selectStmt, ok := stmt.(*parser.SelectStatement); ok {
		isOLAP := storage.IsOLAPQuery(selectStmt.Columns, selectStmt.Where)
		storageType := "BTree (OLTP)"
		if isOLAP {
			storageType = "Parquet (OLAP)"
		}
		
		// First always try OLTP storage directly for freshest data
		fmt.Println("Always trying OLTP storage first for most up-to-date data...")
		oltpRows, oltpErr := s.GetOLTPStorage().Select(selectStmt.Table, selectStmt.Columns, selectStmt.Where)
		
		if oltpErr == nil && len(oltpRows) > 0 {
			// Found data in OLTP, display it
			mapRows := make([]map[string]interface{}, len(oltpRows))
			for i, row := range oltpRows {
				mapRows[i] = row
			}
			fmt.Printf("Retrieved %d rows from OLTP storage\n", len(mapRows))
			printFormattedResults(mapRows)
			duration := time.Since(startTime)
			fmt.Printf("Execution completed in %v\n", duration)
			return
		}
		
		// If OLTP storage returned no rows, continue with regular query
		if oltpErr != nil {
			fmt.Printf("OLTP storage error: %v\n", oltpErr)
		} else {
			fmt.Println("OLTP storage returned no rows, trying hybrid...")
		}
		
		fmt.Printf("Query classified as %s, using %s storage\n",
			map[bool]string{true: "analytical", false: "transactional"}[isOLAP],
			storageType)
		
		// Execute the SELECT statement
		result, err := stmt.(parser.Statement).Execute(s)
		duration := time.Since(startTime)
		
		if err != nil {
			fmt.Printf("Error executing statement: %v\n", err)
			return
		}
		
		// For SELECT statements, also try to retrieve table directly to display rows
		table := s.GetTable(selectStmt.Table)
		rowsEmpty := true
		
		// Check if result set is empty
		if result != nil {
			if rows, ok := result.([]map[string]interface{}); ok {
				rowsEmpty = len(rows) == 0
			} else if typedRows, ok := result.([]types.Row); ok {
				rowsEmpty = len(typedRows) == 0
				if !rowsEmpty {
					// Convert typed rows to interface rows
					mapRows := make([]map[string]interface{}, len(typedRows))
					for i, row := range typedRows {
						mapRows[i] = row
					}
					result = mapRows
				}
			}
		}
		
		// Print the result with timing information
		fmt.Printf("Execution completed in %v\n", duration)
		
		// Display results or table schema
		if rowsEmpty && table != nil {
			// Try direct OLTP query to see what's there (diagnostic measure)
			fmt.Println("Trying direct OLTP query as a diagnostic...")
			oltpRows, _ := s.GetOLTPStorage().Select(selectStmt.Table, selectStmt.Columns, selectStmt.Where)
			if len(oltpRows) > 0 {
				// Found data in OLTP, display it
				mapRows := make([]map[string]interface{}, len(oltpRows))
				for i, row := range oltpRows {
					mapRows[i] = row
				}
				fmt.Printf("Retrieved %d rows directly from OLTP storage\n", len(mapRows))
				printFormattedResults(mapRows)
				return
			} else {
				fmt.Println("Direct OLTP query also returned no rows.")
			}
			
			// If SELECT returned no results but table exists, provide some info about the table
			fmt.Printf("Table '%s' exists but has no rows or no rows match your query.\n", selectStmt.Table)
			fmt.Println("Table schema:")
			for _, col := range table.Columns {
				fmt.Printf("  %s (%s)\n", col.Name, col.Type)
			}
		} else if !rowsEmpty {
			// Display rows if we have them
			if rows, ok := result.([]map[string]interface{}); ok {
				fmt.Printf("Retrieved %d rows\n", len(rows))
				printFormattedResults(rows)
			} else {
				fmt.Println(result)
			}
		} else {
			fmt.Println("Empty result set")
		}
		
		return
	}
	
	// For non-SELECT statements
	result, err := stmt.(parser.Statement).Execute(s)
	duration := time.Since(startTime)
	
	if err != nil {
		fmt.Printf("Error executing statement: %v\n", err)
		return
	}

	// Print the result with timing information
	fmt.Printf("Execution completed in %v\n", duration)
	if result != nil {
		if rows, ok := result.([]map[string]interface{}); ok {
			fmt.Printf("Retrieved %d rows\n", len(rows))
			printFormattedResults(rows)
		} else if typedRows, ok := result.([]types.Row); ok {
			fmt.Printf("Retrieved %d rows\n", len(typedRows))
			// Convert typed rows to interface rows
			mapRows := make([]map[string]interface{}, len(typedRows))
			for i, row := range typedRows {
				mapRows[i] = row
			}
			printFormattedResults(mapRows)
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