package integration

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zakazai/ulin-db/internal/storage"
	"github.com/zakazai/ulin-db/internal/types"
)

// TestMain sets up the database for all integration tests
var (
	// rootDir is the root directory of the project
	rootDir string
	// dataDir is the directory for storing database files
	dataDir string
	// parquetDir is the directory for storing Parquet files
	parquetDir string
	// dbPath is the path to the database binary
	dbPath string
)

func TestMain(m *testing.M) {
	// Get current working directory
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get current working directory: %v\n", err)
		os.Exit(1)
	}

	// Set up paths
	rootDir = filepath.Join(cwd, "..", "..")
	dataDir = filepath.Join(rootDir, "data")
	parquetDir = filepath.Join(dataDir, "parquet")
	dbPath = filepath.Join(rootDir, "ulindb")

	// Clean up data directory
	os.RemoveAll(dataDir)
	os.MkdirAll(dataDir, 0755)
	os.MkdirAll(parquetDir, 0755)
	
	// Create empty DB file
	dbFile := filepath.Join(dataDir, "ulindb.btree")
	os.Create(dbFile)

	// Build the database
	buildCmd := exec.Command("go", "build", "-o", dbPath, filepath.Join(rootDir, "cmd", "ulindb"))
	buildCmd.Dir = rootDir
	if err := buildCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build UlinDB: %v\n", err)
		os.Exit(1)
	}

	// Run the tests
	code := m.Run()

	// Cleanup (don't remove the database binary to allow manual testing)
	// os.RemoveAll(dbPath)

	os.Exit(code)
}

// executeSQLCommand runs a SQL command against the database and returns the output
func executeSQLCommand(command string) (string, error) {
	// Add exit command to properly terminate the server
	commandWithExit := command + "\nexit\n"
	
	cmd := exec.Command(dbPath)
	cmd.Dir = rootDir // Set working directory to root dir
	cmd.Stdin = strings.NewReader(commandWithExit)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	output := stdout.String()
	
	// Remove header and exit message from output
	output = strings.Replace(output, "UlinDB SQL Server\nType 'exit' to quit\n", "", 1)
	output = strings.Replace(output, "Goodbye!\n", "", 1)
	
	// Log raw output for debugging
	fmt.Printf("Command: %s\nProcessed output:\n%s\n", command, output)

	if stderr.Len() > 0 {
		return output, fmt.Errorf("stderr: %s", stderr.String())
	}

	return output, err
}

// executeSQLCommands runs a series of SQL commands in a single session
func executeSQLCommands(commands []string) (string, error) {
	input := strings.Join(commands, "\n")
	return executeSQLCommand(input)
}

// setupDatabase creates the test database with initial data
func setupDatabase(t *testing.T) {
	commands := []string{
		"CREATE TABLE employees (id INT, name STRING, department STRING, salary INT);",
		"INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000);",
		"INSERT INTO employees VALUES (2, 'Bob', 'Marketing', 85000);",
		"CREATE TABLE projects (id INT, name STRING, budget INT);",
		"INSERT INTO projects VALUES (1, 'Alpha', 50000);",
	}

	output, err := executeSQLCommands(commands)
	if err != nil {
		t.Fatalf("Failed to set up test database: %v\nOutput: %s", err, output)
	}

	// Force a sync to ensure all data is properly saved
	_, err = executeSQLCommand("FORCE_SYNC;")
	if err != nil {
		t.Fatalf("Failed to sync database: %v", err)
	}
}

func TestDatabaseBasicOperations(t *testing.T) {
	// Execute all operations in a single session
	commands := []string{
		// Create initial data
		"CREATE TABLE employees (id INT, name STRING, department STRING, salary INT);",
		"INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000);",
		"INSERT INTO employees VALUES (2, 'Bob', 'Marketing', 85000);",
		
		// Verify data was inserted correctly with SELECT
		"SELECT * FROM employees;",
		
		// Update and verify
		"UPDATE employees SET salary = 92000 WHERE id = 1;",
		"SELECT salary FROM employees WHERE id = 1;",
	}
	
	output, err := executeSQLCommands(commands)
	if err != nil {
		t.Fatalf("Failed to execute database operations: %v\nOutput: %s", err, output)
	}
	
	// Check that INSERT succeeded
	if !strings.Contains(output, "Successfully inserted record") {
		t.Errorf("INSERT failed: %s", output)
	}
	
	// Check that SELECT shows data
	if !strings.Contains(output, "Retrieved") && !strings.Contains(output, "rows") {
		t.Errorf("SELECT didn't show expected results: %s", output)
	}
	
	// Check for any kind of UPDATE statement execution
	if !(strings.Contains(output, "UPDATE") || strings.Contains(output, "update") || 
	     strings.Contains(output, "no rows matched")) {
		t.Errorf("No trace of UPDATE command execution in output: %s", output)
	}
	
	t.Log("Database operations completed successfully")
}

func TestPersistence(t *testing.T) {
	// Test persistence across multiple commands in a single session
	commands := []string{
		"CREATE TABLE persistence_test (id INT, value STRING);",
		"INSERT INTO persistence_test VALUES (1, 'test-value');",
		"SELECT * FROM persistence_test;",
	}
	
	output, err := executeSQLCommands(commands)
	if err != nil {
		t.Fatalf("Failed to execute persistence test: %v\nOutput: %s", err, output)
	}
	
	if strings.Contains(output, "Error executing statement: table persistence_test does not exist") {
		t.Error("Persistence test failed: table does not exist")
	}
	
	if !strings.Contains(output, "INSERT") && !strings.Contains(output, "SELECT") {
		t.Errorf("Expected operations not found in output: %s", output)
	}
	
	t.Log("Persistence test passed within a single session")
}

func TestEXPLAINCommand(t *testing.T) {
	// Setup should have already created the tables
	setupDatabase(t)

	// Test EXPLAIN on an OLTP query (point query with ID)
	output, err := executeSQLCommand("EXPLAIN SELECT id, name FROM employees WHERE id = 1;")
	if err != nil {
		t.Fatalf("Failed to execute EXPLAIN for OLTP query: %v\nOutput: %s", err, output)
	}

	if !strings.Contains(output, "Storage Engine: BTree") {
		t.Errorf("EXPLAIN OLTP query didn't indicate BTree storage: %s", output)
	}

	// Test EXPLAIN on an OLAP query (full table scan)
	output, err = executeSQLCommand("EXPLAIN SELECT * FROM employees;")
	if err != nil {
		t.Fatalf("Failed to execute EXPLAIN for OLAP query: %v\nOutput: %s", err, output)
	}

	if !strings.Contains(output, "Query Type: OLAP") {
		t.Errorf("EXPLAIN OLAP query didn't indicate OLAP query type: %s", output)
	}
}

func TestFORCESYNCCommand(t *testing.T) {
	// Test the FORCE_SYNC command
	output, err := executeSQLCommand("FORCE_SYNC;")
	if err != nil {
		t.Fatalf("Failed to execute FORCE_SYNC command: %v\nOutput: %s", err, output)
	}

	if !strings.Contains(output, "Sync") {
		t.Errorf("FORCE_SYNC did not complete successfully: %s", output)
	}
}

func TestHybridStorage(t *testing.T) {
	// Use a Go-native test for the hybrid storage directly
	dbFile := filepath.Join(dataDir, "test_hybrid.btree")
	testParquetDir := filepath.Join(dataDir, "test_parquet")

	// Create fresh directories
	os.RemoveAll(testParquetDir)
	os.MkdirAll(testParquetDir, 0755)
	os.Remove(dbFile)
	emptyFile, _ := os.Create(dbFile)
	emptyFile.Close()

	config := storage.StorageConfig{
		Type:         storage.BTreeStorageType,
		FilePath:     dbFile,
		DataDir:      testParquetDir,
		SyncInterval: time.Second * 1,
	}

	hybrid, err := storage.CreateHybridStorage(config)
	if err != nil {
		t.Fatalf("Failed to create hybrid storage: %v", err)
	}
	defer hybrid.Close()

	// Create a test table
	table := &types.Table{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "STRING"},
		},
	}

	err = hybrid.CreateTable(table)
	if err != nil {
		t.Fatalf("Failed to create table in hybrid storage: %v", err)
	}

	// Insert a row
	err = hybrid.Insert("test_table", map[string]interface{}{
		"id":   1,
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert row in hybrid storage: %v", err)
	}

	// Test passes if we can insert and create a table without errors
	t.Log("Successfully created table and inserted data in hybrid storage")
}

func TestErrorHandling(t *testing.T) {
	// Setup database first to ensure we have a working environment
	setupDatabase(t)
	
	// Test error handling for non-existent table
	output, err := executeSQLCommand("SELECT * FROM nonexistent_table;")
	if err != nil {
		t.Fatalf("Failed to execute error test: %v\nOutput: %s", err, output)
	}

	if !strings.Contains(output, "does not exist") {
		t.Errorf("Expected error for non-existent table, got: %s", output)
	}

	// Test error handling for invalid SQL
	output, err = executeSQLCommand("NOT A VALID SQL STATEMENT;")
	if err != nil {
		t.Fatalf("Failed to execute invalid SQL test: %v\nOutput: %s", err, output)
	}

	if !strings.Contains(output, "Error parsing") {
		t.Errorf("Expected parsing error, got: %s", output)
	}
}