# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands
- Build: `go build -o ulindb ./cmd/ulindb`
- Run interactive mode: `./ulindb`
- Run with test SQL: `./run.sh`
- Run all tests: `go test ./...`
- Run single test: `go test ./internal/package -run=TestName -v`
- Run specific package: `go test ./internal/parser`
- Format code: `go fmt ./...`
- Check for issues: `go vet ./...`
- Manage dependencies: `go mod tidy`
- View BTree storage: `./scripts/view_btree.sh [path/to/btree_file]`
- View Parquet storage: `./scripts/view_parquet.sh [parquet_dir] [table_name]`
- Force sync to Parquet: Use `hybridStorage.SyncNow()` in code

## Project Structure
- `cmd/ulindb`: Entry point for the SQL server
- `internal/lexer`: SQL tokenization
- `internal/parser`: SQL parsing and AST
- `internal/planner`: Query planning and optimization
- `internal/storage`: Storage engines (BTree, JSON, InMemory)
- `internal/types`: Common type definitions
- `scripts`: Utility scripts for testing and development
- `data`: Database file storage location

## Storage Engines
- Hybrid Storage (Default): Uses BTree for OLTP and Parquet for OLAP queries
- BTree: Persistent on-disk storage optimized for transactional workloads
- Parquet: Columnar storage format optimized for analytical queries
- Also supports: InMemory and JSON
- Configure in cmd/ulindb/main.go via storage.StorageConfig

## Testing
- Unit tests use the standard Go testing package
- Table-driven tests are used extensively
- Lexer tests verify token recognition
- Parser tests validate SQL parsing
- Storage tests check data persistence

## Code Style
- Package structure: cmd/, internal/ (lexer, parser, planner, storage, types)
- Naming: PascalCase for exported types/methods, camelCase for internal
- Errors: Return errors explicitly, don't use panics
- Testing: Table-driven tests with descriptive names
- Imports: Stdlib first, then third-party, grouped alphabetically
- Comments: Document exported types/functions with standard Go comments
- Minimal dependencies: Use standard library where possible

## SQL Support
- Basic CRUD operations: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
- Basic WHERE clauses with equality conditions
- Data types: INT, STRING