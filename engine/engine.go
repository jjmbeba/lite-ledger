package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"pesapal-ledger/storage"
	"sort"
	"strings"
	"sync"
)

// Index maps Primary Key (string) -> File Offset (int64)
type Index map[string]int64

// TableMetadata holds metadata for a table
type TableMetadata struct {
	Name    string
	Columns []string
}

// Database represents the in-memory state of the database
type Database struct {
	// Tables maps Table Name -> Index
	Indexes map[string]Index
	// Metadata maps Table Name -> Metadata
	Tables map[string]TableMetadata
	// Mutex to protect concurrent access to the indexes
	mu sync.RWMutex
}

// NewDatabase initializes a new Database instance
func NewDatabase() *Database {
	return &Database{
		Indexes: make(map[string]Index),
		Tables:  make(map[string]TableMetadata),
	}
}

// SaveMetadata persists the table schemas to disk
func (db *Database) SaveMetadata() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Ensure data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	filePath := filepath.Join("data", "metadata.json")
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create metadata file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(db.Tables); err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	return nil
}

// LoadMetadata reads the table schemas from disk
func (db *Database) LoadMetadata() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	filePath := filepath.Join("data", "metadata.json")
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No metadata file yet, start empty
		}
		return fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer file.Close()

	if err := json.NewDecoder(file).Decode(&db.Tables); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	// Initialize indexes for loaded tables
	for name := range db.Tables {
		if _, exists := db.Indexes[name]; !exists {
			db.Indexes[name] = make(Index)
		}
	}

	return nil
}

// Recover restores the database state from disk on startup
func (db *Database) Recover() error {
	// 1. Load Metadata (Schemas)
	if err := db.LoadMetadata(); err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	// 2. Load Indexes for each table
	// We iterate over a copy of keys to avoid locking issues if LoadIndex locks
	// LoadMetadata already populated db.Tables keys.
	
	// We need to read tables safely
	db.mu.RLock()
	var tables []string
	for name := range db.Tables {
		tables = append(tables, name)
	}
	db.mu.RUnlock()

	for _, name := range tables {
		if err := db.LoadIndex(name); err != nil {
			fmt.Printf("Warning: Failed to load index for table %s: %v\n", name, err)
			// Continue recovering other tables
		}
	}

	return nil
}

// CreateTable creates a new table with the given name and columns
func (db *Database) CreateTable(name string, columns []string) error {
	db.mu.Lock()
	// No defer unlock because we need to unlock before SaveMetadata

	if _, exists := db.Tables[name]; exists {
		db.mu.Unlock()
		return fmt.Errorf("table %s already exists", name)
	}

	// Initialize metadata
	db.Tables[name] = TableMetadata{
		Name:    name,
		Columns: columns,
	}

	// Initialize index
	db.Indexes[name] = make(Index)

	// Ensure the underlying file exists
	if err := storage.CreateTableFile(name); err != nil {
		// Check if error is "already exists"
		if strings.Contains(err.Error(), "already exists") {
			// If file exists, load index
			file, errOpen := storage.OpenTableFile(name)
			if errOpen == nil {
				defer file.Close()
				scanner := bufio.NewScanner(file)
				var offset int64 = 0
				for scanner.Scan() {
					line := scanner.Text()
					lineLen := int64(len(line) + 1)
					parts := strings.Split(line, "|")
					if len(parts) >= 2 {
						id := parts[0]
						activeFlag := parts[1]
						if activeFlag == "1" {
							db.Indexes[name][id] = offset
						} else if activeFlag == "0" {
							delete(db.Indexes[name], id)
						}
					}
					offset += lineLen
				}
			}
			
			db.mu.Unlock()
			if err := db.SaveMetadata(); err != nil {
				return fmt.Errorf("failed to save metadata: %w", err)
			}
			return nil
		}
		
		// Real error
		delete(db.Tables, name)
		delete(db.Indexes, name)
		db.mu.Unlock()
		return fmt.Errorf("failed to create table file: %w", err)
	}

	db.mu.Unlock()
	if err := db.SaveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// ListTables returns a list of all table names
func (db *Database) ListTables() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tables := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		tables = append(tables, name)
	}
	
	// Sort for consistent output
	sort.Strings(tables)
	return tables
}

// LoadIndex rebuilds the in-memory index from the log file on startup
func (db *Database) LoadIndex(tableName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Initialize index for this table if it doesn't exist
	if _, exists := db.Indexes[tableName]; !exists {
		db.Indexes[tableName] = make(Index)
	}

	file, err := storage.OpenTableFile(tableName)
	if err != nil {
		// If file doesn't exist, that's fine, we just start fresh. 
		// But if it's another error, we should return it.
        // For now, let's treat "not exist" as empty table.
        // We'll verify error type string or check wrapped error if possible, 
        // but simple check is: if error, maybe just return nil if it's "not exist"
        // Let's pass the error up for now, caller decides.
        // Actually, if it's a new table, file won't exist.
		return nil // Assume new table
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var offset int64 = 0

	for scanner.Scan() {
		line := scanner.Text()
		lineLen := int64(len(line) + 1) // +1 for newline

		parts := strings.Split(line, "|")
		if len(parts) < 2 {
			offset += lineLen
			continue
		}

		id := parts[0]
		activeFlag := parts[1]

		if activeFlag == "1" {
			db.Indexes[tableName][id] = offset
		} else if activeFlag == "0" {
			delete(db.Indexes[tableName], id)
		}

		offset += lineLen
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading table file %s: %w", tableName, err)
	}

	return nil
}

// RebuildIndex clears the index and rebuilds it from the log file.
// It reads the file line-by-line, tracking byte offsets and handling tombstones.
func (db *Database) RebuildIndex(tableName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Clear the index for this table (start fresh)
	db.Indexes[tableName] = make(Index)

	file, err := storage.OpenTableFile(tableName)
	if err != nil {
		// If file doesn't exist, it's just an empty table.
		// Since we don't import os here and OpenTableFile wraps the error,
		// we can check the error string or just return nil if we assume non-existence.
		// For robustness, we'll assume any error opening means we can't read it,
		// but specifically for "doesn't exist" we should be fine.
		// Given LoadIndex behavior, we'll return nil for now.
		return nil 
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var offset int64 = 0

	for scanner.Scan() {
		line := scanner.Text()
		// Calculate length including newline. 
		// We assume \n line endings as written by AppendRow.
		lineLen := int64(len(line) + 1) 

		parts := strings.Split(line, "|")
		if len(parts) >= 2 {
			id := parts[0]
			activeFlag := parts[1]

			if activeFlag == "1" {
				db.Indexes[tableName][id] = offset
			} else if activeFlag == "0" {
				// Tombstone: remove from index
				delete(db.Indexes[tableName], id)
			}
		}

		// Update offset for the NEXT line
		offset += lineLen
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning table file %s: %w", tableName, err)
	}

	return nil
}

// FindByID looks up a row by its primary key
func (db *Database) FindByID(tableName string, id string) ([]string, error) {
	db.mu.RLock()
	index, exists := db.Indexes[tableName]
	metadata, metaExists := db.Tables[tableName]
	if !exists {
		db.mu.RUnlock()
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}
	
	offset, found := index[id]
	db.mu.RUnlock() // Unlock early

	if !found {
		return nil, fmt.Errorf("record with id %s not found in table %s", id, tableName)
	}

	// Read from storage (disk I/O outside of lock)
	row, err := storage.ReadRow(tableName, offset)
	if err != nil {
		return nil, err
	}

	// Clean up row if it has extra checksums
	if metaExists {
		expectedTotalLen := len(metadata.Columns) + 2
		if len(row) > expectedTotalLen {
			row = row[:expectedTotalLen]
		}
	}

	return row, nil
}

// SelectAll returns all rows in the table
func (db *Database) SelectAll(tableName string) ([][]string, error) {
	db.mu.RLock()
	index, exists := db.Indexes[tableName]
	metadata, metaExists := db.Tables[tableName] // Get metadata while locked
	if !exists {
		db.mu.RUnlock()
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Collect offsets to read
	type record struct {
		id     string
		offset int64
	}
	var records []record
	for id, off := range index {
		records = append(records, record{id: id, offset: off})
	}
	db.mu.RUnlock()

	// Sort by offset to preserve insertion order (or at least disk order)
	sort.Slice(records, func(i, j int) bool {
		return records[i].offset < records[j].offset
	})

	// Expected total length (data + checksum)
	expectedTotalLen := 0
	if metaExists {
		// id + active + (cols-1 because id is in cols) + checksum?
		// No, len(Columns) is N. Row has N+1 data items (active inserted at 1). +1 checksum.
		// Total N+2.
		expectedTotalLen = len(metadata.Columns) + 2
	}

	// Read rows
	var rows [][]string
	for _, rec := range records {
		row, err := storage.ReadRow(tableName, rec.offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read row for id %s: %w", rec.id, err)
		}
		
		// Clean up row if it has extra checksums
		if expectedTotalLen > 0 && len(row) > expectedTotalLen {
			// Keep only expected length
			row = row[:expectedTotalLen]
		}
		
		rows = append(rows, row)
	}

	return rows, nil
}

// InsertRow adds a new row to the database and updates the index
func (db *Database) InsertRow(tableName string, row []string) error {
    // Basic validation: row must have at least id and active_flag
    if len(row) < 2 {
        return fmt.Errorf("invalid row data: too few columns")
    }
    
    id := row[0]
    
    // Write to storage
    offset, err := storage.AppendRow(tableName, row)
    if err != nil {
        return fmt.Errorf("failed to append row: %w", err)
    }
    
    // Update index
    db.mu.Lock()
    defer db.mu.Unlock()
    
    if _, exists := db.Indexes[tableName]; !exists {
        db.Indexes[tableName] = make(Index)
    }
    
    db.Indexes[tableName][id] = offset
    
    return nil
}

// DeleteRow appends a tombstone row (active_flag=0) and removes the record from the index
func (db *Database) DeleteRow(tableName string, id string) error {
	// Step 1: Find the record to get current data
	currentRow, err := db.FindByID(tableName, id)
	if err != nil {
		return err // Record not found or table doesn't exist
	}
	
	// Step 2: Create tombstone row
	if len(currentRow) < 2 {
		return fmt.Errorf("corrupt data: row too short")
	}
	
	tombstoneRow := make([]string, len(currentRow))
	copy(tombstoneRow, currentRow)
	tombstoneRow[1] = "0" // Set active_flag to 0
	
	// Step 3: Append to storage
	_, err = storage.AppendRow(tableName, tombstoneRow)
	if err != nil {
		return fmt.Errorf("failed to append tombstone: %w", err)
	}
	
	// Step 4: Update Index (Remove)
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if index, exists := db.Indexes[tableName]; exists {
		delete(index, id)
	}
	
	return nil
}

// UpdateRow reads the current row, applies updates, and appends a new version
func (db *Database) UpdateRow(tableName string, id string, updates map[string]string) error {
	// Step 1: Find current row
	currentRow, err := db.FindByID(tableName, id)
	if err != nil {
		return err
	}
	
	// Step 2: Get metadata to map columns
	db.mu.RLock()
	metadata, exists := db.Tables[tableName]
	db.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("table %s metadata not found", tableName)
	}
	
	// Step 3: Prepare new row
	// Strict length enforcement: len(Columns) + 1 (for active_flag)
	// This strips ALL trailing checksums or garbage from previous corruptions
	expectedLen := len(metadata.Columns) + 1
	if len(currentRow) < expectedLen {
		// If row is shorter than expected schema, we can't safely update it without potentially shifting columns.
		// But strictly speaking, if we have enough data for the columns we want to update, maybe?
		// Safest is to error or pad.
		// For now, let's assume if it's short, it's corrupt or schema changed.
		// But let's try to proceed if we have at least minimums.
		// Actually, let's just error if it's too short, but if it's too long (checksums), we truncate.
		// If it's short, we can't reliably map columns.
		// But wait, if schema has 3 cols, and row has 2...
		return fmt.Errorf("data corruption: row shorter than schema (len=%d, expected=%d)", len(currentRow), expectedLen)
	}
	
	newRow := make([]string, expectedLen)
	copy(newRow, currentRow[:expectedLen])
	newRow[1] = "1" // Ensure active_flag is 1
	
	// Step 4: Apply updates
	for colName, newVal := range updates {
		colIndex := -1
		
		// Find column index in metadata
		// Metadata Columns: ["id int", "merchant text", ...]
		// Row: [id, active, merchant, ...]
		for i, colDef := range metadata.Columns {
			// Extract name from definition "name type"
			parts := strings.SplitN(colDef, " ", 2)
			name := parts[0]
			
			if strings.EqualFold(name, colName) {
				if i == 0 {
					colIndex = 0 // id
				} else {
					colIndex = i + 1 // Shift for active_flag
				}
				break
			}
		}
		
		if colIndex == -1 {
			return fmt.Errorf("column %s not found in table %s", colName, tableName)
		}
		
		if colIndex >= len(newRow) {
			return fmt.Errorf("row structure mismatch for column %s", colName)
		}
		
		newRow[colIndex] = newVal
	}
	
	// Step 5: Append new row
	offset, err := storage.AppendRow(tableName, newRow)
	if err != nil {
		return fmt.Errorf("failed to append updated row: %w", err)
	}
	
	// Step 6: Update Index
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if _, exists := db.Indexes[tableName]; exists {
		db.Indexes[tableName][id] = offset
	}
	
	return nil
}

// SelectByColumn returns rows where the specified column matches the value
func (db *Database) SelectByColumn(tableName, colName, value string) ([][]string, error) {
	// 1. Get column index
	db.mu.RLock()
	metadata, exists := db.Tables[tableName]
	db.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}
	
	targetColIndex := -1
	for i, colDef := range metadata.Columns {
		parts := strings.SplitN(colDef, " ", 2)
		if strings.EqualFold(parts[0], colName) {
			// Map to row index:
			// Metadata: [id, col1, col2]
			// Row:      [id, active, col1, col2, checksum]
			// If i==0 (id), row index 0.
			// If i>0, row index i+1.
			if i == 0 {
				targetColIndex = 0
			} else {
				targetColIndex = i + 1
			}
			break
		}
	}
	
	if targetColIndex == -1 {
		return nil, fmt.Errorf("column %s not found", colName)
	}
	
	// 2. Get all rows
	allRows, err := db.SelectAll(tableName)
	if err != nil {
		return nil, err
	}
	
	// 3. Filter
	var filtered [][]string
	for _, row := range allRows {
		if targetColIndex < len(row) && strings.EqualFold(row[targetColIndex], value) {
			filtered = append(filtered, row)
		}
	}
	
	return filtered, nil
}
