package storage

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// storageMutex protects file access to ensure thread safety
var storageMutex sync.RWMutex

// calculateChecksum computes a SHA-256 checksum of the pipe-joined data
func calculateChecksum(data []string) string {
	content := strings.Join(data, "|")
	hash := sha256.Sum256([]byte(content))
	return hex.EncodeToString(hash[:])
}

// AppendRow appends a new row to the table file.
// The data slice represents the columns of the row.
// Returns the offset at which the row was written and an error if any.
func AppendRow(tableName string, data []string) (int64, error) {
	storageMutex.Lock()
	defer storageMutex.Unlock()

	// Ensure data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		return 0, fmt.Errorf("failed to create data directory: %w", err)
	}

	filePath := filepath.Join("data", tableName+".db")
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open table file %s: %w", tableName, err)
	}
	defer file.Close()

	// Calculate checksum of the row content
	checksum := calculateChecksum(data)
	
	// Create a new slice with checksum appended
	rowWithChecksum := make([]string, len(data)+1)
	copy(rowWithChecksum, data)
	rowWithChecksum[len(data)] = checksum

	// Get current offset
	stat, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %s: %w", tableName, err)
	}
	offset := stat.Size()

	// Join data with pipes and add newline
	line := strings.Join(rowWithChecksum, "|") + "\n"

	if _, err := file.WriteString(line); err != nil {
		return 0, fmt.Errorf("failed to write row to %s: %w", tableName, err)
	}

	return offset, nil
}

// ReadRow reads a row from the table file at the given offset.
func ReadRow(tableName string, offset int64) ([]string, error) {
	storageMutex.RLock()
	defer storageMutex.RUnlock()

	filePath := filepath.Join("data", tableName+".db")
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open table file %s: %w", tableName, err)
	}
	defer file.Close()

	if _, err := file.Seek(offset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d in %s: %w", offset, tableName, err)
	}

	reader := bufio.NewReader(file)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read line at offset %d in %s: %w", offset, tableName, err)
	}

	// Remove newline and split by pipe
	line = strings.TrimSuffix(line, "\n")
	parts := strings.Split(line, "|")
	
	// Checksum verification
	if len(parts) < 2 {
		return nil, fmt.Errorf("corrupt row: insufficient data")
	}

	// The last part is the stored checksum
	storedChecksum := parts[len(parts)-1]
	// The rest is the data
	dataParts := parts[:len(parts)-1]

	calculatedChecksum := calculateChecksum(dataParts)
	if storedChecksum != calculatedChecksum {
		return nil, errors.New("SECURITY ALERT: Row data has been tampered with!")
	}

	return dataParts, nil
}

// OpenTableFile opens the table file for reading. 
// It returns the file handle which the caller is responsible for closing.
func OpenTableFile(tableName string) (*os.File, error) {
    // Note: Caller is responsible for locking if needed, though simply opening for read usually doesn't require global lock 
    // unless we are protecting against file deletion/renaming.
    // For simplicity in this architecture, we assume files persist.
    
	filePath := filepath.Join("data", tableName+".db")
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
            // Return nil, nil if file doesn't exist yet, or error? 
            // Architecture says "Open .db file", implying existence or creation.
            // Let's return the error to be explicit.
			return nil, fmt.Errorf("table file %s does not exist: %w", tableName, err)
		}
		return nil, fmt.Errorf("failed to open table file %s: %w", tableName, err)
	}
	return file, nil
}

// CreateTableFile creates the table file if it doesn't exist.
func CreateTableFile(tableName string) error {
	storageMutex.Lock()
	defer storageMutex.Unlock()

	// Ensure data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	filePath := filepath.Join("data", tableName+".db")
	
	// Create the file. If it exists, it truncates it? No, we shouldn't truncate if it exists.
	// But CreateTable in engine checks if table exists in memory.
	// If file exists on disk but not in memory (restart), we should load it.
	// But CreateTable is for NEW tables.
	// Use os.Create will truncate. Use OpenFile with O_CREATE|O_EXCL to fail if exists?
	// The safest is O_CREATE without O_TRUNC.
	
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("table file %s already exists", tableName)
		}
		return fmt.Errorf("failed to create table file %s: %w", tableName, err)
	}
	file.Close()
	return nil
}
