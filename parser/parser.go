package parser

import (
	"fmt"
	"pesapal-ledger/engine"
	"strings"
)

// ParseSQL parses a raw SQL query and executes it against the database engine
func ParseSQL(query string, db *engine.Database) (interface{}, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("empty query")
	}

	// Normalize for prefix check (case insensitive)
	upperQuery := strings.ToUpper(query)

	if strings.HasPrefix(upperQuery, "CREATE TABLE") {
		return parseCreateTable(query, db)
	} else if strings.HasPrefix(upperQuery, "SHOW TABLES") {
		return db.ListTables(), nil
	} else if strings.HasPrefix(upperQuery, "INSERT INTO") {
		return parseInsert(query, db)
	} else if strings.HasPrefix(upperQuery, "SELECT") {
		return parseSelect(query, db)
	} else if strings.HasPrefix(upperQuery, "DELETE FROM") {
		return parseDelete(query, db)
	} else if strings.HasPrefix(upperQuery, "UPDATE") {
		return parseUpdate(query, db)
	}

	return nil, fmt.Errorf("unknown or unsupported command")
}

// parseDelete parses "DELETE FROM name WHERE id = val"
func parseDelete(query string, db *engine.Database) (interface{}, error) {
	// Logic similar to parseSelect but calls DeleteRow
	upper := strings.ToUpper(query)
	if !strings.HasPrefix(upper, "DELETE FROM ") {
		return nil, fmt.Errorf("invalid DELETE syntax")
	}

	// Remove "DELETE FROM "
	rest := query[12:]
	
	// Split by " WHERE "
	parts := strings.SplitN(upper[12:], " WHERE ", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("missing WHERE clause")
	}

	tableName := strings.TrimSpace(rest[:len(parts[0])])
	whereClause := strings.TrimSpace(rest[len(parts[0])+7:]) // +7 for " WHERE "
	
	// Parse "id = val"
	condParts := strings.Split(whereClause, "=")
	if len(condParts) != 2 {
		return nil, fmt.Errorf("invalid WHERE clause, expected 'id = val'")
	}
	
	col := strings.TrimSpace(condParts[0])
	val := strings.TrimSpace(condParts[1])
	
	if strings.ToLower(col) != "id" {
		return nil, fmt.Errorf("only filtering by 'id' is supported")
	}
	
	if err := db.DeleteRow(tableName, val); err != nil {
		return nil, err
	}
	
	return "Row deleted successfully", nil
}

// parseUpdate parses "UPDATE table SET col1=val1, col2=val2 WHERE id=val"
func parseUpdate(query string, db *engine.Database) (interface{}, error) {
	upper := strings.ToUpper(query)
	if !strings.HasPrefix(upper, "UPDATE ") {
		return nil, fmt.Errorf("invalid UPDATE syntax")
	}

	// Remove "UPDATE "
	rest := query[7:] // len("UPDATE ")
	
	// Find " SET "
	idxSet := strings.Index(upper[7:], " SET ")
	if idxSet == -1 {
		return nil, fmt.Errorf("missing SET clause")
	}
	
	tableName := strings.TrimSpace(rest[:idxSet])
	restAfterTable := rest[idxSet+5:] // len(" SET ")
	upperAfterTable := upper[7+idxSet+5:]

	// Find " WHERE "
	idxWhere := strings.Index(upperAfterTable, " WHERE ")
	if idxWhere == -1 {
		return nil, fmt.Errorf("missing WHERE clause")
	}
	
	setClause := strings.TrimSpace(restAfterTable[:idxWhere])
	whereClause := strings.TrimSpace(restAfterTable[idxWhere+7:]) // len(" WHERE ")
	
	// Parse WHERE clause "id = val"
	condParts := strings.Split(whereClause, "=")
	if len(condParts) != 2 {
		return nil, fmt.Errorf("invalid WHERE clause, expected 'id = val'")
	}
	
	col := strings.TrimSpace(condParts[0])
	idVal := strings.TrimSpace(condParts[1])
	
	if strings.ToLower(col) != "id" {
		return nil, fmt.Errorf("only filtering by 'id' is supported")
	}
	
	// Parse SET clause "col1=val1, col2=val2"
	updates := make(map[string]string)
	assignments := strings.Split(setClause, ",")
	for _, assignment := range assignments {
		parts := strings.Split(assignment, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid assignment in SET clause: %s", assignment)
		}
		
		colName := strings.TrimSpace(parts[0])
		colVal := strings.TrimSpace(parts[1])
		updates[colName] = colVal
	}
	
	if len(updates) == 0 {
		return nil, fmt.Errorf("no columns to update")
	}
	
	if err := db.UpdateRow(tableName, idVal, updates); err != nil {
		return nil, err
	}
	
	return "Row updated successfully", nil
}

// parseCreateTable parses "CREATE TABLE name (col1, col2, ...)"
func parseCreateTable(query string, db *engine.Database) (interface{}, error) {
	// Simple parsing strategy:
	// 1. Remove "CREATE TABLE " prefix
	// 2. Split by "(" to get name and columns part
	// 3. Parse columns
	
	// Case insensitive prefix removal
	rest := query[13:] // len("CREATE TABLE ")
	rest = strings.TrimSpace(rest)

	parts := strings.SplitN(rest, "(", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax: missing '('")
	}

	tableName := strings.TrimSpace(parts[0])
	columnsPart := strings.TrimSuffix(strings.TrimSpace(parts[1]), ")")

	if tableName == "" {
		return nil, fmt.Errorf("invalid table name")
	}

	// Split columns by comma
	colsRaw := strings.Split(columnsPart, ",")
	var columns []string
	for _, c := range colsRaw {
		col := strings.TrimSpace(c)
		// We might want to strip types (e.g. "id int") -> just keep "id" or full string?
		// Architecture says: "CREATE TABLE name (col1 type, col2 type)"
		// Engine CreateTable expects []string columns. 
		// For simplicity, let's keep the full definition for now or just the name?
		// Engine doesn't seem to use types yet, just stores metadata.
		// Let's store the full "name type" string for metadata.
		if col != "" {
			columns = append(columns, col)
		}
	}

	if err := db.CreateTable(tableName, columns); err != nil {
		return nil, err
	}

	return fmt.Sprintf("Table '%s' created successfully", tableName), nil
}

// parseInsert parses "INSERT INTO name VALUES (val1, val2, ...)"
func parseInsert(query string, db *engine.Database) (interface{}, error) {
	// Remove "INSERT INTO "
	rest := query[12:] 
	rest = strings.TrimSpace(rest)

	// Split by " VALUES " (case insensitive search needed? assuming standard casing from user or strict)
	// Let's do a case-insensitive split
	idx := strings.Index(strings.ToUpper(rest), " VALUES ")
	if idx == -1 {
		return nil, fmt.Errorf("invalid INSERT syntax: missing VALUES")
	}

	tableName := strings.TrimSpace(rest[:idx])
	valuesPart := strings.TrimSpace(rest[idx+8:]) // len(" VALUES ")

	if !strings.HasPrefix(valuesPart, "(") || !strings.HasSuffix(valuesPart, ")") {
		return nil, fmt.Errorf("invalid VALUES syntax: must be enclosed in ()")
	}

	valuesContent := valuesPart[1 : len(valuesPart)-1]
	
	// Split values by comma. Note: this breaks if values contain commas. 
	// For "Strict Subset" / MVP, simple split is okay.
	// We need to handle id|active_flag|...
	// User provides: (1, John, ...)
	// System needs: 1|1|John|... (active_flag=1 is automatic?)
	// Architecture says: "INSERT INTO name VALUES (val1, val2)"
	// Architecture row format: id|active_flag|col1|col2|checksum
	// So user provides val1 (id), val2 (col1?). 
	// Wait, architecture: "INSERT INTO name VALUES (val1, val2)"
	// Row: id|active|col1|col2...
	// Does user provide ID? Yes, usually.
	// Does user provide active_flag? No, that's internal.
	// So we need to inject active_flag=1.
	
	valsRaw := strings.Split(valuesContent, ",")
	var values []string
	for _, v := range valsRaw {
		values = append(values, strings.TrimSpace(v))
	}
	
	if len(values) < 1 {
		return nil, fmt.Errorf("no values provided")
	}

	// Construct row: ID | 1 | col1 | col2 ...
	// values[0] is ID.
	// We need to insert "1" (active) after ID.
	
	row := make([]string, 0, len(values)+1)
	row = append(row, values[0]) // ID
	row = append(row, "1")       // Active Flag
	row = append(row, values[1:]...) // Rest of columns

	if err := db.InsertRow(tableName, row); err != nil {
		return nil, err
	}

	return "Row inserted successfully", nil
}

// parseSelect parses "SELECT * FROM name WHERE id = val"
func parseSelect(query string, db *engine.Database) (interface{}, error) {
	// Strict subset: "SELECT * FROM name WHERE id = val"
	// We assume strictly this format for now.
	
	upper := strings.ToUpper(query)
	if !strings.HasPrefix(upper, "SELECT * FROM ") {
		return nil, fmt.Errorf("only 'SELECT * FROM ...' supported")
	}

	rest := query[14:] // len("SELECT * FROM ")
	
	parts := strings.SplitN(upper[14:], " WHERE ", 2)
	
	if len(parts) == 1 {
		// No WHERE clause, assume Select All
		tableName := strings.TrimSpace(query[14:]) // Use original query for case
		rows, err := db.SelectAll(tableName)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	
	// Re-slice from original 'rest' to preserve case of table name (if needed)
	// parts[0] length in rest is same as in upper
	tableName := strings.TrimSpace(rest[:len(parts[0])])
	whereClause := strings.TrimSpace(rest[len(parts[0])+7:]) // +7 for " WHERE "
	
	// Parse "id = val"
	condParts := strings.Split(whereClause, "=")
	if len(condParts) != 2 {
		return nil, fmt.Errorf("invalid WHERE clause, expected 'id = val'")
	}
	
	col := strings.TrimSpace(condParts[0])
	val := strings.TrimSpace(condParts[1])
	
	// Handle search by ID or generic column
	if strings.ToLower(col) == "id" {
		row, err := db.FindByID(tableName, val)
		if err != nil {
			return nil, err
		}
		return [][]string{row}, nil
	} else {
		// Generic column search
		return db.SelectByColumn(tableName, col, val)
	}
}
