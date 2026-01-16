# Architecture: LiteLedger

## 1. Storage Engine (The "Ledger")
* **Pattern:** Log-Structured File System (Append-Only).
* **File Structure:** Each table is a single file (e.g., `data/users.db`).
* **Row Format:** `id|active_flag|col1|col2|checksum\n`
    * `id`: Unique Integer (PK).
    * `active_flag`: 1 = Active, 0 = Deleted (Tombstone).
    * `checksum`: Simple hash of the row content (for "tamper proofing").

## 2. Indexing (The "MemIndex")
* **Structure:** We do NOT use B-Trees. We use a Hash Index.
* **Type:** `map[string]map[string]int64`
    * `Table Name -> Primary Key (String) -> Byte Offset (int64)`
* **Startup Sequence:**
    1.  Open `.db` file.
    2.  Read line by line.
    3.  If `active_flag == 1`, store `id` and `offset` in map.
    4.  If `active_flag == 0`, remove `id` from map (if exists).

## 3. Supported SQL Syntax (Strict Subset)
The parser should strictly support only these patterns to save time:
* `CREATE TABLE name (col1 type, col2 type)`
* `INSERT INTO name VALUES (val1, val2)`
* `SELECT * FROM name WHERE id = val`
* `DELETE FROM name WHERE id = val`
* `JOIN table1 table2 ON table1.id = table2.fk_id` (Nested Loop)

## 4. Web Server
* **Endpoints:**
    * `POST /query`: Accepts raw SQL string, returns JSON.
    * `GET /`: HTML Dashboard (Server-Side Rendered).