# LiteLedger Web Interface Implementation Plan

## Overview
Build a Single Page Application (SPA) web interface for LiteLedger with a dark terminal aesthetic, transaction management features, and demo mode functionality.

## Implementation Details

### Task 1: Create `web/index.html`

#### 1.1 HTML Structure
- **Location:** Create `web/index.html`
- **Structure:**
  - `<head>` with meta tags, title "LiteLedger // Merchant Terminal"
  - Inline CSS for dark terminal theme
  - Inline JavaScript for all functionality
  - Body with header and two-column layout

#### 1.2 Styling (Dark Terminal Theme)
- **Color Scheme:**
  - Background: `#000000` or `#0a0a0a`
  - Text: `#00ff00` (neon green) or `#00ffff` (cyan)
  - Accents: `#0080ff` (blue)
  - Borders: `#333333`
- **Typography:** Monospace font (`'Courier New', monospace` or `'Consolas', monospace`)
- **Layout:** CSS Grid or Flexbox for two-column layout
- **Components:**
  - Terminal-style input fields (dark bg, neon border)
  - Button styling (glow effect on hover)
  - Code block styling for results

#### 1.3 Left Column (Actions)
- **Initialize System Button:**
  - Text: "Initialize System"
  - On click: Send `CREATE TABLE transactions (id int, merchant text, amount int)` to `/sql`
  - Display success/error in results area
- **Record Transaction Form:**
  - Input fields: ID (number), Merchant (text), Amount (number)
  - Submit button: "Record Transaction"
  - On submit: Construct `INSERT INTO transactions VALUES (id, merchant, amount)`
  - POST to `/sql` endpoint

#### 1.4 Right Column (Ledger View)
- **Audit Transaction Section:**
  - Input field: "Transaction ID"
  - Button: "Search"
  - On click: Send `SELECT * FROM transactions WHERE id = <value>`
- **Results Area:**
  - `<div id="results">` or `<pre id="results">` for displaying output
  - Parse pipe-delimited response (e.g., `101|1|Starbucks|550|checksum`)
  - Display formatted: ID | Active | Merchant | Amount | Checksum
  - Show JSON response structure
  - Handle errors gracefully

#### 1.5 JavaScript Functions
- **`executeSQL(query)`:**
  - Uses `fetch` to POST to `/sql`
  - Returns Promise with response
  - Handles errors (network, HTTP errors)
- **`displayResult(response)`:**
  - Parses `SQLResponse` JSON
  - If `success: true`, parse `data` (handle array of strings for SELECT)
  - If `success: false`, display error message
  - Format pipe-delimited strings nicely
- **`formatPipeDelimited(data)`:**
  - If data is array of strings, split by `|`
  - Display in table or formatted text
  - Map columns: ID, Active Flag, Merchant, Amount, Checksum

### Task 2: Update `main.go`

#### 2.1 Add Root Handler
- **Function:** `handleIndex(w http.ResponseWriter, r *http.Request)`
- **Logic:**
  - Use `http.ServeFile(w, r, "web/index.html")`
  - Set `Content-Type: text/html`
  - Handle file not found errors gracefully
- **Route:** `http.HandleFunc("/", handleIndex)`

#### 2.2 File Structure
- Ensure `web/` directory exists
- Consider using `embed` package (Go 1.16+) for embedding HTML, or simple file serving
- For simplicity, use `http.ServeFile` with relative path

### Task 3: Demo Mode Script

#### 3.1 Demo Button
- **Location:** Top-right corner or floating button
- **Text:** "Run Demo"
- **Styling:** Distinctive (different color, maybe pulsing animation)

#### 3.2 Demo Sequence
- **Function:** `runDemo()`
- **Steps:**
  1. Display "Initializing system..." in results
  2. Execute `CREATE TABLE transactions (id int, merchant text, amount int)`
  3. Wait for response, display result
  4. Insert transaction 1: `INSERT INTO transactions VALUES (101, Starbucks, 550)`
  5. Insert transaction 2: `INSERT INTO transactions VALUES (102, Uber, 1200)`
  6. Insert transaction 3: `INSERT INTO transactions VALUES (103, Netflix, 999)`
  7. After each insert, display success message
  8. Query transaction 101: `SELECT * FROM transactions WHERE id = 101`
  9. Display formatted result
- **Error Handling:** If any step fails, stop and show error
- **Timing:** Use `setTimeout` or `async/await` with delays (500ms) between steps for visibility

#### 3.3 Demo Output Formatting
- Show each step's SQL query
- Show response (success/error)
- Format final SELECT result nicely

## File Structure
```
pesapal-ledger/
├── web/
│   └── index.html          # Single HTML file with inline CSS/JS
├── main.go                 # Updated with root handler
└── ...
```

## Technical Considerations

### Response Parsing
- SELECT queries return `data` as `[]string` (pipe-delimited)
- Need to split by `|` and format
- Example: `["101|1|Starbucks|550|abc123"]` -> Display as table

### Error Handling
- Network errors: Show alert or inline message
- SQL errors: Display error message from `response.error`
- Validation: Check inputs before sending (ID must be number, etc.)

### CSS Approach
- Inline `<style>` tag in HTML head
- No external dependencies (or minimal CDN if needed)
- Terminal aesthetic: glitch effects, neon glow, monospace

## Example HTML Structure
```html
<!DOCTYPE html>
<html>
<head>
  <title>LiteLedger // Merchant Terminal</title>
  <style>
    /* Dark terminal CSS */
  </style>
</head>
<body>
  <header>LiteLedger // Merchant Terminal</header>
  <div class="container">
    <div class="left-column">
      <button id="init-btn">Initialize System</button>
      <form id="transaction-form">...</form>
    </div>
    <div class="right-column">
      <div id="audit-section">...</div>
      <div id="results">...</div>
    </div>
  </div>
  <button id="demo-btn">Run Demo</button>
  <script>
    // All JavaScript here
  </script>
</body>
</html>
```

## Testing Checklist
- [ ] HTML loads at `http://localhost:8080/`
- [ ] Initialize System creates table
- [ ] Record Transaction inserts data
- [ ] Audit Transaction queries and displays results
- [ ] Demo mode runs all steps successfully
- [ ] Error messages display correctly
- [ ] Pipe-delimited data formats nicely
