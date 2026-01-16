# Implementation Roadmap

## Phase 1: The Engine (Critical Path)
- [ ] **Structs:** Define `Table`, `Row`, and `Database` structs.
- [ ] **Storage:** Implement `AppendRow(table, data)` function.
- [ ] **Loader:** Implement `LoadIndex(table)` to build the map on startup.
- [ ] **Query:** Implement `FindByID(table, id)` using `Seek()`.

## Phase 2: The Interface
- [ ] **Parser:** specific `ParseCommand(sql string)` function.
- [ ] **REPL:** Build the `main.go` loop to accept stdin input.
- [ ] **Joins:** Implement simple Nested-Loop Join logic.

## Phase 3: The Web App
- [ ] **Server:** Setup `http.HandleFunc`.
- [ ] **API:** connect `POST /query` to the Engine.
- [ ] **UI:** Simple HTML template for "Merchant Dashboard".

## Phase 4: Polish
- [ ] **Readme:** Write documentation explaining the "Fintech Ledger" philosophy.
- [ ] **Demo Script:** Create a `.sql` file with demo commands.