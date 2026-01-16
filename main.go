package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"pesapal-ledger/engine"
	"pesapal-ledger/parser"
)

// Server holds dependencies for the HTTP handlers
type Server struct {
	db *engine.Database
}

// SQLRequest represents the expected JSON request body
type SQLRequest struct {
	Query string `json:"query"`
}

// SQLResponse represents the standard JSON response format
type SQLResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// handleIndex serves the main web interface
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	// Serve the static HTML file
	http.ServeFile(w, r, "web/index.html")
}

// handleSQL processes the SQL query requests
func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request) {
	// Only allow POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SQLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(SQLResponse{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	if req.Query == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(SQLResponse{
			Success: false,
			Error:   "Query cannot be empty",
		})
		return
	}

	// Process the query using the real parser
	result, err := parser.ParseSQL(req.Query, s.db)
	
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		// Distinguish between client errors (syntax) and server errors?
		// For now, 400 for errors like "invalid syntax" could be better, 
		// but generic 500 or 400 is fine for MVP.
		// Let's use 400 Bad Request if it's a parsing error, but parser returns generic error.
		// We'll stick to 200 with success=false or 500. 
		// The previous implementation used 500. Let's use 500 for now.
		w.WriteHeader(http.StatusOK) // Or 500? Client expects JSON. 
		// Actually, returning 200 with Success: false is often easier for clients to parse JSON error.
		// But let's follow the previous pattern: WriteHeader then Encode.
		// If I write 500, I can still write JSON body.
		w.WriteHeader(http.StatusBadRequest) // Assume most errors are bad queries
		json.NewEncoder(w).Encode(SQLResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	// Return success response
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(SQLResponse{
		Success: true,
		Data:    result,
	})
}

func main() {
	fmt.Println("Starting LiteLedger...")

	// Initialize the database engine
	db := engine.NewDatabase()
	
	// Recover database state from disk
	if err := db.Recover(); err != nil {
		// Log error but continue (start fresh if recovery fails completely)
		fmt.Printf("Warning: Database recovery issues: %v\n", err)
	} else {
		fmt.Println("Database recovered successfully.")
	}
	
	// Create server instance
	server := &Server{
		db: db,
	}

	
	fmt.Println("LiteLedger Engine Initialized.")
	
	// Setup HTTP routes
	http.HandleFunc("/", server.handleIndex)
	http.HandleFunc("/sql", server.handleSQL)
	
	// Start HTTP server
	port := ":8080"
	fmt.Printf("Starting HTTP server on %s\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
