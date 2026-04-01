package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// Server holds the web UI server state
type Server struct {
	db           *engine.DB
	history      []QueryRecord
	savedQueries map[string]SavedQuery
	tmpl         *template.Template
	apiToken     string
	authEnabled  bool
	mu           sync.RWMutex
}

// SavedQuery represents a saved query
type SavedQuery struct {
	Name        string    `json:"name"`
	Query       string    `json:"query"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
}

// QueryRecord represents a query in history
type QueryRecord struct {
	Query     string    `json:"query"`
	Timestamp time.Time `json:"timestamp"`
	Duration  string    `json:"duration"`
	Rows      int       `json:"rows"`
}

// QueryRequest represents a query request
type QueryRequest struct {
	Query string `json:"query"`
}

// QueryResponse represents a query response
type QueryResponse struct {
	Success  bool            `json:"success"`
	Message  string          `json:"message,omitempty"`
	Columns  []string        `json:"columns,omitempty"`
	Rows     [][]interface{} `json:"rows,omitempty"`
	Duration string          `json:"duration"`
	RowCount int             `json:"rowCount"`
}

// SchemaInfo represents database schema
type SchemaInfo struct {
	Tables []TableInfo `json:"tables"`
}

// TableInfo represents table information
type TableInfo struct {
	Name    string       `json:"name"`
	Columns []ColumnInfo `json:"columns"`
}

// ColumnInfo represents column information
type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

const authCookieName = "cobaltdb_webui_token"

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	tokenFlag := flag.String("token", "", "Web UI access token (defaults to COBALTDB_WEBUI_TOKEN or a generated token)")
	insecureNoAuth := flag.Bool("insecure-no-auth", false, "disable token auth (unsafe; for trusted local development only)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: webui [flags] <database_file>\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nExample: webui -addr 127.0.0.1:8080 mydb.db\n")
	}
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	dbPath := flag.Arg(0)

	apiToken := strings.TrimSpace(*tokenFlag)
	if apiToken == "" {
		apiToken = strings.TrimSpace(os.Getenv("COBALTDB_WEBUI_TOKEN"))
	}
	authEnabled := !*insecureNoAuth
	if authEnabled && apiToken == "" {
		generatedToken, err := generateToken(24)
		if err != nil {
			log.Fatalf("Failed to generate access token: %v", err)
		}
		apiToken = generatedToken
	}

	// Open database
	db, err := engine.Open(dbPath, nil)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	server := &Server{
		db:           db,
		history:      make([]QueryRecord, 0),
		savedQueries: make(map[string]SavedQuery),
		apiToken:     apiToken,
		authEnabled:  authEnabled,
	}

	// Load templates
	tmpl, err := template.ParseFiles("webui/templates/index.html")
	if err != nil {
		log.Fatalf("Failed to load templates: %v", err)
	}
	server.tmpl = tmpl

	// Setup routes
	mux := http.NewServeMux()
	fs := http.FileServer(http.Dir("webui/static"))
	mux.Handle("/static/", http.StripPrefix("/static/", fs))

	mux.HandleFunc("/", server.handleIndex)
	mux.HandleFunc("/api/query", server.handleQuery)
	mux.HandleFunc("/api/schema", server.handleSchema)
	mux.HandleFunc("/api/history", server.handleHistory)
	mux.HandleFunc("/api/tables/", server.handleTableInfo)
	mux.HandleFunc("/api/export/csv", server.handleExportCSV)
	mux.HandleFunc("/api/export/json", server.handleExportJSON)
	mux.HandleFunc("/api/saved-queries", server.handleSavedQueries)
	mux.HandleFunc("/api/saved-queries/", server.handleSavedQuery)
	mux.HandleFunc("/api/export-saved-queries", server.handleExportSavedQueries)
	mux.HandleFunc("/api/import-saved-queries", server.handleImportSavedQueries)
	mux.HandleFunc("/api/update-row", server.handleUpdateRow)

	handler := http.Handler(mux)
	if authEnabled {
		handler = server.authMiddleware(handler)
	}

	fmt.Printf("CobaltDB Web UI starting...\n")
	fmt.Printf("Database: %s\n", dbPath)
	fmt.Printf("Bind: %s\n", *addr)
	if authEnabled {
		fmt.Printf("Token auth: enabled\n")
		fmt.Printf("Open http://%s/?token=%s in your browser\n", *addr, apiToken)
		fmt.Printf("Tip: token query parameter is converted to an HttpOnly cookie automatically\n")
	} else {
		fmt.Printf("Token auth: DISABLED (unsafe)\n")
		fmt.Printf("Open http://%s in your browser\n", *addr)
	}

	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func generateToken(size int) (string, error) {
	tokenBytes := make([]byte, size)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("failed to generate random token: %w", err)
	}
	return hex.EncodeToString(tokenBytes), nil
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authEnabled {
			next.ServeHTTP(w, r)
			return
		}

		token := s.extractToken(r)
		if !secureTokenCompare(token, s.apiToken) {
			s.writeUnauthorized(w, r)
			return
		}

		queryToken := strings.TrimSpace(r.URL.Query().Get("token"))
		if secureTokenCompare(queryToken, s.apiToken) {
			http.SetCookie(w, &http.Cookie{
				Name:     authCookieName,
				Value:    s.apiToken,
				Path:     "/",
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
			})

			// Strip token from browser URL after first successful auth.
			if r.Method == http.MethodGet && !strings.HasPrefix(r.URL.Path, "/api/") {
				cleanURL := *r.URL
				query := cleanURL.Query()
				query.Del("token")
				cleanURL.RawQuery = query.Encode()
				http.Redirect(w, r, cleanURL.String(), http.StatusFound)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) extractToken(r *http.Request) string {
	if authHeader := strings.TrimSpace(r.Header.Get("Authorization")); authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return strings.TrimSpace(parts[1])
		}
	}

	if headerToken := strings.TrimSpace(r.Header.Get("X-CobaltDB-Token")); headerToken != "" {
		return headerToken
	}

	if cookie, err := r.Cookie(authCookieName); err == nil {
		if cookieToken := strings.TrimSpace(cookie.Value); cookieToken != "" {
			return cookieToken
		}
	}

	return strings.TrimSpace(r.URL.Query().Get("token"))
}

func secureTokenCompare(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (s *Server) writeUnauthorized(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Unauthorized: provide token using query param, Authorization bearer token, or X-CobaltDB-Token header",
		})
		return
	}
	http.Error(w, "Unauthorized: open this URL with ?token=<token>", http.StatusUnauthorized)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	data := struct {
		DatabasePath string
		Version      string
	}{
		DatabasePath: s.db.Path(),
		Version:      "0.3.0",
	}

	if err := s.tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(QueryResponse{
			Success: false,
			Message: "Empty query",
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	start := time.Now()
	ctx := context.Background()

	// Determine if it's a SELECT query
	upperQuery := strings.ToUpper(query)
	isSelect := strings.HasPrefix(upperQuery, "SELECT") ||
		strings.HasPrefix(upperQuery, "WITH") ||
		strings.HasPrefix(upperQuery, "SHOW") ||
		strings.HasPrefix(upperQuery, "DESCRIBE") ||
		strings.HasPrefix(upperQuery, "EXPLAIN")

	resp := QueryResponse{Success: true}

	if isSelect {
		// Execute SELECT query
		rows, err := s.db.Query(ctx, query)
		duration := time.Since(start)
		resp.Duration = formatDuration(duration)

		if err != nil {
			resp.Success = false
			resp.Message = err.Error()
		} else if rows != nil {
			// Get column names
			resp.Columns = rows.Columns()

			// Fetch rows
			rowCount := 0
			for rows.Next() {
				// Create slice to hold values
				values := make([]interface{}, len(resp.Columns))
				valuePtrs := make([]interface{}, len(resp.Columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					continue
				}
				resp.Rows = append(resp.Rows, values)
				rowCount++
			}
			resp.RowCount = rowCount
		}
	} else {
		// Execute non-SELECT query
		_, err := s.db.Exec(ctx, query)
		duration := time.Since(start)
		resp.Duration = formatDuration(duration)

		if err != nil {
			resp.Success = false
			resp.Message = err.Error()
		} else {
			// Return affected rows info
			resp.Columns = []string{"Result"}
			resp.Rows = [][]interface{}{{"Query executed successfully"}}
			resp.RowCount = 1
		}
	}

	// Add to history
	if resp.Success {
		s.addToHistory(query, resp.Duration, resp.RowCount)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	tables := s.db.Tables()

	schema := SchemaInfo{
		Tables: make([]TableInfo, 0, len(tables)),
	}

	for _, tableName := range tables {
		tableInfo := TableInfo{Name: tableName}

		// Try to get column info by querying
		ctx := context.Background()
		rows, err := s.db.Query(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName))
		if err == nil && rows != nil {
			cols := rows.Columns()
			for _, col := range cols {
				tableInfo.Columns = append(tableInfo.Columns, ColumnInfo{
					Name: col,
					Type: "TEXT", // Default type
				})
			}
		}

		schema.Tables = append(schema.Tables, tableInfo)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(schema); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, s.history)
}

func (s *Server) handleTableInfo(w http.ResponseWriter, r *http.Request) {
	tableName := strings.TrimPrefix(r.URL.Path, "/api/tables/")
	if tableName == "" {
		http.Error(w, "Table name required", http.StatusBadRequest)
		return
	}

	tableName, _ = url.QueryUnescape(tableName)

	// Get column info by querying
	ctx := context.Background()
	rows, err := s.db.Query(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	cols := rows.Columns()
	tableInfo := TableInfo{
		Name:    tableName,
		Columns: make([]ColumnInfo, 0, len(cols)),
	}

	for _, col := range cols {
		tableInfo.Columns = append(tableInfo.Columns, ColumnInfo{
			Name: col,
			Type: "TEXT",
		})
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, tableInfo)
}

func (s *Server) handleExportCSV(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	if query == "" {
		http.Error(w, "Query required", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	columns := rows.Columns()

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=export.csv")

	writer := csv.NewWriter(w)
	if err := writer.Write(columns); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		rowStr := make([]string, len(values))
		for i, v := range values {
			if v == nil {
				rowStr[i] = "NULL"
			} else {
				rowStr[i] = fmt.Sprintf("%v", v)
			}
		}
		if err := writer.Write(rowStr); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	writer.Flush()
}

func (s *Server) handleExportJSON(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	if query == "" {
		http.Error(w, "Query required", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	columns := rows.Columns()
	result := make([]map[string]interface{}, 0)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=export.json")
	writeJSON(w, result)
}

func (s *Server) addToHistory(query, duration string, rows int) {
	record := QueryRecord{
		Query:     query,
		Timestamp: time.Now(),
		Duration:  duration,
		Rows:      rows,
	}

	s.history = append([]QueryRecord{record}, s.history...)

	// Keep only last 100 queries
	if len(s.history) > 100 {
		s.history = s.history[:100]
	}
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%d μs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%.2f ms", float64(d.Microseconds())/1000)
	}
	return fmt.Sprintf("%.2f s", d.Seconds())
}

func writeJSON(w http.ResponseWriter, payload interface{}) {
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleSavedQueries handles GET and POST for saved queries
func (s *Server) handleSavedQueries(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		queries := make([]SavedQuery, 0, len(s.savedQueries))
		for _, q := range s.savedQueries {
			queries = append(queries, q)
		}
		s.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, queries)

	case http.MethodPost:
		var req struct {
			Name        string `json:"name"`
			Query       string `json:"query"`
			Description string `json:"description"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.Name == "" || req.Query == "" {
			http.Error(w, "Name and query are required", http.StatusBadRequest)
			return
		}

		s.mu.Lock()
		s.savedQueries[req.Name] = SavedQuery{
			Name:        req.Name,
			Query:       req.Query,
			Description: req.Description,
			CreatedAt:   time.Now(),
		}
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]string{"status": "saved"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleSavedQuery handles individual saved query operations (GET, DELETE)
func (s *Server) handleSavedQuery(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/saved-queries/")
	if name == "" {
		http.Error(w, "Query name required", http.StatusBadRequest)
		return
	}

	name, _ = url.QueryUnescape(name)

	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		query, exists := s.savedQueries[name]
		s.mu.RUnlock()

		if !exists {
			http.Error(w, "Query not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, query)

	case http.MethodDelete:
		s.mu.Lock()
		delete(s.savedQueries, name)
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleExportSavedQueries exports all saved queries as JSON file
func (s *Server) handleExportSavedQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	queries := make([]SavedQuery, 0, len(s.savedQueries))
	for _, q := range s.savedQueries {
		queries = append(queries, q)
	}
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=saved-queries.json")
	writeJSON(w, queries)
}

// handleImportSavedQueries imports saved queries from JSON file
func (s *Server) handleImportSavedQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse multipart form
	if err := r.ParseMultipartForm(10 << 20); err != nil { // 10 MB max
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to get file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	var queries []SavedQuery
	if err := json.NewDecoder(file).Decode(&queries); err != nil {
		http.Error(w, "Invalid JSON file", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	for _, q := range queries {
		if q.Name != "" && q.Query != "" {
			q.CreatedAt = time.Now()
			s.savedQueries[q.Name] = q
		}
	}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, map[string]string{"status": "imported", "count": fmt.Sprintf("%d", len(queries))})
}

// handleUpdateRow handles updating a specific row in a table
func (s *Server) handleUpdateRow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Table  string                 `json:"table"`
		Column string                 `json:"column"`
		Value  interface{}            `json:"value"`
		Where  map[string]interface{} `json:"where"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build WHERE clause
	var whereClauses []string
	var args []interface{}

	for col, val := range req.Where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", col))
		args = append(args, val)
	}

	whereStr := strings.Join(whereClauses, " AND ")

	// Build UPDATE query
	query := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s",
		req.Table, req.Column, whereStr)

	// Add the new value as first arg
	args = append([]interface{}{req.Value}, args...)

	// Execute query
	ctx := context.Background()
	_, err := s.db.Exec(ctx, query, args...)

	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, map[string]interface{}{
		"success": true,
	})
}
