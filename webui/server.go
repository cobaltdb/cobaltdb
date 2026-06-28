package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
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
	apiTokenHash [sha256.Size]byte
	apiTokenSet  bool
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

const (
	authCookieName = "cobaltdb_webui_token"

	maxWebUIJSONBodyBytes = 1 << 20
	maxWebUIImportBytes   = 10 << 20
	maxWebUIHeaderBytes   = 1 << 20
	maxWebUITokenBytes    = 1024
	maxWebUIResultRows    = 1000
	maxWebUIQueryBytes    = 10000
	maxWebUISavedQueries  = 1000
	maxWebUISavedName     = 256
	maxWebUISavedQuery    = 10000
	maxWebUISavedDesc     = 2048
	maxWebUIIdentifier    = 256
	maxWebUIWhereTerms    = 64
)

// toUpperFast returns an uppercased copy of s only if s contains lowercase
// letters. This avoids an allocation when s is already uppercase.
func toUpperFast(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			return strings.ToUpper(s)
		}
	}
	return s
}

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

	if *insecureNoAuth {
		log.Fatalf("FATAL: --insecure-no-auth is set; the Web UI will accept connections without authentication. " +
			"This is unsafe and must not be used in production. " +
			"Either remove the flag or use COBALTDB_WEBUI_TOKEN to set a token.")
	}

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

	server := &Server{
		db:           db,
		history:      make([]QueryRecord, 0),
		savedQueries: make(map[string]SavedQuery),
		authEnabled:  authEnabled,
	}
	server.setAPIToken(apiToken)

	// Load templates
	tmpl, err := template.ParseFiles("webui/templates/index.html")
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Failed to close database: %v", closeErr)
		}
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
		// Show only first 8 chars to avoid full token in logs/shell history
		maskedToken := apiToken
		if len(apiToken) > 8 {
			maskedToken = apiToken[:8] + "..."
		}
		fmt.Printf("Open http://%s/?token=%s in your browser\n", *addr, maskedToken)
		fmt.Printf("Tip: token query parameter is converted to an HttpOnly cookie automatically\n")
	} else {
		fmt.Printf("Token auth: DISABLED (unsafe)\n")
		fmt.Printf("Open http://%s in your browser\n", *addr)
	}

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    maxWebUIHeaderBytes,
	}
	if err := httpServer.ListenAndServe(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Failed to close database: %v", closeErr)
		}
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

func (s *Server) setAPIToken(token string) {
	token = strings.TrimSpace(token)
	if token == "" || len(token) > maxWebUITokenBytes {
		s.apiTokenHash = [sha256.Size]byte{}
		s.apiTokenSet = false
		return
	}
	s.apiTokenHash = sha256.Sum256([]byte(token))
	s.apiTokenSet = true
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authEnabled {
			next.ServeHTTP(w, r)
			return
		}

		token := s.extractToken(r)
		if !s.secureTokenCompare(token) {
			s.writeUnauthorized(w, r)
			return
		}

		queryToken := strings.TrimSpace(r.URL.Query().Get("token"))
		if s.secureTokenCompare(queryToken) {
			// #nosec G124 -- Secure is set for HTTPS and HTTPS proxy requests; plain HTTP local UI needs a usable cookie.
			http.SetCookie(w, &http.Cookie{
				Name:     authCookieName,
				Value:    queryToken,
				Path:     "/",
				Secure:   requestUsesHTTPS(r),
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
			})

			// Strip token from browser URL after first successful auth.
			if r.Method == http.MethodGet && !strings.HasPrefix(r.URL.Path, "/api/") {
				cleanURL := *r.URL
				query := cleanURL.Query()
				query.Del("token")
				cleanURL.RawQuery = query.Encode()
				http.Redirect(w, r, "/", http.StatusFound)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func requestUsesHTTPS(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")), "https")
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

func (s *Server) secureTokenCompare(token string) bool {
	token = strings.TrimSpace(token)
	if !s.apiTokenSet || token == "" || len(token) > maxWebUITokenBytes {
		return false
	}
	tokenHash := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(tokenHash[:], s.apiTokenHash[:]) == 1
}

func quoteSQLIdentifier(identifier string) (string, error) {
	if identifier == "" || strings.ContainsRune(identifier, 0) {
		return "", fmt.Errorf("identifier must be non-empty and cannot contain NUL")
	}
	if len(identifier) > maxWebUIIdentifier {
		return "", fmt.Errorf("identifier too large")
	}
	escaped := strings.ReplaceAll(identifier, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return `"` + escaped + `"`, nil
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
	if !decodeJSONRequest(w, r, &req) {
		return
	}

	query := strings.TrimSpace(req.Query)
	if err := validateWebUIQuery(query); err != nil {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(QueryResponse{
			Success: false,
			Message: err.Error(),
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	start := time.Now()
	ctx := context.Background()

	// Determine if it's a SELECT query
	upperQuery := toUpperFast(query)
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
			defer rows.Close()
			// Get column names
			resp.Columns = rows.Columns()

			// Fetch rows
			rowCount := 0
			for rows.Next() {
				if rowCount >= maxWebUIResultRows {
					resp.Message = fmt.Sprintf("Result truncated to first %d rows", maxWebUIResultRows)
					break
				}
				// Create slice to hold values
				values := make([]interface{}, len(resp.Columns))
				valuePtrs := make([]interface{}, len(resp.Columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					resp.Success = false
					resp.Message = err.Error()
					break
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
		quotedTable, err := quoteSQLIdentifier(tableName)
		if err != nil {
			continue
		}
		rows, err := s.db.Query(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", quotedTable))
		if err == nil && rows != nil {
			cols := rows.Columns()
			for _, col := range cols {
				tableInfo.Columns = append(tableInfo.Columns, ColumnInfo{
					Name: col,
					Type: "TEXT", // Default type
				})
			}
			if err := rows.Close(); err != nil {
				continue
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
	s.mu.RLock()
	history := append([]QueryRecord(nil), s.history...)
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, history)
}

func (s *Server) handleTableInfo(w http.ResponseWriter, r *http.Request) {
	tableName := strings.TrimPrefix(r.URL.Path, "/api/tables/")
	if tableName == "" {
		http.Error(w, "Table name required", http.StatusBadRequest)
		return
	}

	tableName, _ = url.QueryUnescape(tableName)
	quotedTable, err := quoteSQLIdentifier(tableName)
	if err != nil {
		http.Error(w, "invalid table name", http.StatusBadRequest)
		return
	}

	// Get column info by querying
	ctx := context.Background()
	rows, err := s.db.Query(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", quotedTable))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	cols := rows.Columns()
	if err := rows.Close(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
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

// sanitizeCSVField neutralizes CSV/spreadsheet formula injection (CWE-1236).
// A cell whose first character is one of =, +, -, @, or a control character is
// interpreted as a formula by Excel/LibreOffice/Sheets; prefix it with a single
// quote so it is treated as literal text on import.
func sanitizeCSVField(s string) string {
	if s == "" {
		return s
	}
	switch s[0] {
	case '=', '+', '-', '@', '\t', '\r', '\n':
		return "'" + s
	}
	return s
}

func (s *Server) handleExportCSV(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("query"))
	if err := validateWebUIQuery(query); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	columns := rows.Columns()
	exportRows, truncated, err := collectWebUIExportRows(rows, len(columns))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	headerRow := make([]string, len(columns))
	for i, col := range columns {
		headerRow[i] = sanitizeCSVField(col)
	}
	if err := writer.Write(headerRow); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, values := range exportRows {
		rowStr := make([]string, len(values))
		for i, v := range values {
			if v == nil {
				rowStr[i] = "NULL"
			} else {
				rowStr[i] = sanitizeCSVField(fmt.Sprintf("%v", v))
			}
		}
		if err := writer.Write(rowStr); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=export.csv")
	if truncated {
		w.Header().Set("X-CobaltDB-Truncated", "true")
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return
	}
}

func (s *Server) handleExportJSON(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("query"))
	if err := validateWebUIQuery(query); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	columns := rows.Columns()
	exportRows, truncated, err := collectWebUIExportRows(rows, len(columns))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonRows := make([]map[string]interface{}, 0, len(exportRows))
	for _, values := range exportRows {
		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		jsonRows = append(jsonRows, row)
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(jsonRows); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=export.json")
	if truncated {
		w.Header().Set("X-CobaltDB-Truncated", "true")
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return
	}
}

func collectWebUIExportRows(rows *engine.Rows, columnCount int) ([][]interface{}, bool, error) {
	exportRows := make([][]interface{}, 0, maxWebUIResultRows)
	truncated := false
	for rows.Next() {
		if len(exportRows) >= maxWebUIResultRows {
			truncated = true
			break
		}

		values := make([]interface{}, columnCount)
		valuePtrs := make([]interface{}, columnCount)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, false, err
		}
		exportRows = append(exportRows, values)
	}
	return exportRows, truncated, nil
}

func validateWebUIQuery(query string) error {
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("empty query")
	}
	if len(query) > maxWebUIQueryBytes {
		return fmt.Errorf("query too large")
	}
	return nil
}

func (s *Server) addToHistory(query, duration string, rows int) {
	record := QueryRecord{
		Query:     query,
		Timestamp: time.Now(),
		Duration:  duration,
		Rows:      rows,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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

func decodeJSONRequest(w http.ResponseWriter, r *http.Request, dst interface{}) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxWebUIJSONBodyBytes)
	if err := decodeSingleJSON(r.Body, dst); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return false
	}
	return true
}

func decodeSingleJSON(reader io.Reader, dst interface{}) error {
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("input must contain a single JSON document")
	}
	return nil
}

func decodeSavedQueriesImport(file io.Reader) ([]SavedQuery, error) {
	data, err := io.ReadAll(io.LimitReader(file, maxWebUIImportBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxWebUIImportBytes {
		return nil, fmt.Errorf("saved queries import too large")
	}

	var queries []SavedQuery
	if err := decodeSingleJSON(bytes.NewReader(data), &queries); err != nil {
		return nil, err
	}
	return queries, nil
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

		if !decodeJSONRequest(w, r, &req) {
			return
		}

		if req.Name == "" || req.Query == "" {
			http.Error(w, "Name and query are required", http.StatusBadRequest)
			return
		}
		sq := SavedQuery{
			Name:        req.Name,
			Query:       req.Query,
			Description: req.Description,
			CreatedAt:   time.Now(),
		}
		if err := validateSavedQuery(sq); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		s.mu.Lock()
		if s.savedQueries == nil {
			s.savedQueries = make(map[string]SavedQuery)
		}
		if _, exists := s.savedQueries[sq.Name]; !exists && len(s.savedQueries) >= maxWebUISavedQueries {
			s.mu.Unlock()
			http.Error(w, "too many saved queries", http.StatusBadRequest)
			return
		}
		s.savedQueries[sq.Name] = sq
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

	r.Body = http.MaxBytesReader(w, r.Body, maxWebUIImportBytes)

	// Parse multipart form
	// #nosec G120 -- MaxBytesReader above caps the request body before multipart parsing.
	if err := r.ParseMultipartForm(maxWebUIImportBytes); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to get file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	queries, err := decodeSavedQueriesImport(file)
	if err != nil {
		http.Error(w, "Invalid JSON file", http.StatusBadRequest)
		return
	}
	if len(queries) > maxWebUISavedQueries {
		http.Error(w, "too many saved queries", http.StatusBadRequest)
		return
	}
	now := time.Now()
	validated := make([]SavedQuery, 0, len(queries))
	for _, q := range queries {
		if q.Name == "" && q.Query == "" && q.Description == "" {
			continue
		}
		q.CreatedAt = now
		if err := validateSavedQuery(q); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		validated = append(validated, q)
	}

	s.mu.Lock()
	if s.savedQueries == nil {
		s.savedQueries = make(map[string]SavedQuery)
	}
	newNames := make(map[string]struct{}, len(validated))
	for _, q := range validated {
		if _, exists := s.savedQueries[q.Name]; !exists {
			newNames[q.Name] = struct{}{}
		}
	}
	if len(s.savedQueries)+len(newNames) > maxWebUISavedQueries {
		s.mu.Unlock()
		http.Error(w, "too many saved queries", http.StatusBadRequest)
		return
	}
	for _, q := range validated {
		s.savedQueries[q.Name] = q
	}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, map[string]string{"status": "imported", "count": fmt.Sprintf("%d", len(validated))})
}

func validateSavedQuery(q SavedQuery) error {
	if strings.TrimSpace(q.Name) == "" || strings.TrimSpace(q.Query) == "" {
		return fmt.Errorf("name and query are required")
	}
	if len(q.Name) > maxWebUISavedName {
		return fmt.Errorf("saved query name too large")
	}
	if len(q.Query) > maxWebUISavedQuery {
		return fmt.Errorf("saved query too large")
	}
	if len(q.Description) > maxWebUISavedDesc {
		return fmt.Errorf("saved query description too large")
	}
	return nil
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

	if !decodeJSONRequest(w, r, &req) {
		return
	}
	if len(req.Where) > maxWebUIWhereTerms {
		http.Error(w, "too many where terms", http.StatusBadRequest)
		return
	}

	// Build WHERE clause
	var whereClauses []string
	var args []interface{}
	quotedTable, err := quoteSQLIdentifier(req.Table)
	if err != nil {
		http.Error(w, "invalid table name", http.StatusBadRequest)
		return
	}
	quotedColumn, err := quoteSQLIdentifier(req.Column)
	if err != nil {
		http.Error(w, "invalid column name", http.StatusBadRequest)
		return
	}

	for col, val := range req.Where {
		quotedCol, err := quoteSQLIdentifier(col)
		if err != nil {
			http.Error(w, "invalid where column name", http.StatusBadRequest)
			return
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", quotedCol))
		args = append(args, val)
	}
	if len(whereClauses) == 0 {
		http.Error(w, "where clause required", http.StatusBadRequest)
		return
	}

	whereStr := strings.Join(whereClauses, " AND ")

	// Build UPDATE query
	query := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s",
		quotedTable, quotedColumn, whereStr)

	// Add the new value as first arg
	args = append([]interface{}{req.Value}, args...)

	// Execute query
	ctx := context.Background()
	_, err = s.db.Exec(ctx, query, args...)

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
