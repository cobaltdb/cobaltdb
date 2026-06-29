package main

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"strconv"
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
	tokens       *tokenStore
	limiter      *rateLimiter
	audit        *auditLog
	tokenTTL     time.Duration
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

	maxWebUIJSONBodyBytes   = 1 << 20
	maxWebUIImportBytes     = 10 << 20
	maxWebUIHeaderBytes     = 1 << 20
	maxWebUITokenBytes      = 1024
	maxWebUIResultRows      = 1000
	maxWebUIQueryBytes      = 10000
	maxWebUISavedQueries    = 1000
	maxWebUISavedName       = 256
	maxWebUISavedQuery      = 10000
	maxWebUISavedDesc       = 2048
	maxWebUIIdentifier      = 256
	maxWebUIWhereTerms      = 64
	maxWebUITokenName       = 256
	maxWebUIAllowListTables = 256

	// Defaults for the hardening subsystems (overridable via flags).
	defaultWebUITokenTTL     = 24 * time.Hour
	defaultWebUIRatePerMin   = 120
	defaultWebUIRateBurst    = 30
	webUIAuditRingSize       = 1000
	webUIAuditMaxSQL         = 4096
	maxWebUIAdminMintTokens  = 256
	tokenExpirySweepInterval = 5 * time.Minute
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
	tokenFlag := flag.String("token", "", "Web UI bootstrap admin token (defaults to COBALTDB_WEBUI_TOKEN or a generated token)")
	insecureNoAuth := flag.Bool("insecure-no-auth", false, "disable token auth (unsafe; for trusted local development only)")
	tokenTTL := flag.Duration("token-ttl", defaultWebUITokenTTL, "lifetime of minted tokens (0 = no expiry); the bootstrap token never expires")
	ratePerMin := flag.Int("rate-limit", defaultWebUIRatePerMin, "max API requests per principal per minute (0 = unlimited)")
	rateBurst := flag.Int("rate-burst", defaultWebUIRateBurst, "burst allowance for the per-principal rate limiter")
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
		tokens:       newTokenStore(),
		limiter:      newRateLimiter(*ratePerMin, *rateBurst),
		audit:        newAuditLog(os.Stderr, webUIAuditRingSize, webUIAuditMaxSQL),
		authEnabled:  authEnabled,
	}
	// The bootstrap token is an admin credential that never expires, so the
	// operator is never locked out of token management.
	server.tokens.setBootstrap(apiToken)

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
	// Current principal — lets the UI show admin-only controls.
	mux.HandleFunc("/api/me", server.handleMe)
	// Admin-only token management + audit (RBAC enforced inside the handlers).
	mux.HandleFunc("/api/admin/tokens", server.handleAdminTokens)
	mux.HandleFunc("/api/admin/tokens/", server.handleAdminToken)
	mux.HandleFunc("/api/admin/audit", server.handleAdminAudit)

	handler := http.Handler(mux)
	if authEnabled {
		handler = server.authMiddleware(handler)
		server.startTokenExpirySweeper()
	}

	fmt.Printf("CobaltDB Web UI starting...\n")
	fmt.Printf("Database: %s\n", dbPath)
	fmt.Printf("Bind: %s\n", *addr)
	if authEnabled {
		fmt.Printf("Token auth: enabled (bootstrap admin token; mint scoped tokens via /api/admin/tokens)\n")
		if server.limiter.enabled() {
			fmt.Printf("Rate limit: %d req/min per principal (burst %d)\n", *ratePerMin, *rateBurst)
		}
		if *tokenTTL > 0 {
			fmt.Printf("Minted-token TTL: %s\n", *tokenTTL)
		}
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
	server.tokenTTL = *tokenTTL

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

// startTokenExpirySweeper periodically purges expired tokens so the store does
// not accumulate dead records over a long-lived server.
func (s *Server) startTokenExpirySweeper() {
	if s.tokens == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(tokenExpirySweepInterval)
		defer ticker.Stop()
		for range ticker.C {
			s.tokens.purgeExpired()
		}
	}()
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authEnabled {
			next.ServeHTTP(w, r)
			return
		}

		token := s.extractToken(r)
		p, ok := s.tokens.resolve(token)
		if !ok {
			s.recordAudit(r, principal{}, classRead, "", "denied", "unauthorized")
			s.writeUnauthorized(w, r)
			return
		}

		// Per-principal rate limiting (keyed by token ID, not raw token).
		if !s.limiter.allow(p.ID) {
			s.recordAudit(r, p, classRead, "", "denied", "rate limited")
			s.writeRateLimited(w, r)
			return
		}

		// Convert a query-string token into an HttpOnly cookie and strip it from
		// the browser URL on the first interactive load.
		queryToken := strings.TrimSpace(r.URL.Query().Get("token"))
		if queryToken != "" && subtleConstantEq(queryToken, token) {
			// #nosec G124 -- Secure is set for HTTPS and HTTPS proxy requests; plain HTTP local UI needs a usable cookie.
			http.SetCookie(w, &http.Cookie{
				Name:     authCookieName,
				Value:    queryToken,
				Path:     "/",
				Secure:   requestUsesHTTPS(r),
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
			})

			if r.Method == http.MethodGet && !strings.HasPrefix(r.URL.Path, "/api/") {
				cleanURL := *r.URL
				query := cleanURL.Query()
				query.Del("token")
				cleanURL.RawQuery = query.Encode()
				http.Redirect(w, r, "/", http.StatusFound)
				return
			}
		}

		next.ServeHTTP(w, withPrincipal(r, p))
	})
}

// subtleConstantEq compares two strings in constant time.
func subtleConstantEq(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// effectivePrincipal returns the principal stored by the middleware, or — when
// auth is disabled — a synthetic admin so handlers behave uniformly.
func (s *Server) effectivePrincipal(r *http.Request) principal {
	if p, ok := principalFromRequest(r); ok {
		return p
	}
	return principal{ID: "anonymous", Name: "anonymous", Role: RoleAdmin}
}

// recordAudit emits one audit event if auditing is configured.
func (s *Server) recordAudit(r *http.Request, p principal, class queryClass, sql, outcome, detail string) {
	if s.audit == nil {
		return
	}
	ev := auditEvent{
		PrincipalID: p.ID,
		Principal:   p.Name,
		Role:        p.Role,
		RemoteAddr:  clientIP(r),
		Method:      r.Method,
		Path:        r.URL.Path,
		Outcome:     outcome,
		Detail:      detail,
	}
	if sql != "" {
		ev.QueryClass = class.String()
		ev.SQL = sql
	}
	s.audit.record(ev)
}

// authorizeQueryReason enforces, in order: RBAC by statement class, then (for
// non-admin tokens carrying a table allow-list) that every base table the
// statement touches is allow-listed. It returns a human-readable denial reason.
// Table extraction is fail-closed: a statement that cannot be parsed/understood
// for a restricted principal is denied.
func (s *Server) authorizeQueryReason(r *http.Request, sql string) (principal, queryClass, string, bool) {
	p := s.effectivePrincipal(r)
	class := classifyQuery(sql)
	if !p.Role.allows(class) {
		return p, class, "insufficient role", false
	}
	if !p.tableRestricted() {
		return p, class, "", true
	}
	tables, err := extractTableRefs(sql)
	if err != nil {
		// Fail closed: if we cannot determine which tables are touched, a
		// table-restricted principal must not run the statement.
		return p, class, "statement not permitted under table allow-list", false
	}
	for _, t := range tables {
		if !p.allowsTable(t) {
			return p, class, fmt.Sprintf("table %q is not in this token's allow-list", t), false
		}
	}
	return p, class, "", true
}

func clientIP(r *http.Request) string {
	if r == nil {
		return ""
	}
	if fwd := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); fwd != "" {
		if i := strings.IndexByte(fwd, ','); i >= 0 {
			return strings.TrimSpace(fwd[:i])
		}
		return fwd
	}
	return r.RemoteAddr
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

// setAPIToken installs a single bootstrap admin token. Retained as a
// convenience for tests and the legacy single-token configuration path; the
// store underneath supports the full multi-token RBAC model.
func (s *Server) setAPIToken(token string) {
	if s.tokens == nil {
		s.tokens = newTokenStore()
	}
	s.tokens.setBootstrap(token)
}

// secureTokenCompare reports whether the raw token authenticates as any
// non-expired principal. Kept for backward compatibility with existing tests.
func (s *Server) secureTokenCompare(token string) bool {
	if s.tokens == nil {
		return false
	}
	_, ok := s.tokens.resolve(token)
	return ok
}

func (s *Server) writeRateLimited(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Retry-After", "1")
	if strings.HasPrefix(r.URL.Path, "/api/") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Rate limit exceeded; slow down and retry",
		})
		return
	}
	http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
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

	// RBAC + table allow-list: enforce the principal's role and table scope.
	principal, class, reason, allowed := s.authorizeQueryReason(r, query)
	if !allowed {
		s.recordAudit(r, principal, class, query, "denied", reason)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		if err := json.NewEncoder(w).Encode(QueryResponse{
			Success: false,
			Message: "Forbidden: " + reason,
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
		s.recordAudit(r, principal, class, query, "allowed", "")
	} else {
		s.recordAudit(r, principal, class, query, "error", resp.Message)
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
	if p, class, reason, ok := s.authorizeQueryReason(r, query); !ok {
		s.recordAudit(r, p, class, query, "denied", reason)
		http.Error(w, "forbidden: "+reason, http.StatusForbidden)
		return
	} else {
		s.recordAudit(r, p, class, query, "allowed", "export csv")
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
	if p, class, reason, ok := s.authorizeQueryReason(r, query); !ok {
		s.recordAudit(r, p, class, query, "denied", reason)
		http.Error(w, "forbidden: "+reason, http.StatusForbidden)
		return
	} else {
		s.recordAudit(r, p, class, query, "allowed", "export json")
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

	// Inline editing always issues an UPDATE — require write privileges.
	p := s.effectivePrincipal(r)
	if !p.Role.allows(classWrite) {
		s.recordAudit(r, p, classWrite, "UPDATE (inline edit)", "denied", "insufficient role")
		http.Error(w, "forbidden", http.StatusForbidden)
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

	// Table allow-list: an inline edit targets exactly req.Table.
	if p.tableRestricted() && !p.allowsTable(req.Table) {
		s.recordAudit(r, p, classWrite, "UPDATE "+req.Table+" (inline edit)", "denied",
			fmt.Sprintf("table %q is not in this token's allow-list", strings.ToLower(strings.TrimSpace(req.Table))))
		http.Error(w, "forbidden: table not in allow-list", http.StatusForbidden)
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
		s.recordAudit(r, p, classWrite, query, "error", err.Error())
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	s.recordAudit(r, p, classWrite, query, "allowed", "inline edit")
	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, map[string]interface{}{
		"success": true,
	})
}

// handleMe reports the current principal so the UI can decide which controls to
// show (e.g. the admin token panel). It exposes only non-secret metadata.
func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p := s.effectivePrincipal(r)
	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, map[string]interface{}{
		"id":          p.ID,
		"name":        p.Name,
		"role":        p.Role,
		"isAdmin":     p.Role.isAdmin(),
		"authEnabled": s.authEnabled,
	})
}

// --- Admin: token management + audit log (RBAC: admin only) ---

// requireAdmin returns the principal if it has the admin role, else writes 403.
func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) (principal, bool) {
	p := s.effectivePrincipal(r)
	if !p.Role.isAdmin() {
		s.recordAudit(r, p, classRead, "", "denied", "admin required")
		http.Error(w, "forbidden: admin role required", http.StatusForbidden)
		return principal{}, false
	}
	return p, true
}

// handleAdminTokens lists tokens (GET) or mints a new one (POST).
func (s *Server) handleAdminTokens(w http.ResponseWriter, r *http.Request) {
	admin, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}

	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]interface{}{"tokens": s.tokens.list()})

	case http.MethodPost:
		var req struct {
			Name   string   `json:"name"`
			Role   string   `json:"role"`
			TTL    string   `json:"ttl"`    // optional Go duration string; empty = server default
			Tables []string `json:"tables"` // optional base-table allow-list; empty = unrestricted
		}
		if !decodeJSONRequest(w, r, &req) {
			return
		}
		role, err := parseRole(req.Role)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(req.Name) == "" || len(req.Name) > maxWebUITokenName {
			http.Error(w, "token name required (1..256 bytes)", http.StatusBadRequest)
			return
		}
		ttl := s.tokenTTL
		if strings.TrimSpace(req.TTL) != "" {
			parsed, perr := time.ParseDuration(req.TTL)
			if perr != nil || parsed < 0 {
				http.Error(w, "invalid ttl (want a Go duration like 1h, 30m, or 0 for no expiry)", http.StatusBadRequest)
				return
			}
			ttl = parsed
		}
		if role == RoleAdmin && len(req.Tables) > 0 {
			http.Error(w, "table allow-list cannot be applied to an admin token", http.StatusBadRequest)
			return
		}
		if s.tokens.count() >= maxWebUIAdminMintTokens {
			http.Error(w, "too many tokens", http.StatusBadRequest)
			return
		}
		value, p, err := s.tokens.mint(req.Name, role, ttl, req.Tables)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		auditDetail := "mint token " + p.ID + " role=" + string(role)
		if len(p.Tables) > 0 {
			auditDetail += " tables=" + strings.Join(p.Tables, ",")
		}
		s.recordAudit(r, admin, classDDL, "", "allowed", auditDetail)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		// The raw token is surfaced exactly once; it cannot be recovered later.
		writeJSON(w, map[string]interface{}{"token": value, "principal": p})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleAdminToken rotates (POST .../rotate) or revokes (DELETE) one token.
func (s *Server) handleAdminToken(w http.ResponseWriter, r *http.Request) {
	admin, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/api/admin/tokens/")
	rest = strings.Trim(rest, "/")
	id := rest
	action := ""
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		id = rest[:i]
		action = rest[i+1:]
	}
	id, _ = url.QueryUnescape(id)
	if id == "" {
		http.Error(w, "token id required", http.StatusBadRequest)
		return
	}
	if id == bootstrapTokenID {
		http.Error(w, "the bootstrap token cannot be managed via the API", http.StatusBadRequest)
		return
	}

	switch {
	case r.Method == http.MethodPost && action == "rotate":
		value, p, found := s.tokens.rotate(id)
		if !found {
			http.Error(w, "token not found", http.StatusNotFound)
			return
		}
		s.recordAudit(r, admin, classDDL, "", "allowed", "rotate token "+id)
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]interface{}{"token": value, "principal": p})

	case r.Method == http.MethodDelete && action == "":
		if !s.tokens.revoke(id) {
			http.Error(w, "token not found", http.StatusNotFound)
			return
		}
		s.recordAudit(r, admin, classDDL, "", "allowed", "revoke token "+id)
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]string{"status": "revoked"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleAdminAudit returns the most recent audit events (admin only).
func (s *Server) handleAdminAudit(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	limit := 100
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, map[string]interface{}{"events": s.audit.recent(limit)})
}
