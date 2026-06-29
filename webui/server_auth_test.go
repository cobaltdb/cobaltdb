package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestAuthMiddlewareRejectsMissingToken(t *testing.T) {
	srv := &Server{
		authEnabled: true,
	}
	srv.setAPIToken("test-token")
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}
}

func TestAuthMiddlewareAcceptsHeaderToken(t *testing.T) {
	srv := &Server{
		authEnabled: true,
	}
	srv.setAPIToken("test-token")
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	req.Header.Set("X-CobaltDB-Token", "test-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}
}

func TestAuthMiddlewareSetsCookieAndRedirectsWhenTokenInQuery(t *testing.T) {
	srv := &Server{
		authEnabled: true,
	}
	srv.setAPIToken("test-token")
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/?token=test-token", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected status %d, got %d", http.StatusFound, rec.Code)
	}
	if rec.Header().Get("Location") != "/" {
		t.Fatalf("expected redirect location '/', got %q", rec.Header().Get("Location"))
	}
	cookies := rec.Result().Cookies()
	if len(cookies) == 0 || cookies[0].Name != authCookieName {
		t.Fatalf("expected auth cookie %q to be set", authCookieName)
	}
	if cookies[0].Secure {
		t.Fatal("expected HTTP auth cookie to be usable without Secure flag")
	}
	if !cookies[0].HttpOnly {
		t.Fatal("expected auth cookie to be HttpOnly")
	}
	if cookies[0].Value != "test-token" {
		t.Fatalf("expected auth cookie to carry submitted token, got %q", cookies[0].Value)
	}
}

func TestAuthMiddlewareSetsSecureCookieForHTTPS(t *testing.T) {
	srv := &Server{
		authEnabled: true,
	}
	srv.setAPIToken("test-token")
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "https://example.com/?token=test-token", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	cookies := rec.Result().Cookies()
	if len(cookies) == 0 || !cookies[0].Secure {
		t.Fatal("expected HTTPS auth cookie to include Secure flag")
	}
}

func TestAuthMiddlewareSetsSecureCookieBehindHTTPSProxy(t *testing.T) {
	srv := &Server{
		authEnabled: true,
	}
	srv.setAPIToken("test-token")
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/?token=test-token", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	cookies := rec.Result().Cookies()
	if len(cookies) == 0 || !cookies[0].Secure {
		t.Fatal("expected proxied HTTPS auth cookie to include Secure flag")
	}
}

func TestAuthMiddlewareBypassWhenDisabled(t *testing.T) {
	srv := &Server{
		authEnabled: false,
	}
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}
}

func TestWebUITokenStoredAsDigest(t *testing.T) {
	srv := &Server{}
	srv.setAPIToken("test-token")

	if srv.tokens == nil || srv.tokens.count() == 0 {
		t.Fatal("expected token to be configured")
	}
	if !srv.secureTokenCompare("test-token") {
		t.Fatal("expected configured token to validate")
	}
	if srv.secureTokenCompare("wrong-token") {
		t.Fatal("expected wrong token to be rejected")
	}
	// Ensure the raw token is not retained anywhere in the store records.
	srv.tokens.mu.RLock()
	for _, rec := range srv.tokens.tokens {
		if string(rec.hash[:]) == "test-token" {
			srv.tokens.mu.RUnlock()
			t.Fatal("raw token stored in digest field")
		}
	}
	srv.tokens.mu.RUnlock()
}

func TestWebUIRejectsOversizedTokens(t *testing.T) {
	srv := &Server{authEnabled: true}
	srv.setAPIToken("test-token")
	if srv.secureTokenCompare(strings.Repeat("x", maxWebUITokenBytes+1)) {
		t.Fatal("oversized token candidate should be rejected")
	}

	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", maxWebUITokenBytes+1))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("oversized token status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestWebUIOversizedConfiguredTokenFailsClosed(t *testing.T) {
	srv := &Server{}
	srv.setAPIToken(strings.Repeat("x", maxWebUITokenBytes+1))

	if srv.tokens != nil && srv.tokens.count() != 0 {
		t.Fatal("oversized configured token should not enable API token auth")
	}
	if srv.secureTokenCompare(strings.Repeat("x", maxWebUITokenBytes+1)) {
		t.Fatal("oversized configured token should not authenticate")
	}
}

func TestQuoteSQLIdentifier(t *testing.T) {
	got, err := quoteSQLIdentifier(`users"; DROP TABLE users;--`)
	if err != nil {
		t.Fatalf("quoteSQLIdentifier returned error: %v", err)
	}
	want := `"users\"; DROP TABLE users;--"`
	if got != want {
		t.Fatalf("quoteSQLIdentifier = %q, want %q", got, want)
	}
	if _, err := quoteSQLIdentifier(""); err == nil {
		t.Fatal("expected empty identifier to be rejected")
	}
	if _, err := quoteSQLIdentifier(strings.Repeat("a", maxWebUIIdentifier+1)); err == nil {
		t.Fatal("expected oversized identifier to be rejected")
	}
}

func TestHandleUpdateRowRejectsUnsafeIdentifier(t *testing.T) {
	srv := &Server{}
	body, err := json.Marshal(map[string]interface{}{
		"table":  "",
		"column": "name",
		"value":  "alice",
		"where":  map[string]interface{}{"id": 1},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/update-row", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.handleUpdateRow(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleUpdateRowRejectsTooManyWhereTerms(t *testing.T) {
	srv := &Server{}
	where := make(map[string]interface{}, maxWebUIWhereTerms+1)
	for i := 0; i <= maxWebUIWhereTerms; i++ {
		where[fmt.Sprintf("id_%d", i)] = i
	}
	body, err := json.Marshal(map[string]interface{}{
		"table":  "users",
		"column": "name",
		"value":  "alice",
		"where":  where,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/update-row", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.handleUpdateRow(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "too many where terms") {
		t.Fatalf("expected where term limit error, got %q", rec.Body.String())
	}
}

func TestJSONAPIRejectsOversizedBody(t *testing.T) {
	oversizedQueryBody := `{"query":"` + strings.Repeat("x", maxWebUIJSONBodyBytes) + `"}`
	oversizedSavedQueryBody := `{"name":"n","query":"` + strings.Repeat("x", maxWebUIJSONBodyBytes) + `"}`
	oversizedUpdateBody := `{"table":"t","column":"c","value":"` + strings.Repeat("x", maxWebUIJSONBodyBytes) + `","where":{"id":1}}`

	tests := []struct {
		name    string
		path    string
		body    string
		handler func(http.ResponseWriter, *http.Request)
	}{
		{
			name:    "query",
			path:    "/api/query",
			body:    oversizedQueryBody,
			handler: (&Server{}).handleQuery,
		},
		{
			name: "saved query",
			path: "/api/saved-queries",
			body: oversizedSavedQueryBody,
			handler: (&Server{
				savedQueries: make(map[string]SavedQuery),
			}).handleSavedQueries,
		},
		{
			name:    "update row",
			path:    "/api/update-row",
			body:    oversizedUpdateBody,
			handler: (&Server{}).handleUpdateRow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()

			tt.handler(rec, req)

			if rec.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("expected status %d, got %d: %s", http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHandleSavedQueriesRejectsResourceLimitViolations(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "large name",
			body: `{"name":"` + strings.Repeat("n", maxWebUISavedName+1) + `","query":"SELECT 1"}`,
			want: "name too large",
		},
		{
			name: "large query",
			body: `{"name":"n","query":"` + strings.Repeat("x", maxWebUISavedQuery+1) + `"}`,
			want: "query too large",
		},
		{
			name: "large description",
			body: `{"name":"n","query":"SELECT 1","description":"` + strings.Repeat("d", maxWebUISavedDesc+1) + `"}`,
			want: "description too large",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := &Server{savedQueries: make(map[string]SavedQuery)}
			req := httptest.NewRequest(http.MethodPost, "/api/saved-queries", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()

			srv.handleSavedQueries(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tt.want) {
				t.Fatalf("expected error containing %q, got %q", tt.want, rec.Body.String())
			}
		})
	}
}

func TestHandleSavedQueriesRejectsTooManyEntries(t *testing.T) {
	srv := &Server{savedQueries: make(map[string]SavedQuery)}
	for i := 0; i < maxWebUISavedQueries; i++ {
		name := fmt.Sprintf("q_%d", i)
		srv.savedQueries[name] = SavedQuery{Name: name, Query: "SELECT 1"}
	}

	req := httptest.NewRequest(http.MethodPost, "/api/saved-queries", strings.NewReader(`{"name":"new","query":"SELECT 1"}`))
	rec := httptest.NewRecorder()
	srv.handleSavedQueries(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "too many saved queries") {
		t.Fatalf("expected too many saved queries error, got %q", rec.Body.String())
	}
}

func TestHandleImportSavedQueriesValidatesLimits(t *testing.T) {
	srv := &Server{savedQueries: make(map[string]SavedQuery)}
	queries := []SavedQuery{{Name: "ok", Query: "SELECT 1"}, {Name: "bad", Query: strings.Repeat("x", maxWebUISavedQuery+1)}}
	body, contentType := savedQueriesMultipartBody(t, queries)

	req := httptest.NewRequest(http.MethodPost, "/api/import-saved-queries", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	srv.handleImportSavedQueries(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
	if len(srv.savedQueries) != 0 {
		t.Fatalf("invalid import should not partially save queries: %+v", srv.savedQueries)
	}
}

func TestHandleImportSavedQueriesCountsImportedValidRows(t *testing.T) {
	srv := &Server{savedQueries: make(map[string]SavedQuery)}
	queries := []SavedQuery{
		{Name: "one", Query: "SELECT 1"},
		{},
		{Name: "two", Query: "SELECT 2"},
	}
	body, contentType := savedQueriesMultipartBody(t, queries)

	req := httptest.NewRequest(http.MethodPost, "/api/import-saved-queries", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	srv.handleImportSavedQueries(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if len(srv.savedQueries) != 2 {
		t.Fatalf("expected 2 saved queries, got %d", len(srv.savedQueries))
	}
	if !strings.Contains(rec.Body.String(), `"count":"2"`) {
		t.Fatalf("expected imported count 2, got %q", rec.Body.String())
	}
}

func TestHandleImportSavedQueriesRejectsTrailingJSON(t *testing.T) {
	srv := &Server{savedQueries: make(map[string]SavedQuery)}
	body, contentType := savedQueriesMultipartRawBody(t, `[{"name":"ok","query":"SELECT 1"}] []`)

	req := httptest.NewRequest(http.MethodPost, "/api/import-saved-queries", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	srv.handleImportSavedQueries(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
	if len(srv.savedQueries) != 0 {
		t.Fatalf("trailing JSON import should not save queries: %+v", srv.savedQueries)
	}
}

func TestDecodeSavedQueriesImportRejectsOversizedFile(t *testing.T) {
	_, err := decodeSavedQueriesImport(strings.NewReader(strings.Repeat(" ", maxWebUIImportBytes+1)))
	if err == nil {
		t.Fatal("expected oversized import error")
	}
	if !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected too large error, got %v", err)
	}
}

func savedQueriesMultipartBody(t *testing.T, queries []SavedQuery) (*bytes.Buffer, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "saved-queries.json")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if err := json.NewEncoder(part).Encode(queries); err != nil {
		t.Fatalf("encode saved queries: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return &body, writer.FormDataContentType()
}

func savedQueriesMultipartRawBody(t *testing.T, content string) (*bytes.Buffer, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "saved-queries.json")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := part.Write([]byte(content)); err != nil {
		t.Fatalf("write saved queries: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return &body, writer.FormDataContentType()
}

func TestHandleQueryCapsReturnedRows(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(t.Context(), "CREATE TABLE capped_rows (id INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < maxWebUIResultRows+1; i++ {
		if _, err := db.Exec(t.Context(), fmt.Sprintf("INSERT INTO capped_rows VALUES (%d)", i)); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	body := `{"query":"SELECT * FROM capped_rows"}`
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv := &Server{db: db}

	srv.handleQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var resp QueryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected successful response, got message %q", resp.Message)
	}
	if len(resp.Rows) != maxWebUIResultRows {
		t.Fatalf("expected %d returned rows, got %d", maxWebUIResultRows, len(resp.Rows))
	}
	if resp.RowCount != maxWebUIResultRows {
		t.Fatalf("expected rowCount %d, got %d", maxWebUIResultRows, resp.RowCount)
	}
	if !strings.Contains(resp.Message, "truncated") {
		t.Fatalf("expected truncation message, got %q", resp.Message)
	}
}

func TestHandleQueryRejectsOversizedSQL(t *testing.T) {
	srv := &Server{}
	body := `{"query":"` + strings.Repeat("x", maxWebUIQueryBytes+1) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.handleQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	var resp QueryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Success {
		t.Fatal("oversized query should not succeed")
	}
	if resp.Message != "query too large" {
		t.Fatalf("message = %q, want query too large", resp.Message)
	}
}

func TestExportEndpointsRejectOversizedSQL(t *testing.T) {
	srv := &Server{}
	largeQuery := strings.Repeat("x", maxWebUIQueryBytes+1)
	for _, tt := range []struct {
		name    string
		path    string
		handler func(http.ResponseWriter, *http.Request)
	}{
		{name: "csv", path: "/api/export/csv?query=" + largeQuery, handler: srv.handleExportCSV},
		{name: "json", path: "/api/export/json?query=" + largeQuery, handler: srv.handleExportJSON},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			tt.handler(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "query too large") {
				t.Fatalf("expected query too large error, got %q", rec.Body.String())
			}
		})
	}
}

func TestExportEndpointsCapRows(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(t.Context(), "CREATE TABLE export_capped_rows (id INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < maxWebUIResultRows+1; i++ {
		if _, err := db.Exec(t.Context(), fmt.Sprintf("INSERT INTO export_capped_rows VALUES (%d)", i)); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	srv := &Server{db: db}

	t.Run("csv", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/export/csv?query=SELECT%20*%20FROM%20export_capped_rows", nil)
		rec := httptest.NewRecorder()
		srv.handleExportCSV(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("X-CobaltDB-Truncated") != "true" {
			t.Fatalf("expected truncation header, got %q", rec.Header().Get("X-CobaltDB-Truncated"))
		}
		records, err := csv.NewReader(strings.NewReader(rec.Body.String())).ReadAll()
		if err != nil {
			t.Fatalf("read csv response: %v", err)
		}
		if len(records) != maxWebUIResultRows+1 {
			t.Fatalf("csv records = %d, want header + %d rows", len(records), maxWebUIResultRows)
		}
	})

	t.Run("json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/export/json?query=SELECT%20*%20FROM%20export_capped_rows", nil)
		rec := httptest.NewRecorder()
		srv.handleExportJSON(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("X-CobaltDB-Truncated") != "true" {
			t.Fatalf("expected truncation header, got %q", rec.Header().Get("X-CobaltDB-Truncated"))
		}
		var rows []map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
			t.Fatalf("decode json export: %v", err)
		}
		if len(rows) != maxWebUIResultRows {
			t.Fatalf("json rows = %d, want %d", len(rows), maxWebUIResultRows)
		}
	})
}

func TestHistoryConcurrentAccess(t *testing.T) {
	srv := &Server{}
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			srv.addToHistory(fmt.Sprintf("SELECT %d", i), "1 ms", 1)
		}(i)
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/api/history", nil)
			rec := httptest.NewRecorder()
			srv.handleHistory(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("expected status %d, got %d", http.StatusOK, rec.Code)
			}
		}()
	}

	wg.Wait()

	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if len(srv.history) > 100 {
		t.Fatalf("expected history cap of 100, got %d", len(srv.history))
	}
}
