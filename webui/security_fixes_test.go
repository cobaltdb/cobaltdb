package main

// Regression tests for Web UI security fixes:
//   - Schema/table-info endpoints must honor per-token table allow-lists.
//   - Rate limiting must apply before token resolution (IP-keyed), and
//     "unauthorized" audit records must be rate-limited per IP.
//   - X-Forwarded-For must only be trusted when explicitly configured.
//   - walkExpr must fail closed on unrecognized expression node types.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func restrictedPrincipal(tables ...string) principal {
	return principal{ID: "tok1", Name: "restricted", Role: RoleReadOnly, Tables: tables}
}

func TestHandleSchemaFiltersByTableAllowList(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	for _, ddl := range []string{
		"CREATE TABLE visible_t (id INT)",
		"CREATE TABLE hidden_t (id INT)",
	} {
		if _, err := db.Exec(t.Context(), ddl); err != nil {
			t.Fatalf("exec %q: %v", ddl, err)
		}
	}

	srv := newAuthedServer(t)
	srv.db = db

	// Restricted token: only visible_t may be listed.
	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	req = withPrincipal(req, restrictedPrincipal("visible_t"))
	rec := httptest.NewRecorder()
	srv.handleSchema(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("schema status = %d, want 200", rec.Code)
	}
	var schema SchemaInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &schema); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	names := make([]string, 0, len(schema.Tables))
	for _, ti := range schema.Tables {
		names = append(names, ti.Name)
	}
	if len(names) != 1 || names[0] != "visible_t" {
		t.Errorf("restricted schema tables = %v, want [visible_t]", names)
	}

	// Admin (no restriction) sees everything.
	reqAdmin := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	reqAdmin = withPrincipal(reqAdmin, principal{ID: "a", Name: "admin", Role: RoleAdmin})
	recAdmin := httptest.NewRecorder()
	srv.handleSchema(recAdmin, reqAdmin)
	var adminSchema SchemaInfo
	if err := json.Unmarshal(recAdmin.Body.Bytes(), &adminSchema); err != nil {
		t.Fatalf("decode admin schema: %v", err)
	}
	if len(adminSchema.Tables) != 2 {
		t.Errorf("admin schema tables = %d, want 2", len(adminSchema.Tables))
	}
}

func TestHandleSchemaRejectsNonGET(t *testing.T) {
	srv := newAuthedServer(t)
	srv.db = newMemDB(t)
	defer srv.db.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/schema", nil)
	req = withPrincipal(req, principal{ID: "a", Name: "admin", Role: RoleAdmin})
	rec := httptest.NewRecorder()
	srv.handleSchema(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /api/schema status = %d, want 405", rec.Code)
	}
}

func TestHandleTableInfoRejectsNonAllowedTable(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	if _, err := db.Exec(t.Context(), "CREATE TABLE secret_t (id INT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	srv := newAuthedServer(t)
	srv.db = db

	req := httptest.NewRequest(http.MethodGet, "/api/tables/secret_t", nil)
	req = withPrincipal(req, restrictedPrincipal("other_t"))
	rec := httptest.NewRecorder()
	srv.handleTableInfo(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("table info status = %d, want 403", rec.Code)
	}

	// The denial must be audit-recorded.
	events := srv.audit.recent(10)
	found := false
	for _, ev := range events {
		if ev.Outcome == "denied" && strings.Contains(ev.Detail, "secret_t") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a denied audit record mentioning secret_t, got %+v", events)
	}

	// The same table is served when allow-listed.
	reqOK := httptest.NewRequest(http.MethodGet, "/api/tables/secret_t", nil)
	reqOK = withPrincipal(reqOK, restrictedPrincipal("secret_t"))
	recOK := httptest.NewRecorder()
	srv.handleTableInfo(recOK, reqOK)
	if recOK.Code != http.StatusOK {
		t.Errorf("allow-listed table info status = %d, want 200", recOK.Code)
	}
}

func TestMiddlewareIPRateLimitAppliesBeforeTokenResolution(t *testing.T) {
	now := time.Unix(0, 0)
	srv := newAuthedServer(t)
	srv.ipLimiter = newRateLimiter(60, 1)
	srv.ipLimiter.now = func() time.Time { return now }

	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	do := func() int {
		req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
		req.RemoteAddr = "203.0.113.9:1234"
		req.Header.Set("X-CobaltDB-Token", "definitely-not-a-valid-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	// First invalid-token request: unauthorized. Second: throttled by the
	// IP limiter BEFORE token resolution (previously it bypassed limiting).
	if code := do(); code != http.StatusUnauthorized {
		t.Fatalf("first invalid-token request = %d, want 401", code)
	}
	if code := do(); code != http.StatusTooManyRequests {
		t.Fatalf("second invalid-token request = %d, want 429", code)
	}

	// A different IP has its own bucket.
	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	req.RemoteAddr = "198.51.100.7:9999"
	req.Header.Set("X-CobaltDB-Token", "also-invalid")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("other IP first request = %d, want 401", rec.Code)
	}
}

func TestUnauthorizedAuditRecordsAreRateLimited(t *testing.T) {
	now := time.Unix(0, 0)
	srv := newAuthedServer(t)
	// No IP request limiting; only the unauthorized-audit sampler is active.
	srv.unauthAuditLimiter = newRateLimiter(60, 2)
	srv.unauthAuditLimiter.now = func() time.Time { return now }

	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
		req.RemoteAddr = "203.0.113.9:1234"
		req.Header.Set("X-CobaltDB-Token", "invalid-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("request %d = %d, want 401", i, rec.Code)
		}
	}

	unauth := 0
	for _, ev := range srv.audit.recent(100) {
		if ev.Outcome == "denied" && ev.Detail == "unauthorized" {
			unauth++
		}
	}
	if unauth > 2 {
		t.Errorf("unauthorized audit records = %d, want <= 2 (rate limited)", unauth)
	}
	if unauth == 0 {
		t.Error("expected at least one unauthorized audit record")
	}
}

func TestClientIPTrustsForwardedForOnlyWhenConfigured(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "192.0.2.1:5555"
	req.Header.Set("X-Forwarded-For", "10.0.0.99, 172.16.0.1")

	// Default (untrusted): the spoofable header is ignored.
	srv := &Server{}
	if got := srv.clientIP(req); got != "192.0.2.1:5555" {
		t.Errorf("untrusted clientIP = %q, want RemoteAddr", got)
	}

	// Opt-in behind a trusted proxy: first XFF hop is used.
	srvTrusted := &Server{trustProxyHeaders: true}
	if got := srvTrusted.clientIP(req); got != "10.0.0.99" {
		t.Errorf("trusted clientIP = %q, want 10.0.0.99", got)
	}

	// Without the header, RemoteAddr either way.
	plain := httptest.NewRequest(http.MethodGet, "/", nil)
	plain.RemoteAddr = "192.0.2.1:5555"
	if got := srvTrusted.clientIP(plain); got != "192.0.2.1:5555" {
		t.Errorf("trusted clientIP without XFF = %q, want RemoteAddr", got)
	}
}

// unknownExpr embeds the query.Expression interface so it type-asserts as an
// Expression the walker has never seen — simulating a future AST node.
type unknownExpr struct {
	query.Expression
}

func TestWalkExprFailsClosedOnUnknownExpression(t *testing.T) {
	acc := &tableAccumulator{tables: map[string]struct{}{}}
	err := acc.walkExpr(&unknownExpr{}, map[string]struct{}{})
	if err == nil {
		t.Fatal("walkExpr must fail closed on an unrecognized expression type")
	}
	if !strings.Contains(err.Error(), "unsupported expression") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestWalkExprStillAcceptsKnownShapes guards against the fail-closed default
// over-rejecting: every leaf/recursive shape in real statements must pass.
func TestWalkExprStillAcceptsKnownShapes(t *testing.T) {
	stmts := []struct {
		sql    string
		tables []string
	}{
		{"SELECT id, name FROM users WHERE age > 18 AND name LIKE 'a%'", []string{"users"}},
		{"SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM t1", []string{"t1"}},
		{"SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)", []string{"t1", "t2"}},
		{"SELECT COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 2 ORDER BY status", []string{"orders"}},
		{"INSERT INTO t1 (a) VALUES (1)", []string{"t1"}},
		{"UPDATE t1 SET a = 2 WHERE b BETWEEN 1 AND 10", []string{"t1"}},
		{"DELETE FROM t1 WHERE a IS NULL", []string{"t1"}},
		{"WITH x AS (SELECT * FROM base) SELECT * FROM x", []string{"base"}},
	}
	for _, tc := range stmts {
		got, err := extractTableRefs(tc.sql)
		if err != nil {
			t.Errorf("extractTableRefs(%q) unexpected error: %v", tc.sql, err)
			continue
		}
		want := map[string]struct{}{}
		for _, w := range tc.tables {
			want[w] = struct{}{}
		}
		if len(got) != len(want) {
			t.Errorf("extractTableRefs(%q) = %v, want %v", tc.sql, got, tc.tables)
			continue
		}
		for _, g := range got {
			if _, ok := want[g]; !ok {
				t.Errorf("extractTableRefs(%q) = %v, want %v", tc.sql, got, tc.tables)
			}
		}
	}
}
