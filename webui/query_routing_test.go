package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// Regression tests for statement routing in handleQuery. Routing used a
// hand-rolled HasPrefix list that omitted DESC (and PRAGMA/VALUES), diverging
// from classifyQuery — the authorization authority — so "DESC t" was
// authorized as a read but executed via db.Exec, which rejects reads with
// "use Query() instead of Exec() for SELECT/SHOW statements". The fix routes
// on the already-computed classification (class == classRead).

func newQueryRoutingServer(t *testing.T) *Server {
	t.Helper()
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT, name VARCHAR(20))"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1,'x')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return &Server{db: db}
}

func postQueryForRouting(t *testing.T, srv *Server, sql string) *QueryResponse {
	t.Helper()
	body := fmt.Sprintf(`{"query":%q}`, sql)
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	req = withPrincipal(req, principal{ID: "admin", Name: "admin", Role: RoleAdmin})
	rec := httptest.NewRecorder()
	srv.handleQuery(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST %q status = %d, body %s", sql, rec.Code, rec.Body.String())
	}
	var resp QueryResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response for %q: %v", sql, err)
	}
	return &resp
}

func TestQueryRoutingDescReturnsColumns(t *testing.T) {
	srv := newQueryRoutingServer(t)
	resp := postQueryForRouting(t, srv, "DESC t")
	if !resp.Success {
		t.Fatalf("DESC t failed: %s — reads must route through Query, not Exec", resp.Message)
	}
	foundField := false
	for _, col := range resp.Columns {
		if col == "Field" {
			foundField = true
		}
	}
	if !foundField || len(resp.Rows) == 0 {
		t.Fatalf("DESC t returned cols=%v rows=%d; want the DESCRIBE listing", resp.Columns, resp.Rows)
	}
}

func TestQueryRoutingReadOnlyDescReturnsColumns(t *testing.T) {
	srv := newQueryRoutingServer(t)
	body := fmt.Sprintf(`{"query":%q}`, "DESC t")
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	req = withPrincipal(req, principal{ID: "ro", Name: "ro", Role: RoleReadOnly})
	rec := httptest.NewRecorder()
	srv.handleQuery(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("readonly DESC t status = %d, body %s", rec.Code, rec.Body.String())
	}
	var resp QueryResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.Success {
		t.Fatalf("readonly DESC t failed: %s", resp.Message)
	}
}

func TestQueryRoutingControlsUnchanged(t *testing.T) {
	srv := newQueryRoutingServer(t)

	// Control: DESCRIBE (spelled out) already routed correctly.
	if resp := postQueryForRouting(t, srv, "DESCRIBE t"); !resp.Success || len(resp.Columns) == 0 {
		t.Fatalf("DESCRIBE t = %+v; want success with columns", resp)
	}

	// Control: writes still route through Exec and report success.
	resp := postQueryForRouting(t, srv, "INSERT INTO t VALUES (2,'y')")
	if !resp.Success {
		t.Fatalf("INSERT t = %+v; want success", resp)
	}

	// Control: plain SELECT still returns rows.
	resp = postQueryForRouting(t, srv, "SELECT * FROM t")
	if !resp.Success || len(resp.Rows) == 0 {
		t.Fatalf("SELECT * FROM t = %+v; want rows", resp)
	}
}
