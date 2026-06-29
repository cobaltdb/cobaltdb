package main

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

func sortedTables(t *testing.T, sql string) []string {
	t.Helper()
	got, err := extractTableRefs(sql)
	if err != nil {
		t.Fatalf("extractTableRefs(%q): %v", sql, err)
	}
	sort.Strings(got)
	return got
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestExtractTableRefsBasic(t *testing.T) {
	cases := []struct {
		sql  string
		want []string
	}{
		{"SELECT * FROM users", []string{"users"}},
		{"SELECT * FROM Users", []string{"users"}}, // case-insensitive
		{"SELECT * FROM a JOIN b ON a.id = b.id", []string{"a", "b"}},
		{"SELECT * FROM a, b", []string{"a", "b"}},
		{"INSERT INTO logs VALUES (1)", []string{"logs"}},
		{"UPDATE accounts SET x = 1 WHERE id = 2", []string{"accounts"}},
		{"DELETE FROM sessions WHERE id = 1", []string{"sessions"}},
	}
	for _, c := range cases {
		got := sortedTables(t, c.sql)
		sort.Strings(c.want)
		if !eqStrings(got, c.want) {
			t.Errorf("extractTableRefs(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestExtractTableRefsSubqueries(t *testing.T) {
	// Subquery in WHERE ... IN (...)
	got := sortedTables(t, "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers)")
	if !eqStrings(got, []string{"customers", "orders"}) {
		t.Errorf("IN-subquery tables = %v", got)
	}

	// Derived table in FROM.
	got = sortedTables(t, "SELECT * FROM (SELECT id FROM secret_table) AS t")
	if !eqStrings(got, []string{"secret_table"}) {
		t.Errorf("derived-table tables = %v", got)
	}

	// EXISTS correlated subquery.
	got = sortedTables(t, "SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)")
	if !eqStrings(got, []string{"a", "b"}) {
		t.Errorf("EXISTS-subquery tables = %v", got)
	}

	// Scalar subquery in the SELECT list.
	got = sortedTables(t, "SELECT (SELECT max(v) FROM metrics) AS m FROM dashboards")
	if !eqStrings(got, []string{"dashboards", "metrics"}) {
		t.Errorf("scalar-subquery tables = %v", got)
	}
}

func TestExtractTableRefsCTE(t *testing.T) {
	// The CTE name `recent` must NOT count as a base table, but `events` must.
	got := sortedTables(t, "WITH recent AS (SELECT * FROM events) SELECT * FROM recent")
	if !eqStrings(got, []string{"events"}) {
		t.Errorf("CTE tables = %v, want [events]", got)
	}

	// Two CTEs, one referencing a real table the other a CTE.
	got = sortedTables(t, "WITH a AS (SELECT * FROM real_t), b AS (SELECT * FROM a) SELECT * FROM b")
	if !eqStrings(got, []string{"real_t"}) {
		t.Errorf("chained-CTE tables = %v, want [real_t]", got)
	}
}

func TestExtractTableRefsUnion(t *testing.T) {
	got := sortedTables(t, "SELECT id FROM a UNION SELECT id FROM b")
	if !eqStrings(got, []string{"a", "b"}) {
		t.Errorf("UNION tables = %v", got)
	}
}

func TestExtractTableRefsFailClosedOnGarbage(t *testing.T) {
	if _, err := extractTableRefs("NOTASTATEMENT @#$%"); err == nil {
		t.Fatal("expected an error for unparseable SQL (fail-closed)")
	}
}

// --- normalizeTableList ---

func TestNormalizeTableList(t *testing.T) {
	got, err := normalizeTableList([]string{" Users ", "orders", "USERS", ""})
	if err != nil {
		t.Fatalf("normalizeTableList: %v", err)
	}
	sort.Strings(got)
	if !eqStrings(got, []string{"orders", "users"}) {
		t.Fatalf("normalize = %v, want [orders users]", got)
	}

	if _, err := normalizeTableList([]string{"bad name"}); err == nil {
		t.Fatal("expected error for table name with space")
	}
	if _, err := normalizeTableList([]string{"a;b"}); err == nil {
		t.Fatal("expected error for table name with semicolon")
	}
	if got, _ := normalizeTableList(nil); got != nil {
		t.Fatal("nil input should yield nil (unrestricted)")
	}
}

// --- principal.allowsTable / tableRestricted ---

func TestPrincipalTableRestriction(t *testing.T) {
	ro := principal{Role: RoleReadOnly, Tables: []string{"users", "orders"}}
	if !ro.tableRestricted() {
		t.Fatal("readonly with table list should be restricted")
	}
	if !ro.allowsTable("USERS") || !ro.allowsTable("orders") {
		t.Fatal("listed tables must be allowed (case-insensitive)")
	}
	if ro.allowsTable("secrets") {
		t.Fatal("unlisted table must be denied")
	}

	// Admins ignore allow-lists entirely.
	admin := principal{Role: RoleAdmin, Tables: []string{"users"}}
	if admin.tableRestricted() {
		t.Fatal("admin must never be table-restricted")
	}

	// Empty list = unrestricted.
	open := principal{Role: RoleReadWrite}
	if open.tableRestricted() {
		t.Fatal("empty table list means unrestricted")
	}
}

// --- end-to-end enforcement through handleQuery ---

func TestHandleQueryTableAllowListBlocksUnlisted(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	for _, ddl := range []string{
		"CREATE TABLE allowed_t (id INT)",
		"CREATE TABLE secret_t (id INT)",
	} {
		if _, err := db.Exec(t.Context(), ddl); err != nil {
			t.Fatalf("ddl %q: %v", ddl, err)
		}
	}

	srv := newAuthedServer(t)
	srv.db = db
	scoped := principal{ID: "s", Name: "scoped", Role: RoleReadOnly, Tables: []string{"allowed_t"}}

	// Allowed table: 200.
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(`{"query":"SELECT * FROM allowed_t"}`))
	req = withPrincipal(req, scoped)
	rec := httptest.NewRecorder()
	srv.handleQuery(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("allowed-table query = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	// Unlisted table: 403.
	req2 := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(`{"query":"SELECT * FROM secret_t"}`))
	req2 = withPrincipal(req2, scoped)
	rec2 := httptest.NewRecorder()
	srv.handleQuery(rec2, req2)
	if rec2.Code != http.StatusForbidden {
		t.Fatalf("unlisted-table query = %d, want 403: %s", rec2.Code, rec2.Body.String())
	}
	if !strings.Contains(rec2.Body.String(), "allow-list") {
		t.Fatalf("expected allow-list denial message, got %s", rec2.Body.String())
	}
}

func TestHandleQueryTableAllowListBlocksHiddenSubqueryTable(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	for _, ddl := range []string{
		"CREATE TABLE allowed_t (id INT)",
		"CREATE TABLE secret_t (id INT)",
	} {
		if _, err := db.Exec(t.Context(), ddl); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	srv := newAuthedServer(t)
	srv.db = db
	scoped := principal{ID: "s", Role: RoleReadOnly, Tables: []string{"allowed_t"}}

	// A query whose top-level table is allowed but which reaches a secret table
	// through a subquery must still be denied.
	body := `{"query":"SELECT * FROM allowed_t WHERE id IN (SELECT id FROM secret_t)"}`
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	req = withPrincipal(req, scoped)
	rec := httptest.NewRecorder()
	srv.handleQuery(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("hidden-subquery query = %d, want 403: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleQueryTableAllowListAdminUnrestricted(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	if _, err := db.Exec(t.Context(), "CREATE TABLE anything_t (id INT)"); err != nil {
		t.Fatalf("ddl: %v", err)
	}
	srv := newAuthedServer(t)
	srv.db = db

	// Admin carries a (meaningless) table list but is never restricted.
	admin := principal{ID: "a", Role: RoleAdmin, Tables: []string{"only_this"}}
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(`{"query":"SELECT * FROM anything_t"}`))
	req = withPrincipal(req, admin)
	rec := httptest.NewRecorder()
	srv.handleQuery(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin query = %d, want 200: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleUpdateRowTableAllowList(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	if _, err := db.Exec(t.Context(), "CREATE TABLE editable (id INT, name TEXT)"); err != nil {
		t.Fatalf("ddl: %v", err)
	}
	if _, err := db.Exec(t.Context(), "INSERT INTO editable VALUES (1, 'a')"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	srv := newAuthedServer(t)
	srv.db = db

	// readwrite token scoped to a different table cannot inline-edit `editable`.
	scoped := principal{ID: "s", Role: RoleReadWrite, Tables: []string{"other_table"}}
	body := `{"table":"editable","column":"name","value":"b","where":{"id":1}}`
	req := httptest.NewRequest(http.MethodPost, "/api/update-row", strings.NewReader(body))
	req = withPrincipal(req, scoped)
	rec := httptest.NewRecorder()
	srv.handleUpdateRow(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("scoped inline-edit = %d, want 403: %s", rec.Code, rec.Body.String())
	}
}

// --- admin mint with table allow-list ---

func TestAdminMintWithTableAllowList(t *testing.T) {
	srv := newAuthedServer(t)
	body := `{"name":"reporting","role":"readonly","tables":["Users","orders"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/admin/tokens", strings.NewReader(body))
	req = withPrincipal(req, principal{ID: bootstrapTokenID, Role: RoleAdmin})
	rec := httptest.NewRecorder()
	srv.handleAdminTokens(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("mint with tables = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	// The list must be normalized (lower-cased) on the stored principal.
	for _, p := range srv.tokens.list() {
		if p.Name == "reporting" {
			if !p.allowsTable("users") || !p.allowsTable("orders") {
				t.Fatalf("minted token has wrong allow-list: %v", p.Tables)
			}
			return
		}
	}
	t.Fatal("minted token not found in store")
}

func TestAdminMintRejectsTablesForAdminRole(t *testing.T) {
	srv := newAuthedServer(t)
	body := `{"name":"x","role":"admin","tables":["users"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/admin/tokens", strings.NewReader(body))
	req = withPrincipal(req, principal{ID: bootstrapTokenID, Role: RoleAdmin})
	rec := httptest.NewRecorder()
	srv.handleAdminTokens(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("admin+tables mint = %d, want 400", rec.Code)
	}
}
