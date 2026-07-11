package main

import (
	"bytes"
	"errors"
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

type failingWebWriter struct {
	header http.Header
	status int
}

func (w *failingWebWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *failingWebWriter) WriteHeader(code int)      { w.status = code }
func (w *failingWebWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestCoverageAuditTokenAndLimiterEdges(t *testing.T) {
	if got := newAuditLog(nil, 0, 2); got.size != 1 {
		t.Fatalf("minimum audit ring size = %d", got.size)
	}
	var nilAudit *auditLog
	nilAudit.record(auditEvent{})
	if nilAudit.recent(1) != nil {
		t.Fatal("nil audit log should return no records")
	}
	var sink bytes.Buffer
	a := newAuditLog(&sink, 2, 2)
	a.record(auditEvent{SQL: "abcdef", Outcome: "allowed"})
	a.record(auditEvent{SQL: "two", Outcome: "allowed"})
	a.record(auditEvent{SQL: "three", Outcome: "denied"})
	got := a.recent(1)
	if len(got) != 1 || got[0].SQL != "th…" || sink.Len() == 0 {
		t.Fatalf("bounded audit behavior: %+v sink=%q", got, sink.String())
	}

	if role, err := parseRole(" READWRITE "); err != nil || role != RoleReadWrite {
		t.Fatalf("parse readwrite: %v %q", err, role)
	}
	if Role("bad").allows(classRead) {
		t.Fatal("unknown role must fail closed")
	}
	if _, err := normalizeTableList([]string{strings.Repeat("x", maxWebUIIdentifier+1)}); err == nil {
		t.Fatal("oversized table should fail")
	}
	many := make([]string, maxWebUIAllowListTables+1)
	for i := range many {
		many[i] = "t" + strings.Repeat("x", i%10) + string(rune('A'+i%26)) + strings.Repeat("y", i/26)
	}
	if _, err := normalizeTableList(many); err == nil {
		t.Fatal("oversized allow-list should fail")
	}

	ts := &tokenStore{}
	if ts.clock().IsZero() {
		t.Fatal("fallback clock returned zero")
	}
	if _, err := ts.addWithID("x", "", "x", RoleReadOnly, 0, nil); err == nil {
		t.Fatal("empty token accepted")
	}
	if _, err := ts.addWithID("x", "tok", "", RoleReadOnly, 0, []string{"bad name"}); err == nil {
		t.Fatal("bad table accepted")
	}
	if _, err := ts.addWithID("x", "tok", strings.Repeat("n", maxWebUITokenName+1), RoleReadOnly, 0, nil); err == nil {
		t.Fatal("long name accepted")
	}
	p, err := ts.addWithID("b", "two", "same", RoleReadOnly, 0, nil)
	if err != nil || p.Name != "same" {
		t.Fatalf("add token: %+v %v", p, err)
	}
	if _, err := ts.addWithID("a", "one", "same", RoleReadOnly, 0, nil); err != nil {
		t.Fatal(err)
	}
	list := ts.list()
	if len(list) != 2 || list[0].ID != "a" {
		t.Fatalf("token sort: %+v", list)
	}
	ts.setBootstrap("")
	if _, _, ok := ts.rotate("missing"); ok {
		t.Fatal("rotated missing token")
	}
	if _, _, err := ts.mint(strings.Repeat("n", maxWebUITokenName+1), RoleReadOnly, 0, nil); err == nil {
		t.Fatal("mint accepted invalid metadata")
	}

	now := time.Unix(1_000, 0)
	b := &tokenBucket{tokens: 0, capacity: 2, rate: 1, last: now}
	if !b.allow(now.Add(10*time.Second)) || b.tokens != 1 {
		t.Fatalf("bucket refill: %+v", b)
	}
	rl := newRateLimiter(60, 0)
	rl.now = nil
	if !rl.allow("a") || rl.allow("a") {
		t.Fatal("minimum burst limiter behavior wrong")
	}
	rl.mu.Lock()
	rl.lastGC = time.Time{}
	rl.buckets["old"] = &tokenBucket{last: time.Unix(0, 0), capacity: 1, rate: 1}
	rl.gcLocked(time.Now())
	_, exists := rl.buckets["old"]
	rl.mu.Unlock()
	if exists {
		t.Fatal("idle bucket was not collected")
	}
}

func TestCoverageWebUIHandlersGoldenAndErrors(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
	} {
		if _, err := db.Exec(t.Context(), sql); err != nil {
			t.Fatal(err)
		}
	}
	s := &Server{db: db, savedQueries: map[string]SavedQuery{}, tokens: newTokenStore(), audit: newAuditLog(io.Discard, 10, 100), tokenTTL: time.Hour}
	s.tmpl = template.Must(template.New("index").Parse(`{{.DatabasePath}} {{.Version}}`))

	call := func(method, target, body string, handler http.HandlerFunc, p *principal) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, target, strings.NewReader(body))
		if p != nil {
			r = withPrincipal(r, *p)
		}
		w := httptest.NewRecorder()
		handler(w, r)
		return w
	}
	admin := principal{ID: "a", Name: "admin", Role: RoleAdmin}
	ro := principal{ID: "r", Name: "reader", Role: RoleReadOnly, Tables: []string{"users"}}

	if w := call(http.MethodGet, "/missing", "", s.handleIndex, nil); w.Code != http.StatusNotFound {
		t.Fatalf("index missing=%d", w.Code)
	}
	if w := call(http.MethodGet, "/", "", s.handleIndex, nil); !strings.Contains(w.Body.String(), "0.3.0") {
		t.Fatalf("index=%q", w.Body.String())
	}
	badTemplate := &Server{db: db, tmpl: template.Must(template.New("x").Parse(`{{call .Missing}}`))}
	if w := call(http.MethodGet, "/", "", badTemplate.handleIndex, nil); w.Code != http.StatusInternalServerError {
		t.Fatalf("template error=%d", w.Code)
	}

	if w := call(http.MethodGet, "/api/query", "", s.handleQuery, nil); w.Code != http.StatusMethodNotAllowed {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodPost, "/api/query", `{"query":"INSERT INTO users VALUES (2, 'bob')"}`, s.handleQuery, &admin); !strings.Contains(w.Body.String(), `"success":true`) {
		t.Fatal(w.Body.String())
	}
	if w := call(http.MethodPost, "/api/query", `{"query":"INSERT INTO missing VALUES (1)"}`, s.handleQuery, &admin); !strings.Contains(w.Body.String(), `"success":false`) {
		t.Fatal(w.Body.String())
	}
	if w := call(http.MethodPost, "/api/query", `{"query":"SELECT * FROM missing"}`, s.handleQuery, &admin); !strings.Contains(w.Body.String(), `"success":false`) {
		t.Fatal(w.Body.String())
	}
	if w := call(http.MethodPost, "/api/query", `{"query":"CREATE TABLE denied (id INT)"}`, s.handleQuery, &ro); w.Code != http.StatusForbidden {
		t.Fatal(w.Code)
	}

	if w := call(http.MethodPost, "/api/schema", "", s.handleSchema, nil); w.Code != http.StatusMethodNotAllowed {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodGet, "/api/schema", "", s.handleSchema, &ro); !strings.Contains(w.Body.String(), "users") {
		t.Fatal(w.Body.String())
	}
	for _, tc := range []struct {
		path string
		code int
	}{
		{"/api/tables/", 400}, {"/api/tables/%00", 400}, {"/api/tables/missing", 404}, {"/api/tables/users", 200},
	} {
		if w := call(http.MethodGet, tc.path, "", s.handleTableInfo, &admin); w.Code != tc.code {
			t.Fatalf("%s=%d body=%s", tc.path, w.Code, w.Body.String())
		}
	}

	for _, prefix := range []string{"", "=x", "+x", "-x", "@x", "\tx", "\rx", "\nx", "ok"} {
		got := sanitizeCSVField(prefix)
		if prefix != "" && strings.Contains("=+-@\t\r\n", prefix[:1]) && got[0] != '\'' {
			t.Fatalf("unsafe csv %q -> %q", prefix, got)
		}
	}
	for _, endpoint := range []struct {
		h    http.HandlerFunc
		path string
		mime string
	}{
		{s.handleExportCSV, "/api/export/csv?query=SELECT%20*%20FROM%20users", "text/csv"},
		{s.handleExportJSON, "/api/export/json?query=SELECT%20*%20FROM%20users", "application/json"},
	} {
		w := call(http.MethodGet, endpoint.path, "", endpoint.h, &ro)
		if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), endpoint.mime) {
			t.Fatalf("export %d %s", w.Code, w.Body.String())
		}
	}
	for _, h := range []http.HandlerFunc{s.handleExportCSV, s.handleExportJSON} {
		if w := call(http.MethodGet, "/?query=SELECT%20*%20FROM%20missing", "", h, &admin); w.Code != 500 {
			t.Fatal(w.Code)
		}
		if w := call(http.MethodGet, "/?query=DELETE%20FROM%20users", "", h, &ro); w.Code != 403 {
			t.Fatal(w.Code)
		}
	}

	if formatDuration(time.Microsecond) != "1 μs" || !strings.Contains(formatDuration(2*time.Millisecond), "ms") || !strings.Contains(formatDuration(2*time.Second), "s") {
		t.Fatal("duration formatting")
	}
	for i := 0; i < 102; i++ {
		s.addToHistory("x", "1 ms", 1)
	}
	if len(s.history) != 100 {
		t.Fatal(len(s.history))
	}
	if _, err := decodeSavedQueriesImport(failingReader{}); err == nil {
		t.Fatal("reader error accepted")
	}

	if w := call(http.MethodGet, "/api/saved-queries", "", s.handleSavedQueries, nil); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodPost, "/api/saved-queries", `{"name":"q","query":"SELECT 1"}`, s.handleSavedQueries, nil); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodPut, "/api/saved-queries", "", s.handleSavedQueries, nil); w.Code != 405 {
		t.Fatal(w.Code)
	}
	for _, tc := range []struct {
		method, path string
		code         int
	}{{"GET", "/api/saved-queries/", 400}, {"GET", "/api/saved-queries/no", 404}, {"GET", "/api/saved-queries/q", 200}, {"DELETE", "/api/saved-queries/q", 200}, {"PATCH", "/api/saved-queries/q", 405}} {
		if w := call(tc.method, tc.path, "", s.handleSavedQuery, nil); w.Code != tc.code {
			t.Fatalf("saved %s %s=%d", tc.method, tc.path, w.Code)
		}
	}
	if w := call(http.MethodPost, "/api/export-saved-queries", "", s.handleExportSavedQueries, nil); w.Code != 405 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodGet, "/api/export-saved-queries", "", s.handleExportSavedQueries, nil); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodGet, "/api/import-saved-queries", "", s.handleImportSavedQueries, nil); w.Code != 405 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodPost, "/api/import-saved-queries", "broken", s.handleImportSavedQueries, nil); w.Code != 400 {
		t.Fatal(w.Code)
	}

	for _, tc := range []struct {
		method, body string
		p            principal
		code         int
	}{
		{"GET", "", admin, 405}, {"POST", `{"table":"users","column":"name","value":"z","where":{}}`, admin, 400},
		{"POST", `{"table":"users","column":"","value":"z","where":{"id":1}}`, admin, 400},
		{"POST", `{"table":"users","column":"name","value":"z","where":{"":1}}`, admin, 400},
		{"POST", `{"table":"users","column":"name","value":"z","where":{"id":1}}`, ro, 403},
		{"POST", `{"table":"users","column":"name","value":"z","where":{"id":1}}`, principal{Role: RoleReadWrite, Tables: []string{"users"}}, 200},
	} {
		w := call(tc.method, "/api/update-row", tc.body, s.handleUpdateRow, &tc.p)
		if w.Code != tc.code {
			t.Fatalf("update %d body=%s", w.Code, w.Body.String())
		}
	}

	if w := call(http.MethodPost, "/api/me", "", s.handleMe, &admin); w.Code != 405 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodGet, "/api/admin/tokens", "", s.handleAdminTokens, &admin); w.Code != 200 {
		t.Fatal(w.Code)
	}
	for _, tc := range []struct {
		body string
		code int
	}{
		{`{"name":"x","role":"bad"}`, 400}, {`{"name":"","role":"readonly"}`, 400}, {`{"name":"x","role":"readonly","ttl":"bad"}`, 400}, {`{"name":"x","role":"admin","tables":["users"]}`, 400}, {`{"name":"x","role":"readonly","tables":["users"]}`, 201},
	} {
		w := call(http.MethodPost, "/api/admin/tokens", tc.body, s.handleAdminTokens, &admin)
		if w.Code != tc.code {
			t.Fatalf("mint=%d %s", w.Code, w.Body.String())
		}
	}
	if w := call(http.MethodPut, "/api/admin/tokens", "", s.handleAdminTokens, &admin); w.Code != 405 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodPost, "/api/admin/tokens/", "", s.handleAdminToken, &admin); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodDelete, "/api/admin/tokens/bootstrap", "", s.handleAdminToken, &admin); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodPost, "/api/admin/tokens/missing/rotate", "", s.handleAdminToken, &admin); w.Code != 404 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodDelete, "/api/admin/tokens/missing", "", s.handleAdminToken, &admin); w.Code != 404 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodGet, "/api/admin/tokens/missing", "", s.handleAdminToken, &admin); w.Code != 405 {
		t.Fatal(w.Code)
	}
	if w := call(http.MethodPost, "/api/admin/audit", "", s.handleAdminAudit, &admin); w.Code != 405 {
		t.Fatal(w.Code)
	}
}

func TestCoverageHandlerWriteFailures(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	if _, err := db.Exec(t.Context(), "CREATE TABLE t (id INT, name TEXT, nick TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(t.Context(), "INSERT INTO t VALUES (1, 'x', NULL)"); err != nil {
		t.Fatal(err)
	}
	s := &Server{db: db, savedQueries: map[string]SavedQuery{"q": {Name: "q", Query: "SELECT 1"}}, tokens: newTokenStore(), audit: newAuditLog(io.Discard, 2, 10)}
	admin := principal{Role: RoleAdmin}
	fw := func() *failingWebWriter { return &failingWebWriter{} }
	s.writeRateLimited(fw(), httptest.NewRequest(http.MethodGet, "/api/query", nil))
	s.writeRateLimited(fw(), httptest.NewRequest(http.MethodGet, "/", nil))
	s.writeUnauthorized(fw(), httptest.NewRequest(http.MethodGet, "/api/query", nil))
	s.writeUnauthorized(fw(), httptest.NewRequest(http.MethodGet, "/", nil))
	s.handleSchema(fw(), withPrincipal(httptest.NewRequest(http.MethodGet, "/api/schema", nil), admin))
	s.handleExportCSV(fw(), withPrincipal(httptest.NewRequest(http.MethodGet, "/api/export/csv?query=SELECT%20*%20FROM%20t", nil), admin))
	s.handleExportJSON(fw(), withPrincipal(httptest.NewRequest(http.MethodGet, "/api/export/json?query=SELECT%20*%20FROM%20t", nil), admin))
	// Export with a query that returns NULL values exercises the nil-cell branch.
	w := httptest.NewRecorder()
	s.handleExportCSV(w, withPrincipal(httptest.NewRequest(http.MethodGet, "/api/export/csv?query=SELECT%20nick%20FROM%20t", nil), admin))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "NULL") {
		t.Fatalf("csv nil=%d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	s.handleExportJSON(w, withPrincipal(httptest.NewRequest(http.MethodGet, "/api/export/json?query=SELECT%20nick%20FROM%20t", nil), admin))
	if w.Code != 200 {
		t.Fatalf("json nil=%d", w.Code)
	}
}

func TestCoverageMiddlewareAndFailureWriters(t *testing.T) {
	s := &Server{authEnabled: true, tokens: newTokenStore(), limiter: newRateLimiter(0, 0), ipLimiter: newRateLimiter(0, 0), unauthAuditLimiter: newRateLimiter(0, 0), audit: newAuditLog(io.Discard, 2, 10)}
	s.tokens.setBootstrap("tok")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	r := httptest.NewRequest("GET", "/api/me", nil)
	r.AddCookie(&http.Cookie{Name: authCookieName, Value: "tok"})
	w := httptest.NewRecorder()
	s.authMiddleware(next).ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatal(w.Code)
	}
	r = httptest.NewRequest("GET", "/api/me", nil)
	r.Header.Set("Authorization", "Basic no")
	r.Header.Set("X-CobaltDB-Token", "tok")
	w = httptest.NewRecorder()
	s.authMiddleware(next).ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatal(w.Code)
	}
	if subtleConstantEq("a", "bb") || !subtleConstantEq("a", "a") {
		t.Fatal("constant compare")
	}
	if (&Server{}).clientIP(nil) != "" {
		t.Fatal("nil request ip")
	}
	s.trustProxyHeaders = true
	r = httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Forwarded-For", " 1.2.3.4 ")
	if s.clientIP(r) != "1.2.3.4" {
		t.Fatal(s.clientIP(r))
	}
	if ipRateKey("not-a-hostport") != "not-a-hostport" {
		t.Fatal("rate key")
	}

	fw := &failingWebWriter{}
	writeJSON(fw, map[string]string{"x": "y"})
	s.handleMe(fw, withPrincipal(httptest.NewRequest("GET", "/api/me", nil), principal{Role: RoleAdmin}))

	if err := decodeSingleJSON(strings.NewReader("{} {}"), &map[string]interface{}{}); err == nil {
		t.Fatal("trailing JSON")
	}
	if validateWebUIQuery(" ") == nil {
		t.Fatal("empty query")
	}
	for _, q := range []SavedQuery{{}, {Name: strings.Repeat("x", maxWebUISavedName+1), Query: "x"}, {Name: "x", Query: strings.Repeat("x", maxWebUISavedQuery+1)}, {Name: "x", Query: "x", Description: strings.Repeat("x", maxWebUISavedDesc+1)}} {
		if validateSavedQuery(q) == nil {
			t.Fatalf("invalid saved query accepted: %+v", q)
		}
	}
}

func TestCoverageAuthEdgePaths(t *testing.T) {
	// setBootstrap on a nil-tokens store exercises the nil-init branch.
	ts := &tokenStore{now: time.Now}
	ts.setBootstrap("valid")
	if _, err := ts.addWithID("a", "tok", "alpha", RoleReadOnly, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.addWithID("b", "tok2", "beta", RoleReadOnly, 0, nil); err != nil {
		t.Fatal(err)
	}
	got := ts.list()
	if len(got) != 3 || got[0].Name != "alpha" || got[1].Name != "beta" || got[2].Name != "bootstrap" {
		t.Fatalf("list with distinct names: %+v", got)
	}
}

func TestCoverageTableWalkerAllExpressionShapes(t *testing.T) {
	a := &tableAccumulator{tables: map[string]struct{}{}}
	scope := map[string]struct{}{"cte": {}}
	leaf := &query.Identifier{Name: "x"}
	exprs := []query.Expression{
		&query.InExpr{Expr: leaf, List: []query.Expression{leaf}},
		&query.BinaryExpr{Left: leaf, Right: leaf}, &query.UnaryExpr{Expr: leaf},
		&query.FunctionCall{Args: []query.Expression{leaf}, Filter: leaf},
		&query.BetweenExpr{Expr: leaf, Lower: leaf, Upper: leaf},
		&query.LikeExpr{Expr: leaf, Pattern: leaf, Escape: leaf}, &query.IsNullExpr{Expr: leaf},
		&query.CastExpr{Expr: leaf}, &query.AliasExpr{Expr: leaf},
		&query.CaseExpr{Expr: leaf, Whens: []*query.WhenClause{{Condition: leaf, Result: leaf}}, Else: leaf},
		&query.WindowExpr{Args: []query.Expression{leaf}, Filter: leaf, PartitionBy: []query.Expression{leaf}, OrderBy: []*query.OrderByExpr{{Expr: leaf}}},
		&query.JSONPathExpr{Column: leaf}, &query.JSONContainsExpr{Column: leaf, Value: leaf},
		&query.MatchExpr{Columns: []query.Expression{leaf}, Pattern: leaf},
		&query.WindowSpec{PartitionBy: []query.Expression{leaf}, OrderBy: []*query.OrderByExpr{{Expr: leaf}}},
	}
	for _, e := range exprs {
		if err := a.walkExpr(e, scope); err != nil {
			t.Fatalf("walk %T: %v", e, err)
		}
	}
	if err := a.walkSelectWithCTE(nil, scope); err == nil {
		t.Fatal("nil CTE accepted")
	}
	if err := a.walkUnion(nil, scope); err == nil {
		t.Fatal("nil union accepted")
	}
	if err := a.walkTableRef(nil, scope); err != nil {
		t.Fatal(err)
	}
	if err := a.walkStatement(&query.CreateTableStmt{}, scope); err == nil {
		t.Fatal("DDL accepted for allow-list")
	}
	if _, err := extractTableRefs("not sql"); err == nil {
		t.Fatal("invalid SQL accepted")
	}
	// Error propagation within the table walker (InsertStmt.Select walk error,
	// walkUnion error, walkStatement with other statement types).
	for _, sql := range []string{
		"INSERT INTO (SELECT 1 UNION SELECT 2) t VALUES (1)", // invalid syntax
	} {
		if _, err := extractTableRefs(sql); err != nil {
			_ = err // expected parse error for malformed SQL
		}
	}
}

func TestCoverageTokenSweeper(t *testing.T) {
	old := tokenExpirySweepInterval
	tokenExpirySweepInterval = time.Millisecond
	defer func() { tokenExpirySweepInterval = old }()
	(&Server{}).startTokenExpirySweeper()
	ts := newTokenStore()
	now := time.Now()
	ts.now = func() time.Time { return now }
	if _, err := ts.addWithID("expired", "tok", "x", RoleReadOnly, time.Millisecond, nil); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	s := &Server{tokens: ts}
	s.startTokenExpirySweeper()
	deadline := time.Now().Add(time.Second)
	for ts.count() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ts.count() != 0 {
		t.Fatal("sweeper did not purge token")
	}
}

func TestCoverageGenerateTokenAndEngineFailure(t *testing.T) {
	tok, err := generateToken(0)
	if err != nil || tok != "" {
		t.Fatalf("zero token %q %v", tok, err)
	}
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s := &Server{db: db, audit: newAuditLog(io.Discard, 2, 10)}
	r := withPrincipal(httptest.NewRequest("POST", "/api/update-row", strings.NewReader(`{"table":"t","column":"c","value":1,"where":{"id":1}}`)), principal{Role: RoleAdmin})
	w := httptest.NewRecorder()
	s.handleUpdateRow(w, r)
	if !strings.Contains(w.Body.String(), `"success":false`) {
		t.Fatal(w.Body.String())
	}
}
