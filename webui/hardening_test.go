package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// --- query classification + RBAC ---

func TestClassifyQuery(t *testing.T) {
	cases := []struct {
		sql  string
		want queryClass
	}{
		{"SELECT * FROM t", classRead},
		{"  with cte as (select 1) select * from cte", classRead},
		{"SHOW TABLES", classRead},
		{"DESCRIBE t", classRead},
		{"EXPLAIN SELECT 1", classRead},
		{"insert into t values (1)", classWrite},
		{"UPDATE t SET a=1", classWrite},
		{"DELETE FROM t", classWrite},
		{"REPLACE INTO t VALUES (1)", classWrite},
		{"CREATE TABLE t (id INT)", classDDL},
		{"DROP TABLE t", classDDL},
		{"ALTER TABLE t ADD COLUMN c INT", classDDL},
		{"/* sneaky */ DROP TABLE t", classDDL}, // unknown leading keyword -> ddl (fail-closed)
		{"", classDDL},
	}
	for _, c := range cases {
		if got := classifyQuery(c.sql); got != c.want {
			t.Errorf("classifyQuery(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestRoleAllows(t *testing.T) {
	cases := []struct {
		role  Role
		class queryClass
		want  bool
	}{
		{RoleAdmin, classRead, true},
		{RoleAdmin, classWrite, true},
		{RoleAdmin, classDDL, true},
		{RoleReadWrite, classRead, true},
		{RoleReadWrite, classWrite, true},
		{RoleReadWrite, classDDL, false},
		{RoleReadOnly, classRead, true},
		{RoleReadOnly, classWrite, false},
		{RoleReadOnly, classDDL, false},
	}
	for _, c := range cases {
		if got := c.role.allows(c.class); got != c.want {
			t.Errorf("%s.allows(%s) = %v, want %v", c.role, c.class, got, c.want)
		}
	}
}

// --- token store: expiry, rotation, revocation ---

func TestTokenStoreExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	ts := newTokenStore()
	ts.now = func() time.Time { return now }

	if _, err := ts.addWithID("u1", "tok-1", "user one", RoleReadOnly, time.Minute, nil); err != nil {
		t.Fatalf("addWithID: %v", err)
	}
	if _, ok := ts.resolve("tok-1"); !ok {
		t.Fatal("token should resolve before expiry")
	}

	now = now.Add(2 * time.Minute)
	if _, ok := ts.resolve("tok-1"); ok {
		t.Fatal("token should be rejected after expiry")
	}

	if removed := ts.purgeExpired(); removed != 1 {
		t.Fatalf("purgeExpired removed %d, want 1", removed)
	}
	if ts.count() != 0 {
		t.Fatalf("store count = %d, want 0 after purge", ts.count())
	}
}

func TestTokenStoreNoExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	ts := newTokenStore()
	ts.now = func() time.Time { return now }
	if _, err := ts.addWithID("u1", "tok-1", "user one", RoleAdmin, 0, nil); err != nil {
		t.Fatalf("addWithID: %v", err)
	}
	now = now.Add(1000 * time.Hour)
	if _, ok := ts.resolve("tok-1"); !ok {
		t.Fatal("ttl=0 token should never expire")
	}
}

func TestTokenStoreRotateInvalidatesOld(t *testing.T) {
	ts := newTokenStore()
	if _, err := ts.addWithID("u1", "old-token", "user", RoleReadWrite, 0, nil); err != nil {
		t.Fatalf("addWithID: %v", err)
	}
	newVal, p, ok := ts.rotate("u1")
	if !ok {
		t.Fatal("rotate should succeed for existing id")
	}
	if p.Role != RoleReadWrite {
		t.Fatalf("rotate must preserve role; got %s", p.Role)
	}
	if _, ok := ts.resolve("old-token"); ok {
		t.Fatal("old token must not resolve after rotation")
	}
	if _, ok := ts.resolve(newVal); !ok {
		t.Fatal("new token must resolve after rotation")
	}
	if _, _, ok := ts.rotate("missing"); ok {
		t.Fatal("rotate on missing id should fail")
	}
}

func TestTokenStoreRevoke(t *testing.T) {
	ts := newTokenStore()
	if _, err := ts.addWithID("u1", "tok", "user", RoleReadOnly, 0, nil); err != nil {
		t.Fatalf("addWithID: %v", err)
	}
	if !ts.revoke("u1") {
		t.Fatal("revoke should report success")
	}
	if _, ok := ts.resolve("tok"); ok {
		t.Fatal("revoked token must not resolve")
	}
	if ts.revoke("u1") {
		t.Fatal("revoke of missing id should report false")
	}
}

func TestTokenStoreMintReturnsUsableToken(t *testing.T) {
	ts := newTokenStore()
	val, p, err := ts.mint("ci", RoleReadOnly, time.Hour, nil)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if p.ID == "" || p.Name != "ci" || p.Role != RoleReadOnly {
		t.Fatalf("unexpected principal: %+v", p)
	}
	got, ok := ts.resolve(val)
	if !ok || got.ID != p.ID {
		t.Fatal("minted token should resolve to its principal")
	}
}

// --- rate limiter ---

func TestRateLimiterBurstThenThrottle(t *testing.T) {
	now := time.Unix(0, 0)
	rl := newRateLimiter(60, 3) // 1/sec, burst 3
	rl.now = func() time.Time { return now }

	// Burst of 3 should pass immediately.
	for i := 0; i < 3; i++ {
		if !rl.allow("p1") {
			t.Fatalf("request %d in burst should be allowed", i)
		}
	}
	// 4th in the same instant is throttled.
	if rl.allow("p1") {
		t.Fatal("4th request should be rate limited")
	}
	// After 1 second, one token refills.
	now = now.Add(time.Second)
	if !rl.allow("p1") {
		t.Fatal("request after 1s refill should be allowed")
	}
	if rl.allow("p1") {
		t.Fatal("second request after single refill should be throttled")
	}
}

func TestRateLimiterPerPrincipalIsolation(t *testing.T) {
	now := time.Unix(0, 0)
	rl := newRateLimiter(60, 1)
	rl.now = func() time.Time { return now }

	if !rl.allow("p1") {
		t.Fatal("p1 first request should pass")
	}
	if rl.allow("p1") {
		t.Fatal("p1 second request should be throttled")
	}
	if !rl.allow("p2") {
		t.Fatal("p2 must have its own independent bucket")
	}
}

func TestRateLimiterDisabled(t *testing.T) {
	rl := newRateLimiter(0, 0)
	for i := 0; i < 100; i++ {
		if !rl.allow("p1") {
			t.Fatal("disabled limiter must always allow")
		}
	}
}

// --- audit log ring buffer ---

func TestAuditLogRingBuffer(t *testing.T) {
	al := newAuditLog(nil, 3, 100)
	for i := 0; i < 5; i++ {
		al.record(auditEvent{PrincipalID: "p", Path: "/api/query", Outcome: "allowed", Detail: string(rune('a' + i))})
	}
	got := al.recent(10)
	if len(got) != 3 {
		t.Fatalf("ring should retain 3 events, got %d", len(got))
	}
	// Newest first: 'e', 'd', 'c'.
	if got[0].Detail != "e" || got[2].Detail != "c" {
		t.Fatalf("unexpected ordering: %q, %q", got[0].Detail, got[2].Detail)
	}
}

func TestAuditLogTruncatesSQL(t *testing.T) {
	al := newAuditLog(nil, 4, 5)
	al.record(auditEvent{SQL: "SELECT abcdefghij"})
	got := al.recent(1)
	if len(got) != 1 || !strings.HasSuffix(got[0].SQL, "…") || len([]rune(got[0].SQL)) != 6 {
		t.Fatalf("expected truncated SQL with ellipsis, got %q", got[0].SQL)
	}
}

// --- end-to-end middleware: rate limit + expiry through HTTP ---

func newAuthedServer(t *testing.T) *Server {
	t.Helper()
	srv := &Server{
		authEnabled: true,
		tokens:      newTokenStore(),
		limiter:     newRateLimiter(0, 0),
		audit:       newAuditLog(nil, 100, 100),
	}
	return srv
}

func TestMiddlewareRejectsExpiredToken(t *testing.T) {
	now := time.Unix(1000, 0)
	srv := newAuthedServer(t)
	srv.tokens.now = func() time.Time { return now }
	if _, err := srv.tokens.addWithID("u1", "exp-token", "u", RoleReadOnly, time.Minute, nil); err != nil {
		t.Fatalf("addWithID: %v", err)
	}

	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	now = now.Add(2 * time.Minute)
	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	req.Header.Set("X-CobaltDB-Token", "exp-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expired token status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestMiddlewareRateLimits(t *testing.T) {
	now := time.Unix(0, 0)
	srv := newAuthedServer(t)
	srv.limiter = newRateLimiter(60, 1)
	srv.limiter.now = func() time.Time { return now }
	if _, err := srv.tokens.addWithID("u1", "tok", "u", RoleReadOnly, 0, nil); err != nil {
		t.Fatalf("addWithID: %v", err)
	}

	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	do := func() int {
		req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
		req.Header.Set("X-CobaltDB-Token", "tok")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := do(); code != http.StatusNoContent {
		t.Fatalf("first request = %d, want 204", code)
	}
	if code := do(); code != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429", code)
	}
}

func TestMiddlewareStashesPrincipal(t *testing.T) {
	srv := newAuthedServer(t)
	if _, err := srv.tokens.addWithID("u1", "tok", "alice", RoleReadWrite, 0, nil); err != nil {
		t.Fatalf("addWithID: %v", err)
	}
	var seen principal
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = srv.effectivePrincipal(r)
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	req.Header.Set("X-CobaltDB-Token", "tok")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if seen.Name != "alice" || seen.Role != RoleReadWrite {
		t.Fatalf("middleware did not stash principal: %+v", seen)
	}
}

// --- RBAC enforcement on handleQuery ---

func TestHandleQueryRBACReadOnlyBlocksWrite(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	if _, err := db.Exec(t.Context(), "CREATE TABLE rbac_t (id INT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	srv := newAuthedServer(t)
	srv.db = db

	// A readonly principal issuing an INSERT must be forbidden.
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(`{"query":"INSERT INTO rbac_t VALUES (1)"}`))
	req = withPrincipal(req, principal{ID: "ro", Name: "ro", Role: RoleReadOnly})
	rec := httptest.NewRecorder()
	srv.handleQuery(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("readonly INSERT status = %d, want 403: %s", rec.Code, rec.Body.String())
	}

	// The same principal may SELECT.
	req2 := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(`{"query":"SELECT * FROM rbac_t"}`))
	req2 = withPrincipal(req2, principal{ID: "ro", Name: "ro", Role: RoleReadOnly})
	rec2 := httptest.NewRecorder()
	srv.handleQuery(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("readonly SELECT status = %d, want 200: %s", rec2.Code, rec2.Body.String())
	}
}

func TestHandleQueryRBACReadWriteBlocksDDL(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	srv := newAuthedServer(t)
	srv.db = db

	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(`{"query":"CREATE TABLE ddl_t (id INT)"}`))
	req = withPrincipal(req, principal{ID: "rw", Name: "rw", Role: RoleReadWrite})
	rec := httptest.NewRecorder()
	srv.handleQuery(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("readwrite DDL status = %d, want 403: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleQueryRecordsAudit(t *testing.T) {
	db := newMemDB(t)
	defer db.Close()
	if _, err := db.Exec(t.Context(), "CREATE TABLE audit_t (id INT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	srv := newAuthedServer(t)
	srv.db = db

	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(`{"query":"SELECT * FROM audit_t"}`))
	req = withPrincipal(req, principal{ID: "a", Name: "auditor", Role: RoleAdmin})
	srv.handleQuery(httptest.NewRecorder(), req)

	events := srv.audit.recent(10)
	if len(events) == 0 {
		t.Fatal("expected an audit event for the query")
	}
	ev := events[0]
	if ev.Principal != "auditor" || ev.QueryClass != "read" || ev.Outcome != "allowed" {
		t.Fatalf("unexpected audit event: %+v", ev)
	}
}

// --- admin token management endpoints ---

func TestAdminTokensRequireAdminRole(t *testing.T) {
	srv := newAuthedServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/tokens", nil)
	req = withPrincipal(req, principal{ID: "ro", Name: "ro", Role: RoleReadOnly})
	rec := httptest.NewRecorder()
	srv.handleAdminTokens(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-admin token list status = %d, want 403", rec.Code)
	}
}

func TestAdminMintAndUseToken(t *testing.T) {
	srv := newAuthedServer(t)

	body := `{"name":"reporting","role":"readonly","ttl":"1h"}`
	req := httptest.NewRequest(http.MethodPost, "/api/admin/tokens", strings.NewReader(body))
	req = withPrincipal(req, principal{ID: bootstrapTokenID, Name: "bootstrap", Role: RoleAdmin})
	rec := httptest.NewRecorder()
	srv.handleAdminTokens(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("mint status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Token     string    `json:"token"`
		Principal principal `json:"principal"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode mint response: %v", err)
	}
	if resp.Token == "" || resp.Principal.Role != RoleReadOnly {
		t.Fatalf("unexpected mint response: %+v", resp)
	}
	// The freshly minted token must authenticate.
	if _, ok := srv.tokens.resolve(resp.Token); !ok {
		t.Fatal("minted token should resolve in the store")
	}
}

func TestAdminMintRejectsBadRole(t *testing.T) {
	srv := newAuthedServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/tokens", strings.NewReader(`{"name":"x","role":"superuser"}`))
	req = withPrincipal(req, principal{ID: bootstrapTokenID, Role: RoleAdmin})
	rec := httptest.NewRecorder()
	srv.handleAdminTokens(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad role status = %d, want 400", rec.Code)
	}
}

func TestAdminRotateAndRevokeToken(t *testing.T) {
	srv := newAuthedServer(t)
	if _, err := srv.tokens.addWithID("svc1", "orig", "service", RoleReadWrite, 0, nil); err != nil {
		t.Fatalf("addWithID: %v", err)
	}

	// Rotate.
	req := httptest.NewRequest(http.MethodPost, "/api/admin/tokens/svc1/rotate", nil)
	req = withPrincipal(req, principal{ID: bootstrapTokenID, Role: RoleAdmin})
	rec := httptest.NewRecorder()
	srv.handleAdminToken(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rotate status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if _, ok := srv.tokens.resolve("orig"); ok {
		t.Fatal("original token should be invalid after rotate")
	}

	// Revoke.
	delReq := httptest.NewRequest(http.MethodDelete, "/api/admin/tokens/svc1", nil)
	delReq = withPrincipal(delReq, principal{ID: bootstrapTokenID, Role: RoleAdmin})
	delRec := httptest.NewRecorder()
	srv.handleAdminToken(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("revoke status = %d, want 200: %s", delRec.Code, delRec.Body.String())
	}
	if srv.tokens.count() != 0 {
		t.Fatalf("store should be empty after revoke, got %d", srv.tokens.count())
	}
}

func TestAdminCannotManageBootstrapToken(t *testing.T) {
	srv := newAuthedServer(t)
	srv.tokens.setBootstrap("boot")
	req := httptest.NewRequest(http.MethodDelete, "/api/admin/tokens/"+bootstrapTokenID, nil)
	req = withPrincipal(req, principal{ID: bootstrapTokenID, Role: RoleAdmin})
	rec := httptest.NewRecorder()
	srv.handleAdminToken(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("managing bootstrap token status = %d, want 400", rec.Code)
	}
	if _, ok := srv.tokens.resolve("boot"); !ok {
		t.Fatal("bootstrap token must remain valid")
	}
}

func TestAdminAuditEndpoint(t *testing.T) {
	srv := newAuthedServer(t)
	srv.audit.record(auditEvent{PrincipalID: "x", Path: "/api/query", Outcome: "allowed"})

	req := httptest.NewRequest(http.MethodGet, "/api/admin/audit?limit=5", nil)
	req = withPrincipal(req, principal{ID: bootstrapTokenID, Role: RoleAdmin})
	rec := httptest.NewRecorder()
	srv.handleAdminAudit(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("audit status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Events []auditEvent `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode audit response: %v", err)
	}
	if len(resp.Events) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(resp.Events))
	}
}

func TestAdminAuditRequiresAdmin(t *testing.T) {
	srv := newAuthedServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/audit", nil)
	req = withPrincipal(req, principal{ID: "rw", Role: RoleReadWrite})
	rec := httptest.NewRecorder()
	srv.handleAdminAudit(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-admin audit status = %d, want 403", rec.Code)
	}
}

func newMemDB(t *testing.T) *engine.DB {
	t.Helper()
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	return db
}
