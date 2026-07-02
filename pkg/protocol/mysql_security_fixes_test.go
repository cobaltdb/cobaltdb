package protocol

// Regression tests for MySQL wire-path security/correctness fixes:
//   1. Per-statement authorization + RLS user propagation (handleQuery,
//      handleStmtPrepare, handleStmtExecute, handleFieldList).
//   2. acceptLoop surviving transient Accept errors.

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/auth"
	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/security"
)

func TestRequiredMySQLPermission(t *testing.T) {
	tests := []struct {
		sql        string
		action     string
		needsCheck bool
	}{
		{"SELECT * FROM t", "SELECT", true},
		{"select 1", "SELECT", true},
		{"  WITH x AS (SELECT 1) SELECT * FROM x", "SELECT", true},
		{"SHOW TABLES", "SELECT", true},
		{"DESCRIBE t", "SELECT", true},
		{"EXPLAIN SELECT 1", "SELECT", true},
		{"INSERT INTO t VALUES (1)", "INSERT", true},
		{"REPLACE INTO t VALUES (1)", "INSERT", true},
		{"UPDATE t SET a = 1", "UPDATE", true},
		{"DELETE FROM t", "DELETE", true},
		{"CREATE TABLE t (a INT)", "CREATE", true},
		{"DROP TABLE t", "DROP", true},
		{"TRUNCATE TABLE t", "DROP", true},
		{"ALTER TABLE t ADD COLUMN b INT", "ALTER", true},
		// Session/transaction control needs no data permission.
		{"SET NAMES utf8", "", false},
		{"BEGIN", "", false},
		{"COMMIT", "", false},
		{"ROLLBACK", "", false},
		{"USE mydb", "", false},
		// Unknown statements must fail closed.
		{"GRANT SELECT ON t TO bob", "", true},
		{"FLUSH PRIVILEGES", "", true},
		{"", "", true},
	}
	for _, tt := range tests {
		action, needsCheck := requiredMySQLPermission(tt.sql)
		if action != tt.action || needsCheck != tt.needsCheck {
			t.Errorf("requiredMySQLPermission(%q) = (%q, %v); want (%q, %v)",
				tt.sql, action, needsCheck, tt.action, tt.needsCheck)
		}
	}
}

func newTestAuthorizer(t *testing.T) *auth.Authenticator {
	t.Helper()
	a := auth.NewAuthenticator()
	t.Cleanup(a.Stop)
	a.Enable()
	if err := a.CreateUser("boss", "Adm1n-Str0ng-P@ss!", true); err != nil {
		t.Fatalf("create admin: %v", err)
	}
	if err := a.CreateUser("reader", "R3ader-Str0ng-P@ss!", false); err != nil {
		t.Fatalf("create reader: %v", err)
	}
	if err := a.GrantPermission("reader", "", "", []string{"SELECT"}); err != nil {
		t.Fatalf("grant: %v", err)
	}
	return a
}

func TestAuthorizeStatementEnforcesPermissions(t *testing.T) {
	a := newTestAuthorizer(t)
	server := NewMySQLServer(nil, "test")
	server.SetAuthenticator(a)

	client := func(user string) *MySQLClient {
		return &MySQLClient{server: server, username: user}
	}

	// Admin may run everything.
	for _, sql := range []string{"SELECT 1", "DROP TABLE t", "GRANT X"} {
		if err := client("boss").authorizeStatement(sql); err != nil {
			t.Errorf("admin should be allowed %q, got %v", sql, err)
		}
	}

	// SELECT-only user.
	reader := client("reader")
	allowed := []string{"SELECT 1", "SHOW TABLES", "DESCRIBE t", "SET NAMES utf8", "BEGIN", "COMMIT"}
	for _, sql := range allowed {
		if err := reader.authorizeStatement(sql); err != nil {
			t.Errorf("reader should be allowed %q, got %v", sql, err)
		}
	}
	denied := []string{"DROP TABLE t", "INSERT INTO t VALUES (1)", "UPDATE t SET a=1",
		"DELETE FROM t", "CREATE TABLE x (a INT)", "ALTER TABLE t ADD b INT",
		"TRUNCATE TABLE t", "GRANT SELECT ON t TO bob"}
	for _, sql := range denied {
		if err := reader.authorizeStatement(sql); err == nil {
			t.Errorf("reader should be denied %q", sql)
		}
	}

	// Unknown user is denied.
	if err := client("ghost").authorizeStatement("SELECT 1"); err == nil {
		t.Error("unknown user should be denied")
	}

	// No authenticator configured => allow all (embedded / no-auth mode).
	openServer := NewMySQLServer(nil, "test")
	open := &MySQLClient{server: openServer, username: "anyone"}
	if err := open.authorizeStatement("DROP TABLE t"); err != nil {
		t.Errorf("no-auth server should allow all, got %v", err)
	}

	// Authenticator present but disabled => allow all.
	disabled := auth.NewAuthenticator()
	t.Cleanup(disabled.Stop)
	disabledServer := NewMySQLServer(nil, "test")
	disabledServer.SetAuthenticator(disabled)
	dc := &MySQLClient{server: disabledServer, username: "anyone"}
	if err := dc.authorizeStatement("DROP TABLE t"); err != nil {
		t.Errorf("disabled auth should allow all, got %v", err)
	}
}

func TestQueryContextCarriesRLSUser(t *testing.T) {
	c := &MySQLClient{username: "alice"}
	ctx, cancel := c.queryContext()
	defer cancel()
	if got, _ := ctx.Value(security.RLSUserKey).(string); got != "alice" {
		t.Errorf("queryContext RLS user = %q, want %q", got, "alice")
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Error("queryContext should carry a deadline")
	}

	anon := &MySQLClient{}
	ctx2, cancel2 := anon.queryContext()
	defer cancel2()
	if v := ctx2.Value(security.RLSUserKey); v != nil {
		t.Errorf("empty username should not set RLS user, got %v", v)
	}
}

// readPacketT reads one MySQL packet (header + payload) from r.
func readPacketT(t *testing.T, r io.Reader) []byte {
	t.Helper()
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		t.Fatalf("read packet header: %v", err)
	}
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		t.Fatalf("read packet payload: %v", err)
	}
	return payload
}

// TestHandleQueryDeniedForReadOnlyUser drives handleQuery directly over a pipe
// and asserts a SELECT-only user receives an access-denied error packet for
// DROP TABLE instead of the statement executing.
func TestHandleQueryDeniedForReadOnlyUser(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(t.Context(), "CREATE TABLE guarded (id INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	a := newTestAuthorizer(t)
	server := NewMySQLServer(db, "test")
	server.SetAuthenticator(a)

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &MySQLClient{conn: serverConn, server: server, username: "reader"}

	done := make(chan error, 1)
	go func() { done <- c.handleQuery("DROP TABLE guarded") }()

	payload := readPacketT(t, clientConn)
	if len(payload) < 3 || payload[0] != 0xff {
		t.Fatalf("expected error packet, got % x", payload)
	}
	code := binary.LittleEndian.Uint16(payload[1:3])
	if code != mysqlErrAccessDenied {
		t.Errorf("error code = %d, want %d", code, mysqlErrAccessDenied)
	}
	if !strings.Contains(string(payload), "access denied") {
		t.Errorf("error message should mention access denial: %q", payload)
	}
	if err := <-done; err != nil {
		t.Fatalf("handleQuery returned error: %v", err)
	}

	// The table must still exist: the DROP must not have run.
	rows, err := db.Query(t.Context(), "SELECT * FROM guarded")
	if err != nil {
		t.Fatalf("guarded table should still exist: %v", err)
	}
	_ = rows.Close()
}

// TestHandleQueryPropagatesRLSUser proves the wire path injects the
// authenticated username into the query context: with an owner-based RLS
// policy, two different connection users must see different rows. Before the
// fix the handler used a bare context and policies saw an empty user.
func TestHandleQueryPropagatesRLSUser(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		CoreStorage: engine.CoreStorage{InMemory: true},
		Security:    engine.Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer db.Close()
	for _, stmt := range []string{
		"CREATE TABLE docs (id INTEGER PRIMARY KEY, owner TEXT)",
		"INSERT INTO docs VALUES (1,'alice'),(2,'bob'),(3,'alice')",
		"CREATE POLICY p1 ON docs FOR ALL USING (owner = current_user())",
	} {
		if _, err := db.Exec(t.Context(), stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	server := NewMySQLServer(db, "test") // no authenticator: RLS still applies

	countRows := func(user string) int {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		c := &MySQLClient{conn: serverConn, server: server, username: user}
		done := make(chan error, 1)
		go func() { done <- c.handleQuery("SELECT id, owner FROM docs ORDER BY id") }()

		if err := clientConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set deadline: %v", err)
		}
		// Result set: column count, column defs, EOF, row packets, EOF.
		isEOF := func(p []byte) bool { return len(p) > 0 && len(p) < 9 && p[0] == 0xfe }
		first := readPacketT(t, clientConn)
		if len(first) > 0 && first[0] == 0xff {
			t.Fatalf("unexpected error packet for user %s: %q", user, first)
		}
		// Skip column definitions until first EOF.
		for {
			p := readPacketT(t, clientConn)
			if isEOF(p) {
				break
			}
		}
		rows := 0
		for {
			p := readPacketT(t, clientConn)
			if isEOF(p) {
				break
			}
			rows++
		}
		if err := <-done; err != nil {
			t.Fatalf("handleQuery(%s): %v", user, err)
		}
		return rows
	}

	if got := countRows("alice"); got != 2 {
		t.Errorf("alice sees %d rows, want 2 (RLS user not propagated?)", got)
	}
	if got := countRows("bob"); got != 1 {
		t.Errorf("bob sees %d rows, want 1 (RLS user not propagated?)", got)
	}
}

// flakyListener returns transient errors before yielding real connections,
// letting the test prove acceptLoop keeps accepting after Accept failures.
type flakyListener struct {
	results chan flakyResult
	closed  atomic.Bool
	addr    net.Addr
}

type flakyResult struct {
	conn net.Conn
	err  error
}

func (l *flakyListener) Accept() (net.Conn, error) {
	res, ok := <-l.results
	if !ok {
		return nil, net.ErrClosed
	}
	return res.conn, res.err
}

func (l *flakyListener) Close() error {
	if l.closed.CompareAndSwap(false, true) {
		close(l.results)
	}
	return nil
}

func (l *flakyListener) Addr() net.Addr { return l.addr }

func TestAcceptLoopSurvivesTransientErrors(t *testing.T) {
	fake := &flakyListener{
		results: make(chan flakyResult, 8),
		addr:    &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0},
	}

	server := NewMySQLServer(nil, "test")
	server.mu.Lock()
	server.listener = fake
	server.mu.Unlock()
	go server.acceptLoop()

	// Feed a few transient errors, then a real connection.
	fake.results <- flakyResult{err: errors.New("accept tcp: too many open files")}
	fake.results <- flakyResult{err: errors.New("accept tcp: too many open files")}

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	fake.results <- flakyResult{conn: serverConn}

	// If the loop survived the transient errors, the connection is handled and
	// the server sends its handshake packet.
	if err := clientConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	payload := readPacketT(t, clientConn)
	if len(payload) == 0 || payload[0] != 0x0a {
		t.Fatalf("expected protocol-10 handshake after transient accept errors, got % x", payload)
	}

	if err := server.Close(); err != nil {
		t.Fatalf("close server: %v", err)
	}
}

func TestAcceptLoopExitsOnClosedListener(t *testing.T) {
	fake := &flakyListener{
		results: make(chan flakyResult, 1),
		addr:    &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0},
	}
	server := NewMySQLServer(nil, "test")
	server.mu.Lock()
	server.listener = fake
	server.mu.Unlock()

	loopDone := make(chan struct{})
	go func() {
		server.acceptLoop()
		close(loopDone)
	}()

	// A closed listener (net.ErrClosed) must terminate the loop promptly.
	_ = fake.Close()

	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("acceptLoop did not exit after listener close")
	}
}
