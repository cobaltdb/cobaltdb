package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func newClassificationTestServer(t *testing.T) *ProductionServer {
	t.Helper()
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	config := DefaultProductionConfig()
	config.EnableHealthServer = false
	config.Lifecycle.EnableSignalHandling = false
	config.Lifecycle.ShutdownTimeout = time.Second
	config.Lifecycle.DrainTimeout = 100 * time.Millisecond
	config.Lifecycle.StartupTimeout = time.Second

	ps := NewProductionServer(db, config)
	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { ps.Stop() })
	return ps
}

// TestBreakerNotOpenedByClientErrors verifies that malformed SQL and unknown
// tables — client errors — do not open the shared statement-class breaker.
// Before the fix, 5 malformed SELECTs from any client blocked ALL SELECTs
// server-wide for 30s.
func TestBreakerNotOpenedByClientErrors(t *testing.T) {
	ps := newClassificationTestServer(t)
	ctx := context.Background()

	// Far more malformed statements than MaxFailures (default 5).
	for i := 0; i < 20; i++ {
		if _, err := ps.Query(ctx, "SELECT FRM WHERE OOPS"); err == nil {
			t.Fatal("expected parse error")
		}
		if _, err := ps.Query(ctx, "SELECT * FROM definitely_missing_table"); err == nil {
			t.Fatal("expected unknown-table error")
		}
		if _, err := ps.Exec(ctx, "INSERT INTO missing_table (x) VALUES (1)"); err == nil {
			t.Fatal("expected insert error")
		}
	}

	// Valid statements of the same classes must still work.
	if _, err := ps.Exec(ctx, "CREATE TABLE breaker_ok (id INTEGER)"); err != nil {
		t.Fatalf("DDL after client errors: %v", err)
	}
	if _, err := ps.Exec(ctx, "INSERT INTO breaker_ok (id) VALUES (1)"); err != nil {
		t.Fatalf("INSERT blocked by breaker after client errors: %v", err)
	}
	rows, err := ps.Query(ctx, "SELECT id FROM breaker_ok")
	if err != nil {
		t.Fatalf("SELECT blocked by breaker after client errors: %v", err)
	}
	rows.Close()

	// The SELECT breaker must be closed.
	if cb, ok := ps.CircuitBreakers.Get("SELECT"); ok {
		if cb.State() != engine.CircuitClosed {
			t.Fatalf("SELECT breaker opened by client errors: %s", cb.State())
		}
	}
}

// TestCircuitBreakerKeyBoundedClasses verifies the breaker key comes from a
// fixed set of statement classes, never raw client tokens.
func TestCircuitBreakerKeyBoundedClasses(t *testing.T) {
	ps := &ProductionServer{}
	cases := map[string]string{
		"SELECT * FROM t":             "SELECT",
		"  select 1":                  "SELECT",
		"WITH x AS (SELECT 1) SELECT": "SELECT",
		"ExPlAiN SELECT 1":            "SELECT",
		"INSERT INTO t VALUES (1)":    "INSERT",
		"replace into t values (1)":   "INSERT",
		"UPDATE t SET x = 1":          "UPDATE",
		"DELETE FROM t":               "DELETE",
		"CREATE TABLE t (x INTEGER)":  "DDL",
		"drop table t":                "DDL",
		"ALTER TABLE t ADD c INTEGER": "DDL",
		"GARBAGE!! ' OR 1=1 --":       "OTHER",
		"":                            "OTHER",
		"   \t\n":                     "OTHER",
		"§ütf8-junk":                  "OTHER",
	}
	allowed := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "DDL": true, "OTHER": true}
	for sql, want := range cases {
		got := ps.circuitBreakerKey(sql)
		if got != want {
			t.Errorf("circuitBreakerKey(%q) = %q, want %q", sql, got, want)
		}
		if !allowed[got] {
			t.Errorf("circuitBreakerKey(%q) = %q not in bounded class set", sql, got)
		}
	}
}

// TestIsCircuitBreakerFailureClassification verifies infrastructure errors
// count and client errors never do.
func TestIsCircuitBreakerFailureClassification(t *testing.T) {
	countable := []error{
		context.DeadlineExceeded,
		engine.ErrDatabaseClosed,
		errors.New("storage backend failure"),
		errors.New("query timeout exceeded"),
		errors.New("service unavailable"),
	}
	for _, err := range countable {
		if !isCircuitBreakerFailure(err) {
			t.Errorf("expected %q to count as breaker failure", err)
		}
	}

	notCountable := []error{
		nil,
		context.Canceled,
		errors.New("parse error: unexpected token"),
		errors.New("table not found"),
		errors.New("UNIQUE constraint failed: id"),
		errors.New("permission denied for table x"),
		errors.New("unknown column 'y'"),
	}
	for _, err := range notCountable {
		if isCircuitBreakerFailure(err) {
			t.Errorf("expected %q NOT to count as breaker failure", err)
		}
	}
}

// TestExecDoesNotRetryNonIdempotentFailures verifies Exec attempts a failing
// write exactly once unless the error is positively transient-and-not-applied.
func TestExecDoesNotRetryNonIdempotentFailures(t *testing.T) {
	ps := newClassificationTestServer(t)
	ctx := context.Background()

	if _, err := ps.Exec(ctx, "CREATE TABLE retry_once (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := ps.Exec(ctx, "INSERT INTO retry_once (id) VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// A duplicate-key insert is deterministic; with the old blanket retry it
	// was attempted MaxAttempts times. Measure indirectly via duration: the
	// default retry backoff starts at 100ms, so 3 attempts would take >200ms.
	start := time.Now()
	_, err := ps.Exec(ctx, "INSERT INTO retry_once (id) VALUES (1)")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected duplicate-key error")
	}
	if elapsed > 90*time.Millisecond {
		t.Fatalf("duplicate-key insert appears to have been retried with backoff (took %v)", elapsed)
	}
}
