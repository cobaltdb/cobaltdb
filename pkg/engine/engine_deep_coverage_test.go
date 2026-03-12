package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ---- DB Stats/Health/Metrics ----

func TestDBStats(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO t1 VALUES (1, 'test')")

	stats, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if !stats.InMemory {
		t.Error("expected InMemory=true")
	}
	if stats.Tables < 1 {
		t.Error("expected at least 1 table")
	}
}

func TestDBHealthCheck(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}

	err = db.HealthCheck()
	if err != nil {
		t.Errorf("expected healthy, got: %v", err)
	}

	if !db.IsHealthy() {
		t.Error("expected IsHealthy=true")
	}

	db.Close()

	err = db.HealthCheck()
	if err == nil {
		t.Error("expected error after close")
	}

	if db.IsHealthy() {
		t.Error("expected IsHealthy=false after close")
	}
}

func TestDBStatsAfterClose(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	_, err = db.Stats()
	if err == nil {
		t.Error("expected error after close")
	}
}

func TestDBGetMetrics(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data, err := db.GetMetrics()
	if err != nil {
		t.Errorf("expected success, got: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty metrics JSON")
	}

	coll := db.GetMetricsCollector()
	if coll == nil {
		t.Error("expected non-nil metrics collector")
	}
}

// ---- Shutdown ----

func TestDBShutdown(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.Shutdown(ctx)
	if err != nil {
		t.Errorf("expected clean shutdown, got: %v", err)
	}
}

func TestDBShutdownTimeout(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate active connection
	db.activeConns.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = db.Shutdown(ctx)
	// Should force close after timeout
	if err != nil {
		t.Logf("shutdown error (expected possible): %v", err)
	}
}

// ---- Circuit Breaker Stop and Get ----

func TestCircuitBreakerStop(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())

	// Before stop, ReportFailure counts
	cb.ReportFailure()
	stats := cb.Stats()
	if stats.Failures != 1 {
		t.Errorf("expected 1 failure, got %d", stats.Failures)
	}

	cb.Stop()

	// After stop, ReportSuccess/ReportFailure are no-ops
	cb.ReportFailure()
	cb.ReportSuccess()
	stats = cb.Stats()
	if stats.Failures != 1 {
		t.Errorf("expected 1 failure after stop, got %d", stats.Failures)
	}
}

func TestCircuitBreakerManagerGet(t *testing.T) {
	mgr := NewCircuitBreakerManager()

	// Get non-existent
	_, exists := mgr.Get("foo")
	if exists {
		t.Error("expected not found")
	}

	// Create one
	mgr.GetOrCreate("foo", DefaultCircuitBreakerConfig())

	// Now Get should find it
	cb, exists := mgr.Get("foo")
	if !exists {
		t.Error("expected found")
	}
	if cb == nil {
		t.Error("expected non-nil breaker")
	}
}

// ---- CircuitState String ----

func TestCircuitStateString(t *testing.T) {
	tests := []struct {
		state CircuitState
		want  string
	}{
		{CircuitClosed, "closed"},
		{CircuitOpen, "open"},
		{CircuitHalfOpen, "half-open"},
		{CircuitState(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("CircuitState(%d).String() = %s, want %s", tt.state, got, tt.want)
		}
	}
}

// ---- NonRetriableError Unwrap ----

func TestNonRetriableErrorUnwrap(t *testing.T) {
	inner := errors.New("inner error")
	nre := &NonRetriableError{Err: inner}

	if nre.Error() != "inner error" {
		t.Errorf("expected 'inner error', got '%s'", nre.Error())
	}

	unwrapped := nre.Unwrap()
	if unwrapped != inner {
		t.Error("expected Unwrap to return inner error")
	}

	if !IsNonRetriable(nre) {
		t.Error("expected IsNonRetriable=true")
	}

	if IsNonRetriable(inner) {
		t.Error("expected IsNonRetriable=false for plain error")
	}
}

// ---- Execute/Query error paths ----

func TestExecWithCancelledContext(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = db.Exec(ctx, "CREATE TABLE t (id INTEGER)")
	if err == nil {
		t.Error("expected error with cancelled context")
	}
}

func TestQueryWithCancelledContext(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = db.Query(ctx, "SELECT 1")
	if err == nil {
		t.Error("expected error with cancelled context")
	}
}

func TestExecShowTablesViaExec(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// SHOW TABLES should return error via Exec
	_, err = db.Exec(context.Background(), "SHOW TABLES")
	if err == nil {
		t.Error("expected error for SHOW TABLES via Exec")
	}
}

func TestExecSetVar(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// SET should succeed silently
	_, err = db.Exec(context.Background(), "SET NAMES utf8")
	if err != nil {
		t.Errorf("expected success for SET, got: %v", err)
	}
}

func TestExecUseDb(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// USE should succeed silently
	_, err = db.Exec(context.Background(), "USE testdb")
	if err != nil {
		t.Errorf("expected success for USE, got: %v", err)
	}
}

func TestQueryExplain(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	rows, err := db.Query(ctx, "EXPLAIN SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
}

func TestQueryShowDatabases(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(context.Background(), "SHOW DATABASES")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
}

func TestQueryDescribe(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE mytbl (id INTEGER PRIMARY KEY, name TEXT)")

	rows, err := db.Query(ctx, "DESCRIBE mytbl")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
}

func TestQueryShowColumns(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")

	rows, err := db.Query(ctx, "SHOW COLUMNS FROM t2")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
}

func TestQueryShowCreateTable(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE t3 (id INTEGER PRIMARY KEY, val TEXT)")

	rows, err := db.Query(ctx, "SHOW CREATE TABLE t3")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
}

// ---- ALTER TABLE paths ----

func TestExecAlterTableAddColumn(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE alt (id INTEGER PRIMARY KEY)")

	_, err = db.Exec(ctx, "ALTER TABLE alt ADD COLUMN name TEXT")
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecAlterTableDropColumn(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE alt2 (id INTEGER PRIMARY KEY, name TEXT, val TEXT)")

	_, err = db.Exec(ctx, "ALTER TABLE alt2 DROP COLUMN val")
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecAlterTableRename(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE old_name (id INTEGER PRIMARY KEY)")

	_, err = db.Exec(ctx, "ALTER TABLE old_name RENAME TO new_name")
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecAlterTableRenameColumn(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE ren (id INTEGER PRIMARY KEY, old_col TEXT)")

	_, err = db.Exec(ctx, "ALTER TABLE ren RENAME COLUMN old_col TO new_col")
	if err != nil {
		t.Fatal(err)
	}
}

// ---- Drop table, view, trigger, procedure, index via engine ----

func TestExecDropTableIfExists(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "DROP TABLE IF EXISTS nonexistent")
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecCreateDropView(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	_, err = db.Exec(ctx, "CREATE VIEW v1 AS SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "DROP VIEW v1")
	if err != nil {
		t.Fatal(err)
	}
	// Drop IF EXISTS on non-existent
	_, err = db.Exec(ctx, "DROP VIEW IF EXISTS nonexistent")
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecCreateDropTrigger(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	_, _ = db.Exec(ctx, "CREATE TABLE log (msg TEXT)")
	_, err = db.Exec(ctx, "CREATE TRIGGER trig1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES ('inserted'); END")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "DROP TRIGGER trig1")
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecCreateDropIndex(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE idx_t (id INTEGER PRIMARY KEY, val TEXT)")
	_, err = db.Exec(ctx, "CREATE INDEX idx1 ON idx_t (val)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "DROP INDEX idx1")
	if err != nil {
		t.Fatal(err)
	}
}

// ---- View IF NOT EXISTS ----

func TestExecCreateViewIfNotExists(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	_, _ = db.Exec(ctx, "CREATE VIEW v1 AS SELECT * FROM t")

	// Should not error with IF NOT EXISTS
	_, err = db.Exec(ctx, "CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
}

// ---- Double Close ----

func TestDBDoubleClose(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Double close should not panic or error
	if err := db.Close(); err != nil {
		t.Errorf("expected nil on double close, got: %v", err)
	}
}

// ---- Transaction via engine ----

func TestExecBeginCommitRollback(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	// COMMIT without txn
	_, err = db.Exec(ctx, "COMMIT")
	if err == nil {
		t.Error("expected error for COMMIT without txn")
	}

	// ROLLBACK without txn
	_, err = db.Exec(ctx, "ROLLBACK")
	if err == nil {
		t.Error("expected error for ROLLBACK without txn")
	}

	// SAVEPOINT without txn
	_, err = db.Exec(ctx, "SAVEPOINT sp1")
	if err == nil {
		t.Error("expected error for SAVEPOINT without txn")
	}

	// RELEASE without txn
	_, err = db.Exec(ctx, "RELEASE SAVEPOINT sp1")
	if err == nil {
		t.Error("expected error for RELEASE without txn")
	}

	// Double BEGIN
	_, err = db.Exec(ctx, "BEGIN")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "BEGIN")
	if err == nil {
		t.Error("expected error for double BEGIN")
	}

	_, err = db.Exec(ctx, "ROLLBACK")
	if err != nil {
		t.Fatal(err)
	}
}

// ---- VACUUM via engine ----

func TestExecVacuum(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(context.Background(), "VACUUM")
	if err != nil {
		t.Fatal(err)
	}
}

// ---- ANALYZE via engine ----

func TestExecAnalyze(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO t VALUES (1, 'a')")

	_, err = db.Exec(ctx, "ANALYZE t")
	if err != nil {
		t.Fatal(err)
	}
}
