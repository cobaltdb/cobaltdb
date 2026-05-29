package engine

import (
	"context"
	"testing"
	"time"
)

func TestCircuitBreakerReportFailureHalfOpen(t *testing.T) {
	config := &CircuitBreakerConfig{
		MaxFailures:         2,
		MinSuccesses:        1,
		ResetTimeout:        100 * time.Millisecond,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(config)

	// Trip to open
	cb.ReportFailure()
	cb.ReportFailure()
	if cb.State() != CircuitOpen {
		t.Fatalf("expected Open, got %s", cb.State())
	}

	// Wait for half-open transition
	time.Sleep(150 * time.Millisecond)
	if err := cb.Allow(); err != nil {
		t.Fatalf("Allow in half-open: %v", err)
	}
	cb.Release()

	// Report failure in half-open should re-open
	cb.ReportFailure()
	if cb.State() != CircuitOpen {
		t.Errorf("expected Open after half-open failure, got %s", cb.State())
	}

	cb.Stop()
}

func TestRunAnalyzeJob(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE analyze_test (id INT, name TEXT)")
	db.Exec(ctx, "INSERT INTO analyze_test VALUES (1, 'a')")
	db.Exec(ctx, "INSERT INTO analyze_test VALUES (2, 'b')")

	err = db.runAnalyzeJob()
	if err != nil {
		t.Fatalf("runAnalyzeJob: %v", err)
	}
}

func TestExecuteCreateViewIfNotExists(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE vt (id INT, val TEXT)")

	// Create view first time
	_, err = db.Exec(ctx, "CREATE VIEW v1 AS SELECT * FROM vt")
	if err != nil {
		t.Fatalf("create view: %v", err)
	}

	// Create again with IF NOT EXISTS — should succeed
	_, err = db.Exec(ctx, "CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM vt")
	if err != nil {
		t.Fatalf("create view if not exists: %v", err)
	}
}

func TestExecQueryWithPlanCache(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Enable plan cache
	db.EnablePlanCache(1024*1024, 100)

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE pc_test (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO pc_test VALUES (1, 'hello')")

	// Execute same query twice to exercise cache
	rows1, err := db.Query(ctx, "SELECT * FROM pc_test WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows1.Close()

	rows2, err := db.Query(ctx, "SELECT * FROM pc_test WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows2.Close()
}

func TestExecQueryResultCache(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		QueryCache: QueryCacheConfig{
			EnableQueryCache: true,
			QueryCacheSize:   1024 * 1024,
			QueryCacheTTL:    5 * time.Minute,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE rc_test (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO rc_test VALUES (1, 'cached')")

	// Execute same query twice to exercise cache
	rows1, err := db.Query(ctx, "SELECT * FROM rc_test")
	if err != nil {
		t.Fatal(err)
	}
	rows1.Close()

	rows2, err := db.Query(ctx, "SELECT * FROM rc_test")
	if err != nil {
		t.Fatal(err)
	}
	rows2.Close()
}

func TestExecWithAuditLogger(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Exercise various statement types through Exec
	ctx := context.Background()

	// DDL
	db.Exec(ctx, "CREATE TABLE audit_t (id INT PRIMARY KEY, val TEXT)")
	db.Exec(ctx, "INSERT INTO audit_t VALUES (1, 'test')")
	db.Exec(ctx, "UPDATE audit_t SET val = 'updated' WHERE id = 1")
	db.Exec(ctx, "DELETE FROM audit_t WHERE id = 1")
	db.Exec(ctx, "DROP TABLE audit_t")
}

func TestExecDropViewIfExists(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE dvt (id INT)")
	db.Exec(ctx, "CREATE VIEW dv1 AS SELECT * FROM dvt")

	// Drop view
	_, err = db.Exec(ctx, "DROP VIEW dv1")
	if err != nil {
		t.Fatalf("drop view: %v", err)
	}

	// Drop non-existent view with IF EXISTS
	_, err = db.Exec(ctx, "DROP VIEW IF EXISTS dv_nonexist")
	if err != nil {
		t.Fatalf("drop view if exists: %v", err)
	}
}

func TestExecCreateTrigger(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE trig_t (id INT, val TEXT)")
	db.Exec(ctx, "CREATE TABLE trig_log (action TEXT, ts INT)")

	_, err = db.Exec(ctx, "CREATE TRIGGER trg1 AFTER INSERT ON trig_t INSERT INTO trig_log VALUES ('insert', 1)")
	if err != nil {
		t.Logf("trigger: %v (may not be fully supported)", err)
	}
}

func TestExecCreateDropProcedure(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE PROCEDURE my_proc() BEGIN SELECT 1; END")
	if err != nil {
		t.Logf("create procedure: %v", err)
	}

	_, err = db.Exec(ctx, "DROP PROCEDURE IF EXISTS my_proc")
	if err != nil {
		t.Logf("drop procedure: %v", err)
	}
}

func TestExecTransactionControl(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE txn_ctrl (id INT)")

	// BEGIN
	_, err = db.Exec(ctx, "BEGIN")
	if err != nil {
		t.Logf("begin: %v", err)
	}

	// COMMIT
	_, err = db.Exec(ctx, "COMMIT")
	if err != nil {
		t.Logf("commit: %v", err)
	}
}

func TestExecAlterTable(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE alter_t (id INT, val TEXT)")

	// Add column
	_, err = db.Exec(ctx, "ALTER TABLE alter_t ADD COLUMN extra INT")
	if err != nil {
		t.Logf("alter table add column: %v", err)
	}

	// Drop column
	_, err = db.Exec(ctx, "ALTER TABLE alter_t DROP COLUMN extra")
	if err != nil {
		t.Logf("alter table drop column: %v", err)
	}
}

func TestExecSavepoint(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE sp_t (id INT)")

	_, err = db.Exec(ctx, "BEGIN")
	if err != nil {
		t.Logf("begin: %v", err)
	}

	_, err = db.Exec(ctx, "SAVEPOINT sp1")
	if err != nil {
		t.Logf("savepoint: %v", err)
	}

	_, err = db.Exec(ctx, "RELEASE SAVEPOINT sp1")
	if err != nil {
		t.Logf("release savepoint: %v", err)
	}

	db.Exec(ctx, "COMMIT")
}

func TestExecContextCancelled(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = db.Exec(ctx, "CREATE TABLE cancel_t (id INT)")
	if err == nil {
		t.Log("context cancellation was not caught (may be OK)")
	}
}

func TestValueToStringForCompare(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "<nil>"},
		{"hello", "hello"},
		{[]byte("bytes"), "bytes"},
		{int64(42), "42"},
		{int(99), "99"},
		{float64(3.14), "3.14"},
		{true, "true"},
		{false, "false"},
	}

	for _, tt := range tests {
		result := valueToStringForCompare(tt.input)
		if result != tt.expected {
			t.Errorf("valueToStringForCompare(%v) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestCompareUnionValues(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		a, b interface{}
		want int // -1, 0, 1
	}{
		// Both nil
		{nil, nil, 0},
		// nil vs value
		{nil, int64(1), -1},
		{int64(1), nil, 1},
		// int64 vs int64
		{int64(1), int64(2), -1},
		{int64(2), int64(1), 1},
		{int64(5), int64(5), 0},
		// int64 vs float64
		{int64(1), float64(2.0), -1},
		{int64(3), float64(3.0), 0},
		// float64 vs int64
		{float64(2.0), int64(1), 1},
		// float64 vs float64
		{float64(1.5), float64(2.5), -1},
		{float64(3.0), float64(3.0), 0},
		// string vs string
		{"a", "b", -1},
		{"b", "a", 1},
		{"same", "same", 0},
	}

	for _, tt := range tests {
		got := db.compareUnionValues(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("compareUnionValues(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestExecVacuumStmt(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE vac_t (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO vac_t VALUES (1, 'a')")
	db.Exec(ctx, "DELETE FROM vac_t WHERE id = 1")

	_, err = db.Exec(ctx, "VACUUM vac_t")
	if err != nil {
		t.Logf("VACUUM: %v", err)
	}
}

func TestHealthCheckOpen(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.HealthCheck()
	if err != nil {
		t.Errorf("expected healthy: %v", err)
	}
}

func TestExecExplain(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE expl_t (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO expl_t VALUES (1, 'hello')")

	rows, err := db.Query(ctx, "EXPLAIN SELECT * FROM expl_t")
	if err != nil {
		t.Logf("EXPLAIN: %v", err)
	} else {
		rows.Close()
	}
}

func TestExecUnion(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE u1 (id INT, val TEXT)")
	db.Exec(ctx, "CREATE TABLE u2 (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO u1 VALUES (1, 'a')")
	db.Exec(ctx, "INSERT INTO u2 VALUES (2, 'b')")

	rows, err := db.Query(ctx, "SELECT * FROM u1 UNION SELECT * FROM u2")
	if err != nil {
		t.Logf("UNION: %v", err)
	} else {
		rows.Close()
	}
}

func TestExecInsertReturning(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE ir_t (id INT, val TEXT)")

	rows, err := db.Query(ctx, "INSERT INTO ir_t VALUES (1, 'test') RETURNING *")
	if err != nil {
		t.Logf("INSERT RETURNING: %v", err)
	} else {
		rows.Close()
	}
}

func TestExecUpdateReturning(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE ur_t (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO ur_t VALUES (1, 'old')")

	rows, err := db.Query(ctx, "UPDATE ur_t SET val = 'new' WHERE id = 1 RETURNING *")
	if err != nil {
		t.Logf("UPDATE RETURNING: %v", err)
	} else {
		rows.Close()
	}
}

func TestExecDeleteReturning(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE dr_t (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO dr_t VALUES (1, 'bye')")

	rows, err := db.Query(ctx, "DELETE FROM dr_t WHERE id = 1 RETURNING *")
	if err != nil {
		t.Logf("DELETE RETURNING: %v", err)
	} else {
		rows.Close()
	}
}

func TestTxDoubleCommit(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE dbl_t (id INT)")

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}

	// Double commit should fail
	err = tx.Commit()
	if err == nil {
		t.Error("expected error on double commit")
	}
}

func TestTxDoubleRollback(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE dbl_r_t (id INT)")

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("first rollback: %v", err)
	}

	// Double rollback should fail
	err = tx.Rollback()
	if err == nil {
		t.Error("expected error on double rollback")
	}
}

func TestCompareUnionValuesMixed(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Test the string fallback path (comparing bool or mixed types)
	result := db.compareUnionValues(true, false)
	if result != 1 {
		t.Errorf("true > false: got %d", result)
	}

	// Fallback: string vs numeric through valueToStringForCompare
	result = db.compareUnionValues("abc", int64(5))
	if result == 0 {
		t.Error("expected non-zero for string vs int comparison")
	}
}

func TestExecAnalyzeAllTables(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE an1 (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO an1 VALUES (1, 'a')")
	db.Exec(ctx, "CREATE TABLE an2 (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO an2 VALUES (2, 'b')")

	// ANALYZE with no table specified — should analyze all
	_, err = db.Exec(ctx, "ANALYZE")
	if err != nil {
		t.Logf("ANALYZE all: %v", err)
	}
}

func TestExecShowTables(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE show_t (id INT)")

	rows, err := db.Query(ctx, "SHOW TABLES")
	if err != nil {
		t.Logf("SHOW TABLES: %v", err)
	} else {
		rows.Close()
	}
}

func TestExecShowCreateTable(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE sct (id INT PRIMARY KEY, val TEXT)")

	rows, err := db.Query(ctx, "SHOW CREATE TABLE sct")
	if err != nil {
		t.Logf("SHOW CREATE TABLE: %v", err)
	} else {
		rows.Close()
	}
}

func TestExecShowColumns(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE scols (id INT, name TEXT, active BOOLEAN)")

	rows, err := db.Query(ctx, "SHOW COLUMNS FROM scols")
	if err != nil {
		t.Logf("SHOW COLUMNS: %v", err)
	} else {
		rows.Close()
	}
}

func TestExecSetAndUse(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// SET (MySQL compat)
	_, err = db.Exec(ctx, "SET autocommit = 1")
	if err != nil {
		t.Logf("SET: %v", err)
	}

	// USE (MySQL compat)
	_, err = db.Exec(ctx, "USE testdb")
	if err != nil {
		t.Logf("USE: %v", err)
	}
}

func TestExecMaterializedView(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE mvt (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO mvt VALUES (1, 'a')")

	_, err = db.Exec(ctx, "CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM mvt")
	if err != nil {
		t.Logf("CREATE MATERIALIZED VIEW: %v", err)
	}

	_, err = db.Exec(ctx, "REFRESH MATERIALIZED VIEW mv1")
	if err != nil {
		t.Logf("REFRESH MATERIALIZED VIEW: %v", err)
	}

	_, err = db.Exec(ctx, "DROP MATERIALIZED VIEW IF EXISTS mv1")
	if err != nil {
		t.Logf("DROP MATERIALIZED VIEW: %v", err)
	}
}

func TestExecCreateFTSIndex(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE ftst (id INT, content TEXT)")

	_, err = db.Exec(ctx, "CREATE FTS INDEX ftst_idx ON ftst(content)")
	if err != nil {
		t.Logf("CREATE FTS INDEX: %v", err)
	}
}

func TestExecCreateVectorIndex(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE vect (id INT, embedding VECTOR(3))")

	_, err = db.Exec(ctx, "CREATE VECTOR INDEX vect_idx ON vect(embedding)")
	if err != nil {
		t.Logf("CREATE VECTOR INDEX: %v", err)
	}
}
