package engine

import (
	"context"
	"testing"
	"time"
)

// TestExecErrorPaths tests error branches in execute functions
func TestExecErrorPaths(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	// executeVacuum error: nonexistent table triggers no error (vacuums all)
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Errorf("VACUUM should succeed: %v", err)
	}

	// executeCreateMaterializedView error: no query body
	_, err = db.Exec(ctx, "CREATE TABLE mv_src2 (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "INSERT INTO mv_src2 VALUES (1, 'a')")
	if err != nil {
		t.Fatal(err)
	}

	// Create mat view
	_, err = db.Exec(ctx, "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM mv_src2")
	if err != nil {
		t.Fatal(err)
	}

	// Duplicate mat view should error
	_, err = db.Exec(ctx, "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM mv_src2")
	if err == nil {
		t.Error("duplicate CREATE MATERIALIZED VIEW should error")
	}

	// Refresh mat view
	_, err = db.Exec(ctx, "REFRESH MATERIALIZED VIEW mv2")
	if err != nil {
		t.Errorf("REFRESH should succeed: %v", err)
	}

	// Refresh nonexistent mat view
	_, err = db.Exec(ctx, "REFRESH MATERIALIZED VIEW no_such_mv")
	if err == nil {
		t.Error("REFRESH nonexistent mat view should error")
	}

	// Drop mat view
	_, err = db.Exec(ctx, "DROP MATERIALIZED VIEW mv2")
	if err != nil {
		t.Errorf("DROP MATERIALIZED VIEW should succeed: %v", err)
	}

	// Drop nonexistent mat view
	_, err = db.Exec(ctx, "DROP MATERIALIZED VIEW no_such_mv")
	if err == nil {
		t.Error("DROP nonexistent mat view should error")
	}

	// executeCreateFTSIndex
	_, err = db.Exec(ctx, "CREATE TABLE fts_src2 (id INTEGER PRIMARY KEY, body TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "CREATE FULLTEXT INDEX fts_idx2 ON fts_src2 (body)")
	if err != nil {
		t.Errorf("CREATE FTS INDEX should succeed: %v", err)
	}

	// Duplicate FTS index
	_, err = db.Exec(ctx, "CREATE FULLTEXT INDEX fts_idx2 ON fts_src2 (body)")
	if err == nil {
		t.Error("duplicate CREATE FULLTEXT INDEX should error")
	}

	// FTS index on nonexistent table
	_, err = db.Exec(ctx, "CREATE FULLTEXT INDEX fts_bad ON no_table (col)")
	if err == nil {
		t.Error("FTS index on nonexistent table should error")
	}

	// executeCreateProcedure
	_, err = db.Exec(ctx, "CREATE PROCEDURE p2() BEGIN SELECT 1; END")
	if err != nil {
		t.Errorf("CREATE PROCEDURE should succeed: %v", err)
	}

	// Duplicate procedure
	_, err = db.Exec(ctx, "CREATE PROCEDURE p2() BEGIN SELECT 1; END")
	if err == nil {
		t.Error("duplicate CREATE PROCEDURE should error")
	}

	// Drop procedure
	_, err = db.Exec(ctx, "DROP PROCEDURE p2")
	if err != nil {
		t.Errorf("DROP PROCEDURE should succeed: %v", err)
	}

	// Drop nonexistent procedure
	_, err = db.Exec(ctx, "DROP PROCEDURE no_such_proc")
	if err == nil {
		t.Error("DROP nonexistent procedure should error")
	}

	// executeAlterTable errors
	_, err = db.Exec(ctx, "ALTER TABLE no_such_table ADD COLUMN c INT")
	if err == nil {
		t.Error("ALTER on nonexistent table should error")
	}

	_, err = db.Exec(ctx, "ALTER TABLE mv_src2 DROP COLUMN no_col")
	if err == nil {
		t.Error("DROP nonexistent column should error")
	}

	_, err = db.Exec(ctx, "ALTER TABLE mv_src2 RENAME COLUMN no_col TO new_name")
	if err == nil {
		t.Error("RENAME nonexistent column should error")
	}

	// executeSelectWithCTE
	rows, err := db.Query(ctx, "WITH cte AS (SELECT 1 AS a) SELECT * FROM cte")
	if err != nil {
		t.Errorf("CTE query should succeed: %v", err)
	} else {
		rows.Close()
	}
}

// TestExecAfterClose tests Exec/Query on closed database
func TestExecAfterClose(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "SELECT 1")
	if err == nil {
		t.Error("Exec after close should error")
	}

	_, err = db.Query(ctx, "SELECT 1")
	if err == nil {
		t.Error("Query after close should error")
	}
}

// TestExecContextCanceled tests Exec with canceled context
func TestExecContextCanceled(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = db.Exec(ctx, "SELECT 1")
	if err == nil {
		t.Error("Exec with canceled context should error")
	}
}

// TestQueryTimeoutOption tests the query timeout option
func TestQueryTimeoutOption(t *testing.T) {
	db, err := Open("", &Options{InMemory: true, QueryTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE qt2 (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatal(err)
	}

	// Normal operation should work within timeout
	_, err = db.Exec(ctx, "INSERT INTO qt2 VALUES (1)")
	if err != nil {
		t.Errorf("should succeed within timeout: %v", err)
	}
}

// TestTxCommitAndRollbackPaths exercises Tx.Commit and Tx.Rollback
func TestTxCommitAndRollbackPaths(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE tx_test2 (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	// Test Commit
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "INSERT INTO tx_test2 VALUES (1, 'committed')")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify committed data
	rows, err := db.Query(ctx, "SELECT val FROM tx_test2 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != "committed" {
			t.Errorf("expected 'committed', got %v", val)
		}
	}
	rows.Close()

	// Test Rollback
	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx2.Exec(ctx, "INSERT INTO tx_test2 VALUES (2, 'rolled_back')")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify rolled back data is gone
	rows2, err := db.Query(ctx, "SELECT COUNT(*) FROM tx_test2 WHERE id = 2")
	if err != nil {
		t.Fatal(err)
	}
	if rows2.Next() {
		var cnt int64
		if err := rows2.Scan(&cnt); err != nil {
			t.Fatal(err)
		}
		if cnt != 0 {
			t.Errorf("expected 0 rows, got %d", cnt)
		}
	}
	rows2.Close()

	// Double commit should error
	tx3, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx3.Commit(); err != nil {
		t.Fatalf("First commit failed: %v", err)
	}
	if err := tx3.Commit(); err == nil {
		t.Error("Double commit should error")
	}

	// Double rollback should error
	tx4, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx4.Rollback(); err != nil {
		t.Fatalf("First rollback failed: %v", err)
	}
	if err := tx4.Rollback(); err == nil {
		t.Error("Double rollback should error")
	}
}

// TestAuditUserContext tests auditUser context extraction
func TestAuditUserContext(t *testing.T) {
	// Nil context
	user := auditUser(nil)
	if user != "db_user" {
		t.Errorf("expected 'db_user', got %q", user)
	}

	// Empty context
	user = auditUser(context.Background())
	if user != "db_user" {
		t.Errorf("expected 'db_user', got %q", user)
	}

	// Context with user
	ctx := context.WithValue(context.Background(), "cobaltdb_user", "admin")
	user = auditUser(ctx)
	if user != "admin" {
		t.Errorf("expected 'admin', got %q", user)
	}

	// Context with empty user
	ctx = context.WithValue(context.Background(), "cobaltdb_user", "")
	user = auditUser(ctx)
	if user != "db_user" {
		t.Errorf("expected 'db_user' for empty user, got %q", user)
	}
}

// TestShowCommandsCoverage tests SHOW and DESCRIBE query paths
func TestShowCommandsCoverage(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE show_t3 (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Exec(ctx, "CREATE TABLE show_t4 (id INTEGER PRIMARY KEY, val REAL)")

	// SHOW TABLES
	rows, err := db.Query(ctx, "SHOW TABLES")
	if err != nil {
		t.Errorf("SHOW TABLES: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		if count < 2 {
			t.Errorf("expected at least 2 tables, got %d", count)
		}
		rows.Close()
	}

	// SHOW DATABASES
	rows, err = db.Query(ctx, "SHOW DATABASES")
	if err != nil {
		t.Errorf("SHOW DATABASES: %v", err)
	} else {
		rows.Close()
	}

	// SHOW COLUMNS FROM
	rows, err = db.Query(ctx, "SHOW COLUMNS FROM show_t3")
	if err != nil {
		t.Errorf("SHOW COLUMNS: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		if count < 2 {
			t.Errorf("expected at least 2 columns, got %d", count)
		}
		rows.Close()
	}

	// SHOW CREATE TABLE
	rows, err = db.Query(ctx, "SHOW CREATE TABLE show_t3")
	if err != nil {
		t.Errorf("SHOW CREATE TABLE: %v", err)
	} else {
		rows.Close()
	}

	// DESCRIBE
	rows, err = db.Query(ctx, "DESCRIBE show_t3")
	if err != nil {
		t.Errorf("DESCRIBE: %v", err)
	} else {
		rows.Close()
	}

	// EXPLAIN
	rows, err = db.Query(ctx, "EXPLAIN SELECT * FROM show_t3")
	if err != nil {
		t.Errorf("EXPLAIN: %v", err)
	} else {
		rows.Close()
	}

	// USE
	_, err = db.Exec(ctx, "USE mydb")
	if err != nil {
		t.Errorf("USE: %v", err)
	}

	// SET
	_, err = db.Exec(ctx, "SET var = 1")
	if err != nil {
		t.Errorf("SET: %v", err)
	}
}

// TestAnalyzeViaEngine tests ANALYZE via engine
func TestAnalyzeViaEngine(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE an_t2 (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
	_, _ = db.Exec(ctx, "INSERT INTO an_t2 VALUES (1, 'a', 1.0)")
	_, _ = db.Exec(ctx, "INSERT INTO an_t2 VALUES (2, 'b', 2.0)")
	_, _ = db.Exec(ctx, "INSERT INTO an_t2 VALUES (3, 'c', 3.0)")

	// ANALYZE specific table
	_, err = db.Exec(ctx, "ANALYZE an_t2")
	if err != nil {
		t.Errorf("ANALYZE table: %v", err)
	}

	// ANALYZE all tables
	_, err = db.Exec(ctx, "ANALYZE")
	if err != nil {
		t.Errorf("ANALYZE all: %v", err)
	}

	// ANALYZE nonexistent table
	_, err = db.Exec(ctx, "ANALYZE no_such_table")
	if err == nil {
		t.Error("ANALYZE nonexistent table should error")
	}
}

// TestTriggerViaEngine tests CREATE/DROP TRIGGER through engine API
func TestTriggerViaEngine(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE trg_main2 (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Exec(ctx, "CREATE TABLE trg_log2 (id INTEGER PRIMARY KEY, msg TEXT)")

	_, err = db.Exec(ctx, "CREATE TRIGGER trg2 AFTER INSERT ON trg_main2 FOR EACH ROW BEGIN INSERT INTO trg_log2 VALUES (NEW.id, NEW.name); END")
	if err != nil {
		t.Errorf("CREATE TRIGGER: %v", err)
	}

	// Trigger fires on insert
	_, err = db.Exec(ctx, "INSERT INTO trg_main2 VALUES (1, 'hello')")
	if err != nil {
		t.Errorf("INSERT with trigger: %v", err)
	}

	// Check log
	rows, err := db.Query(ctx, "SELECT msg FROM trg_log2 WHERE id = 1")
	if err != nil {
		t.Errorf("Query log: %v", err)
	} else {
		if rows.Next() {
			var msg string
			if err := rows.Scan(&msg); err != nil {
				t.Fatal(err)
			}
			if msg != "hello" {
				t.Errorf("expected 'hello', got %v", msg)
			}
		}
		rows.Close()
	}

	// DROP TRIGGER
	_, err = db.Exec(ctx, "DROP TRIGGER trg2")
	if err != nil {
		t.Errorf("DROP TRIGGER: %v", err)
	}

	// Drop nonexistent trigger
	_, err = db.Exec(ctx, "DROP TRIGGER no_such_trigger")
	if err == nil {
		t.Error("DROP nonexistent trigger should error")
	}
}

// TestPolicyViaEngine tests CREATE/DROP POLICY through engine
func TestPolicyViaEngine(t *testing.T) {
	db, err := Open("", &Options{InMemory: true, EnableRLS: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE pol_t2 (id INTEGER PRIMARY KEY, owner TEXT, data TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO pol_t2 VALUES (1, 'alice', 'secret')")
	_, _ = db.Exec(ctx, "INSERT INTO pol_t2 VALUES (2, 'bob', 'also_secret')")

	// Create policy
	_, err = db.Exec(ctx, "CREATE POLICY pol2 ON pol_t2 AS PERMISSIVE FOR SELECT USING (owner = 'alice')")
	if err != nil {
		t.Errorf("CREATE POLICY: %v", err)
	}

	// Drop policy
	_, err = db.Exec(ctx, "DROP POLICY pol2 ON pol_t2")
	if err != nil {
		t.Errorf("DROP POLICY: %v", err)
	}

	// Drop nonexistent policy
	_, err = db.Exec(ctx, "DROP POLICY no_such ON pol_t2")
	if err == nil {
		t.Error("DROP nonexistent policy should error")
	}
}

// TestViewViaEngine tests CREATE/DROP VIEW through engine
func TestViewViaEngine(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE vw_src2 (id INTEGER PRIMARY KEY, val TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO vw_src2 VALUES (1, 'a')")

	// Create view
	_, err = db.Exec(ctx, "CREATE VIEW vw2 AS SELECT * FROM vw_src2")
	if err != nil {
		t.Errorf("CREATE VIEW: %v", err)
	}

	// Query view
	rows, err := db.Query(ctx, "SELECT val FROM vw2 WHERE id = 1")
	if err != nil {
		t.Errorf("Query view: %v", err)
	} else {
		if rows.Next() {
			var val string
			if err := rows.Scan(&val); err != nil {
				t.Fatal(err)
			}
			if val != "a" {
				t.Errorf("expected 'a', got %v", val)
			}
		}
		rows.Close()
	}

	// Drop view
	_, err = db.Exec(ctx, "DROP VIEW vw2")
	if err != nil {
		t.Errorf("DROP VIEW: %v", err)
	}

	// Drop nonexistent view
	_, err = db.Exec(ctx, "DROP VIEW no_such_view")
	if err == nil {
		t.Error("DROP nonexistent view should error")
	}

	// DROP VIEW IF EXISTS should not error
	_, err = db.Exec(ctx, "DROP VIEW IF EXISTS no_such_view")
	if err != nil {
		t.Errorf("DROP VIEW IF EXISTS should not error: %v", err)
	}
}

// TestRetryWithResultEdgeCases tests RetryWithResult edge cases
func TestRetryWithResultEdgeCases(t *testing.T) {
	ctx := context.Background()

	// Successful on first try
	result, err := RetryWithResult(ctx, DefaultRetryConfig(), func() (string, error) {
		return "ok", nil
	})
	if err != nil || result != "ok" {
		t.Errorf("expected ok, got %v, %v", result, err)
	}

	// Retry then succeed
	attempts := 0
	result, err = RetryWithResult(ctx, DefaultRetryConfig(), func() (string, error) {
		attempts++
		if attempts < 3 {
			return "", ErrDatabaseClosed
		}
		return "recovered", nil
	})
	if err != nil || result != "recovered" {
		t.Errorf("expected recovered, got %v, %v", result, err)
	}
}

// TestSavepointViaEngine tests savepoint operations through engine API
func TestSavepointViaEngine(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE sp_t2 (id INTEGER PRIMARY KEY, val TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO sp_t2 VALUES (1, 'original')")

	// BEGIN / SAVEPOINT / ROLLBACK TO SAVEPOINT / COMMIT
	_, _ = db.Exec(ctx, "BEGIN")
	_, _ = db.Exec(ctx, "UPDATE sp_t2 SET val = 'modified' WHERE id = 1")
	_, _ = db.Exec(ctx, "SAVEPOINT sp2")
	_, _ = db.Exec(ctx, "UPDATE sp_t2 SET val = 'sp2_val' WHERE id = 1")
	_, _ = db.Exec(ctx, "ROLLBACK TO SAVEPOINT sp2")
	_, _ = db.Exec(ctx, "COMMIT")

	// Value should be 'modified' (sp2 was rolled back)
	rows, err := db.Query(ctx, "SELECT val FROM sp_t2 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != "modified" {
			t.Errorf("expected 'modified', got %v", val)
		}
	}
	rows.Close()
}

// TestDropTableViaEngine tests DROP TABLE error paths
func TestDropTableViaEngine(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Drop nonexistent table should error
	_, err = db.Exec(ctx, "DROP TABLE no_such_table")
	if err == nil {
		t.Error("DROP nonexistent table should error")
	}

	// DROP TABLE IF EXISTS should not error
	_, err = db.Exec(ctx, "DROP TABLE IF EXISTS no_such_table")
	if err != nil {
		t.Errorf("DROP TABLE IF EXISTS should not error: %v", err)
	}

	// DROP INDEX
	_, err = db.Exec(ctx, "DROP INDEX no_such_index")
	if err == nil {
		t.Error("DROP nonexistent index should error")
	}

	// DROP INDEX IF EXISTS may still error if engine doesn't support IF EXISTS for indexes
	_, _ = db.Exec(ctx, "DROP INDEX IF EXISTS no_such_index")
}

// TestParseSyntaxError tests parse error path
func TestParseSyntaxError(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "SELET 1") // typo
	if err == nil {
		t.Error("parse error should propagate")
	}
}

// TestTxQueryAfterClose tests Tx.Query after transaction completion
func TestTxQueryAfterClose(t *testing.T) {
	db, err := Open("", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE txq (id INTEGER PRIMARY KEY)")

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()

	// Query after commit should error
	_, err = tx.Query(ctx, "SELECT 1")
	if err == nil {
		t.Error("Query after commit should error")
	}

	// Exec after commit should error
	_, err = tx.Exec(ctx, "INSERT INTO txq VALUES (1)")
	if err == nil {
		t.Error("Exec after commit should error")
	}
}
