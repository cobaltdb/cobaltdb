package test

import (
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestStoredProcedure_Basic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create a test table
	afExec(t, db, ctx, "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

	t.Run("Create and call simple procedure", func(t *testing.T) {
		// Create procedure that inserts data
		afExec(t, db, ctx, "CREATE PROCEDURE insert_test() BEGIN INSERT INTO test_table VALUES (1, 'test'); END")

		// Call the procedure
		afExec(t, db, ctx, "CALL insert_test()")

		// Verify data was inserted
		rows := afQuery(t, db, ctx, "SELECT * FROM test_table WHERE id = 1")
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})

	t.Run("Drop procedure", func(t *testing.T) {
		afExec(t, db, ctx, "DROP PROCEDURE insert_test")

		// Verify procedure was dropped by trying to call it
		_, err := db.Exec(ctx, "CALL insert_test()")
		if err == nil {
			t.Error("Expected error calling dropped procedure")
		}
	})
}

func TestStoredProcedure_WithParams(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE params_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")

	t.Run("Create procedure with parameters", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE PROCEDURE insert_with_params(p_id INTEGER, p_name TEXT, p_value INTEGER) BEGIN INSERT INTO params_table VALUES (p_id, p_name, p_value); END")

		// Call with literal arguments
		afExec(t, db, ctx, "CALL insert_with_params(1, 'hello', 100)")

		rows := afQuery(t, db, ctx, "SELECT * FROM params_table WHERE id = 1")
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
		if len(rows) > 0 {
			t.Logf("Row: %v", rows[0])
		}
	})

	t.Run("Procedure with different param values", func(t *testing.T) {
		afExec(t, db, ctx, "CALL insert_with_params(2, 'world', 200)")

		rows := afQuery(t, db, ctx, "SELECT * FROM params_table WHERE id = 2")
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})
}

func TestStoredProcedure_IF_NOT_EXISTS(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	t.Run("CREATE PROCEDURE IF NOT EXISTS", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE IF NOT EXISTS t (id INTEGER)")
		afExec(t, db, ctx, "CREATE PROCEDURE IF NOT EXISTS test_proc() BEGIN INSERT INTO t VALUES (1); END")

		// Should succeed when creating again with IF NOT EXISTS
		afExec(t, db, ctx, "CREATE PROCEDURE IF NOT EXISTS test_proc() BEGIN INSERT INTO t VALUES (2); END")

		// Call should still work (using the first procedure)
		afExec(t, db, ctx, "CALL test_proc()")
	})
}

func TestStoredProcedure_IF_EXISTS(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	t.Run("DROP PROCEDURE IF EXISTS", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE IF NOT EXISTS t (id INTEGER)")
		afExec(t, db, ctx, "CREATE PROCEDURE drop_test() BEGIN INSERT INTO t VALUES (1); END")

		// Drop with IF EXISTS
		afExec(t, db, ctx, "DROP PROCEDURE IF EXISTS drop_test")

		// Should succeed dropping again with IF EXISTS
		afExec(t, db, ctx, "DROP PROCEDURE IF EXISTS drop_test")
	})
}

func TestStoredProcedure_MultipleStatements(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY)")

	t.Run("Procedure with multiple statements", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE PROCEDURE multi_insert() BEGIN INSERT INTO t1 VALUES (1); INSERT INTO t2 VALUES (1); INSERT INTO t1 VALUES (2); END")

		afExec(t, db, ctx, "CALL multi_insert()")

		// Verify both tables have data
		rows1 := afQuery(t, db, ctx, "SELECT COUNT(*) FROM t1")
		rows2 := afQuery(t, db, ctx, "SELECT COUNT(*) FROM t2")

		if rows1[0][0] != int64(2) {
			t.Errorf("Expected 2 rows in t1, got %v", rows1[0][0])
		}
		if rows2[0][0] != int64(1) {
			t.Errorf("Expected 1 row in t2, got %v", rows2[0][0])
		}
	})
}

func TestStoredProcedure_CallArgumentSemantics(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE proc_args (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
	afExec(t, db, ctx, "CREATE PROCEDURE insert_proc_arg(p_id INTEGER, p_name TEXT, p_value INTEGER) BEGIN INSERT INTO proc_args VALUES (p_id, p_name, p_value); END")

	if _, err := db.Exec(ctx, "CALL insert_proc_arg(?, ?, ?)", 10, "ten", 100); err != nil {
		t.Fatalf("CALL with placeholders failed: %v", err)
	}
	rows := afQuery(t, db, ctx, "SELECT name, value FROM proc_args WHERE id = 10")
	if len(rows) != 1 {
		t.Fatalf("Expected 1 inserted row, got %d", len(rows))
	}
	if rows[0][0] != "ten" || rows[0][1] != int64(100) {
		t.Fatalf("Unexpected placeholder CALL row: %v", rows[0])
	}

	for _, sql := range []string{
		"CALL insert_proc_arg(11, 'missing')",
		"CALL insert_proc_arg(12, 'extra', 120, 999)",
	} {
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Fatalf("Expected argument count error for %s", sql)
		}
		if !strings.Contains(err.Error(), "expects 3 arguments") {
			t.Fatalf("Expected argument count error for %s, got %v", sql, err)
		}
	}
}

func TestStoredProcedure_ParameterSubstitutionInComplexExpressions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE proc_expr (id INTEGER PRIMARY KEY, score INTEGER, flag TEXT)")
	afExec(t, db, ctx, "INSERT INTO proc_expr VALUES (1, 75, NULL), (2, 25, NULL)")
	afExec(t, db, ctx, `CREATE PROCEDURE mark_proc_expr(p_id INTEGER, p_min INTEGER, p_label TEXT)
		BEGIN
			UPDATE proc_expr
			SET flag = CASE WHEN score BETWEEN p_min AND 100 THEN p_label ELSE 'low' END
			WHERE id IN (p_id);
		END`)

	afExec(t, db, ctx, "CALL mark_proc_expr(1, 50, 'hit')")
	afExec(t, db, ctx, "CALL mark_proc_expr(2, 50, 'hit')")

	rows := afQuery(t, db, ctx, "SELECT flag FROM proc_expr WHERE id = 1")
	if len(rows) != 1 || rows[0][0] != "hit" {
		t.Fatalf("Expected id=1 flag hit, got %v", rows)
	}
	rows = afQuery(t, db, ctx, "SELECT flag FROM proc_expr WHERE id = 2")
	if len(rows) != 1 || rows[0][0] != "low" {
		t.Fatalf("Expected id=2 flag low, got %v", rows)
	}
}
