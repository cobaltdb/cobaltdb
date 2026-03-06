package test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// Helper to create a fresh in-memory DB for each test
func newTestDB(t *testing.T) *engine.DB {
	t.Helper()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func execSQL(t *testing.T, db *engine.DB, sql string) {
	t.Helper()
	ctx := context.Background()
	_, err := db.Exec(ctx, sql)
	if err != nil {
		t.Fatalf("Exec '%s' failed: %v", sql, err)
	}
}

func queryRows(t *testing.T, db *engine.DB, sql string) ([]string, [][]interface{}) {
	t.Helper()
	ctx := context.Background()
	rows, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("Query '%s' failed: %v", sql, err)
	}
	defer rows.Close()

	cols := rows.Columns()
	var result [][]interface{}

	for rows.Next() {
		row := make([]interface{}, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range dest {
			dest[i] = &row[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		result = append(result, row)
	}

	return cols, result
}

func expectRowCount(t *testing.T, db *engine.DB, sql string, expected int) {
	t.Helper()
	_, rows := queryRows(t, db, sql)
	if len(rows) != expected {
		t.Fatalf("Query '%s': expected %d rows, got %d", sql, expected, len(rows))
	}
}

func expectSingleValue(t *testing.T, db *engine.DB, sql string, expected interface{}) {
	t.Helper()
	_, rows := queryRows(t, db, sql)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("Query '%s': expected 1x1 result, got %dx%d", sql, len(rows), func() int {
			if len(rows) > 0 {
				return len(rows[0])
			}
			return 0
		}())
	}
	got := rows[0][0]
	if !valuesEqual(got, expected) {
		t.Fatalf("Query '%s': expected %v (%T), got %v (%T)", sql, expected, expected, got, got)
	}
}

func valuesEqual(a, b interface{}) bool {
	// Handle numeric comparisons (int64 vs float64 etc)
	af, aok := toFloat64(a)
	bf, bok := toFloat64(b)
	if aok && bok {
		return math.Abs(af-bf) < 0.001
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// ===== DDL Tests =====

func TestSQLCreateTable(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL, active BOOLEAN DEFAULT true)")

	tables := db.Tables()
	if len(tables) != 1 || tables[0] != "t1" {
		t.Fatalf("Expected [t1], got %v", tables)
	}
}

func TestSQLCreateTableIfNotExists(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY)") // Should not fail
}

func TestSQLDropTable(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "DROP TABLE t1")

	tables := db.Tables()
	if len(tables) != 0 {
		t.Fatalf("Expected no tables after DROP, got %v", tables)
	}
}

func TestSQLDropTableIfExists(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "DROP TABLE IF EXISTS nonexistent") // Should not fail
}

func TestSQLAlterTableAddColumn(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "ALTER TABLE t1 ADD COLUMN email TEXT")
	execSQL(t, db, "INSERT INTO t1 (id, name, email) VALUES (1, 'Alice', 'alice@test.com')")

	_, rows := queryRows(t, db, "SELECT email FROM t1 WHERE id = 1")
	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "alice@test.com" {
		t.Fatalf("Expected 'alice@test.com', got %v", rows[0][0])
	}
}

func TestSQLCreateIndex(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "CREATE INDEX idx_name ON t1 (name)")
}

// ===== INSERT Tests =====

func TestSQLInsertBasic(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'Alice')")
	expectRowCount(t, db, "SELECT * FROM t1", 1)
}

func TestSQLInsertMultiRow(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
	expectRowCount(t, db, "SELECT * FROM t1", 3)
}

func TestSQLInsertNullValues(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, NULL)")

	_, rows := queryRows(t, db, "SELECT name FROM t1 WHERE id = 1")
	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != nil {
		t.Fatalf("Expected NULL, got %v", rows[0][0])
	}
}

func TestSQLAutoIncrement(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (name) VALUES ('Alice')")
	execSQL(t, db, "INSERT INTO t1 (name) VALUES ('Bob')")

	expectRowCount(t, db, "SELECT * FROM t1", 2)
	expectSingleValue(t, db, "SELECT COUNT(DISTINCT id) FROM t1", int64(2))
}

// ===== SELECT Tests =====

func TestSQLSelectAll(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
	expectRowCount(t, db, "SELECT * FROM t1", 2)
}

func TestSQLSelectColumns(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name, email) VALUES (1, 'Alice', 'a@b.c')")

	cols, rows := queryRows(t, db, "SELECT name, email FROM t1")
	if len(cols) != 2 || cols[0] != "name" || cols[1] != "email" {
		t.Fatalf("Unexpected columns: %v", cols)
	}
	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}
}

func TestSQLSelectWhere(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30)")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE val > 15", 2)
	expectRowCount(t, db, "SELECT * FROM t1 WHERE val = 20", 1)
	expectRowCount(t, db, "SELECT * FROM t1 WHERE val >= 20", 2)
	expectRowCount(t, db, "SELECT * FROM t1 WHERE val < 20", 1)
	expectRowCount(t, db, "SELECT * FROM t1 WHERE val != 20", 2)
}

func TestSQLSelectWhereAND(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, a, b) VALUES (1, 1, 1), (2, 1, 2), (3, 2, 1)")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE a = 1 AND b = 1", 1)
}

func TestSQLSelectWhereOR(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30)")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE val = 10 OR val = 30", 2)
}

func TestSQLSelectWhereParens(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, a, b) VALUES (1, 1, 'x'), (2, 2, 'y'), (3, 1, 'y')")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE (a = 1 AND b = 'x') OR (a = 2 AND b = 'y')", 2)
}

func TestSQLSelectBETWEEN(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 5), (2, 10), (3, 15), (4, 20)")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE val BETWEEN 10 AND 15", 2)
}

func TestSQLSelectNOTBETWEEN(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 5), (2, 10), (3, 15), (4, 20)")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE val NOT BETWEEN 10 AND 15", 2)
}

func TestSQLSelectIN(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE name IN ('Alice', 'Charlie')", 2)
}

func TestSQLSelectNOTIN(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE name NOT IN ('Bob')", 2)
}

func TestSQLSelectLIKE(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Adam')")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE name LIKE 'A%'", 2)
	expectRowCount(t, db, "SELECT * FROM t1 WHERE name LIKE '%o%'", 1)
}

func TestSQLSelectNOTLIKE(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Adam')")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE name NOT LIKE 'A%'", 1)
}

func TestSQLSelectISNULL(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 'x'), (2, NULL)")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE val IS NULL", 1)
	expectRowCount(t, db, "SELECT * FROM t1 WHERE val IS NOT NULL", 1)
}

func TestSQLSelectDISTINCT(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, category TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, category) VALUES (1, 'A'), (2, 'B'), (3, 'A')")

	expectRowCount(t, db, "SELECT DISTINCT category FROM t1", 2)
}

func TestSQLSelectORDERBY(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 30), (2, 10), (3, 20)")

	_, rows := queryRows(t, db, "SELECT val FROM t1 ORDER BY val ASC")
	if len(rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(rows))
	}
	if !valuesEqual(rows[0][0], int64(10)) || !valuesEqual(rows[1][0], int64(20)) || !valuesEqual(rows[2][0], int64(30)) {
		t.Fatalf("Unexpected order: %v, %v, %v", rows[0][0], rows[1][0], rows[2][0])
	}
}

func TestSQLSelectORDERBYDESC(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 30), (2, 10), (3, 20)")

	_, rows := queryRows(t, db, "SELECT val FROM t1 ORDER BY val DESC")
	if !valuesEqual(rows[0][0], int64(30)) || !valuesEqual(rows[1][0], int64(20)) || !valuesEqual(rows[2][0], int64(10)) {
		t.Fatalf("Unexpected order: %v, %v, %v", rows[0][0], rows[1][0], rows[2][0])
	}
}

func TestSQLSelectLIMIT(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "INSERT INTO t1 (id) VALUES (1), (2), (3), (4), (5)")

	expectRowCount(t, db, "SELECT * FROM t1 LIMIT 3", 3)
}

func TestSQLSelectLIMITOFFSET(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "INSERT INTO t1 (id) VALUES (1), (2), (3), (4), (5)")

	_, rows := queryRows(t, db, "SELECT id FROM t1 ORDER BY id LIMIT 2 OFFSET 2")
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}
	if !valuesEqual(rows[0][0], int64(3)) {
		t.Fatalf("Expected first row id=3, got %v", rows[0][0])
	}
}

func TestSQLSelectAlias(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'Alice')")

	cols, _ := queryRows(t, db, "SELECT name AS user_name FROM t1")
	if len(cols) != 1 || cols[0] != "user_name" {
		t.Fatalf("Expected column 'user_name', got %v", cols)
	}
}

// ===== Aggregate Tests =====

func TestSQLAggCOUNT(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 'a'), (2, NULL), (3, 'b')")

	expectSingleValue(t, db, "SELECT COUNT(*) FROM t1", int64(3))
	expectSingleValue(t, db, "SELECT COUNT(val) FROM t1", int64(2)) // excludes NULL
}

func TestSQLAggSUM(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val REAL)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10.5), (2, 20.5), (3, 30.0)")

	expectSingleValue(t, db, "SELECT SUM(val) FROM t1", 61.0)
}

func TestSQLAggAVG(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val REAL)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10.0), (2, 20.0), (3, 30.0)")

	expectSingleValue(t, db, "SELECT AVG(val) FROM t1", 20.0)
}

func TestSQLAggMINMAX(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 5), (2, 15), (3, 10)")

	expectSingleValue(t, db, "SELECT MIN(val) FROM t1", int64(5))
	expectSingleValue(t, db, "SELECT MAX(val) FROM t1", int64(15))
}

func TestSQLGroupBy(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, category, val) VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'B', 40)")

	_, rows := queryRows(t, db, "SELECT category, SUM(val) FROM t1 GROUP BY category ORDER BY category")
	if len(rows) != 2 {
		t.Fatalf("Expected 2 groups, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("Expected first group 'A', got %v", rows[0][0])
	}
	if !valuesEqual(rows[0][1], 40.0) {
		t.Fatalf("Expected SUM(A)=40, got %v", rows[0][1])
	}
}

func TestSQLHaving(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, category, val) VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'C', 5)")

	expectRowCount(t, db, "SELECT category, SUM(val) FROM t1 GROUP BY category HAVING SUM(val) > 15", 2)
}

// ===== JOIN Tests =====

func TestSQLJoin(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, val TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'A'), (2, 'B')")
	execSQL(t, db, "INSERT INTO t2 (id, t1_id, val) VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 2, 'z')")

	expectRowCount(t, db, "SELECT t1.name, t2.val FROM t1 JOIN t2 ON t1.id = t2.t1_id", 3)
}

func TestSQLLeftJoin(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, val TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'A'), (2, 'B'), (3, 'C')")
	execSQL(t, db, "INSERT INTO t2 (id, t1_id, val) VALUES (1, 1, 'x'), (2, 2, 'y')")

	expectRowCount(t, db, "SELECT t1.name, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id", 3)
}

// ===== UPDATE Tests =====

func TestSQLUpdateBasic(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20)")
	execSQL(t, db, "UPDATE t1 SET val = 99 WHERE id = 1")

	expectSingleValue(t, db, "SELECT val FROM t1 WHERE id = 1", int64(99))
}

func TestSQLUpdateMultiColumn(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, a, b) VALUES (1, 'old_a', 'old_b')")
	execSQL(t, db, "UPDATE t1 SET a = 'new_a', b = 'new_b' WHERE id = 1")

	_, rows := queryRows(t, db, "SELECT a, b FROM t1 WHERE id = 1")
	if fmt.Sprintf("%v", rows[0][0]) != "new_a" || fmt.Sprintf("%v", rows[0][1]) != "new_b" {
		t.Fatalf("Expected (new_a, new_b), got (%v, %v)", rows[0][0], rows[0][1])
	}
}

func TestSQLUpdateWithExpression(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10)")
	execSQL(t, db, "UPDATE t1 SET val = val + 5 WHERE id = 1")

	expectSingleValue(t, db, "SELECT val FROM t1 WHERE id = 1", int64(15))
}

// ===== DELETE Tests =====

func TestSQLDeleteBasic(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	execSQL(t, db, "DELETE FROM t1 WHERE id = 2")

	expectRowCount(t, db, "SELECT * FROM t1", 2)
}

func TestSQLDeleteAll(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "INSERT INTO t1 (id) VALUES (1), (2), (3)")
	execSQL(t, db, "DELETE FROM t1")

	expectRowCount(t, db, "SELECT * FROM t1", 0)
}

func TestSQLDeleteWithComplexWhere(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, a, b) VALUES (1, 1, 'x'), (2, 2, 'y'), (3, 1, 'y')")
	execSQL(t, db, "DELETE FROM t1 WHERE a = 1 AND b = 'y'")

	expectRowCount(t, db, "SELECT * FROM t1", 2)
}

// ===== Transaction Tests =====

func TestSQLTransaction(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")

	execSQL(t, db, "BEGIN")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (2, 20)")
	execSQL(t, db, "COMMIT")

	expectRowCount(t, db, "SELECT * FROM t1", 2)
}

func TestSQLRollback(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10)")

	execSQL(t, db, "BEGIN")
	execSQL(t, db, "DELETE FROM t1")
	execSQL(t, db, "ROLLBACK")

	expectRowCount(t, db, "SELECT * FROM t1", 1)
}

// ===== Expression Tests =====

func TestSQLArithmetic(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, a, b) VALUES (1, 10, 3)")

	expectSingleValue(t, db, "SELECT a + b FROM t1", int64(13))
	expectSingleValue(t, db, "SELECT a - b FROM t1", int64(7))
	expectSingleValue(t, db, "SELECT a * b FROM t1", int64(30))
}

func TestSQLStringConcat(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, first TEXT, last TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, first, last) VALUES (1, 'John', 'Doe')")

	_, rows := queryRows(t, db, "SELECT first || ' ' || last FROM t1")
	if fmt.Sprintf("%v", rows[0][0]) != "John Doe" {
		t.Fatalf("Expected 'John Doe', got %v", rows[0][0])
	}
}

func TestSQLCaseWhen(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30)")

	_, rows := queryRows(t, db, "SELECT id, CASE WHEN val < 15 THEN 'low' WHEN val < 25 THEN 'mid' ELSE 'high' END FROM t1 ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][1]) != "low" {
		t.Fatalf("Expected 'low', got %v", rows[0][1])
	}
	if fmt.Sprintf("%v", rows[1][1]) != "mid" {
		t.Fatalf("Expected 'mid', got %v", rows[1][1])
	}
	if fmt.Sprintf("%v", rows[2][1]) != "high" {
		t.Fatalf("Expected 'high', got %v", rows[2][1])
	}
}

func TestSQLCast(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, '42')")

	_, rows := queryRows(t, db, "SELECT CAST(val AS INTEGER) FROM t1")
	if !valuesEqual(rows[0][0], int64(42)) {
		t.Fatalf("Expected 42, got %v (%T)", rows[0][0], rows[0][0])
	}
}

// ===== Function Tests =====

func TestSQLFunctionUPPER(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'hello')")

	_, rows := queryRows(t, db, "SELECT UPPER(name) FROM t1")
	if fmt.Sprintf("%v", rows[0][0]) != "HELLO" {
		t.Fatalf("Expected 'HELLO', got %v", rows[0][0])
	}
}

func TestSQLFunctionLOWER(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'HELLO')")

	_, rows := queryRows(t, db, "SELECT LOWER(name) FROM t1")
	if fmt.Sprintf("%v", rows[0][0]) != "hello" {
		t.Fatalf("Expected 'hello', got %v", rows[0][0])
	}
}

func TestSQLFunctionLENGTH(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'hello')")

	expectSingleValue(t, db, "SELECT LENGTH(name) FROM t1", int64(5))
}

func TestSQLFunctionSUBSTR(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, name) VALUES (1, 'hello world')")

	_, rows := queryRows(t, db, "SELECT SUBSTR(name, 1, 5) FROM t1")
	if fmt.Sprintf("%v", rows[0][0]) != "hello" {
		t.Fatalf("Expected 'hello', got %v", rows[0][0])
	}
}

func TestSQLFunctionCOALESCE(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, a, b) VALUES (1, NULL, 'fallback')")

	_, rows := queryRows(t, db, "SELECT COALESCE(a, b) FROM t1")
	if fmt.Sprintf("%v", rows[0][0]) != "fallback" {
		t.Fatalf("Expected 'fallback', got %v", rows[0][0])
	}
}

func TestSQLFunctionIFNULL(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, NULL)")

	_, rows := queryRows(t, db, "SELECT IFNULL(val, 'default') FROM t1")
	if fmt.Sprintf("%v", rows[0][0]) != "default" {
		t.Fatalf("Expected 'default', got %v", rows[0][0])
	}
}

func TestSQLFunctionABS(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, -5)")

	expectSingleValue(t, db, "SELECT ABS(val) FROM t1", int64(5))
}

// ===== Subquery Tests =====

func TestSQLSubqueryInWhere(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30)")

	expectRowCount(t, db, "SELECT * FROM t1 WHERE val > (SELECT AVG(val) FROM t1)", 1)
}

func TestSQLSubqueryInSELECT(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20)")

	_, rows := queryRows(t, db, "SELECT id, (SELECT MAX(val) FROM t1) FROM t1 ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}
}

// ===== CTE Tests =====

func TestSQLCTE(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30)")

	expectRowCount(t, db, "WITH highval AS (SELECT * FROM t1 WHERE val > 15) SELECT * FROM highval", 2)
}

// ===== View Tests =====

func TestSQLCreateView(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20)")
	execSQL(t, db, "CREATE VIEW v1 AS SELECT * FROM t1 WHERE val > 15")

	expectRowCount(t, db, "SELECT * FROM v1", 1)
}

// ===== Negative Number Tests =====

func TestSQLNegativeNumbers(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	execSQL(t, db, "INSERT INTO t1 (id, val) VALUES (1, -10)")

	expectSingleValue(t, db, "SELECT val FROM t1 WHERE id = 1", int64(-10))
}

// ===== Empty Table Tests =====

func TestSQLEmptyTable(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")

	expectRowCount(t, db, "SELECT * FROM t1", 0)
	expectSingleValue(t, db, "SELECT COUNT(*) FROM t1", int64(0))
}

// ===== Mixed Case Keywords =====

func TestSQLMixedCase(t *testing.T) {
	db := newTestDB(t)
	execSQL(t, db, "create table T1 (id integer primary key, name text)")
	execSQL(t, db, "Insert Into T1 (id, name) Values (1, 'test')")

	expectRowCount(t, db, "Select * From T1", 1)
}

// ===== Full E2E Workflow =====

func TestSQLFullWorkflow(t *testing.T) {
	db := newTestDB(t)

	// Create schema
	execSQL(t, db, "CREATE TABLE customers (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT)")
	execSQL(t, db, "CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, customer_id INTEGER, amount REAL, status TEXT DEFAULT 'pending')")

	// Insert data
	execSQL(t, db, "INSERT INTO customers (name, email) VALUES ('Alice', 'alice@test.com')")
	execSQL(t, db, "INSERT INTO customers (name, email) VALUES ('Bob', 'bob@test.com')")
	execSQL(t, db, "INSERT INTO customers (name, email) VALUES ('Charlie', 'charlie@test.com')")

	execSQL(t, db, "INSERT INTO orders (customer_id, amount, status) VALUES (1, 50.00, 'completed')")
	execSQL(t, db, "INSERT INTO orders (customer_id, amount, status) VALUES (1, 75.00, 'completed')")
	execSQL(t, db, "INSERT INTO orders (customer_id, amount, status) VALUES (2, 100.00, 'pending')")
	execSQL(t, db, "INSERT INTO orders (customer_id, amount, status) VALUES (2, 25.00, 'cancelled')")
	execSQL(t, db, "INSERT INTO orders (customer_id, amount, status) VALUES (3, 200.00, 'completed')")

	// Verify data
	expectRowCount(t, db, "SELECT * FROM customers", 3)
	expectRowCount(t, db, "SELECT * FROM orders", 5)

	// Join with aggregation
	_, rows := queryRows(t, db, `
		SELECT c.name, COUNT(o.id), SUM(o.amount)
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		WHERE o.status = 'completed'
		GROUP BY c.name
		ORDER BY c.name
	`)
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows (Alice, Charlie), got %d", len(rows))
	}

	// Update
	execSQL(t, db, "UPDATE orders SET status = 'completed' WHERE status = 'pending'")
	expectSingleValue(t, db, "SELECT COUNT(*) FROM orders WHERE status = 'completed'", int64(4))

	// Delete
	execSQL(t, db, "DELETE FROM orders WHERE status = 'cancelled'")
	expectRowCount(t, db, "SELECT * FROM orders", 4)

	// Transaction
	execSQL(t, db, "BEGIN")
	execSQL(t, db, "INSERT INTO orders (customer_id, amount, status) VALUES (3, 300.00, 'completed')")
	execSQL(t, db, "COMMIT")
	expectRowCount(t, db, "SELECT * FROM orders WHERE customer_id = 3", 2)

	t.Log("Full workflow completed successfully!")
}
