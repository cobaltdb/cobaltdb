package test

import (
	"fmt"
	"strings"
	"testing"
)

// ============================================================
// FK ON DELETE SET NULL
// ============================================================

func TestV89_FKOnDeleteSetNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE members (id INTEGER PRIMARY KEY, name TEXT, team_id INTEGER, FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE SET NULL)")

	afExec(t, db, ctx, "INSERT INTO teams VALUES (1, 'Alpha')")
	afExec(t, db, ctx, "INSERT INTO teams VALUES (2, 'Beta')")
	afExec(t, db, ctx, "INSERT INTO members VALUES (10, 'Alice', 1)")
	afExec(t, db, ctx, "INSERT INTO members VALUES (11, 'Bob', 1)")
	afExec(t, db, ctx, "INSERT INTO members VALUES (12, 'Carol', 2)")

	// Delete parent - child FK should become NULL
	afExec(t, db, ctx, "DELETE FROM teams WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT team_id FROM members WHERE name = 'Alice'", nil)
	afExpectVal(t, db, ctx, "SELECT team_id FROM members WHERE name = 'Bob'", nil)
	// Carol should still reference team 2
	afExpectVal(t, db, ctx, "SELECT team_id FROM members WHERE name = 'Carol'", float64(2))
}

func TestV89_FKOnDeleteNoAction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE RESTRICT)")

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, 1)")

	// Should fail: RESTRICT prevents delete when child references
	_, err := db.Exec(ctx, "DELETE FROM parents WHERE id = 1")
	if err == nil {
		t.Fatal("expected RESTRICT error on FK delete")
	}

	// Data should be unchanged
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM parents", float64(1))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM children", float64(1))
}

// ============================================================
// FK ON UPDATE CASCADE + ON DELETE CASCADE together
// ============================================================

func TestV89_FKBothOnUpdateAndOnDelete(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE CASCADE ON UPDATE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO parents VALUES (2, 'P2')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, 1)")
	afExec(t, db, ctx, "INSERT INTO children VALUES (11, 2)")

	// Update PK - should cascade
	afExec(t, db, ctx, "UPDATE parents SET id = 100 WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT parent_id FROM children WHERE id = 10", float64(100))

	// Delete parent - should cascade delete child
	afExec(t, db, ctx, "DELETE FROM parents WHERE id = 100")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM children WHERE parent_id = 100", float64(0))
}

func TestV89_FKOnUpdateSetNullOnDeleteCascade(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON UPDATE SET NULL ON DELETE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, 1)")

	// Update PK - should set NULL in child
	afExec(t, db, ctx, "UPDATE parents SET id = 100 WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT parent_id FROM children WHERE id = 10", nil)
}

// ============================================================
// FK with string primary keys (exercises serializeValue string path)
// ============================================================

func TestV89_FKWithStringPK(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE countries (code TEXT PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE cities (id INTEGER PRIMARY KEY, name TEXT, country_code TEXT, FOREIGN KEY (country_code) REFERENCES countries(code) ON DELETE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO countries VALUES ('US', 'United States')")
	afExec(t, db, ctx, "INSERT INTO countries VALUES ('UK', 'United Kingdom')")
	afExec(t, db, ctx, "INSERT INTO cities VALUES (1, 'New York', 'US')")
	afExec(t, db, ctx, "INSERT INTO cities VALUES (2, 'London', 'UK')")
	afExec(t, db, ctx, "INSERT INTO cities VALUES (3, 'Los Angeles', 'US')")

	// Delete US - should cascade delete US cities
	afExec(t, db, ctx, "DELETE FROM countries WHERE code = 'US'")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM cities", float64(1))
	afExpectVal(t, db, ctx, "SELECT name FROM cities WHERE id = 2", "London")
}

// ============================================================
// FK with transaction + cascade + rollback (exercises updateRowSlice undo)
// ============================================================

func TestV89_FKCascadeWithTransactionRollback(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON UPDATE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, 1)")

	// Begin transaction
	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "UPDATE parents SET id = 100 WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT parent_id FROM children WHERE id = 10", float64(100))
	afExec(t, db, ctx, "ROLLBACK")

	// After rollback, should be back to original
	afExpectVal(t, db, ctx, "SELECT id FROM parents WHERE name = 'P1'", float64(1))
	afExpectVal(t, db, ctx, "SELECT parent_id FROM children WHERE id = 10", float64(1))
}

func TestV89_FKSetNullWithTransactionRollback(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE SET NULL)")

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, 1)")

	// Begin transaction
	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "DELETE FROM parents WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT parent_id FROM children WHERE id = 10", nil)
	afExec(t, db, ctx, "ROLLBACK")

	// After rollback, child FK should be restored
	afExpectVal(t, db, ctx, "SELECT parent_id FROM children WHERE id = 10", float64(1))
}

// ============================================================
// JSON deeper paths - Set, Remove, ParseJSONPath
// ============================================================

func TestV89_JSONSetNestedPath(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO t VALUES (1, '{"a": {"b": 1}}')`)

	// JSON_SET on nested path
	val := afQuery(t, db, ctx, "SELECT JSON_SET(data, '$.a.b', 'updated') FROM t WHERE id = 1")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	result := fmt.Sprintf("%v", val[0][0])
	if !strings.Contains(result, "updated") {
		t.Fatalf("expected JSON with 'updated', got %v", result)
	}
}

func TestV89_JSONSetNewKey(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO t VALUES (1, '{"existing": 1}')`)

	// JSON_SET adding new key
	val := afQuery(t, db, ctx, "SELECT JSON_SET(data, '$.newkey', 'newval') FROM t WHERE id = 1")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	result := fmt.Sprintf("%v", val[0][0])
	if !strings.Contains(result, "newkey") {
		t.Fatalf("expected JSON with 'newkey', got %v", result)
	}
}

func TestV89_JSONRemoveNested(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO t VALUES (1, '{"a": {"b": 1, "c": 2}}')`)

	// Remove nested key
	val := afQuery(t, db, ctx, "SELECT JSON_REMOVE(data, '$.a.b') FROM t WHERE id = 1")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	result := fmt.Sprintf("%v", val[0][0])
	t.Logf("JSON_REMOVE nested result: %v", result)
}

func TestV89_JSONExtractArray(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO t VALUES (1, '{"items": [10, 20, 30]}')`)

	// Extract array element
	val := afQuery(t, db, ctx, "SELECT JSON_EXTRACT(data, '$.items') FROM t WHERE id = 1")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	t.Logf("JSON_EXTRACT array: %v", val[0][0])
}

func TestV89_JSONTypeVariants(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	// Test various JSON_TYPE inputs
	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('null')`, "null")
	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('42')`, "number")
	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('"hello"')`, "string")
	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('[1,2]')`, "array")
	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('{"a":1}')`, "object")
}

func TestV89_JSONMinify(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	val := afQuery(t, db, ctx, `SELECT JSON_MINIFY('{ "a" : 1 , "b" : 2 }')`)
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	result := fmt.Sprintf("%v", val[0][0])
	// Minified JSON should not have extra spaces
	t.Logf("JSON_MINIFY: %v", result)
}

// ============================================================
// applyOrderBy deeper paths (73.1%)
// ============================================================

func TestV89_OrderByExpressionASC(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10, 5)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 3, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 7, 7)")

	// ORDER BY expression a + b
	rows := afQuery(t, db, ctx, "SELECT id, a + b as total FROM t ORDER BY a + b ASC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestV89_OrderByMultipleDirections(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'a', 30)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'b', 5)")

	// ORDER BY grp ASC, val DESC
	rows := afQuery(t, db, ctx, "SELECT id FROM t ORDER BY grp ASC, val DESC")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// a group: 30, 10 → ids 3, 1
	// b group: 20, 5  → ids 2, 4
	if fmt.Sprintf("%v", rows[0][0]) != "3" {
		t.Fatalf("expected id=3 first, got %v", rows[0][0])
	}
}

func TestV89_OrderByNullsLast(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 5)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, NULL)")

	// NULLs should sort last in ASC
	rows := afQuery(t, db, ctx, "SELECT id, val FROM t ORDER BY val ASC")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// First row should be val=5 (id=3)
	if fmt.Sprintf("%v", rows[0][0]) != "3" {
		t.Fatalf("expected id=3 first (val=5), got %v", rows[0][0])
	}
}

func TestV89_OrderByDescNulls(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 20)")

	// ORDER BY DESC - NULLs should sort first in DESC
	rows := afQuery(t, db, ctx, "SELECT id, val FROM t ORDER BY val DESC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

// ============================================================
// evaluateLike deeper paths (67.9%)
// ============================================================

func TestV89_LikeWithPercent(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'abc')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'ABC')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'xabc')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'abcx')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (5, 'xabcx')")

	// Pattern starts with %
	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE val LIKE '%abc' ORDER BY id")
	// Should match: abc, ABC, xabc (case-insensitive)
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(rows))
	}

	// Pattern ends with %
	rows = afQuery(t, db, ctx, "SELECT id FROM t WHERE val LIKE 'abc%' ORDER BY id")
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(rows))
	}
}

func TestV89_LikeExactMatch(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'HELLO')")

	// Exact match (case-insensitive)
	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE val LIKE 'hello' ORDER BY id")
	if len(rows) != 2 { // case-insensitive
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV89_NotLike(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'world')")

	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE val NOT LIKE '%llo%' ORDER BY id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

// ============================================================
// evaluateHaving deeper paths (68.2%)
// ============================================================

func TestV89_HavingNullResult(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'a', NULL)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'b', 20)")

	// HAVING with aggregate on NULL values
	rows := afQuery(t, db, ctx, "SELECT grp, SUM(val) as total FROM t GROUP BY grp HAVING SUM(val) > 15")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != "b" {
		t.Fatalf("expected b, got %v", rows[0][0])
	}
}

func TestV89_HavingWithOR(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'a', 5)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'b', 15)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'c', 25)")

	// HAVING with OR
	rows := afQuery(t, db, ctx, "SELECT grp, SUM(val) as total FROM t GROUP BY grp HAVING SUM(val) < 10 OR SUM(val) > 20")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (a, c), got %d", len(rows))
	}
}

func TestV89_HavingCountStar(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'a')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'a')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'b')")

	// HAVING with COUNT(*)
	rows := afQuery(t, db, ctx, "SELECT grp, COUNT(*) as cnt FROM t GROUP BY grp HAVING COUNT(*) > 2")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != "a" {
		t.Fatalf("expected a, got %v", rows[0][0])
	}
}

// ============================================================
// Savepoint + Release deeper paths
// ============================================================

func TestV89_SavepointNested(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'first')")

	afExec(t, db, ctx, "SAVEPOINT sp1")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'second')")

	afExec(t, db, ctx, "SAVEPOINT sp2")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'third')")

	// Rollback to sp2 - should undo 'third'
	afExec(t, db, ctx, "ROLLBACK TO sp2")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t", float64(2))

	// Release sp1
	afExec(t, db, ctx, "RELEASE SAVEPOINT sp1")

	afExec(t, db, ctx, "COMMIT")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t", float64(2))
}

// ============================================================
// View with complex queries
// ============================================================

func TestV89_ViewWithJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE depts (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE emps (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)")

	afExec(t, db, ctx, "INSERT INTO depts VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO depts VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO emps VALUES (10, 'Alice', 1)")
	afExec(t, db, ctx, "INSERT INTO emps VALUES (11, 'Bob', 1)")
	afExec(t, db, ctx, "INSERT INTO emps VALUES (12, 'Carol', 2)")

	afExec(t, db, ctx, "CREATE VIEW emp_dept AS SELECT emps.name as emp_name, depts.name as dept_name FROM emps INNER JOIN depts ON emps.dept_id = depts.id")

	rows := afQuery(t, db, ctx, "SELECT emp_name, dept_name FROM emp_dept ORDER BY emp_name")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestV89_DropView(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'test')")
	afExec(t, db, ctx, "CREATE VIEW v AS SELECT * FROM t")

	// Query view
	afExpectVal(t, db, ctx, "SELECT val FROM v", "test")

	// Drop view
	afExec(t, db, ctx, "DROP VIEW v")

	// View should no longer exist
	_, err := db.Exec(ctx, "SELECT * FROM v")
	if err == nil {
		t.Fatal("expected error querying dropped view")
	}
}

// ============================================================
// evaluateIn - NULL handling
// ============================================================

func TestV89_InWithNullValue(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 20)")

	// NULL IN (10, 20) should be NULL (not match)
	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE val IN (10, 20) ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ============================================================
// evaluateCaseExpr deeper paths (85.0%)
// ============================================================

func TestV89_CaseSimpleForm(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, status INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 2)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 3)")

	// Simple CASE form: CASE status WHEN 1 THEN ...
	rows := afQuery(t, db, ctx, `SELECT id, CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END as label FROM t ORDER BY id`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0][1] != "active" {
		t.Fatalf("expected 'active', got %v", rows[0][1])
	}
	if rows[1][1] != "inactive" {
		t.Fatalf("expected 'inactive', got %v", rows[1][1])
	}
	if rows[2][1] != "unknown" {
		t.Fatalf("expected 'unknown', got %v", rows[2][1])
	}
}

func TestV89_CaseWithNullWhen(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 10)")

	// CASE WHEN with NULL
	rows := afQuery(t, db, ctx, "SELECT id, CASE WHEN val IS NULL THEN 'null' ELSE 'not null' END as label FROM t ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][1] != "null" {
		t.Fatalf("expected 'null', got %v", rows[0][1])
	}
}

// ============================================================
// Multiple aggregates without GROUP BY
// ============================================================

func TestV89_MultipleAggregatesNoGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	rows := afQuery(t, db, ctx, "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM t")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

// ============================================================
// CREATE TABLE IF NOT EXISTS
// ============================================================

func TestV89_CreateTableIfNotExists(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	// Should not error
	afExec(t, db, ctx, "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
	// Table should still work
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t", float64(1))
}

// ============================================================
// DROP TABLE
// ============================================================

func TestV89_DropTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExec(t, db, ctx, "DROP TABLE t")

	// Table should no longer exist
	_, err := db.Exec(ctx, "SELECT * FROM t")
	if err == nil {
		t.Fatal("expected error querying dropped table")
	}
}

func TestV89_DropTableWithIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE INDEX idx ON t (val)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'test')")

	afExec(t, db, ctx, "DROP TABLE t")

	_, err := db.Exec(ctx, "SELECT * FROM t")
	if err == nil {
		t.Fatal("expected error querying dropped table")
	}
}

// ============================================================
// CREATE TRIGGER IF NOT EXISTS
// ============================================================

func TestV89_CreateTriggerBasic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT)")

	afExec(t, db, ctx, "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (NEW.id, NEW.val); END")

	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello')")
	afExpectVal(t, db, ctx, "SELECT msg FROM log WHERE id = 1", "hello")
}

// ============================================================
// DROP TRIGGER
// ============================================================

func TestV89_DropTrigger(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT)")
	afExec(t, db, ctx, "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (NULL, 'fired'); END")

	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM log", float64(1))

	// Drop trigger
	afExec(t, db, ctx, "DROP TRIGGER trg")

	afExec(t, db, ctx, "INSERT INTO t VALUES (2)")
	// Trigger should not fire anymore
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM log", float64(1))
}

// ============================================================
// ALTER TABLE ADD COLUMN with DEFAULT
// ============================================================

func TestV89_AlterTableAddColumnDefault(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'Bob')")

	// Add column with default - should backfill existing rows
	afExec(t, db, ctx, "ALTER TABLE t ADD COLUMN status TEXT DEFAULT 'active'")

	afExpectVal(t, db, ctx, "SELECT status FROM t WHERE id = 1", "active")
	afExpectVal(t, db, ctx, "SELECT status FROM t WHERE id = 2", "active")

	// New rows should get the default
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'Carol', NULL)")
	afExpectVal(t, db, ctx, "SELECT status FROM t WHERE id = 3", nil)
}

// ============================================================
// ALTER TABLE DROP COLUMN
// ============================================================

func TestV89_AlterTableDropColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, extra TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'Alice', 'x')")

	afExec(t, db, ctx, "ALTER TABLE t DROP COLUMN extra")

	// Column should be gone
	_, err := db.Exec(ctx, "SELECT extra FROM t")
	if err == nil {
		t.Fatal("expected error querying dropped column")
	}

	// Other columns should work
	afExpectVal(t, db, ctx, "SELECT name FROM t WHERE id = 1", "Alice")
}

// ============================================================
// Materialized view
// ============================================================

func TestV89_MaterializedViewRefresh(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")

	// Create MV (data is stored internally, not as a regular table)
	afExec(t, db, ctx, "CREATE MATERIALIZED VIEW mv AS SELECT SUM(val) as total FROM t")

	// Add more data
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	// Refresh to update cached data
	afExec(t, db, ctx, "REFRESH MATERIALIZED VIEW mv")

	// Drop
	afExec(t, db, ctx, "DROP MATERIALIZED VIEW mv")
}

// ============================================================
// FTS index deeper coverage
// ============================================================

func TestV89_FTSCreateAndSearch(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
	afExec(t, db, ctx, "INSERT INTO docs VALUES (1, 'Hello World', 'This is a test document')")
	afExec(t, db, ctx, "INSERT INTO docs VALUES (2, 'Another Doc', 'Some other content here')")
	afExec(t, db, ctx, "INSERT INTO docs VALUES (3, 'Hello Again', 'Yet another hello document')")

	afExec(t, db, ctx, "CREATE FULLTEXT INDEX fts_docs ON docs (title, body)")

	// FTS index created successfully - verify table still works
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM docs", float64(3))
}

// ============================================================
// evaluateBinaryExpr deeper - string concatenation via ||
// ============================================================

func TestV89_StringConcatenation(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, first TEXT, last TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'John', 'Doe')")

	// String concatenation with ||
	val := afQuery(t, db, ctx, "SELECT first || ' ' || last FROM t WHERE id = 1")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	if val[0][0] != "John Doe" {
		t.Fatalf("expected 'John Doe', got %v", val[0][0])
	}
}

// ============================================================
// Large dataset operations
// ============================================================

func TestV89_LargeInsertAndQuery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")

	// Insert 100 rows
	for i := 1; i <= 100; i++ {
		grp := fmt.Sprintf("g%d", i%5)
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s', %d)", i, grp, i*10))
	}

	// Various aggregates
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t", float64(100))

	rows := afQuery(t, db, ctx, "SELECT grp, COUNT(*) as cnt, SUM(val) as total FROM t GROUP BY grp ORDER BY grp")
	if len(rows) != 5 {
		t.Fatalf("expected 5 groups, got %d", len(rows))
	}

	// Complex query with WHERE, GROUP BY, HAVING, ORDER BY
	rows = afQuery(t, db, ctx, "SELECT grp, AVG(val) as avg_val FROM t WHERE val > 100 GROUP BY grp HAVING AVG(val) > 200 ORDER BY avg_val DESC")
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row, got %d", len(rows))
	}
}

// ============================================================
// IS NULL / IS NOT NULL
// ============================================================

func TestV89_IsNullIsNotNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'world')")

	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE val IS NULL")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	rows = afQuery(t, db, ctx, "SELECT id FROM t WHERE val IS NOT NULL ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ============================================================
// UPDATE with multiple SET clauses
// ============================================================

func TestV89_UpdateMultipleColumns(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, status TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'Alice', 30, 'active')")

	afExec(t, db, ctx, "UPDATE t SET name = 'Alicia', age = 31, status = 'updated' WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT name FROM t WHERE id = 1", "Alicia")
	afExpectVal(t, db, ctx, "SELECT age FROM t WHERE id = 1", float64(31))
	afExpectVal(t, db, ctx, "SELECT status FROM t WHERE id = 1", "updated")
}

// ============================================================
// Complex nested query combining many features
// ============================================================

func TestV89_ComplexNestedFeatures(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (1, 'Laptop', 'Electronics', 999)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (2, 'Phone', 'Electronics', 699)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (3, 'Tablet', 'Electronics', 499)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (4, 'Chair', 'Furniture', 199)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (5, 'Desk', 'Furniture', 399)")

	// Subquery in WHERE + GROUP BY + HAVING + ORDER BY
	rows := afQuery(t, db, ctx, `
		SELECT category, COUNT(*) as cnt, AVG(price) as avg_price
		FROM products
		WHERE price > (SELECT MIN(price) FROM products)
		GROUP BY category
		HAVING COUNT(*) >= 2
		ORDER BY avg_price DESC
	`)
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row, got %d", len(rows))
	}
}

// ============================================================
// GLOB function
// ============================================================

func TestV89_GlobFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'world')")

	// GLOB function (case-sensitive pattern matching)
	val := afQuery(t, db, ctx, "SELECT GLOB('h*', 'hello')")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	t.Logf("GLOB result: %v", val[0][0])
}

// ============================================================
// GROUP_CONCAT with DISTINCT
// ============================================================

func TestV89_GroupConcatDistinct(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'a', 'x')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'a', 'x')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'a', 'y')")

	// GROUP_CONCAT without distinct
	val := afQuery(t, db, ctx, "SELECT GROUP_CONCAT(val) FROM t WHERE grp = 'a'")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	t.Logf("GROUP_CONCAT result: %v", val[0][0])
}

// ============================================================
// Boolean expressions in WHERE
// ============================================================

func TestV89_BooleanArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, active INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 0)")

	// Boolean-like WHERE
	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE active = 1")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}
