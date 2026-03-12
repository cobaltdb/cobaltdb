package test

import (
	"fmt"
	"testing"
)

func TestV18ComprehensiveSQL(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		exp := fmt.Sprintf("%v", expected)
		if got != exp {
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, exp)
			return
		}
		pass++
	}

	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: expected %d rows, got %d", desc, expected, len(rows))
			return
		}
		pass++
	}

	checkNoError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: %v", desc, err)
			return
		}
		pass++
	}

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
			return
		}
		pass++
	}

	// ============================================================
	// === ALTER TABLE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE alter_test (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO alter_test VALUES (1, 'Alice')")

	checkNoError("ALTER TABLE ADD COLUMN", "ALTER TABLE alter_test ADD COLUMN age INTEGER")
	check("Column added with NULL default", "SELECT age FROM alter_test WHERE id = 1", "<nil>")

	checkNoError("INSERT after ALTER", "INSERT INTO alter_test VALUES (2, 'Bob', 30)")
	check("New column has value", "SELECT age FROM alter_test WHERE id = 2", 30)

	checkNoError("ALTER TABLE DROP COLUMN", "ALTER TABLE alter_test DROP COLUMN age")
	checkRowCount("Table still works after DROP COLUMN", "SELECT * FROM alter_test", 2)

	// ============================================================
	// === DROP TABLE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE drop_me (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO drop_me VALUES (1, 'test')")
	checkNoError("DROP TABLE", "DROP TABLE drop_me")
	checkError("SELECT from dropped table", "SELECT * FROM drop_me")

	checkNoError("DROP TABLE IF EXISTS (already dropped)", "DROP TABLE IF EXISTS drop_me")
	checkNoError("CREATE TABLE after drop", "CREATE TABLE drop_me (id INTEGER PRIMARY KEY, val TEXT)")
	checkRowCount("Recreated table is empty", "SELECT * FROM drop_me", 0)

	// ============================================================
	// === LIKE PATTERNS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE like_test (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO like_test VALUES (1, 'Apple')")
	afExec(t, db, ctx, "INSERT INTO like_test VALUES (2, 'Banana')")
	afExec(t, db, ctx, "INSERT INTO like_test VALUES (3, 'Cherry')")
	afExec(t, db, ctx, "INSERT INTO like_test VALUES (4, 'Avocado')")
	afExec(t, db, ctx, "INSERT INTO like_test VALUES (5, 'Blueberry')")

	checkRowCount("LIKE starts with A", "SELECT * FROM like_test WHERE name LIKE 'A%'", 2)
	checkRowCount("LIKE ends with y", "SELECT * FROM like_test WHERE name LIKE '%y'", 2)
	checkRowCount("LIKE contains an", "SELECT * FROM like_test WHERE name LIKE '%an%'", 1)
	checkRowCount("LIKE single char _", "SELECT * FROM like_test WHERE name LIKE '_pple'", 1)
	checkRowCount("NOT LIKE", "SELECT * FROM like_test WHERE name NOT LIKE 'A%'", 3)

	// ============================================================
	// === CAST ===
	// ============================================================
	check("CAST int to text", "SELECT CAST(42 AS TEXT)", "42")
	check("CAST text to integer", "SELECT CAST('123' AS INTEGER)", 123)
	check("CAST float to integer", "SELECT CAST(3.7 AS INTEGER)", 3)

	// ============================================================
	// === STRING FUNCTIONS ===
	// ============================================================
	check("UPPER", "SELECT UPPER('hello')", "HELLO")
	check("LOWER", "SELECT LOWER('HELLO')", "hello")
	check("LENGTH", "SELECT LENGTH('hello')", 5)
	check("SUBSTR 2-arg", "SELECT SUBSTR('hello', 2)", "ello")
	check("SUBSTR 3-arg", "SELECT SUBSTR('hello', 2, 3)", "ell")
	check("REPLACE", "SELECT REPLACE('hello world', 'world', 'go')", "hello go")
	check("TRIM", "SELECT TRIM('  hello  ')", "hello")

	// ============================================================
	// === MATH FUNCTIONS ===
	// ============================================================
	check("ABS positive", "SELECT ABS(5)", 5)
	check("ABS negative", "SELECT ABS(-5)", 5)
	check("ROUND", "SELECT ROUND(3.567, 2)", 3.57)

	// ============================================================
	// === COALESCE / IFNULL / NULLIF ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE null_test (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO null_test VALUES (1, 10, 20)")
	afExec(t, db, ctx, "INSERT INTO null_test VALUES (2, NULL, 30)")
	afExec(t, db, ctx, "INSERT INTO null_test VALUES (3, NULL, NULL)")

	check("COALESCE first non-null", "SELECT COALESCE(a, b, 0) FROM null_test WHERE id = 1", 10)
	check("COALESCE skip null", "SELECT COALESCE(a, b, 0) FROM null_test WHERE id = 2", 30)
	check("COALESCE all null fallback", "SELECT COALESCE(a, b, 0) FROM null_test WHERE id = 3", 0)
	check("IFNULL non-null", "SELECT IFNULL(a, 99) FROM null_test WHERE id = 1", 10)
	check("IFNULL with null", "SELECT IFNULL(a, 99) FROM null_test WHERE id = 2", 99)
	check("NULLIF equal", "SELECT NULLIF(10, 10) FROM null_test WHERE id = 1", "<nil>")
	check("NULLIF not equal", "SELECT NULLIF(10, 20) FROM null_test WHERE id = 1", 10)

	// ============================================================
	// === NULL HANDLING IN AGGREGATES ===
	// ============================================================
	check("COUNT(*) includes NULL rows", "SELECT COUNT(*) FROM null_test", 3)
	check("COUNT(col) excludes NULLs", "SELECT COUNT(a) FROM null_test", 1)
	check("SUM ignores NULLs", "SELECT SUM(a) FROM null_test", 10)
	check("AVG ignores NULLs", "SELECT AVG(a) FROM null_test", 10)

	// ============================================================
	// === IS NULL / IS NOT NULL ===
	// ============================================================
	checkRowCount("IS NULL", "SELECT * FROM null_test WHERE a IS NULL", 2)
	checkRowCount("IS NOT NULL", "SELECT * FROM null_test WHERE a IS NOT NULL", 1)

	// ============================================================
	// === CREATE/DROP INDEX ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE idx_test (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	afExec(t, db, ctx, "INSERT INTO idx_test VALUES (1, 'Alice', 'alice@test.com')")
	afExec(t, db, ctx, "INSERT INTO idx_test VALUES (2, 'Bob', 'bob@test.com')")

	checkNoError("CREATE INDEX", "CREATE INDEX idx_name ON idx_test(name)")
	check("Query with index", "SELECT email FROM idx_test WHERE name = 'Alice'", "alice@test.com")
	checkNoError("DROP INDEX", "DROP INDEX idx_name")

	checkNoError("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX idx_email ON idx_test(email)")
	checkError("Unique index violation", "INSERT INTO idx_test VALUES (3, 'Charlie', 'alice@test.com')")

	// ============================================================
	// === VIEWS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE view_source (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO view_source VALUES (1, 'Alice', 90)")
	afExec(t, db, ctx, "INSERT INTO view_source VALUES (2, 'Bob', 80)")
	afExec(t, db, ctx, "INSERT INTO view_source VALUES (3, 'Charlie', 95)")

	checkNoError("CREATE VIEW", "CREATE VIEW top_scorers AS SELECT name, score FROM view_source WHERE score >= 90")
	checkRowCount("Query view", "SELECT * FROM top_scorers", 2)
	check("View data correct", "SELECT name FROM top_scorers ORDER BY score DESC LIMIT 1", "Charlie")
	checkNoError("DROP VIEW", "DROP VIEW top_scorers")

	// ============================================================
	// === INSERT OR REPLACE / INSERT OR IGNORE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE upsert_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO upsert_test VALUES (1, 'Original', 100)")

	checkNoError("INSERT OR REPLACE", "INSERT OR REPLACE INTO upsert_test VALUES (1, 'Replaced', 200)")
	check("Replace updated value", "SELECT name FROM upsert_test WHERE id = 1", "Replaced")
	check("Replace updated val", "SELECT val FROM upsert_test WHERE id = 1", 200)
	checkRowCount("Replace didn't add row", "SELECT * FROM upsert_test", 1)

	checkNoError("INSERT OR IGNORE duplicate", "INSERT OR IGNORE INTO upsert_test VALUES (1, 'Ignored', 300)")
	check("Ignore kept original", "SELECT name FROM upsert_test WHERE id = 1", "Replaced")
	checkNoError("INSERT OR IGNORE new", "INSERT OR IGNORE INTO upsert_test VALUES (2, 'New', 400)")
	checkRowCount("Ignore added new row", "SELECT * FROM upsert_test", 2)

	// ============================================================
	// === COMPLEX WHERE WITH AND/OR/NOT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE complex_where (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)")
	afExec(t, db, ctx, "INSERT INTO complex_where VALUES (1, 10, 20, 'x')")
	afExec(t, db, ctx, "INSERT INTO complex_where VALUES (2, 30, 40, 'y')")
	afExec(t, db, ctx, "INSERT INTO complex_where VALUES (3, 50, 60, 'x')")
	afExec(t, db, ctx, "INSERT INTO complex_where VALUES (4, 10, 60, 'y')")

	checkRowCount("AND condition", "SELECT * FROM complex_where WHERE a = 10 AND c = 'x'", 1)
	checkRowCount("OR condition", "SELECT * FROM complex_where WHERE a = 10 OR a = 30", 3)
	checkRowCount("NOT condition", "SELECT * FROM complex_where WHERE NOT c = 'x'", 2)
	checkRowCount("Complex AND/OR", "SELECT * FROM complex_where WHERE (a = 10 AND c = 'x') OR (a = 50 AND c = 'x')", 2)
	checkRowCount("BETWEEN in WHERE", "SELECT * FROM complex_where WHERE a BETWEEN 20 AND 50", 2)
	checkRowCount("IN in WHERE", "SELECT * FROM complex_where WHERE a IN (10, 50)", 3)

	// ============================================================
	// === MULTI-COLUMN ORDER BY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE multi_order (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO multi_order VALUES (1, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO multi_order VALUES (2, 'B', 10)")
	afExec(t, db, ctx, "INSERT INTO multi_order VALUES (3, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO multi_order VALUES (4, 'B', 30)")

	check("Multi ORDER BY ASC ASC",
		"SELECT id FROM multi_order ORDER BY cat ASC, val ASC LIMIT 1", 3)
	check("Multi ORDER BY ASC DESC",
		"SELECT id FROM multi_order ORDER BY cat ASC, val DESC LIMIT 1", 1)
	check("Multi ORDER BY DESC ASC",
		"SELECT id FROM multi_order ORDER BY cat DESC, val ASC LIMIT 1", 2)

	// ============================================================
	// === COMPLEX CASE EXPRESSIONS ===
	// ============================================================
	check("CASE with multiple WHEN",
		"SELECT CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END FROM multi_order WHERE id = 2", "two")

	check("CASE in ORDER BY",
		"SELECT id FROM multi_order ORDER BY CASE WHEN cat = 'B' THEN 0 ELSE 1 END, val ASC LIMIT 1", 2)

	// ============================================================
	// === ARITHMETIC EXPRESSIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE arith (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO arith VALUES (1, 10, 3)")

	check("Addition", "SELECT a + b FROM arith WHERE id = 1", 13)
	check("Subtraction", "SELECT a - b FROM arith WHERE id = 1", 7)
	check("Multiplication", "SELECT a * b FROM arith WHERE id = 1", 30)
	check("Division", "SELECT a / b FROM arith WHERE id = 1", 3.3333333333333335)
	check("Modulo", "SELECT a % b FROM arith WHERE id = 1", 1)
	check("Compound expression", "SELECT (a + b) * 2 FROM arith WHERE id = 1", 26)

	// ============================================================
	// === NESTED SUBQUERIES ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE sub_outer (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO sub_outer VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO sub_outer VALUES (2, 200)")
	afExec(t, db, ctx, "INSERT INTO sub_outer VALUES (3, 300)")

	check("Scalar subquery",
		"SELECT val FROM sub_outer WHERE val = (SELECT MAX(val) FROM sub_outer)", 300)

	checkRowCount("IN with subquery",
		"SELECT * FROM sub_outer WHERE val IN (SELECT val FROM sub_outer WHERE val > 150)", 2)

	check("Subquery in SELECT",
		"SELECT (SELECT COUNT(*) FROM sub_outer) FROM sub_outer LIMIT 1", 3)

	// ============================================================
	// === GROUP BY WITH EXPRESSIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE gb_expr (id INTEGER PRIMARY KEY, val INTEGER, cat TEXT)")
	afExec(t, db, ctx, "INSERT INTO gb_expr VALUES (1, 15, 'A')")
	afExec(t, db, ctx, "INSERT INTO gb_expr VALUES (2, 25, 'A')")
	afExec(t, db, ctx, "INSERT INTO gb_expr VALUES (3, 35, 'B')")
	afExec(t, db, ctx, "INSERT INTO gb_expr VALUES (4, 45, 'B')")

	check("GROUP BY with SUM",
		"SELECT cat, SUM(val) FROM gb_expr GROUP BY cat ORDER BY SUM(val) ASC LIMIT 1", "A")
	check("GROUP BY with AVG",
		"SELECT cat FROM gb_expr GROUP BY cat ORDER BY AVG(val) DESC LIMIT 1", "B")

	// ============================================================
	// === DELETE WITH COMPLEX WHERE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE del_test (id INTEGER PRIMARY KEY, status TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO del_test VALUES (1, 'active', 10)")
	afExec(t, db, ctx, "INSERT INTO del_test VALUES (2, 'inactive', 20)")
	afExec(t, db, ctx, "INSERT INTO del_test VALUES (3, 'active', 30)")
	afExec(t, db, ctx, "INSERT INTO del_test VALUES (4, 'inactive', 40)")

	checkNoError("DELETE with AND", "DELETE FROM del_test WHERE status = 'inactive' AND val < 30")
	checkRowCount("After DELETE", "SELECT * FROM del_test", 3)
	check("Correct row deleted", "SELECT COUNT(*) FROM del_test WHERE id = 2", 0)

	// ============================================================
	// === UPDATE WITH EXPRESSIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE upd_test (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO upd_test VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO upd_test VALUES (2, 20)")

	checkNoError("UPDATE with expression", "UPDATE upd_test SET val = val * 2 WHERE id = 1")
	check("Updated value", "SELECT val FROM upd_test WHERE id = 1", 20)
	check("Untouched value", "SELECT val FROM upd_test WHERE id = 2", 20)

	checkNoError("UPDATE multiple rows", "UPDATE upd_test SET val = val + 5")
	check("Updated row 1", "SELECT val FROM upd_test WHERE id = 1", 25)
	check("Updated row 2", "SELECT val FROM upd_test WHERE id = 2", 25)

	// ============================================================
	// === DISTINCT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE dist_test (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "INSERT INTO dist_test VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO dist_test VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO dist_test VALUES (3, 'A')")
	afExec(t, db, ctx, "INSERT INTO dist_test VALUES (4, 'C')")
	afExec(t, db, ctx, "INSERT INTO dist_test VALUES (5, 'B')")

	checkRowCount("SELECT DISTINCT", "SELECT DISTINCT cat FROM dist_test", 3)
	check("COUNT DISTINCT", "SELECT COUNT(DISTINCT cat) FROM dist_test", 3)

	// ============================================================
	// === TRANSACTIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_test (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO txn_test VALUES (1, 100)")

	checkNoError("BEGIN", "BEGIN")
	checkNoError("UPDATE in txn", "UPDATE txn_test SET val = 200 WHERE id = 1")
	check("Read in txn sees change", "SELECT val FROM txn_test WHERE id = 1", 200)
	checkNoError("ROLLBACK", "ROLLBACK")
	check("After rollback value restored", "SELECT val FROM txn_test WHERE id = 1", 100)

	checkNoError("BEGIN for commit", "BEGIN")
	checkNoError("UPDATE for commit", "UPDATE txn_test SET val = 300 WHERE id = 1")
	checkNoError("COMMIT", "COMMIT")
	check("After commit value persisted", "SELECT val FROM txn_test WHERE id = 1", 300)

	// ============================================================
	// === STRING CONCATENATION ===
	// ============================================================
	check("String concat ||", "SELECT 'Hello' || ' ' || 'World'", "Hello World")

	// ============================================================
	// === SHOW/DESCRIBE ===
	// ============================================================
	// SHOW TABLES must use Query() not Exec()
	total++
	if _, err := db.Query(ctx, "SHOW TABLES"); err != nil {
		t.Errorf("[FAIL] SHOW TABLES works: %v", err)
	} else {
		pass++
	}

	t.Logf("\n=== V18 COMPREHENSIVE SQL: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
