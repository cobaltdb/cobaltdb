package test

import (
	"fmt"
	"testing"
)

// TestV83CoverageBoost targets the lowest-coverage catalog functions to boost coverage
func TestV83CoverageBoost(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			if expected == nil {
				pass++
				return
			}
			t.Errorf("[FAIL] %s: no rows returned, expected %v", desc, expected)
			return
		}
		got := rows[0][0]
		if expected == nil {
			if got != nil {
				t.Errorf("[FAIL] %s: got %v (%T), expected nil", desc, got, got)
				return
			}
			pass++
			return
		}
		gotStr := fmt.Sprintf("%v", got)
		expStr := fmt.Sprintf("%v", expected)
		if gotStr != expStr {
			t.Errorf("[FAIL] %s: got %s (%T), expected %s", desc, gotStr, got, expStr)
			return
		}
		pass++
	}

	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: got %d rows, expected %d", desc, len(rows), expected)
			return
		}
		pass++
	}

	checkNoError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: exec error: %v", desc, err)
			return
		}
		pass++
	}

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			pass++
			return
		}
		t.Errorf("[FAIL] %s: expected error but got none", desc)
	}

	// ============================================================
	// === SECTION 1: SCALAR AGGREGATES WITHOUT FROM (0% coverage) ===
	// ============================================================

	check("COUNT(*) no FROM", "SELECT COUNT(*)", float64(1))
	check("SUM(42) no FROM", "SELECT SUM(42)", float64(42))
	check("AVG(100) no FROM", "SELECT AVG(100)", float64(100))
	check("MIN(5) no FROM", "SELECT MIN(5)", float64(5))
	check("MAX(10) no FROM", "SELECT MAX(10)", float64(10))
	check("SUM(0) no FROM", "SELECT SUM(0)", float64(0))
	check("AVG(3.14) no FROM", "SELECT AVG(3.14)", float64(3.14))
	check("MIN(-100) no FROM", "SELECT MIN(-100)", float64(-100))
	check("MAX(999) no FROM", "SELECT MAX(999)", float64(999))

	// ============================================================
	// === SECTION 2: LIKE EDGE CASES (64.3% coverage) ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_like (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v83_like VALUES (1, 'Hello World')")
	afExec(t, db, ctx, "INSERT INTO v83_like VALUES (2, 'hello')")
	afExec(t, db, ctx, "INSERT INTO v83_like VALUES (3, 'HELLO')")
	afExec(t, db, ctx, "INSERT INTO v83_like VALUES (4, '')")
	afExec(t, db, ctx, "INSERT INTO v83_like VALUES (5, 'h%llo')")
	afExec(t, db, ctx, "INSERT INTO v83_like VALUES (6, 'h_llo')")
	afExec(t, db, ctx, "INSERT INTO v83_like VALUES (7, NULL)")

	// LIKE with % at start
	checkRowCount("LIKE '%orld'", "SELECT * FROM v83_like WHERE val LIKE '%orld'", 1)
	// LIKE with % at end (case insensitive: hello, HELLO, Hello World)
	checkRowCount("LIKE 'hel%'", "SELECT * FROM v83_like WHERE val LIKE 'hel%'", 3)
	// LIKE with % both sides
	checkRowCount("LIKE '%ell%'", "SELECT * FROM v83_like WHERE val LIKE '%ell%'", 3)
	// LIKE single char _ (case insensitive: hello, HELLO, h%llo, h_llo)
	checkRowCount("LIKE 'h_llo'", "SELECT * FROM v83_like WHERE val LIKE 'h_llo'", 4)
	// LIKE exact match (case insensitive: hello, HELLO)
	checkRowCount("LIKE 'hello'", "SELECT * FROM v83_like WHERE val LIKE 'hello'", 2)
	// NOT LIKE
	checkRowCount("NOT LIKE '%orld%'", "SELECT * FROM v83_like WHERE val NOT LIKE '%orld%'", 5)
	// LIKE empty string
	checkRowCount("LIKE ''", "SELECT * FROM v83_like WHERE val LIKE ''", 1)
	// LIKE with NULL (NULL LIKE pattern => NULL => excluded)
	checkRowCount("NULL LIKE", "SELECT * FROM v83_like WHERE val LIKE '%' AND val IS NOT NULL", 6)

	// ============================================================
	// === SECTION 3: HAVING EDGE CASES (68.2% coverage) ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_sales (id INTEGER PRIMARY KEY, dept TEXT, amount INTEGER, product TEXT)")
	afExec(t, db, ctx, "INSERT INTO v83_sales VALUES (1, 'A', 100, 'Widget')")
	afExec(t, db, ctx, "INSERT INTO v83_sales VALUES (2, 'A', 200, 'Gadget')")
	afExec(t, db, ctx, "INSERT INTO v83_sales VALUES (3, 'B', 300, 'Widget')")
	afExec(t, db, ctx, "INSERT INTO v83_sales VALUES (4, 'B', 400, 'Widget')")
	afExec(t, db, ctx, "INSERT INTO v83_sales VALUES (5, 'C', 50, 'Gadget')")
	afExec(t, db, ctx, "INSERT INTO v83_sales VALUES (6, 'A', 150, 'Widget')")

	// HAVING with SUM
	checkRowCount("HAVING SUM > 500",
		"SELECT dept, SUM(amount) FROM v83_sales GROUP BY dept HAVING SUM(amount) > 500", 1)
	// HAVING with COUNT
	checkRowCount("HAVING COUNT > 2",
		"SELECT dept, COUNT(*) FROM v83_sales GROUP BY dept HAVING COUNT(*) > 2", 1)
	// HAVING with AVG
	checkRowCount("HAVING AVG > 200",
		"SELECT dept, AVG(amount) FROM v83_sales GROUP BY dept HAVING AVG(amount) > 200", 1)
	// HAVING with MIN
	checkRowCount("HAVING MIN >= 100",
		"SELECT dept, MIN(amount) FROM v83_sales GROUP BY dept HAVING MIN(amount) >= 100", 2)
	// HAVING with MAX (A has max 200, C has max 50 - both < 300)
	checkRowCount("HAVING MAX < 300",
		"SELECT dept, MAX(amount) FROM v83_sales GROUP BY dept HAVING MAX(amount) < 300", 2)
	// HAVING with complex expression
	checkRowCount("HAVING SUM/COUNT > 100",
		"SELECT dept FROM v83_sales GROUP BY dept HAVING SUM(amount) / COUNT(*) > 100", 2)
	// HAVING with AND
	checkRowCount("HAVING AND",
		"SELECT dept FROM v83_sales GROUP BY dept HAVING SUM(amount) > 200 AND COUNT(*) >= 2", 2)
	// HAVING with OR
	checkRowCount("HAVING OR",
		"SELECT dept FROM v83_sales GROUP BY dept HAVING SUM(amount) > 600 OR COUNT(*) = 1", 2)

	// ============================================================
	// === SECTION 4: GROUP BY + ORDER BY WITH AGGREGATES (66.7%) ===
	// ============================================================

	check("ORDER BY SUM ASC",
		"SELECT dept FROM v83_sales GROUP BY dept ORDER BY SUM(amount) ASC LIMIT 1", "C")
	check("ORDER BY SUM DESC",
		"SELECT dept FROM v83_sales GROUP BY dept ORDER BY SUM(amount) DESC LIMIT 1", "B")
	check("ORDER BY COUNT DESC",
		"SELECT dept FROM v83_sales GROUP BY dept ORDER BY COUNT(*) DESC LIMIT 1", "A")
	check("ORDER BY AVG",
		"SELECT dept FROM v83_sales GROUP BY dept ORDER BY AVG(amount) DESC LIMIT 1", "B")
	check("ORDER BY MAX",
		"SELECT dept FROM v83_sales GROUP BY dept ORDER BY MAX(amount) ASC LIMIT 1", "C")

	// GROUP BY with HAVING + ORDER BY
	check("GROUP BY + HAVING + ORDER BY",
		"SELECT dept FROM v83_sales GROUP BY dept HAVING COUNT(*) >= 2 ORDER BY SUM(amount) DESC LIMIT 1", "B")

	// ============================================================
	// === SECTION 5: JOIN + GROUP BY (evaluateExprWithGroupAggregatesJoin 68.4%) ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_dept (id INTEGER PRIMARY KEY, name TEXT, location TEXT)")
	afExec(t, db, ctx, "INSERT INTO v83_dept VALUES (1, 'A', 'NYC')")
	afExec(t, db, ctx, "INSERT INTO v83_dept VALUES (2, 'B', 'LA')")
	afExec(t, db, ctx, "INSERT INTO v83_dept VALUES (3, 'C', 'SF')")

	// JOIN + GROUP BY + aggregate
	check("JOIN GROUP BY SUM",
		`SELECT v83_dept.name
		 FROM v83_sales
		 JOIN v83_dept ON v83_dept.name = v83_sales.dept
		 GROUP BY v83_dept.name
		 ORDER BY SUM(v83_sales.amount) DESC LIMIT 1`, "B")

	// JOIN + GROUP BY + HAVING
	checkRowCount("JOIN GROUP BY HAVING",
		`SELECT v83_dept.name, COUNT(*) AS cnt
		 FROM v83_sales
		 JOIN v83_dept ON v83_dept.name = v83_sales.dept
		 GROUP BY v83_dept.name
		 HAVING COUNT(*) >= 2`, 2)

	// JOIN + GROUP BY + HAVING + ORDER BY
	check("JOIN GROUP HAVING ORDER",
		`SELECT v83_dept.location
		 FROM v83_sales
		 JOIN v83_dept ON v83_dept.name = v83_sales.dept
		 GROUP BY v83_dept.location
		 HAVING SUM(v83_sales.amount) > 200
		 ORDER BY SUM(v83_sales.amount) DESC LIMIT 1`, "LA")

	// JOIN + GROUP BY with CASE expression in SELECT
	check("JOIN GROUP CASE",
		`SELECT CASE WHEN SUM(v83_sales.amount) > 500 THEN 'high' ELSE 'low' END AS level
		 FROM v83_sales
		 JOIN v83_dept ON v83_dept.name = v83_sales.dept
		 GROUP BY v83_dept.name
		 ORDER BY SUM(v83_sales.amount) DESC LIMIT 1`, "high")

	// JOIN + GROUP BY + MIN/MAX
	check("JOIN GROUP BY MIN",
		`SELECT v83_dept.name
		 FROM v83_sales
		 JOIN v83_dept ON v83_dept.name = v83_sales.dept
		 GROUP BY v83_dept.name
		 ORDER BY MIN(v83_sales.amount) ASC LIMIT 1`, "C")

	// ============================================================
	// === SECTION 6: VIEW AGGREGATES (computeViewAggregate 68.9%) ===
	// ============================================================

	checkNoError("Create view SUM",
		`CREATE VIEW v83_dept_totals AS
		 SELECT dept, SUM(amount) AS total FROM v83_sales GROUP BY dept`)
	check("View SUM query",
		"SELECT total FROM v83_dept_totals WHERE dept = 'B'", float64(700))

	checkNoError("Create view AVG",
		`CREATE VIEW v83_dept_avgs AS
		 SELECT dept, AVG(amount) AS avg_amount FROM v83_sales GROUP BY dept`)
	check("View AVG query",
		"SELECT avg_amount FROM v83_dept_avgs WHERE dept = 'A'", float64(150))

	checkNoError("Create view COUNT",
		`CREATE VIEW v83_dept_counts AS
		 SELECT dept, COUNT(*) AS cnt FROM v83_sales GROUP BY dept`)
	check("View COUNT query",
		"SELECT cnt FROM v83_dept_counts WHERE dept = 'A'", float64(3))

	checkNoError("Create view MIN MAX",
		`CREATE VIEW v83_dept_minmax AS
		 SELECT dept, MIN(amount) AS mn, MAX(amount) AS mx FROM v83_sales GROUP BY dept`)
	check("View MIN query",
		"SELECT mn FROM v83_dept_minmax WHERE dept = 'B'", float64(300))
	check("View MAX query",
		"SELECT mx FROM v83_dept_minmax WHERE dept = 'B'", float64(400))

	check("View ORDER BY total",
		"SELECT dept FROM v83_dept_totals ORDER BY total DESC LIMIT 1", "B")

	checkRowCount("View WHERE filter",
		"SELECT * FROM v83_dept_totals WHERE total > 200", 2)

	// ============================================================
	// === SECTION 7: OUTER REFERENCES (resolveOuterRefsInExpr 52.2%) ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_items (id INTEGER PRIMARY KEY, category TEXT, price INTEGER, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v83_items VALUES (1, 'A', 10, 100)")
	afExec(t, db, ctx, "INSERT INTO v83_items VALUES (2, 'A', 20, 50)")
	afExec(t, db, ctx, "INSERT INTO v83_items VALUES (3, 'B', 30, 30)")
	afExec(t, db, ctx, "INSERT INTO v83_items VALUES (4, 'B', 15, 80)")
	afExec(t, db, ctx, "INSERT INTO v83_items VALUES (5, 'C', 50, 10)")

	// Correlated subquery in WHERE with >
	checkRowCount("Correlated > avg",
		`SELECT * FROM v83_items i1
		 WHERE price > (SELECT AVG(price) FROM v83_items i2 WHERE i2.category = i1.category)`, 2)

	// Correlated subquery in WHERE with =
	check("Correlated = max",
		`SELECT id FROM v83_items i1
		 WHERE price = (SELECT MAX(price) FROM v83_items i2 WHERE i2.category = i1.category)
		 ORDER BY id LIMIT 1`, float64(2))

	// EXISTS correlated subquery
	checkRowCount("EXISTS correlated",
		`SELECT * FROM v83_items i1
		 WHERE EXISTS (SELECT 1 FROM v83_items i2 WHERE i2.category = i1.category AND i2.id != i1.id)`, 4)

	// NOT EXISTS
	checkRowCount("NOT EXISTS correlated",
		`SELECT * FROM v83_items i1
		 WHERE NOT EXISTS (SELECT 1 FROM v83_items i2 WHERE i2.category = i1.category AND i2.id != i1.id)`, 1)

	// Correlated subquery in SELECT list
	check("Correlated in SELECT",
		`SELECT (SELECT SUM(price) FROM v83_items i2 WHERE i2.category = i1.category)
		 FROM v83_items i1 WHERE id = 1`, float64(30))

	// IN with subquery
	checkRowCount("IN subquery",
		`SELECT * FROM v83_items
		 WHERE category IN (SELECT category FROM v83_items GROUP BY category HAVING COUNT(*) > 1)`, 4)

	// ============================================================
	// === SECTION 8: JSON DEEPER PATHS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_json (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO v83_json VALUES (1, '{"name":"Alice","age":30,"address":{"city":"NYC","zip":"10001"}}')`)
	afExec(t, db, ctx, `INSERT INTO v83_json VALUES (2, '{"items":[1,2,3],"count":3}')`)
	afExec(t, db, ctx, `INSERT INTO v83_json VALUES (3, '{"nested":{"deep":{"value":42}}}')`)

	// JSON_EXTRACT nested path
	check("JSON_EXTRACT nested",
		"SELECT JSON_EXTRACT(data, '$.address.city') FROM v83_json WHERE id = 1", "NYC")
	check("JSON_EXTRACT deep nested",
		"SELECT JSON_EXTRACT(data, '$.nested.deep.value') FROM v83_json WHERE id = 3", float64(42))

	// JSON_TYPE on different value types
	check("JSON_TYPE string",
		`SELECT JSON_TYPE('{"k":"v"}', '$.k')`, "string")
	check("JSON_TYPE number",
		`SELECT JSON_TYPE('{"k":42}', '$.k')`, "number")
	check("JSON_TYPE array",
		`SELECT JSON_TYPE('{"k":[1,2]}', '$.k')`, "array")
	check("JSON_TYPE object",
		`SELECT JSON_TYPE('{"k":{"a":1}}', '$.k')`, "object")
	check("JSON_TYPE boolean",
		`SELECT JSON_TYPE('{"k":true}', '$.k')`, "boolean")
	check("JSON_TYPE null",
		`SELECT JSON_TYPE('{"k":null}', '$.k')`, "null")

	// JSON_VALID (returns bool)
	check("JSON_VALID valid", `SELECT JSON_VALID('{"a":1}')`, true)
	check("JSON_VALID invalid", "SELECT JSON_VALID('not json')", false)

	// JSON_ARRAY_LENGTH on top-level array
	check("JSON_ARRAY_LENGTH",
		"SELECT JSON_ARRAY_LENGTH('[1,2,3]')", float64(3))

	// JSON_QUOTE / UNQUOTE
	check("JSON_QUOTE", "SELECT JSON_QUOTE('hello')", `"hello"`)
	check("JSON_UNQUOTE", `SELECT JSON_UNQUOTE('"hello"')`, "hello")

	// ============================================================
	// === SECTION 9: SAVEPOINT ROLLBACK EDGE CASES (43.6%) ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_sp (id INTEGER PRIMARY KEY, val TEXT)")

	// Multiple savepoints, partial rollback
	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "INSERT INTO v83_sp VALUES (1, 'one')")
	afExec(t, db, ctx, "SAVEPOINT s1")
	afExec(t, db, ctx, "INSERT INTO v83_sp VALUES (2, 'two')")
	afExec(t, db, ctx, "SAVEPOINT s2")
	afExec(t, db, ctx, "INSERT INTO v83_sp VALUES (3, 'three')")
	afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT s2")
	check("After rollback to s2", "SELECT COUNT(*) FROM v83_sp", float64(2))

	// Rollback to s1 undoes s2 work too
	afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT s1")
	check("After rollback to s1", "SELECT COUNT(*) FROM v83_sp", float64(1))

	// RELEASE savepoint
	afExec(t, db, ctx, "SAVEPOINT s3")
	afExec(t, db, ctx, "INSERT INTO v83_sp VALUES (4, 'four')")
	afExec(t, db, ctx, "RELEASE SAVEPOINT s3")
	check("After release s3", "SELECT COUNT(*) FROM v83_sp", float64(2))
	afExec(t, db, ctx, "COMMIT")
	check("After commit sp", "SELECT COUNT(*) FROM v83_sp", float64(2))

	// Savepoint with UPDATE and rollback
	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "SAVEPOINT su")
	afExec(t, db, ctx, "UPDATE v83_sp SET val = 'updated' WHERE id = 1")
	check("Before rollback update", "SELECT val FROM v83_sp WHERE id = 1", "updated")
	afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT su")
	check("After rollback update", "SELECT val FROM v83_sp WHERE id = 1", "one")
	afExec(t, db, ctx, "COMMIT")

	// Savepoint with DELETE and rollback
	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "SAVEPOINT sd")
	afExec(t, db, ctx, "DELETE FROM v83_sp WHERE id = 4")
	check("Before rollback delete", "SELECT COUNT(*) FROM v83_sp", float64(1))
	afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sd")
	check("After rollback delete", "SELECT COUNT(*) FROM v83_sp", float64(2))
	afExec(t, db, ctx, "COMMIT")

	// ============================================================
	// === SECTION 10: FK ON DELETE CASCADE/SET NULL ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_parent (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, `CREATE TABLE v83_child_cascade (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		val TEXT,
		FOREIGN KEY (parent_id) REFERENCES v83_parent(id) ON DELETE CASCADE
	)`)
	afExec(t, db, ctx, `CREATE TABLE v83_child_setnull (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		val TEXT,
		FOREIGN KEY (parent_id) REFERENCES v83_parent(id) ON DELETE SET NULL
	)`)

	afExec(t, db, ctx, "INSERT INTO v83_parent VALUES (1, 'Parent1')")
	afExec(t, db, ctx, "INSERT INTO v83_parent VALUES (2, 'Parent2')")
	afExec(t, db, ctx, "INSERT INTO v83_child_cascade VALUES (1, 1, 'c1')")
	afExec(t, db, ctx, "INSERT INTO v83_child_cascade VALUES (2, 1, 'c2')")
	afExec(t, db, ctx, "INSERT INTO v83_child_cascade VALUES (3, 2, 'c3')")
	afExec(t, db, ctx, "INSERT INTO v83_child_setnull VALUES (1, 1, 's1')")
	afExec(t, db, ctx, "INSERT INTO v83_child_setnull VALUES (2, 2, 's2')")

	// CASCADE delete
	checkNoError("Delete parent 1", "DELETE FROM v83_parent WHERE id = 1")
	check("Cascade count", "SELECT COUNT(*) FROM v83_child_cascade", float64(1))
	check("Cascade remaining", "SELECT val FROM v83_child_cascade WHERE id = 3", "c3")

	// SET NULL delete
	check("SetNull parent_id", "SELECT parent_id FROM v83_child_setnull WHERE id = 1", nil)
	check("SetNull other intact", "SELECT parent_id FROM v83_child_setnull WHERE id = 2", float64(2))

	// ============================================================
	// === SECTION 11: COMPLEX EXPRESSIONS AND TYPE COERCION ===
	// ============================================================

	check("CAST string to INTEGER",
		"SELECT CAST('42' AS INTEGER) + 8", float64(50))
	check("CAST string to REAL",
		"SELECT CAST('3.14' AS REAL)", float64(3.14))
	check("CAST int to TEXT",
		"SELECT CAST(42 AS TEXT)", "42")
	check("CAST NULL to TEXT",
		"SELECT CAST(NULL AS TEXT)", nil)

	// Complex nested CASE
	check("Nested CASE",
		`SELECT CASE
			WHEN 1 > 2 THEN 'no'
			WHEN 2 > 3 THEN 'no'
			ELSE CASE
				WHEN 3 > 2 THEN 'yes'
				ELSE 'no'
			END
		END`, "yes")

	// COALESCE with mixed types
	check("COALESCE mixed", "SELECT COALESCE(NULL, NULL, 42)", float64(42))
	check("COALESCE first non-null", "SELECT COALESCE(1, 2, 3)", float64(1))

	// IIF function
	check("IIF true", "SELECT IIF(1=1, 'yes', 'no')", "yes")
	check("IIF false", "SELECT IIF(1=2, 'yes', 'no')", "no")

	// NULLIF
	check("NULLIF equal", "SELECT NULLIF(1, 1)", nil)
	check("NULLIF not equal", "SELECT NULLIF(1, 2)", float64(1))

	// TYPEOF
	check("TYPEOF int", "SELECT TYPEOF(42)", "integer")
	check("TYPEOF real", "SELECT TYPEOF(3.14)", "real")
	check("TYPEOF text", "SELECT TYPEOF('hello')", "text")
	check("TYPEOF null", "SELECT TYPEOF(NULL)", "null")

	// ============================================================
	// === SECTION 12: COMPLEX SELECT PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_data (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, cat TEXT)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v83_data VALUES (%d, %d, %d, '%s')",
			i, i*10, i*5, []string{"X", "Y", "Z"}[i%3]))
	}

	// GROUP BY with expression in select
	check("GROUP BY expr select",
		"SELECT cat FROM v83_data GROUP BY cat ORDER BY SUM(a) + SUM(b) DESC LIMIT 1", "Y")

	// GROUP BY with arithmetic in HAVING
	checkRowCount("GROUP BY arith HAVING",
		"SELECT cat FROM v83_data GROUP BY cat HAVING SUM(a) - SUM(b) > 100", 3)

	// DISTINCT with expression
	checkRowCount("DISTINCT expr",
		"SELECT DISTINCT cat FROM v83_data", 3)

	// DISTINCT with ORDER BY
	check("DISTINCT ORDER BY",
		"SELECT DISTINCT cat FROM v83_data ORDER BY cat ASC LIMIT 1", "X")

	// Subquery in WHERE with multiple conditions
	checkRowCount("Subquery multi WHERE",
		"SELECT * FROM v83_data WHERE a > 50 AND b < (SELECT AVG(b) FROM v83_data)", 5)

	// ============================================================
	// === SECTION 13: INDEX OPERATIONS ===
	// ============================================================

	checkNoError("Create index on cat",
		"CREATE INDEX idx_v83_cat ON v83_data(cat)")
	// After section 15: id=2 deleted (was Z), id=21 added (Q). X: ids 3,6,9,12,15,18 = 6
	checkRowCount("Index query", "SELECT * FROM v83_data WHERE cat = 'X'", 6)

	checkNoError("Create composite index",
		"CREATE INDEX idx_v83_ab ON v83_data(a, b)")

	checkNoError("Drop index", "DROP INDEX idx_v83_cat")

	checkNoError("Create index a",
		"CREATE INDEX idx_v83_a ON v83_data(a)")
	checkRowCount("Index range query", "SELECT * FROM v83_data WHERE a > 100 AND a < 150", 4)

	// ============================================================
	// === SECTION 14: VACUUM AND ANALYZE ===
	// ============================================================

	checkNoError("VACUUM", "VACUUM")
	checkNoError("ANALYZE", "ANALYZE v83_data")
	check("After vacuum count", "SELECT COUNT(*) FROM v83_data", float64(20))

	// ============================================================
	// === SECTION 15: COMPLEX DML ===
	// ============================================================

	// UPDATE with subquery in SET
	checkNoError("UPDATE subquery SET",
		"UPDATE v83_data SET a = (SELECT MAX(a) FROM v83_data) WHERE id = 1")
	check("After subquery SET", "SELECT a FROM v83_data WHERE id = 1", float64(200))

	// UPDATE with CASE
	checkNoError("UPDATE CASE",
		`UPDATE v83_data SET b = CASE
			WHEN cat = 'X' THEN b + 10
			WHEN cat = 'Y' THEN b + 20
			ELSE b + 30
		END WHERE id <= 6`)

	// DELETE with subquery
	checkNoError("DELETE subquery",
		"DELETE FROM v83_data WHERE a = (SELECT MIN(a) FROM v83_data)")
	check("After delete subquery", "SELECT COUNT(*) FROM v83_data", float64(19))

	// INSERT with computed values
	checkNoError("INSERT computed",
		"INSERT INTO v83_data VALUES (21, 100 + 50, 200 - 100, 'Q')")
	check("Computed insert a", "SELECT a FROM v83_data WHERE id = 21", float64(150))
	check("Computed insert b", "SELECT b FROM v83_data WHERE id = 21", float64(100))

	// ============================================================
	// === SECTION 16: WINDOW FUNCTIONS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_win (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v83_win VALUES (1, 'ENG', 100)")
	afExec(t, db, ctx, "INSERT INTO v83_win VALUES (2, 'ENG', 200)")
	afExec(t, db, ctx, "INSERT INTO v83_win VALUES (3, 'ENG', 150)")
	afExec(t, db, ctx, "INSERT INTO v83_win VALUES (4, 'HR', 120)")
	afExec(t, db, ctx, "INSERT INTO v83_win VALUES (5, 'HR', 180)")

	check("ROW_NUMBER",
		`SELECT rn FROM (
			SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM v83_win
		) sub WHERE id = 2`, float64(1))

	check("RANK partitioned",
		`SELECT rnk FROM (
			SELECT id, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk FROM v83_win
		) sub WHERE id = 2`, float64(1))

	check("DENSE_RANK",
		`SELECT dr FROM (
			SELECT id, DENSE_RANK() OVER (ORDER BY salary DESC) AS dr FROM v83_win
		) sub WHERE id = 2`, float64(1))

	check("SUM window",
		`SELECT total FROM (
			SELECT id, SUM(salary) OVER (PARTITION BY dept) AS total FROM v83_win
		) sub WHERE id = 1`, float64(450))

	check("COUNT window",
		`SELECT cnt FROM (
			SELECT id, COUNT(*) OVER (PARTITION BY dept) AS cnt FROM v83_win
		) sub WHERE id = 4`, float64(2))

	check("AVG window",
		`SELECT avg_sal FROM (
			SELECT id, AVG(salary) OVER (PARTITION BY dept) AS avg_sal FROM v83_win
		) sub WHERE id = 1`, float64(150))

	// ============================================================
	// === SECTION 17: COMPLEX CTE PATTERNS ===
	// ============================================================

	check("CTE with GROUP BY",
		`WITH dept_totals AS (
			SELECT dept, SUM(salary) AS total FROM v83_win GROUP BY dept
		)
		SELECT dept FROM dept_totals ORDER BY total DESC LIMIT 1`, "ENG")

	check("CTE with JOIN",
		`WITH dept_info AS (
			SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total FROM v83_win GROUP BY dept
		)
		SELECT v83_win.id FROM v83_win
		JOIN dept_info ON dept_info.dept = v83_win.dept
		WHERE dept_info.cnt > 2
		ORDER BY v83_win.salary DESC LIMIT 1`, float64(2))

	// Recursive CTE
	check("Recursive sequence",
		`WITH RECURSIVE nums(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM nums WHERE n < 5
		)
		SELECT SUM(n) FROM nums`, float64(15))

	// ============================================================
	// === SECTION 18: STRING FUNCTIONS ===
	// ============================================================

	check("UPPER", "SELECT UPPER('hello')", "HELLO")
	check("LOWER", "SELECT LOWER('HELLO')", "hello")
	check("LENGTH", "SELECT LENGTH('hello')", float64(5))
	check("TRIM", "SELECT TRIM('  hello  ')", "hello")
	check("LTRIM", "SELECT LTRIM('  hello')", "hello")
	check("RTRIM", "SELECT RTRIM('hello  ')", "hello")
	check("SUBSTR 2-arg", "SELECT SUBSTR('hello', 2)", "ello")
	check("SUBSTR 3-arg", "SELECT SUBSTR('hello', 2, 3)", "ell")
	check("REPLACE", "SELECT REPLACE('hello world', 'world', 'go')", "hello go")
	check("INSTR", "SELECT INSTR('hello', 'ell')", float64(2))
	check("REVERSE", "SELECT REVERSE('hello')", "olleh")
	check("REPEAT", "SELECT REPEAT('ab', 3)", "ababab")
	check("LEFT", "SELECT LEFT('hello', 3)", "hel")
	check("RIGHT", "SELECT RIGHT('hello', 3)", "llo")
	check("LPAD", "SELECT LPAD('hi', 5, '0')", "000hi")
	check("RPAD", "SELECT RPAD('hi', 5, '0')", "hi000")
	check("CONCAT_WS", "SELECT CONCAT_WS('-', 'a', 'b', 'c')", "a-b-c")
	check("HEX", "SELECT HEX('A')", "41")
	check("PRINTF", "SELECT PRINTF('%d items', 5)", "5 items")

	// ============================================================
	// === SECTION 19: MATH AND ARITHMETIC ===
	// ============================================================

	check("ABS positive", "SELECT ABS(42)", float64(42))
	check("ABS negative", "SELECT ABS(-42)", float64(42))
	check("ABS zero", "SELECT ABS(0)", float64(0))
	check("Modulo", "SELECT 17 % 5", float64(2))

	// ============================================================
	// === SECTION 20: SET OPERATIONS ===
	// ============================================================

	checkRowCount("UNION basic",
		`SELECT id, val FROM v83_sp
		 UNION
		 SELECT id, val FROM v83_sp`, 2)

	checkRowCount("UNION ALL",
		`SELECT id, val FROM v83_sp
		 UNION ALL
		 SELECT id, val FROM v83_sp`, 4)

	checkRowCount("INTERSECT",
		`SELECT cat FROM v83_data WHERE a > 100
		 INTERSECT
		 SELECT cat FROM v83_data WHERE b > 50`, 4)

	checkRowCount("EXCEPT basic",
		`SELECT cat FROM v83_data
		 EXCEPT
		 SELECT cat FROM v83_data WHERE cat = 'X'`, 3)

	// ============================================================
	// === SECTION 21: ALTER TABLE ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_alter (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO v83_alter VALUES (1, 'test')")

	checkNoError("ALTER ADD COLUMN",
		"ALTER TABLE v83_alter ADD COLUMN age INTEGER DEFAULT 25")
	check("After add column", "SELECT age FROM v83_alter WHERE id = 1", float64(25))

	checkNoError("ALTER ADD COLUMN no default",
		"ALTER TABLE v83_alter ADD COLUMN email TEXT")
	check("After add no default", "SELECT email FROM v83_alter WHERE id = 1", nil)

	checkNoError("ALTER RENAME TABLE",
		"ALTER TABLE v83_alter RENAME TO v83_alter2")
	check("After rename", "SELECT name FROM v83_alter2 WHERE id = 1", "test")

	checkNoError("ALTER RENAME COLUMN",
		"ALTER TABLE v83_alter2 RENAME COLUMN name TO full_name")
	check("After rename col", "SELECT full_name FROM v83_alter2 WHERE id = 1", "test")

	checkNoError("ALTER DROP COLUMN",
		"ALTER TABLE v83_alter2 DROP COLUMN email")

	// ============================================================
	// === SECTION 22: TRIGGERS WITH EXPRESSIONS IN BODY ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_trig (id INTEGER PRIMARY KEY, val INTEGER, status TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v83_trig_log (id INTEGER PRIMARY KEY, msg TEXT)")

	// Trigger with CASE WHEN in body (exercises resolveTriggerExpr CaseExpr)
	afExec(t, db, ctx, `CREATE TRIGGER v83_trig_case AFTER INSERT ON v83_trig
		FOR EACH ROW
		BEGIN
			INSERT INTO v83_trig_log VALUES (NEW.id,
				CASE WHEN NEW.val > 50 THEN 'high' ELSE 'low' END);
		END`)

	afExec(t, db, ctx, "INSERT INTO v83_trig VALUES (1, 15, 'active')")
	afExec(t, db, ctx, "INSERT INTO v83_trig VALUES (2, 75, 'active')")
	check("Trigger CASE low", "SELECT msg FROM v83_trig_log WHERE id = 1", "low")
	check("Trigger CASE high", "SELECT msg FROM v83_trig_log WHERE id = 2", "high")

	// Drop first trigger before creating another on same table/event
	afExec(t, db, ctx, "DROP TRIGGER v83_trig_case")

	// Trigger with arithmetic expression
	afExec(t, db, ctx, "CREATE TABLE v83_trig_log2 (id INTEGER PRIMARY KEY, computed INTEGER)")
	afExec(t, db, ctx, `CREATE TRIGGER v83_trig_arith AFTER INSERT ON v83_trig
		FOR EACH ROW
		BEGIN
			INSERT INTO v83_trig_log2 VALUES (NEW.id, NEW.val * 2 + 10);
		END`)

	afExec(t, db, ctx, "INSERT INTO v83_trig VALUES (3, 30, 'active')")
	check("Trigger arithmetic", "SELECT computed FROM v83_trig_log2 WHERE id = 3", float64(70))

	// Drop and create trigger with function call
	afExec(t, db, ctx, "DROP TRIGGER v83_trig_arith")
	afExec(t, db, ctx, `CREATE TRIGGER v83_trig_func AFTER INSERT ON v83_trig
		FOR EACH ROW
		BEGIN
			INSERT INTO v83_trig_log VALUES (NEW.id + 100, UPPER(NEW.status));
		END`)

	afExec(t, db, ctx, "INSERT INTO v83_trig VALUES (4, 40, 'pending')")
	check("Trigger UPPER func", "SELECT msg FROM v83_trig_log WHERE id = 104", "PENDING")

	afExec(t, db, ctx, "DROP TRIGGER v83_trig_func")

	// Trigger on UPDATE (exercises OLD references)
	afExec(t, db, ctx, `CREATE TRIGGER v83_trig_upd AFTER UPDATE ON v83_trig
		FOR EACH ROW
		BEGIN
			INSERT INTO v83_trig_log VALUES (OLD.id + 200, CASE WHEN NEW.val > OLD.val THEN 'increased' ELSE 'decreased' END);
		END`)

	afExec(t, db, ctx, "UPDATE v83_trig SET val = 100 WHERE id = 1")
	check("Trigger UPDATE increased", "SELECT msg FROM v83_trig_log WHERE id = 201", "increased")

	// ============================================================
	// === SECTION 23: CONSTRAINT EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v83_constr (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		age INTEGER CHECK(age >= 0 AND age <= 150),
		email TEXT UNIQUE
	)`)

	checkNoError("Insert constr 1", "INSERT INTO v83_constr VALUES (1, 'Alice', 30, 'alice@test.com')")
	checkError("NOT NULL violation", "INSERT INTO v83_constr VALUES (2, NULL, 25, 'bob@test.com')")
	checkError("CHECK violation negative", "INSERT INTO v83_constr VALUES (3, 'Charlie', -1, 'charlie@test.com')")
	checkError("CHECK violation high", "INSERT INTO v83_constr VALUES (4, 'Dave', 200, 'dave@test.com')")
	checkError("UNIQUE violation", "INSERT INTO v83_constr VALUES (5, 'Eve', 25, 'alice@test.com')")
	checkNoError("Insert constr ok", "INSERT INTO v83_constr VALUES (6, 'Frank', 0, 'frank@test.com')")

	// ============================================================
	// === SECTION 24: MATERIALIZED VIEW OPERATIONS ===
	// ============================================================

	checkNoError("Create matview",
		`CREATE MATERIALIZED VIEW v83_mv AS
		 SELECT cat, SUM(a) AS total_a, COUNT(*) AS cnt FROM v83_data GROUP BY cat`)
	checkNoError("Refresh matview", "REFRESH MATERIALIZED VIEW v83_mv")
	checkNoError("Drop matview", "DROP MATERIALIZED VIEW v83_mv")

	checkNoError("Create matview2",
		`CREATE MATERIALIZED VIEW v83_mv2 AS
		 SELECT dept, AVG(salary) AS avg_sal FROM v83_win GROUP BY dept`)
	afExec(t, db, ctx, "INSERT INTO v83_win VALUES (6, 'ENG', 300)")
	checkNoError("Refresh matview2", "REFRESH MATERIALIZED VIEW v83_mv2")
	checkNoError("Drop matview2", "DROP MATERIALIZED VIEW v83_mv2")

	// ============================================================
	// === SECTION 25: IF EXISTS / IF NOT EXISTS ===
	// ============================================================

	checkNoError("CREATE TABLE IF NOT EXISTS new",
		"CREATE TABLE IF NOT EXISTS v83_ifne (id INTEGER PRIMARY KEY)")
	checkNoError("CREATE TABLE IF NOT EXISTS existing",
		"CREATE TABLE IF NOT EXISTS v83_ifne (id INTEGER PRIMARY KEY)")
	checkNoError("DROP TABLE IF EXISTS",
		"DROP TABLE IF EXISTS v83_ifne")
	checkNoError("DROP TABLE IF EXISTS nonexistent",
		"DROP TABLE IF EXISTS v83_nonexistent")

	checkNoError("CREATE VIEW IF NOT EXISTS",
		"CREATE VIEW IF NOT EXISTS v83_v AS SELECT 1")
	checkNoError("CREATE VIEW IF NOT EXISTS dup",
		"CREATE VIEW IF NOT EXISTS v83_v AS SELECT 1")
	checkNoError("DROP VIEW IF EXISTS",
		"DROP VIEW IF EXISTS v83_v")
	checkNoError("DROP VIEW IF EXISTS nonexistent",
		"DROP VIEW IF EXISTS v83_nonexistent2")

	// ============================================================
	// === SECTION 26: COMPLEX WHERE EXPRESSIONS ===
	// ============================================================

	// Data after section 15: id=1(a=200), id=2 deleted, id=21(a=150,b=100,cat=Q). 20 rows total.
	checkRowCount("BETWEEN expr",
		"SELECT * FROM v83_data WHERE a BETWEEN 50 AND 100", 6)
	checkRowCount("IN expr",
		"SELECT * FROM v83_data WHERE id IN (1, 3, 5, 7, 9)", 5)
	checkRowCount("Complex bool WHERE",
		"SELECT * FROM v83_data WHERE (cat = 'X' OR cat = 'Y') AND a > 100", 7)
	checkRowCount("NOT IN",
		"SELECT * FROM v83_data WHERE cat NOT IN ('X', 'Y')", 7)
	checkRowCount("NOT BETWEEN",
		"SELECT * FROM v83_data WHERE a NOT BETWEEN 50 AND 150", 8)
	checkRowCount("IS NOT NULL",
		"SELECT * FROM v83_data WHERE cat IS NOT NULL", 20)

	// ============================================================
	// === SECTION 27: INSERT INTO SELECT ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v83_target (id INTEGER PRIMARY KEY, val INTEGER, cat TEXT)")
	checkNoError("INSERT INTO SELECT",
		"INSERT INTO v83_target SELECT id, a, cat FROM v83_data WHERE cat = 'X' AND id <= 10")
	check("INSERT INTO SELECT count",
		"SELECT COUNT(*) FROM v83_target", float64(3))

	// ============================================================
	// === SECTION 28: GROUP_CONCAT ===
	// ============================================================

	// GROUP_CONCAT returns concatenated string for dept A (3 rows)
	check("GROUP_CONCAT",
		"SELECT GROUP_CONCAT(dept) FROM v83_sales WHERE dept IN ('A', 'B') GROUP BY dept ORDER BY dept LIMIT 1", "A,A,A")

	// ============================================================
	// FINAL SCORE
	// ============================================================

	t.Logf("v83 Score: %d/%d tests passed", pass, total)
	if pass < total {
		t.Fatalf("v83: %d tests failed", total-pass)
	}
}
