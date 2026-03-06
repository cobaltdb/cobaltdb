package test

import (
	"fmt"
	"testing"
)

func TestV80MatViewFTSCoverage(t *testing.T) {
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

	_ = checkError

	// ============================================================
	// === SECTION 1: MATERIALIZED VIEWS ===
	// ============================================================

	// Setup base table
	afExec(t, db, ctx, "CREATE TABLE v80_mv_src (id INTEGER PRIMARY KEY, region TEXT, sales INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v80_mv_src VALUES (1, 'North', 100)")
	afExec(t, db, ctx, "INSERT INTO v80_mv_src VALUES (2, 'North', 200)")
	afExec(t, db, ctx, "INSERT INTO v80_mv_src VALUES (3, 'South', 150)")
	afExec(t, db, ctx, "INSERT INTO v80_mv_src VALUES (4, 'South', 250)")
	afExec(t, db, ctx, "INSERT INTO v80_mv_src VALUES (5, 'East', 300)")

	// CREATE MATERIALIZED VIEW
	checkNoError("CREATE MATERIALIZED VIEW",
		`CREATE MATERIALIZED VIEW v80_mv_summary AS
		 SELECT region, SUM(sales) AS total FROM v80_mv_src GROUP BY region`)

	// Cannot create duplicate
	checkError("Duplicate materialized view",
		`CREATE MATERIALIZED VIEW v80_mv_summary AS
		 SELECT region FROM v80_mv_src`)

	// Add more data to base table
	afExec(t, db, ctx, "INSERT INTO v80_mv_src VALUES (6, 'East', 100)")

	// REFRESH should update materialized view data
	checkNoError("REFRESH MATERIALIZED VIEW", "REFRESH MATERIALIZED VIEW v80_mv_summary")

	// Refresh non-existent should fail
	checkError("REFRESH non-existent matview", "REFRESH MATERIALIZED VIEW v80_nonexistent")

	// DROP MATERIALIZED VIEW
	checkNoError("DROP MATERIALIZED VIEW", "DROP MATERIALIZED VIEW v80_mv_summary")

	// Cannot drop non-existent
	checkError("DROP non-existent matview", "DROP MATERIALIZED VIEW v80_mv_summary")

	// ============================================================
	// === SECTION 2: FTS INDEX ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v80_docs (id INTEGER PRIMARY KEY, title TEXT, content TEXT)")
	afExec(t, db, ctx, "INSERT INTO v80_docs VALUES (1, 'Hello World', 'This is a hello world document')")
	afExec(t, db, ctx, "INSERT INTO v80_docs VALUES (2, 'Go Programming', 'Go is a compiled programming language')")
	afExec(t, db, ctx, "INSERT INTO v80_docs VALUES (3, 'SQL Tutorial', 'SQL is used for database queries')")

	// CREATE FULLTEXT INDEX
	checkNoError("CREATE FULLTEXT INDEX",
		"CREATE FULLTEXT INDEX v80_fts_docs ON v80_docs(title, content)")

	// ============================================================
	// === SECTION 3: JSON_REMOVE ===
	// ============================================================

	// JSON_REMOVE from object
	total++
	rows80r := afQuery(t, db, ctx, `SELECT JSON_REMOVE('{"name":"Alice","age":30,"city":"NYC"}', '$.age')`)
	if len(rows80r) > 0 && rows80r[0][0] != nil {
		s := fmt.Sprintf("%v", rows80r[0][0])
		if s == `{"city":"NYC","name":"Alice"}` || s == `{"name":"Alice","city":"NYC"}` {
			pass++
		} else {
			t.Errorf("[FAIL] JSON_REMOVE: got %s", s)
		}
	} else {
		t.Errorf("[FAIL] JSON_REMOVE: returned nil")
	}

	// ============================================================
	// === SECTION 4: JOIN + GROUP BY + AGGREGATES ===
	// (targets evaluateExprWithGroupAggregatesJoin)
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v80_depts (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v80_emps (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)")

	afExec(t, db, ctx, "INSERT INTO v80_depts VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO v80_depts VALUES (2, 'Marketing')")
	afExec(t, db, ctx, "INSERT INTO v80_depts VALUES (3, 'Sales')")

	afExec(t, db, ctx, "INSERT INTO v80_emps VALUES (1, 'Alice', 1, 80000)")
	afExec(t, db, ctx, "INSERT INTO v80_emps VALUES (2, 'Bob', 1, 90000)")
	afExec(t, db, ctx, "INSERT INTO v80_emps VALUES (3, 'Charlie', 2, 70000)")
	afExec(t, db, ctx, "INSERT INTO v80_emps VALUES (4, 'Diana', 2, 75000)")
	afExec(t, db, ctx, "INSERT INTO v80_emps VALUES (5, 'Eve', 3, 60000)")

	// JOIN + GROUP BY + COUNT
	check("JOIN GROUP BY COUNT",
		`SELECT v80_depts.name, COUNT(v80_emps.id)
		 FROM v80_depts
		 JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 GROUP BY v80_depts.name
		 ORDER BY COUNT(v80_emps.id) DESC
		 LIMIT 1`, "Engineering")

	// JOIN + GROUP BY + SUM
	check("JOIN GROUP BY SUM",
		`SELECT v80_depts.name, SUM(v80_emps.salary)
		 FROM v80_depts
		 JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 GROUP BY v80_depts.name
		 ORDER BY SUM(v80_emps.salary) DESC
		 LIMIT 1`, "Engineering")

	// JOIN + GROUP BY + AVG
	check("JOIN GROUP BY AVG",
		`SELECT v80_depts.name, AVG(v80_emps.salary)
		 FROM v80_depts
		 JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 GROUP BY v80_depts.name
		 ORDER BY AVG(v80_emps.salary) DESC
		 LIMIT 1`, "Engineering")

	// JOIN + GROUP BY + MIN/MAX
	check("JOIN GROUP BY MIN",
		`SELECT MIN(v80_emps.salary)
		 FROM v80_depts
		 JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 WHERE v80_depts.name = 'Engineering'`, float64(80000))

	check("JOIN GROUP BY MAX",
		`SELECT MAX(v80_emps.salary)
		 FROM v80_depts
		 JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 WHERE v80_depts.name = 'Engineering'`, float64(90000))

	// JOIN + GROUP BY + HAVING
	checkRowCount("JOIN GROUP BY HAVING",
		`SELECT v80_depts.name, SUM(v80_emps.salary)
		 FROM v80_depts
		 JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 GROUP BY v80_depts.name
		 HAVING SUM(v80_emps.salary) > 100000`, 2) // Engineering=170000, Marketing=145000

	// JOIN + GROUP BY + ORDER BY aggregate
	check("JOIN GROUP BY ORDER BY agg",
		`SELECT v80_depts.name
		 FROM v80_depts
		 JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 GROUP BY v80_depts.name
		 ORDER BY SUM(v80_emps.salary) ASC
		 LIMIT 1`, "Sales")

	// JOIN + GROUP BY + CASE in SELECT
	check("JOIN GROUP BY CASE",
		`SELECT CASE WHEN SUM(v80_emps.salary) > 100000 THEN 'large' ELSE 'small' END
		 FROM v80_depts
		 JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 WHERE v80_depts.name = 'Engineering'`, "large")

	// Multiple aggregates in JOIN + GROUP BY
	checkRowCount("JOIN multiple aggregates",
		`SELECT v80_depts.name, COUNT(*), SUM(v80_emps.salary), AVG(v80_emps.salary), MIN(v80_emps.salary), MAX(v80_emps.salary)
		 FROM v80_depts
		 JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 GROUP BY v80_depts.name`, 3)

	// JOIN + GROUP BY with expression in HAVING
	checkRowCount("JOIN HAVING with expression",
		`SELECT v80_depts.name
		 FROM v80_depts
		 JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 GROUP BY v80_depts.name
		 HAVING AVG(v80_emps.salary) > 70000`, 2) // Engineering=85000, Marketing=72500

	// LEFT JOIN + GROUP BY
	afExec(t, db, ctx, "INSERT INTO v80_depts VALUES (4, 'HR')") // No employees
	checkRowCount("LEFT JOIN GROUP BY",
		`SELECT v80_depts.name, COUNT(v80_emps.id)
		 FROM v80_depts
		 LEFT JOIN v80_emps ON v80_emps.dept_id = v80_depts.id
		 GROUP BY v80_depts.name`, 4)

	// ============================================================
	// === SECTION 5: GROUP BY + ORDER BY (no JOIN) ===
	// (targets applyGroupByOrderBy)
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v80_orders (id INTEGER PRIMARY KEY, product TEXT, qty INTEGER, price REAL)")
	afExec(t, db, ctx, "INSERT INTO v80_orders VALUES (1, 'Widget', 10, 5.0)")
	afExec(t, db, ctx, "INSERT INTO v80_orders VALUES (2, 'Widget', 20, 5.0)")
	afExec(t, db, ctx, "INSERT INTO v80_orders VALUES (3, 'Gadget', 5, 15.0)")
	afExec(t, db, ctx, "INSERT INTO v80_orders VALUES (4, 'Gadget', 8, 15.0)")
	afExec(t, db, ctx, "INSERT INTO v80_orders VALUES (5, 'Doohickey', 3, 25.0)")

	// ORDER BY aggregate ASC
	check("GROUP BY ORDER BY SUM ASC",
		`SELECT product FROM v80_orders GROUP BY product ORDER BY SUM(qty) ASC LIMIT 1`, "Doohickey")

	// ORDER BY aggregate DESC
	check("GROUP BY ORDER BY SUM DESC",
		`SELECT product FROM v80_orders GROUP BY product ORDER BY SUM(qty) DESC LIMIT 1`, "Widget")

	// ORDER BY COUNT
	check("GROUP BY ORDER BY COUNT",
		`SELECT product FROM v80_orders GROUP BY product ORDER BY COUNT(*) DESC LIMIT 1`, "Widget")

	// ORDER BY positional with GROUP BY
	check("GROUP BY ORDER BY positional",
		`SELECT product, SUM(qty) AS total FROM v80_orders GROUP BY product ORDER BY 2 DESC LIMIT 1`, "Widget")

	// ORDER BY AVG
	check("GROUP BY ORDER BY AVG",
		`SELECT product FROM v80_orders GROUP BY product ORDER BY AVG(price) DESC LIMIT 1`, "Doohickey")

	// ORDER BY multiple aggregates
	check("GROUP BY ORDER BY multi-agg",
		`SELECT product FROM v80_orders GROUP BY product ORDER BY COUNT(*) DESC, SUM(qty) DESC LIMIT 1`, "Widget")

	// ============================================================
	// === SECTION 6: HAVING EDGE CASES ===
	// ============================================================

	// HAVING with AND: Widget sum=30 avg=5, Gadget sum=13 avg=15, Doohickey sum=3 avg=25
	checkRowCount("HAVING AND",
		`SELECT product FROM v80_orders GROUP BY product
		 HAVING SUM(qty) > 5 AND AVG(price) < 20`, 2) // Widget and Gadget

	// HAVING with OR
	checkRowCount("HAVING OR",
		`SELECT product FROM v80_orders GROUP BY product
		 HAVING SUM(qty) > 20 OR AVG(price) > 20`, 2) // Widget (sum=30), Doohickey (avg=25)

	// HAVING with NOT
	checkRowCount("HAVING NOT",
		`SELECT product FROM v80_orders GROUP BY product
		 HAVING NOT SUM(qty) > 20`, 2) // Gadget (13), Doohickey (3)

	// HAVING with COUNT(*)
	checkRowCount("HAVING COUNT",
		`SELECT product FROM v80_orders GROUP BY product HAVING COUNT(*) >= 2`, 2) // Widget, Gadget

	// HAVING with MIN/MAX
	checkRowCount("HAVING MIN",
		`SELECT product FROM v80_orders GROUP BY product HAVING MIN(qty) >= 5`, 2) // Widget (10,20), Gadget (5,8)

	// HAVING with expression
	checkRowCount("HAVING expression",
		`SELECT product FROM v80_orders GROUP BY product
		 HAVING SUM(qty * price) > 100`, 2) // Widget=150, Gadget=195

	// ============================================================
	// === SECTION 7: COMPLEX LIKE PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v80_like (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO v80_like VALUES (1, 'apple')")
	afExec(t, db, ctx, "INSERT INTO v80_like VALUES (2, 'application')")
	afExec(t, db, ctx, "INSERT INTO v80_like VALUES (3, 'banana')")
	afExec(t, db, ctx, "INSERT INTO v80_like VALUES (4, 'APPLE')")
	afExec(t, db, ctx, "INSERT INTO v80_like VALUES (5, 'grape')")
	afExec(t, db, ctx, "INSERT INTO v80_like VALUES (6, NULL)")

	// Case-insensitive LIKE
	checkRowCount("LIKE case insensitive",
		"SELECT * FROM v80_like WHERE s LIKE 'apple'", 2) // apple, APPLE

	// LIKE with leading wildcard
	checkRowCount("LIKE leading %",
		"SELECT * FROM v80_like WHERE s LIKE '%ape'", 1) // grape

	// LIKE with both wildcards
	checkRowCount("LIKE both %",
		"SELECT * FROM v80_like WHERE s LIKE '%an%'", 1) // banana

	// LIKE with underscore
	checkRowCount("LIKE with _",
		"SELECT * FROM v80_like WHERE s LIKE '_____'", 3) // apple, APPLE, grape (5 chars)

	// LIKE with NULL - should be excluded
	checkRowCount("LIKE NULL excluded",
		"SELECT * FROM v80_like WHERE s LIKE '%'", 5) // NULL excluded

	// NOT LIKE
	checkRowCount("NOT LIKE",
		"SELECT * FROM v80_like WHERE s NOT LIKE 'app%'", 2) // banana, grape

	// LIKE exact match
	checkRowCount("LIKE exact",
		"SELECT * FROM v80_like WHERE s LIKE 'banana'", 1)

	// ============================================================
	// === SECTION 8: MORE FUNCTION CALLS ===
	// (targets evaluateFunctionCall)
	// ============================================================

	// Date/time functions (if supported)
	check("ABS negative", "SELECT ABS(-42)", float64(42))
	check("ABS positive", "SELECT ABS(42)", float64(42))
	check("ABS float", "SELECT ABS(-3.14)", 3.14)

	// MAX/MIN are aggregate-only, not scalar 2-arg
	// check("MAX scalar", "SELECT MAX(10, 20)", float64(20)) -- not supported as scalar

	// String functions
	check("LTRIM", "SELECT LTRIM('   hello')", "hello")
	check("RTRIM", "SELECT RTRIM('hello   ')", "hello")
	check("UPPER", "SELECT UPPER('hello')", "HELLO")
	check("LOWER", "SELECT LOWER('HELLO')", "hello")
	check("LENGTH", "SELECT LENGTH('hello')", float64(5))
	check("SUBSTR 2 args", "SELECT SUBSTR('hello', 2)", "ello")
	check("SUBSTR 3 args", "SELECT SUBSTR('hello', 2, 3)", "ell")
	check("REPLACE", "SELECT REPLACE('hello world', 'world', 'go')", "hello go")
	check("INSTR", "SELECT INSTR('hello world', 'world')", float64(7))
	check("TRIM", "SELECT TRIM('  hello  ')", "hello")

	// TYPEOF
	check("TYPEOF int", "SELECT TYPEOF(42)", "integer")
	check("TYPEOF real", "SELECT TYPEOF(3.14)", "real")
	check("TYPEOF text", "SELECT TYPEOF('hello')", "text")
	check("TYPEOF null", "SELECT TYPEOF(NULL)", "null")

	// COALESCE
	check("COALESCE first", "SELECT COALESCE(1, 2, 3)", float64(1))
	check("COALESCE skip null", "SELECT COALESCE(NULL, 2, 3)", float64(2))
	check("COALESCE all null", "SELECT COALESCE(NULL, NULL)", nil)

	// IIF
	check("IIF true", "SELECT IIF(1, 'yes', 'no')", "yes")
	check("IIF false", "SELECT IIF(0, 'yes', 'no')", "no")
	check("IIF null", "SELECT IIF(NULL, 'yes', 'no')", "no")

	// NULLIF
	check("NULLIF same", "SELECT NULLIF(1, 1)", nil)
	check("NULLIF different", "SELECT NULLIF(1, 2)", float64(1))

	// PRINTF
	check("PRINTF string", "SELECT PRINTF('%s world', 'hello')", "hello world")
	check("PRINTF int", "SELECT PRINTF('%d items', 42)", "42 items")

	// HEX
	check("HEX", "SELECT HEX('ABC')", "414243")

	// CHAR
	check("CHAR", "SELECT CHAR(65)", "A")

	// UNICODE
	check("UNICODE", "SELECT UNICODE('A')", float64(65))

	// REVERSE
	check("REVERSE", "SELECT REVERSE('hello')", "olleh")

	// REPEAT
	check("REPEAT", "SELECT REPEAT('ab', 3)", "ababab")

	// LEFT/RIGHT
	check("LEFT", "SELECT LEFT('hello', 3)", "hel")
	check("RIGHT", "SELECT RIGHT('hello', 3)", "llo")

	// LPAD/RPAD
	check("LPAD", "SELECT LPAD('hi', 5, '*')", "***hi")
	check("RPAD", "SELECT RPAD('hi', 5, '*')", "hi***")

	// CONCAT_WS
	check("CONCAT_WS", "SELECT CONCAT_WS(', ', 'a', 'b', 'c')", "a, b, c")

	// GLOB
	check("GLOB match", "SELECT GLOB('*ello*', 'hello world')", true)
	check("GLOB no match", "SELECT GLOB('xyz*', 'hello')", false)

	// ============================================================
	// === SECTION 9: COMPLEX WHERE EXPRESSIONS ===
	// (targets evaluateWhere deeper paths)
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v80_where (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL)")
	afExec(t, db, ctx, "INSERT INTO v80_where VALUES (1, 10, 'hello', 1.5)")
	afExec(t, db, ctx, "INSERT INTO v80_where VALUES (2, 20, 'world', 2.5)")
	afExec(t, db, ctx, "INSERT INTO v80_where VALUES (3, 30, 'foo', 3.5)")
	afExec(t, db, ctx, "INSERT INTO v80_where VALUES (4, NULL, NULL, NULL)")
	afExec(t, db, ctx, "INSERT INTO v80_where VALUES (5, 0, '', 0.0)")

	// WHERE with boolean expression from integer
	checkRowCount("WHERE int truthy",
		"SELECT * FROM v80_where WHERE a", 3) // 10, 20, 30 (non-zero)

	// WHERE with string truthy
	checkRowCount("WHERE string truthy",
		"SELECT * FROM v80_where WHERE b", 3) // 'hello', 'world', 'foo'

	// WHERE with complex boolean
	checkRowCount("WHERE complex AND OR",
		"SELECT * FROM v80_where WHERE (a > 15 OR b = 'hello') AND c IS NOT NULL", 3)

	// WHERE with IN list
	checkRowCount("WHERE IN list",
		"SELECT * FROM v80_where WHERE a IN (10, 30)", 2)

	// WHERE with NOT IN
	checkRowCount("WHERE NOT IN",
		"SELECT * FROM v80_where WHERE a NOT IN (10, 30) AND a IS NOT NULL", 2) // 20, 0

	// WHERE with BETWEEN
	checkRowCount("WHERE BETWEEN",
		"SELECT * FROM v80_where WHERE a BETWEEN 15 AND 25", 1)

	// WHERE with IS NULL
	checkRowCount("WHERE IS NULL",
		"SELECT * FROM v80_where WHERE a IS NULL", 1)

	// WHERE with nested functions
	checkRowCount("WHERE nested function",
		"SELECT * FROM v80_where WHERE LENGTH(b) > 3", 2) // hello(5), world(5)

	// WHERE with arithmetic: a=10->15, a=20->25, a=30->35, a=NULL->NULL, a=0->5
	// Only a=30 (35 > 25)
	checkRowCount("WHERE arithmetic",
		"SELECT * FROM v80_where WHERE a + 5 > 25", 1)

	// WHERE with CASE
	checkRowCount("WHERE CASE",
		`SELECT * FROM v80_where WHERE CASE WHEN a > 20 THEN 1 ELSE 0 END = 1`, 1) // a=30

	// ============================================================
	// === SECTION 10: COMPLEX ORDER BY ===
	// (targets applyOrderBy)
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v80_sort (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, grade TEXT)")
	afExec(t, db, ctx, "INSERT INTO v80_sort VALUES (1, 'Alice', 90, 'A')")
	afExec(t, db, ctx, "INSERT INTO v80_sort VALUES (2, 'Bob', 85, 'B')")
	afExec(t, db, ctx, "INSERT INTO v80_sort VALUES (3, 'Charlie', 90, 'A')")
	afExec(t, db, ctx, "INSERT INTO v80_sort VALUES (4, 'Diana', 70, 'C')")
	afExec(t, db, ctx, "INSERT INTO v80_sort VALUES (5, 'Eve', NULL, NULL)")

	// ORDER BY DESC - NULLs sort first in DESC in CobaltDB
	check("ORDER BY DESC",
		"SELECT name FROM v80_sort WHERE score IS NOT NULL ORDER BY score DESC LIMIT 1", "Alice")

	// ORDER BY multiple columns
	check("ORDER BY multi-col",
		"SELECT name FROM v80_sort WHERE score IS NOT NULL ORDER BY score DESC, name ASC LIMIT 1", "Alice")

	// ORDER BY with NULLs (NULLs sort last in ASC)
	check("ORDER BY NULL last ASC",
		"SELECT name FROM v80_sort ORDER BY score ASC LIMIT 1", "Diana")

	// ORDER BY expression
	check("ORDER BY expression",
		"SELECT name FROM v80_sort WHERE score IS NOT NULL ORDER BY score * -1 LIMIT 1", "Alice")

	// ORDER BY positional
	check("ORDER BY positional",
		"SELECT name, score FROM v80_sort WHERE score IS NOT NULL ORDER BY 2 DESC LIMIT 1", "Alice")

	// ORDER BY CASE expression
	check("ORDER BY CASE",
		`SELECT name FROM v80_sort WHERE score IS NOT NULL
		 ORDER BY CASE grade WHEN 'A' THEN 1 WHEN 'B' THEN 2 ELSE 3 END LIMIT 1`, "Alice")

	// ============================================================
	// === SECTION 11: WINDOW FUNCTION EDGE CASES ===
	// ============================================================

	// NTILE not implemented - skip
	// check("NTILE", "SELECT NTILE(2) OVER ...", float64(1))

	// Window aggregates in different partitions
	afExec(t, db, ctx, "CREATE TABLE v80_win (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v80_win VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v80_win VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO v80_win VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO v80_win VALUES (4, 'B', 100)")
	afExec(t, db, ctx, "INSERT INTO v80_win VALUES (5, 'B', 200)")

	// Window functions operate on the full result set before WHERE filter
	// Use subquery to get specific row's window result

	// Running SUM: A group cumulative: id1=10, id2=10+20=30, id3=10+20+30=60
	check("Window running SUM partition",
		`SELECT s FROM (SELECT id, SUM(val) OVER (PARTITION BY grp ORDER BY id) AS s FROM v80_win) sub WHERE id = 2`, float64(30))

	// Running COUNT: A group: id1=1, id2=2, id3=3
	check("Window running COUNT partition",
		`SELECT c FROM (SELECT id, COUNT(*) OVER (PARTITION BY grp ORDER BY id) AS c FROM v80_win) sub WHERE id = 3`, float64(3))

	// RANK: ordering by val DESC: 200(1), 100(2), 30(3), 20(4), 10(5)
	check("Window RANK",
		`SELECT rnk FROM (SELECT id, RANK() OVER (ORDER BY val DESC) AS rnk FROM v80_win) sub WHERE id = 1`, float64(5))

	// DENSE_RANK: same ordering
	check("Window DENSE_RANK",
		`SELECT dr FROM (SELECT id, DENSE_RANK() OVER (ORDER BY val DESC) AS dr FROM v80_win) sub WHERE id = 1`, float64(5))

	// LAG: prev value of id=2 (val=20) is id=1 (val=10)
	check("Window LAG",
		`SELECT lg FROM (SELECT id, LAG(val, 1) OVER (ORDER BY id) AS lg FROM v80_win) sub WHERE id = 2`, float64(10))

	// LEAD: next value of id=2 (val=20) is id=3 (val=30)
	check("Window LEAD",
		`SELECT ld FROM (SELECT id, LEAD(val, 1) OVER (ORDER BY id) AS ld FROM v80_win) sub WHERE id = 2`, float64(30))

	// FIRST_VALUE: A group first by id is val=10
	check("Window FIRST_VALUE",
		`SELECT fv FROM (SELECT id, FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY id) AS fv FROM v80_win) sub WHERE id = 3`, float64(10))

	// LAST_VALUE: A group last by id is val=30
	check("Window LAST_VALUE",
		`SELECT lv FROM (SELECT id, LAST_VALUE(val) OVER (PARTITION BY grp ORDER BY id) AS lv FROM v80_win) sub WHERE id = 1`, float64(30))

	// ============================================================
	// === SECTION 12: TRIGGER WITH EXPRESSIONS IN BODY ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v80_trig_src (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v80_trig_audit (id INTEGER PRIMARY KEY, doubled INTEGER, msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER v80_trig_calc
		AFTER INSERT ON v80_trig_src
		FOR EACH ROW
		BEGIN
			INSERT INTO v80_trig_audit VALUES (NEW.id, NEW.val * 2, 'inserted');
		END`)

	afExec(t, db, ctx, "INSERT INTO v80_trig_src VALUES (1, 50)")
	check("Trigger with expression",
		"SELECT doubled FROM v80_trig_audit WHERE id = 1", float64(100))
	check("Trigger msg",
		"SELECT msg FROM v80_trig_audit WHERE id = 1", "inserted")

	// Trigger with CASE + NEW refs: now fixed in resolveTriggerExpr
	afExec(t, db, ctx, "CREATE TABLE v80_trig_cond_src (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v80_trig_cond_log (id INTEGER PRIMARY KEY, category TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER v80_trig_cond
		AFTER INSERT ON v80_trig_cond_src
		FOR EACH ROW
		BEGIN
			INSERT INTO v80_trig_cond_log VALUES (NEW.id, CASE WHEN NEW.val > 50 THEN 'high' ELSE 'low' END);
		END`)

	afExec(t, db, ctx, "INSERT INTO v80_trig_cond_src VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO v80_trig_cond_src VALUES (2, 30)")

	check("Trigger CASE high",
		"SELECT category FROM v80_trig_cond_log WHERE id = 1", "high")
	check("Trigger CASE low",
		"SELECT category FROM v80_trig_cond_log WHERE id = 2", "low")

	// ============================================================
	// === SECTION 13: COMPLEX SUBQUERY PATTERNS ===
	// ============================================================

	// Subquery in SELECT list
	check("Subquery in SELECT",
		`SELECT (SELECT MAX(salary) FROM v80_emps) - (SELECT MIN(salary) FROM v80_emps)`, float64(30000))

	// Correlated subquery in WHERE
	checkRowCount("Correlated subquery WHERE",
		`SELECT name FROM v80_emps e
		 WHERE salary > (SELECT AVG(salary) FROM v80_emps e2 WHERE e2.dept_id = e.dept_id)`, 2) // Bob (90000 > 85000), Diana (75000 > 72500)

	// Scalar subquery in CASE
	check("Scalar subquery in CASE",
		`SELECT CASE
			WHEN (SELECT COUNT(*) FROM v80_emps) > 3 THEN 'many'
			ELSE 'few'
		 END`, "many")

	// Subquery in FROM (derived table)
	check("Derived table",
		`SELECT MAX(total) FROM (
			SELECT dept_id, SUM(salary) AS total FROM v80_emps GROUP BY dept_id
		) sub`, float64(170000))

	// ============================================================
	// === SECTION 14: UNION / INTERSECT / EXCEPT ===
	// ============================================================

	// UNION with different queries
	checkRowCount("UNION",
		`SELECT name FROM v80_emps WHERE dept_id = 1
		 UNION
		 SELECT name FROM v80_emps WHERE dept_id = 2`, 4)

	// UNION ALL (with duplicates)
	afExec(t, db, ctx, "CREATE TABLE v80_u1 (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v80_u1 VALUES ('a')")
	afExec(t, db, ctx, "INSERT INTO v80_u1 VALUES ('b')")
	afExec(t, db, ctx, "CREATE TABLE v80_u2 (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v80_u2 VALUES ('b')")
	afExec(t, db, ctx, "INSERT INTO v80_u2 VALUES ('c')")

	checkRowCount("UNION ALL",
		"SELECT val FROM v80_u1 UNION ALL SELECT val FROM v80_u2", 4)

	// UNION dedup
	checkRowCount("UNION dedup",
		"SELECT val FROM v80_u1 UNION SELECT val FROM v80_u2", 3) // a, b, c

	// INTERSECT
	checkRowCount("INTERSECT",
		"SELECT val FROM v80_u1 INTERSECT SELECT val FROM v80_u2", 1) // b

	// EXCEPT
	checkRowCount("EXCEPT",
		"SELECT val FROM v80_u1 EXCEPT SELECT val FROM v80_u2", 1) // a

	// ============================================================
	// === SECTION 15: COMPLEX CTE PATTERNS ===
	// ============================================================

	// CTE with aggregation
	check("CTE aggregate",
		`WITH dept_totals AS (
			SELECT dept_id, SUM(salary) AS total
			FROM v80_emps
			GROUP BY dept_id
		)
		SELECT MAX(total) FROM dept_totals`, float64(170000))

	// Chained CTEs
	check("Chained CTEs",
		`WITH
			raw AS (SELECT dept_id, salary FROM v80_emps),
			dept_avg AS (SELECT dept_id, AVG(salary) AS avg_sal FROM raw GROUP BY dept_id)
		SELECT MAX(avg_sal) FROM dept_avg`, float64(85000))

	// CTE used multiple times
	check("CTE used twice",
		`WITH nums AS (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3)
		SELECT (SELECT MAX(n) FROM nums) + (SELECT MIN(n) FROM nums)`, float64(4))

	// Recursive CTE - generate powers of 2
	check("Recursive CTE powers of 2",
		`WITH RECURSIVE powers(n, val) AS (
			SELECT 0, 1
			UNION ALL
			SELECT n + 1, val * 2 FROM powers WHERE n < 10
		)
		SELECT val FROM powers WHERE n = 10`, float64(1024))

	// ============================================================
	// === SECTION 16: ALTER TABLE EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v80_alter (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v80_alter VALUES (1, 'Alice', 30)")

	// ADD COLUMN with DEFAULT
	checkNoError("ALTER ADD COLUMN DEFAULT",
		"ALTER TABLE v80_alter ADD COLUMN city TEXT DEFAULT 'Unknown'")
	check("New column has default",
		"SELECT city FROM v80_alter WHERE id = 1", "Unknown")

	// New inserts work with new column
	afExec(t, db, ctx, "INSERT INTO v80_alter VALUES (2, 'Bob', 25, 'NYC')")
	check("New insert with added column",
		"SELECT city FROM v80_alter WHERE id = 2", "NYC")

	// RENAME TABLE
	checkNoError("ALTER RENAME TABLE",
		"ALTER TABLE v80_alter RENAME TO v80_altered")
	check("Query renamed table",
		"SELECT name FROM v80_altered WHERE id = 1", "Alice")

	// RENAME COLUMN
	checkNoError("ALTER RENAME COLUMN",
		"ALTER TABLE v80_altered RENAME COLUMN city TO location")
	check("Query renamed column",
		"SELECT location FROM v80_altered WHERE id = 1", "Unknown")

	// DROP COLUMN
	checkNoError("ALTER DROP COLUMN",
		"ALTER TABLE v80_altered DROP COLUMN age")
	checkRowCount("After drop column",
		"SELECT * FROM v80_altered", 2) // Should have id, name, location

	// ============================================================
	// === SECTION 17: INSERT INTO SELECT PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v80_ins_src (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v80_ins_src VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v80_ins_src VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v80_ins_src VALUES (3, 30)")

	afExec(t, db, ctx, "CREATE TABLE v80_ins_dest (id INTEGER PRIMARY KEY, val INTEGER)")

	// Basic INSERT INTO SELECT
	checkNoError("INSERT INTO SELECT basic",
		"INSERT INTO v80_ins_dest SELECT id, val FROM v80_ins_src WHERE val > 15")
	checkRowCount("INSERT INTO SELECT result", "SELECT * FROM v80_ins_dest", 2)

	// INSERT INTO SELECT with expression
	afExec(t, db, ctx, "CREATE TABLE v80_ins_expr (id INTEGER PRIMARY KEY, doubled INTEGER)")
	checkNoError("INSERT INTO SELECT expr",
		"INSERT INTO v80_ins_expr SELECT id, val * 2 FROM v80_ins_src")
	check("INSERT INTO SELECT expr result",
		"SELECT doubled FROM v80_ins_expr WHERE id = 1", float64(20))

	// ============================================================
	// === SECTION 18: CONSTRAINT EDGE CASES ===
	// ============================================================

	// NOT NULL constraint
	afExec(t, db, ctx, "CREATE TABLE v80_nn (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	checkNoError("NOT NULL insert valid", "INSERT INTO v80_nn VALUES (1, 'Alice')")
	checkError("NOT NULL insert null", "INSERT INTO v80_nn VALUES (2, NULL)")

	// UNIQUE constraint
	afExec(t, db, ctx, "CREATE TABLE v80_uniq (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
	checkNoError("UNIQUE insert 1", "INSERT INTO v80_uniq VALUES (1, 'a@b.com')")
	checkError("UNIQUE insert duplicate", "INSERT INTO v80_uniq VALUES (2, 'a@b.com')")

	// Multiple NULLs in UNIQUE (should be allowed)
	afExec(t, db, ctx, "INSERT INTO v80_uniq VALUES (3, NULL)")
	checkNoError("UNIQUE NULL second", "INSERT INTO v80_uniq VALUES (4, NULL)")

	// DEFAULT value
	afExec(t, db, ctx, "CREATE TABLE v80_def (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active')")
	// Can't easily test DEFAULT without explicit NULL insert since we need column list syntax

	// CHECK constraint
	afExec(t, db, ctx, "CREATE TABLE v80_chk (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0))")
	checkNoError("CHECK valid", "INSERT INTO v80_chk VALUES (1, 25)")
	checkError("CHECK violation", "INSERT INTO v80_chk VALUES (2, -5)")

	// ============================================================
	// === SECTION 19: MORE JSON EDGE CASES ===
	// ============================================================

	// JSON_EXTRACT with array index
	check("JSON_EXTRACT array[0]",
		`SELECT JSON_EXTRACT('[10,20,30]', '$[0]')`, float64(10))
	check("JSON_EXTRACT array[2]",
		`SELECT JSON_EXTRACT('[10,20,30]', '$[2]')`, float64(30))

	// JSON_EXTRACT deep nesting
	check("JSON_EXTRACT deep",
		`SELECT JSON_EXTRACT('{"a":{"b":{"c":"deep"}}}', '$.a.b.c')`, "deep")

	// JSON_TYPE with path
	check("JSON_TYPE with path",
		`SELECT JSON_TYPE('{"a":42}', '$.a')`, "number")

	// JSON_VALID edge cases
	check("JSON_VALID null literal", `SELECT JSON_VALID('null')`, true)
	check("JSON_VALID number", `SELECT JSON_VALID('42')`, true)
	check("JSON_VALID string", `SELECT JSON_VALID('"hello"')`, true)

	// JSON_ARRAY_LENGTH on non-array returns 0
	check("JSON_ARRAY_LENGTH object",
		`SELECT JSON_ARRAY_LENGTH('{"a":1}')`, float64(0))

	// JSON on table data
	afExec(t, db, ctx, "CREATE TABLE v80_json (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO v80_json VALUES (1, '{"items":[1,2,3],"name":"test"}')`)

	check("JSON_EXTRACT from table nested",
		`SELECT JSON_EXTRACT(data, '$.items[1]') FROM v80_json WHERE id = 1`, float64(2))
	check("JSON_VALID from table",
		`SELECT JSON_VALID(data) FROM v80_json WHERE id = 1`, true)

	// ============================================================
	// === SECTION 20: EXPRESSIONS IN INSERT VALUES ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v80_expr_ins (id INTEGER PRIMARY KEY, val INTEGER, computed TEXT)")
	afExec(t, db, ctx, "INSERT INTO v80_expr_ins VALUES (1, 10 + 20, UPPER('hello'))")
	check("Expression in INSERT int",
		"SELECT val FROM v80_expr_ins WHERE id = 1", float64(30))
	check("Expression in INSERT text",
		"SELECT computed FROM v80_expr_ins WHERE id = 1", "HELLO")

	// Subquery in INSERT VALUES
	afExec(t, db, ctx, "INSERT INTO v80_expr_ins VALUES (2, (SELECT MAX(val) FROM v80_expr_ins), 'from_sub')")
	check("Subquery in INSERT",
		"SELECT val FROM v80_expr_ins WHERE id = 2", float64(30))

	t.Logf("v80 Score: %d/%d tests passed", pass, total)
}
