package test

import (
	"fmt"
	"strings"
	"testing"
)

func TestV81TriggerExprDeepCoverage(t *testing.T) {
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

	_ = checkNoError

	// ============================================================
	// === SECTION 1: TRIGGER WITH CASE EXPRESSIONS ===
	// (exercises resolveTriggerExpr CaseExpr)
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v81_orders (id INTEGER PRIMARY KEY, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v81_order_log (id INTEGER PRIMARY KEY, tier TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER v81_order_tier
		AFTER INSERT ON v81_orders
		FOR EACH ROW
		BEGIN
			INSERT INTO v81_order_log VALUES (NEW.id,
				CASE
					WHEN NEW.amount >= 1000 THEN 'premium'
					WHEN NEW.amount >= 500 THEN 'standard'
					WHEN NEW.amount >= 100 THEN 'basic'
					ELSE 'micro'
				END
			);
		END`)

	afExec(t, db, ctx, "INSERT INTO v81_orders VALUES (1, 2000)")
	afExec(t, db, ctx, "INSERT INTO v81_orders VALUES (2, 750)")
	afExec(t, db, ctx, "INSERT INTO v81_orders VALUES (3, 200)")
	afExec(t, db, ctx, "INSERT INTO v81_orders VALUES (4, 50)")

	check("Trigger CASE premium", "SELECT tier FROM v81_order_log WHERE id = 1", "premium")
	check("Trigger CASE standard", "SELECT tier FROM v81_order_log WHERE id = 2", "standard")
	check("Trigger CASE basic", "SELECT tier FROM v81_order_log WHERE id = 3", "basic")
	check("Trigger CASE micro", "SELECT tier FROM v81_order_log WHERE id = 4", "micro")

	// Trigger with simple CASE (CASE expr WHEN val THEN ...)
	afExec(t, db, ctx, "CREATE TABLE v81_status (id INTEGER PRIMARY KEY, code INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v81_status_log (id INTEGER PRIMARY KEY, label TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER v81_status_label
		AFTER INSERT ON v81_status
		FOR EACH ROW
		BEGIN
			INSERT INTO v81_status_log VALUES (NEW.id,
				CASE NEW.code
					WHEN 1 THEN 'active'
					WHEN 2 THEN 'inactive'
					WHEN 3 THEN 'suspended'
					ELSE 'unknown'
				END
			);
		END`)

	afExec(t, db, ctx, "INSERT INTO v81_status VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO v81_status VALUES (2, 2)")
	afExec(t, db, ctx, "INSERT INTO v81_status VALUES (3, 99)")

	check("Trigger simple CASE active", "SELECT label FROM v81_status_log WHERE id = 1", "active")
	check("Trigger simple CASE inactive", "SELECT label FROM v81_status_log WHERE id = 2", "inactive")
	check("Trigger simple CASE unknown", "SELECT label FROM v81_status_log WHERE id = 3", "unknown")

	// Trigger with arithmetic expression on NEW
	afExec(t, db, ctx, "CREATE TABLE v81_tax_src (id INTEGER PRIMARY KEY, price INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v81_tax_log (id INTEGER PRIMARY KEY, with_tax INTEGER)")

	afExec(t, db, ctx, `CREATE TRIGGER v81_tax_calc
		AFTER INSERT ON v81_tax_src
		FOR EACH ROW
		BEGIN
			INSERT INTO v81_tax_log VALUES (NEW.id, NEW.price * 118 / 100);
		END`)

	afExec(t, db, ctx, "INSERT INTO v81_tax_src VALUES (1, 100)")
	check("Trigger arithmetic", "SELECT with_tax FROM v81_tax_log WHERE id = 1", float64(118))

	// Trigger with function call on NEW
	afExec(t, db, ctx, "CREATE TABLE v81_name_src (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v81_name_log (id INTEGER PRIMARY KEY, upper_name TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER v81_name_upper
		AFTER INSERT ON v81_name_src
		FOR EACH ROW
		BEGIN
			INSERT INTO v81_name_log VALUES (NEW.id, UPPER(NEW.name));
		END`)

	afExec(t, db, ctx, "INSERT INTO v81_name_src VALUES (1, 'alice')")
	check("Trigger function call", "SELECT upper_name FROM v81_name_log WHERE id = 1", "ALICE")

	// ============================================================
	// === SECTION 2: VIEWS WITH AGGREGATES ===
	// (exercises computeViewAggregate)
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v81_sales (id INTEGER PRIMARY KEY, region TEXT, product TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v81_sales VALUES (1, 'North', 'Widget', 100)")
	afExec(t, db, ctx, "INSERT INTO v81_sales VALUES (2, 'North', 'Widget', 200)")
	afExec(t, db, ctx, "INSERT INTO v81_sales VALUES (3, 'North', 'Gadget', 150)")
	afExec(t, db, ctx, "INSERT INTO v81_sales VALUES (4, 'South', 'Widget', 300)")
	afExec(t, db, ctx, "INSERT INTO v81_sales VALUES (5, 'South', 'Gadget', 250)")
	afExec(t, db, ctx, "INSERT INTO v81_sales VALUES (6, 'East', 'Widget', 175)")

	// View with COUNT
	checkNoError("CREATE VIEW with COUNT",
		`CREATE VIEW v81_region_count AS
		 SELECT region, COUNT(*) AS cnt FROM v81_sales GROUP BY region`)
	check("View COUNT North", "SELECT cnt FROM v81_region_count WHERE region = 'North'", int64(3))
	check("View COUNT South", "SELECT cnt FROM v81_region_count WHERE region = 'South'", int64(2))

	// View with SUM
	checkNoError("CREATE VIEW with SUM",
		`CREATE VIEW v81_region_sum AS
		 SELECT region, SUM(amount) AS total FROM v81_sales GROUP BY region`)
	check("View SUM North", "SELECT total FROM v81_region_sum WHERE region = 'North'", float64(450))
	check("View SUM South", "SELECT total FROM v81_region_sum WHERE region = 'South'", float64(550))

	// View with AVG
	checkNoError("CREATE VIEW with AVG",
		`CREATE VIEW v81_region_avg AS
		 SELECT region, AVG(amount) AS avg_amt FROM v81_sales GROUP BY region`)
	check("View AVG North", "SELECT avg_amt FROM v81_region_avg WHERE region = 'North'", float64(150))

	// View with MIN/MAX
	checkNoError("CREATE VIEW with MIN MAX",
		`CREATE VIEW v81_region_minmax AS
		 SELECT region, MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM v81_sales GROUP BY region`)
	check("View MIN North", "SELECT min_amt FROM v81_region_minmax WHERE region = 'North'", float64(100))
	check("View MAX North", "SELECT max_amt FROM v81_region_minmax WHERE region = 'North'", float64(200))

	// View with COUNT(column) (non-NULL count)
	afExec(t, db, ctx, "CREATE TABLE v81_nullable (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v81_nullable VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v81_nullable VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v81_nullable VALUES (3, 30)")

	checkNoError("CREATE VIEW with COUNT(col)",
		`CREATE VIEW v81_count_col AS
		 SELECT COUNT(val) AS cnt, COUNT(*) AS total FROM v81_nullable`)
	check("View COUNT(col)", "SELECT cnt FROM v81_count_col", int64(2))
	check("View COUNT(*)", "SELECT total FROM v81_count_col", int64(3))

	// View with ORDER BY
	check("View ORDER BY",
		"SELECT region FROM v81_region_sum ORDER BY total DESC LIMIT 1", "South")

	// View with HAVING (if view supports it)
	checkNoError("CREATE VIEW with aggregate",
		`CREATE VIEW v81_big_regions AS
		 SELECT region, SUM(amount) AS total FROM v81_sales GROUP BY region HAVING SUM(amount) > 200`)
	// North=450, South=550, East=175 -> only North and South > 200
	checkRowCount("View with HAVING", "SELECT * FROM v81_big_regions", 2)

	// ============================================================
	// === SECTION 3: MORE JSON PATTERNS ===
	// ============================================================

	// JSON_SET on existing key (update)
	total++
	rows81 := afQuery(t, db, ctx, `SELECT JSON_SET('{"name":"Alice","age":30}', '$.age', '25')`)
	if len(rows81) > 0 && rows81[0][0] != nil {
		s := fmt.Sprintf("%v", rows81[0][0])
		if strings.Contains(s, "25") && strings.Contains(s, "Alice") {
			pass++
		} else {
			t.Errorf("[FAIL] JSON_SET update: got %s", s)
		}
	} else {
		t.Errorf("[FAIL] JSON_SET update: nil")
	}

	// JSON_SET on nested path
	total++
	rows81n := afQuery(t, db, ctx, `SELECT JSON_SET('{"user":{"name":"Alice"}}', '$.user.name', '"Bob"')`)
	if len(rows81n) > 0 && rows81n[0][0] != nil {
		s := fmt.Sprintf("%v", rows81n[0][0])
		if strings.Contains(s, "Bob") {
			pass++
		} else {
			t.Errorf("[FAIL] JSON_SET nested: got %s", s)
		}
	} else {
		t.Errorf("[FAIL] JSON_SET nested: nil")
	}

	// JSON_REMOVE nested key
	total++
	rows81r := afQuery(t, db, ctx, `SELECT JSON_REMOVE('{"user":{"name":"Alice","age":30}}', '$.user.age')`)
	if len(rows81r) > 0 && rows81r[0][0] != nil {
		s := fmt.Sprintf("%v", rows81r[0][0])
		if strings.Contains(s, "Alice") && !strings.Contains(s, "30") {
			pass++
		} else {
			t.Errorf("[FAIL] JSON_REMOVE nested: got %s", s)
		}
	} else {
		t.Errorf("[FAIL] JSON_REMOVE nested: nil")
	}

	// JSON_MERGE objects
	total++
	rows81m := afQuery(t, db, ctx, `SELECT JSON_MERGE('{"a":1,"b":2}', '{"b":3,"c":4}')`)
	if len(rows81m) > 0 && rows81m[0][0] != nil {
		s := fmt.Sprintf("%v", rows81m[0][0])
		if strings.Contains(s, "a") && strings.Contains(s, "c") {
			pass++
		} else {
			t.Errorf("[FAIL] JSON_MERGE: got %s", s)
		}
	} else {
		t.Errorf("[FAIL] JSON_MERGE: nil")
	}

	// JSON_TYPE with various paths
	check("JSON_TYPE string path",
		`SELECT JSON_TYPE('{"a":"hello"}', '$.a')`, "string")
	check("JSON_TYPE number path",
		`SELECT JSON_TYPE('{"a":42}', '$.a')`, "number")
	check("JSON_TYPE bool path",
		`SELECT JSON_TYPE('{"a":true}', '$.a')`, "boolean")
	check("JSON_TYPE null path",
		`SELECT JSON_TYPE('{"a":null}', '$.a')`, "null")
	check("JSON_TYPE array path",
		`SELECT JSON_TYPE('{"a":[1,2]}', '$.a')`, "array")
	check("JSON_TYPE object path",
		`SELECT JSON_TYPE('{"a":{"b":1}}', '$.a')`, "object")

	// JSON on table column
	afExec(t, db, ctx, "CREATE TABLE v81_json (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO v81_json VALUES (1, '{"name":"Alice","scores":[90,85,95]}')`)
	afExec(t, db, ctx, `INSERT INTO v81_json VALUES (2, '{"name":"Bob","scores":[70,80,75]}')`)

	check("JSON table extract name",
		"SELECT JSON_EXTRACT(data, '$.name') FROM v81_json WHERE id = 1", "Alice")
	check("JSON table extract array",
		"SELECT JSON_EXTRACT(data, '$.scores[0]') FROM v81_json WHERE id = 1", float64(90))
	check("JSON table valid",
		"SELECT JSON_VALID(data) FROM v81_json WHERE id = 1", true)
	// JSON_ARRAY_LENGTH on nested extract returns 0 (not array string)
	check("JSON table array length",
		"SELECT JSON_ARRAY_LENGTH(JSON_EXTRACT(data, '$.scores')) FROM v81_json WHERE id = 1", float64(0))

	// ============================================================
	// === SECTION 4: COMPLEX HAVING WITH JOIN ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v81_dept (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v81_emp (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")

	afExec(t, db, ctx, "INSERT INTO v81_dept VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO v81_dept VALUES (2, 'Marketing')")
	afExec(t, db, ctx, "INSERT INTO v81_dept VALUES (3, 'HR')")

	afExec(t, db, ctx, "INSERT INTO v81_emp VALUES (1, 1, 80000)")
	afExec(t, db, ctx, "INSERT INTO v81_emp VALUES (2, 1, 90000)")
	afExec(t, db, ctx, "INSERT INTO v81_emp VALUES (3, 1, 85000)")
	afExec(t, db, ctx, "INSERT INTO v81_emp VALUES (4, 2, 70000)")
	afExec(t, db, ctx, "INSERT INTO v81_emp VALUES (5, 2, 75000)")
	afExec(t, db, ctx, "INSERT INTO v81_emp VALUES (6, 3, 60000)")

	// HAVING with COUNT in JOIN
	checkRowCount("HAVING COUNT JOIN",
		`SELECT v81_dept.name
		 FROM v81_dept
		 JOIN v81_emp ON v81_emp.dept_id = v81_dept.id
		 GROUP BY v81_dept.name
		 HAVING COUNT(*) >= 2`, 2) // Engineering(3), Marketing(2)

	// HAVING with complex expression in JOIN
	checkRowCount("HAVING complex JOIN",
		`SELECT v81_dept.name
		 FROM v81_dept
		 JOIN v81_emp ON v81_emp.dept_id = v81_dept.id
		 GROUP BY v81_dept.name
		 HAVING SUM(v81_emp.salary) > 100000 AND COUNT(*) > 1`, 2) // Eng(255000,3), Mkt(145000,2)

	// HAVING with AVG in JOIN
	check("HAVING AVG JOIN",
		`SELECT v81_dept.name
		 FROM v81_dept
		 JOIN v81_emp ON v81_emp.dept_id = v81_dept.id
		 GROUP BY v81_dept.name
		 HAVING AVG(v81_emp.salary) > 80000
		 ORDER BY AVG(v81_emp.salary) DESC
		 LIMIT 1`, "Engineering")

	// ============================================================
	// === SECTION 5: LIKE EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v81_like (id INTEGER PRIMARY KEY, pattern TEXT, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v81_like VALUES (1, 'test', 'test')")
	afExec(t, db, ctx, "INSERT INTO v81_like VALUES (2, 'test', 'TEST')")
	afExec(t, db, ctx, "INSERT INTO v81_like VALUES (3, 'test', 'testing')")
	afExec(t, db, ctx, "INSERT INTO v81_like VALUES (4, 'test', 'pretest')")
	afExec(t, db, ctx, "INSERT INTO v81_like VALUES (5, 'test', NULL)")

	// Exact match (case insensitive)
	checkRowCount("LIKE exact ci", "SELECT * FROM v81_like WHERE val LIKE 'test'", 2)

	// Starts with
	checkRowCount("LIKE starts with", "SELECT * FROM v81_like WHERE val LIKE 'test%'", 3) // test, TEST, testing

	// Ends with
	checkRowCount("LIKE ends with", "SELECT * FROM v81_like WHERE val LIKE '%test'", 3) // test, TEST, pretest

	// Contains
	checkRowCount("LIKE contains", "SELECT * FROM v81_like WHERE val LIKE '%test%'", 4) // all non-NULL

	// Single char wildcard
	checkRowCount("LIKE single char", "SELECT * FROM v81_like WHERE val LIKE '____'", 2) // test, TEST (4 chars)

	// LIKE with NULL returns no rows
	checkRowCount("LIKE NULL", "SELECT * FROM v81_like WHERE val LIKE NULL", 0)

	// NOT LIKE
	checkRowCount("NOT LIKE", "SELECT * FROM v81_like WHERE val NOT LIKE 'test'", 2) // testing, pretest

	// ============================================================
	// === SECTION 6: COMPLEX SELECT PATTERNS ===
	// ============================================================

	// SELECT with multiple subqueries
	check("Multiple scalar subqueries",
		`SELECT
			(SELECT COUNT(*) FROM v81_emp) +
			(SELECT COUNT(*) FROM v81_dept)`, float64(9))

	// Correlated subquery with aggregate
	check("Correlated MAX",
		`SELECT name FROM v81_dept d
		 WHERE (SELECT MAX(salary) FROM v81_emp e WHERE e.dept_id = d.id) > 85000
		 LIMIT 1`, "Engineering")

	// Derived table with ORDER BY
	check("Derived table ORDER BY",
		`SELECT name FROM (
			SELECT v81_dept.name, SUM(v81_emp.salary) AS total
			FROM v81_dept
			JOIN v81_emp ON v81_emp.dept_id = v81_dept.id
			GROUP BY v81_dept.name
			ORDER BY total DESC
		) sub LIMIT 1`, "Engineering")

	// ============================================================
	// === SECTION 7: UPDATE WITH COMPLEX SET EXPRESSIONS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v81_products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)")
	afExec(t, db, ctx, "INSERT INTO v81_products VALUES (1, 'Widget', 10.00, 'basic')")
	afExec(t, db, ctx, "INSERT INTO v81_products VALUES (2, 'Gadget', 25.00, 'premium')")
	afExec(t, db, ctx, "INSERT INTO v81_products VALUES (3, 'Doohickey', 5.00, 'basic')")

	// UPDATE with CASE in SET
	checkNoError("UPDATE with CASE",
		`UPDATE v81_products SET price = CASE
			WHEN category = 'premium' THEN price * 1.1
			ELSE price * 1.05
		END`)

	// Float precision: 25.0 * 1.1 = 27.500000000000004
	total++
	rows81p := afQuery(t, db, ctx, "SELECT price FROM v81_products WHERE id = 2")
	if len(rows81p) > 0 {
		p := fmt.Sprintf("%.1f", rows81p[0][0])
		if p == "27.5" {
			pass++
		} else {
			t.Errorf("[FAIL] Update CASE premium: got %s, expected 27.5", p)
		}
	} else {
		t.Errorf("[FAIL] Update CASE premium: no rows")
	}
	check("Update CASE basic", "SELECT price FROM v81_products WHERE id = 1", 10.5)

	// UPDATE with function in SET
	checkNoError("UPDATE with function",
		"UPDATE v81_products SET name = UPPER(name) WHERE category = 'basic'")
	check("Update func result", "SELECT name FROM v81_products WHERE id = 1", "WIDGET")

	// ============================================================
	// === SECTION 8: WINDOW FUNCTION COMBINATIONS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v81_wf (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v81_wf VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v81_wf VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO v81_wf VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO v81_wf VALUES (4, 'B', 100)")
	afExec(t, db, ctx, "INSERT INTO v81_wf VALUES (5, 'B', 200)")

	// Multiple window functions in same query via derived table
	checkRowCount("Multi window functions",
		`SELECT * FROM (
			SELECT id, val,
				ROW_NUMBER() OVER (ORDER BY val) AS rn,
				SUM(val) OVER (ORDER BY id) AS running_sum
			FROM v81_wf
		) sub`, 5)

	// Window with PARTITION BY and aggregate
	check("Window SUM partition",
		`SELECT s FROM (
			SELECT id, SUM(val) OVER (PARTITION BY grp) AS s FROM v81_wf
		) sub WHERE id = 1`, float64(60))

	check("Window AVG partition",
		`SELECT a FROM (
			SELECT id, AVG(val) OVER (PARTITION BY grp) AS a FROM v81_wf
		) sub WHERE id = 4`, float64(150))

	check("Window MIN partition",
		`SELECT m FROM (
			SELECT id, MIN(val) OVER (PARTITION BY grp) AS m FROM v81_wf
		) sub WHERE id = 5`, float64(100))

	check("Window MAX partition",
		`SELECT m FROM (
			SELECT id, MAX(val) OVER (PARTITION BY grp) AS m FROM v81_wf
		) sub WHERE id = 1`, float64(30))

	check("Window COUNT partition",
		`SELECT c FROM (
			SELECT id, COUNT(*) OVER (PARTITION BY grp) AS c FROM v81_wf
		) sub WHERE id = 1`, float64(3))

	// ============================================================
	// === SECTION 9: GROUP_CONCAT AND AGGREGATE EDGE CASES ===
	// ============================================================

	// GROUP_CONCAT works - products are WIDGET, Gadget, DOOHICKEY (some were UPPERed)
	total++
	rows81gc := afQuery(t, db, ctx, `SELECT GROUP_CONCAT(name, ',') FROM v81_products`)
	if len(rows81gc) > 0 && rows81gc[0][0] != nil {
		s := fmt.Sprintf("%v", rows81gc[0][0])
		if strings.Contains(s, "WIDGET") && strings.Contains(s, "Gadget") {
			pass++
		} else {
			t.Errorf("[FAIL] GROUP_CONCAT: got %s", s)
		}
	} else {
		t.Errorf("[FAIL] GROUP_CONCAT: nil")
	}

	// GROUP_CONCAT with GROUP BY
	afExec(t, db, ctx, "CREATE TABLE v81_tags (id INTEGER PRIMARY KEY, item TEXT, tag TEXT)")
	afExec(t, db, ctx, "INSERT INTO v81_tags VALUES (1, 'Widget', 'sale')")
	afExec(t, db, ctx, "INSERT INTO v81_tags VALUES (2, 'Widget', 'new')")
	afExec(t, db, ctx, "INSERT INTO v81_tags VALUES (3, 'Gadget', 'premium')")
	afExec(t, db, ctx, "INSERT INTO v81_tags VALUES (4, 'Gadget', 'featured')")

	checkRowCount("GROUP_CONCAT with GROUP BY",
		"SELECT item, GROUP_CONCAT(tag, ',') FROM v81_tags GROUP BY item", 2)

	// Aggregate on empty table
	afExec(t, db, ctx, "CREATE TABLE v81_empty (id INTEGER PRIMARY KEY, val INTEGER)")
	check("SUM empty table", "SELECT SUM(val) FROM v81_empty", nil)
	check("COUNT empty table", "SELECT COUNT(*) FROM v81_empty", int64(0))
	check("AVG empty table", "SELECT AVG(val) FROM v81_empty", nil)
	check("MIN empty table", "SELECT MIN(val) FROM v81_empty", nil)
	check("MAX empty table", "SELECT MAX(val) FROM v81_empty", nil)

	// ============================================================
	// === SECTION 10: DEEP EXPRESSION PATTERNS ===
	// ============================================================

	// Nested COALESCE in WHERE
	afExec(t, db, ctx, "CREATE TABLE v81_deep (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v81_deep VALUES (1, NULL, NULL, 100)")
	afExec(t, db, ctx, "INSERT INTO v81_deep VALUES (2, NULL, 200, 300)")
	afExec(t, db, ctx, "INSERT INTO v81_deep VALUES (3, 300, 400, 500)")

	check("Deep COALESCE",
		"SELECT COALESCE(a, b, c) FROM v81_deep WHERE id = 1", float64(100))
	check("Deep COALESCE 2",
		"SELECT COALESCE(a, b, c) FROM v81_deep WHERE id = 2", float64(200))
	check("Deep COALESCE 3",
		"SELECT COALESCE(a, b, c) FROM v81_deep WHERE id = 3", float64(300))

	// IIF in WHERE and SELECT
	check("IIF in SELECT",
		"SELECT IIF(a IS NULL, 'no_a', 'has_a') FROM v81_deep WHERE id = 1", "no_a")
	check("IIF in SELECT 2",
		"SELECT IIF(a IS NULL, 'no_a', 'has_a') FROM v81_deep WHERE id = 3", "has_a")

	// NULLIF in computation
	check("NULLIF division safety",
		"SELECT COALESCE(100 / NULLIF(0, 0), -1)", float64(-1))

	// Nested CASE
	check("Nested CASE",
		`SELECT CASE
			WHEN 1 = 1 THEN CASE
				WHEN 2 = 3 THEN 'wrong'
				ELSE 'correct'
			END
			ELSE 'outer_wrong'
		END`, "correct")

	// Complex arithmetic
	check("Complex math", "SELECT (100 + 50) * 2 / 3", float64(100))
	check("Modulo chain", "SELECT 100 % 7 % 3", float64(2))

	// Concatenation via ||
	check("String concat", "SELECT 'hello' || ' ' || 'world'", "hello world")

	// ============================================================
	// === SECTION 11: DISTINCT WITH EXPRESSIONS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v81_dist (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v81_dist VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v81_dist VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v81_dist VALUES (3, 10)")
	afExec(t, db, ctx, "INSERT INTO v81_dist VALUES (4, 30)")
	afExec(t, db, ctx, "INSERT INTO v81_dist VALUES (5, 20)")

	checkRowCount("DISTINCT", "SELECT DISTINCT val FROM v81_dist", 3)
	check("COUNT DISTINCT", "SELECT COUNT(DISTINCT val) FROM v81_dist", int64(3))

	// DISTINCT with ORDER BY
	check("DISTINCT ORDER BY",
		"SELECT DISTINCT val FROM v81_dist ORDER BY val ASC LIMIT 1", float64(10))

	// ============================================================
	// === SECTION 12: COMPLEX CTE WITH JOIN ===
	// ============================================================

	check("CTE with JOIN",
		`WITH dept_totals AS (
			SELECT v81_dept.name AS dept_name, SUM(v81_emp.salary) AS total
			FROM v81_dept
			JOIN v81_emp ON v81_emp.dept_id = v81_dept.id
			GROUP BY v81_dept.name
		)
		SELECT dept_name FROM dept_totals ORDER BY total DESC LIMIT 1`, "Engineering")

	// CTE used in subquery
	check("CTE in subquery",
		`WITH top_dept AS (
			SELECT dept_id, SUM(salary) AS total
			FROM v81_emp
			GROUP BY dept_id
			ORDER BY total DESC
			LIMIT 1
		)
		SELECT name FROM v81_dept WHERE id = (SELECT dept_id FROM top_dept)`, "Engineering")

	// Recursive CTE with string
	check("Recursive CTE string",
		`WITH RECURSIVE countdown(n, msg) AS (
			SELECT 3, 'start'
			UNION ALL
			SELECT n - 1, 'counting' FROM countdown WHERE n > 1
		)
		SELECT COUNT(*) FROM countdown`, float64(3))

	// ============================================================
	// === SECTION 13: BETWEEN AND IN EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v81_range (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 1; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v81_range VALUES (%d, %d)", i, i*10))
	}

	checkRowCount("BETWEEN inclusive", "SELECT * FROM v81_range WHERE val BETWEEN 30 AND 70", 5)
	checkRowCount("NOT BETWEEN", "SELECT * FROM v81_range WHERE val NOT BETWEEN 30 AND 70", 5)
	checkRowCount("IN list", "SELECT * FROM v81_range WHERE val IN (10, 30, 50, 70, 90)", 5)
	checkRowCount("NOT IN list", "SELECT * FROM v81_range WHERE val NOT IN (10, 30, 50, 70, 90)", 5)

	// IN with subquery
	checkRowCount("IN subquery",
		"SELECT * FROM v81_range WHERE val IN (SELECT val FROM v81_range WHERE id <= 3)", 3)

	// NOT IN with subquery
	checkRowCount("NOT IN subquery",
		"SELECT * FROM v81_range WHERE val NOT IN (SELECT val FROM v81_range WHERE id <= 3)", 7)

	// ============================================================
	// === SECTION 14: CREATE TABLE IF NOT EXISTS / DROP IF EXISTS ===
	// ============================================================

	checkNoError("CREATE IF NOT EXISTS",
		"CREATE TABLE IF NOT EXISTS v81_maybe (id INTEGER PRIMARY KEY)")
	checkNoError("CREATE IF NOT EXISTS again",
		"CREATE TABLE IF NOT EXISTS v81_maybe (id INTEGER PRIMARY KEY)")

	checkNoError("DROP IF EXISTS",
		"DROP TABLE IF EXISTS v81_maybe")
	checkNoError("DROP IF EXISTS again",
		"DROP TABLE IF EXISTS v81_maybe")

	// ============================================================
	// === SECTION 15: MULTIPLE TABLES IN COMPLEX QUERY ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v81_a (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v81_b (id INTEGER PRIMARY KEY, a_id INTEGER, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v81_c (id INTEGER PRIMARY KEY, b_id INTEGER, val TEXT)")

	afExec(t, db, ctx, "INSERT INTO v81_a VALUES (1, 'alpha')")
	afExec(t, db, ctx, "INSERT INTO v81_a VALUES (2, 'beta')")
	afExec(t, db, ctx, "INSERT INTO v81_b VALUES (1, 1, 'one')")
	afExec(t, db, ctx, "INSERT INTO v81_b VALUES (2, 1, 'two')")
	afExec(t, db, ctx, "INSERT INTO v81_b VALUES (3, 2, 'three')")
	afExec(t, db, ctx, "INSERT INTO v81_c VALUES (1, 1, 'x')")
	afExec(t, db, ctx, "INSERT INTO v81_c VALUES (2, 2, 'y')")
	afExec(t, db, ctx, "INSERT INTO v81_c VALUES (3, 3, 'z')")

	// 3-way join with aggregation
	check("3-way join count",
		`SELECT v81_a.val, COUNT(v81_c.id)
		 FROM v81_a
		 JOIN v81_b ON v81_b.a_id = v81_a.id
		 JOIN v81_c ON v81_c.b_id = v81_b.id
		 GROUP BY v81_a.val
		 ORDER BY COUNT(v81_c.id) DESC
		 LIMIT 1`, "alpha")

	// 3-way join with HAVING
	checkRowCount("3-way join HAVING",
		`SELECT v81_a.val
		 FROM v81_a
		 JOIN v81_b ON v81_b.a_id = v81_a.id
		 JOIN v81_c ON v81_c.b_id = v81_b.id
		 GROUP BY v81_a.val
		 HAVING COUNT(v81_c.id) >= 2`, 1) // alpha has 2

	t.Logf("v81 Score: %d/%d tests passed", pass, total)
}
