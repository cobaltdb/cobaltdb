package test

import (
	"fmt"
	"testing"
)

// TestV78PreparedEdge tests prepared statements with placeholders, complex DML patterns,
// advanced constraint combinations, and expression evaluation edge cases.
func TestV78PreparedEdge(t *testing.T) {
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
		if err != nil {
			pass++
		} else {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
		}
	}

	checkNull := func(desc string, sql string) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			pass++
			return
		}
		if rows[0][0] == nil {
			pass++
			return
		}
		t.Errorf("[FAIL] %s: expected NULL, got %v", desc, rows[0][0])
	}

	// Helper for parameterized queries
	checkParam := func(desc string, sql string, args []interface{}, expected interface{}) {
		t.Helper()
		total++
		rows, err := db.Query(ctx, sql, args...)
		if err != nil {
			t.Errorf("[FAIL] %s: query error: %v", desc, err)
			return
		}
		defer rows.Close()
		cols := rows.Columns()
		var result [][]interface{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			rows.Scan(ptrs...)
			row := make([]interface{}, len(cols))
			copy(row, vals)
			result = append(result, row)
		}
		if len(result) == 0 || len(result[0]) == 0 {
			if expected == nil {
				pass++
				return
			}
			t.Errorf("[FAIL] %s: no rows returned, expected %v", desc, expected)
			return
		}
		got := fmt.Sprintf("%v", result[0][0])
		exp := fmt.Sprintf("%v", expected)
		if got != exp {
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, exp)
			return
		}
		pass++
	}

	execParam := func(desc string, sql string, args []interface{}) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql, args...)
		if err != nil {
			t.Errorf("[FAIL] %s: %v", desc, err)
			return
		}
		pass++
	}

	// ============================================================
	// === PREPARED STATEMENTS: SELECT WITH PLACEHOLDERS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_prep (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_prep VALUES (1, 'Alice', 90)")
	afExec(t, db, ctx, "INSERT INTO v78_prep VALUES (2, 'Bob', 80)")
	afExec(t, db, ctx, "INSERT INTO v78_prep VALUES (3, 'Carol', 70)")
	afExec(t, db, ctx, "INSERT INTO v78_prep VALUES (4, 'Dave', 60)")
	afExec(t, db, ctx, "INSERT INTO v78_prep VALUES (5, 'Eve', 50)")

	// Basic parameterized SELECT
	checkParam("PREP: basic where",
		"SELECT name FROM v78_prep WHERE id = ?", []interface{}{1}, "Alice")

	checkParam("PREP: string param",
		"SELECT score FROM v78_prep WHERE name = ?", []interface{}{"Carol"}, 70)

	// Multiple params
	checkParam("PREP: multi param",
		"SELECT COUNT(*) FROM v78_prep WHERE score >= ? AND score <= ?",
		[]interface{}{60, 80}, 3)

	// Param in BETWEEN
	checkParam("PREP: BETWEEN params",
		"SELECT COUNT(*) FROM v78_prep WHERE score BETWEEN ? AND ?",
		[]interface{}{70, 90}, 3)

	// Param in LIKE
	checkParam("PREP: LIKE param",
		"SELECT COUNT(*) FROM v78_prep WHERE name LIKE ?",
		[]interface{}{"%a%"}, 3) // Alice, Carol, Dave (case insensitive)

	// ============================================================
	// === PREPARED STATEMENTS: INSERT WITH PLACEHOLDERS ===
	// ============================================================

	execParam("PREP: insert param",
		"INSERT INTO v78_prep VALUES (?, ?, ?)", []interface{}{6, "Frank", 95})
	check("PREP: verify insert", "SELECT name FROM v78_prep WHERE id = 6", "Frank")
	check("PREP: verify score", "SELECT score FROM v78_prep WHERE id = 6", 95)

	// ============================================================
	// === PREPARED STATEMENTS: UPDATE WITH PLACEHOLDERS ===
	// ============================================================

	execParam("PREP: update param",
		"UPDATE v78_prep SET score = ? WHERE name = ?", []interface{}{99, "Alice"})
	check("PREP: verify update", "SELECT score FROM v78_prep WHERE id = 1", 99)

	// ============================================================
	// === PREPARED STATEMENTS: DELETE WITH PLACEHOLDERS ===
	// ============================================================

	execParam("PREP: delete param",
		"DELETE FROM v78_prep WHERE score < ?", []interface{}{60})
	check("PREP: verify delete", "SELECT COUNT(*) FROM v78_prep", 5) // Eve removed

	// ============================================================
	// === COMPLEX INSERT INTO SELECT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_summary (category TEXT, total_score INTEGER, avg_score REAL)`)
	afExec(t, db, ctx, `CREATE TABLE v78_categories (id INTEGER PRIMARY KEY, name TEXT, min_score INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_categories VALUES (1, 'high', 80)")
	afExec(t, db, ctx, "INSERT INTO v78_categories VALUES (2, 'medium', 60)")
	afExec(t, db, ctx, "INSERT INTO v78_categories VALUES (3, 'low', 0)")

	// INSERT INTO SELECT with GROUP BY
	checkNoError("INS-SEL: with agg",
		`INSERT INTO v78_summary
		 SELECT CASE
			WHEN score >= 80 THEN 'high'
			WHEN score >= 60 THEN 'medium'
			ELSE 'low'
		 END, SUM(score), AVG(score)
		 FROM v78_prep
		 GROUP BY CASE
			WHEN score >= 80 THEN 'high'
			WHEN score >= 60 THEN 'medium'
			ELSE 'low'
		 END`)

	// After deleting score<60, remaining: Alice(99), Bob(80), Carol(70), Dave(60), Frank(95)
	// high(>=80): Alice,Bob,Frank=3; medium(>=60): Carol,Dave=2; low: none
	checkRowCount("INS-SEL: verify rows", "SELECT * FROM v78_summary", 2)

	// ============================================================
	// === UPDATE WITH COMPLEX SET EXPRESSIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_complex_upd (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v78_complex_upd VALUES (1, 10, 20, 'old')")
	afExec(t, db, ctx, "INSERT INTO v78_complex_upd VALUES (2, 30, 40, 'old')")
	afExec(t, db, ctx, "INSERT INTO v78_complex_upd VALUES (3, 50, 60, 'old')")

	// UPDATE with arithmetic expression
	checkNoError("UPD-EXPR: arithmetic",
		"UPDATE v78_complex_upd SET a = a + b WHERE id = 1")
	check("UPD-EXPR: verify arithmetic", "SELECT a FROM v78_complex_upd WHERE id = 1", 30)

	// UPDATE with CASE in SET
	checkNoError("UPD-EXPR: CASE in SET",
		`UPDATE v78_complex_upd SET c = CASE
			WHEN a + b > 100 THEN 'high'
			WHEN a + b > 50 THEN 'medium'
			ELSE 'low'
		 END`)
	check("UPD-EXPR: verify case 1", "SELECT c FROM v78_complex_upd WHERE id = 1", "low")
	check("UPD-EXPR: verify case 2", "SELECT c FROM v78_complex_upd WHERE id = 2", "medium")
	check("UPD-EXPR: verify case 3", "SELECT c FROM v78_complex_upd WHERE id = 3", "high")

	// UPDATE with subquery (SET val = (SELECT ...))
	checkNoError("UPD-EXPR: subquery SET",
		"UPDATE v78_complex_upd SET a = (SELECT MAX(b) FROM v78_complex_upd) WHERE id = 1")
	check("UPD-EXPR: verify subquery", "SELECT a FROM v78_complex_upd WHERE id = 1", 60)

	// ============================================================
	// === DELETE WITH COMPLEX WHERE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_del (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_del VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v78_del VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO v78_del VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO v78_del VALUES (4, 'B', 40)")
	afExec(t, db, ctx, "INSERT INTO v78_del VALUES (5, 'C', 50)")

	// DELETE with IN subquery
	checkNoError("DEL-COMPLEX: IN subquery",
		`DELETE FROM v78_del WHERE cat IN (
			SELECT cat FROM v78_del GROUP BY cat HAVING COUNT(*) > 1
		)`)
	check("DEL-COMPLEX: after IN subq", "SELECT COUNT(*) FROM v78_del", 1) // only C left

	// ============================================================
	// === CONSTRAINT COMBINATIONS ===
	// ============================================================

	// NOT NULL + DEFAULT
	afExec(t, db, ctx, `CREATE TABLE v78_const (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'active',
		score INTEGER DEFAULT 0,
		email TEXT UNIQUE
	)`)

	checkNoError("CONST: minimal insert",
		"INSERT INTO v78_const (id, name, email) VALUES (1, 'Alice', 'alice@test.com')")
	check("CONST: default status", "SELECT status FROM v78_const WHERE id = 1", "active")
	check("CONST: default score", "SELECT score FROM v78_const WHERE id = 1", 0)

	// NOT NULL violation
	checkError("CONST: not null violation",
		"INSERT INTO v78_const (id, email) VALUES (2, 'bob@test.com')") // name is NOT NULL

	// UNIQUE violation
	checkError("CONST: unique violation",
		"INSERT INTO v78_const (id, name, email) VALUES (2, 'Bob', 'alice@test.com')")

	// ============================================================
	// === COMPLEX COALESCE PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_coal (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_coal VALUES (1, NULL, NULL, 30)")
	afExec(t, db, ctx, "INSERT INTO v78_coal VALUES (2, NULL, 20, 30)")
	afExec(t, db, ctx, "INSERT INTO v78_coal VALUES (3, 10, 20, 30)")

	check("COAL: all null except last",
		"SELECT COALESCE(a, b, c) FROM v78_coal WHERE id = 1", 30)
	check("COAL: second non-null",
		"SELECT COALESCE(a, b, c) FROM v78_coal WHERE id = 2", 20)
	check("COAL: first non-null",
		"SELECT COALESCE(a, b, c) FROM v78_coal WHERE id = 3", 10)

	// COALESCE in WHERE
	checkRowCount("COAL: in WHERE",
		"SELECT * FROM v78_coal WHERE COALESCE(a, b, c) >= 20", 2) // id=2(20), id=3(10) → wait, 10 < 20

	// Actually: id=1 → COALESCE=30, id=2 → 20, id=3 → 10. So >= 20: id=1 and id=2
	checkRowCount("COAL: in WHERE correct",
		"SELECT * FROM v78_coal WHERE COALESCE(a, b, c) >= 25", 1) // only id=1(30)

	// COALESCE in ORDER BY
	check("COAL: in ORDER BY",
		"SELECT id FROM v78_coal ORDER BY COALESCE(a, 0) + COALESCE(b, 0) + COALESCE(c, 0) DESC LIMIT 1", 3) // 10+20+30=60

	// ============================================================
	// === NULLIF PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_nf (id INTEGER PRIMARY KEY, status TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_nf VALUES (1, 'active', 100)")
	afExec(t, db, ctx, "INSERT INTO v78_nf VALUES (2, 'inactive', 0)")
	afExec(t, db, ctx, "INSERT INTO v78_nf VALUES (3, 'active', 50)")

	// NULLIF to avoid division by zero
	check("NULLIF: non-zero",
		"SELECT 100 / NULLIF(val, 0) FROM v78_nf WHERE id = 1", 1) // 100/100
	checkNull("NULLIF: zero becomes null",
		"SELECT 100 / NULLIF(val, 0) FROM v78_nf WHERE id = 2") // 100/NULL = NULL

	// ============================================================
	// === COMPLEX CTE WITH UNION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v78_items VALUES (1, 'Widget', 10, 'A')")
	afExec(t, db, ctx, "INSERT INTO v78_items VALUES (2, 'Gadget', 25, 'A')")
	afExec(t, db, ctx, "INSERT INTO v78_items VALUES (3, 'Doohickey', 50, 'B')")
	afExec(t, db, ctx, "INSERT INTO v78_items VALUES (4, 'Thingamajig', 100, 'B')")
	afExec(t, db, ctx, "INSERT INTO v78_items VALUES (5, 'Whatchamacallit', 5, 'C')")

	check("CTE-UNION: category stats",
		`WITH cheap AS (
			SELECT category, COUNT(*) as cnt FROM v78_items WHERE price < 30 GROUP BY category
		),
		expensive AS (
			SELECT category, COUNT(*) as cnt FROM v78_items WHERE price >= 30 GROUP BY category
		)
		SELECT SUM(cnt) FROM (
			SELECT cnt FROM cheap
			UNION ALL
			SELECT cnt FROM expensive
		) combined`, 5) // all 5 items

	// CTE with INTERSECT
	check("CTE-INTERSECT: common categories",
		`WITH cheap_cats AS (
			SELECT DISTINCT category FROM v78_items WHERE price < 30
		),
		expensive_cats AS (
			SELECT DISTINCT category FROM v78_items WHERE price >= 30
		)
		SELECT COUNT(*) FROM (
			SELECT category FROM cheap_cats
			INTERSECT
			SELECT category FROM expensive_cats
		) common`, 0) // A has only cheap, B has only expensive, C has only cheap

	// Actually: A has Widget(10,cheap) and Gadget(25,cheap). B has Doohickey(50,expensive) and Thingamajig(100,expensive). C has Whatchamacallit(5,cheap).
	// No category has both cheap and expensive items → 0

	// ============================================================
	// === VIEW WITH COMPLEX QUERY ===
	// ============================================================

	checkNoError("VIEW: create complex",
		`CREATE VIEW v78_item_stats AS
		 SELECT category, COUNT(*) as item_count, SUM(price) as total_price, AVG(price) as avg_price
		 FROM v78_items GROUP BY category`)

	checkRowCount("VIEW: query", "SELECT * FROM v78_item_stats", 3)
	check("VIEW: category A count",
		"SELECT item_count FROM v78_item_stats WHERE category = 'A'", 2)
	check("VIEW: category B total",
		"SELECT total_price FROM v78_item_stats WHERE category = 'B'", 150)

	// View used in JOIN
	check("VIEW: in JOIN",
		`SELECT i.name FROM v78_items i
		 JOIN v78_item_stats s ON i.category = s.category
		 WHERE s.avg_price > 40 AND i.price = (SELECT MAX(price) FROM v78_items WHERE category = i.category)`, "Thingamajig")

	checkNoError("VIEW: drop", "DROP VIEW v78_item_stats")

	// ============================================================
	// === TRIGGER ON UPDATE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_audit (id INTEGER PRIMARY KEY, table_name TEXT, action TEXT, old_val INTEGER, new_val INTEGER)`)
	afExec(t, db, ctx, `CREATE TABLE v78_trig_data (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_trig_data VALUES (1, 100)")

	afExec(t, db, ctx, `CREATE TRIGGER v78_upd_trig AFTER UPDATE ON v78_trig_data
		BEGIN INSERT INTO v78_audit VALUES (NEW.id, 'v78_trig_data', 'update', OLD.val, NEW.val); END`)

	checkNoError("TRIG-UPD: trigger update",
		"UPDATE v78_trig_data SET val = 200 WHERE id = 1")
	// Verify trigger fired
	check("TRIG-UPD: audit count", "SELECT COUNT(*) FROM v78_audit", 1)
	check("TRIG-UPD: audit old", "SELECT old_val FROM v78_audit LIMIT 1", 100)
	check("TRIG-UPD: audit new", "SELECT new_val FROM v78_audit LIMIT 1", 200)

	checkNoError("TRIG-UPD: drop trigger", "DROP TRIGGER v78_upd_trig")

	// ============================================================
	// === TRIGGER ON DELETE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_del_audit (id INTEGER PRIMARY KEY, deleted_val INTEGER)`)
	afExec(t, db, ctx, `CREATE TRIGGER v78_del_trig AFTER DELETE ON v78_trig_data
		BEGIN INSERT INTO v78_del_audit VALUES (OLD.id, OLD.val); END`)

	checkNoError("TRIG-DEL: delete", "DELETE FROM v78_trig_data WHERE id = 1")
	check("TRIG-DEL: audit count", "SELECT COUNT(*) FROM v78_del_audit", 1)
	check("TRIG-DEL: audit val", "SELECT deleted_val FROM v78_del_audit LIMIT 1", 200)

	checkNoError("TRIG-DEL: drop trigger", "DROP TRIGGER v78_del_trig")

	// ============================================================
	// === COMPLEX EXPRESSION EVALUATION ===
	// ============================================================

	// Nested CASE
	check("EXPR: nested CASE",
		`SELECT CASE
			WHEN 1 = 1 THEN
				CASE WHEN 2 > 3 THEN 'a' ELSE 'b' END
			ELSE 'c'
		END`, "b")

	// Nested functions
	check("EXPR: nested functions",
		"SELECT LENGTH(UPPER(REPLACE('hello world', 'world', 'earth')))", 11)

	// Complex arithmetic
	check("EXPR: complex arithmetic",
		"SELECT (10 + 20) * 3 - 5", 85)

	// String concatenation
	check("EXPR: concat operator",
		"SELECT 'hello' || ' ' || 'world'", "hello world")

	// Boolean expressions
	check("EXPR: boolean AND",
		"SELECT (1 = 1) AND (2 = 2)", true)
	check("EXPR: boolean OR",
		"SELECT (1 = 0) OR (2 = 2)", true)
	check("EXPR: boolean NOT",
		"SELECT NOT (1 = 0)", true)

	// ============================================================
	// === SUBQUERY IN VARIOUS POSITIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_sub (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_sub VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v78_sub VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v78_sub VALUES (3, 30)")

	// Subquery in SELECT list
	check("SUB-SEL: scalar in select",
		"SELECT id, (SELECT MAX(val) FROM v78_sub) as max_val FROM v78_sub WHERE id = 1", 1)

	// Subquery in WHERE with >
	checkRowCount("SUB-WHERE: greater than avg",
		"SELECT * FROM v78_sub WHERE val > (SELECT AVG(val) FROM v78_sub)", 1) // 30 > 20

	// Subquery in WHERE with IN
	checkRowCount("SUB-WHERE: IN subquery",
		"SELECT * FROM v78_sub WHERE val IN (SELECT val FROM v78_sub WHERE val > 15)", 2) // 20, 30

	// Subquery in INSERT VALUES (newly fixed!)
	afExec(t, db, ctx, `CREATE TABLE v78_sub_ins (id INTEGER PRIMARY KEY, computed INTEGER)`)
	checkNoError("SUB-INS: with subquery",
		"INSERT INTO v78_sub_ins VALUES (1, (SELECT SUM(val) FROM v78_sub))")
	check("SUB-INS: verify", "SELECT computed FROM v78_sub_ins WHERE id = 1", 60)

	// ============================================================
	// === EDGE CASES: EMPTY STRING vs NULL ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_empty_str (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v78_empty_str VALUES (1, '')")
	afExec(t, db, ctx, "INSERT INTO v78_empty_str VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v78_empty_str VALUES (3, 'hello')")

	check("EMPTY-STR: length empty", "SELECT LENGTH(val) FROM v78_empty_str WHERE id = 1", 0)
	checkNull("EMPTY-STR: length null", "SELECT LENGTH(val) FROM v78_empty_str WHERE id = 2")
	check("EMPTY-STR: count all", "SELECT COUNT(*) FROM v78_empty_str", 3)
	check("EMPTY-STR: count val", "SELECT COUNT(val) FROM v78_empty_str", 2) // NULL excluded

	// Empty string is NOT NULL
	checkRowCount("EMPTY-STR: is not null",
		"SELECT * FROM v78_empty_str WHERE val IS NOT NULL", 2) // empty + hello

	// Empty string IS distinct from NULL
	checkRowCount("EMPTY-STR: where empty",
		"SELECT * FROM v78_empty_str WHERE val = ''", 1)

	// ============================================================
	// === EDGE CASES: TYPE COERCION IN COMPARISONS ===
	// ============================================================

	check("COERCE: int = text", "SELECT 42 = '42'", true)
	check("COERCE: float = int", "SELECT 42.0 = 42", true)

	// ============================================================
	// === COMPLEX ORDER BY WITH EXPRESSIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_ord (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_ord VALUES (1, 10, 30)")
	afExec(t, db, ctx, "INSERT INTO v78_ord VALUES (2, 20, 20)")
	afExec(t, db, ctx, "INSERT INTO v78_ord VALUES (3, 30, 10)")

	// ORDER BY computed expression
	check("ORD-EXPR: sum desc",
		"SELECT id FROM v78_ord ORDER BY a + b DESC LIMIT 1", 1) // all have sum=40, stable sort → id=1

	// ORDER BY with ABS
	afExec(t, db, ctx, `CREATE TABLE v78_abs_ord (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_abs_ord VALUES (1, -5)")
	afExec(t, db, ctx, "INSERT INTO v78_abs_ord VALUES (2, 3)")
	afExec(t, db, ctx, "INSERT INTO v78_abs_ord VALUES (3, -1)")
	afExec(t, db, ctx, "INSERT INTO v78_abs_ord VALUES (4, 10)")

	check("ORD-EXPR: ABS asc",
		"SELECT id FROM v78_abs_ord ORDER BY ABS(val) ASC LIMIT 1", 3) // ABS(-1)=1

	// ============================================================
	// === MULTIPLE JOINS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_a (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v78_a VALUES (1, 'x')")
	afExec(t, db, ctx, "INSERT INTO v78_a VALUES (2, 'y')")

	afExec(t, db, ctx, `CREATE TABLE v78_b (id INTEGER PRIMARY KEY, a_id INTEGER, bval TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v78_b VALUES (1, 1, 'b1')")
	afExec(t, db, ctx, "INSERT INTO v78_b VALUES (2, 1, 'b2')")
	afExec(t, db, ctx, "INSERT INTO v78_b VALUES (3, 2, 'b3')")

	afExec(t, db, ctx, `CREATE TABLE v78_c (id INTEGER PRIMARY KEY, b_id INTEGER, cval TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v78_c VALUES (1, 1, 'c1')")
	afExec(t, db, ctx, "INSERT INTO v78_c VALUES (2, 2, 'c2')")

	// 3-table JOIN
	checkRowCount("3JOIN: all",
		"SELECT * FROM v78_a a JOIN v78_b b ON a.id = b.a_id JOIN v78_c c ON b.id = c.b_id", 2)

	check("3JOIN: filtered",
		"SELECT c.cval FROM v78_a a JOIN v78_b b ON a.id = b.a_id JOIN v78_c c ON b.id = c.b_id WHERE a.val = 'x' AND b.bval = 'b2'", "c2")

	// ============================================================
	// === AGGREGATE WITH COMPLEX EXPRESSIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v78_agg (id INTEGER PRIMARY KEY, qty INTEGER, price INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_agg VALUES (1, 5, 10)")
	afExec(t, db, ctx, "INSERT INTO v78_agg VALUES (2, 3, 20)")
	afExec(t, db, ctx, "INSERT INTO v78_agg VALUES (3, 8, 15)")

	check("AGG-EXPR: SUM of product",
		"SELECT SUM(qty * price) FROM v78_agg", 230) // 50+60+120

	check("AGG-EXPR: AVG of product",
		"SELECT ROUND(AVG(qty * price), 1) FROM v78_agg", 76.7) // 230/3

	// ============================================================
	// === RECURSIVE CTE EDGE CASES ===
	// ============================================================

	// Fibonacci sequence
	check("REC-CTE: fibonacci",
		`WITH RECURSIVE fib(n, a, b) AS (
			SELECT 1, 0, 1
			UNION ALL
			SELECT n+1, b, a+b FROM fib WHERE n < 10
		) SELECT b FROM fib WHERE n = 10`, 55) // 10th Fibonacci

	// Hierarchical query
	afExec(t, db, ctx, `CREATE TABLE v78_org (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT, depth INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v78_org VALUES (1, NULL, 'CEO', 0)")
	afExec(t, db, ctx, "INSERT INTO v78_org VALUES (2, 1, 'VP1', 1)")
	afExec(t, db, ctx, "INSERT INTO v78_org VALUES (3, 1, 'VP2', 1)")
	afExec(t, db, ctx, "INSERT INTO v78_org VALUES (4, 2, 'Dir1', 2)")
	afExec(t, db, ctx, "INSERT INTO v78_org VALUES (5, 2, 'Dir2', 2)")

	check("REC-CTE: hierarchy count",
		`WITH RECURSIVE subordinates(id) AS (
			SELECT id FROM v78_org WHERE name = 'VP1'
			UNION ALL
			SELECT o.id FROM v78_org o JOIN subordinates s ON o.parent_id = s.id
		) SELECT COUNT(*) FROM subordinates`, 3) // VP1 + Dir1 + Dir2

	// ============================================================
	// === TOTAL COUNT ===
	// ============================================================

	t.Logf("\n=== V78 PREPARED EDGE: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
	_ = checkNull  // keep used
	_ = checkError // keep used
}
