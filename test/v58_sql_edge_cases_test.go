package test

import (
	"fmt"
	"testing"
)

// TestV58SQLEdgeCases probes SQL edge cases: HAVING with OR, nested aggregate CTEs,
// COALESCE multi-arg, NULLIF, complex LEFT JOIN aggregates, LIMIT 0, empty results,
// complex boolean WHERE, multiple JOINs with aggregates, and INSERT edge cases.
func TestV58SQLEdgeCases(t *testing.T) {
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

	checkNull := func(desc string, sql string) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		if rows[0][0] != nil {
			t.Errorf("[FAIL] %s: expected NULL, got %v", desc, rows[0][0])
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
	_ = checkNull
	_ = checkNoError
	_ = checkError

	// ============================================================
	// === SETUP ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v58_products (
		id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER, stock INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v58_products VALUES (1, 'Laptop', 'Electronics', 999, 50)")
	afExec(t, db, ctx, "INSERT INTO v58_products VALUES (2, 'Phone', 'Electronics', 699, 100)")
	afExec(t, db, ctx, "INSERT INTO v58_products VALUES (3, 'Tablet', 'Electronics', 499, 75)")
	afExec(t, db, ctx, "INSERT INTO v58_products VALUES (4, 'Desk', 'Furniture', 299, 30)")
	afExec(t, db, ctx, "INSERT INTO v58_products VALUES (5, 'Chair', 'Furniture', 199, 60)")
	afExec(t, db, ctx, "INSERT INTO v58_products VALUES (6, 'Lamp', 'Furniture', 49, 200)")
	afExec(t, db, ctx, "INSERT INTO v58_products VALUES (7, 'Book', 'Media', 15, 500)")
	afExec(t, db, ctx, "INSERT INTO v58_products VALUES (8, 'DVD', 'Media', 10, 300)")

	afExec(t, db, ctx, `CREATE TABLE v58_orders (
		id INTEGER PRIMARY KEY, product_id INTEGER, customer TEXT, qty INTEGER, discount INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v58_orders VALUES (1, 1, 'Alice', 2, 10)")
	afExec(t, db, ctx, "INSERT INTO v58_orders VALUES (2, 2, 'Alice', 1, 0)")
	afExec(t, db, ctx, "INSERT INTO v58_orders VALUES (3, 1, 'Bob', 1, 5)")
	afExec(t, db, ctx, "INSERT INTO v58_orders VALUES (4, 4, 'Bob', 3, 0)")
	afExec(t, db, ctx, "INSERT INTO v58_orders VALUES (5, 7, 'Carol', 10, 2)")
	afExec(t, db, ctx, "INSERT INTO v58_orders VALUES (6, 3, 'Carol', 1, 0)")
	afExec(t, db, ctx, "INSERT INTO v58_orders VALUES (7, 6, 'Dave', 5, 0)")
	afExec(t, db, ctx, "INSERT INTO v58_orders VALUES (8, 2, 'Eve', 2, 15)")

	// ============================================================
	// === HAVING WITH COMPLEX CONDITIONS ===
	// ============================================================

	// HC1: HAVING with OR
	checkRowCount("HC1 HAVING OR",
		`SELECT customer, SUM(qty) AS total_qty FROM v58_orders
		 GROUP BY customer
		 HAVING SUM(qty) >= 10 OR SUM(discount) >= 10`, 3)
	// Alice: qty=3,disc=10; Bob: qty=4,disc=5; Carol: qty=11,disc=2; Dave: qty=5,disc=0; Eve: qty=2,disc=15
	// Alice (disc>=10), Carol (qty>=10), Eve (disc>=15>=10) → 3

	// HC2: HAVING with AND
	checkRowCount("HC2 HAVING AND",
		`SELECT customer, SUM(qty) AS total_qty FROM v58_orders
		 GROUP BY customer
		 HAVING SUM(qty) > 1 AND SUM(discount) > 0`, 4)
	// Alice: qty=3,disc=10 ✓; Bob: qty=4,disc=5 ✓; Carol: qty=11,disc=2 ✓; Eve: qty=2,disc=15 ✓ = 4
	// Actually Eve qty=2 > 1 ✓ and disc=15 > 0 ✓. So 4.

	// HC3: HAVING with COUNT
	checkRowCount("HC3 HAVING COUNT",
		`SELECT customer, COUNT(*) AS order_count FROM v58_orders
		 GROUP BY customer
		 HAVING COUNT(*) >= 2`, 3)
	// Alice: 2, Bob: 2, Carol: 2, Dave: 1, Eve: 1 → Alice, Bob, Carol = 3

	// HC4: HAVING with AVG
	checkRowCount("HC4 HAVING AVG",
		`SELECT category, AVG(price) AS avg_price FROM v58_products
		 GROUP BY category
		 HAVING AVG(price) > 100`, 2)
	// Electronics: (999+699+499)/3 = 732.33; Furniture: (299+199+49)/3 = 182.33; Media: (15+10)/2 = 12.5
	// Electronics ✓, Furniture ✓ = 2

	// ============================================================
	// === COALESCE WITH MULTIPLE ARGUMENTS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v58_nulls (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v58_nulls VALUES (1, NULL, NULL, 30)")
	afExec(t, db, ctx, "INSERT INTO v58_nulls VALUES (2, NULL, 20, 30)")
	afExec(t, db, ctx, "INSERT INTO v58_nulls VALUES (3, 10, 20, 30)")
	afExec(t, db, ctx, "INSERT INTO v58_nulls VALUES (4, NULL, NULL, NULL)")

	// CO1: COALESCE skips NULLs
	check("CO1 COALESCE skip nulls",
		"SELECT COALESCE(a, b, c) FROM v58_nulls WHERE id = 1", 30)

	// CO2: COALESCE returns first non-null
	check("CO2 COALESCE first non-null",
		"SELECT COALESCE(a, b, c) FROM v58_nulls WHERE id = 2", 20)

	// CO3: COALESCE with first value
	check("CO3 COALESCE first value",
		"SELECT COALESCE(a, b, c) FROM v58_nulls WHERE id = 3", 10)

	// CO4: COALESCE all NULL
	checkNull("CO4 COALESCE all null",
		"SELECT COALESCE(a, b, c) FROM v58_nulls WHERE id = 4")

	// CO5: COALESCE with default literal
	check("CO5 COALESCE default",
		"SELECT COALESCE(a, b, c, 99) FROM v58_nulls WHERE id = 4", 99)

	// ============================================================
	// === NULLIF ===
	// ============================================================

	// NI1: NULLIF returns NULL when equal
	checkNull("NI1 NULLIF equal",
		"SELECT NULLIF(1, 1) FROM v58_nulls WHERE id = 1")

	// NI2: NULLIF returns first when not equal
	check("NI2 NULLIF not equal",
		"SELECT NULLIF(1, 2) FROM v58_nulls WHERE id = 1", 1)

	// NI3: NULLIF with strings
	checkNull("NI3 NULLIF string equal",
		"SELECT NULLIF('abc', 'abc') FROM v58_nulls WHERE id = 1")

	// NI4: NULLIF with column
	check("NI4 NULLIF column",
		"SELECT NULLIF(c, 30) FROM v58_nulls WHERE id = 3", nil)

	// ============================================================
	// === COMPLEX LEFT JOIN + AGGREGATE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v58_depts (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v58_depts VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO v58_depts VALUES (2, 'Marketing')")
	afExec(t, db, ctx, "INSERT INTO v58_depts VALUES (3, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO v58_depts VALUES (4, 'Support')")

	afExec(t, db, ctx, `CREATE TABLE v58_employees (
		id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v58_employees VALUES (1, 'Alice', 1, 90000)")
	afExec(t, db, ctx, "INSERT INTO v58_employees VALUES (2, 'Bob', 1, 85000)")
	afExec(t, db, ctx, "INSERT INTO v58_employees VALUES (3, 'Carol', 2, 75000)")
	afExec(t, db, ctx, "INSERT INTO v58_employees VALUES (4, 'Dave', 3, 70000)")

	// LJ1: LEFT JOIN shows all depts including those with no employees
	checkRowCount("LJ1 LEFT JOIN all depts",
		`SELECT d.name, COUNT(e.id) FROM v58_depts d
		 LEFT JOIN v58_employees e ON d.id = e.dept_id
		 GROUP BY d.name`, 4)

	// LJ2: Support dept has 0 employees
	check("LJ2 LEFT JOIN zero count",
		`SELECT COUNT(e.id) FROM v58_depts d
		 LEFT JOIN v58_employees e ON d.id = e.dept_id
		 WHERE d.name = 'Support'`, 0)

	// LJ3: SUM with NULL from LEFT JOIN
	check("LJ3 LEFT JOIN SUM null",
		`SELECT COALESCE(SUM(e.salary), 0) FROM v58_depts d
		 LEFT JOIN v58_employees e ON d.id = e.dept_id
		 WHERE d.name = 'Support'`, 0)

	// LJ4: LEFT JOIN with aggregate and HAVING
	checkRowCount("LJ4 LEFT JOIN HAVING",
		`SELECT d.name, COUNT(e.id) AS emp_count FROM v58_depts d
		 LEFT JOIN v58_employees e ON d.id = e.dept_id
		 GROUP BY d.name
		 HAVING COUNT(e.id) > 0`, 3)

	// ============================================================
	// === LIMIT AND OFFSET EDGE CASES ===
	// ============================================================

	// LO1: LIMIT 0 returns no rows
	checkRowCount("LO1 LIMIT 0",
		"SELECT * FROM v58_products LIMIT 0", 0)

	// LO2: OFFSET beyond row count returns empty
	checkRowCount("LO2 OFFSET beyond",
		"SELECT * FROM v58_products LIMIT 10 OFFSET 100", 0)

	// LO3: LIMIT 1 with ORDER BY
	check("LO3 LIMIT 1 ORDER",
		"SELECT name FROM v58_products ORDER BY price DESC LIMIT 1", "Laptop")

	// LO4: OFFSET with ORDER BY
	check("LO4 OFFSET ORDER",
		"SELECT name FROM v58_products ORDER BY price DESC LIMIT 1 OFFSET 1", "Phone")

	// ============================================================
	// === EMPTY TABLE AND EMPTY RESULT OPERATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v58_empty (id INTEGER PRIMARY KEY, val INTEGER)`)

	// ET1: COUNT on empty table
	check("ET1 COUNT empty",
		"SELECT COUNT(*) FROM v58_empty", 0)

	// ET2: SUM on empty table is NULL
	checkNull("ET2 SUM empty",
		"SELECT SUM(val) FROM v58_empty")

	// ET3: MAX on empty table is NULL
	checkNull("ET3 MAX empty",
		"SELECT MAX(val) FROM v58_empty")

	// ET4: AVG on empty table is NULL
	checkNull("ET4 AVG empty",
		"SELECT AVG(val) FROM v58_empty")

	// ET5: WHERE returns no rows — aggregate still returns one row
	check("ET5 WHERE no match COUNT",
		"SELECT COUNT(*) FROM v58_products WHERE price > 99999", 0)

	// ============================================================
	// === MULTI-TABLE JOIN + AGGREGATE ===
	// ============================================================

	// MJ1: Three-table JOIN (self-join on products effectively doubles)
	check("MJ1 3-table JOIN",
		`SELECT SUM(o.qty * p.price) FROM v58_orders o
		 JOIN v58_products p ON o.product_id = p.id
		 JOIN v58_products p2 ON o.product_id = p2.id
		 WHERE o.customer = 'Alice'`, 2697)

	// MJ1b: Two-table JOIN with computed column
	check("MJ1b 2-table JOIN computed",
		`SELECT SUM(o.qty * p.price) FROM v58_orders o
		 JOIN v58_products p ON o.product_id = p.id
		 WHERE o.customer = 'Alice'`, 2697)
	// Alice: order 1 (laptop, qty=2, price=999) + order 2 (phone, qty=1, price=699) = 1998 + 699 = 2697

	// MJ2: JOIN with GROUP BY customer
	checkRowCount("MJ2 JOIN GROUP customer",
		`SELECT o.customer, SUM(o.qty * p.price) AS spend
		 FROM v58_orders o
		 JOIN v58_products p ON o.product_id = p.id
		 GROUP BY o.customer`, 5)

	// MJ3: Top spender
	check("MJ3 top spender",
		`WITH spend AS (
			SELECT o.customer, SUM(o.qty * p.price) AS total
			FROM v58_orders o
			JOIN v58_products p ON o.product_id = p.id
			GROUP BY o.customer
		)
		SELECT customer FROM spend ORDER BY total DESC LIMIT 1`, "Alice")

	// ============================================================
	// === COMPLEX WHERE WITH BOOLEAN LOGIC ===
	// ============================================================

	// BL1: AND + OR with parens
	checkRowCount("BL1 AND OR parens",
		`SELECT * FROM v58_products
		 WHERE (category = 'Electronics' AND price > 500) OR category = 'Media'`, 4)
	// Electronics > 500: Laptop(999), Phone(699) = 2; Media: Book, DVD = 2; total = 4

	// BL2: NOT with AND
	checkRowCount("BL2 NOT AND",
		`SELECT * FROM v58_products
		 WHERE NOT (category = 'Electronics') AND price > 100`, 2)
	// Non-electronics > 100: Desk(299), Chair(199) = 2

	// BL3: BETWEEN
	checkRowCount("BL3 BETWEEN",
		`SELECT * FROM v58_products WHERE price BETWEEN 100 AND 500`, 3)
	// Tablet(499), Desk(299), Chair(199) = 3

	// BL4: NOT BETWEEN
	checkRowCount("BL4 NOT BETWEEN",
		`SELECT * FROM v58_products WHERE price NOT BETWEEN 100 AND 500`, 5)
	// Laptop(999), Phone(699), Lamp(49), Book(15), DVD(10) = 5

	// BL5: IN with multiple values
	checkRowCount("BL5 IN multiple",
		`SELECT * FROM v58_products WHERE category IN ('Electronics', 'Media')`, 5)

	// BL6: NOT IN
	checkRowCount("BL6 NOT IN",
		`SELECT * FROM v58_products WHERE category NOT IN ('Electronics', 'Media')`, 3)

	// ============================================================
	// === SUBQUERY IN SELECT LIST ===
	// ============================================================

	// SS1: Scalar subquery in SELECT
	check("SS1 scalar subquery SELECT",
		`SELECT name, (SELECT MAX(price) FROM v58_products) AS max_price
		 FROM v58_products WHERE id = 1`, "Laptop")

	// SS2: Correlated scalar subquery
	check("SS2 correlated scalar",
		`SELECT name, (SELECT COUNT(*) FROM v58_orders WHERE product_id = v58_products.id) AS order_count
		 FROM v58_products WHERE id = 1`, "Laptop")

	// ============================================================
	// === UPDATE WITH COMPLEX CONDITIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v58_prices (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v58_prices VALUES (1, 'A', 100)")
	afExec(t, db, ctx, "INSERT INTO v58_prices VALUES (2, 'B', 200)")
	afExec(t, db, ctx, "INSERT INTO v58_prices VALUES (3, 'C', 300)")
	afExec(t, db, ctx, "INSERT INTO v58_prices VALUES (4, 'D', 400)")

	// UP1: UPDATE with arithmetic
	checkNoError("UP1 UPDATE arithmetic",
		"UPDATE v58_prices SET price = price * 2 WHERE price > 200")
	check("UP1 verify C", "SELECT price FROM v58_prices WHERE name = 'C'", 600)
	check("UP1 verify D", "SELECT price FROM v58_prices WHERE name = 'D'", 800)
	check("UP1 verify A unchanged", "SELECT price FROM v58_prices WHERE name = 'A'", 100)

	// UP2: UPDATE with CASE
	checkNoError("UP2 UPDATE CASE",
		`UPDATE v58_prices SET price = CASE
			WHEN price >= 600 THEN price - 100
			ELSE price + 50
		 END`)
	check("UP2 verify A", "SELECT price FROM v58_prices WHERE name = 'A'", 150)
	check("UP2 verify B", "SELECT price FROM v58_prices WHERE name = 'B'", 250)
	check("UP2 verify C", "SELECT price FROM v58_prices WHERE name = 'C'", 500)
	check("UP2 verify D", "SELECT price FROM v58_prices WHERE name = 'D'", 700)

	// ============================================================
	// === DELETE WITH COMPLEX CONDITIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v58_del (id INTEGER PRIMARY KEY, val INTEGER, cat TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v58_del VALUES (1, 10, 'a')")
	afExec(t, db, ctx, "INSERT INTO v58_del VALUES (2, 20, 'b')")
	afExec(t, db, ctx, "INSERT INTO v58_del VALUES (3, 30, 'a')")
	afExec(t, db, ctx, "INSERT INTO v58_del VALUES (4, 40, 'b')")
	afExec(t, db, ctx, "INSERT INTO v58_del VALUES (5, 50, 'a')")

	// DL1: DELETE with AND
	checkNoError("DL1 DELETE AND",
		"DELETE FROM v58_del WHERE cat = 'a' AND val > 20")
	checkRowCount("DL1 verify", "SELECT * FROM v58_del", 3)
	// Deleted: id=3(30,a), id=5(50,a). Remaining: 1(10,a), 2(20,b), 4(40,b)

	// DL2: DELETE with OR
	checkNoError("DL2 DELETE OR",
		"DELETE FROM v58_del WHERE val < 15 OR val > 35")
	checkRowCount("DL2 verify", "SELECT * FROM v58_del", 1)
	// Deleted: id=1(10<15), id=4(40>35). Remaining: 2(20,b)

	// ============================================================
	// === WINDOW FUNCTIONS: NTILE ===
	// ============================================================

	// (NTILE may not be supported — test and skip gracefully)

	// ============================================================
	// === AGGREGATE + DISTINCT ===
	// ============================================================

	// AD1: SUM DISTINCT
	afExec(t, db, ctx, `CREATE TABLE v58_dup_vals (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v58_dup_vals VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v58_dup_vals VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v58_dup_vals VALUES (3, 10)")
	afExec(t, db, ctx, "INSERT INTO v58_dup_vals VALUES (4, 30)")
	afExec(t, db, ctx, "INSERT INTO v58_dup_vals VALUES (5, 20)")

	// AD1: COUNT DISTINCT
	check("AD1 COUNT DISTINCT",
		"SELECT COUNT(DISTINCT val) FROM v58_dup_vals", 3)

	// ============================================================
	// === SELF JOIN ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v58_hierarchy (id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v58_hierarchy VALUES (1, 'CEO', NULL)")
	afExec(t, db, ctx, "INSERT INTO v58_hierarchy VALUES (2, 'VP1', 1)")
	afExec(t, db, ctx, "INSERT INTO v58_hierarchy VALUES (3, 'VP2', 1)")
	afExec(t, db, ctx, "INSERT INTO v58_hierarchy VALUES (4, 'Mgr1', 2)")
	afExec(t, db, ctx, "INSERT INTO v58_hierarchy VALUES (5, 'Mgr2', 3)")

	// SJ1: Self-join to get parent name
	check("SJ1 self join parent",
		`SELECT p.name FROM v58_hierarchy c
		 JOIN v58_hierarchy p ON c.parent_id = p.id
		 WHERE c.name = 'Mgr1'`, "VP1")

	// SJ2: Count direct reports
	check("SJ2 count reports",
		`SELECT COUNT(*) FROM v58_hierarchy c
		 JOIN v58_hierarchy p ON c.parent_id = p.id
		 WHERE p.name = 'CEO'`, 2)

	// ============================================================
	// === MIXED OPERATIONS ===
	// ============================================================

	// MO1: CTE + subquery + aggregate
	check("MO1 CTE subquery agg",
		`WITH category_totals AS (
			SELECT category, SUM(price * stock) AS inventory_value
			FROM v58_products GROUP BY category
		)
		SELECT category FROM category_totals
		WHERE inventory_value = (SELECT MAX(inventory_value) FROM category_totals)`, "Electronics")
	// Electronics: 999*50+699*100+499*75=49950+69900+37425=157275
	// Furniture: 299*30+199*60+49*200=8970+11940+9800=30710
	// Media: 15*500+10*300=7500+3000=10500

	// MO2: Nested CASE with aggregate
	check("MO2 nested CASE",
		`SELECT CASE
			WHEN COUNT(*) > 5 THEN 'many'
			WHEN COUNT(*) > 2 THEN 'some'
			ELSE 'few'
		 END FROM v58_products`, "many")

	// MO3: Multiple aggregates in one query
	check("MO3 multi agg",
		`SELECT COUNT(*) FROM v58_products
		 WHERE price > (SELECT AVG(price) FROM v58_products)`, 3)
	// AVG = (999+699+499+299+199+49+15+10)/8 = 2769/8 = 346.125
	// > 346.125: Laptop(999), Phone(699), Tablet(499) = 3

	t.Logf("\n=== V58 SQL EDGE CASES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
