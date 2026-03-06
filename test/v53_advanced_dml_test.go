package test

import (
	"fmt"
	"testing"
)

// TestV53AdvancedDML exercises UPDATE/DELETE with subqueries, INSERT...SELECT,
// complex WHERE with multiple operators, CASE in DML, and multi-table interactions.
func TestV53AdvancedDML(t *testing.T) {
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

	// ============================================================
	// === SETUP TABLES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v53_products (
		id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER, stock INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v53_products VALUES (1, 'Laptop', 'Electronics', 1200, 50)")
	afExec(t, db, ctx, "INSERT INTO v53_products VALUES (2, 'Phone', 'Electronics', 800, 100)")
	afExec(t, db, ctx, "INSERT INTO v53_products VALUES (3, 'Shirt', 'Clothing', 30, 200)")
	afExec(t, db, ctx, "INSERT INTO v53_products VALUES (4, 'Pants', 'Clothing', 50, 150)")
	afExec(t, db, ctx, "INSERT INTO v53_products VALUES (5, 'Book', 'Education', 15, 500)")
	afExec(t, db, ctx, "INSERT INTO v53_products VALUES (6, 'Pen', 'Education', 2, 1000)")

	afExec(t, db, ctx, `CREATE TABLE v53_sales (
		id INTEGER PRIMARY KEY, product_id INTEGER, qty INTEGER, sale_date TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v53_sales VALUES (1, 1, 5, '2024-01-15')")
	afExec(t, db, ctx, "INSERT INTO v53_sales VALUES (2, 2, 10, '2024-01-16')")
	afExec(t, db, ctx, "INSERT INTO v53_sales VALUES (3, 3, 20, '2024-01-17')")
	afExec(t, db, ctx, "INSERT INTO v53_sales VALUES (4, 1, 3, '2024-02-01')")
	afExec(t, db, ctx, "INSERT INTO v53_sales VALUES (5, 5, 50, '2024-02-10')")
	afExec(t, db, ctx, "INSERT INTO v53_sales VALUES (6, 2, 15, '2024-02-15')")

	// ============================================================
	// === UPDATE WITH SUBQUERY IN WHERE ===
	// ============================================================

	// US1: UPDATE products that have been sold
	checkNoError("US1 UPDATE with IN subquery",
		`UPDATE v53_products SET stock = stock - 1
		 WHERE id IN (SELECT DISTINCT product_id FROM v53_sales)`)

	check("US1 verify Laptop stock",
		"SELECT stock FROM v53_products WHERE id = 1", 49)
	check("US1 verify Pen unchanged",
		"SELECT stock FROM v53_products WHERE id = 6", 1000)

	// US2: UPDATE with EXISTS subquery
	checkNoError("US2 UPDATE with EXISTS",
		`UPDATE v53_products SET price = price + 10
		 WHERE EXISTS (SELECT 1 FROM v53_sales WHERE v53_sales.product_id = v53_products.id AND qty > 10)`)

	check("US2 Phone got price increase",
		"SELECT price FROM v53_products WHERE id = 2", 810)
	check("US2 Shirt got price increase",
		"SELECT price FROM v53_products WHERE id = 3", 40)
	check("US2 Book got price increase",
		"SELECT price FROM v53_products WHERE id = 5", 25)

	// ============================================================
	// === DELETE WITH SUBQUERY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v53_archive (
		id INTEGER PRIMARY KEY, product_id INTEGER, qty INTEGER, sale_date TEXT)`)

	// DS1: INSERT INTO SELECT to archive old sales
	checkNoError("DS1 INSERT SELECT archive",
		`INSERT INTO v53_archive SELECT * FROM v53_sales WHERE sale_date < '2024-02-01'`)

	checkRowCount("DS1 verify archive", "SELECT * FROM v53_archive", 3)

	// DS2: DELETE with IN subquery
	checkNoError("DS2 DELETE with IN",
		`DELETE FROM v53_sales WHERE id IN (SELECT id FROM v53_archive)`)

	checkRowCount("DS2 verify sales", "SELECT * FROM v53_sales", 3)

	// ============================================================
	// === COMPLEX WHERE WITH MULTIPLE OPERATORS ===
	// ============================================================

	// CW1: AND + OR + parentheses
	// After CU1: Laptop=2400, Phone=1620, Shirt=45, Pants=55, Book=25, Pen=2
	// Electronics price>500: Laptop(2400), Phone(1620) = 2
	// Education stock>100: Book(499), Pen(999) = 2
	// Total = 4
	checkRowCount("CW1 complex WHERE",
		`SELECT * FROM v53_products
		 WHERE (category = 'Electronics' AND price > 500)
		    OR (category = 'Education' AND stock > 100)`, 4)

	// CW2: BETWEEN + IN
	checkRowCount("CW2 BETWEEN + IN",
		`SELECT * FROM v53_products
		 WHERE price BETWEEN 10 AND 100
		   AND category IN ('Clothing', 'Education')`, 3)

	// CW3: NOT IN
	checkRowCount("CW3 NOT IN",
		`SELECT * FROM v53_products
		 WHERE category NOT IN ('Electronics')`, 4)

	// CW4: IS NULL / IS NOT NULL
	afExec(t, db, ctx, `CREATE TABLE v53_nulltest (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v53_nulltest VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO v53_nulltest VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v53_nulltest VALUES (3, 'world')")
	afExec(t, db, ctx, "INSERT INTO v53_nulltest VALUES (4, NULL)")

	checkRowCount("CW4 IS NULL", "SELECT * FROM v53_nulltest WHERE val IS NULL", 2)
	checkRowCount("CW4 IS NOT NULL", "SELECT * FROM v53_nulltest WHERE val IS NOT NULL", 2)

	// ============================================================
	// === CASE IN UPDATE ===
	// ============================================================

	// CU1: CASE expression in UPDATE SET
	checkNoError("CU1 CASE in UPDATE",
		`UPDATE v53_products SET price = CASE
			WHEN category = 'Electronics' THEN price * 2
			WHEN category = 'Clothing' THEN price + 5
			ELSE price
		 END`)

	check("CU1 Laptop doubled",
		"SELECT price FROM v53_products WHERE id = 1", 2400)
	check("CU1 Shirt +5",
		"SELECT price FROM v53_products WHERE id = 3", 45)
	check("CU1 Book unchanged",
		"SELECT price FROM v53_products WHERE id = 5", 25)

	// ============================================================
	// === CASE IN INSERT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v53_status (id INTEGER PRIMARY KEY, label TEXT)`)

	// CI1: Insert with CASE
	checkNoError("CI1 CASE in INSERT",
		`INSERT INTO v53_status VALUES (1,
			CASE WHEN 1 > 0 THEN 'positive' ELSE 'non-positive' END)`)

	check("CI1 verify", "SELECT label FROM v53_status WHERE id = 1", "positive")

	// ============================================================
	// === ARITHMETIC EXPRESSIONS IN WHERE ===
	// ============================================================

	// AW1: Computed comparison
	checkRowCount("AW1 computed WHERE",
		`SELECT * FROM v53_products WHERE price * stock > 100000`, 2)

	// AW2: Modulo operator
	checkRowCount("AW2 modulo",
		`SELECT * FROM v53_products WHERE id % 2 = 0`, 3)

	// ============================================================
	// === INSERT...SELECT WITH TRANSFORMATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v53_summary (
		category TEXT, total_value INTEGER, product_count INTEGER)`)

	// IS1: INSERT SELECT with GROUP BY
	checkNoError("IS1 INSERT SELECT GROUP BY",
		`INSERT INTO v53_summary
		 SELECT category, SUM(price * stock), COUNT(*)
		 FROM v53_products GROUP BY category`)

	checkRowCount("IS1 verify rows", "SELECT * FROM v53_summary", 3)
	check("IS1 verify Electronics count",
		"SELECT product_count FROM v53_summary WHERE category = 'Electronics'", 2)

	// ============================================================
	// === SUBQUERY IN SELECT LIST ===
	// ============================================================

	// SS1: Scalar subquery in SELECT
	check("SS1 scalar subquery",
		`SELECT (SELECT COUNT(*) FROM v53_sales) AS sale_count FROM v53_products WHERE id = 1`, 3)

	// SS2: Correlated subquery in SELECT
	check("SS2 correlated subquery",
		`SELECT (SELECT SUM(qty) FROM v53_sales WHERE product_id = v53_products.id) AS total_qty
		 FROM v53_products WHERE id = 1`, 3)

	// ============================================================
	// === MULTIPLE UPDATES ON SAME TABLE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v53_counter (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v53_counter VALUES (1, 0)")

	// MU1: Multiple sequential updates
	checkNoError("MU1 update 1", "UPDATE v53_counter SET val = val + 1 WHERE id = 1")
	checkNoError("MU1 update 2", "UPDATE v53_counter SET val = val + 1 WHERE id = 1")
	checkNoError("MU1 update 3", "UPDATE v53_counter SET val = val + 1 WHERE id = 1")

	check("MU1 verify", "SELECT val FROM v53_counter WHERE id = 1", 3)

	// ============================================================
	// === DELETE WITH COMPLEX CONDITIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v53_del (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v53_del VALUES (1, 10, 'keep')")
	afExec(t, db, ctx, "INSERT INTO v53_del VALUES (2, 20, 'delete')")
	afExec(t, db, ctx, "INSERT INTO v53_del VALUES (3, 30, 'keep')")
	afExec(t, db, ctx, "INSERT INTO v53_del VALUES (4, 40, 'delete')")
	afExec(t, db, ctx, "INSERT INTO v53_del VALUES (5, 50, 'keep')")

	// DC1: Delete with AND condition
	checkNoError("DC1 DELETE AND",
		"DELETE FROM v53_del WHERE b = 'delete' AND a > 10")

	checkRowCount("DC1 verify", "SELECT * FROM v53_del", 3)

	// DC2: Delete with OR condition
	checkNoError("DC2 DELETE OR",
		"DELETE FROM v53_del WHERE a = 10 OR b = 'delete'")

	checkRowCount("DC2 verify", "SELECT * FROM v53_del", 2)

	// ============================================================
	// === TRANSACTIONS WITH DML ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v53_txn (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v53_txn VALUES (1, 'original')")

	// TX1: Rollback undoes changes
	checkNoError("TX1 BEGIN", "BEGIN")
	checkNoError("TX1 UPDATE", "UPDATE v53_txn SET val = 'changed' WHERE id = 1")
	check("TX1 see change in txn", "SELECT val FROM v53_txn WHERE id = 1", "changed")
	checkNoError("TX1 ROLLBACK", "ROLLBACK")
	check("TX1 rollback undid change", "SELECT val FROM v53_txn WHERE id = 1", "original")

	// TX2: Commit persists changes
	checkNoError("TX2 BEGIN", "BEGIN")
	checkNoError("TX2 INSERT", "INSERT INTO v53_txn VALUES (2, 'new')")
	checkNoError("TX2 COMMIT", "COMMIT")
	check("TX2 committed", "SELECT val FROM v53_txn WHERE id = 2", "new")

	// ============================================================
	// === ORDER BY + LIMIT + OFFSET ===
	// ============================================================

	// OL1: ORDER BY + LIMIT
	check("OL1 top expensive",
		"SELECT name FROM v53_products ORDER BY price DESC LIMIT 1", "Laptop")

	// OL2: ORDER BY + LIMIT + OFFSET
	check("OL2 second expensive",
		"SELECT name FROM v53_products ORDER BY price DESC LIMIT 1 OFFSET 1", "Phone")

	// OL3: LIMIT 0 returns no rows
	checkRowCount("OL3 LIMIT 0",
		"SELECT * FROM v53_products LIMIT 0", 0)

	// ============================================================
	// === LIKE PATTERNS ===
	// ============================================================

	// LP1: LIKE with %
	checkRowCount("LP1 LIKE percent",
		"SELECT * FROM v53_products WHERE name LIKE 'P%'", 3)

	// LP2: LIKE with _
	checkRowCount("LP2 LIKE underscore",
		"SELECT * FROM v53_products WHERE name LIKE 'P_n'", 1)

	// LP3: NOT LIKE
	checkRowCount("LP3 NOT LIKE",
		"SELECT * FROM v53_products WHERE name NOT LIKE 'P%'", 3)

	// ============================================================
	// === STRING FUNCTIONS ===
	// ============================================================

	// SF1: LENGTH
	check("SF1 LENGTH", "SELECT LENGTH('hello') FROM v53_products WHERE id = 1", 5)

	// SF2: UPPER
	check("SF2 UPPER", "SELECT UPPER('hello') FROM v53_products WHERE id = 1", "HELLO")

	// SF3: LOWER
	check("SF3 LOWER", "SELECT LOWER('HELLO') FROM v53_products WHERE id = 1", "hello")

	// ============================================================
	// === MULTI-TABLE OPERATIONS ===
	// ============================================================

	// MT1: Join + Aggregate (after DS2, sales left: id=4(product=1,qty=3), id=5(product=5,qty=50), id=6(product=2,qty=15))
	// Electronics: product 1 (qty=3) + product 2 (qty=15) = 18
	check("MT1 join aggregate",
		`SELECT SUM(s.qty) FROM v53_sales s
		 JOIN v53_products p ON s.product_id = p.id
		 WHERE p.category = 'Electronics'`, 18)

	// MT2: Join + GROUP BY + ORDER BY (only Electronics and Education have sales)
	checkRowCount("MT2 join group order",
		`SELECT p.category, SUM(s.qty) AS total
		 FROM v53_sales s
		 JOIN v53_products p ON s.product_id = p.id
		 GROUP BY p.category
		 ORDER BY total DESC`, 2)

	// ============================================================
	// === REPLACE INTO ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v53_kv (key TEXT PRIMARY KEY, value TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v53_kv VALUES ('a', 'first')")

	// RI1: Replace existing key
	checkNoError("RI1 REPLACE", "INSERT OR REPLACE INTO v53_kv VALUES ('a', 'second')")
	check("RI1 verify", "SELECT value FROM v53_kv WHERE key = 'a'", "second")
	checkRowCount("RI1 no dup", "SELECT * FROM v53_kv", 1)

	// RI2: Replace non-existing key (acts as insert)
	checkNoError("RI2 REPLACE new", "INSERT OR REPLACE INTO v53_kv VALUES ('b', 'new')")
	checkRowCount("RI2 two rows", "SELECT * FROM v53_kv", 2)

	t.Logf("\n=== V53 ADVANCED DML: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
