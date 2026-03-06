package test

import (
	"fmt"
	"testing"
)

// TestV56FinalCoverage exercises remaining SQL patterns to find any lingering bugs:
// multi-column GROUP BY, ORDER BY with NULLs, complex JOINs with aggregates,
// recursive patterns, and real-world query patterns.
func TestV56FinalCoverage(t *testing.T) {
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
	_ = checkNoError

	// ============================================================
	// === MULTI-COLUMN GROUP BY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v56_sales (
		id INTEGER PRIMARY KEY, product TEXT, region TEXT, year INTEGER, amount INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v56_sales VALUES (1, 'Laptop', 'NA', 2023, 1000)")
	afExec(t, db, ctx, "INSERT INTO v56_sales VALUES (2, 'Laptop', 'NA', 2023, 1500)")
	afExec(t, db, ctx, "INSERT INTO v56_sales VALUES (3, 'Laptop', 'EU', 2023, 1200)")
	afExec(t, db, ctx, "INSERT INTO v56_sales VALUES (4, 'Phone', 'NA', 2023, 800)")
	afExec(t, db, ctx, "INSERT INTO v56_sales VALUES (5, 'Phone', 'NA', 2024, 900)")
	afExec(t, db, ctx, "INSERT INTO v56_sales VALUES (6, 'Phone', 'EU', 2024, 700)")
	afExec(t, db, ctx, "INSERT INTO v56_sales VALUES (7, 'Laptop', 'NA', 2024, 1100)")

	// MG1: GROUP BY two columns
	checkRowCount("MG1 group by 2 cols",
		`SELECT product, region, SUM(amount) FROM v56_sales
		 GROUP BY product, region`, 4)

	// MG2: GROUP BY three columns
	checkRowCount("MG2 group by 3 cols",
		`SELECT product, region, year, SUM(amount) FROM v56_sales
		 GROUP BY product, region, year`, 6)

	// MG3: Specific group result
	check("MG3 specific group",
		`SELECT SUM(amount) FROM v56_sales WHERE product = 'Laptop' AND region = 'NA'`, 3600)

	// ============================================================
	// === ORDER BY WITH NULL VALUES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v56_nullsort (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v56_nullsort VALUES (1, 30)")
	afExec(t, db, ctx, "INSERT INTO v56_nullsort VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v56_nullsort VALUES (3, 10)")
	afExec(t, db, ctx, "INSERT INTO v56_nullsort VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO v56_nullsort VALUES (5, 20)")

	// NS1: ORDER BY ASC - NULLs should come first or last consistently
	checkRowCount("NS1 order all rows",
		"SELECT val FROM v56_nullsort ORDER BY val ASC", 5)

	// NS2: Non-null ordering is correct
	check("NS2 first non-null ASC",
		`WITH ordered AS (
			SELECT val, ROW_NUMBER() OVER (ORDER BY val ASC) AS rn
			FROM v56_nullsort WHERE val IS NOT NULL
		)
		SELECT val FROM ordered WHERE rn = 1`, 10)

	// ============================================================
	// === COMPLEX JOIN + CTE + AGGREGATE PIPELINE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v56_customers (
		id INTEGER PRIMARY KEY, name TEXT, tier TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v56_customers VALUES (1, 'Acme', 'Gold')")
	afExec(t, db, ctx, "INSERT INTO v56_customers VALUES (2, 'Beta', 'Silver')")
	afExec(t, db, ctx, "INSERT INTO v56_customers VALUES (3, 'Corp', 'Gold')")
	afExec(t, db, ctx, "INSERT INTO v56_customers VALUES (4, 'Delta', 'Bronze')")

	afExec(t, db, ctx, `CREATE TABLE v56_orders (
		id INTEGER PRIMARY KEY, customer_id INTEGER, total INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v56_orders VALUES (1, 1, 500)")
	afExec(t, db, ctx, "INSERT INTO v56_orders VALUES (2, 1, 300)")
	afExec(t, db, ctx, "INSERT INTO v56_orders VALUES (3, 2, 200)")
	afExec(t, db, ctx, "INSERT INTO v56_orders VALUES (4, 3, 700)")
	afExec(t, db, ctx, "INSERT INTO v56_orders VALUES (5, 3, 100)")

	// CJA1: CTE + JOIN pipeline
	check("CJA1 CTE join pipeline",
		`WITH customer_totals AS (
			SELECT customer_id, SUM(total) AS spend
			FROM v56_orders GROUP BY customer_id
		)
		SELECT c.name FROM customer_totals ct
		JOIN v56_customers c ON ct.customer_id = c.id
		ORDER BY ct.spend DESC LIMIT 1`, "Acme")

	// CJA2: CTE aggregating joined data
	check("CJA2 CTE agg join",
		`WITH tier_totals AS (
			SELECT c.tier, SUM(o.total) AS tier_spend
			FROM v56_customers c
			JOIN v56_orders o ON c.id = o.customer_id
			GROUP BY c.tier
		)
		SELECT tier FROM tier_totals ORDER BY tier_spend DESC LIMIT 1`, "Gold")

	// CJA3: Tier spend value
	check("CJA3 tier spend",
		`SELECT SUM(o.total) FROM v56_orders o
		 JOIN v56_customers c ON o.customer_id = c.id
		 WHERE c.tier = 'Gold'`, 1600)

	// ============================================================
	// === WINDOW FUNCTIONS: RANK AND DENSE_RANK ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v56_scores (
		id INTEGER PRIMARY KEY, name TEXT, score INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v56_scores VALUES (1, 'Alice', 95)")
	afExec(t, db, ctx, "INSERT INTO v56_scores VALUES (2, 'Bob', 90)")
	afExec(t, db, ctx, "INSERT INTO v56_scores VALUES (3, 'Carol', 95)")
	afExec(t, db, ctx, "INSERT INTO v56_scores VALUES (4, 'Dave', 85)")
	afExec(t, db, ctx, "INSERT INTO v56_scores VALUES (5, 'Eve', 90)")

	// WF1: RANK with ties
	check("WF1 RANK ties",
		`WITH ranked AS (
			SELECT name, RANK() OVER (ORDER BY score DESC) AS rnk FROM v56_scores
		)
		SELECT rnk FROM ranked WHERE name = 'Bob'`, 3)

	// WF2: DENSE_RANK with ties
	check("WF2 DENSE_RANK ties",
		`WITH ranked AS (
			SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM v56_scores
		)
		SELECT drnk FROM ranked WHERE name = 'Bob'`, 2)

	// WF3: ROW_NUMBER breaks ties by order
	check("WF3 ROW_NUMBER",
		`WITH numbered AS (
			SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM v56_scores
		)
		SELECT rn FROM numbered WHERE name = 'Dave'`, 5)

	// ============================================================
	// === WINDOW FUNCTIONS: LAG AND LEAD ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v56_timeline (
		id INTEGER PRIMARY KEY, month INTEGER, revenue INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v56_timeline VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO v56_timeline VALUES (2, 2, 120)")
	afExec(t, db, ctx, "INSERT INTO v56_timeline VALUES (3, 3, 90)")
	afExec(t, db, ctx, "INSERT INTO v56_timeline VALUES (4, 4, 150)")

	// LL1: LAG
	check("LL1 LAG",
		`WITH lagged AS (
			SELECT month, revenue, LAG(revenue) OVER (ORDER BY month) AS prev
			FROM v56_timeline
		)
		SELECT prev FROM lagged WHERE month = 3`, 120)

	// LL2: LEAD
	check("LL2 LEAD",
		`WITH led AS (
			SELECT month, revenue, LEAD(revenue) OVER (ORDER BY month) AS nxt
			FROM v56_timeline
		)
		SELECT nxt FROM led WHERE month = 2`, 90)

	// ============================================================
	// === NESTED SUBQUERIES ===
	// ============================================================

	// NQ1: Subquery in WHERE with IN
	check("NQ1 nested IN",
		`SELECT name FROM v56_customers WHERE id IN (
			SELECT customer_id FROM v56_orders WHERE total > 400
		) ORDER BY name ASC LIMIT 1`, "Acme")

	// NQ2: Scalar subquery comparison
	check("NQ2 scalar subquery",
		`SELECT name FROM v56_customers
		 WHERE id = (SELECT customer_id FROM v56_orders WHERE total = 700)`, "Corp")

	// NQ3: EXISTS subquery
	checkRowCount("NQ3 EXISTS",
		`SELECT name FROM v56_customers
		 WHERE EXISTS (SELECT 1 FROM v56_orders WHERE customer_id = v56_customers.id)`, 3)

	// NQ4: NOT EXISTS
	checkRowCount("NQ4 NOT EXISTS",
		`SELECT name FROM v56_customers
		 WHERE NOT EXISTS (SELECT 1 FROM v56_orders WHERE customer_id = v56_customers.id)`, 1)

	// ============================================================
	// === REAL-WORLD PATTERNS ===
	// ============================================================

	// RW1: Top-N per group using chained CTEs
	check("RW1 top per group",
		`WITH customer_spend AS (
			SELECT c.tier, c.name, SUM(o.total) AS spend
			FROM v56_customers c
			JOIN v56_orders o ON c.id = o.customer_id
			GROUP BY c.tier, c.name
		),
		ranked AS (
			SELECT tier, name, spend,
				   ROW_NUMBER() OVER (PARTITION BY tier ORDER BY spend DESC) AS rn
			FROM customer_spend
		)
		SELECT name FROM ranked WHERE rn = 1 AND tier = 'Gold'`, "Acme")

	// RW2: Running total
	check("RW2 running total",
		`WITH running AS (
			SELECT month, revenue,
				   SUM(revenue) OVER (ORDER BY month) AS cumulative
			FROM v56_timeline
		)
		SELECT cumulative FROM running WHERE month = 3`, 310)

	// RW3: Year-over-year comparison using CTE
	check("RW3 total by year",
		`WITH yearly AS (
			SELECT year, SUM(amount) AS total FROM v56_sales GROUP BY year
		)
		SELECT total FROM yearly WHERE year = 2023`, 4500)

	// ============================================================
	// === UNION OPERATIONS ===
	// ============================================================

	// UO1: UNION removes duplicates
	check("UO1 UNION dedup",
		`WITH combined AS (
			SELECT name FROM v56_customers WHERE tier = 'Gold'
			UNION
			SELECT name FROM v56_customers WHERE id = 1
		)
		SELECT COUNT(*) FROM combined`, 2)

	// UO2: UNION ALL keeps duplicates
	check("UO2 UNION ALL",
		`WITH combined AS (
			SELECT name FROM v56_customers WHERE tier = 'Gold'
			UNION ALL
			SELECT name FROM v56_customers WHERE id = 1
		)
		SELECT COUNT(*) FROM combined`, 3)

	// ============================================================
	// === COMPLEX UPDATE WITH SUBQUERY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v56_inventory (
		id INTEGER PRIMARY KEY, product TEXT, qty INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v56_inventory VALUES (1, 'Laptop', 100)")
	afExec(t, db, ctx, "INSERT INTO v56_inventory VALUES (2, 'Phone', 200)")

	// CU1: UPDATE with subquery in SET
	checkNoError("CU1 UPDATE subquery SET",
		`UPDATE v56_inventory SET qty = qty - (
			SELECT COALESCE(SUM(amount), 0) FROM v56_sales WHERE product = v56_inventory.product
		)`)

	// Laptop sales total: 1000+1500+1200+1100 = 4800; 100 - 4800 = -4700
	check("CU1 Laptop after deduction",
		"SELECT qty FROM v56_inventory WHERE product = 'Laptop'", -4700)

	// ============================================================
	// === DELETE WITH SUBQUERY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v56_del_test (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v56_del_test VALUES (1, 'keep')")
	afExec(t, db, ctx, "INSERT INTO v56_del_test VALUES (2, 'remove')")
	afExec(t, db, ctx, "INSERT INTO v56_del_test VALUES (3, 'keep')")

	// DS1: DELETE with IN subquery
	checkNoError("DS1 DELETE IN subquery",
		"DELETE FROM v56_del_test WHERE id IN (SELECT id FROM v56_del_test WHERE val = 'remove')")

	checkRowCount("DS1 verify", "SELECT * FROM v56_del_test", 2)

	// ============================================================
	// === EXPRESSION EVALUATION EDGE CASES ===
	// ============================================================

	// EE1: Concatenation with numbers
	check("EE1 concat number",
		"SELECT 'item-' || CAST(42 AS TEXT) FROM v56_scores WHERE id = 1", "item-42")

	// EE2: CASE with aggregate result
	check("EE2 CASE aggregate",
		`SELECT CASE
			WHEN SUM(score) > 400 THEN 'high'
			ELSE 'low'
		 END FROM v56_scores`, "high")

	// EE3: Arithmetic in CASE
	check("EE3 CASE arithmetic",
		`SELECT CASE
			WHEN 2 + 2 = 4 THEN 'correct'
			ELSE 'wrong'
		 END FROM v56_scores WHERE id = 1`, "correct")

	// ============================================================
	// === DISTINCT + ORDER BY + LIMIT ===
	// ============================================================

	// DOL1: DISTINCT with ORDER BY
	check("DOL1 DISTINCT ORDER",
		"SELECT DISTINCT region FROM v56_sales ORDER BY region ASC LIMIT 1", "EU")

	// DOL2: DISTINCT with LIMIT
	checkRowCount("DOL2 DISTINCT LIMIT",
		"SELECT DISTINCT product FROM v56_sales LIMIT 1", 1)

	t.Logf("\n=== V56 FINAL COVERAGE: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
