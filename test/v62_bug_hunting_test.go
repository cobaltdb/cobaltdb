package test

import (
	"fmt"
	"testing"
)

// TestV62BugHunting probes for bugs in tricky SQL patterns:
// aliased ORDER BY/GROUP BY, SELECT * with JOIN, UNION edge cases,
// NULL in DISTINCT, empty strings, nested aggregates, derived tables,
// multiple window functions, and complex expression evaluation.
func TestV62BugHunting(t *testing.T) {
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

	checkColCount := func(desc string, sql string, expectedCols int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		if len(rows[0]) != expectedCols {
			t.Errorf("[FAIL] %s: expected %d cols, got %d", desc, expectedCols, len(rows[0]))
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

	// Setup
	afExec(t, db, ctx, `CREATE TABLE v62_a (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v62_a VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO v62_a VALUES (2, 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO v62_a VALUES (3, 'Carol', 150)")

	afExec(t, db, ctx, `CREATE TABLE v62_b (id INTEGER PRIMARY KEY, a_id INTEGER, amount INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v62_b VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO v62_b VALUES (2, 1, 20)")
	afExec(t, db, ctx, "INSERT INTO v62_b VALUES (3, 2, 30)")
	afExec(t, db, ctx, "INSERT INTO v62_b VALUES (4, 3, 15)")
	afExec(t, db, ctx, "INSERT INTO v62_b VALUES (5, 3, 25)")

	// ============================================================
	// === SELECT * WITH JOIN ===
	// ============================================================

	// SJ1: SELECT * from single table
	checkColCount("SJ1 SELECT * single", "SELECT * FROM v62_a WHERE id = 1", 3)

	// SJ2: SELECT with specific columns from JOIN
	checkRowCount("SJ2 JOIN specific cols",
		"SELECT v62_a.name, v62_b.amount FROM v62_a JOIN v62_b ON v62_a.id = v62_b.a_id", 5)

	// ============================================================
	// === ALIASED COLUMNS IN ORDER BY ===
	// ============================================================

	// AO1: ORDER BY alias
	check("AO1 ORDER BY alias",
		"SELECT name AS n, val AS v FROM v62_a ORDER BY v DESC LIMIT 1", "Bob")

	// AO2: ORDER BY expression alias
	check("AO2 ORDER BY expr alias",
		`SELECT name, val * 2 AS double_val FROM v62_a ORDER BY double_val DESC LIMIT 1`, "Bob")

	// AO3: ORDER BY aggregate alias
	check("AO3 ORDER BY agg alias",
		`SELECT a.name, SUM(b.amount) AS total
		 FROM v62_a a JOIN v62_b b ON a.id = b.a_id
		 GROUP BY a.name
		 ORDER BY total DESC LIMIT 1`, "Carol")
	// Alice: 10+20=30, Bob: 30, Carol: 15+25=40 → Carol

	// ============================================================
	// === GROUP BY WITH ALIASES ===
	// ============================================================

	// GA1: GROUP BY column name (not alias)
	checkRowCount("GA1 GROUP BY col",
		"SELECT name, COUNT(*) FROM v62_a GROUP BY name", 3)

	// ============================================================
	// === MULTIPLE AGGREGATES ===
	// ============================================================

	// MA1: Multiple aggregates in one SELECT
	check("MA1 multi agg SUM",
		"SELECT SUM(val) FROM v62_a", 450)

	check("MA2 multi agg AVG",
		"SELECT AVG(val) FROM v62_a", 150)

	check("MA3 multi agg MIN",
		"SELECT MIN(val) FROM v62_a", 100)

	check("MA4 multi agg MAX",
		"SELECT MAX(val) FROM v62_a", 200)

	// MA5: All aggregates at once
	checkColCount("MA5 all aggs",
		"SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM v62_a", 5)

	// ============================================================
	// === HAVING WITH AGGREGATES ===
	// ============================================================

	// HA1: HAVING with SUM
	checkRowCount("HA1 HAVING SUM",
		`SELECT a.name, SUM(b.amount) AS total
		 FROM v62_a a JOIN v62_b b ON a.id = b.a_id
		 GROUP BY a.name
		 HAVING SUM(b.amount) >= 30`, 3)
	// Alice: 30, Bob: 30, Carol: 40 → all ≥ 30

	// HA2: HAVING with COUNT
	checkRowCount("HA2 HAVING COUNT",
		`SELECT a.name FROM v62_a a
		 JOIN v62_b b ON a.id = b.a_id
		 GROUP BY a.name
		 HAVING COUNT(*) > 1`, 2)
	// Alice: 2, Bob: 1, Carol: 2 → Alice, Carol

	// ============================================================
	// === DERIVED TABLES (SUBQUERY IN FROM) ===
	// ============================================================

	// DT1: Simple derived table
	check("DT1 derived table",
		`SELECT MAX(total) FROM (
			SELECT a_id, SUM(amount) AS total FROM v62_b GROUP BY a_id
		) sub`, 40)

	// DT2: Derived table with WHERE
	checkRowCount("DT2 derived WHERE",
		`SELECT * FROM (
			SELECT name, val FROM v62_a WHERE val > 100
		) sub`, 2)

	// ============================================================
	// === UNION EDGE CASES ===
	// ============================================================

	// UE1: UNION with same types
	checkRowCount("UE1 UNION same type",
		`SELECT name FROM v62_a WHERE val > 150
		 UNION
		 SELECT name FROM v62_a WHERE val < 120`, 2)
	// >150: Bob; <120: Alice → 2 distinct names

	// UE2: UNION ALL preserves duplicates
	checkRowCount("UE2 UNION ALL",
		`SELECT name FROM v62_a WHERE val >= 100
		 UNION ALL
		 SELECT name FROM v62_a WHERE val <= 200`, 6)
	// First: Alice,Bob,Carol (3); Second: Alice,Bob,Carol (3); UNION ALL = 6

	// UE3: UNION with ORDER BY
	check("UE3 UNION ORDER BY",
		`SELECT name FROM v62_a WHERE val > 100
		 UNION
		 SELECT name FROM v62_a WHERE val <= 100
		 ORDER BY name ASC LIMIT 1`, "Alice")

	// ============================================================
	// === NULL IN DISTINCT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v62_nulldist (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v62_nulldist VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v62_nulldist VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO v62_nulldist VALUES (3, NULL)")
	afExec(t, db, ctx, "INSERT INTO v62_nulldist VALUES (4, 'a')")
	afExec(t, db, ctx, "INSERT INTO v62_nulldist VALUES (5, NULL)")

	// ND1: DISTINCT with NULLs (NULL should be treated as one value)
	checkRowCount("ND1 DISTINCT NULL",
		"SELECT DISTINCT val FROM v62_nulldist", 3)
	// 'a', 'b', NULL = 3 distinct values

	// ============================================================
	// === EMPTY STRING VS NULL ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v62_empty (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v62_empty VALUES (1, '')")
	afExec(t, db, ctx, "INSERT INTO v62_empty VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v62_empty VALUES (3, 'text')")

	// ES1: Empty string is not NULL
	checkRowCount("ES1 empty not null",
		"SELECT * FROM v62_empty WHERE val IS NOT NULL", 2)
	// '' and 'text' are not null

	// ES2: Empty string comparison
	checkRowCount("ES2 empty string eq",
		"SELECT * FROM v62_empty WHERE val = ''", 1)

	// ES3: LENGTH of empty string
	check("ES3 LENGTH empty",
		"SELECT LENGTH(val) FROM v62_empty WHERE id = 1", 0)

	// ============================================================
	// === MULTIPLE WINDOW FUNCTIONS ===
	// ============================================================

	// MW1: ROW_NUMBER and RANK together
	check("MW1 ROW_NUMBER",
		`WITH w AS (
			SELECT name, val,
				   ROW_NUMBER() OVER (ORDER BY val DESC) AS rn,
				   RANK() OVER (ORDER BY val DESC) AS rnk
			FROM v62_a
		)
		SELECT name FROM w WHERE rn = 1`, "Bob")

	// MW2: Window function with partition
	check("MW2 window partition",
		`WITH data AS (
			SELECT a.name, b.amount,
				   SUM(b.amount) OVER (PARTITION BY a.id ORDER BY b.id) AS running
			FROM v62_a a JOIN v62_b b ON a.id = b.a_id
		)
		SELECT running FROM data WHERE name = 'Alice' AND amount = 20`, 30)
	// Alice: amounts 10, 20. Running: 10, 30. For amount=20: 30

	// ============================================================
	// === COMPLEX EXPRESSION EVALUATION ===
	// ============================================================

	// CE1: Nested arithmetic
	check("CE1 nested arith",
		"SELECT (1 + 2) * (3 + 4) FROM v62_a WHERE id = 1", 21)

	// CE2: CASE inside CASE
	check("CE2 CASE in CASE",
		`SELECT CASE
			WHEN val > 150 THEN CASE WHEN val > 250 THEN 'very high' ELSE 'high' END
			WHEN val > 100 THEN 'medium'
			ELSE 'low'
		 END FROM v62_a WHERE id = 2`, "high")

	// CE3: COALESCE with NULLIF
	check("CE3 COALESCE NULLIF",
		"SELECT COALESCE(NULLIF(val, 100), 999) FROM v62_a WHERE id = 1", 999)
	// NULLIF(100, 100) = NULL, COALESCE(NULL, 999) = 999

	// CE4: Arithmetic with NULL
	checkNull("CE4 NULL arithmetic",
		"SELECT NULL + 5 FROM v62_a WHERE id = 1")

	// ============================================================
	// === INSERT INTO SELECT WITH TRANSFORM ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v62_transformed (id INTEGER PRIMARY KEY, label TEXT, doubled INTEGER)`)
	checkNoError("IT1 INSERT SELECT transform",
		"INSERT INTO v62_transformed SELECT id, UPPER(name), val * 2 FROM v62_a")
	check("IT1 verify", "SELECT label FROM v62_transformed WHERE id = 1", "ALICE")
	check("IT2 verify doubled", "SELECT doubled FROM v62_transformed WHERE id = 2", 400)

	// ============================================================
	// === UPDATE WITH EXPRESSION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v62_upd (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v62_upd VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v62_upd VALUES (2, 20)")

	checkNoError("UE1 UPDATE expr",
		"UPDATE v62_upd SET val = val + (SELECT MAX(val) FROM v62_a) WHERE id = 1")
	check("UE1 verify", "SELECT val FROM v62_upd WHERE id = 1", 210)
	// 10 + MAX(100,200,150) = 10 + 200 = 210

	// ============================================================
	// === DELETE WITH COMPLEX WHERE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v62_del (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v62_del VALUES (1, 'x', 10)")
	afExec(t, db, ctx, "INSERT INTO v62_del VALUES (2, 'y', 20)")
	afExec(t, db, ctx, "INSERT INTO v62_del VALUES (3, 'x', 30)")
	afExec(t, db, ctx, "INSERT INTO v62_del VALUES (4, 'y', 40)")

	checkNoError("DC1 DELETE complex",
		"DELETE FROM v62_del WHERE (cat = 'x' AND val > 20) OR (cat = 'y' AND val < 30)")
	checkRowCount("DC1 verify", "SELECT * FROM v62_del", 2)
	// Delete: id=3 (x,30>20), id=2 (y,20<30). Remaining: 1,4

	// ============================================================
	// === MULTIPLE UPDATES VERIFY ISOLATION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v62_iso (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v62_iso VALUES (1, 10, 20)")

	// Update both columns in same statement — b should see old a value
	checkNoError("ISO1 multi col update",
		"UPDATE v62_iso SET a = b, b = a WHERE id = 1")
	check("ISO1 verify a", "SELECT a FROM v62_iso WHERE id = 1", 20)
	check("ISO1 verify b", "SELECT b FROM v62_iso WHERE id = 1", 10)
	// a=b(20)=20, b=a(10)=10 — swap!

	t.Logf("\n=== V62 BUG HUNTING: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
