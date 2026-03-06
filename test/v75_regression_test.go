package test

import (
	"fmt"
	"testing"
)

// TestV75Regression is a comprehensive regression test that verifies all known
// bug fixes remain working. Each test case references the specific bug that was fixed.
func TestV75Regression(t *testing.T) {
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

	// ============================================================
	// === BUG: LIMIT 0 not returning empty result (Task #223) ===
	// ============================================================
	afExec(t, db, ctx, `CREATE TABLE v75_limit (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v75_limit VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v75_limit VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v75_limit VALUES (3, 30)")

	checkRowCount("LIMIT-0: basic", "SELECT * FROM v75_limit LIMIT 0", 0)
	checkRowCount("LIMIT-0: with ORDER BY", "SELECT * FROM v75_limit ORDER BY id LIMIT 0", 0)
	checkRowCount("LIMIT-0: with WHERE", "SELECT * FROM v75_limit WHERE val > 5 LIMIT 0", 0)

	// With JOIN
	afExec(t, db, ctx, `CREATE TABLE v75_lj (id INTEGER PRIMARY KEY, ref_id INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v75_lj VALUES (1, 1)")
	checkRowCount("LIMIT-0: with JOIN",
		"SELECT * FROM v75_limit l JOIN v75_lj j ON l.id = j.ref_id LIMIT 0", 0)

	// With GROUP BY
	checkRowCount("LIMIT-0: with GROUP BY",
		"SELECT val, COUNT(*) FROM v75_limit GROUP BY val LIMIT 0", 0)

	// ============================================================
	// === BUG: NULL dedup in DISTINCT (Task #224) ===
	// ============================================================
	afExec(t, db, ctx, `CREATE TABLE v75_dedup (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v75_dedup VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v75_dedup VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v75_dedup VALUES (3, 'a')")
	afExec(t, db, ctx, "INSERT INTO v75_dedup VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO v75_dedup VALUES (5, '<nil>')")

	// DISTINCT should collapse duplicate NULLs but NOT collapse NULL with "<nil>" string
	checkRowCount("DEDUP: DISTINCT with NULLs",
		"SELECT DISTINCT val FROM v75_dedup", 3) // 'a', NULL, '<nil>'

	// ============================================================
	// === BUG: NULL dedup in UNION (Task #224) ===
	// ============================================================
	afExec(t, db, ctx, `CREATE TABLE v75_u1 (val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v75_u1 VALUES ('a')")
	afExec(t, db, ctx, "INSERT INTO v75_u1 VALUES (NULL)")

	afExec(t, db, ctx, `CREATE TABLE v75_u2 (val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v75_u2 VALUES ('a')")
	afExec(t, db, ctx, "INSERT INTO v75_u2 VALUES (NULL)")
	afExec(t, db, ctx, "INSERT INTO v75_u2 VALUES ('b')")

	// UNION should dedup NULL across both sides
	checkRowCount("DEDUP: UNION NULL dedup",
		"SELECT val FROM v75_u1 UNION SELECT val FROM v75_u2", 3) // 'a', NULL, 'b'

	// UNION ALL should keep all NULLs
	checkRowCount("DEDUP: UNION ALL NULLs",
		"SELECT val FROM v75_u1 UNION ALL SELECT val FROM v75_u2", 5)

	// INTERSECT with NULLs
	checkRowCount("DEDUP: INTERSECT NULLs",
		"SELECT val FROM v75_u1 INTERSECT SELECT val FROM v75_u2", 2) // 'a', NULL

	// EXCEPT with NULLs
	checkRowCount("DEDUP: EXCEPT NULLs",
		"SELECT val FROM v75_u2 EXCEPT SELECT val FROM v75_u1", 1) // 'b'

	// ============================================================
	// === BUG: WHERE ignored in scalar SELECT (Task #229) ===
	// ============================================================

	// WHERE should filter even without FROM clause
	checkRowCount("SCALAR-WHERE: true", "SELECT 1 WHERE 1 = 1", 1)
	checkRowCount("SCALAR-WHERE: false", "SELECT 1 WHERE 1 = 0", 0)

	// NULL comparison in WHERE (SQL standard: unknown = false)
	checkRowCount("SCALAR-WHERE: NULL = NULL", "SELECT 1 WHERE NULL = NULL", 0)
	checkRowCount("SCALAR-WHERE: NULL != NULL", "SELECT 1 WHERE NULL != NULL", 0)
	checkRowCount("SCALAR-WHERE: IS NULL true", "SELECT 1 WHERE NULL IS NULL", 1)

	// ============================================================
	// === BUG: ALTER TABLE ADD COLUMN not backfilling (Task #222) ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v75_alter (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v75_alter VALUES (1, 'alice')")
	afExec(t, db, ctx, "INSERT INTO v75_alter VALUES (2, 'bob')")

	// Add column with DEFAULT should backfill existing rows
	checkNoError("ALTER: ADD COLUMN DEFAULT",
		"ALTER TABLE v75_alter ADD COLUMN score INTEGER DEFAULT 50")
	check("ALTER: backfill row 1", "SELECT score FROM v75_alter WHERE id = 1", 50)
	check("ALTER: backfill row 2", "SELECT score FROM v75_alter WHERE id = 2", 50)

	// New inserts should use default too
	checkNoError("ALTER: insert after", "INSERT INTO v75_alter (id, name) VALUES (3, 'carol')")
	check("ALTER: default new row", "SELECT score FROM v75_alter WHERE id = 3", 50)

	// Explicit value overrides default
	checkNoError("ALTER: explicit", "INSERT INTO v75_alter VALUES (4, 'dave', 99)")
	check("ALTER: explicit value", "SELECT score FROM v75_alter WHERE id = 4", 99)

	// ============================================================
	// === BUG: INTERSECT/EXCEPT not supported (Task #219) ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v75_seta (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v75_seta VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v75_seta VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO v75_seta VALUES (3, 'Carol')")

	afExec(t, db, ctx, `CREATE TABLE v75_setb (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v75_setb VALUES (1, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO v75_setb VALUES (2, 'Carol')")
	afExec(t, db, ctx, "INSERT INTO v75_setb VALUES (3, 'Dave')")

	// INTERSECT works
	checkRowCount("SET-OPS: INTERSECT",
		"SELECT name FROM v75_seta INTERSECT SELECT name FROM v75_setb", 2) // Bob, Carol

	// EXCEPT works
	checkRowCount("SET-OPS: EXCEPT",
		"SELECT name FROM v75_seta EXCEPT SELECT name FROM v75_setb", 1) // Alice

	// In CTE
	check("SET-OPS: INTERSECT in CTE",
		`WITH common AS (
			SELECT name FROM v75_seta INTERSECT SELECT name FROM v75_setb
		) SELECT COUNT(*) FROM common`, 2)

	// In derived table
	check("SET-OPS: EXCEPT in derived",
		`SELECT COUNT(*) FROM (
			SELECT name FROM v75_setb EXCEPT SELECT name FROM v75_seta
		) sub`, 1) // Dave

	// ============================================================
	// === BUG: UNION in derived tables (Task #218) ===
	// ============================================================

	// UNION should work inside FROM (derived table)
	check("UNION-DERIVED: basic",
		`SELECT COUNT(*) FROM (
			SELECT name FROM v75_seta UNION ALL SELECT name FROM v75_setb
		) sub`, 6)

	check("UNION-DERIVED: dedup",
		`SELECT COUNT(*) FROM (
			SELECT name FROM v75_seta UNION SELECT name FROM v75_setb
		) sub`, 4) // Alice, Bob, Carol, Dave

	// ============================================================
	// === BUG: SUM() returning 0 instead of NULL (Task #204) ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v75_nullagg (id INTEGER PRIMARY KEY, val INTEGER)`)
	// Empty table
	checkNull("NULL-AGG: SUM empty", "SELECT SUM(val) FROM v75_nullagg")
	checkNull("NULL-AGG: AVG empty", "SELECT AVG(val) FROM v75_nullagg")
	checkNull("NULL-AGG: MIN empty", "SELECT MIN(val) FROM v75_nullagg")
	checkNull("NULL-AGG: MAX empty", "SELECT MAX(val) FROM v75_nullagg")
	check("NULL-AGG: COUNT empty", "SELECT COUNT(*) FROM v75_nullagg", 0)

	// Table with only NULLs
	afExec(t, db, ctx, "INSERT INTO v75_nullagg VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO v75_nullagg VALUES (2, NULL)")
	checkNull("NULL-AGG: SUM all null", "SELECT SUM(val) FROM v75_nullagg")
	check("NULL-AGG: COUNT all null", "SELECT COUNT(val) FROM v75_nullagg", 0)
	check("NULL-AGG: COUNT(*) all null", "SELECT COUNT(*) FROM v75_nullagg", 2)

	// ============================================================
	// === BUG: Window running mode (Task #207) ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v75_win (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v75_win VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v75_win VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v75_win VALUES (3, 30)")

	// Running SUM with ORDER BY
	check("WINDOW: running SUM row 1",
		"SELECT SUM(val) OVER (ORDER BY id) FROM v75_win WHERE id = 1", 10)

	// ROW_NUMBER
	check("WINDOW: ROW_NUMBER",
		`WITH numbered AS (
			SELECT id, val, ROW_NUMBER() OVER (ORDER BY val DESC) as rn
			FROM v75_win
		) SELECT id FROM numbered WHERE rn = 1`, 3)

	// RANK
	afExec(t, db, ctx, `CREATE TABLE v75_rank (id INTEGER PRIMARY KEY, score INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v75_rank VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO v75_rank VALUES (2, 100)")
	afExec(t, db, ctx, "INSERT INTO v75_rank VALUES (3, 90)")
	check("WINDOW: RANK tie",
		`WITH ranked AS (
			SELECT id, RANK() OVER (ORDER BY score DESC) as rk FROM v75_rank
		) SELECT rk FROM ranked WHERE id = 3`, 3) // Tied at 1, so 3rd gets rank 3

	// ============================================================
	// === BUG: Chained CTEs (Task #208) ===
	// ============================================================

	check("CTE-CHAIN: second references first",
		`WITH step1 AS (
			SELECT 42 as val
		),
		step2 AS (
			SELECT val * 2 as doubled FROM step1
		)
		SELECT doubled FROM step2`, 84)

	// 3-level chain
	check("CTE-CHAIN: 3 levels",
		`WITH a AS (SELECT 10 as x),
		 b AS (SELECT x + 20 as y FROM a),
		 c AS (SELECT y * 3 as z FROM b)
		 SELECT z FROM c`, 90)

	// ============================================================
	// === BUG: GROUP BY positional refs (Task #191) ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v75_gpos (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v75_gpos VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO v75_gpos VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO v75_gpos VALUES (3, 'a', 30)")

	checkRowCount("POS-REF: GROUP BY 1",
		"SELECT cat, SUM(val) FROM v75_gpos GROUP BY 1", 2)

	// ============================================================
	// === BUG: ORDER BY positional refs (Task #192) ===
	// ============================================================

	check("POS-REF: ORDER BY 2 DESC",
		"SELECT cat, SUM(val) as total FROM v75_gpos GROUP BY 1 ORDER BY 2 DESC LIMIT 1", "a")
	// a=40, b=20 → a first

	// ============================================================
	// === BUG: SAVEPOINT support (Task #198) ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v75_sp (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v75_sp VALUES (1, 100)")

	checkNoError("SAVEPOINT: BEGIN", "BEGIN")
	checkNoError("SAVEPOINT: UPDATE 1", "UPDATE v75_sp SET val = 200 WHERE id = 1")
	checkNoError("SAVEPOINT: SAVEPOINT", "SAVEPOINT sp1")
	checkNoError("SAVEPOINT: UPDATE 2", "UPDATE v75_sp SET val = 300 WHERE id = 1")
	checkNoError("SAVEPOINT: ROLLBACK TO", "ROLLBACK TO sp1")
	checkNoError("SAVEPOINT: COMMIT", "COMMIT")
	check("SAVEPOINT: verify", "SELECT val FROM v75_sp WHERE id = 1", 200)

	// ============================================================
	// === BUG: Derived tables (Task #193) ===
	// ============================================================

	check("DERIVED: subquery in FROM",
		`SELECT MAX(total) FROM (
			SELECT cat, SUM(val) as total FROM v75_gpos GROUP BY cat
		) sub`, 40)

	// ============================================================
	// === FULL REGRESSION COUNT ===
	// ============================================================

	t.Logf("\n=== V75 REGRESSION: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
