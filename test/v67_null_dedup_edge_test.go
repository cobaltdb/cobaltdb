package test

import (
	"fmt"
	"testing"
)

// TestV67NullDedupEdge tests NULL handling in DISTINCT, UNION dedup,
// INTERSECT/EXCEPT with NULLs, negative/expression LIMIT/OFFSET,
// and various edge cases around deduplication and boundaries.
func TestV67NullDedupEdge(t *testing.T) {
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
			pass++ // no rows = NULL-ish
			return
		}
		if rows[0][0] == nil {
			pass++
			return
		}
		t.Errorf("[FAIL] %s: expected NULL, got %v", desc, rows[0][0])
	}

	// ============================================================
	// === NULL IN DISTINCT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_nd (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_nd VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v67_nd VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v67_nd VALUES (3, 'a')")
	afExec(t, db, ctx, "INSERT INTO v67_nd VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO v67_nd VALUES (5, 'b')")

	// ND1: DISTINCT should collapse duplicate NULLs
	checkRowCount("ND1 DISTINCT with NULLs",
		"SELECT DISTINCT val FROM v67_nd", 3)
	// a, NULL, b = 3 distinct values

	// ND2: DISTINCT with NULL and the string '<nil>' should NOT be equal
	afExec(t, db, ctx, `CREATE TABLE v67_nil_str (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_nil_str VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO v67_nil_str VALUES (2, '<nil>')")
	checkRowCount("ND2 NULL vs '<nil>' string",
		"SELECT DISTINCT val FROM v67_nil_str", 2)

	// ND3: DISTINCT with all NULLs
	afExec(t, db, ctx, `CREATE TABLE v67_allnull (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_allnull VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO v67_allnull VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v67_allnull VALUES (3, NULL)")
	checkRowCount("ND3 DISTINCT all NULLs",
		"SELECT DISTINCT val FROM v67_allnull", 1)

	// ND4: DISTINCT multi-column with mixed NULLs
	afExec(t, db, ctx, `CREATE TABLE v67_mc (id INTEGER PRIMARY KEY, a TEXT, b TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_mc VALUES (1, 'x', NULL)")
	afExec(t, db, ctx, "INSERT INTO v67_mc VALUES (2, NULL, 'y')")
	afExec(t, db, ctx, "INSERT INTO v67_mc VALUES (3, 'x', NULL)")
	afExec(t, db, ctx, "INSERT INTO v67_mc VALUES (4, NULL, 'y')")
	afExec(t, db, ctx, "INSERT INTO v67_mc VALUES (5, NULL, NULL)")
	checkRowCount("ND4 DISTINCT multi-col NULLs",
		"SELECT DISTINCT a, b FROM v67_mc", 3)
	// (x, NULL), (NULL, y), (NULL, NULL) = 3

	// ============================================================
	// === NULL IN UNION DEDUP ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_u1 (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_u1 VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v67_u1 VALUES (2, NULL)")

	afExec(t, db, ctx, `CREATE TABLE v67_u2 (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_u2 VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v67_u2 VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v67_u2 VALUES (3, 'b')")

	// UN1: UNION should dedup NULLs across tables
	checkRowCount("UN1 UNION NULL dedup",
		`SELECT val FROM v67_u1
		 UNION
		 SELECT val FROM v67_u2`, 3)
	// a, NULL, b = 3

	// UN2: UNION ALL should keep all NULLs
	checkRowCount("UN2 UNION ALL keeps NULLs",
		`SELECT val FROM v67_u1
		 UNION ALL
		 SELECT val FROM v67_u2`, 5)
	// a, NULL, a, NULL, b = 5

	// ============================================================
	// === NULL IN INTERSECT ===
	// ============================================================

	// NI1: INTERSECT with NULLs on both sides
	checkRowCount("NI1 INTERSECT with NULLs",
		`SELECT val FROM v67_u1
		 INTERSECT
		 SELECT val FROM v67_u2`, 2)
	// Common: a, NULL = 2

	// ============================================================
	// === NULL IN EXCEPT ===
	// ============================================================

	// NE1: EXCEPT with NULLs
	checkRowCount("NE1 EXCEPT with NULLs",
		`SELECT val FROM v67_u2
		 EXCEPT
		 SELECT val FROM v67_u1`, 1)
	// u2 minus u1: b only = 1

	// ============================================================
	// === LIMIT/OFFSET EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_lo (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v67_lo VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v67_lo VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v67_lo VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO v67_lo VALUES (4, 40)")
	afExec(t, db, ctx, "INSERT INTO v67_lo VALUES (5, 50)")

	// LO1: LIMIT 0 should return empty
	checkRowCount("LO1 LIMIT 0",
		"SELECT * FROM v67_lo LIMIT 0", 0)

	// LO2: LIMIT 1
	checkRowCount("LO2 LIMIT 1",
		"SELECT * FROM v67_lo ORDER BY id LIMIT 1", 1)

	// LO3: LIMIT larger than table
	checkRowCount("LO3 LIMIT > count",
		"SELECT * FROM v67_lo LIMIT 100", 5)

	// LO4: OFFSET 0 (same as no offset)
	checkRowCount("LO4 OFFSET 0",
		"SELECT * FROM v67_lo LIMIT 5 OFFSET 0", 5)

	// LO5: OFFSET skips exactly N rows
	check("LO5 OFFSET 2 first row",
		"SELECT val FROM v67_lo ORDER BY id LIMIT 1 OFFSET 2", 30)

	// LO6: OFFSET beyond table size
	checkRowCount("LO6 OFFSET beyond",
		"SELECT * FROM v67_lo LIMIT 10 OFFSET 100", 0)

	// LO7: LIMIT 0 with OFFSET
	checkRowCount("LO7 LIMIT 0 OFFSET 2",
		"SELECT * FROM v67_lo LIMIT 0 OFFSET 2", 0)

	// LO8: Negative LIMIT (treated as no limit per SQLite behavior)
	checkRowCount("LO8 negative LIMIT",
		"SELECT * FROM v67_lo LIMIT -1", 5)

	// LO9: LIMIT with expression
	checkRowCount("LO9 LIMIT expression",
		"SELECT * FROM v67_lo LIMIT 2 + 1", 3)

	// ============================================================
	// === ORDER BY + DISTINCT INTERACTION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_od (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v67_od VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO v67_od VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO v67_od VALUES (3, 'a', 30)")
	afExec(t, db, ctx, "INSERT INTO v67_od VALUES (4, 'c', 40)")
	afExec(t, db, ctx, "INSERT INTO v67_od VALUES (5, 'b', 50)")

	// OD1: DISTINCT with ORDER BY
	checkRowCount("OD1 DISTINCT ORDER BY",
		"SELECT DISTINCT cat FROM v67_od ORDER BY cat", 3)

	// ============================================================
	// === AGGREGATE WITH NULL ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_agg (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v67_agg VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v67_agg VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v67_agg VALUES (3, 20)")
	afExec(t, db, ctx, "INSERT INTO v67_agg VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO v67_agg VALUES (5, 30)")

	// AG1: COUNT(*) includes NULLs
	check("AG1 COUNT(*) with NULLs", "SELECT COUNT(*) FROM v67_agg", 5)

	// AG2: COUNT(col) excludes NULLs
	check("AG2 COUNT(col) excludes NULL", "SELECT COUNT(val) FROM v67_agg", 3)

	// AG3: SUM ignores NULLs
	check("AG3 SUM ignores NULL", "SELECT SUM(val) FROM v67_agg", 60)

	// AG4: AVG ignores NULLs (60/3 = 20)
	check("AG4 AVG ignores NULL", "SELECT AVG(val) FROM v67_agg", 20)

	// AG5: MIN/MAX with NULLs
	check("AG5 MIN ignores NULL", "SELECT MIN(val) FROM v67_agg", 10)
	check("AG5 MAX ignores NULL", "SELECT MAX(val) FROM v67_agg", 30)

	// AG6: COUNT(DISTINCT col) with NULLs
	check("AG6 COUNT DISTINCT NULL", "SELECT COUNT(DISTINCT val) FROM v67_agg", 3)

	// AG7: SUM of empty table returns NULL
	afExec(t, db, ctx, `CREATE TABLE v67_empty (id INTEGER PRIMARY KEY, val INTEGER)`)
	checkNull("AG7 SUM empty", "SELECT SUM(val) FROM v67_empty")

	// AG8: AVG of empty table returns NULL
	checkNull("AG8 AVG empty", "SELECT AVG(val) FROM v67_empty")

	// AG9: MIN/MAX of empty table returns NULL
	checkNull("AG9 MIN empty", "SELECT MIN(val) FROM v67_empty")
	checkNull("AG9 MAX empty", "SELECT MAX(val) FROM v67_empty")

	// AG10: COUNT of empty table returns 0
	check("AG10 COUNT empty", "SELECT COUNT(*) FROM v67_empty", 0)

	// ============================================================
	// === GROUP BY WITH NULL ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_gb (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v67_gb VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO v67_gb VALUES (2, NULL, 20)")
	afExec(t, db, ctx, "INSERT INTO v67_gb VALUES (3, 'a', 30)")
	afExec(t, db, ctx, "INSERT INTO v67_gb VALUES (4, NULL, 40)")
	afExec(t, db, ctx, "INSERT INTO v67_gb VALUES (5, 'b', 50)")

	// GB1: GROUP BY with NULLs should group NULLs together
	checkRowCount("GB1 GROUP BY NULLs",
		"SELECT grp, SUM(val) FROM v67_gb GROUP BY grp", 3)
	// a=40, NULL=60, b=50 = 3 groups

	// GB2: HAVING with NULL group
	check("GB2 NULL group SUM",
		"SELECT SUM(val) FROM v67_gb WHERE grp IS NULL", 60)

	// ============================================================
	// === WHERE NULL COMPARISONS ===
	// ============================================================

	// WN1: = NULL should return no rows (NULL = NULL is unknown)
	checkRowCount("WN1 val = NULL",
		"SELECT * FROM v67_agg WHERE val = NULL", 0)

	// WN2: IS NULL should return NULL rows
	checkRowCount("WN2 IS NULL",
		"SELECT * FROM v67_agg WHERE val IS NULL", 2)

	// WN3: IS NOT NULL
	checkRowCount("WN3 IS NOT NULL",
		"SELECT * FROM v67_agg WHERE val IS NOT NULL", 3)

	// ============================================================
	// === COALESCE / NULLIF WITH NULL ===
	// ============================================================

	// CN1: COALESCE returns first non-null
	check("CN1 COALESCE", "SELECT COALESCE(NULL, NULL, 42)", 42)

	// CN2: COALESCE all nulls returns null
	checkNull("CN2 COALESCE all null", "SELECT COALESCE(NULL, NULL)")

	// CN3: NULLIF equal values returns NULL
	checkNull("CN3 NULLIF equal", "SELECT NULLIF(1, 1)")

	// CN4: NULLIF different values returns first
	check("CN4 NULLIF different", "SELECT NULLIF(1, 2)", 1)

	// ============================================================
	// === CASE WITH NULL ===
	// ============================================================

	// CW1: CASE WHEN with NULL
	check("CW1 CASE NULL",
		"SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END", "no")

	// CW2: CASE with NULL comparison
	check("CW2 CASE IS NULL",
		`SELECT CASE WHEN val IS NULL THEN 'null' ELSE 'not null' END FROM v67_agg WHERE id = 2`, "null")

	// ============================================================
	// === SUBQUERY EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_sub (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v67_sub VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v67_sub VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v67_sub VALUES (3, 30)")

	// SQ1: Scalar subquery returning single value
	check("SQ1 scalar subquery",
		"SELECT val FROM v67_sub WHERE val = (SELECT MAX(val) FROM v67_sub)", 30)

	// SQ2: IN subquery
	checkRowCount("SQ2 IN subquery",
		"SELECT * FROM v67_sub WHERE val IN (SELECT val FROM v67_sub WHERE val > 15)", 2)

	// SQ3: NOT IN subquery
	checkRowCount("SQ3 NOT IN subquery",
		"SELECT * FROM v67_sub WHERE val NOT IN (SELECT val FROM v67_sub WHERE val > 15)", 1)

	// SQ4: EXISTS subquery
	checkRowCount("SQ4 EXISTS subquery",
		"SELECT * FROM v67_sub WHERE EXISTS (SELECT 1 FROM v67_sub WHERE val > 25)", 3)

	// SQ5: NOT EXISTS with empty result
	checkRowCount("SQ5 NOT EXISTS empty",
		"SELECT * FROM v67_sub WHERE NOT EXISTS (SELECT 1 FROM v67_sub WHERE val > 100)", 3)

	// ============================================================
	// === INSERT OR REPLACE / INSERT OR IGNORE WITH NULLs ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_upsert (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v67_upsert VALUES (1, 'alice', 100)")

	// UP1: INSERT OR REPLACE overwrites
	checkNoError("UP1 INSERT OR REPLACE",
		"INSERT OR REPLACE INTO v67_upsert VALUES (1, 'bob', 200)")
	check("UP1 verify", "SELECT name FROM v67_upsert WHERE id = 1", "bob")
	check("UP1 verify val", "SELECT val FROM v67_upsert WHERE id = 1", 200)

	// UP2: INSERT OR IGNORE skips duplicate
	checkNoError("UP2 INSERT OR IGNORE",
		"INSERT OR IGNORE INTO v67_upsert VALUES (1, 'carol', 300)")
	check("UP2 verify unchanged", "SELECT name FROM v67_upsert WHERE id = 1", "bob")

	// UP3: INSERT OR IGNORE with non-duplicate succeeds
	checkNoError("UP3 INSERT OR IGNORE new",
		"INSERT OR IGNORE INTO v67_upsert VALUES (2, 'dave', 400)")
	check("UP3 verify", "SELECT name FROM v67_upsert WHERE id = 2", "dave")

	// ============================================================
	// === COMPLEX EXPRESSIONS ===
	// ============================================================

	// CE1: Arithmetic with mixed types
	check("CE1 int + float", "SELECT 10 + 2.5", "12.5")

	// CE2: String concatenation via ||
	check("CE2 concat", "SELECT 'hello' || ' ' || 'world'", "hello world")

	// CE3: Modulo
	check("CE3 modulo", "SELECT 17 % 5", 2)

	// CE4: Integer division
	check("CE4 division", "SELECT 10 / 3", "3.3333333333333335")

	// CE5: Nested arithmetic
	check("CE5 nested arith", "SELECT (2 + 3) * (4 - 1) + 1", 16)

	// ============================================================
	// === UNION WITH ORDER BY AND LIMIT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_ua (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_ua VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v67_ua VALUES (2, 'b')")

	afExec(t, db, ctx, `CREATE TABLE v67_ub (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_ub VALUES (1, 'c')")
	afExec(t, db, ctx, "INSERT INTO v67_ub VALUES (2, 'd')")

	// UO1: UNION with ORDER BY
	check("UO1 UNION ORDER BY",
		`SELECT name FROM v67_ua
		 UNION ALL
		 SELECT name FROM v67_ub
		 ORDER BY name
		 LIMIT 1`, "a")

	// UO2: UNION with LIMIT
	checkRowCount("UO2 UNION LIMIT",
		`SELECT name FROM v67_ua
		 UNION ALL
		 SELECT name FROM v67_ub
		 LIMIT 2`, 2)

	// ============================================================
	// === COMPLEX CTE PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_cte (id INTEGER PRIMARY KEY, val INTEGER, cat TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_cte VALUES (1, 10, 'a')")
	afExec(t, db, ctx, "INSERT INTO v67_cte VALUES (2, 20, 'b')")
	afExec(t, db, ctx, "INSERT INTO v67_cte VALUES (3, 30, 'a')")
	afExec(t, db, ctx, "INSERT INTO v67_cte VALUES (4, 40, 'b')")
	afExec(t, db, ctx, "INSERT INTO v67_cte VALUES (5, 50, 'a')")

	// CT1: CTE with aggregate
	check("CT1 CTE aggregate",
		`WITH totals AS (
			SELECT cat, SUM(val) as total FROM v67_cte GROUP BY cat
		)
		SELECT MAX(total) FROM totals`, 90)
	// a: 10+30+50=90, b: 20+40=60 → MAX=90

	// CT2: CTE referencing CTE
	check("CT2 chained CTE",
		`WITH step1 AS (
			SELECT cat, SUM(val) as total FROM v67_cte GROUP BY cat
		),
		step2 AS (
			SELECT SUM(total) as grand_total FROM step1
		)
		SELECT grand_total FROM step2`, 150)

	// CT3: CTE used multiple times
	check("CT3 CTE reused",
		`WITH data AS (
			SELECT * FROM v67_cte WHERE val > 15
		)
		SELECT (SELECT COUNT(*) FROM data) + (SELECT SUM(val) FROM data)`, 144)
	// COUNT=4 (20,30,40,50), SUM=140, total=144

	// ============================================================
	// === WINDOW FUNCTION EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_win (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v67_win VALUES (1, 'eng', 100)")
	afExec(t, db, ctx, "INSERT INTO v67_win VALUES (2, 'eng', 120)")
	afExec(t, db, ctx, "INSERT INTO v67_win VALUES (3, 'eng', 110)")
	afExec(t, db, ctx, "INSERT INTO v67_win VALUES (4, 'sales', 80)")
	afExec(t, db, ctx, "INSERT INTO v67_win VALUES (5, 'sales', 90)")

	// WF1: ROW_NUMBER
	check("WF1 ROW_NUMBER",
		"SELECT ROW_NUMBER() OVER (ORDER BY salary DESC) FROM v67_win WHERE id = 2", 1)

	// WF2: RANK with ties
	afExec(t, db, ctx, `CREATE TABLE v67_rank (id INTEGER PRIMARY KEY, score INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v67_rank VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO v67_rank VALUES (2, 90)")
	afExec(t, db, ctx, "INSERT INTO v67_rank VALUES (3, 100)")
	afExec(t, db, ctx, "INSERT INTO v67_rank VALUES (4, 80)")

	checkRowCount("WF2 RANK ties",
		"SELECT id, RANK() OVER (ORDER BY score DESC) as r FROM v67_rank", 4)

	// WF3: Window SUM partitioned - WHERE filters to single row so partition has 1 row
	check("WF3 partition SUM",
		`SELECT SUM(salary) OVER (PARTITION BY dept) FROM v67_win WHERE id = 1`, 100)

	// ============================================================
	// === UPDATE/DELETE EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_upd (id INTEGER PRIMARY KEY, val INTEGER, status TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_upd VALUES (1, 10, 'a')")
	afExec(t, db, ctx, "INSERT INTO v67_upd VALUES (2, 20, 'b')")
	afExec(t, db, ctx, "INSERT INTO v67_upd VALUES (3, 30, 'a')")
	afExec(t, db, ctx, "INSERT INTO v67_upd VALUES (4, 40, 'b')")

	// UD1: UPDATE with arithmetic expression
	checkNoError("UD1 UPDATE arithmetic",
		"UPDATE v67_upd SET val = val * 2 WHERE status = 'a'")
	check("UD1 verify", "SELECT SUM(val) FROM v67_upd WHERE status = 'a'", 80)

	// UD2: UPDATE with subquery in SET
	// After UD1: id=1 val=20, id=2 val=20, id=3 val=60, id=4 val=40 → MAX=60
	checkNoError("UD2 UPDATE subquery SET",
		"UPDATE v67_upd SET val = (SELECT MAX(val) FROM v67_upd) WHERE id = 1")
	check("UD2 verify", "SELECT val FROM v67_upd WHERE id = 1", 60)

	// UD3: DELETE with IN subquery
	// After UD2: id=1 val=60, id=2 val=20, id=3 val=60, id=4 val=40
	// val < 30 → only id=2 deleted → 3 remain
	checkNoError("UD3 DELETE IN subquery",
		"DELETE FROM v67_upd WHERE id IN (SELECT id FROM v67_upd WHERE val < 30)")
	check("UD3 count", "SELECT COUNT(*) FROM v67_upd", 3)

	// ============================================================
	// === BETWEEN OPERATOR ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_btwn (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v67_btwn VALUES (1, 5)")
	afExec(t, db, ctx, "INSERT INTO v67_btwn VALUES (2, 10)")
	afExec(t, db, ctx, "INSERT INTO v67_btwn VALUES (3, 15)")
	afExec(t, db, ctx, "INSERT INTO v67_btwn VALUES (4, 20)")
	afExec(t, db, ctx, "INSERT INTO v67_btwn VALUES (5, 25)")

	// BT1: BETWEEN inclusive
	checkRowCount("BT1 BETWEEN",
		"SELECT * FROM v67_btwn WHERE val BETWEEN 10 AND 20", 3)

	// BT2: NOT BETWEEN
	checkRowCount("BT2 NOT BETWEEN",
		"SELECT * FROM v67_btwn WHERE val NOT BETWEEN 10 AND 20", 2)

	// ============================================================
	// === IN LIST ===
	// ============================================================

	// IL1: IN with values
	checkRowCount("IL1 IN list",
		"SELECT * FROM v67_btwn WHERE val IN (5, 15, 25)", 3)

	// IL2: NOT IN with values
	checkRowCount("IL2 NOT IN list",
		"SELECT * FROM v67_btwn WHERE val NOT IN (5, 15, 25)", 2)

	// ============================================================
	// === MULTIPLE AGGREGATES IN ONE SELECT ===
	// ============================================================

	// MA1: All aggregates at once
	check("MA1 multi-agg COUNT",
		`SELECT COUNT(*) FROM v67_btwn`, 5)
	check("MA1 multi-agg SUM",
		"SELECT SUM(val) FROM v67_btwn", 75)
	check("MA1 multi-agg AVG",
		"SELECT AVG(val) FROM v67_btwn", 15)
	check("MA1 multi-agg MIN",
		"SELECT MIN(val) FROM v67_btwn", 5)
	check("MA1 multi-agg MAX",
		"SELECT MAX(val) FROM v67_btwn", 25)

	// ============================================================
	// === EXPRESSION IN ORDER BY ===
	// ============================================================

	// EO1: ORDER BY expression
	check("EO1 ORDER BY expr",
		"SELECT val FROM v67_btwn ORDER BY val * -1 LIMIT 1", 25)
	// val*-1: -5,-10,-15,-20,-25 → ascending = -25 first → val=25

	// ============================================================
	// === LIKE PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_like (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v67_like VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v67_like VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO v67_like VALUES (3, 'Carol')")
	afExec(t, db, ctx, "INSERT INTO v67_like VALUES (4, 'albert')")
	afExec(t, db, ctx, "INSERT INTO v67_like VALUES (5, 'Anna')")

	// LK1: LIKE prefix
	checkRowCount("LK1 LIKE prefix",
		"SELECT * FROM v67_like WHERE name LIKE 'A%'", 3)
	// Alice, albert, Anna (case-insensitive)

	// LK2: LIKE suffix
	checkRowCount("LK2 LIKE suffix",
		"SELECT * FROM v67_like WHERE name LIKE '%l'", 1)
	// Carol

	// LK3: LIKE contains
	checkRowCount("LK3 LIKE contains",
		"SELECT * FROM v67_like WHERE name LIKE '%o%'", 2)
	// Bob, Carol

	// LK4: LIKE single char wildcard
	checkRowCount("LK4 LIKE underscore",
		"SELECT * FROM v67_like WHERE name LIKE '_o%'", 1)
	// Bob (second char is 'o'; Carol's second char is 'a')

	// ============================================================
	// === INSERT INTO SELECT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v67_src (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v67_src VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO v67_src VALUES (2, 200)")

	afExec(t, db, ctx, `CREATE TABLE v67_dst (id INTEGER PRIMARY KEY, val INTEGER)`)

	// IS1: INSERT INTO SELECT
	checkNoError("IS1 INSERT INTO SELECT",
		"INSERT INTO v67_dst SELECT * FROM v67_src")
	check("IS1 verify count", "SELECT COUNT(*) FROM v67_dst", 2)
	check("IS1 verify val", "SELECT SUM(val) FROM v67_dst", 300)

	t.Logf("\n=== V67 NULL DEDUP EDGE CASES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
