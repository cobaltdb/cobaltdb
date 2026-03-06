package test

import (
	"fmt"
	"strings"
	"testing"
)

// TestV76DeepCoverage targets the least-covered code paths in the SQL engine:
// - Window functions: DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
// - SQL functions: REVERSE, REPEAT, LEFT, RIGHT, LPAD, RPAD, HEX, QUOTE, UNICODE, CHAR,
//   ROUND, FLOOR, CEIL, PRINTF, GLOB, IIF, ZEROBLOB, CONCAT_WS, GROUP_CONCAT
// - CAST edge cases
// - ALTER TABLE DROP COLUMN
// - Nested SAVEPOINT rollback
// - ORDER BY edge cases
func TestV76DeepCoverage(t *testing.T) {
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

	// ============================================================
	// === WINDOW FUNCTIONS: DENSE_RANK ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_scores (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_scores VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO v76_scores VALUES (2, 'Bob', 100)")
	afExec(t, db, ctx, "INSERT INTO v76_scores VALUES (3, 'Carol', 90)")
	afExec(t, db, ctx, "INSERT INTO v76_scores VALUES (4, 'Dave', 80)")
	afExec(t, db, ctx, "INSERT INTO v76_scores VALUES (5, 'Eve', 80)")
	afExec(t, db, ctx, "INSERT INTO v76_scores VALUES (6, 'Frank', 70)")

	// DENSE_RANK: no gaps (1,1,2,3,3,4 vs RANK's 1,1,3,4,4,6)
	check("DENSE_RANK: tied first",
		`WITH ranked AS (
			SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) as dr FROM v76_scores
		) SELECT dr FROM ranked WHERE name = 'Alice'`, 1)

	check("DENSE_RANK: tied first (Bob)",
		`WITH ranked AS (
			SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) as dr FROM v76_scores
		) SELECT dr FROM ranked WHERE name = 'Bob'`, 1)

	check("DENSE_RANK: after tie (no gap)",
		`WITH ranked AS (
			SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) as dr FROM v76_scores
		) SELECT dr FROM ranked WHERE name = 'Carol'`, 2)

	check("DENSE_RANK: second tie",
		`WITH ranked AS (
			SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) as dr FROM v76_scores
		) SELECT dr FROM ranked WHERE name = 'Dave'`, 3)

	check("DENSE_RANK: last",
		`WITH ranked AS (
			SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) as dr FROM v76_scores
		) SELECT dr FROM ranked WHERE name = 'Frank'`, 4)

	// Compare RANK vs DENSE_RANK
	check("RANK: after tie has gap",
		`WITH ranked AS (
			SELECT name, RANK() OVER (ORDER BY score DESC) as rk FROM v76_scores
		) SELECT rk FROM ranked WHERE name = 'Carol'`, 3) // gap: 1,1,3

	// ============================================================
	// === WINDOW FUNCTIONS: LAG / LEAD ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_seq (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_seq VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v76_seq VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v76_seq VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO v76_seq VALUES (4, 40)")
	afExec(t, db, ctx, "INSERT INTO v76_seq VALUES (5, 50)")

	// LAG: previous row value
	check("LAG: default offset 1",
		`WITH lagged AS (
			SELECT id, val, LAG(val) OVER (ORDER BY id) as prev FROM v76_seq
		) SELECT prev FROM lagged WHERE id = 3`, 20)

	// LAG: first row has no previous → NULL
	checkNull("LAG: first row is NULL",
		`WITH lagged AS (
			SELECT id, val, LAG(val) OVER (ORDER BY id) as prev FROM v76_seq
		) SELECT prev FROM lagged WHERE id = 1`)

	// LAG with offset 2
	check("LAG: offset 2",
		`WITH lagged AS (
			SELECT id, val, LAG(val, 2) OVER (ORDER BY id) as prev2 FROM v76_seq
		) SELECT prev2 FROM lagged WHERE id = 4`, 20)

	// LAG with default value
	check("LAG: with default",
		`WITH lagged AS (
			SELECT id, val, LAG(val, 1, -1) OVER (ORDER BY id) as prev FROM v76_seq
		) SELECT prev FROM lagged WHERE id = 1`, -1)

	// LEAD: next row value
	check("LEAD: default offset 1",
		`WITH led AS (
			SELECT id, val, LEAD(val) OVER (ORDER BY id) as nxt FROM v76_seq
		) SELECT nxt FROM led WHERE id = 3`, 40)

	// LEAD: last row has no next → NULL
	checkNull("LEAD: last row is NULL",
		`WITH led AS (
			SELECT id, val, LEAD(val) OVER (ORDER BY id) as nxt FROM v76_seq
		) SELECT nxt FROM led WHERE id = 5`)

	// LEAD with offset 2
	check("LEAD: offset 2",
		`WITH led AS (
			SELECT id, val, LEAD(val, 2) OVER (ORDER BY id) as nxt2 FROM v76_seq
		) SELECT nxt2 FROM led WHERE id = 2`, 40)

	// LEAD with default value
	check("LEAD: with default",
		`WITH led AS (
			SELECT id, val, LEAD(val, 1, 999) OVER (ORDER BY id) as nxt FROM v76_seq
		) SELECT nxt FROM led WHERE id = 5`, 999)

	// ============================================================
	// === WINDOW FUNCTIONS: FIRST_VALUE / LAST_VALUE / NTH_VALUE ===
	// ============================================================

	check("FIRST_VALUE: in partition",
		`WITH fv AS (
			SELECT id, val, FIRST_VALUE(val) OVER (ORDER BY id) as fst FROM v76_seq
		) SELECT fst FROM fv WHERE id = 3`, 10)

	check("LAST_VALUE: in partition",
		`WITH lv AS (
			SELECT id, val, LAST_VALUE(val) OVER (ORDER BY id) as lst FROM v76_seq
		) SELECT lst FROM lv WHERE id = 3`, 50)

	check("NTH_VALUE: 2nd value",
		`WITH nv AS (
			SELECT id, val, NTH_VALUE(val, 2) OVER (ORDER BY id) as nth FROM v76_seq
		) SELECT nth FROM nv WHERE id = 4`, 20)

	// NTH_VALUE out of range
	checkNull("NTH_VALUE: out of range",
		`WITH nv AS (
			SELECT id, val, NTH_VALUE(val, 10) OVER (ORDER BY id) as nth FROM v76_seq
		) SELECT nth FROM nv WHERE id = 1`)

	// ============================================================
	// === WINDOW FUNCTIONS: PARTITION BY with LAG/LEAD ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_part (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_part VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v76_part VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO v76_part VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO v76_part VALUES (4, 'B', 100)")
	afExec(t, db, ctx, "INSERT INTO v76_part VALUES (5, 'B', 200)")

	// LAG within partition boundary
	checkNull("LAG: partition boundary null",
		`WITH plag AS (
			SELECT id, grp, val, LAG(val) OVER (PARTITION BY grp ORDER BY id) as prev FROM v76_part
		) SELECT prev FROM plag WHERE id = 4`)

	check("LAG: within partition",
		`WITH plag AS (
			SELECT id, grp, val, LAG(val) OVER (PARTITION BY grp ORDER BY id) as prev FROM v76_part
		) SELECT prev FROM plag WHERE id = 5`, 100)

	// FIRST_VALUE with PARTITION BY
	check("FIRST_VALUE: partition B",
		`WITH pfv AS (
			SELECT id, grp, val, FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY id) as fst FROM v76_part
		) SELECT fst FROM pfv WHERE id = 5`, 100)

	// ============================================================
	// === STRING FUNCTIONS ===
	// ============================================================

	// REVERSE
	check("REVERSE: basic", "SELECT REVERSE('hello')", "olleh")
	check("REVERSE: empty", "SELECT REVERSE('')", "")
	checkNull("REVERSE: NULL", "SELECT REVERSE(NULL)")

	// REPEAT
	check("REPEAT: basic", "SELECT REPEAT('ab', 3)", "ababab")
	check("REPEAT: zero", "SELECT REPEAT('x', 0)", "")

	// LEFT / RIGHT
	check("LEFT: basic", "SELECT LEFT('hello', 3)", "hel")
	check("LEFT: exceed length", "SELECT LEFT('hi', 10)", "hi")
	check("RIGHT: basic", "SELECT RIGHT('hello', 3)", "llo")
	check("RIGHT: exceed length", "SELECT RIGHT('hi', 10)", "hi")

	// LPAD / RPAD
	check("LPAD: basic", "SELECT LPAD('42', 5, '0')", "00042")
	check("LPAD: truncate", "SELECT LPAD('hello', 3, 'x')", "hel")
	check("RPAD: basic", "SELECT RPAD('42', 5, '0')", "42000")
	check("RPAD: truncate", "SELECT RPAD('hello', 3, 'x')", "hel")

	// HEX
	check("HEX: number", "SELECT HEX(255)", "FF")
	check("HEX: string", "SELECT HEX('A')", "41")

	// QUOTE
	check("QUOTE: string", "SELECT QUOTE('hello')", "'hello'")
	check("QUOTE: NULL returns text NULL", "SELECT QUOTE(NULL)", "NULL")

	// UNICODE
	check("UNICODE: basic", "SELECT UNICODE('A')", 65)

	// CHAR
	check("CHAR: basic", "SELECT CHAR(65)", "A")

	// CONCAT_WS
	check("CONCAT_WS: basic", "SELECT CONCAT_WS('-', 'a', 'b', 'c')", "a-b-c")

	// ============================================================
	// === NUMERIC FUNCTIONS ===
	// ============================================================

	// ROUND
	check("ROUND: basic", "SELECT ROUND(3.14159, 2)", 3.14)
	check("ROUND: no decimals", "SELECT ROUND(3.7)", 4)

	// FLOOR
	check("FLOOR: positive", "SELECT FLOOR(3.7)", 3)
	check("FLOOR: negative", "SELECT FLOOR(-3.2)", -4)

	// CEIL / CEILING
	check("CEIL: positive", "SELECT CEIL(3.2)", 4)
	check("CEIL: negative", "SELECT CEIL(-3.7)", -3)

	// ABS
	check("ABS: negative", "SELECT ABS(-42)", 42)
	check("ABS: positive", "SELECT ABS(42)", 42)
	check("ABS: zero", "SELECT ABS(0)", 0)

	// ============================================================
	// === CONDITIONAL FUNCTIONS ===
	// ============================================================

	// IIF
	check("IIF: true", "SELECT IIF(1=1, 'yes', 'no')", "yes")
	check("IIF: false", "SELECT IIF(1=0, 'yes', 'no')", "no")
	check("IIF: null condition", "SELECT IIF(NULL, 'yes', 'no')", "no") // NULL = falsy

	// NULLIF
	check("NULLIF: different", "SELECT NULLIF(1, 2)", 1)
	checkNull("NULLIF: same", "SELECT NULLIF(1, 1)")

	// ============================================================
	// === PRINTF ===
	// ============================================================

	check("PRINTF: string", "SELECT PRINTF('%s world', 'hello')", "hello world")
	check("PRINTF: decimal", "SELECT PRINTF('%d items', 42)", "42 items")

	// ============================================================
	// === GLOB ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_glob (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v76_glob VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO v76_glob VALUES (2, 'help')")
	afExec(t, db, ctx, "INSERT INTO v76_glob VALUES (3, 'world')")

	checkRowCount("GLOB: wildcard",
		"SELECT * FROM v76_glob WHERE GLOB('hel*', name)", 2)
	checkRowCount("GLOB: question mark",
		"SELECT * FROM v76_glob WHERE GLOB('hel?', name)", 1) // "help" matches

	// ============================================================
	// === TYPEOF ===
	// ============================================================

	check("TYPEOF: integer", "SELECT TYPEOF(42)", "integer")
	check("TYPEOF: real", "SELECT TYPEOF(3.14)", "real")
	check("TYPEOF: text", "SELECT TYPEOF('hello')", "text")
	check("TYPEOF: null", "SELECT TYPEOF(NULL)", "null")
	check("TYPEOF: boolean true", "SELECT TYPEOF(1=1)", "integer")

	// ============================================================
	// === CAST EDGE CASES ===
	// ============================================================

	check("CAST: int to text", "SELECT CAST(42 AS TEXT)", "42")
	check("CAST: text to int", "SELECT CAST('123' AS INTEGER)", 123)
	check("CAST: float to int", "SELECT CAST(3.7 AS INTEGER)", 3)
	check("CAST: int to real", "SELECT CAST(42 AS REAL)", 42)
	checkNull("CAST: NULL to int", "SELECT CAST(NULL AS INTEGER)")
	check("CAST: bool to int", "SELECT CAST((1=1) AS INTEGER)", 1)
	check("CAST: text to real", "SELECT CAST('3.14' AS REAL)", 3.14)

	// ============================================================
	// === ALTER TABLE DROP COLUMN ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_drop (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, city TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v76_drop VALUES (1, 'Alice', 30, 'NYC')")
	afExec(t, db, ctx, "INSERT INTO v76_drop VALUES (2, 'Bob', 25, 'LA')")
	afExec(t, db, ctx, "INSERT INTO v76_drop VALUES (3, 'Carol', 35, 'Chicago')")

	// Drop a middle column
	checkNoError("DROP-COL: drop age", "ALTER TABLE v76_drop DROP COLUMN age")

	// Verify remaining data
	check("DROP-COL: name ok", "SELECT name FROM v76_drop WHERE id = 1", "Alice")
	check("DROP-COL: city ok", "SELECT city FROM v76_drop WHERE id = 1", "NYC")
	check("DROP-COL: count", "SELECT COUNT(*) FROM v76_drop", 3)

	// Insert after drop
	checkNoError("DROP-COL: insert after",
		"INSERT INTO v76_drop VALUES (4, 'Dave', 'Boston')")
	check("DROP-COL: verify insert", "SELECT city FROM v76_drop WHERE id = 4", "Boston")

	// Can't drop PK
	checkError("DROP-COL: pk error", "ALTER TABLE v76_drop DROP COLUMN id")

	// Can't drop non-existent column
	checkError("DROP-COL: nonexistent", "ALTER TABLE v76_drop DROP COLUMN age") // already dropped

	// Drop another column
	checkNoError("DROP-COL: drop city", "ALTER TABLE v76_drop DROP COLUMN city")
	check("DROP-COL: only name left", "SELECT name FROM v76_drop WHERE id = 2", "Bob")

	// ============================================================
	// === ALTER TABLE DROP COLUMN WITH INDEX ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_idx_drop (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_idx_drop VALUES (1, 'Alice', 95)")
	afExec(t, db, ctx, "CREATE INDEX idx_score ON v76_idx_drop(score)")

	// Drop indexed column → index should be cleaned up
	checkNoError("DROP-COL: indexed column", "ALTER TABLE v76_idx_drop DROP COLUMN score")
	check("DROP-COL: table still works", "SELECT name FROM v76_idx_drop WHERE id = 1", "Alice")

	// ============================================================
	// === ALTER TABLE DROP COLUMN IN TRANSACTION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_txn_drop (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v76_txn_drop VALUES (1, 'x', 'y', 'z')")

	checkNoError("DROP-COL TXN: begin", "BEGIN")
	checkNoError("DROP-COL TXN: drop b", "ALTER TABLE v76_txn_drop DROP COLUMN b")
	checkNoError("DROP-COL TXN: rollback", "ROLLBACK")

	// After rollback, column b should be restored
	check("DROP-COL TXN: b restored", "SELECT b FROM v76_txn_drop WHERE id = 1", "y")

	// ============================================================
	// === NESTED SAVEPOINTS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_nsp (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_nsp VALUES (1, 100)")

	// 3-level nested savepoints
	checkNoError("NSP: BEGIN", "BEGIN")
	checkNoError("NSP: update 1", "UPDATE v76_nsp SET val = 200 WHERE id = 1")
	checkNoError("NSP: SAVEPOINT s1", "SAVEPOINT s1")
	checkNoError("NSP: update 2", "UPDATE v76_nsp SET val = 300 WHERE id = 1")
	checkNoError("NSP: SAVEPOINT s2", "SAVEPOINT s2")
	checkNoError("NSP: update 3", "UPDATE v76_nsp SET val = 400 WHERE id = 1")

	// Verify innermost value
	check("NSP: innermost val", "SELECT val FROM v76_nsp WHERE id = 1", 400)

	// Rollback to s2 → should be 300
	checkNoError("NSP: ROLLBACK TO s2", "ROLLBACK TO s2")
	check("NSP: after rollback s2", "SELECT val FROM v76_nsp WHERE id = 1", 300)

	// Rollback to s1 → should be 200
	checkNoError("NSP: ROLLBACK TO s1", "ROLLBACK TO s1")
	check("NSP: after rollback s1", "SELECT val FROM v76_nsp WHERE id = 1", 200)

	// Commit → should keep 200
	checkNoError("NSP: COMMIT", "COMMIT")
	check("NSP: after commit", "SELECT val FROM v76_nsp WHERE id = 1", 200)

	// ============================================================
	// === RELEASE SAVEPOINT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_release (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_release VALUES (1, 10)")

	checkNoError("REL: BEGIN", "BEGIN")
	checkNoError("REL: update", "UPDATE v76_release SET val = 20 WHERE id = 1")
	checkNoError("REL: SAVEPOINT sp", "SAVEPOINT sp_rel")
	checkNoError("REL: update 2", "UPDATE v76_release SET val = 30 WHERE id = 1")
	checkNoError("REL: RELEASE", "RELEASE SAVEPOINT sp_rel")
	// After release, changes from savepoint are merged into parent
	checkNoError("REL: COMMIT", "COMMIT")
	check("REL: after commit", "SELECT val FROM v76_release WHERE id = 1", 30)

	// ============================================================
	// === ORDER BY with multiple expressions ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_ord (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v76_ord VALUES (1, 10, 'z')")
	afExec(t, db, ctx, "INSERT INTO v76_ord VALUES (2, 20, 'a')")
	afExec(t, db, ctx, "INSERT INTO v76_ord VALUES (3, 10, 'a')")
	afExec(t, db, ctx, "INSERT INTO v76_ord VALUES (4, 20, 'z')")

	// Multi-column ORDER BY: a ASC, b DESC
	check("ORDER: multi-col first",
		"SELECT id FROM v76_ord ORDER BY a ASC, b DESC LIMIT 1", 1) // a=10, b='z'

	// ORDER BY with expression
	check("ORDER: expr",
		"SELECT id FROM v76_ord ORDER BY a * -1 LIMIT 1", 2) // most negative = 20*-1 = -20

	// ORDER BY with CASE
	check("ORDER: CASE",
		`SELECT id FROM v76_ord ORDER BY CASE WHEN b = 'z' THEN 0 ELSE 1 END, id LIMIT 1`, 1)

	// ============================================================
	// === GROUP_CONCAT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_gc (id INTEGER PRIMARY KEY, grp TEXT, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v76_gc VALUES (1, 'A', 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v76_gc VALUES (2, 'A', 'Adam')")
	afExec(t, db, ctx, "INSERT INTO v76_gc VALUES (3, 'B', 'Bob')")

	// GROUP_CONCAT with GROUP BY
	checkRowCount("GROUP_CONCAT: groups",
		"SELECT grp, GROUP_CONCAT(name) FROM v76_gc GROUP BY grp", 2)

	// ============================================================
	// === WINDOW: Running aggregates with PARTITION BY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_wrun (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_wrun VALUES (1, 'eng', 100)")
	afExec(t, db, ctx, "INSERT INTO v76_wrun VALUES (2, 'eng', 200)")
	afExec(t, db, ctx, "INSERT INTO v76_wrun VALUES (3, 'eng', 300)")
	afExec(t, db, ctx, "INSERT INTO v76_wrun VALUES (4, 'hr', 150)")
	afExec(t, db, ctx, "INSERT INTO v76_wrun VALUES (5, 'hr', 250)")

	// Running SUM partitioned
	check("WRUN: running SUM eng row 2",
		`WITH rs AS (
			SELECT id, dept, salary, SUM(salary) OVER (PARTITION BY dept ORDER BY id) as rsum
			FROM v76_wrun
		) SELECT rsum FROM rs WHERE id = 2`, 300) // 100+200

	check("WRUN: running SUM hr row 5",
		`WITH rs AS (
			SELECT id, dept, salary, SUM(salary) OVER (PARTITION BY dept ORDER BY id) as rsum
			FROM v76_wrun
		) SELECT rsum FROM rs WHERE id = 5`, 400) // 150+250

	// Running COUNT partitioned
	check("WRUN: running COUNT",
		`WITH rc AS (
			SELECT id, dept, COUNT(*) OVER (PARTITION BY dept ORDER BY id) as rcnt
			FROM v76_wrun
		) SELECT rcnt FROM rc WHERE id = 2`, 2)

	// Running AVG
	check("WRUN: running AVG",
		`WITH ra AS (
			SELECT id, dept, AVG(salary) OVER (PARTITION BY dept ORDER BY id) as ravg
			FROM v76_wrun
		) SELECT ravg FROM ra WHERE id = 2`, 150) // (100+200)/2

	// Running MIN
	check("WRUN: running MIN",
		`WITH rm AS (
			SELECT id, dept, MIN(salary) OVER (PARTITION BY dept ORDER BY id) as rmin
			FROM v76_wrun
		) SELECT rmin FROM rm WHERE id = 3`, 100)

	// Running MAX
	check("WRUN: running MAX",
		`WITH rx AS (
			SELECT id, dept, MAX(salary) OVER (PARTITION BY dept ORDER BY id) as rmax
			FROM v76_wrun
		) SELECT rmax FROM rx WHERE id = 2`, 200)

	// ============================================================
	// === COMPLEX WINDOW COMBINATIONS ===
	// ============================================================

	// Multiple window functions in same query
	check("MULTI-WIN: ROW_NUMBER + RANK",
		`WITH mw AS (
			SELECT name, score,
				ROW_NUMBER() OVER (ORDER BY score DESC) as rn,
				RANK() OVER (ORDER BY score DESC) as rk,
				DENSE_RANK() OVER (ORDER BY score DESC) as dr
			FROM v76_scores
		) SELECT rn FROM mw WHERE name = 'Carol'`, 3)

	// ============================================================
	// === HAVING without SELECT aggregate (hidden aggregate) ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_hav (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_hav VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO v76_hav VALUES (2, 'a', 20)")
	afExec(t, db, ctx, "INSERT INTO v76_hav VALUES (3, 'b', 5)")
	afExec(t, db, ctx, "INSERT INTO v76_hav VALUES (4, 'b', 15)")
	afExec(t, db, ctx, "INSERT INTO v76_hav VALUES (5, 'c', 100)")

	// HAVING with aggregate not in SELECT
	checkRowCount("HAVING: hidden agg",
		"SELECT cat FROM v76_hav GROUP BY cat HAVING SUM(val) > 25", 2) // a=30, c=100 (b=20 excluded)

	// HAVING with COUNT
	checkRowCount("HAVING: COUNT",
		"SELECT cat FROM v76_hav GROUP BY cat HAVING COUNT(*) > 1", 2) // a, b

	// HAVING with AVG
	checkRowCount("HAVING: AVG",
		"SELECT cat FROM v76_hav GROUP BY cat HAVING AVG(val) > 20", 1) // c=100

	// ============================================================
	// === ALTER TABLE RENAME ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_oldname (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_oldname VALUES (1, 42)")

	checkNoError("RENAME TABLE", "ALTER TABLE v76_oldname RENAME TO v76_newname")
	check("RENAME: query new name", "SELECT val FROM v76_newname WHERE id = 1", 42)
	checkError("RENAME: old name gone", "SELECT * FROM v76_oldname")

	// ============================================================
	// === ALTER TABLE RENAME COLUMN ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_rencol (id INTEGER PRIMARY KEY, old_name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v76_rencol VALUES (1, 'hello')")

	checkNoError("RENAME COL", "ALTER TABLE v76_rencol RENAME COLUMN old_name TO new_name")
	check("RENAME COL: query", "SELECT new_name FROM v76_rencol WHERE id = 1", "hello")

	// ============================================================
	// === DROP TABLE IF EXISTS ===
	// ============================================================

	checkNoError("DROP IF EXISTS: nonexistent", "DROP TABLE IF EXISTS v76_no_such_table")
	afExec(t, db, ctx, `CREATE TABLE v76_todrop (id INTEGER PRIMARY KEY)`)
	checkNoError("DROP IF EXISTS: exists", "DROP TABLE IF EXISTS v76_todrop")
	checkError("DROP: already dropped", "SELECT * FROM v76_todrop")

	// ============================================================
	// === CREATE TABLE IF NOT EXISTS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_ifne (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_ifne VALUES (1, 100)")
	// Should not error and not overwrite
	checkNoError("IF NOT EXISTS", "CREATE TABLE IF NOT EXISTS v76_ifne (id INTEGER PRIMARY KEY, val TEXT)")
	check("IF NOT EXISTS: data preserved", "SELECT val FROM v76_ifne WHERE id = 1", 100)

	// ============================================================
	// === INSERT INTO ... SELECT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_src (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_src VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v76_src VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v76_src VALUES (3, 30)")

	afExec(t, db, ctx, `CREATE TABLE v76_dst (id INTEGER PRIMARY KEY, val INTEGER)`)
	checkNoError("INSERT SELECT",
		"INSERT INTO v76_dst SELECT * FROM v76_src WHERE val >= 20")
	check("INSERT SELECT: count", "SELECT COUNT(*) FROM v76_dst", 2)
	check("INSERT SELECT: val", "SELECT val FROM v76_dst WHERE id = 3", 30)

	// ============================================================
	// === COMPLEX WHERE CLAUSES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_where (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_where VALUES (1, 10, 'x', 100)")
	afExec(t, db, ctx, "INSERT INTO v76_where VALUES (2, 20, 'y', 200)")
	afExec(t, db, ctx, "INSERT INTO v76_where VALUES (3, 30, 'x', 300)")
	afExec(t, db, ctx, "INSERT INTO v76_where VALUES (4, 40, 'y', 400)")
	afExec(t, db, ctx, "INSERT INTO v76_where VALUES (5, 50, 'x', 500)")

	// Complex boolean with NOT, IN, BETWEEN, LIKE
	checkRowCount("WHERE: NOT IN",
		"SELECT * FROM v76_where WHERE a NOT IN (10, 30, 50)", 2)

	checkRowCount("WHERE: BETWEEN AND",
		"SELECT * FROM v76_where WHERE a BETWEEN 20 AND 40 AND b = 'y'", 2) // 20,40

	checkRowCount("WHERE: OR complex",
		"SELECT * FROM v76_where WHERE (a < 20 AND b = 'x') OR (a > 40 AND b = 'x')", 2) // 1,5

	// ============================================================
	// === SUBQUERY IN WHERE (EXISTS, NOT EXISTS) ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_parent (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v76_parent VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v76_parent VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO v76_parent VALUES (3, 'Carol')")

	afExec(t, db, ctx, `CREATE TABLE v76_child (id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v76_child VALUES (1, 1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v76_child VALUES (2, 1, 'b')")
	afExec(t, db, ctx, "INSERT INTO v76_child VALUES (3, 3, 'c')")

	// EXISTS
	checkRowCount("EXISTS: with children",
		`SELECT * FROM v76_parent p WHERE EXISTS (
			SELECT 1 FROM v76_child c WHERE c.parent_id = p.id
		)`, 2) // Alice, Carol

	// NOT EXISTS
	checkRowCount("NOT EXISTS: without children",
		`SELECT * FROM v76_parent p WHERE NOT EXISTS (
			SELECT 1 FROM v76_child c WHERE c.parent_id = p.id
		)`, 1) // Bob

	// ============================================================
	// === MULTI-TABLE UNION with ORDER BY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_ua (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_ua VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v76_ua VALUES (2, 20)")

	afExec(t, db, ctx, `CREATE TABLE v76_ub (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_ub VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO v76_ub VALUES (4, 40)")

	check("UNION ORDER BY: desc first",
		"SELECT val FROM v76_ua UNION ALL SELECT val FROM v76_ub ORDER BY val DESC LIMIT 1", 40)

	checkRowCount("UNION: dedup",
		"SELECT val FROM v76_ua UNION SELECT val FROM v76_ua", 2) // 10, 20 (deduped)

	// ============================================================
	// === RECURSIVE CTE ===
	// ============================================================

	check("RECURSIVE CTE: count to 5",
		`WITH RECURSIVE cnt(x) AS (
			SELECT 1
			UNION ALL
			SELECT x + 1 FROM cnt WHERE x < 5
		) SELECT MAX(x) FROM cnt`, 5)

	check("RECURSIVE CTE: sum 1..10",
		`WITH RECURSIVE nums(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM nums WHERE n < 10
		) SELECT SUM(n) FROM nums`, 55)

	// ============================================================
	// === TRIGGERS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_log (id INTEGER PRIMARY KEY, action TEXT, val INTEGER)`)
	afExec(t, db, ctx, `CREATE TABLE v76_trig (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, `CREATE TRIGGER v76_trig_insert AFTER INSERT ON v76_trig
		BEGIN INSERT INTO v76_log VALUES (NEW.id, 'insert', NEW.val); END`)

	checkNoError("TRIGGER: insert", "INSERT INTO v76_trig VALUES (1, 42)")
	check("TRIGGER: log created", "SELECT action FROM v76_log WHERE id = 1", "insert")
	check("TRIGGER: log val", "SELECT val FROM v76_log WHERE id = 1", 42)

	// Drop trigger
	checkNoError("TRIGGER: drop", "DROP TRIGGER v76_trig_insert")
	checkNoError("TRIGGER: insert after drop", "INSERT INTO v76_trig VALUES (2, 99)")
	check("TRIGGER: no new log", "SELECT COUNT(*) FROM v76_log", 1) // only 1 log entry

	// ============================================================
	// === VIEW ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_view_base (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v76_view_base VALUES (1, 'A', 90)")
	afExec(t, db, ctx, "INSERT INTO v76_view_base VALUES (2, 'B', 80)")
	afExec(t, db, ctx, "INSERT INTO v76_view_base VALUES (3, 'C', 70)")

	checkNoError("VIEW: create", "CREATE VIEW v76_high_score AS SELECT * FROM v76_view_base WHERE score >= 80")
	checkRowCount("VIEW: query", "SELECT * FROM v76_high_score", 2)
	check("VIEW: first row", "SELECT name FROM v76_high_score ORDER BY score DESC LIMIT 1", "A")

	// Drop view
	checkNoError("VIEW: drop", "DROP VIEW v76_high_score")

	// ============================================================
	// === COMPLEX STRING OPERATIONS IN TABLE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v76_str (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v76_str VALUES (1, 'hello world')")
	afExec(t, db, ctx, "INSERT INTO v76_str VALUES (2, '  spaces  ')")

	check("STR: UPPER", "SELECT UPPER(name) FROM v76_str WHERE id = 1", "HELLO WORLD")
	check("STR: LOWER", "SELECT LOWER(name) FROM v76_str WHERE id = 1", "hello world")
	check("STR: LENGTH", "SELECT LENGTH(name) FROM v76_str WHERE id = 1", 11)
	check("STR: TRIM", "SELECT TRIM(name) FROM v76_str WHERE id = 2", "spaces")
	check("STR: LTRIM", "SELECT LTRIM(name) FROM v76_str WHERE id = 2", "spaces  ")
	check("STR: RTRIM", "SELECT RTRIM(name) FROM v76_str WHERE id = 2", "  spaces")
	check("STR: SUBSTR", "SELECT SUBSTR(name, 1, 5) FROM v76_str WHERE id = 1", "hello")
	check("STR: REPLACE", "SELECT REPLACE(name, 'world', 'earth') FROM v76_str WHERE id = 1", "hello earth")
	check("STR: INSTR", "SELECT INSTR(name, 'world') FROM v76_str WHERE id = 1", 7)

	// ============================================================
	// === TOTAL COUNT ===
	// ============================================================

	t.Logf("\n=== V76 DEEP COVERAGE: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
	_ = strings.Repeat // keep import
}
