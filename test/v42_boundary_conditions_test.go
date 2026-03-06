package test

import (
	"fmt"
	"testing"
)

// TestV42BoundaryConditions exercises the boundary and edge-value conditions
// that most commonly harbour off-by-one errors, wrong NULL semantics, and
// incorrect aggregate behaviour.  Every expected value is derived from first
// principles so that the test documents WHY the result is what it is.
//
// Ten sections are covered:
//
//  1. Single-row table operations       (tests SR1-SR8)
//  2. Two-row table edge cases          (tests TR1-TR8)
//  3. LIMIT / OFFSET edge cases         (tests LM1-LM10)
//  4. WHERE with boundary values        (tests WB1-WB10)
//  5. GROUP BY boundary cases           (tests GB1-GB10)
//  6. JOIN boundary cases               (tests JB1-JB10)
//  7. UPDATE / DELETE boundary cases    (tests UD1-UD10)
//  8. Expression boundary cases         (tests EX1-EX10)
//  9. Recursive CTE boundaries          (tests RC1-RC5)
// 10. Transaction boundaries            (tests TX1-TX5)
//
// All table names carry the v42_ prefix to prevent collisions.
//
// Engine notes (consistent with observations in earlier test suites):
//   - Integer division yields float64  (e.g., 7/2 = 3.5, not 3).
//   - AVG of integers yields float64   (e.g., AVG(3,5) = 4 stored as float64).
//   - NULL renders as "<nil>" when formatted via fmt.Sprintf("%v", ...).
//   - LIKE is case-insensitive.
//   - NULLs sort LAST in ASC, FIRST in DESC order.
//   - CASE with no matching WHEN and no ELSE evaluates to NULL.
func TestV42BoundaryConditions(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	// check verifies that the first column of the first returned row equals expected.
	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned (sql: %s)", desc, sql)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		exp := fmt.Sprintf("%v", expected)
		if got != exp {
			t.Errorf("[FAIL] %s: got %q, expected %q", desc, got, exp)
			return
		}
		pass++
	}

	// checkRowCount verifies that the query returns exactly expected rows.
	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: expected %d rows, got %d (sql: %s)", desc, expected, len(rows), sql)
			return
		}
		pass++
	}

	// checkNoError verifies that the statement executes without error.
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

	// checkError verifies that the statement returns an error (failure path).
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

	_ = checkError // suppress unused-variable error when section is absent

	// ============================================================
	// SECTION 1: SINGLE-ROW TABLE OPERATIONS
	// ============================================================
	//
	// Schema
	// ------
	//   v42_one (id INTEGER PK, val INTEGER, label TEXT)
	//
	// Initial data:  (1, 42, 'only')
	//
	// For a table with a single row every aggregate (SUM, AVG, COUNT, MIN, MAX)
	// must equal that row's value.  Off-by-one errors in iteration, boundary
	// checks in the buffer pool, and special-casing of empty results are all
	// triggered here.

	afExec(t, db, ctx, `CREATE TABLE v42_one (
		id    INTEGER PRIMARY KEY,
		val   INTEGER,
		label TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v42_one VALUES (1, 42, 'only')")

	// SR1: SELECT on a single-row table returns exactly one row.
	checkRowCount("SR1 SELECT *: single-row table returns 1 row",
		"SELECT * FROM v42_one", 1)

	// SR2: All five aggregates equal the single row's value (42).
	// SUM(42)=42, AVG(42)=42, COUNT(*)=1, MIN(42)=42, MAX(42)=42.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT SUM(val), AVG(val), COUNT(*), MIN(val), MAX(val) FROM v42_one")
		if len(rows) != 1 || len(rows[0]) != 5 {
			t.Errorf("[FAIL] SR2 aggregates on 1 row: wrong shape, got %d rows", len(rows))
		} else {
			s := fmt.Sprintf("%v", rows[0][0])
			a := fmt.Sprintf("%v", rows[0][1])
			c := fmt.Sprintf("%v", rows[0][2])
			mn := fmt.Sprintf("%v", rows[0][3])
			mx := fmt.Sprintf("%v", rows[0][4])
			if s != "42" || a != "42" || c != "1" || mn != "42" || mx != "42" {
				t.Errorf("[FAIL] SR2 aggregates on 1 row: SUM=%s AVG=%s COUNT=%s MIN=%s MAX=%s",
					s, a, c, mn, mx)
			} else {
				pass++
			}
		}
	}

	// SR3: UPDATE on a single-row table must affect exactly that row.
	checkNoError("SR3 UPDATE only row: val = 99",
		"UPDATE v42_one SET val = 99 WHERE id = 1")
	check("SR3a after UPDATE: val is now 99",
		"SELECT val FROM v42_one WHERE id = 1", 99)

	// SR4: After UPDATE, COUNT(*) is still 1 (no phantom rows created).
	check("SR4 COUNT(*) after UPDATE still 1",
		"SELECT COUNT(*) FROM v42_one", 1)

	// SR5: DELETE the only row yields an empty table (0 rows).
	checkNoError("SR5 DELETE only row",
		"DELETE FROM v42_one WHERE id = 1")
	checkRowCount("SR5a table is empty after DELETE",
		"SELECT * FROM v42_one", 0)

	// SR6: Aggregates on an empty table return NULL (or 0 for COUNT).
	// COUNT(*) on empty = 0.  SUM/AVG/MIN/MAX on empty = NULL (<nil>).
	check("SR6 COUNT(*) on empty table = 0",
		"SELECT COUNT(*) FROM v42_one", 0)
	check("SR6a SUM on empty table = NULL",
		"SELECT SUM(val) FROM v42_one", "<nil>")

	// SR7: Re-insert after DELETE — table must behave normally again.
	checkNoError("SR7 re-insert after DELETE",
		"INSERT INTO v42_one VALUES (1, 7, 'reborn')")
	check("SR7a COUNT(*) after re-insert = 1",
		"SELECT COUNT(*) FROM v42_one", 1)
	check("SR7b value is correct after re-insert",
		"SELECT val FROM v42_one WHERE id = 1", 7)

	// SR8: DELETE with WHERE that matches 0 rows does nothing (table stays at 1).
	checkNoError("SR8 DELETE WHERE no-match: 0 rows affected",
		"DELETE FROM v42_one WHERE id = 999")
	checkRowCount("SR8a table still has 1 row after no-op DELETE",
		"SELECT * FROM v42_one", 1)

	// ============================================================
	// SECTION 2: TWO-ROW TABLE EDGE CASES
	// ============================================================
	//
	// Schema
	// ------
	//   v42_two (id INTEGER PK, val INTEGER)
	//
	// Data: (1, 10), (2, 20)
	//
	// With only two rows, MIN and MAX have exactly one candidate each,
	// ORDER BY simply swaps the two rows, and AVG is their midpoint.

	afExec(t, db, ctx, `CREATE TABLE v42_two (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v42_two VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v42_two VALUES (2, 20)")

	// TR1: MIN is the smaller value (10).
	check("TR1 MIN on 2 rows = 10",
		"SELECT MIN(val) FROM v42_two", 10)

	// TR2: MAX is the larger value (20).
	check("TR2 MAX on 2 rows = 20",
		"SELECT MAX(val) FROM v42_two", 20)

	// TR3: AVG of (10, 20) = 15.  Engine returns float64 for AVG.
	check("TR3 AVG on 2 rows = 15",
		"SELECT AVG(val) FROM v42_two", 15)

	// TR4: ORDER BY ASC — first row must be the smaller value (10).
	check("TR4 ORDER BY val ASC: first row = 10",
		"SELECT val FROM v42_two ORDER BY val ASC", 10)

	// TR5: ORDER BY DESC — first row must be the larger value (20).
	check("TR5 ORDER BY val DESC: first row = 20",
		"SELECT val FROM v42_two ORDER BY val DESC", 20)

	// TR6: SUM of 2 rows = 30.
	check("TR6 SUM on 2 rows = 30",
		"SELECT SUM(val) FROM v42_two", 30)

	// TR7: DELETE one of the two rows and verify only one remains.
	checkNoError("TR7 DELETE one of two rows (id=2)",
		"DELETE FROM v42_two WHERE id = 2")
	checkRowCount("TR7a one row remains after DELETE",
		"SELECT * FROM v42_two", 1)
	// Aggregates now reflect a single-row table again.
	check("TR7b MIN = MAX = SUM = remaining value (10)",
		"SELECT MIN(val) FROM v42_two", 10)
	check("TR7c MAX equals remaining value (10)",
		"SELECT MAX(val) FROM v42_two", 10)

	// TR8: Re-insert the deleted row to restore two-row state, verify COUNT.
	checkNoError("TR8 re-insert to restore 2-row state",
		"INSERT INTO v42_two VALUES (2, 20)")
	check("TR8a COUNT(*) = 2 after re-insert",
		"SELECT COUNT(*) FROM v42_two", 2)

	// ============================================================
	// SECTION 3: LIMIT / OFFSET EDGE CASES
	// ============================================================
	//
	// Schema
	// ------
	//   v42_lim (id INTEGER PK)
	//
	// Data: 5 rows with id = 1..5
	//
	// LIMIT and OFFSET interact with the row iterator in ways that trigger
	// fence-post errors: LIMIT 0, LIMIT > count, OFFSET = count, etc.

	afExec(t, db, ctx, `CREATE TABLE v42_lim (id INTEGER PRIMARY KEY)`)
	afExec(t, db, ctx, "INSERT INTO v42_lim VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO v42_lim VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO v42_lim VALUES (3)")
	afExec(t, db, ctx, "INSERT INTO v42_lim VALUES (4)")
	afExec(t, db, ctx, "INSERT INTO v42_lim VALUES (5)")

	// LM1: LIMIT 0 — no rows returned regardless of table size.
	checkRowCount("LM1 LIMIT 0: 0 rows returned",
		"SELECT id FROM v42_lim LIMIT 0", 0)

	// LM2: LIMIT 1 on a multi-row table — exactly 1 row.
	checkRowCount("LM2 LIMIT 1 on 5-row table: 1 row",
		"SELECT id FROM v42_lim ORDER BY id LIMIT 1", 1)

	// LM3: LIMIT greater than table size — all rows returned (no error, no truncation).
	// 5 rows in table, LIMIT 100 => 5 rows.
	checkRowCount("LM3 LIMIT 100 on 5-row table: all 5 rows",
		"SELECT id FROM v42_lim LIMIT 100", 5)

	// LM4: LIMIT 1 OFFSET 0 — first row.
	check("LM4 LIMIT 1 OFFSET 0: first row (id=1)",
		"SELECT id FROM v42_lim ORDER BY id LIMIT 1 OFFSET 0", 1)

	// LM5: LIMIT 1 OFFSET 4 — last row (5th row with 0-based offset).
	check("LM5 LIMIT 1 OFFSET 4: last row (id=5)",
		"SELECT id FROM v42_lim ORDER BY id LIMIT 1 OFFSET 4", 5)

	// LM6: OFFSET equal to table size — 0 rows (past the end).
	// 5 rows, OFFSET 5 => skip all 5 => 0 rows remain.
	checkRowCount("LM6 OFFSET = table size (5): 0 rows",
		"SELECT id FROM v42_lim LIMIT 100 OFFSET 5", 0)

	// LM7: OFFSET greater than table size — 0 rows.
	checkRowCount("LM7 OFFSET > table size (99): 0 rows",
		"SELECT id FROM v42_lim LIMIT 100 OFFSET 99", 0)

	// LM8: LIMIT 2 OFFSET 2 — two middle rows (id=3, id=4).
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT id FROM v42_lim ORDER BY id LIMIT 2 OFFSET 2")
		if len(rows) != 2 {
			t.Errorf("[FAIL] LM8 LIMIT 2 OFFSET 2: expected 2 rows, got %d", len(rows))
		} else {
			r0 := fmt.Sprintf("%v", rows[0][0])
			r1 := fmt.Sprintf("%v", rows[1][0])
			if r0 != "3" || r1 != "4" {
				t.Errorf("[FAIL] LM8 LIMIT 2 OFFSET 2: expected id=3,4 got %s,%s", r0, r1)
			} else {
				pass++
			}
		}
	}

	// LM9: LIMIT 1 OFFSET 1 — second row (id=2).
	check("LM9 LIMIT 1 OFFSET 1: second row (id=2)",
		"SELECT id FROM v42_lim ORDER BY id LIMIT 1 OFFSET 1", 2)

	// LM10: COUNT(*) is never affected by LIMIT/OFFSET — always counts all rows.
	// LIMIT and OFFSET apply to the result set, not to what COUNT sees.
	check("LM10 COUNT(*) ignores LIMIT: still 5",
		"SELECT COUNT(*) FROM v42_lim", 5)

	// ============================================================
	// SECTION 4: WHERE WITH BOUNDARY VALUES
	// ============================================================
	//
	// Schema
	// ------
	//   v42_wbnd (id INTEGER PK, val INTEGER, tag TEXT)
	//
	// Data (7 rows):
	//   (1,  0,   'zero')
	//   (2,  1,   'one')
	//   (3,  100, 'hundred')
	//   (4, -1,   'neg_one')
	//   (5, -100, 'neg_hundred')
	//   (6,  50,  'fifty')
	//   (7,  0,   '')        -- second zero, empty-string tag

	afExec(t, db, ctx, `CREATE TABLE v42_wbnd (
		id  INTEGER PRIMARY KEY,
		val INTEGER,
		tag TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v42_wbnd VALUES (1,    0,  'zero')")
	afExec(t, db, ctx, "INSERT INTO v42_wbnd VALUES (2,    1,  'one')")
	afExec(t, db, ctx, "INSERT INTO v42_wbnd VALUES (3,  100,  'hundred')")
	afExec(t, db, ctx, "INSERT INTO v42_wbnd VALUES (4,   -1,  'neg_one')")
	afExec(t, db, ctx, "INSERT INTO v42_wbnd VALUES (5, -100,  'neg_hundred')")
	afExec(t, db, ctx, "INSERT INTO v42_wbnd VALUES (6,   50,  'fifty')")
	afExec(t, db, ctx, "INSERT INTO v42_wbnd VALUES (7,    0,  '')")

	// WB1: WHERE col = MIN_VALUE — only the minimum (-100) row matches.
	checkRowCount("WB1 WHERE val = MIN_VALUE (-100): 1 row",
		"SELECT id FROM v42_wbnd WHERE val = -100", 1)
	check("WB1a WHERE val = MIN_VALUE: correct row (id=5)",
		"SELECT id FROM v42_wbnd WHERE val = -100", 5)

	// WB2: WHERE col = MAX_VALUE — only the maximum (100) row matches.
	checkRowCount("WB2 WHERE val = MAX_VALUE (100): 1 row",
		"SELECT id FROM v42_wbnd WHERE val = 100", 1)
	check("WB2a WHERE val = MAX_VALUE: correct row (id=3)",
		"SELECT id FROM v42_wbnd WHERE val = 100", 3)

	// WB3: WHERE col BETWEEN x AND x (same value both ends) — exact match only.
	// BETWEEN 50 AND 50 => only id=6 (val=50).
	checkRowCount("WB3 WHERE val BETWEEN 50 AND 50: 1 row (exact match)",
		"SELECT id FROM v42_wbnd WHERE val BETWEEN 50 AND 50", 1)

	// WB4: WHERE 1=1 — tautology, all rows returned (7 rows).
	checkRowCount("WB4 WHERE 1=1: all 7 rows",
		"SELECT id FROM v42_wbnd WHERE 1=1", 7)

	// WB5: WHERE 1=0 — contradiction, no rows returned.
	checkRowCount("WB5 WHERE 1=0: 0 rows",
		"SELECT id FROM v42_wbnd WHERE 1=0", 0)

	// WB6: WHERE col > MAX_VALUE — nothing exceeds 100, so 0 rows.
	checkRowCount("WB6 WHERE val > 100 (above max): 0 rows",
		"SELECT id FROM v42_wbnd WHERE val > 100", 0)

	// WB7: WHERE col < MIN_VALUE — nothing below -100, so 0 rows.
	checkRowCount("WB7 WHERE val < -100 (below min): 0 rows",
		"SELECT id FROM v42_wbnd WHERE val < -100", 0)

	// WB8: WHERE with empty string — only id=7 has tag=''.
	checkRowCount("WB8 WHERE tag = '': 1 row",
		"SELECT id FROM v42_wbnd WHERE tag = ''", 1)
	check("WB8a WHERE tag = '': correct row (id=7)",
		"SELECT id FROM v42_wbnd WHERE tag = ''", 7)

	// WB9: WHERE val = 0 (not NULL) — id=1 and id=7 both have val=0.
	// Zero is a real value; it must not be confused with NULL.
	checkRowCount("WB9 WHERE val = 0: 2 rows (zero is not NULL)",
		"SELECT id FROM v42_wbnd WHERE val = 0", 2)

	// WB10: WHERE val BETWEEN -1 AND 1 — matches val=-1 (id=4), val=0 (id=1,7), val=1 (id=2).
	// 4 rows total.
	checkRowCount("WB10 WHERE val BETWEEN -1 AND 1: 4 rows",
		"SELECT id FROM v42_wbnd WHERE val BETWEEN -1 AND 1", 4)

	// ============================================================
	// SECTION 5: GROUP BY BOUNDARY CASES
	// ============================================================
	//
	// Schema
	// ------
	//   v42_grp (id INTEGER PK, category TEXT, amount INTEGER)
	//
	// Data (6 rows):
	//   (1, 'A', 10)
	//   (2, 'A', 20)
	//   (3, 'A', 30)   -- three rows all in category A → all same → 1 group possible
	//   (4, 'B', 40)
	//   (5, 'C', 50)
	//   (6, 'D', 60)   -- three distinct single-member categories

	afExec(t, db, ctx, `CREATE TABLE v42_grp (
		id       INTEGER PRIMARY KEY,
		category TEXT,
		amount   INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v42_grp VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v42_grp VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO v42_grp VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO v42_grp VALUES (4, 'B', 40)")
	afExec(t, db, ctx, "INSERT INTO v42_grp VALUES (5, 'C', 50)")
	afExec(t, db, ctx, "INSERT INTO v42_grp VALUES (6, 'D', 60)")

	// GB1: GROUP BY where all rows share the same category ('A' for ids 1-3) →
	// produces exactly 1 group when grouped by category within that subset.
	// Verify via HAVING: category A group has COUNT(*) = 3, confirming it is a single group.
	checkRowCount("GB1 GROUP BY all-same category: 1 group for category A",
		"SELECT category FROM v42_grp GROUP BY category HAVING category = 'A'", 1)

	// GB2: GROUP BY on column where all values are different (id column) → N groups.
	// id is unique per row, so GROUP BY id = 6 distinct groups (each with COUNT=1).
	// All 6 groups have COUNT=1; verify via HAVING COUNT(*) = 1 returns all 6.
	checkRowCount("GB2 GROUP BY unique column: 6 groups (one per row via id)",
		"SELECT id FROM v42_grp GROUP BY id HAVING COUNT(*) = 1", 6)

	// GB3: GROUP BY on single-row table (v42_one, which has 1 row) → 1 group.
	// v42_one currently has 1 row (label='reborn'); GROUP BY label yields 1 group.
	checkRowCount("GB3 GROUP BY on single-row table: 1 group",
		"SELECT label FROM v42_one GROUP BY label", 1)

	// GB4: GROUP BY on empty table → 0 groups.
	// v42_lim is not empty, so create a dedicated empty table.
	afExec(t, db, ctx, `CREATE TABLE v42_grp_empty (id INTEGER PRIMARY KEY, cat TEXT)`)
	checkRowCount("GB4 GROUP BY on empty table: 0 groups",
		"SELECT cat FROM v42_grp_empty GROUP BY cat", 0)

	// GB5: GROUP BY with HAVING COUNT(*) = 1 → only categories with exactly 1 member.
	// Category A has 3 members; B, C, D each have 1 member.  Result: B, C, D → 3 groups.
	checkRowCount("GB5 HAVING COUNT(*) = 1: 3 single-member categories (B, C, D)",
		"SELECT category FROM v42_grp GROUP BY category HAVING COUNT(*) = 1", 3)

	// GB6: GROUP BY with HAVING COUNT(*) > total_rows → 0 groups.
	// No category can have more than 6 members when the whole table has 6 rows.
	checkRowCount("GB6 HAVING COUNT(*) > 6 (impossible): 0 groups",
		"SELECT category FROM v42_grp GROUP BY category HAVING COUNT(*) > 6", 0)

	// GB7: GROUP BY with HAVING COUNT(*) >= 3 → only category A qualifies.
	checkRowCount("GB7 HAVING COUNT(*) >= 3: 1 group (category A)",
		"SELECT category FROM v42_grp GROUP BY category HAVING COUNT(*) >= 3", 1)
	check("GB7a the qualifying group is category A",
		"SELECT category FROM v42_grp GROUP BY category HAVING COUNT(*) >= 3", "A")

	// GB8: SUM per group — verify category A sum = 10+20+30 = 60.
	check("GB8 SUM per group: category A = 60",
		"SELECT SUM(amount) FROM v42_grp WHERE category = 'A'", 60)

	// GB9: COUNT per group — verify category A has 3 members.
	// Use WHERE to filter to category A first, then COUNT(*) the remaining rows.
	// Equivalent to the per-group count for category A.
	check("GB9 COUNT per group: category A has 3 members",
		"SELECT COUNT(*) FROM v42_grp WHERE category = 'A'", 3)

	// GB10: MAX per group — category A max is 30, categories B/C/D are their only value.
	// Verify D's max is 60.
	check("GB10 MAX per group: category D max = 60",
		"SELECT MAX(amount) FROM v42_grp WHERE category = 'D'", 60)

	// ============================================================
	// SECTION 6: JOIN BOUNDARY CASES
	// ============================================================
	//
	// Schema
	// ------
	//   v42_jleft  (id INTEGER PK, val INTEGER)   -- 3 rows
	//   v42_jright (id INTEGER PK, val INTEGER)   -- 2 rows, 1 of which matches left
	//   v42_jempty (id INTEGER PK, val INTEGER)   -- 0 rows (empty)
	//
	// Left data:  (1,10), (2,20), (3,30)
	// Right data: (1,10), (2,99)   -- id=1 matches left; id=2 has different val

	afExec(t, db, ctx, `CREATE TABLE v42_jleft  (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, `CREATE TABLE v42_jright (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, `CREATE TABLE v42_jempty (id INTEGER PRIMARY KEY, val INTEGER)`)

	afExec(t, db, ctx, "INSERT INTO v42_jleft VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v42_jleft VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v42_jleft VALUES (3, 30)")

	afExec(t, db, ctx, "INSERT INTO v42_jright VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v42_jright VALUES (2, 99)")

	// JB1: INNER JOIN empty table with non-empty table → 0 rows.
	checkRowCount("JB1 INNER JOIN non-empty with empty: 0 rows",
		"SELECT l.id FROM v42_jleft l INNER JOIN v42_jempty e ON l.id = e.id", 0)

	// JB2: LEFT JOIN with empty right table → all left rows, right columns NULL.
	// Left has 3 rows; all get NULL for right columns.
	checkRowCount("JB2 LEFT JOIN empty right: all 3 left rows preserved",
		"SELECT l.id FROM v42_jleft l LEFT JOIN v42_jempty e ON l.id = e.id", 3)

	// JB3: LEFT JOIN empty right — right column is NULL for every row.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT l.id, e.val FROM v42_jleft l LEFT JOIN v42_jempty e ON l.id = e.id ORDER BY l.id")
		if len(rows) != 3 {
			t.Errorf("[FAIL] JB3 LEFT JOIN empty right NULL check: expected 3 rows, got %d", len(rows))
		} else {
			allNull := true
			for _, row := range rows {
				if fmt.Sprintf("%v", row[1]) != "<nil>" {
					allNull = false
				}
			}
			if !allNull {
				t.Errorf("[FAIL] JB3 LEFT JOIN empty right: expected all right.val = NULL")
			} else {
				pass++
			}
		}
	}

	// JB4: INNER JOIN where exactly 1 row matches (left.id=1 matches right.id=1).
	// left.id=2 has val=20, right.id=2 has val=99 — different vals so ON val match fails.
	// Join ON l.id = r.id AND l.val = r.val: only (1,10)=(1,10) matches → 1 row.
	checkRowCount("JB4 INNER JOIN with only 1 matching row: 1 row",
		"SELECT l.id FROM v42_jleft l INNER JOIN v42_jright r ON l.id = r.id AND l.val = r.val",
		1)

	// JB5: JOIN with ON 1=1 (Cartesian product) — left(3) × right(2) = 6 rows.
	checkRowCount("JB5 JOIN ON 1=1: Cartesian product = 3*2 = 6 rows",
		"SELECT l.id FROM v42_jleft l INNER JOIN v42_jright r ON 1=1", 6)

	// JB6: JOIN with ON 1=1 on left × empty — 3 * 0 = 0 rows.
	checkRowCount("JB6 JOIN ON 1=1 with empty right: 3*0 = 0 rows",
		"SELECT l.id FROM v42_jleft l INNER JOIN v42_jempty e ON 1=1", 0)

	// JB7: Self-join on single-row table — the row joins with itself → 1 row.
	// v42_one has 1 row (id=1).
	checkRowCount("JB7 self-join on single-row table: 1 row (itself)",
		"SELECT a.id FROM v42_one a INNER JOIN v42_one b ON a.id = b.id", 1)

	// JB8: INNER JOIN where no rows match → 0 rows.
	// Join left.val = right.val: left has (10,20,30), right has (10,99).
	// val=10 matches (id=1 left, id=1 right). val=20 no match. val=30 no match.
	// Join on val = 50 (nonexistent in both tables) → 0 rows.
	checkRowCount("JB8 INNER JOIN no rows match: 0 rows",
		"SELECT l.id FROM v42_jleft l INNER JOIN v42_jright r ON l.val = 50 AND r.val = 50",
		0)

	// JB9: LEFT JOIN — count of right NULLs equals left rows that have no match.
	// left.id=3 (val=30) has no match in right (right has id 1 and 2).
	// On join by id: left.id=3 has no match → NULL right.
	// So 1 NULL right row.
	checkRowCount("JB9 LEFT JOIN: 1 left row with no right match (NULL right)",
		"SELECT l.id FROM v42_jleft l LEFT JOIN v42_jright r ON l.id = r.id WHERE r.id IS NULL",
		1)

	// JB10: INNER JOIN between two non-empty tables where all left rows match — 2 rows.
	// Join left.id = right.id: left has id=1,2,3; right has id=1,2.  Matches: id=1 and id=2 → 2 rows.
	checkRowCount("JB10 INNER JOIN: 2 rows match (left id 1 and 2 found in right)",
		"SELECT l.id FROM v42_jleft l INNER JOIN v42_jright r ON l.id = r.id", 2)

	// ============================================================
	// SECTION 7: UPDATE / DELETE BOUNDARY CASES
	// ============================================================
	//
	// Schema
	// ------
	//   v42_ud (id INTEGER PK, score INTEGER, tag TEXT)
	//
	// Data (4 rows):
	//   (1, 10, 'a')
	//   (2, 20, 'b')
	//   (3, 30, 'c')
	//   (4, 40, 'd')

	afExec(t, db, ctx, `CREATE TABLE v42_ud (
		id    INTEGER PRIMARY KEY,
		score INTEGER,
		tag   TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v42_ud VALUES (1, 10, 'a')")
	afExec(t, db, ctx, "INSERT INTO v42_ud VALUES (2, 20, 'b')")
	afExec(t, db, ctx, "INSERT INTO v42_ud VALUES (3, 30, 'c')")
	afExec(t, db, ctx, "INSERT INTO v42_ud VALUES (4, 40, 'd')")

	// UD1: UPDATE WHERE matches 0 rows — table unchanged.
	checkNoError("UD1 UPDATE WHERE no-match: 0 rows affected",
		"UPDATE v42_ud SET score = 999 WHERE id = 999")
	check("UD1a table unchanged after no-op UPDATE: COUNT still 4",
		"SELECT COUNT(*) FROM v42_ud", 4)
	check("UD1b no spurious row with score=999",
		"SELECT COUNT(*) FROM v42_ud WHERE score = 999", 0)

	// UD2: UPDATE WHERE matches all rows — every row updated.
	checkNoError("UD2 UPDATE all rows: score = score + 1",
		"UPDATE v42_ud SET score = score + 1")
	// Scores are now 11, 21, 31, 41.  MIN should be 11.
	check("UD2a all rows updated: MIN(score) = 11",
		"SELECT MIN(score) FROM v42_ud", 11)
	check("UD2b all rows updated: MAX(score) = 41",
		"SELECT MAX(score) FROM v42_ud", 41)

	// UD3: DELETE WHERE matches 0 rows — table unchanged.
	checkNoError("UD3 DELETE WHERE no-match: 0 rows affected",
		"DELETE FROM v42_ud WHERE id = 999")
	check("UD3a table unchanged after no-op DELETE: COUNT still 4",
		"SELECT COUNT(*) FROM v42_ud", 4)

	// UD4: DELETE WHERE matches all rows → empty table.
	checkNoError("UD4 DELETE all rows",
		"DELETE FROM v42_ud WHERE 1=1")
	checkRowCount("UD4a table empty after DELETE all",
		"SELECT * FROM v42_ud", 0)

	// UD5: Re-insert rows and verify the table works normally again.
	checkNoError("UD5 re-insert row 1 after full DELETE",
		"INSERT INTO v42_ud VALUES (1, 10, 'a')")
	checkNoError("UD5b re-insert row 2",
		"INSERT INTO v42_ud VALUES (2, 20, 'b')")
	check("UD5c COUNT(*) = 2 after re-insert",
		"SELECT COUNT(*) FROM v42_ud", 2)

	// UD6: UPDATE SET col = col (no actual change in value) — must not corrupt data.
	checkNoError("UD6 UPDATE SET score = score (identity update)",
		"UPDATE v42_ud SET score = score")
	check("UD6a score unchanged after identity UPDATE: id=1 score=10",
		"SELECT score FROM v42_ud WHERE id = 1", 10)

	// UD7: UPDATE SET col = NULL on a nullable column.
	checkNoError("UD7 UPDATE SET tag = NULL on id=2",
		"UPDATE v42_ud SET tag = NULL WHERE id = 2")
	check("UD7a tag is NULL after UPDATE: id=2",
		"SELECT tag FROM v42_ud WHERE id = 2", "<nil>")
	// Other row must be unaffected.
	check("UD7b tag unchanged for id=1",
		"SELECT tag FROM v42_ud WHERE id = 1", "a")

	// UD8: DELETE all then re-insert — table must still accept new rows correctly.
	checkNoError("UD8 DELETE all rows again",
		"DELETE FROM v42_ud")
	checkRowCount("UD8a table empty", "SELECT * FROM v42_ud", 0)
	checkNoError("UD8b insert fresh row",
		"INSERT INTO v42_ud VALUES (10, 100, 'fresh')")
	check("UD8c fresh row readable",
		"SELECT tag FROM v42_ud WHERE id = 10", "fresh")

	// UD9: UPDATE affects only the matching subset — partial update correctness.
	// Insert a second fresh row, then update only id=10.
	checkNoError("UD9 insert second row (id=11)",
		"INSERT INTO v42_ud VALUES (11, 200, 'second')")
	checkNoError("UD9b UPDATE only id=10",
		"UPDATE v42_ud SET score = 999 WHERE id = 10")
	check("UD9c id=10 score updated to 999",
		"SELECT score FROM v42_ud WHERE id = 10", 999)
	check("UD9d id=11 score unchanged (200)",
		"SELECT score FROM v42_ud WHERE id = 11", 200)

	// UD10: DELETE one of two rows — verify only the correct row is removed.
	checkNoError("UD10 DELETE id=11",
		"DELETE FROM v42_ud WHERE id = 11")
	checkRowCount("UD10a one row remains (id=10)", "SELECT * FROM v42_ud", 1)
	check("UD10b remaining row is id=10",
		"SELECT id FROM v42_ud", 10)

	// ============================================================
	// SECTION 8: EXPRESSION BOUNDARY CASES
	// ============================================================
	//
	// These tests use direct SELECT without a FROM clause to probe the
	// expression evaluator at boundary values.  Off-by-one errors in
	// constant folding, wrong NULL propagation in CASE, and integer
	// boundary handling are common failure sites.

	// EX1: 0 / 1 = 0.  Division of zero by a non-zero value must be zero.
	check("EX1 0 / 1 = 0",
		"SELECT 0 / 1", 0)

	// EX2: 1 / 1 = 1.
	check("EX2 1 / 1 = 1",
		"SELECT 1 / 1", 1)

	// EX3: -1 * -1 = 1.  Negative times negative is positive.
	check("EX3 -1 * -1 = 1",
		"SELECT -1 * -1", 1)

	// EX4: 0 * anything = 0.  Multiplicative zero annihilator.
	check("EX4 0 * 12345 = 0",
		"SELECT 0 * 12345", 0)

	// EX5: LENGTH of a single-character string = 1.
	check("EX5 LENGTH('x') = 1",
		"SELECT LENGTH('x')", 1)

	// EX6: UPPER of a single-character lowercase string.
	check("EX6 UPPER('a') = 'A'",
		"SELECT UPPER('a')", "A")

	// EX7: LOWER of a single-character uppercase string.
	check("EX7 LOWER('Z') = 'z'",
		"SELECT LOWER('Z')", "z")

	// EX8: Empty string operations — LENGTH('') = 0.
	check("EX8 LENGTH('') = 0",
		"SELECT LENGTH('')", 0)

	// EX8a: UPPER of empty string = empty string.
	check("EX8a UPPER('') = ''",
		"SELECT UPPER('')", "")

	// EX8b: TRIM of empty string = empty string.
	check("EX8b TRIM('') = ''",
		"SELECT TRIM('')", "")

	// EX9: CASE with no matching WHEN and no ELSE → NULL.
	// CASE WHEN 0=1 THEN 'never' END  — no ELSE, no match → NULL.
	check("EX9 CASE no-match no-ELSE: NULL",
		"SELECT CASE WHEN 0=1 THEN 'never' END", "<nil>")

	// EX10: CASE with matching WHEN — must return the matched value, not NULL.
	check("EX10 CASE with match: 'yes'",
		"SELECT CASE WHEN 1=1 THEN 'yes' END", "yes")

	// ============================================================
	// SECTION 9: RECURSIVE CTE BOUNDARIES
	// ============================================================
	//
	// Recursive CTEs have a base case and a recursive case.  The boundary
	// conditions that matter are: depth-1 (only base case runs), termination
	// on the first recursive step, and the case where the base case itself
	// produces no rows (empty recursion).

	// RC1: Depth-1 — base case produces 1 row; recursive part's WHERE prevents
	// any further iteration (n < 1 is false immediately for n=1).
	// WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 1)
	// Base: {1}. Recursive: n=1, WHERE 1<1 = false → stops. Total: 1 row.
	checkRowCount("RC1 recursive CTE depth 1 (base only): 1 row",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 1) SELECT n FROM r",
		1)
	check("RC1a depth-1 CTE: only value is 1",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 1) SELECT n FROM r",
		1)

	// RC2: Depth-2 — base produces 1 row (n=1); recursive adds n=2 then stops (WHERE n<2 is false for n=2).
	// Wait — WHERE n < 2: step 1: n=1, 1<2=true → add n=2; step 2: n=2, 2<2=false → done.
	// Rows: {1, 2} → exactly 2 rows.
	checkRowCount("RC2 recursive CTE terminates at depth 2: 2 rows",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 2) SELECT n FROM r",
		2)
	check("RC2a depth-2 CTE: MAX(n) = 2",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 2) SELECT MAX(n) FROM r",
		2)

	// RC3: Terminates immediately after 1 recursive step — base={0}, recursive: 0→1, WHERE 1<1=false → done.
	// Rows: 0 and 1 → 2 rows total.
	checkRowCount("RC3 recursive CTE terminates after 1 step: 2 rows (0 and 1)",
		"WITH RECURSIVE r(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM r WHERE n < 1) SELECT n FROM r",
		2)
	total++
	{
		rows := afQuery(t, db, ctx,
			"WITH RECURSIVE r(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM r WHERE n < 1) SELECT n FROM r ORDER BY n")
		if len(rows) != 2 {
			t.Errorf("[FAIL] RC3a order check: expected 2 rows, got %d", len(rows))
		} else {
			v0 := fmt.Sprintf("%v", rows[0][0])
			v1 := fmt.Sprintf("%v", rows[1][0])
			if v0 != "0" || v1 != "1" {
				t.Errorf("[FAIL] RC3a order check: expected 0,1 got %s,%s", v0, v1)
			} else {
				pass++
			}
		}
	}

	// RC4: Standard depth-5 sequence — values 1..5.
	// WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5)
	// Base: {1}. Steps: 1→2, 2→3, 3→4, 4→5, 5→WHERE 5<5=false → stops.
	// Total: 5 rows.
	checkRowCount("RC4 depth-5 recursive CTE: 5 rows",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT n FROM r",
		5)
	check("RC4a last value in depth-5 CTE = 5",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT MAX(n) FROM r",
		5)
	check("RC4b SUM(1..5) = 15",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT SUM(n) FROM r",
		15)

	// RC5: Recursive CTE with LIMIT — stop after first 3 rows of an infinite-like sequence.
	// LIMIT 3 must truncate to exactly 3 rows.
	checkRowCount("RC5 recursive CTE with LIMIT 3: 3 rows",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 1000) SELECT n FROM r LIMIT 3",
		3)

	// ============================================================
	// SECTION 10: TRANSACTION BOUNDARIES
	// ============================================================
	//
	// Schema
	// ------
	//   v42_txn (id INTEGER PK, val TEXT)
	//
	// Transactions at their extremes: empty BEGIN/COMMIT, empty BEGIN/ROLLBACK,
	// a single-operation transaction, and a transaction against an empty table.

	afExec(t, db, ctx, `CREATE TABLE v42_txn (
		id  INTEGER PRIMARY KEY,
		val TEXT
	)`)

	// TX1: Empty transaction — BEGIN then COMMIT with no operations in between.
	// Must not error and must leave the database unchanged (table still empty).
	checkNoError("TX1 BEGIN (empty transaction start)",
		"BEGIN")
	checkNoError("TX1a COMMIT empty transaction",
		"COMMIT")
	checkRowCount("TX1b table still empty after empty COMMIT transaction",
		"SELECT * FROM v42_txn", 0)

	// TX2: Empty ROLLBACK — BEGIN then ROLLBACK with no operations.
	checkNoError("TX2 BEGIN (empty rollback transaction start)",
		"BEGIN")
	checkNoError("TX2a ROLLBACK empty transaction",
		"ROLLBACK")
	checkRowCount("TX2b table still empty after empty ROLLBACK",
		"SELECT * FROM v42_txn", 0)

	// TX3: Single-operation transaction — INSERT committed successfully.
	checkNoError("TX3 BEGIN single-op transaction",
		"BEGIN")
	checkNoError("TX3a INSERT within transaction",
		"INSERT INTO v42_txn VALUES (1, 'committed')")
	// Row must be visible within the same transaction.
	check("TX3b row visible within transaction",
		"SELECT val FROM v42_txn WHERE id = 1", "committed")
	checkNoError("TX3c COMMIT single-op transaction",
		"COMMIT")
	// Row must persist after COMMIT.
	check("TX3d row persists after COMMIT",
		"SELECT val FROM v42_txn WHERE id = 1", "committed")
	checkRowCount("TX3e table has 1 row after COMMIT",
		"SELECT * FROM v42_txn", 1)

	// TX4: Transaction on empty table — INSERT then ROLLBACK.
	// After ROLLBACK the table should return to its pre-transaction state (1 row from TX3).
	checkNoError("TX4 BEGIN rollback on non-empty table",
		"BEGIN")
	checkNoError("TX4a INSERT within transaction (will be rolled back)",
		"INSERT INTO v42_txn VALUES (2, 'rollback_me')")
	check("TX4b row visible in transaction before rollback",
		"SELECT COUNT(*) FROM v42_txn", 2)
	checkNoError("TX4c ROLLBACK",
		"ROLLBACK")
	// Only the original committed row should remain.
	check("TX4d COUNT(*) = 1 after ROLLBACK (uncommitted INSERT reverted)",
		"SELECT COUNT(*) FROM v42_txn", 1)
	check("TX4e rolled-back row gone",
		"SELECT COUNT(*) FROM v42_txn WHERE id = 2", 0)

	// TX5: UPDATE inside transaction that is rolled back — original value restored.
	checkNoError("TX5 BEGIN update-rollback transaction",
		"BEGIN")
	checkNoError("TX5a UPDATE committed row inside transaction",
		"UPDATE v42_txn SET val = 'modified' WHERE id = 1")
	check("TX5b updated value visible inside transaction",
		"SELECT val FROM v42_txn WHERE id = 1", "modified")
	checkNoError("TX5c ROLLBACK update transaction",
		"ROLLBACK")
	check("TX5d original value restored after ROLLBACK",
		"SELECT val FROM v42_txn WHERE id = 1", "committed")

	// ============================================================
	// FINAL REPORT
	// ============================================================
	t.Logf("TestV42BoundaryConditions: %d/%d tests passed", pass, total)
	if pass < total {
		t.Errorf("SUMMARY: %d tests FAILED out of %d", total-pass, total)
	}
}
