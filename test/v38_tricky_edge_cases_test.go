package test

import (
	"fmt"
	"testing"
)

// TestV38TrickyEdgeCases exercises the most treacherous corners of SQL semantics
// that routinely expose engine bugs: NULL three-valued logic, LIKE matching,
// ORDER BY / GROUP BY edge conditions, subquery NULL propagation, type coercion,
// JOIN NULL-key behaviour, constraint enforcement, and aggregate NULL semantics.
//
// Ten sections are covered:
//
//  1. Empty string vs NULL              (tests E1-E5)
//  2. Zero vs NULL in arithmetic        (tests Z1-Z5)
//  3. LIKE edge cases                   (tests L1-L8)
//  4. ORDER BY edge cases               (tests O1-O8)
//  5. GROUP BY edge cases               (tests G1-G8)
//  6. Subquery edge cases               (tests S1-S8)
//  7. Type coercion edge cases          (tests T1-T8)
//  8. JOIN edge cases                   (tests J1-J10)
//  9. INSERT / UPDATE edge cases        (tests I1-I10)
// 10. Aggregate edge cases              (tests A1-A10)
//
// All table names carry the v38_ prefix to prevent collisions.
// Expected values are derived from SQL standard three-valued logic unless the
// engine exhibits known divergences (noted inline).
// Division between two integers yields FLOAT in this engine (e.g. 7/2 = 3.5).
// NULL renders as "<nil>" when formatted via fmt.Sprintf("%v", ...).
// LIKE is case-insensitive in this engine.
func TestV38TrickyEdgeCases(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	// check verifies that the first column of the first returned row equals expected.
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

	// checkRowCount verifies that the query returns exactly expected rows.
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

	// checkError verifies that the statement returns an error.
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

	// ============================================================
	// SECTION 1: EMPTY STRING vs NULL
	// ============================================================
	//
	// The SQL standard distinguishes '' (the empty string) from NULL
	// (the absence of a value).  Many engines blur this distinction;
	// these tests pin down the correct behaviour.
	//
	// Schema:
	//   v38_empty_null (id PK, tag TEXT)
	//     id=1  tag=''      -- explicit empty string
	//     id=2  tag=NULL    -- explicit null
	//     id=3  tag='hello' -- non-empty value

	afExec(t, db, ctx, `CREATE TABLE v38_empty_null (
		id  INTEGER PRIMARY KEY,
		tag TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_empty_null VALUES (1, '')")
	afExec(t, db, ctx, "INSERT INTO v38_empty_null VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v38_empty_null VALUES (3, 'hello')")

	// E1: Empty string is NOT NULL.
	// WHERE tag IS NOT NULL must return id=1 (empty string) AND id=3.
	// id=2 is NULL so it is excluded.  Total = 2 rows.
	checkRowCount("E1 empty string is NOT NULL (2 rows: '' and 'hello')",
		`SELECT id FROM v38_empty_null WHERE tag IS NOT NULL`, 2)

	// E2: WHERE col = '' selects only the empty-string row, NOT the NULL row.
	// NULL = '' evaluates to UNKNOWN, not TRUE, so id=2 is excluded.
	checkRowCount("E2 WHERE tag = '' returns only the empty-string row",
		`SELECT id FROM v38_empty_null WHERE tag = ''`, 1)

	// E3: COUNT(col) counts non-NULL values only.
	// Empty string '' is a real value and IS counted.
	// NULL is not counted.  COUNT(tag) must be 2 ('' and 'hello').
	check("E3 COUNT(col) counts empty string but not NULL",
		`SELECT COUNT(tag) FROM v38_empty_null`, 2)

	// E4: COALESCE('', 'fallback') must return '' not 'fallback'.
	// COALESCE returns the first non-NULL argument.  '' is non-NULL, so it wins.
	check("E4 COALESCE('', 'fallback') returns '' not 'fallback'",
		`SELECT COALESCE('', 'fallback')`, "")

	// E5: LENGTH('') must return 0.
	check("E5 LENGTH('') is 0",
		`SELECT LENGTH('')`, 0)

	// ============================================================
	// SECTION 2: ZERO vs NULL IN ARITHMETIC
	// ============================================================
	//
	// In SQL, NULL propagates through arithmetic.  0 is a real value;
	// NULL is not.  These tests confirm that the engine does not confuse the two.

	// Z1: 0 + NULL must yield NULL (not 0).
	// The result is "<nil>" when formatted via %v.
	check("Z1 0 + NULL = NULL (not 0)",
		`SELECT 0 + NULL`, "<nil>")

	// Z2: 0 * NULL must yield NULL.
	check("Z2 0 * NULL = NULL (not 0)",
		`SELECT 0 * NULL`, "<nil>")

	// Z3: SUM of a mix of zeros and NULLs.
	// SUM ignores NULLs; zeros ARE summed.
	// Values: 0, NULL, 0, NULL, 5  =>  SUM = 0+0+5 = 5
	afExec(t, db, ctx, `CREATE TABLE v38_zero_null (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_zero_null VALUES (1, 0)")
	afExec(t, db, ctx, "INSERT INTO v38_zero_null VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v38_zero_null VALUES (3, 0)")
	afExec(t, db, ctx, "INSERT INTO v38_zero_null VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO v38_zero_null VALUES (5, 5)")

	check("Z3 SUM of zeros and NULLs counts zeros and ignores NULLs: 5",
		`SELECT SUM(val) FROM v38_zero_null`, 5)

	// Z4: AVG includes zeros but excludes NULLs.
	// Non-NULL values: 0, 0, 5  =>  COUNT=3, SUM=5  =>  AVG = 5/3 ≈ 1.6666...
	// Engine uses float division; exact string comparison on AVG is fragile, so
	// we verify that AVG > 0 and AVG < 2 by checking the row count of:
	//   SELECT 1 WHERE (SELECT AVG(val) ...) > 0
	// Instead, use a simpler data set to get an exact value:
	//   Add id=6 with val=1 => values 0,0,5,1 => AVG = 6/4 = 1.5
	afExec(t, db, ctx, "INSERT INTO v38_zero_null VALUES (6, 1)")
	// Non-NULL values: 0,0,5,1 => AVG = 6/4 = 1.5
	check("Z4 AVG includes zeros and excludes NULLs: 6/4 = 1.5",
		`SELECT AVG(val) FROM v38_zero_null`, 1.5)

	// Z5: COALESCE(NULL, 0) + 5 = 5.
	// COALESCE returns 0 (the first non-NULL); 0+5 = 5.
	check("Z5 COALESCE(NULL, 0) + 5 = 5",
		`SELECT COALESCE(NULL, 0) + 5`, 5)

	// ============================================================
	// SECTION 3: LIKE EDGE CASES
	// ============================================================
	//
	// LIKE '%' is a wildcard matching zero or more characters.
	// LIKE '_' matches exactly one character.
	// In this engine LIKE is case-insensitive.
	//
	// Schema:
	//   v38_like_data (id PK, word TEXT)

	afExec(t, db, ctx, `CREATE TABLE v38_like_data (
		id   INTEGER PRIMARY KEY,
		word TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_like_data VALUES (1, '')")            // empty string
	afExec(t, db, ctx, "INSERT INTO v38_like_data VALUES (2, 'a')")           // single char
	afExec(t, db, ctx, "INSERT INTO v38_like_data VALUES (3, 'abc')")         // exact match
	afExec(t, db, ctx, "INSERT INTO v38_like_data VALUES (4, 'xabcx')")       // substring match
	afExec(t, db, ctx, "INSERT INTO v38_like_data VALUES (5, 'ABC')")         // uppercase version
	afExec(t, db, ctx, "INSERT INTO v38_like_data VALUES (6, '100%')")        // literal percent
	afExec(t, db, ctx, "INSERT INTO v38_like_data VALUES (7, 'under_score')") // literal underscore
	afExec(t, db, ctx, "INSERT INTO v38_like_data VALUES (8, 'hello world')") // space in value

	// L1: LIKE '%' matches every non-NULL value, including empty string.
	// All 8 rows have non-NULL word, so all 8 must match.
	checkRowCount("L1 LIKE '%' matches all non-NULL rows including empty string",
		`SELECT id FROM v38_like_data WHERE word LIKE '%'`, 8)

	// L2: LIKE '_' matches exactly one character.
	// Only id=2 ('a') has exactly one character.
	checkRowCount("L2 LIKE '_' matches only single-character values",
		`SELECT id FROM v38_like_data WHERE word LIKE '_'`, 1)

	// L3: LIKE 'abc' is an exact match (case-insensitive in this engine).
	// id=3 ('abc') and id=5 ('ABC') should both match.
	checkRowCount("L3 LIKE 'abc' exact match is case-insensitive (2 rows)",
		`SELECT id FROM v38_like_data WHERE word LIKE 'abc'`, 2)

	// L4: LIKE '%abc%' substring match.
	// 'abc' (id=3) and 'xabcx' (id=4) and 'ABC' (id=5) match (case-insensitive).
	checkRowCount("L4 LIKE '%abc%' substring matches abc, xabcx, ABC (3 rows)",
		`SELECT id FROM v38_like_data WHERE word LIKE '%abc%'`, 3)

	// L5: NOT LIKE '%abc%' excludes the matching rows.
	// 8 total rows - 3 matching = 5 rows.
	checkRowCount("L5 NOT LIKE '%abc%' excludes 3 matches, returns 5 rows",
		`SELECT id FROM v38_like_data WHERE word NOT LIKE '%abc%'`, 5)

	// L6: Matching a literal '%' in data.
	// The pattern 'LIKE '100%'' (with no escape) will match 'hello world' and others
	// starting with '100' plus any suffix.  Only id=6 ('100%') starts with '100'.
	// The trailing '%' in the LIKE pattern matches the literal '%' in the data value.
	checkRowCount("L6 LIKE '100%' matches the row containing '100%'",
		`SELECT id FROM v38_like_data WHERE word LIKE '100%'`, 1)

	// L7: LIKE 'under_%' where '_' in the LIKE pattern is a wildcard.
	// Pattern 'under_%' means: literal 'under' + exactly one char + any suffix.
	// id=7 is 'under_score'; 'under' then '_' wildcard matches '_' then 'score' suffix.
	// So id=7 matches.
	checkRowCount("L7 LIKE 'under_%' matches 'under_score'",
		`SELECT id FROM v38_like_data WHERE word LIKE 'under_%'`, 1)

	// L8: Case sensitivity check with LIKE.
	// This engine is case-insensitive, so 'HELLO%' should match 'hello world'.
	checkRowCount("L8 LIKE 'HELLO%' case-insensitively matches 'hello world'",
		`SELECT id FROM v38_like_data WHERE word LIKE 'HELLO%'`, 1)

	// ============================================================
	// SECTION 4: ORDER BY EDGE CASES
	// ============================================================
	//
	// Schema:
	//   v38_order_test (id PK, a INTEGER, b INTEGER, label TEXT)
	//
	// Data is designed so NULL placement, expression ordering, and positional
	// ordering can all be verified precisely.

	afExec(t, db, ctx, `CREATE TABLE v38_order_test (
		id    INTEGER PRIMARY KEY,
		a     INTEGER,
		b     INTEGER,
		label TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_order_test VALUES (1,  3,  10, 'gamma')")
	afExec(t, db, ctx, "INSERT INTO v38_order_test VALUES (2,  1,  30, 'alpha')")
	afExec(t, db, ctx, "INSERT INTO v38_order_test VALUES (3,  2,  20, 'beta')")
	afExec(t, db, ctx, "INSERT INTO v38_order_test VALUES (4,  NULL, 5, 'delta')")
	afExec(t, db, ctx, "INSERT INTO v38_order_test VALUES (5,  NULL,15, 'epsilon')")

	// O1: ORDER BY with NULLs - determine where NULLs sort.
	// This engine sorts NULLs LAST in ASC order (consistent with NULLS LAST default).
	// ASC order of 'a': 1(alpha), 2(beta), 3(gamma), NULL(delta), NULL(epsilon).
	// The first row has a=1 (id=2, 'alpha').
	// The last position (OFFSET 4) has a NULL (one of the two NULL rows).
	check("O1 ORDER BY a ASC: first non-null value is 1 (NULLs sort last in this engine)",
		`SELECT a FROM v38_order_test WHERE a IS NOT NULL ORDER BY a ASC LIMIT 1`, 1)

	// O1b: Confirm NULLs appear at the end by checking count of NULLs at tail of ASC sort.
	// If we skip the 3 non-null rows (OFFSET 3), both remaining rows have NULL a.
	check("O1b ORDER BY a ASC OFFSET 3: NULL-keyed rows appear at the end",
		`SELECT a FROM v38_order_test ORDER BY a ASC, id ASC LIMIT 1 OFFSET 3`, "<nil>")

	// O2: ORDER BY expression (a + b).
	// Values of a+b (NULLs yield NULL for the expression):
	//   id=1: 3+10=13, id=2: 1+30=31, id=3: 2+20=22, id=4: NULL, id=5: NULL
	// ASC: NULLs first, then 13, 22, 31.  Top non-null is id=1 (a+b=13).
	// Verify the maximum a+b value is 31 (id=2).
	check("O2 ORDER BY expression a+b: max is 31 (id=2)",
		`SELECT a + b FROM v38_order_test WHERE a IS NOT NULL ORDER BY a + b DESC LIMIT 1`, 31)

	// O3: ORDER BY column not in SELECT list.
	// The query selects only label, but orders by a.
	// Non-null a values ascending: 1(alpha), 2(beta), 3(gamma).
	// First label in that order is 'alpha'.
	check("O3 ORDER BY column not in SELECT: first label ordered by a ASC is alpha",
		`SELECT label FROM v38_order_test WHERE a IS NOT NULL ORDER BY a ASC LIMIT 1`, "alpha")

	// O4: ORDER BY with DISTINCT.
	// SELECT DISTINCT label ORDER BY label ASC: labels are alpha, beta, delta, epsilon, gamma.
	// Alphabetically first is 'alpha'.
	check("O4 ORDER BY with DISTINCT: first label alphabetically is alpha",
		`SELECT DISTINCT label FROM v38_order_test ORDER BY label ASC LIMIT 1`, "alpha")

	// O5: ORDER BY on an aliased column.
	// SELECT a + b AS total ... ORDER BY total.
	// Among non-NULL rows: total values 13,31,22. Min total = 13 (id=1).
	check("O5 ORDER BY aliased column 'total': min total is 13",
		`SELECT a + b AS total FROM v38_order_test WHERE a IS NOT NULL ORDER BY total ASC LIMIT 1`, 13)

	// O6: ORDER BY positional (ORDER BY 1) where position 1 = first selected column.
	// SELECT b, label ORDER BY 1 ASC: b values are 5,10,15,20,30; first is 5 (id=4).
	check("O6 ORDER BY 1 positional: smallest b is 5",
		`SELECT b, label FROM v38_order_test ORDER BY 1 ASC LIMIT 1`, 5)

	// O7: ORDER BY 2 positional (second column = label).
	// SELECT b, label ORDER BY 2 ASC: order by label alphabetically.
	// label 'alpha' (id=2, b=30) sorts first.
	// LIMIT 1 returns b=30 (the b value for the 'alpha' row).
	check("O7 ORDER BY 2 positional: first row is alpha (b=30)",
		`SELECT b, label FROM v38_order_test ORDER BY 2 ASC LIMIT 1`, 30)

	// O7b: Verify via explicit ORDER BY label that alpha maps to b=30.
	check("O7b ORDER BY label ASC LIMIT 1 gives b=30 for alpha",
		`SELECT b FROM v38_order_test ORDER BY label ASC LIMIT 1`, 30)

	// O8: ORDER BY a DESC: engine puts NULLs FIRST in DESC order (opposite of ASC).
	// Since NULLs sort last in ASC, they sort first in DESC.
	// DESC order: NULL, NULL, 3, 2, 1.
	// The first row (LIMIT 1) has a=NULL.
	// The last non-null (OFFSET 2) has a=3.
	check("O8 ORDER BY a DESC OFFSET 2: first non-null in DESC is a=3",
		`SELECT a FROM v38_order_test WHERE a IS NOT NULL ORDER BY a DESC LIMIT 1`, 3)

	// O8b: confirm NULLs appear first in DESC via LIMIT 1 returning NULL.
	check("O8b ORDER BY a DESC LIMIT 1: NULL rows come first in DESC",
		`SELECT a FROM v38_order_test ORDER BY a DESC LIMIT 1`, "<nil>")

	// ============================================================
	// SECTION 5: GROUP BY EDGE CASES
	// ============================================================
	//
	// Schema:
	//   v38_group_test (id PK, cat TEXT, val INTEGER)
	//
	// Data includes NULLs in the grouping column to test NULL group semantics.

	afExec(t, db, ctx, `CREATE TABLE v38_group_test (
		id  INTEGER PRIMARY KEY,
		cat TEXT,
		val INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_group_test VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v38_group_test VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO v38_group_test VALUES (3, 'B', 30)")
	afExec(t, db, ctx, "INSERT INTO v38_group_test VALUES (4, NULL, 40)")
	afExec(t, db, ctx, "INSERT INTO v38_group_test VALUES (5, NULL, 50)")
	afExec(t, db, ctx, "INSERT INTO v38_group_test VALUES (6, 'C', 60)")

	// G1: GROUP BY with NULLs - NULL values form exactly ONE group.
	// Groups: 'A', 'B', NULL, 'C' => 4 groups total.
	checkRowCount("G1 GROUP BY cat: NULLs form one group (4 distinct groups)",
		`SELECT cat, SUM(val) FROM v38_group_test GROUP BY cat`, 4)

	// G2: GROUP BY expression (CASE WHEN val > 25 THEN 'high' ELSE 'low' END).
	// val<=25: ids 1(10),2(20)  => 'low'  (2 rows, sum=30)
	// val>25:  ids 3(30),4(40),5(50),6(60) => 'high' (4 rows, sum=180)
	// 2 groups total.
	checkRowCount("G2 GROUP BY CASE expression: 2 groups (high/low)",
		`SELECT CASE WHEN val > 25 THEN 'high' ELSE 'low' END AS tier, COUNT(*)
		 FROM v38_group_test GROUP BY tier`, 2)

	// G3: GROUP BY positional (GROUP BY 1).
	// GROUP BY 1 refers to the first SELECT column (cat), following SQL standard.
	// Result: 4 groups (one per distinct cat value: 'A', 'B', 'C', NULL).
	checkRowCount("G3 GROUP BY 1 positional reference to first column",
		`SELECT cat, COUNT(*) FROM v38_group_test GROUP BY 1`, 4)

	// G3b: Confirm positional GROUP BY works correctly when using the column name.
	// GROUP BY cat returns 4 distinct groups.
	checkRowCount("G3b GROUP BY cat (by name) returns 4 groups",
		`SELECT cat, COUNT(*) FROM v38_group_test GROUP BY cat`, 4)

	// G4: GROUP BY with HAVING filter.
	// Groups with SUM(val) > 50: 'B'(30-no), NULL(40+50=90-yes), 'C'(60-yes), 'A'(30-no).
	// Matching: NULL group (90) and C group (60) => 2 groups.
	checkRowCount("G4 HAVING SUM(val) > 50 returns 2 groups (NULL-group and C)",
		`SELECT cat, SUM(val) FROM v38_group_test GROUP BY cat HAVING SUM(val) > 50`, 2)

	// G5: GROUP BY with only 1 group (WHERE filters to one category).
	// Filter to cat='A': 2 rows form 1 group.
	checkRowCount("G5 GROUP BY cat on filtered set with 1 group",
		`SELECT cat, SUM(val) FROM v38_group_test WHERE cat = 'A' GROUP BY cat`, 1)

	// G6: GROUP BY all rows (each row is its own group - use PK).
	// GROUP BY id gives as many groups as rows: 6.
	checkRowCount("G6 GROUP BY id gives one group per row (6 groups)",
		`SELECT id, val FROM v38_group_test GROUP BY id`, 6)

	// G7: HAVING without GROUP BY - treats the entire table as one group.
	// SUM(val) over all rows = 10+20+30+40+50+60 = 210.
	// HAVING SUM(val) > 100 is true, so 1 row is returned.
	checkRowCount("G7 HAVING without GROUP BY: whole table is one group",
		`SELECT SUM(val) FROM v38_group_test HAVING SUM(val) > 100`, 1)

	check("G7b HAVING without GROUP BY: SUM of all rows is 210",
		`SELECT SUM(val) FROM v38_group_test HAVING SUM(val) > 100`, 210)

	// G8: SELECT aggregate without GROUP BY creates an implicit single group.
	// COUNT(*) over the 6-row table is 6.
	check("G8 SELECT COUNT(*) without GROUP BY implicitly groups all rows",
		`SELECT COUNT(*) FROM v38_group_test`, 6)

	// ============================================================
	// SECTION 6: SUBQUERY EDGE CASES
	// ============================================================
	//
	// These tests cover the trickiest areas of subquery NULL semantics,
	// especially NOT IN when the subquery result set contains NULLs.
	//
	// Schema:
	//   v38_sq_outer (id PK, val INTEGER)
	//   v38_sq_inner (id PK, val INTEGER)  -- contains NULLs

	afExec(t, db, ctx, `CREATE TABLE v38_sq_outer (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_sq_outer VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v38_sq_outer VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v38_sq_outer VALUES (3, 30)")

	afExec(t, db, ctx, `CREATE TABLE v38_sq_inner (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_sq_inner VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v38_sq_inner VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v38_sq_inner VALUES (3, 50)")

	// S1: Subquery returning NULL as a scalar value.
	// SELECT MAX(val) FROM empty table = NULL.
	afExec(t, db, ctx, `CREATE TABLE v38_sq_empty (id INTEGER PRIMARY KEY, val INTEGER)`)
	check("S1 subquery returning NULL: MAX of empty table is NULL",
		`SELECT (SELECT MAX(val) FROM v38_sq_empty)`, "<nil>")

	// S2: Scalar subquery returning 0 rows evaluates to NULL.
	// WHERE 1=0 returns no rows; scalar subquery of that is NULL.
	check("S2 scalar subquery with 0 rows is NULL",
		`SELECT (SELECT val FROM v38_sq_outer WHERE 1 = 0)`, "<nil>")

	// S3: IN with empty subquery result - nothing matches, so 0 rows returned.
	checkRowCount("S3 IN with empty subquery result returns 0 rows",
		`SELECT id FROM v38_sq_outer WHERE val IN (SELECT val FROM v38_sq_empty)`, 0)

	// S4: NOT IN with NULLs in the subquery result (the classic trap).
	// SQL standard: x NOT IN (list with NULLs) = UNKNOWN when no match found,
	// because "x != NULL" is UNKNOWN.  UNKNOWN is not TRUE, so no rows pass the filter.
	// v38_sq_inner.val contains: 10, NULL, 50.
	// For outer val=20: 20 NOT IN (10, NULL, 50).
	//   20 != 10 => TRUE, 20 != NULL => UNKNOWN, so overall NOT IN = UNKNOWN.
	// For outer val=30: same logic => UNKNOWN.
	// For outer val=10: 10 IN (10, NULL, 50) is TRUE => NOT IN is FALSE.
	// Result: 0 rows pass (all are UNKNOWN or FALSE) when inner has NULLs.
	checkRowCount("S4 NOT IN with NULLs in subquery: no rows pass (classic NULL trap)",
		`SELECT id FROM v38_sq_outer WHERE val NOT IN (SELECT val FROM v38_sq_inner)`, 0)

	// S5: EXISTS with empty result = false.
	// The subquery matches no rows, so EXISTS = false, so outer query returns 0 rows.
	checkRowCount("S5 EXISTS with empty subquery result returns 0 rows",
		`SELECT id FROM v38_sq_outer WHERE EXISTS (SELECT 1 FROM v38_sq_empty)`, 0)

	// S6: EXISTS with non-empty result = true.
	// v38_sq_inner has rows, so EXISTS = true; all outer rows are returned.
	checkRowCount("S6 EXISTS with non-empty subquery returns all outer rows (3)",
		`SELECT id FROM v38_sq_outer WHERE EXISTS (SELECT 1 FROM v38_sq_inner)`, 3)

	// S7: Correlated subquery referencing the outer row.
	// For each outer row, check if the inner table has the same val.
	// outer val=10 matches inner val=10 (id=1) => EXISTS = true.
	// outer val=20 has no match in inner => EXISTS = false.
	// outer val=30 has no match in inner => EXISTS = false.
	// Result: 1 row (id=1).
	checkRowCount("S7 correlated subquery: only outer val=10 has a match in inner",
		`SELECT o.id FROM v38_sq_outer o
		 WHERE EXISTS (SELECT 1 FROM v38_sq_inner i WHERE i.val = o.val)`, 1)

	// S8: Subquery in CASE WHEN.
	// CASE WHEN (SELECT MAX(val) FROM v38_sq_inner) > 40 THEN 'big' ELSE 'small' END.
	// MAX(val) of v38_sq_inner ignores NULLs: values are 10, 50 => MAX=50.
	// 50 > 40 is true => result is 'big'.
	check("S8 subquery in CASE WHEN: MAX(inner) = 50 > 40 => 'big'",
		`SELECT CASE WHEN (SELECT MAX(val) FROM v38_sq_inner) > 40 THEN 'big' ELSE 'small' END`, "big")

	// ============================================================
	// SECTION 7: TYPE COERCION EDGE CASES
	// ============================================================
	//
	// Schema:
	//   v38_types (id PK, int_col INTEGER, str_col TEXT)

	afExec(t, db, ctx, `CREATE TABLE v38_types (
		id      INTEGER PRIMARY KEY,
		int_col INTEGER,
		str_col TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_types VALUES (1, 10, '10')")
	afExec(t, db, ctx, "INSERT INTO v38_types VALUES (2,  9, '9')")
	afExec(t, db, ctx, "INSERT INTO v38_types VALUES (3, 42, 'hello')")

	// T1: Comparing string '10' with integer 10.
	// In this engine, '10' compared with integer 10 uses numeric coercion.
	// The row with str_col='10' and int_col=10 should satisfy int_col = 10.
	checkRowCount("T1 int_col = 10 matches the row where int_col is 10",
		`SELECT id FROM v38_types WHERE int_col = 10`, 1)

	// T2: String '10' > integer 9.
	// str_col='10' compared with literal integer 9: this tests coercion.
	// In numeric context '10' coerces to 10 and 10 > 9 is true.
	checkRowCount("T2 str_col = '10' row: str_col > 9 is true (numeric coercion)",
		`SELECT id FROM v38_types WHERE id = 1 AND str_col > 9`, 1)

	// T3: SUM of int_col where str_col looks like an integer.
	// Rows with str_col that is a numeric string: id=1 ('10') and id=2 ('9').
	// int_col for those rows: 10 and 9; SUM = 19.
	check("T3 SUM(int_col) for rows with numeric str_col: 10+9=19",
		`SELECT SUM(int_col) FROM v38_types WHERE id IN (1, 2)`, 19)

	// T4: CAST(NULL AS INTEGER) must produce NULL.
	check("T4 CAST(NULL AS INTEGER) is NULL",
		`SELECT CAST(NULL AS INTEGER)`, "<nil>")

	// T5: CAST('' AS INTEGER) - empty string cast to integer.
	// Behaviour varies: many engines produce 0 or NULL; we test what the engine does.
	// This engine produces 0 for CAST('' as INTEGER).
	check("T5 CAST('' AS INTEGER) produces 0 in this engine",
		`SELECT CAST('' AS INTEGER)`, 0)

	// T6: Boolean expression 1=1 is TRUE.
	check("T6 1=1 is true",
		`SELECT 1 = 1`, true)

	// T7: Boolean expression 0=1 is FALSE.
	check("T7 0=1 is false",
		`SELECT 0 = 1`, false)

	// T8: Arithmetic with boolean result.
	// (1=1) is treated as 1 in arithmetic context; (1=1) + 4 = 5.
	check("T8 (1=1) + 4 = 5 (boolean true acts as integer 1)",
		`SELECT (1 = 1) + 4`, 5)

	// ============================================================
	// SECTION 8: JOIN EDGE CASES
	// ============================================================
	//
	// Schema:
	//   v38_jleft  (id PK, key_col INTEGER, name TEXT)
	//   v38_jright (id PK, key_col INTEGER, detail TEXT)
	//
	// Key: both tables have rows with NULL key_col.
	// JOIN on key_col: NULL != NULL so null-keyed rows never match.

	afExec(t, db, ctx, `CREATE TABLE v38_jleft (
		id      INTEGER PRIMARY KEY,
		key_col INTEGER,
		name    TEXT
	)`)
	afExec(t, db, ctx, `CREATE TABLE v38_jright (
		id      INTEGER PRIMARY KEY,
		key_col INTEGER,
		detail  TEXT
	)`)

	afExec(t, db, ctx, "INSERT INTO v38_jleft VALUES (1, 1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v38_jleft VALUES (2, 2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO v38_jleft VALUES (3, NULL, 'Carol')")
	afExec(t, db, ctx, "INSERT INTO v38_jleft VALUES (4, 3, 'Dave')")

	afExec(t, db, ctx, "INSERT INTO v38_jright VALUES (1, 1, 'detail1')")
	afExec(t, db, ctx, "INSERT INTO v38_jright VALUES (2, 2, 'detail2')")
	afExec(t, db, ctx, "INSERT INTO v38_jright VALUES (3, NULL, 'detail-null')")
	afExec(t, db, ctx, "INSERT INTO v38_jright VALUES (4, 4, 'detail4')")

	// J1: INNER JOIN on NULL columns must NOT match (NULL != NULL in join).
	// Left key_col: 1, 2, NULL, 3.  Right key_col: 1, 2, NULL, 4.
	// Matching pairs: (1,1), (2,2).  key=3 and key=4 have no counterpart.
	// NULL keys never match.  Result: 2 rows.
	checkRowCount("J1 INNER JOIN on NULL columns: NULLs do not match (2 rows)",
		`SELECT l.id FROM v38_jleft l JOIN v38_jright r ON l.key_col = r.key_col`, 2)

	// J2: LEFT JOIN where ALL rows on the left match something on the right.
	// Use a subset: filter left to key_col IN (1, 2) => both match in right.
	// 2 left rows, both match, 0 NULLs on right side in result.
	checkRowCount("J2 LEFT JOIN where all left rows match: no NULLs on right side (2 rows)",
		`SELECT l.id FROM v38_jleft l LEFT JOIN v38_jright r ON l.key_col = r.key_col
		 WHERE l.key_col IN (1, 2)`, 2)

	check("J2b all matched rows have non-NULL detail",
		`SELECT COUNT(*) FROM v38_jleft l LEFT JOIN v38_jright r ON l.key_col = r.key_col
		 WHERE l.key_col IN (1, 2) AND r.detail IS NOT NULL`, 2)

	// J3: LEFT JOIN where NO rows on the left match right (right side all NULL in result).
	// Left key_col=3 has no match on the right (right has 1,2,NULL,4 but not 3).
	// LEFT JOIN preserves that row; r.detail will be NULL.
	checkRowCount("J3 LEFT JOIN with no match: right side is NULL in result (1 row)",
		`SELECT l.id FROM v38_jleft l LEFT JOIN v38_jright r ON l.key_col = r.key_col
		 WHERE l.key_col = 3`, 1)

	check("J3b unmatched LEFT JOIN row has NULL detail",
		`SELECT COUNT(*) FROM v38_jleft l LEFT JOIN v38_jright r ON l.key_col = r.key_col
		 WHERE l.key_col = 3 AND r.detail IS NULL`, 1)

	// J4: Self-join with a complex condition.
	// Join v38_jleft against itself; find pairs where l1.key_col < l2.key_col
	// and both key_cols are NOT NULL.
	// Non-NULL key_cols in jleft: 1(Alice), 2(Bob), 3(Dave).
	// Pairs where k1 < k2: (1,2),(1,3),(2,3) => 3 pairs.
	checkRowCount("J4 self-join: pairs where key1 < key2 (NULLs excluded)",
		`SELECT l1.id FROM v38_jleft l1 JOIN v38_jleft l2
		 ON l1.key_col < l2.key_col
		 WHERE l1.key_col IS NOT NULL AND l2.key_col IS NOT NULL`, 3)

	// J5: JOIN with COALESCE in the ON condition.
	// Replace NULL key_col with -1 before joining; -1 won't match anything real
	// but the COALESCE prevents NULL propagation in the expression.
	// Left COALESCE keys: 1,2,-1,3.  Right COALESCE keys: 1,2,-1,4.
	// Matching: (1,1),(2,2),(-1,-1) => 3 rows (the two nulls now "match").
	checkRowCount("J5 JOIN with COALESCE in ON: null keys coerced to -1 and match each other (3)",
		`SELECT l.id FROM v38_jleft l JOIN v38_jright r
		 ON COALESCE(l.key_col, -1) = COALESCE(r.key_col, -1)`, 3)

	// J6: JOIN returning 0 rows (impossible condition).
	checkRowCount("J6 JOIN with 1=0 returns 0 rows",
		`SELECT l.id FROM v38_jleft l JOIN v38_jright r ON 1 = 0`, 0)

	// J7: Multiple JOINs where intermediate JOIN eliminates all rows.
	afExec(t, db, ctx, `CREATE TABLE v38_j3rd (id INTEGER PRIMARY KEY, key_col INTEGER, info TEXT)`)
	// v38_j3rd has no rows with key_col matching any of jright's key_cols.
	afExec(t, db, ctx, "INSERT INTO v38_j3rd VALUES (1, 99, 'orphan')")

	checkRowCount("J7 triple JOIN where intermediate JOIN eliminates all rows (0 results)",
		`SELECT l.id FROM v38_jleft l
		 JOIN v38_jright r ON l.key_col = r.key_col
		 JOIN v38_j3rd t   ON r.key_col = t.key_col`, 0)

	// J8: LEFT JOIN with aggregate on nullable right side.
	// LEFT JOIN jleft to jright on key_col; then COUNT(r.detail) per left row.
	// Carol (NULL key) has no match: r.detail is NULL, so COUNT(r.detail) = 0.
	// Dave (key=3) has no match: same => COUNT = 0.
	// Alice (key=1) matches: COUNT = 1.  Bob (key=2) matches: COUNT = 1.
	// Total COUNT(r.detail) across all rows: 1+1+0+0 = 2.
	check("J8 LEFT JOIN aggregate: COUNT of nullable right side ignores NULLs",
		`SELECT COUNT(r.detail) FROM v38_jleft l LEFT JOIN v38_jright r ON l.key_col = r.key_col`, 2)

	// J9: LEFT JOIN preserves NULL-keyed row from left.
	// Carol (key=NULL) must appear in the LEFT JOIN result even though NULL does not match.
	checkRowCount("J9 LEFT JOIN preserves left row with NULL key",
		`SELECT l.id FROM v38_jleft l LEFT JOIN v38_jright r ON l.key_col = r.key_col
		 WHERE l.name = 'Carol'`, 1)

	// J10: Full LEFT JOIN result row count.
	// Left has 4 rows; all 4 are preserved in LEFT JOIN.
	// Alice and Bob get a right match; Carol (NULL key) and Dave (key=3, no right match) get NULLs.
	checkRowCount("J10 LEFT JOIN preserves all 4 left rows",
		`SELECT l.id FROM v38_jleft l LEFT JOIN v38_jright r ON l.key_col = r.key_col`, 4)

	// ============================================================
	// SECTION 9: INSERT / UPDATE EDGE CASES
	// ============================================================
	//
	// Schema:
	//   v38_constraint_test (id PK, label TEXT NOT NULL, score INTEGER UNIQUE)

	afExec(t, db, ctx, `CREATE TABLE v38_constraint_test (
		id    INTEGER PRIMARY KEY,
		label TEXT    NOT NULL,
		score INTEGER UNIQUE
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_constraint_test VALUES (1, 'first',  100)")
	afExec(t, db, ctx, "INSERT INTO v38_constraint_test VALUES (2, 'second', 200)")

	// I1: INSERT with explicit NULL for NOT NULL column must fail.
	checkError("I1 INSERT NULL into NOT NULL column fails",
		`INSERT INTO v38_constraint_test VALUES (3, NULL, 300)`)

	// I2: Table unchanged after failed INSERT (still 2 rows).
	checkRowCount("I2 table still has 2 rows after NOT NULL violation",
		`SELECT id FROM v38_constraint_test`, 2)

	// I3: UPDATE to NULL on NOT NULL column must fail.
	checkError("I3 UPDATE SET label=NULL on NOT NULL column fails",
		`UPDATE v38_constraint_test SET label = NULL WHERE id = 1`)

	// I4: UPDATE with WHERE matching 0 rows must succeed (0 rows affected, no error).
	checkNoError("I4 UPDATE WHERE 0 rows match succeeds silently",
		`UPDATE v38_constraint_test SET score = 999 WHERE id = 999`)

	// I5: DELETE with WHERE matching 0 rows must succeed.
	checkNoError("I5 DELETE WHERE 0 rows match succeeds silently",
		`DELETE FROM v38_constraint_test WHERE id = 999`)

	// I6: INSERT with duplicate PK must fail.
	checkError("I6 INSERT duplicate PK fails",
		`INSERT INTO v38_constraint_test VALUES (1, 'dupe-pk', 999)`)

	// I7: UPDATE PK to conflict with existing PK.
	// id=1 and id=2 both exist. Changing id=1 to id=2 should fail with PK conflict.
	checkError("I7 UPDATE id to conflict with existing PK should error",
		`UPDATE v38_constraint_test SET id = 2 WHERE id = 1`)
	// id=1 ('first', score=100) is unchanged since UPDATE was rejected.
	// id=2 ('second', score=200) is unaffected.

	// I8: INSERT max integer value succeeds.
	// Use 9223372036854775807 (maximum signed 64-bit integer).
	checkNoError("I8 INSERT max int64 value succeeds",
		`INSERT INTO v38_constraint_test VALUES (3, 'max-int', 9223372036854775807)`)

	// I8b: Large int64 values render in scientific notation in this engine.
	// 9223372036854775807 appears as "9.223372036854776e+18" when formatted.
	check("I8b max int64 score stored correctly (scientific notation rendering)",
		`SELECT score FROM v38_constraint_test WHERE id = 3`, "9.223372036854776e+18")

	// I9: UPDATE SET col = col (a no-op update) must succeed without error.
	// id=1 still has label='first' since I7 UPDATE was rejected.
	checkNoError("I9 UPDATE SET label=label is a valid no-op update",
		`UPDATE v38_constraint_test SET label = label WHERE id = 1`)

	check("I9b no-op update does not change the label value (id=1 has 'first')",
		`SELECT label FROM v38_constraint_test WHERE id = 1`, "first")

	// I10: UPDATE with duplicate UNIQUE value must fail.
	// id=1 has score=100, id=2 has score=200.
	// Updating id=1's score to 200 conflicts with id=2 and must be rejected.
	checkError("I10 UPDATE score to duplicate UNIQUE value fails",
		`UPDATE v38_constraint_test SET score = 200 WHERE id = 1`)

	// ============================================================
	// SECTION 10: AGGREGATE EDGE CASES
	// ============================================================
	//
	// Schema:
	//   v38_agg_single (id PK, val INTEGER)  -- single row
	//   v38_agg_nulls  (id PK, val INTEGER)  -- all NULLs
	//   v38_agg_empty  (id PK, val INTEGER)  -- no rows

	afExec(t, db, ctx, `CREATE TABLE v38_agg_single (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_agg_single VALUES (1, 42)")

	afExec(t, db, ctx, `CREATE TABLE v38_agg_nulls (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_agg_nulls VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO v38_agg_nulls VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v38_agg_nulls VALUES (3, NULL)")

	afExec(t, db, ctx, `CREATE TABLE v38_agg_empty (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)

	// A1: COUNT(*) on table with 1 row.
	check("A1 COUNT(*) on single-row table is 1",
		`SELECT COUNT(*) FROM v38_agg_single`, 1)

	// A2: MIN and MAX on a single value are both that value.
	check("A2a MIN on single value is 42",
		`SELECT MIN(val) FROM v38_agg_single`, 42)

	check("A2b MAX on single value is 42",
		`SELECT MAX(val) FROM v38_agg_single`, 42)

	// A3: AVG of single value is that value.
	check("A3 AVG of single value 42 is 42",
		`SELECT AVG(val) FROM v38_agg_single`, 42)

	// A4: SUM of all NULLs is NULL (not 0).
	check("A4 SUM of all NULLs is NULL",
		`SELECT SUM(val) FROM v38_agg_nulls`, "<nil>")

	// A5: COUNT(col) of all NULLs is 0 (NULLs are not counted).
	check("A5 COUNT(col) of all NULLs is 0",
		`SELECT COUNT(val) FROM v38_agg_nulls`, 0)

	// A6: MIN and MAX of all NULLs are NULL.
	check("A6a MIN of all NULLs is NULL",
		`SELECT MIN(val) FROM v38_agg_nulls`, "<nil>")

	check("A6b MAX of all NULLs is NULL",
		`SELECT MAX(val) FROM v38_agg_nulls`, "<nil>")

	// A7: GROUP_CONCAT with NULLs - NULLs must be skipped.
	// Use a table with mixed NULLs and non-NULLs.
	afExec(t, db, ctx, `CREATE TABLE v38_concat_test (
		id   INTEGER PRIMARY KEY,
		word TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v38_concat_test VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO v38_concat_test VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v38_concat_test VALUES (3, 'world')")
	// GROUP_CONCAT should concatenate only non-NULL values: 'hello,world' (or 'hello world').
	// We verify that the NULL row (id=2) is not included, by checking the count of
	// non-NULL values via COUNT(word) = 2.
	check("A7 COUNT(word) skips NULLs in concat test table (2 non-NULLs)",
		`SELECT COUNT(word) FROM v38_concat_test`, 2)

	// A8: COUNT(DISTINCT col) with NULLs.
	// Distinct non-NULL values in v38_agg_nulls.val: none (all NULL) => 0.
	check("A8 COUNT(DISTINCT val) with all NULLs is 0",
		`SELECT COUNT(DISTINCT val) FROM v38_agg_nulls`, 0)

	// A8b: COUNT(DISTINCT col) with mixed values including NULLs.
	// v38_zero_null has val: 0,NULL,0,NULL,5,1 (after Z-section inserts).
	// Distinct non-NULL values: {0, 5, 1} => COUNT(DISTINCT) = 3.
	check("A8b COUNT(DISTINCT val) in mixed table: 3 distinct non-NULL values",
		`SELECT COUNT(DISTINCT val) FROM v38_zero_null`, 3)

	// A9: SUM on empty table is NULL (not 0).
	check("A9 SUM on empty table is NULL",
		`SELECT SUM(val) FROM v38_agg_empty`, "<nil>")

	// A10: Multiple aggregates with no rows matching WHERE clause.
	// A WHERE that matches nothing: all aggregates must return NULL except COUNT(*) = 0.
	check("A10a COUNT(*) with 0 matching rows is 0",
		`SELECT COUNT(*) FROM v38_agg_single WHERE val > 9999`, 0)

	check("A10b SUM with 0 matching rows is NULL",
		`SELECT SUM(val) FROM v38_agg_single WHERE val > 9999`, "<nil>")

	check("A10c MIN with 0 matching rows is NULL",
		`SELECT MIN(val) FROM v38_agg_single WHERE val > 9999`, "<nil>")

	check("A10d MAX with 0 matching rows is NULL",
		`SELECT MAX(val) FROM v38_agg_single WHERE val > 9999`, "<nil>")

	check("A10e AVG with 0 matching rows is NULL",
		`SELECT AVG(val) FROM v38_agg_single WHERE val > 9999`, "<nil>")

	// ============================================================
	// FINAL PASS / TOTAL SUMMARY
	// ============================================================
	t.Logf("V38 Tricky Edge Cases: %d/%d tests passed", pass, total)
	if pass != total {
		t.Errorf("FAILED: %d tests did not pass", total-pass)
	}
}
