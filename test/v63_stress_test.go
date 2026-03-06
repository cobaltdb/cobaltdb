package test

import (
	"fmt"
	"testing"
)

// TestV63Stress pushes CobaltDB with deeply nested subqueries, larger datasets,
// complex CTE chains, correlated subqueries with aliases, and multi-column sorting.
func TestV63Stress(t *testing.T) {
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

	// ============================================================
	// === LARGER DATASET ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v63_data (
		id INTEGER PRIMARY KEY, grp TEXT, sub_grp TEXT, val INTEGER, ts INTEGER)`)

	// Insert 50 rows across 5 groups x 2 sub-groups
	groups := []string{"A", "B", "C", "D", "E"}
	subGroups := []string{"x", "y"}
	id := 1
	for _, g := range groups {
		for _, sg := range subGroups {
			for i := 0; i < 5; i++ {
				val := id * 10
				ts := 2024*100 + id
				sql := fmt.Sprintf("INSERT INTO v63_data VALUES (%d, '%s', '%s', %d, %d)",
					id, g, sg, val, ts)
				afExec(t, db, ctx, sql)
				id++
			}
		}
	}
	// 50 rows total: 5 groups * 2 subgroups * 5 rows each

	// ============================================================
	// === LARGE DATASET QUERIES ===
	// ============================================================

	// LD1: Total rows
	check("LD1 total rows", "SELECT COUNT(*) FROM v63_data", 50)

	// LD2: Group counts
	checkRowCount("LD2 group counts",
		"SELECT grp, COUNT(*) FROM v63_data GROUP BY grp", 5)

	// LD3: Subgroup counts
	checkRowCount("LD3 subgroup counts",
		"SELECT grp, sub_grp, COUNT(*) FROM v63_data GROUP BY grp, sub_grp", 10)

	// LD4: Aggregate per group
	check("LD4 sum group A",
		"SELECT SUM(val) FROM v63_data WHERE grp = 'A'", 550)
	// Group A: ids 1-10, vals 10,20,...,100 → sum = 10+20+30+40+50+60+70+80+90+100=550

	// LD5: Global aggregates
	check("LD5 global sum",
		"SELECT SUM(val) FROM v63_data", 12750)
	// sum(10+20+...+500) = sum of 10*i for i=1..50 = 10*(50*51/2) = 10*1275 = 12750

	// ============================================================
	// === DEEPLY NESTED SUBQUERIES ===
	// ============================================================

	// DN1: 3-level nested subquery
	check("DN1 3-level nested",
		`SELECT COUNT(*) FROM v63_data
		 WHERE grp IN (
			SELECT grp FROM v63_data
			GROUP BY grp
			HAVING AVG(val) > (
				SELECT AVG(val) FROM v63_data
			)
		)`, 20)
	// Global AVG = 255. Group D(355), E(455) have avg > 255 → 2 groups × 10 rows = 20

	// DN2: Nested aggregate comparison
	check("DN2 nested agg compare",
		`SELECT MAX(val) FROM v63_data
		 WHERE val > (SELECT AVG(val) FROM v63_data WHERE grp = 'C')`, 500)
	// AVG of C = 255, MAX of all where val > 255 = 500

	// ============================================================
	// === COMPLEX CTE CHAINS (4 CTEs) ===
	// ============================================================

	// CC1: Four-level CTE
	check("CC1 four-level CTE",
		`WITH
		 grp_totals AS (
			SELECT grp, SUM(val) AS total FROM v63_data GROUP BY grp
		 ),
		 grp_ranked AS (
			SELECT grp, total, RANK() OVER (ORDER BY total DESC) AS rnk FROM grp_totals
		 ),
		 top_grps AS (
			SELECT grp FROM grp_ranked WHERE rnk <= 2
		 ),
		 top_data AS (
			SELECT COUNT(*) AS cnt FROM v63_data WHERE grp IN (SELECT grp FROM top_grps)
		 )
		 SELECT cnt FROM top_data`, 20)
	// Top 2 groups by total: E(4550), D(3550) → 20 rows

	// CC2: CTE with aggregate chaining
	check("CC2 CTE agg chain",
		`WITH
		 sub_totals AS (
			SELECT grp, sub_grp, SUM(val) AS total FROM v63_data GROUP BY grp, sub_grp
		 ),
		 grp_avgs AS (
			SELECT grp, AVG(total) AS avg_total FROM sub_totals GROUP BY grp
		 ),
		 best AS (
			SELECT grp FROM grp_avgs ORDER BY avg_total DESC LIMIT 1
		 )
		 SELECT grp FROM best`, "E")

	// ============================================================
	// === CORRELATED SUBQUERIES WITH ALIASES ===
	// ============================================================

	// CS1: Correlated subquery — rows above their group's average
	check("CS1 above group avg",
		`SELECT COUNT(*) FROM v63_data d
		 WHERE val > (SELECT AVG(val) FROM v63_data d2 WHERE d2.grp = d.grp)`, 25)
	// Each group has avg=middle. 5 above avg per group × 5 groups = 25

	// CS2: EXISTS with alias
	checkRowCount("CS2 EXISTS alias",
		`SELECT DISTINCT grp FROM v63_data d
		 WHERE EXISTS (
			SELECT 1 FROM v63_data d2
			WHERE d2.grp = d.grp AND d2.val > 400
		)`, 1)
	// Only group E has val > 400 (vals 410-500)

	// ============================================================
	// === MULTIPLE ORDER BY COLUMNS ===
	// ============================================================

	// MO1: ORDER BY two columns
	check("MO1 ORDER 2 cols",
		"SELECT id FROM v63_data ORDER BY grp ASC, val DESC LIMIT 1", 10)
	// First group A, highest val = 100 → id=10

	// MO2: ORDER BY with DESC/ASC mix
	check("MO2 ORDER mixed",
		"SELECT id FROM v63_data ORDER BY grp DESC, val ASC LIMIT 1", 41)
	// Last group E, lowest val = 410 → id=41

	// ============================================================
	// === LARGE IN LIST ===
	// ============================================================

	// LI1: IN with many values
	checkRowCount("LI1 large IN",
		`SELECT * FROM v63_data WHERE id IN (1,5,10,15,20,25,30,35,40,45,50)`, 11)

	// LI2: NOT IN with many values
	checkRowCount("LI2 large NOT IN",
		`SELECT * FROM v63_data WHERE id NOT IN (1,5,10,15,20,25,30,35,40,45,50)`, 39)

	// ============================================================
	// === WINDOW FUNCTIONS OVER LARGE DATASET ===
	// ============================================================

	// WL1: Running SUM over full dataset
	check("WL1 running sum last",
		`WITH w AS (
			SELECT id, val, SUM(val) OVER (ORDER BY id) AS running_total
			FROM v63_data
		)
		SELECT running_total FROM w WHERE id = 50`, 12750)
	// Running total at last row = total sum = 12750

	// WL2: RANK over full dataset
	check("WL2 RANK full",
		`WITH w AS (
			SELECT id, val, RANK() OVER (ORDER BY val DESC) AS rnk FROM v63_data
		)
		SELECT id FROM w WHERE rnk = 1`, 50)
	// Highest val = 500, id=50

	// WL3: ROW_NUMBER per partition
	check("WL3 ROW_NUMBER partition",
		`WITH w AS (
			SELECT id, grp, val,
				   ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) AS rn
			FROM v63_data
		)
		SELECT id FROM w WHERE grp = 'C' AND rn = 1`, 30)
	// Group C highest val = 300, id=30

	// WL4: Running SUM per partition
	check("WL4 partition running sum",
		`WITH w AS (
			SELECT id, grp, val,
				   SUM(val) OVER (PARTITION BY grp ORDER BY id) AS running
			FROM v63_data
		)
		SELECT running FROM w WHERE grp = 'A' AND id = 5`, 150)
	// Group A ids 1-5: vals 10+20+30+40+50=150

	// ============================================================
	// === COMPLEX BOOLEAN IN WHERE ===
	// ============================================================

	// CB1: Complex nested boolean
	checkRowCount("CB1 complex boolean",
		`SELECT * FROM v63_data
		 WHERE (grp = 'A' OR grp = 'E')
		 AND (sub_grp = 'x')
		 AND val > 50`, 5)
	// Group A sub_grp x: vals 10-50 → none > 50. Group E sub_grp x: 410-450 → all 5.

	// CB2: NOT with parentheses
	checkRowCount("CB2 NOT paren",
		"SELECT * FROM v63_data WHERE NOT (grp = 'A' OR grp = 'B')", 30)
	// Exclude A(10) and B(10) → 30 remaining

	// ============================================================
	// === MULTIPLE UNION ===
	// ============================================================

	// MU1: Multiple UNION ALL via CTE
	check("MU1 multi UNION ALL",
		`WITH combined AS (
			SELECT COUNT(*) AS cnt FROM v63_data WHERE grp = 'A'
			UNION ALL
			SELECT COUNT(*) AS cnt FROM v63_data WHERE grp = 'B'
			UNION ALL
			SELECT COUNT(*) AS cnt FROM v63_data WHERE grp = 'C'
		)
		SELECT SUM(cnt) FROM combined`, 30)

	// ============================================================
	// === COMPLEX JOIN + CTE + WINDOW ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v63_ref (grp TEXT PRIMARY KEY, multiplier INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v63_ref VALUES ('A', 1)")
	afExec(t, db, ctx, "INSERT INTO v63_ref VALUES ('B', 2)")
	afExec(t, db, ctx, "INSERT INTO v63_ref VALUES ('C', 3)")
	afExec(t, db, ctx, "INSERT INTO v63_ref VALUES ('D', 4)")
	afExec(t, db, ctx, "INSERT INTO v63_ref VALUES ('E', 5)")

	// CJW1: CTE from JOIN with window
	check("CJW1 CTE JOIN window",
		`WITH weighted AS (
			SELECT d.grp, d.val * r.multiplier AS weighted_val
			FROM v63_data d
			JOIN v63_ref r ON d.grp = r.grp
		),
		ranked AS (
			SELECT grp, weighted_val,
				   RANK() OVER (ORDER BY weighted_val DESC) AS rnk
			FROM weighted
		)
		SELECT grp FROM ranked WHERE rnk = 1`, "E")
	// E: val=500 * mult=5 = 2500 is max

	// CJW2: Aggregate of JOIN result
	check("CJW2 agg JOIN",
		`SELECT SUM(d.val * r.multiplier) FROM v63_data d
		 JOIN v63_ref r ON d.grp = r.grp
		 WHERE d.grp = 'E'`, 22750)
	// E vals: 410,420,430,440,450,460,470,480,490,500 * 5 each
	// = 5 * (410+420+430+440+450+460+470+480+490+500) = 5 * 4550 = 22750

	t.Logf("\n=== V63 STRESS TEST: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
