package test

import (
	"fmt"
	"strings"
	"testing"
)

// TestV69StressExtreme pushes the database engine to its limits with deeply nested
// queries, large datasets, complex combinations, boundary conditions, and unusual SQL patterns.
func TestV69StressExtreme(t *testing.T) {
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

	// ============================================================
	// === LARGE DATASET GENERATION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v69_big (id INTEGER PRIMARY KEY, grp TEXT, sub_grp TEXT, val INTEGER, name TEXT)`)

	// Insert 100 rows across 5 groups x 4 sub-groups
	groups := []string{"A", "B", "C", "D", "E"}
	subGroups := []string{"x", "y", "z", "w"}
	names := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i := 0; i < 100; i++ {
		g := groups[i%5]
		sg := subGroups[i%4]
		n := names[i%5]
		v := (i + 1) * 10
		afExec(t, db, ctx, fmt.Sprintf(
			"INSERT INTO v69_big VALUES (%d, '%s', '%s', %d, '%s')", i+1, g, sg, v, n))
	}

	// ============================================================
	// === LARGE DATASET AGGREGATES ===
	// ============================================================

	// LA1: COUNT all
	check("LA1 COUNT all", "SELECT COUNT(*) FROM v69_big", 100)

	// LA2: SUM all
	check("LA2 SUM all", "SELECT SUM(val) FROM v69_big", 50500)
	// SUM(10+20+...+1000) = 100*1010/2 = 50500

	// LA3: AVG
	check("LA3 AVG all", "SELECT AVG(val) FROM v69_big", 505)

	// LA4: GROUP BY count
	checkRowCount("LA4 GROUP BY groups",
		"SELECT grp, COUNT(*) FROM v69_big GROUP BY grp", 5)

	// LA5: Each group has 20 rows
	check("LA5 group size",
		"SELECT COUNT(*) FROM v69_big WHERE grp = 'A'", 20)

	// LA6: GROUP BY with HAVING - check which groups exceed threshold
	checkRowCount("LA6 HAVING SUM > 10000",
		"SELECT grp, SUM(val) as s FROM v69_big GROUP BY grp HAVING SUM(val) > 10000", 3)
	// Groups have varied sums based on id distribution

	// ============================================================
	// === DEEPLY NESTED SUBQUERIES ===
	// ============================================================

	// DN1: 3-level nested subquery
	check("DN1 3-level nested",
		`SELECT COUNT(*) FROM v69_big
		 WHERE val > (
			SELECT AVG(val) FROM v69_big
			WHERE grp IN (
				SELECT DISTINCT grp FROM v69_big WHERE val > 500
			)
		 )`, 50)
	// AVG of all groups (all have val>500) = AVG(all) = 505 → >505 = 50 rows (val 510..1000)

	// DN2: Subquery in SELECT list
	check("DN2 subquery in SELECT",
		`SELECT (SELECT MAX(val) FROM v69_big) - (SELECT MIN(val) FROM v69_big)`, 990)
	// 1000 - 10 = 990

	// DN3: Correlated subquery with alias
	check("DN3 correlated above avg",
		`SELECT COUNT(*) FROM v69_big b1
		 WHERE val > (SELECT AVG(val) FROM v69_big b2 WHERE b2.grp = b1.grp)`, 50)

	// ============================================================
	// === COMPLEX CTE CHAINS ===
	// ============================================================

	// CC1: 4-level CTE chain
	check("CC1 4-level CTE",
		`WITH step1 AS (
			SELECT grp, SUM(val) as total FROM v69_big GROUP BY grp
		),
		step2 AS (
			SELECT grp, total, total * 100.0 / (SELECT SUM(total) FROM step1) as pct
			FROM step1
		),
		step3 AS (
			SELECT COUNT(*) as above_avg_groups FROM step2
			WHERE pct > 19.0
		),
		step4 AS (
			SELECT above_avg_groups FROM step3
		)
		SELECT above_avg_groups FROM step4`, 5)

	// CC2: CTE with JOIN
	check("CC2 CTE with JOIN",
		`WITH group_stats AS (
			SELECT grp, COUNT(*) as cnt, SUM(val) as total
			FROM v69_big GROUP BY grp
		)
		SELECT SUM(gs.total) FROM group_stats gs
		JOIN (SELECT DISTINCT grp FROM v69_big WHERE sub_grp = 'x') sg
		ON gs.grp = sg.grp`, 50500)
	// All groups have sub_grp='x', so all are included

	// CC3: CTE with window function
	check("CC3 CTE + window",
		`WITH ranked AS (
			SELECT id, val, ROW_NUMBER() OVER (ORDER BY val DESC) as rn
			FROM v69_big
		)
		SELECT val FROM ranked WHERE rn = 1`, 1000)

	// ============================================================
	// === COMPLEX JOIN + GROUP BY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v69_dept (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v69_dept VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO v69_dept VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO v69_dept VALUES (3, 'HR')")

	afExec(t, db, ctx, `CREATE TABLE v69_emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v69_emp VALUES (1, 'Alice', 1, 120000)")
	afExec(t, db, ctx, "INSERT INTO v69_emp VALUES (2, 'Bob', 1, 110000)")
	afExec(t, db, ctx, "INSERT INTO v69_emp VALUES (3, 'Carol', 2, 90000)")
	afExec(t, db, ctx, "INSERT INTO v69_emp VALUES (4, 'Dave', 2, 85000)")
	afExec(t, db, ctx, "INSERT INTO v69_emp VALUES (5, 'Eve', 3, 75000)")
	afExec(t, db, ctx, "INSERT INTO v69_emp VALUES (6, 'Frank', 1, 130000)")
	afExec(t, db, ctx, "INSERT INTO v69_emp VALUES (7, 'Grace', 3, 80000)")

	// JG1: JOIN with GROUP BY
	check("JG1 dept avg salary",
		`SELECT AVG(e.salary) FROM v69_emp e
		 JOIN v69_dept d ON e.dept_id = d.id
		 WHERE d.name = 'Engineering'`, 120000)
	// (120000+110000+130000)/3 = 120000

	// JG2: JOIN GROUP BY with HAVING
	checkRowCount("JG2 JOIN GROUP HAVING",
		`SELECT d.name, AVG(e.salary) as avg_sal
		 FROM v69_emp e
		 JOIN v69_dept d ON e.dept_id = d.id
		 GROUP BY d.name
		 HAVING AVG(e.salary) > 80000`, 2)
	// Eng: 120000, Sales: 87500, HR: 77500 → 2

	// JG3: Multiple aggregates in JOIN
	check("JG3 multiple aggs",
		`SELECT COUNT(*) FROM v69_emp e
		 JOIN v69_dept d ON e.dept_id = d.id
		 WHERE e.salary > (SELECT AVG(salary) FROM v69_emp)`, 3)
	// AVG = (120+110+90+85+75+130+80)*1000/7 ≈ 98571 → >98571: Alice(120k), Bob(110k), Frank(130k) = 3

	// ============================================================
	// === WINDOW FUNCTIONS OVER LARGE DATASET ===
	// ============================================================

	// WL1: ROW_NUMBER over entire dataset
	check("WL1 ROW_NUMBER last",
		`WITH numbered AS (
			SELECT id, val, ROW_NUMBER() OVER (ORDER BY val ASC) as rn
			FROM v69_big
		)
		SELECT rn FROM numbered WHERE id = 100`, 100)

	// WL2: Running SUM
	check("WL2 running SUM first",
		`SELECT SUM(val) OVER (ORDER BY id) FROM v69_big WHERE id = 1`, 10)

	// WL3: Partition ROW_NUMBER
	check("WL3 partition ROW_NUMBER",
		`WITH partitioned AS (
			SELECT id, grp, val,
				   ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) as rn
			FROM v69_big
		)
		SELECT COUNT(*) FROM partitioned WHERE rn = 1`, 5)
	// One top per group = 5

	// ============================================================
	// === COMPLEX BOOLEAN LOGIC ===
	// ============================================================

	// BL1: Complex boolean
	checkRowCount("BL1 complex boolean",
		`SELECT * FROM v69_big
		 WHERE (grp = 'A' OR grp = 'B')
		   AND (sub_grp = 'x' OR sub_grp = 'y')
		   AND val > 100`, 17)
	// A or B = 40 rows; x or y = half of those = ~20; val > 100 = most → count them
	// Actually: grp A has ids 1,6,11,16,21,26,31,36,41,46,51,56,61,66,71,76,81,86,91,96
	// grp B has ids 2,7,12,17,22,27,32,37,42,47,52,57,62,67,72,77,82,87,92,97
	// sub_grp x: i%4==0 (ids 1,5,9,...); y: i%4==1 (ids 2,6,10,...)
	// Let me just check the actual count

	// BL2: NOT IN list
	checkRowCount("BL2 NOT IN list",
		"SELECT * FROM v69_big WHERE grp NOT IN ('A', 'B', 'C')", 40)
	// D + E = 20 + 20 = 40

	// BL3: BETWEEN
	checkRowCount("BL3 BETWEEN",
		"SELECT * FROM v69_big WHERE val BETWEEN 400 AND 600", 21)
	// 400,410,...,600 = 21 values

	// ============================================================
	// === ORDER BY EDGE CASES ===
	// ============================================================

	// OE1: Multi-column ORDER BY
	check("OE1 multi ORDER",
		"SELECT val FROM v69_big ORDER BY grp ASC, val DESC LIMIT 1", 960)
	// Group A, highest val: id 96 has val=960

	// OE2: ORDER BY with NULL
	afExec(t, db, ctx, `CREATE TABLE v69_nullord (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v69_nullord VALUES (1, 30)")
	afExec(t, db, ctx, "INSERT INTO v69_nullord VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v69_nullord VALUES (3, 10)")
	afExec(t, db, ctx, "INSERT INTO v69_nullord VALUES (4, 20)")

	// OE3: ORDER BY DESC LIMIT - NULL sorts first in DESC (NULL is "largest")
	check("OE3 ORDER DESC LIMIT",
		"SELECT val FROM v69_nullord ORDER BY val DESC LIMIT 1", nil)

	// ============================================================
	// === UNION/INTERSECT/EXCEPT WITH LARGE DATA ===
	// ============================================================

	// UL1: UNION of filtered results
	check("UL1 UNION count",
		`SELECT COUNT(*) FROM (
			SELECT id FROM v69_big WHERE grp = 'A'
			UNION
			SELECT id FROM v69_big WHERE grp = 'B'
		) u`, 40)

	// UL2: INTERSECT on overlapping data
	check("UL2 INTERSECT count",
		`SELECT COUNT(*) FROM (
			SELECT val FROM v69_big WHERE grp = 'A'
			INTERSECT
			SELECT val FROM v69_big WHERE grp = 'B'
		) i`, 0)
	// Group A and B have different val values (different ids, val=id*10)

	// UL3: EXCEPT
	check("UL3 EXCEPT count",
		`SELECT COUNT(*) FROM (
			SELECT grp FROM v69_big
			EXCEPT
			SELECT grp FROM v69_big WHERE val > 500
		) e`, 0)
	// All groups have some val > 500, so all appear in right side

	// ============================================================
	// === MULTI-TABLE OPERATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v69_orders (id INTEGER PRIMARY KEY, emp_id INTEGER, amount INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v69_orders VALUES (1, 1, 5000)")
	afExec(t, db, ctx, "INSERT INTO v69_orders VALUES (2, 3, 3000)")
	afExec(t, db, ctx, "INSERT INTO v69_orders VALUES (3, 1, 7000)")
	afExec(t, db, ctx, "INSERT INTO v69_orders VALUES (4, 4, 2000)")
	afExec(t, db, ctx, "INSERT INTO v69_orders VALUES (5, 6, 8000)")

	// MT1: 3-table JOIN
	check("MT1 3-table JOIN",
		`SELECT SUM(o.amount) FROM v69_orders o
		 JOIN v69_emp e ON o.emp_id = e.id
		 JOIN v69_dept d ON e.dept_id = d.id
		 WHERE d.name = 'Engineering'`, 20000)
	// Eng employees: Alice(1), Bob(2), Frank(6)
	// Alice orders: 5000+7000=12000; Frank orders: 8000 → 20000

	// MT2: LEFT JOIN with aggregate
	check("MT2 LEFT JOIN agg",
		`SELECT COUNT(*) FROM v69_emp e
		 LEFT JOIN v69_orders o ON e.id = o.emp_id
		 WHERE o.id IS NULL`, 3)
	// Employees without orders: Bob(2), Eve(5), Grace(7) = 3

	// ============================================================
	// === INSERT/UPDATE/DELETE STRESS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v69_iud (id INTEGER PRIMARY KEY, val INTEGER, status TEXT)`)

	// IUD1: Bulk INSERT
	for i := 1; i <= 50; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v69_iud VALUES (%d, %d, 'active')", i, i*100))
	}
	check("IUD1 bulk count", "SELECT COUNT(*) FROM v69_iud", 50)

	// IUD2: UPDATE with condition
	checkNoError("IUD2 UPDATE", "UPDATE v69_iud SET status = 'inactive' WHERE val > 2500")
	check("IUD2 inactive count",
		"SELECT COUNT(*) FROM v69_iud WHERE status = 'inactive'", 25)

	// IUD3: DELETE half
	checkNoError("IUD3 DELETE", "DELETE FROM v69_iud WHERE status = 'inactive'")
	check("IUD3 remaining", "SELECT COUNT(*) FROM v69_iud", 25)

	// IUD4: UPDATE remaining
	checkNoError("IUD4 UPDATE all",
		"UPDATE v69_iud SET val = val + 1000")
	check("IUD4 verify min", "SELECT MIN(val) FROM v69_iud", 1100)

	// ============================================================
	// === EXPRESSION EDGE CASES ===
	// ============================================================

	// EE1: Deeply nested CASE
	check("EE1 nested CASE",
		`SELECT CASE
			WHEN 1 = 1 THEN
				CASE
					WHEN 2 = 2 THEN
						CASE WHEN 3 = 3 THEN 'deep' ELSE 'fail' END
					ELSE 'fail'
				END
			ELSE 'fail'
		 END`, "deep")

	// EE2: COALESCE chain
	check("EE2 COALESCE chain",
		"SELECT COALESCE(NULL, NULL, NULL, NULL, 42)", 42)

	// EE3: Complex arithmetic
	check("EE3 complex arith",
		"SELECT (100 + 50) * 2 - (30 / 3) + 7", 297)

	// EE4: String comparison in CASE
	check("EE4 string CASE",
		`SELECT CASE 'hello'
			WHEN 'hello' THEN 'matched'
			WHEN 'world' THEN 'wrong'
			ELSE 'none'
		 END`, "matched")

	// ============================================================
	// === SELF-JOIN ===
	// ============================================================

	// SJ1: Self-join to find employees in same dept
	check("SJ1 self-join",
		`SELECT COUNT(*) FROM v69_emp e1
		 JOIN v69_emp e2 ON e1.dept_id = e2.dept_id
		 WHERE e1.id < e2.id`, 5)
	// Eng: (1,2),(1,6),(2,6)=3; Sales: (3,4)=1; HR: (5,7)=1 → 5

	// SJ2: Self-join to find higher salary in same dept
	check("SJ2 higher salary pairs",
		`SELECT COUNT(*) FROM v69_emp e1
		 JOIN v69_emp e2 ON e1.dept_id = e2.dept_id AND e1.salary > e2.salary
		 WHERE e1.id != e2.id`, 5)
	// Eng: Frank>Alice, Frank>Bob, Alice>Bob = 3
	// Sales: Carol>Dave = 1
	// HR: Grace>Eve = 1
	// Total = 5

	// ============================================================
	// === COMPLEX STRING OPERATIONS ===
	// ============================================================

	// SO1: String length on query result
	check("SO1 LENGTH query",
		"SELECT LENGTH(name) FROM v69_emp WHERE id = 1", 5) // Alice

	// SO2: UPPER on column
	check("SO2 UPPER column",
		"SELECT UPPER(name) FROM v69_emp WHERE id = 1", "ALICE")

	// SO3: SUBSTR on column
	check("SO3 SUBSTR column",
		"SELECT SUBSTR(name, 1, 3) FROM v69_emp WHERE id = 1", "Ali")

	// SO4: Concatenation with column
	check("SO4 concat column",
		"SELECT name || ' - ' || dept_id FROM v69_emp WHERE id = 1", "Alice - 1")

	// ============================================================
	// === TABLE WITH MANY COLUMNS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v69_wide (
		id INTEGER PRIMARY KEY,
		a TEXT, b TEXT, c TEXT, d TEXT, e TEXT,
		f INTEGER, g INTEGER, h INTEGER, i INTEGER, j INTEGER
	)`)
	checkNoError("WD1 wide insert",
		"INSERT INTO v69_wide VALUES (1, 'a','b','c','d','e', 1,2,3,4,5)")
	check("WD1 verify a", "SELECT a FROM v69_wide WHERE id = 1", "a")
	check("WD1 verify j", "SELECT j FROM v69_wide WHERE id = 1", 5)
	check("WD1 sum nums", "SELECT f+g+h+i+j FROM v69_wide WHERE id = 1", 15)

	// ============================================================
	// === COMPLEX HAVING ===
	// ============================================================

	// CH1: HAVING with multiple conditions
	checkRowCount("CH1 HAVING multi",
		`SELECT grp, COUNT(*) as cnt, SUM(val) as total
		 FROM v69_big
		 GROUP BY grp
		 HAVING COUNT(*) >= 20 AND SUM(val) > 9000`, 5)

	// CH2: HAVING with expression
	checkRowCount("CH2 HAVING expr",
		`SELECT grp, AVG(val) as avg_val
		 FROM v69_big
		 GROUP BY grp
		 HAVING AVG(val) > 400`, 5)

	// ============================================================
	// === LARGE IN LIST ===
	// ============================================================

	// Generate large IN list
	var inVals []string
	for i := 1; i <= 30; i++ {
		inVals = append(inVals, fmt.Sprintf("%d", i*10))
	}
	inList := strings.Join(inVals, ", ")

	check("LI1 large IN list",
		fmt.Sprintf("SELECT COUNT(*) FROM v69_big WHERE val IN (%s)", inList), 30)

	// ============================================================
	// === COMBINED FEATURES ===
	// ============================================================

	// CF1: CTE + JOIN + Window + GROUP BY
	check("CF1 CTE+JOIN+Window",
		`WITH dept_totals AS (
			SELECT d.name as dept_name, SUM(e.salary) as total_salary
			FROM v69_emp e
			JOIN v69_dept d ON e.dept_id = d.id
			GROUP BY d.name
		),
		ranked AS (
			SELECT dept_name, total_salary,
				   ROW_NUMBER() OVER (ORDER BY total_salary DESC) as rn
			FROM dept_totals
		)
		SELECT dept_name FROM ranked WHERE rn = 1`, "Engineering")
	// Eng: 360000, Sales: 175000, HR: 155000

	// CF2: Subquery + aggregate + CASE
	check("CF2 subquery+agg+CASE",
		`SELECT SUM(CASE
			WHEN salary > (SELECT AVG(salary) FROM v69_emp) THEN 1
			ELSE 0
		 END)
		 FROM v69_emp`, 3)
	// AVG ≈ 98571; above: Alice(120k), Bob(110k), Frank(130k) = 3

	// CF3: Derived table with UNION
	check("CF3 derived UNION",
		`SELECT COUNT(*) FROM (
			SELECT name FROM v69_emp WHERE dept_id = 1
			UNION ALL
			SELECT name FROM v69_emp WHERE dept_id = 2
		) sub`, 5)
	// Eng: Alice, Bob, Frank = 3; Sales: Carol, Dave = 2 → 5

	t.Logf("\n=== V69 STRESS EXTREME: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
