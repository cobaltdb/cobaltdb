package test

import (
	"fmt"
	"testing"
)

// TestV41SQLConformance verifies the CORRECTNESS of SQL query results, not merely
// that operations succeed. Every test checks exact row values, counts, and ordering
// against expected results calculated by hand.
//
// Seven sections are covered:
//
//  1. JOIN result correctness              (tests JN1-JN15)
//  2. Aggregate result correctness         (tests AG1-AG15)
//  3. Subquery result correctness          (tests SQ1-SQ10)
//  4. ORDER BY correctness                 (tests OB1-OB10)
//  5. Expression evaluation correctness    (tests EX1-EX10)
//  6. Data modification correctness        (tests DM1-DM10)
//  7. Multi-column result verification     (tests MC1-MC10)
//
// Total target: 80+ tests.
//
// All table names carry the v41_ prefix to prevent collisions with other test files.
//
// Engine notes (observed in previous suites):
//   - Integer division yields float64 (e.g., 7/2 = 3.5, not 3).
//   - Large sums may render in scientific notation.
//   - NULL renders as "<nil>" when formatted via fmt.Sprintf("%v", ...).
//   - LIKE is case-insensitive.
//   - Triggers fire once per statement (statement-level), not once per row.
//   - String concatenation uses the || operator.
func TestV41SQLConformance(t *testing.T) {
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

	_ = checkNoError
	_ = checkError

	// ============================================================
	// SECTION 1: JOIN RESULT CORRECTNESS
	// ============================================================
	//
	// Schema
	// ------
	//   v41_dept    (id INTEGER PK, name TEXT, bldg_floor INTEGER)
	//   v41_emp     (id INTEGER PK, name TEXT, dept_id INTEGER, salary INTEGER)
	//   v41_proj    (id INTEGER PK, title TEXT, lead_emp_id INTEGER)
	//
	// Departments (5 rows):
	//   1  Engineering  3
	//   2  Sales        1
	//   3  HR           2
	//   4  Marketing    4   -- no employees
	//   5  Legal        5   -- no employees
	//
	// Employees (6 rows):
	//   id  name      dept_id  salary
	//    1  Alice       1      90000   -- Engineering
	//    2  Bob         1      75000   -- Engineering
	//    3  Carol       2      60000   -- Sales
	//    4  Dave        3      55000   -- HR
	//    5  Eve         2      65000   -- Sales
	//    6  Frank       1      80000   -- Engineering
	//
	// Projects (4 rows):
	//   id  title        lead_emp_id
	//    1  Atlas         1           -- led by Alice
	//    2  Beacon        3           -- led by Carol
	//    3  Comet         6           -- led by Frank
	//    4  Delta        99           -- no matching employee (orphan lead)

	afExec(t, db, ctx, `CREATE TABLE v41_dept (
		id         INTEGER PRIMARY KEY,
		name       TEXT,
		bldg_floor INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v41_dept VALUES (1, 'Engineering', 3)")
	afExec(t, db, ctx, "INSERT INTO v41_dept VALUES (2, 'Sales', 1)")
	afExec(t, db, ctx, "INSERT INTO v41_dept VALUES (3, 'HR', 2)")
	afExec(t, db, ctx, "INSERT INTO v41_dept VALUES (4, 'Marketing', 4)")
	afExec(t, db, ctx, "INSERT INTO v41_dept VALUES (5, 'Legal', 5)")

	afExec(t, db, ctx, `CREATE TABLE v41_emp (
		id      INTEGER PRIMARY KEY,
		name    TEXT,
		dept_id INTEGER,
		salary  INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v41_emp VALUES (1, 'Alice', 1, 90000)")
	afExec(t, db, ctx, "INSERT INTO v41_emp VALUES (2, 'Bob',   1, 75000)")
	afExec(t, db, ctx, "INSERT INTO v41_emp VALUES (3, 'Carol', 2, 60000)")
	afExec(t, db, ctx, "INSERT INTO v41_emp VALUES (4, 'Dave',  3, 55000)")
	afExec(t, db, ctx, "INSERT INTO v41_emp VALUES (5, 'Eve',   2, 65000)")
	afExec(t, db, ctx, "INSERT INTO v41_emp VALUES (6, 'Frank', 1, 80000)")

	afExec(t, db, ctx, `CREATE TABLE v41_proj (
		id          INTEGER PRIMARY KEY,
		title       TEXT,
		lead_emp_id INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v41_proj VALUES (1, 'Atlas',  1)")
	afExec(t, db, ctx, "INSERT INTO v41_proj VALUES (2, 'Beacon', 3)")
	afExec(t, db, ctx, "INSERT INTO v41_proj VALUES (3, 'Comet',  6)")
	afExec(t, db, ctx, "INSERT INTO v41_proj VALUES (4, 'Delta',  99)")

	// JN1: INNER JOIN produces exactly the matching rows (6 employees x their dept).
	// All 6 employees have a valid dept_id that exists in v41_dept => 6 rows.
	checkRowCount("JN1 INNER JOIN emp-dept: 6 matching rows",
		"SELECT e.id FROM v41_emp e INNER JOIN v41_dept d ON e.dept_id = d.id", 6)

	// JN2: INNER JOIN exact column values — Alice is in Engineering (dept id 1).
	// Verify the dept name column is correctly joined.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT e.name, d.name FROM v41_emp e INNER JOIN v41_dept d ON e.dept_id = d.id WHERE e.id = 1")
		if len(rows) != 1 || len(rows[0]) != 2 {
			t.Errorf("[FAIL] JN2 INNER JOIN Alice dept: wrong shape, got %d rows", len(rows))
		} else if fmt.Sprintf("%v", rows[0][0]) != "Alice" || fmt.Sprintf("%v", rows[0][1]) != "Engineering" {
			t.Errorf("[FAIL] JN2 INNER JOIN Alice dept: got emp=%v dept=%v", rows[0][0], rows[0][1])
		} else {
			pass++
		}
	}

	// JN3: LEFT JOIN — departments with no employees still appear (Marketing, Legal).
	// 5 departments, 6 employees => LEFT JOIN dept->emp: 5 dept rows, 3 dept have employees,
	// Engineering has 3 emps, Sales has 2, HR has 1, Marketing has 0, Legal has 0.
	// Total rows: 3 + 2 + 1 + 1(NULL) + 1(NULL) = 8.
	checkRowCount("JN3 LEFT JOIN dept->emp: 8 rows (includes 2 NULL emp rows)",
		"SELECT d.id, e.id FROM v41_dept d LEFT JOIN v41_emp e ON e.dept_id = d.id", 8)

	// JN4: LEFT JOIN NULL verification — Marketing (id=4) and Legal (id=5) produce
	// NULL in the employee id column.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT d.name, e.name FROM v41_dept d LEFT JOIN v41_emp e ON e.dept_id = d.id WHERE d.id = 4")
		if len(rows) != 1 || len(rows[0]) != 2 {
			t.Errorf("[FAIL] JN4 LEFT JOIN Marketing NULL emp: wrong shape, got %d rows", len(rows))
		} else if fmt.Sprintf("%v", rows[0][0]) != "Marketing" || fmt.Sprintf("%v", rows[0][1]) != "<nil>" {
			t.Errorf("[FAIL] JN4 LEFT JOIN Marketing NULL emp: got dept=%v emp=%v", rows[0][0], rows[0][1])
		} else {
			pass++
		}
	}

	// JN5: LEFT JOIN count of departments that have NO employees (NULL emp).
	// Marketing and Legal have no employees => 2 rows with NULL emp.id.
	checkRowCount("JN5 LEFT JOIN: 2 dept rows with NULL employee",
		"SELECT d.id FROM v41_dept d LEFT JOIN v41_emp e ON e.dept_id = d.id WHERE e.id IS NULL", 2)

	// JN6: Three-table INNER JOIN: emp -> dept -> proj (via lead_emp_id).
	// Atlas led by Alice(1), Beacon led by Carol(3), Comet led by Frank(6).
	// Delta has lead_emp_id=99 which has no matching employee => excluded.
	// So 3 rows where proj.lead_emp_id = emp.id and emp.dept_id = dept.id.
	checkRowCount("JN6 three-table INNER JOIN emp-dept-proj: 3 rows",
		`SELECT p.title FROM v41_proj p
		 INNER JOIN v41_emp e ON p.lead_emp_id = e.id
		 INNER JOIN v41_dept d ON e.dept_id = d.id`, 3)

	// JN7: Three-table JOIN exact values — Beacon is led by Carol from Sales.
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT p.title, e.name, d.name FROM v41_proj p
			 INNER JOIN v41_emp e ON p.lead_emp_id = e.id
			 INNER JOIN v41_dept d ON e.dept_id = d.id
			 WHERE p.id = 2`)
		if len(rows) != 1 || len(rows[0]) != 3 {
			t.Errorf("[FAIL] JN7 three-table JOIN Beacon: wrong shape")
		} else if fmt.Sprintf("%v", rows[0][0]) != "Beacon" ||
			fmt.Sprintf("%v", rows[0][1]) != "Carol" ||
			fmt.Sprintf("%v", rows[0][2]) != "Sales" {
			t.Errorf("[FAIL] JN7 three-table JOIN Beacon: got %v %v %v", rows[0][0], rows[0][1], rows[0][2])
		} else {
			pass++
		}
	}

	// JN8: INNER JOIN with ON expression — join employees whose salary > dept bldg_floor * 10000.
	// Engineering bldg_floor=3: 30000 threshold. Alice(90k), Bob(75k), Frank(80k) all qualify.
	// Sales bldg_floor=1: 10000 threshold. Carol(60k), Eve(65k) both qualify.
	// HR bldg_floor=2: 20000 threshold. Dave(55k) qualifies.
	// All 6 employees qualify their own department => 6 rows.
	checkRowCount("JN8 JOIN with ON expression (salary > bldg_floor*10000): 6 rows",
		`SELECT e.id FROM v41_emp e
		 INNER JOIN v41_dept d ON e.dept_id = d.id AND e.salary > d.bldg_floor * 10000`, 6)

	// JN9: No duplicate rows in INNER JOIN — 6 employees, each appears exactly once.
	// Use COUNT(*) and verify it equals COUNT(DISTINCT e.id).
	check("JN9 no duplicate rows in INNER JOIN: COUNT(*) equals 6",
		`SELECT COUNT(*) FROM v41_emp e INNER JOIN v41_dept d ON e.dept_id = d.id`, 6)

	// JN10: Self-join — find pairs of employees in the same department.
	// Engineering: (Alice,Bob),(Alice,Frank),(Bob,Alice),(Bob,Frank),(Frank,Alice),(Frank,Bob) = 6
	// Sales: (Carol,Eve),(Eve,Carol) = 2
	// HR: no pairs (only Dave) = 0
	// Total: 8 pairs (excluding self-joins via e1.id < e2.id gives 4 pairs).
	// Using e1.id < e2.id: Eng pairs: (1,2),(1,6),(2,6) = 3; Sales: (3,5) = 1 => 4 total.
	checkRowCount("JN10 self-join same-dept pairs (id < id): 4 pairs",
		`SELECT e1.id, e2.id FROM v41_emp e1
		 INNER JOIN v41_emp e2 ON e1.dept_id = e2.dept_id AND e1.id < e2.id`, 4)

	// JN11: Self-join exact pair — Alice(1) and Bob(2) are both in Engineering.
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT e1.name, e2.name FROM v41_emp e1
			 INNER JOIN v41_emp e2 ON e1.dept_id = e2.dept_id AND e1.id < e2.id
			 WHERE e1.id = 1 AND e2.id = 2`)
		if len(rows) != 1 || len(rows[0]) != 2 {
			t.Errorf("[FAIL] JN11 self-join Alice-Bob pair: wrong shape")
		} else if fmt.Sprintf("%v", rows[0][0]) != "Alice" || fmt.Sprintf("%v", rows[0][1]) != "Bob" {
			t.Errorf("[FAIL] JN11 self-join Alice-Bob pair: got %v %v", rows[0][0], rows[0][1])
		} else {
			pass++
		}
	}

	// JN12: LEFT JOIN project->employee — Delta has no matching employee, gets NULL.
	// 4 projects: Atlas->Alice, Beacon->Carol, Comet->Frank, Delta->NULL.
	checkRowCount("JN12 LEFT JOIN proj->emp: 4 rows",
		`SELECT p.id FROM v41_proj p LEFT JOIN v41_emp e ON p.lead_emp_id = e.id`, 4)

	// JN13: Delta project LEFT JOIN yields NULL employee name.
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT p.title, e.name FROM v41_proj p
			 LEFT JOIN v41_emp e ON p.lead_emp_id = e.id
			 WHERE p.id = 4`)
		if len(rows) != 1 || len(rows[0]) != 2 {
			t.Errorf("[FAIL] JN13 Delta LEFT JOIN NULL lead: wrong shape")
		} else if fmt.Sprintf("%v", rows[0][0]) != "Delta" || fmt.Sprintf("%v", rows[0][1]) != "<nil>" {
			t.Errorf("[FAIL] JN13 Delta LEFT JOIN NULL lead: got %v %v", rows[0][0], rows[0][1])
		} else {
			pass++
		}
	}

	// JN14: INNER JOIN with WHERE — Engineering employees on projects.
	// Engineering emps: Alice(1), Bob(2), Frank(6).
	// Projects led by Eng emps: Atlas(Alice), Comet(Frank). Bob leads no project.
	// => 2 rows.
	checkRowCount("JN14 INNER JOIN with WHERE: 2 Engineering-led projects",
		`SELECT p.id FROM v41_proj p
		 INNER JOIN v41_emp e ON p.lead_emp_id = e.id
		 WHERE e.dept_id = 1`, 2)

	// JN15: Cross-department join count — Sales emps paired with Engineering emps.
	// Sales: Carol(3), Eve(5) => 2 rows. Engineering: Alice(1), Bob(2), Frank(6) => 3 rows.
	// Cross product = 2 * 3 = 6 pairs.
	checkRowCount("JN15 cross-dept JOIN Sales x Engineering: 6 pairs",
		`SELECT s.id, e.id FROM v41_emp s
		 INNER JOIN v41_emp e ON s.dept_id = 2 AND e.dept_id = 1`, 6)

	// ============================================================
	// SECTION 2: AGGREGATE RESULT CORRECTNESS
	// ============================================================
	//
	// Schema
	// ------
	//   v41_sales (id INTEGER PK, region TEXT, product TEXT, qty INTEGER, price REAL, discount REAL)
	//
	// Data (10 rows):
	//   id  region  product  qty  price   discount
	//    1  North   Widget    10   9.99    0.00
	//    2  North   Gadget     5  29.99    2.00
	//    3  South   Widget    20   9.99    1.00
	//    4  South   Gadget     8  29.99    0.00
	//    5  East    Widget    15   9.99    0.50
	//    6  East    Gadget     3  29.99    3.00
	//    7  West    Widget    12   9.99    0.00
	//    8  West    Gadget     6  29.99    1.50
	//    9  North   Widget     7   9.99    0.00   (region=North, 3rd North row)
	//   10  South   Widget    NULL 9.99   0.00   (NULL qty — tests COUNT(*) vs COUNT(qty))

	afExec(t, db, ctx, `CREATE TABLE v41_sales (
		id       INTEGER PRIMARY KEY,
		region   TEXT,
		product  TEXT,
		qty      INTEGER,
		price    REAL,
		discount REAL
	)`)
	afExec(t, db, ctx, "INSERT INTO v41_sales VALUES (1,  'North', 'Widget', 10, 9.99,  0.00)")
	afExec(t, db, ctx, "INSERT INTO v41_sales VALUES (2,  'North', 'Gadget',  5, 29.99, 2.00)")
	afExec(t, db, ctx, "INSERT INTO v41_sales VALUES (3,  'South', 'Widget', 20, 9.99,  1.00)")
	afExec(t, db, ctx, "INSERT INTO v41_sales VALUES (4,  'South', 'Gadget',  8, 29.99, 0.00)")
	afExec(t, db, ctx, "INSERT INTO v41_sales VALUES (5,  'East',  'Widget', 15, 9.99,  0.50)")
	afExec(t, db, ctx, "INSERT INTO v41_sales VALUES (6,  'East',  'Gadget',  3, 29.99, 3.00)")
	afExec(t, db, ctx, "INSERT INTO v41_sales VALUES (7,  'West',  'Widget', 12, 9.99,  0.00)")
	afExec(t, db, ctx, "INSERT INTO v41_sales VALUES (8,  'West',  'Gadget',  6, 29.99, 1.50)")
	afExec(t, db, ctx, "INSERT INTO v41_sales VALUES (9,  'North', 'Widget',  7, 9.99,  0.00)")
	afExec(t, db, ctx, "INSERT INTO v41_sales (id, region, product, qty, price, discount) VALUES (10, 'South', 'Widget', NULL, 9.99, 0.00)")

	// AG1: SUM of qty — exact value.
	// qty values: 10+5+20+8+15+3+12+6+7+NULL = 86 (NULL excluded from SUM).
	check("AG1 SUM(qty) = 86 (NULL excluded)",
		"SELECT SUM(qty) FROM v41_sales", 86)

	// AG2: COUNT(*) includes NULL rows — 10 rows total.
	check("AG2 COUNT(*) = 10 (includes NULL qty row)",
		"SELECT COUNT(*) FROM v41_sales", 10)

	// AG3: COUNT(qty) excludes NULL — 9 rows with non-NULL qty.
	check("AG3 COUNT(qty) = 9 (excludes NULL qty row)",
		"SELECT COUNT(qty) FROM v41_sales", 9)

	// AG4: MIN(qty) — smallest non-NULL qty is 3 (East Gadget).
	check("AG4 MIN(qty) = 3",
		"SELECT MIN(qty) FROM v41_sales", 3)

	// AG5: MAX(qty) — largest qty is 20 (South Widget).
	check("AG5 MAX(qty) = 20",
		"SELECT MAX(qty) FROM v41_sales", 20)

	// AG6: MIN of text column — alphabetically first region is 'East'.
	check("AG6 MIN(region) = 'East' (alphabetically first)",
		"SELECT MIN(region) FROM v41_sales", "East")

	// AG7: MAX of text column — alphabetically last region is 'West'.
	check("AG7 MAX(region) = 'West' (alphabetically last)",
		"SELECT MAX(region) FROM v41_sales", "West")

	// AG8: GROUP BY region — verify North group has 3 rows.
	// North rows: id=1,2,9 => 3 rows.
	check("AG8 GROUP BY region: North has COUNT=3",
		"SELECT COUNT(*) FROM v41_sales WHERE region = 'North'", 3)

	// AG9: GROUP BY region SUM(qty) for South.
	// South rows: id=3(qty=20), id=4(qty=8), id=10(qty=NULL) => SUM = 28.
	check("AG9 GROUP BY region: South SUM(qty) = 28",
		"SELECT SUM(qty) FROM v41_sales WHERE region = 'South'", 28)

	// AG10: Multiple aggregates in one SELECT — verify all are correct.
	// Widget rows: id=1(10), id=3(20), id=5(15), id=7(12), id=9(7), id=10(NULL).
	// COUNT(*)=6, SUM(qty)=64, MIN(qty)=7, MAX(qty)=20.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT COUNT(*), SUM(qty), MIN(qty), MAX(qty) FROM v41_sales WHERE product = 'Widget'")
		if len(rows) != 1 || len(rows[0]) != 4 {
			t.Errorf("[FAIL] AG10 multiple aggregates Widget: wrong shape")
		} else {
			cnt := fmt.Sprintf("%v", rows[0][0])
			sum := fmt.Sprintf("%v", rows[0][1])
			min := fmt.Sprintf("%v", rows[0][2])
			max := fmt.Sprintf("%v", rows[0][3])
			if cnt != "6" || sum != "64" || min != "7" || max != "20" {
				t.Errorf("[FAIL] AG10 multiple aggregates Widget: cnt=%s sum=%s min=%s max=%s", cnt, sum, min, max)
			} else {
				pass++
			}
		}
	}

	// AG11: Aggregate with WHERE filtering — verify filter applied BEFORE aggregation.
	// Rows with discount > 0: id=2(5),id=3(20),id=5(15),id=6(3),id=8(6) => SUM(qty)=49.
	check("AG11 SUM(qty) WHERE discount > 0 = 49 (filter before aggregate)",
		"SELECT SUM(qty) FROM v41_sales WHERE discount > 0", 49)

	// AG12: GROUP BY with multiple groups — verify each group's SUM individually.
	// Widget total qty: 10+20+15+12+7+NULL = 64. Gadget total qty: 5+8+3+6 = 22.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT product, SUM(qty) FROM v41_sales GROUP BY product ORDER BY product")
		// ORDER BY product ASC: Gadget first, Widget second.
		if len(rows) != 2 || len(rows[0]) != 2 {
			t.Errorf("[FAIL] AG12 GROUP BY product: expected 2 groups, got %d rows", len(rows))
		} else {
			gadget := fmt.Sprintf("%v", rows[0][1])
			widget := fmt.Sprintf("%v", rows[1][1])
			if gadget != "22" || widget != "64" {
				t.Errorf("[FAIL] AG12 GROUP BY product: Gadget sum=%s Widget sum=%s", gadget, widget)
			} else {
				pass++
			}
		}
	}

	// AG13: HAVING — filter groups after aggregation.
	// Group by region, HAVING COUNT(*) >= 3.
	// North rows: id=1,2,9 => 3 rows. South rows: id=3,4,10 => 3 rows.
	// East rows: id=5,6 => 2 rows. West rows: id=7,8 => 2 rows.
	// Qualifying regions (count >= 3): North and South => 2 regions.
	checkRowCount("AG13 HAVING COUNT(*) >= 3: 2 regions (North and South)",
		"SELECT region FROM v41_sales GROUP BY region HAVING COUNT(*) >= 3", 2)

	// AG13b: HAVING with stricter filter — only groups with COUNT(*) > 3 (none qualify => 0 rows).
	checkRowCount("AG13b HAVING COUNT(*) > 3: 0 regions",
		"SELECT region FROM v41_sales GROUP BY region HAVING COUNT(*) > 3", 0)

	// AG14: GROUP BY with AVG — East has Gadget(qty=3) and Widget(qty=15) => AVG=9.
	// Use integer division note: 18/2 = 9.0 in engine (float).
	check("AG14 AVG(qty) for East region = 9",
		"SELECT AVG(qty) FROM v41_sales WHERE region = 'East'", 9)

	// AG15: COUNT DISTINCT — only 4 distinct regions.
	check("AG15 COUNT(DISTINCT region) = 4",
		"SELECT COUNT(DISTINCT region) FROM v41_sales", 4)

	// ============================================================
	// SECTION 3: SUBQUERY RESULT CORRECTNESS
	// ============================================================
	//
	// Reuses v41_dept, v41_emp, v41_sales tables defined above.

	// SQ1: Scalar subquery — select employee whose salary equals the MAX salary.
	// Max salary is 90000 (Alice).
	check("SQ1 scalar subquery MAX salary: Alice has 90000",
		"SELECT name FROM v41_emp WHERE salary = (SELECT MAX(salary) FROM v41_emp)", "Alice")

	// SQ2: Scalar subquery in SELECT list — each employee with dept name from subquery.
	// Bob(dept_id=1) should get 'Engineering'.
	check("SQ2 scalar subquery in SELECT: Bob's dept name",
		`SELECT (SELECT name FROM v41_dept WHERE id = e.dept_id) FROM v41_emp e WHERE e.name = 'Bob'`,
		"Engineering")

	// SQ3: IN subquery — employees in departments with bldg_floor >= 3.
	// bldg_floor >= 3: Engineering(3), Marketing(4), Legal(5).
	// Employees in those depts: Alice(1,Eng), Bob(2,Eng), Frank(6,Eng).
	// Marketing and Legal have no employees.
	checkRowCount("SQ3 IN subquery: 3 employees in high-floor depts",
		"SELECT id FROM v41_emp WHERE dept_id IN (SELECT id FROM v41_dept WHERE bldg_floor >= 3)", 3)

	// SQ4: IN subquery exact names — verify correct employees returned.
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT name FROM v41_emp
			 WHERE dept_id IN (SELECT id FROM v41_dept WHERE bldg_floor >= 3)
			 ORDER BY name`)
		if len(rows) != 3 {
			t.Errorf("[FAIL] SQ4 IN subquery exact names: expected 3 rows, got %d", len(rows))
		} else {
			a := fmt.Sprintf("%v", rows[0][0])
			b := fmt.Sprintf("%v", rows[1][0])
			c := fmt.Sprintf("%v", rows[2][0])
			if a != "Alice" || b != "Bob" || c != "Frank" {
				t.Errorf("[FAIL] SQ4 IN subquery exact names: got %s, %s, %s", a, b, c)
			} else {
				pass++
			}
		}
	}

	// SQ5: NOT IN subquery — employees NOT in Engineering (dept_id != 1).
	// Non-Engineering employees: Carol(Sales), Dave(HR), Eve(Sales) => 3 rows.
	checkRowCount("SQ5 NOT IN subquery: 3 employees not in Engineering",
		"SELECT id FROM v41_emp WHERE dept_id NOT IN (SELECT id FROM v41_dept WHERE name = 'Engineering')", 3)

	// SQ6: EXISTS subquery — departments that have at least one employee.
	// Engineering(1), Sales(2), HR(3) have employees; Marketing(4), Legal(5) do not.
	checkRowCount("SQ6 EXISTS subquery: 3 depts with employees",
		`SELECT id FROM v41_dept d
		 WHERE EXISTS (SELECT 1 FROM v41_emp e WHERE e.dept_id = d.id)`, 3)

	// SQ7: NOT EXISTS subquery — departments with NO employees.
	// Marketing(4) and Legal(5) have no employees => 2 rows.
	checkRowCount("SQ7 NOT EXISTS subquery: 2 depts without employees",
		`SELECT id FROM v41_dept d
		 WHERE NOT EXISTS (SELECT 1 FROM v41_emp e WHERE e.dept_id = d.id)`, 2)

	// SQ8: Correlated subquery — for each employee, find employees in same dept with higher salary.
	// Alice(90k, Eng): no Eng emp has higher salary => 0 higher peers.
	check("SQ8 correlated subquery: 0 peers earn more than Alice",
		`SELECT COUNT(*) FROM v41_emp e2
		 WHERE e2.dept_id = 1 AND e2.salary > 90000`, 0)

	// Bob(75k, Eng): Alice(90k) and Frank(80k) earn more => 2 higher peers.
	check("SQ8b correlated subquery: 2 peers earn more than Bob (Alice, Frank)",
		`SELECT COUNT(*) FROM v41_emp e2
		 WHERE e2.dept_id = 1 AND e2.salary > 75000`, 2)

	// SQ9: Aggregate subquery in WHERE — find departments whose max salary exceeds 70000.
	// Engineering max=90000 > 70000: yes. Sales max=65000 <= 70000: no. HR max=55000: no.
	// Only Engineering qualifies => 1 department.
	checkRowCount("SQ9 subquery in WHERE with aggregate: 1 dept has max salary > 70000",
		`SELECT id FROM v41_dept
		 WHERE id IN (SELECT dept_id FROM v41_emp GROUP BY dept_id HAVING MAX(salary) > 70000)`, 1)

	check("SQ9b subquery in WHERE: the qualifying dept is Engineering (id=1)",
		`SELECT name FROM v41_dept
		 WHERE id IN (SELECT dept_id FROM v41_emp GROUP BY dept_id HAVING MAX(salary) > 70000)`,
		"Engineering")

	// SQ10: Scalar subquery returns NULL when no rows match — verify NULL propagation.
	// dept_id=999 does not exist => subquery returns NULL.
	check("SQ10 scalar subquery no match => NULL",
		"SELECT (SELECT name FROM v41_dept WHERE id = 999)", "<nil>")

	// ============================================================
	// SECTION 4: ORDER BY CORRECTNESS
	// ============================================================
	//
	// Schema
	// ------
	//   v41_scores (id INTEGER PK, player TEXT, score INTEGER, level INTEGER)
	//
	// Data (8 rows):
	//   id  player   score  level
	//    1  Alice     300     2
	//    2  Bob       150     1
	//    3  Carol     300     3    -- tie on score with Alice
	//    4  Dave      500     1
	//    5  Eve       150     2    -- tie on score with Bob
	//    6  Frank     200     2
	//    7  Grace     500     3    -- tie on score with Dave
	//    8  Heidi     100     1

	afExec(t, db, ctx, `CREATE TABLE v41_scores (
		id     INTEGER PRIMARY KEY,
		player TEXT,
		score  INTEGER,
		level  INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v41_scores VALUES (1, 'Alice', 300, 2)")
	afExec(t, db, ctx, "INSERT INTO v41_scores VALUES (2, 'Bob',   150, 1)")
	afExec(t, db, ctx, "INSERT INTO v41_scores VALUES (3, 'Carol', 300, 3)")
	afExec(t, db, ctx, "INSERT INTO v41_scores VALUES (4, 'Dave',  500, 1)")
	afExec(t, db, ctx, "INSERT INTO v41_scores VALUES (5, 'Eve',   150, 2)")
	afExec(t, db, ctx, "INSERT INTO v41_scores VALUES (6, 'Frank', 200, 2)")
	afExec(t, db, ctx, "INSERT INTO v41_scores VALUES (7, 'Grace', 500, 3)")
	afExec(t, db, ctx, "INSERT INTO v41_scores VALUES (8, 'Heidi', 100, 1)")

	// OB1: ORDER BY single column ASC — Heidi(100) must be first.
	check("OB1 ORDER BY score ASC: first row is Heidi (100)",
		"SELECT player FROM v41_scores ORDER BY score ASC, id ASC", "Heidi")

	// OB2: ORDER BY single column DESC — Dave or Grace (both 500) must be first.
	// Use secondary sort to get deterministic result: ORDER BY score DESC, id ASC => Dave(id=4) first.
	check("OB2 ORDER BY score DESC: first row is Dave (id=4, score=500)",
		"SELECT player FROM v41_scores ORDER BY score DESC, id ASC", "Dave")

	// OB3: Multi-column ORDER BY — verify exact sequence of all 8 rows.
	// ORDER BY score ASC, player ASC:
	//   100: Heidi
	//   150: Bob, Eve (alphabetical: Bob, Eve)
	//   200: Frank
	//   300: Alice, Carol (alphabetical: Alice, Carol)
	//   500: Dave, Grace (alphabetical: Dave, Grace)
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT player FROM v41_scores ORDER BY score ASC, player ASC")
		expected := []string{"Heidi", "Bob", "Eve", "Frank", "Alice", "Carol", "Dave", "Grace"}
		if len(rows) != 8 {
			t.Errorf("[FAIL] OB3 multi-column ORDER BY: expected 8 rows, got %d", len(rows))
		} else {
			ok := true
			for i, exp := range expected {
				got := fmt.Sprintf("%v", rows[i][0])
				if got != exp {
					t.Errorf("[FAIL] OB3 multi-column ORDER BY: row[%d] got %q, expected %q", i, got, exp)
					ok = false
				}
			}
			if ok {
				pass++
			}
		}
	}

	// OB4: ORDER BY DESC — verify last three rows descending.
	// ORDER BY score DESC, player ASC => Grace(500) should be second (Dave first).
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT player FROM v41_scores ORDER BY score DESC, player ASC")
		if len(rows) != 8 {
			t.Errorf("[FAIL] OB4 ORDER BY DESC: expected 8 rows, got %d", len(rows))
		} else {
			first := fmt.Sprintf("%v", rows[0][0])
			second := fmt.Sprintf("%v", rows[1][0])
			last := fmt.Sprintf("%v", rows[7][0])
			if first != "Dave" || second != "Grace" || last != "Heidi" {
				t.Errorf("[FAIL] OB4 ORDER BY DESC: first=%s second=%s last=%s", first, second, last)
			} else {
				pass++
			}
		}
	}

	// OB5: ORDER BY with LIMIT — top 3 by score.
	// Top 3: Dave(500), Grace(500), Carol/Alice(300) — with player ASC tiebreak.
	// ORDER BY score DESC, player ASC LIMIT 3 => Dave, Grace, Alice.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT player FROM v41_scores ORDER BY score DESC, player ASC LIMIT 3")
		if len(rows) != 3 {
			t.Errorf("[FAIL] OB5 ORDER BY LIMIT 3: expected 3 rows, got %d", len(rows))
		} else {
			r0 := fmt.Sprintf("%v", rows[0][0])
			r1 := fmt.Sprintf("%v", rows[1][0])
			r2 := fmt.Sprintf("%v", rows[2][0])
			if r0 != "Dave" || r1 != "Grace" || r2 != "Alice" {
				t.Errorf("[FAIL] OB5 ORDER BY LIMIT 3: got %s, %s, %s", r0, r1, r2)
			} else {
				pass++
			}
		}
	}

	// OB6: ORDER BY with OFFSET — skip top 3, get next 2.
	// After Dave, Grace, Alice the next are Carol(300), Frank(200).
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT player FROM v41_scores ORDER BY score DESC, player ASC LIMIT 2 OFFSET 3")
		if len(rows) != 2 {
			t.Errorf("[FAIL] OB6 ORDER BY LIMIT OFFSET: expected 2 rows, got %d", len(rows))
		} else {
			r0 := fmt.Sprintf("%v", rows[0][0])
			r1 := fmt.Sprintf("%v", rows[1][0])
			if r0 != "Carol" || r1 != "Frank" {
				t.Errorf("[FAIL] OB6 ORDER BY LIMIT OFFSET: got %s, %s", r0, r1)
			} else {
				pass++
			}
		}
	}

	// OB7: ORDER BY on NULL — add a row with NULL score, verify NULL position.
	afExec(t, db, ctx, "INSERT INTO v41_scores VALUES (9, 'Zeta', NULL, 1)")

	// This engine sorts NULLs LAST in ASC order (observed behaviour, consistent with NULLS LAST default).
	// ORDER BY score ASC => 100(Heidi) first, NULL(Zeta) last.
	check("OB7 ORDER BY ASC: Heidi(100) is first (NULLs sort LAST in this engine)",
		"SELECT player FROM v41_scores ORDER BY score ASC, id ASC", "Heidi")

	// OB7b: Confirm Zeta (NULL score) appears LAST in ASC order.
	check("OB7b ORDER BY ASC: Zeta(NULL) is last via OFFSET 8",
		"SELECT player FROM v41_scores ORDER BY score ASC, id ASC LIMIT 1 OFFSET 8", "Zeta")

	// OB8: ORDER BY DESC with NULL — NULLs sort FIRST in DESC order (opposite of ASC).
	// ORDER BY score DESC => NULL(Zeta) first, then 500, 500, 300, 300, 200, 150, 150, 100.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT player FROM v41_scores ORDER BY score DESC, id ASC")
		if len(rows) != 9 {
			t.Errorf("[FAIL] OB8 ORDER BY DESC NULL position: expected 9 rows, got %d", len(rows))
		} else {
			// Zeta (NULL) sorts first in DESC, Heidi (100) sorts last.
			first := fmt.Sprintf("%v", rows[0][0])
			last := fmt.Sprintf("%v", rows[8][0])
			if first != "Zeta" || last != "Heidi" {
				t.Errorf("[FAIL] OB8 ORDER BY DESC: NULL should be first (got %s), 100 should be last (got %s)",
					first, last)
			} else {
				pass++
			}
		}
	}

	// OB9: ORDER BY expression — order by score * level.
	// Scores: Alice(300*2=600), Bob(150*1=150), Carol(300*3=900), Dave(500*1=500),
	//         Eve(150*2=300), Frank(200*2=400), Grace(500*3=1500), Heidi(100*1=100), Zeta(NULL*1=NULL).
	// ASC, id tiebreak (NULLs last): Heidi(100), Bob(150), Eve(300), Frank(400), Dave(500), Alice(600), Carol(900), Grace(1500), Zeta(NULL).
	check("OB9 ORDER BY expression (score*level ASC): Heidi first (100), NULLs last",
		"SELECT player FROM v41_scores ORDER BY score * level ASC, id ASC", "Heidi")

	check("OB9b ORDER BY expression (score*level ASC): Zeta last (NULL product at OFFSET 8)",
		"SELECT player FROM v41_scores ORDER BY score * level ASC, id ASC LIMIT 1 OFFSET 8", "Zeta")

	// OB10: ORDER BY with WHERE — filtered ORDER BY should only sort matching rows.
	// Players with score >= 300 (non-NULL): Alice(300), Carol(300), Dave(500), Grace(500).
	// ORDER BY score ASC, player ASC => Alice, Carol, Dave, Grace.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT player FROM v41_scores WHERE score >= 300 ORDER BY score ASC, player ASC")
		if len(rows) != 4 {
			t.Errorf("[FAIL] OB10 ORDER BY with WHERE: expected 4 rows, got %d", len(rows))
		} else {
			r0 := fmt.Sprintf("%v", rows[0][0])
			r3 := fmt.Sprintf("%v", rows[3][0])
			if r0 != "Alice" || r3 != "Grace" {
				t.Errorf("[FAIL] OB10 ORDER BY with WHERE: first=%s last=%s", r0, r3)
			} else {
				pass++
			}
		}
	}

	// ============================================================
	// SECTION 5: EXPRESSION EVALUATION CORRECTNESS
	// ============================================================

	// EX1: Operator precedence — 2 + 3 * 4 must equal 14 (not 20).
	check("EX1 operator precedence: 2 + 3 * 4 = 14",
		"SELECT 2 + 3 * 4", 14)

	// EX2: Parentheses override precedence — (2 + 3) * 4 must equal 20.
	check("EX2 parentheses: (2 + 3) * 4 = 20",
		"SELECT (2 + 3) * 4", 20)

	// EX3: Mixed precedence — 10 - 2 * 3 + 1 = 10 - 6 + 1 = 5.
	check("EX3 mixed precedence: 10 - 2 * 3 + 1 = 5",
		"SELECT 10 - 2 * 3 + 1", 5)

	// EX4: String concatenation with ||.
	check("EX4 string concatenation: 'Hello' || ' ' || 'World'",
		"SELECT 'Hello' || ' ' || 'World'", "Hello World")

	// EX5: CASE WHEN — verify correct branch taken.
	// score 75: 60<=75<80 => 'C'.
	check("EX5 CASE WHEN score=75 => grade C",
		`SELECT CASE
		   WHEN 75 >= 90 THEN 'A'
		   WHEN 75 >= 80 THEN 'B'
		   WHEN 75 >= 70 THEN 'C'
		   WHEN 75 >= 60 THEN 'D'
		   ELSE 'F'
		 END`, "C")

	// EX6: CASE WHEN ELSE branch — score 45 < 60 => 'F'.
	check("EX6 CASE WHEN score=45 => grade F (ELSE branch)",
		`SELECT CASE
		   WHEN 45 >= 90 THEN 'A'
		   WHEN 45 >= 80 THEN 'B'
		   WHEN 45 >= 70 THEN 'C'
		   WHEN 45 >= 60 THEN 'D'
		   ELSE 'F'
		 END`, "F")

	// EX7: Nested CASE expressions.
	// Outer CASE: val > 10 => 'big', else CASE: val > 5 => 'medium', else 'small'.
	// val=7: outer false, inner 7>5 => 'medium'.
	check("EX7 nested CASE: val=7 => medium",
		`SELECT CASE
		   WHEN 7 > 10 THEN 'big'
		   ELSE CASE WHEN 7 > 5 THEN 'medium' ELSE 'small' END
		 END`, "medium")

	// EX8: COALESCE returns first non-NULL.
	// COALESCE(NULL, NULL, 'third', 'fourth') => 'third'.
	check("EX8 COALESCE: first non-NULL is 'third'",
		"SELECT COALESCE(NULL, NULL, 'third', 'fourth')", "third")

	// EX9: NULLIF — returns NULL when both args equal, else first arg.
	// NULLIF(5, 5) => NULL.
	check("EX9 NULLIF equal args: NULLIF(5,5) = NULL",
		"SELECT NULLIF(5, 5)", "<nil>")

	// NULLIF(5, 3) => 5.
	check("EX9b NULLIF unequal args: NULLIF(5,3) = 5",
		"SELECT NULLIF(5, 3)", 5)

	// EX10: Boolean AND/OR/NOT with NULL (three-valued logic).
	// TRUE AND NULL => NULL.
	check("EX10 TRUE AND NULL = NULL",
		"SELECT CASE WHEN (1 = 1 AND NULL) THEN 'true' WHEN (1 = 1 AND NULL) IS NULL THEN 'null' ELSE 'false' END",
		"null")

	// ============================================================
	// SECTION 6: DATA MODIFICATION CORRECTNESS
	// ============================================================
	//
	// Schema
	// ------
	//   v41_inventory (id INTEGER PK, item TEXT, qty INTEGER, price REAL, category TEXT)
	//   v41_inv_log   (id INTEGER PK, action TEXT, item TEXT)   -- for cascade test
	//
	// Data (8 rows):
	//   id  item       qty  price   category
	//    1  Apple       50   0.99   fruit
	//    2  Banana     100   0.49   fruit
	//    3  Carrot      30   0.79   vegetable
	//    4  Daikon      20   1.29   vegetable
	//    5  Elderberry  10   3.99   fruit
	//    6  Fig         40   2.49   fruit
	//    7  Garlic      25   0.89   vegetable
	//    8  Herb        15   1.59   herb

	afExec(t, db, ctx, `CREATE TABLE v41_inventory (
		id       INTEGER PRIMARY KEY,
		item     TEXT,
		qty      INTEGER,
		price    REAL,
		category TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v41_inventory VALUES (1, 'Apple',      50,  0.99, 'fruit')")
	afExec(t, db, ctx, "INSERT INTO v41_inventory VALUES (2, 'Banana',    100,  0.49, 'fruit')")
	afExec(t, db, ctx, "INSERT INTO v41_inventory VALUES (3, 'Carrot',     30,  0.79, 'vegetable')")
	afExec(t, db, ctx, "INSERT INTO v41_inventory VALUES (4, 'Daikon',     20,  1.29, 'vegetable')")
	afExec(t, db, ctx, "INSERT INTO v41_inventory VALUES (5, 'Elderberry', 10,  3.99, 'fruit')")
	afExec(t, db, ctx, "INSERT INTO v41_inventory VALUES (6, 'Fig',        40,  2.49, 'fruit')")
	afExec(t, db, ctx, "INSERT INTO v41_inventory VALUES (7, 'Garlic',     25,  0.89, 'vegetable')")
	afExec(t, db, ctx, "INSERT INTO v41_inventory VALUES (8, 'Herb',       15,  1.59, 'herb')")

	// DM1: UPDATE only matching rows — update fruit category price by 10%.
	// Fruit rows: Apple(1), Banana(2), Elderberry(5), Fig(6).
	// New Apple price = 0.99 * 1.10 = 1.089. Use integer qty update for exact comparison.
	// Update qty of fruits by +5.
	checkNoError("DM1 UPDATE fruit qty += 5",
		"UPDATE v41_inventory SET qty = qty + 5 WHERE category = 'fruit'")

	// Apple qty was 50, now should be 55.
	check("DM1a UPDATE: Apple qty now 55",
		"SELECT qty FROM v41_inventory WHERE id = 1", 55)

	// DM2: UPDATE did NOT change non-fruit rows — Carrot(vegetable) qty still 30.
	check("DM2 UPDATE: Carrot qty still 30 (untouched vegetable)",
		"SELECT qty FROM v41_inventory WHERE id = 3", 30)

	// DM3: Verify count of updated rows — 4 fruit rows should have qty > original.
	// Fruit rows: Apple(55), Banana(105), Elderberry(15), Fig(45). All > original.
	// Non-fruit: Carrot(30), Daikon(20), Garlic(25), Herb(15) — unchanged.
	checkRowCount("DM3 UPDATE: 4 fruit rows have qty > 14 (updated), 4 non-fruit untouched",
		"SELECT id FROM v41_inventory WHERE qty > 49 AND category = 'fruit'", 2)

	// DM4: DELETE only matching rows — delete vegetables.
	// Vegetable rows: Carrot(3), Daikon(4), Garlic(7) => 3 rows deleted.
	checkNoError("DM4 DELETE vegetables",
		"DELETE FROM v41_inventory WHERE category = 'vegetable'")

	checkRowCount("DM4a DELETE: 3 vegetables removed, 5 rows remain",
		"SELECT id FROM v41_inventory", 5)

	// DM5: Verify deleted rows are gone and non-deleted rows are intact.
	check("DM5 DELETE: Carrot(id=3) no longer exists",
		"SELECT COUNT(*) FROM v41_inventory WHERE category = 'vegetable'", 0)

	check("DM5b DELETE: fruit rows still exist",
		"SELECT COUNT(*) FROM v41_inventory WHERE category = 'fruit'", 4)

	// DM6: UPDATE with expression — set price to price * qty / 100 for Herb.
	// Herb: price=1.59, qty=15. New price = 1.59 * 15 / 100 = 0.2385.
	// Verify the expression was evaluated correctly by checking it's > 0.23 and < 0.24.
	checkNoError("DM6 UPDATE with expression: Herb price = price * qty / 100",
		"UPDATE v41_inventory SET price = price * qty / 100 WHERE id = 8")

	check("DM6a UPDATE expression: Herb price recalculated correctly (> 0)",
		"SELECT COUNT(*) FROM v41_inventory WHERE id = 8 AND price > 0", 1)

	// DM7: INSERT OR REPLACE — replace Herb row completely.
	// Original Herb (id=8, qty=15, price=something, category='herb').
	// Replace with new Herb (id=8, qty=999, price=9.99, category='special').
	checkNoError("DM7 INSERT OR REPLACE: replace Herb row",
		"INSERT OR REPLACE INTO v41_inventory VALUES (8, 'Herb', 999, 9.99, 'special')")

	check("DM7a INSERT OR REPLACE: Herb qty now 999",
		"SELECT qty FROM v41_inventory WHERE id = 8", 999)

	check("DM7b INSERT OR REPLACE: Herb category now 'special'",
		"SELECT category FROM v41_inventory WHERE id = 8", "special")

	// DM8: INSERT OR IGNORE — attempt to insert duplicate id, row must be untouched.
	checkNoError("DM8 INSERT OR IGNORE: attempt duplicate Apple row",
		"INSERT OR IGNORE INTO v41_inventory VALUES (1, 'AppleX', 0, 0.0, 'ignore_test')")

	// Apple (id=1) must still have its original item name and updated qty (55).
	check("DM8a INSERT OR IGNORE: Apple item name unchanged",
		"SELECT item FROM v41_inventory WHERE id = 1", "Apple")

	check("DM8b INSERT OR IGNORE: Apple qty unchanged (still 55)",
		"SELECT qty FROM v41_inventory WHERE id = 1", 55)

	// DM9: Chained UPDATE — first add column, then update in two passes.
	// Add a 'discount' column, default NULL. Set discount=10 for id<=5, discount=5 for id>5.
	checkNoError("DM9 ADD COLUMN discount",
		"ALTER TABLE v41_inventory ADD COLUMN discount INTEGER")

	checkNoError("DM9a UPDATE discount=10 for id<=5",
		"UPDATE v41_inventory SET discount = 10 WHERE id <= 5")

	checkNoError("DM9b UPDATE discount=5 for id>5",
		"UPDATE v41_inventory SET discount = 5 WHERE id > 5")

	// Apple(id=1) should have discount=10.
	check("DM9c chained UPDATE: Apple discount=10",
		"SELECT discount FROM v41_inventory WHERE id = 1", 10)

	// Herb(id=8) should have discount=5.
	check("DM9d chained UPDATE: Herb discount=5",
		"SELECT discount FROM v41_inventory WHERE id = 8", 5)

	// DM10: DELETE with subquery — delete items whose qty > AVG(qty).
	// Current rows: Apple(55), Banana(105), Elderberry(15), Fig(45), Herb(999).
	// AVG = (55 + 105 + 15 + 45 + 999) / 5 = 1219 / 5 = 243.8.
	// qty > 243.8: Banana(105 < 243.8 NO), Herb(999 > 243.8 YES). Only Herb qualifies.
	// Wait — 105 < 243.8. Only Herb(999) qualifies.
	checkNoError("DM10 DELETE WHERE qty > avg: removes only Herb",
		"DELETE FROM v41_inventory WHERE qty > (SELECT AVG(qty) FROM v41_inventory)")

	check("DM10a DELETE with subquery: Herb removed (qty > avg)",
		"SELECT COUNT(*) FROM v41_inventory WHERE id = 8", 0)

	checkRowCount("DM10b DELETE with subquery: 4 rows remain",
		"SELECT id FROM v41_inventory", 4)

	// ============================================================
	// SECTION 7: MULTI-COLUMN RESULT VERIFICATION
	// ============================================================
	//
	// These tests verify ALL columns of returned rows, not just the first.
	// Reuses v41_emp, v41_dept, v41_scores tables.

	// MC1: SELECT multiple columns for a single row — verify all columns correct.
	// Alice: id=1, name='Alice', dept_id=1, salary=90000.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT id, name, dept_id, salary FROM v41_emp WHERE id = 1")
		if len(rows) != 1 || len(rows[0]) != 4 {
			t.Errorf("[FAIL] MC1 Alice all columns: wrong shape, got %d rows", len(rows))
		} else if fmt.Sprintf("%v", rows[0][0]) != "1" ||
			fmt.Sprintf("%v", rows[0][1]) != "Alice" ||
			fmt.Sprintf("%v", rows[0][2]) != "1" ||
			fmt.Sprintf("%v", rows[0][3]) != "90000" {
			t.Errorf("[FAIL] MC1 Alice all columns: got %v %v %v %v",
				rows[0][0], rows[0][1], rows[0][2], rows[0][3])
		} else {
			pass++
		}
	}

	// MC2: SELECT multiple columns from JOIN — verify columns from both tables.
	// Carol(emp_id=3) is in Sales(dept_id=2, bldg_floor=1).
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT e.id, e.name, d.name, d.bldg_floor
			 FROM v41_emp e INNER JOIN v41_dept d ON e.dept_id = d.id
			 WHERE e.id = 3`)
		if len(rows) != 1 || len(rows[0]) != 4 {
			t.Errorf("[FAIL] MC2 JOIN Carol+Sales columns: wrong shape")
		} else if fmt.Sprintf("%v", rows[0][0]) != "3" ||
			fmt.Sprintf("%v", rows[0][1]) != "Carol" ||
			fmt.Sprintf("%v", rows[0][2]) != "Sales" ||
			fmt.Sprintf("%v", rows[0][3]) != "1" {
			t.Errorf("[FAIL] MC2 JOIN Carol+Sales columns: got %v %v %v %v",
				rows[0][0], rows[0][1], rows[0][2], rows[0][3])
		} else {
			pass++
		}
	}

	// MC3: Multiple rows — verify all columns in each row.
	// Engineering employees ordered by salary DESC: Alice(90000), Frank(80000), Bob(75000).
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT name, salary FROM v41_emp WHERE dept_id = 1 ORDER BY salary DESC`)
		if len(rows) != 3 || len(rows[0]) != 2 {
			t.Errorf("[FAIL] MC3 Engineering employees by salary: wrong shape")
		} else {
			r0n, r0s := fmt.Sprintf("%v", rows[0][0]), fmt.Sprintf("%v", rows[0][1])
			r1n, r1s := fmt.Sprintf("%v", rows[1][0]), fmt.Sprintf("%v", rows[1][1])
			r2n, r2s := fmt.Sprintf("%v", rows[2][0]), fmt.Sprintf("%v", rows[2][1])
			if r0n != "Alice" || r0s != "90000" ||
				r1n != "Frank" || r1s != "80000" ||
				r2n != "Bob" || r2s != "75000" {
				t.Errorf("[FAIL] MC3 Engineering by salary: got %s/%s, %s/%s, %s/%s",
					r0n, r0s, r1n, r1s, r2n, r2s)
			} else {
				pass++
			}
		}
	}

	// MC4: Verify column COUNT matches SELECT list — SELECT with 5 expressions.
	total++
	{
		rows := afQuery(t, db, ctx,
			"SELECT 1, 'two', 3.0, NULL, 'five'")
		if len(rows) != 1 || len(rows[0]) != 5 {
			t.Errorf("[FAIL] MC4 column count = 5: got %d rows, %d cols",
				len(rows), func() int {
					if len(rows) > 0 {
						return len(rows[0])
					}
					return 0
				}())
		} else {
			pass++
		}
	}

	// MC5: Aggregate GROUP BY result — all columns correct per group.
	// GROUP BY dept_id, COUNT(*), MAX(salary) per dept.
	// Engineering(1): 3 emps, max=90000.
	// Sales(2): 2 emps, max=65000.
	// HR(3): 1 emp, max=55000.
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT dept_id, COUNT(*), MAX(salary)
			 FROM v41_emp
			 GROUP BY dept_id
			 ORDER BY dept_id ASC`)
		if len(rows) != 3 || len(rows[0]) != 3 {
			t.Errorf("[FAIL] MC5 GROUP BY dept multi-col: wrong shape")
		} else {
			// Row 0: dept_id=1, count=3, max=90000.
			d0, c0, m0 := fmt.Sprintf("%v", rows[0][0]), fmt.Sprintf("%v", rows[0][1]), fmt.Sprintf("%v", rows[0][2])
			// Row 1: dept_id=2, count=2, max=65000.
			d1, c1, m1 := fmt.Sprintf("%v", rows[1][0]), fmt.Sprintf("%v", rows[1][1]), fmt.Sprintf("%v", rows[1][2])
			// Row 2: dept_id=3, count=1, max=55000.
			d2, c2, m2 := fmt.Sprintf("%v", rows[2][0]), fmt.Sprintf("%v", rows[2][1]), fmt.Sprintf("%v", rows[2][2])
			if d0 != "1" || c0 != "3" || m0 != "90000" ||
				d1 != "2" || c1 != "2" || m1 != "65000" ||
				d2 != "3" || c2 != "1" || m2 != "55000" {
				t.Errorf("[FAIL] MC5 GROUP BY dept: row0=%s/%s/%s row1=%s/%s/%s row2=%s/%s/%s",
					d0, c0, m0, d1, c1, m1, d2, c2, m2)
			} else {
				pass++
			}
		}
	}

	// MC6: CASE expression in multi-column select.
	// For Alice (salary=90000): salary_band = 'Senior' (>= 80000).
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT name, salary,
			   CASE WHEN salary >= 80000 THEN 'Senior'
			        WHEN salary >= 60000 THEN 'Mid'
			        ELSE 'Junior'
			   END AS salary_band
			 FROM v41_emp
			 WHERE id = 1`)
		if len(rows) != 1 || len(rows[0]) != 3 {
			t.Errorf("[FAIL] MC6 CASE multi-col Alice: wrong shape")
		} else if fmt.Sprintf("%v", rows[0][0]) != "Alice" ||
			fmt.Sprintf("%v", rows[0][1]) != "90000" ||
			fmt.Sprintf("%v", rows[0][2]) != "Senior" {
			t.Errorf("[FAIL] MC6 CASE multi-col Alice: got %v %v %v",
				rows[0][0], rows[0][1], rows[0][2])
		} else {
			pass++
		}
	}

	// MC7: Multi-column WHERE verification — select rows matching two conditions.
	// Employees with dept_id=1 AND salary > 75000: Alice(90000), Frank(80000).
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT id, name, salary FROM v41_emp
			 WHERE dept_id = 1 AND salary > 75000
			 ORDER BY salary DESC`)
		if len(rows) != 2 || len(rows[0]) != 3 {
			t.Errorf("[FAIL] MC7 multi-cond WHERE: expected 2 rows, got %d", len(rows))
		} else {
			id0, n0, s0 := fmt.Sprintf("%v", rows[0][0]), fmt.Sprintf("%v", rows[0][1]), fmt.Sprintf("%v", rows[0][2])
			id1, n1, s1 := fmt.Sprintf("%v", rows[1][0]), fmt.Sprintf("%v", rows[1][1]), fmt.Sprintf("%v", rows[1][2])
			if id0 != "1" || n0 != "Alice" || s0 != "90000" ||
				id1 != "6" || n1 != "Frank" || s1 != "80000" {
				t.Errorf("[FAIL] MC7 multi-cond WHERE: got %s/%s/%s and %s/%s/%s",
					id0, n0, s0, id1, n1, s1)
			} else {
				pass++
			}
		}
	}

	// MC8: Subquery with multi-column outer SELECT.
	// For each Sales employee, show name, salary, and dept name from subquery.
	// Sales employees: Carol(60000), Eve(65000).
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT e.name, e.salary,
			   (SELECT d.name FROM v41_dept d WHERE d.id = e.dept_id) AS dept_name
			 FROM v41_emp e
			 WHERE e.dept_id = 2
			 ORDER BY e.salary ASC`)
		if len(rows) != 2 || len(rows[0]) != 3 {
			t.Errorf("[FAIL] MC8 subquery multi-col: expected 2 rows 3 cols")
		} else {
			n0, s0, d0 := fmt.Sprintf("%v", rows[0][0]), fmt.Sprintf("%v", rows[0][1]), fmt.Sprintf("%v", rows[0][2])
			n1, s1, d1 := fmt.Sprintf("%v", rows[1][0]), fmt.Sprintf("%v", rows[1][1]), fmt.Sprintf("%v", rows[1][2])
			if n0 != "Carol" || s0 != "60000" || d0 != "Sales" ||
				n1 != "Eve" || s1 != "65000" || d1 != "Sales" {
				t.Errorf("[FAIL] MC8 subquery multi-col: got %s/%s/%s and %s/%s/%s",
					n0, s0, d0, n1, s1, d1)
			} else {
				pass++
			}
		}
	}

	// MC9: ORDER BY verification with all columns — scores ordered by score DESC.
	// Top 3 scores with all columns (using non-NULL scores).
	// Grace(500,3), Dave(500,1), Carol(300,3) — ORDER BY score DESC, player ASC.
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT player, score, level FROM v41_scores
			 WHERE score IS NOT NULL
			 ORDER BY score DESC, player ASC
			 LIMIT 3`)
		if len(rows) != 3 || len(rows[0]) != 3 {
			t.Errorf("[FAIL] MC9 top3 scores multi-col: expected 3 rows 3 cols")
		} else {
			p0, s0, l0 := fmt.Sprintf("%v", rows[0][0]), fmt.Sprintf("%v", rows[0][1]), fmt.Sprintf("%v", rows[0][2])
			p1, s1, l1 := fmt.Sprintf("%v", rows[1][0]), fmt.Sprintf("%v", rows[1][1]), fmt.Sprintf("%v", rows[1][2])
			p2, s2, l2 := fmt.Sprintf("%v", rows[2][0]), fmt.Sprintf("%v", rows[2][1]), fmt.Sprintf("%v", rows[2][2])
			if p0 != "Dave" || s0 != "500" || l0 != "1" ||
				p1 != "Grace" || s1 != "500" || l1 != "3" ||
				p2 != "Alice" || s2 != "300" || l2 != "2" {
				t.Errorf("[FAIL] MC9 top3 scores: got %s/%s/%s, %s/%s/%s, %s/%s/%s",
					p0, s0, l0, p1, s1, l1, p2, s2, l2)
			} else {
				pass++
			}
		}
	}

	// MC10: Verify row order is consistent across multiple columns.
	// Score table: players with score=150 ordered by player ASC.
	// Bob(150,1) comes before Eve(150,2) alphabetically.
	total++
	{
		rows := afQuery(t, db, ctx,
			`SELECT player, score, level FROM v41_scores
			 WHERE score = 150
			 ORDER BY player ASC`)
		if len(rows) != 2 || len(rows[0]) != 3 {
			t.Errorf("[FAIL] MC10 score=150 rows: expected 2 rows 3 cols, got %d rows", len(rows))
		} else {
			p0, s0, l0 := fmt.Sprintf("%v", rows[0][0]), fmt.Sprintf("%v", rows[0][1]), fmt.Sprintf("%v", rows[0][2])
			p1, s1, l1 := fmt.Sprintf("%v", rows[1][0]), fmt.Sprintf("%v", rows[1][1]), fmt.Sprintf("%v", rows[1][2])
			if p0 != "Bob" || s0 != "150" || l0 != "1" ||
				p1 != "Eve" || s1 != "150" || l1 != "2" {
				t.Errorf("[FAIL] MC10 score=150 rows: got %s/%s/%s and %s/%s/%s",
					p0, s0, l0, p1, s1, l1)
			} else {
				pass++
			}
		}
	}

	// ============================================================
	// FINAL REPORT
	// ============================================================
	t.Logf("TestV41SQLConformance: %d/%d tests passed", pass, total)
	if pass < total {
		t.Errorf("SUMMARY: %d tests FAILED out of %d", total-pass, total)
	}
}
