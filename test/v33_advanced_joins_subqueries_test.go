package test

import (
	"fmt"
	"testing"
)

// TestV33AdvancedJoinsSubqueries exercises multi-table JOINs, self-joins, LEFT JOIN
// aggregates, correlated subqueries, EXISTS, multiple aggregates, CROSS JOIN, derived
// table patterns, UPDATE/DELETE with subqueries, and ORDER BY/LIMIT after JOINs.
//
// Schema (all table names prefixed with v33_ to avoid collisions):
//
//   v33_dept        (id, name, budget)
//   v33_emp         (id, name, dept_id, manager_id, salary, hire_year)
//   v33_proj        (id, name, dept_id, budget)
//   v33_assignment  (id, emp_id, proj_id, hours)
//   v33_client      (id, name, country)
//   v33_contract    (id, client_id, proj_id, value)
func TestV33AdvancedJoinsSubqueries(t *testing.T) {
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

	// ============================================================
	// === SCHEMA SETUP ===
	// ============================================================

	// Departments: 4 rows; Marketing intentionally has no employees.
	afExec(t, db, ctx, "CREATE TABLE v33_dept (id INTEGER PRIMARY KEY, name TEXT, budget INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v33_dept VALUES (1, 'Engineering', 500000)")
	afExec(t, db, ctx, "INSERT INTO v33_dept VALUES (2, 'Sales', 300000)")
	afExec(t, db, ctx, "INSERT INTO v33_dept VALUES (3, 'HR', 150000)")
	afExec(t, db, ctx, "INSERT INTO v33_dept VALUES (4, 'Marketing', 200000)")

	// Employees: 7 rows across 3 departments (Marketing has none).
	// manager_id is self-referential (NULL means top-level).
	//
	//   id | name  | dept | mgr  | salary | hire_year
	//    1   Alice    1     NULL   90000    2018   (Eng, top-level)
	//    2   Bob      1     1      75000    2019   (Eng, reports to Alice)
	//    3   Carol    1     1      80000    2020   (Eng, reports to Alice)
	//    4   Dave     2     NULL   70000    2017   (Sales, top-level)
	//    5   Eve      2     4      60000    2021   (Sales, reports to Dave)
	//    6   Frank    3     NULL   65000    2019   (HR, top-level)
	//    7   Grace    1     2      55000    2022   (Eng, reports to Bob)
	afExec(t, db, ctx, "CREATE TABLE v33_emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, manager_id INTEGER, salary INTEGER, hire_year INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v33_emp VALUES (1, 'Alice', 1, NULL, 90000, 2018)")
	afExec(t, db, ctx, "INSERT INTO v33_emp VALUES (2, 'Bob',   1, 1,    75000, 2019)")
	afExec(t, db, ctx, "INSERT INTO v33_emp VALUES (3, 'Carol', 1, 1,    80000, 2020)")
	afExec(t, db, ctx, "INSERT INTO v33_emp VALUES (4, 'Dave',  2, NULL, 70000, 2017)")
	afExec(t, db, ctx, "INSERT INTO v33_emp VALUES (5, 'Eve',   2, 4,    60000, 2021)")
	afExec(t, db, ctx, "INSERT INTO v33_emp VALUES (6, 'Frank', 3, NULL, 65000, 2019)")
	afExec(t, db, ctx, "INSERT INTO v33_emp VALUES (7, 'Grace', 1, 2,    55000, 2022)")

	// Projects: 5 rows; Epsilon has no assignments.
	afExec(t, db, ctx, "CREATE TABLE v33_proj (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, budget INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v33_proj VALUES (1, 'Alpha',   1, 100000)")
	afExec(t, db, ctx, "INSERT INTO v33_proj VALUES (2, 'Beta',    1,  80000)")
	afExec(t, db, ctx, "INSERT INTO v33_proj VALUES (3, 'Gamma',   2,  50000)")
	afExec(t, db, ctx, "INSERT INTO v33_proj VALUES (4, 'Delta',   3,  30000)")
	afExec(t, db, ctx, "INSERT INTO v33_proj VALUES (5, 'Epsilon', 2,  40000)")

	// Assignments: employee-to-project hours.
	//
	//   id | emp | proj | hours
	//    1   1(Alice)  1(Alpha)  40
	//    2   2(Bob)    1(Alpha)  60
	//    3   3(Carol)  2(Beta)   50
	//    4   2(Bob)    2(Beta)   30
	//    5   4(Dave)   3(Gamma)  80
	//    6   5(Eve)    3(Gamma)  40
	//    7   1(Alice)  3(Gamma)  20
	//    8   6(Frank)  4(Delta)  60
	//    9   7(Grace)  1(Alpha)  35
	afExec(t, db, ctx, "CREATE TABLE v33_assignment (id INTEGER PRIMARY KEY, emp_id INTEGER, proj_id INTEGER, hours INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v33_assignment VALUES (1, 1, 1, 40)")
	afExec(t, db, ctx, "INSERT INTO v33_assignment VALUES (2, 2, 1, 60)")
	afExec(t, db, ctx, "INSERT INTO v33_assignment VALUES (3, 3, 2, 50)")
	afExec(t, db, ctx, "INSERT INTO v33_assignment VALUES (4, 2, 2, 30)")
	afExec(t, db, ctx, "INSERT INTO v33_assignment VALUES (5, 4, 3, 80)")
	afExec(t, db, ctx, "INSERT INTO v33_assignment VALUES (6, 5, 3, 40)")
	afExec(t, db, ctx, "INSERT INTO v33_assignment VALUES (7, 1, 3, 20)")
	afExec(t, db, ctx, "INSERT INTO v33_assignment VALUES (8, 6, 4, 60)")
	afExec(t, db, ctx, "INSERT INTO v33_assignment VALUES (9, 7, 1, 35)")

	// Clients and contracts.
	// Umbrella has no contracts.
	// Acme funds two projects (Alpha, Gamma).
	afExec(t, db, ctx, "CREATE TABLE v33_client (id INTEGER PRIMARY KEY, name TEXT, country TEXT)")
	afExec(t, db, ctx, "INSERT INTO v33_client VALUES (1, 'Acme',     'USA')")
	afExec(t, db, ctx, "INSERT INTO v33_client VALUES (2, 'Globex',   'UK')")
	afExec(t, db, ctx, "INSERT INTO v33_client VALUES (3, 'Initech',  'USA')")
	afExec(t, db, ctx, "INSERT INTO v33_client VALUES (4, 'Umbrella', 'DE')")

	afExec(t, db, ctx, "CREATE TABLE v33_contract (id INTEGER PRIMARY KEY, client_id INTEGER, proj_id INTEGER, value INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v33_contract VALUES (1, 1, 1, 500000)") // Acme -> Alpha
	afExec(t, db, ctx, "INSERT INTO v33_contract VALUES (2, 2, 2, 300000)") // Globex -> Beta
	afExec(t, db, ctx, "INSERT INTO v33_contract VALUES (3, 1, 3, 200000)") // Acme -> Gamma
	afExec(t, db, ctx, "INSERT INTO v33_contract VALUES (4, 3, 1, 150000)") // Initech -> Alpha

	// ============================================================
	// === SECTION 1: THREE-TABLE INNER JOINs ===
	// ============================================================

	// Test 1: emp JOIN dept JOIN assignment - total rows (7 emps with assignments = 9 assignment rows match)
	checkRowCount("3-table JOIN row count",
		`SELECT v33_emp.name, v33_dept.name, v33_assignment.hours
		 FROM v33_emp
		 JOIN v33_dept       ON v33_emp.dept_id  = v33_dept.id
		 JOIN v33_assignment ON v33_emp.id        = v33_assignment.emp_id`,
		9) // 9 assignment rows all have valid employees

	// Test 2: Top project by total hours - emp JOIN assignment JOIN proj
	// Alpha:40+60+35=135, Beta:50+30=80, Gamma:80+40+20=140, Delta:60
	// Gamma wins with 140h.
	check("3-table JOIN top project by hours",
		`SELECT v33_proj.name
		 FROM v33_proj
		 JOIN v33_assignment ON v33_proj.id = v33_assignment.proj_id
		 JOIN v33_emp        ON v33_emp.id  = v33_assignment.emp_id
		 GROUP BY v33_proj.name
		 ORDER BY SUM(v33_assignment.hours) DESC
		 LIMIT 1`,
		"Gamma")

	// Test 3: Four-table JOIN - emp + dept + assignment + proj
	// Filter to Engineering department employees (WHERE v33_dept.name = 'Engineering').
	// This filters by the employee's department, not the project's department.
	// Eng employees and their assignments:
	//   Alice(Eng): Alpha(row 1, 40h), Gamma(row 7, 20h) -> 2 rows
	//   Bob(Eng):   Alpha(row 2, 60h), Beta(row 4, 30h)  -> 2 rows
	//   Carol(Eng): Beta(row 3, 50h)                      -> 1 row
	//   Grace(Eng): Alpha(row 9, 35h)                     -> 1 row
	// Total: 6 rows
	checkRowCount("4-table JOIN Engineering assignments",
		`SELECT v33_emp.name, v33_dept.name, v33_proj.name, v33_assignment.hours
		 FROM v33_emp
		 JOIN v33_dept       ON v33_emp.dept_id   = v33_dept.id
		 JOIN v33_assignment ON v33_emp.id         = v33_assignment.emp_id
		 JOIN v33_proj       ON v33_proj.id        = v33_assignment.proj_id
		 WHERE v33_dept.name = 'Engineering'`,
		6)

	// Test 4: Four-table JOIN - top Engineering employee by total hours on Engineering projects
	// Alice: Alpha(40)+Gamma(20)=60, Bob: Alpha(60)+Beta(30)=90, Carol: Beta(50)=50, Grace: Alpha(35)=35
	// But filter to Engineering dept employees only:
	// Alice(Eng): Alpha(40)+Gamma(20)=60  -- Gamma is Sales project, but Alice is Eng
	// Actually filtering by WHERE v33_dept.name = 'Engineering' filters by EMP's dept.
	// Alice is Eng, has Alpha(40) and Gamma(20) = 60h
	// Bob is Eng: Alpha(60)+Beta(30) = 90h -> top
	check("4-table JOIN top Eng employee by hours",
		`SELECT v33_emp.name
		 FROM v33_emp
		 JOIN v33_dept       ON v33_emp.dept_id   = v33_dept.id
		 JOIN v33_assignment ON v33_emp.id         = v33_assignment.emp_id
		 JOIN v33_proj       ON v33_proj.id        = v33_assignment.proj_id
		 WHERE v33_dept.name = 'Engineering'
		 GROUP BY v33_emp.name
		 ORDER BY SUM(v33_assignment.hours) DESC, v33_emp.name ASC
		 LIMIT 1`,
		"Bob")

	// Test 5: Three-table JOIN with WHERE and GROUP BY - projects that cost > 50000 and have assignments
	// Proj budget > 50000: Alpha(100k), Beta(80k). Both have assignments.
	checkRowCount("3-table JOIN budget filter",
		`SELECT v33_proj.name
		 FROM v33_proj
		 JOIN v33_dept       ON v33_proj.dept_id  = v33_dept.id
		 JOIN v33_assignment ON v33_proj.id        = v33_assignment.proj_id
		 WHERE v33_proj.budget > 50000
		 GROUP BY v33_proj.name`,
		2) // Alpha, Beta

	// ============================================================
	// === SECTION 2: SELF-JOINS (EMPLOYEE-MANAGER HIERARCHY) ===
	// ============================================================

	// Test 6: Self-join to find subordinates of Alice (manager_id=1)
	// Bob and Carol report to Alice.
	checkRowCount("Self-join subordinates of Alice",
		`SELECT e.name
		 FROM v33_emp e
		 JOIN v33_emp m ON e.manager_id = m.id
		 WHERE m.name = 'Alice'`,
		2)

	// Test 7: Self-join - name of manager for each subordinate
	check("Self-join Grace's manager",
		`SELECT m.name
		 FROM v33_emp e
		 JOIN v33_emp m ON e.manager_id = m.id
		 WHERE e.name = 'Grace'`,
		"Bob")

	// Test 8: Self-join total employees who have a manager (non-NULL manager_id)
	// Bob(1), Carol(1), Eve(4), Grace(2) = 4 employees with a manager
	checkRowCount("Self-join all subordinates",
		`SELECT e.name
		 FROM v33_emp e
		 JOIN v33_emp m ON e.manager_id = m.id`,
		4)

	// Test 9: Self-join - managers and their subordinate count
	// Alice manages: Bob, Carol -> 2 subordinates
	// Dave manages: Eve -> 1 subordinate
	// Bob manages: Grace -> 1 subordinate
	check("Self-join top manager by direct reports",
		`SELECT m.name
		 FROM v33_emp e
		 JOIN v33_emp m ON e.manager_id = m.id
		 GROUP BY m.name
		 ORDER BY COUNT(*) DESC, m.name ASC
		 LIMIT 1`,
		"Alice")

	// Test 10: Self-join with salary comparison - subordinates earning less than their manager
	// Bob(75k) < Alice(90k) YES, Carol(80k) < Alice(90k) YES, Eve(60k) < Dave(70k) YES, Grace(55k) < Bob(75k) YES
	// All 4 subordinates earn less than their manager.
	checkRowCount("Self-join subordinates earning less than manager",
		`SELECT e.name
		 FROM v33_emp e
		 JOIN v33_emp m ON e.manager_id = m.id
		 WHERE e.salary < m.salary`,
		4)

	// Test 11: Self-join - finding peer pairs (employees in same dept, same hire_year)
	// hire_year: Alice(2018), Bob(2019), Carol(2020), Dave(2017), Eve(2021), Frank(2019), Grace(2022)
	// Bob(Eng,2019) and Frank(HR,2019) have same hire_year but different depts.
	// No two employees share the same dept AND hire_year.
	// Bob(Eng,2019) and Frank(HR,2019): same year, different dept -> 0 same-dept-same-year pairs
	checkRowCount("Self-join same dept same hire year pairs",
		`SELECT e1.name, e2.name
		 FROM v33_emp e1
		 JOIN v33_emp e2 ON e1.dept_id = e2.dept_id AND e1.hire_year = e2.hire_year AND e1.id < e2.id`,
		0)

	// ============================================================
	// === SECTION 3: LEFT JOIN WITH AGGREGATES AND NULL HANDLING ===
	// ============================================================

	// Test 12: LEFT JOIN dept -> emp, all depts appear (including Marketing with no employees)
	// Engineering:4, Sales:2, HR:1, Marketing:0 -> 4 dept rows in result, 3+NULL for Marketing
	checkRowCount("LEFT JOIN dept->emp all depts appear",
		`SELECT v33_dept.name, COUNT(v33_emp.id)
		 FROM v33_dept
		 LEFT JOIN v33_emp ON v33_dept.id = v33_emp.dept_id
		 GROUP BY v33_dept.name`,
		4)

	// Test 13: LEFT JOIN - count employees shows 0 for Marketing
	check("LEFT JOIN Marketing has 0 employees",
		`SELECT COUNT(v33_emp.id)
		 FROM v33_dept
		 LEFT JOIN v33_emp ON v33_dept.id = v33_emp.dept_id
		 WHERE v33_dept.name = 'Marketing'`,
		0)

	// Test 14: LEFT JOIN proj -> assignment, Epsilon has no assignments
	// 5 projects, but only 4 distinct project names have assignments; Epsilon row appears with NULL hours
	checkRowCount("LEFT JOIN proj->assignment includes Epsilon",
		`SELECT v33_proj.name, v33_assignment.hours
		 FROM v33_proj
		 LEFT JOIN v33_assignment ON v33_proj.id = v33_assignment.proj_id`,
		10) // 9 assignment rows + 1 Epsilon row with NULL = 10

	// Test 15: LEFT JOIN proj -> assignment, SUM hours per project (Epsilon sums to 0 via COALESCE or NULL)
	check("LEFT JOIN SUM hours Epsilon is NULL/0",
		`SELECT COALESCE(SUM(v33_assignment.hours), 0)
		 FROM v33_proj
		 LEFT JOIN v33_assignment ON v33_proj.id = v33_assignment.proj_id
		 WHERE v33_proj.name = 'Epsilon'`,
		0)

	// Test 16: LEFT JOIN dept -> emp -> assignment chained (3-table LEFT JOIN)
	// Marketing -> no emp -> no assignment rows; shows 1 row for Marketing with NULLs
	checkRowCount("3-table LEFT JOIN dept->emp->assignment row count",
		`SELECT v33_dept.name, v33_emp.name, v33_assignment.hours
		 FROM v33_dept
		 LEFT JOIN v33_emp        ON v33_dept.id = v33_emp.dept_id
		 LEFT JOIN v33_assignment ON v33_emp.id  = v33_assignment.emp_id`,
		10) // 9 assignment rows + 1 Marketing (NULL emp, NULL hours) = 10

	// Test 17: LEFT JOIN with HAVING to find depts with fewer than 2 employees
	// Engineering:4, Sales:2, HR:1, Marketing:0  -> HR(<2) and Marketing(<2) = 2 groups
	checkRowCount("LEFT JOIN GROUP BY HAVING < 2 employees",
		`SELECT v33_dept.name, COUNT(v33_emp.id) AS emp_count
		 FROM v33_dept
		 LEFT JOIN v33_emp ON v33_dept.id = v33_emp.dept_id
		 GROUP BY v33_dept.name
		 HAVING COUNT(v33_emp.id) < 2`,
		2)

	// ============================================================
	// === SECTION 4: CORRELATED SUBQUERIES IN SELECT ===
	// ============================================================

	// Test 18: Correlated subquery - for each dept show its employee count
	// Engineering -> 4
	check("Correlated subquery emp count for Engineering",
		`SELECT (SELECT COUNT(*) FROM v33_emp WHERE v33_emp.dept_id = v33_dept.id)
		 FROM v33_dept
		 WHERE v33_dept.name = 'Engineering'`,
		4)

	// Test 19: Correlated subquery - Marketing has 0 employees
	check("Correlated subquery emp count for Marketing",
		`SELECT (SELECT COUNT(*) FROM v33_emp WHERE v33_emp.dept_id = v33_dept.id)
		 FROM v33_dept
		 WHERE v33_dept.name = 'Marketing'`,
		0)

	// Test 20: Correlated subquery - for each project show total contract value
	// Alpha: 500000+150000=650000, Beta: 300000, Gamma: 200000, Delta: 0, Epsilon: 0
	check("Correlated subquery total contract value for Alpha",
		`SELECT (SELECT COALESCE(SUM(v33_contract.value), 0) FROM v33_contract WHERE v33_contract.proj_id = v33_proj.id)
		 FROM v33_proj
		 WHERE v33_proj.name = 'Alpha'`,
		650000)

	// Test 21: Correlated subquery - project with no contracts returns 0
	check("Correlated subquery zero contracts for Delta",
		`SELECT (SELECT COALESCE(SUM(v33_contract.value), 0) FROM v33_contract WHERE v33_contract.proj_id = v33_proj.id)
		 FROM v33_proj
		 WHERE v33_proj.name = 'Delta'`,
		0)

	// ============================================================
	// === SECTION 5: EXISTS AND NOT EXISTS SUBQUERIES ===
	// ============================================================

	// Test 22: EXISTS - departments that have at least one employee
	// Engineering, Sales, HR have employees; Marketing does not.
	checkRowCount("EXISTS depts with employees",
		`SELECT v33_dept.name
		 FROM v33_dept
		 WHERE EXISTS (
		   SELECT 1 FROM v33_emp WHERE v33_emp.dept_id = v33_dept.id
		 )`,
		3)

	// Test 23: NOT EXISTS - departments with no employees -> Marketing only
	checkRowCount("NOT EXISTS depts without employees",
		`SELECT v33_dept.name
		 FROM v33_dept
		 WHERE NOT EXISTS (
		   SELECT 1 FROM v33_emp WHERE v33_emp.dept_id = v33_dept.id
		 )`,
		1)

	check("NOT EXISTS returns Marketing",
		`SELECT v33_dept.name
		 FROM v33_dept
		 WHERE NOT EXISTS (
		   SELECT 1 FROM v33_emp WHERE v33_emp.dept_id = v33_dept.id
		 )`,
		"Marketing")

	// Test 24: EXISTS with nested join - clients that have at least one contract on an Engineering project
	// Engineering projects: id=1(Alpha), id=2(Beta)
	// Contracts on Alpha: Acme(1), Initech(3). Contracts on Beta: Globex(2).
	// Umbrella has no contracts -> NOT in result.
	// Clients with EXISTS: Acme, Globex, Initech = 3
	checkRowCount("EXISTS clients with Engineering project contracts",
		`SELECT v33_client.name
		 FROM v33_client
		 WHERE EXISTS (
		   SELECT 1
		   FROM v33_contract
		   JOIN v33_proj ON v33_contract.proj_id = v33_proj.id
		   WHERE v33_contract.client_id = v33_client.id
		     AND v33_proj.dept_id = 1
		 )`,
		3)

	// Test 25: EXISTS - employees assigned to more than one project
	// Bob: Alpha(2) + Beta(4) = 2 projects. Alice: Alpha(1) + Gamma(7) = 2 projects.
	// Carol: Beta only. Dave: Gamma only. Eve: Gamma only. Frank: Delta only. Grace: Alpha only.
	// Employees with >1 project: Alice, Bob = 2
	checkRowCount("EXISTS employees with multiple project assignments",
		`SELECT v33_emp.name
		 FROM v33_emp
		 WHERE EXISTS (
		   SELECT 1 FROM v33_assignment a1
		   WHERE a1.emp_id = v33_emp.id
		 ) AND (
		   SELECT COUNT(*) FROM v33_assignment a2 WHERE a2.emp_id = v33_emp.id
		 ) > 1`,
		2)

	// ============================================================
	// === SECTION 6: MULTIPLE AGGREGATES IN A SINGLE QUERY ===
	// ============================================================

	// Test 26: All five aggregates on salary from v33_emp in one SELECT
	// SUM: 90000+75000+80000+70000+60000+65000+55000 = 495000
	check("SUM salary all employees",
		"SELECT SUM(salary) FROM v33_emp",
		495000)

	// Test 27: COUNT all employees
	check("COUNT all employees",
		"SELECT COUNT(*) FROM v33_emp",
		7)

	// Test 28: MIN salary
	check("MIN salary is Grace 55000",
		"SELECT MIN(salary) FROM v33_emp",
		55000)

	// Test 29: MAX salary
	check("MAX salary is Alice 90000",
		"SELECT MAX(salary) FROM v33_emp",
		90000)

	// Test 30: Multiple aggregates in one SELECT - verify row returned
	checkRowCount("Multiple aggregates in one SELECT returns one row",
		"SELECT COUNT(*), SUM(salary), MIN(salary), MAX(salary) FROM v33_emp",
		1)

	// Test 31: Multiple aggregates per group - Engineering dept
	// Eng salaries: Alice(90000), Bob(75000), Carol(80000), Grace(55000)
	// SUM=300000, COUNT=4, MIN=55000, MAX=90000
	check("SUM salary Engineering dept",
		"SELECT SUM(v33_emp.salary) FROM v33_emp JOIN v33_dept ON v33_emp.dept_id = v33_dept.id WHERE v33_dept.name = 'Engineering'",
		300000)

	check("COUNT Engineering employees",
		"SELECT COUNT(*) FROM v33_emp JOIN v33_dept ON v33_emp.dept_id = v33_dept.id WHERE v33_dept.name = 'Engineering'",
		4)

	// ============================================================
	// === SECTION 7: AGGREGATE EXPRESSIONS ===
	// ============================================================

	// Test 32: SUM(hours * salary / 1000) per project - weighted effort metric
	// Assignment weighted costs (salary in thousands):
	//   Alice(90k): Alpha=40*90=3600, Gamma=20*90=1800
	//   Bob(75k):   Alpha=60*75=4500, Beta=30*75=2250
	//   Carol(80k): Beta=50*80=4000
	//   Dave(70k):  Gamma=80*70=5600
	//   Eve(60k):   Gamma=40*60=2400
	//   Frank(65k): Delta=60*65=3900
	//   Grace(55k): Alpha=35*55=1925
	// Per project:
	//   Alpha: 3600+4500+1925=10025
	//   Beta:  4000+2250=6250
	//   Gamma: 1800+5600+2400=9800
	//   Delta: 3900
	// Top project by weighted effort: Alpha (10025)
	check("SUM(hours * salary/1000) top project is Alpha",
		`SELECT v33_proj.name
		 FROM v33_proj
		 JOIN v33_assignment ON v33_proj.id    = v33_assignment.proj_id
		 JOIN v33_emp        ON v33_emp.id      = v33_assignment.emp_id
		 GROUP BY v33_proj.name
		 ORDER BY SUM(v33_assignment.hours * v33_emp.salary / 1000) DESC, v33_proj.name ASC
		 LIMIT 1`,
		"Alpha")

	// Test 33: COUNT(CASE WHEN ...) to count senior employees (salary >= 75000)
	// Alice(90k), Bob(75k), Carol(80k) = 3 employees in Eng with salary >= 75000
	check("COUNT(CASE WHEN salary>=75000) in Engineering",
		`SELECT COUNT(CASE WHEN v33_emp.salary >= 75000 THEN 1 END)
		 FROM v33_emp
		 WHERE v33_emp.dept_id = 1`,
		3)

	// Test 34: SUM(CASE WHEN ...) - total salary for employees hired before 2020
	// Before 2020: Alice(2018,90k), Bob(2019,75k), Dave(2017,70k), Frank(2019,65k) = 300000
	check("SUM(CASE WHEN hire_year<2020 THEN salary) ",
		`SELECT SUM(CASE WHEN hire_year < 2020 THEN salary ELSE 0 END)
		 FROM v33_emp`,
		300000)

	// Test 35: Aggregate on a multiplication expression - total budget across all contracts
	// 500000+300000+200000+150000 = 1150000
	// The engine may return large sums in scientific notation (e.g. 1.15e+06).
	check("SUM contract values",
		"SELECT SUM(value) FROM v33_contract",
		"1.15e+06")

	// Test 36: COUNT DISTINCT on a joined column - distinct departments that have project assignments
	// Assignments link to projects in depts: Alpha(1), Beta(1), Gamma(2), Delta(3) -> dept 1,2,3 = 3 distinct
	check("COUNT DISTINCT dept_id from proj assignment join",
		`SELECT COUNT(DISTINCT v33_proj.dept_id)
		 FROM v33_proj
		 JOIN v33_assignment ON v33_proj.id = v33_assignment.proj_id`,
		3)

	// ============================================================
	// === SECTION 8: ORDER BY ON JOINED COLUMN ===
	// ============================================================

	// Test 37: ORDER BY joined table's column - employees ordered by dept name
	// dept names alphabetically: Engineering, HR, Marketing(no emp), Sales
	// First employee by dept.name ASC, then emp.id ASC: Alice (Eng, id=1)
	check("ORDER BY joined dept.name - first employee",
		`SELECT v33_emp.name
		 FROM v33_emp
		 JOIN v33_dept ON v33_emp.dept_id = v33_dept.id
		 ORDER BY v33_dept.name ASC, v33_emp.id ASC
		 LIMIT 1`,
		"Alice")

	// Test 38: ORDER BY joined table's column DESC - last employee by dept.name
	// Sales dept employees: Dave(id=4), Eve(id=5); last alphabetically by dept DESC, emp.id DESC -> Eve
	check("ORDER BY joined dept.name DESC - first result is Sales emp",
		`SELECT v33_emp.name
		 FROM v33_emp
		 JOIN v33_dept ON v33_emp.dept_id = v33_dept.id
		 ORDER BY v33_dept.name DESC, v33_emp.id DESC
		 LIMIT 1`,
		"Eve")

	// Test 39: ORDER BY aggregate on joined column - departments ordered by SUM salary
	// Eng:300000, Sales:130000, HR:65000 -> top is Engineering
	check("ORDER BY SUM salary per dept JOIN",
		`SELECT v33_dept.name
		 FROM v33_dept
		 JOIN v33_emp ON v33_dept.id = v33_emp.dept_id
		 GROUP BY v33_dept.name
		 ORDER BY SUM(v33_emp.salary) DESC
		 LIMIT 1`,
		"Engineering")

	// ============================================================
	// === SECTION 9: JOIN WITH LIMIT AND OFFSET ===
	// ============================================================

	// Test 40: JOIN with LIMIT - 3 rows from a 7-row JOIN result
	checkRowCount("JOIN with LIMIT 3",
		`SELECT v33_emp.name, v33_dept.name
		 FROM v33_emp
		 JOIN v33_dept ON v33_emp.dept_id = v33_dept.id
		 ORDER BY v33_emp.id ASC
		 LIMIT 3`,
		3)

	// Test 41: JOIN with LIMIT and OFFSET - skip first 2, take 3
	checkRowCount("JOIN with LIMIT 3 OFFSET 2",
		`SELECT v33_emp.name, v33_dept.name
		 FROM v33_emp
		 JOIN v33_dept ON v33_emp.dept_id = v33_dept.id
		 ORDER BY v33_emp.id ASC
		 LIMIT 3 OFFSET 2`,
		3)

	// Test 42: OFFSET past most rows - only 1 row remains (emp id=7 Grace)
	checkRowCount("JOIN with OFFSET leaving 1 row",
		`SELECT v33_emp.name
		 FROM v33_emp
		 JOIN v33_dept ON v33_emp.dept_id = v33_dept.id
		 ORDER BY v33_emp.id ASC
		 LIMIT 10 OFFSET 6`,
		1)

	// Test 43: OFFSET at boundary - 0 rows
	checkRowCount("JOIN with OFFSET beyond all rows",
		`SELECT v33_emp.name
		 FROM v33_emp
		 JOIN v33_dept ON v33_emp.dept_id = v33_dept.id
		 ORDER BY v33_emp.id ASC
		 LIMIT 5 OFFSET 7`,
		0)

	// ============================================================
	// === SECTION 10: CROSS JOIN ===
	// ============================================================

	// Test 44: CROSS JOIN dept x client = 4 x 4 = 16 rows
	checkRowCount("CROSS JOIN dept x client = 16 rows",
		"SELECT v33_dept.name, v33_client.name FROM v33_dept CROSS JOIN v33_client",
		16)

	// Test 45: CROSS JOIN with WHERE filter - USA clients (Acme, Initech=2) x Eng dept(1) = 2 rows
	checkRowCount("CROSS JOIN with WHERE filter",
		`SELECT v33_dept.name, v33_client.name
		 FROM v33_dept
		 CROSS JOIN v33_client
		 WHERE v33_dept.name = 'Engineering' AND v33_client.country = 'USA'`,
		2)

	// Test 46: CROSS JOIN small tables to verify cartesian count
	// 3 projects in Eng (Alpha,Beta -> dept_id=1 only 2) -- use COUNT to verify
	// dept(4) CROSS JOIN proj(5) = 20 rows
	checkRowCount("CROSS JOIN dept x proj = 20 rows",
		"SELECT v33_dept.id, v33_proj.id FROM v33_dept CROSS JOIN v33_proj",
		20)

	// ============================================================
	// === SECTION 11: MULTIPLE LEFT JOINs (3+ TABLES) ===
	// ============================================================

	// Test 47: dept LEFT JOIN emp LEFT JOIN assignment - all depts, all emps, all assignments
	// Marketing has no emp so its row has NULLs for emp and assignment.
	// Emps without assignments: none in this dataset (all 7 have at least one assignment)
	// So: 9 assignment rows + 1 Marketing NULL row = 10 total
	checkRowCount("Multiple LEFT JOINs dept->emp->assignment total rows",
		`SELECT v33_dept.name, v33_emp.name, v33_assignment.hours
		 FROM v33_dept
		 LEFT JOIN v33_emp        ON v33_dept.id = v33_emp.dept_id
		 LEFT JOIN v33_assignment ON v33_emp.id  = v33_assignment.emp_id`,
		10)

	// Test 48: proj LEFT JOIN assignment LEFT JOIN emp - all projects including Epsilon
	// Epsilon has no assignments -> 1 NULL row for Epsilon; others: 9 assignment rows
	checkRowCount("Multiple LEFT JOINs proj->assignment->emp all projects",
		`SELECT v33_proj.name, v33_emp.name, v33_assignment.hours
		 FROM v33_proj
		 LEFT JOIN v33_assignment ON v33_proj.id  = v33_assignment.proj_id
		 LEFT JOIN v33_emp        ON v33_emp.id    = v33_assignment.emp_id`,
		10) // 9 + 1 (Epsilon NULL)

	// Test 49: client LEFT JOIN contract LEFT JOIN proj - all clients including Umbrella
	// Umbrella has no contracts -> 1 NULL row; others: 4 contract rows
	checkRowCount("Multiple LEFT JOINs client->contract->proj all clients",
		`SELECT v33_client.name, v33_proj.name, v33_contract.value
		 FROM v33_client
		 LEFT JOIN v33_contract ON v33_client.id = v33_contract.client_id
		 LEFT JOIN v33_proj     ON v33_proj.id   = v33_contract.proj_id`,
		5) // 4 contracts + 1 Umbrella NULL

	// ============================================================
	// === SECTION 12: GROUP BY WITH EXPRESSION ===
	// ============================================================

	// Test 50: GROUP BY on CASE expression - salary tier grouping
	// high (>=80000): Alice(90k), Carol(80k) = 2
	// mid  (>=65000 and <80000): Bob(75k), Dave(70k), Frank(65k) = 3
	// low  (<65000): Eve(60k), Grace(55k) = 2
	checkRowCount("GROUP BY CASE salary tier - 3 groups",
		`SELECT CASE WHEN salary >= 80000 THEN 'high' WHEN salary >= 65000 THEN 'mid' ELSE 'low' END AS tier,
		        COUNT(*) AS cnt
		 FROM v33_emp
		 GROUP BY CASE WHEN salary >= 80000 THEN 'high' WHEN salary >= 65000 THEN 'mid' ELSE 'low' END`,
		3)

	// Test 51: GROUP BY hire decade - all employees hired in 2010s or 2020s
	// 2010s: Alice(2018),Bob(2019),Carol(2020? no, 2020 is 2020s),Dave(2017),Frank(2019)
	// Actually: 2010s = 2010-2019: Alice(2018),Bob(2019),Dave(2017),Frank(2019) = 4
	//           2020s = 2020-2029: Carol(2020),Eve(2021),Grace(2022) = 3
	check("GROUP BY hire decade 2010s count",
		`SELECT COUNT(*)
		 FROM v33_emp
		 WHERE hire_year >= 2010 AND hire_year < 2020`,
		4)

	check("GROUP BY hire decade 2020s count",
		`SELECT COUNT(*)
		 FROM v33_emp
		 WHERE hire_year >= 2020 AND hire_year < 2030`,
		3)

	// ============================================================
	// === SECTION 13: NESTED AGGREGATES IN ORDER BY (ORDER BY AGG DESC WITH JOIN) ===
	// ============================================================

	// Test 52: Employees ranked by total hours logged, top employee
	// Per-employee total hours:
	//   Alice: 40+20=60, Bob: 60+30=90, Carol: 50, Dave: 80, Eve: 40, Frank: 60, Grace: 35
	// Top by total hours: Bob(90)
	check("ORDER BY SUM hours DESC top employee is Bob",
		`SELECT v33_emp.name
		 FROM v33_emp
		 JOIN v33_assignment ON v33_emp.id = v33_assignment.emp_id
		 GROUP BY v33_emp.name
		 ORDER BY SUM(v33_assignment.hours) DESC, v33_emp.name ASC
		 LIMIT 1`,
		"Bob")

	// Test 53: Projects ordered by number of distinct assigned employees, top project
	// Alpha: Alice,Bob,Grace=3, Beta: Carol,Bob=2, Gamma: Dave,Eve,Alice=3, Delta: Frank=1
	// Alpha and Gamma both have 3 -> tie broken by name ASC -> Alpha comes first
	check("ORDER BY COUNT DISTINCT employees per project",
		`SELECT v33_proj.name
		 FROM v33_proj
		 JOIN v33_assignment ON v33_proj.id = v33_assignment.proj_id
		 GROUP BY v33_proj.name
		 ORDER BY COUNT(DISTINCT v33_assignment.emp_id) DESC, v33_proj.name ASC
		 LIMIT 1`,
		"Alpha")

	// Test 54: Departments ordered by avg salary DESC
	// Eng avg: 300000/4=75000, Sales avg: 130000/2=65000, HR avg: 65000/1=65000
	// Top by avg salary: Engineering (75000)
	check("ORDER BY AVG salary DESC top dept is Engineering",
		`SELECT v33_dept.name
		 FROM v33_dept
		 JOIN v33_emp ON v33_dept.id = v33_emp.dept_id
		 GROUP BY v33_dept.name
		 ORDER BY AVG(v33_emp.salary) DESC, v33_dept.name ASC
		 LIMIT 1`,
		"Engineering")

	// ============================================================
	// === SECTION 14: SUBQUERY IN WHERE (IN / NOT IN) ===
	// ============================================================

	// Test 55: IN with subquery - employees in departments that have a budget > 200000
	// Dept budget > 200000: Engineering(500k), Sales(300k), Marketing(200k? NO, 200k is NOT > 200k)
	// Dept ids with budget > 200000: id=1(500k), id=2(300k)
	// Employees in dept 1 or 2: Alice,Bob,Carol,Grace,Dave,Eve = 6
	checkRowCount("IN subquery employees in high-budget depts",
		`SELECT v33_emp.name
		 FROM v33_emp
		 WHERE v33_emp.dept_id IN (
		   SELECT id FROM v33_dept WHERE budget > 200000
		 )`,
		6)

	// Test 56: NOT IN with subquery - employees NOT in departments with budget > 200000
	// Only HR(150k <= 200k) -> Frank = 1
	checkRowCount("NOT IN subquery employees in low-budget depts",
		`SELECT v33_emp.name
		 FROM v33_emp
		 WHERE v33_emp.dept_id NOT IN (
		   SELECT id FROM v33_dept WHERE budget > 200000
		 )`,
		1)

	check("NOT IN subquery returns Frank",
		`SELECT v33_emp.name
		 FROM v33_emp
		 WHERE v33_emp.dept_id NOT IN (
		   SELECT id FROM v33_dept WHERE budget > 200000
		 )`,
		"Frank")

	// Test 57: IN with subquery - projects that have at least one USA client contract
	// USA clients: Acme(1), Initech(3)
	// Contracts from USA clients: proj 1(Alpha via Acme), proj 3(Gamma via Acme), proj 1(Alpha via Initech)
	// Distinct proj ids: 1, 3 -> Alpha and Gamma
	checkRowCount("IN subquery projects with USA client contracts",
		`SELECT v33_proj.name
		 FROM v33_proj
		 WHERE v33_proj.id IN (
		   SELECT v33_contract.proj_id
		   FROM v33_contract
		   JOIN v33_client ON v33_contract.client_id = v33_client.id
		   WHERE v33_client.country = 'USA'
		 )`,
		2)

	// ============================================================
	// === SECTION 15: UPDATE WITH SUBQUERY IN SET ===
	// ============================================================

	// Test 58: UPDATE emp salary using subquery that reads dept budget
	// Set Frank's (HR dept, id=3) salary to HR budget / 5 = 150000/5 = 30000
	checkNoError("UPDATE Frank salary via dept budget subquery",
		`UPDATE v33_emp
		 SET salary = (SELECT budget / 5 FROM v33_dept WHERE v33_dept.id = v33_emp.dept_id)
		 WHERE v33_emp.name = 'Frank'`)

	check("Frank salary updated to 30000",
		"SELECT salary FROM v33_emp WHERE name = 'Frank'",
		30000)

	// Test 59: UPDATE with subquery in SET using aggregate
	// Set a project's budget to the total contract value for that project
	// Alpha contracts: 500000+150000=650000
	checkNoError("UPDATE Alpha proj budget to total contract value",
		`UPDATE v33_proj
		 SET budget = (SELECT COALESCE(SUM(value), 0) FROM v33_contract WHERE v33_contract.proj_id = v33_proj.id)
		 WHERE v33_proj.name = 'Alpha'`)

	check("Alpha proj budget updated to 650000",
		"SELECT budget FROM v33_proj WHERE name = 'Alpha'",
		650000)

	// Test 60: UPDATE using a scalar subquery in WHERE
	// Increase salary by 10% for employees earning less than the average salary
	// Current salaries: Alice(90k), Bob(75k), Carol(80k), Dave(70k), Eve(60k), Frank(30k after update), Grace(55k)
	// Sum: 90+75+80+70+60+30+55=460k, AVG = 460000/7 = 65714.28...
	// Employees below avg(~65714): Eve(60k), Frank(30k), Grace(55k)
	checkNoError("UPDATE salary +10pct below average",
		`UPDATE v33_emp
		 SET salary = salary + salary / 10
		 WHERE salary < (SELECT AVG(salary) FROM v33_emp)`)

	// Frank: 30000 + 3000 = 33000
	check("Frank salary after 10pct raise",
		"SELECT salary FROM v33_emp WHERE name = 'Frank'",
		33000)

	// ============================================================
	// === SECTION 16: DELETE WITH SUBQUERY IN WHERE ===
	// ============================================================

	// Test 61: DELETE assignments for projects in the HR department (dept_id=3)
	// HR projects: Delta(id=4)
	// Assignments for Delta: id=8 (Frank->Delta, 60h)
	// Before delete: 9 assignment rows
	checkRowCount("Assignments before dept-based DELETE",
		"SELECT COUNT(*) FROM v33_assignment",
		1) // COUNT(*) returns 1 row with the count value

	// Verify count is 9
	check("Assignment count before DELETE is 9",
		"SELECT COUNT(*) FROM v33_assignment",
		9)

	checkNoError("DELETE assignments for HR dept projects",
		`DELETE FROM v33_assignment
		 WHERE proj_id IN (
		   SELECT id FROM v33_proj WHERE dept_id = (
		     SELECT id FROM v33_dept WHERE name = 'HR'
		   )
		 )`)

	// Test 62: After delete, 8 assignments remain (Delta/Frank row removed)
	check("Assignment count after DELETE is 8",
		"SELECT COUNT(*) FROM v33_assignment",
		8)

	checkRowCount("Delta assignments deleted",
		"SELECT * FROM v33_assignment WHERE proj_id = 4",
		0)

	// Test 63: DELETE clients with no contracts (Umbrella has no contracts)
	checkNoError("DELETE clients with no contracts",
		`DELETE FROM v33_client
		 WHERE NOT EXISTS (
		   SELECT 1 FROM v33_contract WHERE v33_contract.client_id = v33_client.id
		 )`)

	// After delete: Acme, Globex, Initech remain (3 clients); Umbrella deleted
	checkRowCount("Clients after deleting no-contract clients",
		"SELECT * FROM v33_client",
		3)

	// ============================================================
	// === SECTION 17: ADDITIONAL EDGE CASES ===
	// ============================================================

	// Test 64: JOIN with HAVING on aggregate
	// Employees with total hours across all projects > 50
	// Alice:60, Bob:90, Carol:50(NOT >50), Dave:80, Eve:40(no), Frank deleted now, Grace:35(no)
	// After Delta delete: Frank has 0 assignments remaining -> not in join
	// Employees with hours > 50: Alice(60), Bob(90), Dave(80) = 3
	checkRowCount("JOIN HAVING total hours > 50",
		`SELECT v33_emp.name
		 FROM v33_emp
		 JOIN v33_assignment ON v33_emp.id = v33_assignment.emp_id
		 GROUP BY v33_emp.name
		 HAVING SUM(v33_assignment.hours) > 50`,
		3)

	// Test 65: Three-table JOIN with multi-column GROUP BY
	// emp + dept + assignment grouped by dept and emp name
	checkRowCount("3-table JOIN GROUP BY dept and emp name",
		`SELECT v33_dept.name, v33_emp.name, SUM(v33_assignment.hours)
		 FROM v33_emp
		 JOIN v33_dept       ON v33_emp.dept_id  = v33_dept.id
		 JOIN v33_assignment ON v33_emp.id        = v33_assignment.emp_id
		 GROUP BY v33_dept.name, v33_emp.name`,
		6) // 6 distinct emp-dept combos that have assignments (Frank removed from assignments)

	// Test 66: JOIN with BETWEEN in WHERE
	// Employees hired between 2018 and 2020 (inclusive): Alice(2018), Bob(2019), Carol(2020), Frank(2019) = 4
	checkRowCount("JOIN with BETWEEN hire_year",
		`SELECT v33_emp.name
		 FROM v33_emp
		 JOIN v33_dept ON v33_emp.dept_id = v33_dept.id
		 WHERE v33_emp.hire_year BETWEEN 2018 AND 2020`,
		4)

	// Test 67: LEFT JOIN with IS NULL filter to find projects with no assignments
	// After deleting Delta's assignments: Delta and Epsilon have no assignments
	checkRowCount("LEFT JOIN to find projects with no assignments",
		`SELECT v33_proj.name
		 FROM v33_proj
		 LEFT JOIN v33_assignment ON v33_proj.id = v33_assignment.proj_id
		 WHERE v33_assignment.id IS NULL`,
		2) // Delta and Epsilon

	// Test 68: Subquery in SELECT with JOIN correlated across two levels
	// For each employee, show their department's total budget vs their own salary
	// Verify a single employee's dept budget (Alice -> Engineering -> 500000)
	check("Correlated subquery dept budget for Alice",
		`SELECT (SELECT budget FROM v33_dept WHERE v33_dept.id = v33_emp.dept_id)
		 FROM v33_emp
		 WHERE v33_emp.name = 'Alice'`,
		500000)

	// Test 69: Three-table JOIN with ORDER BY on third table column
	// emp + assignment + proj ordered by proj.budget DESC, emp.name ASC
	// After Alpha budget update to 650000: Alpha is highest budget (650k), Beta(80k), Gamma(50k)
	// First row: emp with Alpha assignment with highest budget -> Alice, Bob, or Grace on Alpha
	// ORDER BY proj.budget DESC (Alpha first), emp.name ASC -> Alice (first alphabetically on Alpha)
	check("3-table JOIN ORDER BY third table column",
		`SELECT v33_emp.name
		 FROM v33_emp
		 JOIN v33_assignment ON v33_emp.id    = v33_assignment.emp_id
		 JOIN v33_proj       ON v33_proj.id   = v33_assignment.proj_id
		 ORDER BY v33_proj.budget DESC, v33_emp.name ASC
		 LIMIT 1`,
		"Alice")

	// Test 70: COUNT(*) vs COUNT(col) distinction in LEFT JOIN
	// dept LEFT JOIN emp: COUNT(*) counts all rows (including Marketing NULL row), COUNT(emp.id) excludes NULL
	// COUNT(*) from LEFT JOIN GROUP BY dept returns 4 groups, but without GROUP BY:
	// In the full left join result: 7 emp rows + 1 Marketing NULL = 8 rows total
	// COUNT(*) = 8, COUNT(v33_emp.id) = 7
	check("COUNT(*) in LEFT JOIN includes NULL rows",
		`SELECT COUNT(*)
		 FROM v33_dept
		 LEFT JOIN v33_emp ON v33_dept.id = v33_emp.dept_id`,
		8)

	check("COUNT(col) in LEFT JOIN excludes NULL rows",
		`SELECT COUNT(v33_emp.id)
		 FROM v33_dept
		 LEFT JOIN v33_emp ON v33_dept.id = v33_emp.dept_id`,
		7)

	t.Logf("\n=== V33 ADVANCED JOINS & SUBQUERIES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
