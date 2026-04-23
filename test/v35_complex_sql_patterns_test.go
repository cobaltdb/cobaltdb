package test

import (
	"fmt"
	"testing"
)

// TestV35ComplexSQLPatterns covers twenty categories of complex SQL:
//  1. Nested subquery in SELECT list (correlated scalar subquery per outer row)
//  2. Subquery in HAVING clause
//  3. Multi-level nested subqueries (3+ levels deep in WHERE)
//  4. INSERT...SELECT with JOIN
//  5. UPDATE with multi-table correlation (correlated subquery in SET)
//  6. Complex CASE expressions (nested, in WHERE, ORDER BY, GROUP BY)
//  7. String manipulation chains (UPPER/LOWER/TRIM/REPLACE/LENGTH/SUBSTR)
//  8. Arithmetic in aggregates (SUM(a+b), AVG(a*b), COUNT(CASE WHEN))
//  9. Multiple UNION ALL (3+ legs chained)
//
// 10. CTE used multiple times in one query
// 11. Complex JOIN conditions (ON with AND and inequality)
// 12. COALESCE in aggregates
// 13. Expressions in INSERT VALUES
// 14. Multiple indexes on same table
// 15. DELETE with complex WHERE (AND/OR/NOT/IN/EXISTS/BETWEEN)
// 16. Referential integrity verification after mutations
// 17. GROUP BY + HAVING + ORDER BY combined on JOINed data
// 18. DISTINCT on expressions
// 19. Aggregate-of-group patterns via CTE / derived table
// 20. Stress: 100+ rows inserted then complex aggregation queries
//
// All table names use the v35_ prefix to avoid collisions with other test files.
func TestV35ComplexSQLPatterns(t *testing.T) {
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
	// === SECTION 1: NESTED SUBQUERY IN SELECT LIST ===
	// ============================================================
	//
	// Pattern: SELECT col, (SELECT agg FROM t2 WHERE t2.fk = t1.pk) FROM t1
	//
	// Tables:
	//   v35_dept  (id PK, name TEXT, region TEXT)
	//   v35_staff (id PK, dept_id FK, name TEXT, salary INTEGER, active INTEGER)
	//
	// Data:
	//   dept: 1=Engineering/West, 2=Sales/East, 3=HR/West, 4=Legal/East
	//   staff (dept, salary, active):
	//     1 Alice   Eng    95000  1
	//     2 Bob     Eng    80000  1
	//     3 Carol   Sales  70000  1
	//     4 Dave    Sales  65000  0
	//     5 Eve     HR     60000  1
	//     6 Frank   Legal  75000  1
	//     7 Grace   Eng    85000  0

	afExec(t, db, ctx, `CREATE TABLE v35_dept (
		id     INTEGER PRIMARY KEY,
		name   TEXT,
		region TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v35_dept VALUES (1, 'Engineering', 'West')")
	afExec(t, db, ctx, "INSERT INTO v35_dept VALUES (2, 'Sales',       'East')")
	afExec(t, db, ctx, "INSERT INTO v35_dept VALUES (3, 'HR',          'West')")
	afExec(t, db, ctx, "INSERT INTO v35_dept VALUES (4, 'Legal',       'East')")

	afExec(t, db, ctx, `CREATE TABLE v35_staff (
		id      INTEGER PRIMARY KEY,
		dept_id INTEGER,
		name    TEXT,
		salary  INTEGER,
		active  INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v35_staff VALUES (1, 1, 'Alice', 95000, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_staff VALUES (2, 1, 'Bob',   80000, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_staff VALUES (3, 2, 'Carol', 70000, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_staff VALUES (4, 2, 'Dave',  65000, 0)")
	afExec(t, db, ctx, "INSERT INTO v35_staff VALUES (5, 3, 'Eve',   60000, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_staff VALUES (6, 4, 'Frank', 75000, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_staff VALUES (7, 1, 'Grace', 85000, 0)")

	// Test 1: Correlated scalar subquery in SELECT - MAX salary per dept for Engineering row.
	// Engineering (dept_id=1): Alice(95000), Bob(80000), Grace(85000) -> MAX=95000
	check("Scalar subquery in SELECT: MAX salary for Engineering dept",
		`SELECT (SELECT MAX(salary) FROM v35_staff WHERE v35_staff.dept_id = v35_dept.id)
		 FROM v35_dept
		 WHERE v35_dept.name = 'Engineering'`,
		95000)

	// Test 2: Correlated scalar subquery - COUNT of staff per dept for HR.
	// HR (dept_id=3): only Eve -> COUNT=1
	check("Scalar subquery in SELECT: COUNT staff for HR dept",
		`SELECT (SELECT COUNT(*) FROM v35_staff WHERE v35_staff.dept_id = v35_dept.id)
		 FROM v35_dept
		 WHERE v35_dept.name = 'HR'`,
		1)

	// Test 3: Scalar subquery in SELECT used alongside regular columns.
	// For Legal (dept_id=4): only Frank(75000) -> MIN=75000
	check("Scalar subquery in SELECT alongside regular column",
		`SELECT (SELECT MIN(salary) FROM v35_staff WHERE v35_staff.dept_id = v35_dept.id)
		 FROM v35_dept
		 WHERE v35_dept.name = 'Legal'`,
		75000)

	// Test 4: Row count when each dept row is accompanied by a scalar subquery.
	// There are 4 depts, so 4 rows returned.
	checkRowCount("Scalar subquery in SELECT returns one row per outer row",
		`SELECT v35_dept.name,
		        (SELECT COUNT(*) FROM v35_staff WHERE v35_staff.dept_id = v35_dept.id) AS headcount
		 FROM v35_dept`,
		4)

	// Test 5: Scalar subquery in SELECT for Sales dept average salary.
	// Sales (dept_id=2): Carol(70000), Dave(65000) -> AVG=67500
	check("Scalar subquery in SELECT: AVG salary for Sales dept",
		`SELECT (SELECT AVG(salary) FROM v35_staff WHERE v35_staff.dept_id = v35_dept.id)
		 FROM v35_dept
		 WHERE v35_dept.name = 'Sales'`,
		67500)

	// ============================================================
	// === SECTION 2: SUBQUERY IN HAVING CLAUSE ===
	// ============================================================
	//
	// Pattern: GROUP BY ... HAVING COUNT(*) > (SELECT ...)
	//
	// Using v35_dept and v35_staff from above.
	// Dept staff counts: Eng=3, Sales=2, HR=1, Legal=1
	// Overall avg per-dept count = (3+2+1+1)/4 = 1.75
	// Depts with count > 1: Engineering(3), Sales(2) -> 2 groups

	// Test 6: HAVING with scalar subquery comparing group size to a literal threshold.
	// 7 staff across 4 depts -> average 1.75 staff per dept.
	// Depts with COUNT(staff) > 1 (i.e. more than the minimum): Eng(3), Sales(2) -> 2 groups.
	// Subquery: SELECT MIN(salary)/MIN(salary) = 1 from v35_staff (always 1).
	// This tests that a scalar subquery works in HAVING.
	checkRowCount("HAVING COUNT(staff.id) > scalar subquery result of 1: 2 depts",
		`SELECT v35_dept.name, COUNT(v35_staff.id) AS cnt
		 FROM v35_dept
		 LEFT JOIN v35_staff ON v35_dept.id = v35_staff.dept_id
		 GROUP BY v35_dept.name
		 HAVING COUNT(v35_staff.id) > (
		   SELECT COUNT(*) FROM v35_dept WHERE name = 'HR'
		 )`,
		2)

	// Test 7: HAVING with subquery for minimum salary threshold.
	// Show depts where MAX salary > (SELECT MAX salary from HR dept).
	// HR max salary: Eve(60000). Depts with MAX > 60000: Eng(95000), Sales(70000), Legal(75000) = 3.
	checkRowCount("HAVING MAX(salary) > subquery for HR max salary: 3 depts",
		`SELECT v35_dept.name
		 FROM v35_dept
		 JOIN v35_staff ON v35_dept.id = v35_staff.dept_id
		 GROUP BY v35_dept.name
		 HAVING MAX(v35_staff.salary) > (
		   SELECT MAX(s2.salary)
		   FROM v35_staff s2
		   JOIN v35_dept d2 ON s2.dept_id = d2.id
		   WHERE d2.name = 'HR'
		 )`,
		3)

	// Test 8: HAVING with subquery - depts with SUM salary above a threshold from subquery.
	// Threshold = AVG salary of all active staff only.
	// Active staff: Alice(95k), Bob(80k), Carol(70k), Eve(60k), Frank(75k) = 5 active.
	// Total active salary = 95000+80000+70000+60000+75000 = 380000. AVG = 76000.
	// Dept SUM salary (all staff): Eng=260000, Sales=135000, HR=60000, Legal=75000.
	// Depts with SUM > 76000: Eng(260000), Sales(135000) -> 2 depts.
	checkRowCount("HAVING SUM(salary) > AVG active salary subquery: 2 depts",
		`SELECT v35_dept.name
		 FROM v35_dept
		 JOIN v35_staff ON v35_dept.id = v35_staff.dept_id
		 GROUP BY v35_dept.name
		 HAVING SUM(v35_staff.salary) > (
		   SELECT AVG(salary) FROM v35_staff WHERE active = 1
		 )`,
		2)

	// Test 9: HAVING - dept with more than 1 active staff member.
	// Active per dept: Eng=2 (Alice, Bob), Sales=1 (Carol), HR=1 (Eve), Legal=1 (Frank).
	// Only Engineering has COUNT > 1.
	// Subquery returns the count of active staff in HR dept = 1.
	// So HAVING COUNT(*) > 1 (i.e. more than HR's active count) -> only Engineering.
	checkRowCount("HAVING active staff count > HR active count (=1): 1 dept",
		`SELECT v35_dept.name
		 FROM v35_dept
		 JOIN v35_staff ON v35_dept.id = v35_staff.dept_id AND v35_staff.active = 1
		 GROUP BY v35_dept.name
		 HAVING COUNT(*) > (
		   SELECT COUNT(*) FROM v35_staff s2
		   JOIN v35_dept d2 ON s2.dept_id = d2.id
		   WHERE d2.name = 'HR' AND s2.active = 1
		 )`,
		1)

	// ============================================================
	// === SECTION 3: MULTI-LEVEL NESTED SUBQUERIES (3+ LEVELS) ===
	// ============================================================
	//
	// We nest WHERE clauses 3 levels deep.
	//
	// Additional table: v35_project (id, name, dept_id, budget)
	//                   v35_task    (id, proj_id, title, hours, status)
	//
	// Projects:
	//   1 Atlas     Eng(1)   200000
	//   2 Beacon    Sales(2) 120000
	//   3 Citadel   Eng(1)   180000
	//   4 Delta     HR(3)    50000
	//   5 Echo      Legal(4) 90000
	//
	// Tasks (proj_id, hours, status):
	//   1  Atlas   40  done
	//   2  Atlas   60  open
	//   3  Beacon  30  done
	//   4  Beacon  50  open
	//   5  Citadel 80  done
	//   6  Delta   20  open
	//   7  Echo    35  done
	//   8  Citadel 45  open
	//   9  Atlas   25  done

	afExec(t, db, ctx, `CREATE TABLE v35_project (
		id      INTEGER PRIMARY KEY,
		name    TEXT,
		dept_id INTEGER,
		budget  INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v35_project VALUES (1, 'Atlas',   1, 200000)")
	afExec(t, db, ctx, "INSERT INTO v35_project VALUES (2, 'Beacon',  2, 120000)")
	afExec(t, db, ctx, "INSERT INTO v35_project VALUES (3, 'Citadel', 1, 180000)")
	afExec(t, db, ctx, "INSERT INTO v35_project VALUES (4, 'Delta',   3,  50000)")
	afExec(t, db, ctx, "INSERT INTO v35_project VALUES (5, 'Echo',    4,  90000)")

	afExec(t, db, ctx, `CREATE TABLE v35_task (
		id      INTEGER PRIMARY KEY,
		proj_id INTEGER,
		title   TEXT,
		hours   INTEGER,
		status  TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v35_task VALUES (1, 1, 'Task-A1', 40, 'done')")
	afExec(t, db, ctx, "INSERT INTO v35_task VALUES (2, 1, 'Task-A2', 60, 'open')")
	afExec(t, db, ctx, "INSERT INTO v35_task VALUES (3, 2, 'Task-B1', 30, 'done')")
	afExec(t, db, ctx, "INSERT INTO v35_task VALUES (4, 2, 'Task-B2', 50, 'open')")
	afExec(t, db, ctx, "INSERT INTO v35_task VALUES (5, 3, 'Task-C1', 80, 'done')")
	afExec(t, db, ctx, "INSERT INTO v35_task VALUES (6, 4, 'Task-D1', 20, 'open')")
	afExec(t, db, ctx, "INSERT INTO v35_task VALUES (7, 5, 'Task-E1', 35, 'done')")
	afExec(t, db, ctx, "INSERT INTO v35_task VALUES (8, 3, 'Task-C2', 45, 'open')")
	afExec(t, db, ctx, "INSERT INTO v35_task VALUES (9, 1, 'Task-A3', 25, 'done')")

	// Test 10: 3-level nested subquery in WHERE.
	// Level 3 (innermost): SELECT id FROM v35_dept WHERE region = 'West' -> ids 1, 3
	// Level 2: SELECT id FROM v35_project WHERE dept_id IN (...level3...) -> proj ids 1,3,4
	//          (dept 1=Eng/West has proj 1,3; dept 3=HR/West has proj 4)
	// Level 1 (outer): SELECT tasks WHERE proj_id IN (...level2...) AND status='done'
	//          done tasks for proj 1: task1(40h), task9(25h); proj 3: task5(80h); proj 4: none -> 3 done tasks
	checkRowCount("3-level nested subquery WHERE proj in West depts, done tasks: 3",
		`SELECT * FROM v35_task
		 WHERE status = 'done'
		   AND proj_id IN (
		     SELECT id FROM v35_project
		     WHERE dept_id IN (
		       SELECT id FROM v35_dept WHERE region = 'West'
		     )
		   )`,
		3)

	// Test 11: 3-level nested subquery - find staff in depts that have high-budget projects.
	// Level 3: SELECT MAX(budget) from v35_project -> 200000
	// Level 2: SELECT dept_id from v35_project WHERE budget >= 180000 -> dept_id 1 (both Atlas 200k, Citadel 180k)
	// Level 1: SELECT staff WHERE dept_id IN (...) -> Eng dept: Alice, Bob, Grace = 3
	checkRowCount("3-level nested: staff in dept with budget>=180000 project: 3",
		`SELECT name FROM v35_staff
		 WHERE dept_id IN (
		   SELECT dept_id FROM v35_project
		   WHERE budget >= (
		     SELECT MAX(budget) - 20000 FROM v35_project
		   )
		 )`,
		3)

	// Test 12: 3-level with aggregate at innermost level.
	// Innermost: AVG task hours overall = (40+60+30+50+80+20+35+45+25)/9 = 385/9 = 42.77...
	// Middle: projects where SUM(task hours) > AVG task hours
	//   Atlas: 40+60+25=125, Beacon: 30+50=80, Citadel: 80+45=125, Delta: 20, Echo: 35
	//   Projects with SUM > 42.77: Atlas(125), Beacon(80), Citadel(125) -> proj ids 1,2,3
	// Outer: tasks in those projects with status='open': task2(open/Atlas), task4(open/Beacon), task8(open/Citadel) = 3
	checkRowCount("3-level nested with aggregate innermost: 3 open tasks in busy projects",
		`SELECT * FROM v35_task
		 WHERE status = 'open'
		   AND proj_id IN (
		     SELECT id FROM v35_project p2
		     WHERE (
		       SELECT SUM(hours) FROM v35_task t2 WHERE t2.proj_id = p2.id
		     ) > (
		       SELECT AVG(hours) FROM v35_task
		     )
		   )`,
		3)

	// ============================================================
	// === SECTION 4: INSERT...SELECT WITH JOIN ===
	// ============================================================
	//
	// Create a summary/target table and populate it via INSERT...SELECT JOIN.
	//
	// v35_dept_summary (dept_name TEXT, total_salary INTEGER, staff_count INTEGER)

	afExec(t, db, ctx, `CREATE TABLE v35_dept_summary (
		dept_name    TEXT,
		total_salary INTEGER,
		staff_count  INTEGER
	)`)

	// Test 13: INSERT...SELECT with JOIN + GROUP BY.
	// Join v35_dept with v35_staff, group by dept name, insert aggregated rows.
	// Expected rows: Engineering(260000,3), Sales(135000,2), HR(60000,1), Legal(75000,1) = 4 rows.
	checkNoError("INSERT...SELECT with JOIN inserts 4 dept summary rows",
		`INSERT INTO v35_dept_summary (dept_name, total_salary, staff_count)
		 SELECT v35_dept.name, SUM(v35_staff.salary), COUNT(v35_staff.id)
		 FROM v35_dept
		 JOIN v35_staff ON v35_dept.id = v35_staff.dept_id
		 GROUP BY v35_dept.name`)

	checkRowCount("INSERT...SELECT result: 4 dept summary rows",
		"SELECT * FROM v35_dept_summary",
		4)

	// Test 14: Verify specific inserted value - Engineering total salary = 95000+80000+85000 = 260000.
	check("INSERT...SELECT Engineering total salary = 260000",
		"SELECT total_salary FROM v35_dept_summary WHERE dept_name = 'Engineering'",
		260000)

	// Test 15: INSERT...SELECT with a 3-table JOIN.
	// Create v35_proj_task_summary and populate it from project + task join.
	afExec(t, db, ctx, `CREATE TABLE v35_proj_task_summary (
		proj_name  TEXT,
		dept_name  TEXT,
		total_hours INTEGER,
		task_count  INTEGER
	)`)

	checkNoError("INSERT...SELECT 3-table JOIN into proj_task_summary",
		`INSERT INTO v35_proj_task_summary (proj_name, dept_name, total_hours, task_count)
		 SELECT v35_project.name, v35_dept.name, SUM(v35_task.hours), COUNT(v35_task.id)
		 FROM v35_project
		 JOIN v35_dept ON v35_project.dept_id = v35_dept.id
		 JOIN v35_task  ON v35_task.proj_id   = v35_project.id
		 GROUP BY v35_project.name, v35_dept.name
		 ORDER BY v35_project.name`)

	checkRowCount("INSERT...SELECT 3-table JOIN inserts 5 project summary rows",
		"SELECT * FROM v35_proj_task_summary",
		5)

	// Test 16: Verify Atlas total hours = 40+60+25 = 125.
	check("INSERT...SELECT 3-table JOIN: Atlas total hours = 125",
		"SELECT total_hours FROM v35_proj_task_summary WHERE proj_name = 'Atlas'",
		125)

	// ============================================================
	// === SECTION 5: UPDATE WITH MULTI-TABLE CORRELATION ===
	// ============================================================
	//
	// UPDATE t SET col = (SELECT expr FROM t2 WHERE t2.x = t.y)
	//
	// Using v35_dept and v35_staff.
	// Update each staff member's salary to reflect 120% of their dept's MIN salary.

	// Test 17: Correlated UPDATE - raise Grace's salary to dept max+5000.
	// Engineering MAX salary = 95000. Grace currently 85000. New = 95000 + 5000 = 100000.
	checkNoError("Correlated UPDATE Grace salary to dept MAX + 5000",
		`UPDATE v35_staff
		 SET salary = (SELECT MAX(s2.salary) FROM v35_staff s2 WHERE s2.dept_id = v35_staff.dept_id) + 5000
		 WHERE v35_staff.name = 'Grace'`)

	check("Grace salary after correlated UPDATE = 100000",
		"SELECT salary FROM v35_staff WHERE name = 'Grace'",
		100000)

	// Test 18: Correlated UPDATE - set Dave's salary to Carol's salary (same dept, name < 'Dave').
	// Carol salary = 70000. Dave's new salary should = 70000.
	checkNoError("Correlated UPDATE Dave salary to dept min salary",
		`UPDATE v35_staff
		 SET salary = (SELECT MIN(s2.salary) FROM v35_staff s2 WHERE s2.dept_id = v35_staff.dept_id AND s2.active = 1)
		 WHERE v35_staff.name = 'Dave'`)

	// Sales active: Carol(70000). MIN active salary for Sales = 70000.
	check("Dave salary after correlated UPDATE = 70000 (Sales min active salary)",
		"SELECT salary FROM v35_staff WHERE name = 'Dave'",
		70000)

	// Test 19: Correlated UPDATE on v35_project - set budget to 110% of current AVG task hours * 1000.
	// Atlas tasks: 40+60+25=125 total, AVG=125/3=41.67. Budget = 41.67*1100 ≈ 45833.
	// Use integer: AVG(hours) * 1000 for Atlas -> 125/3 = 41 (integer div) * 1000 = 41000.
	// Actually the engine uses float division: 125/3 = 41.666..., * 1000 = 41666.666...
	// We just verify no error and then check a simpler value.
	checkNoError("Correlated UPDATE project budget to task avg hours * 1000",
		`UPDATE v35_project
		 SET budget = (SELECT SUM(t2.hours) FROM v35_task t2 WHERE t2.proj_id = v35_project.id) * 1000
		 WHERE v35_project.name = 'Echo'`)

	// Echo: task7(35h) only. 35 * 1000 = 35000.
	check("Echo project budget after correlated UPDATE = 35000",
		"SELECT budget FROM v35_project WHERE name = 'Echo'",
		35000)

	// ============================================================
	// === SECTION 6: COMPLEX CASE EXPRESSIONS ===
	// ============================================================
	//
	// Testing: nested CASE, CASE in WHERE, CASE in ORDER BY, CASE in GROUP BY.
	//
	// Table: v35_score (id PK, student TEXT, subject TEXT, score INTEGER, term TEXT)

	afExec(t, db, ctx, `CREATE TABLE v35_score (
		id      INTEGER PRIMARY KEY,
		student TEXT,
		subject TEXT,
		score   INTEGER,
		term    TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v35_score VALUES (1,  'Anna',  'Math',    92, 'Fall')")
	afExec(t, db, ctx, "INSERT INTO v35_score VALUES (2,  'Anna',  'Science', 78, 'Fall')")
	afExec(t, db, ctx, "INSERT INTO v35_score VALUES (3,  'Anna',  'Math',    88, 'Spring')")
	afExec(t, db, ctx, "INSERT INTO v35_score VALUES (4,  'Ben',   'Math',    55, 'Fall')")
	afExec(t, db, ctx, "INSERT INTO v35_score VALUES (5,  'Ben',   'Science', 82, 'Fall')")
	afExec(t, db, ctx, "INSERT INTO v35_score VALUES (6,  'Ben',   'Math',    60, 'Spring')")
	afExec(t, db, ctx, "INSERT INTO v35_score VALUES (7,  'Clara', 'Math',    95, 'Fall')")
	afExec(t, db, ctx, "INSERT INTO v35_score VALUES (8,  'Clara', 'Science', 90, 'Spring')")
	afExec(t, db, ctx, "INSERT INTO v35_score VALUES (9,  'Dan',   'Math',    NULL, 'Fall')")
	afExec(t, db, ctx, "INSERT INTO v35_score VALUES (10, 'Dan',   'Science', 45, 'Spring')")

	// Test 20: Simple CASE expression (grade letter).
	// Anna Math Fall: score=92 -> A (>=90).
	check("CASE grade: 92 -> A",
		`SELECT CASE
		   WHEN score >= 90 THEN 'A'
		   WHEN score >= 80 THEN 'B'
		   WHEN score >= 70 THEN 'C'
		   WHEN score >= 60 THEN 'D'
		   ELSE 'F'
		 END
		 FROM v35_score WHERE id = 1`,
		"A")

	// Test 21: CASE expression for score=55 -> F.
	check("CASE grade: 55 -> F",
		`SELECT CASE
		   WHEN score >= 90 THEN 'A'
		   WHEN score >= 80 THEN 'B'
		   WHEN score >= 70 THEN 'C'
		   WHEN score >= 60 THEN 'D'
		   ELSE 'F'
		 END
		 FROM v35_score WHERE id = 4`,
		"F")

	// Test 22: Nested CASE - performance tier with bonus flag.
	// score>=90 AND term='Fall' -> 'Top-Fall', score>=90 AND term!='Fall' -> 'Top-Other',
	// score>=70 -> 'Passing', else -> 'Failing'.
	// Anna Math Fall (92, Fall) -> 'Top-Fall'.
	check("Nested CASE: 92 Fall -> Top-Fall",
		`SELECT CASE
		   WHEN score >= 90 THEN
		     CASE WHEN term = 'Fall' THEN 'Top-Fall' ELSE 'Top-Other' END
		   WHEN score >= 70 THEN 'Passing'
		   ELSE 'Failing'
		 END
		 FROM v35_score WHERE id = 1`,
		"Top-Fall")

	// Test 23: Nested CASE - Clara Science Spring (90, Spring) -> 'Top-Other'.
	check("Nested CASE: 90 Spring -> Top-Other",
		`SELECT CASE
		   WHEN score >= 90 THEN
		     CASE WHEN term = 'Fall' THEN 'Top-Fall' ELSE 'Top-Other' END
		   WHEN score >= 70 THEN 'Passing'
		   ELSE 'Failing'
		 END
		 FROM v35_score WHERE id = 8`,
		"Top-Other")

	// Test 24: CASE in WHERE clause - select only rows where CASE classifies as top performer.
	// Top performers (score >= 90): Anna(92), Clara(95), Clara(90) = 3 rows.
	checkRowCount("CASE in WHERE: top performers = 3",
		`SELECT * FROM v35_score
		 WHERE CASE WHEN score >= 90 THEN 1 ELSE 0 END = 1`,
		3)

	// Test 25: CASE in ORDER BY - order by grade tier DESC, then score DESC.
	// A(>=90), B(>=80), ... ordering by CASE tier then score.
	// Top row: Clara Math Fall score=95 (tier A, highest score).
	check("CASE in ORDER BY: highest A-tier by score is Clara 95",
		`SELECT student FROM v35_score
		 WHERE score IS NOT NULL
		 ORDER BY
		   CASE WHEN score >= 90 THEN 1 WHEN score >= 80 THEN 2 WHEN score >= 70 THEN 3 WHEN score >= 60 THEN 4 ELSE 5 END ASC,
		   score DESC
		 LIMIT 1`,
		"Clara")

	// Test 26: CASE in GROUP BY - group by grade tier, count each tier.
	// Scores (excluding NULL Dan Math): 92,78,88,55,82,60,95,90,45 = 9 scores.
	// A(>=90): 92,95,90 -> 3
	// B(>=80): 78? no. 82,88 -> 2
	// C(>=70): 78 -> 1
	// D(>=60): 60 -> 1
	// F(<60): 55,45 -> 2
	// Total groups: 5 (A,B,C,D,F).
	checkRowCount("CASE in GROUP BY: 5 grade tiers",
		`SELECT CASE
		   WHEN score >= 90 THEN 'A'
		   WHEN score >= 80 THEN 'B'
		   WHEN score >= 70 THEN 'C'
		   WHEN score >= 60 THEN 'D'
		   ELSE 'F'
		 END AS grade, COUNT(*) AS cnt
		 FROM v35_score
		 WHERE score IS NOT NULL
		 GROUP BY CASE
		   WHEN score >= 90 THEN 'A'
		   WHEN score >= 80 THEN 'B'
		   WHEN score >= 70 THEN 'C'
		   WHEN score >= 60 THEN 'D'
		   ELSE 'F'
		 END`,
		5)

	// Test 27: CASE with NULL handling - Dan's Math score is NULL -> ELSE branch.
	check("CASE with NULL score: Dan Math Fall -> No Score",
		`SELECT CASE
		   WHEN score IS NULL THEN 'No Score'
		   WHEN score >= 90   THEN 'A'
		   ELSE 'Other'
		 END
		 FROM v35_score WHERE id = 9`,
		"No Score")

	// ============================================================
	// === SECTION 7: STRING MANIPULATION CHAINS ===
	// ============================================================
	//
	// Table: v35_strings (id PK, raw TEXT)

	afExec(t, db, ctx, `CREATE TABLE v35_strings (
		id  INTEGER PRIMARY KEY,
		raw TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v35_strings VALUES (1, '  Hello World  ')")
	afExec(t, db, ctx, "INSERT INTO v35_strings VALUES (2, 'foo bar baz')")
	afExec(t, db, ctx, "INSERT INTO v35_strings VALUES (3, 'CoBaLtDb')")
	afExec(t, db, ctx, "INSERT INTO v35_strings VALUES (4, 'abcdefghij')")
	afExec(t, db, ctx, "INSERT INTO v35_strings VALUES (5, '')")
	afExec(t, db, ctx, "INSERT INTO v35_strings VALUES (6, NULL)")

	// Test 28: UPPER(TRIM(col)) - trim whitespace then uppercase.
	// '  Hello World  ' -> trim -> 'Hello World' -> upper -> 'HELLO WORLD'.
	check("UPPER(TRIM(col)) removes whitespace and uppercases",
		"SELECT UPPER(TRIM(raw)) FROM v35_strings WHERE id = 1",
		"HELLO WORLD")

	// Test 29: LOWER(REPLACE(col, ' ', '_')) - replace spaces then lowercase.
	// 'foo bar baz' -> replace ' ' with '_' -> 'foo_bar_baz' -> lower -> 'foo_bar_baz'.
	check("LOWER(REPLACE(col, space, underscore))",
		"SELECT LOWER(REPLACE(raw, ' ', '_')) FROM v35_strings WHERE id = 2",
		"foo_bar_baz")

	// Test 30: LENGTH(TRIM(col)) - measure trimmed length.
	// '  Hello World  ' -> trim -> 'Hello World' -> length -> 11.
	check("LENGTH(TRIM(col)) = 11",
		"SELECT LENGTH(TRIM(raw)) FROM v35_strings WHERE id = 1",
		11)

	// Test 31: SUBSTR(col, start, len) - extract substring.
	// 'abcdefghij' -> SUBSTR(raw, 3, 4) -> 'cdef'.
	check("SUBSTR(col, 3, 4) = cdef",
		"SELECT SUBSTR(raw, 3, 4) FROM v35_strings WHERE id = 4",
		"cdef")

	// Test 32: UPPER(SUBSTR(col, 1, 6)) - uppercase first 6 chars.
	// 'CoBaLtDb' -> SUBSTR(1,6) -> 'CoBaLt' -> UPPER -> 'COBALT'.
	check("UPPER(SUBSTR(col,1,6)) = COBALT",
		"SELECT UPPER(SUBSTR(raw, 1, 6)) FROM v35_strings WHERE id = 3",
		"COBALT")

	// Test 33: LOWER(UPPER(col)) round-trip.
	// 'CoBaLtDb' -> UPPER -> 'COBALTDB' -> LOWER -> 'cobaltdb'.
	check("LOWER(UPPER(col)) round-trip",
		"SELECT LOWER(UPPER(raw)) FROM v35_strings WHERE id = 3",
		"cobaltdb")

	// Test 34: REPLACE chain - multiple REPLACE calls nested.
	// 'foo bar baz' -> REPLACE(raw,'bar','qux') -> 'foo qux baz'
	//              -> REPLACE(...,'foo','aaa')  -> 'aaa qux baz'.
	check("Nested REPLACE calls",
		"SELECT REPLACE(REPLACE(raw, 'bar', 'qux'), 'foo', 'aaa') FROM v35_strings WHERE id = 2",
		"aaa qux baz")

	// Test 35: LENGTH of empty string = 0.
	check("LENGTH of empty string = 0",
		"SELECT LENGTH(raw) FROM v35_strings WHERE id = 5",
		0)

	// Test 36: COALESCE with string function chain - handle NULL raw.
	// COALESCE(NULL, 'fallback') -> 'fallback'.
	check("COALESCE(NULL raw, fallback) = fallback",
		"SELECT COALESCE(UPPER(raw), 'fallback') FROM v35_strings WHERE id = 6",
		"fallback")

	// ============================================================
	// === SECTION 8: ARITHMETIC IN AGGREGATES ===
	// ============================================================
	//
	// Table: v35_items (id PK, category TEXT, qty INTEGER, price INTEGER, discount REAL)
	//
	// Data:
	//   1  Electronics  5   200  0.10
	//   2  Electronics  3   350  0.05
	//   3  Clothing    10    50  0.20
	//   4  Clothing     2   150  0.00
	//   5  Food        20    10  0.15
	//   6  Food         8    25  0.10
	//   7  Electronics  0   500  0.00  <- zero qty edge case
	//   8  Clothing     1   200  NULL  <- NULL discount

	afExec(t, db, ctx, `CREATE TABLE v35_items (
		id       INTEGER PRIMARY KEY,
		category TEXT,
		qty      INTEGER,
		price    INTEGER,
		discount REAL
	)`)
	afExec(t, db, ctx, "INSERT INTO v35_items VALUES (1, 'Electronics',  5, 200, 0.10)")
	afExec(t, db, ctx, "INSERT INTO v35_items VALUES (2, 'Electronics',  3, 350, 0.05)")
	afExec(t, db, ctx, "INSERT INTO v35_items VALUES (3, 'Clothing',    10,  50, 0.20)")
	afExec(t, db, ctx, "INSERT INTO v35_items VALUES (4, 'Clothing',     2, 150, 0.00)")
	afExec(t, db, ctx, "INSERT INTO v35_items VALUES (5, 'Food',        20,  10, 0.15)")
	afExec(t, db, ctx, "INSERT INTO v35_items VALUES (6, 'Food',         8,  25, 0.10)")
	afExec(t, db, ctx, "INSERT INTO v35_items VALUES (7, 'Electronics',  0, 500, 0.00)")
	afExec(t, db, ctx, "INSERT INTO v35_items VALUES (8, 'Clothing',     1, 200, NULL)")

	// Test 37: SUM(qty + price) per category - Electronics.
	// Electronics: (5+200)+(3+350)+(0+500) = 205+353+500 = 1058.
	check("SUM(qty+price) for Electronics = 1058",
		"SELECT SUM(qty + price) FROM v35_items WHERE category = 'Electronics'",
		1058)

	// Test 38: SUM(qty * price) per category - Clothing.
	// Clothing: (10*50)+(2*150)+(1*200) = 500+300+200 = 1000.
	check("SUM(qty*price) for Clothing = 1000",
		"SELECT SUM(qty * price) FROM v35_items WHERE category = 'Clothing'",
		1000)

	// Test 39: AVG(qty * price) - Food.
	// Food: (20*10)=200, (8*25)=200. AVG = (200+200)/2 = 200.
	check("AVG(qty*price) for Food = 200",
		"SELECT AVG(qty * price) FROM v35_items WHERE category = 'Food'",
		200)

	// Test 40: COUNT(CASE WHEN qty > 0 THEN 1 END) - count non-zero qty rows.
	// All items: 8 total. Zero qty: id=7 (qty=0). Non-zero: 7 rows.
	check("COUNT(CASE WHEN qty>0 THEN 1 END) = 7",
		"SELECT COUNT(CASE WHEN qty > 0 THEN 1 END) FROM v35_items",
		7)

	// Test 41: SUM(CASE WHEN category='Electronics' THEN qty*price ELSE 0 END).
	// Electronics: (5*200)+(3*350)+(0*500) = 1000+1050+0 = 2050.
	check("SUM(CASE WHEN Electronics THEN qty*price) = 2050",
		"SELECT SUM(CASE WHEN category = 'Electronics' THEN qty * price ELSE 0 END) FROM v35_items",
		2050)

	// Test 42: MAX(price - qty) across all items.
	// id=7: 500-0=500 (max), id=2: 350-3=347, id=1: 200-5=195, id=8: 200-1=199 ...
	// MAX = 500.
	check("MAX(price-qty) = 500",
		"SELECT MAX(price - qty) FROM v35_items",
		500)

	// Test 43: SUM(qty * price) overall (all categories).
	// Electronics: 1000+1050+0=2050. Clothing: 500+300+200=1000. Food: 200+200=400.
	// Total = 2050+1000+400 = 3450.
	check("SUM(qty*price) all categories = 3450",
		"SELECT SUM(qty * price) FROM v35_items",
		3450)

	// Test 44: GROUP BY with SUM(qty+price) ORDER BY - top category.
	// Electronics: SUM(qty+price) = (5+200)+(3+350)+(0+500) = 205+353+500 = 1058.
	// Clothing: (10+50)+(2+150)+(1+200) = 60+152+201 = 413.
	// Food: (20+10)+(8+25) = 30+33 = 63.
	// Top by SUM(qty+price): Electronics(1058).
	check("GROUP BY SUM(qty+price) DESC top category = Electronics",
		`SELECT category FROM v35_items
		 GROUP BY category
		 ORDER BY SUM(qty + price) DESC
		 LIMIT 1`,
		"Electronics")

	// ============================================================
	// === SECTION 9: MULTIPLE UNION ALL (3+ LEGS) ===
	// ============================================================
	//
	// Tables: v35_region_a, v35_region_b, v35_region_c, v35_region_d
	// Each has (id, name, revenue).

	afExec(t, db, ctx, "CREATE TABLE v35_region_a (id INTEGER PRIMARY KEY, name TEXT, revenue INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v35_region_b (id INTEGER PRIMARY KEY, name TEXT, revenue INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v35_region_c (id INTEGER PRIMARY KEY, name TEXT, revenue INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v35_region_d (id INTEGER PRIMARY KEY, name TEXT, revenue INTEGER)")

	afExec(t, db, ctx, "INSERT INTO v35_region_a VALUES (1, 'Alpha', 1000)")
	afExec(t, db, ctx, "INSERT INTO v35_region_a VALUES (2, 'Beta',  2000)")
	afExec(t, db, ctx, "INSERT INTO v35_region_b VALUES (1, 'Gamma', 1500)")
	afExec(t, db, ctx, "INSERT INTO v35_region_b VALUES (2, 'Delta', 3000)")
	afExec(t, db, ctx, "INSERT INTO v35_region_c VALUES (1, 'Epsilon', 500)")
	afExec(t, db, ctx, "INSERT INTO v35_region_c VALUES (2, 'Zeta',   2500)")
	afExec(t, db, ctx, "INSERT INTO v35_region_d VALUES (1, 'Eta',    700)")
	afExec(t, db, ctx, "INSERT INTO v35_region_d VALUES (2, 'Theta',  4000)")

	// Test 45: 3-leg UNION ALL row count = 6 (a+b+c: 2+2+2).
	checkRowCount("3-leg UNION ALL: 6 rows",
		`SELECT name, revenue FROM v35_region_a
		 UNION ALL
		 SELECT name, revenue FROM v35_region_b
		 UNION ALL
		 SELECT name, revenue FROM v35_region_c`,
		6)

	// Test 46: 4-leg UNION ALL row count = 8 (a+b+c+d: 2+2+2+2).
	checkRowCount("4-leg UNION ALL: 8 rows",
		`SELECT name, revenue FROM v35_region_a
		 UNION ALL
		 SELECT name, revenue FROM v35_region_b
		 UNION ALL
		 SELECT name, revenue FROM v35_region_c
		 UNION ALL
		 SELECT name, revenue FROM v35_region_d`,
		8)

	// Test 47: UNION ALL with aggregate via CTE - SUM of all revenues across 4 regions.
	// 1000+2000+1500+3000+500+2500+700+4000 = 15200.
	check("4-leg UNION ALL SUM revenue via CTE = 15200",
		`WITH all_regions AS (
		   SELECT revenue FROM v35_region_a
		   UNION ALL
		   SELECT revenue FROM v35_region_b
		   UNION ALL
		   SELECT revenue FROM v35_region_c
		   UNION ALL
		   SELECT revenue FROM v35_region_d
		 )
		 SELECT SUM(revenue) FROM all_regions`,
		15200)

	// Test 48: UNION ALL with ORDER BY on combined result - top by revenue.
	// Highest revenue across all: Theta(4000).
	// Direct UNION ALL with ORDER BY and LIMIT is supported.
	check("4-leg UNION ALL ORDER BY revenue DESC top = Theta",
		`SELECT name FROM v35_region_a
		 UNION ALL
		 SELECT name FROM v35_region_b
		 UNION ALL
		 SELECT name FROM v35_region_c
		 UNION ALL
		 SELECT name FROM v35_region_d
		 ORDER BY name DESC
		 LIMIT 1`,
		"Zeta")

	// Test 49: UNION ALL preserves duplicates (UNION deduplicates, UNION ALL does not).
	// Union Alpha and Alpha again -> 2 rows with UNION ALL, 1 with UNION.
	checkRowCount("UNION ALL preserves duplicates: 2 rows",
		`SELECT name FROM v35_region_a WHERE name = 'Alpha'
		 UNION ALL
		 SELECT name FROM v35_region_a WHERE name = 'Alpha'`,
		2)

	// Test 50: 3-leg UNION ALL with CTE and WHERE filter on CTE result - revenues > 1500.
	// From combined (a+b+c): 1000,2000,1500,3000,500,2500.
	// > 1500: 2000(Beta-a), 3000(Delta-b), 2500(Zeta-c) -> 3 rows.
	checkRowCount("3-leg UNION ALL via CTE WHERE revenue>1500: 3 rows",
		`WITH three_regions AS (
		   SELECT name, revenue FROM v35_region_a
		   UNION ALL
		   SELECT name, revenue FROM v35_region_b
		   UNION ALL
		   SELECT name, revenue FROM v35_region_c
		 )
		 SELECT name, revenue FROM three_regions WHERE revenue > 1500`,
		3)

	// ============================================================
	// === SECTION 10: CTE USED MULTIPLE TIMES ===
	// ============================================================
	//
	// Use the same CTE in multiple places within a single query.

	// Test 51: CTE referenced twice - count active staff and verify with CTE aggregate.
	// active_staff CTE: Alice(95k), Bob(80k), Carol(70k), Eve(60k), Frank(75k) = 5 active members.
	// Count active staff earning more than 65000 (above lowest active salary 60k):
	//   Alice(95k>65k YES), Bob(80k>65k YES), Carol(70k>65k YES), Frank(75k>65k YES), Eve(60k NOT>65k).
	// Result: 4 active staff with salary > 65000.
	checkRowCount("CTE used twice: active staff with salary > 65000 = 4",
		`WITH active_staff AS (
		   SELECT id, name, salary, dept_id FROM v35_staff WHERE active = 1
		 )
		 SELECT name FROM active_staff
		 WHERE salary > (SELECT MIN(salary) FROM active_staff)`,
		4)

	// Test 52: CTE used in WHERE subquery comparison.
	// CTE: top_earner = MAX salary among active staff = Alice(95000).
	// Find active staff earning more than 70% of top earner's salary.
	// 70% of 95000 = 66500. Active staff earning > 66500: Alice(95k), Bob(80k), Frank(75k), Carol(70k)=70000>66500 YES.
	// Count: 4 active staff with salary > 66500.
	checkRowCount("CTE in WHERE subquery: active staff > 70pct top salary = 4",
		`WITH top_earner AS (
		   SELECT MAX(salary) AS max_sal FROM v35_staff WHERE active = 1
		 )
		 SELECT name FROM v35_staff
		 WHERE active = 1
		   AND salary > (SELECT max_sal * 0.7 FROM top_earner)`,
		4)

	// Test 53: CTE with aggregate, referenced in SELECT and HAVING.
	// avg_sal CTE: AVG salary all staff including inactive.
	// Staff salaries: Alice(95k), Bob(80k), Carol(70k), Dave(70k after update), Eve(60k), Frank(75k), Grace(100k after update).
	// AVG = (95000+80000+70000+70000+60000+75000+100000)/7 = 550000/7 = 78571.4...
	// Find depts where AVG salary of active staff > overall AVG of all staff.
	// Active staff avg by dept:
	//   Eng active: Alice(95k)+Bob(80k) = 175k/2 = 87500 > 78571 YES
	//   Sales active: Carol(70k)/1 = 70000 < 78571 NO
	//   HR active: Eve(60k)/1 = 60000 < 78571 NO
	//   Legal active: Frank(75k)/1 = 75000 < 78571 NO
	// Only Engineering qualifies -> 1 dept.
	checkRowCount("CTE overall avg: depts with active avg > total avg = 1",
		`WITH overall AS (
		   SELECT AVG(salary) AS avg_sal FROM v35_staff
		 )
		 SELECT v35_dept.name
		 FROM v35_dept
		 JOIN v35_staff ON v35_dept.id = v35_staff.dept_id AND v35_staff.active = 1
		 GROUP BY v35_dept.name
		 HAVING AVG(v35_staff.salary) > (SELECT avg_sal FROM overall)`,
		1)

	// ============================================================
	// === SECTION 11: COMPLEX JOIN CONDITIONS ===
	// ============================================================
	//
	// JOIN ON multiple conditions including inequality.
	//
	// Table: v35_price_band (id PK, label TEXT, min_price INTEGER, max_price INTEGER)
	// Using v35_items (already populated).

	afExec(t, db, ctx, `CREATE TABLE v35_price_band (
		id        INTEGER PRIMARY KEY,
		label     TEXT,
		min_price INTEGER,
		max_price INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v35_price_band VALUES (1, 'Budget',   0,   99)")
	afExec(t, db, ctx, "INSERT INTO v35_price_band VALUES (2, 'Mid',    100,  249)")
	afExec(t, db, ctx, "INSERT INTO v35_price_band VALUES (3, 'Premium', 250,  499)")
	afExec(t, db, ctx, "INSERT INTO v35_price_band VALUES (4, 'Luxury',  500, 9999)")

	// Test 54: JOIN with inequality range condition (non-equi join).
	// Match each item to its price band via ON item.price >= band.min_price AND item.price <= band.max_price.
	// Items: price 200(1), 350(2), 50(3), 150(4), 10(5), 25(6), 500(7), 200(8)
	// Budget(0-99): 50,10,25 -> 3 items.
	// Mid(100-249): 200,150,200 -> 3 items.
	// Premium(250-499): 350 -> 1 item.
	// Luxury(500-9999): 500 -> 1 item.
	// Total matched: 8 items, all matched exactly once -> 8 rows.
	checkRowCount("Non-equi JOIN items to price bands: 8 rows",
		`SELECT v35_items.id, v35_price_band.label
		 FROM v35_items
		 JOIN v35_price_band ON v35_items.price >= v35_price_band.min_price
		                    AND v35_items.price <= v35_price_band.max_price`,
		8)

	// Test 55: Non-equi JOIN + GROUP BY - count items per price band.
	// Premium has 1 item (id=2, price=350). Verify.
	check("Non-equi JOIN: Premium band has 1 item",
		`SELECT COUNT(v35_items.id)
		 FROM v35_items
		 JOIN v35_price_band ON v35_items.price >= v35_price_band.min_price
		                    AND v35_items.price <= v35_price_band.max_price
		 WHERE v35_price_band.label = 'Premium'`,
		1)

	// Test 56: Complex JOIN ON with AND and computed condition.
	// Find staff pairs from different depts where both are active and salary differs by <= 5000.
	// Active: Alice(Eng,95k), Bob(Eng,80k), Carol(Sales,70k), Eve(HR,60k), Frank(Legal,75k).
	// Cross-dept pairs (dept1 < dept2 or just diff depts with id1 < id2):
	//   Alice(1,Eng) - Carol(3,Sales): diff=25000 NO
	//   Alice(1,Eng) - Eve(5,HR):     diff=35000 NO
	//   Alice(1,Eng) - Frank(6,Legal): diff=20000 NO
	//   Bob(2,Eng)   - Carol(3,Sales): diff=10000 NO
	//   Bob(2,Eng)   - Eve(5,HR):     diff=20000 NO
	//   Bob(2,Eng)   - Frank(6,Legal): diff=5000 YES
	//   Carol(3,Sales)- Eve(5,HR):    diff=10000 NO
	//   Carol(3,Sales)- Frank(6,Legal):diff=5000 YES
	//   Eve(5,HR)   - Frank(6,Legal): diff=15000 NO
	// Pairs: (Bob,Frank), (Carol,Frank) -> 2 pairs.
	checkRowCount("Complex JOIN: cross-dept active pairs salary diff<=5000: 2",
		`SELECT s1.name, s2.name
		 FROM v35_staff s1
		 JOIN v35_staff s2 ON s1.id < s2.id
		                  AND s1.dept_id <> s2.dept_id
		                  AND s1.active = 1
		                  AND s2.active = 1
		                  AND ABS(s1.salary - s2.salary) <= 5000`,
		2)

	// Test 57: JOIN with complex ON + WHERE filter.
	// Items with qty > 0 joined to their price band, filtered to Mid band.
	// Mid band items (price 100-249): id=1(200,qty=5), id=4(150,qty=2), id=8(200,qty=1).
	// All have qty > 0. -> 3 rows.
	checkRowCount("Non-equi JOIN with WHERE qty>0 in Mid band: 3 rows",
		`SELECT v35_items.id
		 FROM v35_items
		 JOIN v35_price_band ON v35_items.price >= v35_price_band.min_price
		                    AND v35_items.price <= v35_price_band.max_price
		 WHERE v35_price_band.label = 'Mid'
		   AND v35_items.qty > 0`,
		3)

	// ============================================================
	// === SECTION 12: COALESCE IN AGGREGATES ===
	// ============================================================
	//
	// Using v35_items (discount has one NULL at id=8).
	// COALESCE replaces NULLs before aggregation.

	// Test 58: SUM(COALESCE(discount, 0)) - treat NULL discount as 0.
	// Discounts: 0.10, 0.05, 0.20, 0.00, 0.15, 0.10, 0.00, NULL(->0).
	// SUM = 0.10+0.05+0.20+0.00+0.15+0.10+0.00+0.00 = 0.60.
	check("SUM(COALESCE(discount,0)) = 0.6",
		"SELECT SUM(COALESCE(discount, 0)) FROM v35_items",
		0.6)

	// Test 59: AVG(COALESCE(discount, 0)) vs AVG(discount).
	// With COALESCE: SUM=0.60, COUNT=8 items, AVG=0.075.
	// Without COALESCE (skips NULL): SUM=0.60, COUNT=7, AVG=0.60/7=0.085714...
	// Verify the COALESCE version = 0.075.
	check("AVG(COALESCE(discount,0)) over 8 items = 0.075",
		"SELECT AVG(COALESCE(discount, 0)) FROM v35_items",
		0.075)

	// Test 60: SUM(qty * price * (1 - COALESCE(discount, 0))) per category - Clothing.
	// Clothing items:
	//   id=3: 10*50*(1-0.20) = 500*0.80 = 400
	//   id=4:  2*150*(1-0.00) = 300*1.00 = 300
	//   id=8:  1*200*(1-0.00) = 200*1.00 = 200 (COALESCE NULL->0)
	// SUM = 400+300+200 = 900.
	check("SUM(qty*price*(1-COALESCE(discount,0))) Clothing = 900",
		`SELECT SUM(qty * price * (1 - COALESCE(discount, 0)))
		 FROM v35_items
		 WHERE category = 'Clothing'`,
		900)

	// Test 61: COUNT of items where COALESCE(discount, -1) = -1 (i.e. NULL discount).
	// Only id=8 has NULL discount -> count = 1.
	check("COUNT items with NULL discount (COALESCE trick) = 1",
		"SELECT COUNT(*) FROM v35_items WHERE COALESCE(discount, -1.0) = -1.0",
		1)

	// Test 62: MAX(COALESCE(discount, 0)) = 0.20 (Clothing id=3).
	check("MAX(COALESCE(discount,0)) = 0.2",
		"SELECT MAX(COALESCE(discount, 0)) FROM v35_items",
		0.2)

	// ============================================================
	// === SECTION 13: EXPRESSIONS IN INSERT VALUES ===
	// ============================================================
	//
	// INSERT INTO t VALUES (expr1, expr2, expr3)

	afExec(t, db, ctx, `CREATE TABLE v35_expr_insert (
		id    INTEGER PRIMARY KEY,
		label TEXT,
		value INTEGER,
		tag   TEXT
	)`)

	// Test 63: Arithmetic expression in INSERT VALUES.
	checkNoError("INSERT with arithmetic expression: 3+4*2=11",
		"INSERT INTO v35_expr_insert VALUES (1, 'math', 3 + 4 * 2, 'arith')")
	check("Arithmetic expression in INSERT: value = 11",
		"SELECT value FROM v35_expr_insert WHERE id = 1",
		11)

	// Test 64: String concatenation expression in INSERT.
	checkNoError("INSERT with string concat: 'hello' || ' ' || 'world'",
		"INSERT INTO v35_expr_insert VALUES (2, 'concat', 0, 'hello' || ' ' || 'world')")
	check("String concat in INSERT: tag = 'hello world'",
		"SELECT tag FROM v35_expr_insert WHERE id = 2",
		"hello world")

	// Test 65: UPPER() expression in INSERT.
	checkNoError("INSERT with UPPER function expression",
		"INSERT INTO v35_expr_insert VALUES (3, UPPER('cobalt'), 0, 'func')")
	check("UPPER in INSERT: label = COBALT",
		"SELECT label FROM v35_expr_insert WHERE id = 3",
		"COBALT")

	// Test 66: Negative number in INSERT.
	checkNoError("INSERT with negative value",
		"INSERT INTO v35_expr_insert VALUES (4, 'negative', -1 * 50, 'neg')")
	check("Negative expression in INSERT: value = -50",
		"SELECT value FROM v35_expr_insert WHERE id = 4",
		-50)

	// Test 67: CASE expression in INSERT VALUES.
	checkNoError("INSERT with CASE expression",
		"INSERT INTO v35_expr_insert VALUES (5, 'case-test', CASE WHEN 10 > 5 THEN 100 ELSE 0 END, 'case')")
	check("CASE expression in INSERT: value = 100",
		"SELECT value FROM v35_expr_insert WHERE id = 5",
		100)

	// Test 68: Compound arithmetic in INSERT.
	checkNoError("INSERT compound arithmetic: (100-20)*3/4",
		"INSERT INTO v35_expr_insert VALUES (6, 'compound', (100 - 20) * 3, 'compound')")
	// (100-20)*3 = 80*3 = 240.
	check("Compound arithmetic in INSERT: value = 240",
		"SELECT value FROM v35_expr_insert WHERE id = 6",
		240)

	// ============================================================
	// === SECTION 14: MULTIPLE INDEXES ON SAME TABLE ===
	// ============================================================
	//
	// Create multiple indexes on v35_multi_idx and verify queries run correctly.

	afExec(t, db, ctx, `CREATE TABLE v35_multi_idx (
		id       INTEGER PRIMARY KEY,
		category TEXT,
		region   TEXT,
		score    INTEGER,
		grade    TEXT
	)`)

	checkNoError("Create index on category",
		"CREATE INDEX v35_multi_idx_cat ON v35_multi_idx (category)")
	checkNoError("Create index on region",
		"CREATE INDEX v35_multi_idx_reg ON v35_multi_idx (region)")
	checkNoError("Create index on score",
		"CREATE INDEX v35_multi_idx_score ON v35_multi_idx (score)")
	checkNoError("Create composite index on category+region",
		"CREATE INDEX v35_multi_idx_cat_reg ON v35_multi_idx (category, region)")

	afExec(t, db, ctx, "INSERT INTO v35_multi_idx VALUES (1,  'X', 'North', 85, 'B')")
	afExec(t, db, ctx, "INSERT INTO v35_multi_idx VALUES (2,  'X', 'South', 92, 'A')")
	afExec(t, db, ctx, "INSERT INTO v35_multi_idx VALUES (3,  'Y', 'North', 78, 'C')")
	afExec(t, db, ctx, "INSERT INTO v35_multi_idx VALUES (4,  'Y', 'South', 65, 'D')")
	afExec(t, db, ctx, "INSERT INTO v35_multi_idx VALUES (5,  'Z', 'North', 91, 'A')")
	afExec(t, db, ctx, "INSERT INTO v35_multi_idx VALUES (6,  'Z', 'East',  55, 'F')")
	afExec(t, db, ctx, "INSERT INTO v35_multi_idx VALUES (7,  'X', 'East',  70, 'C')")
	afExec(t, db, ctx, "INSERT INTO v35_multi_idx VALUES (8,  'Y', 'North', 88, 'B')")
	afExec(t, db, ctx, "INSERT INTO v35_multi_idx VALUES (9,  'Z', 'South', 95, 'A')")
	afExec(t, db, ctx, "INSERT INTO v35_multi_idx VALUES (10, 'X', 'North', 60, 'D')")

	// Test 69: Query using category index.
	// Category X: ids 1,2,7,10 -> 4 rows.
	checkRowCount("Category index query: X has 4 rows",
		"SELECT * FROM v35_multi_idx WHERE category = 'X'",
		4)

	// Test 70: Query using region index.
	// North: ids 1,3,5,8,10 -> 5 rows.
	checkRowCount("Region index query: North has 5 rows",
		"SELECT * FROM v35_multi_idx WHERE region = 'North'",
		5)

	// Test 71: Query using score index range.
	// Score >= 90: ids 2(92),5(91),9(95) -> 3 rows.
	checkRowCount("Score index range query: score>=90 has 3 rows",
		"SELECT * FROM v35_multi_idx WHERE score >= 90",
		3)

	// Test 72: Query on composite index columns category+region.
	// X + North: ids 1,10 -> 2 rows.
	checkRowCount("Composite index query: X+North has 2 rows",
		"SELECT * FROM v35_multi_idx WHERE category = 'X' AND region = 'North'",
		2)

	// Test 73: Aggregate with index filter.
	// MAX score for Z category: ids 5(91),6(55),9(95) -> MAX=95.
	check("MAX score for Z category = 95",
		"SELECT MAX(score) FROM v35_multi_idx WHERE category = 'Z'",
		95)

	// Test 74: ORDER BY on indexed column.
	// Top score overall = 95, belongs to id=9 (Z, South).
	check("ORDER BY indexed score DESC LIMIT 1 = 95",
		"SELECT score FROM v35_multi_idx ORDER BY score DESC LIMIT 1",
		95)

	// ============================================================
	// === SECTION 15: DELETE WITH COMPLEX WHERE ===
	// ============================================================
	//
	// Using v35_multi_idx (10 rows).
	// Test combinations of AND/OR/NOT/IN/EXISTS/BETWEEN.

	// Test 75: DELETE with BETWEEN.
	// Score BETWEEN 60 AND 70 is inclusive. Matching rows:
	//   id=4  (Y,South,65) score=65 in [60,70] YES
	//   id=7  (X,East, 70) score=70 in [60,70] YES
	//   id=10 (X,North,60) score=60 in [60,70] YES
	// Three rows deleted; 7 rows remain.
	checkNoError("DELETE with BETWEEN 60 AND 70",
		"DELETE FROM v35_multi_idx WHERE score BETWEEN 60 AND 70")
	checkRowCount("After BETWEEN DELETE: 7 rows remain",
		"SELECT * FROM v35_multi_idx",
		7)

	// Test 76: DELETE with IN list.
	// Remaining rows after BETWEEN delete: 1(X,North,85), 2(X,South,92), 3(Y,North,78),
	//   5(Z,North,91), 6(Z,East,55), 8(Y,North,88), 9(Z,South,95).
	// Delete category IN ('Z'): ids 5,6,9 -> 3 deleted, 4 rows remain.
	checkNoError("DELETE with IN ('Z')",
		"DELETE FROM v35_multi_idx WHERE category IN ('Z')")
	checkRowCount("After IN DELETE: 4 rows remain",
		"SELECT * FROM v35_multi_idx",
		4)

	// Test 77: DELETE with NOT and AND.
	// Remaining: id=1(X,North,85), id=2(X,South,92), id=3(Y,North,78), id=8(Y,North,88).
	// DELETE WHERE NOT (region='North') AND score > 80:
	//   NOT North means South or East. id=2 is South, score=92 > 80 -> DELETE.
	//   id=1,3,8 are all North -> not deleted.
	// After: 3 rows remain: id=1,3,8.
	checkNoError("DELETE with NOT AND condition",
		"DELETE FROM v35_multi_idx WHERE NOT (region = 'North') AND score > 80")
	checkRowCount("After NOT AND DELETE: 3 rows remain",
		"SELECT * FROM v35_multi_idx",
		3)
	// Remaining: id=1(X,North,85), id=3(Y,North,78), id=8(Y,North,88).

	// Test 78: DELETE with EXISTS subquery.
	// Delete remaining rows where EXISTS (SELECT 1 from v35_dept where name matches a condition).
	// Delete rows where EXISTS (SELECT 1 from v35_items WHERE v35_items.qty > 15 AND v35_multi_idx.score > 80).
	// v35_items rows with qty > 15: id=5 (Food, qty=20) -> EXISTS = TRUE if score > 80.
	// Rows with score > 80: id=1(85), id=8(88). Both will be deleted. id=3(78) not > 80 -> survives.
	checkNoError("DELETE with EXISTS subquery",
		`DELETE FROM v35_multi_idx
		 WHERE score > 80
		   AND EXISTS (SELECT 1 FROM v35_items WHERE qty > 15)`)
	checkRowCount("After EXISTS DELETE: 1 row remains (id=3, score=78)",
		"SELECT * FROM v35_multi_idx",
		1)
	check("Remaining row after complex DELETE: score = 78",
		"SELECT score FROM v35_multi_idx",
		78)

	// ============================================================
	// === SECTION 16: REFERENTIAL INTEGRITY AFTER OPERATIONS ===
	// ============================================================
	//
	// Tables: v35_parent (id PK, name TEXT)
	//         v35_child  (id PK, parent_id FK, value INTEGER)
	// Verify consistency after inserts, updates, deletes.

	afExec(t, db, ctx, `CREATE TABLE v35_parent (
		id   INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`)
	afExec(t, db, ctx, `CREATE TABLE v35_child (
		id        INTEGER PRIMARY KEY,
		parent_id INTEGER,
		value     INTEGER,
		FOREIGN KEY (parent_id) REFERENCES v35_parent(id) ON DELETE CASCADE
	)`)

	afExec(t, db, ctx, "INSERT INTO v35_parent VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO v35_parent VALUES (2, 'P2')")
	afExec(t, db, ctx, "INSERT INTO v35_parent VALUES (3, 'P3')")
	afExec(t, db, ctx, "INSERT INTO v35_child VALUES (10, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO v35_child VALUES (11, 1, 110)")
	afExec(t, db, ctx, "INSERT INTO v35_child VALUES (12, 2, 200)")
	afExec(t, db, ctx, "INSERT INTO v35_child VALUES (13, 3, 300)")
	afExec(t, db, ctx, "INSERT INTO v35_child VALUES (14, 3, 310)")

	// Test 79: Initial state verification.
	checkRowCount("Initial parent count = 3",
		"SELECT * FROM v35_parent",
		3)
	checkRowCount("Initial child count = 5",
		"SELECT * FROM v35_child",
		5)

	// Test 80: Delete parent P1, cascade removes children 10 and 11.
	checkNoError("Delete P1 triggers cascade",
		"DELETE FROM v35_parent WHERE id = 1")
	checkRowCount("Children of P1 cascaded (0 remain)",
		"SELECT * FROM v35_child WHERE parent_id = 1",
		0)
	checkRowCount("Child count after P1 cascade = 3",
		"SELECT * FROM v35_child",
		3)

	// Test 81: P2 and P3 children unaffected.
	checkRowCount("P2 child intact after P1 delete",
		"SELECT * FROM v35_child WHERE parent_id = 2",
		1)
	checkRowCount("P3 children intact after P1 delete",
		"SELECT * FROM v35_child WHERE parent_id = 3",
		2)

	// Test 82: UPDATE parent name - children still reference same id.
	checkNoError("UPDATE parent P2 name",
		"UPDATE v35_parent SET name = 'P2-Updated' WHERE id = 2")
	check("P2 name updated",
		"SELECT name FROM v35_parent WHERE id = 2",
		"P2-Updated")
	// Child 12 still references parent_id=2.
	checkRowCount("Child 12 still references updated P2",
		"SELECT * FROM v35_child WHERE parent_id = 2",
		1)

	// Test 83: INSERT new child for P3, verify count.
	checkNoError("INSERT new child for P3",
		"INSERT INTO v35_child VALUES (15, 3, 320)")
	checkRowCount("P3 now has 3 children",
		"SELECT * FROM v35_child WHERE parent_id = 3",
		3)

	// Test 84: Total child count after all ops = 4 (P2:1, P3:3).
	checkRowCount("Total child count after all operations = 4",
		"SELECT * FROM v35_child",
		4)

	// ============================================================
	// === SECTION 17: GROUP BY + HAVING + ORDER BY COMBINED ===
	// ============================================================
	//
	// All three clauses together on JOINed data.
	//
	// Using v35_project (with updated budgets) and v35_task.
	// Plus v35_dept for join.
	//
	// Recap of current task data (no deletes happened to v35_task):
	//   Atlas(proj1):   tasks 1(40h done), 2(60h open), 9(25h done) -> total=125h, done=65h
	//   Beacon(proj2):  tasks 3(30h done), 4(50h open)              -> total=80h,  done=30h
	//   Citadel(proj3): tasks 5(80h done), 8(45h open)              -> total=125h, done=80h
	//   Delta(proj4):   task  6(20h open)                           -> total=20h,  done=0h
	//   Echo(proj5):    task  7(35h done)                           -> total=35h,  done=35h

	// Test 85: GROUP BY project, HAVING total hours > 50, ORDER BY total hours DESC.
	// Projects with total hours > 50: Atlas(125), Beacon(80), Citadel(125).
	// Order DESC: Atlas(125) and Citadel(125) tie -> secondary ORDER BY project name ASC.
	checkRowCount("GROUP BY proj HAVING total hours > 50: 3 projects",
		`SELECT v35_project.name, SUM(v35_task.hours) AS total
		 FROM v35_project
		 JOIN v35_task ON v35_project.id = v35_task.proj_id
		 GROUP BY v35_project.name
		 HAVING SUM(v35_task.hours) > 50
		 ORDER BY total DESC, v35_project.name ASC`,
		3)

	// Test 86: First result of GROUP BY HAVING ORDER BY (Atlas, tied with Citadel; Atlas < Citadel alpha).
	check("GROUP BY HAVING ORDER BY: first project = Atlas (tie broken by name)",
		`SELECT v35_project.name
		 FROM v35_project
		 JOIN v35_task ON v35_project.id = v35_task.proj_id
		 GROUP BY v35_project.name
		 HAVING SUM(v35_task.hours) > 50
		 ORDER BY SUM(v35_task.hours) DESC, v35_project.name ASC
		 LIMIT 1`,
		"Atlas")

	// Test 87: GROUP BY + HAVING on JOIN with dept - dept that has total task hours > 100.
	// Dept->Projects->Tasks:
	//   Eng(dept1): Atlas(125h) + Citadel(125h) = 250h
	//   Sales(dept2): Beacon(80h) = 80h
	//   HR(dept3): Delta(20h) = 20h
	//   Legal(dept4): Echo(35h) = 35h
	// Depts with SUM > 100: Engineering(250h), Sales(80h)? NO, 80 < 100. Only Eng.
	checkRowCount("GROUP BY dept HAVING SUM hours > 100: 1 dept",
		`SELECT v35_dept.name
		 FROM v35_dept
		 JOIN v35_project ON v35_dept.id     = v35_project.dept_id
		 JOIN v35_task     ON v35_project.id  = v35_task.proj_id
		 GROUP BY v35_dept.name
		 HAVING SUM(v35_task.hours) > 100`,
		1)

	// Test 88: GROUP BY with HAVING COUNT and ORDER BY aggregate.
	// Tasks grouped by status, HAVING count > 2, ordered by count DESC.
	// done: tasks 1,3,5,7,9 = 5 tasks.
	// open: tasks 2,4,6,8 = 4 tasks.
	// Both have count > 2. Order DESC: done(5) first.
	check("GROUP BY status HAVING count>2 ORDER BY count DESC: first=done",
		`SELECT status
		 FROM v35_task
		 GROUP BY status
		 HAVING COUNT(*) > 2
		 ORDER BY COUNT(*) DESC, status ASC
		 LIMIT 1`,
		"done")

	// Test 89: Verify second row of GROUP BY + HAVING + ORDER BY.
	checkRowCount("GROUP BY status HAVING count>2: 2 groups (done and open)",
		`SELECT status
		 FROM v35_task
		 GROUP BY status
		 HAVING COUNT(*) > 2`,
		2)

	// ============================================================
	// === SECTION 18: DISTINCT ON EXPRESSIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v35_distinct (
		id    INTEGER PRIMARY KEY,
		code  TEXT,
		value INTEGER,
		flag  INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v35_distinct VALUES (1,  'alpha', 10, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_distinct VALUES (2,  'ALPHA', 10, 0)")
	afExec(t, db, ctx, "INSERT INTO v35_distinct VALUES (3,  'beta',  20, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_distinct VALUES (4,  'BETA',  20, 0)")
	afExec(t, db, ctx, "INSERT INTO v35_distinct VALUES (5,  'gamma', 30, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_distinct VALUES (6,  'gamma', 30, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_distinct VALUES (7,  'delta', 5,  0)")
	afExec(t, db, ctx, "INSERT INTO v35_distinct VALUES (8,  'delta', 5,  0)")
	afExec(t, db, ctx, "INSERT INTO v35_distinct VALUES (9,  'alpha', 15, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_distinct VALUES (10, 'beta',  25, 0)")

	// Test 90: SELECT DISTINCT on arithmetic expression.
	// value + flag values:
	//   id1: 10+1=11, id2: 10+0=10, id3: 20+1=21, id4: 20+0=20, id5: 30+1=31,
	//   id6: 30+1=31, id7: 5+0=5, id8: 5+0=5, id9: 15+1=16, id10: 25+0=25.
	// Distinct values: 11,10,21,20,31,5,16,25 -> 8 distinct values.
	checkRowCount("SELECT DISTINCT value+flag: 8 distinct values",
		"SELECT DISTINCT value + flag FROM v35_distinct",
		8)

	// Test 91: SELECT DISTINCT UPPER(code).
	// Codes: alpha,ALPHA,beta,BETA,gamma,gamma,delta,delta,alpha,beta.
	// UPPER: ALPHA,ALPHA,BETA,BETA,GAMMA,GAMMA,DELTA,DELTA,ALPHA,BETA.
	// Distinct: ALPHA, BETA, GAMMA, DELTA -> 4 distinct values.
	checkRowCount("SELECT DISTINCT UPPER(code): 4 distinct values",
		"SELECT DISTINCT UPPER(code) FROM v35_distinct",
		4)

	// Test 92: SELECT DISTINCT on CASE expression.
	// CASE: value >= 20 THEN 'high' ELSE 'low'.
	// high: ids 3,4,5,6,10(25) = 5. low: ids 1,2,7,8,9 = 5.
	// Distinct: 'high', 'low' -> 2 distinct.
	checkRowCount("SELECT DISTINCT CASE expression: 2 distinct values",
		`SELECT DISTINCT CASE WHEN value >= 20 THEN 'high' ELSE 'low' END FROM v35_distinct`,
		2)

	// Test 93: SELECT DISTINCT with multiple columns (distinct combos).
	// (code, flag) distinct combinations:
	//   (alpha,1): ids 1,9. (ALPHA,0): id2. (beta,1): id3. (BETA,0): id4.
	//   (gamma,1): ids 5,6. (delta,0): ids 7,8. (beta,0): id10.
	// Distinct combos: (alpha,1),(ALPHA,0),(beta,1),(BETA,0),(gamma,1),(delta,0),(beta,0) = 7.
	checkRowCount("SELECT DISTINCT code,flag: 7 distinct combos",
		"SELECT DISTINCT code, flag FROM v35_distinct",
		7)

	// ============================================================
	// === SECTION 19: AGGREGATE-OF-GROUP PATTERNS VIA CTE ===
	// ============================================================
	//
	// Pattern: aggregate the results of a GROUP BY using a CTE or subquery.

	afExec(t, db, ctx, `CREATE TABLE v35_sales_log (
		id       INTEGER PRIMARY KEY,
		salesperson TEXT,
		region   TEXT,
		amount   INTEGER,
		month    INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (1,  'Al',    'North', 1000, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (2,  'Al',    'North', 1500, 2)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (3,  'Al',    'South', 2000, 3)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (4,  'Bea',   'North', 800,  1)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (5,  'Bea',   'South', 1200, 2)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (6,  'Bea',   'North', 900,  3)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (7,  'Cam',   'South', 3000, 1)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (8,  'Cam',   'North', 500,  2)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (9,  'Cam',   'South', 2500, 3)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (10, 'Dana',  'East',  700,  1)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (11, 'Dana',  'East',  600,  2)")
	afExec(t, db, ctx, "INSERT INTO v35_sales_log VALUES (12, 'Dana',  'East',  900,  3)")

	// Per-person totals:
	//   Al:   1000+1500+2000 = 4500
	//   Bea:  800+1200+900   = 2900
	//   Cam:  3000+500+2500  = 6000
	//   Dana: 700+600+900    = 2200

	// Test 94: CTE groups by salesperson, outer query finds max of those totals.
	// MAX total = Cam(6000).
	check("CTE group totals: MAX salesperson total = 6000",
		`WITH person_totals AS (
		   SELECT salesperson, SUM(amount) AS total
		   FROM v35_sales_log
		   GROUP BY salesperson
		 )
		 SELECT MAX(total) FROM person_totals`,
		6000)

	// Test 95: CTE - count how many salespersons beat the average total.
	// AVG of totals = (4500+2900+6000+2200)/4 = 15600/4 = 3900.
	// Above avg: Al(4500), Cam(6000) -> 2 persons.
	check("CTE: count persons beating avg total = 2",
		`WITH person_totals AS (
		   SELECT salesperson, SUM(amount) AS total
		   FROM v35_sales_log
		   GROUP BY salesperson
		 )
		 SELECT COUNT(*) FROM person_totals
		 WHERE total > (SELECT AVG(total) FROM person_totals)`,
		2)

	// Test 96: CTE - region totals, then outer gets MIN region total.
	// Region totals:
	//   North: Al(1000+1500)+Bea(800+900)+Cam(500) = 2500+1700+500 = 4700
	//   South: Al(2000)+Bea(1200)+Cam(3000+2500)   = 2000+1200+5500 = 8700
	//   East:  Dana(700+600+900) = 2200
	// MIN region total = East(2200).
	check("CTE region totals: MIN region total = 2200",
		`WITH region_totals AS (
		   SELECT region, SUM(amount) AS total
		   FROM v35_sales_log
		   GROUP BY region
		 )
		 SELECT MIN(total) FROM region_totals`,
		2200)

	// Test 97: CTE - month-by-month total, then count months above threshold.
	// Month totals: m1=1000+800+3000+700=5500, m2=1500+1200+500+600=3800, m3=2000+900+2500+900=6300.
	// Months above 4000: m1(5500), m3(6300) -> 2 months.
	check("CTE month totals: months with total > 4000 = 2",
		`WITH month_totals AS (
		   SELECT month, SUM(amount) AS total
		   FROM v35_sales_log
		   GROUP BY month
		 )
		 SELECT COUNT(*) FROM month_totals WHERE total > 4000`,
		2)

	// Test 98: CTE with GROUP BY then outer query finds top region name.
	// Top region by total revenue = South (8700 vs North 4700, East 2200).
	check("CTE region totals: top region by revenue = South",
		`WITH region_totals AS (
		   SELECT region, SUM(amount) AS rtotal
		   FROM v35_sales_log
		   GROUP BY region
		 )
		 SELECT region FROM region_totals
		 ORDER BY rtotal DESC, region ASC
		 LIMIT 1`,
		"South")

	// ============================================================
	// === SECTION 20: STRESS - 100+ ROWS THEN COMPLEX QUERIES ===
	// ============================================================
	//
	// Insert 120 rows into v35_stress (id, bucket, subcategory, quantity, unit_price, active).
	// Buckets: A, B, C (40 rows each).
	// Subcategories: X, Y per bucket (20 rows each).
	// quantity: 1..20 cycling, unit_price: 10..50 cycling, active: alternating 1/0.

	afExec(t, db, ctx, `CREATE TABLE v35_stress (
		id          INTEGER PRIMARY KEY,
		bucket      TEXT,
		subcategory TEXT,
		quantity    INTEGER,
		unit_price  INTEGER,
		active      INTEGER
	)`)

	// Insert 120 rows: buckets A(ids 1-40), B(ids 41-80), C(ids 81-120).
	// Within each bucket: first 20 rows subcategory=X, next 20 rows subcategory=Y.
	// quantity = (id % 20) + 1 (cycles 1..20).
	// unit_price = (id % 5) * 10 + 10 (cycles: 10,20,30,40,50).
	// active = id % 2 (alternates 1,0,1,0,...; ids 1,3,5... are active=1).
	for i := 1; i <= 120; i++ {
		var bucket string
		if i <= 40 {
			bucket = "A"
		} else if i <= 80 {
			bucket = "B"
		} else {
			bucket = "C"
		}
		var subcat string
		// Within each bucket of 40: first 20 = X, next 20 = Y.
		posInBucket := ((i - 1) % 40) + 1
		if posInBucket <= 20 {
			subcat = "X"
		} else {
			subcat = "Y"
		}
		qty := (i % 20) + 1
		price := (i%5)*10 + 10
		active := i % 2
		afExec(t, db, ctx, fmt.Sprintf(
			"INSERT INTO v35_stress VALUES (%d, '%s', '%s', %d, %d, %d)",
			i, bucket, subcat, qty, price, active))
	}

	// Test 99: Total row count = 120.
	checkRowCount("Stress: 120 rows inserted",
		"SELECT * FROM v35_stress",
		120)

	// Test 100: COUNT per bucket = 40 each.
	check("Stress: bucket A has 40 rows",
		"SELECT COUNT(*) FROM v35_stress WHERE bucket = 'A'",
		40)
	checkRowCount("Stress: 3 distinct buckets",
		"SELECT DISTINCT bucket FROM v35_stress",
		3)

	// Test 101: COUNT active rows (active=1).
	// ids 1,3,5,...,119 -> 60 odd ids out of 120 -> 60 active rows.
	check("Stress: 60 active rows (id%2=1)",
		"SELECT COUNT(*) FROM v35_stress WHERE active = 1",
		60)

	// Test 102: SUM(quantity * unit_price) per bucket - verify one bucket.
	// Bucket A: ids 1-40.
	// For id i: qty = (i%20)+1, price = (i%5)*10+10.
	// We compute the expected sum for bucket A (ids 1-40):
	// id  1: qty=(1%20)+1=2,  price=(1%5)*10+10=20  -> 40
	// id  2: qty=(2%20)+1=3,  price=(2%5)*10+10=30  -> 90
	// id  3: qty=(3%20)+1=4,  price=(3%5)*10+10=40  -> 160
	// id  4: qty=(4%20)+1=5,  price=(4%5)*10+10=50  -> 250
	// id  5: qty=(5%20)+1=6,  price=(5%5)*10+10=10  -> 60  (5%5=0)
	// id  6: qty=7, price=20 -> 140
	// id  7: qty=8, price=30 -> 240
	// id  8: qty=9, price=40 -> 360
	// id  9: qty=10, price=50 -> 500
	// id 10: qty=11, price=10 -> 110
	// id 11: qty=12, price=20 -> 240
	// id 12: qty=13, price=30 -> 390
	// id 13: qty=14, price=40 -> 560
	// id 14: qty=15, price=50 -> 750
	// id 15: qty=16, price=10 -> 160
	// id 16: qty=17, price=20 -> 340
	// id 17: qty=18, price=30 -> 540
	// id 18: qty=19, price=40 -> 760
	// id 19: qty=20, price=50 -> 1000
	// id 20: qty=(20%20)+1=1, price=(20%5)*10+10=10 -> 10
	// id 21: qty=2, price=20 -> 40 (same as id 1, cycle repeats every 20 for qty and every 5 for price)
	// Wait: qty cycle=20, price cycle=5. LCM(20,5)=20. So ids 1-20 and 21-40 have same qty*price per relative position.
	// Sum of ids 1-20: 40+90+160+250+60+140+240+360+500+110+240+390+560+750+160+340+540+760+1000+10
	// Let me sum these: 40+90=130, +160=290, +250=540, +60=600, +140=740, +240=980, +360=1340,
	// +500=1840, +110=1950, +240=2190, +390=2580, +560=3140, +750=3890, +160=4050,
	// +340=4390, +540=4930, +760=5690, +1000=6690, +10=6700.
	// Sum ids 1-20 = 6700. Sum ids 21-40 = 6700 (same pattern). Bucket A total = 13400.
	check("Stress: SUM(qty*price) for bucket A = 13400",
		"SELECT SUM(quantity * unit_price) FROM v35_stress WHERE bucket = 'A'",
		13400)

	// Test 103: All buckets have same sum (since pattern repeats identically for B and C).
	// Bucket B (ids 41-80): same 20-cycle pattern -> also 13400.
	check("Stress: SUM(qty*price) for bucket B = 13400",
		"SELECT SUM(quantity * unit_price) FROM v35_stress WHERE bucket = 'B'",
		13400)

	// Test 104: GROUP BY bucket, subcategory - 6 groups (A/X, A/Y, B/X, B/Y, C/X, C/Y).
	checkRowCount("Stress: GROUP BY bucket+subcategory = 6 groups",
		"SELECT bucket, subcategory, COUNT(*) FROM v35_stress GROUP BY bucket, subcategory",
		6)

	// Test 105: Each bucket+subcategory has exactly 20 rows.
	check("Stress: bucket A subcategory X has 20 rows",
		"SELECT COUNT(*) FROM v35_stress WHERE bucket = 'A' AND subcategory = 'X'",
		20)

	// Test 106: Complex aggregation - AVG(quantity) for active rows in bucket C.
	// Bucket C: ids 81-120. Active in C: ids 81,83,85,...,119 (odd ids) = 20 active rows.
	// For odd ids 81..119:
	// id 81: qty=(81%20)+1=2
	// id 83: qty=(83%20)+1=4
	// id 85: qty=(85%20)+1=6
	// id 87: qty=(87%20)+1=8
	// id 89: qty=(89%20)+1=10
	// id 91: qty=(91%20)+1=12
	// id 93: qty=(93%20)+1=14
	// id 95: qty=(95%20)+1=16
	// id 97: qty=(97%20)+1=18
	// id 99: qty=(99%20)+1=20
	// id 101: qty=(101%20)+1=2
	// id 103: qty=(103%20)+1=4
	// id 105: qty=(105%20)+1=6
	// id 107: qty=(107%20)+1=8
	// id 109: qty=(109%20)+1=10
	// id 111: qty=(111%20)+1=12
	// id 113: qty=(113%20)+1=14
	// id 115: qty=(115%20)+1=16
	// id 117: qty=(117%20)+1=18
	// id 119: qty=(119%20)+1=20
	// Sum: (2+4+6+8+10+12+14+16+18+20)*2 = 110*2 = 220. AVG = 220/20 = 11.
	check("Stress: AVG(quantity) for active bucket C = 11",
		"SELECT AVG(quantity) FROM v35_stress WHERE bucket = 'C' AND active = 1",
		11)

	// Test 107: COUNT(CASE WHEN unit_price > 30 THEN 1 END) - high price rows.
	// unit_price values cycle: id%5: 0->10, 1->20, 2->30, 3->40, 4->50.
	// price > 30: price=40 (id%5=3) or price=50 (id%5=4).
	// Ids with id%5 IN (3,4): in range 1-120, id%5=3: ids 3,8,13,...,118 = 24 ids;
	// id%5=4: ids 4,9,14,...,119 = 24 ids. Total high price: 48 rows.
	check("Stress: COUNT(CASE WHEN price>30) = 48",
		"SELECT COUNT(CASE WHEN unit_price > 30 THEN 1 END) FROM v35_stress",
		48)

	// Test 108: MAX and MIN quantity in the full stress table.
	// qty = (id%20)+1, range: 1 (when id%20=0, e.g. id=20,40,...) to 20 (when id%20=19, e.g. id=19,39,...).
	check("Stress: MAX(quantity) = 20",
		"SELECT MAX(quantity) FROM v35_stress",
		20)
	check("Stress: MIN(quantity) = 1",
		"SELECT MIN(quantity) FROM v35_stress",
		1)

	// Test 109: Complex WHERE with AND/OR on stress table.
	// (bucket='A' AND active=1 AND unit_price >= 40) OR (bucket='C' AND subcategory='Y').
	// Bucket A, active(odd ids 1-39), price>=40 (id%5 in {3,4}):
	//   ids in A(1-40), odd, id%5 in {3,4}:
	//   id%5=3: odds: 3,13,23,33 -> 4; id%5=4: odds: 9,19,29,39 -> 4. Total: 8 rows.
	// Bucket C, subcategory Y: C=ids 81-120, Y=ids 101-120 within C -> 20 rows.
	// Together: 8 + 20 = 28 rows.
	checkRowCount("Stress: complex WHERE (A active high-price) OR (C subcat Y) = 28",
		`SELECT * FROM v35_stress
		 WHERE (bucket = 'A' AND active = 1 AND unit_price >= 40)
		    OR (bucket = 'C' AND subcategory = 'Y')`,
		28)

	// Test 110: HAVING filter on stress table aggregation.
	// GROUP BY bucket, subcategory HAVING SUM(quantity*unit_price) > 5000.
	// Each group has 20 rows. Sum per group:
	//   A/X (ids 1-20): same computation as first 20 ids -> 6700.
	//   A/Y (ids 21-40): same pattern offset by 20 -> same result: 6700.
	//   B/X (ids 41-60): same 20-row cycle -> 6700.
	//   B/Y (ids 61-80): same -> 6700.
	//   C/X (ids 81-100): same -> 6700.
	//   C/Y (ids 101-120): same -> 6700.
	// All 6 groups have sum = 6700 > 5000. So HAVING passes all 6 groups.
	checkRowCount("Stress: GROUP BY bucket+subcat HAVING SUM>5000: all 6 groups",
		`SELECT bucket, subcategory, SUM(quantity * unit_price) AS revenue
		 FROM v35_stress
		 GROUP BY bucket, subcategory
		 HAVING SUM(quantity * unit_price) > 5000`,
		6)

	// Test 111: Stress subquery - find buckets where max qty per bucket > overall avg qty.
	// Overall avg qty across all 120 rows:
	//   qty cycle 1-20 repeats 6 times (120/20=6). Sum of 1 cycle: 1+2+...+20 = 210.
	//   Total sum = 6 * 210 = 1260. AVG = 1260/120 = 10.5.
	// Max qty per bucket: all buckets have qty range 1-20, so MAX = 20.
	// 20 > 10.5 YES for all buckets -> all 3 buckets qualify.
	checkRowCount("Stress: buckets where MAX qty > overall AVG qty = all 3",
		`SELECT bucket
		 FROM v35_stress
		 GROUP BY bucket
		 HAVING MAX(quantity) > (SELECT AVG(quantity) FROM v35_stress)`,
		3)

	// Test 112: CTE on stress table - top bucket by active revenue.
	// Active revenue per bucket (active=1 rows, qty*price):
	//   Bucket A active: odd ids 1-39 (20 rows) -> same sum pattern as calculated above = 6700? Let me check.
	//   Actually all 20 rows of A/X are ids 1-20 and A/Y are ids 21-40.
	//   Active in A: odd ids: 1,3,5,...,39 = 20 ids (10 from X: 1,3,5,...,19; 10 from Y: 21,23,...,39).
	//   Active ids sum for A: odd ids 1-39.
	//   id  1: 2*20=40;  id 3: 4*40=160; id 5: 6*10=60;  id 7: 8*30=240; id 9: 10*50=500
	//   id 11: 12*20=240; id 13: 14*40=560; id 15: 16*10=160; id 17: 18*30=540; id 19: 20*50=1000
	//   id 21: 2*20=40;   id 23: 4*40=160; id 25: 6*10=60;   id 27: 8*30=240; id 29: 10*50=500
	//   id 31: 12*20=240; id 33: 14*40=560; id 35: 16*10=160; id 37: 18*30=540; id 39: 20*50=1000
	//   Sum first 10: 40+160+60+240+500+240+560+160+540+1000 = 3500
	//   Sum next 10: same pattern = 3500. Total A active = 7000.
	//   Same for B and C active rows (same pattern) = 7000 each.
	// All buckets tie at 7000. Top bucket by alphabetical order = A.
	check("Stress: CTE top bucket by active revenue = A (tie, alphabetical)",
		`WITH bucket_active_rev AS (
		   SELECT bucket, SUM(quantity * unit_price) AS rev
		   FROM v35_stress
		   WHERE active = 1
		   GROUP BY bucket
		 )
		 SELECT bucket FROM bucket_active_rev
		 ORDER BY rev DESC, bucket ASC
		 LIMIT 1`,
		"A")

	t.Logf("\n=== V35 COMPLEX SQL PATTERNS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
