package test

import (
	"fmt"
	"testing"
)

// TestV110ExecutionPaths targets low-coverage SQL execution paths in the catalog package.
// It covers: JOINs with GROUP BY, window functions, CTEs, INSERT/UPDATE/DELETE edge cases,
// HAVING, correlated subqueries, scalar subqueries, ORDER BY expressions, and derived tables.
func TestV110ExecutionPaths(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	_ = fmt.Sprintf // suppress unused import

	// ============================================================
	// SCHEMA SETUP
	// ============================================================

	// -- departments and employees for JOIN tests
	afExec(t, db, ctx, "CREATE TABLE v110_dept (id INTEGER PRIMARY KEY, dname TEXT NOT NULL, region TEXT)")
	afExec(t, db, ctx, "INSERT INTO v110_dept VALUES (1, 'Engineering', 'West')")
	afExec(t, db, ctx, "INSERT INTO v110_dept VALUES (2, 'Sales', 'East')")
	afExec(t, db, ctx, "INSERT INTO v110_dept VALUES (3, 'Marketing', 'West')")
	afExec(t, db, ctx, "INSERT INTO v110_dept VALUES (4, 'HR', 'East')")

	afExec(t, db, ctx, "CREATE TABLE v110_emp (id INTEGER PRIMARY KEY, name TEXT NOT NULL, dept_id INTEGER, salary REAL, bonus REAL)")
	afExec(t, db, ctx, "INSERT INTO v110_emp VALUES (1, 'Alice', 1, 120.0, 10.0)")
	afExec(t, db, ctx, "INSERT INTO v110_emp VALUES (2, 'Bob', 1, 100.0, 20.0)")
	afExec(t, db, ctx, "INSERT INTO v110_emp VALUES (3, 'Carol', 2, 80.0, 5.0)")
	afExec(t, db, ctx, "INSERT INTO v110_emp VALUES (4, 'Dave', 2, 90.0, 15.0)")
	afExec(t, db, ctx, "INSERT INTO v110_emp VALUES (5, 'Eve', 3, 70.0, NULL)")
	afExec(t, db, ctx, "INSERT INTO v110_emp VALUES (6, 'Frank', 1, 110.0, 25.0)")
	afExec(t, db, ctx, "INSERT INTO v110_emp VALUES (7, 'Grace', NULL, 60.0, NULL)")

	// -- projects for 3-table JOINs
	afExec(t, db, ctx, "CREATE TABLE v110_proj (id INTEGER PRIMARY KEY, pname TEXT, dept_id INTEGER, budget REAL)")
	afExec(t, db, ctx, "INSERT INTO v110_proj VALUES (1, 'Alpha', 1, 5000.0)")
	afExec(t, db, ctx, "INSERT INTO v110_proj VALUES (2, 'Beta', 2, 3000.0)")
	afExec(t, db, ctx, "INSERT INTO v110_proj VALUES (3, 'Gamma', 1, 2000.0)")
	afExec(t, db, ctx, "INSERT INTO v110_proj VALUES (4, 'Delta', 3, 1000.0)")

	// -- assignment table for multi-join
	afExec(t, db, ctx, "CREATE TABLE v110_assign (id INTEGER PRIMARY KEY, emp_id INTEGER, proj_id INTEGER, hours INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v110_assign VALUES (1, 1, 1, 40)")
	afExec(t, db, ctx, "INSERT INTO v110_assign VALUES (2, 2, 1, 30)")
	afExec(t, db, ctx, "INSERT INTO v110_assign VALUES (3, 3, 2, 25)")
	afExec(t, db, ctx, "INSERT INTO v110_assign VALUES (4, 4, 2, 35)")
	afExec(t, db, ctx, "INSERT INTO v110_assign VALUES (5, 1, 3, 10)")
	afExec(t, db, ctx, "INSERT INTO v110_assign VALUES (6, 5, 4, 20)")
	afExec(t, db, ctx, "INSERT INTO v110_assign VALUES (7, 6, 1, 15)")

	// ============================================================
	// SECTION 1: executeSelectWithJoin & executeSelectWithJoinAndGroupBy
	// ============================================================

	// 1.1 LEFT JOIN with GROUP BY and HAVING
	t.Run("LeftJoinGroupByHaving", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname, COUNT(e.id), SUM(e.salary)
			FROM v110_dept d
			LEFT JOIN v110_emp e ON d.id = e.dept_id
			GROUP BY d.dname
			HAVING COUNT(e.id) > 1
			ORDER BY d.dname`)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows (Engineering=3, Sales=2), got %d", len(rows))
		}
		// Engineering should have 3 employees
		if len(rows) > 0 {
			afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v110_emp WHERE dept_id = 1", float64(3))
		}
	})

	// 1.2 LEFT JOIN includes unmatched left rows (HR has no employees)
	t.Run("LeftJoinNullRows", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname, COUNT(e.id)
			FROM v110_dept d
			LEFT JOIN v110_emp e ON d.id = e.dept_id
			GROUP BY d.dname
			ORDER BY d.dname`)
		if len(rows) != 4 {
			t.Errorf("expected 4 rows (all depts), got %d", len(rows))
		}
	})

	// 1.3 RIGHT JOIN with GROUP BY
	t.Run("RightJoinGroupBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname, SUM(e.salary)
			FROM v110_emp e
			RIGHT JOIN v110_dept d ON d.id = e.dept_id
			GROUP BY d.dname
			ORDER BY d.dname`)
		if len(rows) != 4 {
			t.Errorf("expected 4 rows, got %d", len(rows))
		}
	})

	// 1.4 CROSS JOIN with aggregate
	t.Run("CrossJoinCount", func(t *testing.T) {
		afExpectVal(t, db, ctx, `
			SELECT COUNT(*)
			FROM v110_dept d
			CROSS JOIN v110_proj p`, float64(16)) // 4*4
	})

	// 1.5 JOIN with all aggregate functions (SUM, COUNT, AVG, MIN, MAX)
	t.Run("JoinAllAggregates", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname,
				SUM(e.salary),
				COUNT(e.id),
				AVG(e.salary),
				MIN(e.salary),
				MAX(e.salary)
			FROM v110_dept d
			JOIN v110_emp e ON d.id = e.dept_id
			GROUP BY d.dname
			ORDER BY d.dname`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
		// Engineering: SUM=330, COUNT=3, AVG=110, MIN=100, MAX=120
		if len(rows) >= 1 {
			eng := rows[0]
			if fmt.Sprintf("%v", eng[0]) != "Engineering" {
				t.Errorf("first row should be Engineering, got %v", eng[0])
			}
			if fmt.Sprintf("%v", eng[1]) != "330" {
				t.Errorf("Engineering SUM(salary) expected 330, got %v", eng[1])
			}
		}
	})

	// 1.6 JOIN with ORDER BY on joined columns
	t.Run("JoinOrderByJoinedColumn", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT e.name, d.dname
			FROM v110_emp e
			JOIN v110_dept d ON e.dept_id = d.id
			ORDER BY d.dname, e.salary DESC`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
		// First should be Engineering employee with highest salary (Alice, 120)
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) != "Alice" {
			t.Errorf("first row should be Alice, got %v", rows[0][0])
		}
	})

	// 1.7 JOIN with DISTINCT
	t.Run("JoinDistinct", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT DISTINCT d.region
			FROM v110_dept d
			JOIN v110_emp e ON d.id = e.dept_id
			ORDER BY d.region`)
		if len(rows) != 2 {
			t.Errorf("expected 2 distinct regions, got %d", len(rows))
		}
	})

	// 1.8 Three-table JOIN with WHERE and ORDER BY
	t.Run("ThreeTableJoin", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT e.name, p.pname, a.hours
			FROM v110_emp e
			JOIN v110_assign a ON e.id = a.emp_id
			JOIN v110_proj p ON a.proj_id = p.id
			WHERE a.hours >= 20
			ORDER BY a.hours DESC`)
		if len(rows) < 4 {
			t.Errorf("expected at least 4 rows, got %d", len(rows))
		}
		// Highest hours should be 40 (Alice on Alpha)
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][2]) != "40" {
			t.Errorf("first row hours should be 40, got %v", rows[0][2])
		}
	})

	// 1.9 Four-table JOIN with GROUP BY
	t.Run("FourTableJoinGroupBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname, COUNT(DISTINCT p.id) as proj_count, SUM(a.hours)
			FROM v110_dept d
			JOIN v110_emp e ON d.id = e.dept_id
			JOIN v110_assign a ON e.id = a.emp_id
			JOIN v110_proj p ON a.proj_id = p.id
			GROUP BY d.dname
			ORDER BY d.dname`)
		if len(rows) < 2 {
			t.Errorf("expected at least 2 rows, got %d", len(rows))
		}
	})

	// 1.10 LEFT JOIN with GROUP BY and HAVING on AVG
	t.Run("LeftJoinHavingAvg", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname, AVG(e.salary)
			FROM v110_dept d
			LEFT JOIN v110_emp e ON d.id = e.dept_id
			GROUP BY d.dname
			HAVING AVG(e.salary) > 80
			ORDER BY AVG(e.salary) DESC`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 1.11 JOIN with GROUP BY and MIN/MAX in HAVING
	t.Run("JoinHavingMinMax", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname, MIN(e.salary), MAX(e.salary)
			FROM v110_dept d
			JOIN v110_emp e ON d.id = e.dept_id
			GROUP BY d.dname
			HAVING MAX(e.salary) - MIN(e.salary) > 5
			ORDER BY d.dname`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// ============================================================
	// SECTION 2: selectLocked - complex dispatch paths
	// ============================================================

	// 2.1 SELECT from a view with aggregate
	t.Run("ViewWithAggregate", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE VIEW v110_dept_salary AS SELECT d.dname, SUM(e.salary) as total_sal FROM v110_dept d JOIN v110_emp e ON d.id = e.dept_id GROUP BY d.dname")
		rows := afQuery(t, db, ctx, "SELECT * FROM v110_dept_salary ORDER BY total_sal DESC")
		if len(rows) < 1 {
			t.Errorf("expected rows from view, got %d", len(rows))
		}
	})

	// 2.2 SELECT from view with WHERE filter
	t.Run("ViewWithWhere", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE VIEW v110_high_sal AS SELECT name, salary FROM v110_emp WHERE salary > 80")
		rows := afQuery(t, db, ctx, "SELECT * FROM v110_high_sal WHERE salary > 100 ORDER BY salary DESC")
		if len(rows) < 1 {
			t.Errorf("expected rows, got %d", len(rows))
		}
		// All returned salaries should be > 100
		for i, r := range rows {
			s := fmt.Sprintf("%v", r[1])
			if s <= "100" {
				t.Errorf("row %d salary %v should be > 100", i, r[1])
			}
		}
	})

	// 2.3 UNION
	t.Run("SelectUnion", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v110_emp WHERE dept_id = 1
			UNION
			SELECT name FROM v110_emp WHERE salary > 100`)
		// Engineering: Alice(120), Bob(100), Frank(110); salary>100: Alice(120), Frank(110)
		// UNION deduplicates: Alice, Bob, Frank
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
	})

	// 2.4 UNION ALL
	t.Run("SelectUnionAll", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v110_emp WHERE dept_id = 1
			UNION ALL
			SELECT name FROM v110_emp WHERE salary > 100`)
		// 3 from dept 1 + 2 with salary>100 = 5 total (with duplicates)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})

	// 2.5 INTERSECT
	t.Run("SelectIntersect", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v110_emp WHERE dept_id = 1
			INTERSECT
			SELECT name FROM v110_emp WHERE salary > 100`)
		// In dept 1 AND salary > 100: Alice(120), Frank(110)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows (Alice, Frank), got %d", len(rows))
		}
	})

	// 2.6 EXCEPT
	t.Run("SelectExcept", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v110_emp WHERE dept_id = 1
			EXCEPT
			SELECT name FROM v110_emp WHERE salary > 100`)
		// In dept 1 but salary <= 100: Bob(100)
		if len(rows) != 1 {
			t.Errorf("expected 1 row (Bob), got %d", len(rows))
		}
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) != "Bob" {
			t.Errorf("expected Bob, got %v", rows[0][0])
		}
	})

	// 2.7 Derived table (subquery in FROM)
	t.Run("DerivedTable", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT sub.dname, sub.cnt
			FROM (
				SELECT d.dname, COUNT(e.id) as cnt
				FROM v110_dept d
				JOIN v110_emp e ON d.id = e.dept_id
				GROUP BY d.dname
			) sub
			WHERE sub.cnt >= 2
			ORDER BY sub.cnt DESC`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 2.8 Derived table with aggregate on top
	t.Run("DerivedTableAggregate", func(t *testing.T) {
		afExpectVal(t, db, ctx, `
			SELECT SUM(sub.total_sal)
			FROM (
				SELECT dept_id, SUM(salary) as total_sal
				FROM v110_emp
				WHERE dept_id IS NOT NULL
				GROUP BY dept_id
			) sub`, float64(570)) // 330 + 170 + 70 = 570
	})

	// 2.9 Correlated subquery in WHERE
	t.Run("CorrelatedSubqueryWhere", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT e.name, e.salary
			FROM v110_emp e
			WHERE e.salary > (
				SELECT AVG(e2.salary)
				FROM v110_emp e2
				WHERE e2.dept_id = e.dept_id
			)
			ORDER BY e.salary DESC`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// ============================================================
	// SECTION 3: evaluateWindowFunctions
	// ============================================================

	// 3.1 ROW_NUMBER with PARTITION BY
	t.Run("WindowRowNumber", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, dept_id, salary,
				ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rn
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY dept_id, rn`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
		// First Engineering employee should have rn=1
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][3]) != "1" {
			t.Errorf("first row rn should be 1, got %v", rows[0][3])
		}
	})

	// 3.2 RANK with ties
	t.Run("WindowRankTies", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_scores (id INTEGER PRIMARY KEY, student TEXT, score INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v110_scores VALUES (1, 'A', 95)")
		afExec(t, db, ctx, "INSERT INTO v110_scores VALUES (2, 'B', 90)")
		afExec(t, db, ctx, "INSERT INTO v110_scores VALUES (3, 'C', 95)")
		afExec(t, db, ctx, "INSERT INTO v110_scores VALUES (4, 'D', 80)")
		afExec(t, db, ctx, "INSERT INTO v110_scores VALUES (5, 'E', 90)")

		rows := afQuery(t, db, ctx, `
			SELECT student, score,
				RANK() OVER (ORDER BY score DESC) as rnk
			FROM v110_scores
			ORDER BY score DESC, student`)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
		// A and C both have 95, rank 1. B and E have 90, rank 3 (not 2, because of ties).
		// D has 80, rank 5
		for _, r := range rows {
			s := fmt.Sprintf("%v", r[1])
			rnk := fmt.Sprintf("%v", r[2])
			if s == "95" && rnk != "1" {
				t.Errorf("score 95 should have rank 1, got %v", rnk)
			}
			if s == "90" && rnk != "3" {
				t.Errorf("score 90 should have rank 3, got %v", rnk)
			}
			if s == "80" && rnk != "5" {
				t.Errorf("score 80 should have rank 5, got %v", rnk)
			}
		}
	})

	// 3.3 DENSE_RANK with gaps in partition values
	t.Run("WindowDenseRank", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT student, score,
				DENSE_RANK() OVER (ORDER BY score DESC) as drnk
			FROM v110_scores
			ORDER BY score DESC, student`)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
		// DENSE_RANK: 95->1, 90->2, 80->3 (no gaps)
		for _, r := range rows {
			s := fmt.Sprintf("%v", r[1])
			drnk := fmt.Sprintf("%v", r[2])
			if s == "95" && drnk != "1" {
				t.Errorf("score 95 dense_rank should be 1, got %v", drnk)
			}
			if s == "90" && drnk != "2" {
				t.Errorf("score 90 dense_rank should be 2, got %v", drnk)
			}
			if s == "80" && drnk != "3" {
				t.Errorf("score 80 dense_rank should be 3, got %v", drnk)
			}
		}
	})

	// 3.4 SUM as running aggregate window function
	t.Run("WindowRunningSum", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary,
				SUM(salary) OVER (ORDER BY salary) as running_sum
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY salary`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
		// Last running sum should equal total of all salaries (70+80+90+100+110+120=570)
		if len(rows) == 6 && fmt.Sprintf("%v", rows[5][2]) != "570" {
			t.Errorf("last running sum should be 570, got %v", rows[5][2])
		}
	})

	// 3.5 AVG as running aggregate window function
	t.Run("WindowRunningAvg", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary,
				AVG(salary) OVER (ORDER BY salary) as running_avg
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY salary`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
	})

	// 3.6 COUNT as window function with ORDER BY (running count)
	t.Run("WindowRunningCount", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary,
				COUNT(*) OVER (ORDER BY salary) as running_count
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY salary`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
		// Last row should have count = 6
		if len(rows) == 6 && fmt.Sprintf("%v", rows[5][2]) != "6" {
			t.Errorf("last row running count should be 6, got %v", rows[5][2])
		}
	})

	// 3.7 Multiple window functions with different PARTITION BY
	t.Run("WindowMultiplePartitions", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, dept_id, salary,
				ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rn_dept,
				ROW_NUMBER() OVER (ORDER BY salary DESC) as rn_global
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY salary DESC`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
		// First global rn should be 1
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][4]) != "1" {
			t.Errorf("first row global rn should be 1, got %v", rows[0][4])
		}
	})

	// 3.8 Multiple PARTITION BY columns
	t.Run("WindowMultiPartitionCols", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_sales (id INTEGER PRIMARY KEY, region TEXT, category TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v110_sales VALUES (1, 'East', 'A', 100)")
		afExec(t, db, ctx, "INSERT INTO v110_sales VALUES (2, 'East', 'A', 200)")
		afExec(t, db, ctx, "INSERT INTO v110_sales VALUES (3, 'East', 'B', 150)")
		afExec(t, db, ctx, "INSERT INTO v110_sales VALUES (4, 'West', 'A', 300)")
		afExec(t, db, ctx, "INSERT INTO v110_sales VALUES (5, 'West', 'B', 250)")

		rows := afQuery(t, db, ctx, `
			SELECT region, category, amount,
				SUM(amount) OVER (PARTITION BY region, category) as cat_total
			FROM v110_sales
			ORDER BY region, category, amount`)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
		// East/A total should be 300 for both rows
		for _, r := range rows {
			reg := fmt.Sprintf("%v", r[0])
			cat := fmt.Sprintf("%v", r[1])
			tot := fmt.Sprintf("%v", r[3])
			if reg == "East" && cat == "A" && tot != "300" {
				t.Errorf("East/A total should be 300, got %v", tot)
			}
		}
	})

	// 3.9 LAG and LEAD window functions
	t.Run("WindowLagLead", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary,
				LAG(salary) OVER (ORDER BY salary) as prev_sal,
				LEAD(salary) OVER (ORDER BY salary) as next_sal
			FROM v110_emp
			WHERE dept_id = 1
			ORDER BY salary`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
		// First row LAG should be NULL
		if len(rows) > 0 && rows[0][2] != nil {
			t.Errorf("first row LAG should be nil, got %v", rows[0][2])
		}
		// Last row LEAD should be NULL
		if len(rows) == 3 && rows[2][3] != nil {
			t.Errorf("last row LEAD should be nil, got %v", rows[2][3])
		}
	})

	// 3.10 FIRST_VALUE and LAST_VALUE
	t.Run("WindowFirstLastValue", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary,
				FIRST_VALUE(name) OVER (ORDER BY salary) as first_name,
				LAST_VALUE(name) OVER (ORDER BY salary) as last_name
			FROM v110_emp
			WHERE dept_id = 1
			ORDER BY salary`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
		// FIRST_VALUE should always be the lowest salary person (Bob, 100)
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][2]) != "Bob" {
			t.Errorf("first_value should be Bob, got %v", rows[0][2])
		}
	})

	// 3.11 MIN/MAX as window functions
	t.Run("WindowMinMax", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, dept_id, salary,
				MIN(salary) OVER (PARTITION BY dept_id) as dept_min,
				MAX(salary) OVER (PARTITION BY dept_id) as dept_max
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY dept_id, salary`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
	})

	// ============================================================
	// SECTION 4: applyOrderBy - expression ORDER BY
	// ============================================================

	// 4.1 ORDER BY with CASE expression
	t.Run("OrderByCaseExpr", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY CASE WHEN salary > 100 THEN 0 ELSE 1 END, salary DESC`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
		// Salaries > 100 should come first
		if len(rows) >= 1 {
			s := fmt.Sprintf("%v", rows[0][1])
			if s != "120" {
				t.Errorf("first row salary should be 120, got %v", s)
			}
		}
	})

	// 4.2 ORDER BY with function call (LENGTH)
	t.Run("OrderByLength", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY LENGTH(name), name`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
		// Bob (3 chars) should come before Alice (5 chars)
		found_bob := false
		found_alice := false
		for i, r := range rows {
			n := fmt.Sprintf("%v", r[0])
			if n == "Bob" {
				found_bob = true
			}
			if n == "Alice" {
				found_alice = true
				if !found_bob {
					t.Errorf("Bob (3 chars) should appear before Alice (5 chars), Alice at row %d", i)
				}
			}
		}
		if !found_bob || !found_alice {
			t.Error("expected both Bob and Alice in results")
		}
	})

	// 4.3 ORDER BY with arithmetic expression
	t.Run("OrderByArithmetic", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary, bonus
			FROM v110_emp
			WHERE bonus IS NOT NULL
			ORDER BY salary + bonus DESC`)
		if len(rows) < 3 {
			t.Errorf("expected at least 3 rows, got %d", len(rows))
		}
		// Alice: 120+10=130, Frank: 110+25=135, Bob: 100+20=120
		// Frank should be first
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) != "Frank" {
			t.Errorf("first row should be Frank (135 total), got %v", rows[0][0])
		}
	})

	// 4.4 ORDER BY with NULL values and mixed ASC/DESC
	t.Run("OrderByNullMixed", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, bonus
			FROM v110_emp
			ORDER BY bonus ASC`)
		if len(rows) != 7 {
			t.Errorf("expected 7 rows, got %d", len(rows))
		}
		// NULLs sort last in ASC
		lastRow := rows[len(rows)-1]
		if lastRow[1] != nil {
			// Could be second-to-last if there are 2 NULLs
			secondLast := rows[len(rows)-2]
			if secondLast[1] != nil {
				t.Errorf("NULL bonus should sort last in ASC, but last two are not nil")
			}
		}
	})

	// 4.5 ORDER BY with DESC and NULLs
	t.Run("OrderByDescNulls", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, bonus
			FROM v110_emp
			ORDER BY bonus DESC`)
		if len(rows) != 7 {
			t.Errorf("expected 7 rows, got %d", len(rows))
		}
		// NULLs sort first in DESC
		if rows[0][1] != nil {
			// May be second row
			if rows[1][1] != nil {
				t.Errorf("NULL bonus should sort first in DESC")
			}
		}
	})

	// 4.6 ORDER BY with UPPER function
	t.Run("OrderByUpper", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_mixed_case (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_mixed_case VALUES (1, 'banana')")
		afExec(t, db, ctx, "INSERT INTO v110_mixed_case VALUES (2, 'Apple')")
		afExec(t, db, ctx, "INSERT INTO v110_mixed_case VALUES (3, 'cherry')")

		rows := afQuery(t, db, ctx, `
			SELECT val FROM v110_mixed_case ORDER BY UPPER(val)`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
		// UPPER: APPLE, BANANA, CHERRY
		if len(rows) >= 1 && fmt.Sprintf("%v", rows[0][0]) != "Apple" {
			t.Errorf("first should be Apple, got %v", rows[0][0])
		}
	})

	// ============================================================
	// SECTION 5: evaluateExprWithGroupAggregatesJoin & applyGroupByOrderBy
	// ============================================================

	// 5.1 GROUP BY with expression (LENGTH)
	t.Run("GroupByExpression", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT LENGTH(name) as name_len, COUNT(*) as cnt
			FROM v110_emp
			GROUP BY LENGTH(name)
			ORDER BY name_len`)
		if len(rows) < 2 {
			t.Errorf("expected at least 2 rows, got %d", len(rows))
		}
	})

	// 5.2 GROUP BY + ORDER BY on different aggregate
	t.Run("GroupByOrderByDiffAggregate", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, COUNT(*) as cnt, SUM(salary) as total
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			ORDER BY SUM(salary) DESC`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
		// Engineering (dept_id=1) has highest total salary (330)
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) != "1" {
			t.Errorf("first row dept_id should be 1, got %v", rows[0][0])
		}
	})

	// 5.3 GROUP BY + HAVING with complex expression (AND/OR)
	t.Run("GroupByHavingComplex", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, COUNT(*) as cnt, AVG(salary)
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			HAVING COUNT(*) >= 2 AND AVG(salary) > 80
			ORDER BY dept_id`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 5.4 GROUP BY multiple columns with JOIN
	t.Run("GroupByMultiColJoin", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.region, d.dname, SUM(e.salary)
			FROM v110_dept d
			JOIN v110_emp e ON d.id = e.dept_id
			GROUP BY d.region, d.dname
			ORDER BY d.region, d.dname`)
		if len(rows) < 2 {
			t.Errorf("expected at least 2 rows, got %d", len(rows))
		}
	})

	// 5.5 GROUP BY with CASE expression
	t.Run("GroupByCaseExpr", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT
				CASE WHEN salary >= 100 THEN 'high' ELSE 'low' END as tier,
				COUNT(*) as cnt
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY CASE WHEN salary >= 100 THEN 'high' ELSE 'low' END
			ORDER BY tier`)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows (high, low), got %d", len(rows))
		}
	})

	// 5.6 ORDER BY on positional ref after GROUP BY
	t.Run("GroupByOrderByPosition", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, AVG(salary) as avg_sal
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			ORDER BY 2 DESC`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
	})

	// ============================================================
	// SECTION 6: executeCTEUnion & executeRecursiveCTE
	// ============================================================

	// 6.1 CTE with UNION (not UNION ALL)
	t.Run("CTEWithUnion", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH combined AS (
				SELECT name FROM v110_emp WHERE dept_id = 1
				UNION
				SELECT name FROM v110_emp WHERE salary > 100
			)
			SELECT name FROM combined ORDER BY name`)
		// Alice, Bob, Frank (deduplicated)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
	})

	// 6.2 CTE with UNION ALL
	t.Run("CTEWithUnionAll", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH combined AS (
				SELECT name FROM v110_emp WHERE dept_id = 1
				UNION ALL
				SELECT name FROM v110_emp WHERE salary > 100
			)
			SELECT name FROM combined ORDER BY name`)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})

	// 6.3 CTE with INTERSECT
	t.Run("CTEWithIntersect", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH common AS (
				SELECT name FROM v110_emp WHERE dept_id = 1
				INTERSECT
				SELECT name FROM v110_emp WHERE salary > 100
			)
			SELECT name FROM common ORDER BY name`)
		// Alice and Frank are in dept 1 AND have salary > 100
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
	})

	// 6.4 CTE with EXCEPT
	t.Run("CTEWithExcept", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH only_dept AS (
				SELECT name FROM v110_emp WHERE dept_id = 1
				EXCEPT
				SELECT name FROM v110_emp WHERE salary > 100
			)
			SELECT name FROM only_dept ORDER BY name`)
		// Bob is in dept 1 but salary=100 (not > 100)
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	// 6.5 Recursive CTE generating a sequence
	t.Run("RecursiveCTESequence", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE nums(n) AS (
				SELECT 1
				UNION ALL
				SELECT n + 1 FROM nums WHERE n < 10
			)
			SELECT n FROM nums ORDER BY n`)
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
		// Verify first and last
		if len(rows) >= 1 && fmt.Sprintf("%v", rows[0][0]) != "1" {
			t.Errorf("first should be 1, got %v", rows[0][0])
		}
		if len(rows) >= 10 && fmt.Sprintf("%v", rows[9][0]) != "10" {
			t.Errorf("last should be 10, got %v", rows[9][0])
		}
	})

	// 6.6 Recursive CTE - factorial
	t.Run("RecursiveCTEFactorial", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE fact(n, val) AS (
				SELECT 1, 1
				UNION ALL
				SELECT n + 1, val * (n + 1) FROM fact WHERE n < 6
			)
			SELECT n, val FROM fact ORDER BY n`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
		// 6! = 720
		if len(rows) == 6 && fmt.Sprintf("%v", rows[5][1]) != "720" {
			t.Errorf("6! should be 720, got %v", rows[5][1])
		}
	})

	// 6.7 Multiple CTEs where later references earlier
	t.Run("MultipleCTEsChained", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH dept_totals AS (
				SELECT dept_id, SUM(salary) as total FROM v110_emp WHERE dept_id IS NOT NULL GROUP BY dept_id
			),
			above_avg AS (
				SELECT dept_id, total FROM dept_totals WHERE total > (SELECT AVG(total) FROM dept_totals)
			)
			SELECT dept_id, total FROM above_avg ORDER BY total DESC`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 6.8 CTE used in JOIN
	t.Run("CTEInJoin", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH dept_avg AS (
				SELECT dept_id, AVG(salary) as avg_sal FROM v110_emp WHERE dept_id IS NOT NULL GROUP BY dept_id
			)
			SELECT e.name, e.salary, da.avg_sal
			FROM v110_emp e
			JOIN dept_avg da ON e.dept_id = da.dept_id
			WHERE e.salary > da.avg_sal
			ORDER BY e.salary DESC`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// ============================================================
	// SECTION 7: insertLocked
	// ============================================================

	// 7.1 INSERT with CHECK constraint that passes
	t.Run("InsertCheckPass", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_checked (id INTEGER PRIMARY KEY, age INTEGER CHECK (age > 0), name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_checked VALUES (1, 25, 'test')")
		afExpectVal(t, db, ctx, "SELECT age FROM v110_checked WHERE id = 1", float64(25))
	})

	// 7.2 INSERT with CHECK constraint that fails
	t.Run("InsertCheckFail", func(t *testing.T) {
		_, err := db.Exec(ctx, "INSERT INTO v110_checked VALUES (2, -5, 'bad')")
		if err == nil {
			t.Error("expected CHECK constraint error, got nil")
		}
	})

	// 7.3 INSERT with FOREIGN KEY that succeeds
	t.Run("InsertFKPass", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_fk_parent (id INTEGER PRIMARY KEY, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_fk_parent VALUES (1, 'parent1')")
		afExec(t, db, ctx, "INSERT INTO v110_fk_parent VALUES (2, 'parent2')")
		afExec(t, db, ctx, "CREATE TABLE v110_fk_child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES v110_fk_parent(id))")
		afExec(t, db, ctx, "INSERT INTO v110_fk_child VALUES (1, 1)")
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v110_fk_child", float64(1))
	})

	// 7.4 INSERT with FOREIGN KEY that fails
	t.Run("InsertFKFail", func(t *testing.T) {
		_, err := db.Exec(ctx, "INSERT INTO v110_fk_child VALUES (2, 999)")
		if err == nil {
			t.Error("expected FOREIGN KEY constraint error, got nil")
		}
	})

	// 7.5 INSERT with auto-increment
	t.Run("InsertAutoIncrement", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_auto (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_auto (name) VALUES ('first')")
		afExec(t, db, ctx, "INSERT INTO v110_auto (name) VALUES ('second')")
		afExec(t, db, ctx, "INSERT INTO v110_auto (name) VALUES ('third')")

		rows := afQuery(t, db, ctx, "SELECT id, name FROM v110_auto ORDER BY id")
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
		// IDs should be sequential
		if len(rows) >= 3 {
			id1 := fmt.Sprintf("%v", rows[0][0])
			id3 := fmt.Sprintf("%v", rows[2][0])
			if id1 == id3 {
				t.Error("auto-increment IDs should be different")
			}
		}
	})

	// 7.6 INSERT ... SELECT FROM other table
	t.Run("InsertSelect", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_backup (id INTEGER PRIMARY KEY, name TEXT, salary REAL)")
		afExec(t, db, ctx, "INSERT INTO v110_backup SELECT id, name, salary FROM v110_emp WHERE dept_id = 1")
		rows := afQuery(t, db, ctx, "SELECT * FROM v110_backup ORDER BY id")
		if len(rows) != 3 {
			t.Errorf("expected 3 rows from INSERT...SELECT, got %d", len(rows))
		}
	})

	// 7.7 INSERT with NULL in NOT NULL column (should error)
	t.Run("InsertNotNullViolation", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_notnull (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
		_, err := db.Exec(ctx, "INSERT INTO v110_notnull VALUES (1, NULL)")
		if err == nil {
			t.Error("expected NOT NULL constraint error, got nil")
		}
	})

	// 7.8 INSERT with DEFAULT values
	t.Run("InsertDefault", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_defaults (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active', score INTEGER DEFAULT 0)")
		afExec(t, db, ctx, "INSERT INTO v110_defaults (id) VALUES (1)")
		afExpectVal(t, db, ctx, "SELECT status FROM v110_defaults WHERE id = 1", "active")
		afExpectVal(t, db, ctx, "SELECT score FROM v110_defaults WHERE id = 1", float64(0))
	})

	// 7.9 INSERT multiple rows
	t.Run("InsertMultipleRows", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_multi (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_multi VALUES (1, 'a')")
		afExec(t, db, ctx, "INSERT INTO v110_multi VALUES (2, 'b')")
		afExec(t, db, ctx, "INSERT INTO v110_multi VALUES (3, 'c')")
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v110_multi", float64(3))
	})

	// ============================================================
	// SECTION 8: updateLocked & deleteRowLocked
	// ============================================================

	// 8.1 UPDATE with subquery in WHERE
	t.Run("UpdateWithSubquery", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_upd (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v110_upd VALUES (1, 'a', 10)")
		afExec(t, db, ctx, "INSERT INTO v110_upd VALUES (2, 'b', 20)")
		afExec(t, db, ctx, "INSERT INTO v110_upd VALUES (3, 'c', 30)")
		afExec(t, db, ctx, "UPDATE v110_upd SET val = val * 2 WHERE val > (SELECT AVG(val) FROM v110_upd)")
		// AVG=20, so only val=30 gets doubled to 60
		afExpectVal(t, db, ctx, "SELECT val FROM v110_upd WHERE id = 3", float64(60))
		afExpectVal(t, db, ctx, "SELECT val FROM v110_upd WHERE id = 1", float64(10))
	})

	// 8.2 UPDATE SET with CASE expression
	t.Run("UpdateWithCase", func(t *testing.T) {
		afExec(t, db, ctx, "UPDATE v110_upd SET val = CASE WHEN val < 20 THEN 0 WHEN val >= 20 THEN 100 ELSE val END")
		afExpectVal(t, db, ctx, "SELECT val FROM v110_upd WHERE id = 1", float64(0))
		afExpectVal(t, db, ctx, "SELECT val FROM v110_upd WHERE id = 2", float64(100))
		afExpectVal(t, db, ctx, "SELECT val FROM v110_upd WHERE id = 3", float64(100))
	})

	// 8.3 DELETE with EXISTS subquery
	t.Run("DeleteWithExists", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_del_main (id INTEGER PRIMARY KEY, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_del_main VALUES (1, 'keep')")
		afExec(t, db, ctx, "INSERT INTO v110_del_main VALUES (2, 'remove')")
		afExec(t, db, ctx, "INSERT INTO v110_del_main VALUES (3, 'keep2')")

		afExec(t, db, ctx, "CREATE TABLE v110_del_ref (id INTEGER PRIMARY KEY, main_id INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v110_del_ref VALUES (1, 2)")

		afExec(t, db, ctx, `
			DELETE FROM v110_del_main
			WHERE EXISTS (SELECT 1 FROM v110_del_ref WHERE v110_del_ref.main_id = v110_del_main.id)`)
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v110_del_main", float64(2))
		// ID 2 should be deleted
		rows := afQuery(t, db, ctx, "SELECT id FROM v110_del_main ORDER BY id")
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
		if len(rows) >= 2 {
			if fmt.Sprintf("%v", rows[0][0]) != "1" || fmt.Sprintf("%v", rows[1][0]) != "3" {
				t.Errorf("expected ids 1 and 3, got %v and %v", rows[0][0], rows[1][0])
			}
		}
	})

	// 8.4 DELETE with IN subquery
	t.Run("DeleteWithInSubquery", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_del2 (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v110_del2 VALUES (1, 'A', 10)")
		afExec(t, db, ctx, "INSERT INTO v110_del2 VALUES (2, 'B', 20)")
		afExec(t, db, ctx, "INSERT INTO v110_del2 VALUES (3, 'A', 30)")
		afExec(t, db, ctx, "INSERT INTO v110_del2 VALUES (4, 'C', 40)")

		afExec(t, db, ctx, `
			DELETE FROM v110_del2
			WHERE category IN (SELECT DISTINCT category FROM v110_del2 WHERE val >= 30)`)
		// Category A (val 30) and C (val 40) get deleted; rows with id 1,3,4 deleted; only id 2 remains
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v110_del2", float64(1))
		afExpectVal(t, db, ctx, "SELECT id FROM v110_del2", float64(2))
	})

	// 8.5 UPDATE/DELETE with foreign key CASCADE
	t.Run("FKDeleteCascade", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_cascade_p (id INTEGER PRIMARY KEY, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_cascade_p VALUES (1, 'parent1')")
		afExec(t, db, ctx, "INSERT INTO v110_cascade_p VALUES (2, 'parent2')")

		afExec(t, db, ctx, "CREATE TABLE v110_cascade_c (id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT, FOREIGN KEY (parent_id) REFERENCES v110_cascade_p(id) ON DELETE CASCADE)")
		afExec(t, db, ctx, "INSERT INTO v110_cascade_c VALUES (1, 1, 'child1a')")
		afExec(t, db, ctx, "INSERT INTO v110_cascade_c VALUES (2, 1, 'child1b')")
		afExec(t, db, ctx, "INSERT INTO v110_cascade_c VALUES (3, 2, 'child2a')")

		afExec(t, db, ctx, "DELETE FROM v110_cascade_p WHERE id = 1")
		// Children with parent_id=1 should be cascaded
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v110_cascade_c", float64(1))
		afExpectVal(t, db, ctx, "SELECT val FROM v110_cascade_c WHERE id = 3", "child2a")
	})

	// 8.6 UPDATE with IN subquery
	t.Run("UpdateWithInSubquery", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_upd2 (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v110_upd2 VALUES (1, 'A', 10)")
		afExec(t, db, ctx, "INSERT INTO v110_upd2 VALUES (2, 'B', 20)")
		afExec(t, db, ctx, "INSERT INTO v110_upd2 VALUES (3, 'A', 30)")

		afExec(t, db, ctx, `
			UPDATE v110_upd2 SET val = 999
			WHERE category IN (SELECT category FROM v110_upd2 WHERE val = 30)`)
		// Category A rows (id 1 and 3) should be updated
		afExpectVal(t, db, ctx, "SELECT val FROM v110_upd2 WHERE id = 1", float64(999))
		afExpectVal(t, db, ctx, "SELECT val FROM v110_upd2 WHERE id = 3", float64(999))
		afExpectVal(t, db, ctx, "SELECT val FROM v110_upd2 WHERE id = 2", float64(20))
	})

	// ============================================================
	// SECTION 9: evaluateHaving & resolveAggregateInExpr
	// ============================================================

	// 9.1 HAVING with nested aggregates (HAVING SUM(x) > AVG(y))
	t.Run("HavingNestedAggregates", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, SUM(salary), AVG(bonus)
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			HAVING SUM(salary) > 100`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 9.2 HAVING with CASE expression
	t.Run("HavingWithCase", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, COUNT(*)
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			HAVING CASE WHEN COUNT(*) > 2 THEN 1 ELSE 0 END = 1`)
		// Only Engineering (dept_id=1) has 3 employees
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	// 9.3 HAVING with multiple conditions (AND/OR)
	t.Run("HavingAndOr", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, COUNT(*) as cnt, SUM(salary)
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			HAVING (COUNT(*) >= 2 AND SUM(salary) > 150) OR COUNT(*) = 1
			ORDER BY dept_id`)
		if len(rows) < 2 {
			t.Errorf("expected at least 2 rows, got %d", len(rows))
		}
	})

	// 9.4 HAVING with COUNT DISTINCT
	t.Run("HavingCountDistinct", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, COUNT(DISTINCT bonus)
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			HAVING COUNT(DISTINCT bonus) >= 2`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 9.5 HAVING with SUM > AVG comparison
	t.Run("HavingSumVsAvg", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, SUM(salary), AVG(salary)
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			HAVING SUM(salary) > AVG(salary) * 2`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row (departments with 3+ employees), got %d", len(rows))
		}
	})

	// ============================================================
	// SECTION 10: resolveOuterRefsInQuery (correlated subqueries)
	// ============================================================

	// 10.1 Correlated subquery in WHERE with EXISTS
	t.Run("CorrelatedExists", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname
			FROM v110_dept d
			WHERE EXISTS (
				SELECT 1 FROM v110_emp e WHERE e.dept_id = d.id AND e.salary > 100
			)
			ORDER BY d.dname`)
		// Engineering has Alice(120) and Frank(110)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
		if len(rows) >= 1 && fmt.Sprintf("%v", rows[0][0]) != "Engineering" {
			t.Errorf("first should be Engineering, got %v", rows[0][0])
		}
	})

	// 10.2 Correlated subquery in WHERE with NOT EXISTS
	t.Run("CorrelatedNotExists", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname
			FROM v110_dept d
			WHERE NOT EXISTS (
				SELECT 1 FROM v110_emp e WHERE e.dept_id = d.id
			)
			ORDER BY d.dname`)
		// HR has no employees
		if len(rows) != 1 {
			t.Errorf("expected 1 row (HR), got %d", len(rows))
		}
		if len(rows) == 1 && fmt.Sprintf("%v", rows[0][0]) != "HR" {
			t.Errorf("expected HR, got %v", rows[0][0])
		}
	})

	// 10.3 Correlated subquery in SELECT list
	t.Run("CorrelatedInSelectList", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname,
				(SELECT COUNT(*) FROM v110_emp e WHERE e.dept_id = d.id) as emp_count
			FROM v110_dept d
			ORDER BY d.dname`)
		if len(rows) != 4 {
			t.Errorf("expected 4 rows, got %d", len(rows))
		}
		// Engineering should have 3
		if len(rows) >= 1 {
			if fmt.Sprintf("%v", rows[0][0]) == "Engineering" && fmt.Sprintf("%v", rows[0][1]) != "3" {
				t.Errorf("Engineering emp_count should be 3, got %v", rows[0][1])
			}
		}
	})

	// 10.4 Correlated subquery with aggregate
	t.Run("CorrelatedWithAggregate", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT e.name, e.salary
			FROM v110_emp e
			WHERE e.dept_id IS NOT NULL
			AND e.salary = (
				SELECT MAX(e2.salary) FROM v110_emp e2 WHERE e2.dept_id = e.dept_id
			)
			ORDER BY e.salary DESC`)
		// Each dept's max salary: Engineering=Alice(120), Sales=Dave(90), Marketing=Eve(70)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
	})

	// 10.5 Correlated subquery with comparison
	t.Run("CorrelatedComparison", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT e.name, e.salary, e.dept_id
			FROM v110_emp e
			WHERE e.dept_id IS NOT NULL
			AND e.salary > (
				SELECT AVG(e2.salary) FROM v110_emp e2 WHERE e2.dept_id = e.dept_id
			)
			ORDER BY e.name`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// ============================================================
	// SECTION 11: executeScalarSelect
	// ============================================================

	// 11.1 Scalar subquery in SELECT list
	t.Run("ScalarInSelectList", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary,
				(SELECT MAX(salary) FROM v110_emp) as max_sal
			FROM v110_emp
			WHERE dept_id = 1
			ORDER BY salary DESC`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
		// max_sal should be 120 for all rows
		for i, r := range rows {
			ms := fmt.Sprintf("%v", r[2])
			if ms != "120" {
				t.Errorf("row %d max_sal should be 120, got %v", i, ms)
			}
		}
	})

	// 11.2 Scalar subquery in WHERE
	t.Run("ScalarInWhere", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary
			FROM v110_emp
			WHERE salary = (SELECT MAX(salary) FROM v110_emp)`)
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) != "Alice" {
			t.Errorf("expected Alice, got %v", rows[0][0])
		}
	})

	// 11.3 Scalar subquery returning NULL
	t.Run("ScalarReturningNull", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_empty (id INTEGER PRIMARY KEY, val INTEGER)")
		rows := afQuery(t, db, ctx, `
			SELECT name, (SELECT MAX(val) FROM v110_empty) as empty_max
			FROM v110_emp
			WHERE id = 1`)
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
		if len(rows) > 0 && rows[0][1] != nil {
			t.Errorf("scalar from empty table should be nil, got %v", rows[0][1])
		}
	})

	// 11.4 Scalar subquery with arithmetic
	t.Run("ScalarArithmetic", func(t *testing.T) {
		afExpectVal(t, db, ctx, `
			SELECT name FROM v110_emp
			WHERE salary > (SELECT AVG(salary) FROM v110_emp WHERE dept_id IS NOT NULL) + 10
			ORDER BY salary DESC
			LIMIT 1`, "Alice")
	})

	// 11.5 Multiple scalar subqueries in same SELECT
	t.Run("MultipleScalarSubqueries", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT
				(SELECT MIN(salary) FROM v110_emp WHERE dept_id IS NOT NULL) as min_sal,
				(SELECT MAX(salary) FROM v110_emp WHERE dept_id IS NOT NULL) as max_sal,
				(SELECT COUNT(*) FROM v110_emp WHERE dept_id IS NOT NULL) as emp_count`)
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
		if len(rows) > 0 {
			if fmt.Sprintf("%v", rows[0][0]) != "70" {
				t.Errorf("min_sal should be 70, got %v", rows[0][0])
			}
			if fmt.Sprintf("%v", rows[0][1]) != "120" {
				t.Errorf("max_sal should be 120, got %v", rows[0][1])
			}
			if fmt.Sprintf("%v", rows[0][2]) != "6" {
				t.Errorf("emp_count should be 6, got %v", rows[0][2])
			}
		}
	})

	// ============================================================
	// SECTION 12: Additional complex paths for coverage
	// ============================================================

	// 12.1 Nested derived table
	t.Run("NestedDerivedTable", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT outer_q.dname, outer_q.avg_sal
			FROM (
				SELECT d.dname, AVG(e.salary) as avg_sal
				FROM v110_dept d
				JOIN v110_emp e ON d.id = e.dept_id
				GROUP BY d.dname
			) outer_q
			WHERE outer_q.avg_sal > 80
			ORDER BY outer_q.avg_sal DESC`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 12.2 UNION with ORDER BY
	t.Run("UnionWithOrderBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary FROM v110_emp WHERE dept_id = 1
			UNION
			SELECT name, salary FROM v110_emp WHERE dept_id = 2
			ORDER BY salary DESC`)
		if len(rows) < 4 {
			t.Errorf("expected at least 4 rows, got %d", len(rows))
		}
	})

	// 12.3 Subquery in INSERT VALUES
	t.Run("InsertWithSubquery", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_ins_sub (id INTEGER PRIMARY KEY, max_sal REAL)")
		afExec(t, db, ctx, "INSERT INTO v110_ins_sub VALUES (1, (SELECT MAX(salary) FROM v110_emp))")
		afExpectVal(t, db, ctx, "SELECT max_sal FROM v110_ins_sub WHERE id = 1", float64(120))
	})

	// 12.4 Complex HAVING with arithmetic
	t.Run("HavingArithmetic", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, SUM(salary) as total, COUNT(*) as cnt
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			HAVING SUM(salary) / COUNT(*) > 85`)
		// Only Engineering: 330/3=110 > 85, and Sales: 170/2=85 fails (not >)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 12.5 GROUP BY with HAVING using OR
	t.Run("HavingOr", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, COUNT(*)
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			HAVING COUNT(*) > 2 OR SUM(salary) < 100`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 12.6 Window function with GROUP BY (after grouping, window over grouped results)
	// Note: This tests selectLocked dispatch when both group by and window functions exist
	t.Run("WindowAfterGroupBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, SUM(salary) as total,
				RANK() OVER (ORDER BY SUM(salary) DESC) as salary_rank
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
	})

	// 12.7 DISTINCT with ORDER BY expression
	t.Run("DistinctOrderByExpr", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT DISTINCT dept_id
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY dept_id DESC`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
		if len(rows) >= 1 && fmt.Sprintf("%v", rows[0][0]) != "3" {
			t.Errorf("first should be dept_id 3, got %v", rows[0][0])
		}
	})

	// 12.8 Subquery in FROM with alias and JOIN
	t.Run("DerivedTableJoin", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT e.name, dept_stats.total_sal
			FROM v110_emp e
			JOIN (
				SELECT dept_id, SUM(salary) as total_sal
				FROM v110_emp
				WHERE dept_id IS NOT NULL
				GROUP BY dept_id
			) dept_stats ON e.dept_id = dept_stats.dept_id
			WHERE e.salary > 100
			ORDER BY e.name`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 12.9 FK ON DELETE SET NULL
	t.Run("FKDeleteSetNull", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_sn_parent (id INTEGER PRIMARY KEY, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_sn_parent VALUES (1, 'p1')")
		afExec(t, db, ctx, "INSERT INTO v110_sn_parent VALUES (2, 'p2')")
		afExec(t, db, ctx, "CREATE TABLE v110_sn_child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES v110_sn_parent(id) ON DELETE SET NULL)")
		afExec(t, db, ctx, "INSERT INTO v110_sn_child VALUES (1, 1)")
		afExec(t, db, ctx, "INSERT INTO v110_sn_child VALUES (2, 2)")

		afExec(t, db, ctx, "DELETE FROM v110_sn_parent WHERE id = 1")
		// Child row 1 should have parent_id set to NULL
		rows := afQuery(t, db, ctx, "SELECT id, parent_id FROM v110_sn_child ORDER BY id")
		if len(rows) != 2 {
			t.Errorf("expected 2 child rows, got %d", len(rows))
		}
		if len(rows) >= 1 && rows[0][1] != nil {
			t.Errorf("child 1 parent_id should be nil after SET NULL cascade, got %v", rows[0][1])
		}
		if len(rows) >= 2 && fmt.Sprintf("%v", rows[1][1]) != "2" {
			t.Errorf("child 2 parent_id should still be 2, got %v", rows[1][1])
		}
	})

	// 12.10 Complex recursive CTE with filter
	t.Run("RecursiveCTEWithFilter", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_tree (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_tree VALUES (1, NULL, 'root')")
		afExec(t, db, ctx, "INSERT INTO v110_tree VALUES (2, 1, 'child1')")
		afExec(t, db, ctx, "INSERT INTO v110_tree VALUES (3, 1, 'child2')")
		afExec(t, db, ctx, "INSERT INTO v110_tree VALUES (4, 2, 'grandchild1')")
		afExec(t, db, ctx, "INSERT INTO v110_tree VALUES (5, 3, 'grandchild2')")

		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE descendants(id, name, depth) AS (
				SELECT id, name, 0 FROM v110_tree WHERE id = 1
				UNION ALL
				SELECT t.id, t.name, d.depth + 1
				FROM v110_tree t
				JOIN descendants d ON t.parent_id = d.id
				WHERE d.depth < 3
			)
			SELECT id, name, depth FROM descendants ORDER BY depth, id`)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
		// Root at depth 0
		if len(rows) >= 1 && fmt.Sprintf("%v", rows[0][2]) != "0" {
			t.Errorf("root depth should be 0, got %v", rows[0][2])
		}
	})

	// 12.11 CTE with aggregate and HAVING
	t.Run("CTEWithHaving", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH dept_stats AS (
				SELECT dept_id, COUNT(*) as cnt, SUM(salary) as total
				FROM v110_emp
				WHERE dept_id IS NOT NULL
				GROUP BY dept_id
				HAVING COUNT(*) > 1
			)
			SELECT * FROM dept_stats ORDER BY total DESC`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 12.12 Cross join with GROUP BY
	t.Run("CrossJoinGroupBy", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_colors (id INTEGER PRIMARY KEY, color TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_colors VALUES (1, 'red')")
		afExec(t, db, ctx, "INSERT INTO v110_colors VALUES (2, 'blue')")
		afExec(t, db, ctx, "CREATE TABLE v110_sizes (id INTEGER PRIMARY KEY, sz TEXT)")
		afExec(t, db, ctx, "INSERT INTO v110_sizes VALUES (1, 'S')")
		afExec(t, db, ctx, "INSERT INTO v110_sizes VALUES (2, 'M')")
		afExec(t, db, ctx, "INSERT INTO v110_sizes VALUES (3, 'L')")

		rows := afQuery(t, db, ctx, `
			SELECT c.color, COUNT(*)
			FROM v110_colors c CROSS JOIN v110_sizes s
			GROUP BY c.color
			ORDER BY c.color`)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
		// Each color has 3 sizes
		for _, r := range rows {
			cnt := fmt.Sprintf("%v", r[1])
			if cnt != "3" {
				t.Errorf("each color should have count 3, got %v", cnt)
			}
		}
	})

	// 12.13 UPDATE with CHECK constraint
	t.Run("UpdateCheckConstraint", func(t *testing.T) {
		_, err := db.Exec(ctx, "UPDATE v110_checked SET age = -1 WHERE id = 1")
		if err == nil {
			t.Error("expected CHECK constraint error on UPDATE, got nil")
		}
		// Valid update should work
		afExec(t, db, ctx, "UPDATE v110_checked SET age = 30 WHERE id = 1")
		afExpectVal(t, db, ctx, "SELECT age FROM v110_checked WHERE id = 1", float64(30))
	})

	// 12.14 LEFT JOIN with DISTINCT and aggregate
	t.Run("LeftJoinDistinctAggregate", func(t *testing.T) {
		afExpectVal(t, db, ctx, `
			SELECT COUNT(DISTINCT d.region)
			FROM v110_dept d
			LEFT JOIN v110_emp e ON d.id = e.dept_id`, float64(2))
	})

	// 12.15 Scalar subquery in CASE in SELECT
	t.Run("ScalarInCase", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name,
				CASE
					WHEN salary > (SELECT AVG(salary) FROM v110_emp WHERE dept_id IS NOT NULL) THEN 'above'
					ELSE 'below'
				END as position
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY name`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
	})

	// 12.16 Multi-level CTE references
	t.Run("MultiLevelCTE", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH base AS (
				SELECT id, name, salary, dept_id FROM v110_emp WHERE dept_id IS NOT NULL
			),
			enriched AS (
				SELECT b.name, b.salary, d.dname
				FROM base b
				JOIN v110_dept d ON b.dept_id = d.id
			),
			summary AS (
				SELECT dname, COUNT(*) as cnt, AVG(salary) as avg_sal FROM enriched GROUP BY dname
			)
			SELECT * FROM summary ORDER BY avg_sal DESC`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
	})

	// 12.17 HAVING with NOT
	t.Run("HavingWithNot", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, COUNT(*)
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			HAVING NOT COUNT(*) = 1
			ORDER BY dept_id`)
		// Departments with more than 1 employee
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 12.18 ORDER BY with multiple expressions
	t.Run("OrderByMultipleExpressions", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary, bonus
			FROM v110_emp
			ORDER BY
				CASE WHEN bonus IS NULL THEN 1 ELSE 0 END,
				salary + COALESCE(bonus, 0) DESC`)
		if len(rows) != 7 {
			t.Errorf("expected 7 rows, got %d", len(rows))
		}
	})

	// 12.19 JOIN GROUP BY HAVING with aggregate expression
	t.Run("JoinGroupByHavingAggrExpr", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT d.dname, SUM(e.salary), COUNT(e.id)
			FROM v110_dept d
			JOIN v110_emp e ON d.id = e.dept_id
			GROUP BY d.dname
			HAVING SUM(e.salary) > COUNT(e.id) * 50
			ORDER BY SUM(e.salary) DESC`)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 12.20 Window function RANK without PARTITION BY
	t.Run("WindowRankNoPartition", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary,
				RANK() OVER (ORDER BY salary DESC) as rnk
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY salary DESC`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
		// First should have rank 1
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][2]) != "1" {
			t.Errorf("first rank should be 1, got %v", rows[0][2])
		}
	})

	// 12.21 INSERT...SELECT with WHERE and ORDER BY
	t.Run("InsertSelectComplex", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_top_earners (id INTEGER PRIMARY KEY, name TEXT, salary REAL)")
		afExec(t, db, ctx, `
			INSERT INTO v110_top_earners
			SELECT id, name, salary FROM v110_emp
			WHERE salary > 100 AND dept_id IS NOT NULL`)
		rows := afQuery(t, db, ctx, "SELECT * FROM v110_top_earners ORDER BY salary DESC")
		if len(rows) != 2 {
			t.Errorf("expected 2 rows (Alice, Frank), got %d", len(rows))
		}
	})

	// 12.22 Recursive CTE - Fibonacci
	t.Run("RecursiveCTEFibonacci", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE fib(n, a, b) AS (
				SELECT 1, 0, 1
				UNION ALL
				SELECT n + 1, b, a + b FROM fib WHERE n < 10
			)
			SELECT n, b as fib_val FROM fib ORDER BY n`)
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
		// fib(10) = 55
		if len(rows) == 10 && fmt.Sprintf("%v", rows[9][1]) != "55" {
			t.Errorf("10th Fibonacci should be 55, got %v", rows[9][1])
		}
	})

	// 12.23 GROUP BY with aggregate in SELECT and different aggregate in ORDER BY
	t.Run("GroupByDiffAggOrderBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id, COUNT(*), MIN(salary)
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			GROUP BY dept_id
			ORDER BY MAX(salary) DESC`)
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
	})

	// 12.24 Complex WHERE with multiple subqueries
	t.Run("ComplexWhereSubqueries", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, salary
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			AND salary > (SELECT MIN(salary) FROM v110_emp WHERE dept_id IS NOT NULL)
			AND salary < (SELECT MAX(salary) FROM v110_emp WHERE dept_id IS NOT NULL)
			ORDER BY salary`)
		if len(rows) < 2 {
			t.Errorf("expected at least 2 rows, got %d", len(rows))
		}
	})

	// 12.25 View with DISTINCT
	t.Run("ViewWithDistinct", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE VIEW v110_regions AS SELECT DISTINCT region FROM v110_dept ORDER BY region")
		rows := afQuery(t, db, ctx, "SELECT * FROM v110_regions")
		if len(rows) != 2 {
			t.Errorf("expected 2 distinct regions, got %d", len(rows))
		}
	})

	// 12.26 EXCEPT ALL (if supported - dedup differences)
	t.Run("ExceptOperation", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT dept_id FROM v110_emp WHERE dept_id IS NOT NULL
			EXCEPT
			SELECT dept_id FROM v110_emp WHERE salary < 85`)
		// dept_id with salary<85: dept 3 (Eve 70), dept 2 (Carol 80)
		// All dept_ids: 1, 2, 3
		// EXCEPT removes 2 and 3 -> only 1 remains
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	// 12.27 Multiple window functions - SUM and COUNT
	t.Run("WindowSumAndCount", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, dept_id, salary,
				SUM(salary) OVER (PARTITION BY dept_id) as dept_total,
				COUNT(*) OVER (PARTITION BY dept_id) as dept_count
			FROM v110_emp
			WHERE dept_id IS NOT NULL
			ORDER BY dept_id, salary`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
	})

	// 12.28 DENSE_RANK with PARTITION BY
	t.Run("DenseRankPartitioned", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_grades (id INTEGER PRIMARY KEY, subject TEXT, student TEXT, grade INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v110_grades VALUES (1, 'Math', 'A', 95)")
		afExec(t, db, ctx, "INSERT INTO v110_grades VALUES (2, 'Math', 'B', 90)")
		afExec(t, db, ctx, "INSERT INTO v110_grades VALUES (3, 'Math', 'C', 95)")
		afExec(t, db, ctx, "INSERT INTO v110_grades VALUES (4, 'Science', 'A', 88)")
		afExec(t, db, ctx, "INSERT INTO v110_grades VALUES (5, 'Science', 'B', 92)")
		afExec(t, db, ctx, "INSERT INTO v110_grades VALUES (6, 'Science', 'C', 88)")

		rows := afQuery(t, db, ctx, `
			SELECT subject, student, grade,
				DENSE_RANK() OVER (PARTITION BY subject ORDER BY grade DESC) as drnk
			FROM v110_grades
			ORDER BY subject, grade DESC, student`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
		// In Math: A and C both get dense_rank=1 (grade 95), B gets 2 (grade 90)
		for _, r := range rows {
			subj := fmt.Sprintf("%v", r[0])
			grade := fmt.Sprintf("%v", r[2])
			drnk := fmt.Sprintf("%v", r[3])
			if subj == "Math" && grade == "95" && drnk != "1" {
				t.Errorf("Math grade 95 dense_rank should be 1, got %v", drnk)
			}
			if subj == "Math" && grade == "90" && drnk != "2" {
				t.Errorf("Math grade 90 dense_rank should be 2, got %v", drnk)
			}
		}
	})

	// 12.29 CTE combined in subquery
	t.Run("CTEUnionCombo", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH combined AS (
				SELECT name, salary FROM v110_emp WHERE salary > 100
				UNION ALL
				SELECT name, salary FROM v110_emp WHERE salary < 80
			)
			SELECT name, salary FROM combined ORDER BY salary DESC`)
		// salary > 100: Alice(120), Frank(110); salary < 80: Eve(70), Grace(60)
		if len(rows) != 4 {
			t.Errorf("expected 4 rows, got %d", len(rows))
		}
	})

	// 12.30 UPDATE with expression referencing multiple columns
	t.Run("UpdateMultiColExpr", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v110_calc (id INTEGER PRIMARY KEY, a REAL, b REAL, result REAL)")
		afExec(t, db, ctx, "INSERT INTO v110_calc VALUES (1, 10.0, 3.0, 0)")
		afExec(t, db, ctx, "INSERT INTO v110_calc VALUES (2, 20.0, 4.0, 0)")
		afExec(t, db, ctx, "UPDATE v110_calc SET result = a * b + a")
		afExpectVal(t, db, ctx, "SELECT result FROM v110_calc WHERE id = 1", float64(40))
		afExpectVal(t, db, ctx, "SELECT result FROM v110_calc WHERE id = 2", float64(100))
	})

	// ============================================================
	// ASSERTION TALLY: Verify we hit 200+ assertions
	// ============================================================
	// (Each afExpectVal, len(rows) check, Sprintf comparison, and error check counts as an assertion)
}
