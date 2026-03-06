package test

import (
	"fmt"
	"testing"
)

// TestV74FinalValidation is a comprehensive final validation test exercising
// realistic business scenarios: HR analytics, inventory management,
// financial reporting, and complex multi-step data transformations.
func TestV74FinalValidation(t *testing.T) {
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
	// === SCENARIO 1: HR ANALYTICS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v74_departments (
		id INTEGER PRIMARY KEY, name TEXT NOT NULL, location TEXT
	)`)
	afExec(t, db, ctx, `CREATE TABLE v74_employees (
		id INTEGER PRIMARY KEY, name TEXT NOT NULL, dept_id INTEGER,
		salary INTEGER, level TEXT, hire_year INTEGER
	)`)
	afExec(t, db, ctx, `CREATE TABLE v74_reviews (
		id INTEGER PRIMARY KEY, emp_id INTEGER, score INTEGER,
		review_year INTEGER
	)`)

	// Departments
	afExec(t, db, ctx, "INSERT INTO v74_departments VALUES (1, 'Engineering', 'NYC')")
	afExec(t, db, ctx, "INSERT INTO v74_departments VALUES (2, 'Product', 'SF')")
	afExec(t, db, ctx, "INSERT INTO v74_departments VALUES (3, 'Sales', 'CHI')")
	afExec(t, db, ctx, "INSERT INTO v74_departments VALUES (4, 'Support', 'NYC')")

	// Employees
	afExec(t, db, ctx, "INSERT INTO v74_employees VALUES (1, 'Alice', 1, 150000, 'senior', 2018)")
	afExec(t, db, ctx, "INSERT INTO v74_employees VALUES (2, 'Bob', 1, 120000, 'mid', 2020)")
	afExec(t, db, ctx, "INSERT INTO v74_employees VALUES (3, 'Carol', 1, 140000, 'senior', 2017)")
	afExec(t, db, ctx, "INSERT INTO v74_employees VALUES (4, 'Dave', 2, 130000, 'senior', 2019)")
	afExec(t, db, ctx, "INSERT INTO v74_employees VALUES (5, 'Eve', 2, 110000, 'mid', 2021)")
	afExec(t, db, ctx, "INSERT INTO v74_employees VALUES (6, 'Frank', 3, 95000, 'mid', 2020)")
	afExec(t, db, ctx, "INSERT INTO v74_employees VALUES (7, 'Grace', 3, 105000, 'senior', 2018)")
	afExec(t, db, ctx, "INSERT INTO v74_employees VALUES (8, 'Hank', 4, 80000, 'junior', 2022)")
	afExec(t, db, ctx, "INSERT INTO v74_employees VALUES (9, 'Ivy', 4, 85000, 'mid', 2021)")
	afExec(t, db, ctx, "INSERT INTO v74_employees VALUES (10, 'Jack', 1, 160000, 'principal', 2015)")

	// Reviews
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (1, 1, 95, 2023)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (2, 2, 85, 2023)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (3, 3, 90, 2023)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (4, 4, 88, 2023)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (5, 5, 82, 2023)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (6, 6, 78, 2023)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (7, 7, 92, 2023)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (8, 8, 70, 2023)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (9, 9, 75, 2023)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (10, 10, 98, 2023)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (11, 1, 92, 2022)")
	afExec(t, db, ctx, "INSERT INTO v74_reviews VALUES (12, 4, 85, 2022)")

	// HR1: Department headcount and average salary
	check("HR1 eng headcount",
		"SELECT COUNT(*) FROM v74_employees WHERE dept_id = 1", 4)
	check("HR1 eng avg salary",
		"SELECT AVG(salary) FROM v74_employees WHERE dept_id = 1", 142500)

	// HR2: Top earner per department
	check("HR2 top earner",
		`WITH ranked AS (
			SELECT e.name, d.name as dept, e.salary,
				   ROW_NUMBER() OVER (PARTITION BY e.dept_id ORDER BY e.salary DESC) as rn
			FROM v74_employees e
			JOIN v74_departments d ON e.dept_id = d.id
		)
		SELECT name FROM ranked WHERE dept = 'Engineering' AND rn = 1`, "Jack")

	// HR3: Salary distribution
	check("HR3 salary distribution",
		`SELECT COUNT(*) FROM v74_employees WHERE salary > 100000`, 7)
	// >100k: Alice(150k), Bob(120k), Carol(140k), Dave(130k), Eve(110k), Grace(105k), Jack(160k) = 7

	// HR4: Department salary report via CTE
	check("HR4 dept report",
		`WITH dept_stats AS (
			SELECT d.name, COUNT(*) as emp_count, SUM(e.salary) as total_salary,
				   AVG(e.salary) as avg_salary
			FROM v74_employees e
			JOIN v74_departments d ON e.dept_id = d.id
			GROUP BY d.name
		)
		SELECT name FROM dept_stats ORDER BY total_salary DESC LIMIT 1`, "Engineering")

	// HR5: Employee with highest review score
	check("HR5 top reviewed",
		`SELECT e.name FROM v74_employees e
		 JOIN v74_reviews r ON e.id = r.emp_id
		 WHERE r.review_year = 2023
		 ORDER BY r.score DESC LIMIT 1`, "Jack")

	// HR6: Average review score by department
	check("HR6 avg review by dept",
		`SELECT d.name FROM v74_departments d
		 JOIN v74_employees e ON d.id = e.dept_id
		 JOIN v74_reviews r ON e.id = r.emp_id
		 WHERE r.review_year = 2023
		 GROUP BY d.name
		 ORDER BY AVG(r.score) DESC LIMIT 1`, "Engineering")
	// Eng: (95+85+90+98)/4=92; Product: (88+82)/2=85; Sales: (78+92)/2=85; Support: (70+75)/2=72.5

	// HR7: Employees above department average salary
	check("HR7 above dept avg",
		`SELECT COUNT(*) FROM v74_employees e1
		 WHERE salary > (
			SELECT AVG(salary) FROM v74_employees e2 WHERE e2.dept_id = e1.dept_id
		 )`, 5)

	// HR8: Level distribution
	checkRowCount("HR8 level dist",
		"SELECT level, COUNT(*) FROM v74_employees GROUP BY level", 4)
	// senior, mid, junior, principal = 4 levels

	// ============================================================
	// === SCENARIO 2: INVENTORY MANAGEMENT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v74_products (
		id INTEGER PRIMARY KEY, name TEXT NOT NULL, category TEXT,
		price INTEGER, stock INTEGER, min_stock INTEGER DEFAULT 5
	)`)
	afExec(t, db, ctx, `CREATE TABLE v74_sales (
		id INTEGER PRIMARY KEY, product_id INTEGER, qty INTEGER,
		sale_date TEXT, customer TEXT
	)`)

	// Products
	afExec(t, db, ctx, "INSERT INTO v74_products VALUES (1, 'Laptop', 'electronics', 999, 50, 10)")
	afExec(t, db, ctx, "INSERT INTO v74_products VALUES (2, 'Phone', 'electronics', 699, 100, 20)")
	afExec(t, db, ctx, "INSERT INTO v74_products VALUES (3, 'Headphones', 'electronics', 149, 200, 30)")
	afExec(t, db, ctx, "INSERT INTO v74_products VALUES (4, 'Desk', 'furniture', 399, 25, 5)")
	afExec(t, db, ctx, "INSERT INTO v74_products VALUES (5, 'Chair', 'furniture', 299, 30, 5)")
	afExec(t, db, ctx, "INSERT INTO v74_products VALUES (6, 'Monitor', 'electronics', 449, 3, 10)")
	afExec(t, db, ctx, "INSERT INTO v74_products VALUES (7, 'Keyboard', 'accessories', 79, 150, 20)")
	afExec(t, db, ctx, "INSERT INTO v74_products VALUES (8, 'Mouse', 'accessories', 49, 180, 20)")

	// Sales
	afExec(t, db, ctx, "INSERT INTO v74_sales VALUES (1, 1, 5, '2024-01', 'Acme')")
	afExec(t, db, ctx, "INSERT INTO v74_sales VALUES (2, 2, 10, '2024-01', 'Acme')")
	afExec(t, db, ctx, "INSERT INTO v74_sales VALUES (3, 3, 20, '2024-01', 'Beta')")
	afExec(t, db, ctx, "INSERT INTO v74_sales VALUES (4, 1, 3, '2024-02', 'Beta')")
	afExec(t, db, ctx, "INSERT INTO v74_sales VALUES (5, 2, 15, '2024-02', 'Acme')")
	afExec(t, db, ctx, "INSERT INTO v74_sales VALUES (6, 4, 2, '2024-02', 'Gamma')")
	afExec(t, db, ctx, "INSERT INTO v74_sales VALUES (7, 7, 30, '2024-01', 'Acme')")
	afExec(t, db, ctx, "INSERT INTO v74_sales VALUES (8, 8, 25, '2024-01', 'Beta')")
	afExec(t, db, ctx, "INSERT INTO v74_sales VALUES (9, 6, 1, '2024-02', 'Gamma')")
	afExec(t, db, ctx, "INSERT INTO v74_sales VALUES (10, 5, 4, '2024-02', 'Acme')")

	// INV1: Products below minimum stock
	checkRowCount("INV1 below min stock",
		"SELECT * FROM v74_products WHERE stock < min_stock", 1)
	// Monitor: stock=3, min=10 → below

	// INV2: Revenue by category
	check("INV2 electronics revenue",
		`SELECT SUM(p.price * s.qty) FROM v74_sales s
		 JOIN v74_products p ON s.product_id = p.id
		 WHERE p.category = 'electronics'`, 28896)
	// Laptop(999): 5*999+3*999=7992; Phone(699): 10*699+15*699=17475; Headphones(149): 20*149=2980; Monitor(449): 1*449=449 = 28896

	// INV3: Top selling product by quantity
	check("INV3 top product",
		`SELECT p.name FROM v74_products p
		 JOIN v74_sales s ON p.id = s.product_id
		 GROUP BY p.name
		 ORDER BY SUM(s.qty) DESC LIMIT 1`, "Keyboard")
	// Keyboard: 30; Phone: 25; Headphones: 20; Mouse: 25

	// INV4: Revenue by customer
	check("INV4 top customer",
		`SELECT customer FROM v74_sales s
		 JOIN v74_products p ON s.product_id = p.id
		 GROUP BY customer
		 ORDER BY SUM(p.price * s.qty) DESC LIMIT 1`, "Acme")

	// INV5: Stock value (price * stock)
	check("INV5 total stock value",
		"SELECT SUM(price * stock) FROM v74_products", 190612)
	// 999*50+699*100+149*200+399*25+299*30+449*3+79*150+49*180 = 190612

	// INV6: Products with no sales - all products have at least one sale
	checkRowCount("INV6 no sales",
		`SELECT p.name FROM v74_products p
		 LEFT JOIN v74_sales s ON p.id = s.product_id
		 WHERE s.id IS NULL`, 0)

	// ============================================================
	// === SCENARIO 3: FINANCIAL REPORTING ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v74_accounts (
		id INTEGER PRIMARY KEY, name TEXT, acc_type TEXT, balance INTEGER
	)`)
	afExec(t, db, ctx, `CREATE TABLE v74_transactions (
		id INTEGER PRIMARY KEY, from_acc INTEGER, to_acc INTEGER,
		amount INTEGER, txn_date TEXT
	)`)

	afExec(t, db, ctx, "INSERT INTO v74_accounts VALUES (1, 'Checking', 'asset', 5000)")
	afExec(t, db, ctx, "INSERT INTO v74_accounts VALUES (2, 'Savings', 'asset', 15000)")
	afExec(t, db, ctx, "INSERT INTO v74_accounts VALUES (3, 'Credit Card', 'liability', 2000)")
	afExec(t, db, ctx, "INSERT INTO v74_accounts VALUES (4, 'Revenue', 'income', 0)")

	afExec(t, db, ctx, "INSERT INTO v74_transactions VALUES (1, 4, 1, 3000, '2024-01')")
	afExec(t, db, ctx, "INSERT INTO v74_transactions VALUES (2, 1, 2, 1000, '2024-01')")
	afExec(t, db, ctx, "INSERT INTO v74_transactions VALUES (3, 1, 3, 500, '2024-02')")
	afExec(t, db, ctx, "INSERT INTO v74_transactions VALUES (4, 4, 1, 5000, '2024-02')")
	afExec(t, db, ctx, "INSERT INTO v74_transactions VALUES (5, 2, 1, 2000, '2024-03')")

	// FR1: Total assets
	check("FR1 total assets",
		"SELECT SUM(balance) FROM v74_accounts WHERE acc_type = 'asset'", 20000)

	// FR2: Transaction volume
	check("FR2 txn volume", "SELECT SUM(amount) FROM v74_transactions", 11500)

	// FR3: Net flow per account using CTE
	check("FR3 net flow checking",
		`WITH outflows AS (
			SELECT from_acc as acc_id, SUM(amount) as out_total
			FROM v74_transactions GROUP BY from_acc
		),
		inflows AS (
			SELECT to_acc as acc_id, SUM(amount) as in_total
			FROM v74_transactions GROUP BY to_acc
		)
		SELECT COALESCE(i.in_total, 0) - COALESCE(o.out_total, 0)
		FROM v74_accounts a
		LEFT JOIN inflows i ON a.id = i.acc_id
		LEFT JOIN outflows o ON a.id = o.acc_id
		WHERE a.id = 1`, 8500)
	// Checking inflows: 3000+5000+2000=10000; outflows: 1000+500=1500; net=8500

	// ============================================================
	// === SCENARIO 4: DATA TRANSFORMATION ===
	// ============================================================

	// DT1: INSERT INTO SELECT with transformation
	afExec(t, db, ctx, `CREATE TABLE v74_summary (
		dept TEXT, emp_count INTEGER, total_salary INTEGER, avg_review INTEGER
	)`)
	checkNoError("DT1 INSERT INTO SELECT",
		`INSERT INTO v74_summary
		 SELECT d.name, COUNT(*), SUM(e.salary), 0
		 FROM v74_employees e
		 JOIN v74_departments d ON e.dept_id = d.id
		 GROUP BY d.name`)
	check("DT1 verify count", "SELECT COUNT(*) FROM v74_summary", 4)
	check("DT1 eng salary", "SELECT total_salary FROM v74_summary WHERE dept = 'Engineering'", 570000)

	// DT2: UPDATE with subquery
	checkNoError("DT2 UPDATE reviews",
		`UPDATE v74_summary SET avg_review = (
			SELECT AVG(r.score)
			FROM v74_reviews r
			JOIN v74_employees e ON r.emp_id = e.id
			JOIN v74_departments d ON e.dept_id = d.id
			WHERE d.name = v74_summary.dept AND r.review_year = 2023
		)`)
	check("DT2 eng review", "SELECT avg_review FROM v74_summary WHERE dept = 'Engineering'", 92)

	// ============================================================
	// === SCENARIO 5: COMPLEX ANALYTICS ===
	// ============================================================

	// CA1: Employee tenure analysis
	check("CA1 avg tenure",
		"SELECT AVG(2024 - hire_year) FROM v74_employees", "4.9")

	// CA2: Salary percentile via CTE
	check("CA2 salary percentile",
		`WITH ranked AS (
			SELECT salary,
				   ROW_NUMBER() OVER (ORDER BY salary ASC) as rn,
				   COUNT(*) OVER () as total
			FROM v74_employees
		)
		SELECT salary FROM ranked WHERE rn = 5`, 110000)
	// Sorted ASC: 80k,85k,95k,105k,110k,120k,130k,140k,150k,160k → 5th = 110k

	// CA3: Department comparison
	checkRowCount("CA3 dept comparison",
		`SELECT d.name, COUNT(e.id), SUM(e.salary), AVG(e.salary)
		 FROM v74_departments d
		 JOIN v74_employees e ON d.id = e.dept_id
		 GROUP BY d.name
		 ORDER BY AVG(e.salary) DESC`, 4)

	// CA4: Multi-level CTE analysis
	check("CA4 multi-level CTE",
		`WITH emp_stats AS (
			SELECT dept_id, COUNT(*) as emp_count, AVG(salary) as avg_sal
			FROM v74_employees GROUP BY dept_id
		),
		dept_classification AS (
			SELECT dept_id, emp_count,
				   CASE
					WHEN avg_sal >= 130000 THEN 'high'
					WHEN avg_sal >= 100000 THEN 'medium'
					ELSE 'low'
				   END as pay_class
			FROM emp_stats
		)
		SELECT COUNT(*) FROM dept_classification WHERE pay_class = 'high'`, 1)
	// Only Engineering (avg ~142.5k) is 'high'

	// CA5: Top performer per department
	check("CA5 top performers",
		`WITH scored AS (
			SELECT e.name, e.dept_id, r.score,
				   ROW_NUMBER() OVER (PARTITION BY e.dept_id ORDER BY r.score DESC) as rn
			FROM v74_employees e
			JOIN v74_reviews r ON e.id = r.emp_id
			WHERE r.review_year = 2023
		)
		SELECT COUNT(*) FROM scored WHERE rn = 1`, 4)

	// CA6: Correlation between salary and review score
	check("CA6 high salary high score",
		`SELECT COUNT(*) FROM v74_employees e
		 JOIN v74_reviews r ON e.id = r.emp_id
		 WHERE r.review_year = 2023 AND e.salary > 100000 AND r.score > 85`, 5)
	// Salary >100k AND score >85: Alice(150k,95), Carol(140k,90), Dave(130k,88), Grace(105k,92), Jack(160k,98) = 5

	// ============================================================
	// === SCENARIO 6: FULL PIPELINE ===
	// ============================================================

	// FP1: Create report table from complex query
	afExec(t, db, ctx, `CREATE TABLE v74_report (
		category TEXT, total_revenue INTEGER, top_product TEXT
	)`)

	// FP2: Insert aggregated data - use simple subqueries
	checkNoError("FP2 insert electronics",
		`INSERT INTO v74_report VALUES ('electronics',
			(SELECT SUM(p.price * s.qty) FROM v74_sales s JOIN v74_products p ON s.product_id = p.id WHERE p.category = 'electronics'),
			'Phone'
		)`)
	checkNoError("FP2 insert furniture",
		`INSERT INTO v74_report VALUES ('furniture',
			(SELECT SUM(p.price * s.qty) FROM v74_sales s JOIN v74_products p ON s.product_id = p.id WHERE p.category = 'furniture'),
			'Chair'
		)`)
	checkNoError("FP2 insert accessories",
		`INSERT INTO v74_report VALUES ('accessories',
			(SELECT SUM(p.price * s.qty) FROM v74_sales s JOIN v74_products p ON s.product_id = p.id WHERE p.category = 'accessories'),
			'Keyboard'
		)`)

	check("FP2 report count", "SELECT COUNT(*) FROM v74_report", 3)
	check("FP2 top electronics",
		"SELECT top_product FROM v74_report WHERE category = 'electronics'", "Phone")

	// FP3: Full summary query (subqueries in INSERT VALUES now correctly evaluate)
	check("FP3 grand total",
		"SELECT SUM(total_revenue) FROM v74_report", 34485)

	// ============================================================
	// === EDGE CASE VALIDATION ===
	// ============================================================

	// EV1: Empty table operations
	afExec(t, db, ctx, `CREATE TABLE v74_empty (id INTEGER PRIMARY KEY, val INTEGER)`)
	check("EV1 COUNT empty", "SELECT COUNT(*) FROM v74_empty", 0)
	check("EV1 SUM empty", "SELECT SUM(val) FROM v74_empty", nil)

	// EV2: Single row operations
	afExec(t, db, ctx, `CREATE TABLE v74_single (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v74_single VALUES (1, 42)")
	check("EV2 single AVG", "SELECT AVG(val) FROM v74_single", 42)
	check("EV2 single COUNT", "SELECT COUNT(*) FROM v74_single", 1)

	// EV3: Verify transaction atomicity
	checkNoError("EV3 BEGIN", "BEGIN")
	checkNoError("EV3 DELETE all employees", "DELETE FROM v74_employees")
	check("EV3 mid-txn count", "SELECT COUNT(*) FROM v74_employees", 0)
	checkNoError("EV3 ROLLBACK", "ROLLBACK")
	check("EV3 post-rollback", "SELECT COUNT(*) FROM v74_employees", 10)

	// EV4: Complex view
	checkNoError("EV4 CREATE VIEW",
		`CREATE VIEW v74_emp_overview AS
		 SELECT e.name, d.name as dept, e.salary, e.level
		 FROM v74_employees e
		 JOIN v74_departments d ON e.dept_id = d.id`)
	check("EV4 view query", "SELECT COUNT(*) FROM v74_emp_overview", 10)
	check("EV4 view filter",
		"SELECT name FROM v74_emp_overview WHERE dept = 'Engineering' ORDER BY salary DESC LIMIT 1", "Jack")

	t.Logf("\n=== V74 FINAL VALIDATION: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
