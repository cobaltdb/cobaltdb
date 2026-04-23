package test

import (
	"fmt"
	"testing"
)

// ==================== V87: BUG-HUNTING DEEP EDGE CASES ====================
// Focuses on complex scenarios that might expose real bugs.

// ==================== COMPLEX NESTED SUBQUERIES ====================

func TestV87_ThreeLevelNestedSubquery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_users (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v87_depts (id INTEGER PRIMARY KEY, name TEXT, region TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v87_sales (user_id INTEGER, amount INTEGER)")

	afExec(t, db, ctx, "INSERT INTO v87_depts VALUES (1, 'eng', 'west')")
	afExec(t, db, ctx, "INSERT INTO v87_depts VALUES (2, 'hr', 'east')")
	afExec(t, db, ctx, "INSERT INTO v87_users VALUES (1, 'Alice', 1)")
	afExec(t, db, ctx, "INSERT INTO v87_users VALUES (2, 'Bob', 1)")
	afExec(t, db, ctx, "INSERT INTO v87_users VALUES (3, 'Carol', 2)")
	afExec(t, db, ctx, "INSERT INTO v87_sales VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO v87_sales VALUES (2, 200)")
	afExec(t, db, ctx, "INSERT INTO v87_sales VALUES (3, 50)")

	// Users in the department with the highest total sales
	rows := afQuery(t, db, ctx, `SELECT name FROM v87_users WHERE dept_id = (
		SELECT dept_id FROM (
			SELECT u.dept_id, SUM(s.amount) as total
			FROM v87_users u INNER JOIN v87_sales s ON u.id = s.user_id
			GROUP BY u.dept_id ORDER BY total DESC LIMIT 1
		) sub
	) ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (Alice, Bob in eng), got %d: %v", len(rows), rows)
	}
}

func TestV87_SubqueryInHaving(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_sh (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_sh VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO v87_sh VALUES ('a', 20)")
	afExec(t, db, ctx, "INSERT INTO v87_sh VALUES ('b', 50)")
	afExec(t, db, ctx, "INSERT INTO v87_sh VALUES ('c', 5)")

	// Categories where SUM > overall AVG
	rows := afQuery(t, db, ctx, `SELECT cat, SUM(val) FROM v87_sh GROUP BY cat
		HAVING SUM(val) > (SELECT AVG(val) FROM v87_sh) ORDER BY cat`)
	// Overall AVG = 85/4 = 21.25
	// a=30 > 21.25, b=50 > 21.25, c=5 < 21.25
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (a, b), got %d: %v", len(rows), rows)
	}
}

func TestV87_SubqueryInUpdateSET(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_sus (id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v87_sus_ref (max_val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_sus VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v87_sus VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v87_sus_ref VALUES (100)")

	afExec(t, db, ctx, "UPDATE v87_sus SET val = (SELECT max_val FROM v87_sus_ref) WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT val FROM v87_sus WHERE id = 1", float64(100))
}

// ==================== COMPLEX WHERE CLAUSES ====================

func TestV87_WhereBETWEENWithExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_bwe (id INTEGER, val INTEGER)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v87_bwe VALUES (%d, %d)", i, i*5))
	}

	// BETWEEN with computed bounds
	rows := afQuery(t, db, ctx, "SELECT id FROM v87_bwe WHERE val BETWEEN 10 + 5 AND 50 - 5 ORDER BY id")
	// val between 15 and 45: val=15(id=3), 20(id=4), 25(id=5), 30(id=6), 35(id=7), 40(id=8), 45(id=9)
	if len(rows) != 7 {
		t.Fatalf("expected 7 rows, got %d: %v", len(rows), rows)
	}
}

func TestV87_WhereINWithSubquery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_win_main (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v87_win_ids (id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_win_main VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v87_win_main VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO v87_win_main VALUES (3, 'Carol')")
	afExec(t, db, ctx, "INSERT INTO v87_win_ids VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO v87_win_ids VALUES (3)")

	rows := afQuery(t, db, ctx, "SELECT name FROM v87_win_main WHERE id IN (SELECT id FROM v87_win_ids) ORDER BY name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
}

func TestV87_WhereCASE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_wc (id INTEGER, status TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_wc VALUES (1, 'active', 100)")
	afExec(t, db, ctx, "INSERT INTO v87_wc VALUES (2, 'inactive', 200)")
	afExec(t, db, ctx, "INSERT INTO v87_wc VALUES (3, 'active', 300)")

	rows := afQuery(t, db, ctx, `SELECT id FROM v87_wc WHERE
		CASE WHEN status = 'active' THEN val ELSE 0 END > 150`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (id=3), got %d: %v", len(rows), rows)
	}
}

// ==================== LARGE DATASET OPERATIONS ====================

func TestV87_LargeInsertAndQuery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_large (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	for i := 0; i < 200; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v87_large VALUES (%d, 'cat%d', %d)", i, i%10, i*7%100))
	}

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_large", float64(200))

	// Aggregate over large set
	rows := afQuery(t, db, ctx, "SELECT cat, COUNT(*) as cnt, SUM(val) as total FROM v87_large GROUP BY cat ORDER BY cat")
	if len(rows) != 10 {
		t.Fatalf("expected 10 groups, got %d", len(rows))
	}

	// Filter + aggregate
	rows = afQuery(t, db, ctx, "SELECT cat, AVG(val) FROM v87_large WHERE val > 50 GROUP BY cat HAVING COUNT(*) > 3")
	t.Logf("Filtered groups: %d", len(rows))
}

func TestV87_LargeDeleteAndRecount(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_ldel (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 0; i < 100; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v87_ldel VALUES (%d, %d)", i, i))
	}

	afExec(t, db, ctx, "DELETE FROM v87_ldel WHERE val % 2 = 0")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_ldel", float64(50))

	// Remaining should all be odd
	rows := afQuery(t, db, ctx, "SELECT MIN(val), MAX(val) FROM v87_ldel")
	if fmt.Sprintf("%v", rows[0][0]) != "1" {
		t.Fatalf("expected min=1, got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[0][1]) != "99" {
		t.Fatalf("expected max=99, got %v", rows[0][1])
	}
}

func TestV87_LargeUpdateBatch(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_lupd (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 0; i < 100; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v87_lupd VALUES (%d, %d)", i, i))
	}

	afExec(t, db, ctx, "UPDATE v87_lupd SET val = val * 2 WHERE val > 50")
	rows := afQuery(t, db, ctx, "SELECT MAX(val) FROM v87_lupd")
	if fmt.Sprintf("%v", rows[0][0]) != "198" {
		t.Fatalf("expected max=198, got %v", rows[0][0])
	}
}

// ==================== TYPE COERCION EDGE CASES ====================

func TestV87_StringNumberComparison(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_snc (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v87_snc VALUES ('10')")
	afExec(t, db, ctx, "INSERT INTO v87_snc VALUES ('2')")
	afExec(t, db, ctx, "INSERT INTO v87_snc VALUES ('100')")

	// String comparison vs numeric comparison
	rows := afQuery(t, db, ctx, "SELECT val FROM v87_snc ORDER BY val ASC")
	t.Logf("String order: %v, %v, %v", rows[0][0], rows[1][0], rows[2][0])
}

func TestV87_MixedTypeArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Integer + float
	afExpectVal(t, db, ctx, "SELECT 5 + 3.14", float64(8.14))

	// Integer - float
	afExpectVal(t, db, ctx, "SELECT 10 - 0.5", float64(9.5))

	// Integer * float
	afExpectVal(t, db, ctx, "SELECT 3 * 2.5", float64(7.5))
}

func TestV87_NullArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Any arithmetic with NULL should return NULL
	rows := afQuery(t, db, ctx, "SELECT NULL + 5")
	if rows[0][0] != nil {
		t.Fatalf("expected NULL for NULL+5, got %v", rows[0][0])
	}

	rows = afQuery(t, db, ctx, "SELECT NULL * 10")
	if rows[0][0] != nil {
		t.Fatalf("expected NULL for NULL*10, got %v", rows[0][0])
	}

	rows = afQuery(t, db, ctx, "SELECT 100 / NULL")
	if rows[0][0] != nil {
		t.Fatalf("expected NULL for 100/NULL, got %v", rows[0][0])
	}
}

func TestV87_BooleanArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Boolean in arithmetic context
	rows := afQuery(t, db, ctx, "SELECT (1 = 1) + 0")
	t.Logf("(1=1)+0 = %v (%T)", rows[0][0], rows[0][0])
}

// ==================== COMPLEX JOIN + GROUP BY + HAVING ====================

func TestV87_SelfJoinGroupByHaving(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_emp (id INTEGER, name TEXT, manager_id INTEGER, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_emp VALUES (1, 'CEO', NULL, 200)")
	afExec(t, db, ctx, "INSERT INTO v87_emp VALUES (2, 'VP1', 1, 150)")
	afExec(t, db, ctx, "INSERT INTO v87_emp VALUES (3, 'VP2', 1, 160)")
	afExec(t, db, ctx, "INSERT INTO v87_emp VALUES (4, 'Mgr1', 2, 100)")
	afExec(t, db, ctx, "INSERT INTO v87_emp VALUES (5, 'Mgr2', 2, 110)")
	afExec(t, db, ctx, "INSERT INTO v87_emp VALUES (6, 'Mgr3', 3, 120)")

	// Managers with more than 1 direct report
	rows := afQuery(t, db, ctx, `SELECT m.name, COUNT(*) as reports
		FROM v87_emp e INNER JOIN v87_emp m ON e.manager_id = m.id
		GROUP BY m.name HAVING COUNT(*) > 1 ORDER BY reports DESC`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (CEO=2, VP1=2), got %d: %v", len(rows), rows)
	}
}

func TestV87_LeftJoinGroupByHavingCount(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_ljgh_cat (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v87_ljgh_item (id INTEGER, cat_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_ljgh_cat VALUES (1, 'electronics')")
	afExec(t, db, ctx, "INSERT INTO v87_ljgh_cat VALUES (2, 'books')")
	afExec(t, db, ctx, "INSERT INTO v87_ljgh_cat VALUES (3, 'empty')")
	afExec(t, db, ctx, "INSERT INTO v87_ljgh_item VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO v87_ljgh_item VALUES (2, 1)")
	afExec(t, db, ctx, "INSERT INTO v87_ljgh_item VALUES (3, 2)")

	rows := afQuery(t, db, ctx, `SELECT c.name, COUNT(i.id) as item_count
		FROM v87_ljgh_cat c LEFT JOIN v87_ljgh_item i ON c.id = i.cat_id
		GROUP BY c.name ORDER BY item_count DESC`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(rows), rows)
	}
}

// ==================== TRANSACTION + DDL INTERLEAVING ====================

func TestV87_TransactionDDLDML(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "CREATE TABLE v87_txn1 (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v87_txn1 VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v87_txn1 VALUES (2, 'b')")
	afExec(t, db, ctx, "CREATE TABLE v87_txn2 (id INTEGER, ref_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_txn2 VALUES (10, 1)")
	afExec(t, db, ctx, "COMMIT")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_txn1", float64(2))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_txn2", float64(1))
}

func TestV87_TransactionRollbackDDL(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_txr_persist (id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_txr_persist VALUES (1)")

	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "INSERT INTO v87_txr_persist VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO v87_txr_persist VALUES (3)")
	afExec(t, db, ctx, "ROLLBACK")

	// Only id=1 should remain
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_txr_persist", float64(1))
}

func TestV87_SavepointComplex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_sp (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "INSERT INTO v87_sp VALUES (1, 'keep1')")
	afExec(t, db, ctx, "SAVEPOINT sp1")
	afExec(t, db, ctx, "INSERT INTO v87_sp VALUES (2, 'lose1')")
	afExec(t, db, ctx, "SAVEPOINT sp2")
	afExec(t, db, ctx, "INSERT INTO v87_sp VALUES (3, 'lose2')")
	afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp2")
	// sp2 rolled back, sp1 still active
	afExec(t, db, ctx, "INSERT INTO v87_sp VALUES (4, 'keep2')")
	afExec(t, db, ctx, "RELEASE SAVEPOINT sp1")
	afExec(t, db, ctx, "INSERT INTO v87_sp VALUES (5, 'keep3')")
	afExec(t, db, ctx, "COMMIT")

	// Should have ids 1, 2, 4, 5 (3 was rolled back, 2 was in sp1 which was released)
	rows := afQuery(t, db, ctx, "SELECT id FROM v87_sp ORDER BY id")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d: %v", len(rows), rows)
	}
}

// ==================== WINDOW FUNCTIONS DEEP ====================

func TestV87_WindowSumPartition(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_wsp (dept TEXT, emp TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_wsp VALUES ('eng', 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO v87_wsp VALUES ('eng', 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO v87_wsp VALUES ('hr', 'Carol', 150)")
	afExec(t, db, ctx, "INSERT INTO v87_wsp VALUES ('hr', 'Dave', 180)")

	rows := afQuery(t, db, ctx, `SELECT emp, dept,
		SUM(salary) OVER (PARTITION BY dept) as dept_total
		FROM v87_wsp ORDER BY dept, emp`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestV87_WindowDenseRank(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_wdr (score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_wdr VALUES (100)")
	afExec(t, db, ctx, "INSERT INTO v87_wdr VALUES (90)")
	afExec(t, db, ctx, "INSERT INTO v87_wdr VALUES (90)")
	afExec(t, db, ctx, "INSERT INTO v87_wdr VALUES (80)")

	rows := afQuery(t, db, ctx, `SELECT score,
		RANK() OVER (ORDER BY score DESC) as rk,
		DENSE_RANK() OVER (ORDER BY score DESC) as drk
		FROM v87_wdr ORDER BY score DESC`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestV87_WindowLagDefault(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_wld (id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_wld VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v87_wld VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v87_wld VALUES (3, 30)")

	rows := afQuery(t, db, ctx, `SELECT id, val,
		LAG(val, 1) OVER (ORDER BY id) as prev,
		LEAD(val, 1) OVER (ORDER BY id) as next
		FROM v87_wld ORDER BY id`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// First row: prev should be NULL
	if rows[0][2] != nil {
		t.Fatalf("expected NULL for LAG at row 0, got %v", rows[0][2])
	}
	// Last row: next should be NULL
	if rows[2][3] != nil {
		t.Fatalf("expected NULL for LEAD at last row, got %v", rows[2][3])
	}
}

// ==================== CTE WITH SET OPERATIONS ====================

func TestV87_CTEWithUnion(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_ctu1 (val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v87_ctu2 (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_ctu1 VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO v87_ctu1 VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO v87_ctu2 VALUES (3)")

	rows := afQuery(t, db, ctx, `WITH combined AS (
		SELECT val FROM v87_ctu1 UNION ALL SELECT val FROM v87_ctu2
	) SELECT SUM(val) FROM combined`)
	if len(rows) == 0 {
		t.Fatalf("expected result from CTE UNION query")
	}
	t.Logf("CTE UNION SUM = %v", rows[0][0])
}

func TestV87_CTERecursiveWithFilter(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `WITH RECURSIVE nums(n) AS (
		SELECT 1
		UNION ALL
		SELECT n + 1 FROM nums WHERE n < 20
	) SELECT n FROM nums WHERE n % 3 = 0 ORDER BY n`)
	// Numbers divisible by 3 from 1 to 20: 3,6,9,12,15,18
	if len(rows) != 6 {
		t.Fatalf("expected 6 rows, got %d: %v", len(rows), rows)
	}
}

// ==================== TRIGGER CASCADES ====================

func TestV87_TriggerCascade(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_tc_a (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v87_tc_b (id INTEGER PRIMARY KEY, source TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v87_tc_log (msg TEXT)")

	// Trigger on A inserts into B
	afExec(t, db, ctx, `CREATE TRIGGER v87_tc_a_trig AFTER INSERT ON v87_tc_a
		FOR EACH ROW
		BEGIN
			INSERT INTO v87_tc_b VALUES (NEW.id, 'from_a');
		END`)

	// Trigger on B inserts into log
	afExec(t, db, ctx, `CREATE TRIGGER v87_tc_b_trig AFTER INSERT ON v87_tc_b
		FOR EACH ROW
		BEGIN
			INSERT INTO v87_tc_log VALUES ('cascade:' || CAST(NEW.id AS TEXT));
		END`)

	afExec(t, db, ctx, "INSERT INTO v87_tc_a VALUES (1, 'test')")

	// Should cascade: A insert → B insert → log insert
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_tc_b", float64(1))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_tc_log", float64(1))
	afExpectVal(t, db, ctx, "SELECT msg FROM v87_tc_log", "cascade:1")
}

func TestV87_TriggerWhenCascade(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_twc_orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v87_twc_premium (order_id INTEGER, note TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v87_twc_audit (msg TEXT)")

	// Trigger: large orders get flagged as premium
	afExec(t, db, ctx, `CREATE TRIGGER v87_twc_big AFTER INSERT ON v87_twc_orders
		FOR EACH ROW WHEN NEW.amount > 1000
		BEGIN
			INSERT INTO v87_twc_premium VALUES (NEW.id, 'premium_order');
		END`)

	// Trigger: any premium gets audited
	afExec(t, db, ctx, `CREATE TRIGGER v87_twc_audit AFTER INSERT ON v87_twc_premium
		FOR EACH ROW
		BEGIN
			INSERT INTO v87_twc_audit VALUES ('premium_added:' || CAST(NEW.order_id AS TEXT));
		END`)

	afExec(t, db, ctx, "INSERT INTO v87_twc_orders VALUES (1, 500, 'normal')") // too small
	afExec(t, db, ctx, "INSERT INTO v87_twc_orders VALUES (2, 2000, 'big')")   // cascade!
	afExec(t, db, ctx, "INSERT INTO v87_twc_orders VALUES (3, 100, 'tiny')")   // too small

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_twc_premium", float64(1))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_twc_audit", float64(1))
}

// ==================== FK EDGE CASES ====================

func TestV87_ForeignKeyRestrictDelete(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_fk_p (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v87_fk_c (id INTEGER, pid INTEGER, FOREIGN KEY (pid) REFERENCES v87_fk_p(id) ON DELETE RESTRICT)")

	afExec(t, db, ctx, "INSERT INTO v87_fk_p VALUES (1, 'parent1')")
	afExec(t, db, ctx, "INSERT INTO v87_fk_c VALUES (10, 1)")

	// RESTRICT should prevent delete
	_, err := db.Exec(ctx, "DELETE FROM v87_fk_p WHERE id = 1")
	if err == nil {
		t.Fatalf("expected error for RESTRICT delete with child rows")
	}

	// After removing child, delete should work
	afExec(t, db, ctx, "DELETE FROM v87_fk_c WHERE pid = 1")
	afExec(t, db, ctx, "DELETE FROM v87_fk_p WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_fk_p", float64(0))
}

func TestV87_ForeignKeyCascadeChain(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_fkcc_a (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "CREATE TABLE v87_fkcc_b (id INTEGER, a_id INTEGER, FOREIGN KEY (a_id) REFERENCES v87_fkcc_a(id) ON DELETE CASCADE)")
	afExec(t, db, ctx, "INSERT INTO v87_fkcc_a VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO v87_fkcc_a VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO v87_fkcc_b VALUES (10, 1)")
	afExec(t, db, ctx, "INSERT INTO v87_fkcc_b VALUES (11, 1)")
	afExec(t, db, ctx, "INSERT INTO v87_fkcc_b VALUES (12, 2)")

	afExec(t, db, ctx, "DELETE FROM v87_fkcc_a WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_fkcc_b", float64(1))
	afExpectVal(t, db, ctx, "SELECT id FROM v87_fkcc_b", float64(12))
}

// ==================== COMPLEX REAL-WORLD PATTERNS ====================

func TestV87_ECommerceReporting(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)")
	afExec(t, db, ctx, "CREATE TABLE v87_order_items (id INTEGER, product_id INTEGER, qty INTEGER)")

	afExec(t, db, ctx, "INSERT INTO v87_products VALUES (1, 'Laptop', 'electronics', 999.99)")
	afExec(t, db, ctx, "INSERT INTO v87_products VALUES (2, 'Phone', 'electronics', 599.99)")
	afExec(t, db, ctx, "INSERT INTO v87_products VALUES (3, 'Book', 'books', 29.99)")
	afExec(t, db, ctx, "INSERT INTO v87_products VALUES (4, 'Pen', 'office', 4.99)")

	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v87_order_items VALUES (%d, %d, %d)", i, (i%4)+1, (i%3)+1))
	}

	// Revenue by category
	rows := afQuery(t, db, ctx, `SELECT p.category, SUM(p.price * oi.qty) as revenue, COUNT(*) as orders
		FROM v87_products p INNER JOIN v87_order_items oi ON p.id = oi.product_id
		GROUP BY p.category ORDER BY revenue DESC`)
	if len(rows) == 0 {
		t.Fatalf("expected results from revenue query")
	}
	t.Logf("Categories: %d", len(rows))
}

func TestV87_InventoryManagement(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_inv (id INTEGER PRIMARY KEY, product TEXT, stock INTEGER, reorder_level INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_inv VALUES (1, 'Widget', 100, 20)")
	afExec(t, db, ctx, "INSERT INTO v87_inv VALUES (2, 'Gadget', 15, 20)")
	afExec(t, db, ctx, "INSERT INTO v87_inv VALUES (3, 'Doohickey', 50, 30)")

	// Items below reorder level
	rows := afQuery(t, db, ctx, "SELECT product FROM v87_inv WHERE stock < reorder_level")
	if len(rows) != 1 {
		t.Fatalf("expected 1 item below reorder (Gadget), got %d: %v", len(rows), rows)
	}

	// Update stock with trigger
	afExec(t, db, ctx, "CREATE TABLE v87_inv_log (msg TEXT)")
	afExec(t, db, ctx, `CREATE TRIGGER v87_inv_low AFTER UPDATE ON v87_inv
		FOR EACH ROW WHEN NEW.stock < NEW.reorder_level
		BEGIN
			INSERT INTO v87_inv_log VALUES ('LOW_STOCK:' || NEW.product);
		END`)

	afExec(t, db, ctx, "UPDATE v87_inv SET stock = stock - 40 WHERE product = 'Doohickey'")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_inv_log", float64(1))
}

func TestV87_UserRoles(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_roles (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v87_user_roles (user_id INTEGER, role_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v87_role_perms (role_id INTEGER, permission TEXT)")

	afExec(t, db, ctx, "INSERT INTO v87_roles VALUES (1, 'admin')")
	afExec(t, db, ctx, "INSERT INTO v87_roles VALUES (2, 'editor')")
	afExec(t, db, ctx, "INSERT INTO v87_roles VALUES (3, 'viewer')")
	afExec(t, db, ctx, "INSERT INTO v87_user_roles VALUES (100, 1)")
	afExec(t, db, ctx, "INSERT INTO v87_user_roles VALUES (100, 2)")
	afExec(t, db, ctx, "INSERT INTO v87_user_roles VALUES (200, 3)")
	afExec(t, db, ctx, "INSERT INTO v87_role_perms VALUES (1, 'delete')")
	afExec(t, db, ctx, "INSERT INTO v87_role_perms VALUES (1, 'write')")
	afExec(t, db, ctx, "INSERT INTO v87_role_perms VALUES (2, 'write')")
	afExec(t, db, ctx, "INSERT INTO v87_role_perms VALUES (3, 'read')")

	// All permissions for user 100
	rows := afQuery(t, db, ctx, `SELECT DISTINCT rp.permission
		FROM v87_user_roles ur
		INNER JOIN v87_role_perms rp ON ur.role_id = rp.role_id
		WHERE ur.user_id = 100 ORDER BY rp.permission`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 distinct permissions (delete, write), got %d: %v", len(rows), rows)
	}
}

// ==================== EMPTY TABLE / NULL EDGE CASES ====================

func TestV87_EmptyTableAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_empty (val INTEGER)")

	rows := afQuery(t, db, ctx, "SELECT COUNT(*) FROM v87_empty")
	if fmt.Sprintf("%v", rows[0][0]) != "0" {
		t.Fatalf("expected COUNT(*)=0, got %v", rows[0][0])
	}

	rows = afQuery(t, db, ctx, "SELECT SUM(val), AVG(val), MIN(val), MAX(val) FROM v87_empty")
	for i := 0; i < 4; i++ {
		if rows[0][i] != nil {
			t.Fatalf("expected NULL for agg[%d] on empty table, got %v", i, rows[0][i])
		}
	}
}

func TestV87_AllNullColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_nulls (id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_nulls VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO v87_nulls VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v87_nulls VALUES (3, NULL)")

	// SUM/AVG/MIN/MAX of all nulls
	rows := afQuery(t, db, ctx, "SELECT SUM(val), AVG(val), MIN(val), MAX(val) FROM v87_nulls")
	for i := 0; i < 4; i++ {
		if rows[0][i] != nil {
			t.Fatalf("expected NULL for agg[%d] on all-null column, got %v", i, rows[0][i])
		}
	}

	// COUNT(col) vs COUNT(*)
	afExpectVal(t, db, ctx, "SELECT COUNT(val) FROM v87_nulls", float64(0))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v87_nulls", float64(3))
}

func TestV87_NullInGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_ngb (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_ngb VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO v87_ngb VALUES ('a', 20)")
	afExec(t, db, ctx, "INSERT INTO v87_ngb VALUES (NULL, 30)")
	afExec(t, db, ctx, "INSERT INTO v87_ngb VALUES (NULL, 40)")
	afExec(t, db, ctx, "INSERT INTO v87_ngb VALUES ('b', 50)")

	// NULL should form its own group
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) FROM v87_ngb GROUP BY cat ORDER BY cat")
	if len(rows) != 3 {
		t.Fatalf("expected 3 groups (NULL, a, b), got %d: %v", len(rows), rows)
	}
}

// ==================== ORDER BY COMPLEX ====================

func TestV87_OrderByMultipleColumns(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_omc (cat TEXT, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_omc VALUES ('b', 'zoe', 10)")
	afExec(t, db, ctx, "INSERT INTO v87_omc VALUES ('a', 'alice', 30)")
	afExec(t, db, ctx, "INSERT INTO v87_omc VALUES ('b', 'adam', 20)")
	afExec(t, db, ctx, "INSERT INTO v87_omc VALUES ('a', 'bob', 40)")

	rows := afQuery(t, db, ctx, "SELECT cat, name FROM v87_omc ORDER BY cat ASC, name ASC")
	if fmt.Sprintf("%v", rows[0][1]) != "alice" {
		t.Fatalf("expected alice first, got %v", rows[0][1])
	}
}

func TestV87_OrderByDESCNullHandling(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_odn (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v87_odn VALUES (10)")
	afExec(t, db, ctx, "INSERT INTO v87_odn VALUES (NULL)")
	afExec(t, db, ctx, "INSERT INTO v87_odn VALUES (30)")
	afExec(t, db, ctx, "INSERT INTO v87_odn VALUES (NULL)")
	afExec(t, db, ctx, "INSERT INTO v87_odn VALUES (20)")

	// DESC: NULL should be first
	rows := afQuery(t, db, ctx, "SELECT val FROM v87_odn ORDER BY val DESC")
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
}

// ==================== EXPRESSION EVALUATION EDGE CASES ====================

func TestV87_DivisionByZero(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Division by zero should return NULL or error
	rows, err := db.Query(ctx, "SELECT 10 / 0")
	if err != nil {
		// Error is acceptable
		return
	}
	defer rows.Close()
	// If no error, check result
	t.Logf("10/0 result obtained without error")
}

func TestV87_ModuloByZero(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows, err := db.Query(ctx, "SELECT 10 % 0")
	if err != nil {
		return // Error is acceptable
	}
	defer rows.Close()
	t.Logf("10%%0 result obtained without error")
}

func TestV87_StringConcatTypes(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT 'hello' || ' ' || 'world'", "hello world")
	afExpectVal(t, db, ctx, "SELECT 'count=' || CAST(42 AS TEXT)", "count=42")
}

// ==================== ALTER TABLE EDGE CASES ====================

func TestV87_AlterAddDropColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_alt (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO v87_alt VALUES (1, 'test')")

	// Add column
	afExec(t, db, ctx, "ALTER TABLE v87_alt ADD COLUMN age INTEGER DEFAULT 25")
	rows := afQuery(t, db, ctx, "SELECT age FROM v87_alt WHERE id = 1")
	if fmt.Sprintf("%v", rows[0][0]) != "25" {
		t.Fatalf("expected default 25, got %v", rows[0][0])
	}

	// Drop column
	afExec(t, db, ctx, "ALTER TABLE v87_alt DROP COLUMN age")
	rows = afQuery(t, db, ctx, "SELECT * FROM v87_alt WHERE id = 1")
	if len(rows[0]) != 2 {
		t.Fatalf("expected 2 columns after drop, got %d", len(rows[0]))
	}
}

// ==================== COMPLEX INSERT PATTERNS ====================

func TestV87_InsertIntoSelectWithJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v87_isj_src (id INTEGER, cat_id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v87_isj_cat (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v87_isj_dst (cat_name TEXT, total INTEGER)")

	afExec(t, db, ctx, "INSERT INTO v87_isj_cat VALUES (1, 'electronics')")
	afExec(t, db, ctx, "INSERT INTO v87_isj_cat VALUES (2, 'books')")
	afExec(t, db, ctx, "INSERT INTO v87_isj_src VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO v87_isj_src VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO v87_isj_src VALUES (3, 2, 50)")

	afExec(t, db, ctx, `INSERT INTO v87_isj_dst
		SELECT c.name, SUM(s.val)
		FROM v87_isj_src s INNER JOIN v87_isj_cat c ON s.cat_id = c.id
		GROUP BY c.name`)
	rows := afQuery(t, db, ctx, "SELECT * FROM v87_isj_dst ORDER BY cat_name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}
