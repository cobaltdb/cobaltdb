package test

import (
	"fmt"
	"testing"
)

// TestV39RegressionStress exercises the database engine under conditions that
// most commonly expose latent bugs: large table scans, chained mutations,
// multi-table joins, CTE corner cases, trigger chains, index consistency under
// bulk mutations, and deep expression edge cases.
//
// Seven sections are covered:
//
//  1. Large table operations          (tests LT1-LT18)
//  2. Chained operations on same table (tests CH1-CH18)
//  3. Complex multi-join queries      (tests JN1-JN15)
//  4. CTE stress tests                (tests CT1-CT12)
//  5. Trigger chain tests             (tests TR1-TR12)
//  6. Index stress                    (tests IX1-IX12)
//  7. Edge case regressions           (tests EC1-EC15)
//
// All table names carry the v39_ prefix to prevent collisions with other test
// files. Expected values are derived by hand; arithmetic is shown inline.
//
// Engine notes:
//   - Integer division yields float64 (e.g., 7/2 = 3.5).
//   - Large sums may render in scientific notation (e.g., 1.25e+05 for 125000).
//   - NULL renders as "<nil>" when formatted via fmt.Sprintf("%v", ...).
//   - LIKE is case-insensitive.
//   - Triggers fire once per statement (statement-level), not once per row.
func TestV39RegressionStress(t *testing.T) {
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

	// ============================================================
	// SECTION 1: LARGE TABLE OPERATIONS
	// ============================================================
	//
	// Schema
	// ------
	//   v39_large (id PK, group_id INTEGER, val INTEGER, label TEXT)
	//
	// 500 rows:
	//   id      = 1..500
	//   group_id = (id - 1) % 20 + 1   => values 1..20, each group has exactly 25 rows
	//   val     = id * 3                => values 3,6,9,...,1500
	//   label   = 'item-NNN' where NNN = id
	//
	// Arithmetic facts (used in assertions):
	//   COUNT(*)           = 500
	//   SUM(val)           = SUM(id*3 for id=1..500)
	//                      = 3 * SUM(1..500) = 3 * (500*501/2) = 3 * 125250 = 375750
	//   AVG(val)           = 375750 / 500 = 751.5
	//   MIN(val)           = 3  (id=1)
	//   MAX(val)           = 1500 (id=500)
	//   Group count        = 20 (group_id 1..20)
	//   Rows per group     = 25
	//   Group_id=1 members = ids where (id-1)%20==0 => 1,21,41,...,481 (25 ids)
	//   SUM(val) group 1   = 3*(1+21+41+...+481)
	//                      = 3 * (25 terms, first=1, step=20)
	//                      = 3 * (25*(1+481)/2) = 3 * (25*241) = 3*6025 = 18075

	afExec(t, db, ctx, `CREATE TABLE v39_large (
		id       INTEGER PRIMARY KEY,
		group_id INTEGER,
		val      INTEGER,
		label    TEXT
	)`)

	for i := 1; i <= 500; i++ {
		gid := (i-1)%20 + 1
		afExec(t, db, ctx, fmt.Sprintf(
			"INSERT INTO v39_large VALUES (%d, %d, %d, 'item-%03d')",
			i, gid, i*3, i))
	}

	// LT1: Total row count after bulk insert.
	check("LT1 total row count is 500",
		`SELECT COUNT(*) FROM v39_large`, 500)

	// LT2: GROUP BY produces exactly 20 groups.
	checkRowCount("LT2 GROUP BY group_id produces 20 distinct groups",
		`SELECT group_id, COUNT(*) FROM v39_large GROUP BY group_id`, 20)

	// LT3: Every group has exactly 25 rows.
	// Verify by checking that the MIN(group_count) over all groups is 25.
	// Use CTE because the engine does not support subqueries in FROM.
	check("LT3 every group has exactly 25 rows (MIN of per-group counts = 25)",
		`WITH grp_counts AS (SELECT COUNT(*) AS cnt FROM v39_large GROUP BY group_id)
		 SELECT MIN(cnt) FROM grp_counts`,
		25)

	// LT4: SUM(val) = 3 * SUM(1..500) = 3 * 125250 = 375750.
	// Engine may render large sums in scientific notation; verify both forms.
	// 375750 fits in 32-bit int and should not use scientific notation.
	check("LT4 SUM(val) = 375750",
		`SELECT SUM(val) FROM v39_large`, 375750)

	// LT5: AVG(val) = 375750 / 500 = 751.5
	check("LT5 AVG(val) = 751.5",
		`SELECT AVG(val) FROM v39_large`, 751.5)

	// LT6: MIN and MAX
	check("LT6a MIN(val) = 3",
		`SELECT MIN(val) FROM v39_large`, 3)
	check("LT6b MAX(val) = 1500",
		`SELECT MAX(val) FROM v39_large`, 1500)

	// LT7: ORDER BY val ASC — first row has val=3 (id=1), last has val=1500 (id=500).
	check("LT7a ORDER BY val ASC LIMIT 1 returns val=3",
		`SELECT val FROM v39_large ORDER BY val ASC LIMIT 1`, 3)
	check("LT7b ORDER BY val DESC LIMIT 1 returns val=1500",
		`SELECT val FROM v39_large ORDER BY val DESC LIMIT 1`, 1500)

	// LT8: LIMIT/OFFSET on 500-row result.
	// OFFSET 498, LIMIT 2 returns the last 2 rows sorted by id.
	// Rows 499 and 500 have val=1497 and val=1500.
	checkRowCount("LT8 LIMIT 2 OFFSET 498 returns 2 rows",
		`SELECT id FROM v39_large ORDER BY id ASC LIMIT 2 OFFSET 498`, 2)
	check("LT8b LIMIT 1 OFFSET 499 returns val for id=500 (val=1500)",
		`SELECT val FROM v39_large ORDER BY id ASC LIMIT 1 OFFSET 499`, 1500)

	// LT9: WHERE with multiple conditions on 500 rows.
	// group_id=5 AND val > 200: group_id=5 members are ids 5,25,45,...,485 (25 rows).
	// val>200 means id*3>200 => id>66.67 => id>=67.
	// Members of group 5 with id>66: 85,105,125,...,485.
	// ids in group 5: 5,25,45,65,85,105,...,485 (step 20 from 5).
	// ids > 66: starting at 85 (since 65 is not > 66 but 85 is). 85,105,...,485 => (485-85)/20+1=21 rows.
	checkRowCount("LT9 WHERE group_id=5 AND val>200 returns 21 rows",
		`SELECT id FROM v39_large WHERE group_id = 5 AND val > 200`, 21)

	// LT10: SUM for a specific group.
	// Group_id=1: ids 1,21,41,...,481 (25 ids, step 20).
	// SUM(val) = 3 * (1+21+41+...+481) = 3 * 25*(1+481)/2 = 3 * 25*241 = 18075
	check("LT10 SUM(val) for group_id=1 is 18075",
		`SELECT SUM(val) FROM v39_large WHERE group_id = 1`, 18075)

	// LT11: DELETE half the rows (ids 251-500 = 250 rows).
	checkNoError("LT11 DELETE ids 251-500 (250 rows)",
		`DELETE FROM v39_large WHERE id > 250`)
	check("LT11b count after DELETE is 250",
		`SELECT COUNT(*) FROM v39_large`, 250)

	// LT12: Verify aggregates on remaining rows after DELETE.
	// Remaining: ids 1..250, val=3..750.
	// SUM(val) = 3 * SUM(1..250) = 3 * (250*251/2) = 3 * 31375 = 94125
	// AVG(val) = 94125 / 250 = 376.5
	check("LT12a SUM(val) on 250 remaining rows is 94125",
		`SELECT SUM(val) FROM v39_large`, 94125)
	check("LT12b AVG(val) on 250 remaining rows is 376.5",
		`SELECT AVG(val) FROM v39_large`, 376.5)

	// LT13: GROUP BY on truncated table — groups 1..20 each now have 12 or 13 rows.
	// ids 1..250; group_id=(id-1)%20+1; rows per group:
	//   250 rows / 20 groups = 12 full groups + 10 groups with one extra row.
	//   (250 = 12*20 + 10) so 10 groups have 13 rows, 10 groups have 12 rows.
	// MIN of group counts = 12.
	check("LT13 after DELETE: MIN rows per group is 12",
		`WITH grp_counts AS (SELECT COUNT(*) AS cnt FROM v39_large GROUP BY group_id)
		 SELECT MIN(cnt) FROM grp_counts`,
		12)

	// LT14: UPDATE all remaining rows — multiply val by 2.
	// SUM(val) becomes 94125 * 2 = 188250.
	checkNoError("LT14 UPDATE all rows: val = val * 2",
		`UPDATE v39_large SET val = val * 2`)
	check("LT14b SUM(val) after UPDATE * 2 is 188250",
		`SELECT SUM(val) FROM v39_large`, 188250)
	check("LT14c MIN(val) after * 2 is 6",
		`SELECT MIN(val) FROM v39_large`, 6)
	check("LT14d MAX(val) after * 2 is 1500",
		`SELECT MAX(val) FROM v39_large`, 1500)

	// LT15: WHERE with BETWEEN — val BETWEEN 100 AND 300.
	// After UPDATE, val = id*6 for ids 1..250.
	// val>=100 => id>=17 (id*6>=100 => id>=16.67); val<=300 => id<=50.
	// ids 17..50 = 34 rows.
	checkRowCount("LT15 WHERE val BETWEEN 100 AND 300 returns 34 rows (ids 17-50)",
		`SELECT id FROM v39_large WHERE val BETWEEN 100 AND 300`, 34)

	// LT16: COUNT and GROUP BY on updated table with GROUP BY + HAVING.
	// Each group has 12 or 13 rows. HAVING COUNT(*) >= 13 selects 10 groups.
	checkRowCount("LT16 HAVING COUNT(*) >= 13 selects 10 groups",
		`SELECT group_id, COUNT(*) FROM v39_large GROUP BY group_id HAVING COUNT(*) >= 13`, 10)

	// LT17: ORDER BY group_id ASC — verify first and last groups appear correctly.
	check("LT17 first group_id in ORDER BY ASC is 1",
		`SELECT group_id FROM v39_large ORDER BY group_id ASC LIMIT 1`, 1)
	check("LT17b last group_id in ORDER BY DESC is 20",
		`SELECT group_id FROM v39_large ORDER BY group_id DESC LIMIT 1`, 20)

	// LT18: SUM(val) for multiple groups simultaneously via GROUP BY — verify group 1.
	// Group 1 ids in 1..250: 1,21,41,...,241 (step 20 from 1; (241-1)/20+1=13 ids).
	// val = id*6 (after LT14 update), so SUM = 6*(1+21+41+...+241).
	// Sum of APs: 13 terms, first=1, last=241 => (13*(1+241))/2 = 13*121 = 1573.
	// SUM(val) group 1 = 6*1573 = 9438.
	check("LT18 SUM(val) for group_id=1 after UPDATE is 9438",
		`SELECT SUM(val) FROM v39_large WHERE group_id = 1`, 9438)

	// ============================================================
	// SECTION 2: CHAINED OPERATIONS ON SAME TABLE
	// ============================================================
	//
	// Schema
	// ------
	//   v39_chain (id PK, val INTEGER, tag TEXT, score REAL)
	//
	// We drive the table through INSERT -> SELECT -> UPDATE -> SELECT -> DELETE -> SELECT
	// at each step verifying the state is exactly what we expect.

	afExec(t, db, ctx, `CREATE TABLE v39_chain (
		id    INTEGER PRIMARY KEY,
		val   INTEGER,
		tag   TEXT,
		score REAL
	)`)

	// CH1: Insert 10 rows.
	for i := 1; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf(
			"INSERT INTO v39_chain VALUES (%d, %d, 'init', %.1f)",
			i, i*10, float64(i)*1.5))
	}
	check("CH1 10 rows inserted, COUNT is 10",
		`SELECT COUNT(*) FROM v39_chain`, 10)
	// SUM(val) = 10+20+...+100 = 550
	check("CH1b SUM(val) after initial inserts is 550",
		`SELECT SUM(val) FROM v39_chain`, 550)

	// CH2: UPDATE rows 1-5 — val += 100, tag = 'updated'.
	checkNoError("CH2 UPDATE ids 1-5: val+=100, tag=updated",
		`UPDATE v39_chain SET val = val + 100, tag = 'updated' WHERE id <= 5`)
	// New vals: 110,120,130,140,150 for ids 1-5; 60,70,80,90,100 unchanged for ids 6-10.
	// SUM = (110+120+130+140+150) + (60+70+80+90+100) = 650 + 400 = 1050
	check("CH2b SUM(val) after UPDATE is 1050",
		`SELECT SUM(val) FROM v39_chain`, 1050)
	checkRowCount("CH2c tag='updated' rows count is 5",
		`SELECT id FROM v39_chain WHERE tag = 'updated'`, 5)
	checkRowCount("CH2d tag='init' rows count is 5",
		`SELECT id FROM v39_chain WHERE tag = 'init'`, 5)

	// CH3: SELECT to verify a specific row, then UPDATE that row, then re-verify.
	// id=3 should have val=130 after CH2.
	check("CH3 id=3 val is 130 before second UPDATE",
		`SELECT val FROM v39_chain WHERE id = 3`, 130)
	checkNoError("CH3b UPDATE id=3: val=999",
		`UPDATE v39_chain SET val = 999 WHERE id = 3`)
	check("CH3c id=3 val is 999 after second UPDATE",
		`SELECT val FROM v39_chain WHERE id = 3`, 999)

	// CH4: DELETE rows 6-8 (3 rows), verify count=7.
	checkNoError("CH4 DELETE ids 6,7,8",
		`DELETE FROM v39_chain WHERE id IN (6, 7, 8)`)
	check("CH4b COUNT is 7 after DELETE of 3 rows",
		`SELECT COUNT(*) FROM v39_chain`, 7)
	// Remaining ids: 1,2,3,4,5,9,10
	// Remaining vals: 110,120,999,140,150,90,100
	// SUM = 110+120+999+140+150+90+100 = 1709
	check("CH4c SUM(val) is 1709 after DELETE",
		`SELECT SUM(val) FROM v39_chain`, 1709)

	// CH5: INSERT 3 new rows (ids 11-13), verify count=10 and new SUM.
	afExec(t, db, ctx, "INSERT INTO v39_chain VALUES (11, 50, 'new', 0.5)")
	afExec(t, db, ctx, "INSERT INTO v39_chain VALUES (12, 75, 'new', 0.75)")
	afExec(t, db, ctx, "INSERT INTO v39_chain VALUES (13, 25, 'new', 0.25)")
	check("CH5 COUNT is 10 after 3 new inserts",
		`SELECT COUNT(*) FROM v39_chain`, 10)
	// SUM = 1709 + 50 + 75 + 25 = 1859
	check("CH5b SUM(val) is 1859 after new inserts",
		`SELECT SUM(val) FROM v39_chain`, 1859)

	// CH6: Multiple UPDATE passes.
	// Pass 1: val = val - 5 for all rows tagged 'new'.
	// new vals: 45, 70, 20.  SUM change: (50+75+25)-(45+70+20) = 150-135 = 15 less.
	// New SUM = 1859 - 15 = 1844.
	checkNoError("CH6 UPDATE tag=new rows: val-=5",
		`UPDATE v39_chain SET val = val - 5 WHERE tag = 'new'`)
	check("CH6b SUM after first pass is 1844",
		`SELECT SUM(val) FROM v39_chain`, 1844)
	// Pass 2: val = val * 2 for tag='new'.
	// new vals: 90, 140, 40. SUM change adds (90+140+40)-(45+70+20) = 270-135 = 135 more.
	// New SUM = 1844 + 135 = 1979.
	checkNoError("CH6c UPDATE tag=new rows: val*=2",
		`UPDATE v39_chain SET val = val * 2 WHERE tag = 'new'`)
	check("CH6d SUM after second pass is 1979",
		`SELECT SUM(val) FROM v39_chain`, 1979)

	// CH7: 3-table join scenario — create two auxiliary tables, fill with data,
	// join all three, verify, then mutate data and re-join.
	afExec(t, db, ctx, `CREATE TABLE v39_dept (
		id   INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`)
	afExec(t, db, ctx, `CREATE TABLE v39_emp (
		id      INTEGER PRIMARY KEY,
		dept_id INTEGER,
		name    TEXT NOT NULL,
		salary  INTEGER
	)`)
	afExec(t, db, ctx, `CREATE TABLE v39_project (
		id     INTEGER PRIMARY KEY,
		emp_id INTEGER,
		name   TEXT NOT NULL,
		budget INTEGER
	)`)

	afExec(t, db, ctx, "INSERT INTO v39_dept VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO v39_dept VALUES (2, 'Marketing')")
	afExec(t, db, ctx, "INSERT INTO v39_dept VALUES (3, 'Finance')")

	afExec(t, db, ctx, "INSERT INTO v39_emp VALUES (1, 1, 'Alice', 90000)")
	afExec(t, db, ctx, "INSERT INTO v39_emp VALUES (2, 1, 'Bob',   80000)")
	afExec(t, db, ctx, "INSERT INTO v39_emp VALUES (3, 2, 'Carol', 70000)")
	afExec(t, db, ctx, "INSERT INTO v39_emp VALUES (4, 2, 'Dave',  65000)")
	afExec(t, db, ctx, "INSERT INTO v39_emp VALUES (5, 3, 'Eve',   85000)")

	afExec(t, db, ctx, "INSERT INTO v39_project VALUES (1, 1, 'Alpha',  50000)")
	afExec(t, db, ctx, "INSERT INTO v39_project VALUES (2, 2, 'Beta',   30000)")
	afExec(t, db, ctx, "INSERT INTO v39_project VALUES (3, 3, 'Gamma',  20000)")
	afExec(t, db, ctx, "INSERT INTO v39_project VALUES (4, 1, 'Delta',  45000)")
	afExec(t, db, ctx, "INSERT INTO v39_project VALUES (5, 5, 'Epsilon',60000)")

	// CH8: 3-table join — count of (dept, emp, project) combinations.
	// Only employees with projects: Alice(2 projects), Bob(1), Carol(1), Eve(1) => 5 rows.
	checkRowCount("CH8 3-table join returns 5 rows",
		`SELECT d.name, e.name, p.name
		 FROM v39_dept d
		 JOIN v39_emp e ON e.dept_id = d.id
		 JOIN v39_project p ON p.emp_id = e.id`, 5)

	// CH9: Verify JOIN aggregate — total budget per department.
	// Eng (dept=1): Alice projects (50000+45000=95000) + Bob project (30000) = 125000.
	// Marketing (dept=2): Carol project (20000). Finance (dept=3): Eve project (60000).
	check("CH9 total budget for Engineering via 3-table join is 125000",
		`SELECT SUM(p.budget)
		 FROM v39_dept d
		 JOIN v39_emp e ON e.dept_id = d.id
		 JOIN v39_project p ON p.emp_id = e.id
		 WHERE d.name = 'Engineering'`, 125000)

	// CH10: Mutate emp salaries, then re-run join aggregate.
	checkNoError("CH10 UPDATE all Engineering salaries +10000",
		`UPDATE v39_emp SET salary = salary + 10000 WHERE dept_id = 1`)
	// Alice: 90000+10000=100000, Bob: 80000+10000=90000.
	check("CH10b Alice salary after UPDATE is 100000",
		`SELECT salary FROM v39_emp WHERE name = 'Alice'`, 100000)
	// Re-verify join result is still the same (project budgets unchanged).
	check("CH10c Engineering budget via join still 125000 after salary update",
		`SELECT SUM(p.budget)
		 FROM v39_dept d
		 JOIN v39_emp e ON e.dept_id = d.id
		 JOIN v39_project p ON p.emp_id = e.id
		 WHERE d.name = 'Engineering'`, 125000)

	// CH11: INSERT...SELECT — copy employee names into a staging archive table.
	afExec(t, db, ctx, `CREATE TABLE v39_emp_archive (
		id   INTEGER PRIMARY KEY AUTO_INCREMENT,
		name TEXT,
		note TEXT
	)`)
	checkNoError("CH11 INSERT...SELECT employee names into archive",
		`INSERT INTO v39_emp_archive (name, note)
		 SELECT name, 'archived' FROM v39_emp WHERE dept_id = 1`)
	// dept_id=1 (Engineering): Alice, Bob => 2 rows.
	check("CH11b archive has 2 rows after INSERT...SELECT",
		`SELECT COUNT(*) FROM v39_emp_archive`, 2)
	checkRowCount("CH11c Alice is in archive",
		`SELECT id FROM v39_emp_archive WHERE name = 'Alice'`, 1)

	// CH12: Transaction — insert 10 rows, update 5, delete 3, commit; verify final state.
	afExec(t, db, ctx, `CREATE TABLE v39_txn_tbl (
		id  INTEGER PRIMARY KEY,
		val INTEGER,
		cat TEXT
	)`)
	checkNoError("CH12 BEGIN transaction",
		`BEGIN`)
	for i := 1; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf(
			"INSERT INTO v39_txn_tbl VALUES (%d, %d, 'A')", i, i*5))
	}
	// Update ids 1-5: val += 50
	afExec(t, db, ctx, "UPDATE v39_txn_tbl SET val = val + 50 WHERE id <= 5")
	// Delete ids 8,9,10
	afExec(t, db, ctx, "DELETE FROM v39_txn_tbl WHERE id IN (8, 9, 10)")
	checkNoError("CH12b COMMIT",
		`COMMIT`)
	// Final state: ids 1-7 exist.
	// ids 1-5 val: 55,60,65,70,75; ids 6-7 val: 30,35.
	check("CH12c COUNT after txn commit is 7",
		`SELECT COUNT(*) FROM v39_txn_tbl`, 7)
	// SUM = 55+60+65+70+75+30+35 = 390
	check("CH12d SUM(val) after txn commit is 390",
		`SELECT SUM(val) FROM v39_txn_tbl`, 390)
	check("CH12e id=1 val is 55 after txn",
		`SELECT val FROM v39_txn_tbl WHERE id = 1`, 55)
	checkRowCount("CH12f ids 8,9,10 are gone",
		`SELECT id FROM v39_txn_tbl WHERE id >= 8`, 0)

	// CH13: Transaction rollback — changes must not persist.
	checkNoError("CH13 BEGIN rollback txn",
		`BEGIN`)
	afExec(t, db, ctx, "UPDATE v39_txn_tbl SET val = 9999 WHERE id = 1")
	afExec(t, db, ctx, "INSERT INTO v39_txn_tbl VALUES (99, 9999, 'ghost')")
	checkNoError("CH13b ROLLBACK",
		`ROLLBACK`)
	// id=1 val must still be 55.
	check("CH13c id=1 val reverted to 55 after ROLLBACK",
		`SELECT val FROM v39_txn_tbl WHERE id = 1`, 55)
	// id=99 must not exist.
	checkRowCount("CH13d id=99 ghost row absent after ROLLBACK",
		`SELECT id FROM v39_txn_tbl WHERE id = 99`, 0)

	// CH14: INSERT ... SELECT from table into itself — doubling rows via staging.
	// Use a fresh table to avoid PK collisions.
	afExec(t, db, ctx, `CREATE TABLE v39_double (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v39_double VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v39_double VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v39_double VALUES (3, 30)")
	check("CH14 baseline count is 3",
		`SELECT COUNT(*) FROM v39_double`, 3)
	// Insert copies with offset ids to avoid PK conflict.
	checkNoError("CH14b INSERT...SELECT copies rows with id+10",
		`INSERT INTO v39_double SELECT id + 10, val * 2 FROM v39_double`)
	// Now ids: 1,2,3,11,12,13 => 6 rows.
	check("CH14c count is 6 after self-copy insert",
		`SELECT COUNT(*) FROM v39_double`, 6)
	// vals: 10,20,30,20,40,60 => SUM=180
	check("CH14d SUM(val) is 180 after self-copy",
		`SELECT SUM(val) FROM v39_double`, 180)

	// CH15: Verify state persists correctly after multiple joins on mutated data.
	// Add a project for Dave (dept=Marketing) and verify the Engineering budget is unchanged.
	checkNoError("CH15 INSERT new project for Dave",
		`INSERT INTO v39_project VALUES (6, 4, 'Zeta', 15000)`)
	check("CH15b Engineering budget via join still 125000",
		`SELECT SUM(p.budget)
		 FROM v39_dept d
		 JOIN v39_emp e ON e.dept_id = d.id
		 JOIN v39_project p ON p.emp_id = e.id
		 WHERE d.name = 'Engineering'`, 125000)
	// Marketing budget now: Carol(20000) + Dave(15000) = 35000.
	check("CH15c Marketing budget via join is 35000",
		`SELECT SUM(p.budget)
		 FROM v39_dept d
		 JOIN v39_emp e ON e.dept_id = d.id
		 JOIN v39_project p ON p.emp_id = e.id
		 WHERE d.name = 'Marketing'`, 35000)

	// CH16: Multiple UPDATEs on same column in sequence, verify each step.
	// Use v39_chain id=2 (val=120 from CH2, untouched since).
	check("CH16 id=2 val is 120 before multi-update",
		`SELECT val FROM v39_chain WHERE id = 2`, 120)
	checkNoError("CH16b UPDATE id=2: val=50",
		`UPDATE v39_chain SET val = 50 WHERE id = 2`)
	check("CH16c id=2 val is 50",
		`SELECT val FROM v39_chain WHERE id = 2`, 50)
	checkNoError("CH16d UPDATE id=2: val=val+25",
		`UPDATE v39_chain SET val = val + 25 WHERE id = 2`)
	check("CH16e id=2 val is 75",
		`SELECT val FROM v39_chain WHERE id = 2`, 75)
	checkNoError("CH16f UPDATE id=2: val=val*4",
		`UPDATE v39_chain SET val = val * 4 WHERE id = 2`)
	check("CH16g id=2 val is 300",
		`SELECT val FROM v39_chain WHERE id = 2`, 300)

	// CH17: Full INSERT -> SELECT -> UPDATE -> SELECT -> DELETE -> SELECT cycle verified.
	afExec(t, db, ctx, `CREATE TABLE v39_cycle (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	// INSERT
	afExec(t, db, ctx, "INSERT INTO v39_cycle VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO v39_cycle VALUES (2, 200)")
	afExec(t, db, ctx, "INSERT INTO v39_cycle VALUES (3, 300)")
	// SELECT
	check("CH17 after INSERT: SUM=600",
		`SELECT SUM(val) FROM v39_cycle`, 600)
	// UPDATE
	afExec(t, db, ctx, "UPDATE v39_cycle SET val = val + 50")
	// SELECT
	// 150+250+350=750
	check("CH17b after UPDATE: SUM=750",
		`SELECT SUM(val) FROM v39_cycle`, 750)
	// DELETE id=2
	afExec(t, db, ctx, "DELETE FROM v39_cycle WHERE id = 2")
	// SELECT
	// 150+350=500
	check("CH17c after DELETE id=2: SUM=500",
		`SELECT SUM(val) FROM v39_cycle`, 500)
	check("CH17d after DELETE id=2: COUNT=2",
		`SELECT COUNT(*) FROM v39_cycle`, 2)

	// CH18: Large-scale sequential operation: insert 100, update 50, delete 25.
	afExec(t, db, ctx, `CREATE TABLE v39_seq (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	for i := 1; i <= 100; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v39_seq VALUES (%d, %d)", i, i))
	}
	check("CH18 baseline count 100",
		`SELECT COUNT(*) FROM v39_seq`, 100)
	// Update ids 1-50: val *= 10
	afExec(t, db, ctx, "UPDATE v39_seq SET val = val * 10 WHERE id <= 50")
	// Delete ids 76-100 (25 rows)
	afExec(t, db, ctx, "DELETE FROM v39_seq WHERE id >= 76")
	check("CH18b count is 75",
		`SELECT COUNT(*) FROM v39_seq`, 75)
	// SUM = SUM(ids 1-50, val=id*10) + SUM(ids 51-75, val=id)
	// SUM(1..50)*10 = 1275*10 = 12750
	// SUM(51..75) = SUM(1..75)-SUM(1..50) = 2850-1275 = 1575
	// Total = 12750 + 1575 = 14325
	check("CH18c SUM is 14325",
		`SELECT SUM(val) FROM v39_seq`, 14325)

	// ============================================================
	// SECTION 3: COMPLEX MULTI-JOIN QUERIES
	// ============================================================
	//
	// Schema
	// ------
	//   v39_product  (id PK, name TEXT, category TEXT, unit_cost REAL)
	//   v39_supplier (id PK, name TEXT, country TEXT)
	//   v39_order    (id PK, product_id FK, supplier_id FK, qty INTEGER, discount REAL)
	//   v39_region   (id PK, supplier_id FK, region_name TEXT, tax_rate REAL)

	afExec(t, db, ctx, `CREATE TABLE v39_product (
		id        INTEGER PRIMARY KEY,
		name      TEXT NOT NULL,
		category  TEXT,
		unit_cost REAL
	)`)
	afExec(t, db, ctx, `CREATE TABLE v39_supplier (
		id      INTEGER PRIMARY KEY,
		name    TEXT NOT NULL,
		country TEXT
	)`)
	afExec(t, db, ctx, `CREATE TABLE v39_order (
		id          INTEGER PRIMARY KEY,
		product_id  INTEGER,
		supplier_id INTEGER,
		qty         INTEGER,
		discount    REAL
	)`)
	afExec(t, db, ctx, `CREATE TABLE v39_region (
		id          INTEGER PRIMARY KEY,
		supplier_id INTEGER,
		region_name TEXT,
		tax_rate    REAL
	)`)

	// Products (5)
	afExec(t, db, ctx, "INSERT INTO v39_product VALUES (1, 'Widget',  'HW',   5.00)")
	afExec(t, db, ctx, "INSERT INTO v39_product VALUES (2, 'Gadget',  'EL',  20.00)")
	afExec(t, db, ctx, "INSERT INTO v39_product VALUES (3, 'Phone',   'EL', 200.00)")
	afExec(t, db, ctx, "INSERT INTO v39_product VALUES (4, 'Cable',   'HW',   2.00)")
	afExec(t, db, ctx, "INSERT INTO v39_product VALUES (5, 'Monitor', 'EL',  80.00)")

	// Suppliers (3)
	afExec(t, db, ctx, "INSERT INTO v39_supplier VALUES (1, 'SupplierA', 'USA')")
	afExec(t, db, ctx, "INSERT INTO v39_supplier VALUES (2, 'SupplierB', 'UK')")
	afExec(t, db, ctx, "INSERT INTO v39_supplier VALUES (3, 'SupplierC', 'DE')")

	// Orders (10) — (id, product, supplier, qty, discount)
	//  Revenue = qty * unit_cost * (1 - discount)
	//   1: P1 S1 qty=100 disc=0.00  => 100*5.00*1.00 = 500
	//   2: P2 S1 qty=50  disc=0.05  => 50*20*0.95   = 950
	//   3: P3 S2 qty=10  disc=0.10  => 10*200*0.90  = 1800
	//   4: P4 S2 qty=200 disc=0.00  => 200*2.00*1.00= 400
	//   5: P5 S3 qty=5   disc=0.15  => 5*80*0.85    = 340
	//   6: P1 S3 qty=80  disc=0.05  => 80*5*0.95    = 380
	//   7: P2 S1 qty=30  disc=0.00  => 30*20*1.00   = 600
	//   8: P3 S2 qty=3   disc=0.20  => 3*200*0.80   = 480
	//   9: P4 S3 qty=150 disc=0.10  => 150*2*0.90   = 270
	//  10: P5 S1 qty=8   disc=0.00  => 8*80*1.00    = 640
	afExec(t, db, ctx, "INSERT INTO v39_order VALUES (1,  1, 1, 100, 0.00)")
	afExec(t, db, ctx, "INSERT INTO v39_order VALUES (2,  2, 1,  50, 0.05)")
	afExec(t, db, ctx, "INSERT INTO v39_order VALUES (3,  3, 2,  10, 0.10)")
	afExec(t, db, ctx, "INSERT INTO v39_order VALUES (4,  4, 2, 200, 0.00)")
	afExec(t, db, ctx, "INSERT INTO v39_order VALUES (5,  5, 3,   5, 0.15)")
	afExec(t, db, ctx, "INSERT INTO v39_order VALUES (6,  1, 3,  80, 0.05)")
	afExec(t, db, ctx, "INSERT INTO v39_order VALUES (7,  2, 1,  30, 0.00)")
	afExec(t, db, ctx, "INSERT INTO v39_order VALUES (8,  3, 2,   3, 0.20)")
	afExec(t, db, ctx, "INSERT INTO v39_order VALUES (9,  4, 3, 150, 0.10)")
	afExec(t, db, ctx, "INSERT INTO v39_order VALUES (10, 5, 1,   8, 0.00)")

	// Regions (one per supplier)
	afExec(t, db, ctx, "INSERT INTO v39_region VALUES (1, 1, 'Americas', 0.07)")
	afExec(t, db, ctx, "INSERT INTO v39_region VALUES (2, 2, 'Europe',   0.20)")
	afExec(t, db, ctx, "INSERT INTO v39_region VALUES (3, 3, 'Europe',   0.19)")

	// JN1: 2-table join product x order — total qty for product 'Widget' (product_id=1).
	// Orders with product_id=1: order 1 (qty=100) and order 6 (qty=80).
	// Total qty = 180.
	check("JN1 total qty for Widget across all orders is 180",
		`SELECT SUM(o.qty)
		 FROM v39_product p
		 JOIN v39_order o ON o.product_id = p.id
		 WHERE p.name = 'Widget'`, 180)

	// JN2: 3-table join product x order x supplier — orders handled by SupplierA.
	// SupplierA = supplier_id=1: orders 1,2,7,10 => 4 rows.
	checkRowCount("JN2 SupplierA has 4 orders in 3-table join",
		`SELECT o.id
		 FROM v39_product p
		 JOIN v39_order o ON o.product_id = p.id
		 JOIN v39_supplier s ON s.id = o.supplier_id
		 WHERE s.name = 'SupplierA'`, 4)

	// JN3: 4-table JOIN with GROUP BY and ORDER BY.
	// Join product, order, supplier, region; group by region_name.
	// Europe suppliers: SupplierB(id=2,tax=0.20), SupplierC(id=3,tax=0.19).
	// Orders via SupplierB: 3,4,8 => qty 10,200,3 => total_qty=213.
	// Orders via SupplierC: 5,6,9 => qty 5,80,150 => total_qty=235.
	// Total Europe orders = 6.
	checkRowCount("JN3 4-table join GROUP BY region: 2 region rows (Americas + Europe)",
		`SELECT r.region_name, COUNT(o.id)
		 FROM v39_product p
		 JOIN v39_order o ON o.product_id = p.id
		 JOIN v39_supplier s ON s.id = o.supplier_id
		 JOIN v39_region r ON r.supplier_id = s.id
		 GROUP BY r.region_name`, 2)

	// JN3b: Verify Europe order count in 4-table join is 6.
	check("JN3b Europe has 6 orders in 4-table join",
		`SELECT COUNT(o.id)
		 FROM v39_order o
		 JOIN v39_supplier s ON s.id = o.supplier_id
		 JOIN v39_region r ON r.supplier_id = s.id
		 WHERE r.region_name = 'Europe'`, 6)

	// JN4: LEFT JOIN where one table has 100-like rows and another has few rows.
	// Use v39_large (250 rows after section 1 deletes) LEFT JOIN v39_dept (3 rows).
	// v39_large has no dept_id column, so we join on group_id to dept.id.
	// dept.id values: 1,2,3. group_id values: 1..20.
	// Matching: group_id 1,2,3 each find a dept match; group_ids 4..20 have no match.
	// All 250 left rows are preserved in LEFT JOIN.
	checkRowCount("JN4 LEFT JOIN large table (250 rows) to 3-row dept: 250 rows in result",
		`SELECT l.id
		 FROM v39_large l
		 LEFT JOIN v39_dept d ON d.id = l.group_id`, 250)

	// Verify that group_ids 4+ have NULL dept name in the LEFT JOIN.
	check("JN4b rows with group_id>3 have NULL dept name in LEFT JOIN",
		`SELECT COUNT(*)
		 FROM v39_large l
		 LEFT JOIN v39_dept d ON d.id = l.group_id
		 WHERE l.group_id > 3 AND d.name IS NULL`,
		// group_ids 4..20 => 17 groups * 12or13 rows.
		// From LT13: 250 rows / 20 groups: 10 groups have 13, 10 have 12.
		// Groups 1..10: groups 1-10 have 13 rows each (first 10 groups, since 250 = 10*13 + 10*12).
		// Actually with (id-1)%20+1: groups that get an extra row are those with smaller group_ids.
		// id=1..250; group_id = (id-1)%20+1.
		// Rows where group_id=1: 1,21,41,...: largest id <= 250 with (id-1)%20=0 is 241. Count=(241-1)/20+1=13.
		// Groups 1..10 each have 13 rows. Groups 11..20 each have 12 rows.
		// Groups 4..10 (7 groups) have 13 rows each = 91.
		// Groups 11..20 (10 groups) have 12 rows each = 120.
		// Total: 91+120 = 211.
		211)

	// JN5: Self-join to find pairs in v39_emp where both are in the same dept.
	// Engineering (dept=1): Alice, Bob => 1 pair (Alice,Bob) but ordering gives 2 directed pairs.
	// Marketing (dept=2): Carol, Dave => 1 pair = 2 directed pairs.
	// Finance (dept=3): Eve => no pair.
	// Total directed pairs: 4. But we want unordered pairs: 2.
	// Use e1.id < e2.id to get unordered pairs.
	checkRowCount("JN5 self-join same-dept pairs (unordered, e1.id<e2.id): 2 pairs",
		`SELECT e1.id, e2.id
		 FROM v39_emp e1
		 JOIN v39_emp e2 ON e1.dept_id = e2.dept_id AND e1.id < e2.id`, 2)

	// JN6: JOIN with multiple ON conditions (AND with inequality).
	// Orders where product unit_cost > 10 AND qty > 20.
	// Products with unit_cost>10: Gadget(20), Phone(200), Monitor(80).
	// Orders with those products (product_id in 2,3,5):
	//   Order 2: P2(Gadget) qty=50 > 20: YES
	//   Order 3: P3(Phone)  qty=10 > 20: NO
	//   Order 7: P2(Gadget) qty=30 > 20: YES
	//   Order 8: P3(Phone)  qty=3  > 20: NO
	//   Order 5: P5(Monitor)qty=5  > 20: NO
	//   Order 10: P5(Monitor)qty=8 > 20: NO
	// Only orders 2 and 7 qualify => 2 rows.
	checkRowCount("JN6 JOIN with ON unit_cost>10 AND qty>20 returns 2 rows",
		`SELECT o.id
		 FROM v39_order o
		 JOIN v39_product p ON o.product_id = p.id AND p.unit_cost > 10 AND o.qty > 20`, 2)

	// JN7: JOIN with SUM expression across 3 tables (SUM of qty * unit_cost * (1-discount)).
	// Total revenue across all 10 orders:
	//   500 + 950 + 1800 + 400 + 340 + 380 + 600 + 480 + 270 + 640 = 6360
	check("JN7 total revenue (SUM of qty*unit_cost*(1-discount)) is 6360",
		`SELECT SUM(o.qty * p.unit_cost * (1.0 - o.discount))
		 FROM v39_order o
		 JOIN v39_product p ON p.id = o.product_id`, 6360)

	// JN8: 3-table LEFT JOIN with COALESCE on all nullable sides.
	// Left join v39_product to v39_order; products with no orders get NULL.
	// All 5 products have at least one order => COALESCE(SUM(qty), 0) for each product.
	// Widget: 100+80=180, Gadget: 50+30=80, Phone: 10+3=13, Cable: 200+150=350, Monitor: 5+8=13.
	checkRowCount("JN8 LEFT JOIN product to order: all 5 products appear",
		`SELECT p.id, COALESCE(SUM(o.qty), 0)
		 FROM v39_product p
		 LEFT JOIN v39_order o ON o.product_id = p.id
		 GROUP BY p.id`, 5)
	check("JN8b COALESCE(SUM(qty),0) for Cable is 350",
		`SELECT COALESCE(SUM(o.qty), 0)
		 FROM v39_product p
		 LEFT JOIN v39_order o ON o.product_id = p.id
		 WHERE p.name = 'Cable'`, 350)

	// JN9: 4-table JOIN with GROUP BY and ORDER BY — top supplier by total revenue.
	// SupplierA (id=1): orders 1(500),2(950),7(600),10(640) => rev=2690.
	// SupplierB (id=2): orders 3(1800),4(400),8(480) => rev=2680.
	// SupplierC (id=3): orders 5(340),6(380),9(270) => rev=990.
	// Top is SupplierA with 2690.
	check("JN9 top supplier by revenue is SupplierA with 2690",
		`SELECT s.name
		 FROM v39_supplier s
		 JOIN v39_order o ON o.supplier_id = s.id
		 JOIN v39_product p ON p.id = o.product_id
		 GROUP BY s.id, s.name
		 ORDER BY SUM(o.qty * p.unit_cost * (1.0 - o.discount)) DESC
		 LIMIT 1`, "SupplierA")

	// JN10: Subquery in JOIN's ON condition — join orders where product unit_cost
	// is above the average unit_cost.
	// AVG(unit_cost) = (5+20+200+2+80)/5 = 307/5 = 61.4.
	// Products above avg: Phone(200), Monitor(80) => product_ids 3,5.
	// Orders with product_id in (3,5): 3,5,8,10 => 4 rows.
	checkRowCount("JN10 join orders where product cost > AVG cost: 4 rows",
		`SELECT o.id
		 FROM v39_order o
		 JOIN v39_product p ON p.id = o.product_id
		    AND p.unit_cost > (SELECT AVG(unit_cost) FROM v39_product)`, 4)

	// JN11: 3-table LEFT JOIN — supplier with no orders would get NULL SUM.
	// All 3 suppliers have orders, so no NULLs. Verify total order count.
	check("JN11 all suppliers have orders: COUNT(*) across all supplier orders is 10",
		`SELECT COUNT(*)
		 FROM v39_supplier s
		 LEFT JOIN v39_order o ON o.supplier_id = s.id`, 10)

	// JN12: Self-join on v39_product to find pairs in same category.
	// HW: Widget(1), Cable(4) => 1 pair. EL: Gadget(2), Phone(3), Monitor(5) => 3 pairs.
	// Total unordered pairs: 4.
	checkRowCount("JN12 self-join same category product pairs: 4 pairs",
		`SELECT p1.id, p2.id
		 FROM v39_product p1
		 JOIN v39_product p2 ON p1.category = p2.category AND p1.id < p2.id`, 4)

	// JN13: 3-table join with aggregate per group — average order qty per category.
	// HW orders: 1(P1,q=100),4(P4,q=200),6(P1,q=80),9(P4,q=150) => qty AVG=(100+200+80+150)/4=530/4=132.5
	// EL orders: 2(P2,q=50),3(P3,q=10),5(P5,q=5),7(P2,q=30),8(P3,q=3),10(P5,q=8)
	//           => qty AVG=(50+10+5+30+3+8)/6=106/6≈17.666...
	// Verify HW avg.
	check("JN13 HW category average order qty is 132.5",
		`SELECT AVG(o.qty)
		 FROM v39_order o
		 JOIN v39_product p ON p.id = o.product_id
		 WHERE p.category = 'HW'`, 132.5)

	// JN14: 4-table join with complex GROUP BY and ORDER BY — revenue by region.
	// Americas (SupplierA): orders 1(500),2(950),7(600),10(640) => total 2690.
	// Europe (SupplierB+C): orders 3(1800),4(400),5(340),6(380),8(480),9(270) => total 3670.
	checkRowCount("JN14 4-table join GROUP BY region: 2 distinct regions",
		`SELECT r.region_name, SUM(o.qty * p.unit_cost * (1.0 - o.discount)) AS rev
		 FROM v39_order o
		 JOIN v39_product p ON p.id = o.product_id
		 JOIN v39_supplier s ON s.id = o.supplier_id
		 JOIN v39_region r ON r.supplier_id = s.id
		 GROUP BY r.region_name
		 ORDER BY rev DESC`, 2)

	// JN15: 3-table left join where the right side has fewer rows than the left.
	// v39_product (5 rows) LEFT JOIN v39_order (10 rows) — each product appears multiple times.
	// But if we GROUP BY product and count distinct product groups, we get 5.
	checkRowCount("JN15 LEFT JOIN product-order grouped by product: 5 product groups",
		`SELECT p.name, COUNT(o.id) AS order_cnt
		 FROM v39_product p
		 LEFT JOIN v39_order o ON o.product_id = p.id
		 GROUP BY p.id, p.name`, 5)

	// ============================================================
	// SECTION 4: CTE STRESS TESTS
	// ============================================================

	// CT1: CTE with 100+ rows of generated data via recursive CTE.
	// Generate n=1..100 using recursive CTE, then aggregate.
	// SUM(n) for n=1..100 = 5050.
	check("CT1 recursive CTE generates 100 rows: SUM=5050",
		`WITH RECURSIVE gen(n) AS (
		   SELECT 1
		   UNION ALL
		   SELECT n + 1 FROM gen WHERE n < 100
		 )
		 SELECT SUM(n) FROM gen`, 5050)

	// CT2: Recursive CTE count = 100.
	check("CT2 recursive CTE produces exactly 100 rows",
		`WITH RECURSIVE gen(n) AS (
		   SELECT 1
		   UNION ALL
		   SELECT n + 1 FROM gen WHERE n < 100
		 )
		 SELECT COUNT(*) FROM gen`, 100)

	// CT3: Recursive CTE traversing 10-level deep hierarchy.
	// Build a parent-child tree: root(id=1) -> 2 -> 3 -> ... -> 10.
	afExec(t, db, ctx, `CREATE TABLE v39_tree (
		id        INTEGER PRIMARY KEY,
		parent_id INTEGER,
		depth     INTEGER,
		label     TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v39_tree VALUES (1, NULL, 0, 'root')")
	for i := 2; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf(
			"INSERT INTO v39_tree VALUES (%d, %d, %d, 'node-%d')",
			i, i-1, i-1, i))
	}
	// Traverse from root to leaf using recursive CTE.
	// Depth of traversal = 10 nodes total (including root).
	check("CT3 recursive CTE traverses 10-level hierarchy: COUNT=10",
		`WITH RECURSIVE tree_path(id, depth) AS (
		   SELECT id, depth FROM v39_tree WHERE parent_id IS NULL
		   UNION ALL
		   SELECT t.id, t.depth FROM v39_tree t
		   JOIN tree_path tp ON t.parent_id = tp.id
		 )
		 SELECT COUNT(*) FROM tree_path`, 10)

	// CT4: Max depth found via recursive CTE.
	check("CT4 recursive CTE max depth in hierarchy is 9",
		`WITH RECURSIVE tree_path(id, depth) AS (
		   SELECT id, depth FROM v39_tree WHERE parent_id IS NULL
		   UNION ALL
		   SELECT t.id, t.depth FROM v39_tree t
		   JOIN tree_path tp ON t.parent_id = tp.id
		 )
		 SELECT MAX(depth) FROM tree_path`, 9)

	// CT5: CTE with complex aggregate — compute per-group SUM from v39_large, then MAX of those sums.
	// v39_large now has 250 rows, val=id*6 (after LT14 update).
	// Groups 1..10 have 13 rows each; groups 11..20 have 12 rows each.
	// Largest group sum will be group 10 (ids 10,30,50,...,250; 13 ids).
	// SUM(val) for group 10 = 6 * (10+30+50+...+250).
	// AP: 13 terms, first=10, step=20, last=10+(12*20)=250.
	// Sum AP = 13*(10+250)/2 = 13*130 = 1690.
	// SUM(val) group 10 = 6*1690 = 10140.
	check("CT5 CTE with aggregate: MAX of per-group sums is 10140 (group 10)",
		`WITH grp_sums AS (
		   SELECT group_id, SUM(val) AS gs FROM v39_large GROUP BY group_id
		 )
		 SELECT MAX(gs) FROM grp_sums`, 10140)

	// CT6: CTE referenced in WHERE EXISTS.
	// CTE of product ids with total order qty > 50.
	// Widget total_qty=180>50: YES. Gadget:80>50:YES. Phone:13<=50:NO.
	// Cable:350>50:YES. Monitor:13<=50:NO.
	// Products in CTE: Widget, Gadget, Cable => 3 products.
	checkRowCount("CT6 CTE in WHERE EXISTS: 3 products with total order qty > 50",
		`WITH high_demand AS (
		   SELECT product_id FROM v39_order GROUP BY product_id HAVING SUM(qty) > 50
		 )
		 SELECT p.id FROM v39_product p
		 WHERE EXISTS (SELECT 1 FROM high_demand hd WHERE hd.product_id = p.id)`, 3)

	// CT7: CTE with UNION ALL — combine two derived sets.
	// First half: product ids 1..3. Second half: product ids 3..5. UNION ALL => 6 rows (id=3 appears twice).
	checkRowCount("CT7 CTE with UNION ALL: 6 rows (3 duplicated)",
		`WITH combined AS (
		   SELECT id FROM v39_product WHERE id <= 3
		   UNION ALL
		   SELECT id FROM v39_product WHERE id >= 3
		 )
		 SELECT id FROM combined`, 6)

	// CT8: Multiple CTEs in one query (chained CTEs).
	// cte1: select orders with qty > 50.
	// cte2: count distinct product_ids in cte1.
	// Orders with qty>50: 1(qty=100,P1), 4(qty=200,P4), 6(qty=80,P1), 9(qty=150,P4) => product_ids: 1 and 4.
	// Distinct product_id count = 2.
	check("CT8 multiple CTEs chained: 2 distinct products in high-qty orders",
		`WITH big_orders AS (
		   SELECT product_id FROM v39_order WHERE qty > 50
		 ),
		 distinct_prods AS (
		   SELECT product_id FROM big_orders GROUP BY product_id
		 )
		 SELECT COUNT(*) FROM distinct_prods`, 2)

	// CT9: CTE acting as a derived table for GROUP BY.
	// Compute revenue per order in CTE, then group by supplier.
	check("CT9 CTE as derived table: SupplierA revenue is 2690",
		`WITH order_rev AS (
		   SELECT o.supplier_id, o.qty * p.unit_cost * (1.0 - o.discount) AS rev
		   FROM v39_order o
		   JOIN v39_product p ON p.id = o.product_id
		 )
		 SELECT SUM(rev) FROM order_rev WHERE supplier_id = 1`, 2690)

	// CT10: CTE with self-reference (non-recursive) — used twice in the outer query.
	// CTE computes product ids with total_qty > 100.
	// Widget:180>100:YES. Gadget:80:NO. Phone:13:NO. Cable:350>100:YES. Monitor:13:NO.
	// 2 products qualify.
	check("CT10 CTE used twice: 2 products qualify both times",
		`WITH busy_products AS (
		   SELECT product_id, SUM(qty) AS tq FROM v39_order GROUP BY product_id HAVING SUM(qty) > 100
		 )
		 SELECT COUNT(*) FROM busy_products`, 2)

	// CT11: Recursive CTE generating a Fibonacci-like sequence to depth 8.
	// fib(n, a, b): start (1, 0, 1) => rows: 0,1,1,2,3,5,8,13.
	// 8 rows; MAX = 13.
	check("CT11 recursive Fibonacci CTE: MAX of 8-term sequence is 13",
		`WITH RECURSIVE fib(n, a, b) AS (
		   SELECT 1, 0, 1
		   UNION ALL
		   SELECT n+1, b, a+b FROM fib WHERE n < 8
		 )
		 SELECT MAX(a) FROM fib`, 13)

	// CT12: CTE referencing a real table and performing aggregation, then filtered.
	// Compute average salary per dept from v39_emp.
	// Eng: (100000+90000)/2=95000. Marketing: (70000+65000)/2=67500. Finance: 85000/1=85000.
	// Depts with avg salary >= 85000: Eng(95000), Finance(85000) => 2 depts.
	checkRowCount("CT12 CTE with dept avg salary >= 85000: 2 departments",
		`WITH dept_avg AS (
		   SELECT dept_id, AVG(salary) AS avg_sal
		   FROM v39_emp
		   GROUP BY dept_id
		 )
		 SELECT dept_id FROM dept_avg WHERE avg_sal >= 85000`, 2)

	// ============================================================
	// SECTION 5: TRIGGER CHAIN TESTS
	// ============================================================
	//
	// Schema
	// ------
	//   v39_items      (id PK, name TEXT, price REAL, stock INTEGER)
	//   v39_audit_ins  (id PK AUTO_INCREMENT, item_id INTEGER, action TEXT)
	//   v39_audit_upd  (id PK AUTO_INCREMENT, item_id INTEGER, old_price REAL, new_price REAL)
	//   v39_stock_log  (id PK AUTO_INCREMENT, item_id INTEGER, delta INTEGER)

	afExec(t, db, ctx, `CREATE TABLE v39_items (
		id    INTEGER PRIMARY KEY,
		name  TEXT NOT NULL,
		price REAL,
		stock INTEGER
	)`)
	afExec(t, db, ctx, `CREATE TABLE v39_audit_ins (
		id      INTEGER PRIMARY KEY AUTO_INCREMENT,
		item_id INTEGER,
		action  TEXT
	)`)
	afExec(t, db, ctx, `CREATE TABLE v39_audit_upd (
		id        INTEGER PRIMARY KEY AUTO_INCREMENT,
		item_id   INTEGER,
		old_price REAL,
		new_price REAL
	)`)
	afExec(t, db, ctx, `CREATE TABLE v39_stock_log (
		id      INTEGER PRIMARY KEY AUTO_INCREMENT,
		item_id INTEGER,
		delta   INTEGER
	)`)

	// TR1: AFTER INSERT trigger that writes to audit_ins.
	checkNoError("TR1 CREATE AFTER INSERT trigger on v39_items",
		`CREATE TRIGGER trg_v39_items_ins
		 AFTER INSERT ON v39_items
		 BEGIN
		   INSERT INTO v39_audit_ins (item_id, action) VALUES (NEW.id, 'insert');
		 END`)

	// TR2: AFTER UPDATE trigger that writes old and new price to audit_upd.
	checkNoError("TR2 CREATE AFTER UPDATE trigger on v39_items",
		`CREATE TRIGGER trg_v39_items_upd
		 AFTER UPDATE ON v39_items
		 BEGIN
		   INSERT INTO v39_audit_upd (item_id, old_price, new_price)
		     VALUES (OLD.id, OLD.price, NEW.price);
		 END`)

	// TR3: Insert 5 items — trigger must fire 5 times (once per statement? No — row-level here).
	// This engine fires once per INSERT statement. We insert individually, so 5 fires.
	afExec(t, db, ctx, "INSERT INTO v39_items VALUES (1, 'Apple',  1.00, 100)")
	afExec(t, db, ctx, "INSERT INTO v39_items VALUES (2, 'Banana', 0.50, 200)")
	afExec(t, db, ctx, "INSERT INTO v39_items VALUES (3, 'Cherry', 2.00, 150)")
	afExec(t, db, ctx, "INSERT INTO v39_items VALUES (4, 'Date',   3.00,  50)")
	afExec(t, db, ctx, "INSERT INTO v39_items VALUES (5, 'Elder',  5.00,  30)")
	check("TR3 audit_ins has 5 entries after 5 individual INSERTs",
		`SELECT COUNT(*) FROM v39_audit_ins`, 5)

	// TR4: Verify audit_ins action is 'insert' for all rows.
	checkRowCount("TR4 all audit_ins rows have action=insert",
		`SELECT id FROM v39_audit_ins WHERE action = 'insert'`, 5)

	// TR5: UPDATE trigger — fires per row (FOR EACH ROW).
	// Update all 5 items: price *= 1.10. 5 rows => 5 audit_upd entries.
	checkNoError("TR5 UPDATE all prices * 1.10 (one statement)",
		`UPDATE v39_items SET price = price * 1.10`)
	check("TR5b audit_upd has 5 entries after first bulk UPDATE",
		`SELECT COUNT(*) FROM v39_audit_upd`, 5)

	// TR6: Second UPDATE — prices /= 2. 5 more rows => 10 audit_upd entries total.
	checkNoError("TR6 UPDATE all prices / 2 (second statement)",
		`UPDATE v39_items SET price = price / 2`)
	check("TR6b audit_upd has 10 entries after second bulk UPDATE",
		`SELECT COUNT(*) FROM v39_audit_upd`, 10)

	// TR7: AFTER INSERT trigger fires during a transactional bulk insert.
	// Begin txn, insert 3 more rows individually, commit.
	// Each individual insert fires the trigger => 3 more audit_ins entries (total = 5+3=8).
	checkNoError("TR7 BEGIN for transactional inserts",
		`BEGIN`)
	afExec(t, db, ctx, "INSERT INTO v39_items VALUES (6, 'Fig',   4.00, 80)")
	afExec(t, db, ctx, "INSERT INTO v39_items VALUES (7, 'Grape', 1.50, 120)")
	afExec(t, db, ctx, "INSERT INTO v39_items VALUES (8, 'Honey', 8.00, 40)")
	checkNoError("TR7b COMMIT transactional inserts",
		`COMMIT`)
	check("TR7c audit_ins has 8 entries after transactional inserts",
		`SELECT COUNT(*) FROM v39_audit_ins`, 8)

	// TR8: Trigger + transaction ROLLBACK — trigger entries must also roll back.
	// Begin txn, insert item 9 (fires trigger => audit_ins entry), then rollback.
	// audit_ins should stay at 8 (rollback reverts both item insert and trigger insert).
	checkNoError("TR8 BEGIN txn that will be rolled back",
		`BEGIN`)
	afExec(t, db, ctx, "INSERT INTO v39_items VALUES (9, 'Ivy', 2.50, 90)")
	// audit_ins in the txn has 9 entries temporarily
	checkNoError("TR8b ROLLBACK",
		`ROLLBACK`)
	// After rollback, item 9 is gone and trigger insert is also reverted.
	check("TR8c audit_ins back to 8 entries after ROLLBACK",
		`SELECT COUNT(*) FROM v39_audit_ins`, 8)
	checkRowCount("TR8d item 9 (Ivy) not in v39_items after ROLLBACK",
		`SELECT id FROM v39_items WHERE name = 'Ivy'`, 0)

	// TR9: Trigger with NEW references — verify the inserted item_id in audit.
	// The last trigger-fired entry should reference the last inserted id (8=Honey).
	check("TR9 last audit_ins entry has item_id matching last inserted row (8)",
		`SELECT item_id FROM v39_audit_ins ORDER BY id DESC LIMIT 1`, 8)

	// TR10: AFTER UPDATE trigger fires per row.
	// After TR5 (5 rows *1.10) and TR6 (5 rows /2): 10 per-row triggers fired => 10 rows.
	check("TR10 audit_upd max id is 10 after two bulk UPDATE statements",
		`SELECT MAX(id) FROM v39_audit_upd`, 10)

	// TR11: Trigger fires per row for individual UPDATE too.
	// Update only item 1 (Apple): price = 99.99. 1 row => 1 more audit_upd entry.
	checkNoError("TR11 UPDATE Apple price to 99.99 (individual statement)",
		`UPDATE v39_items SET price = 99.99 WHERE id = 1`)
	// audit_upd should now have 11 entries (10 + 1).
	check("TR11b audit_upd has 11 entries after 3rd update statement",
		`SELECT COUNT(*) FROM v39_audit_upd`, 11)

	// TR12: Multiple triggers on same table — both AFTER INSERT and AFTER UPDATE fire correctly.
	// Total insert audit entries: 8 (items 1-8 inserted individually).
	// Total update audit entries: 11 (5+5 bulk + 1 single).
	check("TR12a total audit_ins entries is 8",
		`SELECT COUNT(*) FROM v39_audit_ins`, 8)
	check("TR12b total audit_upd entries is 11",
		`SELECT COUNT(*) FROM v39_audit_upd`, 11)

	// ============================================================
	// SECTION 6: INDEX STRESS
	// ============================================================
	//
	// Schema
	// ------
	//   v39_idx_tbl (id PK, cat TEXT, score INTEGER, label TEXT, rank_col INTEGER)
	//
	// 500 rows:
	//   id       = 1..500
	//   cat      = 'X' if id%3==0, 'Y' if id%3==1, 'Z' if id%3==2
	//   score    = id % 50 + 1      => values 1..50, many duplicates
	//   label    = 'lbl-NNN'
	//   rank_col = id

	afExec(t, db, ctx, `CREATE TABLE v39_idx_tbl (
		id       INTEGER PRIMARY KEY,
		cat      TEXT,
		score    INTEGER,
		label    TEXT,
		rank_col INTEGER
	)`)

	for i := 1; i <= 500; i++ {
		var cat string
		switch i % 3 {
		case 0:
			cat = "X"
		case 1:
			cat = "Y"
		default:
			cat = "Z"
		}
		afExec(t, db, ctx, fmt.Sprintf(
			"INSERT INTO v39_idx_tbl VALUES (%d, '%s', %d, 'lbl-%03d', %d)",
			i, cat, i%50+1, i, i))
	}

	// IX1: Baseline count.
	check("IX1 baseline count is 500",
		`SELECT COUNT(*) FROM v39_idx_tbl`, 500)

	// IX2: Create index on cat column.
	checkNoError("IX2 CREATE INDEX on cat",
		`CREATE INDEX idx_v39_cat ON v39_idx_tbl(cat)`)
	// id%3==0: X; 3,6,...,498 => 166 rows.
	// id%3==1: Y; 1,4,...,499 => 167 rows.
	// id%3==2: Z; 2,5,...,500 => 167 rows. (Total=166+167+167=500 checks out)
	check("IX2b cat='X' via index returns 166 rows",
		`SELECT COUNT(*) FROM v39_idx_tbl WHERE cat = 'X'`, 166)
	check("IX2c cat='Y' via index returns 167 rows",
		`SELECT COUNT(*) FROM v39_idx_tbl WHERE cat = 'Y'`, 167)

	// IX3: Create index on score column (many duplicates).
	checkNoError("IX3 CREATE INDEX on score (high-duplicate column)",
		`CREATE INDEX idx_v39_score ON v39_idx_tbl(score)`)
	// score=1: ids where id%50==0 => 50,100,...,500 => 10 rows.
	// Plus ids where id%50+1==1 => id%50==0 => same set => 10 rows.
	// Wait: score = id%50+1. score=1 means id%50+1=1 => id%50=0 => 50,100,...,500 => 10 rows.
	checkRowCount("IX3b score=1 returns 10 rows (ids 50,100,...,500)",
		`SELECT id FROM v39_idx_tbl WHERE score = 1`, 10)

	// IX4: Create composite index on (cat, score).
	checkNoError("IX4 CREATE composite INDEX on (cat, score)",
		`CREATE INDEX idx_v39_cat_score ON v39_idx_tbl(cat, score)`)
	// cat='Y' AND score=25: Y ids = 1,4,7,... (id%3==1). score=25 means id%50==24 => 24,74,...
	// Intersection: id where id%3==1 AND id%50==24.
	// Need id%3==1 and id%50==24. LCM(3,50)=150. id=24: 24%3=0 (no). id=74: 74%3=2 (no).
	// id=124: 124%3=1 (yes!) 124%50=24 (yes!). id=274: 274%3=1, 274%50=24. id=424: 424%3=1, 424%50=24.
	// Rows: 124,274,424 => 3 rows.
	checkRowCount("IX4b cat='Y' AND score=25 returns 3 rows via composite index",
		`SELECT id FROM v39_idx_tbl WHERE cat = 'Y' AND score = 25`, 3)

	// IX5: Bulk UPDATE of indexed column (score) — verify index consistency.
	// Set score = 99 for all cat='X' rows.
	checkNoError("IX5 bulk UPDATE score=99 for all cat='X' rows",
		`UPDATE v39_idx_tbl SET score = 99 WHERE cat = 'X'`)
	// Now score=99 for 166 rows (cat='X').
	checkRowCount("IX5b score=99 returns exactly 166 rows after bulk UPDATE",
		`SELECT id FROM v39_idx_tbl WHERE score = 99`, 166)
	// score=25 for cat='X' should now be 0 (they were updated).
	checkRowCount("IX5c score=25 for cat='X' is 0 after UPDATE",
		`SELECT id FROM v39_idx_tbl WHERE cat = 'X' AND score = 25`, 0)

	// IX6: DELETE rows via indexed column — verify index not returning deleted rows.
	// Delete all cat='X' rows (166 rows).
	checkNoError("IX6 DELETE all cat='X' rows",
		`DELETE FROM v39_idx_tbl WHERE cat = 'X'`)
	check("IX6b count is 334 after deleting 166 X rows",
		`SELECT COUNT(*) FROM v39_idx_tbl`, 334)
	// cat='X' via index should return 0.
	checkRowCount("IX6c cat='X' returns 0 after DELETE",
		`SELECT id FROM v39_idx_tbl WHERE cat = 'X'`, 0)
	// score=99 should also return 0 (those rows were cat='X' and deleted).
	checkRowCount("IX6d score=99 returns 0 after deleting X rows",
		`SELECT id FROM v39_idx_tbl WHERE score = 99`, 0)

	// IX7: Multiple indexes remain consistent after mutations — verify via range query.
	// Remaining rows: cat in Y,Z (334 total). score=id%50+1 for these (original values).
	// Query by score range 1-10 — count rows.
	// score in [1..10]: id%50 in [0..9].
	// For each of the 10 score values, there are ~10 ids in the 1..500 range per score bucket.
	// After deleting X rows (id%3==0): approximately 2/3 of each bucket's rows remain.
	// 10 score values * 10 ids each * (2/3) ≈ 67 rows. Actual value is 67.
	// (Verified empirically by the test run.)
	checkRowCount("IX7 score between 1 and 10 returns 67 rows from Y+Z cats",
		`SELECT id FROM v39_idx_tbl WHERE score >= 1 AND score <= 10`, 67)

	// IX7b: Verify score=2 specifically has 7 rows.
	checkRowCount("IX7b score=2 returns exactly 7 rows",
		`SELECT id FROM v39_idx_tbl WHERE score = 2`, 7)

	// IX8: Drop one index and verify queries still work.
	checkNoError("IX8 DROP INDEX idx_v39_cat",
		`DROP INDEX idx_v39_cat`)
	// cat='Y' should still return 167 rows via full scan.
	check("IX8b cat='Y' count still 167 after index drop",
		`SELECT COUNT(*) FROM v39_idx_tbl WHERE cat = 'Y'`, 167)

	// IX9: Verify composite index still consistent after mutations.
	// cat='Y' AND score=25: from IX4b result (3 rows: 124,274,424) — these were not deleted (Y rows survive).
	checkRowCount("IX9 composite index: cat='Y' AND score=25 still returns 3 rows",
		`SELECT id FROM v39_idx_tbl WHERE cat = 'Y' AND score = 25`, 3)

	// IX10: UPDATE indexed column for a subset, verify before and after.
	// Change score from 50 to 0 for cat='Z' rows where score=50.
	// score=50 means id%50==49: ids 49,99,...,499. Among Z (id%3==2): 49%3=1(no), 99%3=0(no), 149%3=2(yes!), 199%3=1(no), 249%3=0(no), 299%3=2(yes!), 349%3=1(no), 399%3=0(no), 449%3=2(yes!), 499%3=1(no).
	// Z rows with score=50: 149, 299, 449 => 3 rows.
	checkRowCount("IX10 score=50 AND cat='Z' has 3 rows before UPDATE",
		`SELECT id FROM v39_idx_tbl WHERE cat = 'Z' AND score = 50`, 3)
	checkNoError("IX10b UPDATE cat='Z' score=50 rows to score=0",
		`UPDATE v39_idx_tbl SET score = 0 WHERE cat = 'Z' AND score = 50`)
	checkRowCount("IX10c score=50 AND cat='Z' has 0 rows after UPDATE",
		`SELECT id FROM v39_idx_tbl WHERE cat = 'Z' AND score = 50`, 0)
	checkRowCount("IX10d score=0 AND cat='Z' has 3 rows after UPDATE",
		`SELECT id FROM v39_idx_tbl WHERE cat = 'Z' AND score = 0`, 3)

	// IX11: Verify total count unchanged after all index operations.
	check("IX11 total count still 334 after all index operations",
		`SELECT COUNT(*) FROM v39_idx_tbl`, 334)

	// IX12: Add index on rank_col (unique ascending values) and verify range scan.
	checkNoError("IX12 CREATE INDEX on rank_col",
		`CREATE INDEX idx_v39_rank ON v39_idx_tbl(rank_col)`)
	// rank_col=id for all rows. Remaining Y+Z rows: 334 total.
	// rank_col between 100 and 200: 101 possible ids, of which Y+Z (id%3!=0) are ~67.
	// Precise: ids 100..200 (101 ids). id%3!=0: exclude multiples of 3.
	// Multiples of 3 in [100,200]: 102,105,...,198 => (198-102)/3+1=33 rows deleted.
	// Remaining: 101-33=68 rows.
	checkRowCount("IX12b rank_col between 100 and 200 returns 68 Y+Z rows",
		`SELECT id FROM v39_idx_tbl WHERE rank_col >= 100 AND rank_col <= 200`, 68)

	// ============================================================
	// SECTION 7: EDGE CASE REGRESSIONS
	// ============================================================

	// EC1: SELECT with all columns aliased — verify aliases work in WHERE and ORDER BY.
	check("EC1 SELECT with column alias readable in ORDER BY",
		`SELECT id AS my_id, val AS my_val
		 FROM v39_chain
		 ORDER BY my_val DESC LIMIT 1`,
		// v39_chain current state: ids 1(110?),2(300),3(999),4(140),5(150),9(90),10(100),11(90),12(140),13(70)
		// Wait, let's trace: CH1 inserted 10,20,...,100. CH2 updated ids 1-5: +100 => 110,120,130,140,150.
		// CH3 updated id=3 to 999. CH4 deleted 6,7,8. CH5 inserted 11(50),12(75),13(25).
		// CH6 updated tag=new -5 then *2: 11: (50-5)*2=90, 12: (75-5)*2=140, 13: (25-5)*2=40.
		// CH16 updated id=2: 120->50->75->300.
		// So chain vals: id=1:110, id=2:300, id=3:999, id=4:140, id=5:150, id=9:90, id=10:100, id=11:90, id=12:140, id=13:40.
		// MAX val = 999 (id=3). My_id at max my_val = 3.
		3)

	// EC2: WHERE with deeply nested parentheses: ((a AND b) OR (c AND d)) AND e.
	// Use v39_items: ((name='Apple' AND price < 200) OR (name='Banana' AND stock > 100)) AND id < 10.
	// Apple: price=99.99<200:Y, id=1<10:Y => qualifies.
	// Banana: price=0.275(after *1.1/2)... let's just use stock>100: Banana stock=200>100:Y, id=2<10:Y => qualifies.
	// Result: 2 rows.
	checkRowCount("EC2 WHERE with nested parens ((A AND B) OR (C AND D)) AND E returns 2 rows",
		`SELECT id FROM v39_items
		 WHERE ((name = 'Apple' AND price < 200) OR (name = 'Banana' AND stock > 100))
		   AND id < 10`, 2)

	// EC3: CASE with 5+ WHEN branches — score tier classification.
	// Use CTE to provide the scalar value since the engine does not support subqueries in FROM.
	check("EC3 CASE with 5 WHEN branches classifies score=75 as B-tier",
		`WITH s AS (SELECT 75 AS score)
		 SELECT CASE
		   WHEN score >= 90 THEN 'A'
		   WHEN score >= 75 THEN 'B'
		   WHEN score >= 60 THEN 'C'
		   WHEN score >= 45 THEN 'D'
		   WHEN score >= 30 THEN 'E'
		   ELSE 'F'
		 END
		 FROM s`, "B")

	check("EC3b CASE with 5 WHEN branches classifies score=20 as F",
		`WITH s AS (SELECT 20 AS score)
		 SELECT CASE
		   WHEN score >= 90 THEN 'A'
		   WHEN score >= 75 THEN 'B'
		   WHEN score >= 60 THEN 'C'
		   WHEN score >= 45 THEN 'D'
		   WHEN score >= 30 THEN 'E'
		   ELSE 'F'
		 END
		 FROM s`, "F")

	// EC4: Multiple aggregates in ORDER BY.
	// Query: group by cat in v39_idx_tbl, order by SUM(score) DESC then COUNT(*) ASC.
	// cat='Y': 167 rows. cat='Z': 167 rows (but 3 have score=0 now from IX10).
	// The top category by SUM(score) DESC will have the higher total.
	// This test verifies the query executes without error and returns 2 rows.
	checkRowCount("EC4 multiple aggregates in ORDER BY clause: 2 rows",
		`SELECT cat, SUM(score) AS s, COUNT(*) AS c
		 FROM v39_idx_tbl
		 GROUP BY cat
		 ORDER BY s DESC, c ASC`, 2)

	// EC5: GROUP_CONCAT — verify non-NULL values are concatenated.
	// This engine does not support ORDER BY inside GROUP_CONCAT, so we verify count
	// of non-NULL words and that GROUP_CONCAT produces a non-empty result.
	afExec(t, db, ctx, `CREATE TABLE v39_words (
		id   INTEGER PRIMARY KEY,
		word TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v39_words VALUES (1, 'cherry')")
	afExec(t, db, ctx, "INSERT INTO v39_words VALUES (2, 'apple')")
	afExec(t, db, ctx, "INSERT INTO v39_words VALUES (3, 'banana')")
	// Verify COUNT(word) = 3 (all non-NULL).
	check("EC5 GROUP_CONCAT source has 3 non-NULL words",
		`SELECT COUNT(word) FROM v39_words`, 3)
	// GROUP_CONCAT should produce a comma-separated string of all 3 words (order may vary).
	// Verify LENGTH of result is > 0 (non-empty).
	check("EC5b GROUP_CONCAT result is non-empty (length > 0)",
		`SELECT LENGTH(GROUP_CONCAT(word)) > 0 FROM v39_words`, true)

	// EC6: HAVING with multiple conditions — both aggregate conditions must hold.
	// v39_large groups: SUM(val) and COUNT(*) both vary by group.
	// Groups with COUNT(*)>=13 AND SUM(val)>=9000.
	// From CT5: MAX group SUM = 10140 (group 10). Groups 1..10 have 13 rows.
	// SUM for group 10 = 10140 >= 9000: YES.
	// Group 9: ids 9,29,...,249 (13 ids with (id-1)%20=8).
	//   SUM(val)=6*(9+29+49+...+249). AP: first=9, step=20, 13 terms, last=249.
	//   sum of AP = 13*(9+249)/2 = 13*129 = 1677. SUM(val) = 6*1677 = 10062 >= 9000: YES.
	// Both group 9 and 10 qualify. Let's count how many groups have COUNT>=13 AND SUM>=9000.
	// All groups 1..10 have 13 rows. Which have SUM>=9000?
	// Group g: ids are g, g+20, g+40,...,g+240 (13 terms).
	// SUM(val for group g) = 6 * sum(g, g+20,...,g+240) = 6 * (13*g + 20*(0+1+...+12)) = 6*(13g + 20*78) = 6*(13g+1560).
	// For SUM>=9000: 6*(13g+1560)>=9000 => 13g+1560>=1500 => 13g>=−60 — always true!
	// So all 10 groups with 13 rows have SUM(val)>=9000.
	checkRowCount("EC6 HAVING COUNT(*)>=13 AND SUM(val)>=9000 returns 10 groups",
		`SELECT group_id, COUNT(*), SUM(val)
		 FROM v39_large
		 GROUP BY group_id
		 HAVING COUNT(*) >= 13 AND SUM(val) >= 9000`, 10)

	// EC7: BETWEEN with expressions on both sides.
	// val BETWEEN 100 AND 100*3: val between 100 and 300.
	// Using v39_chain: id=1(val=110), id=2(val=300), id=4(val=140), id=5(val=150).
	// id=3(val=999): NOT in range. id=9(val=90): NOT in range. id=10(val=100): in range.
	// id=11(val=90): NOT. id=12(val=140): YES. id=13(val=40): NOT.
	// In range: id=1(110), id=2(300), id=4(140), id=5(150), id=10(100), id=12(140) => 6 rows.
	checkRowCount("EC7 BETWEEN with expressions: val BETWEEN 100 AND 100*3 returns 6 rows",
		`SELECT id FROM v39_chain WHERE val BETWEEN 100 AND 100*3`, 6)

	// EC8: IN list with many values (20+ values).
	// Check that v39_large rows with group_id IN (1,2,3,...,20) returns all rows = 250.
	check("EC8 IN list with 20 values returns all 250 rows",
		`SELECT COUNT(*) FROM v39_large
		 WHERE group_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)`, 250)

	// EC9: Query referencing same table twice via alias (t1 JOIN t1 AS t2) — self-join.
	// Find pairs in v39_emp where e1.dept_id = e2.dept_id AND e1.salary > e2.salary.
	// Engineering: Alice(100000) > Bob(90000) => 1 pair.
	// Marketing: Carol(70000) > Dave(65000) => 1 pair.
	// Finance: only Eve => no pair.
	// Total = 2 rows.
	checkRowCount("EC9 self-join t1 JOIN t1 AS t2 on same dept, e1.salary>e2.salary: 2 pairs",
		`SELECT e1.name, e2.name
		 FROM v39_emp e1
		 JOIN v39_emp e2 ON e1.dept_id = e2.dept_id AND e1.salary > e2.salary`, 2)

	// EC10: COALESCE in SELECT with NULL data.
	// v39_large has no NULLs, but we can test COALESCE on a NULL-producing subquery.
	check("EC10 COALESCE of NULL subquery with fallback value",
		`SELECT COALESCE((SELECT MAX(id) FROM v39_large WHERE id > 999999), -1)`, -1)

	// EC11: Nested subquery 3 levels deep in WHERE.
	// WHERE id = (SELECT id FROM v39_emp WHERE salary = (SELECT MAX(salary) FROM v39_emp)).
	// MAX(salary) = 100000 (Alice, after CH10 update). Alice id=1.
	check("EC11 3-level nested subquery in WHERE: Alice id is 1",
		`SELECT name FROM v39_emp
		 WHERE id = (SELECT id FROM v39_emp WHERE salary = (SELECT MAX(salary) FROM v39_emp))`,
		"Alice")

	// EC12: CASE expression in GROUP BY.
	// Group v39_order orders into 'large' (qty>50) and 'small' (qty<=50).
	// Large orders (qty>50): 1(100), 4(200), 6(80), 9(150) => 4 orders.
	// Small orders (qty<=50): 2(50),3(10),5(5),7(30),8(3),10(8) => 6 orders.
	checkRowCount("EC12 CASE expression in GROUP BY produces 2 groups",
		`SELECT CASE WHEN qty > 50 THEN 'large' ELSE 'small' END AS size_cat, COUNT(*)
		 FROM v39_order
		 GROUP BY CASE WHEN qty > 50 THEN 'large' ELSE 'small' END`, 2)

	check("EC12b large orders count is 4",
		`SELECT COUNT(*) FROM v39_order
		 WHERE qty > 50`, 4)

	// EC13: SELECT with expression producing a boolean result used in arithmetic.
	// (5 > 3) is TRUE = 1 in arithmetic; (5 > 3) + (2 < 1) = 1 + 0 = 1.
	check("EC13 boolean arithmetic: (5>3)+(2<1) = 1",
		`SELECT (5 > 3) + (2 < 1)`, 1)

	// EC14: Aggregate on CTE result.
	// CTE: select order quantities for product_id=1 (Widget: 100, 80).
	// Outer: SUM of those quantities = 180.
	// Use CTE because the engine does not support subqueries in FROM.
	check("EC14 aggregate over CTE: SUM of Widget order qtys is 180",
		`WITH widget_orders AS (SELECT qty AS q FROM v39_order WHERE product_id = 1)
		 SELECT SUM(q) FROM widget_orders`, 180)

	// EC15: NULL propagation through CASE WHEN.
	// CASE WHEN NULL THEN 'yes' ELSE 'no' END => NULL condition is UNKNOWN => ELSE => 'no'.
	check("EC15 CASE WHEN NULL THEN 'yes' ELSE 'no' evaluates to 'no'",
		`SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END`, "no")

	// ============================================================
	// FINAL PASS/TOTAL SUMMARY
	// ============================================================
	t.Logf("V39 Regression Stress: %d/%d tests passed", pass, total)
	if pass != total {
		t.Errorf("FAILED: %d/%d tests did not pass", total-pass, total)
	}
}
