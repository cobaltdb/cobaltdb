package test

import (
	"fmt"
	"strings"
	"testing"
)

func TestV101DeepCoverage(t *testing.T) {
	_ = fmt.Sprintf
	_ = strings.Contains

	t.Run("GroupByOrderBy", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()

		afExec(t, db, ctx, "CREATE TABLE gb_emp (id INTEGER PRIMARY KEY, dept TEXT, salary REAL, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO gb_emp VALUES (1, 'eng', 120000, 'Alice')")
		afExec(t, db, ctx, "INSERT INTO gb_emp VALUES (2, 'eng', 100000, 'Bob')")
		afExec(t, db, ctx, "INSERT INTO gb_emp VALUES (3, 'sales', 90000, 'Carol')")
		afExec(t, db, ctx, "INSERT INTO gb_emp VALUES (4, 'sales', 110000, 'Dave')")
		afExec(t, db, ctx, "INSERT INTO gb_emp VALUES (5, 'hr', 80000, 'Eve')")
		afExec(t, db, ctx, "INSERT INTO gb_emp VALUES (6, 'hr', NULL, 'Frank')")

		t.Run("AggInOrderBy", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT dept, SUM(salary) as s FROM gb_emp GROUP BY dept ORDER BY SUM(salary) DESC")
			if len(rows) != 3 {
				t.Fatalf("expected 3 rows, got %d", len(rows))
			}
			if fmt.Sprintf("%v", rows[0][0]) != "eng" {
				t.Fatalf("expected eng first, got %v", rows[0][0])
			}
		})

		t.Run("PositionalOrderBy", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT dept, COUNT(*) as cnt FROM gb_emp GROUP BY dept ORDER BY 1")
			if len(rows) != 3 {
				t.Fatalf("expected 3 rows, got %d", len(rows))
			}
			if fmt.Sprintf("%v", rows[0][0]) != "eng" {
				t.Fatalf("expected eng first, got %v", rows[0][0])
			}
			if fmt.Sprintf("%v", rows[2][0]) != "sales" {
				t.Fatalf("expected sales last, got %v", rows[2][0])
			}
		})

		t.Run("StringOrderByGroupBy", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT dept, MAX(name) as max_name FROM gb_emp GROUP BY dept ORDER BY dept DESC")
			if len(rows) != 3 {
				t.Fatalf("expected 3, got %d", len(rows))
			}
			if fmt.Sprintf("%v", rows[0][0]) != "sales" {
				t.Fatalf("expected sales first, got %v", rows[0][0])
			}
		})

		t.Run("NullOrderByGroupBy", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT dept, AVG(salary) as avg_sal FROM gb_emp GROUP BY dept ORDER BY AVG(salary)")
			if len(rows) != 3 {
				t.Fatalf("expected 3, got %d", len(rows))
			}
		})

		t.Run("ExprArgAggregate", func(t *testing.T) {
			afExec(t, db, ctx, "CREATE TABLE gb_orders (id INTEGER PRIMARY KEY, dept TEXT, price REAL, qty INTEGER)")
			afExec(t, db, ctx, "INSERT INTO gb_orders VALUES (1, 'A', 10.0, 5)")
			afExec(t, db, ctx, "INSERT INTO gb_orders VALUES (2, 'A', 20.0, 3)")
			afExec(t, db, ctx, "INSERT INTO gb_orders VALUES (3, 'B', 15.0, 2)")
			rows := afQuery(t, db, ctx, "SELECT dept, SUM(price * qty) as total FROM gb_orders GROUP BY dept ORDER BY SUM(price * qty) DESC")
			if len(rows) != 2 {
				t.Fatalf("expected 2, got %d", len(rows))
			}
			got := fmt.Sprintf("%.1f", rows[0][1])
			if got != "110.0" {
				t.Fatalf("expected 110.0, got %s", got)
			}
		})
	})

	t.Run("JoinGroupByAggregates", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE jg_dept (id INTEGER PRIMARY KEY, dname TEXT)")
		afExec(t, db, ctx, "INSERT INTO jg_dept VALUES (1, 'Engineering')")
		afExec(t, db, ctx, "INSERT INTO jg_dept VALUES (2, 'Sales')")
		afExec(t, db, ctx, "CREATE TABLE jg_emp (id INTEGER PRIMARY KEY, dept_id INTEGER, salary REAL, bonus REAL)")
		afExec(t, db, ctx, "INSERT INTO jg_emp VALUES (1, 1, 100, 10)")
		afExec(t, db, ctx, "INSERT INTO jg_emp VALUES (2, 1, 120, 15)")
		afExec(t, db, ctx, "INSERT INTO jg_emp VALUES (3, 2, 90, NULL)")
		afExec(t, db, ctx, "INSERT INTO jg_emp VALUES (4, 2, 80, 5)")

		t.Run("SUM_Join", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT jg_dept.dname, SUM(jg_emp.salary) FROM jg_dept JOIN jg_emp ON jg_dept.id = jg_emp.dept_id GROUP BY jg_dept.dname ORDER BY SUM(jg_emp.salary) DESC")
			if len(rows) != 2 {
				t.Fatalf("expected 2, got %d", len(rows))
			}
			if fmt.Sprintf("%.0f", rows[0][1]) != "220" {
				t.Fatalf("expected 220, got %v", rows[0][1])
			}
		})
		t.Run("AVG_Join", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT jg_dept.dname, AVG(jg_emp.salary) FROM jg_dept JOIN jg_emp ON jg_dept.id = jg_emp.dept_id GROUP BY jg_dept.dname ORDER BY jg_dept.dname")
			if len(rows) != 2 {
				t.Fatalf("expected 2, got %d", len(rows))
			}
			if fmt.Sprintf("%.0f", rows[0][1]) != "110" {
				t.Fatalf("expected 110, got %v", rows[0][1])
			}
		})
		t.Run("MinMax_Join", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT jg_dept.dname, MIN(jg_emp.salary), MAX(jg_emp.salary) FROM jg_dept JOIN jg_emp ON jg_dept.id = jg_emp.dept_id GROUP BY jg_dept.dname ORDER BY jg_dept.dname")
			if len(rows) != 2 {
				t.Fatalf("expected 2, got %d", len(rows))
			}
			if fmt.Sprintf("%v", rows[0][1]) != "100" {
				t.Fatalf("expected min 100, got %v", rows[0][1])
			}
			if fmt.Sprintf("%v", rows[0][2]) != "120" {
				t.Fatalf("expected max 120, got %v", rows[0][2])
			}
		})
		t.Run("Count_Join", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT jg_dept.dname, COUNT(*), COUNT(jg_emp.bonus) FROM jg_dept JOIN jg_emp ON jg_dept.id = jg_emp.dept_id GROUP BY jg_dept.dname ORDER BY jg_dept.dname")
			if len(rows) != 2 {
				t.Fatalf("expected 2, got %d", len(rows))
			}
			salesRow := rows[1]
			if fmt.Sprintf("%v", salesRow[1]) != "2" {
				t.Fatalf("expected COUNT(*) = 2, got %v", salesRow[1])
			}
			if fmt.Sprintf("%v", salesRow[2]) != "1" {
				t.Fatalf("expected COUNT(bonus) = 1, got %v", salesRow[2])
			}
		})
	})

	t.Run("OrderByEdgeCases", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE ob_items (id INTEGER PRIMARY KEY, name TEXT, price REAL, qty INTEGER)")
		afExec(t, db, ctx, "INSERT INTO ob_items VALUES (1, 'Widget', 10.0, 5)")
		afExec(t, db, ctx, "INSERT INTO ob_items VALUES (2, 'Gadget', 25.0, 2)")
		afExec(t, db, ctx, "INSERT INTO ob_items VALUES (3, 'Doohickey', 5.0, 10)")
		t.Run("QualifiedOrderBy", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT ob_items.name, ob_items.price FROM ob_items ORDER BY ob_items.price ASC")
			if len(rows) != 3 {
				t.Fatalf("expected 3, got %d", len(rows))
			}
			if fmt.Sprintf("%v", rows[0][0]) != "Doohickey" {
				t.Fatalf("expected Doohickey, got %v", rows[0][0])
			}
		})
		t.Run("ExpressionOrderBy", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT name, price * qty as total FROM ob_items ORDER BY price * qty DESC")
			if len(rows) != 3 {
				t.Fatalf("expected 3, got %d", len(rows))
			}
			got0 := fmt.Sprintf("%v", rows[0][1])
			if got0 != "50" && got0 != "50.0" {
				t.Fatalf("expected 50 or 50.0, got %s", got0)
			}
		})
	})

	t.Run("CorrelatedSubqueries", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE cs_t1 (id INTEGER PRIMARY KEY, val REAL, lo REAL, hi REAL, name TEXT, pattern TEXT)")
		afExec(t, db, ctx, "INSERT INTO cs_t1 VALUES (1, 5.0, 3.0, 7.0, 'alice', '%li%')")
		afExec(t, db, ctx, "INSERT INTO cs_t1 VALUES (2, NULL, 10.0, 20.0, 'bob', '%xx%')")
		afExec(t, db, ctx, "INSERT INTO cs_t1 VALUES (3, -3.0, 1.0, 2.0, 'charlie', '%ar%')")
		afExec(t, db, ctx, "CREATE TABLE cs_t2 (id INTEGER PRIMARY KEY, val REAL, a REAL, b REAL, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO cs_t2 VALUES (1, 5.0, 5.0, 10.0, 'alice_record')")
		afExec(t, db, ctx, "INSERT INTO cs_t2 VALUES (2, 15.0, 15.0, 20.0, 'bob_record')")
		afExec(t, db, ctx, "INSERT INTO cs_t2 VALUES (3, 1.5, 1.5, 3.0, 'charlie_record')")

		t.Run("BETWEEN", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT cs_t1.name FROM cs_t1 WHERE EXISTS (SELECT 1 FROM cs_t2 WHERE cs_t2.val BETWEEN cs_t1.lo AND cs_t1.hi)")
			if len(rows) < 1 {
				t.Fatal("expected at least 1 row")
			}
			found := false
			for _, r := range rows {
				if fmt.Sprintf("%v", r[0]) == "alice" {
					found = true
				}
			}
			if !found {
				t.Fatal("expected alice")
			}
		})
		t.Run("IsNull", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT cs_t1.name FROM cs_t1 WHERE EXISTS (SELECT 1 FROM cs_t2 WHERE cs_t1.val IS NOT NULL)")
			if len(rows) < 2 {
				t.Fatalf("expected >= 2, got %d", len(rows))
			}
		})
		t.Run("LIKE", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT cs_t1.name FROM cs_t1 WHERE EXISTS (SELECT 1 FROM cs_t2 WHERE cs_t2.name LIKE cs_t1.pattern)")
			if len(rows) < 1 {
				t.Fatalf("expected >= 1, got %d", len(rows))
			}
		})
		t.Run("CASE", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT cs_t1.name, (SELECT CASE WHEN cs_t1.val > 0 THEN cs_t2.a ELSE cs_t2.b END FROM cs_t2 WHERE cs_t2.id = 1) FROM cs_t1 WHERE cs_t1.id = 1")
			if len(rows) != 1 {
				t.Fatalf("expected 1, got %d", len(rows))
			}
			if fmt.Sprintf("%v", rows[0][1]) != "5" {
				t.Fatalf("expected 5, got %v", rows[0][1])
			}
		})
		t.Run("FunctionCall", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT cs_t1.name, (SELECT UPPER(cs_t1.name) FROM cs_t2 WHERE cs_t2.id = 1) FROM cs_t1 WHERE cs_t1.id = 1")
			if len(rows) != 1 {
				t.Fatalf("expected 1, got %d", len(rows))
			}
			if fmt.Sprintf("%v", rows[0][1]) != "ALICE" {
				t.Fatalf("expected ALICE, got %v", rows[0][1])
			}
		})
		t.Run("UnaryExpr", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT cs_t1.name, (SELECT -cs_t1.val FROM cs_t2 WHERE cs_t2.id = 1) FROM cs_t1 WHERE cs_t1.id = 3")
			if len(rows) != 1 {
				t.Fatalf("expected 1, got %d", len(rows))
			}
			if fmt.Sprintf("%.0f", rows[0][1]) != "3" {
				t.Fatalf("expected 3, got %v", rows[0][1])
			}
		})
		t.Run("IN", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT cs_t1.name FROM cs_t1 WHERE EXISTS (SELECT 1 FROM cs_t2 WHERE cs_t1.val IN (cs_t2.a, cs_t2.b))")
			if len(rows) < 1 {
				t.Fatalf("expected >= 1, got %d", len(rows))
			}
		})
		t.Run("AliasExpr", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT cs_t1.name, (SELECT cs_t1.val + cs_t2.a AS combined FROM cs_t2 WHERE cs_t2.id = 1) FROM cs_t1 WHERE cs_t1.id = 1")
			if len(rows) != 1 {
				t.Fatalf("expected 1, got %d", len(rows))
			}
			if fmt.Sprintf("%v", rows[0][1]) != "10" {
				t.Fatalf("expected 10, got %v", rows[0][1])
			}
		})
	})

	t.Run("DerivedTables", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		t.Run("UnionInDerived", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT * FROM (SELECT 1 as n UNION SELECT 2 UNION SELECT 3) sub ORDER BY n")
			if len(rows) != 3 {
				t.Fatalf("expected 3, got %d", len(rows))
			}
			if fmt.Sprintf("%v", rows[0][0]) != "1" {
				t.Fatalf("expected 1, got %v", rows[0][0])
			}
			if fmt.Sprintf("%v", rows[2][0]) != "3" {
				t.Fatalf("expected 3, got %v", rows[2][0])
			}
		})
		t.Run("SubqueryInFrom", func(t *testing.T) {
			afExec(t, db, ctx, "CREATE TABLE dt_data (id INTEGER PRIMARY KEY, val TEXT)")
			afExec(t, db, ctx, "INSERT INTO dt_data VALUES (1, 'alpha')")
			afExec(t, db, ctx, "INSERT INTO dt_data VALUES (2, 'beta')")
			rows := afQuery(t, db, ctx, "SELECT sub.id, sub.val FROM (SELECT id, val FROM dt_data) sub ORDER BY sub.id")
			if len(rows) != 2 {
				t.Fatalf("expected 2, got %d", len(rows))
			}
			if fmt.Sprintf("%v", rows[0][1]) != "alpha" {
				t.Fatalf("expected alpha, got %v", rows[0][1])
			}
		})
	})

	t.Run("Analyze", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE az_data (id INTEGER PRIMARY KEY, category TEXT, score REAL, notes TEXT)")
		afExec(t, db, ctx, "INSERT INTO az_data VALUES (1, 'A', 85.5, 'good')")
		afExec(t, db, ctx, "INSERT INTO az_data VALUES (2, 'B', 92.0, NULL)")
		afExec(t, db, ctx, "INSERT INTO az_data VALUES (3, 'A', 78.0, 'fair')")
		afExec(t, db, ctx, "INSERT INTO az_data VALUES (4, 'C', NULL, NULL)")
		afExec(t, db, ctx, "INSERT INTO az_data VALUES (5, 'B', 95.5, 'excellent')")
		afExec(t, db, ctx, "ANALYZE az_data")
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM az_data", float64(5))
		afExpectVal(t, db, ctx, "SELECT COUNT(notes) FROM az_data", float64(3))
		afExpectVal(t, db, ctx, "SELECT COUNT(score) FROM az_data", float64(4))
		afExpectVal(t, db, ctx, "SELECT MIN(score) FROM az_data", float64(78))
		afExpectVal(t, db, ctx, "SELECT MAX(score) FROM az_data", 95.5)
		afExpectRows(t, db, ctx, "SELECT DISTINCT category FROM az_data", 3)
	})

	t.Run("SaveLoad", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE sl_t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, status TEXT DEFAULT 'active', age INTEGER CHECK(age > 0))")
		afExec(t, db, ctx, "CREATE TABLE sl_t2 (id INTEGER PRIMARY KEY, ref_id INTEGER, val REAL)")
		afExec(t, db, ctx, "CREATE INDEX idx_sl_t2_ref ON sl_t2 (ref_id)")
		afExec(t, db, ctx, "INSERT INTO sl_t1 (id, name, age) VALUES (1, 'Alice', 30)")
		afExec(t, db, ctx, "INSERT INTO sl_t1 (id, name, age) VALUES (2, 'Bob', 25)")
		afExec(t, db, ctx, "INSERT INTO sl_t2 VALUES (1, 1, 100.5)")
		afExec(t, db, ctx, "INSERT INTO sl_t2 VALUES (2, 2, 200.0)")
		afExpectVal(t, db, ctx, "SELECT status FROM sl_t1 WHERE id = 1", "active")
		_, err := db.Exec(ctx, "INSERT INTO sl_t1 (id, name, age) VALUES (3, 'Charlie', -1)")
		if err == nil {
			t.Fatal("expected CHECK constraint error")
		}
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM sl_t1", float64(2))
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM sl_t2", float64(2))
	})

	t.Run("ForeignKeyOnDelete", func(t *testing.T) {
		t.Run("Cascade", func(t *testing.T) {
			db, ctx := af(t)
			defer db.Close()
			afExec(t, db, ctx, "CREATE TABLE fk_parent_c (id INTEGER PRIMARY KEY, name TEXT)")
			afExec(t, db, ctx, "CREATE TABLE fk_child_c (id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT, FOREIGN KEY (parent_id) REFERENCES fk_parent_c(id) ON DELETE CASCADE)")
			afExec(t, db, ctx, "INSERT INTO fk_parent_c VALUES (1, 'Parent1')")
			afExec(t, db, ctx, "INSERT INTO fk_parent_c VALUES (2, 'Parent2')")
			afExec(t, db, ctx, "INSERT INTO fk_child_c VALUES (1, 1, 'Child1a')")
			afExec(t, db, ctx, "INSERT INTO fk_child_c VALUES (2, 1, 'Child1b')")
			afExec(t, db, ctx, "INSERT INTO fk_child_c VALUES (3, 2, 'Child2a')")
			afExec(t, db, ctx, "DELETE FROM fk_parent_c WHERE id = 1")
			rows := afExpectRows(t, db, ctx, "SELECT * FROM fk_child_c", 1)
			if fmt.Sprintf("%v", rows[0][2]) != "Child2a" {
				t.Fatalf("expected Child2a, got %v", rows[0][2])
			}
		})
		t.Run("SetNull", func(t *testing.T) {
			db, ctx := af(t)
			defer db.Close()
			afExec(t, db, ctx, "CREATE TABLE fk_parent_sn (id INTEGER PRIMARY KEY, name TEXT)")
			afExec(t, db, ctx, "CREATE TABLE fk_child_sn (id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT, FOREIGN KEY (parent_id) REFERENCES fk_parent_sn(id) ON DELETE SET NULL)")
			afExec(t, db, ctx, "INSERT INTO fk_parent_sn VALUES (1, 'Parent1')")
			afExec(t, db, ctx, "INSERT INTO fk_parent_sn VALUES (2, 'Parent2')")
			afExec(t, db, ctx, "INSERT INTO fk_child_sn VALUES (1, 1, 'Child1a')")
			afExec(t, db, ctx, "INSERT INTO fk_child_sn VALUES (2, 1, 'Child1b')")
			afExec(t, db, ctx, "INSERT INTO fk_child_sn VALUES (3, 2, 'Child2a')")
			afExec(t, db, ctx, "DELETE FROM fk_parent_sn WHERE id = 1")
			rows := afExpectRows(t, db, ctx, "SELECT * FROM fk_child_sn ORDER BY id", 3)
			if rows[0][1] != nil {
				t.Fatalf("expected NULL, got %v", rows[0][1])
			}
			if rows[1][1] != nil {
				t.Fatalf("expected NULL, got %v", rows[1][1])
			}
			if fmt.Sprintf("%v", rows[2][1]) != "2" {
				t.Fatalf("expected 2, got %v", rows[2][1])
			}
		})
		t.Run("Restrict", func(t *testing.T) {
			db, ctx := af(t)
			defer db.Close()
			afExec(t, db, ctx, "CREATE TABLE fk_parent_r (id INTEGER PRIMARY KEY, name TEXT)")
			afExec(t, db, ctx, "CREATE TABLE fk_child_r (id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT, FOREIGN KEY (parent_id) REFERENCES fk_parent_r(id))")
			afExec(t, db, ctx, "INSERT INTO fk_parent_r VALUES (1, 'Parent1')")
			afExec(t, db, ctx, "INSERT INTO fk_child_r VALUES (1, 1, 'Child1')")
			_, err := db.Exec(ctx, "DELETE FROM fk_parent_r WHERE id = 1")
			if err == nil {
				t.Fatal("expected RESTRICT error")
			}
			afExpectRows(t, db, ctx, "SELECT * FROM fk_parent_r", 1)
			afExpectRows(t, db, ctx, "SELECT * FROM fk_child_r", 1)
		})
	})

	t.Run("WindowFunctionEdgeCases", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE wf_data (id INTEGER PRIMARY KEY, grp TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO wf_data VALUES (1, 'X', 10)")
		afExec(t, db, ctx, "INSERT INTO wf_data VALUES (2, 'X', 20)")
		afExec(t, db, ctx, "INSERT INTO wf_data VALUES (3, 'X', 30)")
		afExec(t, db, ctx, "INSERT INTO wf_data VALUES (4, 'Y', 15)")
		afExec(t, db, ctx, "INSERT INTO wf_data VALUES (5, 'Y', NULL)")
		afExec(t, db, ctx, "INSERT INTO wf_data VALUES (6, 'Y', 25)")

		t.Run("RunningSUM", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT id, grp, amount, SUM(amount) OVER (PARTITION BY grp ORDER BY id) as running_sum FROM wf_data ORDER BY grp, id")
			if len(rows) != 6 {
				t.Fatalf("expected 6, got %d", len(rows))
			}
			for _, r := range rows {
				if fmt.Sprintf("%v", r[0]) == "1" && fmt.Sprintf("%.0f", r[3]) != "10" {
					t.Fatalf("id=1 sum expected 10, got %v", r[3])
				}
				if fmt.Sprintf("%v", r[0]) == "3" && fmt.Sprintf("%.0f", r[3]) != "60" {
					t.Fatalf("id=3 sum expected 60, got %v", r[3])
				}
			}
		})
		t.Run("PartitionAVG", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT id, grp, amount, AVG(amount) OVER (PARTITION BY grp) as grp_avg FROM wf_data ORDER BY grp, id")
			if len(rows) != 6 {
				t.Fatalf("expected 6, got %d", len(rows))
			}
			for _, r := range rows {
				if fmt.Sprintf("%v", r[1]) == "X" {
					if fmt.Sprintf("%.0f", r[3]) != "20" {
						t.Fatalf("X avg expected 20, got %v", r[3])
					}
					break
				}
			}
		})
		t.Run("PartitionMinMax", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT id, grp, MIN(amount) OVER (PARTITION BY grp) as grp_min, MAX(amount) OVER (PARTITION BY grp) as grp_max FROM wf_data ORDER BY grp, id")
			if len(rows) != 6 {
				t.Fatalf("expected 6, got %d", len(rows))
			}
			for _, r := range rows {
				if fmt.Sprintf("%v", r[1]) == "X" {
					if fmt.Sprintf("%v", r[2]) != "10" {
						t.Fatalf("X min expected 10, got %v", r[2])
					}
					if fmt.Sprintf("%v", r[3]) != "30" {
						t.Fatalf("X max expected 30, got %v", r[3])
					}
					break
				}
			}
		})
		t.Run("RunningCount", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT id, grp, COUNT(*) OVER (PARTITION BY grp ORDER BY id) as running_count FROM wf_data ORDER BY grp, id")
			if len(rows) != 6 {
				t.Fatalf("expected 6, got %d", len(rows))
			}
			for _, r := range rows {
				if fmt.Sprintf("%v", r[0]) == "3" && fmt.Sprintf("%v", r[2]) != "3" {
					t.Fatalf("id=3 count expected 3, got %v", r[2])
				}
			}
		})
		t.Run("WindowNullValues", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT id, grp, SUM(amount) OVER (PARTITION BY grp) as grp_sum FROM wf_data ORDER BY grp, id")
			if len(rows) != 6 {
				t.Fatalf("expected 6, got %d", len(rows))
			}
			for _, r := range rows {
				if fmt.Sprintf("%v", r[0]) == "4" {
					if fmt.Sprintf("%.0f", r[2]) != "40" {
						t.Fatalf("Y sum expected 40, got %v", r[2])
					}
					break
				}
			}
		})
	})

	t.Run("JSONFunctions", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()

		t.Run("JSON_QUOTE", func(t *testing.T) {
			rows := afQuery(t, db, ctx, `SELECT JSON_QUOTE('hello world')`)
			if len(rows) == 0 || len(rows[0]) == 0 {
				t.Fatal("expected result")
			}
			got := fmt.Sprintf("%v", rows[0][0])
			if !strings.Contains(got, "hello world") {
				t.Fatalf("expected quoted, got %v", got)
			}
		})

		t.Run("JSON_PRETTY", func(t *testing.T) {
			rows := afQuery(t, db, ctx, `SELECT JSON_PRETTY('{"a":1,"b":2}')`)
			if len(rows) == 0 || len(rows[0]) == 0 {
				t.Fatal("expected result")
			}
			result := fmt.Sprintf("%v", rows[0][0])
			if !strings.Contains(result, "a") {
				t.Fatalf("expected prettified, got %v", result)
			}
		})

		t.Run("JSON_MINIFY", func(t *testing.T) {
			rows := afQuery(t, db, ctx, `SELECT JSON_MINIFY('{ "a" : 1 , "b" : 2 }')`)
			if len(rows) == 0 || len(rows[0]) == 0 {
				t.Fatal("expected result")
			}
			result := fmt.Sprintf("%v", rows[0][0])
			if strings.Contains(result, " : ") {
				t.Fatalf("expected minified, got %v", result)
			}
		})

		t.Run("JSON_TYPE_array", func(t *testing.T) {
			afExpectVal(t, db, ctx, `SELECT JSON_TYPE('[1,2,3]')`, "array")
		})

		t.Run("JSON_TYPE_object", func(t *testing.T) {
			afExpectVal(t, db, ctx, `SELECT JSON_TYPE('{"k":"v"}')`, "object")
		})

		t.Run("JSON_TYPE_string", func(t *testing.T) {
			afExpectVal(t, db, ctx, `SELECT JSON_TYPE('"hello"')`, "string")
		})

		t.Run("JSON_TYPE_number", func(t *testing.T) {
			afExpectVal(t, db, ctx, "SELECT JSON_TYPE('42')", "number")
		})

		t.Run("JSON_TYPE_bool", func(t *testing.T) {
			afExpectVal(t, db, ctx, "SELECT JSON_TYPE('true')", "boolean")
		})

		t.Run("JSON_TYPE_null_val", func(t *testing.T) {
			afExpectVal(t, db, ctx, "SELECT JSON_TYPE('null')", "null")
		})
	})
}
