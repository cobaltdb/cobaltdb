package test

import (
	"fmt"
	"testing"
)

// =============================================================================
// TestV95_ edge case bug-hunting tests targeting:
//   1. DELETE with UNIQUE index cleanup
//   2. UPDATE that changes UNIQUE column value
//   3. Multi-column (composite) index operations
//   4. Transaction rollback with index changes
//   5. ALTER TABLE DROP COLUMN on indexed column
//   6. INSERT NULL into PRIMARY KEY
//   7. Multiple JOINs (3+ tables) with GROUP BY
//   8. Recursive CTEs
//   9. Window functions with PARTITION BY
//  10. COALESCE / NULLIF / IFNULL edge cases
// =============================================================================

// ---------------------------------------------------------------------------
// 1. DELETE with UNIQUE index cleanup
// ---------------------------------------------------------------------------

func TestV95_DeleteUniqueIndexCleanup(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_del_uniq (id INTEGER PRIMARY KEY, email TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_del_email ON t95_del_uniq(email)")
	afExec(t, db, ctx, "INSERT INTO t95_del_uniq VALUES (1, 'alice@test.com')")
	afExec(t, db, ctx, "INSERT INTO t95_del_uniq VALUES (2, 'bob@test.com')")

	// Delete the row with 'alice@test.com'
	afExec(t, db, ctx, "DELETE FROM t95_del_uniq WHERE id = 1")

	// Now re-inserting the same email should succeed if index was cleaned up
	_, err := db.Exec(ctx, "INSERT INTO t95_del_uniq VALUES (3, 'alice@test.com')")
	if err != nil {
		t.Fatalf("BUG: UNIQUE index entry not cleaned up after DELETE: %v", err)
	}

	// Verify the table state
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t95_del_uniq", 2)
	afExpectVal(t, db, ctx, "SELECT id FROM t95_del_uniq WHERE email = 'alice@test.com'", 3)
}

func TestV95_DeleteAllThenReinsertUnique(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_del_all (id INTEGER PRIMARY KEY, code TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_del_all_code ON t95_del_all(code)")
	afExec(t, db, ctx, "INSERT INTO t95_del_all VALUES (1, 'AAA')")
	afExec(t, db, ctx, "INSERT INTO t95_del_all VALUES (2, 'BBB')")
	afExec(t, db, ctx, "INSERT INTO t95_del_all VALUES (3, 'CCC')")

	// Delete all rows
	afExec(t, db, ctx, "DELETE FROM t95_del_all")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t95_del_all", 0)

	// Re-insert the same codes -- should succeed if all index entries were cleaned up
	afExec(t, db, ctx, "INSERT INTO t95_del_all VALUES (10, 'AAA')")
	afExec(t, db, ctx, "INSERT INTO t95_del_all VALUES (20, 'BBB')")
	afExec(t, db, ctx, "INSERT INTO t95_del_all VALUES (30, 'CCC')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t95_del_all", 3)
}

func TestV95_DeleteWithWhereUniqueIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_del_where (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_dw_name ON t95_del_where(name)")
	afExec(t, db, ctx, "INSERT INTO t95_del_where VALUES (1, 'Alpha')")
	afExec(t, db, ctx, "INSERT INTO t95_del_where VALUES (2, 'Beta')")
	afExec(t, db, ctx, "INSERT INTO t95_del_where VALUES (3, 'Gamma')")

	// Delete only Beta
	afExec(t, db, ctx, "DELETE FROM t95_del_where WHERE name = 'Beta'")

	// Alpha and Gamma still exist -- must reject duplicate
	_, err := db.Exec(ctx, "INSERT INTO t95_del_where VALUES (4, 'Alpha')")
	if err == nil {
		t.Fatal("BUG: UNIQUE constraint should reject duplicate 'Alpha'")
	}

	// Beta was deleted -- must accept
	_, err = db.Exec(ctx, "INSERT INTO t95_del_where VALUES (5, 'Beta')")
	if err != nil {
		t.Fatalf("BUG: Re-insert 'Beta' after deletion failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 2. UPDATE that changes a UNIQUE column value
// ---------------------------------------------------------------------------

func TestV95_UpdateUniqueColumnValue(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_upd_uniq (id INTEGER PRIMARY KEY, email TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_upd_email ON t95_upd_uniq(email)")
	afExec(t, db, ctx, "INSERT INTO t95_upd_uniq VALUES (1, 'old@test.com')")
	afExec(t, db, ctx, "INSERT INTO t95_upd_uniq VALUES (2, 'other@test.com')")

	// Change email from old to new
	afExec(t, db, ctx, "UPDATE t95_upd_uniq SET email = 'new@test.com' WHERE id = 1")

	// Old value should be freed -- inserting it should work
	_, err := db.Exec(ctx, "INSERT INTO t95_upd_uniq VALUES (3, 'old@test.com')")
	if err != nil {
		t.Fatalf("BUG: Old UNIQUE value 'old@test.com' still in index after UPDATE: %v", err)
	}

	// New value is taken -- inserting it should fail
	_, err = db.Exec(ctx, "INSERT INTO t95_upd_uniq VALUES (4, 'new@test.com')")
	if err == nil {
		t.Fatal("BUG: UNIQUE constraint should reject duplicate 'new@test.com' after UPDATE")
	}
}

func TestV95_UpdateUniqueToExistingValueFails(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_upd_dup (id INTEGER PRIMARY KEY, code TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_upd_dup ON t95_upd_dup(code)")
	afExec(t, db, ctx, "INSERT INTO t95_upd_dup VALUES (1, 'X')")
	afExec(t, db, ctx, "INSERT INTO t95_upd_dup VALUES (2, 'Y')")

	// Attempt to change code of row 2 to 'X' -- should fail
	_, err := db.Exec(ctx, "UPDATE t95_upd_dup SET code = 'X' WHERE id = 2")
	if err == nil {
		t.Fatal("BUG: UPDATE to existing UNIQUE value should fail")
	}

	// Data should be unchanged
	afExpectVal(t, db, ctx, "SELECT code FROM t95_upd_dup WHERE id = 2", "Y")
}

func TestV95_UpdateSwapUniqueValues(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// This tests updating unique values in sequence (not a true swap, but close)
	afExec(t, db, ctx, "CREATE TABLE t95_swap (id INTEGER PRIMARY KEY, tag TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_swap ON t95_swap(tag)")
	afExec(t, db, ctx, "INSERT INTO t95_swap VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t95_swap VALUES (2, 'B')")

	// First, change A -> C (freeing 'A')
	afExec(t, db, ctx, "UPDATE t95_swap SET tag = 'C' WHERE id = 1")
	// Then change B -> A (which should now be free)
	afExec(t, db, ctx, "UPDATE t95_swap SET tag = 'A' WHERE id = 2")

	afExpectVal(t, db, ctx, "SELECT tag FROM t95_swap WHERE id = 1", "C")
	afExpectVal(t, db, ctx, "SELECT tag FROM t95_swap WHERE id = 2", "A")
}

// ---------------------------------------------------------------------------
// 3. Multi-column (composite) indexes
// ---------------------------------------------------------------------------

func TestV95_CompositeIndexInsertDuplicate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_comp (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_comp_ab ON t95_comp(a, b)")
	afExec(t, db, ctx, "INSERT INTO t95_comp VALUES (1, 'x', 'y', 'data1')")

	// Same (a,b) pair should fail
	_, err := db.Exec(ctx, "INSERT INTO t95_comp VALUES (2, 'x', 'y', 'data2')")
	if err == nil {
		t.Fatal("BUG: Composite UNIQUE index should reject duplicate (a,b) pair")
	}

	// Different b with same a should succeed
	afExec(t, db, ctx, "INSERT INTO t95_comp VALUES (3, 'x', 'z', 'data3')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t95_comp", 2)
}

func TestV95_CompositeIndexDeleteAndReinsert(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_comp2 (id INTEGER PRIMARY KEY, dept TEXT, emp_id TEXT, name TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_comp2 ON t95_comp2(dept, emp_id)")
	afExec(t, db, ctx, "INSERT INTO t95_comp2 VALUES (1, 'ENG', 'E001', 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t95_comp2 VALUES (2, 'MKT', 'E001', 'Bob')")

	// Delete Alice
	afExec(t, db, ctx, "DELETE FROM t95_comp2 WHERE id = 1")

	// Re-insert (ENG, E001) -- should succeed after deletion
	_, err := db.Exec(ctx, "INSERT INTO t95_comp2 VALUES (3, 'ENG', 'E001', 'Carol')")
	if err != nil {
		t.Fatalf("BUG: Composite index not cleaned up after DELETE: %v", err)
	}

	// (MKT, E001) still occupied
	_, err = db.Exec(ctx, "INSERT INTO t95_comp2 VALUES (4, 'MKT', 'E001', 'Dave')")
	if err == nil {
		t.Fatal("BUG: Composite UNIQUE should reject duplicate (MKT, E001)")
	}
}

func TestV95_CompositeIndexUpdate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_comp3 (id INTEGER PRIMARY KEY, region TEXT, sku TEXT, qty INTEGER)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_comp3 ON t95_comp3(region, sku)")
	afExec(t, db, ctx, "INSERT INTO t95_comp3 VALUES (1, 'US', 'WIDGET', 10)")
	afExec(t, db, ctx, "INSERT INTO t95_comp3 VALUES (2, 'EU', 'WIDGET', 20)")

	// Update region of row 1 from US to EU -- (EU, WIDGET) already exists
	_, err := db.Exec(ctx, "UPDATE t95_comp3 SET region = 'EU' WHERE id = 1")
	if err == nil {
		t.Fatal("BUG: Composite UNIQUE should reject UPDATE that creates duplicate")
	}

	// Update to a free combination should work
	afExec(t, db, ctx, "UPDATE t95_comp3 SET region = 'AP' WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT region FROM t95_comp3 WHERE id = 1", "AP")
}

// ---------------------------------------------------------------------------
// 4. Transaction rollback with index changes
// ---------------------------------------------------------------------------

func TestV95_TxnRollbackInsertUniqueIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_txn_ins (id INTEGER PRIMARY KEY, code TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_txn_code ON t95_txn_ins(code)")
	afExec(t, db, ctx, "INSERT INTO t95_txn_ins VALUES (1, 'EXISTING')")

	// Begin, insert, then rollback
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO t95_txn_ins VALUES (2, 'TEMP')")
	afExec(t, db, ctx, "ROLLBACK")

	// 'TEMP' should be available again after rollback
	_, err := db.Exec(ctx, "INSERT INTO t95_txn_ins VALUES (3, 'TEMP')")
	if err != nil {
		t.Fatalf("BUG: Index entry for 'TEMP' not rolled back: %v", err)
	}

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t95_txn_ins", 2)
}

func TestV95_TxnRollbackDeleteUniqueIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_txn_del (id INTEGER PRIMARY KEY, tag TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_txn_del ON t95_txn_del(tag)")
	afExec(t, db, ctx, "INSERT INTO t95_txn_del VALUES (1, 'KEEP')")

	// Begin, delete, then rollback
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "DELETE FROM t95_txn_del WHERE id = 1")
	afExec(t, db, ctx, "ROLLBACK")

	// After rollback, 'KEEP' should still be in the index
	_, err := db.Exec(ctx, "INSERT INTO t95_txn_del VALUES (2, 'KEEP')")
	if err == nil {
		t.Fatal("BUG: UNIQUE index entry restored after rollback -- duplicate should be rejected")
	}

	afExpectVal(t, db, ctx, "SELECT tag FROM t95_txn_del WHERE id = 1", "KEEP")
}

func TestV95_TxnRollbackUpdateUniqueIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_txn_upd (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_txn_upd ON t95_txn_upd(val)")
	afExec(t, db, ctx, "INSERT INTO t95_txn_upd VALUES (1, 'OLD')")

	// Begin, update, then rollback
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "UPDATE t95_txn_upd SET val = 'NEW' WHERE id = 1")
	afExec(t, db, ctx, "ROLLBACK")

	// After rollback, 'OLD' should still be taken in the index
	_, err := db.Exec(ctx, "INSERT INTO t95_txn_upd VALUES (2, 'OLD')")
	if err == nil {
		t.Fatal("BUG: After rollback of UPDATE, old UNIQUE value should still be in index")
	}

	// 'NEW' should be free because the update was rolled back
	_, err = db.Exec(ctx, "INSERT INTO t95_txn_upd VALUES (3, 'NEW')")
	if err != nil {
		t.Fatalf("BUG: 'NEW' index entry not cleaned up after rollback of UPDATE: %v", err)
	}
}

func TestV95_TxnCommitThenCheckIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_txn_commit (id INTEGER PRIMARY KEY, code TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t95_txn_commit ON t95_txn_commit(code)")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO t95_txn_commit VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t95_txn_commit VALUES (2, 'B')")
	afExec(t, db, ctx, "COMMIT")

	// After commit, both should be enforced
	_, err := db.Exec(ctx, "INSERT INTO t95_txn_commit VALUES (3, 'A')")
	if err == nil {
		t.Fatal("BUG: UNIQUE constraint should be enforced after COMMIT")
	}

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t95_txn_commit", 2)
}

// ---------------------------------------------------------------------------
// 5. ALTER TABLE DROP COLUMN on indexed column
// ---------------------------------------------------------------------------

func TestV95_AlterDropIndexedColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_drop_idx (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	afExec(t, db, ctx, "CREATE INDEX idx_t95_drop_score ON t95_drop_idx(score)")
	afExec(t, db, ctx, "INSERT INTO t95_drop_idx VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO t95_drop_idx VALUES (2, 'Bob', 90)")

	// Drop the indexed column -- should not crash and data should remain accessible
	_, err := db.Exec(ctx, "ALTER TABLE t95_drop_idx DROP COLUMN score")
	if err != nil {
		t.Fatalf("ALTER TABLE DROP COLUMN on indexed column failed: %v", err)
	}

	// Table should still be queryable
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t95_drop_idx", 2)
	afExpectVal(t, db, ctx, "SELECT name FROM t95_drop_idx WHERE id = 1", "Alice")
}

func TestV95_AlterDropColumnThenInsert(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_drop_ins (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)")
	afExec(t, db, ctx, "INSERT INTO t95_drop_ins VALUES (1, 'x', 'y', 'z')")

	afExec(t, db, ctx, "ALTER TABLE t95_drop_ins DROP COLUMN b")

	// Insert with new schema (id, a, c)
	afExec(t, db, ctx, "INSERT INTO t95_drop_ins (id, a, c) VALUES (2, 'p', 'q')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t95_drop_ins", 2)
	afExpectVal(t, db, ctx, "SELECT c FROM t95_drop_ins WHERE id = 2", "q")
}

// ---------------------------------------------------------------------------
// 6. INSERT NULL into PRIMARY KEY
// ---------------------------------------------------------------------------

func TestV95_InsertNullPrimaryKey(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_null_pk (id INTEGER PRIMARY KEY, name TEXT)")

	// Attempt to insert NULL as PK -- should fail or auto-generate
	_, err := db.Exec(ctx, "INSERT INTO t95_null_pk (id, name) VALUES (NULL, 'test')")
	// For INTEGER PRIMARY KEY, SQLite auto-generates; CobaltDB may reject or auto-gen
	// Either behavior is acceptable -- what matters is no crash
	if err != nil {
		t.Logf("INSERT NULL PK correctly rejected: %v", err)
	} else {
		// If it succeeded, there should be exactly one row with a non-null PK
		rows := afQuery(t, db, ctx, "SELECT id FROM t95_null_pk")
		if len(rows) == 1 {
			t.Logf("INSERT NULL PK auto-generated ID: %v", rows[0][0])
		}
	}
}

func TestV95_InsertExplicitNullIntoPKColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// TEXT primary key -- NULL should be rejected
	afExec(t, db, ctx, "CREATE TABLE t95_null_pk2 (code TEXT PRIMARY KEY, description TEXT)")
	_, err := db.Exec(ctx, "INSERT INTO t95_null_pk2 (code, description) VALUES (NULL, 'test')")
	if err == nil {
		// Check if a row with NULL PK was actually stored
		rows := afQuery(t, db, ctx, "SELECT code FROM t95_null_pk2")
		if len(rows) > 0 && rows[0][0] == nil {
			t.Fatal("BUG: NULL value stored as PRIMARY KEY")
		}
		t.Logf("INSERT NULL into TEXT PK: row stored with code=%v", rows[0][0])
	} else {
		t.Logf("INSERT NULL into TEXT PK correctly rejected: %v", err)
	}
}

func TestV95_DuplicatePrimaryKey(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_dup_pk (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t95_dup_pk VALUES (1, 'first')")

	_, err := db.Exec(ctx, "INSERT INTO t95_dup_pk VALUES (1, 'second')")
	if err == nil {
		t.Fatal("BUG: Duplicate PRIMARY KEY should be rejected")
	}

	afExpectVal(t, db, ctx, "SELECT val FROM t95_dup_pk WHERE id = 1", "first")
}

// ---------------------------------------------------------------------------
// 7. Multiple JOINs (3+ tables) with GROUP BY
// ---------------------------------------------------------------------------

func TestV95_ThreeTableJoinGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_dept (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t95_emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL)")
	afExec(t, db, ctx, "CREATE TABLE t95_proj (id INTEGER PRIMARY KEY, name TEXT, lead_id INTEGER)")

	afExec(t, db, ctx, "INSERT INTO t95_dept VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO t95_dept VALUES (2, 'Marketing')")

	afExec(t, db, ctx, "INSERT INTO t95_emp VALUES (1, 'Alice', 1, 120000)")
	afExec(t, db, ctx, "INSERT INTO t95_emp VALUES (2, 'Bob', 1, 100000)")
	afExec(t, db, ctx, "INSERT INTO t95_emp VALUES (3, 'Carol', 2, 90000)")
	afExec(t, db, ctx, "INSERT INTO t95_emp VALUES (4, 'Dave', 2, 85000)")

	afExec(t, db, ctx, "INSERT INTO t95_proj VALUES (1, 'Alpha', 1)")
	afExec(t, db, ctx, "INSERT INTO t95_proj VALUES (2, 'Beta', 2)")
	afExec(t, db, ctx, "INSERT INTO t95_proj VALUES (3, 'Gamma', 3)")

	// 3-table JOIN with GROUP BY: count projects per department
	rows := afQuery(t, db, ctx, `
		SELECT d.name, COUNT(DISTINCT p.id) as proj_count
		FROM t95_dept d
		INNER JOIN t95_emp e ON e.dept_id = d.id
		INNER JOIN t95_proj p ON p.lead_id = e.id
		GROUP BY d.name
		ORDER BY d.name
	`)
	if len(rows) != 2 {
		t.Fatalf("Expected 2 department rows, got %d: %v", len(rows), rows)
	}
	// Engineering: Alice(Alpha), Bob(Beta) = 2 projects
	if fmt.Sprintf("%v", rows[0][0]) != "Engineering" {
		t.Errorf("Expected 'Engineering' first, got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[0][1]) != "2" {
		t.Errorf("Expected 2 projects for Engineering, got %v", rows[0][1])
	}
}

func TestV95_ThreeTableJoinWithAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_cust (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t95_ord (id INTEGER PRIMARY KEY, cust_id INTEGER, status TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t95_item (id INTEGER PRIMARY KEY, ord_id INTEGER, amount REAL)")

	afExec(t, db, ctx, "INSERT INTO t95_cust VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t95_cust VALUES (2, 'Bob')")

	afExec(t, db, ctx, "INSERT INTO t95_ord VALUES (1, 1, 'completed')")
	afExec(t, db, ctx, "INSERT INTO t95_ord VALUES (2, 1, 'completed')")
	afExec(t, db, ctx, "INSERT INTO t95_ord VALUES (3, 2, 'pending')")

	afExec(t, db, ctx, "INSERT INTO t95_item VALUES (1, 1, 100.0)")
	afExec(t, db, ctx, "INSERT INTO t95_item VALUES (2, 1, 200.0)")
	afExec(t, db, ctx, "INSERT INTO t95_item VALUES (3, 2, 150.0)")
	afExec(t, db, ctx, "INSERT INTO t95_item VALUES (4, 3, 75.0)")

	// Total amount per customer across all their orders
	rows := afQuery(t, db, ctx, `
		SELECT c.name, SUM(i.amount) as total
		FROM t95_cust c
		INNER JOIN t95_ord o ON o.cust_id = c.id
		INNER JOIN t95_item i ON i.ord_id = o.id
		GROUP BY c.name
		ORDER BY c.name
	`)
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d: %v", len(rows), rows)
	}
	// Alice: orders 1,2 -> items(100+200+150) = 450
	if fmt.Sprintf("%v", rows[0][1]) != "450" {
		t.Errorf("Alice total: expected 450, got %v", rows[0][1])
	}
	// Bob: order 3 -> items(75)
	if fmt.Sprintf("%v", rows[1][1]) != "75" {
		t.Errorf("Bob total: expected 75, got %v", rows[1][1])
	}
}

func TestV95_FourTableJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_country (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t95_city (id INTEGER PRIMARY KEY, name TEXT, country_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t95_store (id INTEGER PRIMARY KEY, name TEXT, city_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t95_sale (id INTEGER PRIMARY KEY, store_id INTEGER, revenue REAL)")

	afExec(t, db, ctx, "INSERT INTO t95_country VALUES (1, 'USA')")
	afExec(t, db, ctx, "INSERT INTO t95_country VALUES (2, 'UK')")
	afExec(t, db, ctx, "INSERT INTO t95_city VALUES (1, 'NYC', 1)")
	afExec(t, db, ctx, "INSERT INTO t95_city VALUES (2, 'London', 2)")
	afExec(t, db, ctx, "INSERT INTO t95_store VALUES (1, 'StoreA', 1)")
	afExec(t, db, ctx, "INSERT INTO t95_store VALUES (2, 'StoreB', 2)")
	afExec(t, db, ctx, "INSERT INTO t95_sale VALUES (1, 1, 1000)")
	afExec(t, db, ctx, "INSERT INTO t95_sale VALUES (2, 1, 2000)")
	afExec(t, db, ctx, "INSERT INTO t95_sale VALUES (3, 2, 500)")

	// 4-table join: revenue per country
	rows := afQuery(t, db, ctx, `
		SELECT co.name, SUM(sa.revenue) as total_revenue
		FROM t95_country co
		INNER JOIN t95_city ci ON ci.country_id = co.id
		INNER JOIN t95_store st ON st.city_id = ci.id
		INNER JOIN t95_sale sa ON sa.store_id = st.id
		GROUP BY co.name
		ORDER BY co.name
	`)
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d: %v", len(rows), rows)
	}
	if fmt.Sprintf("%v", rows[1][1]) != "3000" {
		t.Errorf("USA total: expected 3000, got %v", rows[1][1])
	}
	if fmt.Sprintf("%v", rows[0][1]) != "500" {
		t.Errorf("UK total: expected 500, got %v", rows[0][1])
	}
}

// ---------------------------------------------------------------------------
// 8. Recursive CTEs
// ---------------------------------------------------------------------------

func TestV95_RecursiveCTEFibonacci(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Generate Fibonacci-like sequence: 1, 1, 2, 3, 5, 8 ...
	rows := afQuery(t, db, ctx, `
		WITH RECURSIVE fib(a, b) AS (
			SELECT 1, 1
			UNION ALL
			SELECT b, a + b FROM fib WHERE b < 50
		)
		SELECT a FROM fib
	`)
	if len(rows) < 5 {
		t.Fatalf("Expected at least 5 Fibonacci rows, got %d: %v", len(rows), rows)
	}
	// First values should be 1, 1, 2, 3, 5
	expected := []string{"1", "1", "2", "3", "5"}
	for i, exp := range expected {
		if i < len(rows) {
			got := fmt.Sprintf("%v", rows[i][0])
			if got != exp {
				t.Errorf("Fibonacci[%d]: expected %s, got %s", i, exp, got)
			}
		}
	}
}

func TestV95_RecursiveCTEHierarchicalPath(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_org (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t95_org VALUES (1, 'CEO', NULL)")
	afExec(t, db, ctx, "INSERT INTO t95_org VALUES (2, 'VP_Eng', 1)")
	afExec(t, db, ctx, "INSERT INTO t95_org VALUES (3, 'VP_Sales', 1)")
	afExec(t, db, ctx, "INSERT INTO t95_org VALUES (4, 'Dev_Lead', 2)")
	afExec(t, db, ctx, "INSERT INTO t95_org VALUES (5, 'Dev1', 4)")

	// Count all employees under CEO (including CEO)
	rows := afQuery(t, db, ctx, `
		WITH RECURSIVE chain(id, name, manager_id) AS (
			SELECT id, name, manager_id FROM t95_org WHERE id = 1
			UNION ALL
			SELECT o.id, o.name, o.manager_id
			FROM t95_org o
			INNER JOIN chain c ON o.manager_id = c.id
		)
		SELECT COUNT(*) FROM chain
	`)
	if len(rows) != 1 || fmt.Sprintf("%v", rows[0][0]) != "5" {
		t.Fatalf("Expected 5 total in org hierarchy, got %v", rows)
	}
}

func TestV95_RecursiveCTEWithLimit(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Generate sequence 1..100 with LIMIT 5
	rows := afQuery(t, db, ctx, `
		WITH RECURSIVE seq(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 100
		)
		SELECT n FROM seq LIMIT 5
	`)
	if len(rows) != 5 {
		t.Fatalf("Expected 5 rows with LIMIT, got %d", len(rows))
	}
	for i, row := range rows {
		expected := fmt.Sprintf("%d", i+1)
		if fmt.Sprintf("%v", row[0]) != expected {
			t.Errorf("Row %d: expected %s, got %v", i, expected, row[0])
		}
	}
}

// ---------------------------------------------------------------------------
// 9. Window functions with PARTITION BY
// ---------------------------------------------------------------------------

func TestV95_WindowRowNumberPartition(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_win (id INTEGER PRIMARY KEY, dept TEXT, name TEXT, salary REAL)")
	afExec(t, db, ctx, "INSERT INTO t95_win VALUES (1, 'Eng', 'Alice', 120000)")
	afExec(t, db, ctx, "INSERT INTO t95_win VALUES (2, 'Eng', 'Bob', 100000)")
	afExec(t, db, ctx, "INSERT INTO t95_win VALUES (3, 'Eng', 'Carol', 110000)")
	afExec(t, db, ctx, "INSERT INTO t95_win VALUES (4, 'Sales', 'Dave', 90000)")
	afExec(t, db, ctx, "INSERT INTO t95_win VALUES (5, 'Sales', 'Eve', 95000)")

	rows := afQuery(t, db, ctx, `
		SELECT name, dept, salary,
			ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
		FROM t95_win
	`)
	if len(rows) != 5 {
		t.Fatalf("Expected 5 rows, got %d", len(rows))
	}

	// Build a map for easy checking
	results := make(map[string]string) // name -> row_number
	for _, row := range rows {
		name := fmt.Sprintf("%v", row[0])
		rn := fmt.Sprintf("%v", row[3])
		results[name] = rn
	}

	// In Eng: Alice(120k)=1, Carol(110k)=2, Bob(100k)=3
	if results["Alice"] != "1" {
		t.Errorf("Alice should be rn=1 in Eng, got %s", results["Alice"])
	}
	if results["Carol"] != "2" {
		t.Errorf("Carol should be rn=2 in Eng, got %s", results["Carol"])
	}
	if results["Bob"] != "3" {
		t.Errorf("Bob should be rn=3 in Eng, got %s", results["Bob"])
	}
	// In Sales: Eve(95k)=1, Dave(90k)=2
	if results["Eve"] != "1" {
		t.Errorf("Eve should be rn=1 in Sales, got %s", results["Eve"])
	}
	if results["Dave"] != "2" {
		t.Errorf("Dave should be rn=2 in Sales, got %s", results["Dave"])
	}
}

func TestV95_WindowSumPartition(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_win2 (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t95_win2 VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t95_win2 VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t95_win2 VALUES (3, 'B', 30)")
	afExec(t, db, ctx, "INSERT INTO t95_win2 VALUES (4, 'B', 40)")
	afExec(t, db, ctx, "INSERT INTO t95_win2 VALUES (5, 'B', 50)")

	rows := afQuery(t, db, ctx, `
		SELECT grp, val, SUM(val) OVER (PARTITION BY grp) as grp_total
		FROM t95_win2
		ORDER BY grp, val
	`)
	if len(rows) != 5 {
		t.Fatalf("Expected 5 rows, got %d", len(rows))
	}

	// Group A total = 30, Group B total = 120
	for _, row := range rows {
		grp := fmt.Sprintf("%v", row[0])
		total := fmt.Sprintf("%v", row[2])
		switch grp {
		case "A":
			if total != "30" {
				t.Errorf("Group A total should be 30, got %s", total)
			}
		case "B":
			if total != "120" {
				t.Errorf("Group B total should be 120, got %s", total)
			}
		}
	}
}

func TestV95_WindowRankPartition(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_win3 (id INTEGER PRIMARY KEY, cat TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t95_win3 VALUES (1, 'X', 100)")
	afExec(t, db, ctx, "INSERT INTO t95_win3 VALUES (2, 'X', 90)")
	afExec(t, db, ctx, "INSERT INTO t95_win3 VALUES (3, 'X', 90)")
	afExec(t, db, ctx, "INSERT INTO t95_win3 VALUES (4, 'Y', 80)")
	afExec(t, db, ctx, "INSERT INTO t95_win3 VALUES (5, 'Y', 70)")

	rows := afQuery(t, db, ctx, `
		SELECT cat, score, RANK() OVER (PARTITION BY cat ORDER BY score DESC) as rnk
		FROM t95_win3
	`)
	if len(rows) != 5 {
		t.Fatalf("Expected 5 rows, got %d: %v", len(rows), rows)
	}

	// In X: 100->rank1, 90->rank2, 90->rank2
	// In Y: 80->rank1, 70->rank2
	for _, row := range rows {
		cat := fmt.Sprintf("%v", row[0])
		score := fmt.Sprintf("%v", row[1])
		rank := fmt.Sprintf("%v", row[2])
		if cat == "X" && score == "100" && rank != "1" {
			t.Errorf("X/100 should be rank 1, got %s", rank)
		}
		if cat == "X" && score == "90" && rank != "2" {
			t.Errorf("X/90 should be rank 2 (tie), got %s", rank)
		}
		if cat == "Y" && score == "80" && rank != "1" {
			t.Errorf("Y/80 should be rank 1, got %s", rank)
		}
	}
}

// ---------------------------------------------------------------------------
// 10. COALESCE / NULLIF / IFNULL edge cases
// ---------------------------------------------------------------------------

func TestV95_CoalesceBasic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// COALESCE returns first non-null
	afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, NULL, 'third')", "third")
	afExpectVal(t, db, ctx, "SELECT COALESCE('first', 'second')", "first")
	afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, 42)", 42)
}

func TestV95_CoalesceWithColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_coal (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)")
	afExec(t, db, ctx, "INSERT INTO t95_coal VALUES (1, NULL, NULL, 'fallback')")
	afExec(t, db, ctx, "INSERT INTO t95_coal VALUES (2, NULL, 'middle', 'fallback')")
	afExec(t, db, ctx, "INSERT INTO t95_coal VALUES (3, 'first', NULL, 'fallback')")

	afExpectVal(t, db, ctx, "SELECT COALESCE(a, b, c) FROM t95_coal WHERE id = 1", "fallback")
	afExpectVal(t, db, ctx, "SELECT COALESCE(a, b, c) FROM t95_coal WHERE id = 2", "middle")
	afExpectVal(t, db, ctx, "SELECT COALESCE(a, b, c) FROM t95_coal WHERE id = 3", "first")
}

func TestV95_CoalesceAllNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, "SELECT COALESCE(NULL, NULL, NULL)")
	if len(rows) != 1 || len(rows[0]) == 0 {
		t.Fatal("Expected 1 row, 1 column")
	}
	if rows[0][0] != nil {
		t.Fatalf("COALESCE(NULL,NULL,NULL) should be NULL, got %v", rows[0][0])
	}
}

func TestV95_NullifBasic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// NULLIF returns NULL if equal, first arg if not
	rows := afQuery(t, db, ctx, "SELECT NULLIF(1, 1)")
	if len(rows) != 1 {
		t.Fatal("Expected 1 row")
	}
	if rows[0][0] != nil {
		t.Fatalf("NULLIF(1,1) should be NULL, got %v", rows[0][0])
	}

	afExpectVal(t, db, ctx, "SELECT NULLIF(1, 2)", 1)
	afExpectVal(t, db, ctx, "SELECT NULLIF('a', 'b')", "a")
}

func TestV95_NullifWithColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_nif (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t95_nif VALUES (1, 'special')")
	afExec(t, db, ctx, "INSERT INTO t95_nif VALUES (2, 'normal')")

	// NULLIF(val, 'special') -> NULL for id=1, 'normal' for id=2
	rows := afQuery(t, db, ctx, "SELECT NULLIF(val, 'special') FROM t95_nif WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("Expected 1 row")
	}
	if rows[0][0] != nil {
		t.Fatalf("NULLIF(val, 'special') should be NULL for val='special', got %v", rows[0][0])
	}

	afExpectVal(t, db, ctx, "SELECT NULLIF(val, 'special') FROM t95_nif WHERE id = 2", "normal")
}

func TestV95_IfnullBasic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT IFNULL(NULL, 'fallback')", "fallback")
	afExpectVal(t, db, ctx, "SELECT IFNULL('value', 'fallback')", "value")
	afExpectVal(t, db, ctx, "SELECT IFNULL(42, 0)", 42)
}

func TestV95_CoalesceNullifCombo(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Nesting: COALESCE(NULLIF(x, x), 'default')
	afExpectVal(t, db, ctx, "SELECT COALESCE(NULLIF(5, 5), 99)", 99)
	afExpectVal(t, db, ctx, "SELECT COALESCE(NULLIF(5, 3), 99)", 5)

	// IFNULL with NULLIF
	afExpectVal(t, db, ctx, "SELECT IFNULL(NULLIF(10, 10), 'was_null')", "was_null")
}

func TestV95_CoalesceWithArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t95_arith (id INTEGER PRIMARY KEY, bonus INTEGER, commission INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t95_arith VALUES (1, NULL, NULL)")
	afExec(t, db, ctx, "INSERT INTO t95_arith VALUES (2, 100, NULL)")
	afExec(t, db, ctx, "INSERT INTO t95_arith VALUES (3, NULL, 200)")

	// Use COALESCE to handle NULLs in arithmetic
	afExpectVal(t, db, ctx,
		"SELECT COALESCE(bonus, 0) + COALESCE(commission, 0) FROM t95_arith WHERE id = 1", 0)
	afExpectVal(t, db, ctx,
		"SELECT COALESCE(bonus, 0) + COALESCE(commission, 0) FROM t95_arith WHERE id = 2", 100)
	afExpectVal(t, db, ctx,
		"SELECT COALESCE(bonus, 0) + COALESCE(commission, 0) FROM t95_arith WHERE id = 3", 200)
}
