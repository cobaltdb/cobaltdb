package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestV111CoverageDeep targets low-coverage catalog paths through integration tests:
// - RollbackToSavepoint DDL paths (CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX, ALTER TABLE)
// - RollbackTransaction DDL (CREATE INDEX, DROP INDEX)
// - ExecuteCTE (UNION, INTERSECT, EXCEPT, multi-CTE, recursive)
// - CommitTransaction with operations
// - Vacuum
// - storeIndexDef (unique, composite indexes)
func TestV111CoverageDeep(t *testing.T) {
	_ = fmt.Sprintf

	// ============================================================
	// SECTION 1: RollbackToSavepoint DDL Paths
	// ============================================================

	// --- 1.1 SAVEPOINT + CREATE TABLE + ROLLBACK TO SAVEPOINT ---
	t.Run("SP_CreateTable_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_ct1")
		afExec(t, db, ctx, "CREATE TABLE v111_sp_ct1 (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_sp_ct1 VALUES (1, 'hello')")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_sp_ct1 WHERE id = 1", "hello")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_ct1")
		_, err := db.Exec(ctx, "SELECT * FROM v111_sp_ct1")
		if err == nil {
			t.Error("expected error: table should not exist after rollback")
		}
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_CreateTable_Commit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_ct2")
		afExec(t, db, ctx, "CREATE TABLE v111_sp_ct2 (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_sp_ct2 VALUES (1, 'kept')")
		afExec(t, db, ctx, "COMMIT")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_sp_ct2 WHERE id = 1", "kept")
	})

	t.Run("SP_CreateTable_MultipleRollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_ct3a")
		afExec(t, db, ctx, "CREATE TABLE v111_sp_ct3a (id INTEGER PRIMARY KEY)")
		afExec(t, db, ctx, "SAVEPOINT sp_ct3b")
		afExec(t, db, ctx, "CREATE TABLE v111_sp_ct3b (id INTEGER PRIMARY KEY)")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_ct3b")
		_, err := db.Exec(ctx, "SELECT * FROM v111_sp_ct3b")
		if err == nil {
			t.Error("expected error: v111_sp_ct3b should not exist")
		}
		afExec(t, db, ctx, "INSERT INTO v111_sp_ct3a VALUES (1)")
		afExpectVal(t, db, ctx, "SELECT id FROM v111_sp_ct3a WHERE id = 1", float64(1))
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_ct3a")
		_, err = db.Exec(ctx, "SELECT * FROM v111_sp_ct3a")
		if err == nil {
			t.Error("expected error: v111_sp_ct3a should not exist")
		}
		afExec(t, db, ctx, "COMMIT")
	})

	// --- 1.2 SAVEPOINT + CREATE INDEX + ROLLBACK TO SAVEPOINT ---
	t.Run("SP_CreateIndex_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_idx_base (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v111_idx_base VALUES (1, 'alice', 90)")
		afExec(t, db, ctx, "INSERT INTO v111_idx_base VALUES (2, 'bob', 80)")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_ci1")
		afExec(t, db, ctx, "CREATE INDEX v111_idx_score ON v111_idx_base (score)")
		afExpectVal(t, db, ctx, "SELECT name FROM v111_idx_base WHERE score = 90", "alice")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_ci1")
		afExpectVal(t, db, ctx, "SELECT name FROM v111_idx_base WHERE score = 90", "alice")
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_CreateIndex_Commit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_idx_c (id INTEGER PRIMARY KEY, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_idx_c VALUES (1, 'bob')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_ci2")
		afExec(t, db, ctx, "CREATE INDEX v111_idx_name ON v111_idx_c (name)")
		afExec(t, db, ctx, "COMMIT")
		afExpectVal(t, db, ctx, "SELECT name FROM v111_idx_c WHERE id = 1", "bob")
	})

	t.Run("SP_CreateUniqueIndex_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_uidx_sp (id INTEGER PRIMARY KEY, score INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v111_uidx_sp VALUES (1, 90)")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_ci3")
		afExec(t, db, ctx, "CREATE UNIQUE INDEX v111_uidx_score ON v111_uidx_sp (score)")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_ci3")
		afExec(t, db, ctx, "COMMIT")
		// After rollback of unique index, duplicate should be allowed
		afExec(t, db, ctx, "INSERT INTO v111_uidx_sp VALUES (2, 90)")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_uidx_sp", 2)
	})

	// --- 1.3 SAVEPOINT + DROP TABLE + ROLLBACK TO SAVEPOINT ---
	t.Run("SP_DropTable_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_drop_me (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_drop_me VALUES (1, 'survive')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_dt1")
		afExec(t, db, ctx, "DROP TABLE v111_drop_me")
		_, err := db.Exec(ctx, "SELECT * FROM v111_drop_me")
		if err == nil {
			t.Error("expected error after drop")
		}
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_dt1")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_drop_me WHERE id = 1", "survive")
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_DropTable_Nested", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_drop_nest (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_drop_nest VALUES (1, 'nested')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_dt2a")
		afExec(t, db, ctx, "INSERT INTO v111_drop_nest VALUES (2, 'extra')")
		afExec(t, db, ctx, "SAVEPOINT sp_dt2b")
		afExec(t, db, ctx, "DROP TABLE v111_drop_nest")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_dt2b")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_drop_nest", 2)
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_dt2a")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_drop_nest", 1)
		afExec(t, db, ctx, "COMMIT")
	})

	// --- 1.4 SAVEPOINT + DROP INDEX + ROLLBACK TO SAVEPOINT ---
	t.Run("SP_DropIndex_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_didx (id INTEGER PRIMARY KEY, category TEXT)")
		afExec(t, db, ctx, "CREATE INDEX v111_didx_cat ON v111_didx (category)")
		afExec(t, db, ctx, "INSERT INTO v111_didx VALUES (1, 'a')")
		afExec(t, db, ctx, "INSERT INTO v111_didx VALUES (2, 'b')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_di1")
		afExec(t, db, ctx, "DROP INDEX v111_didx_cat")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_di1")
		afExpectVal(t, db, ctx, "SELECT category FROM v111_didx WHERE id = 1", "a")
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_DropIndex_Commit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_didx2 (id INTEGER PRIMARY KEY, category TEXT)")
		afExec(t, db, ctx, "CREATE INDEX v111_didx2_cat ON v111_didx2 (category)")
		afExec(t, db, ctx, "INSERT INTO v111_didx2 VALUES (1, 'a')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_di2")
		afExec(t, db, ctx, "DROP INDEX v111_didx2_cat")
		afExec(t, db, ctx, "COMMIT")
		afExpectVal(t, db, ctx, "SELECT category FROM v111_didx2 WHERE id = 1", "a")
	})

	// --- 1.5 SAVEPOINT + ALTER TABLE ADD COLUMN + ROLLBACK TO SAVEPOINT ---
	t.Run("SP_AlterAddColumn_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_alter_add (id INTEGER PRIMARY KEY, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_alter_add VALUES (1, 'test')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_aac1")
		afExec(t, db, ctx, "ALTER TABLE v111_alter_add ADD COLUMN age INTEGER")
		afExec(t, db, ctx, "UPDATE v111_alter_add SET age = 25 WHERE id = 1")
		afExpectVal(t, db, ctx, "SELECT age FROM v111_alter_add WHERE id = 1", float64(25))
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_aac1")
		_, err := db.Exec(ctx, "SELECT age FROM v111_alter_add WHERE id = 1")
		if err == nil {
			t.Error("expected error: age column should not exist after rollback")
		}
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_AlterAddColumn_Commit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_alter_add2 (id INTEGER PRIMARY KEY, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_alter_add2 VALUES (1, 'test')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_aac2")
		afExec(t, db, ctx, "ALTER TABLE v111_alter_add2 ADD COLUMN email TEXT")
		afExec(t, db, ctx, "COMMIT")
		afExec(t, db, ctx, "UPDATE v111_alter_add2 SET email = 'a@b.com' WHERE id = 1")
		afExpectVal(t, db, ctx, "SELECT email FROM v111_alter_add2 WHERE id = 1", "a@b.com")
	})

	// --- 1.6 SAVEPOINT + ALTER TABLE RENAME + ROLLBACK TO SAVEPOINT ---
	t.Run("SP_AlterRename_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_rename_src (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_rename_src VALUES (1, 'rename_me')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "ALTER TABLE v111_rename_src RENAME TO v111_renamed_dst")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_renamed_dst WHERE id = 1", "rename_me")
		afExec(t, db, ctx, "ROLLBACK")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_rename_src WHERE id = 1", "rename_me")
		_, err := db.Exec(ctx, "SELECT * FROM v111_renamed_dst")
		if err == nil {
			t.Error("expected error: renamed table should not exist after rollback")
		}
	})

	// --- 1.7 SAVEPOINT + Multiple DDL + Rollback ---
	t.Run("SP_MultipleDDL_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_multi")
		afExec(t, db, ctx, "CREATE TABLE v111_multi_a (id INTEGER PRIMARY KEY)")
		afExec(t, db, ctx, "CREATE TABLE v111_multi_b (id INTEGER PRIMARY KEY, ref INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v111_multi_a VALUES (1)")
		afExec(t, db, ctx, "INSERT INTO v111_multi_b VALUES (1, 1)")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_multi")
		_, err := db.Exec(ctx, "SELECT * FROM v111_multi_a")
		if err == nil {
			t.Error("v111_multi_a should not exist")
		}
		_, err = db.Exec(ctx, "SELECT * FROM v111_multi_b")
		if err == nil {
			t.Error("v111_multi_b should not exist")
		}
		afExec(t, db, ctx, "COMMIT")
	})

	// ============================================================
	// SECTION 2: RollbackTransaction DDL
	// ============================================================

	t.Run("Txn_CreateIndex_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_idx (id INTEGER PRIMARY KEY, tag TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_idx VALUES (1, 'alpha')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "CREATE INDEX v111_txn_idx_tag ON v111_txn_idx (tag)")
		afExec(t, db, ctx, "ROLLBACK")
		afExpectVal(t, db, ctx, "SELECT tag FROM v111_txn_idx WHERE id = 1", "alpha")
	})

	t.Run("Txn_CreateIndex_Commit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_idx2 (id INTEGER PRIMARY KEY, tag TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_idx2 VALUES (1, 'beta')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "CREATE INDEX v111_txn_idx2_tag ON v111_txn_idx2 (tag)")
		afExec(t, db, ctx, "COMMIT")
		afExpectVal(t, db, ctx, "SELECT tag FROM v111_txn_idx2 WHERE id = 1", "beta")
	})

	t.Run("Txn_DropIndex_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_di_rb (id INTEGER PRIMARY KEY, label TEXT)")
		afExec(t, db, ctx, "CREATE INDEX v111_di_rb_label ON v111_di_rb (label)")
		afExec(t, db, ctx, "INSERT INTO v111_di_rb VALUES (1, 'x')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "DROP INDEX v111_di_rb_label")
		afExec(t, db, ctx, "ROLLBACK")
		afExpectVal(t, db, ctx, "SELECT label FROM v111_di_rb WHERE id = 1", "x")
	})

	t.Run("Txn_DropIndex_Commit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_di_cm (id INTEGER PRIMARY KEY, label TEXT)")
		afExec(t, db, ctx, "CREATE INDEX v111_di_cm_label ON v111_di_cm (label)")
		afExec(t, db, ctx, "INSERT INTO v111_di_cm VALUES (1, 'y')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "DROP INDEX v111_di_cm_label")
		afExec(t, db, ctx, "COMMIT")
		afExpectVal(t, db, ctx, "SELECT label FROM v111_di_cm WHERE id = 1", "y")
	})

	t.Run("Txn_CreateTable_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "CREATE TABLE v111_txn_ct (id INTEGER PRIMARY KEY, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_ct VALUES (1, 'ephemeral')")
		afExec(t, db, ctx, "ROLLBACK")
		_, err := db.Exec(ctx, "SELECT * FROM v111_txn_ct")
		if err == nil {
			t.Error("table should not exist after rollback")
		}
	})

	t.Run("Txn_DropTable_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_dt (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_dt VALUES (1, 'persist')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "DROP TABLE v111_txn_dt")
		afExec(t, db, ctx, "ROLLBACK")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_txn_dt WHERE id = 1", "persist")
	})

	t.Run("Txn_AlterAddColumn_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_alt (id INTEGER PRIMARY KEY, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_alt VALUES (1, 'orig')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "ALTER TABLE v111_txn_alt ADD COLUMN extra TEXT")
		afExec(t, db, ctx, "UPDATE v111_txn_alt SET extra = 'tmp' WHERE id = 1")
		afExec(t, db, ctx, "ROLLBACK")
		_, err := db.Exec(ctx, "SELECT extra FROM v111_txn_alt WHERE id = 1")
		if err == nil {
			t.Error("extra column should not exist after rollback")
		}
	})

	t.Run("Txn_AlterTableRename_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_ren (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_ren VALUES (1, 'ren')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "ALTER TABLE v111_txn_ren RENAME TO v111_txn_renamed")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_txn_renamed WHERE id = 1", "ren")
		afExec(t, db, ctx, "ROLLBACK")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_txn_ren WHERE id = 1", "ren")
	})

	// ============================================================
	// SECTION 3: ExecuteCTE
	// ============================================================

	t.Run("CTE_Union", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_cd (id INTEGER PRIMARY KEY, category TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_cd VALUES (1, 'A', 10.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cd VALUES (2, 'B', 20.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cd VALUES (3, 'A', 30.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cd VALUES (4, 'C', 40.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cd VALUES (5, 'B', 50.0)")
		rows := afQuery(t, db, ctx, `
			WITH combined AS (
				SELECT id, category FROM v111_cd WHERE category = 'A'
				UNION
				SELECT id, category FROM v111_cd WHERE category = 'B'
			)
			SELECT COUNT(*) FROM combined`)
		if len(rows) == 0 || fmt.Sprintf("%v", rows[0][0]) != "4" {
			t.Errorf("expected 4, got %v", rows)
		}
	})

	t.Run("CTE_UnionAll", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_cd2 (id INTEGER PRIMARY KEY, category TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_cd2 VALUES (1, 'A')")
		afExec(t, db, ctx, "INSERT INTO v111_cd2 VALUES (2, 'A')")
		afExec(t, db, ctx, "INSERT INTO v111_cd2 VALUES (3, 'B')")
		rows := afQuery(t, db, ctx, `
			WITH combined AS (
				SELECT category FROM v111_cd2 WHERE category = 'A'
				UNION ALL
				SELECT category FROM v111_cd2 WHERE category = 'A'
			)
			SELECT COUNT(*) FROM combined`)
		if len(rows) == 0 || fmt.Sprintf("%v", rows[0][0]) != "4" {
			t.Errorf("expected 4, got %v", rows)
		}
	})

	t.Run("CTE_Intersect", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_ci (id INTEGER PRIMARY KEY, category TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_ci VALUES (1, 'A', 10.0)")
		afExec(t, db, ctx, "INSERT INTO v111_ci VALUES (2, 'B', 20.0)")
		afExec(t, db, ctx, "INSERT INTO v111_ci VALUES (3, 'A', 30.0)")
		afExec(t, db, ctx, "INSERT INTO v111_ci VALUES (4, 'C', 40.0)")
		rows := afQuery(t, db, ctx, `
			WITH common AS (
				SELECT category FROM v111_ci WHERE amount >= 20
				INTERSECT
				SELECT category FROM v111_ci WHERE amount <= 40
			)
			SELECT * FROM common ORDER BY category`)
		if len(rows) < 1 {
			t.Error("expected at least 1 row from INTERSECT CTE")
		}
	})

	t.Run("CTE_Except", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_ce (id INTEGER PRIMARY KEY, category TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_ce VALUES (1, 'A')")
		afExec(t, db, ctx, "INSERT INTO v111_ce VALUES (2, 'B')")
		afExec(t, db, ctx, "INSERT INTO v111_ce VALUES (3, 'A')")
		afExec(t, db, ctx, "INSERT INTO v111_ce VALUES (4, 'C')")
		rows := afQuery(t, db, ctx, `
			WITH only_a AS (
				SELECT category FROM v111_ce
				EXCEPT
				SELECT category FROM v111_ce WHERE category != 'A'
			)
			SELECT * FROM only_a`)
		if len(rows) != 1 {
			t.Errorf("expected 1, got %d", len(rows))
		}
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) != "A" {
			t.Errorf("expected 'A', got %v", rows[0][0])
		}
	})

	t.Run("CTE_MultipleChained", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_cm (id INTEGER PRIMARY KEY, category TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_cm VALUES (1, 'A', 10.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cm VALUES (2, 'A', 30.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cm VALUES (3, 'A', 60.0)")
		rows := afQuery(t, db, ctx, `
			WITH
				cte_a AS (SELECT id, category, amount FROM v111_cm WHERE category = 'A'),
				cte_sum AS (SELECT SUM(amount) AS total FROM cte_a)
			SELECT total FROM cte_sum`)
		if len(rows) == 0 || fmt.Sprintf("%v", rows[0][0]) != "100" {
			t.Errorf("expected 100, got %v", rows)
		}
	})

	t.Run("CTE_ThreeChained", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_c3 (id INTEGER PRIMARY KEY, category TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_c3 VALUES (1, 'B', 20.0)")
		afExec(t, db, ctx, "INSERT INTO v111_c3 VALUES (2, 'B', 50.0)")
		afExec(t, db, ctx, "INSERT INTO v111_c3 VALUES (3, 'A', 10.0)")
		rows := afQuery(t, db, ctx, `
			WITH
				base AS (SELECT id, category, amount FROM v111_c3),
				filtered AS (SELECT id, amount FROM base WHERE category = 'B'),
				agg AS (SELECT SUM(amount) AS total FROM filtered)
			SELECT total FROM agg`)
		if len(rows) == 0 || fmt.Sprintf("%v", rows[0][0]) != "70" {
			t.Errorf("expected 70, got %v", rows)
		}
	})

	t.Run("CTE_Recursive_Count", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE cnt(x) AS (
				SELECT 1
				UNION ALL
				SELECT x + 1 FROM cnt WHERE x < 10
			)
			SELECT COUNT(*) FROM cnt`)
		if len(rows) == 0 || fmt.Sprintf("%v", rows[0][0]) != "10" {
			t.Errorf("expected 10, got %v", rows)
		}
	})

	t.Run("CTE_Recursive_Sum", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE seq(n) AS (
				SELECT 1
				UNION ALL
				SELECT n + 1 FROM seq WHERE n < 5
			)
			SELECT SUM(n) FROM seq`)
		if len(rows) == 0 || fmt.Sprintf("%v", rows[0][0]) != "15" {
			t.Errorf("expected 15, got %v", rows)
		}
	})

	t.Run("CTE_Recursive_Tree", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_tree (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_tree VALUES (1, NULL, 'root')")
		afExec(t, db, ctx, "INSERT INTO v111_tree VALUES (2, 1, 'child1')")
		afExec(t, db, ctx, "INSERT INTO v111_tree VALUES (3, 1, 'child2')")
		afExec(t, db, ctx, "INSERT INTO v111_tree VALUES (4, 2, 'grandchild1')")
		afExec(t, db, ctx, "INSERT INTO v111_tree VALUES (5, 3, 'grandchild2')")
		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE tree_walk(id, name, depth) AS (
				SELECT id, name, 0 FROM v111_tree WHERE parent_id IS NULL
				UNION ALL
				SELECT t.id, t.name, tw.depth + 1
				FROM v111_tree t
				JOIN tree_walk tw ON t.parent_id = tw.id
			)
			SELECT COUNT(*) FROM tree_walk`)
		if len(rows) == 0 || fmt.Sprintf("%v", rows[0][0]) != "5" {
			t.Errorf("expected 5, got %v", rows)
		}
	})

	t.Run("CTE_OrderByLimit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_col (id INTEGER PRIMARY KEY, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_col VALUES (1, 10.0)")
		afExec(t, db, ctx, "INSERT INTO v111_col VALUES (2, 20.0)")
		afExec(t, db, ctx, "INSERT INTO v111_col VALUES (3, 30.0)")
		afExec(t, db, ctx, "INSERT INTO v111_col VALUES (4, 40.0)")
		afExec(t, db, ctx, "INSERT INTO v111_col VALUES (5, 50.0)")
		rows := afQuery(t, db, ctx, `
			WITH ranked AS (
				SELECT id, amount FROM v111_col ORDER BY amount DESC
			)
			SELECT id FROM ranked LIMIT 3`)
		if len(rows) != 3 {
			t.Errorf("expected 3, got %d", len(rows))
		}
	})

	t.Run("CTE_WithAggregate", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_ca (id INTEGER PRIMARY KEY, category TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_ca VALUES (1, 'A', 10.0)")
		afExec(t, db, ctx, "INSERT INTO v111_ca VALUES (2, 'B', 20.0)")
		afExec(t, db, ctx, "INSERT INTO v111_ca VALUES (3, 'A', 30.0)")
		afExec(t, db, ctx, "INSERT INTO v111_ca VALUES (4, 'C', 40.0)")
		afExec(t, db, ctx, "INSERT INTO v111_ca VALUES (5, 'B', 50.0)")
		afExec(t, db, ctx, "INSERT INTO v111_ca VALUES (6, 'A', 60.0)")
		rows := afQuery(t, db, ctx, `
			WITH cat_totals AS (
				SELECT category, SUM(amount) AS total
				FROM v111_ca
				GROUP BY category
			)
			SELECT category, total FROM cat_totals ORDER BY total DESC`)
		if len(rows) != 3 {
			t.Errorf("expected 3 categories, got %d", len(rows))
		}
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) != "A" {
			t.Errorf("expected A first, got %v", rows[0][0])
		}
	})

	t.Run("CTE_UnionSubselect", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_cus (id INTEGER PRIMARY KEY, category TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_cus VALUES (1, 'A', 10.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cus VALUES (2, 'A', 30.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cus VALUES (3, 'B', 20.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cus VALUES (4, 'B', 50.0)")
		rows := afQuery(t, db, ctx, `
			WITH merged AS (
				SELECT 'A' AS src, id, amount FROM v111_cus WHERE category = 'A'
				UNION ALL
				SELECT 'B' AS src, id, amount FROM v111_cus WHERE category = 'B'
			)
			SELECT src, COUNT(*) FROM merged GROUP BY src ORDER BY src`)
		if len(rows) != 2 {
			t.Errorf("expected 2, got %d", len(rows))
		}
	})

	t.Run("CTE_Recursive_Fibonacci", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE fib(n, a, b) AS (
				SELECT 1, 0, 1
				UNION ALL
				SELECT n + 1, b, a + b FROM fib WHERE n < 10
			)
			SELECT a FROM fib ORDER BY n`)
		if len(rows) != 10 {
			t.Errorf("expected 10, got %d", len(rows))
		}
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) != "0" {
			t.Errorf("expected 0, got %v", rows[0][0])
		}
	})

	t.Run("CTE_WithJoin", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_cj (id INTEGER PRIMARY KEY, category TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_cj VALUES (1, 'A', 10.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cj VALUES (2, 'B', 20.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cj VALUES (3, 'A', 30.0)")
		rows := afQuery(t, db, ctx, `
			WITH cat_amounts AS (
				SELECT category, SUM(amount) AS total
				FROM v111_cj
				GROUP BY category
			)
			SELECT d.id, ca.total
			FROM v111_cj d
			JOIN cat_amounts ca ON d.category = ca.category
			WHERE d.id <= 3
			ORDER BY d.id`)
		if len(rows) != 3 {
			t.Errorf("expected 3, got %d", len(rows))
		}
	})

	t.Run("CTE_InSubquery", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_csq (id INTEGER PRIMARY KEY, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_csq VALUES (1, 10.0)")
		afExec(t, db, ctx, "INSERT INTO v111_csq VALUES (2, 20.0)")
		afExec(t, db, ctx, "INSERT INTO v111_csq VALUES (3, 40.0)")
		afExec(t, db, ctx, "INSERT INTO v111_csq VALUES (4, 50.0)")
		rows := afQuery(t, db, ctx, `
			WITH high AS (
				SELECT id FROM v111_csq WHERE amount > 30
			)
			SELECT * FROM v111_csq WHERE id IN (SELECT id FROM high) ORDER BY id`)
		if len(rows) != 2 {
			t.Errorf("expected 2, got %d", len(rows))
		}
	})

	t.Run("CTE_NestedAggregates", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_cna (id INTEGER PRIMARY KEY, category TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_cna VALUES (1, 'X', 10.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cna VALUES (2, 'X', 20.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cna VALUES (3, 'Y', 30.0)")
		rows := afQuery(t, db, ctx, `
			WITH stats AS (
				SELECT category, COUNT(*) AS cnt, AVG(amount) AS avg_amt
				FROM v111_cna
				GROUP BY category
			)
			SELECT category FROM stats WHERE cnt > 1 ORDER BY category`)
		if len(rows) != 1 {
			t.Errorf("expected 1 (X), got %d", len(rows))
		}
	})

	t.Run("CTE_Recursive_Powers", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE powers(n, val) AS (
				SELECT 0, 1
				UNION ALL
				SELECT n + 1, val * 2 FROM powers WHERE n < 8
			)
			SELECT val FROM powers ORDER BY n`)
		if len(rows) != 9 {
			t.Errorf("expected 9, got %d", len(rows))
		}
		if len(rows) > 8 && fmt.Sprintf("%v", rows[8][0]) != "256" {
			t.Errorf("expected 256, got %v", rows[8][0])
		}
	})

	t.Run("CTE_LargeRecursion", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE nums(n) AS (
				SELECT 1
				UNION ALL
				SELECT n + 1 FROM nums WHERE n < 100
			)
			SELECT MAX(n), MIN(n), COUNT(*) FROM nums`)
		if len(rows) == 0 {
			t.Fatal("expected result")
		}
		if fmt.Sprintf("%v", rows[0][0]) != "100" {
			t.Errorf("expected max 100, got %v", rows[0][0])
		}
		if fmt.Sprintf("%v", rows[0][2]) != "100" {
			t.Errorf("expected count 100, got %v", rows[0][2])
		}
	})

	// ============================================================
	// SECTION 4: CommitTransaction with operations
	// ============================================================

	t.Run("CommitTransaction_InsertCommit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_commit_t (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "INSERT INTO v111_commit_t VALUES (1, 'committed')")
		afExec(t, db, ctx, "INSERT INTO v111_commit_t VALUES (2, 'also_committed')")
		afExec(t, db, ctx, "COMMIT")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_commit_t", 2)
		afExpectVal(t, db, ctx, "SELECT val FROM v111_commit_t WHERE id = 1", "committed")
	})

	t.Run("CommitTransaction_UpdateCommit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_ct_upd (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_ct_upd VALUES (1, 'original')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "UPDATE v111_ct_upd SET val = 'updated' WHERE id = 1")
		afExec(t, db, ctx, "COMMIT")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_ct_upd WHERE id = 1", "updated")
	})

	t.Run("CommitTransaction_DeleteCommit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_ct_del (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_ct_del VALUES (1, 'a')")
		afExec(t, db, ctx, "INSERT INTO v111_ct_del VALUES (2, 'b')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "DELETE FROM v111_ct_del WHERE id = 2")
		afExec(t, db, ctx, "COMMIT")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_ct_del", 1)
	})

	t.Run("CommitTransaction_MultiOpCommit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_ct_mo (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "INSERT INTO v111_ct_mo VALUES (10, 'ten')")
		afExec(t, db, ctx, "INSERT INTO v111_ct_mo VALUES (11, 'eleven')")
		afExec(t, db, ctx, "UPDATE v111_ct_mo SET val = 'TEN' WHERE id = 10")
		afExec(t, db, ctx, "DELETE FROM v111_ct_mo WHERE id = 11")
		afExec(t, db, ctx, "COMMIT")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_ct_mo WHERE id = 10", "TEN")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_ct_mo WHERE id = 11", 0)
	})

	t.Run("CommitTransaction_CreateTableCommit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "CREATE TABLE v111_ct_new (id INTEGER PRIMARY KEY, note TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_ct_new VALUES (1, 'in_txn')")
		afExec(t, db, ctx, "COMMIT")
		afExpectVal(t, db, ctx, "SELECT note FROM v111_ct_new WHERE id = 1", "in_txn")
	})

	t.Run("CommitTransaction_DDLAndDMLMixed", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "CREATE TABLE v111_mixed_ops (id INTEGER PRIMARY KEY, val INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v111_mixed_ops VALUES (1, 100)")
		afExec(t, db, ctx, "CREATE INDEX v111_mixed_idx ON v111_mixed_ops (val)")
		afExec(t, db, ctx, "INSERT INTO v111_mixed_ops VALUES (2, 200)")
		afExec(t, db, ctx, "COMMIT")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_mixed_ops", 2)
	})

	t.Run("CommitTransaction_EmptyTxn", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("RollbackTransaction_EmptyTxn", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "ROLLBACK")
	})

	// ============================================================
	// SECTION 5: Vacuum
	// ============================================================

	t.Run("Vacuum_Basic", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_vac (id INTEGER PRIMARY KEY, val TEXT)")
		for i := 1; i <= 20; i++ {
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_vac VALUES (%d, 'row_%d')", i, i))
		}
		afExec(t, db, ctx, "DELETE FROM v111_vac WHERE id > 10")
		afExec(t, db, ctx, "VACUUM")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_vac", 10)
		afExpectVal(t, db, ctx, "SELECT val FROM v111_vac WHERE id = 5", "row_5")
	})

	t.Run("Vacuum_EmptyTable", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_vac_empty (id INTEGER PRIMARY KEY)")
		afExec(t, db, ctx, "VACUUM")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_vac_empty", 0)
	})

	t.Run("Vacuum_AfterManyDeletes", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_vac_del (id INTEGER PRIMARY KEY, payload TEXT)")
		for i := 1; i <= 50; i++ {
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_vac_del VALUES (%d, 'payload_%d')", i, i))
		}
		afExec(t, db, ctx, "DELETE FROM v111_vac_del WHERE id <= 40")
		afExec(t, db, ctx, "VACUUM")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_vac_del", 10)
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v111_vac_del", float64(10))
	})

	t.Run("Vacuum_WithIndex", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_vac_idx (id INTEGER PRIMARY KEY, tag TEXT)")
		afExec(t, db, ctx, "CREATE INDEX v111_vac_idx_tag ON v111_vac_idx (tag)")
		for i := 1; i <= 30; i++ {
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_vac_idx VALUES (%d, 'tag_%d')", i, i))
		}
		afExec(t, db, ctx, "DELETE FROM v111_vac_idx WHERE id <= 20")
		afExec(t, db, ctx, "VACUUM")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_vac_idx", 10)
	})

	t.Run("Vacuum_MultipleTables", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_vac_m1 (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "CREATE TABLE v111_vac_m2 (id INTEGER PRIMARY KEY, val TEXT)")
		for i := 1; i <= 10; i++ {
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_vac_m1 VALUES (%d, 'm1_%d')", i, i))
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_vac_m2 VALUES (%d, 'm2_%d')", i, i))
		}
		afExec(t, db, ctx, "DELETE FROM v111_vac_m1 WHERE id > 5")
		afExec(t, db, ctx, "DELETE FROM v111_vac_m2 WHERE id > 7")
		afExec(t, db, ctx, "VACUUM")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_vac_m1", 5)
		afExpectRows(t, db, ctx, "SELECT * FROM v111_vac_m2", 7)
	})

	t.Run("Vacuum_NoDeletedRows", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_vac_nodel (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_vac_nodel VALUES (1, 'keep')")
		afExec(t, db, ctx, "VACUUM")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_vac_nodel WHERE id = 1", "keep")
	})

	t.Run("Vacuum_AfterUpdate", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_vac_upd (id INTEGER PRIMARY KEY, val TEXT)")
		for i := 1; i <= 10; i++ {
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_vac_upd VALUES (%d, 'orig')", i))
		}
		afExec(t, db, ctx, "UPDATE v111_vac_upd SET val = 'modified' WHERE id <= 5")
		afExec(t, db, ctx, "VACUUM")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_vac_upd", 10)
		afExpectVal(t, db, ctx, "SELECT val FROM v111_vac_upd WHERE id = 1", "modified")
	})

	t.Run("Vacuum_AfterTransaction", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_vac_txn (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "BEGIN")
		for i := 1; i <= 10; i++ {
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_vac_txn VALUES (%d, 'val_%d')", i, i))
		}
		afExec(t, db, ctx, "COMMIT")
		afExec(t, db, ctx, "DELETE FROM v111_vac_txn WHERE id > 5")
		afExec(t, db, ctx, "VACUUM")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_vac_txn", 5)
	})

	t.Run("Vacuum_LargeDataset", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_vac_large (id INTEGER PRIMARY KEY, data TEXT)")
		for i := 1; i <= 100; i++ {
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_vac_large VALUES (%d, 'data_%d')", i, i))
		}
		afExec(t, db, ctx, "DELETE FROM v111_vac_large WHERE id > 10")
		afExec(t, db, ctx, "VACUUM")
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v111_vac_large", float64(10))
	})

	// ============================================================
	// SECTION 6: storeIndexDef (unique and composite indexes)
	// ============================================================

	t.Run("StoreIndexDef_UniqueIndex", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_uidx (id INTEGER PRIMARY KEY, email TEXT)")
		afExec(t, db, ctx, "CREATE UNIQUE INDEX v111_uidx_email ON v111_uidx (email)")
		afExec(t, db, ctx, "INSERT INTO v111_uidx VALUES (1, 'a@test.com')")
		afExec(t, db, ctx, "INSERT INTO v111_uidx VALUES (2, 'b@test.com')")
		_, err := db.Exec(ctx, "INSERT INTO v111_uidx VALUES (3, 'a@test.com')")
		if err == nil {
			t.Error("expected unique constraint violation")
		}
		afExpectRows(t, db, ctx, "SELECT * FROM v111_uidx", 2)
	})

	t.Run("StoreIndexDef_CompositeIndex", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_cidx (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT)")
		afExec(t, db, ctx, "CREATE INDEX v111_cidx_name ON v111_cidx (first_name, last_name)")
		afExec(t, db, ctx, "INSERT INTO v111_cidx VALUES (1, 'John', 'Doe')")
		afExec(t, db, ctx, "INSERT INTO v111_cidx VALUES (2, 'Jane', 'Doe')")
		afExec(t, db, ctx, "INSERT INTO v111_cidx VALUES (3, 'John', 'Smith')")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_cidx WHERE first_name = 'John'", 2)
		afExpectRows(t, db, ctx, "SELECT * FROM v111_cidx WHERE last_name = 'Doe'", 2)
	})

	t.Run("StoreIndexDef_UniqueComposite", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_ucidx (id INTEGER PRIMARY KEY, dept TEXT, emp_code TEXT)")
		afExec(t, db, ctx, "CREATE UNIQUE INDEX v111_ucidx_de ON v111_ucidx (dept, emp_code)")
		afExec(t, db, ctx, "INSERT INTO v111_ucidx VALUES (1, 'eng', 'E001')")
		afExec(t, db, ctx, "INSERT INTO v111_ucidx VALUES (2, 'eng', 'E002')")
		afExec(t, db, ctx, "INSERT INTO v111_ucidx VALUES (3, 'sales', 'E001')")
		_, err := db.Exec(ctx, "INSERT INTO v111_ucidx VALUES (4, 'eng', 'E001')")
		if err == nil {
			t.Error("expected unique composite constraint violation")
		}
		afExpectRows(t, db, ctx, "SELECT * FROM v111_ucidx", 3)
	})

	t.Run("StoreIndexDef_IndexAfterData", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_idx_after (id INTEGER PRIMARY KEY, status TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_idx_after VALUES (1, 'active')")
		afExec(t, db, ctx, "INSERT INTO v111_idx_after VALUES (2, 'inactive')")
		afExec(t, db, ctx, "INSERT INTO v111_idx_after VALUES (3, 'active')")
		afExec(t, db, ctx, "CREATE INDEX v111_idx_after_status ON v111_idx_after (status)")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_idx_after WHERE status = 'active'", 2)
	})

	t.Run("StoreIndexDef_DropAndRecreate", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_idx_rc (id INTEGER PRIMARY KEY, grade TEXT)")
		afExec(t, db, ctx, "CREATE INDEX v111_idx_rc_g ON v111_idx_rc (grade)")
		afExec(t, db, ctx, "INSERT INTO v111_idx_rc VALUES (1, 'A')")
		afExec(t, db, ctx, "DROP INDEX v111_idx_rc_g")
		afExec(t, db, ctx, "CREATE INDEX v111_idx_rc_g2 ON v111_idx_rc (grade)")
		afExpectVal(t, db, ctx, "SELECT grade FROM v111_idx_rc WHERE id = 1", "A")
	})

	// ============================================================
	// SECTION 7: Additional DDL in Savepoint Coverage
	// ============================================================

	t.Run("SP_AlterRenameColumn_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_ren_col (id INTEGER PRIMARY KEY, old_col TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_ren_col VALUES (1, 'val1')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "ALTER TABLE v111_ren_col RENAME COLUMN old_col TO new_col")
		afExpectVal(t, db, ctx, "SELECT new_col FROM v111_ren_col WHERE id = 1", "val1")
		afExec(t, db, ctx, "ROLLBACK")
		afExpectVal(t, db, ctx, "SELECT old_col FROM v111_ren_col WHERE id = 1", "val1")
	})

	t.Run("SP_CreateDropTable_Sequence", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_cdseq")
		afExec(t, db, ctx, "CREATE TABLE v111_cdseq (id INTEGER PRIMARY KEY)")
		afExec(t, db, ctx, "DROP TABLE v111_cdseq")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_cdseq")
		_, err := db.Exec(ctx, "SELECT * FROM v111_cdseq")
		if err == nil {
			t.Error("v111_cdseq should not exist")
		}
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_CreateMultipleIndex_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_mi (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v111_mi VALUES (1, 'x', 'y', 10)")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_mi")
		afExec(t, db, ctx, "CREATE INDEX v111_mi_a ON v111_mi (a)")
		afExec(t, db, ctx, "CREATE INDEX v111_mi_b ON v111_mi (b)")
		afExec(t, db, ctx, "CREATE INDEX v111_mi_c ON v111_mi (c)")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_mi")
		afExpectVal(t, db, ctx, "SELECT a FROM v111_mi WHERE id = 1", "x")
		afExec(t, db, ctx, "COMMIT")
	})

	// ============================================================
	// SECTION 8: Complex savepoint scenarios
	// ============================================================

	t.Run("SP_InsertUpdateDelete_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_sp_iud (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_sp_iud VALUES (1, 'a')")
		afExec(t, db, ctx, "INSERT INTO v111_sp_iud VALUES (2, 'b')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_iud")
		afExec(t, db, ctx, "INSERT INTO v111_sp_iud VALUES (3, 'c')")
		afExec(t, db, ctx, "UPDATE v111_sp_iud SET val = 'x' WHERE id = 1")
		afExec(t, db, ctx, "DELETE FROM v111_sp_iud WHERE id = 2")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_iud")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_sp_iud", 2)
		afExpectVal(t, db, ctx, "SELECT val FROM v111_sp_iud WHERE id = 1", "a")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_sp_iud WHERE id = 2", "b")
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_ReuseSavepointName", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_sp_reuse (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_r")
		afExec(t, db, ctx, "INSERT INTO v111_sp_reuse VALUES (1, 'first')")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_r")
		afExec(t, db, ctx, "SAVEPOINT sp_r")
		afExec(t, db, ctx, "INSERT INTO v111_sp_reuse VALUES (2, 'second')")
		afExec(t, db, ctx, "COMMIT")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_sp_reuse", 1)
		afExpectVal(t, db, ctx, "SELECT val FROM v111_sp_reuse WHERE id = 2", "second")
	})

	t.Run("SP_ReleaseSavepoint", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_rel_sp (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_rel")
		afExec(t, db, ctx, "INSERT INTO v111_rel_sp VALUES (1, 'released')")
		afExec(t, db, ctx, "RELEASE SAVEPOINT sp_rel")
		afExec(t, db, ctx, "COMMIT")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_rel_sp WHERE id = 1", "released")
	})

	t.Run("SP_RollbackAfterInsertDelete", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_sp_id (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_sp_id VALUES (1, 'original')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_id1")
		afExec(t, db, ctx, "DELETE FROM v111_sp_id WHERE id = 1")
		afExec(t, db, ctx, "INSERT INTO v111_sp_id VALUES (2, 'new')")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_id1")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_sp_id WHERE id = 1", "original")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_sp_id WHERE id = 2", 0)
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_TripleSavepoint_PartialRollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_tri_sp (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "INSERT INTO v111_tri_sp VALUES (1, 'base')")
		afExec(t, db, ctx, "SAVEPOINT sp_a")
		afExec(t, db, ctx, "INSERT INTO v111_tri_sp VALUES (2, 'a')")
		afExec(t, db, ctx, "SAVEPOINT sp_b")
		afExec(t, db, ctx, "INSERT INTO v111_tri_sp VALUES (3, 'b')")
		afExec(t, db, ctx, "SAVEPOINT sp_c")
		afExec(t, db, ctx, "INSERT INTO v111_tri_sp VALUES (4, 'c')")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_b")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_tri_sp", 2)
		afExec(t, db, ctx, "COMMIT")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_tri_sp", 2)
	})

	t.Run("SP_DDL_DML_Interleaved", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "CREATE TABLE v111_interleave (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_interleave VALUES (1, 'a')")
		afExec(t, db, ctx, "SAVEPOINT sp_il")
		afExec(t, db, ctx, "ALTER TABLE v111_interleave ADD COLUMN extra TEXT")
		afExec(t, db, ctx, "INSERT INTO v111_interleave VALUES (2, 'b', 'e')")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_il")
		_, err := db.Exec(ctx, "SELECT extra FROM v111_interleave")
		if err == nil {
			t.Error("extra column should not exist")
		}
		afExpectVal(t, db, ctx, "SELECT val FROM v111_interleave WHERE id = 1", "a")
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_AlterAddMultipleColumns_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_sp_mac (id INTEGER PRIMARY KEY, name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_sp_mac VALUES (1, 'test')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_mac")
		afExec(t, db, ctx, "ALTER TABLE v111_sp_mac ADD COLUMN col2 TEXT")
		afExec(t, db, ctx, "ALTER TABLE v111_sp_mac ADD COLUMN col3 INTEGER")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_mac")
		_, err := db.Exec(ctx, "SELECT col2 FROM v111_sp_mac")
		if err == nil {
			t.Error("col2 should not exist after rollback")
		}
		afExec(t, db, ctx, "COMMIT")
	})

	// ============================================================
	// SECTION 9: Transaction edge cases
	// ============================================================

	t.Run("Txn_MultipleInsertRollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_multi (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_multi VALUES (1, 'pre')")
		afExec(t, db, ctx, "BEGIN")
		for i := 2; i <= 10; i++ {
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_txn_multi VALUES (%d, 'txn_%d')", i, i))
		}
		afExec(t, db, ctx, "ROLLBACK")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_txn_multi", 1)
	})

	t.Run("Txn_UpdateRollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_upd (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_upd VALUES (1, 'original')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "UPDATE v111_txn_upd SET val = 'modified' WHERE id = 1")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_txn_upd WHERE id = 1", "modified")
		afExec(t, db, ctx, "ROLLBACK")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_txn_upd WHERE id = 1", "original")
	})

	t.Run("Txn_DeleteRollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_del (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_del VALUES (1, 'keep')")
		afExec(t, db, ctx, "INSERT INTO v111_txn_del VALUES (2, 'also_keep')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "DELETE FROM v111_txn_del WHERE id = 1")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_txn_del", 1)
		afExec(t, db, ctx, "ROLLBACK")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_txn_del", 2)
	})

	t.Run("Txn_CreateUniqueIndex_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_uidx (id INTEGER PRIMARY KEY, code TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_uidx VALUES (1, 'abc')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "CREATE UNIQUE INDEX v111_txn_uidx_code ON v111_txn_uidx (code)")
		afExec(t, db, ctx, "ROLLBACK")
		afExec(t, db, ctx, "INSERT INTO v111_txn_uidx VALUES (2, 'abc')")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_txn_uidx", 2)
	})

	t.Run("Txn_CreateCompositeIndex_Commit", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_comp (id INTEGER PRIMARY KEY, x TEXT, y TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_comp VALUES (1, 'a', 'b')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "CREATE INDEX v111_txn_comp_xy ON v111_txn_comp (x, y)")
		afExec(t, db, ctx, "COMMIT")
		afExpectVal(t, db, ctx, "SELECT x FROM v111_txn_comp WHERE id = 1", "a")
	})

	// ============================================================
	// SECTION 10: ANALYZE
	// ============================================================

	t.Run("Analyze_Basic", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_an (id INTEGER PRIMARY KEY, grade TEXT, score INTEGER)")
		for i := 1; i <= 20; i++ {
			grade := "A"
			if i > 10 {
				grade = "B"
			}
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_an VALUES (%d, '%s', %d)", i, grade, i*10))
		}
		afExec(t, db, ctx, "ANALYZE")
	})

	t.Run("Analyze_EmptyTable", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_an_empty (id INTEGER PRIMARY KEY)")
		afExec(t, db, ctx, "ANALYZE")
	})

	// ============================================================
	// SECTION 11: Edge cases
	// ============================================================

	t.Run("CreateTable_IfNotExists", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_ine (id INTEGER PRIMARY KEY)")
		afExec(t, db, ctx, "CREATE TABLE IF NOT EXISTS v111_ine (id INTEGER PRIMARY KEY)")
	})

	t.Run("DropTable_IfExists", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "DROP TABLE IF EXISTS v111_nonexistent_table")
	})

	t.Run("Insert_DefaultValues", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_dv (id INTEGER PRIMARY KEY, val TEXT DEFAULT 'hello', num INTEGER DEFAULT 42)")
		afExec(t, db, ctx, "INSERT INTO v111_dv (id) VALUES (1)")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_dv WHERE id = 1", "hello")
		afExpectVal(t, db, ctx, "SELECT num FROM v111_dv WHERE id = 1", float64(42))
	})

	t.Run("Insert_NullValues", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_nulls (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_nulls VALUES (1, NULL)")
		rows := afQuery(t, db, ctx, "SELECT val FROM v111_nulls WHERE id = 1")
		if len(rows) == 0 {
			t.Fatal("expected 1 row")
		}
		if rows[0][0] != nil {
			t.Errorf("expected nil, got %v", rows[0][0])
		}
	})

	t.Run("SelectDistinct_WithCTE", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_dc (id INTEGER PRIMARY KEY, category TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_dc VALUES (1, 'A')")
		afExec(t, db, ctx, "INSERT INTO v111_dc VALUES (2, 'B')")
		afExec(t, db, ctx, "INSERT INTO v111_dc VALUES (3, 'A')")
		afExec(t, db, ctx, "INSERT INTO v111_dc VALUES (4, 'C')")
		rows := afQuery(t, db, ctx, `
			WITH cats AS (
				SELECT DISTINCT category FROM v111_dc
			)
			SELECT * FROM cats ORDER BY category`)
		if len(rows) != 3 {
			t.Errorf("expected 3, got %d", len(rows))
		}
	})

	t.Run("CTE_ExceptFiltered", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_cef (id INTEGER PRIMARY KEY, category TEXT, amount REAL)")
		afExec(t, db, ctx, "INSERT INTO v111_cef VALUES (1, 'A', 10.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cef VALUES (2, 'B', 20.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cef VALUES (3, 'A', 30.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cef VALUES (4, 'C', 40.0)")
		afExec(t, db, ctx, "INSERT INTO v111_cef VALUES (5, 'A', 60.0)")
		rows := afQuery(t, db, ctx, `
			WITH diff AS (
				SELECT category FROM v111_cef
				EXCEPT
				SELECT category FROM v111_cef WHERE amount > 50
			)
			SELECT * FROM diff ORDER BY category`)
		if len(rows) < 1 {
			t.Error("expected at least 1 row from EXCEPT")
		}
	})

	// ============================================================
	// SECTION 12: More savepoint and transaction combos
	// ============================================================

	t.Run("SP_UpdateOnly_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_sp_uo (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_sp_uo VALUES (1, 'orig')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_uo")
		afExec(t, db, ctx, "UPDATE v111_sp_uo SET val = 'changed' WHERE id = 1")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_sp_uo WHERE id = 1", "changed")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_uo")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_sp_uo WHERE id = 1", "orig")
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_DeleteOnly_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_sp_do (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_sp_do VALUES (1, 'keep')")
		afExec(t, db, ctx, "INSERT INTO v111_sp_do VALUES (2, 'keep2')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_do")
		afExec(t, db, ctx, "DELETE FROM v111_sp_do WHERE id = 1")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_sp_do", 1)
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_do")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_sp_do", 2)
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("SP_InsertOnly_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_sp_io (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_io")
		afExec(t, db, ctx, "INSERT INTO v111_sp_io VALUES (1, 'a')")
		afExec(t, db, ctx, "INSERT INTO v111_sp_io VALUES (2, 'b')")
		afExec(t, db, ctx, "INSERT INTO v111_sp_io VALUES (3, 'c')")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_io")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_sp_io", 0)
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("Txn_AlterRenameColumn_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_txn_rc (id INTEGER PRIMARY KEY, old_name TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_txn_rc VALUES (1, 'test')")
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "ALTER TABLE v111_txn_rc RENAME COLUMN old_name TO new_name")
		afExpectVal(t, db, ctx, "SELECT new_name FROM v111_txn_rc WHERE id = 1", "test")
		afExec(t, db, ctx, "ROLLBACK")
		afExpectVal(t, db, ctx, "SELECT old_name FROM v111_txn_rc WHERE id = 1", "test")
	})

	t.Run("SP_DropTableWithData_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_dtwd (id INTEGER PRIMARY KEY, val TEXT)")
		for i := 1; i <= 5; i++ {
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v111_dtwd VALUES (%d, 'val_%d')", i, i))
		}
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_dtwd")
		afExec(t, db, ctx, "DROP TABLE v111_dtwd")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_dtwd")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_dtwd", 5)
		afExec(t, db, ctx, "COMMIT")
	})

	t.Run("Txn_InsertAfterCreateInSameTxn", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "CREATE TABLE v111_tia (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_tia VALUES (1, 'first')")
		afExec(t, db, ctx, "INSERT INTO v111_tia VALUES (2, 'second')")
		afExec(t, db, ctx, "INSERT INTO v111_tia VALUES (3, 'third')")
		afExec(t, db, ctx, "COMMIT")
		afExpectRows(t, db, ctx, "SELECT * FROM v111_tia", 3)
	})

	t.Run("SP_CreateIndexOnNewTable_Rollback", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "BEGIN")
		afExec(t, db, ctx, "SAVEPOINT sp_cint")
		afExec(t, db, ctx, "CREATE TABLE v111_cint (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "CREATE INDEX v111_cint_idx ON v111_cint (val)")
		afExec(t, db, ctx, "INSERT INTO v111_cint VALUES (1, 'test')")
		afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp_cint")
		_, err := db.Exec(ctx, "SELECT * FROM v111_cint")
		if err == nil {
			t.Error("v111_cint should not exist")
		}
		afExec(t, db, ctx, "COMMIT")
	})

	// ============================================================
	// SECTION 13: CTE with DISTINCT and complex queries
	// ============================================================

	t.Run("CTE_DistinctUnion", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_cdu (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_cdu VALUES (1, 'a')")
		afExec(t, db, ctx, "INSERT INTO v111_cdu VALUES (2, 'b')")
		afExec(t, db, ctx, "INSERT INTO v111_cdu VALUES (3, 'a')")
		rows := afQuery(t, db, ctx, `
			WITH u AS (
				SELECT val FROM v111_cdu WHERE id <= 2
				UNION
				SELECT val FROM v111_cdu WHERE id >= 2
			)
			SELECT * FROM u ORDER BY val`)
		if len(rows) != 2 {
			t.Errorf("expected 2 distinct vals, got %d", len(rows))
		}
	})

	t.Run("CTE_Recursive_StringConcat", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		rows := afQuery(t, db, ctx, `
			WITH RECURSIVE letters(n, s) AS (
				SELECT 1, 'a'
				UNION ALL
				SELECT n + 1, s || 'a' FROM letters WHERE n < 5
			)
			SELECT s FROM letters ORDER BY n`)
		if len(rows) != 5 {
			t.Errorf("expected 5, got %d", len(rows))
		}
		if len(rows) > 4 && fmt.Sprintf("%v", rows[4][0]) != "aaaaa" {
			t.Errorf("expected 'aaaaa', got %v", rows[4][0])
		}
	})

	t.Run("CTE_MultiUnion", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		rows := afQuery(t, db, ctx, `
			WITH nums AS (
				SELECT 1 AS n
				UNION ALL
				SELECT 2
				UNION ALL
				SELECT 3
			)
			SELECT SUM(n) FROM nums`)
		if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) != "6" {
			t.Errorf("expected 6, got %v", rows[0][0])
		}
	})

	t.Run("CTE_WithHaving", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_ch (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v111_ch VALUES (1, 'X', 10)")
		afExec(t, db, ctx, "INSERT INTO v111_ch VALUES (2, 'X', 20)")
		afExec(t, db, ctx, "INSERT INTO v111_ch VALUES (3, 'Y', 30)")
		afExec(t, db, ctx, "INSERT INTO v111_ch VALUES (4, 'X', 40)")
		rows := afQuery(t, db, ctx, `
			WITH grouped AS (
				SELECT grp, SUM(val) AS total
				FROM v111_ch
				GROUP BY grp
				HAVING SUM(val) > 40
			)
			SELECT grp, total FROM grouped`)
		if len(rows) != 1 {
			t.Errorf("expected 1 group with total > 40, got %d", len(rows))
		}
	})

	// Vacuum after various DDL
	t.Run("Vacuum_AfterDropRecreate", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v111_vdr (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_vdr VALUES (1, 'a')")
		afExec(t, db, ctx, "DROP TABLE v111_vdr")
		afExec(t, db, ctx, "CREATE TABLE v111_vdr (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v111_vdr VALUES (1, 'b')")
		afExec(t, db, ctx, "VACUUM")
		afExpectVal(t, db, ctx, "SELECT val FROM v111_vdr WHERE id = 1", "b")
	})
}

// TestV111RLSCoverage tests Row-Level Security paths through the engine API.
func TestV111RLSCoverage(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true, EnableRLS: true})
	if err != nil {
		t.Fatalf("Failed to open DB with RLS: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE v111_rls (id INTEGER PRIMARY KEY, owner TEXT, dept TEXT, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO v111_rls VALUES (1, 'alice', 'eng', 'secret1')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO v111_rls VALUES (2, 'bob', 'sales', 'secret2')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO v111_rls VALUES (3, 'alice', 'eng', 'secret3')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO v111_rls VALUES (4, 'carol', 'hr', 'secret4')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO v111_rls VALUES (5, 'dave', 'eng', 'secret5')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	t.Run("RLS_CreatePolicy_All", func(t *testing.T) {
		_, err := db.Exec(ctx, "CREATE POLICY v111_pol_all ON v111_rls FOR ALL USING (owner = 'alice')")
		if err != nil {
			t.Fatalf("create policy: %v", err)
		}
	})

	t.Run("RLS_SelectAfterPolicy", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT * FROM v111_rls")
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count < 1 {
			t.Error("expected at least 1 row")
		}
	})

	t.Run("RLS_CreatePolicy_Select", func(t *testing.T) {
		_, err := db.Exec(ctx, "CREATE POLICY v111_pol_sel ON v111_rls FOR SELECT USING (dept = 'eng')")
		if err != nil {
			t.Fatalf("create select policy: %v", err)
		}
	})

	t.Run("RLS_CreatePolicy_Insert", func(t *testing.T) {
		_, err := db.Exec(ctx, "CREATE POLICY v111_pol_ins ON v111_rls FOR INSERT USING (dept = 'eng')")
		if err != nil {
			t.Fatalf("create insert policy: %v", err)
		}
	})

	t.Run("RLS_CreatePolicy_Update", func(t *testing.T) {
		_, err := db.Exec(ctx, "CREATE POLICY v111_pol_upd ON v111_rls FOR UPDATE USING (dept = 'eng')")
		if err != nil {
			t.Fatalf("create update policy: %v", err)
		}
	})

	t.Run("RLS_CreatePolicy_Delete", func(t *testing.T) {
		_, err := db.Exec(ctx, "CREATE POLICY v111_pol_del ON v111_rls FOR DELETE USING (dept = 'eng')")
		if err != nil {
			t.Fatalf("create delete policy: %v", err)
		}
	})

	t.Run("RLS_InsertWithPolicy", func(t *testing.T) {
		_, err := db.Exec(ctx, "INSERT INTO v111_rls VALUES (6, 'eve', 'eng', 'secret6')")
		if err != nil {
			t.Fatalf("insert with RLS: %v", err)
		}
	})

	t.Run("RLS_UpdateWithPolicy", func(t *testing.T) {
		_, err := db.Exec(ctx, "UPDATE v111_rls SET val = 'updated' WHERE id = 1")
		if err != nil {
			t.Fatalf("update with RLS: %v", err)
		}
	})

	t.Run("RLS_DeleteWithPolicy", func(t *testing.T) {
		_, err := db.Exec(ctx, "DELETE FROM v111_rls WHERE id = 6")
		if err != nil {
			t.Fatalf("delete with RLS: %v", err)
		}
	})

	t.Run("RLS_SelectWithWhere", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT val FROM v111_rls WHERE dept = 'eng'")
		if err != nil {
			t.Fatalf("select with where: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count < 1 {
			t.Error("expected at least 1 eng row")
		}
	})

	t.Run("RLS_SelectWithJoin", func(t *testing.T) {
		_, err := db.Exec(ctx, "CREATE TABLE v111_rls_dept (dept TEXT, budget INTEGER)")
		if err != nil {
			t.Fatalf("create dept: %v", err)
		}
		_, err = db.Exec(ctx, "INSERT INTO v111_rls_dept VALUES ('eng', 1000)")
		if err != nil {
			t.Fatalf("insert dept: %v", err)
		}
		_, err = db.Exec(ctx, "INSERT INTO v111_rls_dept VALUES ('sales', 2000)")
		if err != nil {
			t.Fatalf("insert dept: %v", err)
		}
		rows, err := db.Query(ctx, "SELECT r.owner, d.budget FROM v111_rls r JOIN v111_rls_dept d ON r.dept = d.dept ORDER BY r.id")
		if err != nil {
			t.Fatalf("join: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count < 1 {
			t.Error("expected at least 1 row from join")
		}
	})

	t.Run("RLS_SecondTable", func(t *testing.T) {
		_, err := db.Exec(ctx, "CREATE TABLE v111_rls2 (id INTEGER PRIMARY KEY, region TEXT, amount REAL)")
		if err != nil {
			t.Fatalf("create table: %v", err)
		}
		_, err = db.Exec(ctx, "INSERT INTO v111_rls2 VALUES (1, 'north', 100.0)")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		_, err = db.Exec(ctx, "INSERT INTO v111_rls2 VALUES (2, 'south', 200.0)")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		_, err = db.Exec(ctx, "CREATE POLICY v111_pol2 ON v111_rls2 FOR ALL USING (region = 'north')")
		if err != nil {
			t.Fatalf("create policy: %v", err)
		}
		rows, err := db.Query(ctx, "SELECT * FROM v111_rls2")
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count < 1 {
			t.Error("expected at least 1 row")
		}
	})

	t.Run("RLS_AggregateWithPolicy", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT COUNT(*), SUM(amount) FROM v111_rls2")
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected result")
		}
	})

	t.Run("RLS_SubqueryWithPolicy", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT * FROM v111_rls WHERE id IN (SELECT id FROM v111_rls WHERE dept = 'eng')")
		if err != nil {
			t.Fatalf("subquery: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count < 1 {
			t.Error("expected at least 1 row from subquery")
		}
	})

	t.Run("RLS_OrderByWithPolicy", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT id, owner FROM v111_rls ORDER BY id")
		if err != nil {
			t.Fatalf("order by: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count < 1 {
			t.Error("expected rows")
		}
	})

	t.Run("RLS_GroupByWithPolicy", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT dept, COUNT(*) FROM v111_rls GROUP BY dept")
		if err != nil {
			t.Fatalf("group by: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count < 1 {
			t.Error("expected groups")
		}
	})

	t.Run("RLS_UpdateMultiple", func(t *testing.T) {
		_, err := db.Exec(ctx, "UPDATE v111_rls SET val = 'bulk_update' WHERE dept = 'eng'")
		if err != nil {
			t.Fatalf("bulk update: %v", err)
		}
	})

	t.Run("RLS_DeleteMultiple", func(t *testing.T) {
		_, err := db.Exec(ctx, "INSERT INTO v111_rls VALUES (7, 'temp', 'eng', 'todelete1')")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		_, err = db.Exec(ctx, "INSERT INTO v111_rls VALUES (8, 'temp', 'eng', 'todelete2')")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		_, err = db.Exec(ctx, "DELETE FROM v111_rls WHERE owner = 'temp'")
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
	})
}
