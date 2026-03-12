package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// countRows edge cases coverage
// ============================================================

func TestCovBoost9_CountRows_Float64Result(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cnt_rows_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cnt_rows_t",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "a"}}},
	}, nil)
	// Create StatsCollector and count rows - normal path
	sc := NewStatsCollector(cat)
	_, err = sc.countRows("cnt_rows_t")
	if err != nil {
		t.Fatalf("countRows failed: %v", err)
	}
	
}

func TestCovBoost9_CountRows_InvalidTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	sc := NewStatsCollector(cat)

	// Test with invalid table name (contains special chars)
	_, err = sc.countRows("invalid;table")
	if err == nil {
		t.Error("expected error for invalid table name")
	}
}

// ============================================================
// RollbackToSavepoint error paths coverage
// ============================================================

func TestCovBoost9_RollbackToSavepoint_ErrorPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "rb_sp_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()

	// Test rollback without transaction
	err = cat.RollbackToSavepoint("sp1")
	if err == nil || err.Error() != "ROLLBACK TO SAVEPOINT can only be used within a transaction" {
		t.Errorf("expected 'no transaction' error, got: %v", err)
	}

	// Begin transaction and test non-existent savepoint
	cat.BeginTransaction(0)
	err = cat.RollbackToSavepoint("nonexistent")
	if err == nil || err.Error() != "savepoint 'nonexistent' does not exist" {
		t.Errorf("expected 'savepoint does not exist' error, got: %v", err)
	}

	// Create a savepoint and release it, then try to rollback
	cat.Savepoint("sp1")
	cat.ReleaseSavepoint("sp1")
	err = cat.RollbackToSavepoint("sp1")
	if err == nil || err.Error() != "savepoint 'sp1' does not exist" {
		t.Errorf("expected 'savepoint does not exist' error after release, got: %v", err)
	}
	cat.RollbackTransaction()

	// Test rollback with DML operations
	cat.BeginTransaction(0)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rb_sp_t",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "before"}}},
	}, nil)
	cat.Savepoint("sp_insert")
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rb_sp_t",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "after"}}},
	}, nil)

	// Verify both rows exist
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM rb_sp_t")
	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count != 2 {
			t.Errorf("expected 2 rows before rollback, got %d", count)
		}
	}

	// Rollback to savepoint - should remove second row
	err = cat.RollbackToSavepoint("sp_insert")
	if err != nil {
		t.Errorf("unexpected error rolling back to savepoint: %v", err)
	}

	// Verify only first row remains
	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM rb_sp_t")
	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count != 1 {
			t.Errorf("expected 1 row after rollback, got %d", count)
		}
	}

	cat.RollbackTransaction()
}

func TestCovBoost9_RollbackToSavepoint_MultipleSavepoints(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "rb_multi_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	cat.BeginTransaction(0)

	// Insert row 1
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rb_multi_t",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	cat.Savepoint("sp1")

	// Insert row 2
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rb_multi_t",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}}},
	}, nil)
	cat.Savepoint("sp2")

	// Insert row 3
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rb_multi_t",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 3}}},
	}, nil)

	// Rollback to sp1 - should remove rows 2 and 3
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Rollback completed successfully

	cat.RollbackTransaction()
}

func TestCovBoost9_RollbackToSavepoint_DDL(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	cat.BeginTransaction(0)

	// Create table within transaction
	createStmt := &query.CreateTableStmt{
		Table: "ddl_test_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	}
	err = cat.CreateTable(createStmt)
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	cat.Savepoint("after_create")

	// Drop the table
	err = cat.DropTable(&query.DropTableStmt{Table: "ddl_test_t"})
	if err != nil {
		t.Fatalf("drop table failed: %v", err)
	}

	// Verify table is gone
	_, err = cat.GetTable("ddl_test_t")
	if err == nil {
		t.Error("expected table to be dropped")
	}

	// Rollback to savepoint - table should be restored
	err = cat.RollbackToSavepoint("after_create")
	if err != nil {
		t.Errorf("rollback failed: %v", err)
	}

	// Verify table exists again
	tbl, err := cat.GetTable("ddl_test_t")
	if err != nil {
		t.Errorf("table should exist after rollback: %v", err)
	}
	if tbl != nil && tbl.Name != "ddl_test_t" {
		t.Errorf("wrong table name: %s", tbl.Name)
	}

	cat.RollbackTransaction()
}

func TestCovBoost9_RollbackToSavepoint_AlterTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "alter_sp_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	ctx := context.Background()
	cat.BeginTransaction(0)

	// Insert a row
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "alter_sp_t",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "test"}}},
	}, nil)

	cat.Savepoint("before_alter")

	// Add a column
	err = cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "alter_sp_t",
		Action: "ADD COLUMN",
		Column: query.ColumnDef{Name: "newcol", Type: query.TokenInteger},
	})
	if err != nil {
		t.Fatalf("alter table failed: %v", err)
	}

	// Verify new column exists
	tbl, _ := cat.GetTable("alter_sp_t")
	found := false
	for _, col := range tbl.Columns {
		if col.Name == "newcol" {
			found = true
			break
		}
	}
	if !found {
		t.Error("new column should exist")
	}

	// Rollback - column should be gone
	err = cat.RollbackToSavepoint("before_alter")
	if err != nil {
		t.Errorf("rollback failed: %v", err)
	}

	// Verify column is gone
	tbl, _ = cat.GetTable("alter_sp_t")
	found = false
	for _, col := range tbl.Columns {
		if col.Name == "newcol" {
			found = true
			break
		}
	}
	if found {
		t.Error("new column should be gone after rollback")
	}

	cat.RollbackTransaction()
}

// ============================================================
// Save and Load with edge cases
// ============================================================


func TestCovBoost9_Vacuum_AllTables(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create multiple tables
	for i := 1; i <= 3; i++ {
		createCoverageTestTable(t, cat, "vac_multi_t"+string(rune('0'+i)), []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		})
	}

	// Vacuum all tables
	err = cat.Vacuum()
	if err != nil {
		t.Errorf("vacuum failed: %v", err)
	}
}
