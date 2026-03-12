package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Savepoint: DROP TABLE, DROP INDEX rollback
// ============================================================

func TestCoverage_SavepointDropTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "sp_drop",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_sp_val", Table: "sp_drop", Columns: []string{"val"}})
	if err != nil {
		t.Fatal(err)
	}

	// Begin txn, savepoint, drop table, rollback to savepoint
	cat.BeginTransaction(1)
	_ = cat.Savepoint("sp1")

	err = cat.DropTable(&query.DropTableStmt{Table: "sp_drop"})
	if err != nil {
		t.Fatal(err)
	}

	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Errorf("rollback to savepoint: %v", err)
	}

	// Table should be restored
	_, getErr := cat.GetTable("sp_drop")
	if getErr != nil {
		t.Error("sp_drop should exist after savepoint rollback")
	}

	_ = cat.CommitTransaction()
	pool.Close()
}

func TestCoverage_SavepointDropIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "sp_didx",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_sp_didx", Table: "sp_didx", Columns: []string{"val"}})
	if err != nil {
		t.Fatal(err)
	}

	// Begin txn, savepoint, drop index, rollback to savepoint
	cat.BeginTransaction(1)
	_ = cat.Savepoint("sp2")

	err = cat.DropIndex("idx_sp_didx")
	if err != nil {
		t.Fatal(err)
	}

	err = cat.RollbackToSavepoint("sp2")
	if err != nil {
		t.Errorf("rollback drop index: %v", err)
	}

	// Index should be restored
	_, getErr := cat.GetIndex("idx_sp_didx")
	if getErr != nil {
		t.Error("index should exist after savepoint rollback")
	}

	_ = cat.CommitTransaction()
	pool.Close()
}

// ============================================================
// Savepoint: ALTER TABLE ADD/DROP COLUMN, RENAME, RENAME COLUMN
// ============================================================

func TestCoverage_SavepointAlterAddColumn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "sp_alt",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	cat.BeginTransaction(1)
	_ = cat.Savepoint("sp_add")

	err = cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "sp_alt",
		Action: "ADD",
		Column: query.ColumnDef{Name: "extra", Type: query.TokenText},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cat.RollbackToSavepoint("sp_add")
	if err != nil {
		t.Errorf("rollback alter add column: %v", err)
	}

	tbl, _ := cat.GetTable("sp_alt")
	for _, col := range tbl.Columns {
		if col.Name == "extra" {
			t.Error("extra column should not exist after savepoint rollback")
		}
	}

	_ = cat.CommitTransaction()
	pool.Close()
}

func TestCoverage_SavepointAlterDropColumn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "sp_altd",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "extra", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table: "sp_altd",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "alice"},
			&query.StringLiteral{Value: "info"},
		}},
	}, nil)

	cat.BeginTransaction(1)
	_ = cat.Savepoint("sp_drop_col")

	err = cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "sp_altd",
		Action:  "DROP",
		NewName: "extra",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cat.RollbackToSavepoint("sp_drop_col")
	if err != nil {
		t.Errorf("rollback alter drop column: %v", err)
	}

	tbl, _ := cat.GetTable("sp_altd")
	found := false
	for _, col := range tbl.Columns {
		if col.Name == "extra" {
			found = true
		}
	}
	if !found {
		t.Error("extra column should exist after savepoint rollback")
	}

	_ = cat.CommitTransaction()
	pool.Close()
}

func TestCoverage_SavepointAutoIncSeq(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "sp_auto",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	cat.BeginTransaction(1)
	_ = cat.Savepoint("sp_auto1")

	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_auto",
		Columns: []string{"val"},
		Values:  [][]query.Expression{{&query.StringLiteral{Value: "a"}}},
	}, nil)

	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_auto",
		Columns: []string{"val"},
		Values:  [][]query.Expression{{&query.StringLiteral{Value: "b"}}},
	}, nil)

	err = cat.RollbackToSavepoint("sp_auto1")
	if err != nil {
		t.Errorf("rollback auto inc: %v", err)
	}

	_ = cat.CommitTransaction()
	pool.Close()
}

// ============================================================
// Full Rollback: DROP INDEX, ALTER paths
// ============================================================

func TestCoverage_RollbackDropIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "rb_idx",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_rb", Table: "rb_idx", Columns: []string{"val"}})
	if err != nil {
		t.Fatal(err)
	}

	// BEGIN, DROP INDEX, ROLLBACK
	cat.BeginTransaction(1)
	err = cat.DropIndex("idx_rb")
	if err != nil {
		t.Fatal(err)
	}
	_ = cat.RollbackTransaction()

	_, getErr := cat.GetIndex("idx_rb")
	if getErr != nil {
		t.Error("index should be restored after rollback")
	}

	pool.Close()
}

func TestCoverage_RollbackAlterDropColumn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "rb_altd",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "extra", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table: "rb_altd",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "alice"},
			&query.StringLiteral{Value: "info"},
		}},
	}, nil)

	// Create index on the column we'll drop
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_rb_extra", Table: "rb_altd", Columns: []string{"extra"}})
	if err != nil {
		t.Fatal(err)
	}

	// BEGIN, DROP COLUMN, ROLLBACK
	cat.BeginTransaction(1)
	err = cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table: "rb_altd", Action: "DROP", NewName: "extra",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = cat.RollbackTransaction()

	tbl, _ := cat.GetTable("rb_altd")
	found := false
	for _, col := range tbl.Columns {
		if col.Name == "extra" {
			found = true
		}
	}
	if !found {
		t.Error("extra column should be restored")
	}

	pool.Close()
}

func TestCoverage_RollbackAlterRename(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "rb_rename",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create an index to test rename propagation
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_rb_ren", Table: "rb_rename", Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}

	// BEGIN, RENAME TABLE, ROLLBACK
	cat.BeginTransaction(1)
	err = cat.AlterTableRename(&query.AlterTableStmt{
		Table: "rb_rename", Action: "RENAME_TABLE", NewName: "rb_newname",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = cat.RollbackTransaction()

	_, getErr := cat.GetTable("rb_rename")
	if getErr != nil {
		t.Error("rb_rename should exist after rename rollback")
	}

	pool.Close()
}

func TestCoverage_RollbackAlterRenameColumn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "rb_rencol",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create index on name column
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_rb_rencol", Table: "rb_rencol", Columns: []string{"name"}})
	if err != nil {
		t.Fatal(err)
	}

	// BEGIN, RENAME COLUMN, ROLLBACK
	cat.BeginTransaction(1)
	err = cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table: "rb_rencol", Action: "RENAME_COLUMN", OldName: "name", NewName: "label",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = cat.RollbackTransaction()

	tbl, _ := cat.GetTable("rb_rencol")
	found := false
	for _, col := range tbl.Columns {
		if col.Name == "name" {
			found = true
		}
	}
	if !found {
		t.Error("name column should exist after rename column rollback")
	}

	pool.Close()
}

// ============================================================
// Delete with index during transaction (undo path)
// ============================================================

func TestCoverage_DeleteWithIndex_Txn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "delidx_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create unique index
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_delidx_u", Table: "delidx_t", Columns: []string{"val"}, Unique: true})
	if err != nil {
		t.Fatal(err)
	}

	// Create non-unique index
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_delidx_n", Table: "delidx_t", Columns: []string{"val"}})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table: "delidx_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "hello"},
		}},
	}, nil)

	// Delete within a transaction (exercises index undo path)
	cat.BeginTransaction(1)
	err = cat.DeleteRow(ctx, "delidx_t", int64(1))
	if err != nil {
		t.Errorf("delete in txn: %v", err)
	}
	_ = cat.RollbackTransaction()

	pool.Close()
}
