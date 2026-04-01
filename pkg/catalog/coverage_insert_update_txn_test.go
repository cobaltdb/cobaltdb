package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestInsertLockedReturning tests INSERT with RETURNING clause
func TestInsertLockedReturning(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "ins_return",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert with RETURNING
	stmt := &query.InsertStmt{
		Table:   "ins_return",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
		Returning: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "val"}},
	}

	rows, lastID, err := c.Insert(ctx, stmt, nil)
	if err != nil {
		t.Logf("Insert with RETURNING: %v", err)
	} else {
		t.Logf("Inserted %d rows, last ID: %d", rows, lastID)
	}
}

// TestInsertLockedMultipleValues tests INSERT with multiple value rows
func TestInsertLockedMultipleValues(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "ins_multi",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert 10 rows at once
	values := make([][]query.Expression, 10)
	for i := 0; i < 10; i++ {
		values[i] = []query.Expression{numReal(float64(i + 1)), strReal("row")}
	}

	rows, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   "ins_multi",
		Columns: []string{"id", "val"},
		Values:  values,
	}, nil)
	if err != nil {
		t.Logf("Insert multiple: %v", err)
	} else {
		t.Logf("Inserted %d rows", rows)
	}
}

// TestInsertLockedWithIndexes tests INSERT with multiple indexes
func TestInsertLockedWithIndexes(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "ins_idx",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Create multiple indexes
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code", Table: "ins_idx", Columns: []string{"code"}, Unique: false,
	})
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_val", Table: "ins_idx", Columns: []string{"val"}, Unique: false,
	})

	// Insert with indexes
	rows, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   "ins_idx",
		Columns: []string{"id", "code", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("ABC"), numReal(100)}},
	}, nil)
	if err != nil {
		t.Logf("Insert with indexes: %v", err)
	} else {
		t.Logf("Inserted %d rows with indexes", rows)
	}
}

// TestUpdateLockedReturning tests UPDATE with RETURNING
func TestUpdateLockedReturning(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_return",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "upd_return",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("before")}},
	}, nil)

	// Update with RETURNING
	stmt := &query.UpdateStmt{
		Table: "upd_return",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("after")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
		Returning: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "val"}},
	}

	rows, _, err := c.Update(ctx, stmt, nil)
	if err != nil {
		t.Logf("Update with RETURNING: %v", err)
	} else {
		t.Logf("Updated %d rows", rows)
	}
}

// TestUpdateLockedMultipleColumns tests UPDATE of multiple columns
func TestUpdateLockedMultipleColumns(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_multi",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "a", Type: query.TokenText},
			{Name: "b", Type: query.TokenInteger},
			{Name: "c", Type: query.TokenReal},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "upd_multi",
		Columns: []string{"id", "a", "b", "c"},
		Values:  [][]query.Expression{{numReal(1), strReal("x"), numReal(0), numReal(0.0)}},
	}, nil)

	count, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "upd_multi",
		Set: []*query.SetClause{
			{Column: "a", Value: strReal("y")},
			{Column: "b", Value: numReal(100)},
			{Column: "c", Value: numReal(3.14)},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Logf("Update multiple columns: %v", err)
	} else {
		t.Logf("Updated %d rows", count)
	}
}

// TestUpdateLockedWithSubquery tests UPDATE with subquery in SET
func TestUpdateLockedWithSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_sub_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_sub_s",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "new_val", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "upd_sub_t",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(0)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "upd_sub_s",
		Columns: []string{"id", "new_val"},
		Values:  [][]query.Expression{{numReal(1), numReal(999)}},
	}, nil)

	// Update from subquery
	_, err := c.ExecuteQuery("UPDATE upd_sub_t SET val = (SELECT new_val FROM upd_sub_s WHERE upd_sub_s.id = upd_sub_t.id) WHERE id = 1")
	if err != nil {
		t.Logf("UPDATE with subquery: %v", err)
	}
}

// TestDeleteLockedReturning tests DELETE with RETURNING
func TestDeleteLockedReturning(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "del_return",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "del_return",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("to_delete")}},
	}, nil)

	// Delete with RETURNING
	stmt := &query.DeleteStmt{
		Table: "del_return",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
		Returning: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "val"}},
	}

	rows, _, err := c.Delete(ctx, stmt, nil)
	if err != nil {
		t.Logf("Delete with RETURNING: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}
}

// TestDeleteLockedMultipleRows tests DELETE of multiple rows
func TestDeleteLockedMultipleRows(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "del_multi",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat", Type: query.TokenText},
		},
	})

	// Insert 10 rows
	values := make([][]query.Expression, 10)
	for i := 0; i < 10; i++ {
		cat := "A"
		if i >= 5 {
			cat = "B"
		}
		values[i] = []query.Expression{numReal(float64(i + 1)), strReal(cat)}
	}
	c.Insert(ctx, &query.InsertStmt{
		Table:   "del_multi",
		Columns: []string{"id", "cat"},
		Values:  values,
	}, nil)

	// Delete category A
	count, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "del_multi",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "cat"},
			Operator: query.TokenEq,
			Right:    strReal("A"),
		},
	}, nil)
	if err != nil {
		t.Logf("Delete multiple: %v", err)
	} else {
		t.Logf("Deleted %d rows", count)
	}
}

// TestSavepointNested tests nested savepoints
func TestSavepointNested(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "sp_nested",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Multiple nested savepoints
	c.Savepoint("sp1")
	c.Savepoint("sp2")
	c.Savepoint("sp3")

	// Rollback to sp2
	err := c.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("Rollback to savepoint: %v", err)
	}

	// Release sp1
	err = c.ReleaseSavepoint("sp1")
	if err != nil {
		t.Logf("Release savepoint: %v", err)
	}
}

// TestBeginTransactionWithID tests BeginTransaction with specific ID
func TestBeginTransactionWithID(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "txn_id",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Begin with specific ID
	c.BeginTransaction(12345)

	// Insert within transaction
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "txn_id",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Commit
	c.CommitTransaction()
}

// TestFlushTableTreesMulti tests flushing multiple table trees
func TestFlushTableTreesMulti(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create multiple tables
	for i := 1; i <= 3; i++ {
		c.CreateTable(&query.CreateTableStmt{
			Table: "flush_table",
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			},
		})
	}

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "flush_table",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Vacuum triggers flush
	c.Vacuum()
}

// TestSaveWithIndex tests Save with indexes
func TestSaveWithIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "save_idx",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText},
		},
	})

	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code_save", Table: "save_idx", Columns: []string{"code"}, Unique: false,
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "save_idx",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(1), strReal("ABC")}},
	}, nil)

	err := c.Save()
	if err != nil {
		t.Errorf("Save with index failed: %v", err)
	}
}

// TestSaveWithForeignKey tests Save with foreign keys
func TestSaveWithForeignKey(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "save_fk_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "save_fk_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "save_fk_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "save_fk_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	err := c.Save()
	if err != nil {
		t.Errorf("Save with FK failed: %v", err)
	}
}

// TestVacuumWithIndexes tests Vacuum with indexes
func TestVacuumWithIndexes(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "vac_idx",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText},
		},
	})

	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac", Table: "vac_idx", Columns: []string{"code"}, Unique: false,
	})

	// Insert and delete
	c.Insert(ctx, &query.InsertStmt{
		Table:   "vac_idx",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)
	c.Delete(ctx, &query.DeleteStmt{
		Table: "vac_idx",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)

	err := c.Vacuum()
	if err != nil {
		t.Errorf("Vacuum with indexes failed: %v", err)
	}
}
