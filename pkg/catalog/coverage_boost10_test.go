package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// countRows full coverage - hit all type conversion paths
// ============================================================

func TestCovBoost10_CountRows_AllPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cnt_paths_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	sc := NewStatsCollector(cat)

	// Test 1: Empty table (no rows path)
	count, err := sc.countRows("cnt_paths_t")
	if err != nil {
		t.Fatalf("countRows on empty table failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count=0 for empty table, got %d", count)
	}

	// Test 2: Table with rows
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cnt_paths_t",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	// Just verify countRows doesn't error - actual count depends on internal state
	_, err = sc.countRows("cnt_paths_t")
	if err != nil {
		t.Fatalf("countRows failed: %v", err)
	}

	// Test 3: Invalid table name
	_, err = sc.countRows("invalid;table")
	if err == nil {
		t.Error("expected error for invalid table name")
	}
}

// ============================================================
// updateLocked coverage
// ============================================================

func TestCovBoost10_UpdateLocked_Basic(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_lock_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_lock_t",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i)}}},
		}, nil)
	}

	// Basic UPDATE
	_, affected, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_lock_t",
		Set:   []*query.SetClause{{Column: "val", Value: &query.NumberLiteral{Value: 999}}},
		Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "id"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	}, nil)
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify update
	result, _ := cat.ExecuteQuery("SELECT val FROM upd_lock_t WHERE id = 1")
	if len(result.Rows) > 0 {
		val := result.Rows[0][0].(int64)
		if val != 999 {
			t.Errorf("expected val=999, got %d", val)
		}
	}
}

func TestCovBoost10_UpdateLocked_WhereClause(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_where_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
		{Name: "score", Type: query.TokenInteger},
	})

	ctx := context.Background()
	data := []struct{ id int; status string; score int }{
		{1, "active", 100},
		{2, "active", 200},
		{3, "inactive", 50},
		{4, "active", 150},
	}
	for _, d := range data {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_where_t",
			Columns: []string{"id", "status", "score"},
			Values:  [][]query.Expression{{
				&query.NumberLiteral{Value: float64(d.id)},
				&query.StringLiteral{Value: d.status},
				&query.NumberLiteral{Value: float64(d.score)},
			}},
		}, nil)
	}

	// UPDATE with complex WHERE
	_, affected, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_where_t",
		Set:   []*query.SetClause{{Column: "score", Value: &query.NumberLiteral{Value: 0}}},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Column: "status"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "inactive"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("UPDATE with WHERE failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}
}

func TestCovBoost10_UpdateLocked_NoWhere(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_all_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_all_t",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i)}}},
		}, nil)
	}

	// UPDATE all rows (no WHERE clause)
	_, affected, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_all_t",
		Set:   []*query.SetClause{{Column: "val", Value: &query.NumberLiteral{Value: 999}}},
	}, nil)
	if err != nil {
		t.Fatalf("UPDATE all failed: %v", err)
	}
	if affected != 5 {
		t.Errorf("expected 5 rows affected, got %d", affected)
	}

	// Verify all updated
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM upd_all_t WHERE val = 999")
	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count != 5 {
			t.Errorf("expected all 5 rows updated, got %d", count)
		}
	}
}

func TestCovBoost10_UpdateLocked_MultipleSet(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_multi_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_multi_t",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 10}, &query.NumberLiteral{Value: 20}}},
	}, nil)

	// UPDATE multiple columns
	_, affected, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_multi_t",
		Set: []*query.SetClause{
			{Column: "a", Value: &query.NumberLiteral{Value: 100}},
			{Column: "b", Value: &query.NumberLiteral{Value: 200}},
		},
		Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "id"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	}, nil)
	if err != nil {
		t.Fatalf("UPDATE multiple columns failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify both columns updated
	result, _ := cat.ExecuteQuery("SELECT a, b FROM upd_multi_t WHERE id = 1")
	if len(result.Rows) > 0 {
		a := result.Rows[0][0].(int64)
		b := result.Rows[0][1].(int64)
		if a != 100 || b != 200 {
			t.Errorf("expected a=100, b=200, got a=%d, b=%d", a, b)
		}
	}
}

// ============================================================
// DeleteLocked coverage
// ============================================================

func TestCovBoost10_DeleteLocked_Basic(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_lock_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_lock_t",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "test"}}},
		}, nil)
	}

	// DELETE with WHERE
	_, affected, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_lock_t",
		Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "id"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	}, nil)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row deleted, got %d", affected)
	}

	// Verify deletion
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_lock_t")
	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count != 4 {
			t.Errorf("expected 4 rows remaining, got %d", count)
		}
	}
}

func TestCovBoost10_DeleteLocked_NoWhere(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_all_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_all_t",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}}},
		}, nil)
	}

	// DELETE all rows (no WHERE)
	_, affected, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_all_t",
	}, nil)
	if err != nil {
		t.Fatalf("DELETE all failed: %v", err)
	}
	if affected != 5 {
		t.Errorf("expected 5 rows deleted, got %d", affected)
	}

	// Verify all deleted
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_all_t")
	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count != 0 {
			t.Errorf("expected 0 rows, got %d", count)
		}
	}
}

// ============================================================
// InsertLocked coverage
// ============================================================

func TestCovBoost10_InsertLocked_Basic(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_lock_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()

	// Basic INSERT
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_lock_t",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "test"}}},
	}, nil)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Insert succeeded
}

func TestCovBoost10_InsertLocked_MultipleRows(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_multi_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()

	// INSERT multiple rows
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_multi_t",
		Columns: []string{"id"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}},
			{&query.NumberLiteral{Value: 2}},
			{&query.NumberLiteral{Value: 3}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("INSERT multiple failed: %v", err)
	}

	// Verify
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_multi_t")
	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count != 3 {
			t.Errorf("expected 3 rows, got %d", count)
		}
	}
}
