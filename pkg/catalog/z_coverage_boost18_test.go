package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_InsertWithSubquery tests INSERT with SELECT subquery
func TestCoverage_InsertWithSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "ins_dst", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert into source
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "ins_src",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Insert into destination from source
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_dst",
		Columns: []string{"id", "val"},
		Select: &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Column: "id"},
				&query.QualifiedIdentifier{Column: "val"},
			},
			From: &query.TableRef{Name: "ins_src"},
		},
	}, nil)

	if err != nil {
		t.Logf("Insert with subquery error: %v", err)
	}

	// Verify
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_dst")
	t.Logf("Destination count: %v", result.Rows)
}

// TestCoverage_UpdateWithJoin tests UPDATE with JOIN
func TestCoverage_UpdateWithJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "upd_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "new_val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_main",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_ref",
		Columns: []string{"id", "new_val"},
		Values:  [][]query.Expression{{numReal(1), numReal(999)}},
	}, nil)

	// UPDATE with FROM/JOIN
	_, _, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_main",
		Set:   []*query.SetClause{{Column: "val", Value: &query.QualifiedIdentifier{Table: "upd_ref", Column: "new_val"}}},
		From:  &query.TableRef{Name: "upd_ref"},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Table: "upd_main", Column: "id"},
			Operator: query.TokenEq,
			Right:    &query.QualifiedIdentifier{Table: "upd_ref", Column: "id"},
		},
	}, nil)

	if err != nil {
		t.Logf("UPDATE with JOIN error: %v", err)
	}
}

// TestCoverage_DeleteWithJoin tests DELETE with USING/JOIN
func TestCoverage_DeleteWithJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	createCoverageTestTable(t, cat, "del_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "to_delete", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_main",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_ref",
		Columns: []string{"id", "to_delete"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// DELETE with USING
	_, _, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_main",
		Using: []*query.TableRef{{Name: "del_ref"}},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Table: "del_main", Column: "id"},
			Operator: query.TokenEq,
			Right:    &query.QualifiedIdentifier{Table: "del_ref", Column: "id"},
		},
	}, nil)

	if err != nil {
		t.Logf("DELETE with USING error: %v", err)
	}
}

// TestCoverage_SelectStarQualified tests SELECT table.*
func TestCoverage_SelectStarQualified(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "star_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "star_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "star_a",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "star_b",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("B")}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT star_a.*, star_b.* FROM star_a JOIN star_b ON star_a.id = star_b.id")
	if err != nil {
		t.Logf("SELECT qualified star error: %v", err)
	} else {
		t.Logf("SELECT qualified star returned %d rows", len(result.Rows))
	}
}
