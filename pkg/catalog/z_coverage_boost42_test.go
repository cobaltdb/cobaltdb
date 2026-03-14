package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_InsertLockedMoreComplex tests insertLocked with more complex scenarios
func TestCoverage_InsertLockedMoreComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_complex2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert in transaction
	cat.BeginTransaction(1)

	for i := 1; i <= 15; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "ins_complex2",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	cat.CommitTransaction()

	// Insert without transaction
	for i := 16; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "ins_complex2",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_complex2")
	t.Logf("Count after inserts: %v", result.Rows)

	// Bulk insert
	var values [][]query.Expression
	for i := 31; i <= 60; i++ {
		values = append(values, []query.Expression{numReal(float64(i)), numReal(float64(i * 10))})
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_complex2",
		Columns: []string{"id", "val"},
		Values:  values,
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM ins_complex2")
	t.Logf("Count after bulk insert: %v", result.Rows)
}

// TestCoverage_UpdateLockedComplexMore tests updateLocked with more complex scenarios
func TestCoverage_UpdateLockedComplexMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_complex2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "status", Type: query.TokenText},
	})

	for i := 1; i <= 25; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_complex2",
			Columns: []string{"id", "val", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("active")}},
		}, nil)
	}

	// Complex AND condition
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_complex2",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    numReal(5),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenLt,
				Right:    numReal(20),
			},
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM upd_complex2 WHERE val = 999")
	t.Logf("Count after complex update: %v", result.Rows)
}

// TestCoverage_DeleteRowLockedComplexMore tests deleteRowLocked with more complex scenarios
func TestCoverage_DeleteRowLockedComplexMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_complex2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "cat", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		catg := "A"
		if i > 15 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_complex2",
			Columns: []string{"id", "val", "cat"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal(catg)}},
		}, nil)
	}

	// Delete with complex where
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_complex2",
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    numReal(5),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenLt,
				Right:    numReal(25),
			},
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_complex2")
	t.Logf("Count after delete: %v", result.Rows)
}

// TestCoverage_WhereComplexMore tests evaluateWhere with more complex conditions
func TestCoverage_WhereComplexMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_complex2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
		{Name: "c", Type: query.TokenInteger},
	})

	for i := 1; i <= 40; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_complex2",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("test"), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_complex2 WHERE a > 10 AND a < 30 AND b = 'test'",
		"SELECT * FROM where_complex2 WHERE (a > 20 AND a < 25) OR (a > 35 AND a < 38)",
		"SELECT * FROM where_complex2 WHERE NOT a > 35",
		"SELECT * FROM where_complex2 WHERE a IN (5, 10, 15, 20, 25, 30, 35)",
		"SELECT * FROM where_complex2 WHERE a BETWEEN 10 AND 20",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("WHERE error: %v", err)
		} else {
			t.Logf("WHERE returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_SelectLockedComplexMore tests selectLocked with more complex scenarios
func TestCoverage_SelectLockedComplexMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_complex3", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	for i := 1; i <= 40; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_complex3",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("value")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM sel_complex3 WHERE a > 100 ORDER BY a DESC LIMIT 15",
		"SELECT * FROM sel_complex3 WHERE a BETWEEN 100 AND 300 ORDER BY a",
		"SELECT DISTINCT b FROM sel_complex3",
		"SELECT COUNT(*), AVG(a), MAX(a), MIN(a) FROM sel_complex3",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Select error: %v", err)
		} else {
			t.Logf("Select returned %d rows", len(result.Rows))
		}
	}
}
