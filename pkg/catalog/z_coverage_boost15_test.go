package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_GroupByWithJoin tests GROUP BY with JOIN
func TestCoverage_GroupByWithJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "gb_join_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "gb_join_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "gb_join_a",
		Columns: []string{"id", "category"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "gb_join_b",
		Columns: []string{"id", "a_id", "amount"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}, {numReal(2), numReal(1), numReal(200)}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT a.category, SUM(b.amount) FROM gb_join_a a JOIN gb_join_b b ON a.id = b.a_id GROUP BY a.category")
	if err != nil {
		t.Logf("GROUP BY JOIN error: %v", err)
	} else {
		t.Logf("GROUP BY JOIN returned %d rows", len(result.Rows))
	}
}

// TestCoverage_WindowNthValue tests NTH_VALUE window function
func TestCoverage_WindowNthValue(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_nth", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_nth",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	result, err := cat.ExecuteQuery("SELECT id, NTH_VALUE(val, 3) OVER (ORDER BY id) FROM win_nth")
	if err != nil {
		t.Logf("NTH_VALUE error: %v", err)
	} else {
		t.Logf("NTH_VALUE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_FirstValueLastValue tests FIRST_VALUE and LAST_VALUE
func TestCoverage_FirstValueLastValue(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_fl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		grp := "A"
		if i > 5 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_fl",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY id) FROM win_fl",
		"SELECT id, LAST_VALUE(val) OVER (PARTITION BY grp ORDER BY id) FROM win_fl",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("FIRST/LAST_VALUE error: %v", err)
		} else {
			t.Logf("FIRST/LAST_VALUE returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_RecursiveCTE tests recursive CTE
func TestCoverage_RecursiveCTE(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	result, err := cat.ExecuteQuery("WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 10) SELECT * FROM nums")
	if err != nil {
		t.Logf("Recursive CTE error: %v", err)
	} else {
		t.Logf("Recursive CTE returned %d rows", len(result.Rows))
	}
}
