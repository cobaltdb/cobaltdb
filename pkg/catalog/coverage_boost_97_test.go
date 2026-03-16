package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_selectLockedComplexWhere tests selectLocked with complex WHERE conditions
func TestCoverage_selectLockedComplexWhere(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "complex_where",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "a", Type: query.TokenInteger},
			{Name: "b", Type: query.TokenInteger},
			{Name: "c", Type: query.TokenText},
		},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "complex_where",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i%5)), numReal(float64(i%7)), strReal("val")}},
		}, nil)
	}

	// Complex WHERE with AND/OR
	result, err := c.ExecuteQuery("SELECT * FROM complex_where WHERE (a = 1 OR a = 2) AND b > 3")
	if err != nil {
		t.Logf("Complex WHERE error: %v", err)
	} else {
		t.Logf("Complex WHERE returned %d rows", len(result.Rows))
	}

	// WHERE with NOT
	result, err = c.ExecuteQuery("SELECT * FROM complex_where WHERE NOT (a = 0)")
	if err != nil {
		t.Logf("NOT WHERE error: %v", err)
	} else {
		t.Logf("NOT WHERE returned %d rows", len(result.Rows))
	}

	// WHERE with IN
	result, err = c.ExecuteQuery("SELECT * FROM complex_where WHERE a IN (1, 2, 3)")
	if err != nil {
		t.Logf("IN WHERE error: %v", err)
	} else {
		t.Logf("IN WHERE returned %d rows", len(result.Rows))
	}

	// WHERE with BETWEEN
	result, err = c.ExecuteQuery("SELECT * FROM complex_where WHERE id BETWEEN 10 AND 20")
	if err != nil {
		t.Logf("BETWEEN WHERE error: %v", err)
	} else {
		t.Logf("BETWEEN WHERE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedSubqueries tests selectLocked with various subqueries
func TestCoverage_selectLockedSubqueries(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "subq_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "subq_ref",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "threshold", Type: query.TokenInteger},
		},
	})

	// Insert data
	for i := 1; i <= 30; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "subq_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	c.Insert(ctx, &query.InsertStmt{
		Table:   "subq_ref",
		Columns: []string{"id", "threshold"},
		Values:  [][]query.Expression{{numReal(1), numReal(150)}},
	}, nil)

	// Subquery in WHERE with comparison
	result, err := c.ExecuteQuery("SELECT * FROM subq_main WHERE val > (SELECT threshold FROM subq_ref WHERE id = 1)")
	if err != nil {
		t.Logf("Subquery comparison error: %v", err)
	} else {
		t.Logf("Subquery comparison returned %d rows", len(result.Rows))
	}

	// EXISTS subquery
	result, err = c.ExecuteQuery("SELECT * FROM subq_main WHERE EXISTS (SELECT 1 FROM subq_ref WHERE threshold < 200)")
	if err != nil {
		t.Logf("EXISTS subquery error: %v", err)
	} else {
		t.Logf("EXISTS subquery returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedJoinTypes tests selectLocked with different JOIN types
func TestCoverage_selectLockedJoinTypes(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "join_left",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "join_right",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "left_id", Type: query.TokenInteger},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "join_left",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("name" + string(rune('0'+i)))}},
		}, nil)
	}

	for i := 1; i <= 10; i++ {
		leftID := i % 6 // Some will be 0 (no match)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "join_right",
			Columns: []string{"id", "left_id", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(leftID)), numReal(float64(i * 10))}},
		}, nil)
	}

	// INNER JOIN
	result, err := c.ExecuteQuery("SELECT l.name, r.value FROM join_left l JOIN join_right r ON l.id = r.left_id")
	if err != nil {
		t.Logf("INNER JOIN error: %v", err)
	} else {
		t.Logf("INNER JOIN returned %d rows", len(result.Rows))
	}

	// LEFT JOIN
	result, err = c.ExecuteQuery("SELECT l.name, r.value FROM join_left l LEFT JOIN join_right r ON l.id = r.left_id")
	if err != nil {
		t.Logf("LEFT JOIN error: %v", err)
	} else {
		t.Logf("LEFT JOIN returned %d rows", len(result.Rows))
	}

	// CROSS JOIN
	result, err = c.ExecuteQuery("SELECT l.name, r.value FROM join_left l CROSS JOIN join_right r")
	if err != nil {
		t.Logf("CROSS JOIN error: %v", err)
	} else {
		t.Logf("CROSS JOIN returned %d rows", len(result.Rows))
	}
}

// TestCoverage_LoadCorruptedData tests Load handling of corrupted data
func TestCoverage_LoadCorruptedData(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()

	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	c := New(tree, pool, nil)

	// Create a table
	c.CreateTable(&query.CreateTableStmt{
		Table: "load_corrupt",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
	})

	// Save
	err = c.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}

	// Load - should handle any issues gracefully
	c2 := New(tree, pool, nil)
	err = c2.Load()
	if err != nil {
		t.Logf("Load error (may be expected): %v", err)
	}
}

// TestCoverage_LoadWithManyTables tests Load with many tables
func TestCoverage_LoadWithManyTables(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()

	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	c := New(tree, pool, nil)

	// Create many tables
	for i := 1; i <= 20; i++ {
		tableName := "load_many_" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		c.CreateTable(&query.CreateTableStmt{
			Table: tableName,
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "val", Type: query.TokenInteger},
			},
		})

		// Insert some data
		for j := 1; j <= 3; j++ {
			c.Insert(ctx, &query.InsertStmt{
				Table:   tableName,
				Columns: []string{"id", "val"},
				Values:  [][]query.Expression{{numReal(float64(j)), numReal(float64(j * i))}},
			}, nil)
		}
	}

	// Save
	err = c.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}

	// Load
	c2 := New(tree, pool, nil)
	err = c2.Load()
	if err != nil {
		t.Logf("Load error: %v", err)
	}

	// Verify tables exist
	tables := c2.ListTables()
	t.Logf("Loaded %d tables", len(tables))
}

// TestCoverage_deleteWithUsingLockedMultipleConditions tests DELETE USING with multiple conditions
func TestCoverage_deleteWithUsingLockedMultipleConditions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "del_multi_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat_id", Type: query.TokenInteger},
			{Name: "status", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "del_multi_cat",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "active", Type: query.TokenBoolean},
		},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "del_multi_main",
			Columns: []string{"id", "cat_id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%4)+1)), strReal("active")}},
		}, nil)
	}

	for i := 1; i <= 4; i++ {
		active := true
		if i%2 == 0 {
			active = false
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "del_multi_cat",
			Columns: []string{"id", "name", "active"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("cat"), bl(active)}},
		}, nil)
	}

	// Delete with multiple conditions
	_, err := c.ExecuteQuery("DELETE FROM del_multi_main USING del_multi_cat WHERE del_multi_main.cat_id = del_multi_cat.id AND del_multi_cat.active = FALSE AND del_multi_main.status = 'active'")
	if err != nil {
		t.Logf("DELETE USING multiple conditions error: %v", err)
	}

	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM del_multi_main")
	t.Logf("Rows after delete: %v", result.Rows[0][0])
}

// TestCoverage_executeSelectWithJoinAndGroupByAggregates tests JOIN+GROUP BY with various aggregates
func TestCoverage_executeSelectWithJoinAndGroupByAggregates(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "agg_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "agg_cat",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	for i := 1; i <= 40; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "agg_main",
			Columns: []string{"id", "cat_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%4)+1)), numReal(float64(i * 5))}},
		}, nil)
	}

	for i := 1; i <= 4; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "agg_cat",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Category" + string(rune('0'+i)))}},
		}, nil)
	}

	// Various aggregates
	result, err := c.ExecuteQuery("SELECT c.name, COUNT(*), SUM(m.amount), AVG(m.amount), MIN(m.amount), MAX(m.amount) FROM agg_main m JOIN agg_cat c ON m.cat_id = c.id GROUP BY c.name")
	if err != nil {
		t.Logf("Multiple aggregates error: %v", err)
	} else {
		t.Logf("Multiple aggregates returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Row: %v", row)
		}
	}

	// GROUP_CONCAT
	result, err = c.ExecuteQuery("SELECT c.name, GROUP_CONCAT(m.id) as ids FROM agg_main m JOIN agg_cat c ON m.cat_id = c.id GROUP BY c.name")
	if err != nil {
		t.Logf("GROUP_CONCAT error: %v", err)
	} else {
		t.Logf("GROUP_CONCAT returned %d rows", len(result.Rows))
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByNested tests nested JOIN + GROUP BY
func TestCoverage_executeSelectWithJoinAndGroupByNested(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create 4 tables for complex join
	tables := []string{"nest_a", "nest_b", "nest_c", "nest_d"}
	for _, tbl := range tables {
		c.CreateTable(&query.CreateTableStmt{
			Table: tbl,
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "parent_id", Type: query.TokenInteger},
				{Name: "val", Type: query.TokenInteger},
			},
		})
	}

	// Insert data
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "nest_a",
			Columns: []string{"id", "parent_id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(0), numReal(float64(i * 10))}},
		}, nil)
	}

	for i := 1; i <= 6; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "nest_b",
			Columns: []string{"id", "parent_id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)/2 + 1)), numReal(float64(i * 5))}},
		}, nil)
	}

	for i := 1; i <= 12; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "nest_c",
			Columns: []string{"id", "parent_id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)/2 + 1)), numReal(float64(i))}},
		}, nil)
	}

	for i := 1; i <= 24; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "nest_d",
			Columns: []string{"id", "parent_id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)/2 + 1)), numReal(float64(i * 2))}},
		}, nil)
	}

	// Complex nested join with GROUP BY
	result, err := c.ExecuteQuery("SELECT a.id, COUNT(DISTINCT d.id) as d_count, SUM(d.val) as total FROM nest_a a JOIN nest_b b ON a.id = b.parent_id JOIN nest_c c ON b.id = c.parent_id JOIN nest_d d ON c.id = d.parent_id GROUP BY a.id")
	if err != nil {
		t.Logf("Nested JOIN GROUP BY error: %v", err)
	} else {
		t.Logf("Nested JOIN GROUP BY returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Row: %v", row)
		}
	}
}
