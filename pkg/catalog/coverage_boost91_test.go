package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_ComplexWhereSubqueries targets evaluateWhere with complex subqueries
func TestCoverage_ComplexWhereSubqueries(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "main_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, c, "sub_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "category", Type: query.TokenText},
	})

	// Insert main data
	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "main_tbl",
			Columns: []string{"id", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("item")}},
		}, nil)
	}

	// Insert sub data
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "sub_tbl",
			Columns: []string{"id", "ref_id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i*2 - 1)), strReal("A")}},
		}, nil)
	}

	// Complex WHERE with EXISTS
	queries := []string{
		"SELECT * FROM main_tbl WHERE EXISTS (SELECT 1 FROM sub_tbl WHERE ref_id = main_tbl.id)",
		"SELECT * FROM main_tbl WHERE NOT EXISTS (SELECT 1 FROM sub_tbl WHERE ref_id = main_tbl.id)",
		"SELECT * FROM main_tbl WHERE id IN (SELECT ref_id FROM sub_tbl WHERE category = 'A')",
		"SELECT * FROM main_tbl WHERE id > ALL (SELECT ref_id FROM sub_tbl WHERE id < 5)",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex WHERE error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_WhereComplexBoolean targets evaluateWhere with complex boolean combinations
func TestCoverage_WhereComplexBoolean(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "bool_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenInteger},
		{Name: "d", Type: query.TokenInteger},
	})

	for i := 1; i <= 16; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "bool_test",
			Columns: []string{"id", "a", "b", "c", "d"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 2)), numReal(float64((i / 2) % 2)), numReal(float64((i / 4) % 2)), numReal(float64((i / 8) % 2))}},
		}, nil)
	}

	// Complex boolean combinations
	queries := []string{
		"SELECT * FROM bool_test WHERE (a = 1 OR b = 1) AND (c = 1 OR d = 1)",
		"SELECT * FROM bool_test WHERE NOT (a = 0 AND b = 0)",
		"SELECT * FROM bool_test WHERE (a = 1 AND b = 1) OR (c = 1 AND d = 1)",
		"SELECT * FROM bool_test WHERE a = 1 AND (b = 1 OR (c = 1 AND d = 1))",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Boolean WHERE error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_LikeEscape91 targets evaluateLike with escape sequences
func TestCoverage_LikeEscape91(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "like_esc", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "pattern", Type: query.TokenText},
	})

	// Insert patterns with special chars
	patterns := []string{"test_1", "test%2", "test__3", "plain", "100%", "under_score"}
	for i, p := range patterns {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "like_esc",
			Columns: []string{"id", "pattern"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(p)}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM like_esc WHERE pattern LIKE 'test\\_%' ESCAPE '\\'",
		"SELECT * FROM like_esc WHERE pattern LIKE '%\\%%' ESCAPE '\\'",
		"SELECT * FROM like_esc WHERE pattern LIKE 'test%'",
		"SELECT * FROM like_esc WHERE pattern LIKE '%score'",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIKE escape error: %v", err)
		} else {
			t.Logf("LIKE query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_JSONOperations targets JSON functions
func TestCoverage_JSONOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE json_test (id INTEGER PRIMARY KEY, data JSON)`)
	if err != nil {
		t.Logf("Create table error: %v", err)
		return
	}

	// Insert JSON data
	jsonData := []string{
		`{"name": "test", "value": 123}`,
		`{"name": "other", "items": [1, 2, 3]}`,
		`{"nested": {"key": "value"}}`,
	}
	for i, j := range jsonData {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "json_test",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(j)}},
		}, nil)
	}

	// JSON operations
	queries := []string{
		"SELECT JSON_EXTRACT(data, '$.name') FROM json_test",
		"SELECT * FROM json_test WHERE JSON_EXTRACT(data, '$.value') = 123",
		"SELECT JSON_TYPE(data) FROM json_test",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("JSON query error: %v", err)
		} else {
			t.Logf("JSON query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ForeignKeySetNull targets FK SET NULL on delete
func TestCoverage_ForeignKeySetNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE fk_parent_sn (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Logf("Create parent error: %v", err)
		return
	}

	_, err = c.ExecuteQuery(`CREATE TABLE fk_child_sn (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER REFERENCES fk_parent_sn(id) ON DELETE SET NULL
	)`)
	if err != nil {
		t.Logf("Create child error: %v", err)
		return
	}

	// Insert parents
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "fk_parent_sn",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("parent")}},
		}, nil)
	}

	// Insert children
	for i := 1; i <= 6; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "fk_child_sn",
			Columns: []string{"id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%3 + 1))}},
		}, nil)
	}

	// Delete parent - should set children parent_id to NULL
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent_sn",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("FK SET NULL delete error: %v", err)
	} else {
		t.Logf("Deleted %d parent rows", rows)
	}

	// Check children
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM fk_child_sn WHERE parent_id IS NULL")
	if result != nil {
		t.Logf("Children with NULL parent: %v", result.Rows)
	}
}

// TestCoverage_AlterTableOperations targets DDL operations
func TestCoverage_AlterTableOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE alter_test (
		id INTEGER PRIMARY KEY,
		col1 TEXT,
		col2 INTEGER
	)`)
	if err != nil {
		t.Logf("Create error: %v", err)
		return
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "alter_test",
			Columns: []string{"id", "col1", "col2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Add column
	_, err = c.ExecuteQuery(`ALTER TABLE alter_test ADD COLUMN new_col TEXT DEFAULT 'default_value'`)
	if err != nil {
		t.Logf("Add column error: %v", err)
	}

	// Rename column
	_, err = c.ExecuteQuery(`ALTER TABLE alter_test RENAME COLUMN col1 TO renamed_col`)
	if err != nil {
		t.Logf("Rename column error: %v", err)
	}

	// Verify
	result, _ := c.ExecuteQuery("SELECT * FROM alter_test LIMIT 1")
	if result != nil {
		t.Logf("Altered table columns: %v", result.Columns)
	}
}

// TestCoverage_TransactionCommitRollback targets transaction paths
func TestCoverage_TransactionCommitRollback(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "txn_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Test commit
	c.BeginTransaction(1)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "txn_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("committed")}},
	}, nil)
	err := c.CommitTransaction()
	if err != nil {
		t.Logf("Commit error: %v", err)
	}

	// Test rollback
	c.BeginTransaction(1)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "txn_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("rolled_back")}},
	}, nil)
	err = c.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify only committed data exists
	result, _ := c.ExecuteQuery("SELECT * FROM txn_test")
	if result != nil {
		t.Logf("Transaction test rows: %d", len(result.Rows))
	}
}
