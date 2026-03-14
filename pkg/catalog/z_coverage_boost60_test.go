package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_JSONFunctionsMore targets JSON utility functions
func TestCoverage_JSONAdvanced(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_funcs", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Insert JSON data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_funcs",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"name": "test", "value": 123, "nested": {"a": 1, "b": 2}}`)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_funcs",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(2), strReal(`[1, 2, 3, 4, 5]`)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_funcs",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(3), strReal(`{"arr": ["a", "b", "c"]}`)}},
	}, nil)

	queries := []string{
		"SELECT id, JSON_EXTRACT(data, '$.name') FROM json_funcs WHERE id = 1",
		"SELECT id, JSON_EXTRACT(data, '$.nested.a') FROM json_funcs WHERE id = 1",
		"SELECT id, JSON_ARRAY_LENGTH(data) FROM json_funcs WHERE id = 2",
		"SELECT id, JSON_TYPE(data) FROM json_funcs",
		"SELECT id, JSON_QUOTE(data) FROM json_funcs WHERE id = 1",
		"SELECT id, JSON_KEYS(data) FROM json_funcs WHERE id = 1",
		"SELECT id, JSON_EXTRACT(data, '$.arr[0]') FROM json_funcs WHERE id = 3",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JSON query error: %v", err)
		} else {
			t.Logf("JSON query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_JSONSetRemove targets JSONSet and JSONRemove
func TestCoverage_JSONSetRemove(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_modify", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_modify",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"a": 1, "b": 2}`)}},
	}, nil)

	// Test JSON_SET in UPDATE
	cat.Update(ctx, &query.UpdateStmt{
		Table: "json_modify",
		Set:   []*query.SetClause{{Column: "data", Value: &query.FunctionCall{Name: "JSON_SET", Args: []query.Expression{&query.Identifier{Name: "data"}, strReal("$.c"), numReal(3)}}}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT data FROM json_modify WHERE id = 1")
	t.Logf("After JSON_SET: %v", result.Rows)

	// Test JSON_REMOVE
	cat.Update(ctx, &query.UpdateStmt{
		Table: "json_modify",
		Set:   []*query.SetClause{{Column: "data", Value: &query.FunctionCall{Name: "JSON_REMOVE", Args: []query.Expression{&query.Identifier{Name: "data"}, strReal("$.a")}}}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT data FROM json_modify WHERE id = 1")
	t.Logf("After JSON_REMOVE: %v", result.Rows)
}

// TestCoverage_JSONMerge targets JSONMerge
func TestCoverage_JSONMerge(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_merge", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data1", Type: query.TokenText},
		{Name: "data2", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_merge",
		Columns: []string{"id", "data1", "data2"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"a": 1}`), strReal(`{"b": 2}`)}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT JSON_MERGE(data1, data2) FROM json_merge WHERE id = 1")
	if err != nil {
		t.Logf("JSON_MERGE error: %v", err)
	} else {
		t.Logf("JSON_MERGE result: %v", result.Rows)
	}
}

// TestCoverage_JSONEach targets JSONEach
func TestCoverage_JSONEach(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_each", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_each",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"a": 1, "b": 2, "c": 3}`)}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT JSON_EACH(data) FROM json_each WHERE id = 1")
	if err != nil {
		t.Logf("JSON_EACH error: %v", err)
	} else {
		t.Logf("JSON_EACH result: %v", result.Rows)
	}
}

// TestCoverage_ExecuteScalarSelectMore targets executeScalarSelect
func TestCoverage_ScalarAdvanced(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "scalar_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		grp := "A"
		if i > 15 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "scalar_more",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT COUNT(*) FROM scalar_more",
		"SELECT COUNT(*) FROM scalar_more WHERE grp = 'A'",
		"SELECT SUM(val) FROM scalar_more WHERE grp = 'B'",
		"SELECT AVG(val) FROM scalar_more",
		"SELECT MIN(val), MAX(val) FROM scalar_more",
		"SELECT COUNT(DISTINCT grp) FROM scalar_more",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Scalar error: %v", err)
		} else {
			t.Logf("Scalar result: %v", result.Rows)
		}
	}
}

// TestCoverage_ForeignKeyOnDeleteSetNull targets FK ON DELETE SET NULL
func TestCoverage_ForeignKeyOnDeleteSetNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_setnull_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_setnull_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	// Create child with ON DELETE SET NULL
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_setnull_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"parent_id"}, ReferencedTable: "fk_setnull_parent", ReferencedColumns: []string{"id"}, OnDelete: "SET NULL"},
		},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_setnull_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}, {numReal(3), numReal(2)}},
	}, nil)

	// Delete parent - should set child.parent_id to NULL
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_setnull_parent",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM fk_setnull_child WHERE parent_id IS NULL")
	t.Logf("Children with NULL parent after SET NULL: %v", result.Rows)
}

// TestCoverage_ForeignKeyOnUpdateCascade targets FK ON UPDATE CASCADE
func TestCoverage_ForeignKeyOnUpdateCascade(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_upcascade_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_upcascade_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	// Create child with ON UPDATE CASCADE
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_upcascade_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"parent_id"}, ReferencedTable: "fk_upcascade_parent", ReferencedColumns: []string{"id"}, OnUpdate: "CASCADE"},
		},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_upcascade_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}},
	}, nil)

	// Update parent id - should cascade to children
	cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_upcascade_parent",
		Set:   []*query.SetClause{{Column: "id", Value: numReal(100)}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM fk_upcascade_child WHERE parent_id = 100")
	t.Logf("Children with updated parent_id: %v", result.Rows)
}

// TestCoverage_DeleteWithUsing targets DELETE with USING clause
func TestCoverage_DeleteWithUsing(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_target", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "del_using", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_target",
			Columns: []string{"id", "ref"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
		code := "keep"
		if i > 5 {
			code = "delete"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_using",
			Columns: []string{"id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(code)}},
		}, nil)
	}

	// DELETE with USING
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_target",
		Using: []*query.TableRef{{Name: "del_using"}},
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: &query.Identifier{Name: "del_target.ref"}, Operator: query.TokenEq, Right: &query.Identifier{Name: "del_using.id"}},
			Operator: query.TokenAnd,
			Right:    &query.BinaryExpr{Left: &query.Identifier{Name: "del_using.code"}, Operator: query.TokenEq, Right: strReal("delete")},
		},
	}, nil)

	if err != nil {
		t.Logf("DELETE USING error: %v", err)
	} else {
		t.Logf("DELETE USING affected %d rows", rows)
	}
}

// TestCoverage_UpdateFrom targets UPDATE with FROM clause
func TestCoverage_UpdateFrom(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_target", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "upd_from", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "new_val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_target",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_from",
			Columns: []string{"id", "new_val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 100))}},
		}, nil)
	}

	// UPDATE with FROM
	_, rows, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_target",
		Set:   []*query.SetClause{{Column: "val", Value: &query.Identifier{Name: "upd_from.new_val"}}},
		From:  &query.TableRef{Name: "upd_from"},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "upd_target.id"}, Operator: query.TokenEq, Right: &query.Identifier{Name: "upd_from.id"}},
	}, nil)

	if err != nil {
		t.Logf("UPDATE FROM error: %v", err)
	} else {
		t.Logf("UPDATE FROM affected %d rows", rows)
	}
}
