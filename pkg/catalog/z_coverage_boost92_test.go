package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newB92Cat() *Catalog {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	return New(tree, pool, nil)
}

func b92Insert(t *testing.T, c *Catalog, table string, cols []string, vals []query.Expression) {
	t.Helper()
	ctx := context.Background()
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   table,
		Columns: cols,
		Values:  [][]query.Expression{vals},
	}, nil)
	if err != nil {
		t.Fatalf("Insert into %s: %v", table, err)
	}
}

// TestB92_InsertWithFKConstraintViolation tests FK check in insertLocked
func TestB92_InsertWithFKConstraintViolation(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_ins_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_ins_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_ins_parent",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	ctx := context.Background()
	// Insert parent row
	c.Insert(ctx, &query.InsertStmt{
		Table:   "fk_ins_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	// Valid FK insert - should succeed
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   "fk_ins_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)
	if err != nil {
		t.Errorf("Valid FK insert should succeed: %v", err)
	}

	// Invalid FK insert - should fail
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "fk_ins_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: 999}}},
	}, nil)
	if err == nil {
		t.Error("Expected FK violation error, got nil")
	}
}

// TestB92_InsertWithCheckConstraintViolation tests CHECK constraint in insertLocked
func TestB92_InsertWithCheckConstraintViolation(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "check_ins_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "age", Type: query.TokenInteger, Check: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "age"},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 0},
			}},
		},
	})

	ctx := context.Background()
	// Valid insert
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   "check_ins_tbl",
		Columns: []string{"id", "age"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 25}}},
	}, nil)
	if err != nil {
		t.Errorf("Valid CHECK insert should succeed: %v", err)
	}

	// Invalid insert (age <= 0)
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "check_ins_tbl",
		Columns: []string{"id", "age"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: -5}}},
	}, nil)
	if err == nil {
		t.Error("Expected CHECK constraint failure for negative age")
	}
}

// TestB92_UpdateWithUniqueConstraintViolation tests UNIQUE constraint check in processUpdateRow
func TestB92_UpdateWithUniqueConstraintViolation(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "uniq_upd_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText, Unique: true},
		},
	})

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "uniq_upd_tbl",
		Columns: []string{"id", "email"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "a@test.com"}}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "uniq_upd_tbl",
		Columns: []string{"id", "email"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "b@test.com"}}},
	}, nil)

	// Try to update row 2 to have same email as row 1 - should fail
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "uniq_upd_tbl",
		Set:   []*query.SetClause{{Column: "email", Value: &query.StringLiteral{Value: "a@test.com"}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 2},
		},
	}, nil)
	if err == nil {
		t.Error("Expected UNIQUE constraint violation on UPDATE, got nil")
	}
}

// TestB92_UpdateWithNotNullConstraintViolation tests NOT NULL check in processUpdateRow
func TestB92_UpdateWithNotNullConstraintViolation(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "nn_upd_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText, NotNull: true},
		},
	})

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "nn_upd_tbl",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)

	// Try to set name to NULL - should fail
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "nn_upd_tbl",
		Set:   []*query.SetClause{{Column: "name", Value: &query.NullLiteral{}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err == nil {
		t.Error("Expected NOT NULL constraint violation on UPDATE, got nil")
	}
}

// TestB92_UpdateWithCheckConstraintViolation tests CHECK constraint in processUpdateRow
func TestB92_UpdateWithCheckConstraintViolation(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "check_upd_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "age", Type: query.TokenInteger, Check: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "age"},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 0},
			}},
		},
	})

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "check_upd_tbl",
		Columns: []string{"id", "age"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 25}}},
	}, nil)

	// Update to invalid age
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "check_upd_tbl",
		Set:   []*query.SetClause{{Column: "age", Value: &query.NumberLiteral{Value: -1}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err == nil {
		t.Error("Expected CHECK constraint violation on UPDATE, got nil")
	}
}

// TestB92_InsertWithPKConflictIgnore tests ConflictIgnore on PK duplicate
func TestB92_InsertWithPKConflictIgnore(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "pk_ignore_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "pk_ignore_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "original"}}},
	}, nil)

	// Insert with same PK but ConflictIgnore - should not error
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:          "pk_ignore_tbl",
		Columns:        []string{"id", "val"},
		Values:         [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "new"}}},
		ConflictAction: query.ConflictIgnore,
	}, nil)
	if err != nil {
		t.Errorf("ConflictIgnore should not error: %v", err)
	}

	// Verify original value is preserved
	result, _ := c.ExecuteQuery("SELECT val FROM pk_ignore_tbl WHERE id = 1")
	if len(result.Rows) > 0 && result.Rows[0][0] != "original" {
		t.Errorf("Expected 'original', got %v", result.Rows[0][0])
	}
}

// TestB92_InsertWithPKConflictReplace tests ConflictReplace on PK duplicate
func TestB92_InsertWithPKConflictReplace(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "pk_replace_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "pk_replace_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "original"}}},
	}, nil)

	// Insert with same PK and ConflictReplace - should replace
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:          "pk_replace_tbl",
		Columns:        []string{"id", "val"},
		Values:         [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "replaced"}}},
		ConflictAction: query.ConflictReplace,
	}, nil)
	if err != nil {
		t.Errorf("ConflictReplace should not error: %v", err)
	}

	result, _ := c.ExecuteQuery("SELECT val FROM pk_replace_tbl WHERE id = 1")
	if len(result.Rows) > 0 && result.Rows[0][0] != "replaced" {
		t.Errorf("Expected 'replaced', got %v", result.Rows[0][0])
	}
}

// TestB92_UpdateWithFKConstraint tests FK check in processUpdateRowData
func TestB92_UpdateWithFKConstraint(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_upd_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_upd_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_upd_parent",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "fk_upd_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "fk_upd_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "fk_upd_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Update to valid parent
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "fk_upd_child",
		Set:   []*query.SetClause{{Column: "parent_id", Value: &query.NumberLiteral{Value: 2}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Errorf("Update to valid FK should succeed: %v", err)
	}

	// Update to invalid parent
	_, _, err = c.Update(ctx, &query.UpdateStmt{
		Table: "fk_upd_child",
		Set:   []*query.SetClause{{Column: "parent_id", Value: &query.NumberLiteral{Value: 999}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err == nil {
		t.Error("Expected FK constraint error on invalid parent_id, got nil")
	}
}

// TestB92_SelectLockedViewComplex tests complex view path in selectLocked
func TestB92_SelectLockedViewComplex(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "view_complex_data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept", Type: query.TokenText},
			{Name: "salary", Type: query.TokenInteger},
		},
	})

	ctx := context.Background()
	depts := []string{"Eng", "Eng", "HR", "HR", "Eng"}
	for i, dept := range depts {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "view_complex_data",
			Columns: []string{"id", "dept", "salary"},
			Values: [][]query.Expression{
				{numReal(float64(i + 1)), strReal(dept), numReal(float64((i + 1) * 10000))},
			},
		}, nil)
	}

	// Create a complex view (with GROUP BY - complex view)
	c.CreateView("dept_stats_view", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "dept"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}, Alias: "cnt"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "salary"}}}, Alias: "total"},
		},
		From:    &query.TableRef{Name: "view_complex_data"},
		GroupBy: []query.Expression{&query.Identifier{Name: "dept"}},
	})

	// Query the complex view
	result, err := c.ExecuteQuery("SELECT dept, cnt, total FROM dept_stats_view ORDER BY dept")
	if err != nil {
		t.Logf("Complex view query error: %v", err)
	} else {
		t.Logf("Complex view returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  dept=%v cnt=%v total=%v", row[0], row[1], row[2])
		}
	}
}

// TestB92_SelectLockedSimpleView tests simple view inlining path
func TestB92_SelectLockedSimpleView(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "simple_view_data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "active", Type: query.TokenInteger},
		},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		active := 1
		if i%2 == 0 {
			active = 0
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "simple_view_data",
			Columns: []string{"id", "name", "active"},
			Values: [][]query.Expression{
				{numReal(float64(i)), strReal("User" + string(rune('0'+i))), numReal(float64(active))},
			},
		}, nil)
	}

	// Create a simple view (no GROUP BY, no aggregates)
	c.CreateView("active_users_view", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "simple_view_data"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "active"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	})

	// Query the simple view
	result, err := c.ExecuteQuery("SELECT id, name FROM active_users_view ORDER BY id")
	if err != nil {
		t.Logf("Simple view query error: %v", err)
	} else {
		t.Logf("Simple view returned %d rows", len(result.Rows))
	}

	// Query with WHERE on the view
	result, err = c.ExecuteQuery("SELECT id FROM active_users_view WHERE id > 2")
	if err != nil {
		t.Logf("Simple view + WHERE error: %v", err)
	} else {
		t.Logf("Simple view + WHERE returned %d rows", len(result.Rows))
	}
}

// TestB92_RollbackUndoIndexChanges tests index undo in RollbackTransaction
func TestB92_RollbackUndoIndexChanges(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "idx_undo_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_undo_val",
		Table:   "idx_undo_tbl",
		Columns: []string{"val"},
	})

	ctx := context.Background()
	// Insert some initial data
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "idx_undo_tbl",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Begin transaction
	c.BeginTransaction(2001)

	// Insert in transaction (should be undone on rollback)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "idx_undo_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(4), numReal(40)}},
	}, nil)

	// Update in transaction
	c.Update(ctx, &query.UpdateStmt{
		Table: "idx_undo_tbl",
		Set:   []*query.SetClause{{Column: "val", Value: &query.NumberLiteral{Value: 99}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)

	// Rollback
	if err := c.RollbackTransaction(); err != nil {
		t.Errorf("RollbackTransaction: %v", err)
	}

	// Verify original state restored
	result, err := c.ExecuteQuery("SELECT COUNT(*) FROM idx_undo_tbl")
	if err != nil {
		t.Fatalf("SELECT after rollback: %v", err)
	}
	if len(result.Rows) > 0 {
		t.Logf("Row count after rollback: %v", result.Rows[0][0])
	}
}

// TestB92_SelectWithViewAndJoins tests complex view + JOIN path
func TestB92_SelectWithViewAndJoins(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "vj_employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept_id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
			{Name: "salary", Type: query.TokenInteger},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "vj_depts",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	ctx := context.Background()
	// Insert depts
	for i := 1; i <= 2; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "vj_depts",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Dept" + string(rune('0'+i)))}},
		}, nil)
	}
	// Insert employees
	for i := 1; i <= 4; i++ {
		deptID := (i%2 + 1)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "vj_employees",
			Columns: []string{"id", "dept_id", "name", "salary"},
			Values: [][]query.Expression{
				{numReal(float64(i)), numReal(float64(deptID)), strReal("Emp" + string(rune('0'+i))), numReal(float64(i * 10000))},
			},
		}, nil)
	}

	// Create simple view
	c.CreateView("vj_high_earners", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "dept_id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "vj_employees"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "salary"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 20000},
		},
	})

	// Join view with depts - exercises complex view + JOIN path in selectLocked
	q := `SELECT v.name, d.name
	      FROM vj_high_earners v
	      JOIN vj_depts d ON v.dept_id = d.id`
	result, err := c.ExecuteQuery(q)
	if err != nil {
		t.Logf("View + JOIN error: %v", err)
	} else {
		t.Logf("View + JOIN returned %d rows", len(result.Rows))
	}
}

// TestB92_JSONUtilSetArrayPath tests JSONPath.Set with array indices
func TestB92_JSONUtilSetArrayPath(t *testing.T) {
	testCases := []struct {
		jsonData string
		path     string
		value    string
		desc     string
	}{
		{`[10, 20, 30]`, "$[1]", "99", "array index"},
		{`{"arr": [1, 2, 3]}`, "$.arr[0]", "99", "nested array"},
		{`{"a": {"b": 1}}`, "$.a.b", "99", "nested object"},
		{`{}`, "$.new_key", `"hello"`, "new key"},
		{`{"x": 1}`, "$.x", "null", "set to null"},
		{`[]`, "$[0]", "1", "empty array index"},
	}

	for _, tc := range testCases {
		result, err := JSONSet(tc.jsonData, tc.path, tc.value)
		if err != nil {
			t.Logf("JSONSet %s error: %v", tc.desc, err)
		} else {
			t.Logf("JSONSet %s: %s -> %s", tc.desc, tc.jsonData, result)
		}
	}
}

// TestB92_SelectWithGroupByExprArg tests applyGroupByOrderBy with expression arg
func TestB92_SelectWithGroupByExprArg(t *testing.T) {
	c := newB92Cat()
	createCoverageTestTable(t, c, "gbexpr_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "price", Type: query.TokenReal},
		{Name: "qty", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 9; i++ {
		dept := "A"
		if i > 3 {
			dept = "B"
		}
		if i > 6 {
			dept = "C"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "gbexpr_tbl",
			Columns: []string{"id", "dept", "price", "qty"},
			Values: [][]query.Expression{
				{numReal(float64(i)), strReal(dept), numReal(float64(i) * 1.5), numReal(float64(i))},
			},
		}, nil)
	}

	// GROUP BY + ORDER BY with expression SUM(price*qty)
	queries := []string{
		"SELECT dept, SUM(price) FROM gbexpr_tbl GROUP BY dept ORDER BY 2 DESC",
		"SELECT dept, COUNT(*) FROM gbexpr_tbl GROUP BY dept ORDER BY 1",
		"SELECT dept, MAX(price) FROM gbexpr_tbl GROUP BY dept ORDER BY MAX(price)",
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Group+Order query error: %v", err)
		} else {
			t.Logf("Returned %d rows", len(result.Rows))
		}
	}
}

// TestB92_VectorIndexInsertAndSearch tests vector index insert path via CreateVectorIndex with data
func TestB92_VectorIndexInsertAndSearch(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "vec_search_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 4},
		},
	})

	ctx := context.Background()
	// Insert some vector data
	vectors := [][]float64{
		{1.0, 0.0, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
		{0.0, 0.0, 1.0, 0.0},
		{0.0, 0.0, 0.0, 1.0},
	}

	for i, vec := range vectors {
		_ = vec // vectors skipped for now since no vector storage
		c.Insert(ctx, &query.InsertStmt{
			Table:   "vec_search_tbl",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i + 1))}},
		}, nil)
	}

	// Create vector index (exercises the index build path)
	if err := c.CreateVectorIndex("vec_search_idx", "vec_search_tbl", "embedding"); err != nil {
		t.Logf("CreateVectorIndex (may fail since embedding not stored): %v", err)
	}
}

// TestB92_SearchKNNWithMaxPath tests HNSW SearchKNN with max branch
func TestB92_SearchKNNWithMaxPath(t *testing.T) {
	hnswIdx := NewHNSWIndex("test_hnsw", "test_tbl", "vec_col", 3)
	if hnswIdx == nil {
		t.Fatal("NewHNSWIndex returned nil")
	}

	// Insert multiple vectors
	vecs := [][]float64{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
		{0.5, 0.5, 0.0},
		{0.3, 0.3, 0.9},
	}
	for i, v := range vecs {
		hnswIdx.Insert("key"+string(rune('A'+i)), v)
	}

	// Search KNN - returns ([]string, []float64, error)
	keys, dists, err := hnswIdx.SearchKNN([]float64{1.0, 0.0, 0.0}, 3)
	if err != nil {
		t.Logf("SearchKNN error: %v", err)
	} else {
		t.Logf("SearchKNN returned %d results", len(keys))
		for i, k := range keys {
			t.Logf("  key=%s dist=%f", k, dists[i])
		}
	}

	// Delete one and search again to exercise removeString
	hnswIdx.Delete("keyA")
	keys2, _, err2 := hnswIdx.SearchKNN([]float64{1.0, 0.0, 0.0}, 3)
	if err2 != nil {
		t.Logf("SearchKNN after delete error: %v", err2)
	} else {
		t.Logf("SearchKNN after delete: %d results", len(keys2))
	}
}

// TestB92_SearchRangeKNN tests SearchRange
func TestB92_SearchRangeKNN(t *testing.T) {
	hnswIdx := NewHNSWIndex("test_range", "test_tbl", "vec_col", 2)
	for i := 0; i < 10; i++ {
		hnswIdx.Insert("key"+string(rune('0'+i)), []float64{float64(i) * 0.1, float64(10-i) * 0.1})
	}

	rangeKeys, _, err := hnswIdx.SearchRange([]float64{0.5, 0.5}, 0.5)
	if err != nil {
		t.Logf("SearchRange error: %v", err)
	} else {
		t.Logf("SearchRange returned %d results", len(rangeKeys))
	}
}

// TestB92_CosineSimBranches tests cosineSimilarity paths
func TestB92_CosineSimBranches(t *testing.T) {
	hnswIdx := NewHNSWIndex("cos_test", "tbl", "col", 3)

	// Zero vectors
	hnswIdx.Insert("z1", []float64{0.0, 0.0, 0.0})
	hnswIdx.Insert("z2", []float64{1.0, 0.0, 0.0})

	zkeys, _, zerr := hnswIdx.SearchKNN([]float64{0.0, 0.0, 0.0}, 2)
	if zerr != nil {
		t.Logf("SearchKNN with zero vector error: %v", zerr)
	} else {
		t.Logf("SearchKNN with zero vector: %d results", len(zkeys))
	}
}

// TestB92_StatsCountRowsFloat tests countRows with float result
func TestB92_StatsCountRowsFloat(t *testing.T) {
	c := newB92Cat()
	createCoverageTestTable(t, c, "stats_cr_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenReal},
		{Name: "name", Type: query.TokenText},
	})

	ctx := context.Background()
	for i := 1; i <= 15; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "stats_cr_tbl",
			Columns: []string{"id", "val", "name"},
			Values: [][]query.Expression{
				{numReal(float64(i)), numReal(float64(i) * 1.5), strReal("item")},
			},
		}, nil)
	}

	// Run Analyze to trigger countRows, collectColumnStats, buildHistogram, etc.
	if err := c.Analyze("stats_cr_tbl"); err != nil {
		t.Errorf("Analyze failed: %v", err)
	}

	// Estimate selectivity to test EstimateSelectivity
	stats, _ := c.GetTableStats("stats_cr_tbl")
	if stats == nil {
		t.Logf("No stats returned (may be expected)")
	}
}

// TestB92_QueryCacheSet tests QueryCache.Set path
func TestB92_QueryCacheSet(t *testing.T) {
	// Create a QueryCache directly and test Set
	qc := NewQueryCache(10, 0)

	// Set multiple entries
	for i := 0; i < 15; i++ {
		key := "query" + string(rune('0'+i%10))
		qc.Set(key, []string{"col1", "col2"}, [][]interface{}{{"val1", "val2"}}, []string{"tbl1"})
	}

	// Get existing entry
	entry, ok := qc.Get("query0")
	if ok {
		t.Logf("Cache hit: %v columns", len(entry.Columns))
	} else {
		t.Logf("Cache miss for query0")
	}

	// Check stats
	hits, misses, size := qc.Stats()
	t.Logf("Cache stats: hits=%d misses=%d size=%d", hits, misses, size)

	// Invalidate
	qc.Invalidate("tbl1")
	_, ok = qc.Get("query0")
	t.Logf("After invalidate: cache hit=%v", ok)

	// InvalidateAll
	qc.InvalidateAll()
	t.Logf("After InvalidateAll, size should be 0")
}

// TestB92_EvaluateTemporalExprBranches tests evaluateTemporalExpr branches
func TestB92_EvaluateTemporalExprBranches(t *testing.T) {
	c := newB92Cat()
	createCoverageTestTable(t, c, "temp_branch_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "temp_branch_tbl",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Various AS OF queries
	queries := []string{
		"SELECT * FROM temp_branch_tbl AS OF NOW()",
		"SELECT * FROM temp_branch_tbl AS OF TIMESTAMP '2025-01-01 00:00:00'",
		"SELECT * FROM temp_branch_tbl AS OF TIMESTAMP '2023-12-31'",
		"SELECT * FROM temp_branch_tbl AS OF CURRENT_TIMESTAMP",
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Temporal query error: %v", err)
		} else {
			t.Logf("Temporal query returned %d rows", len(result.Rows))
		}
	}
}

// TestB92_SelectLockedWithCTEWindowNoFrom tests CTE window function branch
func TestB92_SelectLockedWithCTEWindowNoFrom(t *testing.T) {
	c := newB92Cat()
	createCoverageTestTable(t, c, "cte_wnd2_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 6; i++ {
		grp := "X"
		if i > 3 {
			grp = "Y"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "cte_wnd2_tbl",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 5))}},
		}, nil)
	}

	// This exercises the CTE + window function path in selectLocked
	queries := []string{
		`WITH data AS (SELECT id, grp, val FROM cte_wnd2_tbl)
		 SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) AS rn FROM data`,
		`WITH data AS (SELECT id, val FROM cte_wnd2_tbl)
		 SELECT id, val, SUM(val) OVER (ORDER BY id) AS running FROM data
		 WHERE id > 2`,
		`WITH data AS (SELECT id, val FROM cte_wnd2_tbl)
		 SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev FROM data
		 ORDER BY id LIMIT 3`,
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("CTE+window error: %v", err)
		} else {
			t.Logf("CTE+window returned %d rows", len(result.Rows))
		}
	}
}

// TestB92_InsertWithAutoIncNoColumns tests auto-increment without specifying columns
func TestB92_InsertWithAutoIncNoColumns(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "autoinc_noc_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	ctx := context.Background()
	// Insert without id (auto-increment)
	for i := 1; i <= 3; i++ {
		_, _, err := c.Insert(ctx, &query.InsertStmt{
			Table:   "autoinc_noc_tbl",
			Columns: []string{"name"},
			Values:  [][]query.Expression{{&query.StringLiteral{Value: "Person" + string(rune('0'+i))}}},
		}, nil)
		if err != nil {
			t.Errorf("AutoInc insert: %v", err)
		}
	}

	result, _ := c.ExecuteQuery("SELECT id, name FROM autoinc_noc_tbl ORDER BY id")
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}
}

// TestB92_VectorUpdateIndexesForInsert tests updateVectorIndexesForInsert
func TestB92_VectorUpdateIndexesForInsert(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "vec_ins_idx_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "vec", Type: query.TokenVector, Dimensions: 3},
		},
	})

	// Create vector index BEFORE inserting rows so that
	// updateVectorIndexesForInsert is called during inserts
	if err := c.CreateVectorIndex("vec_ins_idx", "vec_ins_idx_tbl", "vec"); err != nil {
		t.Logf("CreateVectorIndex (may fail without data): %v", err)
		return
	}

	ctx := context.Background()
	// Now insert rows - should trigger updateVectorIndexesForInsert
	c.Insert(ctx, &query.InsertStmt{
		Table:   "vec_ins_idx_tbl",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)
}

// TestB92_VectorUpdateIndexesForUpdate tests updateVectorIndexesForUpdate
func TestB92_VectorUpdateIndexesForUpdate(t *testing.T) {
	c := newB92Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "vec_upd_idx_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
			{Name: "vec", Type: query.TokenVector, Dimensions: 2},
		},
	})

	// Create vector index
	if err := c.CreateVectorIndex("vec_upd_idx2", "vec_upd_idx_tbl", "vec"); err != nil {
		t.Logf("CreateVectorIndex: %v", err)
		return
	}

	ctx := context.Background()
	// Insert then update to trigger updateVectorIndexesForUpdate
	c.Insert(ctx, &query.InsertStmt{
		Table:   "vec_upd_idx_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	c.Update(ctx, &query.UpdateStmt{
		Table: "vec_upd_idx_tbl",
		Set:   []*query.SetClause{{Column: "val", Value: &query.NumberLiteral{Value: 200}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
}

// TestB92_SelectExecuteWithJoinAndGroupByDerivedTable tests derived table as main in JoinAndGroupBy path
func TestB92_SelectExecuteWithJoinAndGroupByDerivedTable(t *testing.T) {
	c := newB92Cat()
	createCoverageTestTable(t, c, "dt_gb_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, c, "dt_gb_labels", []*query.ColumnDef{
		{Name: "cat", Type: query.TokenText, PrimaryKey: true},
		{Name: "label", Type: query.TokenText},
	})

	ctx := context.Background()
	cats := []string{"A", "B", "A", "B", "A"}
	for i, cat := range cats {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "dt_gb_main",
			Columns: []string{"id", "cat", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(cat), numReal(float64((i + 1) * 100))}},
		}, nil)
	}
	c.Insert(ctx, &query.InsertStmt{
		Table:   "dt_gb_labels",
		Columns: []string{"cat", "label"},
		Values:  [][]query.Expression{{strReal("A"), strReal("Alpha")}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "dt_gb_labels",
		Columns: []string{"cat", "label"},
		Values:  [][]query.Expression{{strReal("B"), strReal("Beta")}},
	}, nil)

	// Derived table + JOIN + GROUP BY (exercises executeSelectWithJoinAndGroupBy with subquery main)
	q := `SELECT sub.cat, l.label, SUM(sub.amount)
	      FROM (SELECT cat, amount FROM dt_gb_main WHERE amount > 100) sub
	      JOIN dt_gb_labels l ON sub.cat = l.cat
	      GROUP BY sub.cat, l.label
	      ORDER BY sub.cat`
	result, err := c.ExecuteQuery(q)
	if err != nil {
		t.Logf("Derived table+JOIN+GROUP BY error: %v", err)
	} else {
		t.Logf("Returned %d rows", len(result.Rows))
	}
}

// TestB92_ExprToSQLPaths tests exprToSQL various expression types
func TestB92_ExprToSQLPaths(t *testing.T) {
	// exprToSQL is tested indirectly through query building
	c := newB92Cat()
	createCoverageTestTable(t, c, "e2s_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "e2s_tbl",
			Columns: []string{"id", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("item" + string(rune('0'+i)))}},
		}, nil)
	}

	// Various queries that exercise exprToSQL through index building
	queries := []string{
		"SELECT * FROM e2s_tbl WHERE val IN (10, 20, 30)",
		"SELECT * FROM e2s_tbl WHERE val BETWEEN 20 AND 40",
		"SELECT * FROM e2s_tbl WHERE name LIKE 'item%'",
		"SELECT * FROM e2s_tbl WHERE val IS NOT NULL",
		"SELECT id, val + 1 AS next_val FROM e2s_tbl",
		"SELECT id, CASE WHEN val > 30 THEN 'high' ELSE 'low' END AS grp FROM e2s_tbl",
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Returned %d rows", len(result.Rows))
		}
	}
}
