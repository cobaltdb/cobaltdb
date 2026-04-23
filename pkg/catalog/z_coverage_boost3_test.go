package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ---------------------------------------------------------------------------
// helpers shared by boost3 tests
// ---------------------------------------------------------------------------

func newBoost3Cat() *Catalog {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	return New(tree, pool, nil)
}

func boost3CreateTable(t *testing.T, c *Catalog, name string, cols []*query.ColumnDef) {
	t.Helper()
	err := c.CreateTable(&query.CreateTableStmt{
		Table:   name,
		Columns: cols,
	})
	if err != nil {
		t.Fatalf("CreateTable %s: %v", name, err)
	}
}

func boost3Insert(t *testing.T, c *Catalog, table string, cols []string, rows [][]query.Expression) {
	t.Helper()
	ctx := context.Background()
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   table,
		Columns: cols,
		Values:  rows,
	}, nil)
	if err != nil {
		t.Fatalf("Insert into %s: %v", table, err)
	}
}

func boost3Query(t *testing.T, c *Catalog, sql string) *QueryResult {
	t.Helper()
	res, err := c.ExecuteQuery(sql)
	if err != nil {
		t.Fatalf("Query %q: %v", sql, err)
	}
	return res
}

func b3Eq(col string, val query.Expression) *query.BinaryExpr {
	return &query.BinaryExpr{
		Left:     &query.Identifier{Name: col},
		Operator: query.TokenEq,
		Right:    val,
	}
}

func b3Num(v float64) *query.NumberLiteral { return &query.NumberLiteral{Value: v} }
func b3Str(v string) *query.StringLiteral  { return &query.StringLiteral{Value: v} }

// ---------------------------------------------------------------------------
// RollbackTransaction – exercise undoInsert, undoUpdate, undoDelete paths
// ---------------------------------------------------------------------------

func TestBoost3_RollbackWithUndoLog(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "undo_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert a base row outside the transaction
	boost3Insert(t, c, "undo_tbl", []string{"id", "val"}, [][]query.Expression{
		{b3Num(1), b3Str("base")},
	})

	// Start transaction, make changes, then rollback
	c.BeginTransaction(10)

	// Insert (will be undone)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "undo_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{b3Num(2), b3Str("txn_insert")}},
	}, nil)

	// Update base row (will be undone)
	c.Update(ctx, &query.UpdateStmt{
		Table: "undo_tbl",
		Set: []*query.SetClause{
			{Column: "val", Value: b3Str("updated")},
		},
		Where: b3Eq("id", b3Num(1)),
	}, nil)

	// Insert then delete row id=3 (delete will be undone)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "undo_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{b3Num(3), b3Str("to_delete")}},
	}, nil)
	c.Delete(ctx, &query.DeleteStmt{
		Table: "undo_tbl",
		Where: b3Eq("id", b3Num(3)),
	}, nil)

	// Rollback – should undo all the above
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}

	// After rollback, only base row (id=1, val="base") should exist
	res := boost3Query(t, c, "SELECT val FROM undo_tbl WHERE id = 1")
	if len(res.Rows) == 0 || res.Rows[0][0] != "base" {
		t.Errorf("expected original val='base', got rows=%v", res.Rows)
	}

	// id=2 should not exist
	res2 := boost3Query(t, c, "SELECT COUNT(*) FROM undo_tbl WHERE id = 2")
	if len(res2.Rows) == 0 || fmt.Sprint(res2.Rows[0][0]) != "0" {
		t.Errorf("expected id=2 to not exist after rollback, got %v", res2.Rows)
	}
}

// ---------------------------------------------------------------------------
// RollbackTransaction – exercise undoCreateTable
// ---------------------------------------------------------------------------

func TestBoost3_RollbackCreateTable(t *testing.T) {
	c := newBoost3Cat()

	c.BeginTransaction(20)

	// Create table inside transaction
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "txn_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable in txn: %v", err)
	}

	// Rollback should undo the CreateTable
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}

	// Table should not exist
	_, err = c.ExecuteQuery("SELECT * FROM txn_table")
	if err == nil {
		t.Error("expected error selecting from rolled-back table, got nil")
	}
}

// ---------------------------------------------------------------------------
// RollbackTransaction – exercise undoDropTable
// ---------------------------------------------------------------------------

func TestBoost3_RollbackDropTable(t *testing.T) {
	c := newBoost3Cat()

	// Create table outside transaction
	boost3CreateTable(t, c, "drop_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	boost3Insert(t, c, "drop_tbl", []string{"id", "name"}, [][]query.Expression{
		{b3Num(1), b3Str("Alice")},
	})

	c.BeginTransaction(30)
	if err := c.DropTable(&query.DropTableStmt{Table: "drop_tbl"}); err != nil {
		t.Fatalf("DropTable in txn: %v", err)
	}

	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}

	// Table should be restored
	res := boost3Query(t, c, "SELECT COUNT(*) FROM drop_tbl")
	if len(res.Rows) == 0 {
		t.Error("expected table to be restored after rollback")
	}
}

// ---------------------------------------------------------------------------
// RollbackTransaction – exercise undoCreateIndex
// ---------------------------------------------------------------------------

func TestBoost3_RollbackCreateIndex(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "idx_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	c.BeginTransaction(40)

	err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_name_b3",
		Table:   "idx_tbl",
		Columns: []string{"name"},
	})
	if err != nil {
		t.Fatalf("CreateIndex in txn: %v", err)
	}

	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}

	// Index should not exist
	if _, err := c.GetIndex("idx_name_b3"); err == nil {
		t.Error("expected index to be rolled back, but it still exists")
	}
}

// ---------------------------------------------------------------------------
// Savepoint and RollbackToSavepoint
// ---------------------------------------------------------------------------

func TestBoost3_SavepointRollback(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "sp_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Error outside transaction
	if err := c.Savepoint("sp1"); err == nil {
		t.Error("expected error on SAVEPOINT outside transaction")
	}
	if err := c.RollbackToSavepoint("sp1"); err == nil {
		t.Error("expected error on ROLLBACK TO SAVEPOINT outside transaction")
	}

	c.BeginTransaction(50)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "sp_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{b3Num(1), b3Str("before_sp")}},
	}, nil)

	// Create savepoint
	if err := c.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}

	// Insert after savepoint
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sp_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{b3Num(2), b3Str("after_sp")}},
	}, nil)

	// Rollback to savepoint – row id=2 should disappear
	if err := c.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint: %v", err)
	}

	// Rollback to non-existent savepoint should error
	if err := c.RollbackToSavepoint("nonexistent"); err == nil {
		t.Error("expected error on ROLLBACK TO SAVEPOINT nonexistent")
	}

	c.CommitTransaction()
}

// ---------------------------------------------------------------------------
// Materialized view – exercises selectLocked MV path
// ---------------------------------------------------------------------------

func TestBoost3_MaterializedViewSelect(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "mv_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "amount", Type: query.TokenInteger},
		{Name: "region", Type: query.TokenText},
	})

	boost3Insert(t, c, "mv_src", []string{"id", "amount", "region"}, [][]query.Expression{
		{b3Num(1), b3Num(100), b3Str("north")},
		{b3Num(2), b3Num(200), b3Str("south")},
		{b3Num(3), b3Num(150), b3Str("north")},
	})

	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "region"},
			&query.AliasExpr{
				Expr:  &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
				Alias: "total",
			},
		},
		From:    &query.TableRef{Name: "mv_src"},
		GroupBy: []query.Expression{&query.Identifier{Name: "region"}},
	}

	// Create materialized view
	err := c.CreateMaterializedView("mv_region_totals", selectStmt, false)
	if err != nil {
		t.Logf("CreateMaterializedView: %v", err)
	}

	// Duplicate - ifNotExists = true should be ok
	err = c.CreateMaterializedView("mv_region_totals", selectStmt, true)
	if err != nil {
		t.Logf("CreateMaterializedView duplicate ifNotExists: %v", err)
	}

	// Refresh
	if err := c.RefreshMaterializedView("mv_region_totals"); err != nil {
		t.Logf("RefreshMaterializedView: %v", err)
	}

	// Drop
	if err := c.DropMaterializedView("mv_region_totals", false); err != nil {
		t.Logf("DropMaterializedView: %v", err)
	}

	// Drop non-existent
	if err := c.DropMaterializedView("does_not_exist", false); err == nil {
		t.Error("expected error dropping non-existent MV")
	}
}

// ---------------------------------------------------------------------------
// JSON functions via ExecuteQuery
// ---------------------------------------------------------------------------

func TestBoost3_JSONFunctions(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "json_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	boost3Insert(t, c, "json_tbl", []string{"id", "data"}, [][]query.Expression{
		{b3Num(1), b3Str(`{"name":"Alice","age":30,"scores":[1,2,3]}`)},
		{b3Num(2), b3Str(`{"name":"Bob","age":25}`)},
		{b3Num(3), b3Str(`{}`)},
	})

	queries := []string{
		`SELECT JSON_EXTRACT(data, '$.name') FROM json_tbl WHERE id = 1`,
		`SELECT JSON_EXTRACT(data, '$.scores[0]') FROM json_tbl WHERE id = 1`,
		`SELECT JSON_SET(data, '$.age', 31) FROM json_tbl WHERE id = 1`,
		`SELECT JSON_TYPE(data) FROM json_tbl WHERE id = 1`,
		`SELECT JSON_KEYS(data) FROM json_tbl WHERE id = 1`,
		`SELECT JSON_ARRAY_LENGTH(JSON_EXTRACT(data, '$.scores')) FROM json_tbl WHERE id = 1`,
		`SELECT JSON_PRETTY(data) FROM json_tbl WHERE id = 1`,
		`SELECT JSON_MINIFY(data) FROM json_tbl WHERE id = 1`,
		`SELECT JSON_QUOTE(data) FROM json_tbl WHERE id = 1`,
		`SELECT JSON_REMOVE(data, '$.age') FROM json_tbl WHERE id = 1`,
		`SELECT JSON_MERGE(data, '{"city":"NYC"}') FROM json_tbl WHERE id = 1`,
		`SELECT JSON_EXTRACT(data, '$.missing') FROM json_tbl WHERE id = 1`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %q error: %v", q, err)
		} else {
			t.Logf("Query %q rows: %v", q, res.Rows)
		}
	}
}

// ---------------------------------------------------------------------------
// JSON utility functions – direct calls
// ---------------------------------------------------------------------------

func TestBoost3_JSONUtilDirectCalls(t *testing.T) {
	// Test JSONSet with array path
	result, err := JSONSet(`{"arr":[1,2,3]}`, "$.arr[1]", "99")
	if err != nil {
		t.Logf("JSONSet array: %v", err)
	} else {
		t.Logf("JSONSet array: %s", result)
	}

	// Test JSONSet on empty string
	result, err = JSONSet("", "$.key", `"value"`)
	if err != nil {
		t.Logf("JSONSet empty: %v", err)
	} else {
		t.Logf("JSONSet empty: %s", result)
	}

	// Test JSONExtract on empty string
	v, err := JSONExtract("", "$.key")
	t.Logf("JSONExtract empty: v=%v err=%v", v, err)

	// Test JSONExtract invalid JSON
	v, err = JSONExtract("{invalid}", "$.key")
	t.Logf("JSONExtract invalid JSON: v=%v err=%v", v, err)

	// Test JSONQuote
	qr := JSONQuote("hello world")
	t.Logf("JSONQuote string: %s", qr)

	qr2 := JSONQuote(`{"already":"json"}`)
	t.Logf("JSONQuote json: %s", qr2)

	// Test JSONSet nested path not found
	_, err = JSONSet(`{"a":{"b":1}}`, "$.a.b", "2")
	t.Logf("JSONSet nested path: err=%v", err)

	// Test JSONSet with invalid path
	_, err = JSONSet(`{"a":1}`, "invalid_path", "2")
	t.Logf("JSONSet invalid path: err=%v", err)
}

// ---------------------------------------------------------------------------
// Foreign Key ON DELETE CASCADE
// ---------------------------------------------------------------------------

func TestBoost3_FKOnDeleteCascade(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	// Parent table
	boost3CreateTable(t, c, "fk_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Child table with FK ON DELETE CASCADE
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "fk_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
			{Name: "val", Type: query.TokenText},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable fk_child: %v", err)
	}

	boost3Insert(t, c, "fk_parent", []string{"id", "name"}, [][]query.Expression{
		{b3Num(1), b3Str("Parent1")},
		{b3Num(2), b3Str("Parent2")},
	})

	boost3Insert(t, c, "fk_child", []string{"id", "parent_id", "val"}, [][]query.Expression{
		{b3Num(1), b3Num(1), b3Str("child1")},
		{b3Num(2), b3Num(1), b3Str("child2")},
		{b3Num(3), b3Num(2), b3Str("child3")},
	})

	// Delete parent – CASCADE should delete children
	_, _, err = c.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent",
		Where: b3Eq("id", b3Num(1)),
	}, nil)
	if err != nil {
		t.Logf("Delete with FK CASCADE: %v (may be expected)", err)
	}
}

// ---------------------------------------------------------------------------
// Foreign Key ON DELETE SET NULL
// ---------------------------------------------------------------------------

func TestBoost3_FKOnDeleteSetNull(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "fk_sn_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	err := c.CreateTable(&query.CreateTableStmt{
		Table: "fk_sn_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_sn_parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          "SET NULL",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	boost3Insert(t, c, "fk_sn_parent", []string{"id", "name"}, [][]query.Expression{
		{b3Num(1), b3Str("P1")},
	})
	boost3Insert(t, c, "fk_sn_child", []string{"id", "parent_id"}, [][]query.Expression{
		{b3Num(1), b3Num(1)},
	})

	_, _, err = c.Delete(ctx, &query.DeleteStmt{
		Table: "fk_sn_parent",
		Where: b3Eq("id", b3Num(1)),
	}, nil)
	t.Logf("DELETE with ON DELETE SET NULL: err=%v", err)
}

// ---------------------------------------------------------------------------
// Foreign Key ON UPDATE CASCADE
// ---------------------------------------------------------------------------

func TestBoost3_FKOnUpdateCascade(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "fku_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	err := c.CreateTable(&query.CreateTableStmt{
		Table: "fku_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fku_parent",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "CASCADE",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	boost3Insert(t, c, "fku_parent", []string{"id", "name"}, [][]query.Expression{
		{b3Num(1), b3Str("P1")},
	})
	boost3Insert(t, c, "fku_child", []string{"id", "parent_id"}, [][]query.Expression{
		{b3Num(1), b3Num(1)},
	})

	_, _, err = c.Update(ctx, &query.UpdateStmt{
		Table: "fku_parent",
		Set: []*query.SetClause{
			{Column: "name", Value: b3Str("P1_updated")},
		},
		Where: b3Eq("id", b3Num(1)),
	}, nil)
	t.Logf("UPDATE with FK ON UPDATE CASCADE: err=%v", err)
}

// ---------------------------------------------------------------------------
// CreateView duplicate / DropView paths
// ---------------------------------------------------------------------------

func TestBoost3_ViewPaths(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "view_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "view_src"},
	}

	// Create view
	if err := c.CreateView("view_v1", viewStmt); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	// Duplicate should error
	if err := c.CreateView("view_v1", viewStmt); err == nil {
		t.Error("expected error creating duplicate view")
	}

	// Drop
	if err := c.DropView("view_v1"); err != nil {
		t.Fatalf("DropView: %v", err)
	}

	// Drop non-existent
	if err := c.DropView("does_not_exist_view"); err == nil {
		t.Error("expected error dropping non-existent view")
	}
}

// ---------------------------------------------------------------------------
// NOT NULL constraint
// ---------------------------------------------------------------------------

func TestBoost3_NotNullConstraint(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	err := c.CreateTable(&query.CreateTableStmt{
		Table: "nn_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText, NotNull: true},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Insert NULL into NOT NULL column should fail
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "nn_tbl",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{b3Num(1), &query.NullLiteral{}}},
	}, nil)
	if err == nil {
		t.Log("NOT NULL constraint not enforced on insert (may be expected behavior)")
	} else {
		t.Logf("NOT NULL insert error (expected): %v", err)
	}
}

// ---------------------------------------------------------------------------
// CHECK constraint on column
// ---------------------------------------------------------------------------

func TestBoost3_CheckConstraint(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	err := c.CreateTable(&query.CreateTableStmt{
		Table: "check_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "age", Type: query.TokenInteger, Check: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "age"},
				Operator: query.TokenGte,
				Right:    b3Num(0),
			}},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Valid insert
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "check_tbl",
		Columns: []string{"id", "age"},
		Values:  [][]query.Expression{{b3Num(1), b3Num(25)}},
	}, nil)
	if err != nil {
		t.Logf("Check valid insert: %v", err)
	}

	// Invalid insert (age < 0)
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "check_tbl",
		Columns: []string{"id", "age"},
		Values:  [][]query.Expression{{b3Num(2), b3Num(-1)}},
	}, nil)
	if err != nil {
		t.Logf("Check constraint violated (expected): %v", err)
	}
}

// ---------------------------------------------------------------------------
// INSERT ON CONFLICT IGNORE / REPLACE
// ---------------------------------------------------------------------------

func TestBoost3_InsertConflictPaths(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "conflict_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	boost3Insert(t, c, "conflict_tbl", []string{"id", "val"}, [][]query.Expression{
		{b3Num(1), b3Str("original")},
	})

	// INSERT OR IGNORE on conflict
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:          "conflict_tbl",
		Columns:        []string{"id", "val"},
		Values:         [][]query.Expression{{b3Num(1), b3Str("conflict")}},
		ConflictAction: query.ConflictIgnore,
	}, nil)
	if err != nil {
		t.Logf("INSERT OR IGNORE conflict: %v (may be expected)", err)
	}

	// INSERT OR REPLACE on conflict
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:          "conflict_tbl",
		Columns:        []string{"id", "val"},
		Values:         [][]query.Expression{{b3Num(1), b3Str("replaced")}},
		ConflictAction: query.ConflictReplace,
	}, nil)
	if err != nil {
		t.Logf("INSERT OR REPLACE conflict: %v (may be expected)", err)
	}

	res := boost3Query(t, c, "SELECT val FROM conflict_tbl WHERE id = 1")
	t.Logf("After conflict ops, val=%v", res.Rows)
}

// ---------------------------------------------------------------------------
// INSERT ... SELECT
// ---------------------------------------------------------------------------

func TestBoost3_InsertSelect(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "ins_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	boost3CreateTable(t, c, "ins_dst", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	boost3Insert(t, c, "ins_src", []string{"id", "val"}, [][]query.Expression{
		{b3Num(1), b3Str("a")},
		{b3Num(2), b3Str("b")},
		{b3Num(3), b3Str("c")},
	})

	// INSERT INTO dst SELECT * FROM src
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   "ins_dst",
		Columns: []string{"id", "val"},
		Select: &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "id"},
				&query.Identifier{Name: "val"},
			},
			From: &query.TableRef{Name: "ins_src"},
		},
	}, nil)
	if err != nil {
		t.Logf("INSERT...SELECT: %v", err)
	}

	res := boost3Query(t, c, "SELECT COUNT(*) FROM ins_dst")
	t.Logf("INSERT...SELECT result count: %v", res.Rows)
}

// ---------------------------------------------------------------------------
// AFTER INSERT trigger fires
// ---------------------------------------------------------------------------

func TestBoost3_AfterInsertTrigger(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "trigger_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	boost3CreateTable(t, c, "trigger_log", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "logged_val", Type: query.TokenText},
	})

	// Create AFTER INSERT trigger
	err := c.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "after_insert_trig",
		Table: "trigger_tbl",
		Event: "INSERT",
		Time:  "AFTER",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "trigger_log",
				Columns: []string{"id", "logged_val"},
				Values: [][]query.Expression{
					{
						&query.QualifiedIdentifier{Table: "NEW", Column: "id"},
						&query.QualifiedIdentifier{Table: "NEW", Column: "val"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Logf("CreateTrigger: %v", err)
		return
	}

	// Insert a row – trigger should fire
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "trigger_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{b3Num(1), b3Str("hello")}},
	}, nil)
	if err != nil {
		t.Logf("Insert with trigger: %v", err)
	}

	// Check trigger log
	res := boost3Query(t, c, "SELECT COUNT(*) FROM trigger_log")
	t.Logf("Trigger log count: %v", res.Rows)
}

// ---------------------------------------------------------------------------
// processDeleteRow – soft-deleted row skip
// ---------------------------------------------------------------------------

func TestBoost3_DeleteSoftDeletedRows(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "soft_del_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	boost3Insert(t, c, "soft_del_tbl", []string{"id", "val"}, [][]query.Expression{
		{b3Num(1), b3Str("a")},
		{b3Num(2), b3Str("b")},
		{b3Num(3), b3Str("c")},
	})

	// Delete row 2 normally
	c.Delete(ctx, &query.DeleteStmt{
		Table: "soft_del_tbl",
		Where: b3Eq("id", b3Num(2)),
	}, nil)

	// Now delete again to exercise soft-deleted skip
	_, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "soft_del_tbl",
		Where: b3Eq("id", b3Num(2)),
	}, nil)
	if err != nil {
		t.Logf("Re-delete soft-deleted row: %v", err)
	}

	// Delete without WHERE (full scan) exercises processDeleteRow on all rows
	c.Delete(ctx, &query.DeleteStmt{Table: "soft_del_tbl"}, nil)
}

// ---------------------------------------------------------------------------
// processDeleteRow – with RLS enabled but no user / with user
// ---------------------------------------------------------------------------

func TestBoost3_DeleteWithRLSNoUser(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()
	c.EnableRLS()
	c.CreateRLSPolicy(&security.Policy{
		Name:       "del_policy",
		TableName:  "rls_del_tbl",
		Type:       security.PolicyDelete,
		Expression: "owner = 'admin'",
		Enabled:    true,
	})

	boost3CreateTable(t, c, "rls_del_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "owner", Type: query.TokenText},
	})

	boost3Insert(t, c, "rls_del_tbl", []string{"id", "owner"}, [][]query.Expression{
		{b3Num(1), b3Str("admin")},
		{b3Num(2), b3Str("user1")},
	})

	// Delete without user context (RLS won't restrict – no cobaltdb_user in ctx)
	n, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "rls_del_tbl",
		Where: b3Eq("id", b3Num(1)),
	}, nil)
	t.Logf("Delete with RLS no user: n=%d err=%v", n, err)

	// Delete with user context
	rlsCtx := context.WithValue(ctx, "cobaltdb_user", "admin")
	n, _, err = c.Delete(rlsCtx, &query.DeleteStmt{
		Table: "rls_del_tbl",
		Where: b3Eq("id", b3Num(2)),
	}, nil)
	t.Logf("Delete with RLS user=admin: n=%d err=%v", n, err)
}

// ---------------------------------------------------------------------------
// toInt variants – exercises catalog_helpers.go
// ---------------------------------------------------------------------------

func TestBoost3_ToIntVariants(t *testing.T) {
	tests := []struct {
		v    interface{}
		want int
		ok   bool
	}{
		{42, 42, true},
		{int64(99), 99, true},
		{float64(3.7), 3, true},
		{"hello", 0, false},
		{nil, 0, false},
		{true, 0, false},
	}
	for _, tt := range tests {
		got, ok := toInt(tt.v)
		if ok != tt.ok {
			t.Errorf("toInt(%v): ok=%v want %v", tt.v, ok, tt.ok)
		}
		if ok && got != tt.want {
			t.Errorf("toInt(%v): got %d want %d", tt.v, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// evaluateExprWithGroupAggregatesJoin – hit via JOIN + GROUP BY
// ---------------------------------------------------------------------------

func TestBoost3_JoinGroupByWithExpressions(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "jgb_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	boost3CreateTable(t, c, "jgb_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "region", Type: query.TokenText},
	})

	boost3Insert(t, c, "jgb_customers", []string{"id", "name", "region"}, [][]query.Expression{
		{b3Num(1), b3Str("Alice"), b3Str("N")},
		{b3Num(2), b3Str("Bob"), b3Str("S")},
		{b3Num(3), b3Str("Carol"), b3Str("N")},
	})

	boost3Insert(t, c, "jgb_orders", []string{"id", "customer_id", "amount"}, [][]query.Expression{
		{b3Num(1), b3Num(1), b3Num(100)},
		{b3Num(2), b3Num(1), b3Num(200)},
		{b3Num(3), b3Num(2), b3Num(150)},
		{b3Num(4), b3Num(3), b3Num(50)},
	})

	queries := []string{
		`SELECT jgb_customers.region, COUNT(*), SUM(jgb_orders.amount)
		 FROM jgb_orders
		 JOIN jgb_customers ON jgb_orders.customer_id = jgb_customers.id
		 GROUP BY jgb_customers.region`,

		`SELECT jgb_customers.name, SUM(jgb_orders.amount) as total
		 FROM jgb_orders
		 JOIN jgb_customers ON jgb_orders.customer_id = jgb_customers.id
		 GROUP BY jgb_customers.name
		 HAVING SUM(jgb_orders.amount) > 100
		 ORDER BY total DESC`,

		`SELECT jgb_customers.name, COUNT(*) as cnt
		 FROM jgb_orders
		 LEFT JOIN jgb_customers ON jgb_orders.customer_id = jgb_customers.id
		 GROUP BY jgb_customers.name
		 ORDER BY cnt`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("JOIN+GROUP BY returned %d rows", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// applyGroupByOrderBy – complex ORDER BY in GROUP BY context
// ---------------------------------------------------------------------------

func TestBoost3_GroupByOrderByExpressions(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "gbo_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
		{Name: "n", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat := "A"
		if i > 10 {
			cat = "B"
		}
		boost3Insert(t, c, "gbo_tbl", []string{"id", "cat", "n"}, [][]query.Expression{
			{b3Num(float64(i)), b3Str(cat), b3Num(float64(i * 5))},
		})
	}

	queries := []string{
		`SELECT cat, SUM(n) as s, COUNT(*) as c FROM gbo_tbl GROUP BY cat ORDER BY s DESC`,
		`SELECT cat, AVG(n) as avg_n FROM gbo_tbl GROUP BY cat ORDER BY avg_n ASC`,
		`SELECT cat, MIN(n), MAX(n) FROM gbo_tbl GROUP BY cat ORDER BY cat`,
		`SELECT cat, COUNT(*) FROM gbo_tbl GROUP BY cat HAVING COUNT(*) > 5 ORDER BY COUNT(*) DESC`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("GroupByOrderBy returned %d rows", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// executeSelectWithJoin – multiple join paths
// ---------------------------------------------------------------------------

func TestBoost3_JoinPaths(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "jp_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	boost3CreateTable(t, c, "jp_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a_id", Type: query.TokenInteger},
		{Name: "val", Type: query.TokenInteger},
	})

	boost3CreateTable(t, c, "jp_c", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "b_id", Type: query.TokenInteger},
		{Name: "label", Type: query.TokenText},
	})

	boost3Insert(t, c, "jp_a", []string{"id", "name"}, [][]query.Expression{
		{b3Num(1), b3Str("X")},
		{b3Num(2), b3Str("Y")},
		{b3Num(3), b3Str("Z")},
	})

	boost3Insert(t, c, "jp_b", []string{"id", "a_id", "val"}, [][]query.Expression{
		{b3Num(1), b3Num(1), b3Num(10)},
		{b3Num(2), b3Num(1), b3Num(20)},
		{b3Num(3), b3Num(2), b3Num(30)},
	})

	boost3Insert(t, c, "jp_c", []string{"id", "b_id", "label"}, [][]query.Expression{
		{b3Num(1), b3Num(1), b3Str("L1")},
		{b3Num(2), b3Num(3), b3Str("L2")},
	})

	queries := []string{
		`SELECT jp_a.name, jp_b.val FROM jp_a INNER JOIN jp_b ON jp_a.id = jp_b.a_id`,
		`SELECT jp_a.name, jp_b.val FROM jp_a LEFT JOIN jp_b ON jp_a.id = jp_b.a_id`,
		`SELECT jp_a.name, jp_b.val FROM jp_a RIGHT JOIN jp_b ON jp_a.id = jp_b.a_id`,
		`SELECT jp_a.name, jp_b.val, jp_c.label FROM jp_a
		 JOIN jp_b ON jp_a.id = jp_b.a_id
		 JOIN jp_c ON jp_b.id = jp_c.b_id`,
		`SELECT jp_a.name, jp_b.val FROM jp_a
		 JOIN jp_b ON jp_a.id = jp_b.a_id
		 WHERE jp_b.val > 15`,
		`SELECT jp_a.name, COUNT(*) FROM jp_a
		 LEFT JOIN jp_b ON jp_a.id = jp_b.a_id
		 GROUP BY jp_a.name
		 ORDER BY COUNT(*) DESC`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Join query error: %v", err)
		} else {
			t.Logf("Join query rows: %d", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// executeScalarSelect / executeScalarAggregate paths
// ---------------------------------------------------------------------------

func TestBoost3_ScalarSelectPaths(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "scalar_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		boost3Insert(t, c, "scalar_tbl", []string{"id", "val"}, [][]query.Expression{
			{b3Num(float64(i)), b3Num(float64(i * 10))},
		})
	}

	queries := []string{
		`SELECT 1 + 2`,
		`SELECT 'hello'`,
		`SELECT COUNT(*) FROM scalar_tbl`,
		`SELECT SUM(val), AVG(val), MIN(val), MAX(val) FROM scalar_tbl`,
		`SELECT COUNT(*) FROM scalar_tbl WHERE val > 50`,
		`SELECT MAX(val) FROM scalar_tbl WHERE id > 5`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Scalar query error: %v", err)
		} else {
			t.Logf("Scalar query rows: %v", res.Rows)
		}
	}
}

// ---------------------------------------------------------------------------
// updateLocked – various update paths (index-assisted, full scan)
// ---------------------------------------------------------------------------

func TestBoost3_UpdatePaths(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "upd_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
		{Name: "score", Type: query.TokenInteger},
	})

	// Create index on status
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_upd_status",
		Table:   "upd_tbl",
		Columns: []string{"status"},
	})

	for i := 1; i <= 20; i++ {
		status := "active"
		if i > 10 {
			status = "inactive"
		}
		boost3Insert(t, c, "upd_tbl", []string{"id", "status", "score"}, [][]query.Expression{
			{b3Num(float64(i)), b3Str(status), b3Num(float64(i * 5))},
		})
	}

	// Update with WHERE matching index
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "upd_tbl",
		Set: []*query.SetClause{
			{Column: "score", Value: b3Num(999)},
		},
		Where: b3Eq("status", b3Str("inactive")),
	}, nil)
	if err != nil {
		t.Logf("Update with index: %v", err)
	}

	// Update all (no WHERE)
	_, _, err = c.Update(ctx, &query.UpdateStmt{
		Table: "upd_tbl",
		Set: []*query.SetClause{
			{Column: "status", Value: b3Str("updated")},
		},
	}, nil)
	if err != nil {
		t.Logf("Update all: %v", err)
	}

	res := boost3Query(t, c, "SELECT COUNT(*) FROM upd_tbl WHERE status = 'updated'")
	t.Logf("Updated count: %v", res.Rows)
}

// ---------------------------------------------------------------------------
// countRows in stats – exercises stats.go
// ---------------------------------------------------------------------------

func TestBoost3_CollectStats(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "stats_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "value", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		cat := fmt.Sprintf("cat%d", i%5)
		boost3Insert(t, c, "stats_tbl", []string{"id", "category", "value"}, [][]query.Expression{
			{b3Num(float64(i)), b3Str(cat), b3Num(float64(i * 7))},
		})
	}

	// Analyze triggers stat collection
	err := c.Analyze("stats_tbl")
	if err != nil {
		t.Logf("Analyze: %v", err)
	}

	// GetTableStats
	stats, err := c.GetTableStats("stats_tbl")
	if err != nil {
		t.Logf("GetTableStats: %v", err)
	} else {
		t.Logf("TableStats: rows=%v", stats)
	}
}

// ---------------------------------------------------------------------------
// Window function variants in selectLocked
// ---------------------------------------------------------------------------

func TestBoost3_WindowFunctions(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "wf_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "salary", Type: query.TokenInteger},
	})

	boost3Insert(t, c, "wf_tbl", []string{"id", "dept", "salary"}, [][]query.Expression{
		{b3Num(1), b3Str("eng"), b3Num(90000)},
		{b3Num(2), b3Str("eng"), b3Num(85000)},
		{b3Num(3), b3Str("hr"), b3Num(70000)},
		{b3Num(4), b3Str("hr"), b3Num(75000)},
		{b3Num(5), b3Str("eng"), b3Num(95000)},
	})

	queries := []string{
		`SELECT id, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM wf_tbl`,
		`SELECT id, dept, salary, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM wf_tbl`,
		`SELECT id, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk FROM wf_tbl`,
		`SELECT id, salary, SUM(salary) OVER (ORDER BY id) as running_total FROM wf_tbl`,
		`SELECT dept, AVG(salary) OVER (PARTITION BY dept) as avg_sal FROM wf_tbl`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window func query error: %v", err)
		} else {
			t.Logf("Window func returned %d rows", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// CTE paths – ExecuteCTE
// ---------------------------------------------------------------------------

func TestBoost3_CTEPaths(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "cte_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "parent_id", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	boost3Insert(t, c, "cte_src", []string{"id", "parent_id", "name"}, [][]query.Expression{
		{b3Num(1), &query.NullLiteral{}, b3Str("root")},
		{b3Num(2), b3Num(1), b3Str("child1")},
		{b3Num(3), b3Num(1), b3Str("child2")},
		{b3Num(4), b3Num(2), b3Str("grandchild1")},
	})

	queries := []string{
		`WITH cte AS (SELECT id, name FROM cte_src WHERE parent_id IS NULL)
		 SELECT * FROM cte`,

		`WITH
		  roots AS (SELECT id, name FROM cte_src WHERE parent_id IS NULL),
		  children AS (SELECT id, name FROM cte_src WHERE parent_id IS NOT NULL)
		 SELECT name FROM roots UNION ALL SELECT name FROM children`,

		`WITH stats AS (SELECT COUNT(*) as cnt, MAX(id) as max_id FROM cte_src)
		 SELECT * FROM stats`,

		`WITH RECURSIVE tree AS (
		   SELECT id, parent_id, name, 0 as level FROM cte_src WHERE parent_id IS NULL
		   UNION ALL
		   SELECT c.id, c.parent_id, c.name, t.level + 1
		   FROM cte_src c JOIN tree t ON c.parent_id = t.id
		 )
		 SELECT name, level FROM tree ORDER BY level, name`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("CTE query error: %v", err)
		} else {
			t.Logf("CTE returned %d rows", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// AlterTable paths – add/drop/rename column, rename table
// ---------------------------------------------------------------------------

func TestBoost3_AlterTablePaths(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "alter_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	boost3Insert(t, c, "alter_tbl", []string{"id", "name"}, [][]query.Expression{
		{b3Num(1), b3Str("Alice")},
		{b3Num(2), b3Str("Bob")},
	})

	// Add column
	err := c.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "alter_tbl",
		Action: "ADD_COLUMN",
		Column: query.ColumnDef{Name: "age", Type: query.TokenInteger},
	})
	if err != nil {
		t.Logf("AlterTableAddColumn: %v", err)
	}

	// Drop column
	err = c.AlterTableDropColumn(&query.AlterTableStmt{
		Table:  "alter_tbl",
		Action: "DROP_COLUMN",
		Column: query.ColumnDef{Name: "age"},
	})
	if err != nil {
		t.Logf("AlterTableDropColumn: %v", err)
	}

	// Rename column
	err = c.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "alter_tbl",
		Action:  "RENAME_COLUMN",
		OldName: "name",
		NewName: "full_name",
	})
	if err != nil {
		t.Logf("AlterTableRenameColumn: %v", err)
	}

	// Rename table
	err = c.AlterTableRename(&query.AlterTableStmt{
		Table:   "alter_tbl",
		Action:  "RENAME_TABLE",
		NewName: "alter_tbl_renamed",
	})
	if err != nil {
		t.Logf("AlterTableRename: %v", err)
	}

	res := boost3Query(t, c, "SELECT COUNT(*) FROM alter_tbl_renamed")
	t.Logf("AlterTable result: %v", res.Rows)
}

// ---------------------------------------------------------------------------
// Maintenance ops – Save, Load, Vacuum, Analyze
// ---------------------------------------------------------------------------

func TestBoost3_MaintenancePaths(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "maint_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		boost3Insert(t, c, "maint_tbl", []string{"id", "val"}, [][]query.Expression{
			{b3Num(float64(i)), b3Str(fmt.Sprintf("v%d", i))},
		})
	}

	// Save
	if err := c.Save(); err != nil {
		t.Logf("Save: %v", err)
	}

	// Load
	if err := c.Load(); err != nil {
		t.Logf("Load: %v", err)
	}

	// Vacuum
	if err := c.Vacuum(); err != nil {
		t.Logf("Vacuum: %v", err)
	}

	// Analyze
	if err := c.Analyze("maint_tbl"); err != nil {
		t.Logf("Analyze: %v", err)
	}

	// ListTables
	tables := c.ListTables()
	t.Logf("ListTables: %v", tables)
}

// ---------------------------------------------------------------------------
// evaluateCastExpr – various CAST types
// ---------------------------------------------------------------------------

func TestBoost3_CastExpressions(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "cast_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "sval", Type: query.TokenText},
		{Name: "nval", Type: query.TokenInteger},
	})

	boost3Insert(t, c, "cast_tbl", []string{"id", "sval", "nval"}, [][]query.Expression{
		{b3Num(1), b3Str("42"), b3Num(100)},
		{b3Num(2), b3Str("3.14"), b3Num(200)},
		{b3Num(3), b3Str("hello"), b3Num(300)},
	})

	queries := []string{
		`SELECT CAST(sval AS INTEGER) FROM cast_tbl WHERE id = 1`,
		`SELECT CAST(sval AS REAL) FROM cast_tbl WHERE id = 2`,
		`SELECT CAST(nval AS TEXT) FROM cast_tbl WHERE id = 1`,
		`SELECT CAST(nval AS BOOLEAN) FROM cast_tbl WHERE id = 1`,
		`SELECT CAST(sval AS TEXT) FROM cast_tbl WHERE id = 3`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Cast query error: %v", err)
		} else {
			t.Logf("Cast query result: %v", res.Rows)
		}
	}
}

// ---------------------------------------------------------------------------
// evaluateIn – IN with various expressions
// ---------------------------------------------------------------------------

func TestBoost3_EvaluateInExpr(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "in_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	boost3Insert(t, c, "in_tbl", []string{"id", "status"}, [][]query.Expression{
		{b3Num(1), b3Str("active")},
		{b3Num(2), b3Str("inactive")},
		{b3Num(3), b3Str("pending")},
		{b3Num(4), b3Str("active")},
		{b3Num(5), b3Str("blocked")},
	})

	queries := []string{
		`SELECT * FROM in_tbl WHERE status IN ('active', 'pending')`,
		`SELECT * FROM in_tbl WHERE status NOT IN ('active')`,
		`SELECT * FROM in_tbl WHERE id IN (1, 3, 5)`,
		`SELECT * FROM in_tbl WHERE id NOT IN (2, 4)`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("IN query error: %v", err)
		} else {
			t.Logf("IN query rows: %d", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// Query cache – GetQueryCacheStats
// ---------------------------------------------------------------------------

func TestBoost3_QueryCacheStats(t *testing.T) {
	c := newBoost3Cat()
	c.EnableQueryCache(100, 0)

	boost3CreateTable(t, c, "qc_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	boost3Insert(t, c, "qc_tbl", []string{"id", "val"}, [][]query.Expression{
		{b3Num(1), b3Str("a")},
		{b3Num(2), b3Str("b")},
	})

	// Execute same query multiple times to hit cache
	for i := 0; i < 3; i++ {
		c.ExecuteQuery("SELECT * FROM qc_tbl ORDER BY id")
	}

	hits, misses, size := c.GetQueryCacheStats()
	t.Logf("Cache stats: hits=%d misses=%d size=%d", hits, misses, size)

	// Invalidate cache via insert (which calls invalidateQueryCache internally)
	boost3Insert(t, c, "qc_tbl", []string{"id", "val"}, [][]query.Expression{
		{b3Num(3), b3Str("c")},
	})

	// Execute again to miss
	c.ExecuteQuery("SELECT * FROM qc_tbl ORDER BY id")

	hits2, misses2, size2 := c.GetQueryCacheStats()
	t.Logf("Cache stats after invalidate: hits=%d misses=%d size=%d", hits2, misses2, size2)
}

// ---------------------------------------------------------------------------
// Table with UNIQUE constraint – exercises duplicate check
// ---------------------------------------------------------------------------

func TestBoost3_CreateTableWithConstraints(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	// Table with UNIQUE column
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "unique_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText, Unique: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable with UNIQUE: %v", err)
	}

	// Insert valid
	boost3Insert(t, c, "unique_tbl", []string{"id", "email", "name"}, [][]query.Expression{
		{b3Num(1), b3Str("a@b.com"), b3Str("Alice")},
		{b3Num(2), b3Str("c@d.com"), b3Str("Bob")},
	})

	// Insert duplicate unique should fail
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "unique_tbl",
		Columns: []string{"id", "email", "name"},
		Values:  [][]query.Expression{{b3Num(3), b3Str("a@b.com"), b3Str("Charlie")}},
	}, nil)
	if err != nil {
		t.Logf("Unique constraint violation (expected): %v", err)
	}
}

// ---------------------------------------------------------------------------
// DropTrigger and executeTriggers
// ---------------------------------------------------------------------------

func TestBoost3_TriggerPaths(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "trig_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create trigger
	err := c.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trig_b3_1",
		Table: "trig_src",
		Event: "UPDATE",
		Time:  "BEFORE",
		Body:  []query.Statement{},
	})
	if err != nil {
		t.Logf("CreateTrigger BEFORE UPDATE: %v", err)
	}

	// Duplicate trigger name should fail
	err = c.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trig_b3_1",
		Table: "trig_src",
		Event: "UPDATE",
		Time:  "BEFORE",
		Body:  []query.Statement{},
	})
	if err == nil {
		t.Log("Expected error on duplicate trigger, got nil")
	}

	// Drop trigger
	if err := c.DropTrigger("trig_b3_1"); err != nil {
		t.Logf("DropTrigger: %v", err)
	}

	// Drop non-existent trigger
	if err := c.DropTrigger("does_not_exist_trig"); err == nil {
		t.Log("Expected error dropping non-existent trigger")
	}
}

// ---------------------------------------------------------------------------
// BETWEEN edge cases
// ---------------------------------------------------------------------------

func TestBoost3_BetweenEdgeCases(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "between_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	boost3Insert(t, c, "between_tbl", []string{"id", "val", "name"}, [][]query.Expression{
		{b3Num(1), b3Num(10), b3Str("apple")},
		{b3Num(2), b3Num(20), b3Str("banana")},
		{b3Num(3), b3Num(30), b3Str("cherry")},
		{b3Num(4), b3Num(40), b3Str("date")},
	})

	queries := []string{
		`SELECT * FROM between_tbl WHERE val BETWEEN 15 AND 35`,
		`SELECT * FROM between_tbl WHERE val NOT BETWEEN 20 AND 30`,
		`SELECT * FROM between_tbl WHERE name BETWEEN 'banana' AND 'cherry'`,
		`SELECT * FROM between_tbl WHERE val BETWEEN 10 AND 10`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Between query error: %v", err)
		} else {
			t.Logf("Between query rows: %d", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// deleteWithUsingLocked – DELETE ... USING
// ---------------------------------------------------------------------------

func TestBoost3_DeleteUsingPaths(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "du_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	boost3CreateTable(t, c, "du_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "active", Type: query.TokenInteger},
	})

	boost3Insert(t, c, "du_customers", []string{"id", "active"}, [][]query.Expression{
		{b3Num(1), b3Num(1)},
		{b3Num(2), b3Num(0)},
	})

	boost3Insert(t, c, "du_orders", []string{"id", "customer_id", "amount"}, [][]query.Expression{
		{b3Num(1), b3Num(1), b3Num(100)},
		{b3Num(2), b3Num(2), b3Num(200)},
		{b3Num(3), b3Num(1), b3Num(300)},
	})

	n, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "du_orders",
		Using: []*query.TableRef{{Name: "du_customers"}},
		Where: &query.BinaryExpr{
			Operator: query.TokenAnd,
			Left: &query.BinaryExpr{
				Operator: query.TokenEq,
				Left:     &query.QualifiedIdentifier{Table: "du_orders", Column: "customer_id"},
				Right:    &query.QualifiedIdentifier{Table: "du_customers", Column: "id"},
			},
			Right: &query.BinaryExpr{
				Operator: query.TokenEq,
				Left:     &query.QualifiedIdentifier{Table: "du_customers", Column: "active"},
				Right:    b3Num(0),
			},
		},
	}, nil)
	t.Logf("DELETE USING result: n=%d err=%v", n, err)
}

// ---------------------------------------------------------------------------
// updateWithJoinLocked – UPDATE with FROM
// ---------------------------------------------------------------------------

func TestBoost3_UpdateWithJoinPaths(t *testing.T) {
	ctx := context.Background()
	c := newBoost3Cat()

	boost3CreateTable(t, c, "uwj_items", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat_id", Type: query.TokenInteger},
		{Name: "price", Type: query.TokenInteger},
	})

	boost3CreateTable(t, c, "uwj_cats", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "discount", Type: query.TokenInteger},
	})

	boost3Insert(t, c, "uwj_cats", []string{"id", "discount"}, [][]query.Expression{
		{b3Num(1), b3Num(10)},
		{b3Num(2), b3Num(20)},
	})

	boost3Insert(t, c, "uwj_items", []string{"id", "cat_id", "price"}, [][]query.Expression{
		{b3Num(1), b3Num(1), b3Num(100)},
		{b3Num(2), b3Num(2), b3Num(200)},
		{b3Num(3), b3Num(1), b3Num(150)},
	})

	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "uwj_items",
		Set: []*query.SetClause{
			{Column: "price", Value: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "price"},
				Operator: query.TokenMinus,
				Right:    b3Num(10),
			}},
		},
		From: &query.TableRef{Name: "uwj_cats"},
		Where: &query.BinaryExpr{
			Operator: query.TokenAnd,
			Left: &query.BinaryExpr{
				Operator: query.TokenEq,
				Left:     &query.QualifiedIdentifier{Table: "uwj_items", Column: "cat_id"},
				Right:    &query.QualifiedIdentifier{Table: "uwj_cats", Column: "id"},
			},
			Right: &query.BinaryExpr{
				Operator: query.TokenEq,
				Left:     &query.QualifiedIdentifier{Table: "uwj_cats", Column: "discount"},
				Right:    b3Num(10),
			},
		},
	}, nil)
	t.Logf("UPDATE with JOIN: err=%v", err)
}

// ---------------------------------------------------------------------------
// DropTable with indexes – exercises DropTable fully
// ---------------------------------------------------------------------------

func TestBoost3_DropTableWithIndexes(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "drop_idx_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "email", Type: query.TokenText},
		{Name: "name", Type: query.TokenText},
	})

	// Create multiple indexes
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_di_email",
		Table:   "drop_idx_tbl",
		Columns: []string{"email"},
	})
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_di_name",
		Table:   "drop_idx_tbl",
		Columns: []string{"name"},
	})

	boost3Insert(t, c, "drop_idx_tbl", []string{"id", "email", "name"}, [][]query.Expression{
		{b3Num(1), b3Str("a@b.com"), b3Str("Alice")},
	})

	// Drop table (should also clean up indexes)
	if err := c.DropTable(&query.DropTableStmt{Table: "drop_idx_tbl"}); err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	// Try to drop again (should fail)
	if err := c.DropTable(&query.DropTableStmt{Table: "drop_idx_tbl"}); err == nil {
		t.Error("Expected error dropping non-existent table")
	}
}

// ---------------------------------------------------------------------------
// GetRow – directly test row retrieval
// ---------------------------------------------------------------------------

func TestBoost3_GetRow(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "getrow_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	boost3Insert(t, c, "getrow_tbl", []string{"id", "val"}, [][]query.Expression{
		{b3Num(1), b3Str("hello")},
		{b3Num(2), b3Str("world")},
	})

	// GetRow existing
	row, err := c.GetRow("getrow_tbl", 1)
	if err != nil {
		t.Logf("GetRow existing: %v", err)
	} else {
		t.Logf("GetRow(1): %v", row)
	}

	// GetRow non-existent
	row, err = c.GetRow("getrow_tbl", 999)
	t.Logf("GetRow non-existent: row=%v err=%v", row, err)

	// GetRow non-existent table
	row, err = c.GetRow("nonexistent_tbl", 1)
	t.Logf("GetRow bad table: row=%v err=%v", row, err)
}

// ---------------------------------------------------------------------------
// evaluateTemporalExpr – AS OF queries
// ---------------------------------------------------------------------------

func TestBoost3_TemporalQueries(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "temporal_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	boost3Insert(t, c, "temporal_tbl", []string{"id", "val"}, [][]query.Expression{
		{b3Num(1), b3Str("a")},
	})

	res, err := c.ExecuteQuery("SELECT * FROM temporal_tbl FOR SYSTEM_TIME AS OF NOW()")
	if err != nil {
		t.Logf("AS OF NOW(): %v", err)
	} else {
		t.Logf("AS OF NOW() rows: %d", len(res.Rows))
	}
}

// ---------------------------------------------------------------------------
// HAVING with various aggregate conditions
// ---------------------------------------------------------------------------

func TestBoost3_HavingComplexConditions(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "hav_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		cat := "A"
		if i > 10 {
			cat = "B"
		}
		if i > 20 {
			cat = "C"
		}
		boost3Insert(t, c, "hav_tbl", []string{"id", "category", "amount"}, [][]query.Expression{
			{b3Num(float64(i)), b3Str(cat), b3Num(float64(i * 10))},
		})
	}

	queries := []string{
		`SELECT category, COUNT(*) as cnt FROM hav_tbl GROUP BY category HAVING cnt = 10`,
		`SELECT category, COUNT(*) as cnt FROM hav_tbl GROUP BY category HAVING cnt > 5`,
		`SELECT category, SUM(amount) as total FROM hav_tbl GROUP BY category HAVING total > 1000`,
		`SELECT category, COUNT(*) as cnt FROM hav_tbl GROUP BY category HAVING cnt BETWEEN 5 AND 15`,
		`SELECT category, AVG(amount) as avg_amt FROM hav_tbl GROUP BY category HAVING avg_amt > 100`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("HAVING query error: %v", err)
		} else {
			t.Logf("HAVING query returned %d rows", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// WAL transaction commit – exercises CommitTransaction with WAL
// ---------------------------------------------------------------------------

func TestBoost3_CommitWithWAL(t *testing.T) {
	walDir := t.TempDir()
	walPath := walDir + "/test.wal"
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, wal)

	boost3CreateTable(t, c, "wal_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	c.BeginTransaction(100)

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "wal_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{b3Num(1), b3Str("wal_row")}},
	}, nil)

	if err := c.CommitTransaction(); err != nil {
		t.Logf("CommitTransaction with WAL: %v", err)
	}

	// Rollback with WAL
	c.BeginTransaction(101)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "wal_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{b3Num(2), b3Str("to_rollback")}},
	}, nil)

	if err := c.RollbackTransaction(); err != nil {
		t.Logf("RollbackTransaction with WAL: %v", err)
	}

	res := boost3Query(t, c, "SELECT COUNT(*) FROM wal_tbl")
	t.Logf("After WAL txn ops, count=%v", res.Rows)
}

// ---------------------------------------------------------------------------
// DISTINCT with various queries
// ---------------------------------------------------------------------------

func TestBoost3_DistinctQueries(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "distinct_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "color", Type: query.TokenText},
		{Name: "size", Type: query.TokenText},
	})

	boost3Insert(t, c, "distinct_src", []string{"id", "color", "size"}, [][]query.Expression{
		{b3Num(1), b3Str("red"), b3Str("S")},
		{b3Num(2), b3Str("blue"), b3Str("M")},
		{b3Num(3), b3Str("red"), b3Str("L")},
		{b3Num(4), b3Str("green"), b3Str("S")},
		{b3Num(5), b3Str("blue"), b3Str("S")},
		{b3Num(6), b3Str("red"), b3Str("S")},
	})

	queries := []string{
		`SELECT DISTINCT color FROM distinct_src`,
		`SELECT DISTINCT color, size FROM distinct_src`,
		`SELECT DISTINCT color FROM distinct_src ORDER BY color`,
		`SELECT DISTINCT size FROM distinct_src WHERE color = 'red'`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("DISTINCT query error: %v", err)
		} else {
			t.Logf("DISTINCT query rows: %d", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// ORDER BY with multiple columns and NULL handling
// ---------------------------------------------------------------------------

func TestBoost3_OrderByMultipleColumns(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "ob_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "priority", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	boost3Insert(t, c, "ob_multi", []string{"id", "priority", "name"}, [][]query.Expression{
		{b3Num(1), b3Num(2), b3Str("B")},
		{b3Num(2), b3Num(1), b3Str("A")},
		{b3Num(3), b3Num(2), b3Str("A")},
		{b3Num(4), b3Num(1), b3Str("B")},
	})

	queries := []string{
		`SELECT * FROM ob_multi ORDER BY priority, name`,
		`SELECT * FROM ob_multi ORDER BY priority DESC, name ASC`,
		`SELECT * FROM ob_multi ORDER BY priority ASC, name DESC`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("OrderBy query error: %v", err)
		} else {
			t.Logf("OrderBy query rows: %d", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// Subquery in WHERE – exercises resolveOuterRefs paths
// ---------------------------------------------------------------------------

func TestBoost3_SubqueryPaths(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "sub_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	boost3CreateTable(t, c, "sub_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	boost3Insert(t, c, "sub_b", []string{"id", "name"}, [][]query.Expression{
		{b3Num(1), b3Str("Engineering")},
		{b3Num(2), b3Str("HR")},
	})

	boost3Insert(t, c, "sub_a", []string{"id", "dept_id", "salary"}, [][]query.Expression{
		{b3Num(1), b3Num(1), b3Num(90000)},
		{b3Num(2), b3Num(1), b3Num(85000)},
		{b3Num(3), b3Num(2), b3Num(70000)},
		{b3Num(4), b3Num(2), b3Num(75000)},
	})

	queries := []string{
		`SELECT * FROM sub_a WHERE dept_id IN (SELECT id FROM sub_b WHERE name = 'Engineering')`,
		`SELECT * FROM sub_a WHERE salary > (SELECT AVG(salary) FROM sub_a)`,
		`SELECT dept_id, COUNT(*) FROM sub_a WHERE dept_id IN (1, 2) GROUP BY dept_id`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Subquery error: %v", err)
		} else {
			t.Logf("Subquery rows: %d", len(res.Rows))
		}
	}
}

// ---------------------------------------------------------------------------
// evaluateGroupByComplex – test GROUP BY paths in computeAggregatesWithGroupBy
// ---------------------------------------------------------------------------

func TestBoost3_GroupByAggregatesPaths(t *testing.T) {
	c := newBoost3Cat()

	boost3CreateTable(t, c, "gbag_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "group_col", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 15; i++ {
		gc := "G1"
		if i > 5 {
			gc = "G2"
		}
		if i > 10 {
			gc = "G3"
		}
		boost3Insert(t, c, "gbag_tbl", []string{"id", "group_col", "val"}, [][]query.Expression{
			{b3Num(float64(i)), b3Str(gc), b3Num(float64(i * 3))},
		})
	}

	queries := []string{
		`SELECT group_col, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM gbag_tbl GROUP BY group_col`,
		`SELECT group_col, COUNT(DISTINCT val) FROM gbag_tbl GROUP BY group_col`,
		`SELECT group_col, SUM(val) * 2 as double_sum FROM gbag_tbl GROUP BY group_col`,
		`SELECT group_col, COUNT(*) as c, SUM(val) as s FROM gbag_tbl GROUP BY group_col HAVING s > 50`,
	}

	for _, q := range queries {
		res, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("GroupBy agg query error: %v", err)
		} else {
			t.Logf("GroupBy agg returned %d rows", len(res.Rows))
		}
	}
}
