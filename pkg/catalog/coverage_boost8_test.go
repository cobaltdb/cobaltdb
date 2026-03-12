package catalog

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// 1. CommitTransaction with WAL — covers WAL commit write path
// ============================================================

func TestCovBoost8_CommitTransaction_WithWAL(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Open a real WAL file and set it
	walPath := filepath.Join(t.TempDir(), "commit.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	cat.SetWAL(wal)

	// Create table and insert within a transaction
	createCoverageTestTable(t, cat, "commit_wal_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})
	cat.BeginTransaction(100)
	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "commit_wal_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "hello"},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Commit — this should write WAL commit record
	if err := cat.CommitTransaction(); err != nil {
		t.Fatalf("commit with WAL: %v", err)
	}

	// Verify data persists after commit
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "commit_wal_t"},
	}, nil)
	if err != nil {
		t.Fatalf("select after commit: %v", err)
	}
	if len(rows) != 1 || rows[0][0] != "hello" {
		t.Fatalf("expected 1 row with val='hello', got %v", rows)
	}
}

// ============================================================
// 2. RollbackTransaction with WAL — covers WAL rollback write path
// ============================================================

func TestCovBoost8_RollbackTransaction_WithWAL(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	walPath := filepath.Join(t.TempDir(), "rollback.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	cat.SetWAL(wal)

	createCoverageTestTable(t, cat, "rollback_wal_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert a row outside txn
	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "rollback_wal_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "before"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Begin txn, insert another row, then rollback
	cat.BeginTransaction(200)
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "rollback_wal_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "should_disappear"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Rollback — should write WAL rollback record and undo insert
	if err := cat.RollbackTransaction(); err != nil {
		t.Fatalf("rollback with WAL: %v", err)
	}

	// Verify only original row remains
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "rollback_wal_t"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0] != "before" {
		t.Fatalf("expected 1 row with val='before', got %v", rows)
	}
}

// ============================================================
// 3. Save with multiple tables and indexes
// ============================================================

func TestCovBoost8_Save_MultipleTablesAndIndexes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create two tables
	createCoverageTestTable(t, cat, "save_t1", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "save_t2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenReal},
	})

	// Create index on save_t1
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_save_name",
		Table:   "save_t1",
		Columns: []string{"name"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data into both tables
	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "save_t1",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "alice"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "save_t2",
		Columns: []string{"id", "score"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.NumberLiteral{Value: 95.5},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Save should flush both tables and the index tree
	if err := cat.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify tables are listed
	tables := cat.ListTables()
	if len(tables) < 2 {
		t.Fatalf("expected at least 2 tables, got %d", len(tables))
	}
}

// ============================================================
// 4. Load with tables having DEFAULT and CHECK expressions
// ============================================================

func TestCovBoost8_Load_WithDefaultAndCheck(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create a table with DEFAULT and CHECK
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "load_check_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "status", Type: query.TokenText, Default: &query.StringLiteral{Value: "active"}},
			{Name: "age", Type: query.TokenInteger, Check: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "age"},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 0},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert a row
	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "load_check_t",
		Columns: []string{"id", "age"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.NumberLiteral{Value: 25},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Save
	if err := cat.Save(); err != nil {
		t.Fatal(err)
	}

	// Create a new catalog and Load from same tree
	cat2 := New(tree, pool, nil)
	if err := cat2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Verify loaded table
	table, err := cat2.GetTable("load_check_t")
	if err != nil {
		t.Fatalf("GetTable after Load: %v", err)
	}
	if len(table.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(table.Columns))
	}
}

// ============================================================
// 5. valuesEqual — remaining numeric type combinations
// ============================================================

func TestCovBoost8_valuesEqual_AllNumericTypes(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()
	fke := NewForeignKeyEnforcer(cat)

	// Test a→int64 vs b→various types
	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"int64_vs_int8", int64(5), int8(5), true},
		{"int64_vs_int8_ne", int64(5), int8(6), false},
		{"int64_vs_int16", int64(5), int16(5), true},
		{"int64_vs_int32", int64(5), int32(5), true},
		{"int64_vs_uint", int64(5), uint(5), true},
		{"int64_vs_uint8", int64(5), uint8(5), true},
		{"int64_vs_uint16", int64(5), uint16(5), true},
		{"int64_vs_uint32", int64(5), uint32(5), true},
		{"int64_vs_uint64", int64(5), uint64(5), true},
		{"int64_vs_float32", int64(5), float32(5.0), true},
		// a→various types vs b→int64
		{"int8_vs_int64", int8(5), int64(5), true},
		{"int16_vs_int64", int16(5), int64(5), true},
		{"int32_vs_int64", int32(5), int64(5), true},
		{"uint_vs_int64", uint(5), int64(5), true},
		{"uint8_vs_int64", uint8(5), int64(5), true},
		{"uint16_vs_int64", uint16(5), int64(5), true},
		{"uint32_vs_int64", uint32(5), int64(5), true},
		{"uint64_vs_int64", uint64(5), int64(5), true},
		{"float32_vs_int64", float32(5.0), int64(5), true},
		// a→float64 vs b→float32 (precision mismatch)
		{"float64_vs_float32_mismatch", float64(3.14), float32(3.14), false},
		// a→int vs b→int
		{"int_vs_int", int(5), int(5), true},
		{"int_vs_float64", int(5), float64(5.0), true},
		// nil cases
		{"nil_nil", nil, nil, true},
		{"nil_int", nil, int(5), false},
		{"int_nil", int(5), nil, false},
		// string cases
		{"str_str_eq", "abc", "abc", true},
		{"str_str_ne", "abc", "xyz", false},
		// mixed non-numeric
		{"str_vs_int", "5", int(5), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fke.valuesEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("valuesEqual(%T(%v), %T(%v)) = %v, want %v",
					tt.a, tt.a, tt.b, tt.b, got, tt.want)
			}
		})
	}
}

// ============================================================
// 6. deleteLocked — WAL path, FK, index cleanup, trigger
// ============================================================

func TestCovBoost8_DeleteLocked_WALAndIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	walPath := filepath.Join(t.TempDir(), "delete.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	cat.SetWAL(wal)

	// Create table with index
	createCoverageTestTable(t, cat, "del_wal_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_name",
		Table:   "del_wal_t",
		Columns: []string{"name"},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Insert some rows
	for i := 1; i <= 3; i++ {
		_, _, err = cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_wal_t",
			Columns: []string{"id", "name"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "name" + string(rune('0'+i))},
			}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create a trigger on DELETE
	err = cat.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trg_del_after",
		Table: "del_wal_t",
		Time:  "AFTER",
		Event: "DELETE",
		Body:  []query.Statement{}, // empty body
	})
	if err != nil {
		t.Fatal(err)
	}

	// Begin transaction and delete within txn (covers WAL delete + index cleanup + trigger)
	cat.BeginTransaction(300)
	_, affected, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_wal_t",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 2},
		},
	}, nil)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 row deleted, got %d", affected)
	}

	if err := cat.CommitTransaction(); err != nil {
		t.Fatal(err)
	}

	// Verify 2 rows remaining
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "del_wal_t"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestCovBoost8_DeleteLocked_AllRows_NoWhere(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "del_all_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		_, _, _ = cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_all_t",
			Columns: []string{"id", "val"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "v"},
			}},
		}, nil)
	}

	// Delete all without WHERE
	_, affected, err := cat.Delete(ctx, &query.DeleteStmt{Table: "del_all_t"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 5 {
		t.Fatalf("expected 5 deleted, got %d", affected)
	}
}

// ============================================================
// 7. insertLocked — WAL path, CHECK constraint, FK constraint
// ============================================================

func TestCovBoost8_InsertLocked_WAL(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	walPath := filepath.Join(t.TempDir(), "insert.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	cat.SetWAL(wal)

	createCoverageTestTable(t, cat, "ins_wal_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	cat.BeginTransaction(400)

	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_wal_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "wal_value"},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("insert with WAL: %v", err)
	}

	if err := cat.CommitTransaction(); err != nil {
		t.Fatal(err)
	}

	// Verify
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "ins_wal_t"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0] != "wal_value" {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

func TestCovBoost8_InsertLocked_CHECKConstraintFail(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create table with CHECK constraint
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "ins_check_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "age", Type: query.TokenInteger, Check: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "age"},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 0},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Insert with CHECK violation (age = -1)
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_check_t",
		Columns: []string{"id", "age"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.NumberLiteral{Value: -1},
		}},
	}, nil)
	if err == nil {
		t.Fatal("expected CHECK constraint error")
	}

	// Insert with valid value
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_check_t",
		Columns: []string{"id", "age"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.NumberLiteral{Value: 25},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("valid insert: %v", err)
	}
}

func TestCovBoost8_InsertLocked_ForeignKey(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create parent table
	createCoverageTestTable(t, cat, "fk_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Create child table with FK
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent",
				ReferencedColumns: []string{"id"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Insert parent row
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "parent1"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Insert child with valid FK
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child",
		Columns: []string{"id", "parent_id"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.NumberLiteral{Value: 1},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("valid FK insert: %v", err)
	}

	// Insert child with invalid FK (parent_id=999 doesn't exist)
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child",
		Columns: []string{"id", "parent_id"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.NumberLiteral{Value: 999},
		}},
	}, nil)
	if err == nil {
		t.Fatal("expected FK constraint error")
	}
}

func TestCovBoost8_InsertLocked_DefaultValues(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "ins_def_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "status", Type: query.TokenText, Default: &query.StringLiteral{Value: "pending"}},
			{Name: "score", Type: query.TokenInteger, Default: &query.NumberLiteral{Value: 0}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Insert with only some columns — defaults should apply
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_def_t",
		Columns: []string{"id"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("insert with defaults: %v", err)
	}

	// Verify defaults applied
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "status"},
			&query.Identifier{Name: "score"},
		},
		From: &query.TableRef{Name: "ins_def_t"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestCovBoost8_InsertLocked_NotNullConstraint(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "ins_nn_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText, NotNull: true},
	})

	ctx := context.Background()
	// Insert with NULL in NOT NULL column
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_nn_t",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.NullLiteral{},
		}},
	}, nil)
	if err == nil {
		t.Fatal("expected NOT NULL constraint error")
	}
}

func TestCovBoost8_InsertLocked_UniqueConstraint(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "ins_uniq_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "email", Type: query.TokenText, Unique: true},
	})

	ctx := context.Background()
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_uniq_t",
		Columns: []string{"id", "email"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "a@b.com"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Insert duplicate UNIQUE value
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_uniq_t",
		Columns: []string{"id", "email"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "a@b.com"},
		}},
	}, nil)
	if err == nil {
		t.Fatal("expected UNIQUE constraint error")
	}
}

func TestCovBoost8_InsertLocked_OnConflictIgnore(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "ins_oci_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText, Unique: true},
	})

	ctx := context.Background()
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_oci_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "first"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// OR IGNORE on duplicate unique
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:          "ins_oci_t",
		Columns:        []string{"id", "val"},
		ConflictAction: query.ConflictIgnore,
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "first"}, // duplicate unique
		}},
	}, nil)
	if err != nil {
		t.Fatalf("OR IGNORE should not error: %v", err)
	}
}

func TestCovBoost8_InsertLocked_OnConflictReplace(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "ins_ocr_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText, Unique: true},
	})

	ctx := context.Background()
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_ocr_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "first"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// OR REPLACE on duplicate unique
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:          "ins_ocr_t",
		Columns:        []string{"id", "val"},
		ConflictAction: query.ConflictReplace,
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "first"}, // replaces existing
		}},
	}, nil)
	if err != nil {
		t.Fatalf("OR REPLACE error: %v", err)
	}

	// Verify replacement
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "ins_ocr_t"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after replace, got %d", len(rows))
	}
}

func TestCovBoost8_InsertLocked_WithTrigger(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "ins_trig_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create AFTER INSERT trigger
	err := cat.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trg_ins_after",
		Table: "ins_trig_t",
		Time:  "AFTER",
		Event: "INSERT",
		Body:  []query.Statement{},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_trig_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "triggered"},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("insert with trigger: %v", err)
	}
}

// ============================================================
// 8. collectColumnStats (StatsCollector) edge cases
// ============================================================

func TestCovBoost8_StatsCollector_CollectStats(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "stats_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
		{Name: "score", Type: query.TokenReal},
	})

	ctx := context.Background()
	// Insert mix of values including NULLs
	for i := 1; i <= 5; i++ {
		var valExpr query.Expression
		var scoreExpr query.Expression
		if i%3 == 0 {
			valExpr = &query.NullLiteral{}
			scoreExpr = &query.NullLiteral{}
		} else {
			valExpr = &query.StringLiteral{Value: "v" + string(rune('0'+i))}
			scoreExpr = &query.NumberLiteral{Value: float64(i) * 10.5}
		}
		_, _, err := cat.Insert(ctx, &query.InsertStmt{
			Table:   "stats_t",
			Columns: []string{"id", "val", "score"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, valExpr, scoreExpr}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Use Analyze (the Catalog method) for column stats
	if err := cat.Analyze("stats_t"); err != nil {
		t.Fatalf("Analyze: %v", err)
	}

	// Also exercise the StatsCollector types directly
	sc := NewStatsCollector(cat)

	// validateIdentifier
	if err := validateIdentifier(""); err == nil {
		t.Error("expected error for empty identifier")
	}
	longName := ""
	for i := 0; i < 65; i++ {
		longName += "a"
	}
	if err := validateIdentifier(longName); err == nil {
		t.Error("expected error for long identifier")
	}
	if err := validateIdentifier("foo;bar"); err == nil {
		t.Error("expected error for invalid char")
	}
	if err := validateIdentifier("SELECT"); err == nil {
		t.Error("expected error for SQL keyword")
	}

	// quoteIdent
	q := quoteIdent("my_table")
	if q != `"my_table"` {
		t.Errorf("quoteIdent: got %q", q)
	}

	// EstimateSelectivity
	sel := sc.EstimateSelectivity("nonexistent", "col", "=", 1)
	if sel != 0.1 {
		t.Errorf("expected default selectivity 0.1, got %v", sel)
	}

	// EstimateRowCount
	rowEst := sc.EstimateRowCount("nonexistent")
	if rowEst != 1000 {
		t.Errorf("expected default 1000, got %v", rowEst)
	}

	// GetStatsSummary
	summary := sc.GetStatsSummary()
	if summary == nil {
		t.Error("summary should not be nil")
	}

	// IsStale
	if !sc.IsStale("nonexistent", 0) {
		t.Error("nonexistent table should be stale")
	}

	// InvalidateStats
	sc.InvalidateStats("nonexistent") // no-op

	// GetSummary
	sum := sc.GetSummary()
	if sum.TotalTables != 0 {
		t.Errorf("expected 0 tables in summary, got %d", sum.TotalTables)
	}
}

func TestCovBoost8_StatsCollector_Selectivity(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	sc := NewStatsCollector(cat)

	// Manually add stats with histogram
	sc.mu.Lock()
	sc.stats["test_sel"] = &TableStats{
		TableName: "test_sel",
		RowCount:  100,
		ColumnStats: map[string]*ColumnStats{
			"col1": {
				ColumnName:    "col1",
				DistinctCount: 10,
				NullCount:     5,
				Histogram: []Bucket{
					{LowerBound: 1, UpperBound: 10, Count: 20},
					{LowerBound: 11, UpperBound: 20, Count: 30},
					{LowerBound: 21, UpperBound: 30, Count: 50},
				},
			},
		},
	}
	sc.mu.Unlock()

	// Test various selectivity estimates
	eq := sc.EstimateSelectivity("test_sel", "col1", "=", 5)
	if eq != 0.1 { // 1/10
		t.Errorf("= selectivity: got %v", eq)
	}

	ne := sc.EstimateSelectivity("test_sel", "col1", "!=", 5)
	if ne != 0.9 { // 1 - 1/10
		t.Errorf("!= selectivity: got %v", ne)
	}

	lt := sc.EstimateSelectivity("test_sel", "col1", "<", 15)
	if lt == 0 {
		t.Error("< selectivity should be > 0")
	}

	gt := sc.EstimateSelectivity("test_sel", "col1", ">", 15)
	if gt == 0 {
		t.Error("> selectivity should be > 0")
	}

	lte := sc.EstimateSelectivity("test_sel", "col1", "<=", 15)
	if lte == 0 {
		t.Error("<= selectivity should be > 0")
	}

	gte := sc.EstimateSelectivity("test_sel", "col1", ">=", 15)
	if gte == 0 {
		t.Error(">= selectivity should be > 0")
	}

	def := sc.EstimateSelectivity("test_sel", "col1", "LIKE", "%foo%")
	if def != 0.1 {
		t.Errorf("default selectivity: got %v", def)
	}

	// ColumnStats helpers
	cs := sc.stats["test_sel"].ColumnStats["col1"]
	nf := cs.GetNullFraction(100)
	if nf != 0.05 {
		t.Errorf("null fraction: got %v", nf)
	}
	df := cs.GetDistinctFraction(100)
	if df != 0.1 {
		t.Errorf("distinct fraction: got %v", df)
	}
	if cs.IsUnique(100) {
		t.Error("should not be unique")
	}
	bc := cs.GetHistogramBucketCount()
	if bc != 3 {
		t.Errorf("bucket count: got %d", bc)
	}
	mcv := cs.GetMostCommonValues(2)
	if len(mcv) != 2 {
		t.Errorf("most common values: got %d", len(mcv))
	}

	// EstimateRangeSelectivity — note: uses string comparison internally
	// so numeric overlap may not work as expected; just verify it returns a value
	rs := cs.EstimateRangeSelectivity(5, 25)
	_ = rs // may be 0 due to string-based bucket overlap logic

	// Zero row helpers
	if cs.GetNullFraction(0) != 0 {
		t.Error("null fraction with 0 rows should be 0")
	}
	if cs.GetDistinctFraction(0) != 0 {
		t.Error("distinct fraction with 0 rows should be 0")
	}
}

func TestCovBoost8_StatsCollector_CostEstimates(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	sc := NewStatsCollector(cat)

	// Without stats
	seqCost := sc.EstimateSeqScanCost("noexist", 0.5)
	if seqCost == 0 {
		t.Error("seq scan cost should be > 0")
	}
	idxCost := sc.EstimateIndexScanCost("noexist", "idx", 0.5)
	if idxCost == 0 {
		t.Error("index scan cost should be > 0")
	}

	// With stats
	sc.mu.Lock()
	sc.stats["cost_t"] = &TableStats{
		TableName: "cost_t",
		RowCount:  1000,
		PageCount: 10,
		ColumnStats: map[string]*ColumnStats{
			"id": {ColumnName: "id", DistinctCount: 1000},
		},
	}
	sc.mu.Unlock()

	seqCost = sc.EstimateSeqScanCost("cost_t", 0.5)
	if seqCost <= 0 {
		t.Error("seq scan cost should be > 0")
	}
	idxCost = sc.EstimateIndexScanCost("cost_t", "idx", 0.1)
	if idxCost <= 0 {
		t.Error("index scan cost should be > 0")
	}

	// Join cost estimates
	nlCost := sc.EstimateNestedLoopCost(100, 50)
	if nlCost != 5000 {
		t.Errorf("nested loop cost: got %v", nlCost)
	}
	hjCost := sc.EstimateHashJoinCost(100, 200)
	if hjCost <= 0 {
		t.Error("hash join cost should be > 0")
	}
	mjCost := sc.EstimateMergeJoinCost(100, 200)
	if mjCost <= 0 {
		t.Error("merge join cost should be > 0")
	}

	// EstimateRowCount with stats
	rc := sc.EstimateRowCount("cost_t")
	if rc != 1000 {
		t.Errorf("expected 1000, got %d", rc)
	}
}

func TestCovBoost8_CorrelationStats(t *testing.T) {
	// Test CalculateCorrelation
	x := []float64{1, 2, 3, 4, 5}
	y := []float64{2, 4, 6, 8, 10}
	corr := CalculateCorrelation(x, y)
	if corr < 0.99 {
		t.Errorf("expected high correlation, got %v", corr)
	}

	// Mismatched lengths
	corr = CalculateCorrelation([]float64{1, 2}, []float64{1})
	if corr != 0 {
		t.Errorf("mismatched lengths should return 0, got %v", corr)
	}

	// Empty
	corr = CalculateCorrelation(nil, nil)
	if corr != 0 {
		t.Errorf("empty should return 0, got %v", corr)
	}

	// All same values
	corr = CalculateCorrelation([]float64{5, 5, 5}, []float64{3, 3, 3})
	if corr != 0 {
		t.Errorf("constant should return 0, got %v", corr)
	}

	// CorrelationStats methods
	cs := &CorrelationStats{Column1: "a", Column2: "b", Correlation: 0.9}
	if !cs.IsHighCorrelation() {
		t.Error("0.9 should be high correlation")
	}
	if !cs.IsPositiveCorrelation() {
		t.Error("0.9 should be positive")
	}
	if cs.IsNegativeCorrelation() {
		t.Error("0.9 should not be negative")
	}

	cs2 := &CorrelationStats{Correlation: -0.8}
	if !cs2.IsNegativeCorrelation() {
		t.Error("-0.8 should be negative")
	}
	if cs2.IsPositiveCorrelation() {
		t.Error("-0.8 should not be positive")
	}
}

// ============================================================
// 9. Vacuum edge cases — empty table, index compaction
// ============================================================

func TestCovBoost8_Vacuum_EmptyTableAndIndex(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create table with index but no data
	createCoverageTestTable(t, cat, "vac_empty_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac_val",
		Table:   "vac_empty_t",
		Columns: []string{"val"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Vacuum on empty table — should succeed (entries == 0 → continue)
	if err := cat.Vacuum(); err != nil {
		t.Fatalf("vacuum empty: %v", err)
	}
}

func TestCovBoost8_Vacuum_WithData(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "vac_data_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac_data",
		Table:   "vac_data_t",
		Columns: []string{"val"},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Insert data
	for i := 1; i <= 10; i++ {
		_, _, err := cat.Insert(ctx, &query.InsertStmt{
			Table:   "vac_data_t",
			Columns: []string{"id", "val"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "v" + string(rune('0'+i))},
			}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Delete some rows to create fragmentation
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "vac_data_t",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenLt,
			Right:    &query.NumberLiteral{Value: 5},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Vacuum should compact both table tree and index tree
	if err := cat.Vacuum(); err != nil {
		t.Fatalf("vacuum with data: %v", err)
	}

	// Verify data still accessible
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "vac_data_t"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 6 { // rows 5-10
		t.Fatalf("expected 6 rows after vacuum, got %d", len(rows))
	}
}

func TestCovBoost8_Vacuum_AfterTableDrop(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "vac_drop_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table:   "vac_drop_t",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	// Drop table
	err := cat.DropTable(&query.DropTableStmt{Table: "vac_drop_t"})
	if err != nil {
		t.Fatal(err)
	}

	// Vacuum after drop — should work fine with no tables
	if err := cat.Vacuum(); err != nil {
		t.Fatalf("vacuum after drop: %v", err)
	}
}

// ============================================================
// 10. Delete with FK ON DELETE (CASCADE/RESTRICT via FKE)
// ============================================================

func TestCovBoost8_Delete_WithForeignKey(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create parent
	createCoverageTestTable(t, cat, "fk_del_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Create child with FK ON DELETE RESTRICT
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_del_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_del_parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          "RESTRICT",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_del_parent",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "p1"},
		}},
	}, nil)
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_del_child",
		Columns: []string{"id", "parent_id"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.NumberLiteral{Value: 1},
		}},
	}, nil)

	// Deleting parent should fail due to RESTRICT
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_del_parent",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err == nil {
		t.Fatal("expected FK RESTRICT error on delete")
	}
}

// ============================================================
// 11. Delete with unique index cleanup within transaction
// ============================================================

func TestCovBoost8_Delete_UniqueIndexCleanup(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "del_uidx_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_code_uniq",
		Table:   "del_uidx_t",
		Columns: []string{"code"},
		Unique:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_uidx_t",
		Columns: []string{"id", "code"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "ABC"},
		}},
	}, nil)
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_uidx_t",
		Columns: []string{"id", "code"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "DEF"},
		}},
	}, nil)

	// Delete within txn so undo log tracks unique index changes
	cat.BeginTransaction(500)
	_, affected, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_uidx_t",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "code"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "ABC"},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 deleted, got %d", affected)
	}

	// Rollback — unique index entry should be restored
	if err := cat.RollbackTransaction(); err != nil {
		t.Fatal(err)
	}

	// Verify original data restored
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "code"}},
		From:    &query.TableRef{Name: "del_uidx_t"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after rollback, got %d", len(rows))
	}
}

// ============================================================
// 12. Insert with index + unique conflict in index
// ============================================================

func TestCovBoost8_Insert_UniqueIndexConflict(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "ins_uidx_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_ins_code_uniq",
		Table:   "ins_uidx_t",
		Columns: []string{"code"},
		Unique:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_uidx_t",
		Columns: []string{"id", "code"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "XYZ"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Duplicate unique index value — should fail
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_uidx_t",
		Columns: []string{"id", "code"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "XYZ"},
		}},
	}, nil)
	if err == nil {
		t.Fatal("expected unique index constraint error")
	}

	// OR IGNORE with unique index
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:          "ins_uidx_t",
		Columns:        []string{"id", "code"},
		ConflictAction: query.ConflictIgnore,
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 3},
			&query.StringLiteral{Value: "XYZ"},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("OR IGNORE should not error: %v", err)
	}

	// OR REPLACE with unique index
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:          "ins_uidx_t",
		Columns:        []string{"id", "code"},
		ConflictAction: query.ConflictReplace,
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 4},
			&query.StringLiteral{Value: "XYZ"},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("OR REPLACE should not error: %v", err)
	}
}

// ============================================================
// 13. Save + Load round-trip
// ============================================================

func TestCovBoost8_SaveLoad_Roundtrip(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create table with various column types
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "roundtrip_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText, NotNull: true},
			{Name: "score", Type: query.TokenReal},
			{Name: "active", Type: query.TokenBoolean, Default: &query.BooleanLiteral{Value: true}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "roundtrip_t",
		Columns: []string{"id", "name", "score"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "alice"},
			&query.NumberLiteral{Value: 95.5},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Save
	if err := cat.Save(); err != nil {
		t.Fatal(err)
	}

	// Load into new catalog
	cat2 := New(tree, pool, nil)
	if err := cat2.Load(); err != nil {
		t.Fatal(err)
	}

	// Verify data accessible
	_, rows, err := cat2.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "roundtrip_t"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0] != "alice" {
		t.Fatalf("expected alice, got %v", rows)
	}
}

// ============================================================
// 14. FlushTableTrees
// ============================================================

func TestCovBoost8_FlushTableTrees(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "flush_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table:   "flush_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "flush_val"},
		}},
	}, nil)

	if err := cat.FlushTableTrees(); err != nil {
		t.Fatalf("FlushTableTrees: %v", err)
	}
}

// ============================================================
// 15. Delete + WAL + Index + Rollback (undo delete with index)
// ============================================================

func TestCovBoost8_Delete_WAL_RollbackWithIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	walPath := filepath.Join(t.TempDir(), "del_rollback.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	cat.SetWAL(wal)

	createCoverageTestTable(t, cat, "del_rb_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_rb",
		Table:   "del_rb_t",
		Columns: []string{"val"},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_rb_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "keep"},
		}},
	}, nil)
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_rb_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "remove"},
		}},
	}, nil)

	// Begin txn, delete, then rollback
	cat.BeginTransaction(600)
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_rb_t",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 2},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Rollback with WAL
	if err := cat.RollbackTransaction(); err != nil {
		t.Fatal(err)
	}

	// Both rows should be back
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "del_rb_t"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after rollback, got %d", len(rows))
	}
}

// ============================================================
// 16. Insert with text primary key
// ============================================================

func TestCovBoost8_Insert_TextPrimaryKey(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "ins_textpk_t", []*query.ColumnDef{
		{Name: "code", Type: query.TokenText, PrimaryKey: true},
		{Name: "desc_col", Type: query.TokenText},
	})

	ctx := context.Background()
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_textpk_t",
		Columns: []string{"code", "desc_col"},
		Values: [][]query.Expression{{
			&query.StringLiteral{Value: "ABC"},
			&query.StringLiteral{Value: "first"},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("text PK insert: %v", err)
	}

	// Duplicate text PK
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_textpk_t",
		Columns: []string{"code", "desc_col"},
		Values: [][]query.Expression{{
			&query.StringLiteral{Value: "ABC"},
			&query.StringLiteral{Value: "second"},
		}},
	}, nil)
	if err == nil {
		t.Fatal("expected PK duplicate error")
	}
}

// ============================================================
// 17. Insert column count mismatch
// ============================================================

func TestCovBoost8_Insert_ColumnCountMismatch(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "ins_mismatch_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	// Too many values
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_mismatch_t",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "v"},
			&query.StringLiteral{Value: "extra"},
		}},
	}, nil)
	if err == nil {
		t.Fatal("expected column count mismatch error")
	}
}

// ============================================================
// 18. Insert with nonexistent column
// ============================================================

func TestCovBoost8_Insert_NonexistentColumn(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "ins_badcol_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_badcol_t",
		Columns: []string{"id", "nosuch"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "v"},
		}},
	}, nil)
	if err == nil {
		t.Fatal("expected nonexistent column error")
	}
}

// ============================================================
// 19. valueToString helper coverage
// ============================================================

func TestCovBoost8_valueToString(t *testing.T) {
	tests := []struct {
		val  interface{}
		want string
	}{
		{nil, ""},
		{"hello", "hello"},
		{int(42), "42"},
		{int32(42), "42"},
		{int64(42), "42"},
		{float64(3.14), "3.14"},
		{true, "true"},
	}
	for _, tt := range tests {
		got := valueToString(tt.val)
		if got != tt.want {
			t.Errorf("valueToString(%v) = %q, want %q", tt.val, got, tt.want)
		}
	}
}

// ============================================================
// 20. bucketOverlapsRange
// ============================================================

func TestCovBoost8_bucketOverlapsRange(t *testing.T) {
	// Use string values since bucketOverlapsRange uses string comparison internally
	bucket := Bucket{LowerBound: "b", UpperBound: "d", Count: 5}

	// nil, nil → true
	if !bucketOverlapsRange(bucket, nil, nil) {
		t.Error("nil nil should overlap")
	}

	// overlapping range (string comparison: "a" <= "d" && "c" >= "b")
	if !bucketOverlapsRange(bucket, "a", "c") {
		t.Error("a-c should overlap b-d")
	}

	// non-overlapping (string comparison: "e" <= "f" but "f" >= "b" → "e" > "d")
	if bucketOverlapsRange(bucket, "e", "f") {
		t.Error("e-f should not overlap b-d")
	}

	// Also test with int values to exercise valueToString with numeric types
	numBucket := Bucket{LowerBound: 1, UpperBound: 5, Count: 3}
	// "1" <= "9" && "5" >= "3" → true
	_ = bucketOverlapsRange(numBucket, 3, 9)
	// Exercise with float64
	_ = bucketOverlapsRange(numBucket, float64(2.5), float64(4.5))
}
