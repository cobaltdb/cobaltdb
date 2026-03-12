package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// RollbackToSavepoint: undoAlterRename path
// ============================================================

func TestCovBoost6_RollbackToSavepoint_AlterRename(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_ren_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table: "sp_ren_tbl",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "hello"},
		}},
	}, nil)

	cat.BeginTransaction(200)
	cat.Savepoint("sp_rename")

	// Create an index to test index table-name propagation
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "sp_ren_idx",
		Table:   "sp_ren_tbl",
		Columns: []string{"val"},
	})

	err := cat.AlterTableRename(&query.AlterTableStmt{
		Table:   "sp_ren_tbl",
		Action:  "RENAME",
		NewName: "sp_ren_tbl_new",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify renamed table works
	_, ok := cat.tables["sp_ren_tbl_new"]
	if !ok {
		t.Error("expected renamed table to exist")
	}

	// Rollback to savepoint
	err = cat.RollbackToSavepoint("sp_rename")
	if err != nil {
		t.Fatal(err)
	}

	// Verify old name restored
	_, ok = cat.tables["sp_ren_tbl"]
	if !ok {
		t.Error("expected original table name restored")
	}
	_, ok = cat.tables["sp_ren_tbl_new"]
	if ok {
		t.Error("expected new table name removed")
	}

	cat.CommitTransaction()
}

// ============================================================
// RollbackToSavepoint: undoAlterRenameColumn path
// ============================================================

func TestCovBoost6_RollbackToSavepoint_AlterRenameColumn(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_rencol", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "old_name", Type: query.TokenText},
	})

	// Create an index on the column to test index column reference rollback
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "sp_rencol_idx",
		Table:   "sp_rencol",
		Columns: []string{"old_name"},
	})

	cat.BeginTransaction(201)
	cat.Savepoint("sp_rencol_sp")

	err := cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "sp_rencol",
		Action:  "RENAME COLUMN",
		OldName: "old_name",
		NewName: "new_name",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify column renamed
	tbl := cat.tables["sp_rencol"]
	found := false
	for _, col := range tbl.Columns {
		if col.Name == "new_name" {
			found = true
		}
	}
	if !found {
		t.Error("expected new_name column")
	}

	// Rollback
	err = cat.RollbackToSavepoint("sp_rencol_sp")
	if err != nil {
		t.Fatal(err)
	}

	// Verify old column name restored
	tbl = cat.tables["sp_rencol"]
	found = false
	for _, col := range tbl.Columns {
		if col.Name == "old_name" {
			found = true
		}
	}
	if !found {
		t.Error("expected old_name column restored")
	}

	// Verify index column reference restored
	idx := cat.indexes["sp_rencol_idx"]
	if idx != nil && len(idx.Columns) > 0 && idx.Columns[0] != "old_name" {
		t.Errorf("expected index column 'old_name', got '%s'", idx.Columns[0])
	}

	cat.CommitTransaction()
}

// ============================================================
// RollbackToSavepoint: undoAlterRename with stats
// ============================================================

func TestCovBoost6_RollbackToSavepoint_RenameWithStats(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_ren_stats", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Add stats
	cat.stats["sp_ren_stats"] = &StatsTableStats{TableName: "sp_ren_stats", RowCount: 10}

	cat.BeginTransaction(202)
	cat.Savepoint("sp_stats")

	cat.AlterTableRename(&query.AlterTableStmt{
		Table:   "sp_ren_stats",
		Action:  "RENAME",
		NewName: "sp_ren_stats_new",
	})

	// Rollback
	err := cat.RollbackToSavepoint("sp_stats")
	if err != nil {
		t.Fatal(err)
	}

	// Stats should be back under original name
	if _, ok := cat.stats["sp_ren_stats"]; !ok {
		t.Error("expected stats restored under original name")
	}
	if _, ok := cat.stats["sp_ren_stats_new"]; ok {
		t.Error("expected no stats under new name")
	}

	cat.CommitTransaction()
}

// ============================================================
// RollbackToSavepoint: undoAlterRenameColumn for PK column
// ============================================================

func TestCovBoost6_RollbackToSavepoint_RenamePKColumn(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_renpk", []*query.ColumnDef{
		{Name: "pk_col", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	cat.BeginTransaction(203)
	cat.Savepoint("sp_pk")

	err := cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "sp_renpk",
		Action:  "RENAME COLUMN",
		OldName: "pk_col",
		NewName: "new_pk_col",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Rollback
	err = cat.RollbackToSavepoint("sp_pk")
	if err != nil {
		t.Fatal(err)
	}

	// PK should be restored
	tbl := cat.tables["sp_renpk"]
	if len(tbl.PrimaryKey) > 0 && tbl.PrimaryKey[0] != "pk_col" {
		t.Errorf("expected PK 'pk_col', got '%s'", tbl.PrimaryKey[0])
	}

	cat.CommitTransaction()
}

// ============================================================
// applyRLSFilterInternal with actual policy filtering
// ============================================================

func TestCovBoost6_applyRLSFilterInternal_WithPolicy(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.EnableRLS()

	// Create a policy with expression-based filtering
	cat.rlsManager.CreatePolicy(&security.Policy{
		Name:       "owner_policy",
		TableName:  "rls_test",
		Type:       security.PolicyAll,
		Expression: "owner = current_user",
	})

	ctx := context.WithValue(context.Background(), security.RLSUserKey, "alice")
	cols := []string{"id", "owner", "data"}
	rows := [][]interface{}{
		{1, "alice", "alice_data"},
		{2, "bob", "bob_data"},
		{3, "alice", "alice_data2"},
	}

	filteredCols, filteredRows, err := cat.applyRLSFilterInternal(ctx, "rls_test", cols, rows, "alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(filteredCols) != 3 {
		t.Errorf("expected 3 columns, got %d", len(filteredCols))
	}
	// With expression owner = current_user, alice should see 2 rows
	if len(filteredRows) != 2 {
		t.Errorf("expected 2 rows for alice, got %d", len(filteredRows))
	}

	pool.Close()
}

// ============================================================
// checkRLSFor*Internal with actual policies
// ============================================================

func TestCovBoost6_checkRLSInternal_WithPolicy(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.EnableRLS()

	// Policy using expression: owner = current_user
	cat.rlsManager.CreatePolicy(&security.Policy{
		Name:       "all_ops_policy",
		TableName:  "rls_ops",
		Type:       security.PolicyAll,
		Expression: "owner = current_user",
	})

	ctx := context.WithValue(context.Background(), security.RLSUserKey, "alice")

	// Insert allowed (owner=alice, user=alice)
	ok, err := cat.checkRLSForInsertInternal(ctx, "rls_ops", map[string]interface{}{"owner": "alice"}, "alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected insert allowed")
	}

	// Insert denied (owner=bob, user=alice)
	ok, err = cat.checkRLSForInsertInternal(ctx, "rls_ops", map[string]interface{}{"owner": "bob"}, "alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("expected insert denied")
	}

	// Update allowed
	ok, err = cat.checkRLSForUpdateInternal(ctx, "rls_ops", map[string]interface{}{"owner": "alice"}, "alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected update allowed")
	}

	// Delete allowed
	ok, err = cat.checkRLSForDeleteInternal(ctx, "rls_ops", map[string]interface{}{"owner": "alice"}, "alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected delete allowed")
	}

	pool.Close()
}

// ============================================================
// CreateTable with table-level PK (composite) and FK
// ============================================================

func TestCovBoost6_CreateTable_CompositePK(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "composite_pk",
		Columns: []*query.ColumnDef{
			{Name: "a", Type: query.TokenInteger},
			{Name: "b", Type: query.TokenInteger},
			{Name: "val", Type: query.TokenText},
		},
		PrimaryKey: []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, _ := cat.GetTable("composite_pk")
	if len(tbl.PrimaryKey) != 2 {
		t.Errorf("expected 2 PK columns, got %d", len(tbl.PrimaryKey))
	}
	// Verify columns marked as NOT NULL
	for _, col := range tbl.Columns {
		if col.Name == "a" || col.Name == "b" {
			if !col.NotNull {
				t.Errorf("PK column %s should be NOT NULL", col.Name)
			}
		}
	}
}

func TestCovBoost6_CreateTable_WithFK(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create parent table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
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
				OnDelete:          "CASCADE",
				OnUpdate:          "SET NULL",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, _ := cat.GetTable("fk_child")
	if len(tbl.ForeignKeys) != 1 {
		t.Errorf("expected 1 FK, got %d", len(tbl.ForeignKeys))
	}
	if tbl.ForeignKeys[0].OnDelete != "CASCADE" {
		t.Errorf("expected CASCADE, got %s", tbl.ForeignKeys[0].OnDelete)
	}
}

func TestCovBoost6_CreateTable_InvalidName(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Empty name
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err == nil {
		t.Error("expected error for empty table name")
	}
}

// ============================================================
// Save/Load with indexes
// ============================================================

func TestCovBoost6_SaveLoadWithIndexes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "save_idx",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table: "save_idx",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "test"},
		}},
	}, nil)

	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_save_name",
		Table:   "save_idx",
		Columns: []string{"name"},
	})

	if err := cat.Save(); err != nil {
		t.Fatal(err)
	}

	// Load from same tree
	cat2 := New(tree, pool, nil)
	if err := cat2.Load(); err != nil {
		t.Fatal(err)
	}

	// Verify table loaded
	tables := cat2.ListTables()
	found := false
	for _, tbl := range tables {
		if tbl == "save_idx" {
			found = true
		}
	}
	if !found {
		t.Error("table save_idx not found after Load")
	}

	pool.Close()
}

// ============================================================
// valuesEqual extended types
// ============================================================

func TestCovBoost6_valuesEqual_ExtendedTypes(t *testing.T) {
	fke := &ForeignKeyEnforcer{}

	// uint vs int64
	if !fke.valuesEqual(uint(5), int64(5)) {
		t.Error("uint(5) == int64(5)")
	}

	// uint8 vs float64
	if !fke.valuesEqual(uint8(10), float64(10)) {
		t.Error("uint8(10) == float64(10)")
	}

	// uint16 vs int32
	if !fke.valuesEqual(uint16(100), int32(100)) {
		t.Error("uint16(100) == int32(100)")
	}

	// uint32 vs int
	if !fke.valuesEqual(uint32(1000), int(1000)) {
		t.Error("uint32(1000) == int(1000)")
	}

	// uint64 vs float64
	if !fke.valuesEqual(uint64(50), float64(50)) {
		t.Error("uint64(50) == float64(50)")
	}

	// float32 vs float64
	if !fke.valuesEqual(float32(3.5), float64(3.5)) {
		t.Error("float32(3.5) == float64(3.5)")
	}

	// int8 vs int16
	if !fke.valuesEqual(int8(7), int16(7)) {
		t.Error("int8(7) == int16(7)")
	}

	// String with different case (should be equal via fmt comparison)
	if fke.valuesEqual("abc", "ABC") {
		t.Error("abc != ABC")
	}

	// Different numeric values
	if fke.valuesEqual(int64(5), int64(10)) {
		t.Error("5 != 10")
	}

	// bool values (non-numeric fallback)
	if !fke.valuesEqual(true, true) {
		t.Error("true == true")
	}
	if fke.valuesEqual(true, false) {
		t.Error("true != false")
	}
}

// ============================================================
// ExecuteCTE error: duplicate CTE name
// ============================================================

func TestCovBoost6_ExecuteCTE_DuplicateName(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "cte_dup", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Need 3 CTEs with same name to trigger duplicate detection
	// (originalViews populated on 2nd, checked on 3rd)
	stmt := &query.SelectStmtWithCTE{
		CTEs: []*query.CTEDef{
			{Name: "dup", Query: &query.SelectStmt{
				Columns: []query.Expression{&query.NumberLiteral{Value: 1}},
			}},
			{Name: "dup", Query: &query.SelectStmt{
				Columns: []query.Expression{&query.NumberLiteral{Value: 2}},
			}},
			{Name: "dup", Query: &query.SelectStmt{
				Columns: []query.Expression{&query.NumberLiteral{Value: 3}},
			}},
		},
		Select: &query.SelectStmt{
			Columns: []query.Expression{&query.StarExpr{}},
			From:    &query.TableRef{Name: "dup"},
		},
	}

	_, _, err := cat.ExecuteCTE(stmt, nil)
	if err == nil {
		t.Error("expected error for duplicate CTE name")
	}
	if err != nil && !strings.Contains(err.Error(), "duplicate CTE") {
		t.Errorf("expected 'duplicate CTE' error, got: %v", err)
	}
}

// ============================================================
// ExecuteCTE with UNION CTE (non-recursive)
// ============================================================

func TestCovBoost6_ExecuteCTE_UnionNonRecursive(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "cte_union", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table: "cte_union",
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "v"},
			}},
		}, nil)
	}

	// CTE with UNION query
	stmt := &query.SelectStmtWithCTE{
		CTEs: []*query.CTEDef{
			{
				Name: "combined",
				Query: &query.UnionStmt{
					Left: &query.SelectStmt{
						Columns: []query.Expression{&query.Identifier{Name: "id"}},
						From:    &query.TableRef{Name: "cte_union"},
						Where: &query.BinaryExpr{
							Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq,
							Right: &query.NumberLiteral{Value: 1},
						},
					},
					Right: &query.SelectStmt{
						Columns: []query.Expression{&query.Identifier{Name: "id"}},
						From:    &query.TableRef{Name: "cte_union"},
						Where: &query.BinaryExpr{
							Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq,
							Right: &query.NumberLiteral{Value: 2},
						},
					},
					All: true,
				},
			},
		},
		Select: &query.SelectStmt{
			Columns: []query.Expression{&query.StarExpr{}},
			From:    &query.TableRef{Name: "combined"},
		},
	}

	cols, rows, err := cat.ExecuteCTE(stmt, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) == 0 {
		t.Error("expected columns")
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
}

// ============================================================
// ExecuteCTE: CTE with existing view name (save/restore)
// ============================================================

func TestCovBoost6_ExecuteCTE_OverridesView(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "cte_view_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table: "cte_view_t",
		Values: [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	// Create a view
	cat.CreateView("my_cte", &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "cte_view_t"},
	})

	// Execute CTE with same name - should temporarily override view
	stmt := &query.SelectStmtWithCTE{
		CTEs: []*query.CTEDef{
			{Name: "my_cte", Query: &query.SelectStmt{
				Columns: []query.Expression{&query.NumberLiteral{Value: 42}},
			}},
		},
		Select: &query.SelectStmt{
			Columns: []query.Expression{&query.StarExpr{}},
			From:    &query.TableRef{Name: "my_cte"},
		},
	}

	_, _, err := cat.ExecuteCTE(stmt, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Original view should be restored
	_, ok := cat.views["my_cte"]
	if !ok {
		t.Error("expected view 'my_cte' to be restored after CTE")
	}
}

// ============================================================
// RollbackToSavepoint: combined DML + DDL
// ============================================================

func TestCovBoost6_RollbackToSavepoint_DMLAndDDL(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_combo", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table: "sp_combo",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "original"},
		}},
	}, nil)

	cat.BeginTransaction(300)
	cat.Savepoint("sp_combo_sp")

	// Insert
	cat.Insert(ctx, &query.InsertStmt{
		Table: "sp_combo",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "new"},
		}},
	}, nil)

	// Update
	cat.Update(ctx, &query.UpdateStmt{
		Table: "sp_combo",
		Set: []*query.SetClause{
			{Column: "val", Value: &query.StringLiteral{Value: "modified"}},
		},
		Where: &query.BinaryExpr{
			Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq,
			Right: &query.NumberLiteral{Value: 1},
		},
	}, nil)

	// ALTER ADD COLUMN
	cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "sp_combo",
		Action: "ADD COLUMN",
		Column: query.ColumnDef{Name: "extra", Type: query.TokenText},
	})

	// Rollback everything
	err := cat.RollbackToSavepoint("sp_combo_sp")
	if err != nil {
		t.Fatal(err)
	}

	// Verify: extra column should not exist
	tbl := cat.tables["sp_combo"]
	for _, col := range tbl.Columns {
		if col.Name == "extra" {
			t.Error("extra column should not exist after rollback")
		}
	}

	cat.CommitTransaction()
}

// ============================================================
// RollbackToSavepoint: undoDropIndex (restoration)
// ============================================================

func TestCovBoost6_RollbackToSavepoint_DropIndex(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_didx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "tag", Type: query.TokenText},
	})

	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "sp_didx_tag",
		Table:   "sp_didx",
		Columns: []string{"tag"},
	})

	cat.BeginTransaction(301)
	cat.Savepoint("sp_drop_idx")

	cat.DropIndex("sp_didx_tag")

	// Verify index gone
	_, err := cat.GetIndex("sp_didx_tag")
	if err == nil {
		t.Error("expected index not found after drop")
	}

	// Rollback
	err = cat.RollbackToSavepoint("sp_drop_idx")
	if err != nil {
		t.Fatal(err)
	}

	// Verify index restored
	idx, err := cat.GetIndex("sp_didx_tag")
	if err != nil {
		t.Error("expected index restored after rollback")
	}
	if idx != nil && idx.TableName != "sp_didx" {
		t.Error("expected restored index to reference sp_didx")
	}

	cat.CommitTransaction()
}

// ============================================================
// RollbackToSavepoint: undoAutoIncSeq
// ============================================================

func TestCovBoost6_RollbackToSavepoint_AutoIncSeq(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_autoinc", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(302)
	cat.Savepoint("sp_auto")

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_autoinc",
		Columns: []string{"val"},
		Values:  [][]query.Expression{{&query.StringLiteral{Value: "a"}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_autoinc",
		Columns: []string{"val"},
		Values:  [][]query.Expression{{&query.StringLiteral{Value: "b"}}},
	}, nil)

	// Rollback
	err := cat.RollbackToSavepoint("sp_auto")
	if err != nil {
		t.Fatal(err)
	}

	// AutoIncSeq should be restored to 0
	tbl := cat.tables["sp_autoinc"]
	if tbl.AutoIncSeq != 0 {
		t.Errorf("expected AutoIncSeq 0, got %d", tbl.AutoIncSeq)
	}

	cat.CommitTransaction()
}

// ============================================================
// RollbackToSavepoint: undoDropTable (restoration)
// ============================================================

func TestCovBoost6_RollbackToSavepoint_DropTable(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_drop_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create index too
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "sp_drop_tbl_idx",
		Table:   "sp_drop_tbl",
		Columns: []string{"val"},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table: "sp_drop_tbl",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "keep"},
		}},
	}, nil)

	cat.BeginTransaction(303)
	cat.Savepoint("sp_dtbl")

	cat.DropTable(&query.DropTableStmt{Table: "sp_drop_tbl"})

	// Verify gone
	_, err := cat.GetTable("sp_drop_tbl")
	if err == nil {
		t.Error("expected table not found after drop")
	}

	// Rollback
	err = cat.RollbackToSavepoint("sp_dtbl")
	if err != nil {
		t.Fatal(err)
	}

	// Verify restored
	tbl, err := cat.GetTable("sp_drop_tbl")
	if err != nil {
		t.Fatal("expected table restored")
	}
	if tbl.Name != "sp_drop_tbl" {
		t.Error("wrong table name")
	}

	// Verify index restored
	idx, err := cat.GetIndex("sp_drop_tbl_idx")
	if err != nil {
		t.Error("expected index restored")
	}
	if idx != nil && idx.TableName != "sp_drop_tbl" {
		t.Error("expected index to reference sp_drop_tbl")
	}

	cat.CommitTransaction()
}

// ============================================================
// RollbackToSavepoint: not in transaction
// ============================================================

func TestCovBoost6_RollbackToSavepoint_NoTxn(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	err := cat.RollbackToSavepoint("sp_none")
	if err == nil {
		t.Error("expected error when not in transaction")
	}
}

// ============================================================
// RollbackToSavepoint: nonexistent savepoint
// ============================================================

func TestCovBoost6_RollbackToSavepoint_NotFound(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cat.BeginTransaction(304)
	err := cat.RollbackToSavepoint("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent savepoint")
	}
	cat.CommitTransaction()
}

// ============================================================
// CommitTransaction without WAL
// ============================================================

func TestCovBoost6_CommitTransaction_NoWAL(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cat.BeginTransaction(400)
	err := cat.CommitTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if cat.IsTransactionActive() {
		t.Error("expected transaction inactive after commit")
	}
}

// ============================================================
// RollbackTransaction without WAL
// ============================================================

func TestCovBoost6_RollbackTransaction_NoWAL(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cat.BeginTransaction(401)

	createCoverageTestTable(t, cat, "rb_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	err := cat.RollbackTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if cat.IsTransactionActive() {
		t.Error("expected transaction inactive after rollback")
	}
	// Table should be gone
	_, err = cat.GetTable("rb_test")
	if err == nil {
		t.Error("expected table not found after rollback")
	}
}

// ============================================================
// Vacuum with index trees (ensure index compaction)
// ============================================================

func TestCovBoost6_Vacuum_WithIndexTrees(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "vac_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "tag", Type: query.TokenText},
	})

	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "vac_idx_tag",
		Table:   "vac_idx",
		Columns: []string{"tag"},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table: "vac_idx",
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "t"},
			}},
		}, nil)
	}

	err := cat.Vacuum()
	if err != nil {
		t.Fatal(err)
	}

	// Verify data still accessible
	cols, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "vac_idx"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 || len(rows) != 5 {
		t.Errorf("expected 2 cols, 5 rows; got %d cols, %d rows", len(cols), len(rows))
	}
}
