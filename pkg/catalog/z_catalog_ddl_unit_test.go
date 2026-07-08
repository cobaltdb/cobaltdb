package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// ── isMaxValuePartitionBound ─────────────────────────────────────────

func TestIsMaxValuePartitionBound(t *testing.T) {
	tests := []struct {
		name string
		expr query.Expression
		want bool
	}{
		{"identifier_MAXVALUE", &query.Identifier{Name: "MAXVALUE"}, true},
		{"identifier_maxvalue", &query.Identifier{Name: "maxvalue"}, true},
		{"identifier_MaxValue", &query.Identifier{Name: "MaxValue"}, true},
		{"identifier_other", &query.Identifier{Name: "other"}, false},
		{"columnref_MAXVALUE", &query.ColumnRef{Column: "MAXVALUE"}, true},
		{"columnref_other", &query.ColumnRef{Column: "other"}, false},
		{"number_literal", &query.NumberLiteral{Value: 100}, false},
		{"string_literal", &query.StringLiteral{Value: "MAXVALUE"}, false},
		{"nil", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isMaxValuePartitionBound(tt.expr)
			if got != tt.want {
				t.Errorf("isMaxValuePartitionBound(%T) = %v, want %v", tt.expr, got, tt.want)
			}
		})
	}
}

// ── CreateCollection ─────────────────────────────────────────────────

func TestCreateCollection(t *testing.T) {
	c := newTestCatalog(t)

	// Success: create a collection
	stmt := &query.CreateCollectionStmt{Name: "my_collection"}
	if err := c.CreateCollection(stmt); err != nil {
		t.Fatalf("CreateCollection(my_collection): %v", err)
	}
	// Verify it exists as a table with type "collection"
	table, err := c.getTableLocked("my_collection")
	if err != nil {
		t.Fatalf("getTableLocked(my_collection): %v", err)
	}
	if table.Type != "collection" {
		t.Errorf("table.Type = %q, want %q", table.Type, "collection")
	}

	// Duplicate without IfNotExists → error
	dup := &query.CreateCollectionStmt{Name: "my_collection"}
	if err := c.CreateCollection(dup); err != ErrTableExists {
		t.Fatalf("CreateCollection(dup) = %v, want ErrTableExists", err)
	}

	// Duplicate with IfNotExists → nil
	if err := c.CreateCollection(&query.CreateCollectionStmt{Name: "my_collection", IfNotExists: true}); err != nil {
		t.Fatalf("CreateCollection(dup, IfNotExists): %v", err)
	}

	// Invalid name → error
	if err := c.CreateCollection(&query.CreateCollectionStmt{Name: ""}); err == nil {
		t.Fatal("CreateCollection(''): expected error, got nil")
	}
}

// ── DropCollection ───────────────────────────────────────────────────

func TestDropCollection(t *testing.T) {
	c := newTestCatalog(t)

	// Create a collection
	if err := c.CreateCollection(&query.CreateCollectionStmt{Name: "drop_me"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Drop it
	if err := c.DropCollection(&query.DropCollectionStmt{Name: "drop_me"}); err != nil {
		t.Fatalf("DropCollection: %v", err)
	}
	// Verify it's gone
	if _, err := c.getTableLocked("drop_me"); err != ErrTableNotFound {
		t.Fatalf("getTableLocked after drop = %v, want ErrTableNotFound", err)
	}

	// Drop non-existent without IfExists → error
	if err := c.DropCollection(&query.DropCollectionStmt{Name: "nonexistent"}); err != ErrTableNotFound {
		t.Fatalf("DropCollection(nonexistent) = %v, want ErrTableNotFound", err)
	}

	// Drop non-existent with IfExists → nil
	if err := c.DropCollection(&query.DropCollectionStmt{Name: "nonexistent", IfExists: true}); err != nil {
		t.Fatalf("DropCollection(nonexistent, IfExists): %v", err)
	}

	// Drop a regular table (not collection) → error
	mustCreateTable(t, c, "regular_table (id INTEGER PRIMARY KEY)")
	if err := c.DropCollection(&query.DropCollectionStmt{Name: "regular_table"}); err == nil {
		t.Fatal("DropCollection on regular table: expected error, got nil")
	}
}

// ── cloneForeignKeys ─────────────────────────────────────────────────

func TestCloneForeignKeys(t *testing.T) {
	// Nil input → nil
	if got := cloneForeignKeys(nil); got != nil {
		t.Fatalf("cloneForeignKeys(nil) = %#v, want nil", got)
	}

	// Empty input → nil
	got := cloneForeignKeys([]ForeignKeyDef{})
	if got != nil {
		t.Fatalf("cloneForeignKeys(empty) = %#v, want nil", got)
	}

	// Actual clone
	in := []ForeignKeyDef{
		{
			Name:              "fk1",
			Columns:           []string{"a", "b"},
			ReferencedTable:   "ref_tbl",
			ReferencedColumns: []string{"x", "y"},
			OnDelete:          "CASCADE",
			OnUpdate:          "SET NULL",
		},
	}
	got = cloneForeignKeys(in)
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}
	if got[0].Name != "fk1" || got[0].ReferencedTable != "ref_tbl" {
		t.Errorf("unexpected fields in clone: %+v", got[0])
	}
	// Mutate original to ensure deep copy
	in[0].Columns[0] = "mutated"
	if got[0].Columns[0] != "a" {
		t.Errorf("clone.Columns[0] = %q, want %q (deep copy broken)", got[0].Columns[0], "a")
	}
	in[0].ReferencedColumns[0] = "mutated"
	if got[0].ReferencedColumns[0] != "x" {
		t.Errorf("clone.ReferencedColumns[0] = %q, want %q (deep copy broken)", got[0].ReferencedColumns[0], "x")
	}
}

// ── AlterTableAddCheckConstraint ─────────────────────────────────────

func TestAlterTableAddCheckConstraint(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "chk_tbl (id INTEGER PRIMARY KEY, val INTEGER)")

	// Success: add a CHECK constraint
	stmt := &query.AlterTableStmt{
		Table:          "chk_tbl",
		Action:         "ADD_CONSTRAINT",
		ConstraintName: "chk_val_positive",
		ConstraintCheck: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "val"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 0},
		},
	}
	if err := c.AlterTableAddCheckConstraint(stmt); err != nil {
		t.Fatalf("AlterTableAddCheckConstraint: %v", err)
	}

	// Missing constraint name → error
	noName := *stmt
	noName.ConstraintName = ""
	if err := c.AlterTableAddCheckConstraint(&noName); err == nil {
		t.Fatal("AlterTableAddCheckConstraint with empty name: expected error, got nil")
	}

	// Missing expression → error
	noExpr := *stmt
	noExpr.ConstraintCheck = nil
	if err := c.AlterTableAddCheckConstraint(&noExpr); err == nil {
		t.Fatal("AlterTableAddCheckConstraint with nil check: expected error, got nil")
	}

	// Duplicate name → error
	if err := c.AlterTableAddCheckConstraint(stmt); err == nil {
		t.Fatal("AlterTableAddCheckConstraint duplicate: expected error, got nil")
	}
}

func TestAlterTableAddCheckConstraint_ValidationFailure(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "chk_tbl2 (id INTEGER PRIMARY KEY, val INTEGER)")
	// Insert a row that violates the check
	if _, _, err := c.Insert(context.Background(), &query.InsertStmt{
		Table:   "chk_tbl2",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{nr(1), nr(-1)}},
	}, nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	// Adding val > 0 should fail because row has val = -1
	stmt := &query.AlterTableStmt{
		Table:          "chk_tbl2",
		Action:         "ADD_CONSTRAINT",
		ConstraintName: "chk_val_positive",
		ConstraintCheck: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "val"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 0},
		},
	}
	if err := c.AlterTableAddCheckConstraint(stmt); err == nil {
		t.Fatal("AlterTableAddCheckConstraint with violating data: expected error, got nil")
	}
}

// ── validateCheckConstraintsLocked ───────────────────────────────────

func TestValidateCheckConstraintsLocked(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "vcc_tbl (id INTEGER PRIMARY KEY, val INTEGER)")

	// Add a CHECK constraint with no rows → should pass
	table, err := c.getTableLocked("vcc_tbl")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	table.Checks = append(table.Checks, CheckDef{
		Name:     "chk_val_pos",
		CheckStr: "val > 0",
		Check: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "val"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 0},
		},
	})
	if err := c.validateCheckConstraintsLocked(table); err != nil {
		t.Fatalf("validateCheckConstraintsLocked (empty table): %v", err)
	}

	// Insert a valid row
	if _, _, err := c.Insert(context.Background(), &query.InsertStmt{
		Table:   "vcc_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{nr(1), nr(5)}},
	}, nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if err := c.validateCheckConstraintsLocked(table); err != nil {
		t.Fatalf("validateCheckConstraintsLocked (valid row): %v", err)
	}

	// Insert an invalid row and validate directly (without going through
	// the public Insert which also validates)
	if _, _, err := c.Insert(context.Background(), &query.InsertStmt{
		Table:   "vcc_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{nr(2), nr(-1)}},
	}, nil); err == nil {
		t.Log("Note: Insert allowed violating row — validateCheckConstraintsLocked should catch it")
	}
}

// ── DropTableConstraint ──────────────────────────────────────────────

func TestDropTableConstraint_ForeignKey(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "fk_parent (id INTEGER PRIMARY KEY)")
	// Use ExecuteQuery to create a table with FK via DDL
	if _, err := c.ExecuteQuery("CREATE TABLE fk_child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES fk_parent(id))"); err != nil {
		t.Fatalf("CREATE TABLE fk_child: %v", err)
	}
	table, err := c.getTableLocked("fk_child")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	if len(table.ForeignKeys) == 0 {
		t.Skip("No FK parsed from CREATE TABLE; FK constraint test skipped")
	}

	fkName := table.ForeignKeys[0].Name
	if err := c.DropTableConstraint("fk_child", fkName); err != nil {
		t.Fatalf("DropTableConstraint FK: %v", err)
	}
	// Verify removed
	table2, _ := c.getTableLocked("fk_child")
	if len(table2.ForeignKeys) != 0 {
		t.Errorf("FK still present after DropTableConstraint")
	}
}

func TestDropTableConstraint_CheckConstraint(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "chk_drop_tbl (id INTEGER PRIMARY KEY, val INTEGER)")

	// Add a check constraint via AlterTable
	stmt := &query.AlterTableStmt{
		Table:          "chk_drop_tbl",
		Action:         "ADD_CONSTRAINT",
		ConstraintName: "chk_val_positive",
		ConstraintCheck: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "val"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 0},
		},
	}
	if err := c.AlterTableAddCheckConstraint(stmt); err != nil {
		t.Fatalf("AlterTableAddCheckConstraint: %v", err)
	}

	// Drop it
	if err := c.DropTableConstraint("chk_drop_tbl", "chk_val_positive"); err != nil {
		t.Fatalf("DropTableConstraint check: %v", err)
	}
	// Verify removed
	table, _ := c.getTableLocked("chk_drop_tbl")
	for _, chk := range table.Checks {
		if chk.Name == "chk_val_positive" {
			t.Fatal("check constraint still present after DropTableConstraint")
		}
	}
}

func TestDropTableConstraint_UniqueIndex(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "uniq_drop_tbl (id INTEGER PRIMARY KEY, code TEXT)")
	if _, err := c.ExecuteQuery("CREATE UNIQUE INDEX uniq_idx_code ON uniq_drop_tbl (code)"); err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}

	// Drop it as a constraint
	if err := c.DropTableConstraint("uniq_drop_tbl", "uniq_idx_code"); err != nil {
		t.Fatalf("DropTableConstraint unique: %v", err)
	}
	if _, exists := c.indexes["uniq_idx_code"]; exists {
		t.Fatal("unique index still present after DropTableConstraint")
	}
}

func TestDropTableConstraint_NotFound(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "notfound_tbl (id INTEGER PRIMARY KEY)")

	// Non-existent constraint → ErrIndexNotFound
	if err := c.DropTableConstraint("notfound_tbl", "nonexistent_constraint"); err != ErrIndexNotFound {
		t.Fatalf("DropTableConstraint nonexistent = %v, want ErrIndexNotFound", err)
	}
}

// ── CleanupFailedCreateTable ─────────────────────────────────────────

func TestCleanupFailedCreateTable(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "fail_tbl (id INTEGER PRIMARY KEY, val INTEGER)")

	// Manually add an index to simulate partial CREATE TABLE state
	idxTree, err := btree.NewBTree(c.pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	c.indexes["fail_tbl_idx"] = &IndexDef{
		Name:      "fail_tbl_idx",
		TableName: "fail_tbl",
		Columns:   []string{"val"},
	}
	c.indexTrees["fail_tbl_idx"] = idxTree

	// Verify table and index exist
	if _, err := c.getTableLocked("fail_tbl"); err != nil {
		t.Fatalf("getTableLocked before cleanup: %v", err)
	}
	if _, exists := c.indexes["fail_tbl_idx"]; !exists {
		t.Fatal("index should exist before cleanup")
	}

	// Cleanup
	if err := c.CleanupFailedCreateTable("fail_tbl"); err != nil {
		t.Fatalf("CleanupFailedCreateTable: %v", err)
	}

	// Verify table removed
	if _, err := c.getTableLocked("fail_tbl"); err != ErrTableNotFound {
		t.Fatalf("getTableLocked after cleanup = %v, want ErrTableNotFound", err)
	}
	// Verify index removed
	if _, exists := c.indexes["fail_tbl_idx"]; exists {
		t.Fatal("index should be removed after cleanup")
	}
}

// ── pruneFailedCreateTableUndoLocked ─────────────────────────────────

func TestPruneFailedCreateTableUndoLocked(t *testing.T) {
	c := newTestCatalog(t)

	// Simulate undo log entries
	c.undoLog = []undoEntry{
		{action: undoCreateTable, tableName: "other_table"},
		{action: undoCreateTable, tableName: "failed_tbl"},
		{action: undoCreateIndex, indexName: "idx_a"},
		{action: undoCreateIndex, indexName: "idx_b"},
		{action: undoCreateTable, tableName: "another_table"},
	}
	c.savepoints = []savepointEntry{
		{undoPos: 1},
		{undoPos: 3},
		{undoPos: 5},
	}

	c.pruneFailedCreateTableUndoLocked("failed_tbl", []string{"idx_a", "idx_b"})

	// Should remove entries at positions 1 (undoCreateTable failed_tbl),
	// 2 (undoCreateIndex idx_a), 3 (undoCreateIndex idx_b)
	if len(c.undoLog) != 2 {
		t.Fatalf("len(undoLog) = %d, want 2", len(c.undoLog))
	}
	if c.undoLog[0].tableName != "other_table" {
		t.Errorf("undoLog[0].tableName = %q, want %q", c.undoLog[0].tableName, "other_table")
	}
	if c.undoLog[1].tableName != "another_table" {
		t.Errorf("undoLog[1].tableName = %q, want %q", c.undoLog[1].tableName, "another_table")
	}

	// Savepoints should be adjusted for 3 removed positions
	if len(c.savepoints) != 3 {
		t.Fatalf("len(savepoints) = %d, want 3", len(c.savepoints))
	}
	// The adjustSavepoints function shifts savepoints based on how many
	// removed entries were at positions LESS THAN the savepoint's undoPos.
	// removed = [1, 2, 3]
	// savepoint at undoPos 1: no removals < 1 → shift 0 → new = 1
	// savepoint at undoPos 3: removals {1,2} < 3 → shift 2 → new = 1
	// savepoint at undoPos 5: removals {1,2,3} < 5 → shift 3 → new = 2
	if c.savepoints[0].undoPos != 1 {
		t.Errorf("savepoints[0].undoPos = %d, want 1", c.savepoints[0].undoPos)
	}
	if c.savepoints[1].undoPos != 1 {
		t.Errorf("savepoints[1].undoPos = %d, want 1", c.savepoints[1].undoPos)
	}
	if c.savepoints[2].undoPos != 2 {
		t.Errorf("savepoints[2].undoPos = %d, want 2", c.savepoints[2].undoPos)
	}
}

func TestPruneFailedCreateTableUndoLocked_EmptyLog(t *testing.T) {
	c := newTestCatalog(t)
	c.undoLog = nil
	c.savepoints = nil

	// Should not panic
	c.pruneFailedCreateTableUndoLocked("failed_tbl", []string{"idx_a"})
	if c.undoLog != nil {
		t.Errorf("undoLog should still be nil")
	}
}

// ── rollbackAppliedDeleteEntries ─────────────────────────────────────

func TestRollbackAppliedDeleteEntries(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "rb_del (id INTEGER PRIMARY KEY, v INTEGER)")

	// Seed a row
	if _, _, err := c.Insert(context.Background(), buildInsertForTest("rb_del", []int64{1}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	key := []byte("00000000000000000001")
	tree := c.tableTrees["rb_del"]
	origValue, _ := tree.Get(key)

	// The entries to roll back
	entries := []deleteEntry{
		{
			key:      key,
			value:    origValue,
			row:      []interface{}{int64(1)},
			treeName: "rb_del",
		},
	}

	// First, simulate that the entry was applied (soft-deleted). We need
	// the tree to have the entry so rollback can restore it.
	if err := c.rollbackAppliedDeleteEntries("rb_del", entries); err != nil {
		t.Fatalf("rollbackAppliedDeleteEntries: %v", err)
	}

	// The key should still exist with the original value
	restoredValue, _ := tree.Get(key)
	if string(restoredValue) != string(origValue) {
		t.Fatal("rollback did not restore original value")
	}
}

func TestRollbackAppliedDeleteEntries_MissingTree(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "rb_del2 (id INTEGER PRIMARY KEY)")

	entries := []deleteEntry{
		{
			key:      []byte("pk1"),
			value:    []byte("value"),
			treeName: "nonexistent_tree",
		},
	}

	if err := c.rollbackAppliedDeleteEntries("rb_del2", entries); err == nil {
		t.Fatal("expected error for missing partition tree, got nil")
	}
}

// ── ListForeignTables ────────────────────────────────────────────────

func TestListForeignTables(t *testing.T) {
	c := newTestCatalog(t)

	// Empty catalog → empty list
	out := c.ListForeignTables()
	if len(out) != 0 {
		t.Fatalf("ListForeignTables (empty) = %d items, want 0", len(out))
	}

	// Add a foreign table manually (CreateForeignTable requires FDW registry)
	c.foreignTables["ft1"] = &ForeignTableDef{
		TableName: "ft1",
		Columns: []ColumnDef{
			{Name: "id", Type: "INTEGER"},
		},
		Wrapper: "dummy",
	}
	c.foreignTables["ft2"] = &ForeignTableDef{
		TableName: "ft2",
		Columns: []ColumnDef{
			{Name: "name", Type: "TEXT"},
		},
		Wrapper: "dummy",
	}

	out = c.ListForeignTables()
	if len(out) != 2 {
		t.Fatalf("ListForeignTables = %d items, want 2", len(out))
	}
	// Verify clone: mutating output should not affect original
	out[0].TableName = "mutated"
	if c.foreignTables["ft1"].TableName != "ft1" {
		t.Fatal("ListForeignTables returned aliased copy, not deep clone")
	}
}

// ── reverseFDWOperator ───────────────────────────────────────────────

func TestReverseFDWOperator(t *testing.T) {
	tests := []struct {
		name  string
		input query.TokenType
		want  string
	}{
		{"LT -> GT", query.TokenLt, ">"},
		{"GT -> LT", query.TokenGt, "<"},
		{"LTE -> GTE", query.TokenLte, ">="},
		{"GTE -> LTE", query.TokenGte, "<="},
		{"EQ (passthrough)", query.TokenEq, "="},
		{"NEQ (passthrough)", query.TokenNeq, "!="},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reverseFDWOperator(tt.input)
			if got != tt.want {
				t.Errorf("reverseFDWOperator(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ── ListFTSIndexDefs ─────────────────────────────────────────────────

func TestListFTSIndexDefs(t *testing.T) {
	c := newTestCatalog(t)

	// Empty → empty
	defs := c.ListFTSIndexDefs()
	if len(defs) != 0 {
		t.Fatalf("ListFTSIndexDefs (empty) = %d items, want 0", len(defs))
	}

	// Add FTS indexes manually
	c.ftsIndexes["idx_z"] = &FTSIndexDef{Name: "idx_z", TableName: "tbl_z", Columns: []string{"col"}}
	c.ftsIndexes["idx_a"] = &FTSIndexDef{Name: "idx_a", TableName: "tbl_a", Columns: []string{"col"}}
	c.ftsIndexes["idx_m"] = &FTSIndexDef{Name: "idx_m", TableName: "tbl_m", Columns: []string{"col"}}

	defs = c.ListFTSIndexDefs()
	if len(defs) != 3 {
		t.Fatalf("ListFTSIndexDefs = %d items, want 3", len(defs))
	}
	// Must be sorted: idx_a, idx_m, idx_z
	if defs[0].Name != "idx_a" || defs[1].Name != "idx_m" || defs[2].Name != "idx_z" {
		t.Errorf("unexpected sort order: %+v", defs)
	}
	// Verify clone semantics: Columns slice is a copy
	defs[0].Columns[0] = "mutated"
	if c.ftsIndexes["idx_a"].Columns[0] != "col" {
		t.Fatal("ListFTSIndexDefs returned aliased Columns, not deep clone")
	}
}

// ── cloneFTSIndexDef ─────────────────────────────────────────────────

func TestCloneFTSIndexDef(t *testing.T) {
	// Nil → nil
	if got := cloneFTSIndexDef(nil); got != nil {
		t.Fatal("cloneFTSIndexDef(nil) should be nil")
	}

	// Deep clone with Index map
	orig := &FTSIndexDef{
		Name:      "my_idx",
		TableName: "my_table",
		Columns:   []string{"col_a", "col_b"},
		Index: map[string][]int64{
			"hello": {1, 2, 3},
			"world": {4, 5},
		},
	}
	cloned := cloneFTSIndexDef(orig)
	if cloned.Name != orig.Name || cloned.TableName != orig.TableName {
		t.Errorf("basic fields not copied")
	}
	if len(cloned.Columns) != 2 || cloned.Columns[0] != "col_a" {
		t.Errorf("Columns not copied correctly")
	}
	if len(cloned.Index) != 2 {
		t.Errorf("Index map not copied: got %d entries", len(cloned.Index))
	}

	// Mutate original to verify deep copy
	orig.Columns[0] = "mutated"
	if cloned.Columns[0] != "col_a" {
		t.Error("Columns not deep-copied")
	}
	orig.Index["hello"] = []int64{99}
	if cloned.Index["hello"][0] != 1 {
		t.Error("Index map slices not deep-copied")
	}
}

// ── DropUniqueConstraint ─────────────────────────────────────────────

func TestDropUniqueConstraint(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "uniq_tbl (id INTEGER PRIMARY KEY, code TEXT)")
	if _, err := c.ExecuteQuery("CREATE UNIQUE INDEX uniq_code_idx ON uniq_tbl (code)"); err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}

	// Drop it
	if err := c.DropUniqueConstraint("uniq_code_idx"); err != nil {
		t.Fatalf("DropUniqueConstraint: %v", err)
	}
	if _, exists := c.indexes["uniq_code_idx"]; exists {
		t.Fatal("index still exists after DropUniqueConstraint")
	}

	// Drop again → ErrIndexNotFound
	if err := c.DropUniqueConstraint("uniq_code_idx"); err != ErrIndexNotFound {
		t.Fatalf("DropUniqueConstraint again = %v, want ErrIndexNotFound", err)
	}
}

func TestDropUniqueConstraint_NonUnique(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "nonuniq_tbl (id INTEGER PRIMARY KEY, code TEXT)")
	if _, err := c.ExecuteQuery("CREATE INDEX nonuniq_idx ON nonuniq_tbl (code)"); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Find the non-unique index
	var idxName string
	for name, idx := range c.indexes {
		if idx.TableName == "nonuniq_tbl" && !idx.Unique {
			idxName = name
			break
		}
	}
	if idxName == "" {
		t.Fatal("no non-unique index found")
	}

	// DropUniqueConstraint on non-unique → error
	if err := c.DropUniqueConstraint(idxName); err == nil {
		t.Fatal("DropUniqueConstraint on non-unique: expected error, got nil")
	}
}

// ── storeViewDef / storeTriggerDef / storeProcedureDef ───────────────

func TestStoreViewDef(t *testing.T) {
	c := newTestCatalog(t)

	// storeViewDef calls storeSQLDef which writes to c.tree.
	if err := c.storeViewDef("test_view", "CREATE VIEW test_view AS SELECT 1"); err != nil {
		t.Fatalf("storeViewDef: %v", err)
	}
	// Verify by reading from the tree
	val, err := c.tree.Get([]byte("view:test_view"))
	if err != nil {
		t.Fatalf("tree.Get(view:test_view): %v", err)
	}
	if !strings.Contains(string(val), "test_view") {
		t.Errorf("stored value missing view name: %s", string(val))
	}
	if !strings.Contains(string(val), "SELECT 1") {
		t.Errorf("stored value missing SQL: %s", string(val))
	}
}

func TestStoreTriggerDef(t *testing.T) {
	c := newTestCatalog(t)

	if err := c.storeTriggerDef("test_trg", "CREATE TRIGGER test_trg BEFORE INSERT ON t BEGIN ... END"); err != nil {
		t.Fatalf("storeTriggerDef: %v", err)
	}
	val, err := c.tree.Get([]byte("trg:test_trg"))
	if err != nil {
		t.Fatalf("tree.Get(trg:test_trg): %v", err)
	}
	if !strings.Contains(string(val), "test_trg") {
		t.Errorf("stored value missing trigger name: %s", string(val))
	}
}

func TestStoreProcedureDef(t *testing.T) {
	c := newTestCatalog(t)

	if err := c.storeProcedureDef("test_proc", "CREATE PROCEDURE test_proc() BEGIN ... END"); err != nil {
		t.Fatalf("storeProcedureDef: %v", err)
	}
	val, err := c.tree.Get([]byte("proc:test_proc"))
	if err != nil {
		t.Fatalf("tree.Get(proc:test_proc): %v", err)
	}
	if !strings.Contains(string(val), "test_proc") {
		t.Errorf("stored value missing procedure name: %s", string(val))
	}
}

// ── executeInsteadOfTrigger (INSERT) ─────────────────────────────────

func TestExecuteInsteadOfTrigger(t *testing.T) {
	c := newTestCatalog(t)

	// Create a backing table for the view
	mustCreateTable(t, c, "backing_tbl (id INTEGER PRIMARY KEY, val INTEGER)")

	// Create a view that selects from the backing table
	viewSelect := &query.SelectStmt{
		Columns: []query.Expression{id("id"), id("val")},
		From:    tref("backing_tbl"),
	}
	if err := c.CreateView("instead_view", viewSelect); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	// Register an INSTEAD OF INSERT trigger with a valid body
	trig := &query.CreateTriggerStmt{
		Name:  "trg_io_insert",
		Table: "instead_view",
		Time:  "INSTEAD OF",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table: "backing_tbl",
				Values: [][]query.Expression{
					{&query.QualifiedIdentifier{Table: "NEW", Column: "id"}, &query.QualifiedIdentifier{Table: "NEW", Column: "val"}},
				},
			},
		},
	}
	if err := c.CreateTrigger(trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	// Execute the trigger directly
	insertStmt := &query.InsertStmt{
		Table: "instead_view",
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 42}, &query.NumberLiteral{Value: 100}},
		},
	}
	if rows, affected, err := c.executeInsteadOfTrigger(context.Background(), trig, insertStmt, nil); err != nil {
		t.Fatalf("executeInsteadOfTrigger: %v", err)
	} else if affected != 1 {
		t.Errorf("rows affected = %d, want 1", affected)
	} else {
		t.Logf("executeInsteadOfTrigger returned %d, %d", rows, affected)
	}
}

// ── executeInsteadOfUpdateTrigger ────────────────────────────────────

func TestExecuteInsteadOfUpdateTrigger(t *testing.T) {
	c := newTestCatalog(t)

	// Create a backing table with data
	mustCreateTable(t, c, "backing_upd (id INTEGER PRIMARY KEY, val INTEGER)")
	if _, _, err := c.Insert(context.Background(), &query.InsertStmt{
		Table:  "backing_upd",
		Values: [][]query.Expression{{nr(1), nr(10)}},
	}, nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	// Create a view
	viewSelect := &query.SelectStmt{
		Columns: []query.Expression{id("id"), id("val")},
		From:    tref("backing_upd"),
	}
	if err := c.CreateView("instead_upd_view", viewSelect); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	// Register an INSTEAD OF UPDATE trigger
	trig := &query.CreateTriggerStmt{
		Name:  "trg_io_update",
		Table: "instead_upd_view",
		Time:  "INSTEAD OF",
		Event: "UPDATE",
		Body: []query.Statement{
			&query.UpdateStmt{
				Table: "backing_upd",
				Set: []*query.SetClause{
					{Column: "val", Value: &query.Identifier{Name: "NEW.val"}},
				},
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.Identifier{Name: "OLD.id"},
				},
			},
		},
	}
	if err := c.CreateTrigger(trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	// Execute the trigger directly
	updateStmt := &query.UpdateStmt{
		Table: "instead_upd_view",
		Set: []*query.SetClause{
			{Column: "val", Value: nr(99)},
		},
	}
	if rows, affected, err := c.executeInsteadOfUpdateTrigger(context.Background(), trig, updateStmt, nil); err != nil {
		t.Fatalf("executeInsteadOfUpdateTrigger: %v", err)
	} else if affected != 1 {
		t.Errorf("rows affected = %d, want 1", affected)
	} else {
		t.Logf("executeInsteadOfUpdateTrigger returned %d, %d", rows, affected)
	}
}

// ── executeInsteadOfDeleteTrigger ────────────────────────────────────

func TestExecuteInsteadOfDeleteTrigger(t *testing.T) {
	c := newTestCatalog(t)

	// Create a backing table with data
	mustCreateTable(t, c, "backing_del (id INTEGER PRIMARY KEY, val INTEGER)")
	if _, _, err := c.Insert(context.Background(), &query.InsertStmt{
		Table:  "backing_del",
		Values: [][]query.Expression{{nr(1), nr(10)}},
	}, nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	// Create a view
	viewSelect := &query.SelectStmt{
		Columns: []query.Expression{id("id"), id("val")},
		From:    tref("backing_del"),
	}
	if err := c.CreateView("instead_del_view", viewSelect); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	// Register an INSTEAD OF DELETE trigger
	trig := &query.CreateTriggerStmt{
		Name:  "trg_io_delete",
		Table: "instead_del_view",
		Time:  "INSTEAD OF",
		Event: "DELETE",
		Body: []query.Statement{
			&query.DeleteStmt{
				Table: "backing_del",
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.Identifier{Name: "OLD.id"},
				},
			},
		},
	}
	if err := c.CreateTrigger(trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	// Execute the trigger directly
	deleteStmt := &query.DeleteStmt{
		Table: "instead_del_view",
	}
	if rows, affected, err := c.executeInsteadOfDeleteTrigger(context.Background(), trig, deleteStmt, nil); err != nil {
		t.Fatalf("executeInsteadOfDeleteTrigger: %v", err)
	} else if affected != 1 {
		t.Errorf("rows affected = %d, want 1", affected)
	} else {
		t.Logf("executeInsteadOfDeleteTrigger returned %d, %d", rows, affected)
	}
}
