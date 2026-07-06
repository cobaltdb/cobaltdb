package catalog

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// ── CreateTemporaryViewSQL ────────────────────────────────────────────

func TestCreateTemporaryViewSQL(t *testing.T) {
	c := newTestCatalog(t)
	viewQuery := simpleSelectStmt()

	// Success: create a temporary view
	if err := c.CreateTemporaryViewSQL("tmp_view", viewQuery, ""); err != nil {
		t.Fatalf("CreateTemporaryViewSQL: %v", err)
	}

	// Verify it's stored
	got, err := c.GetView("tmp_view")
	if err != nil {
		t.Fatalf("GetView(tmp_view): %v", err)
	}
	if got == nil {
		t.Fatal("GetView returned nil")
	}
	if !c.viewTemporary["tmp_view"] {
		t.Error("viewTemporary[tmp_view] = false, want true")
	}

	// Verify it's excluded from ListViewSQL
	views := c.ListViewSQL()
	if _, exists := views["tmp_view"]; exists {
		t.Error("ListViewSQL includes temporary view, should not")
	}

	// Duplicate name → error (view already exists, and CreateView path rejects duplicates)
	if err := c.CreateTemporaryViewSQL("tmp_view", viewQuery, ""); err == nil {
		t.Error("CreateTemporaryViewSQL duplicate should fail")
	}
}

func TestCreateTemporaryViewSQL_TableNameConflict(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "conflict_table", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger},
	})

	viewQuery := simpleSelectStmt()
	err := c.CreateTemporaryViewSQL("conflict_table", viewQuery, "")
	if err == nil {
		t.Fatal("CreateTemporaryViewSQL with existing table name should fail")
	}
}

func TestCreateTemporaryViewSQL_WithSQL(t *testing.T) {
	c := newTestCatalog(t)
	viewQuery := simpleSelectStmt()
	sql := "CREATE VIEW custom AS SELECT 1"

	if err := c.CreateTemporaryViewSQL("custom_tmp", viewQuery, sql); err != nil {
		t.Fatalf("CreateTemporaryViewSQL: %v", err)
	}
	if c.viewSQL["custom_tmp"] != sql {
		t.Errorf("viewSQL = %q, want %q", c.viewSQL["custom_tmp"], sql)
	}
}

// ── CreateOrReplaceViewSQL ────────────────────────────────────────────

func TestCreateOrReplaceViewSQL(t *testing.T) {
	c := newTestCatalog(t)
	viewQuery := simpleSelectStmt()

	// Create new
	if err := c.CreateOrReplaceViewSQL("rep_view", viewQuery, ""); err != nil {
		t.Fatalf("CreateOrReplaceViewSQL(new): %v", err)
	}
	got, err := c.GetView("rep_view")
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if got == nil {
		t.Fatal("GetView returned nil")
	}

	// Replace existing
	viewQuery2 := &query.SelectStmt{
		Columns: []query.Expression{&query.NumberLiteral{Value: 2}},
		From:    &query.TableRef{Name: "dual"},
	}
	if err := c.CreateOrReplaceViewSQL("rep_view", viewQuery2, ""); err != nil {
		t.Fatalf("CreateOrReplaceViewSQL(replace): %v", err)
	}
	cols := c.getColumnsForTableOrView("rep_view")
	if len(cols) != 1 {
		t.Fatalf("expected 1 column after replacement, got %d", len(cols))
	}

	// Table name conflict
	createTestTable(t, c, "rep_conflict", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger},
	})
	if err := c.CreateOrReplaceViewSQL("rep_conflict", viewQuery, ""); err == nil {
		t.Error("CreateOrReplaceViewSQL with existing table name should fail")
	}
}

// ── CreateOrReplaceTemporaryViewSQL ───────────────────────────────────

func TestCreateOrReplaceTemporaryViewSQL(t *testing.T) {
	c := newTestCatalog(t)
	viewQuery := simpleSelectStmt()

	// Create new temporary view via CreateOrReplace
	if err := c.CreateOrReplaceTemporaryViewSQL("tmp_rep", viewQuery, ""); err != nil {
		t.Fatalf("CreateOrReplaceTemporaryViewSQL: %v", err)
	}
	if !c.viewTemporary["tmp_rep"] {
		t.Error("viewTemporary[tmp_rep] = false, want true")
	}

	// Replace with non-temporary → should switch flag
	if err := c.CreateOrReplaceViewSQL("tmp_rep", viewQuery, ""); err != nil {
		t.Fatalf("CreateOrReplaceViewSQL second: %v", err)
	}
	if c.viewTemporary["tmp_rep"] {
		t.Error("viewTemporary[tmp_rep] should be false after non-temporary replace")
	}
}

// ── createOrReplaceViewSQL (replace temporary → non-temporary path) ──

func TestCreateOrReplaceViewSQL_ReplaceNonTemporaryWithTemporary(t *testing.T) {
	c := newTestCatalog(t)
	viewQuery := simpleSelectStmt()

	// Create non-temporary view first
	if err := c.CreateViewSQL("switch_me", viewQuery, ""); err != nil {
		t.Fatalf("CreateViewSQL: %v", err)
	}

	// Replace with temporary → triggers deleteCatalogDef path
	// (line 1884: temporary && existed && !oldTemporary)
	if err := c.CreateOrReplaceTemporaryViewSQL("switch_me", viewQuery, ""); err != nil {
		t.Fatalf("CreateOrReplaceTemporaryViewSQL: %v", err)
	}

	if !c.viewTemporary["switch_me"] {
		t.Error("view should now be temporary")
	}
}

func TestCreateOrReplaceViewSQL_ReplaceTemporaryTriggersDelete(t *testing.T) {
	c := newTestCatalog(t)
	viewQuery := simpleSelectStmt()

	// Create temporary view
	if err := c.CreateTemporaryViewSQL("switch_me", viewQuery, ""); err != nil {
		t.Fatalf("CreateTemporaryViewSQL: %v", err)
	}

	// Replace with non-temporary → should trigger deleteCatalogDef path
	if err := c.CreateOrReplaceViewSQL("switch_me", viewQuery, ""); err != nil {
		t.Fatalf("CreateOrReplaceViewSQL: %v", err)
	}

	if c.viewTemporary["switch_me"] {
		t.Error("view should no longer be temporary")
	}
}

func TestCreateOrReplaceViewSQL_WithinTransaction(t *testing.T) {
	c := newTestCatalog(t)
	viewQuery := simpleSelectStmt()

	// Start a transaction so isCurrentTxnActive() returns true
	c.BeginTransaction(42)

	// Create new view within transaction
	if err := c.CreateOrReplaceViewSQL("txn_view", viewQuery, ""); err != nil {
		t.Fatalf("CreateOrReplaceViewSQL in txn: %v", err)
	}

	// Replace existing view within transaction
	viewQuery2 := &query.SelectStmt{
		Columns: []query.Expression{&query.NumberLiteral{Value: 99}},
		From:    &query.TableRef{Name: "dual"},
	}
	if err := c.CreateOrReplaceViewSQL("txn_view", viewQuery2, ""); err != nil {
		t.Fatalf("CreateOrReplaceViewSQL replace in txn: %v", err)
	}
}

func TestCreateOrReplaceViewSQL_NilMaps(t *testing.T) {
	// Create a Catalog where viewSQL and viewTemporary maps are nil
	// to hit the lazy initialization paths (lines 1869-1873).
	c := &Catalog{
		tree:   nil,
		tables: make(map[string]*TableDef),
		views:  make(map[string]*query.SelectStmt),
	}
	viewQuery := simpleSelectStmt()

	if err := c.CreateOrReplaceViewSQL("nilmap_view", viewQuery, ""); err != nil {
		t.Fatalf("CreateOrReplaceViewSQL with nil maps: %v", err)
	}
	if c.viewSQL == nil {
		t.Error("viewSQL should be initialized")
	}
	if c.viewTemporary == nil {
		t.Error("viewTemporary should be initialized")
	}
}

func TestCreateOrReplaceViewSQL_WithProvidedSQL(t *testing.T) {
	c := newTestCatalog(t)
	viewQuery := simpleSelectStmt()
	sql := "CREATE VIEW custom_rep AS SELECT 42"

	if err := c.CreateOrReplaceViewSQL("custom_rep", viewQuery, sql); err != nil {
		t.Fatalf("CreateOrReplaceViewSQL: %v", err)
	}
	if c.viewSQL["custom_rep"] != sql {
		t.Errorf("viewSQL = %q, want %q", c.viewSQL["custom_rep"], sql)
	}
}

// ── ListViewSQL ───────────────────────────────────────────────────────

func TestListViewSQL(t *testing.T) {
	c := newTestCatalog(t)
	viewQuery := simpleSelectStmt()

	// Create one permanent view
	if err := c.CreateViewSQL("perm_view", viewQuery, ""); err != nil {
		t.Fatalf("CreateViewSQL: %v", err)
	}

	// Create one temporary view
	if err := c.CreateTemporaryViewSQL("tmp_view", viewQuery, ""); err != nil {
		t.Fatalf("CreateTemporaryViewSQL: %v", err)
	}

	views := c.ListViewSQL()
	if len(views) != 1 {
		t.Fatalf("ListViewSQL length = %d, want 1", len(views))
	}
	if _, exists := views["perm_view"]; !exists {
		t.Error("ListViewSQL should include permanent view")
	}
	if _, exists := views["tmp_view"]; exists {
		t.Error("ListViewSQL should NOT include temporary view")
	}
}

func TestListViewSQL_Empty(t *testing.T) {
	c := newTestCatalog(t)
	views := c.ListViewSQL()
	if len(views) != 0 {
		t.Errorf("ListViewSQL on empty catalog = %d, want 0", len(views))
	}
}

// ── ListTriggerSQL ────────────────────────────────────────────────────

func TestListTriggerSQL(t *testing.T) {
	c := newTestCatalog(t)

	c.triggerSQL = map[string]string{
		"trg1": "CREATE TRIGGER trg1 AFTER INSERT ON t1 BEGIN ... END",
		"trg2": "CREATE TRIGGER trg2 BEFORE UPDATE ON t2 BEGIN ... END",
	}

	triggers := c.ListTriggerSQL()
	if len(triggers) != 2 {
		t.Fatalf("ListTriggerSQL length = %d, want 2", len(triggers))
	}
	if triggers["trg1"] == "" {
		t.Error("ListTriggerSQL missing trg1")
	}
	if triggers["trg2"] == "" {
		t.Error("ListTriggerSQL missing trg2")
	}

	// Verify it's a copy (mutating the returned map doesn't affect original)
	triggers["trg1"] = "mutated"
	if c.triggerSQL["trg1"] == "mutated" {
		t.Error("ListTriggerSQL returned non-copied map")
	}
}

func TestListTriggerSQL_Empty(t *testing.T) {
	c := newTestCatalog(t)
	triggers := c.ListTriggerSQL()
	if len(triggers) != 0 {
		t.Errorf("ListTriggerSQL on empty catalog = %d, want 0", len(triggers))
	}
}

// ── ListProcedureSQL ──────────────────────────────────────────────────

func TestListProcedureSQL(t *testing.T) {
	c := newTestCatalog(t)

	c.procedureSQL = map[string]string{
		"proc1": "CREATE PROCEDURE proc1() BEGIN SELECT 1; END",
	}

	procs := c.ListProcedureSQL()
	if len(procs) != 1 {
		t.Fatalf("ListProcedureSQL length = %d, want 1", len(procs))
	}
	if procs["proc1"] == "" {
		t.Error("ListProcedureSQL missing proc1")
	}

	// Verify it's a copy
	procs["proc1"] = "mutated"
	if c.procedureSQL["proc1"] == "mutated" {
		t.Error("ListProcedureSQL returned non-copied map")
	}
}

func TestListProcedureSQL_Empty(t *testing.T) {
	c := newTestCatalog(t)
	procs := c.ListProcedureSQL()
	if len(procs) != 0 {
		t.Errorf("ListProcedureSQL on empty catalog = %d, want 0", len(procs))
	}
}

// ── getColumnsForTableOrView ──────────────────────────────────────────

func TestGetColumnsForTableOrView_Table(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "test_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	cols := c.getColumnsForTableOrView("test_tbl")
	if cols == nil {
		t.Fatal("getColumnsForTableOrView returned nil for table")
	}
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
	if cols[0].Name != "id" || cols[1].Name != "name" {
		t.Errorf("unexpected column names: %v", cols)
	}
}

func TestGetColumnsForTableOrView_View(t *testing.T) {
	c := newTestCatalog(t)

	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "col_a"},
			&query.AliasExpr{Expr: &query.Identifier{Name: "raw"}, Alias: "col_b"},
			&query.QualifiedIdentifier{Table: "t", Column: "col_c"},
			&query.NumberLiteral{Value: 42}, // unnamed → auto-name "column_0"
		},
		From: &query.TableRef{Name: "dummy"},
	}
	if err := c.CreateView("test_view", viewQuery); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	cols := c.getColumnsForTableOrView("test_view")
	if cols == nil {
		t.Fatal("getColumnsForTableOrView returned nil for view")
	}
	if len(cols) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(cols))
	}
	if cols[0].Name != "col_a" {
		t.Errorf("col[0].Name = %q, want %q", cols[0].Name, "col_a")
	}
	if cols[1].Name != "col_b" {
		t.Errorf("col[1].Name = %q, want %q", cols[1].Name, "col_b")
	}
	if cols[2].Name != "col_c" {
		t.Errorf("col[2].Name = %q, want %q", cols[2].Name, "col_c")
	}
	// validate all are TEXT type
	for i, col := range cols {
		if col.Type != "TEXT" {
			t.Errorf("col[%d].Type = %q, want TEXT", i, col.Type)
		}
	}
}

func TestGetColumnsForTableOrView_ViewAliasExprNoAlias(t *testing.T) {
	c := newTestCatalog(t)

	// AliasExpr with no Alias and non-Identifier inner expr
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.AliasExpr{Expr: &query.NumberLiteral{Value: 1}},
		},
		From: &query.TableRef{Name: "dummy"},
	}
	if err := c.CreateView("alias_view", viewQuery); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	cols := c.getColumnsForTableOrView("alias_view")
	if cols == nil {
		t.Fatal("getColumnsForTableOrView returned nil")
	}
	// No Alias, inner is not Identifier → colName stays "" → auto-named
	if len(cols) > 0 && cols[0].Name != "column_0" {
		t.Errorf("expected auto-named column_0, got %q", cols[0].Name)
	}
}

func TestGetColumnsForTableOrView_ViewAliasExprNoAliasWithIdentifier(t *testing.T) {
	c := newTestCatalog(t)

	// AliasExpr with no Alias but inner is Identifier
	// → falls through to else if id, ok := c.Expr.(*query.Identifier); ok
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.AliasExpr{Expr: &query.Identifier{Name: "raw_col"}},
		},
		From: &query.TableRef{Name: "dummy"},
	}
	if err := c.CreateView("alias_id_view", viewQuery); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	cols := c.getColumnsForTableOrView("alias_id_view")
	if cols == nil {
		t.Fatal("getColumnsForTableOrView returned nil")
	}
	if len(cols) > 0 && cols[0].Name != "raw_col" {
		t.Errorf("expected raw_col, got %q", cols[0].Name)
	}
}

func TestGetColumnsForTableOrView_NotFound(t *testing.T) {
	c := newTestCatalog(t)
	cols := c.getColumnsForTableOrView("nonexistent")
	if cols != nil {
		t.Error("expected nil for nonexistent name")
	}
}

// ── leadingNumericPrefix ──────────────────────────────────────────────

func TestLeadingNumericPrefix(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		// Basic integers
		{"123", "123"},
		{"0", "0"},
		{"42abc", "42"},
		{"abc", ""},
		{"", ""},
		// Sign prefix
		{"+5", "+5"},
		{"-7", "-7"},
		{"+-12", ""}, // second sign → no valid mantissa
		// Decimal
		{"3.14", "3.14"},
		{".5", ".5"},
		{"-.5", "-.5"},
		{"+.5", "+.5"},
		// Just a dot with no digits → not valid
		{".", ""},
		{"-.", ""},
		// Leading zeros
		{"007", "007"},
		// Sign only
		{"+", ""},
		{"-", ""},
		// Exponent
		{"1e5", "1e5"},
		{"1E5", "1E5"},
		{"1.5e10", "1.5e10"},
		{"1.5e+10", "1.5e+10"},
		{"1.5e-10", "1.5e-10"},
		{"1e", "1"},              // exponent without digits → ignored
		{"1e+", "1"},             // exponent sign without digits → ignored
		{"1e-abc", "1"},          // exponent without digits → ignored
		{"-3.5e2", "-3.5e2"},
		// Trailing garbage
		{"123abc456", "123"},
		{"  123", ""}, // leading whitespace doesn't count
		// Exponent edge: no mantissa digits after sign
		{"-e5", ""},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("leadingNumericPrefix(%q)", tt.input), func(t *testing.T) {
			got := leadingNumericPrefix(tt.input)
			if got != tt.want {
				t.Errorf("leadingNumericPrefix(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ── decodePersistedSQLDef ─────────────────────────────────────────────

func TestDecodePersistedSQLDef(t *testing.T) {
	// Test with name in JSON
	def := persistedSQLDef{Name: "my_view", SQL: "CREATE VIEW my_view AS SELECT 1"}
	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	got, err := decodePersistedSQLDef("view:my_view", "view:", data)
	if err != nil {
		t.Fatalf("decodePersistedSQLDef: %v", err)
	}
	if got.Name != "my_view" {
		t.Errorf("Name = %q, want %q", got.Name, "my_view")
	}
	if got.SQL != "CREATE VIEW my_view AS SELECT 1" {
		t.Errorf("SQL = %q, want %q", got.SQL, "CREATE VIEW my_view AS SELECT 1")
	}

	// Test with empty name → derive from key
	def2 := persistedSQLDef{SQL: "CREATE VIEW derived AS SELECT 2"}
	data2, err := json.Marshal(def2)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	got2, err := decodePersistedSQLDef("view:derived_view", "view:", data2)
	if err != nil {
		t.Fatalf("decodePersistedSQLDef: %v", err)
	}
	if got2.Name != "derived_view" {
		t.Errorf("Name = %q, want %q", got2.Name, "derived_view")
	}

	// Test SQL trimming
	def3 := persistedSQLDef{Name: "trimmed", SQL: "  CREATE VIEW trimmed AS SELECT 3  "}
	data3, _ := json.Marshal(def3)
	got3, err := decodePersistedSQLDef("view:trimmed", "view:", data3)
	if err != nil {
		t.Fatalf("decodePersistedSQLDef: %v", err)
	}
	if got3.SQL != "CREATE VIEW trimmed AS SELECT 3" {
		t.Errorf("SQL = %q, want trimmed", got3.SQL)
	}
}

func TestDecodePersistedSQLDef_InvalidJSON(t *testing.T) {
	_, err := decodePersistedSQLDef("view:x", "view:", []byte(`{invalid json}`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// ── storeRLSEnabledTable ──────────────────────────────────────────────

func TestStoreRLSEnabledTable(t *testing.T) {
	c := newTestCatalog(t)

	if err := c.storeRLSEnabledTable("my_table"); err != nil {
		t.Fatalf("storeRLSEnabledTable: %v", err)
	}

	// Verify it was persisted in the tree
	data, err := c.tree.Get([]byte("rlst:my_table"))
	if err != nil {
		t.Fatalf("tree.Get: %v", err)
	}
	if data == nil {
		t.Fatal("tree.Get returned nil for rlst:my_table")
	}

	// Verify the content
	expected := `{"enabled":true}`
	if string(data) != expected {
		t.Errorf("tree content = %q, want %q", string(data), expected)
	}
}

func TestStoreRLSEnabledTable_NilTree(t *testing.T) {
	cNil := New(nil, nil, nil)
	if err := cNil.storeRLSEnabledTable("noop"); err != nil {
		t.Errorf("storeRLSEnabledTable with nil tree: %v", err)
	}
}

// ── storeRLSPolicyDef ─────────────────────────────────────────────────

func TestStoreRLSPolicyDef(t *testing.T) {
	c := newTestCatalog(t)

	policy := &security.Policy{
		Name:      "policy_one",
		TableName: "my_table",
		Type:      security.PolicySelect,
	}

	if err := c.storeRLSPolicyDef(policy); err != nil {
		t.Fatalf("storeRLSPolicyDef: %v", err)
	}

	// Verify it was persisted
	expectedKey := "rlsp:my_table:policy_one"
	data, err := c.tree.Get([]byte(expectedKey))
	if err != nil {
		t.Fatalf("tree.Get: %v", err)
	}
	if data == nil {
		t.Fatal("tree.Get returned nil for " + expectedKey)
	}

	// Verify serialization round-trips correctly
	var decoded security.Policy
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if decoded.Name != "policy_one" {
		t.Errorf("decoded.Name = %q, want %q", decoded.Name, "policy_one")
	}
}

func TestStoreRLSPolicyDef_NilPolicy(t *testing.T) {
	c := newTestCatalog(t)
	if err := c.storeRLSPolicyDef(nil); err != nil {
		t.Errorf("storeRLSPolicyDef(nil): %v", err)
	}
}

func TestStoreRLSPolicyDef_NilTree(t *testing.T) {
	cNil := New(nil, nil, nil)
	policy := &security.Policy{
		Name:      "p",
		TableName: "t",
	}
	if err := cNil.storeRLSPolicyDef(policy); err != nil {
		t.Errorf("storeRLSPolicyDef with nil tree: %v", err)
	}
}

// ── storeVectorIndexDef ───────────────────────────────────────────────

func TestStoreVectorIndexDef(t *testing.T) {
	c := newTestCatalog(t)

	vid := &VectorIndexDef{
		Name:       "vec_idx",
		TableName:  "vec_table",
		ColumnName: "embedding",
		Dimensions: 128,
		IndexType:  "hnsw",
	}

	if err := c.storeVectorIndexDef(vid); err != nil {
		t.Fatalf("storeVectorIndexDef: %v", err)
	}

	// Verify it was persisted
	data, err := c.tree.Get([]byte("vec:vec_idx"))
	if err != nil {
		t.Fatalf("tree.Get: %v", err)
	}
	if data == nil {
		t.Fatal("tree.Get returned nil for vec:vec_idx")
	}

	// Verify serialization round-trips correctly
	var decoded VectorIndexDef
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if decoded.Name != "vec_idx" {
		t.Errorf("decoded.Name = %q, want %q", decoded.Name, "vec_idx")
	}
	if decoded.Dimensions != 128 {
		t.Errorf("decoded.Dimensions = %d, want 128", decoded.Dimensions)
	}
}

func TestStoreVectorIndexDef_NilTree(t *testing.T) {
	cNil := New(nil, nil, nil)
	vid := &VectorIndexDef{
		Name:       "noop_idx",
		TableName:  "t",
		ColumnName: "c",
		Dimensions: 3,
		IndexType:  "hnsw",
	}
	if err := cNil.storeVectorIndexDef(vid); err != nil {
		t.Errorf("storeVectorIndexDef with nil tree: %v", err)
	}
}

// ── simpleSelectStmt helper ───────────────────────────────────────────

func simpleSelectStmt() *query.SelectStmt {
	return &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "col1"},
			&query.Identifier{Name: "col2"},
		},
		From: &query.TableRef{Name: "dummy"},
	}
}
