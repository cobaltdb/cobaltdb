package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// ==================== ListRLSPolicies / ListRLSEnabledTables tests ====================

func TestListRLSPoliciesNil(t *testing.T) {
	c := &Catalog{}
	policies := c.ListRLSPolicies()
	if policies != nil {
		t.Error("expected nil when rlsManager is nil")
	}
}

func TestListRLSPoliciesDisabled(t *testing.T) {
	c := &Catalog{enableRLS: false}
	policies := c.ListRLSPolicies()
	if policies != nil {
		t.Error("expected nil when RLS is disabled")
	}
}

func TestListRLSPoliciesWithManager(t *testing.T) {
	mgr := security.NewManager()
	policy := &security.Policy{
		Name: "test_policy", TableName: "users",
		Type: security.PolicySelect, Expression: "true",
	}
	if err := mgr.CreatePolicy(policy); err != nil {
		t.Fatalf("CreatePolicy failed: %v", err)
	}
	c := &Catalog{enableRLS: true, rlsManager: mgr}
	policies := c.ListRLSPolicies()
	if len(policies) != 1 || policies[0].Name != "test_policy" {
		t.Fatalf("unexpected policies: %v", policies)
	}
}

func TestListRLSPoliciesSorted(t *testing.T) {
	mgr := security.NewManager()
	for _, p := range []struct{ name, table string }{
		{"z_last", "orders"}, {"a_first", "users"}, {"m_mid", "users"},
	} {
		if err := mgr.CreatePolicy(&security.Policy{
			Name: p.name, TableName: p.table, Type: security.PolicySelect, Expression: "true",
		}); err != nil {
			t.Fatalf("CreatePolicy(%s) failed: %v", p.name, err)
		}
	}
	c := &Catalog{enableRLS: true, rlsManager: mgr}
	policies := c.ListRLSPolicies()
	if len(policies) != 3 {
		t.Fatalf("expected 3 policies, got %d", len(policies))
	}
	expected := [][]string{{"orders", "z_last"}, {"users", "a_first"}, {"users", "m_mid"}}
	for i, e := range expected {
		if policies[i].TableName != e[0] || policies[i].Name != e[1] {
			t.Errorf("policy[%d]: got (%s, %s), want (%s, %s)", i, policies[i].TableName, policies[i].Name, e[0], e[1])
		}
	}
}

func TestListRLSEnabledTablesNil(t *testing.T) {
	c := &Catalog{}
	if tables := c.ListRLSEnabledTables(); tables != nil {
		t.Error("expected nil")
	}
}

func TestListRLSEnabledTablesDisabled(t *testing.T) {
	c := &Catalog{enableRLS: false}
	if tables := c.ListRLSEnabledTables(); tables != nil {
		t.Error("expected nil")
	}
}

func TestListRLSEnabledTablesWithManager(t *testing.T) {
	mgr := security.NewManager()
	mgr.EnableTable("users")
	mgr.EnableTable("orders")
	c := &Catalog{enableRLS: true, rlsManager: mgr}
	tables := c.ListRLSEnabledTables()
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
}

// ==================== EnableRLSTable tests ====================

func TestEnableRLSTableNoSuchTable(t *testing.T) {
	c := &Catalog{tables: make(map[string]*TableDef)}
	if err := c.EnableRLSTable("nonexistent"); err == nil {
		t.Error("expected error")
	}
}

func TestEnableRLSTableCreatesManager(t *testing.T) {
	c := &Catalog{tables: map[string]*TableDef{"users": {}}}
	if err := c.EnableRLSTable("users"); err != nil {
		t.Fatalf("EnableRLSTable failed: %v", err)
	}
	if c.rlsManager == nil || !c.enableRLS || !c.rlsManager.IsEnabled("users") {
		t.Error("RLS not properly enabled")
	}
}

func TestEnableRLSTableExistingManager(t *testing.T) {
	c := &Catalog{
		tables: map[string]*TableDef{"users": {}},
		enableRLS: true, rlsManager: security.NewManager(),
	}
	if err := c.EnableRLSTable("users"); err != nil {
		t.Fatalf("EnableRLSTable failed: %v", err)
	}
	if !c.rlsManager.IsEnabled("users") {
		t.Error("expected users table to be RLS-enabled")
	}
}

// ==================== BeginTransactionWithTxn tests ====================

func TestBeginTransactionWithTxn(t *testing.T) {
	cat := newTestCatalog(t)
	cat.BeginTransactionWithTxn(42, "mock_txn")
	ts := cat.getCurrentTxn()
	if ts == nil || !ts.txnActive || ts.txnID != 42 {
		t.Fatal("transaction not properly initialized")
	}
}

func TestBeginTransactionWithTxnNilManager(t *testing.T) {
	cat := newTestCatalog(t)
	cat.BeginTransactionWithTxn(0, nil)
	ts := cat.getCurrentTxn()
	if ts == nil || !ts.txnActive {
		t.Fatal("transaction not properly initialized")
	}
}

// ==================== undoCreateViewEntry tests ====================

func TestUndoCreateViewEntry(t *testing.T) {
	c := &Catalog{
		views: map[string]*query.SelectStmt{"test_view": {}},
		viewSQL: map[string]string{"test_view": "SELECT 1"},
		viewTemporary: map[string]bool{"test_view": false},
	}
	if err := c.undoCreateViewEntry(undoEntry{viewName: "test_view"}, "test"); err != nil {
		t.Fatalf("undoCreateViewEntry failed: %v", err)
	}
	if _, exists := c.views["test_view"]; exists {
		t.Error("expected view to be deleted")
	}
}

func TestUndoCreateViewEntryNilMaps(t *testing.T) {
	c := &Catalog{}
	if err := c.undoCreateViewEntry(undoEntry{viewName: "test_view"}, "test"); err != nil {
		t.Fatalf("undoCreateViewEntry failed: %v", err)
	}
}

// ==================== undoDropViewEntry tests ====================

func TestUndoDropViewEntry(t *testing.T) {
	sel := &query.SelectStmt{}
	c := &Catalog{}
	err := c.undoDropViewEntry(undoEntry{
		viewName: "test_view", viewQuery: sel, viewSQL: "SELECT 1", viewTemporary: true,
	}, "test")
	if err != nil {
		t.Fatalf("undoDropViewEntry failed: %v", err)
	}
	if c.views["test_view"] != sel || c.viewSQL["test_view"] != "SELECT 1" || !c.viewTemporary["test_view"] {
		t.Error("view not properly restored")
	}
}

func TestUndoDropViewEntryEmptySQL(t *testing.T) {
	sel := &query.SelectStmt{Columns: []query.Expression{&query.Identifier{Name: "id"}}}
	c := &Catalog{}
	err := c.undoDropViewEntry(undoEntry{
		viewName: "test_view", viewQuery: sel, viewSQL: "", viewTemporary: false,
	}, "test")
	if err != nil || c.viewSQL["test_view"] == "" {
		t.Error("expected view SQL to be generated")
	}
}

// ==================== undoCreateTriggerEntry tests ====================

func TestUndoCreateTriggerEntry(t *testing.T) {
	c := &Catalog{
		triggers: map[string]*query.CreateTriggerStmt{"test_trg": {}},
		triggerSQL: map[string]string{"test_trg": "CREATE TRIGGER ..."},
	}
	if err := c.undoCreateTriggerEntry(undoEntry{triggerName: "test_trg"}, "test"); err != nil {
		t.Fatalf("undoCreateTriggerEntry failed: %v", err)
	}
	if _, exists := c.triggers["test_trg"]; exists {
		t.Error("expected trigger to be deleted")
	}
}

func TestUndoCreateTriggerEntryNilMaps(t *testing.T) {
	c := &Catalog{}
	if err := c.undoCreateTriggerEntry(undoEntry{triggerName: "test_trg"}, "test"); err != nil {
		t.Fatalf("undoCreateTriggerEntry failed: %v", err)
	}
}

// ==================== undoDropTriggerEntry tests ====================

func TestUndoDropTriggerEntry(t *testing.T) {
	stmt := &query.CreateTriggerStmt{Name: "test_trg"}
	c := &Catalog{}
	err := c.undoDropTriggerEntry(undoEntry{
		triggerName: "test_trg", triggerStmt: stmt, triggerSQL: "CREATE TRIGGER ...",
	}, "test")
	if err != nil {
		t.Fatalf("undoDropTriggerEntry failed: %v", err)
	}
	if c.triggers["test_trg"] != stmt || c.triggerSQL["test_trg"] != "CREATE TRIGGER ..." {
		t.Error("trigger not properly restored")
	}
}

func TestUndoDropTriggerEntryEmptySQL(t *testing.T) {
	stmt := &query.CreateTriggerStmt{Name: "test_trg", Table: "t", Time: "BEFORE", Event: "INSERT"}
	c := &Catalog{}
	err := c.undoDropTriggerEntry(undoEntry{
		triggerName: "test_trg", triggerStmt: stmt, triggerSQL: "",
	}, "test")
	if err != nil || c.triggerSQL["test_trg"] == "" {
		t.Error("expected trigger SQL to be generated")
	}
}

// ==================== undoCreateProcedureEntry tests ====================

func TestUndoCreateProcedureEntry(t *testing.T) {
	c := &Catalog{
		procedures: map[string]*query.CreateProcedureStmt{"test_proc": {}},
		procedureSQL: map[string]string{"test_proc": "CREATE PROC ..."},
	}
	if err := c.undoCreateProcedureEntry(undoEntry{procedureName: "test_proc"}, "test"); err != nil {
		t.Fatalf("undoCreateProcedureEntry failed: %v", err)
	}
	if _, exists := c.procedures["test_proc"]; exists {
		t.Error("expected procedure to be deleted")
	}
}

func TestUndoCreateProcedureEntryNilMaps(t *testing.T) {
	c := &Catalog{}
	if err := c.undoCreateProcedureEntry(undoEntry{procedureName: "test_proc"}, "test"); err != nil {
		t.Fatalf("undoCreateProcedureEntry failed: %v", err)
	}
}

// ==================== undoDropProcedureEntry tests ====================

func TestUndoDropProcedureEntry(t *testing.T) {
	stmt := &query.CreateProcedureStmt{Name: "test_proc"}
	c := &Catalog{}
	err := c.undoDropProcedureEntry(undoEntry{
		procedureName: "test_proc", procedureStmt: stmt, procedureSQL: "CREATE PROC ...",
	}, "test")
	if err != nil {
		t.Fatalf("undoDropProcedureEntry failed: %v", err)
	}
	if c.procedures["test_proc"] != stmt || c.procedureSQL["test_proc"] != "CREATE PROC ..." {
		t.Error("procedure not properly restored")
	}
}

func TestUndoDropProcedureEntryEmptySQL(t *testing.T) {
	stmt := &query.CreateProcedureStmt{
		Name: "test_proc",
		Body: []query.Statement{&query.SelectStmt{Columns: []query.Expression{&query.Identifier{Name: "id"}}}},
	}
	c := &Catalog{}
	err := c.undoDropProcedureEntry(undoEntry{
		procedureName: "test_proc", procedureStmt: stmt, procedureSQL: "",
	}, "test")
	if err != nil || c.procedureSQL["test_proc"] == "" {
		t.Error("expected procedure SQL to be generated")
	}
}

// ==================== undoCreateMaterializedViewEntry tests ====================

func TestUndoCreateMaterializedViewEntry(t *testing.T) {
	c := &Catalog{
		materializedViews:  map[string]*MaterializedViewDef{"test_mv": {}},
		materializedViewSQL: map[string]string{"test_mv": "SELECT 1"},
	}
	if err := c.undoCreateMaterializedViewEntry(undoEntry{materializedViewName: "test_mv"}, "test"); err != nil {
		t.Fatalf("undoCreateMaterializedViewEntry failed: %v", err)
	}
	if _, exists := c.materializedViews["test_mv"]; exists {
		t.Error("expected MV to be deleted")
	}
}

// ==================== undoDropMaterializedViewEntry tests ====================

func TestUndoDropMaterializedViewEntry(t *testing.T) {
	mvDef := &MaterializedViewDef{}
	c := &Catalog{}
	err := c.undoDropMaterializedViewEntry(undoEntry{
		materializedViewName: "test_mv", materializedViewDef: mvDef, materializedViewSQL: "SELECT 1",
	}, "test")
	if err != nil {
		t.Fatalf("undoDropMaterializedViewEntry failed: %v", err)
	}
	if c.materializedViews["test_mv"] == nil || c.materializedViewSQL["test_mv"] != "SELECT 1" {
		t.Error("MV not properly restored")
	}
}

func TestUndoDropMaterializedViewEntryEmptySQL(t *testing.T) {
	mvDef := &MaterializedViewDef{Query: &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
	}}
	c := &Catalog{}
	err := c.undoDropMaterializedViewEntry(undoEntry{
		materializedViewName: "test_mv", materializedViewDef: mvDef, materializedViewSQL: "",
	}, "test")
	if err != nil || c.materializedViewSQL["test_mv"] == "" {
		t.Error("expected MV SQL to be generated")
	}
}

// ==================== rollbackAppliedDeletes tests ====================

func TestRollbackAppliedDeletesEmpty(t *testing.T) {
	fke := &ForeignKeyEnforcer{}
	err := fke.rollbackAppliedDeletes()
	if err != nil {
		t.Fatalf("rollbackAppliedDeletes failed: %v", err)
	}
}

// ==================== RebuildEntryPoint tests ====================

func TestRebuildEntryPointEmptyKey(t *testing.T) {
	h := &HNSWIndex{}
	h.RebuildEntryPoint()
	if h.EntryPoint != nil {
		t.Error("expected EntryPoint to be nil for empty key")
	}
}

func TestRebuildEntryPointKeyNotFound(t *testing.T) {
	h := &HNSWIndex{
		EntryPointKey: "nonexistent",
		Nodes:         make(map[string]*HNSWNode),
	}
	h.RebuildEntryPoint()
	if h.EntryPoint != nil {
		t.Error("expected EntryPoint to be nil for nonexistent key")
	}
}

func TestRebuildEntryPointFound(t *testing.T) {
	node := &HNSWNode{Key: "node1"}
	h := &HNSWIndex{
		EntryPointKey: "node1",
		Nodes:         map[string]*HNSWNode{"node1": node},
	}
	h.RebuildEntryPoint()
	if h.EntryPoint != node {
		t.Error("expected EntryPoint to be set to the found node")
	}
}

// ==================== SelectWithContext tests ====================

func TestSelectWithContextSimple(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "test_table", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	insertTestRow(t, c, "test_table", []query.Expression{
		nr(1), sr("alice"),
	})

	cols, rows, err := c.SelectWithContext(nil, &query.SelectStmt{
		Columns: []query.Expression{star()},
		From:    tref("test_table"),
	}, nil)
	if err != nil {
		t.Fatalf("SelectWithContext failed: %v", err)
	}
	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}
