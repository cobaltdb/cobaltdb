package engine

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/fdw"
	"github.com/cobaltdb/cobaltdb/pkg/optimizer"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// ---------------------------------------------------------------------------
// Simple accessor / getter tests
// ---------------------------------------------------------------------------

func TestDatabase_GetCatalog(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	c := db.GetCatalog()
	if c == nil {
		t.Fatal("GetCatalog() returned nil")
	}
}

func TestDatabase_GetCurrentLSN_NoWAL(t *testing.T) {
	// In-memory DB has no WAL, so GetCurrentLSN returns 0
	db := openCoverageDB(t)
	defer db.Close()

	lsn := db.GetCurrentLSN()
	if lsn != 0 {
		t.Fatalf("GetCurrentLSN() on in-memory db = %d, want 0", lsn)
	}
}

func TestDatabase_GetOptimizer(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	opt := db.GetOptimizer()
	// May be nil if the optimizer is not initialized for the in-memory path
	// Just verify the call doesn't panic and returns a predictable type
	_ = opt
}

func TestDatabase_GetScheduler(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	sched := db.GetScheduler()
	_ = sched // may be nil depending on options; just verify no panic
}

func TestDatabase_Path(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	p := db.Path()
	if p != ":memory:" {
		t.Fatalf("Path() = %q, want %q", p, ":memory:")
	}
}

func TestDatabase_ListBackups_NoManager(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	bks := db.ListBackups()
	if bks != nil {
		t.Fatal("ListBackups() should be nil when backup manager is nil")
	}
}

func TestDatabase_GetBackup_NoManager(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	bk := db.GetBackup("some-id")
	if bk != nil {
		t.Fatal("GetBackup() should be nil when backup manager is nil")
	}
}

func TestDatabase_ResetIndexAdvisor_Nil(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	// When indexAdvisor is nil, ResetIndexAdvisor should be a no-op (no panic)
	db.ResetIndexAdvisor()
}

func TestDatabase_UpdateTableStatistics_NilOptimizer(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	db.UpdateTableStatistics("tbl", &optimizer.TableStatistics{})
	// Should be a no-op when optimizer is nil; verify no panic
}

func TestDatabase_GetReplicationManager(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	rm := db.GetReplicationManager()
	if rm != nil {
		t.Fatal("GetReplicationManager() should be nil for in-memory db")
	}
}

// ---------------------------------------------------------------------------
// TableSchemaWithoutForeignKeys
// ---------------------------------------------------------------------------

func TestDatabase_TableSchemaWithoutForeignKeys(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	schema, err := db.TableSchemaWithoutForeignKeys("t1")
	if err != nil {
		t.Fatalf("TableSchemaWithoutForeignKeys failed: %v", err)
	}
	if schema == "" {
		t.Fatal("expected non-empty schema")
	}
	if len(schema) < 10 {
		t.Fatalf("schema too short: %q", schema)
	}
}

func TestDatabase_TableSchemaWithoutForeignKeys_Nonexistent(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	schema, err := db.TableSchemaWithoutForeignKeys("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
	if schema != "" {
		t.Fatalf("expected empty schema, got %q", schema)
	}
}

// ---------------------------------------------------------------------------
// TableForeignKeys / TableForeignKeyRefs / TableSelfForeignKeyRefs
// ---------------------------------------------------------------------------

func TestDatabase_TableForeignKeys_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	fks := db.TableForeignKeys("t1")
	if fks == nil {
		t.Fatal("TableForeignKeys should return non-nil slice")
	}
	if len(fks) != 0 {
		t.Fatalf("expected 0 FKs, got %d", len(fks))
	}
}

func TestDatabase_TableForeignKeys_Nonexistent(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	fks := db.TableForeignKeys("nonexistent")
	if fks != nil {
		t.Fatal("TableForeignKeys should return nil for nonexistent table")
	}
}

func TestDatabase_TableForeignKeyRefs_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	refs := db.TableForeignKeyRefs("t1")
	// var refs []string with no loop iterations stays nil
	if refs != nil && len(refs) != 0 {
		t.Fatalf("expected nil or empty refs, got %v", refs)
	}
}

func TestDatabase_TableForeignKeyRefs_Nonexistent(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	refs := db.TableForeignKeyRefs("nonexistent")
	if refs != nil {
		t.Fatal("TableForeignKeyRefs should return nil for nonexistent table")
	}
}

func TestDatabase_TableSelfForeignKeyRefs_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	refs := db.TableSelfForeignKeyRefs("t1")
	// var refs []TableForeignKeyRef with no loop iterations stays nil
	if refs != nil && len(refs) != 0 {
		t.Fatalf("expected nil or empty self-refs, got %v", refs)
	}
}

func TestDatabase_TableSelfForeignKeyRefs_Nonexistent(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	refs := db.TableSelfForeignKeyRefs("nonexistent")
	if refs != nil {
		t.Fatal("TableSelfForeignKeyRefs should return nil for nonexistent table")
	}
}

// ---------------------------------------------------------------------------
// FTSIndexDDL
// ---------------------------------------------------------------------------

func TestDatabase_FTSIndexDDL_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	ddl := db.FTSIndexDDL()
	if ddl == nil {
		t.Fatal("FTSIndexDDL should return non-nil slice")
	}
	if len(ddl) != 0 {
		t.Fatalf("expected 0 DDL statements, got %d", len(ddl))
	}
}

// Add an FTS index by directly manipulating catalog state
func TestDatabase_FTSIndexDDL_WithIndex(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, content TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE FULLTEXT INDEX idx_fts ON t1 (content)")
	if err != nil {
		t.Fatalf("CREATE FULLTEXT INDEX failed: %v", err)
	}

	ddl := db.FTSIndexDDL()
	if len(ddl) != 1 {
		t.Fatalf("expected 1 DDL statement, got %d", len(ddl))
	}
	if ddl[0] != `CREATE FULLTEXT INDEX "idx_fts" ON "t1" ("content");` {
		t.Fatalf("unexpected DDL: %q", ddl[0])
	}
}

// ---------------------------------------------------------------------------
// VectorIndexDDL
// ---------------------------------------------------------------------------

func TestDatabase_VectorIndexDDL_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	ddl := db.VectorIndexDDL()
	if ddl == nil {
		t.Fatal("VectorIndexDDL should return non-nil slice")
	}
	if len(ddl) != 0 {
		t.Fatalf("expected 0 DDL statements, got %d", len(ddl))
	}
}

func TestDatabase_VectorIndexDDL_WithIndex(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE VECTOR INDEX idx_vec ON t1 (embedding)")
	if err != nil {
		t.Fatalf("CREATE VECTOR INDEX failed: %v", err)
	}

	ddl := db.VectorIndexDDL()
	if len(ddl) != 1 {
		t.Fatalf("expected 1 DDL statement, got %d", len(ddl))
	}
	if ddl[0] != `CREATE VECTOR INDEX "idx_vec" ON "t1" ("embedding");` {
		t.Fatalf("unexpected DDL: %q", ddl[0])
	}
}

// ---------------------------------------------------------------------------
// ForeignTableDDL
// ---------------------------------------------------------------------------

func TestDatabase_ForeignTableDDL_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	ddl := db.ForeignTableDDL()
	if ddl == nil {
		t.Fatal("ForeignTableDDL should return non-nil slice")
	}
	if len(ddl) != 0 {
		t.Fatalf("expected 0 DDL statements, got %d", len(ddl))
	}
}

func TestDatabase_ForeignTableDDL_WithForeignTable(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	// Register a mock FDW so the foreign table can be created
	db.RegisterFDW("mock", func() fdw.ForeignDataWrapper {
		return &mockForeignTableWrapper{}
	})

	_, err := db.Exec(ctx, `CREATE FOREIGN TABLE ft1 (id INTEGER, name TEXT) WRAPPER 'mock' OPTIONS (filename 'test.csv')`)
	if err != nil {
		t.Fatalf("CREATE FOREIGN TABLE failed: %v", err)
	}

	ddl := db.ForeignTableDDL()
	if len(ddl) != 1 {
		t.Fatalf("expected 1 DDL statement, got %d", len(ddl))
	}
	// Verify the DDL references the wrapper and options
	if ddl[0] != `CREATE FOREIGN TABLE "ft1" (`+"\n  "+`"id" INTEGER,`+"\n  "+`"name" TEXT`+"\n"+`) WRAPPER 'mock' OPTIONS ("filename" 'test.csv');` {
		t.Fatalf("unexpected DDL:\n%s", ddl[0])
	}
}

// ---------------------------------------------------------------------------
// ViewDDL
// ---------------------------------------------------------------------------

func TestDatabase_ViewDDL_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	ddl := db.ViewDDL()
	if ddl == nil {
		t.Fatal("ViewDDL should return non-nil slice")
	}
	if len(ddl) != 0 {
		t.Fatalf("expected 0 DDL statements, got %d", len(ddl))
	}
}

func TestDatabase_ViewDDL_WithView(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE VIEW v1 AS SELECT id, name FROM t1")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	ddl := db.ViewDDL()
	if len(ddl) != 1 {
		t.Fatalf("expected 1 DDL statement, got %d", len(ddl))
	}
	if ddl[0] == "" {
		t.Fatal("expected non-empty DDL")
	}
}

// ---------------------------------------------------------------------------
// MaterializedViewDDL
// ---------------------------------------------------------------------------

func TestDatabase_MaterializedViewDDL_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	ddl := db.MaterializedViewDDL()
	if ddl == nil {
		t.Fatal("MaterializedViewDDL should return non-nil slice")
	}
	if len(ddl) != 0 {
		t.Fatalf("expected 0 DDL statements, got %d", len(ddl))
	}
}

func TestDatabase_MaterializedViewDDL_WithView(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE MATERIALIZED VIEW mv1 AS SELECT id, name FROM t1")
	if err != nil {
		t.Fatalf("CREATE MATERIALIZED VIEW failed: %v", err)
	}

	ddl := db.MaterializedViewDDL()
	if len(ddl) != 1 {
		t.Fatalf("expected 1 DDL statement, got %d", len(ddl))
	}
	if ddl[0] == "" {
		t.Fatal("expected non-empty DDL")
	}
}

// ---------------------------------------------------------------------------
// TriggerDDL
// ---------------------------------------------------------------------------

func TestDatabase_TriggerDDL_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	ddl := db.TriggerDDL()
	if ddl == nil {
		t.Fatal("TriggerDDL should return non-nil slice")
	}
	if len(ddl) != 0 {
		t.Fatalf("expected 0 DDL statements, got %d", len(ddl))
	}
}

func TestDatabase_TriggerDDL_WithTrigger(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TRIGGER trg1 AFTER INSERT ON t1 BEGIN SELECT 1; END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	ddl := db.TriggerDDL()
	if len(ddl) != 1 {
		t.Fatalf("expected 1 DDL statement, got %d", len(ddl))
	}
	if ddl[0] == "" {
		t.Fatal("expected non-empty DDL")
	}
}

// ---------------------------------------------------------------------------
// ProcedureDDL
// ---------------------------------------------------------------------------

func TestDatabase_ProcedureDDL_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	ddl := db.ProcedureDDL()
	if ddl == nil {
		t.Fatal("ProcedureDDL should return non-nil slice")
	}
	if len(ddl) != 0 {
		t.Fatalf("expected 0 DDL statements, got %d", len(ddl))
	}
}

func TestDatabase_ProcedureDDL_WithProcedure(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, `CREATE PROCEDURE p1() BEGIN SELECT 1; END`)
	if err != nil {
		t.Fatalf("CREATE PROCEDURE failed: %v", err)
	}

	ddl := db.ProcedureDDL()
	if len(ddl) != 1 {
		t.Fatalf("expected 1 DDL statement, got %d", len(ddl))
	}
	if ddl[0] == "" {
		t.Fatal("expected non-empty DDL")
	}
}

// ---------------------------------------------------------------------------
// SearchVectorKNN / SearchVectorRange (direct catalog calls)
// ---------------------------------------------------------------------------

func TestDatabase_SearchVectorKNN_NoIndex(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, _, err = db.SearchVectorKNN("nonexistent_idx", []float64{1.0, 0.0, 0.0}, 5)
	if err == nil {
		t.Fatal("expected error for nonexistent vector index")
	}
}

func TestDatabase_SearchVectorRange_NoIndex(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, _, err = db.SearchVectorRange("nonexistent_idx", []float64{1.0, 0.0, 0.0}, 1.5)
	if err == nil {
		t.Fatal("expected error for nonexistent vector index")
	}
}

func TestDatabase_SearchVectorKNN_ClosedDB(t *testing.T) {
	db := openCoverageDB(t)
	db.Close()

	_, _, err := db.SearchVectorKNN("idx", []float64{1.0}, 1)
	if err != ErrDatabaseClosed {
		t.Fatalf("expected ErrDatabaseClosed, got %v", err)
	}
}

func TestDatabase_SearchVectorRange_ClosedDB(t *testing.T) {
	db := openCoverageDB(t)
	db.Close()

	_, _, err := db.SearchVectorRange("idx", []float64{1.0}, 1.0)
	if err != ErrDatabaseClosed {
		t.Fatalf("expected ErrDatabaseClosed, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// executeCreateForeignTable (direct call via SQL)
// ---------------------------------------------------------------------------

func TestDatabase_executeCreateForeignTable(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	// Register a mock FDW so the foreign table can be created
	db.RegisterFDW("mock_csv", func() fdw.ForeignDataWrapper {
		return &mockForeignTableWrapper{}
	})

	_, err := db.Exec(ctx, `CREATE FOREIGN TABLE ft1 (id INTEGER, name TEXT) WRAPPER 'mock_csv'`)
	if err != nil {
		t.Fatalf("CREATE FOREIGN TABLE failed: %v", err)
	}

	// Verify the foreign table was created
	ddl := db.ForeignTableDDL()
	if len(ddl) != 1 {
		t.Fatalf("expected 1 foreign table, got %d", len(ddl))
	}
}

func TestDatabase_executeCreateForeignTable_IfNotExists(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	db.RegisterFDW("mock_ifn", func() fdw.ForeignDataWrapper {
		return &mockForeignTableWrapper{}
	})

	_, err := db.Exec(ctx, `CREATE FOREIGN TABLE IF NOT EXISTS ft1 (id INTEGER) WRAPPER 'mock_ifn'`)
	if err != nil {
		t.Fatalf("first CREATE FOREIGN TABLE failed: %v", err)
	}

	// Second create with IF NOT EXISTS should succeed
	_, err = db.Exec(ctx, `CREATE FOREIGN TABLE IF NOT EXISTS ft1 (id INTEGER) WRAPPER 'mock_ifn'`)
	if err != nil {
		t.Fatalf("second CREATE FOREIGN TABLE with IF NOT EXISTS failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// RegisterFDW
// ---------------------------------------------------------------------------

func TestDatabase_RegisterFDW(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	db.RegisterFDW("test_fdw", func() fdw.ForeignDataWrapper {
		return &mockForeignTableWrapper{}
	})

	reg := db.catalog.GetFDWRegistry()
	if reg == nil {
		t.Fatal("FDW registry is nil")
	}

	wrapper, ok := reg.Get("test_fdw")
	if !ok {
		t.Fatal("expected to find registered FDW")
	}
	if wrapper == nil {
		t.Fatal("expected non-nil wrapper")
	}
}

// ---------------------------------------------------------------------------
// executeCreateForeignTable direct unit test (using CreateForeignTableStmt)
// ---------------------------------------------------------------------------

func TestDatabase_executeCreateForeignTable_Direct(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	db.RegisterFDW("direct", func() fdw.ForeignDataWrapper {
		return &mockForeignTableWrapper{}
	})

	stmt := &query.CreateForeignTableStmt{
		Table:   "ft_direct",
		Wrapper: "direct",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
		},
	}

	result, err := db.executeCreateForeignTable(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeCreateForeignTable failed: %v", err)
	}
	if result.RowsAffected != 0 {
		t.Fatalf("expected 0 rows affected, got %d", result.RowsAffected)
	}
}

// ---------------------------------------------------------------------------
// Mock FDW for testing
// ---------------------------------------------------------------------------

type mockForeignTableWrapper struct{}

func (m *mockForeignTableWrapper) Name() string { return "mock" }

func (m *mockForeignTableWrapper) Open(options map[string]string) error {
	return nil
}

func (m *mockForeignTableWrapper) Close() error {
	return nil
}

func (m *mockForeignTableWrapper) Scan(table string, columns []string) ([][]interface{}, error) {
	return [][]interface{}{}, nil
}

// Ensure mockForeignTableWrapper implements fdw.ForeignDataWrapper.
var _ fdw.ForeignDataWrapper = (*mockForeignTableWrapper)(nil)

// ---------------------------------------------------------------------------
// Additional coverage: TableIndexDDL edge cases
// ---------------------------------------------------------------------------

func TestDatabase_TableIndexDDL_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	ddl := db.TableIndexDDL("t1")
	// var ddl []string with no loop iterations stays nil
	if ddl != nil && len(ddl) != 0 {
		t.Fatalf("expected nil or empty DDL, got %d", len(ddl))
	}
}

func TestDatabase_TableIndexDDL_WithIndex(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE INDEX idx_name ON t1 (name)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	ddl := db.TableIndexDDL("t1")
	if len(ddl) != 1 {
		t.Fatalf("expected 1 DDL statement, got %d", len(ddl))
	}
}

// ---------------------------------------------------------------------------
// RLSPolicyDDL coverage (no policies)
// ---------------------------------------------------------------------------

func TestDatabase_RLSPolicyDDL_None(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	ddl := db.RLSPolicyDDL()
	if ddl != nil {
		t.Fatal("RLSPolicyDDL should return nil when no RLS policies exist")
	}
}

// ---------------------------------------------------------------------------
// Helper: test catalog-level vector search via the engine's catalog reference
// ---------------------------------------------------------------------------

func TestDatabase_VectorSearchViaCatalog(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// The engine's SearchVectorKNN delegates to catalog.SearchVectorKNN,
	// which requires a valid vector index. Use the catalog directly to test
	// the engine's error path with no index.
	cat := db.GetCatalog()
	_, _, err = cat.SearchVectorKNN("nonexistent", []float64{1.0, 0.0, 0.0}, 5)
	if err == nil {
		t.Fatal("expected error from catalog.SearchVectorKNN with no index")
	}

	_, _, err = cat.SearchVectorRange("nonexistent", []float64{1.0, 0.0, 0.0}, 1.5)
	if err == nil {
		t.Fatal("expected error from catalog.SearchVectorRange with no index")
	}
}

// ---------------------------------------------------------------------------
// Coverage: catalog.TableForeignKeyRefs with actual FKs
// ---------------------------------------------------------------------------

func TestDatabase_TableForeignKeyRefs_WithFKs(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	refs := db.TableForeignKeyRefs("orders")
	if len(refs) != 1 || refs[0] != "users" {
		t.Fatalf("TableForeignKeyRefs(orders) = %v, want [users]", refs)
	}
}

func TestDatabase_TableForeignKeys_WithFKs(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	fks := db.TableForeignKeys("orders")
	if len(fks) != 1 {
		t.Fatalf("expected 1 FK, got %d", len(fks))
	}
	if fks[0].ReferencedTable != "users" {
		t.Fatalf("expected referenced table 'users', got %q", fks[0].ReferencedTable)
	}
}

// ---------------------------------------------------------------------------
// TableSchema / TableSchemaWithoutForeignKeys with FK-bearing table
// ---------------------------------------------------------------------------

func TestDatabase_TableSchemaWithoutForeignKeys_WithFKs(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// With FKs excluded
	schema, err := db.TableSchemaWithoutForeignKeys("orders")
	if err != nil {
		t.Fatalf("TableSchemaWithoutForeignKeys failed: %v", err)
	}
	if schema == "" {
		t.Fatal("expected non-empty schema")
	}
	if len(schema) < 10 {
		t.Fatalf("schema too short: %q", schema)
	}
}
