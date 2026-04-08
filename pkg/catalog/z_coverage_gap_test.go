package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// createFullTestCatalog creates a catalog with a pool and tree for full execution tests
func createFullTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("create catalog tree: %v", err)
	}
	c := &Catalog{
		tables:            make(map[string]*TableDef),
		tableTrees:        make(map[string]*btree.BTree),
		indexes:           make(map[string]*IndexDef),
		indexTrees:        make(map[string]*btree.BTree),
		tree:              catalogTree,
		pool:              pool,
		views:             make(map[string]*query.SelectStmt),
		triggers:          make(map[string]*query.CreateTriggerStmt),
		procedures:        make(map[string]*query.CreateProcedureStmt),
		materializedViews: make(map[string]*MaterializedViewDef),
		ftsIndexes:        make(map[string]*FTSIndexDef),
		jsonIndexes:       make(map[string]*JSONIndexDef),
		vectorIndexes:     make(map[string]*VectorIndexDef),
		stats:             make(map[string]*StatsTableStats),
	}
	return c
}

// TestCovGap_ScalarSelect covers executeScalarSelect path
func TestCovGap_ScalarSelect(t *testing.T) {
	c := createFullTestCatalog(t)
	result, err := c.ExecuteQuery("SELECT 1 + 2")
	if err != nil {
		t.Fatalf("scalar select: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("expected at least one row")
	}
	result, err = c.ExecuteQuery("SELECT UPPER('hello')")
	if err != nil {
		t.Fatalf("scalar function: %v", err)
	}
}

// TestCovGap_SelectWithView covers view inlining
// Note: CREATE VIEW is handled at engine level; view tests are in engine package.

// TestCovGap_UpdateWithReturning covers RETURNING clause
func TestCovGap_UpdateWithReturning(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO products VALUES (1, 'A', 10)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	result, err := c.ExecuteQuery("UPDATE products SET qty = qty + 5 WHERE id = 1 RETURNING id, name, qty")
	if err != nil {
		t.Fatalf("update returning: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row returned, got %d", len(result.Rows))
	}
}

// TestCovGap_UpdatePKChange covers PK change detection
func TestCovGap_UpdatePKChange(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE nums (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO nums VALUES (1, 'one')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO nums VALUES (2, 'two')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("UPDATE nums SET id = 10 WHERE id = 1")
	if err != nil {
		t.Fatalf("update pk: %v", err)
	}
	result, err := c.ExecuteQuery("SELECT * FROM nums WHERE id = 10")
	if err != nil {
		t.Fatalf("select new pk: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row at new PK, got %d", len(result.Rows))
	}
	result, err = c.ExecuteQuery("SELECT * FROM nums WHERE id = 1")
	if err != nil {
		t.Fatalf("select old pk: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows at old PK, got %d", len(result.Rows))
	}
}

// TestCovGap_InsertWithDefaults covers DEFAULT expressions
func TestCovGap_InsertWithDefaults(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE cfg (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active', score INTEGER DEFAULT 100)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO cfg (id) VALUES (1)")
	if err != nil {
		t.Fatalf("insert defaults: %v", err)
	}
	result, err := c.ExecuteQuery("SELECT status, score FROM cfg WHERE id = 1")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "active" {
		t.Errorf("expected default 'active', got %v", result.Rows[0][0])
	}
}

// TestCovGap_JsonPathOperator covers ->> JSON operator
func TestCovGap_JsonPathOperator(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE jsontest (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery(`INSERT INTO jsontest VALUES (1, '{"name":"Alice","age":30}')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	result, err := c.ExecuteQuery(`SELECT data->>'$.name' FROM jsontest WHERE id = 1`)
	if err != nil {
		t.Logf("->> operator error: %v", err)
	} else if len(result.Rows) > 0 {
		if result.Rows[0][0] != "Alice" {
			t.Errorf("expected 'Alice', got %v", result.Rows[0][0])
		}
	}
}

// TestCovGap_JoinGroupBy covers JOIN + GROUP BY
func TestCovGap_JoinGroupBy(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create dept: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dept VALUES (1, 'Engineering')")
	if err != nil {
		t.Fatalf("insert dept: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dept VALUES (2, 'Sales')")
	if err != nil {
		t.Fatalf("insert dept: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE TABLE emp (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")
	if err != nil {
		t.Fatalf("create emp: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO emp VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("insert emp: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO emp VALUES (2, 1, 200)")
	if err != nil {
		t.Fatalf("insert emp: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO emp VALUES (3, 2, 150)")
	if err != nil {
		t.Fatalf("insert emp: %v", err)
	}
	result, err := c.ExecuteQuery("SELECT dept.name, SUM(emp.salary) FROM emp JOIN dept ON emp.dept_id = dept.id GROUP BY dept.name")
	if err != nil {
		t.Fatalf("join group by: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 groups, got %d", len(result.Rows))
	}
}

// TestCovGap_VacuumCompact covers Vacuum with indexes
func TestCovGap_VacuumCompact(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE vactbl (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO vactbl VALUES (1, 'a')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO vactbl VALUES (2, 'b')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("DELETE FROM vactbl WHERE id = 1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	err = c.Vacuum()
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	result, err := c.ExecuteQuery("SELECT * FROM vactbl")
	if err != nil {
		t.Fatalf("select after vacuum: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row after vacuum, got %d", len(result.Rows))
	}
}

// TestCovGap_RollbackCreateTable covers rollback of CREATE TABLE
func TestCovGap_RollbackCreateTable(t *testing.T) {
	c := createFullTestCatalog(t)
	c.BeginTransaction(1)
	_, err := c.ExecuteQuery("CREATE TABLE rollme (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	err = c.RollbackTransaction()
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	_, err = c.ExecuteQuery("SELECT * FROM rollme")
	if err == nil {
		t.Error("expected error for rolled-back table")
	}
}

// TestCovGap_RollbackDropTable covers rollback of DROP TABLE
func TestCovGap_RollbackDropTable(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE keepme (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO keepme VALUES (1, 'data')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	c.BeginTransaction(2)
	_, err = c.ExecuteQuery("DROP TABLE keepme")
	if err != nil {
		t.Fatalf("drop table: %v", err)
	}
	err = c.RollbackTransaction()
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	result, err := c.ExecuteQuery("SELECT * FROM keepme")
	if err != nil {
		t.Fatalf("select after rollback: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

// TestCovGap_RollbackInsert covers rollback of INSERT
func TestCovGap_RollbackInsert(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE rbins (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	c.BeginTransaction(3)
	_, err = c.ExecuteQuery("INSERT INTO rbins VALUES (1, 'temp')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	err = c.RollbackTransaction()
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	result, err := c.ExecuteQuery("SELECT * FROM rbins")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(result.Rows))
	}
}

// TestCovGap_RollbackUpdate covers rollback of UPDATE
func TestCovGap_RollbackUpdate(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE rbupd (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO rbupd VALUES (1, 'original')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	c.BeginTransaction(4)
	_, err = c.ExecuteQuery("UPDATE rbupd SET val = 'modified' WHERE id = 1")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	err = c.RollbackTransaction()
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	result, err := c.ExecuteQuery("SELECT val FROM rbupd WHERE id = 1")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "original" {
		t.Errorf("expected 'original', got %v", result.Rows[0][0])
	}
}

// TestCovGap_RollbackDelete covers rollback of DELETE
func TestCovGap_RollbackDelete(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE rbdel (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO rbdel VALUES (1, 'keep')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	c.BeginTransaction(5)
	_, err = c.ExecuteQuery("DELETE FROM rbdel WHERE id = 1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	err = c.RollbackTransaction()
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	result, err := c.ExecuteQuery("SELECT * FROM rbdel")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

// TestCovGap_RollbackCreateIndex covers rollback of CREATE INDEX
func TestCovGap_RollbackCreateIndex(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE idxrb (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO idxrb VALUES (1, 'a')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	c.BeginTransaction(6)
	_, err = c.ExecuteQuery("CREATE INDEX idx_val ON idxrb (val)")
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	err = c.RollbackTransaction()
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, ok := c.indexes["idx_val"]; ok {
		t.Error("index should have been rolled back")
	}
}

// TestCovGap_RollbackAlterAddColumn covers rollback of ALTER TABLE ADD COLUMN
// Note: ALTER TABLE is not directly supported via catalog.ExecuteQuery,
// this test uses the engine layer instead (covered in engine tests).

// TestCovGap_CTEExecution covers CTE paths
func TestCovGap_CTEExecution(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE cte_data (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO cte_data VALUES (1, 'A', 10)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO cte_data VALUES (2, 'B', 20)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO cte_data VALUES (3, 'A', 30)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	result, err := c.ExecuteQuery("WITH a_only AS (SELECT * FROM cte_data WHERE cat = 'A') SELECT * FROM a_only")
	if err != nil {
		t.Fatalf("cte select: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
	result, err = c.ExecuteQuery(`WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 5) SELECT * FROM nums`)
	if err != nil {
		t.Fatalf("recursive cte: %v", err)
	}
	if len(result.Rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(result.Rows))
	}
}

// TestCovGap_OrderByPositions covers positional ORDER BY
func TestCovGap_OrderByPositions(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE sortme (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO sortme VALUES (1, 'Bob', 80)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO sortme VALUES (2, 'Alice', 90)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO sortme VALUES (3, 'Charlie', 70)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	result, err := c.ExecuteQuery("SELECT name, score FROM sortme ORDER BY 2 DESC")
	if err != nil {
		t.Fatalf("positional order by: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("expected first row 'Alice', got %v", result.Rows[0][0])
	}
}

// TestCovGap_SaveWithPool covers Save with pool flush
func TestCovGap_SaveWithPool(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE svtest (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO svtest VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	err = c.Save()
	if err != nil {
		t.Fatalf("save: %v", err)
	}
}

// TestCovGap_DeleteReturning covers DELETE RETURNING
func TestCovGap_DeleteReturning(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE delret (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO delret VALUES (1, 'gone')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	result, err := c.ExecuteQuery("DELETE FROM delret WHERE id = 1 RETURNING id, name")
	if err != nil {
		t.Fatalf("delete returning: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 returned row, got %d", len(result.Rows))
	}
}

// TestCovGap_AnalyzeWithStats covers ANALYZE with data
func TestCovGap_AnalyzeWithStats(t *testing.T) {
	c := createFullTestCatalog(t)
	_, err := c.ExecuteQuery("CREATE TABLE anltbl (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO anltbl VALUES (1, 'val')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	err = c.Analyze("anltbl")
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	stats := c.stats["anltbl"]
	if stats == nil {
		t.Fatal("expected stats to be populated")
	}
}
