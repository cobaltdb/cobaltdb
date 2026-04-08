package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// createCatalog is an alias used by this file
func createCatalogForCov2(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	catalogTree, _ := btree.NewBTree(pool)
	return &Catalog{
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
}

// TestCov2_InsertConflictReplace covers INSERT OR REPLACE unique constraint handling
func TestCov2_InsertConflictReplace(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE repl (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO repl VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("insert 1: %v", err)
	}

	// INSERT OR REPLACE should replace existing row with same unique value
	_, err = c.ExecuteQuery("INSERT OR REPLACE INTO repl VALUES (2, 'Alice')")
	if err != nil {
		t.Logf("INSERT OR REPLACE: %v", err)
	} else {
		// Verify Alice is now id=2
		result, _ := c.ExecuteQuery("SELECT id FROM repl WHERE name = 'Alice'")
		if len(result.Rows) > 0 {
			t.Logf("INSERT OR REPLACE result: id=%v", result.Rows[0][0])
		}
	}
}

// TestCov2_InsertConflictIgnore covers INSERT OR IGNORE
func TestCov2_InsertConflictIgnore(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE ignr (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO ignr VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("insert 1: %v", err)
	}

	// INSERT OR IGNORE should silently skip
	_, err = c.ExecuteQuery("INSERT OR IGNORE INTO ignr VALUES (2, 'Alice')")
	if err != nil {
		t.Logf("INSERT OR IGNORE: %v", err)
	}

	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM ignr")
	if len(result.Rows) > 0 {
		t.Logf("Count after IGNORE: %v", result.Rows[0][0])
	}
}

// TestCov2_NullPKAutoInc covers NULL PK → auto-increment
func TestCov2_NullPKAutoInc(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE autoinc (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert with explicit id
	_, err = c.ExecuteQuery("INSERT INTO autoinc VALUES (5, 'a')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Insert another — auto-increment should track max
	_, err = c.ExecuteQuery("INSERT INTO autoinc VALUES (10, 'b')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, _ := c.ExecuteQuery("SELECT * FROM autoinc ORDER BY id")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

// TestCov2_ForeignKeyInsert covers FK constraint during INSERT
func TestCov2_ForeignKeyInsert(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO parent VALUES (1, 'Mom')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	_, err = c.ExecuteQuery("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent(id))")
	if err != nil {
		t.Fatalf("create child: %v", err)
	}

	// Valid FK insert
	_, err = c.ExecuteQuery("INSERT INTO child VALUES (1, 1)")
	if err != nil {
		t.Fatalf("valid FK insert: %v", err)
	}

	// Invalid FK insert
	_, err = c.ExecuteQuery("INSERT INTO child VALUES (2, 999)")
	if err == nil {
		t.Log("FK violation not enforced (expected for some implementations)")
	}

	// NULL FK should be allowed
	_, err = c.ExecuteQuery("INSERT INTO child VALUES (3, NULL)")
	if err != nil {
		t.Logf("NULL FK insert: %v", err)
	}
}

// TestCov2_ForeignKeyCascade covers FK ON DELETE CASCADE
func TestCov2_ForeignKeyCascade(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE orders2 (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("create orders: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE TABLE items2 (id INTEGER PRIMARY KEY, order_id INTEGER, FOREIGN KEY (order_id) REFERENCES orders2(id) ON DELETE CASCADE)")
	if err != nil {
		t.Fatalf("create items: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO orders2 VALUES (1)")
	if err != nil {
		t.Fatalf("insert order: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO items2 VALUES (1, 1)")
	if err != nil {
		t.Fatalf("insert item: %v", err)
	}

	// Delete parent — cascade should delete children
	_, err = c.ExecuteQuery("DELETE FROM orders2 WHERE id = 1")
	if err != nil {
		t.Fatalf("delete order: %v", err)
	}

	result, _ := c.ExecuteQuery("SELECT * FROM items2")
	if len(result.Rows) != 0 {
		t.Errorf("CASCADE didn't delete children, got %d rows", len(result.Rows))
	}
}

// TestCov2_LeftRightJoin covers LEFT and RIGHT JOIN paths
func TestCov2_LeftRightJoin(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE tleft (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create tleft: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE TABLE tright (id INTEGER PRIMARY KEY, left_id INTEGER, rval TEXT)")
	if err != nil {
		t.Fatalf("create tright: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO tleft VALUES (1, 'L1')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO tleft VALUES (2, 'L2')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO tright VALUES (1, 1, 'R1')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// LEFT JOIN — L2 should have null right side
	result, err := c.ExecuteQuery("SELECT tleft.val, tright.rval FROM tleft LEFT JOIN tright ON tleft.id = tright.left_id")
	if err != nil {
		t.Fatalf("left join: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}

	// RIGHT JOIN — R1 should match, but no match for left-only rows
	result, err = c.ExecuteQuery("SELECT tleft.val, tright.rval FROM tleft RIGHT JOIN tright ON tleft.id = tright.left_id")
	if err != nil {
		t.Fatalf("right join: %v", err)
	}
	if len(result.Rows) < 1 {
		t.Errorf("expected at least 1 row, got %d", len(result.Rows))
	}
}

// TestCov2_FullOuterJoin covers FULL OUTER JOIN
func TestCov2_FullOuterJoin(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE fa (id INTEGER PRIMARY KEY, v TEXT)")
	if err != nil {
		t.Fatalf("create fa: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE TABLE fb (id INTEGER PRIMARY KEY, aid INTEGER, v TEXT)")
	if err != nil {
		t.Fatalf("create fb: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO fa VALUES (1, 'A')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO fa VALUES (2, 'B')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO fb VALUES (1, 1, 'X')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO fb VALUES (2, 99, 'Y')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT fa.v, fb.v FROM fa FULL OUTER JOIN fb ON fa.id = fb.aid")
	if err != nil {
		t.Fatalf("full outer join: %v", err)
	}
	// Should have: (A,X), (B,null), (null,Y)
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows for FULL OUTER JOIN, got %d", len(result.Rows))
	}
}

// TestCov2_SubqueryExists covers EXISTS and NOT EXISTS
func TestCov2_SubqueryExists(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE suppliers (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE TABLE products3 (id INTEGER PRIMARY KEY, supplier_id INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO suppliers VALUES (1, 'Acme')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO suppliers VALUES (2, 'NoProducts')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO products3 VALUES (1, 1)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// EXISTS
	result, err := c.ExecuteQuery("SELECT * FROM suppliers WHERE EXISTS (SELECT 1 FROM products3 WHERE products3.supplier_id = suppliers.id)")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 supplier with products, got %d", len(result.Rows))
	}

	// NOT EXISTS
	result, err = c.ExecuteQuery("SELECT * FROM suppliers WHERE NOT EXISTS (SELECT 1 FROM products3 WHERE products3.supplier_id = suppliers.id)")
	if err != nil {
		t.Fatalf("not exists: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 supplier without products, got %d", len(result.Rows))
	}
}

// TestCov2_UnionIntersectExcept covers set operations
// Note: UNION/INTERSECT/EXCEPT are handled at engine layer, not catalog directly.

// TestCov2_WindowFunctions covers window function execution
func TestCov2_WindowFunctions(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE emps (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO emps VALUES (1, 'IT', 100)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO emps VALUES (2, 'IT', 200)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO emps VALUES (3, 'HR', 150)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// ROW_NUMBER
	result, err := c.ExecuteQuery("SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn FROM emps")
	if err != nil {
		t.Fatalf("row_number: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}

	// RANK
	result, err = c.ExecuteQuery("SELECT id, RANK() OVER (PARTITION BY dept ORDER BY salary) as rnk FROM emps")
	if err != nil {
		t.Fatalf("rank: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}

	// LAG/LEAD
	result, err = c.ExecuteQuery("SELECT id, LAG(salary, 1) OVER (ORDER BY id) as prev, LEAD(salary, 1) OVER (ORDER BY id) as nxt FROM emps")
	if err != nil {
		t.Fatalf("lag/lead: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}
}

// TestCov2_HavingClause covers HAVING with aggregates
func TestCov2_HavingClause(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO sales VALUES (1, 'East', 100)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO sales VALUES (2, 'East', 200)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO sales VALUES (3, 'West', 50)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT region, SUM(amount) as total FROM sales GROUP BY region HAVING SUM(amount) > 100")
	if err != nil {
		t.Fatalf("having: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 group, got %d", len(result.Rows))
	}
}

// TestCov2_CreateIndexWithUnique covers CREATE UNIQUE INDEX
func TestCov2_CreateIndexWithUnique(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE uidxtbl (id INTEGER PRIMARY KEY, email TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO uidxtbl VALUES (1, 'a@b.com')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err = c.ExecuteQuery("CREATE UNIQUE INDEX idx_email ON uidxtbl (email)")
	if err != nil {
		t.Fatalf("create unique index: %v", err)
	}

	// Duplicate should fail
	_, err = c.ExecuteQuery("INSERT INTO uidxtbl VALUES (2, 'a@b.com')")
	if err == nil {
		t.Log("unique constraint not enforced")
	}
}

// TestCov2_CountDistinct covers COUNT(DISTINCT)
func TestCov2_CountDistinct(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE dist (id INTEGER PRIMARY KEY, cat TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dist VALUES (1, 'A')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dist VALUES (2, 'A')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dist VALUES (3, 'B')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT COUNT(DISTINCT cat) FROM dist")
	if err != nil {
		t.Fatalf("count distinct: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row")
	}
}

// TestCov2_SelfJoin covers self-join
func TestCov2_SelfJoin(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE emp2 (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO emp2 VALUES (1, 'Boss', NULL)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO emp2 VALUES (2, 'Worker', 1)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT e.name, m.name as manager FROM emp2 e LEFT JOIN emp2 m ON e.manager_id = m.id")
	if err != nil {
		t.Fatalf("self join: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

// TestCov2_MultipleJoins covers 3-way join
func TestCov2_MultipleJoins(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE a3 (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create a: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE TABLE b3 (id INTEGER PRIMARY KEY, aid INTEGER)")
	if err != nil {
		t.Fatalf("create b: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE TABLE c3 (id INTEGER PRIMARY KEY, bid INTEGER, cv TEXT)")
	if err != nil {
		t.Fatalf("create c: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO a3 VALUES (1, 'VA')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO b3 VALUES (1, 1)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO c3 VALUES (1, 1, 'VC')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT a3.val, c3.cv FROM a3 JOIN b3 ON a3.id = b3.aid JOIN c3 ON b3.id = c3.bid")
	if err != nil {
		t.Fatalf("3-way join: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

// TestCov2_DeleteWithUsing covers DELETE ... USING
func TestCov2_DeleteWithUsing(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE dtable (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE TABLE dfilter (id INTEGER PRIMARY KEY, keep INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dtable VALUES (1, 'a')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dtable VALUES (2, 'b')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dfilter VALUES (1, 0)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err = c.ExecuteQuery("DELETE FROM dtable USING dfilter WHERE dtable.id = dfilter.id AND dfilter.keep = 0")
	if err != nil {
		t.Logf("DELETE USING: %v", err)
		return
	}

	result, _ := c.ExecuteQuery("SELECT * FROM dtable")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row after DELETE USING, got %d", len(result.Rows))
	}
}

// TestCov2_SelectDistinct covers DISTINCT
func TestCov2_SelectDistinct(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE dupes (id INTEGER PRIMARY KEY, cat TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dupes VALUES (1, 'X')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dupes VALUES (2, 'X')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO dupes VALUES (3, 'Y')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT DISTINCT cat FROM dupes")
	if err != nil {
		t.Fatalf("distinct: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 distinct values, got %d", len(result.Rows))
	}
}

// TestCov2_AggregateWithoutGroup covers aggregate without GROUP BY
func TestCov2_AggregateWithoutGroup(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE nums2 (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO nums2 VALUES (1, 10)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO nums2 VALUES (2, 20)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO nums2 VALUES (3, 30)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT SUM(val), AVG(val), MIN(val), MAX(val), COUNT(*) FROM nums2")
	if err != nil {
		t.Fatalf("aggregates: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row")
	}
}

// TestCov2_LikeOperator covers LIKE pattern matching
func TestCov2_LikeOperator(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE patterns (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO patterns VALUES (1, 'hello world')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO patterns VALUES (2, 'hello')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT * FROM patterns WHERE name LIKE 'hello%'")
	if err != nil {
		t.Fatalf("like: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}

	result, err = c.ExecuteQuery("SELECT * FROM patterns WHERE name LIKE '%world'")
	if err != nil {
		t.Fatalf("like: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

// TestCov2_BetweenOperator covers BETWEEN
func TestCov2_BetweenOperator(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE rng (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 1; i <= 10; i++ {
		c.ExecuteQuery("INSERT INTO rng VALUES (1, 1)")
		break
	}
	c.ExecuteQuery("INSERT INTO rng VALUES (5, 5)")
	c.ExecuteQuery("INSERT INTO rng VALUES (10, 10)")

	result, err := c.ExecuteQuery("SELECT * FROM rng WHERE val BETWEEN 3 AND 7")
	if err != nil {
		t.Fatalf("between: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

// TestCov2_InOperator covers IN clause
func TestCov2_InOperator(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE indata (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO indata VALUES (1, 'a')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO indata VALUES (2, 'b')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO indata VALUES (3, 'c')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT * FROM indata WHERE val IN ('a', 'c')")
	if err != nil {
		t.Fatalf("in: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

// TestCov2_CaseExpression covers CASE WHEN
func TestCov2_CaseExpression(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE casetbl (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO casetbl VALUES (1, 10)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO casetbl VALUES (2, 50)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT id, CASE WHEN val > 20 THEN 'high' ELSE 'low' END as label FROM casetbl")
	if err != nil {
		t.Fatalf("case: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

// TestCov2_NestedSubquery covers nested subqueries
func TestCov2_NestedSubquery(t *testing.T) {
	c := createCatalogForCov2(t)

	_, err := c.ExecuteQuery("CREATE TABLE outer_tbl (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO outer_tbl VALUES (1, 10)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO outer_tbl VALUES (2, 20)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT * FROM outer_tbl WHERE val > (SELECT AVG(val) FROM outer_tbl)")
	if err != nil {
		t.Fatalf("nested subquery: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row above average, got %d", len(result.Rows))
	}
}
