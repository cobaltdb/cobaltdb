package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func createCatalogForCovBoost(t *testing.T) *Catalog {
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
		deadTuples:        make(map[string]int64),
		liveTuples:        make(map[string]int64),
	}
}

// === updateLocked / processUpdateRow coverage ===

func TestCovBoost_UpdateBasic(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE upd_t (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
	c.ExecuteQuery("INSERT INTO upd_t VALUES (1, 'alice', 90.0)")
	c.ExecuteQuery("INSERT INTO upd_t VALUES (2, 'bob', 80.0)")

	r, err := c.ExecuteQuery("UPDATE upd_t SET score = 95.0 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected result")
	}

	r2, _ := c.ExecuteQuery("SELECT score FROM upd_t WHERE id = 1")
	if len(r2.Rows) != 1 || r2.Rows[0][0] == nil {
		t.Fatalf("expected score=95, got %v", r2.Rows)
	}
}

func TestCovBoost_UpdateMultipleColumns(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE upd_mc (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")
	c.ExecuteQuery("INSERT INTO upd_mc VALUES (1, 'x', 10)")

	_, err := c.ExecuteQuery("UPDATE upd_mc SET a = 'y', b = 20 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}

	r, _ := c.ExecuteQuery("SELECT a, b FROM upd_mc WHERE id = 1")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestCovBoost_UpdateExpressionValue(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE upd_expr (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO upd_expr VALUES (1, 10)")

	_, err := c.ExecuteQuery("UPDATE upd_expr SET val = val + 5 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}

	r, _ := c.ExecuteQuery("SELECT val FROM upd_expr WHERE id = 1")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row")
	}
}

func TestCovBoost_UpdateWithIndex(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE upd_idx (id INTEGER PRIMARY KEY, code TEXT)")
	c.ExecuteQuery("CREATE INDEX idx_code ON upd_idx (code)")
	c.ExecuteQuery("INSERT INTO upd_idx VALUES (1, 'AAA')")
	c.ExecuteQuery("INSERT INTO upd_idx VALUES (2, 'BBB')")

	_, err := c.ExecuteQuery("UPDATE upd_idx SET code = 'CCC' WHERE code = 'AAA'")
	if err != nil {
		t.Fatal(err)
	}
}

func TestCovBoost_UpdateNonexistentColumn(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE upd_nc (id INTEGER PRIMARY KEY)")

	_, err := c.ExecuteQuery("UPDATE upd_nc SET badcol = 1 WHERE id = 1")
	if err == nil {
		t.Error("expected error for nonexistent column")
	}
}

func TestCovBoost_DeleteBasic(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE del_t (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO del_t VALUES (1, 100)")
	c.ExecuteQuery("INSERT INTO del_t VALUES (2, 200)")

	_, err := c.ExecuteQuery("DELETE FROM del_t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}

	r, _ := c.ExecuteQuery("SELECT id FROM del_t")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after delete, got %d", len(r.Rows))
	}
}

func TestCovBoost_DeleteWithIndex(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE del_idx (id INTEGER PRIMARY KEY, code TEXT)")
	c.ExecuteQuery("CREATE INDEX idx_d ON del_idx (code)")
	c.ExecuteQuery("INSERT INTO del_idx VALUES (1, 'X')")
	c.ExecuteQuery("INSERT INTO del_idx VALUES (2, 'Y')")

	_, err := c.ExecuteQuery("DELETE FROM del_idx WHERE code = 'X'")
	if err != nil {
		t.Fatal(err)
	}
}

// === Transaction / Rollback coverage ===

func TestCovBoost_RollbackInsert(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE rb_t (id INTEGER PRIMARY KEY, val TEXT)")

	c.BeginTransaction(uint64(1))
	c.ExecuteQuery("INSERT INTO rb_t VALUES (1, 'before_rollback')")
	c.RollbackTransaction()

	r, _ := c.ExecuteQuery("SELECT * FROM rb_t")
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows after rollback, got %d", len(r.Rows))
	}
}

func TestCovBoost_RollbackUpdate(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE rb_u (id INTEGER PRIMARY KEY, val TEXT)")
	c.ExecuteQuery("INSERT INTO rb_u VALUES (1, 'original')")

	c.BeginTransaction(uint64(2))
	c.ExecuteQuery("UPDATE rb_u SET val = 'modified' WHERE id = 1")
	c.RollbackTransaction()

	r, _ := c.ExecuteQuery("SELECT val FROM rb_u WHERE id = 1")
	if len(r.Rows) != 1 || r.Rows[0][0] != "original" {
		t.Fatalf("expected 'original' after rollback, got %v", r.Rows)
	}
}

func TestCovBoost_RollbackDelete(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE rb_d (id INTEGER PRIMARY KEY, val TEXT)")
	c.ExecuteQuery("INSERT INTO rb_d VALUES (1, 'keep_me')")

	c.BeginTransaction(uint64(3))
	c.ExecuteQuery("DELETE FROM rb_d WHERE id = 1")
	c.RollbackTransaction()

	r, _ := c.ExecuteQuery("SELECT val FROM rb_d WHERE id = 1")
	if len(r.Rows) != 1 {
		t.Fatalf("expected row restored after rollback, got %d rows", len(r.Rows))
	}
}

func TestCovBoost_CommitTransaction(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE ct_t (id INTEGER PRIMARY KEY)")

	c.BeginTransaction(uint64(4))
	c.ExecuteQuery("INSERT INTO ct_t VALUES (1)")
	c.CommitTransaction()

	r, _ := c.ExecuteQuery("SELECT * FROM ct_t")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after commit, got %d", len(r.Rows))
	}
}

func TestCovBoost_SavepointRollback(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE sp_t (id INTEGER PRIMARY KEY)")

	c.BeginTransaction(uint64(5))
	c.ExecuteQuery("INSERT INTO sp_t VALUES (1)")
	c.Savepoint("sp1")
	c.ExecuteQuery("INSERT INTO sp_t VALUES (2)")
	c.RollbackToSavepoint("sp1")

	r, _ := c.ExecuteQuery("SELECT * FROM sp_t")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after savepoint rollback, got %d", len(r.Rows))
	}
	c.CommitTransaction()
}

// === CTE coverage ===

func TestCovBoost_CTEBasic(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE cte_t (id INTEGER PRIMARY KEY, grp INTEGER)")
	c.ExecuteQuery("INSERT INTO cte_t VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO cte_t VALUES (2, 20)")

	r, err := c.ExecuteQuery("WITH cte AS (SELECT id FROM cte_t) SELECT * FROM cte")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows from CTE, got %d", len(r.Rows))
	}
}

func TestCovBoost_CTEMultiple(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE cte_m (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO cte_m VALUES (1)")
	c.ExecuteQuery("INSERT INTO cte_m VALUES (2)")

	r, err := c.ExecuteQuery("WITH a AS (SELECT id FROM cte_m), b AS (SELECT id FROM a) SELECT * FROM b")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows from multi-CTE, got %d", len(r.Rows))
	}
}

func TestCovBoost_CTEWithUnion(t *testing.T) {
	c := createCatalogForCovBoost(t)
	r, err := c.ExecuteQuery("WITH cte AS (SELECT 1 AS x UNION ALL SELECT 2 AS x) SELECT * FROM cte")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) < 2 {
		t.Fatalf("expected >= 2 rows from CTE with UNION, got %d", len(r.Rows))
	}
}

// === GROUP BY + HAVING + ORDER BY coverage ===

func TestCovBoost_GroupByHaving(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE gb_t (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	c.ExecuteQuery("INSERT INTO gb_t VALUES (1, 'eng', 100)")
	c.ExecuteQuery("INSERT INTO gb_t VALUES (2, 'eng', 120)")
	c.ExecuteQuery("INSERT INTO gb_t VALUES (3, 'hr', 80)")

	r, err := c.ExecuteQuery("SELECT dept, SUM(salary) AS total FROM gb_t GROUP BY dept HAVING total > 90")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 group (eng), got %d rows: %v", len(r.Rows), r.Rows)
	}
}

func TestCovBoost_GroupByOrderBy(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE gbo_t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO gbo_t VALUES (1, 'b', 10)")
	c.ExecuteQuery("INSERT INTO gbo_t VALUES (2, 'a', 20)")
	c.ExecuteQuery("INSERT INTO gbo_t VALUES (3, 'b', 30)")

	r, err := c.ExecuteQuery("SELECT cat, SUM(val) AS total FROM gbo_t GROUP BY cat ORDER BY cat ASC")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(r.Rows))
	}
}

func TestCovBoost_GroupByMultipleColumns(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE gbm_t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO gbm_t VALUES (1, 'x', 'y', 10)")
	c.ExecuteQuery("INSERT INTO gbm_t VALUES (2, 'x', 'z', 20)")
	c.ExecuteQuery("INSERT INTO gbm_t VALUES (3, 'x', 'y', 30)")

	r, err := c.ExecuteQuery("SELECT a, b, SUM(val) FROM gbm_t GROUP BY a, b")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(r.Rows))
	}
}

// === Vacuum coverage ===

func TestCovBoost_VacuumBasic(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE vac_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO vac_t VALUES (1)")
	c.ExecuteQuery("INSERT INTO vac_t VALUES (2)")
	c.ExecuteQuery("DELETE FROM vac_t WHERE id = 1")

	_, err := c.ExecuteQuery("VACUUM vac_t")
	if err != nil {
		t.Logf("VACUUM: %v", err)
	}
}

// === DISTINCT coverage ===

func TestCovBoost_Distinct(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE dist_t (id INTEGER PRIMARY KEY, val TEXT)")
	c.ExecuteQuery("INSERT INTO dist_t VALUES (1, 'a')")
	c.ExecuteQuery("INSERT INTO dist_t VALUES (2, 'a')")
	c.ExecuteQuery("INSERT INTO dist_t VALUES (3, 'b')")

	r, err := c.ExecuteQuery("SELECT DISTINCT val FROM dist_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 distinct values, got %d", len(r.Rows))
	}
}

// === JOIN + GROUP BY coverage ===

func TestCovBoost_JoinGroupBy(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE jg_a (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE jg_b (id INTEGER PRIMARY KEY, a_id INTEGER, amount INTEGER)")
	c.ExecuteQuery("INSERT INTO jg_a VALUES (1, 'alice')")
	c.ExecuteQuery("INSERT INTO jg_a VALUES (2, 'bob')")
	c.ExecuteQuery("INSERT INTO jg_b VALUES (10, 1, 50)")
	c.ExecuteQuery("INSERT INTO jg_b VALUES (11, 1, 30)")
	c.ExecuteQuery("INSERT INTO jg_b VALUES (12, 2, 70)")

	r, err := c.ExecuteQuery("SELECT jg_a.name, SUM(jg_b.amount) AS total FROM jg_a INNER JOIN jg_b ON jg_a.id = jg_b.a_id GROUP BY jg_a.name")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d: %v", len(r.Rows), r.Rows)
	}
}

// === Subquery coverage ===

func TestCovBoost_SubqueryWhere(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE sq_a (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO sq_a VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO sq_a VALUES (2, 20)")
	c.ExecuteQuery("INSERT INTO sq_a VALUES (3, 30)")

	r, err := c.ExecuteQuery("SELECT * FROM sq_a WHERE val > (SELECT AVG(val) FROM sq_a)")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) < 1 {
		t.Fatalf("expected rows with val > avg, got %d", len(r.Rows))
	}
}

// === Scalar functions coverage ===

func TestCovBoost_ScalarFunctions(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE fn_t (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
	c.ExecuteQuery("INSERT INTO fn_t VALUES (1, 'alice', 95.5)")
	c.ExecuteQuery("INSERT INTO fn_t VALUES (2, 'BOB', 82.0)")

	// UPPER
	r, err := c.ExecuteQuery("SELECT UPPER(name) FROM fn_t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row")
	}

	// LOWER
	r, err = c.ExecuteQuery("SELECT LOWER(name) FROM fn_t WHERE id = 2")
	if err != nil {
		t.Fatal(err)
	}

	// COALESCE
	r, err = c.ExecuteQuery("SELECT COALESCE(score, 0) FROM fn_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows")
	}
}

// === Arithmetic in SELECT ===

func TestCovBoost_ArithmeticSelect(t *testing.T) {
	c := createCatalogForCovBoost(t)

	r, err := c.ExecuteQuery("SELECT 1 + 2, 10 - 3, 4 * 5, 10 / 2")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row with arithmetic results")
	}
}

// === CAST coverage ===

func TestCovBoost_CastExpression(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE cast_t (id INTEGER PRIMARY KEY, val TEXT)")
	c.ExecuteQuery("INSERT INTO cast_t VALUES (1, '42')")

	r, err := c.ExecuteQuery("SELECT CAST(val AS INTEGER) FROM cast_t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row")
	}
}

// === INSERT with DEFAULT coverage ===

func TestCovBoost_InsertDefault(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE def_t (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unknown', val INTEGER DEFAULT 0)")

	_, err := c.ExecuteQuery("INSERT INTO def_t (id) VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}

	r, _ := c.ExecuteQuery("SELECT name, val FROM def_t WHERE id = 1")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row")
	}
}

// === LIKE / BETWEEN / IN coverage ===

func TestCovBoost_LikeBetweenIn(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE lbi_t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO lbi_t VALUES (1, 'apple', 10)")
	c.ExecuteQuery("INSERT INTO lbi_t VALUES (2, 'application', 20)")
	c.ExecuteQuery("INSERT INTO lbi_t VALUES (3, 'banana', 30)")

	// LIKE
	r, err := c.ExecuteQuery("SELECT * FROM lbi_t WHERE name LIKE 'app%'")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("LIKE: expected 2 rows, got %d", len(r.Rows))
	}

	// BETWEEN
	r, err = c.ExecuteQuery("SELECT * FROM lbi_t WHERE val BETWEEN 15 AND 35")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("BETWEEN: expected 2 rows, got %d", len(r.Rows))
	}

	// IN
	r, err = c.ExecuteQuery("SELECT * FROM lbi_t WHERE val IN (10, 30)")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("IN: expected 2 rows, got %d", len(r.Rows))
	}
}

// === NULL handling coverage ===

func TestCovBoost_NullHandling(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE null_t (id INTEGER PRIMARY KEY, val TEXT)")
	c.ExecuteQuery("INSERT INTO null_t VALUES (1, NULL)")
	c.ExecuteQuery("INSERT INTO null_t VALUES (2, 'exists')")

	r, err := c.ExecuteQuery("SELECT * FROM null_t WHERE val IS NULL")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("IS NULL: expected 1 row, got %d", len(r.Rows))
	}

	r, err = c.ExecuteQuery("SELECT * FROM null_t WHERE val IS NOT NULL")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("IS NOT NULL: expected 1 row, got %d", len(r.Rows))
	}
}

// === COUNT(*) fast path ===

func TestCovBoost_CountStar(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE cnt_t (id INTEGER PRIMARY KEY)")
	for i := 0; i < 5; i++ {
		c.ExecuteQuery(fmt.Sprintf("INSERT INTO cnt_t VALUES (%d)", i+1))
	}

	r, err := c.ExecuteQuery("SELECT COUNT(*) FROM cnt_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row with count")
	}
}

// === LEFT JOIN coverage ===

func TestCovBoost_LeftJoin(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE lj_a (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE lj_b (id INTEGER PRIMARY KEY, a_id INTEGER, note TEXT)")
	c.ExecuteQuery("INSERT INTO lj_a VALUES (1, 'alice')")
	c.ExecuteQuery("INSERT INTO lj_a VALUES (2, 'bob')")
	c.ExecuteQuery("INSERT INTO lj_b VALUES (10, 1, 'note1')")

	r, err := c.ExecuteQuery("SELECT lj_a.name, lj_b.note FROM lj_a LEFT JOIN lj_b ON lj_a.id = lj_b.a_id")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows from LEFT JOIN, got %d", len(r.Rows))
	}
}

// === ROLLBACK CREATE/DROP TABLE ===

func TestCovBoost_RollbackCreateTable(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.BeginTransaction(uint64(6))
	c.ExecuteQuery("CREATE TABLE rb_ddl (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO rb_ddl VALUES (1)")
	c.RollbackTransaction()

	_, err := c.ExecuteQuery("SELECT * FROM rb_ddl")
	if err == nil {
		t.Error("expected error for rolled-back table")
	}
}

func TestCovBoost_RollbackDropTable(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE rb_drop (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO rb_drop VALUES (1)")

	c.BeginTransaction(uint64(7))
	c.ExecuteQuery("DROP TABLE rb_drop")
	c.RollbackTransaction()

	r, err := c.ExecuteQuery("SELECT * FROM rb_drop")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected row restored after rollback of DROP, got %d", len(r.Rows))
	}
}

// === context usage ===

func TestCovBoost_UpdateWithContext(t *testing.T) {
	c := createCatalogForCovBoost(t)
	c.ExecuteQuery("CREATE TABLE ctx_t (id INTEGER PRIMARY KEY, val TEXT)")
	c.ExecuteQuery("INSERT INTO ctx_t VALUES (1, 'old')")

	stmt, err := query.Parse("UPDATE ctx_t SET val = 'new' WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	updStmt := stmt.(*query.UpdateStmt)
	_, _, err = c.Update(context.Background(), updStmt, nil)
	if err != nil {
		t.Fatal(err)
	}
}
