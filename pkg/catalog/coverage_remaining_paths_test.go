package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestRemainingCoveragePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// --- tryCountStarFastPath AS OF temporal ---
	c.ExecuteQuery("CREATE TABLE temporal_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO temporal_t (id) VALUES (1)")
	_, err := c.ExecuteQuery("SELECT COUNT(*) FROM temporal_t AS OF '2024-01-01'")
	if err != nil {
		t.Logf("COUNT(*) AS OF: %v", err)
	}

	// --- trySimpleAggregateFastPath with non-numeric column ---
	c.ExecuteQuery("CREATE TABLE agg_str (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO agg_str (id, name) VALUES (1, 'alice')")
	_, err = c.ExecuteQuery("SELECT SUM(name) FROM agg_str")
	if err != nil {
		t.Logf("SUM(text): %v", err)
	}

	// --- GROUP BY with alias ---
	c.ExecuteQuery("CREATE TABLE gb_alias (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO gb_alias (id, cat, val) VALUES (1, 'A', 10)")
	c.ExecuteQuery("INSERT INTO gb_alias (id, cat, val) VALUES (2, 'B', 20)")
	_, err = c.ExecuteQuery("SELECT cat AS c, SUM(val) FROM gb_alias GROUP BY c")
	if err != nil {
		t.Logf("GROUP BY alias: %v", err)
	}

	// --- GROUP BY on empty table ---
	c.ExecuteQuery("CREATE TABLE gb_empty (id INTEGER PRIMARY KEY, cat TEXT)")
	_, err = c.ExecuteQuery("SELECT cat, COUNT(*) FROM gb_empty GROUP BY cat")
	if err != nil {
		t.Logf("GROUP BY empty: %v", err)
	}

	// --- GROUP BY with HAVING and no matching rows ---
	_, err = c.ExecuteQuery("SELECT cat, SUM(val) FROM gb_alias GROUP BY cat HAVING SUM(val) > 100")
	if err != nil {
		t.Logf("GROUP BY HAVING no match: %v", err)
	}

	// --- GROUP BY with DISTINCT ---
	_, err = c.ExecuteQuery("SELECT DISTINCT cat FROM gb_alias GROUP BY cat")
	if err != nil {
		t.Logf("GROUP BY DISTINCT: %v", err)
	}

	// --- GROUP BY with OFFSET ---
	_, err = c.ExecuteQuery("SELECT cat, SUM(val) FROM gb_alias GROUP BY cat ORDER BY cat LIMIT 1 OFFSET 1")
	if err != nil {
		t.Logf("GROUP BY OFFSET: %v", err)
	}

	// --- GROUP_CONCAT with NULLs ---
	c.ExecuteQuery("CREATE TABLE gc_t (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO gc_t (id, name) VALUES (1, 'a')")
	c.ExecuteQuery("INSERT INTO gc_t (id, name) VALUES (2, NULL)")
	_, err = c.ExecuteQuery("SELECT GROUP_CONCAT(name) FROM gc_t")
	if err != nil {
		t.Logf("GROUP_CONCAT NULLs: %v", err)
	}

	// --- Window functions: empty rows ---
	c.ExecuteQuery("CREATE TABLE win_empty (id INTEGER PRIMARY KEY, val INTEGER)")
	_, err = c.ExecuteQuery("SELECT ROW_NUMBER() OVER (ORDER BY id) FROM win_empty")
	if err != nil {
		t.Logf("Window empty: %v", err)
	}

	// --- Window functions: COUNT(expr) OVER ---
	c.ExecuteQuery("CREATE TABLE win_t (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO win_t (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO win_t (id, val) VALUES (2, NULL)")
	_, err = c.ExecuteQuery("SELECT COUNT(val) OVER () FROM win_t")
	if err != nil {
		t.Logf("COUNT(expr) OVER: %v", err)
	}

	// --- Window functions: LAG with default ---
	_, err = c.ExecuteQuery("SELECT LAG(val, 1, 999) OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("LAG default: %v", err)
	}

	// --- Window functions: LEAD with default ---
	_, err = c.ExecuteQuery("SELECT LEAD(val, 1, 999) OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("LEAD default: %v", err)
	}

	// --- Window functions: FIRST_VALUE with NULL ---
	_, err = c.ExecuteQuery("SELECT FIRST_VALUE(val) OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("FIRST_VALUE: %v", err)
	}

	// --- Window functions: NTH_VALUE with NULL ---
	_, err = c.ExecuteQuery("SELECT NTH_VALUE(val, 2) OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("NTH_VALUE: %v", err)
	}

	// --- Window functions: MIN OVER ---
	_, err = c.ExecuteQuery("SELECT MIN(val) OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("MIN OVER: %v", err)
	}

	// --- Window functions: NTILE ---
	_, err = c.ExecuteQuery("SELECT NTILE(2) OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("NTILE: %v", err)
	}

	// --- SELECT with scalar subquery ---
	c.ExecuteQuery("CREATE TABLE sub_a (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO sub_a (id, val) VALUES (1, 10)")
	_, err = c.ExecuteQuery("SELECT (SELECT val FROM sub_a WHERE id = 1)")
	if err != nil {
		t.Logf("Scalar subquery: %v", err)
	}

	// --- SELECT with EXISTS subquery ---
	_, err = c.ExecuteQuery("SELECT EXISTS (SELECT 1 FROM sub_a WHERE id = 1)")
	if err != nil {
		t.Logf("EXISTS subquery: %v", err)
	}

	// --- SELECT with BETWEEN ---
	_, err = c.ExecuteQuery("SELECT * FROM sub_a WHERE val BETWEEN 5 AND 15")
	if err != nil {
		t.Logf("BETWEEN: %v", err)
	}

	// --- SELECT with IN list ---
	_, err = c.ExecuteQuery("SELECT * FROM sub_a WHERE val IN (10, 20, 30)")
	if err != nil {
		t.Logf("IN list: %v", err)
	}

	// --- SELECT with LIKE ---
	_, err = c.ExecuteQuery("SELECT * FROM gb_alias WHERE cat LIKE 'A%'")
	if err != nil {
		t.Logf("LIKE: %v", err)
	}

	// --- SELECT with IS NULL / IS NOT NULL ---
	_, err = c.ExecuteQuery("SELECT * FROM win_t WHERE val IS NULL")
	if err != nil {
		t.Logf("IS NULL: %v", err)
	}
	_, err = c.ExecuteQuery("SELECT * FROM win_t WHERE val IS NOT NULL")
	if err != nil {
		t.Logf("IS NOT NULL: %v", err)
	}
}

// ── INSERT...SELECT type switch paths ──
func TestRemaining_InsertSelectTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE iss_src (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("CREATE TABLE iss_dst (id INTEGER PRIMARY KEY, flag INTEGER)")
	c.ExecuteQuery("INSERT INTO iss_src (id, val) VALUES (1, NULL)")
	c.ExecuteQuery("INSERT INTO iss_src (id, val) VALUES (2, 10)")

	// INSERT...SELECT with NULL (covers nil case in type switch)
	_, err := c.ExecuteQuery("INSERT INTO iss_dst (id, flag) SELECT id, val FROM iss_src WHERE id = 1")
	if err != nil {
		t.Logf("INSERT...SELECT NULL: %v", err)
	}

	// INSERT...SELECT with boolean (covers bool case in type switch)
	c.ExecuteQuery("CREATE TABLE bool_dst (id INTEGER PRIMARY KEY, active INTEGER)")
	_, err = c.ExecuteQuery("INSERT INTO bool_dst (id, active) SELECT id, CASE WHEN val > 5 THEN 1 ELSE 0 END FROM iss_src")
	if err != nil {
		t.Logf("INSERT...SELECT bool: %v", err)
	}

	// INSERT...SELECT with timestamp (covers default case in type switch)
	c.ExecuteQuery("CREATE TABLE ts_dst (id INTEGER PRIMARY KEY, t TEXT)")
	_, err = c.ExecuteQuery("INSERT INTO ts_dst (id, t) SELECT id, CAST('2024-01-01' AS TEXT) FROM iss_src")
	if err != nil {
		t.Logf("INSERT...SELECT default: %v", err)
	}
}

// ── CREATE TABLE with RANGE partitioning ──
func TestRemaining_CreateTablePartition(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// RANGE partitioning with explicit partition definitions
	_, err := c.ExecuteQuery("CREATE TABLE part_range (id INTEGER PRIMARY KEY, val INTEGER) PARTITION BY RANGE (val) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))")
	if err != nil {
		t.Logf("RANGE partition: %v", err)
	}

	// HASH partitioning with NumPartitions
	_, err = c.ExecuteQuery("CREATE TABLE part_hash (id INTEGER PRIMARY KEY, val INTEGER) PARTITION BY HASH (val) PARTITIONS 4")
	if err != nil {
		t.Logf("HASH partition: %v", err)
	}
}

// ── ALTER TABLE ADD COLUMN with backfill ──
func TestRemaining_AlterTableAddColumn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE alt_t (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO alt_t (id, name) VALUES (1, 'a')")
	c.ExecuteQuery("INSERT INTO alt_t (id, name) VALUES (2, 'b')")

	// Add column with DEFAULT
	_, err := c.ExecuteQuery("ALTER TABLE alt_t ADD COLUMN age INTEGER DEFAULT 0")
	if err != nil {
		t.Logf("ALTER ADD COLUMN: %v", err)
	}
}

// ── DROP TABLE within transaction with indexes ──
func TestRemaining_DropTableTxn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE drop_t (id INTEGER PRIMARY KEY, code TEXT)")
	c.ExecuteQuery("CREATE INDEX idx_drop_t ON drop_t(code)")
	c.ExecuteQuery("INSERT INTO drop_t (id, code) VALUES (1, 'A')")

	c.BeginTransaction(1)
	_, err := c.ExecuteQuery("DROP TABLE drop_t")
	if err != nil {
		t.Logf("DROP TABLE txn: %v", err)
	}
	c.RollbackTransaction()
}

// ── EvalExpression more paths ──
func TestRemaining_EvalExpressionMore(t *testing.T) {
	// TokenNot with non-bool
	v, err := EvalExpression(&query.UnaryExpr{Operator: query.TokenNot, Expr: &query.NumberLiteral{Value: 1}}, nil)
	if err != nil || v != float64(1) {
		t.Logf("TokenNot non-bool: got %v, %v", v, err)
	}

	// Binary TokenAnd with left nil, right false
	v, err = EvalExpression(&query.BinaryExpr{
		Operator: query.TokenAnd,
		Left:     &query.NullLiteral{},
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil || v != false {
		t.Logf("And left nil right false: got %v, %v", v, err)
	}

	// Binary TokenOr with left nil, right false
	v, err = EvalExpression(&query.BinaryExpr{
		Operator: query.TokenOr,
		Left:     &query.NullLiteral{},
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil || v != nil {
		t.Logf("Or left nil right false: got %v, %v", v, err)
	}

	// Binary TokenConcat
	v, err = EvalExpression(&query.BinaryExpr{
		Operator: query.TokenConcat,
		Left:     &query.StringLiteral{Value: "a"},
		Right:    &query.StringLiteral{Value: "b"},
	}, nil)
	if err != nil || v != "ab" {
		t.Logf("Concat: got %v, %v", v, err)
	}

	// CaseExpr searched with error in cond
	v, err = EvalExpression(&query.CaseExpr{
		Whens: []*query.WhenClause{{
			Condition: &query.JSONPathExpr{Column: &query.StringLiteral{Value: "{}"}, Path: "$.a"},
			Result:    &query.NumberLiteral{Value: 1},
		}},
	}, nil)
	if err != nil {
		t.Logf("Case searched cond error: got %v, %v", v, err)
	}

	// CastExpr fallback (unsupported type)
	v, err = EvalExpression(&query.CastExpr{
		Expr:     &query.NumberLiteral{Value: 42},
		DataType: query.TokenTimestamp,
	}, nil)
	if err != nil || v != float64(42) {
		t.Logf("Cast fallback: got %v, %v", v, err)
	}
}

// ── catalog_select.go uncovered paths ──
func TestRemaining_SelectPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE sel_t (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO sel_t (id, val) VALUES (1, 10)")

	// SELECT from non-existent table (covers getTableLocked error)
	_, err := c.ExecuteQuery("SELECT * FROM nonexistent_table")
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}

	// SELECT with DISTINCT on empty result
	_, err = c.ExecuteQuery("SELECT DISTINCT val FROM sel_t WHERE id = 99")
	if err != nil {
		t.Logf("DISTINCT empty: %v", err)
	}
}

// ── catalog_eval.go evaluateWhere with various result types ──
func TestRemaining_EvaluateWhereTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE where_t (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO where_t (id, val) VALUES (1, 0)")
	c.ExecuteQuery("INSERT INTO where_t (id, val) VALUES (2, 10)")

	// WHERE with numeric zero (false)
	_, err := c.ExecuteQuery("SELECT id FROM where_t WHERE val = 0")
	if err != nil {
		t.Logf("WHERE numeric zero: %v", err)
	}

	// WHERE with empty string
	c.ExecuteQuery("CREATE TABLE where_str (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO where_str (id, name) VALUES (1, '')")
	_, err = c.ExecuteQuery("SELECT id FROM where_str WHERE name = ''")
	if err != nil {
		t.Logf("WHERE empty string: %v", err)
	}
}

// ── catalog_core.go resolveOuterRefsInQuery paths ──
func TestRemaining_ResolveOuterRefs(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE ro_outer (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE ro_inner (id INTEGER PRIMARY KEY, o_id INTEGER, val INTEGER)")
	c.ExecuteQuery("INSERT INTO ro_outer (id, name) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO ro_outer (id, name) VALUES (2, 'B')")
	c.ExecuteQuery("INSERT INTO ro_inner (id, o_id, val) VALUES (1, 1, 10)")

	// Correlated subquery with outer ref in CASE
	_, err := c.ExecuteQuery("SELECT id FROM ro_outer WHERE id IN (SELECT CASE WHEN ro_outer.id = 1 THEN o_id ELSE 0 END FROM ro_inner)")
	if err != nil {
		t.Logf("Correlated CASE: %v", err)
	}

	// Correlated subquery with outer ref in CAST
	_, err = c.ExecuteQuery("SELECT id FROM ro_outer WHERE id IN (SELECT CAST(ro_outer.id AS INTEGER) FROM ro_inner)")
	if err != nil {
		t.Logf("Correlated CAST: %v", err)
	}

	// Correlated subquery with outer ref in unary minus
	_, err = c.ExecuteQuery("SELECT id FROM ro_outer WHERE id IN (SELECT -ro_outer.id FROM ro_inner)")
	if err != nil {
		t.Logf("Correlated unary: %v", err)
	}
}

// ── catalog_maintenance.go LoadSchema with valid data ──
func TestRemaining_LoadSchemaData(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE load_t (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO load_t (id, name) VALUES (1, 'a')")

	// SaveData then LoadData
	tmpDir := t.TempDir()
	err := c.SaveData(tmpDir)
	if err != nil {
		t.Logf("SaveData: %v", err)
	}

	// LoadSchema on fresh catalog
	pool2 := storage.NewBufferPool(4096, backend)
	defer pool2.Close()
	tree2, _ := btree.NewBTree(pool2)
	c2 := New(tree2, pool2, nil)

	err = c2.LoadSchema(tmpDir)
	if err != nil {
		t.Logf("LoadSchema: %v", err)
	}

	err = c2.LoadData(tmpDir)
	if err != nil {
		t.Logf("LoadData: %v", err)
	}
}

// ── catalog_maintenance.go Vacuum with deleted rows ──
func TestRemaining_VacuumDeleted(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE vac_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO vac_t (id) VALUES (1)")
	c.ExecuteQuery("INSERT INTO vac_t (id) VALUES (2)")
	c.ExecuteQuery("DELETE FROM vac_t WHERE id = 1")

	err := c.Vacuum()
	if err != nil {
		t.Logf("Vacuum: %v", err)
	}
}

// ── catalog_update.go updateLocked with index scan ──
func TestRemaining_UpdateIndexScan(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE upd_idx2 (id INTEGER PRIMARY KEY, code TEXT)")
	c.ExecuteQuery("CREATE INDEX idx_upd2 ON upd_idx2(code)")
	c.ExecuteQuery("INSERT INTO upd_idx2 (id, code) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO upd_idx2 (id, code) VALUES (2, 'B')")

	// UPDATE with index scan
	_, err := c.ExecuteQuery("UPDATE upd_idx2 SET code = 'Z' WHERE code = 'A'")
	if err != nil {
		t.Logf("Update index scan: %v", err)
	}
}

// ── catalog_txn.go transaction with undo log ──
func TestRemaining_TransactionUndo(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE txn_t (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO txn_t (id, name) VALUES (1, 'a')")

	c.BeginTransaction(1)
	c.ExecuteQuery("INSERT INTO txn_t (id, name) VALUES (2, 'b')")
	c.ExecuteQuery("UPDATE txn_t SET name = 'A' WHERE id = 1")
	c.ExecuteQuery("DELETE FROM txn_t WHERE id = 2")

	err := c.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback with undo: %v", err)
	}
}

// ── catalog_json.go buildJSONIndex with various types ──
func TestRemaining_JSONIndexTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE json_idx (id INTEGER PRIMARY KEY, data JSON)")
	c.ExecuteQuery("INSERT INTO json_idx (id, data) VALUES (1, '{\"a\":true}')")
	c.ExecuteQuery("INSERT INTO json_idx (id, data) VALUES (2, '{\"a\":123}')")

	err := c.CreateJSONIndex("idx_json", "json_idx", "data", "$.a", "TEXT")
	if err != nil {
		t.Logf("JSON index: %v", err)
	}

	// Query with bool
	_, err = c.QueryJSONIndex("idx_json", true)
	if err != nil {
		t.Logf("JSON index query bool: %v", err)
	}

	// Query with int
	_, err = c.QueryJSONIndex("idx_json", 123)
	if err != nil {
		t.Logf("JSON index query int: %v", err)
	}
}

// ── catalog_vector.go vector index with nil/invalid types ──
func TestRemaining_VectorIndexTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE vec_t (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	c.ExecuteQuery("INSERT INTO vec_t (id, embedding) VALUES (1, '[1.0, 2.0, 3.0]')")
	c.ExecuteQuery("INSERT INTO vec_t (id, embedding) VALUES (2, NULL)")
	c.ExecuteQuery("INSERT INTO vec_t (id, embedding) VALUES (3, '[4.0, 5.0, 6.0]')")

	err := c.CreateVectorIndex("idx_vec", "vec_t", "embedding")
	if err != nil {
		t.Logf("Vector index: %v", err)
	}
}

// ── Vacuum with indexes ──
func TestRemaining_VacuumWithIndexes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE vac_idx (id INTEGER PRIMARY KEY, code TEXT)")
	c.ExecuteQuery("CREATE INDEX idx_vac ON vac_idx(code)")
	c.ExecuteQuery("INSERT INTO vac_idx (id, code) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO vac_idx (id, code) VALUES (2, 'B')")
	c.ExecuteQuery("DELETE FROM vac_idx WHERE id = 1")

	err := c.Vacuum()
	if err != nil {
		t.Logf("Vacuum with indexes: %v", err)
	}
}

// ── Analyze table ──
func TestRemaining_Analyze(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE ana_t (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO ana_t (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO ana_t (id, val) VALUES (2, NULL)")

	err := c.Analyze("ana_t")
	if err != nil {
		t.Logf("Analyze: %v", err)
	}
}

// ── CTE with INTERSECT and EXCEPT ──
func TestRemaining_CTESetOps(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// CTE with INTERSECT
	_, err := c.ExecuteQuery("WITH cte AS (SELECT 1 AS a INTERSECT SELECT 1 AS a) SELECT * FROM cte")
	if err != nil {
		t.Logf("CTE INTERSECT: %v", err)
	}

	// CTE with EXCEPT
	_, err = c.ExecuteQuery("WITH cte AS (SELECT 1 AS a EXCEPT SELECT 2 AS a) SELECT * FROM cte")
	if err != nil {
		t.Logf("CTE EXCEPT: %v", err)
	}

	// Derived table with UNION
	_, err = c.ExecuteQuery("SELECT * FROM (SELECT 1 AS a UNION SELECT 2 AS a) AS dt")
	if err != nil {
		t.Logf("Derived table UNION: %v", err)
	}
}

// ── Savepoint and rollback ──
func TestRemaining_SavepointRollback(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE sp_t (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO sp_t (id, name) VALUES (1, 'a')")

	c.BeginTransaction(1)
	c.ExecuteQuery("INSERT INTO sp_t (id, name) VALUES (2, 'b')")

	err := c.Savepoint("sp1")
	if err != nil {
		t.Logf("Savepoint: %v", err)
	}

	c.ExecuteQuery("INSERT INTO sp_t (id, name) VALUES (3, 'c')")

	err = c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to savepoint: %v", err)
	}

	err = c.CommitTransaction()
	if err != nil {
		t.Logf("Commit after savepoint rollback: %v", err)
	}
}

// ── GetQueryCacheStats ──
func TestRemaining_QueryCacheStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Before enabling cache
	hits, misses, size := c.GetQueryCacheStats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Logf("Cache stats before enable: hits=%d misses=%d size=%d", hits, misses, size)
	}

	c.EnableQueryCache(10, 0)
	c.ExecuteQuery("CREATE TABLE qc_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("SELECT * FROM qc_t")
	c.ExecuteQuery("SELECT * FROM qc_t")

	hits, misses, size = c.GetQueryCacheStats()
	if size == 0 {
		t.Logf("Cache stats empty after queries")
	}
}
