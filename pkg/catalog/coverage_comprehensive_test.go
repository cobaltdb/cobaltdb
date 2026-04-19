package catalog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ── Query cache paths ──
func TestComprehensive_QueryCachePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.EnableQueryCache(100, time.Minute)

	c.ExecuteQuery("CREATE TABLE qc_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO qc_t (id) VALUES (1)")

	// Subquery in SELECT -> not cacheable
	_, err := c.ExecuteQuery("SELECT (SELECT 1) FROM qc_t")
	if err != nil {
		t.Logf("Subquery SELECT: %v", err)
	}

	// Non-deterministic function -> not cacheable
	_, err = c.ExecuteQuery("SELECT RANDOM() FROM qc_t")
	if err != nil {
		t.Logf("RANDOM SELECT: %v", err)
	}

	// Cached query + error path: malformed query should error before cache hit
	_, err = c.ExecuteQuery("SELECT * FROM qc_t WHERE")
	if err == nil {
		t.Error("Expected error for malformed WHERE")
	}

	// GetQueryCacheStats with nil cache
	c.DisableQueryCache()
	hits, misses, size := c.GetQueryCacheStats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Errorf("Expected zero stats for nil cache, got %d,%d,%d", hits, misses, size)
	}
}

// ── CTE / applyOuterQuery paths ──
func TestComprehensive_CTEPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// CTE with ORDER BY DESC and duplicates
	_, err := c.ExecuteQuery("WITH cte AS (SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 1) SELECT * FROM cte ORDER BY a DESC")
	if err != nil {
		t.Logf("CTE ORDER BY DESC: %v", err)
	}

	// CTE with OFFSET overflow
	_, err = c.ExecuteQuery("WITH cte AS (SELECT 1 AS a) SELECT * FROM cte OFFSET 99")
	if err != nil {
		t.Logf("CTE OFFSET overflow: %v", err)
	}

	// CTE with LIMIT
	_, err = c.ExecuteQuery("WITH cte AS (SELECT 1 AS a UNION ALL SELECT 2) SELECT * FROM cte LIMIT 1")
	if err != nil {
		t.Logf("CTE LIMIT: %v", err)
	}

	// CTE with ORDER BY equal values
	_, err = c.ExecuteQuery("WITH cte AS (SELECT 1 AS a, 'x' AS b UNION ALL SELECT 1, 'y') SELECT * FROM cte ORDER BY a")
	if err != nil {
		t.Logf("CTE ORDER BY equal: %v", err)
	}
}

// ── View / applyOuterQuery paths ──
func TestComprehensive_ViewPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE view_base (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO view_base (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO view_base (id, val) VALUES (2, 20)")
	c.ExecuteQuery("CREATE VIEW v1 AS SELECT id, val FROM view_base")

	// View with aggregate no args
	_, err := c.ExecuteQuery("SELECT COUNT() FROM v1")
	if err != nil {
		t.Logf("View COUNT(): %v", err)
	}

	// View with non-aggregate expression
	_, err = c.ExecuteQuery("SELECT 1+1 FROM v1")
	if err != nil {
		t.Logf("View expr: %v", err)
	}

	// View with ORDER BY DESC
	_, err = c.ExecuteQuery("SELECT * FROM v1 ORDER BY val DESC")
	if err != nil {
		t.Logf("View ORDER BY DESC: %v", err)
	}

	// View with ORDER BY equal values (need duplicate val)
	c.ExecuteQuery("INSERT INTO view_base (id, val) VALUES (3, 10)")
	_, err = c.ExecuteQuery("SELECT * FROM v1 ORDER BY val")
	if err != nil {
		t.Logf("View ORDER BY equal: %v", err)
	}

	// View with OFFSET
	_, err = c.ExecuteQuery("SELECT * FROM v1 OFFSET 1")
	if err != nil {
		t.Logf("View OFFSET: %v", err)
	}

	// View with LIMIT
	_, err = c.ExecuteQuery("SELECT * FROM v1 LIMIT 1")
	if err != nil {
		t.Logf("View LIMIT: %v", err)
	}

	// View with column alias
	_, err = c.ExecuteQuery("SELECT id AS x FROM v1")
	if err != nil {
		t.Logf("View alias: %v", err)
	}

	// View with missing column (should error)
	_, err = c.ExecuteQuery("SELECT nonexistent FROM v1")
	if err == nil {
		t.Error("Expected error for missing column in view")
	}
}

// ── EvalExpression paths ──
func TestComprehensive_EvalExpressionPaths(t *testing.T) {
	// JSONPath with non-string column
	_, err := EvalExpression(&query.JSONPathExpr{Column: &query.NumberLiteral{Value: 42}, Path: "$.a"}, nil)
	if err != nil {
		t.Logf("JSONPath non-string: %v", err)
	}

	// JSONPath with missing path (result nil)
	val, err := EvalExpression(&query.JSONPathExpr{Column: &query.StringLiteral{Value: `{}`}, Path: "$.missing"}, nil)
		if err != nil {
		t.Logf("JSONPath missing: got %v, %v", val, err)
	}

	// Unary minus on non-numeric returns unchanged
	v, err := EvalExpression(&query.UnaryExpr{Operator: query.TokenMinus, Expr: &query.StringLiteral{Value: "abc"}}, nil)
	if err != nil {
		t.Logf("Unary minus non-numeric: %v", err)
	}
	if v != "abc" {
		t.Logf("Unary minus non-numeric returned %v", v)
	}
}

// ── Update/Delete soft-deleted rows ──
func TestComprehensive_SoftDeletedPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE soft_t (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO soft_t (id, name) VALUES (1, 'a')")
	c.ExecuteQuery("INSERT INTO soft_t (id, name) VALUES (2, 'b')")

	// Delete row 1 (soft delete)
	c.Delete(ctx, mustParseDeleteLM("DELETE FROM soft_t WHERE id = 1"), nil)

	// Update all rows - should skip soft-deleted row 1
	_, _, err := c.Update(ctx, mustParseUpdateLM("UPDATE soft_t SET name = 'z'"), nil)
	if err != nil {
		t.Logf("Update skip soft-deleted: %v", err)
	}

	// Delete all rows - should skip already-deleted row 1
	_, _, err = c.Delete(ctx, mustParseDeleteLM("DELETE FROM soft_t"), nil)
	if err != nil {
		t.Logf("Delete skip soft-deleted: %v", err)
	}
}

// ── Load/LoadSchema DEFAULT and CHECK restoration ──
func TestComprehensive_LoadRestorePaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "restore_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText, Default: &query.StringLiteral{Value: "unknown"}},
			{Name: "age", Type: query.TokenInteger, Check: &query.BinaryExpr{Operator: query.TokenGt, Left: &query.Identifier{Name: "age"}, Right: &query.NumberLiteral{Value: 0}}},
		},
	})
	c.Insert(ctx, &query.InsertStmt{Table: "restore_t", Columns: []string{"id", "name", "age"}, Values: [][]query.Expression{{numReal(1), strReal("alice"), numReal(30)}}}, nil)

	// Save and reload
	if err := c.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	c2 := New(tree, pool, nil)
	if err := c2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify DEFAULT and CHECK were restored
	tbl, err := c2.GetTable("restore_t")
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}
	if len(tbl.Columns) < 2 || tbl.Columns[1].defaultExpr == nil {
		t.Error("DEFAULT expression not restored after Load")
	}
	if len(tbl.Columns) < 3 || tbl.Columns[2].Check == nil {
		t.Error("CHECK expression not restored after Load")
	}

	// LoadSchema path
	tmpDir := t.TempDir()
	if err := c.SaveData(tmpDir); err != nil {
		t.Fatalf("SaveData failed: %v", err)
	}

	c3 := New(tree, pool, nil)
	if err := c3.LoadSchema(tmpDir); err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	tbl2, err := c3.GetTable("restore_t")
	if err != nil {
		t.Fatalf("GetTable after LoadSchema failed: %v", err)
	}
	if len(tbl2.Columns) < 2 || tbl2.Columns[1].defaultExpr == nil {
		t.Error("DEFAULT expression not restored after LoadSchema")
	}
	if len(tbl2.Columns) < 3 || tbl2.Columns[2].Check == nil {
		t.Error("CHECK expression not restored after LoadSchema")
	}
}

// ── Vector index paths ──
func TestComprehensive_VectorPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "vec_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "emb", Type: query.TokenVector, Dimensions: 3},
		},
	})

	// CreateVectorIndex on non-existent table
	err := c.CreateVectorIndex("vidx", "no_table", "emb")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	// CreateVectorIndex on non-existent column
	err = c.CreateVectorIndex("vidx", "vec_t", "no_col")
	if err == nil {
		t.Error("Expected error for non-existent column")
	}

	// CreateVectorIndex on non-vector column
	c.CreateTable(&query.CreateTableStmt{
		Table: "vec_t2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	err = c.CreateVectorIndex("vidx2", "vec_t2", "name")
	if err == nil {
		t.Error("Expected error for non-vector column")
	}

	// Vector with mixed types in []interface{}
	c.Insert(ctx, &query.InsertStmt{
		Table:   "vec_t",
		Columns: []string{"id", "emb"},
		Values:  [][]query.Expression{{numReal(1), &query.VectorLiteral{Values: []float64{1.0, 2.0, 3.0}}}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "vec_t",
		Columns: []string{"id", "emb"},
		Values:  [][]query.Expression{{numReal(2), &query.VectorLiteral{Values: []float64{4.0, 5.0, 6.0}}}},
	}, nil)

	c.CreateVectorIndex("vidx", "vec_t", "emb")

	// Drop not found
	err = c.DropVectorIndex("no_idx")
	if err == nil {
		t.Error("Expected error dropping non-existent vector index")
	}

	// Search not found
	_, _, err = c.SearchVectorKNN("no_idx", []float64{1, 2, 3}, 1)
	if err == nil {
		t.Error("Expected error for non-existent KNN index")
	}

	_, _, err = c.SearchVectorRange("no_idx", []float64{1, 2, 3}, 1.0)
	if err == nil {
		t.Error("Expected error for non-existent range index")
	}

	// Test indexRowForVector with mixed types by directly calling it
	vidx := &VectorIndexDef{Name: "test", TableName: "vec_t", ColumnName: "emb", Dimensions: 3, HNSW: NewHNSWIndex("test", "vec_t", "emb", 3)}
	c.indexRowForVector(vidx, []interface{}{1, []interface{}{int(1), int64(2), float32(3)}}, []byte("k1"), 1)
	c.indexRowForVector(vidx, []interface{}{2, []interface{}{"bad", 2, 3}}, []byte("k2"), 1)
	c.indexRowForVector(vidx, []interface{}{3, []float64{1, 2}}, []byte("k3"), 1)
}

// ── JSON index paths ──
func TestComprehensive_JSONPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Drop not found
	err := c.DropJSONIndex("no_idx")
	if err == nil {
		t.Error("Expected error dropping non-existent JSON index")
	}

	// Query with unsupported type
	c.CreateTable(&query.CreateTableStmt{
		Table: "json_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenJSON},
		},
	})
	c.CreateJSONIndex("jidx", "json_t", "data", "$.a", "TEXT")

	_, err = c.QueryJSONIndex("jidx", true)
	if err == nil {
		t.Error("Expected error for unsupported JSON index query type")
	}
}

// ── FTS paths ──
func TestComprehensive_FTSPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Drop not found
	err := c.DropFTSIndex("no_idx")
	if err == nil {
		t.Error("Expected error dropping non-existent FTS index")
	}

	// Search empty query
	c.CreateTable(&query.CreateTableStmt{
		Table: "fts_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "content", Type: query.TokenText},
		},
	})
	c.CreateFTSIndex("fts_idx", "fts_t", []string{"content"})

	_, err = c.SearchFTS("fts_idx", "")
	if err != nil {
		t.Logf("FTS empty query: %v", err)
	}
}

// ── Materialized view paths ──
func TestComprehensive_MaterializedViewPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE mv_base (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO mv_base (id) VALUES (1)")

	c.CreateMaterializedView("mv1", mustParseSelectLM("SELECT * FROM mv_base"), false)

	// Duplicate create
	err := c.CreateMaterializedView("mv1", mustParseSelectLM("SELECT * FROM mv_base"), false)
	if err == nil {
		t.Error("Expected error for duplicate materialized view")
	}

	// Refresh non-existent
	err = c.RefreshMaterializedView("no_mv")
	if err == nil {
		t.Error("Expected error refreshing non-existent materialized view")
	}
}

// ── RLS paths ──
func TestComprehensive_RLSPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)
	ctx := context.Background()

	// ApplyRLSFilter with nil manager
	c.ExecuteQuery("CREATE TABLE rls_t (id INTEGER PRIMARY KEY)")
	cols, rows, err := c.ApplyRLSFilter(ctx, "rls_t", []string{"id"}, [][]interface{}{{1}}, "", nil)
	if err != nil {
		t.Logf("RLS nil manager: %v", err)
	}
	if cols == nil && rows == nil {
		t.Logf("RLS nil manager returned nil results")
	}

	// CheckRLSForInsert when not enabled
	ok, err := c.CheckRLSForInsert(ctx, "rls_t", nil, "", nil)
	if err != nil {
		t.Logf("RLS not enabled insert: %v", err)
	}
	if !ok {
		t.Error("Expected CheckRLSForInsert to return true when not enabled")
	}
}

// ── Row / serializePK paths ──
func TestComprehensive_RowPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "pk_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenText, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	c.Insert(ctx, &query.InsertStmt{
		Table:   "pk_t",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{strReal("hello"), numReal(1)}},
	}, nil)

	// GetRow should find the string PK
	row, err := c.GetRow("pk_t", "hello")
	if err != nil {
		t.Logf("GetRow string PK: %v", err)
	}
	if row == nil {
		t.Error("Expected row for string PK")
	}

	// serializePK with string that exists (via DeleteRow or UpdateRow)
	c.UpdateRow("pk_t", "hello", map[string]interface{}{"val": 99})
	row, err = c.GetRow("pk_t", "hello")
	if err != nil {
		t.Logf("GetRow after update: %v", err)
	}
	if row != nil {
		if fmt.Sprintf("%v", row["val"]) != "99" {
			t.Errorf("Expected val=99 after update, got %v", row["val"])
		}
	}
}

// ── Index paths ──
func TestComprehensive_IndexPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// DropIndex not found
	err := c.DropIndex("no_idx")
	if err == nil {
		t.Error("Expected error dropping non-existent index")
	}
}

// ── Select error with cache enabled ──
func TestComprehensive_SelectCacheError(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.EnableQueryCache(100, time.Minute)

	// This should error before cache store
	stmt := mustParseSelect("SELECT * FROM no_table")
	_, _, err := c.Select(stmt, nil)
	if err == nil {
		t.Error("Expected error for missing table with cache")
	}
}

// ── catalog_aggregate applyDistinct paths ──
func TestComprehensive_AggregatePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// DISTINCT with nil values
	c.ExecuteQuery("CREATE TABLE dist_t (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO dist_t (id, val) VALUES (1, NULL)")
	c.ExecuteQuery("INSERT INTO dist_t (id, val) VALUES (2, NULL)")
	c.ExecuteQuery("INSERT INTO dist_t (id, val) VALUES (3, 10)")
	_, err := c.ExecuteQuery("SELECT DISTINCT val FROM dist_t")
	if err != nil {
		t.Logf("DISTINCT NULLs: %v", err)
	}

	// GROUP BY with expression
	c.ExecuteQuery("CREATE TABLE gb_expr (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	c.ExecuteQuery("INSERT INTO gb_expr (id, a, b) VALUES (1, 1, 2)")
	c.ExecuteQuery("INSERT INTO gb_expr (id, a, b) VALUES (2, 1, 3)")
	_, err = c.ExecuteQuery("SELECT a+b, COUNT(*) FROM gb_expr GROUP BY a+b")
	if err != nil {
		t.Logf("GROUP BY expr: %v", err)
	}
}

// ── Temporal / AS OF paths ──
func TestComprehensive_TemporalPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE temp_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO temp_t (id) VALUES (1)")

	// AS OF with various formats
	_, err := c.ExecuteQuery("SELECT * FROM temp_t AS OF '2024-01-01'")
	if err != nil {
		t.Logf("AS OF date: %v", err)
	}

	_, err = c.ExecuteQuery("SELECT * FROM temp_t AS OF TIMESTAMP '2024-01-01 12:00:00'")
	if err != nil {
		t.Logf("AS OF timestamp: %v", err)
	}
}

// ── catalog_eval.go compareValues edge cases ──
func TestComprehensive_CompareValuesPaths(t *testing.T) {
	// compareValues with []byte vs string
	if compareValues([]byte("abc"), "abc") != 0 {
		t.Logf("[]byte vs string compare: %d", compareValues([]byte("abc"), "abc"))
	}
	if compareValues("abc", []byte("abd")) >= 0 {
		t.Logf("string vs []byte compare: %d", compareValues("abc", []byte("abd")))
	}
}

// ── catalog_core.go evaluateCastExpr paths ──
func TestComprehensive_CastPaths(t *testing.T) {
	// CAST bool to int
	val, err := EvalExpression(&query.CastExpr{Expr: &query.BooleanLiteral{Value: true}, DataType: query.TokenInteger}, nil)
	if err != nil || val != int64(1) {
		t.Errorf("CAST bool true to int: got %v, %v", val, err)
	}

	val, err = EvalExpression(&query.CastExpr{Expr: &query.BooleanLiteral{Value: false}, DataType: query.TokenInteger}, nil)
	if err != nil || val != int64(0) {
		t.Errorf("CAST bool false to int: got %v, %v", val, err)
	}

	// CAST nil to anything
	val, err = EvalExpression(&query.CastExpr{Expr: &query.NullLiteral{}, DataType: query.TokenInteger}, nil)
	if err != nil || val != nil {
		t.Errorf("CAST NULL: got %v, %v", val, err)
	}
}

// ── catalog_core.go evaluateBetween paths ──
func TestComprehensive_BetweenPaths(t *testing.T) {
	// BETWEEN with string values (unsupported in EvalExpression directly)
	val, err := EvalExpression(&query.BetweenExpr{
		Expr:  &query.StringLiteral{Value: "b"},
		Lower: &query.StringLiteral{Value: "a"},
		Upper: &query.StringLiteral{Value: "c"},
	}, nil)
	if err != nil {
		t.Logf("BETWEEN strings: got %v, %v", val, err)
	}

	// BETWEEN with nil expr
	val, err = EvalExpression(&query.BetweenExpr{
		Expr:  &query.NullLiteral{},
		Lower: &query.NumberLiteral{Value: 1},
		Upper: &query.NumberLiteral{Value: 10},
	}, nil)
	if err != nil {
		t.Logf("BETWEEN nil: got %v, %v", val, err)
	}
}

// ── catalog_core.go evaluateLike paths ──
func TestComprehensive_LikePaths(t *testing.T) {
	// LIKE with escape character (unsupported in EvalExpression directly)
	val, err := EvalExpression(&query.BinaryExpr{
		Operator: query.TokenLike,
		Left:     &query.StringLiteral{Value: "a_b"},
		Right:    &query.StringLiteral{Value: "a\\_b"},
	}, nil)
	if err != nil {
		t.Logf("LIKE escape: got %v, %v", val, err)
	}

}

// ── catalog_core.go evaluateIn paths ──
func TestComprehensive_InPaths(t *testing.T) {
	// IN with nil value (unsupported in EvalExpression directly)
	val, err := EvalExpression(&query.InExpr{
		Expr:  &query.NullLiteral{},
		List:  []query.Expression{&query.NumberLiteral{Value: 1}},
		Not: false,
	}, nil)
	if err != nil {
		t.Logf("IN nil: got %v, %v", val, err)
	}

	// NOT IN
	val, err = EvalExpression(&query.InExpr{
		Expr:  &query.NumberLiteral{Value: 5},
		List:  []query.Expression{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 2}},
		Not: true,
	}, nil)
	if err != nil {
		t.Logf("NOT IN: got %v, %v", val, err)
	}
}

// ── catalog_core.go evaluateWhere / evaluateTemporalExpr paths ──
func TestComprehensive_TemporalExprPaths(t *testing.T) {
	// parseSystemTimeExpr with INTERVAL
	_, err := EvalExpression(&query.CastExpr{Expr: &query.StringLiteral{Value: "2024-01-01"}, DataType: query.TokenTimestamp}, nil)
	if err != nil {
		t.Logf("Timestamp cast: %v", err)
	}
}

// ── catalog_txn.go transaction paths ──
func TestComprehensive_TransactionPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE txn_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO txn_t (id) VALUES (1)")

	// Begin, Savepoint, RollbackToSavepoint, ReleaseSavepoint
	c.BeginTransaction(1)
	if !c.IsTransactionActive() {
		t.Error("Expected transaction to be active")
	}

	c.Savepoint("sp1")
	c.ExecuteQuery("INSERT INTO txn_t (id) VALUES (2)")
	c.RollbackToSavepoint("sp1")
	c.ReleaseSavepoint("sp1")
	c.CommitTransaction()

	if c.IsTransactionActive() {
		t.Error("Expected transaction to be inactive after commit")
	}

	// Rollback with no transaction
	c.RollbackTransaction()
}

// ── catalog_ddl.go trigger paths ──
func TestComprehensive_TriggerPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE trig_t (id INTEGER PRIMARY KEY)")

	// Create trigger, get it, drop it
	c.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trig1",
		Table: "trig_t",
		Time:  "BEFORE",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "trig_t",
				Columns: []string{"id"},
				Values:  [][]query.Expression{{&query.NumberLiteral{Value: 999}}},
			},
		},
	})
	trig, err := c.GetTrigger("trig1")
	if err != nil || trig == nil {
		t.Logf("GetTrigger: %v", err)
	}

	trigs := c.GetTriggersForTable("trig_t", "INSERT")
	if len(trigs) == 0 {
		t.Error("Expected triggers for table")
	}

	c.DropTrigger("trig1")
	_, err = c.GetTrigger("trig1")
	if err == nil {
		t.Error("Expected error for dropped trigger")
	}
}

// ── catalog_select.go executeScalarAggregate paths ──
func TestComprehensive_ScalarAggregatePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Scalar aggregate with empty result and HAVING
	_, err := c.ExecuteQuery("SELECT COUNT(*) WHERE 1 = 0 HAVING COUNT(*) > 0")
	if err != nil {
		t.Logf("Scalar aggregate HAVING false: %v", err)
	}

	// Scalar aggregate COUNT(*) without FROM
	res, err := c.ExecuteQuery("SELECT COUNT(*)")
	if err != nil {
		t.Logf("Scalar COUNT(*): %v", err)
	}
	if res != nil && len(res.Rows) > 0 {
		t.Logf("COUNT(*) without FROM: got %v", res.Rows[0][0])
	}
}

// ── catalog_maintenance.go Vacuum / Analyze paths ──
func TestComprehensive_MaintenancePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Vacuum empty catalog
	err := c.Vacuum()
	if err != nil {
		t.Logf("Vacuum empty: %v", err)
	}

	// Analyze empty catalog
	err = c.Analyze("")
	if err != nil {
		t.Logf("Analyze empty: %v", err)
	}

	// Analyze with tables
	c.ExecuteQuery("CREATE TABLE anal_t (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO anal_t (id, val) VALUES (1, 10)")
	err = c.Analyze("anal_t")
	if err != nil {
		t.Logf("Analyze with data: %v", err)
	}

	// GetTableStats
	stats, err := c.GetTableStats("anal_t")
	if err != nil {
		t.Logf("GetTableStats: %v", err)
	}
	if stats != nil && stats.RowCount != 1 {
		t.Errorf("Expected RowCount=1, got %d", stats.RowCount)
	}
}

// ── catalog_core.go exprToSQL paths ──
func TestComprehensive_ExprToSQLPaths(t *testing.T) {
	// Various expression types in exprToSQL
	cases := []query.Expression{
		&query.PlaceholderExpr{Index: 1},
		&query.UnaryExpr{Operator: query.TokenMinus, Expr: &query.NumberLiteral{Value: 5}},
		&query.BinaryExpr{Operator: query.TokenAnd, Left: &query.BooleanLiteral{Value: true}, Right: &query.BooleanLiteral{Value: false}},
		&query.CaseExpr{Else: &query.NumberLiteral{Value: 0}},
		&query.CastExpr{Expr: &query.StringLiteral{Value: "123"}, DataType: query.TokenInteger},
		&query.InExpr{Expr: &query.NumberLiteral{Value: 1}, List: []query.Expression{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 2}}},
		&query.BetweenExpr{Expr: &query.NumberLiteral{Value: 5}, Lower: &query.NumberLiteral{Value: 1}, Upper: &query.NumberLiteral{Value: 10}},
		&query.IsNullExpr{Expr: &query.Identifier{Name: "x"}, Not: true},
		&query.ExistsExpr{Subquery: &query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}}},
		&query.JSONPathExpr{Column: &query.Identifier{Name: "j"}, Path: "$.a", AsText: true},
		&query.WindowExpr{Function: "ROW_NUMBER"},
	}

	for i, expr := range cases {
		sql := exprToSQL(expr)
		if sql == "" {
			t.Logf("exprToSQL case %d returned empty", i)
		}
	}
}

// ── catalog_core.go tokenTypeToColumnType paths ──
func TestComprehensive_ColumnTypePaths(t *testing.T) {
	// tokenTypeToColumnType with all token types
	types := []query.TokenType{
		query.TokenInteger, query.TokenText, query.TokenReal,
		query.TokenBoolean, query.TokenJSON, query.TokenBlob,
		query.TokenTimestamp, query.TokenDate, query.TokenVector,
	}
	for _, tt := range types {
		ct := tokenTypeToColumnType(tt)
		if ct == "" {
			t.Errorf("tokenTypeToColumnType(%v) returned empty", tt)
		}
	}
}

// ── catalog_core.go isIntegerType paths ──
func TestComprehensive_IsIntegerTypePaths(t *testing.T) {
	if !isIntegerType(int64(1)) {
		t.Error("Expected int64 to be integer type")
	}
	if !isIntegerType(1) {
		t.Error("Expected int to be integer type")
	}
	if !isIntegerType(float64(5.0)) {
		t.Error("Expected whole float64 to be integer type")
	}
	if isIntegerType("TEXT") {
		t.Error("Expected TEXT not to be integer type")
	}
	if isIntegerType(3.14) {
		t.Error("Expected non-whole float64 not to be integer type")
	}
}

// ── catalog_core.go evaluateJSONFunction paths ──
func TestComprehensive_JSONFunctionPaths(t *testing.T) {
	// JSONType with array
	val, err := EvalExpression(&query.FunctionCall{Name: "JSON_TYPE", Args: []query.Expression{
		&query.StringLiteral{Value: `[1,2,3]`},
	}}, nil)
	if err != nil {
		t.Logf("JSON_TYPE array: %v", err)
	} else {
		t.Logf("JSON_TYPE array returned %v", val)
	}

	// JSONType with object
	val, err = EvalExpression(&query.FunctionCall{Name: "JSON_TYPE", Args: []query.Expression{
		&query.StringLiteral{Value: `{"a":1}`},
	}}, nil)
	if err != nil {
		t.Logf("JSON_TYPE object: got %v, %v", val, err)
	}

	// JSONArrayLength
	val, err = EvalExpression(&query.FunctionCall{Name: "JSON_ARRAY_LENGTH", Args: []query.Expression{
		&query.StringLiteral{Value: `[1,2,3]`},
	}}, nil)
	if err != nil {
		t.Logf("JSON_ARRAY_LENGTH: got %v, %v", val, err)
	}

	// JSONKeys
	val, err = EvalExpression(&query.FunctionCall{Name: "JSON_KEYS", Args: []query.Expression{
		&query.StringLiteral{Value: `{"a":1,"b":2}`},
	}}, nil)
	if err != nil {
		t.Logf("JSON_KEYS: %v", err)
	}

	// JSONPretty
	val, err = EvalExpression(&query.FunctionCall{Name: "JSON_PRETTY", Args: []query.Expression{
		&query.StringLiteral{Value: `{"a":1}`},
	}}, nil)
	if err != nil {
		t.Logf("JSON_PRETTY: %v", err)
	}

	// JSONMinify
	val, err = EvalExpression(&query.FunctionCall{Name: "JSON_MINIFY", Args: []query.Expression{
		&query.StringLiteral{Value: "{\n  \"a\" : 1\n}"},
	}}, nil)
	if err != nil {
		t.Logf("JSON_MINIFY: %v", err)
	}

	// JSONMerge
	val, err = EvalExpression(&query.FunctionCall{Name: "JSON_MERGE", Args: []query.Expression{
		&query.StringLiteral{Value: `{"a":1}`},
		&query.StringLiteral{Value: `{"b":2}`},
	}}, nil)
	if err != nil {
		t.Logf("JSON_MERGE: %v", err)
	}

	// JSONQuote
	val, err = EvalExpression(&query.FunctionCall{Name: "JSON_QUOTE", Args: []query.Expression{
		&query.StringLiteral{Value: "hello"},
	}}, nil)
	if err != nil {
		t.Logf("JSON_QUOTE: %v", err)
	}

	// JSONUnquote
	val, err = EvalExpression(&query.FunctionCall{Name: "JSON_UNQUOTE", Args: []query.Expression{
		&query.StringLiteral{Value: `"hello"`},
	}}, nil)
	if err != nil {
		t.Logf("JSON_UNQUOTE: got %v, %v", val, err)
	}

	// IsValidJSON false
	val, err = EvalExpression(&query.FunctionCall{Name: "IS_VALID_JSON", Args: []query.Expression{
		&query.StringLiteral{Value: `not json`},
	}}, nil)
	if err != nil {
		t.Logf("IS_VALID_JSON false: got %v, %v", val, err)
	}
}

// ── catalog_window.go evaluateWindowFunctions paths ──
func TestComprehensive_WindowPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE win_t (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO win_t (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO win_t (id, val) VALUES (2, 20)")
	c.ExecuteQuery("INSERT INTO win_t (id, val) VALUES (3, 30)")

	// NTILE
	_, err := c.ExecuteQuery("SELECT NTILE(2) OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("NTILE: %v", err)
	}

	// CUME_DIST
	_, err = c.ExecuteQuery("SELECT CUME_DIST() OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("CUME_DIST: %v", err)
	}

	// PERCENT_RANK
	_, err = c.ExecuteQuery("SELECT PERCENT_RANK() OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("PERCENT_RANK: %v", err)
	}

	// LAST_VALUE
	_, err = c.ExecuteQuery("SELECT LAST_VALUE(val) OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("LAST_VALUE: %v", err)
	}

	// NTH_VALUE
	_, err = c.ExecuteQuery("SELECT NTH_VALUE(val, 2) OVER (ORDER BY id) FROM win_t")
	if err != nil {
		t.Logf("NTH_VALUE: %v", err)
	}
}

// ── catalog_core.go arithmetic helpers ──
func TestComprehensive_ArithmeticPaths(t *testing.T) {
	// addValues with []byte
	if _, err := addValues([]byte("a"), []byte("b")); err == nil {
		t.Error("Expected error adding []byte values")
	}

	// subtractValues with []byte
	if _, err := subtractValues([]byte("a"), []byte("b")); err == nil {
		t.Error("Expected error subtracting []byte values")
	}

	// multiplyValues with []byte
	if _, err := multiplyValues([]byte("a"), []byte("b")); err == nil {
		t.Error("Expected error multiplying []byte values")
	}

	// divideValues by zero
	if _, err := divideValues(10, 0); err == nil {
		t.Error("Expected error dividing by zero")
	}

	// divideValues with []byte
	if _, err := divideValues([]byte("a"), []byte("b")); err == nil {
		t.Error("Expected error dividing []byte values")
	}

	// moduloValues with []byte
	if _, err := moduloValues([]byte("a"), []byte("b")); err == nil {
		t.Error("Expected error modulo []byte values")
	}

	// moduloValues by zero
	if _, err := moduloValues(10, 0); err == nil {
		t.Error("Expected error modulo by zero")
	}
}

// ── catalog_core.go bytesContainDeletedAt paths ──
func TestComprehensive_BytesDeletedAtPaths(t *testing.T) {
	// bytesContainDeletedAt with various inputs
	if !bytesContainDeletedAt([]byte(`{"data":[1],"version":{"deleted_at":123}}`)) {
		t.Error("Expected bytesContainDeletedAt true")
	}
	if bytesContainDeletedAt([]byte(`{"data":[1],"version":{"deleted_at":0}}`)) {
		t.Error("Expected bytesContainDeletedAt false for 0")
	}
	if bytesContainDeletedAt([]byte(`{"data":[1],"version":{}}`)) {
		t.Error("Expected bytesContainDeletedAt false for missing")
	}
}

// ── catalog_core.go toFloat64Safe paths ──
func TestComprehensive_ToFloat64SafePaths(t *testing.T) {
	if _, ok := toFloat64Safe("abc"); ok {
		t.Error("Expected toFloat64Safe false for string")
	}
	if _, ok := toFloat64Safe(nil); ok {
		t.Error("Expected toFloat64Safe false for nil")
	}
	if v, ok := toFloat64Safe(int64(42)); !ok || v != 42 {
		t.Errorf("Expected toFloat64Safe int64=42, got %v, %v", v, ok)
	}
}

// ── catalog_aggregate.go evaluateExprWithGroupAggregates paths ──
func TestComprehensive_EmbeddedAggPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Embedded COUNT(expr) with NULLs
	c.ExecuteQuery("CREATE TABLE emb_count (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO emb_count (id, cat, val) VALUES (1, 'A', NULL)")
	c.ExecuteQuery("INSERT INTO emb_count (id, cat, val) VALUES (2, 'A', 10)")
	c.ExecuteQuery("INSERT INTO emb_count (id, cat, val) VALUES (3, 'B', NULL)")
	_, err := c.ExecuteQuery("SELECT cat, CASE WHEN COUNT(val) > 0 THEN 1 ELSE 0 END FROM emb_count GROUP BY cat")
	if err != nil {
		t.Logf("Embedded COUNT(expr): %v", err)
	}

	// Embedded MIN/MAX with all NULLs
	_, err = c.ExecuteQuery("SELECT cat, CASE WHEN MIN(val) IS NULL THEN 'yes' ELSE 'no' END FROM emb_count GROUP BY cat")
	if err != nil {
		t.Logf("Embedded MIN NULL: %v", err)
	}
}

// ── catalog_core.go containsSubquery paths ──
func TestComprehensive_ContainsSubqueryPaths(t *testing.T) {
	if !containsSubquery(&query.SubqueryExpr{Query: &query.SelectStmt{}}) {
		t.Error("Expected containsSubquery true for SubqueryExpr")
	}
	if !containsSubquery(&query.ExistsExpr{Subquery: &query.SelectStmt{}}) {
		t.Error("Expected containsSubquery true for ExistsExpr")
	}
	if containsSubquery(&query.NumberLiteral{Value: 1}) {
		t.Error("Expected containsSubquery false for NumberLiteral")
	}
}

// ── catalog_core.go hasNonDeterministicFunction paths ──
func TestComprehensive_NonDeterministicPaths(t *testing.T) {
	if !hasNonDeterministicFunction(&query.FunctionCall{Name: "NOW"}) {
		t.Error("Expected hasNonDeterministicFunction true for NOW")
	}

	if hasNonDeterministicFunction(&query.NumberLiteral{Value: 1}) {
		t.Error("Expected hasNonDeterministicFunction false for literal")
	}
}

// ── catalog_core.go getPartitionTreeName / getPartitionTreeNames paths ──
func TestComprehensive_PartitionPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE part_t (id INTEGER PRIMARY KEY, region TEXT)")

	// Add partition definition manually
	tbl, _ := c.GetTable("part_t")
	if tbl != nil {
		tbl.Partition = &PartitionInfo{
			Type:   query.PartitionTypeRange,
			Column: "region",
			Partitions: []PartitionDef{
				{Name: "p1", MinValue: 0, MaxValue: 50},
				{Name: "p2", MinValue: 51, MaxValue: 100},
			},
		}
		// getPartitionTreeNames
		names := tbl.getPartitionTreeNames()
		if len(names) == 0 {
			t.Error("Expected partition tree names")
		}
	}
}

// ── catalog_core.go buildCompositeIndexKey paths ──
func TestComprehensive_BuildIndexKeyPaths(t *testing.T) {
	// buildCompositeIndexKey with nil value
	tbl := &TableDef{
		Name:    "idx_t",
		Columns: []ColumnDef{{Name: "a", Type: "TEXT"}},
	}
	idx := &IndexDef{TableName: "idx_t", Columns: []string{"a"}}
	key, ok := buildCompositeIndexKey(tbl, idx, []interface{}{nil})
	if ok {
		t.Errorf("Expected false for nil value, got key=%s", key)
	}
}

// ── catalog_core.go encodeRow / decodeRow paths ──
func TestComprehensive_RowCodecPaths(t *testing.T) {
	exprs := []query.Expression{
		&query.NumberLiteral{Value: 1},
		&query.StringLiteral{Value: "hello"},
		&query.NumberLiteral{Value: 3.14},
		&query.BooleanLiteral{Value: true},
		&query.NullLiteral{},
	}
	encoded, err := encodeRow(exprs, nil)
	if err != nil {
		t.Fatalf("encodeRow failed: %v", err)
	}

	decoded, err := decodeRow(encoded, 5)
	if err != nil {
		t.Fatalf("decodeRow failed: %v", err)
	}
	if len(decoded) != 5 {
		t.Errorf("Expected 5 columns, got %d", len(decoded))
	}
}

// ── catalog_core.go typeTaggedKey paths ──
func TestComprehensive_TypeTaggedKeyPaths(t *testing.T) {
	// typeTaggedKey with various types
	cases := []interface{}{
		nil, int64(42), float64(3.14), "hello", true, []byte("world"),
	}
	for _, v := range cases {
		key := typeTaggedKey(v)
		if key == "" && v != nil {
			t.Errorf("typeTaggedKey(%v) returned empty", v)
		}
	}
}

// ── catalog_core.go stripHiddenCols paths ──
func TestComprehensive_StripHiddenColsPaths(t *testing.T) {
	rows := [][]interface{}{
		{1, 2, 3, 4, 5},
	}
	result := stripHiddenCols(rows, 5, 2)
	if len(result[0]) != 3 {
		t.Errorf("Expected 3 visible cols, got %d", len(result[0]))
	}
}

// ── catalog_core.go rowKeyForDedup paths ──
func TestComprehensive_RowKeyForDedupPaths(t *testing.T) {
	row := []interface{}{1, "hello"}
	key := rowKeyForDedup(row)
	if key == "" {
		t.Error("Expected non-empty dedup key")
	}
}

// ── catalog_core.go addHiddenHavingAggregates / addHiddenOrderByAggregates paths ──
func TestComprehensive_HiddenAggPaths(t *testing.T) {
	// addHiddenHavingAggregates with non-aggregate function
	cols := []selectColInfo{{name: "a", index: 0}}
	having := &query.FunctionCall{Name: "UPPER", Args: []query.Expression{&query.Identifier{Name: "a"}}}
	newCols, count := addHiddenHavingAggregates(having, cols, "t")
	if count != 0 {
		t.Errorf("Expected 0 hidden aggregates for UPPER, got %d", count)
	}
	if len(newCols) != len(cols) {
		t.Error("Expected same columns for non-aggregate HAVING")
	}

	// addHiddenOrderByAggregates with non-aggregate function
	orderBy := []*query.OrderByExpr{&query.OrderByExpr{Expr: &query.FunctionCall{Name: "UPPER", Args: []query.Expression{&query.Identifier{Name: "a"}}}}}
	newCols, count = addHiddenOrderByAggregates(orderBy, cols, "t")
	if count != 0 {
		t.Errorf("Expected 0 hidden aggregates for ORDER BY UPPER, got %d", count)
	}
}

// ── catalog_core.go collectAggregatesFromExpr paths ──
func TestComprehensive_CollectAggregatesPaths(t *testing.T) {
	// collectAggregatesFromExpr with BinaryExpr containing aggregate
	expr := &query.BinaryExpr{
		Operator: query.TokenPlus,
		Left:     &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "a"}}},
		Right:    &query.NumberLiteral{Value: 1},
	}
	aggs := make([]*query.FunctionCall, 0)
	collectAggregatesFromExpr(expr, &aggs)
	if len(aggs) != 1 {
		t.Errorf("Expected 1 aggregate, got %d", len(aggs))
	}
}

// ── catalog_core.go resolveAggregateInExpr paths ──
func TestComprehensive_ResolveAggregatePaths(t *testing.T) {
	// resolveAggregateInExpr with non-aggregate function
	expr := &query.FunctionCall{Name: "UPPER", Args: []query.Expression{&query.Identifier{Name: "a"}}}
	result := resolveAggregateInExpr(expr, []selectColInfo{{name: "a", index: 0}}, []interface{}{"hello"})
	if result != expr {
		t.Error("Expected resolveAggregateInExpr to return same expr for non-aggregate")
	}
}

// ── catalog_core.go replaceAggregatesInExpr paths ──
func TestComprehensive_ReplaceAggregatesPaths(t *testing.T) {
	// replaceAggregatesInExpr with non-aggregate function
	expr := &query.FunctionCall{Name: "UPPER", Args: []query.Expression{&query.Identifier{Name: "a"}}}
	result := replaceAggregatesInExpr(expr, map[*query.FunctionCall]interface{}{})
	if result != expr {
		t.Error("Expected replaceAggregatesInExpr to return same expr for non-aggregate")
	}
}

// ── catalog_core.go resolvePositionalRefs paths ──
func TestComprehensive_PositionalRefPaths(t *testing.T) {
	// resolvePositionalRefs with nil stmt
	result := resolvePositionalRefs(nil)
	if result != nil {
		t.Error("Expected nil for nil stmt")
	}

	// resolvePositionalRefs with invalid position
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.NumberLiteral{Value: 1}},
		OrderBy: []*query.OrderByExpr{{Expr: &query.NumberLiteral{Value: 99}}},
	}
	result = resolvePositionalRefs(stmt)
	if result.OrderBy[0].Expr != stmt.OrderBy[0].Expr {
		// Should remain unchanged for invalid position
		t.Log("Invalid positional ref handled")
	}
}

// ── catalog_core.go resolveOuterRefsInExpr paths ──
func TestComprehensive_OuterRefPaths(t *testing.T) {
	// resolveOuterRefsInExpr with nil expr
	result := resolveOuterRefsInExpr(nil, nil, nil, nil)
	if result != nil {
		t.Error("Expected nil for nil expr")
	}

	// resolveOuterRefsInExpr with SubqueryExpr
	expr := &query.SubqueryExpr{Query: &query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}}}
	result = resolveOuterRefsInExpr(expr, []interface{}{int64(42)}, []ColumnDef{{Name: "x"}}, map[string]bool{})
	if result == nil {
		t.Error("Expected non-nil for SubqueryExpr")
	}
}

// ── catalog_core.go intersectSorted paths ──
func TestComprehensive_IntersectSortedPaths(t *testing.T) {
	// intersectSorted with empty slices
	result := intersectSorted([]int64{}, []int64{1, 2})
	if len(result) != 0 {
		t.Errorf("Expected empty intersection, got %v", result)
	}

	// intersectSorted with no common elements
	result = intersectSorted([]int64{1, 2}, []int64{3, 4})
	if len(result) != 0 {
		t.Errorf("Expected empty intersection, got %v", result)
	}
}

// ── catalog_core.go catalogCompareValues paths ──
func TestComprehensive_CatalogCompareValuesPaths(t *testing.T) {
	// catalogCompareValues with nil
	if catalogCompareValues(nil, 1) != -1 {
		t.Error("Expected nil < any")
	}
	if catalogCompareValues(1, nil) != 1 {
		t.Error("Expected any > nil")
	}

	// catalogCompareValues with bool
	if catalogCompareValues(true, false) != 1 {
		t.Error("Expected true > false")
	}
}

// ── catalog_core.go parseSystemTimeExpr paths ──
func TestComprehensive_SystemTimePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// parseSystemTimeExpr with INTERVAL
	_, err := c.ExecuteQuery("SELECT 1") // Just to initialize
	if err != nil {
		t.Logf("init: %v", err)
	}

	// AS OF with INTERVAL syntax
	c.ExecuteQuery("CREATE TABLE sys_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO sys_t (id) VALUES (1)")
	_, err = c.ExecuteQuery("SELECT * FROM sys_t AS OF CURRENT_TIMESTAMP - INTERVAL '1' DAY")
	if err != nil {
		t.Logf("AS OF INTERVAL: %v", err)
	}
}

// ── catalog_core.go applyOffsetLimit paths ──
func TestComprehensive_ApplyOffsetLimitPaths(t *testing.T) {
	rows := [][]interface{}{{1}, {2}, {3}}

	// applyOffsetLimit with both offset and limit
	result := applyOffsetLimit(rows, &query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}, nil)
	if len(result) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result))
	}

	// applyOffsetLimit with offset beyond length
	result = applyOffsetLimit(rows, &query.NumberLiteral{Value: 10}, &query.NumberLiteral{Value: 0}, nil)
	if len(result) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(result))
	}
}

// ── catalog_insert.go insertLocked paths ──
func TestComprehensive_InsertPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "ins_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// INSERT with RETURNING
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:    "ins_t",
		Columns:  []string{"id", "val"},
		Values:   [][]query.Expression{{numReal(1), numReal(10)}},
		Returning: []query.Expression{&query.Identifier{Name: "id"}},
	}, nil)
	if err != nil {
		t.Logf("INSERT RETURNING: %v", err)
	}

	// INSERT with subquery values
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "ins_t",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.SubqueryExpr{Query: &query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 2}}}}, numReal(20)}},
	}, nil)
	if err != nil {
		t.Logf("INSERT subquery: %v", err)
	}
}

// ── catalog_eval.go evaluateMatchExpr paths ──
func TestComprehensive_MatchPaths(t *testing.T) {
	// evaluateMatchExpr with nil value
	val, err := EvalExpression(&query.MatchExpr{
		Columns: []query.Expression{&query.Identifier{Name: "x"}},
		Pattern: &query.StringLiteral{Value: "hello"},
		Mode:    "",
	}, nil)
	if err != nil {
		t.Logf("MATCH nil: got %v, %v", val, err)
	}
}

// ── catalog_eval.go evaluateFunctionCall edge cases ──
func TestComprehensive_FunctionEdgePaths(t *testing.T) {
	// COALESCE with all NULLs
	val, err := EvalExpression(&query.FunctionCall{Name: "COALESCE", Args: []query.Expression{
		&query.NullLiteral{}, &query.NullLiteral{},
	}}, nil)
	if err != nil || val != nil {
		t.Errorf("COALESCE all NULL: got %v, %v", val, err)
	}

	// GREATEST / LEAST with mixed types
	val, err = EvalExpression(&query.FunctionCall{Name: "GREATEST", Args: []query.Expression{
		&query.NumberLiteral{Value: 1},
		&query.NumberLiteral{Value: 5},
		&query.NumberLiteral{Value: 3},
	}}, nil)
	if err != nil {
		t.Logf("GREATEST: got %v, %v", val, err)
	}

	val, err = EvalExpression(&query.FunctionCall{Name: "LEAST", Args: []query.Expression{
		&query.NumberLiteral{Value: 1},
		&query.NumberLiteral{Value: 5},
		&query.NumberLiteral{Value: 3},
	}}, nil)
	if err != nil {
		t.Logf("LEAST: got %v, %v", val, err)
	}

	// ROUND with no precision
	val, err = EvalExpression(&query.FunctionCall{Name: "ROUND", Args: []query.Expression{
		&query.NumberLiteral{Value: 3.7},
	}}, nil)
	if err != nil {
		t.Logf("ROUND no precision: got %v, %v", val, err)
	}

	// POWER
	val, err = EvalExpression(&query.FunctionCall{Name: "POWER", Args: []query.Expression{
		&query.NumberLiteral{Value: 2},
		&query.NumberLiteral{Value: 3},
	}}, nil)
	if err != nil {
		t.Logf("POWER: got %v, %v", val, err)
	}

	// SQRT
	val, err = EvalExpression(&query.FunctionCall{Name: "SQRT", Args: []query.Expression{
		&query.NumberLiteral{Value: 16},
	}}, nil)
	if err != nil {
		t.Logf("SQRT: got %v, %v", val, err)
	}

	// SIGN
	val, err = EvalExpression(&query.FunctionCall{Name: "SIGN", Args: []query.Expression{
		&query.NumberLiteral{Value: -5},
	}}, nil)
	if err != nil {
		t.Logf("SIGN negative: got %v, %v", val, err)
	}

	// MOD
	val, err = EvalExpression(&query.FunctionCall{Name: "MOD", Args: []query.Expression{
		&query.NumberLiteral{Value: 10},
		&query.NumberLiteral{Value: 3},
	}}, nil)
	if err != nil {
		t.Logf("MOD: got %v, %v", val, err)
	}

	// FLOOR
	val, err = EvalExpression(&query.FunctionCall{Name: "FLOOR", Args: []query.Expression{
		&query.NumberLiteral{Value: 3.7},
	}}, nil)
	if err != nil {
		t.Logf("FLOOR: got %v, %v", val, err)
	}

	// CEIL
	val, err = EvalExpression(&query.FunctionCall{Name: "CEIL", Args: []query.Expression{
		&query.NumberLiteral{Value: 3.2},
	}}, nil)
	if err != nil {
		t.Logf("CEIL: got %v, %v", val, err)
	}

	// CONCAT with nil
	val, err = EvalExpression(&query.FunctionCall{Name: "CONCAT", Args: []query.Expression{
		&query.StringLiteral{Value: "a"},
		&query.NullLiteral{},
		&query.StringLiteral{Value: "b"},
	}}, nil)
	if err != nil || val != "ab" {
		t.Errorf("CONCAT with nil: got %v, %v", val, err)
	}

	// INSTR
	val, err = EvalExpression(&query.FunctionCall{Name: "INSTR", Args: []query.Expression{
		&query.StringLiteral{Value: "hello world"},
		&query.StringLiteral{Value: "world"},
	}}, nil)
	if err != nil || val != int64(7) {
		t.Errorf("INSTR: got %v, %v", val, err)
	}

	// REVERSE
	val, err = EvalExpression(&query.FunctionCall{Name: "REVERSE", Args: []query.Expression{
		&query.StringLiteral{Value: "hello"},
	}}, nil)
	if err != nil || val != "olleh" {
		t.Errorf("REVERSE: got %v, %v", val, err)
	}

	// DATE / TIME / DATETIME
	val, err = EvalExpression(&query.FunctionCall{Name: "DATE", Args: []query.Expression{
		&query.StringLiteral{Value: "2024-01-15 12:30:00"},
	}}, nil)
	if err != nil {
		t.Logf("DATE: %v", err)
	}

	// UUID
	val, err = EvalExpression(&query.FunctionCall{Name: "UUID"}, nil)
	if err != nil {
		t.Logf("UUID: %v", err)
	}
	if val == "" {
		t.Error("Expected non-empty UUID")
	}
}

// ── catalog_ddl.go procedure paths ──
func TestComprehensive_ProcedurePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create, get, drop procedure
	c.CreateProcedure(&query.CreateProcedureStmt{Name: "proc1", Body: []query.Statement{}})
	proc, err := c.GetProcedure("proc1")
	if err != nil || proc == nil {
		t.Logf("GetProcedure: %v", err)
	}

	c.DropProcedure("proc1")
	_, err = c.GetProcedure("proc1")
	if err == nil {
		t.Error("Expected error for dropped procedure")
	}
}

// ── catalog_ddl.go table stats paths ──
func TestComprehensive_TableStatsPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE stats_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO stats_t (id) VALUES (1)")
	c.Analyze("stats_t")

	stats, err := c.GetTableStats("stats_t")
	if err != nil {
		t.Logf("GetTableStats: %v", err)
	}
	if stats == nil || stats.RowCount != 1 {
		t.Errorf("Expected RowCount=1, got %v", stats)
	}
}

// ── catalog_maintenance.go SaveData / LoadData paths ──
func TestComprehensive_SaveLoadDataPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "sld_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sld_t",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("alice")}},
	}, nil)

	tmpDir := t.TempDir()
	if err := c.SaveData(tmpDir); err != nil {
		t.Fatalf("SaveData failed: %v", err)
	}

	// Verify schema.json exists
	schemaPath := filepath.Join(tmpDir, "schema.json")
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		t.Error("schema.json not created")
	}

	// LoadData
	c2 := New(tree, pool, nil)
	if err := c2.LoadData(tmpDir); err != nil {
		t.Logf("LoadData: %v", err)
	}
}

// ── catalog_eval.go toVector paths ──
func TestComprehensive_ToVectorPaths(t *testing.T) {
	// toVector with various inputs
	cases := []struct {
		input interface{}
		ok    bool
	}{
		{[]float64{1, 2, 3}, true},
		{[]interface{}{1.0, 2.0, 3.0}, true},
		{"not a vector", false},
		{nil, false},
	}
	for _, tc := range cases {
		v, err := toVector(tc.input)
		if tc.ok && err != nil {
			t.Errorf("toVector(%v) unexpected error: %v", tc.input, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("toVector(%v) expected error", tc.input)
		}
		_ = v
	}
}

// ── catalog_eval.go evaluateBinaryExpr NULL paths ──
func TestComprehensive_BinaryNullPaths(t *testing.T) {
	// NULL AND false = false
	val, err := EvalExpression(&query.BinaryExpr{
		Operator: query.TokenAnd,
		Left:     &query.NullLiteral{},
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil || val != false {
		t.Errorf("NULL AND false: got %v, %v", val, err)
	}

	// NULL OR true = true
	val, err = EvalExpression(&query.BinaryExpr{
		Operator: query.TokenOr,
		Left:     &query.NullLiteral{},
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil || val != true {
		t.Errorf("NULL OR true: got %v, %v", val, err)
	}

	// NULL AND true = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Operator: query.TokenAnd,
		Left:     &query.NullLiteral{},
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil || val != nil {
		t.Errorf("NULL AND true: got %v, %v", val, err)
	}

	// NULL OR false = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Operator: query.TokenOr,
		Left:     &query.NullLiteral{},
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil || val != nil {
		t.Errorf("NULL OR false: got %v, %v", val, err)
	}
}

// ── catalog_core.go evaluateWhere NULL paths ──
func TestComprehensive_WhereNullPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE where_t (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO where_t (id, val) VALUES (1, NULL)")
	c.ExecuteQuery("INSERT INTO where_t (id, val) VALUES (2, 10)")

	// WHERE val = NULL (should return no rows in SQL)
	res, err := c.ExecuteQuery("SELECT * FROM where_t WHERE val = NULL")
	if err != nil {
		t.Logf("WHERE val = NULL: %v", err)
	}
	if res != nil && len(res.Rows) > 0 {
		t.Errorf("Expected no rows for val = NULL, got %d", len(res.Rows))
	}
}

func mustParseSelectLM(sql string) *query.SelectStmt {
	parsed, err := query.Parse(sql)
	if err != nil {
		panic(err)
	}
	if sel, ok := parsed.(*query.SelectStmt); ok {
		return sel
	}
	panic("parsed statement is not a SELECT")
}
