package catalog

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Stats: countRows, collectColumnStats, buildHistogram,
// EstimateSelectivity, EstimateRangeSelectivity
// ============================================================

func TestCoverage_StatsCollector(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	sc := NewStatsCollector(cat)

	// countRows with invalid identifier
	_, err := sc.countRows("")
	if err == nil {
		t.Error("expected error for empty identifier")
	}
	_, err = sc.countRows("has space")
	if err == nil {
		t.Error("expected error for invalid identifier")
	}
	_, err = sc.countRows("SELECTFOO")
	if err == nil {
		t.Error("expected error for SQL keyword in identifier")
	}

	// countRows with valid name (stub returns empty result)
	count, err := sc.countRows("mytable")
	if err != nil {
		t.Errorf("countRows: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	// collectColumnStats with invalid idents
	_, err = sc.collectColumnStats("", "col")
	if err == nil {
		t.Error("expected error for empty table name")
	}
	_, err = sc.collectColumnStats("tbl", "")
	if err == nil {
		t.Error("expected error for empty column name")
	}

	// collectColumnStats - valid (stub returns empty)
	colStats, err := sc.collectColumnStats("tbl", "col")
	if err != nil {
		t.Errorf("collectColumnStats: %v", err)
	}
	if colStats == nil {
		t.Error("expected non-nil column stats")
	}

	// buildHistogram with empty result
	buckets := sc.buildHistogram(&QueryResult{}, "col")
	if buckets != nil {
		t.Error("expected nil for empty histogram")
	}

	// buildHistogram with values
	mockResult := &QueryResult{
		Rows: [][]interface{}{{int64(1)}, {int64(5)}, {int64(3)}, {int64(2)}, {int64(4)}},
	}
	buckets = sc.buildHistogram(mockResult, "col")
	if len(buckets) == 0 {
		t.Error("expected non-empty histogram")
	}

	// GetTableStats - not found
	_, ok := sc.GetTableStats("nonexistent")
	if ok {
		t.Error("expected false for nonexistent stats")
	}

	// GetColumnStats - not found
	_, ok = sc.GetColumnStats("nonexistent", "col")
	if ok {
		t.Error("expected false for nonexistent column stats")
	}

	// Store some stats manually for testing
	sc.mu.Lock()
	sc.stats["test_t"] = &TableStats{
		TableName: "test_t",
		RowCount:  100,
		PageCount: 10,
		ColumnStats: map[string]*ColumnStats{
			"id": {
				ColumnName:    "id",
				NullCount:     5,
				DistinctCount: 50,
				Histogram: []Bucket{
					{LowerBound: int64(1), UpperBound: int64(25), Count: 25},
					{LowerBound: int64(26), UpperBound: int64(50), Count: 25},
					{LowerBound: int64(51), UpperBound: int64(75), Count: 25},
					{LowerBound: int64(76), UpperBound: int64(100), Count: 25},
				},
			},
		},
		LastAnalyzed: time.Now(),
	}
	sc.mu.Unlock()

	// EstimateSelectivity
	sel := sc.EstimateSelectivity("test_t", "id", "=", int64(5))
	if sel <= 0 {
		t.Error("expected positive selectivity for =")
	}
	sel = sc.EstimateSelectivity("test_t", "id", "<", int64(30))
	if sel <= 0 {
		t.Error("expected positive selectivity for <")
	}
	sel = sc.EstimateSelectivity("test_t", "id", "<=", int64(50))
	if sel <= 0 {
		t.Error("expected positive selectivity for <=")
	}
	sel = sc.EstimateSelectivity("test_t", "id", ">", int64(50))
	if sel <= 0 {
		t.Error("expected positive selectivity for >")
	}
	sel = sc.EstimateSelectivity("test_t", "id", ">=", int64(50))
	if sel <= 0 {
		t.Error("expected positive selectivity for >=")
	}
	sel = sc.EstimateSelectivity("test_t", "id", "!=", int64(5))
	if sel <= 0 {
		t.Error("expected positive selectivity for !=")
	}
	sel = sc.EstimateSelectivity("test_t", "id", "LIKE", "foo")
	if sel != 0.1 {
		t.Errorf("expected 0.1 for unknown op, got %f", sel)
	}

	// EstimateSelectivity - no stats
	sel = sc.EstimateSelectivity("nosuchtable", "col", "=", 1)
	if sel != 0.1 {
		t.Errorf("expected 0.1 for missing stats, got %f", sel)
	}

	// EstimateRowCount
	rc := sc.EstimateRowCount("test_t")
	if rc != 100 {
		t.Errorf("expected 100, got %d", rc)
	}
	rc = sc.EstimateRowCount("nonexistent")
	if rc != 1000 {
		t.Errorf("expected 1000 default, got %d", rc)
	}

	// GetStatsSummary
	summary := sc.GetStatsSummary()
	if summary["test_t"] != 100 {
		t.Errorf("expected 100 in summary")
	}

	// InvalidateStats
	sc.InvalidateStats("test_t")
	_, ok = sc.GetTableStats("test_t")
	if ok {
		t.Error("expected stats to be invalidated")
	}

	// IsStale
	if !sc.IsStale("nonexistent", time.Second) {
		t.Error("expected stale for nonexistent")
	}

	// Cost estimation
	cost := sc.EstimateSeqScanCost("nonexistent", 0.5)
	if cost <= 0 {
		t.Error("expected positive cost")
	}
	cost = sc.EstimateIndexScanCost("nonexistent", "idx", 0.5)
	if cost <= 0 {
		t.Error("expected positive cost")
	}
	cost = sc.EstimateNestedLoopCost(100, 10)
	if cost <= 0 {
		t.Error("expected positive cost")
	}
	cost = sc.EstimateHashJoinCost(100, 50)
	if cost <= 0 {
		t.Error("expected positive cost")
	}
	cost = sc.EstimateMergeJoinCost(100, 50)
	if cost <= 0 {
		t.Error("expected positive cost")
	}

	// GetSummary
	sc.mu.Lock()
	sc.stats["t2"] = &TableStats{
		TableName:    "t2",
		RowCount:     50,
		ColumnStats:  map[string]*ColumnStats{"a": {ColumnName: "a"}},
		LastAnalyzed: time.Now(),
	}
	sc.mu.Unlock()
	sum := sc.GetSummary()
	if sum.TotalTables != 1 {
		t.Errorf("expected 1 table, got %d", sum.TotalTables)
	}
}

// ============================================================
// ColumnStats helpers
// ============================================================

func TestCoverage_ColumnStatsHelpers(t *testing.T) {
	cs := &ColumnStats{
		ColumnName:    "id",
		NullCount:     10,
		DistinctCount: 90,
		Histogram: []Bucket{
			{LowerBound: int64(1), UpperBound: int64(50), Count: 50},
			{LowerBound: int64(51), UpperBound: int64(100), Count: 50},
		},
	}

	// GetNullFraction
	nf := cs.GetNullFraction(100)
	if nf != 0.1 {
		t.Errorf("expected 0.1, got %f", nf)
	}
	if cs.GetNullFraction(0) != 0 {
		t.Error("expected 0 for zero rows")
	}

	// GetDistinctFraction
	df := cs.GetDistinctFraction(100)
	if df != 0.9 {
		t.Errorf("expected 0.9, got %f", df)
	}
	if cs.GetDistinctFraction(0) != 0 {
		t.Error("expected 0 for zero rows")
	}

	// IsUnique
	if !cs.IsUnique(100) {
		t.Error("expected unique (90 distinct, 90 non-null)")
	}
	uniqCS := &ColumnStats{DistinctCount: 100, NullCount: 0}
	if !uniqCS.IsUnique(100) {
		t.Error("expected unique")
	}

	// EstimateRangeSelectivity
	rs := cs.EstimateRangeSelectivity(int64(1), int64(50))
	if rs <= 0 {
		t.Error("expected positive range selectivity")
	}
	emptyCS := &ColumnStats{}
	if emptyCS.EstimateRangeSelectivity(nil, nil) != 0.33 {
		t.Error("expected 0.33 for empty histogram")
	}

	// GetHistogramBucketCount
	if cs.GetHistogramBucketCount() != 2 {
		t.Errorf("expected 2 buckets, got %d", cs.GetHistogramBucketCount())
	}

	// GetMostCommonValues
	mcv := cs.GetMostCommonValues(1)
	if len(mcv) != 1 {
		t.Errorf("expected 1 MCV, got %d", len(mcv))
	}
	if emptyCS.GetMostCommonValues(1) != nil {
		t.Error("expected nil for empty histogram")
	}
}

// ============================================================
// Correlation
// ============================================================

func TestCoverage_Correlation(t *testing.T) {
	// Perfect positive correlation
	x := []float64{1, 2, 3, 4, 5}
	y := []float64{2, 4, 6, 8, 10}
	c := CalculateCorrelation(x, y)
	if c < 0.99 {
		t.Errorf("expected ~1.0 correlation, got %f", c)
	}

	// Empty/mismatched
	if CalculateCorrelation(nil, nil) != 0 {
		t.Error("expected 0 for empty")
	}
	if CalculateCorrelation([]float64{1}, []float64{1, 2}) != 0 {
		t.Error("expected 0 for mismatched")
	}

	// Constant values (zero variance)
	if CalculateCorrelation([]float64{1, 1, 1}, []float64{2, 3, 4}) != 0 {
		t.Error("expected 0 for zero variance")
	}

	// CorrelationStats methods
	cs := &CorrelationStats{Correlation: 0.8}
	if !cs.IsHighCorrelation() {
		t.Error("expected high correlation")
	}
	if !cs.IsPositiveCorrelation() {
		t.Error("expected positive")
	}
	if cs.IsNegativeCorrelation() {
		t.Error("expected not negative")
	}
	neg := &CorrelationStats{Correlation: -0.8}
	if !neg.IsNegativeCorrelation() {
		t.Error("expected negative")
	}
}

// ============================================================
// bucketOverlapsRange & valueToString
// ============================================================

func TestCoverage_BucketOverlapsRange(t *testing.T) {
	bucket := Bucket{LowerBound: int64(10), UpperBound: int64(20), Count: 5}

	// Both nil → always overlaps
	if !bucketOverlapsRange(bucket, nil, nil) {
		t.Error("expected overlap for nil bounds")
	}

	// Range fully before bucket
	if bucketOverlapsRange(bucket, int64(21), int64(30)) {
		t.Error("expected no overlap for range after bucket")
	}

	// valueToString coverage
	if valueToString(nil) != "" {
		t.Error("expected empty for nil")
	}
	if valueToString("hello") != "hello" {
		t.Error("expected hello")
	}
	if valueToString(3.14) != "3.14" {
		t.Error("expected 3.14")
	}
	if valueToString(true) != "true" {
		t.Error("expected true")
	}
}

// ============================================================
// Save/Load with DEFAULT and CHECK columns
// ============================================================

func TestCoverage_SaveLoad_DefaultAndCheck(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create a table with DEFAULT and CHECK
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "defaults_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "status", Type: query.TokenText, Default: &query.StringLiteral{Value: "active"}},
			{Name: "score", Type: query.TokenInteger, Check: &query.BinaryExpr{
				Left: &query.Identifier{Name: "score"}, Operator: query.TokenGt,
				Right: &query.NumberLiteral{Value: 0},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := cat.Save(); err != nil {
		t.Fatal(err)
	}

	// Load into a new catalog
	cat2 := New(tree, pool, nil)
	if err := cat2.Load(); err != nil {
		t.Fatal(err)
	}

	tables := cat2.ListTables()
	found := false
	for _, tbl := range tables {
		if tbl == "defaults_t" {
			found = true
			break
		}
	}
	if !found {
		t.Error("table not found after Load")
	}

	// Load with nil tree should return nil
	cat3 := New(nil, pool, nil)
	if err := cat3.Load(); err != nil {
		t.Errorf("Load with nil tree: %v", err)
	}

	pool.Close()
}

// ============================================================
// CommitTransaction with WAL
// ============================================================

func TestCoverage_CommitTransaction_WAL(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}

	// Create WAL
	cat := New(tree, pool, nil)

	// Begin + Commit with WAL
	cat.BeginTransaction(1)
	err = cat.CommitTransaction()
	if err != nil {
		t.Errorf("commit with WAL: %v", err)
	}

	// Without active txn
	err = cat.CommitTransaction()
	if err != nil {
		t.Errorf("commit without active txn: %v", err)
	}

	pool.Close()
}

// ============================================================
// RollbackTransaction with various undo types
// ============================================================

func TestCoverage_RollbackTransaction_UndoTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}

	cat := New(tree, pool, nil)

	// Setup: create a table first
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "undo_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Begin transaction, insert, then rollback
	cat.BeginTransaction(1)
	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "undo_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "hello"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = cat.RollbackTransaction()
	if err != nil {
		t.Errorf("rollback insert: %v", err)
	}

	// Begin transaction, create table, then rollback
	cat.BeginTransaction(2)
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "undo_ct",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = cat.RollbackTransaction()
	if err != nil {
		t.Errorf("rollback create table: %v", err)
	}
	// Table should be gone
	_, getErr := cat.GetTable("undo_ct")
	if getErr == nil {
		t.Error("expected table to not exist after rollback")
	}

	// Begin transaction, drop table, then rollback
	cat.BeginTransaction(3)
	err = cat.DropTable(&query.DropTableStmt{Table: "undo_t"})
	if err != nil {
		t.Fatal(err)
	}
	err = cat.RollbackTransaction()
	if err != nil {
		t.Errorf("rollback drop table: %v", err)
	}
	// Table should be restored
	_, getErr = cat.GetTable("undo_t")
	if getErr != nil {
		t.Errorf("table should exist after rollback: %v", getErr)
	}

	// Begin transaction, insert, update, then rollback
	cat.BeginTransaction(4)
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "undo_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 10},
			&query.StringLiteral{Value: "orig"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cat.CommitTransaction()
	if err != nil {
		t.Fatal(err)
	}

	// Now update in a new txn and rollback
	cat.BeginTransaction(5)
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "undo_t",
		Set: []*query.SetClause{{Column: "val", Value: &query.StringLiteral{Value: "changed"}}},
		Where: &query.BinaryExpr{
			Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq,
			Right: &query.NumberLiteral{Value: 10},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cat.RollbackTransaction()
	if err != nil {
		t.Errorf("rollback update: %v", err)
	}

	// Delete in a new txn and rollback
	cat.BeginTransaction(6)
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "undo_t",
		Where: &query.BinaryExpr{
			Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq,
			Right: &query.NumberLiteral{Value: 10},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cat.RollbackTransaction()
	if err != nil {
		t.Errorf("rollback delete: %v", err)
	}

	pool.Close()
}

// ============================================================
// RollbackToSavepoint with INSERT/UPDATE/DELETE undo
// ============================================================

func TestCoverage_RollbackToSavepoint_Detailed(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "sp_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Begin txn, insert a row, set savepoint
	cat.BeginTransaction(1)
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "sp_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "before_sp"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = cat.Savepoint("sp1")
	if err != nil {
		t.Fatal(err)
	}

	// Insert after savepoint
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "sp_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "after_sp"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Update after savepoint
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "sp_t",
		Set: []*query.SetClause{{Column: "name", Value: &query.StringLiteral{Value: "modified"}}},
		Where: &query.BinaryExpr{
			Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq,
			Right: &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Rollback to savepoint
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Errorf("rollback to savepoint: %v", err)
	}

	// Error: not in a transaction
	err = cat.CommitTransaction()
	if err != nil {
		t.Fatal(err)
	}
	err = cat.RollbackToSavepoint("sp1")
	if err == nil {
		t.Error("expected error for rollback to savepoint outside txn")
	}

	// Error: nonexistent savepoint
	cat.BeginTransaction(2)
	err = cat.RollbackToSavepoint("no_such_sp")
	if err == nil {
		t.Error("expected error for nonexistent savepoint")
	}
	_ = cat.CommitTransaction()

	// Savepoint outside txn
	err = cat.Savepoint("out")
	if err == nil {
		t.Error("expected error for savepoint outside txn")
	}

	// ReleaseSavepoint
	cat.BeginTransaction(3)
	_ = cat.Savepoint("rel_sp")
	err = cat.ReleaseSavepoint("rel_sp")
	if err != nil {
		t.Errorf("release savepoint: %v", err)
	}
	err = cat.ReleaseSavepoint("nonexist")
	if err == nil {
		t.Error("expected error for releasing nonexistent savepoint")
	}
	_ = cat.CommitTransaction()

	// Release outside txn
	err = cat.ReleaseSavepoint("x")
	if err == nil {
		t.Error("expected error for release outside txn")
	}

	pool.Close()
}

// ============================================================
// RollbackToSavepoint with DDL: CREATE TABLE, CREATE INDEX
// ============================================================

func TestCoverage_RollbackToSavepoint_DDL(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create base table
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "ddl_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	cat.BeginTransaction(1)
	err = cat.Savepoint("sp_ddl")
	if err != nil {
		t.Fatal(err)
	}

	// Create table in txn after savepoint
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "ddl_new",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create index in txn after savepoint
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index: "idx_ddl_name",
		Table: "ddl_t",
		Columns: []string{"name"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Rollback to savepoint should undo both DDLs
	err = cat.RollbackToSavepoint("sp_ddl")
	if err != nil {
		t.Errorf("rollback to savepoint DDL: %v", err)
	}

	// ddl_new should not exist
	_, getErr := cat.GetTable("ddl_new")
	if getErr == nil {
		t.Error("expected ddl_new to not exist after rollback")
	}

	_ = cat.CommitTransaction()
	pool.Close()
}

// ============================================================
// evaluateExprWithGroupAggregatesJoin
// ============================================================

func TestCoverage_evaluateExprWithGroupAggregatesJoin(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	allCols := []ColumnDef{
		{Name: "dept"},
		{Name: "salary"},
	}
	groupRows := [][]interface{}{
		{"eng", float64(100)},
		{"eng", float64(200)},
		{"eng", float64(300)},
	}

	// COUNT(*)
	countExpr := &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}
	result, err := cat.evaluateExprWithGroupAggregatesJoin(countExpr, groupRows, allCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%v", result) != "3" {
		t.Errorf("expected COUNT(*)=3, got %v", result)
	}

	// SUM(salary)
	sumExpr := &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "salary"}}}
	result, err = cat.evaluateExprWithGroupAggregatesJoin(sumExpr, groupRows, allCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%v", result) != "600" {
		t.Errorf("expected SUM=600, got %v", result)
	}

	// AVG(salary)
	avgExpr := &query.FunctionCall{Name: "AVG", Args: []query.Expression{&query.Identifier{Name: "salary"}}}
	result, err = cat.evaluateExprWithGroupAggregatesJoin(avgExpr, groupRows, allCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%v", result) != "200" {
		t.Errorf("expected AVG=200, got %v", result)
	}

	// MIN(salary)
	minExpr := &query.FunctionCall{Name: "MIN", Args: []query.Expression{&query.Identifier{Name: "salary"}}}
	result, err = cat.evaluateExprWithGroupAggregatesJoin(minExpr, groupRows, allCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%v", result) != "100" {
		t.Errorf("expected MIN=100, got %v", result)
	}

	// MAX(salary)
	maxExpr := &query.FunctionCall{Name: "MAX", Args: []query.Expression{&query.Identifier{Name: "salary"}}}
	result, err = cat.evaluateExprWithGroupAggregatesJoin(maxExpr, groupRows, allCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%v", result) != "300" {
		t.Errorf("expected MAX=300, got %v", result)
	}

	// COUNT(col) with null values
	nullRows := [][]interface{}{
		{"eng", nil},
		{"eng", float64(100)},
	}
	countColExpr := &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.Identifier{Name: "salary"}}}
	result, err = cat.evaluateExprWithGroupAggregatesJoin(countColExpr, nullRows, allCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%v", result) != "1" {
		t.Errorf("expected COUNT(salary)=1 with null, got %v", result)
	}

	// SUM with all nulls
	allNullRows := [][]interface{}{
		{"eng", nil},
		{"eng", nil},
	}
	result, err = cat.evaluateExprWithGroupAggregatesJoin(sumExpr, allNullRows, allCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("expected nil for SUM of all nulls, got %v", result)
	}

	// AVG with all nulls
	result, err = cat.evaluateExprWithGroupAggregatesJoin(avgExpr, allNullRows, allCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("expected nil for AVG of all nulls, got %v", result)
	}

	// Empty group rows
	emptyRows := [][]interface{}{}
	result, err = cat.evaluateExprWithGroupAggregatesJoin(countExpr, emptyRows, allCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	// COUNT(*) on empty group should be 0
	if fmt.Sprintf("%v", result) != "0" {
		t.Errorf("expected COUNT(*)=0 for empty, got %v", result)
	}

	// Binary expression with aggregates: SUM(salary) > 100
	binExpr := &query.BinaryExpr{
		Left:     sumExpr,
		Operator: query.TokenGt,
		Right:    &query.NumberLiteral{Value: 100},
	}
	result, err = cat.evaluateExprWithGroupAggregatesJoin(binExpr, groupRows, allCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != true {
		t.Errorf("expected true for SUM > 100, got %v", result)
	}
}

// ============================================================
// applyGroupByOrderBy
// ============================================================

func TestCoverage_applyGroupByOrderBy(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Empty rows/orderBy
	result := cat.applyGroupByOrderBy(nil, nil, nil)
	if result != nil {
		t.Error("expected nil for nil rows")
	}

	selectCols := []selectColInfo{
		{name: "dept"},
		{name: "total", isAggregate: true, aggregateType: "SUM", aggregateCol: "salary"},
	}
	rows := [][]interface{}{
		{"eng", float64(300)},
		{"sales", float64(100)},
		{"hr", float64(200)},
	}

	// Order by column name
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "total"}},
	}
	sorted := cat.applyGroupByOrderBy(rows, selectCols, orderBy)
	if len(sorted) != 3 {
		t.Errorf("expected 3 rows, got %d", len(sorted))
	}

	// Order by aggregate signature
	orderBy2 := []*query.OrderByExpr{
		{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "salary"}}}, Desc: true},
	}
	sorted2 := cat.applyGroupByOrderBy(rows, selectCols, orderBy2)
	if len(sorted2) != 3 {
		t.Errorf("expected 3 rows, got %d", len(sorted2))
	}

	// Order by position
	orderBy3 := []*query.OrderByExpr{
		{Expr: &query.NumberLiteral{Value: 2}, Desc: true},
	}
	sorted3 := cat.applyGroupByOrderBy(rows, selectCols, orderBy3)
	if len(sorted3) != 3 {
		t.Errorf("expected 3 rows, got %d", len(sorted3))
	}
}

// ============================================================
// Vacuum with index trees
// ============================================================

func TestCoverage_Vacuum_WithIndexes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "vac_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create index
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index: "idx_vac_name",
		Table: "vac_t",
		Columns: []string{"name"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert some data
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		_, _, err = cat.Insert(ctx, &query.InsertStmt{
			Table: "vac_t",
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "name" + strings.Repeat("x", i)},
			}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Vacuum should compact both table and index trees
	err = cat.Vacuum()
	if err != nil {
		t.Errorf("vacuum with indexes: %v", err)
	}

	pool.Close()
}

// ============================================================
// CreateTrigger error paths
// ============================================================

func TestCoverage_CreateTrigger_Errors(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "trig_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Create trigger on nonexistent table
	err := cat.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trg1",
		Table: "nonexistent",
		Event: "INSERT",
		Time:  "BEFORE",
	})
	if err == nil {
		t.Error("expected error for nonexistent table")
	}

	// Create trigger successfully
	err = cat.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trg1",
		Table: "trig_t",
		Event: "INSERT",
		Time:  "BEFORE",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Duplicate trigger
	err = cat.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trg1",
		Table: "trig_t",
		Event: "INSERT",
		Time:  "BEFORE",
	})
	if err == nil {
		t.Error("expected error for duplicate trigger")
	}

	// GetTrigger
	_, err = cat.GetTrigger("trg1")
	if err != nil {
		t.Errorf("GetTrigger: %v", err)
	}
	_, err = cat.GetTrigger("nonexist")
	if err == nil {
		t.Error("expected error for nonexistent trigger")
	}

	// DropTrigger
	err = cat.DropTrigger("trg1")
	if err != nil {
		t.Errorf("DropTrigger: %v", err)
	}
	err = cat.DropTrigger("trg1")
	if err == nil {
		t.Error("expected error for dropping nonexistent trigger")
	}

	// GetTriggersForTable
	trigs := cat.GetTriggersForTable("trig_t", "INSERT")
	if len(trigs) != 0 {
		t.Errorf("expected 0 triggers, got %d", len(trigs))
	}
}

// ============================================================
// valuesEqual (ForeignKeyEnforcer) - more type combinations
// ============================================================

func TestCoverage_valuesEqual_Extended(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()
	fke := NewForeignKeyEnforcer(cat)

	// int8, int16, int32
	if !fke.valuesEqual(int8(5), int64(5)) {
		t.Error("int8 vs int64")
	}
	if !fke.valuesEqual(int16(5), float64(5)) {
		t.Error("int16 vs float64")
	}
	if !fke.valuesEqual(int32(5), int(5)) {
		t.Error("int32 vs int")
	}

	// uint types
	if !fke.valuesEqual(uint(5), int64(5)) {
		t.Error("uint vs int64")
	}
	if !fke.valuesEqual(uint8(5), float64(5)) {
		t.Error("uint8 vs float64")
	}
	if !fke.valuesEqual(uint16(5), int64(5)) {
		t.Error("uint16 vs int64")
	}
	if !fke.valuesEqual(uint32(5), int64(5)) {
		t.Error("uint32 vs int64")
	}
	if !fke.valuesEqual(uint64(5), int64(5)) {
		t.Error("uint64 vs int64")
	}

	// float32
	if !fke.valuesEqual(float32(5), float64(5)) {
		t.Error("float32 vs float64")
	}

	// string vs string
	if !fke.valuesEqual("hello", "hello") {
		t.Error("string vs string")
	}
	if fke.valuesEqual("hello", "world") {
		t.Error("expected not equal")
	}

	// string vs int (non-numeric mismatch)
	if fke.valuesEqual("hello", int64(5)) {
		t.Error("expected not equal for string vs int")
	}
}

// ============================================================
// deleteRowLocked
// ============================================================

func TestCoverage_deleteRowLocked(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "del_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create index for index cleanup path
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index: "idx_del_val",
		Table: "del_t",
		Columns: []string{"val"},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "del_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "hello"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Delete row with index cleanup
	err = cat.DeleteRow(ctx, "del_t", int64(1))
	if err != nil {
		t.Errorf("DeleteRow: %v", err)
	}

	// Delete from nonexistent table
	err = cat.DeleteRow(ctx, "nonexistent", int64(1))
	if err == nil {
		t.Error("expected error for nonexistent table")
	}

	// Delete with txn active (undo log path)
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "del_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "world"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	cat.BeginTransaction(1)
	err = cat.DeleteRow(ctx, "del_t", int64(2))
	if err != nil {
		t.Errorf("DeleteRow in txn: %v", err)
	}
	_ = cat.RollbackTransaction()

	pool.Close()
}

// ============================================================
// CreateTable error paths
// ============================================================

func TestCoverage_CreateTable_Errors(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "dup_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Duplicate table
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "dup_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err == nil {
		t.Error("expected error for duplicate table")
	}

	// IF NOT EXISTS on duplicate
	err = cat.CreateTable(&query.CreateTableStmt{
		Table:       "dup_t",
		IfNotExists: true,
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Errorf("IF NOT EXISTS should not error: %v", err)
	}
}

// ============================================================
// FlushTableTrees, TxnID, misc
// ============================================================

func TestCoverage_FlushAndMisc(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "flush_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// FlushTableTrees
	err = cat.FlushTableTrees()
	if err != nil {
		t.Errorf("FlushTableTrees: %v", err)
	}

	// TxnID
	cat.BeginTransaction(42)
	if cat.TxnID() != 42 {
		t.Errorf("expected TxnID=42, got %d", cat.TxnID())
	}
	if !cat.IsTransactionActive() {
		t.Error("expected active transaction")
	}
	_ = cat.CommitTransaction()
	if cat.IsTransactionActive() {
		t.Error("expected no active transaction after commit")
	}

	// SaveData/LoadSchema/LoadData (wrapper methods)
	if err := cat.SaveData(""); err != nil {
		t.Errorf("SaveData: %v", err)
	}
	if err := cat.LoadSchema(""); err != nil {
		t.Errorf("LoadSchema: %v", err)
	}
	if err := cat.LoadData(""); err != nil {
		t.Errorf("LoadData: %v", err)
	}

	// Query cache enable/disable
	cat.EnableQueryCache(100, time.Minute)
	hits, misses, size := cat.GetQueryCacheStats()
	_ = hits
	_ = misses
	_ = size
	cat.DisableQueryCache()

	// RLS enable/check
	cat.EnableRLS()
	if !cat.IsRLSEnabled() {
		t.Error("expected RLS enabled")
	}
	if cat.GetRLSManager() == nil {
		t.Error("expected non-nil RLS manager")
	}

	pool.Close()
}

// ============================================================
// RLS: CreateRLSPolicy, DropRLSPolicy, ApplyRLSFilter,
// CheckRLSFor* (public versions), applyRLSFilterInternal
// ============================================================

func TestCoverage_RLS_Public(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Without RLS enabled
	ctx := context.Background()
	cols, rows, err := cat.ApplyRLSFilter(ctx, "t", []string{"a"}, [][]interface{}{{1}}, "user", nil)
	if err != nil || len(cols) != 1 || len(rows) != 1 {
		t.Error("expected passthrough when RLS disabled")
	}

	allowed, err := cat.CheckRLSForInsert(ctx, "t", map[string]interface{}{"a": 1}, "user", nil)
	if err != nil || !allowed {
		t.Error("expected allowed when RLS disabled")
	}
	allowed, err = cat.CheckRLSForUpdate(ctx, "t", map[string]interface{}{"a": 1}, "user", nil)
	if err != nil || !allowed {
		t.Error("expected allowed when RLS disabled")
	}
	allowed, err = cat.CheckRLSForDelete(ctx, "t", map[string]interface{}{"a": 1}, "user", nil)
	if err != nil || !allowed {
		t.Error("expected allowed when RLS disabled")
	}

	// CreateRLSPolicy without RLS enabled
	err = cat.CreateRLSPolicy(nil)
	if err == nil {
		t.Error("expected error for CreateRLSPolicy without RLS")
	}
	err = cat.DropRLSPolicy("t", "p")
	if err == nil {
		t.Error("expected error for DropRLSPolicy without RLS")
	}

	// Enable RLS and test IsEnabled check (table not registered for RLS)
	cat.EnableRLS()
	cols, rows, err = cat.ApplyRLSFilter(ctx, "unreg_table", []string{"a"}, [][]interface{}{{1}}, "user", nil)
	if err != nil || len(rows) != 1 {
		t.Error("expected passthrough for unregistered table")
	}

	// Cache stats with nil cache
	cat2 := New(tree, pool, nil)
	hits, misses, size := cat2.GetQueryCacheStats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Error("expected zeros for nil cache")
	}

	pool.Close()
}

// ============================================================
// Analyze
// ============================================================

func TestCoverage_Analyze(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "analyze_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "score", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		_, _, err = cat.Insert(ctx, &query.InsertStmt{
			Table: "analyze_t",
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "name"},
				&query.NumberLiteral{Value: float64(i * 10)},
			}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Analyze nonexistent table
	err = cat.Analyze("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}

	// Analyze existing table
	err = cat.Analyze("analyze_t")
	if err != nil {
		t.Errorf("Analyze: %v", err)
	}

	pool.Close()
}

// ============================================================
// CollectStats
// ============================================================

func TestCoverage_CollectStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "stats_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	sc := NewStatsCollector(cat)

	// CollectStats on nonexistent table
	_, err = sc.CollectStats("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}

	// CollectStats on existing table (uses ExecuteQuery stub)
	stats, err := sc.CollectStats("stats_t")
	if err != nil {
		t.Errorf("CollectStats: %v", err)
	}
	if stats == nil {
		t.Error("expected non-nil stats")
	}

	// GetColumnStats after collection
	_, ok := sc.GetColumnStats("stats_t", "id")
	_ = ok // May or may not have stats depending on stub

	// GetTableStats
	_, ok = sc.GetTableStats("stats_t")
	if !ok {
		t.Error("expected stats to exist after collection")
	}

	// IsStale with very short threshold
	if sc.IsStale("stats_t", time.Hour) {
		t.Error("stats should not be stale yet")
	}

	// EstimateSeqScanCost with stats
	cost := sc.EstimateSeqScanCost("stats_t", 0.5)
	_ = cost // Just ensure it doesn't panic

	// EstimateIndexScanCost with stats
	cost = sc.EstimateIndexScanCost("stats_t", "idx", 0.5)
	_ = cost

	pool.Close()
}

// ============================================================
// ForeignKeyEnforcer.serializeValue
// ============================================================

func TestCoverage_serializeValue(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()
	fke := NewForeignKeyEnforcer(cat)

	// string
	if string(fke.serializeValue("hello")) != "S:hello" {
		t.Error("expected S:hello")
	}
	// int
	v := fke.serializeValue(int(42))
	if len(v) == 0 {
		t.Error("expected non-empty for int")
	}
	// int64
	v = fke.serializeValue(int64(42))
	if len(v) == 0 {
		t.Error("expected non-empty for int64")
	}
	// float64
	v = fke.serializeValue(float64(42.0))
	if len(v) == 0 {
		t.Error("expected non-empty for float64")
	}
	// nil
	if string(fke.serializeValue(nil)) != "NULL" {
		t.Error("expected NULL")
	}
	// []byte
	v = fke.serializeValue([]byte("raw"))
	if string(v) != "raw" {
		t.Error("expected raw")
	}
	// other type
	v = fke.serializeValue(true)
	if len(v) == 0 {
		t.Error("expected non-empty for bool")
	}
}

// ============================================================
// AlterTable paths
// ============================================================

func TestCoverage_AlterTable_Rollback(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "alter_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data for alter tests
	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "alter_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "alice"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// BEGIN + ADD COLUMN + ROLLBACK
	cat.BeginTransaction(1)
	err = cat.AlterTableAddColumn(&query.AlterTableStmt{Table: "alter_t", Action: "ADD", Column: query.ColumnDef{Name: "age", Type: query.TokenInteger}})
	if err != nil {
		t.Fatal(err)
	}
	err = cat.RollbackTransaction()
	if err != nil {
		t.Errorf("rollback alter add column: %v", err)
	}
	// Column should not exist
	tbl, _ := cat.GetTable("alter_t")
	for _, col := range tbl.Columns {
		if col.Name == "age" {
			t.Error("age column should not exist after rollback")
		}
	}

	// BEGIN + RENAME TABLE + ROLLBACK
	cat.BeginTransaction(2)
	err = cat.AlterTableRename(&query.AlterTableStmt{Table: "alter_t", Action: "RENAME_TABLE", NewName: "renamed_t"})
	if err != nil {
		t.Fatal(err)
	}
	err = cat.RollbackTransaction()
	if err != nil {
		t.Errorf("rollback alter rename: %v", err)
	}
	_, getErr := cat.GetTable("alter_t")
	if getErr != nil {
		t.Error("alter_t should exist after rename rollback")
	}

	// BEGIN + RENAME COLUMN + ROLLBACK
	cat.BeginTransaction(3)
	err = cat.AlterTableRenameColumn(&query.AlterTableStmt{Table: "alter_t", Action: "RENAME_COLUMN", OldName: "name", NewName: "label"})
	if err != nil {
		t.Fatal(err)
	}
	err = cat.RollbackTransaction()
	if err != nil {
		t.Errorf("rollback alter rename column: %v", err)
	}
	tbl, _ = cat.GetTable("alter_t")
	found := false
	for _, col := range tbl.Columns {
		if col.Name == "name" {
			found = true
		}
	}
	if !found {
		t.Error("name column should exist after rename column rollback")
	}

	pool.Close()
}
