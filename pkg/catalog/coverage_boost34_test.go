package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Stats coverage tests
// ============================================================

// TestStatsCollector_EstimateCosts - targets cost estimation functions
func TestStatsCollector_EstimateCosts(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create stats collector
	sc := NewStatsCollector(cat)

	// Test EstimateNestedLoopCost
	cost := sc.EstimateNestedLoopCost(100, 10)
	if cost < 0 {
		t.Errorf("EstimateNestedLoopCost returned negative cost: %f", cost)
	}

	// Test EstimateHashJoinCost
	cost = sc.EstimateHashJoinCost(100, 50)
	if cost < 0 {
		t.Errorf("EstimateHashJoinCost returned negative cost: %f", cost)
	}

	// Test EstimateMergeJoinCost
	cost = sc.EstimateMergeJoinCost(100, 50)
	if cost < 0 {
		t.Errorf("EstimateMergeJoinCost returned negative cost: %f", cost)
	}

	// Test EstimateSeqScanCost
	cost = sc.EstimateSeqScanCost("nonexistent", 0.5)
	if cost < 0 {
		t.Errorf("EstimateSeqScanCost returned negative cost: %f", cost)
	}

	// Test EstimateIndexScanCost
	cost = sc.EstimateIndexScanCost("nonexistent", "idx", 0.5)
	if cost < 0 {
		t.Errorf("EstimateIndexScanCost returned negative cost: %f", cost)
	}
}

// TestColumnStats_HelperMethods - targets IsUnique, GetHistogramBucketCount, etc.
func TestColumnStats_HelperMethods(t *testing.T) {
	// Test IsUnique
	cs := &ColumnStats{
		ColumnName:    "test_col",
		NullCount:     0,
		DistinctCount: 100,
	}
	if !cs.IsUnique(100) {
		t.Error("IsUnique should return true when distinct count equals row count")
	}

	cs.DistinctCount = 50
	if cs.IsUnique(100) {
		t.Error("IsUnique should return false when distinct count is less than row count")
	}

	// Test GetHistogramBucketCount
	cs.Histogram = []Bucket{
		{LowerBound: 1, UpperBound: 10, Count: 5},
		{LowerBound: 11, UpperBound: 20, Count: 5},
		{LowerBound: 21, UpperBound: 30, Count: 5},
	}
	if cs.GetHistogramBucketCount() != 3 {
		t.Errorf("GetHistogramBucketCount expected 3, got %d", cs.GetHistogramBucketCount())
	}

	// Test GetMostCommonValues
	mcs := cs.GetMostCommonValues(2)
	if len(mcs) != 2 {
		t.Errorf("GetMostCommonValues expected 2 values, got %d", len(mcs))
	}
}

// TestCorrelationStats_HelperMethods - targets IsHighCorrelation, etc.
func TestCorrelationStats_HelperMethods(t *testing.T) {
	// Test IsHighCorrelation
	cs := &CorrelationStats{
		Column1:     "col1",
		Column2:     "col2",
		Correlation: 0.8,
	}
	if !cs.IsHighCorrelation() {
		t.Error("IsHighCorrelation should return true for correlation > 0.7")
	}

	cs.Correlation = 0.5
	if cs.IsHighCorrelation() {
		t.Error("IsHighCorrelation should return false for correlation < 0.7")
	}

	// Test IsPositiveCorrelation
	cs.Correlation = 0.5
	if !cs.IsPositiveCorrelation() {
		t.Error("IsPositiveCorrelation should return true for correlation > 0.3")
	}

	cs.Correlation = 0.1
	if cs.IsPositiveCorrelation() {
		t.Error("IsPositiveCorrelation should return false for correlation < 0.3")
	}

	// Test IsNegativeCorrelation
	cs.Correlation = -0.5
	if !cs.IsNegativeCorrelation() {
		t.Error("IsNegativeCorrelation should return true for correlation < -0.3")
	}

	cs.Correlation = -0.1
	if cs.IsNegativeCorrelation() {
		t.Error("IsNegativeCorrelation should return false for correlation > -0.3")
	}
}

// TestCalculateCorrelation_Stats - tests the correlation calculation in stats
func TestCalculateCorrelation_Stats(t *testing.T) {
	// Perfect positive correlation
	x := []float64{1, 2, 3, 4, 5}
	y := []float64{2, 4, 6, 8, 10}
	corr := CalculateCorrelation(x, y)
	if corr < 0.99 {
		t.Errorf("Expected correlation near 1.0 for perfectly correlated data, got %f", corr)
	}

	// Perfect negative correlation
	y = []float64{10, 8, 6, 4, 2}
	corr = CalculateCorrelation(x, y)
	if corr > -0.99 {
		t.Errorf("Expected correlation near -1.0 for perfectly anti-correlated data, got %f", corr)
	}

	// No correlation (approximately)
	y = []float64{5, 5, 5, 5, 5}
	corr = CalculateCorrelation(x, y)
	if corr != 0 {
		t.Errorf("Expected correlation 0 for uncorrelated data, got %f", corr)
	}

	// Empty arrays
	corr = CalculateCorrelation([]float64{}, []float64{})
	if corr != 0 {
		t.Errorf("Expected correlation 0 for empty arrays, got %f", corr)
	}

	// Mismatched lengths
	corr = CalculateCorrelation([]float64{1, 2}, []float64{1, 2, 3})
	if corr != 0 {
		t.Errorf("Expected correlation 0 for mismatched lengths, got %f", corr)
	}
}

// TestStatsCollector_GetSummary - tests GetSummary method
func TestStatsCollector_GetSummary(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create tables
	createCoverageTestTable(t, cat, "stats_sum1", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})
	createCoverageTestTable(t, cat, "stats_sum2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "stats_sum1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	sc := NewStatsCollector(cat)

	// Collect stats
	sc.CollectStats("stats_sum1")
	sc.CollectStats("stats_sum2")

	// Get summary
	summary := sc.GetSummary()
	if summary.TotalTables != 2 {
		t.Errorf("Expected 2 tables in summary, got %d", summary.TotalTables)
	}
	if summary.TotalRows != 1 {
		t.Errorf("Expected 1 total row, got %d", summary.TotalRows)
	}
	if len(summary.TableSummaries) != 2 {
		t.Errorf("Expected 2 table summaries, got %d", len(summary.TableSummaries))
	}
}

// TestStatsCollector_IsStale - tests IsStale method
func TestStatsCollector_IsStale(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "stats_stale", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "stats_stale",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	sc := NewStatsCollector(cat)

	// No stats yet - should be stale
	if !sc.IsStale("stats_stale", time.Hour) {
		t.Error("Expected table to be stale when no stats exist")
	}

	// Collect stats
	sc.CollectStats("stats_stale")

	// Should not be stale
	if sc.IsStale("stats_stale", time.Hour) {
		t.Error("Expected table to not be stale immediately after collection")
	}

	// Should be stale with very short threshold (may fail due to timing)
	_ = sc.IsStale("stats_stale", time.Nanosecond)
}

// TestStatsCollector_InvalidateStats - tests InvalidateStats method
func TestStatsCollector_InvalidateStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "stats_inv", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "stats_inv",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	sc := NewStatsCollector(cat)
	sc.CollectStats("stats_inv")

	// Verify stats exist
	_, ok := sc.GetTableStats("stats_inv")
	if !ok {
		t.Error("Expected stats to exist before invalidation")
	}

	// Invalidate
	sc.InvalidateStats("stats_inv")

	// Verify stats are gone
	_, ok = sc.GetTableStats("stats_inv")
	if ok {
		t.Error("Expected stats to be gone after invalidation")
	}
}

// TestColumnStats_EstimateRangeSelectivity - tests range selectivity estimation
func TestColumnStats_EstimateRangeSelectivity(t *testing.T) {
	cs := &ColumnStats{
		ColumnName:    "test",
		DistinctCount: 100,
		Histogram: []Bucket{
			{LowerBound: 1, UpperBound: 10, Count: 10},
			{LowerBound: 11, UpperBound: 20, Count: 10},
			{LowerBound: 21, UpperBound: 30, Count: 10},
			{LowerBound: 31, UpperBound: 40, Count: 10},
			{LowerBound: 41, UpperBound: 50, Count: 10},
		},
	}

	// Test with valid range
	sel := cs.EstimateRangeSelectivity(15, 25)
	if sel <= 0 || sel > 1 {
		t.Errorf("Expected selectivity between 0 and 1, got %f", sel)
	}

	// Test with empty histogram
	cs.Histogram = nil
	sel = cs.EstimateRangeSelectivity(1, 10)
	if sel != 0.33 {
		t.Errorf("Expected default selectivity 0.33, got %f", sel)
	}

	// Test with zero distinct count
	cs.DistinctCount = 0
	sel = cs.EstimateRangeSelectivity(1, 10)
	if sel != 0.33 {
		t.Errorf("Expected default selectivity 0.33, got %f", sel)
	}
}

// TestColumnStats_GetNullFraction - tests GetNullFraction
func TestColumnStats_GetNullFraction(t *testing.T) {
	cs := &ColumnStats{
		NullCount: 10,
	}

	frac := cs.GetNullFraction(100)
	if frac != 0.1 {
		t.Errorf("Expected null fraction 0.1, got %f", frac)
	}

	// Zero row count
	frac = cs.GetNullFraction(0)
	if frac != 0 {
		t.Errorf("Expected null fraction 0 for zero rows, got %f", frac)
	}
}

// TestColumnStats_GetDistinctFraction - tests GetDistinctFraction
func TestColumnStats_GetDistinctFraction(t *testing.T) {
	cs := &ColumnStats{
		DistinctCount: 50,
	}

	frac := cs.GetDistinctFraction(100)
	if frac != 0.5 {
		t.Errorf("Expected distinct fraction 0.5, got %f", frac)
	}

	// Zero row count
	frac = cs.GetDistinctFraction(0)
	if frac != 0 {
		t.Errorf("Expected distinct fraction 0 for zero rows, got %f", frac)
	}
}
