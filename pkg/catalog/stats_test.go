package catalog

import (
	"math"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestStatsCollectorNew(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	if collector == nil {
		t.Fatal("Expected non-nil collector")
	}
	if collector.catalog != catalog {
		t.Error("Expected catalog to be set")
	}
	if collector.stats == nil {
		t.Error("Expected stats map to be initialized")
	}
}

func TestStatsCollectorGetTableStatsNotFound(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	_, found := collector.GetTableStats("non_existent")
	if found {
		t.Error("Expected not found for non-existent table")
	}
}

func TestStatsCollectorGetColumnStatsNotFound(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Test table not found
	_, found := collector.GetColumnStats("non_existent", "col")
	if found {
		t.Error("Expected not found for non-existent table")
	}

	// Create table but no stats collected
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
		},
	})

	_, found = collector.GetColumnStats("test", "id")
	if found {
		t.Error("Expected not found when stats not collected")
	}
}

func TestStatsCollectorInvalidateStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Create table and collect stats
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
		},
	})

	// Manually add stats
	collector.stats["test"] = &TableStats{
		TableName: "test",
		RowCount:  100,
	}

	// Verify stats exist
	_, found := collector.GetTableStats("test")
	if !found {
		t.Error("Expected to find stats")
	}

	// Invalidate stats
	collector.InvalidateStats("test")

	// Verify stats removed
	_, found = collector.GetTableStats("test")
	if found {
		t.Error("Expected stats to be invalidated")
	}
}

func TestStatsCollectorInvalidateStatsNonExistent(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Should not panic when invalidating non-existent stats
	collector.InvalidateStats("non_existent")
}

func TestStatsCollectorIsStaleNoStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// No stats means stale
	if !collector.IsStale("non_existent", time.Hour) {
		t.Error("Expected no stats to be stale")
	}
}

func TestStatsCollectorIsStaleFresh(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Manually add fresh stats
	collector.stats["test"] = &TableStats{
		TableName:    "test",
		LastAnalyzed: time.Now(),
	}

	// Should not be stale
	if collector.IsStale("test", time.Hour) {
		t.Error("Expected fresh stats to not be stale")
	}
}

func TestStatsCollectorIsStaleOld(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Manually add old stats
	collector.stats["test"] = &TableStats{
		TableName:    "test",
		LastAnalyzed: time.Now().Add(-2 * time.Hour),
	}

	// Should be stale
	if !collector.IsStale("test", time.Hour) {
		t.Error("Expected old stats to be stale")
	}
}

func TestStatsCollectorGetStatsSummaryEmpty(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	summary := collector.GetStatsSummary()
	if len(summary) != 0 {
		t.Errorf("Expected empty summary, got %d entries", len(summary))
	}
}

func TestStatsCollectorGetStatsSummaryMultiple(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats for multiple tables
	collector.stats["table1"] = &TableStats{TableName: "table1", RowCount: 100}
	collector.stats["table2"] = &TableStats{TableName: "table2", RowCount: 200}
	collector.stats["table3"] = &TableStats{TableName: "table3", RowCount: 300}

	summary := collector.GetStatsSummary()
	if len(summary) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(summary))
	}

	if summary["table1"] != 100 {
		t.Errorf("Expected table1 row count 100, got %d", summary["table1"])
	}
	if summary["table2"] != 200 {
		t.Errorf("Expected table2 row count 200, got %d", summary["table2"])
	}
	if summary["table3"] != 300 {
		t.Errorf("Expected table3 row count 300, got %d", summary["table3"])
	}
}

func TestStatsCollectorEstimateRowCountDefault(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Default estimate when no stats
	count := collector.EstimateRowCount("non_existent")
	if count != 1000 {
		t.Errorf("Expected default estimate 1000, got %d", count)
	}
}

func TestStatsCollectorEstimateRowCountWithStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats
	collector.stats["test"] = &TableStats{TableName: "test", RowCount: 5000}

	count := collector.EstimateRowCount("test")
	if count != 5000 {
		t.Errorf("Expected row count 5000, got %d", count)
	}
}

func TestStatsCollectorEstimateSelectivityNoStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Default selectivity when no stats
	sel := collector.EstimateSelectivity("non_existent", "col", "=", 42)
	if sel != 0.1 {
		t.Errorf("Expected default selectivity 0.1, got %f", sel)
	}
}

func TestStatsCollectorEstimateSelectivityEquality(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats with column info
	collector.stats["test"] = &TableStats{
		TableName: "test",
		RowCount:  1000,
		ColumnStats: map[string]*ColumnStats{
			"id": {ColumnName: "id", DistinctCount: 100},
		},
	}

	// Equality selectivity should be 1/distinct
	sel := collector.EstimateSelectivity("test", "id", "=", 42)
	expected := 1.0 / 100.0
	if math.Abs(sel-expected) > 0.0001 {
		t.Errorf("Expected selectivity %f, got %f", expected, sel)
	}
}

func TestStatsCollectorEstimateSelectivityNotEqual(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats with column info
	collector.stats["test"] = &TableStats{
		TableName: "test",
		RowCount:  1000,
		ColumnStats: map[string]*ColumnStats{
			"id": {ColumnName: "id", DistinctCount: 100},
		},
	}

	// Not equal selectivity should be 1 - 1/distinct
	sel := collector.EstimateSelectivity("test", "id", "!=", 42)
	expected := 1.0 - (1.0 / 100.0)
	if math.Abs(sel-expected) > 0.0001 {
		t.Errorf("Expected selectivity %f, got %f", expected, sel)
	}
}

func TestStatsCollectorEstimateSelectivityRangeNoHistogram(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats without histogram
	collector.stats["test"] = &TableStats{
		TableName: "test",
		RowCount:  1000,
		ColumnStats: map[string]*ColumnStats{
			"id": {ColumnName: "id", DistinctCount: 100, Histogram: nil},
		},
	}

	// Range selectivity without histogram should be 0.33
	sel := collector.EstimateSelectivity("test", "id", "<", 50)
	if sel != 0.33 {
		t.Errorf("Expected default range selectivity 0.33, got %f", sel)
	}
}

func TestStatsCollectorEstimateSelectivityZeroDistinct(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats with zero distinct count
	collector.stats["test"] = &TableStats{
		TableName: "test",
		RowCount:  1000,
		ColumnStats: map[string]*ColumnStats{
			"id": {ColumnName: "id", DistinctCount: 0},
		},
	}

	// Should use default selectivity
	sel := collector.EstimateSelectivity("test", "id", "=", 42)
	if sel != 0.1 {
		t.Errorf("Expected default selectivity 0.1, got %f", sel)
	}
}

func TestStatsCollectorEstimateSelectivityUnknownOp(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats
	collector.stats["test"] = &TableStats{
		TableName: "test",
		RowCount:  1000,
		ColumnStats: map[string]*ColumnStats{
			"id": {ColumnName: "id", DistinctCount: 100},
		},
	}

	// Unknown operator should use default selectivity
	sel := collector.EstimateSelectivity("test", "id", "LIKE", "pattern")
	if sel != 0.1 {
		t.Errorf("Expected default selectivity 0.1, got %f", sel)
	}
}

func TestStatsCollectorEstimateSeqScanCostNoStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Default cost when no stats
	cost := collector.EstimateSeqScanCost("non_existent", 0.5)
	expected := 1000 * SeqPageCost
	if cost != expected {
		t.Errorf("Expected default cost %f, got %f", expected, cost)
	}
}

func TestStatsCollectorEstimateSeqScanCostWithStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats with page count
	collector.stats["test"] = &TableStats{
		TableName: "test",
		RowCount:  10000,
		PageCount: 100,
	}

	cost := collector.EstimateSeqScanCost("test", 0.5)

	// Cost should be page reads + CPU processing
	expectedPageCost := 100 * SeqPageCost
	expectedCpuCost := 10000 * CpuTupleCost * 0.5
	expected := expectedPageCost + expectedCpuCost

	if math.Abs(cost-expected) > 0.01 {
		t.Errorf("Expected cost %f, got %f", expected, cost)
	}
}

func TestStatsCollectorEstimateSeqScanCostZeroPages(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats without page count (should estimate from row count)
	collector.stats["test"] = &TableStats{
		TableName: "test",
		RowCount:  10000,
		PageCount: 0,
	}

	cost := collector.EstimateSeqScanCost("test", 1.0)

	// Should estimate pages as rows/100
	expectedPages := float64(10000) / 100
	expectedPageCost := expectedPages * SeqPageCost
	expectedCpuCost := 10000 * CpuTupleCost * 1.0
	expected := expectedPageCost + expectedCpuCost

	if math.Abs(cost-expected) > 0.01 {
		t.Errorf("Expected cost %f, got %f", expected, cost)
	}
}

func TestStatsCollectorEstimateIndexScanCostNoStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Default cost when no stats
	cost := collector.EstimateIndexScanCost("non_existent", "idx", 0.1)
	expected := 100 * RandomPageCost
	if cost != expected {
		t.Errorf("Expected default cost %f, got %f", expected, cost)
	}
}

func TestStatsCollectorEstimateIndexScanCostWithStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats
	collector.stats["test"] = &TableStats{
		TableName: "test",
		RowCount:  10000,
	}

	cost := collector.EstimateIndexScanCost("test", "idx", 0.1)

	// Should be greater than 0
	if cost <= 0 {
		t.Errorf("Expected positive cost, got %f", cost)
	}
}

func TestStatsCollectorEstimateNestedLoopCost(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	cost := collector.EstimateNestedLoopCost(100, 5.0)
	expected := 100 * 5.0
	if cost != expected {
		t.Errorf("Expected cost %f, got %f", expected, cost)
	}
}

func TestStatsCollectorEstimateHashJoinCost(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	cost := collector.EstimateHashJoinCost(1000, 500)
	expectedBuild := 500 * CpuTupleCost
	expectedProbe := 1000 * CpuOperatorCost
	expected := expectedBuild + expectedProbe

	if math.Abs(cost-expected) > 0.01 {
		t.Errorf("Expected cost %f, got %f", expected, cost)
	}
}

func TestStatsCollectorEstimateMergeJoinCost(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	outerRows := float64(1000)
	innerRows := float64(500)
	cost := collector.EstimateMergeJoinCost(outerRows, innerRows)

	// Sort cost + merge cost
	sortCost := (outerRows + innerRows) * math.Log2(outerRows+innerRows) * CpuTupleCost
	mergeCost := (outerRows + innerRows) * CpuTupleCost
	expected := sortCost + mergeCost

	if math.Abs(cost-expected) > 0.01 {
		t.Errorf("Expected cost %f, got %f", expected, cost)
	}
}

func TestStatsCollectorEstimateRangeSelectivity(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Create histogram
	histogram := []Bucket{
		{LowerBound: 1, UpperBound: 10, Count: 100},
		{LowerBound: 11, UpperBound: 20, Count: 100},
		{LowerBound: 21, UpperBound: 30, Count: 100},
	}

	colStats := &ColumnStats{
		ColumnName: "id",
		Histogram:  histogram,
	}

	// Test less than
	sel := collector.estimateRangeSelectivity(colStats, "<", 15)
	// Should match first bucket and part of second
	if sel <= 0 || sel > 1 {
		t.Errorf("Expected selectivity between 0 and 1, got %f", sel)
	}

	// Test greater than
	sel = collector.estimateRangeSelectivity(colStats, ">", 15)
	if sel <= 0 || sel > 1 {
		t.Errorf("Expected selectivity between 0 and 1, got %f", sel)
	}

	// Test less than or equal
	sel = collector.estimateRangeSelectivity(colStats, "<=", 20)
	if sel <= 0 || sel > 1 {
		t.Errorf("Expected selectivity between 0 and 1, got %f", sel)
	}

	// Test greater than or equal
	sel = collector.estimateRangeSelectivity(colStats, ">=", 20)
	if sel <= 0 || sel > 1 {
		t.Errorf("Expected selectivity between 0 and 1, got %f", sel)
	}
}

func TestStatsCollectorEstimateRangeSelectivityEmptyHistogram(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	colStats := &ColumnStats{
		ColumnName: "id",
		Histogram:  []Bucket{},
	}

	sel := collector.estimateRangeSelectivity(colStats, "<", 50)
	if sel != 0.33 {
		t.Errorf("Expected default selectivity 0.33, got %f", sel)
	}
}

func TestStatsCollectorEstimateRangeSelectivityZeroTotal(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Create histogram with zero counts
	histogram := []Bucket{
		{LowerBound: 1, UpperBound: 10, Count: 0},
	}

	colStats := &ColumnStats{
		ColumnName: "id",
		Histogram:  histogram,
	}

	sel := collector.estimateRangeSelectivity(colStats, "<", 5)
	if sel != 0.33 {
		t.Errorf("Expected default selectivity 0.33, got %f", sel)
	}
}

func TestStatsCollectorBuildHistogramEmpty(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	result := &QueryResult{
		Columns: []string{"value"},
		Rows:    [][]interface{}{},
	}

	histogram := collector.buildHistogram(result, "value")
	if histogram != nil {
		t.Error("Expected nil histogram for empty result")
	}
}

func TestStatsCollectorBuildHistogramSingleValue(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	result := &QueryResult{
		Columns: []string{"value"},
		Rows:    [][]interface{}{{1}},
	}

	histogram := collector.buildHistogram(result, "value")
	if len(histogram) != 1 {
		t.Errorf("Expected 1 bucket, got %d", len(histogram))
	}
	if histogram[0].Count != 1 {
		t.Errorf("Expected count 1, got %d", histogram[0].Count)
	}
}

func TestStatsCollectorBuildHistogramManyValues(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Create 200 values
	rows := make([][]interface{}, 200)
	for i := 0; i < 200; i++ {
		rows[i] = []interface{}{i}
	}

	result := &QueryResult{
		Columns: []string{"value"},
		Rows:    rows,
	}

	histogram := collector.buildHistogram(result, "value")

	// Should have at most 100 buckets
	if len(histogram) > 100 {
		t.Errorf("Expected at most 100 buckets, got %d", len(histogram))
	}

	// Total count should be 200
	totalCount := uint64(0)
	for _, b := range histogram {
		totalCount += b.Count
	}
	if totalCount != 200 {
		t.Errorf("Expected total count 200, got %d", totalCount)
	}
}

func TestStatsCollectorBuildHistogramWithNulls(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	result := &QueryResult{
		Columns: []string{"value"},
		Rows: [][]interface{}{
			{1},
			{nil},
			{2},
			{nil},
			{3},
		},
	}

	histogram := collector.buildHistogram(result, "value")

	// Should handle nulls gracefully
	if histogram == nil {
		t.Error("Expected non-nil histogram")
	}
}

func TestStatsCollectorCollectStatsNonExistentTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	_, err := collector.CollectStats("non_existent")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestStatsCollectorCollectStatsEmptyTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Create empty table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})

	stats, err := collector.CollectStats("test")
	if err != nil {
		t.Errorf("Expected no error for empty table: %v", err)
	}
	if stats == nil {
		t.Fatal("Expected non-nil stats")
	}
	if stats.RowCount != 0 {
		t.Errorf("Expected row count 0, got %d", stats.RowCount)
	}
	if len(stats.ColumnStats) != 2 {
		t.Errorf("Expected 2 column stats, got %d", len(stats.ColumnStats))
	}
}

func TestStatsCollectorTableStatsConcurrency(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	catalog := New(nil, pool, nil)
	collector := NewStatsCollector(catalog)

	// Add stats
	collector.stats["test"] = &TableStats{
		TableName:    "test",
		RowCount:     100,
		LastAnalyzed: time.Now(),
		ColumnStats:  make(map[string]*ColumnStats),
	}

	// Concurrent reads
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			collector.GetTableStats("test")
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
