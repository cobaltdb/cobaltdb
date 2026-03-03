package catalog

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// TableStats holds statistics for a table
type TableStats struct {
	TableName    string
	RowCount     uint64
	PageCount    uint64
	LastAnalyzed time.Time
	ColumnStats  map[string]*ColumnStats
	mu           sync.RWMutex
}

// ColumnStats holds statistics for a column
type ColumnStats struct {
	ColumnName    string
	NullCount     uint64
	DistinctCount uint64
	MinValue      interface{}
	MaxValue      interface{}
	Histogram     []Bucket
	AvgWidth      int // Average byte width
}

// Bucket represents a histogram bucket
type Bucket struct {
	LowerBound interface{}
	UpperBound interface{}
	Count      uint64
}

// StatsCollector collects and manages table statistics
type StatsCollector struct {
	catalog *Catalog
	stats   map[string]*TableStats
	mu      sync.RWMutex
}

// NewStatsCollector creates a new statistics collector
func NewStatsCollector(catalog *Catalog) *StatsCollector {
	return &StatsCollector{
		catalog: catalog,
		stats:   make(map[string]*TableStats),
	}
}

// CollectStats collects statistics for a table
func (sc *StatsCollector) CollectStats(tableName string) (*TableStats, error) {
	table, err := sc.catalog.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	stats := &TableStats{
		TableName:    tableName,
		ColumnStats:  make(map[string]*ColumnStats),
		LastAnalyzed: time.Now(),
	}

	// Get row count
	rowCount, err := sc.countRows(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to count rows: %w", err)
	}
	stats.RowCount = rowCount

	// Collect column statistics
	for _, col := range table.Columns {
		colStats, err := sc.collectColumnStats(tableName, col.Name)
		if err != nil {
			continue // Skip columns that fail
		}
		stats.ColumnStats[col.Name] = colStats
	}

	// Store stats
	sc.mu.Lock()
	sc.stats[tableName] = stats
	sc.mu.Unlock()

	return stats, nil
}

// countRows counts rows in a table
func (sc *StatsCollector) countRows(tableName string) (uint64, error) {
	// Use SELECT COUNT(*) to get row count
	result, err := sc.catalog.ExecuteQuery(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	if err != nil {
		return 0, err
	}

	if len(result.Rows) == 0 {
		return 0, nil
	}

	// Extract count from result
	count, ok := result.Rows[0][0].(int64)
	if !ok {
		// Try other numeric types
		if f, ok := result.Rows[0][0].(float64); ok {
			count = int64(f)
		} else {
			return 0, fmt.Errorf("unexpected count type")
		}
	}

	return uint64(count), nil
}

// collectColumnStats collects statistics for a column
func (sc *StatsCollector) collectColumnStats(tableName, columnName string) (*ColumnStats, error) {
	stats := &ColumnStats{
		ColumnName: columnName,
	}

	// Get distinct values
	distinctQuery := fmt.Sprintf("SELECT DISTINCT %s FROM %s", columnName, tableName)
	result, err := sc.catalog.ExecuteQuery(distinctQuery)
	if err != nil {
		return nil, err
	}

	stats.DistinctCount = uint64(len(result.Rows))

	// Count nulls
	nullQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NULL", tableName, columnName)
	nullResult, err := sc.catalog.ExecuteQuery(nullQuery)
	if err == nil && len(nullResult.Rows) > 0 {
		if count, ok := nullResult.Rows[0][0].(int64); ok {
			stats.NullCount = uint64(count)
		}
	}

	// Get min/max
	minMaxQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", columnName, columnName, tableName)
	minMaxResult, err := sc.catalog.ExecuteQuery(minMaxQuery)
	if err == nil && len(minMaxResult.Rows) > 0 {
		stats.MinValue = minMaxResult.Rows[0][0]
		stats.MaxValue = minMaxResult.Rows[0][1]
	}

	// Build histogram for columns with reasonable distinct counts
	if stats.DistinctCount > 0 && stats.DistinctCount <= 10000 {
		stats.Histogram = sc.buildHistogram(result, columnName)
	}

	return stats, nil
}

// buildHistogram builds a histogram from distinct values
func (sc *StatsCollector) buildHistogram(result *QueryResult, columnName string) []Bucket {
	if len(result.Rows) == 0 {
		return nil
	}

	// Extract values
	values := make([]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) > 0 {
			values = append(values, row[0])
		}
	}

	// Sort values
	sort.Slice(values, func(i, j int) bool {
		return compareValues(values[i], values[j]) < 0
	})

	// Create buckets (max 100 buckets)
	numBuckets := 100
	if len(values) < numBuckets {
		numBuckets = len(values)
	}

	bucketSize := len(values) / numBuckets
	if bucketSize == 0 {
		bucketSize = 1
	}

	buckets := make([]Bucket, 0, numBuckets)
	for i := 0; i < len(values); i += bucketSize {
		end := i + bucketSize
		if end > len(values) {
			end = len(values)
		}

		bucket := Bucket{
			LowerBound: values[i],
			UpperBound: values[end-1],
			Count:      uint64(end - i),
		}
		buckets = append(buckets, bucket)
	}

	return buckets
}

// GetTableStats returns statistics for a table
func (sc *StatsCollector) GetTableStats(tableName string) (*TableStats, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	stats, ok := sc.stats[tableName]
	return stats, ok
}

// GetColumnStats returns statistics for a column
func (sc *StatsCollector) GetColumnStats(tableName, columnName string) (*ColumnStats, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	stats, ok := sc.stats[tableName]
	if !ok {
		return nil, false
	}

	colStats, ok := stats.ColumnStats[columnName]
	return colStats, ok
}

// EstimateSelectivity estimates the selectivity of a condition
func (sc *StatsCollector) EstimateSelectivity(tableName, columnName string, op string, value interface{}) float64 {
	colStats, ok := sc.GetColumnStats(tableName, columnName)
	if !ok {
		// No stats, use default selectivity
		return 0.1
	}

	tableStats, _ := sc.GetTableStats(tableName)
	if tableStats == nil || tableStats.RowCount == 0 {
		return 0.1
	}

	switch op {
	case "=":
		// Equality selectivity
		if colStats.DistinctCount == 0 {
			return 0.1
		}
		return 1.0 / float64(colStats.DistinctCount)

	case "<", "<=", ">", ">=":
		// Range selectivity using histogram
		if len(colStats.Histogram) == 0 {
			return 0.33 // Default for range
		}
		return sc.estimateRangeSelectivity(colStats, op, value)

	case "!=", "<>":
		// Not equal selectivity
		if colStats.DistinctCount == 0 {
			return 0.9
		}
		return 1.0 - (1.0 / float64(colStats.DistinctCount))

	default:
		return 0.1
	}
}

// estimateRangeSelectivity estimates selectivity for range conditions
func (sc *StatsCollector) estimateRangeSelectivity(colStats *ColumnStats, op string, value interface{}) float64 {
	if len(colStats.Histogram) == 0 {
		return 0.33
	}

	// Find which bucket the value falls into
	var matchingRows uint64
	totalRows := uint64(0)

	for _, bucket := range colStats.Histogram {
		totalRows += bucket.Count

		cmpLow := compareValues(value, bucket.LowerBound)
		cmpHigh := compareValues(value, bucket.UpperBound)

		switch op {
		case "<":
			if cmpLow < 0 {
				matchingRows += bucket.Count
			}
		case "<=":
			if cmpLow <= 0 {
				matchingRows += bucket.Count
			}
		case ">":
			if cmpHigh > 0 {
				matchingRows += bucket.Count
			}
		case ">=":
			if cmpHigh >= 0 {
				matchingRows += bucket.Count
			}
		}
	}

	if totalRows == 0 {
		return 0.33
	}

	return float64(matchingRows) / float64(totalRows)
}

// QueryResult represents a query result for stats collection
type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
}

// ExecuteQuery executes a query and returns results
func (c *Catalog) ExecuteQuery(sql string) (*QueryResult, error) {
	// Parse and execute query
	// This is a simplified version - in production would use proper parsing
	// For now, return empty result
	return &QueryResult{}, nil
}

// EstimateRowCount estimates the number of rows for a table
func (sc *StatsCollector) EstimateRowCount(tableName string) uint64 {
	stats, ok := sc.GetTableStats(tableName)
	if !ok {
		// Default estimate
		return 1000
	}
	return stats.RowCount
}

// GetStatsSummary returns a summary of all statistics
func (sc *StatsCollector) GetStatsSummary() map[string]uint64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	summary := make(map[string]uint64)
	for tableName, stats := range sc.stats {
		summary[tableName] = stats.RowCount
	}
	return summary
}

// InvalidateStats removes statistics for a table
func (sc *StatsCollector) InvalidateStats(tableName string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.stats, tableName)
}

// IsStale checks if statistics are stale (older than threshold)
func (sc *StatsCollector) IsStale(tableName string, threshold time.Duration) bool {
	stats, ok := sc.GetTableStats(tableName)
	if !ok {
		return true // No stats means stale
	}

	return time.Since(stats.LastAnalyzed) > threshold
}

// Cost constants for query optimization
const (
	SeqPageCost      = 1.0
	RandomPageCost   = 4.0
	CpuTupleCost     = 0.01
	CpuIndexTupleCost = 0.005
	CpuOperatorCost  = 0.0025
)

// EstimateCost estimates the cost of a sequential scan
func (sc *StatsCollector) EstimateSeqScanCost(tableName string, selectivity float64) float64 {
	stats, ok := sc.GetTableStats(tableName)
	if !ok {
		// Default estimate
		return 1000 * SeqPageCost
	}

	pages := float64(stats.PageCount)
	if pages == 0 {
		pages = float64(stats.RowCount) / 100 // Assume 100 rows per page
	}

	// Cost = page reads + CPU processing
	pageCost := pages * SeqPageCost
	cpuCost := float64(stats.RowCount) * CpuTupleCost * selectivity

	return pageCost + cpuCost
}

// EstimateIndexScanCost estimates the cost of an index scan
func (sc *StatsCollector) EstimateIndexScanCost(tableName, indexName string, selectivity float64) float64 {
	stats, ok := sc.GetTableStats(tableName)
	if !ok {
		return 100 * RandomPageCost
	}

	// Index scan cost: random page reads + index processing
	indexPages := math.Max(1, float64(stats.RowCount)*selectivity/100)
	pageCost := indexPages * RandomPageCost
	cpuCost := float64(stats.RowCount) * CpuIndexTupleCost * selectivity

	return pageCost + cpuCost
}

// EstimateJoinCost estimates the cost of a nested loop join
func (sc *StatsCollector) EstimateNestedLoopCost(outerRows, innerCost float64) float64 {
	return outerRows * innerCost
}

// EstimateHashJoinCost estimates the cost of a hash join
func (sc *StatsCollector) EstimateHashJoinCost(outerRows, innerRows float64) float64 {
	// Build hash table + probe
	buildCost := innerRows * CpuTupleCost
	probeCost := outerRows * CpuOperatorCost
	return buildCost + probeCost
}

// EstimateMergeJoinCost estimates the cost of a merge join
func (sc *StatsCollector) EstimateMergeJoinCost(outerRows, innerRows float64) float64 {
	// Sort cost (if needed) + merge cost
	sortCost := (outerRows + innerRows) * math.Log2(outerRows+innerRows) * CpuTupleCost
	mergeCost := (outerRows + innerRows) * CpuTupleCost
	return sortCost + mergeCost
}
