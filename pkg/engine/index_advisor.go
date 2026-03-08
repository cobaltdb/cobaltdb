package engine

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// IndexRecommendation represents a suggested index
type IndexRecommendation struct {
	ID               string
	TableName        string
	ColumnNames      []string
	IndexType        string // BTREE, HASH, etc.
	Priority         int    // 1-10, higher is more important
	Reason           string
	EstimatedBenefit float64 // Estimated query speedup factor

	// Statistics
	QueryCount      uint64
	AvgExecTimeMs   float64
	TotalExecTimeMs uint64
	CreatedAt       time.Time

	// Sample queries that would benefit
	SampleQueries []string
}

// IndexAdvisorConfig configures the index advisor
type IndexAdvisorConfig struct {
	Enabled            bool
	MinQueryCount      int           // Minimum queries before considering
	MinAvgExecTimeMs   float64       // Minimum avg execution time to consider
	MaxRecommendations int           // Maximum recommendations to keep
	AnalysisWindow     time.Duration // How long to track queries
	EnableMultiColumn  bool          // Suggest multi-column indexes
}

// DefaultIndexAdvisorConfig returns default configuration
func DefaultIndexAdvisorConfig() *IndexAdvisorConfig {
	return &IndexAdvisorConfig{
		Enabled:            true,
		MinQueryCount:      10,
		MinAvgExecTimeMs:   10.0,
		MaxRecommendations: 100,
		AnalysisWindow:     24 * time.Hour,
		EnableMultiColumn:  true,
	}
}

// columnUsage tracks how a column is used in queries
type columnUsage struct {
	columnName    string
	equalityCount uint64
	rangeCount    uint64
	orderByCount  uint64
	joinCount     uint64
	selectivity   float64 // Estimated selectivity (0-1)
}

// tableQueryStats tracks query statistics for a table
type tableQueryStats struct {
	tableName     string
	queryCount    uint64
	totalExecTime uint64 // milliseconds
	columnUsage   map[string]*columnUsage
	sampleQueries []string
	mu            sync.RWMutex
}

// IndexAdvisor analyzes query patterns and recommends indexes
type IndexAdvisor struct {
	config *IndexAdvisorConfig

	// Query pattern tracking
	tableStats map[string]*tableQueryStats
	statsMu    sync.RWMutex

	// Recommendations
	recommendations map[string]*IndexRecommendation
	recMu           sync.RWMutex

	// Control
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewIndexAdvisor creates a new index advisor
func NewIndexAdvisor(config *IndexAdvisorConfig) *IndexAdvisor {
	if config == nil {
		config = DefaultIndexAdvisorConfig()
	}

	ia := &IndexAdvisor{
		config:          config,
		tableStats:      make(map[string]*tableQueryStats),
		recommendations: make(map[string]*IndexRecommendation),
		stopCh:          make(chan struct{}),
	}

	// Start background analysis
	ia.wg.Add(1)
	go ia.analysisLoop()

	return ia
}

// RecordQuery records a query execution for analysis
func (ia *IndexAdvisor) RecordQuery(tableName string, columns []string, execTimeMs float64, sql string) {
	if !ia.config.Enabled {
		return
	}

	ia.statsMu.Lock()
	stats, exists := ia.tableStats[tableName]
	if !exists {
		stats = &tableQueryStats{
			tableName:   tableName,
			columnUsage: make(map[string]*columnUsage),
		}
		ia.tableStats[tableName] = stats
	}
	ia.statsMu.Unlock()

	stats.mu.Lock()
	defer stats.mu.Unlock()

	stats.queryCount++
	stats.totalExecTime += uint64(execTimeMs)

	// Record column usage
	for _, col := range columns {
		usage, exists := stats.columnUsage[col]
		if !exists {
			usage = &columnUsage{columnName: col}
			stats.columnUsage[col] = usage
		}
		usage.equalityCount++
	}

	// Store sample queries (limit to 5)
	if len(stats.sampleQueries) < 5 {
		// Simple deduplication
		sqlShort := sql
		if len(sqlShort) > 100 {
			sqlShort = sqlShort[:100] + "..."
		}
		for _, existing := range stats.sampleQueries {
			if existing == sqlShort {
				return
			}
		}
		stats.sampleQueries = append(stats.sampleQueries, sqlShort)
	}
}

// RecordWhereCondition records WHERE clause conditions for index analysis
func (ia *IndexAdvisor) RecordWhereCondition(tableName string, columnName string, conditionType string) {
	if !ia.config.Enabled {
		return
	}

	ia.statsMu.Lock()
	stats, exists := ia.tableStats[tableName]
	if !exists {
		stats = &tableQueryStats{
			tableName:   tableName,
			columnUsage: make(map[string]*columnUsage),
		}
		ia.tableStats[tableName] = stats
	}
	ia.statsMu.Unlock()

	stats.mu.Lock()
	defer stats.mu.Unlock()

	usage, exists := stats.columnUsage[columnName]
	if !exists {
		usage = &columnUsage{columnName: columnName}
		stats.columnUsage[columnName] = usage
	}

	switch conditionType {
	case "equality":
		usage.equalityCount++
	case "range":
		usage.rangeCount++
	case "orderby":
		usage.orderByCount++
	case "join":
		usage.joinCount++
	}
}

// GenerateRecommendations analyzes collected statistics and generates index recommendations
func (ia *IndexAdvisor) GenerateRecommendations() []*IndexRecommendation {
	if !ia.config.Enabled {
		return nil
	}

	ia.statsMu.RLock()
	tables := make([]*tableQueryStats, 0, len(ia.tableStats))
	for _, stats := range ia.tableStats {
		tables = append(tables, stats)
	}
	ia.statsMu.RUnlock()

	var newRecommendations []*IndexRecommendation

	for _, stats := range tables {
		stats.mu.RLock()

		// Skip tables with too few queries
		if stats.queryCount < uint64(ia.config.MinQueryCount) {
			stats.mu.RUnlock()
			continue
		}

		avgExecTime := float64(stats.totalExecTime) / float64(stats.queryCount)
		if avgExecTime < ia.config.MinAvgExecTimeMs {
			stats.mu.RUnlock()
			continue
		}

		// Analyze columns for index potential
		columns := make([]*columnUsage, 0, len(stats.columnUsage))
		for _, usage := range stats.columnUsage {
			columns = append(columns, usage)
		}

		// Sort by importance (equality > range > orderby > join)
		sort.Slice(columns, func(i, j int) bool {
			scoreI := columns[i].equalityCount*4 + columns[i].rangeCount*3 +
				columns[i].orderByCount*2 + columns[i].joinCount
			scoreJ := columns[j].equalityCount*4 + columns[j].rangeCount*3 +
				columns[j].orderByCount*2 + columns[j].joinCount
			return scoreI > scoreJ
		})

		// Generate single-column index recommendations
		for i, col := range columns {
			if i >= 3 { // Limit to top 3 columns per table
				break
			}

			rec := ia.createRecommendation(stats, []string{col.columnName}, col, avgExecTime)
			if rec != nil {
				newRecommendations = append(newRecommendations, rec)
			}
		}

		// Generate multi-column index recommendations if enabled
		if ia.config.EnableMultiColumn && len(columns) >= 2 {
			for i := 0; i < len(columns) && i < 2; i++ {
				for j := i + 1; j < len(columns) && j < 3; j++ {
					rec := ia.createMultiColumnRecommendation(stats, []string{columns[i].columnName, columns[j].columnName}, avgExecTime)
					if rec != nil {
						newRecommendations = append(newRecommendations, rec)
					}
				}
			}
		}

		stats.mu.RUnlock()
	}

	// Update recommendations
	ia.recMu.Lock()
	defer ia.recMu.Unlock()

	// Merge with existing recommendations
	for _, rec := range newRecommendations {
		if existing, exists := ia.recommendations[rec.ID]; exists {
			// Update existing
			existing.QueryCount = rec.QueryCount
			existing.AvgExecTimeMs = rec.AvgExecTimeMs
			existing.TotalExecTimeMs = rec.TotalExecTimeMs
		} else {
			ia.recommendations[rec.ID] = rec
		}
	}

	// Prune old recommendations if over limit
	ia.pruneRecommendations()

	// Return sorted recommendations
	result := make([]*IndexRecommendation, 0, len(ia.recommendations))
	for _, rec := range ia.recommendations {
		result = append(result, rec)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Priority > result[j].Priority
	})

	return result
}

func (ia *IndexAdvisor) createRecommendation(stats *tableQueryStats, columns []string, usage *columnUsage, avgExecTime float64) *IndexRecommendation {
	// Calculate priority based on usage patterns
	priority := 1
	if usage.equalityCount > 100 {
		priority += 3
	} else if usage.equalityCount > 10 {
		priority += 2
	} else if usage.equalityCount > 0 {
		priority += 1
	}

	if usage.rangeCount > 50 {
		priority += 2
	} else if usage.rangeCount > 0 {
		priority += 1
	}

	if avgExecTime > 100 {
		priority += 3
	} else if avgExecTime > 50 {
		priority += 2
	} else if avgExecTime > 10 {
		priority += 1
	}

	if priority > 10 {
		priority = 10
	}

	// Generate ID
	id := fmt.Sprintf("%s_%s", stats.tableName, strings.Join(columns, "_"))

	// Build reason
	var reasons []string
	if usage.equalityCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d equality lookups", usage.equalityCount))
	}
	if usage.rangeCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d range scans", usage.rangeCount))
	}
	if usage.orderByCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d ORDER BY operations", usage.orderByCount))
	}

	return &IndexRecommendation{
		ID:               id,
		TableName:        stats.tableName,
		ColumnNames:      columns,
		IndexType:        "BTREE",
		Priority:         priority,
		Reason:           strings.Join(reasons, ", "),
		EstimatedBenefit: avgExecTime / 10, // Rough estimate: 10x speedup
		QueryCount:       stats.queryCount,
		AvgExecTimeMs:    avgExecTime,
		TotalExecTimeMs:  stats.totalExecTime,
		CreatedAt:        time.Now(),
		SampleQueries:    stats.sampleQueries,
	}
}

func (ia *IndexAdvisor) createMultiColumnRecommendation(stats *tableQueryStats, columns []string, avgExecTime float64) *IndexRecommendation {
	id := fmt.Sprintf("%s_%s_multi", stats.tableName, strings.Join(columns, "_"))

	return &IndexRecommendation{
		ID:               id,
		TableName:        stats.tableName,
		ColumnNames:      columns,
		IndexType:        "BTREE",
		Priority:         5, // Medium priority for multi-column
		Reason:           "Multi-column index for composite WHERE conditions",
		EstimatedBenefit: avgExecTime / 5,
		QueryCount:       stats.queryCount,
		AvgExecTimeMs:    avgExecTime,
		TotalExecTimeMs:  stats.totalExecTime,
		CreatedAt:        time.Now(),
		SampleQueries:    stats.sampleQueries,
	}
}

func (ia *IndexAdvisor) pruneRecommendations() {
	if len(ia.recommendations) <= ia.config.MaxRecommendations {
		return
	}

	// Convert to slice for sorting
	recs := make([]*IndexRecommendation, 0, len(ia.recommendations))
	for _, rec := range ia.recommendations {
		recs = append(recs, rec)
	}

	// Sort by priority descending, then by query count
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].Priority != recs[j].Priority {
			return recs[i].Priority > recs[j].Priority
		}
		return recs[i].QueryCount > recs[j].QueryCount
	})

	// Keep only top N
	newRecs := make(map[string]*IndexRecommendation)
	for i, rec := range recs {
		if i >= ia.config.MaxRecommendations {
			break
		}
		newRecs[rec.ID] = rec
	}

	ia.recommendations = newRecs
}

// GetRecommendations returns current index recommendations
func (ia *IndexAdvisor) GetRecommendations() []*IndexRecommendation {
	ia.recMu.RLock()
	defer ia.recMu.RUnlock()

	result := make([]*IndexRecommendation, 0, len(ia.recommendations))
	for _, rec := range ia.recommendations {
		result = append(result, rec)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Priority > result[j].Priority
	})

	return result
}

// GetRecommendationsForTable returns recommendations for a specific table
func (ia *IndexAdvisor) GetRecommendationsForTable(tableName string) []*IndexRecommendation {
	ia.recMu.RLock()
	defer ia.recMu.RUnlock()

	var result []*IndexRecommendation
	for _, rec := range ia.recommendations {
		if rec.TableName == tableName {
			result = append(result, rec)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Priority > result[j].Priority
	})

	return result
}

// ApplyRecommendation marks a recommendation as applied (removes it)
func (ia *IndexAdvisor) ApplyRecommendation(id string) error {
	ia.recMu.Lock()
	defer ia.recMu.Unlock()

	if _, exists := ia.recommendations[id]; !exists {
		return fmt.Errorf("recommendation %s not found", id)
	}

	delete(ia.recommendations, id)
	return nil
}

// IgnoreRecommendation removes a recommendation without applying it
func (ia *IndexAdvisor) IgnoreRecommendation(id string) error {
	return ia.ApplyRecommendation(id)
}

// Reset clears all statistics and recommendations
func (ia *IndexAdvisor) Reset() {
	ia.statsMu.Lock()
	ia.tableStats = make(map[string]*tableQueryStats)
	ia.statsMu.Unlock()

	ia.recMu.Lock()
	ia.recommendations = make(map[string]*IndexRecommendation)
	ia.recMu.Unlock()
}

// Stats returns advisor statistics
func (ia *IndexAdvisor) Stats() IndexAdvisorStats {
	ia.statsMu.RLock()
	tableCount := len(ia.tableStats)
	ia.statsMu.RUnlock()

	ia.recMu.RLock()
	recCount := len(ia.recommendations)
	ia.recMu.RUnlock()

	return IndexAdvisorStats{
		TablesTracked:   tableCount,
		Recommendations: recCount,
		Enabled:         ia.config.Enabled,
	}
}

// IndexAdvisorStats holds advisor statistics
type IndexAdvisorStats struct {
	TablesTracked   int  `json:"tables_tracked"`
	Recommendations int  `json:"recommendations"`
	Enabled         bool `json:"enabled"`
}

func (ia *IndexAdvisor) analysisLoop() {
	defer ia.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ia.stopCh:
			return
		case <-ticker.C:
			ia.GenerateRecommendations()
		}
	}
}

// Close shuts down the index advisor
func (ia *IndexAdvisor) Close() error {
	ia.stopOnce.Do(func() {
		close(ia.stopCh)
	})

	ia.wg.Wait()
	return nil
}

// GenerateIndexSQL generates the CREATE INDEX SQL for a recommendation
func (rec *IndexRecommendation) GenerateIndexSQL() string {
	indexName := fmt.Sprintf("idx_%s_%s", rec.TableName, strings.Join(rec.ColumnNames, "_"))
	columns := strings.Join(rec.ColumnNames, ", ")
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s);", indexName, rec.TableName, columns)
}
