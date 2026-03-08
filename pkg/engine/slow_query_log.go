package engine

import (
	"container/ring"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// SlowQueryConfig configures slow query logging
type SlowQueryConfig struct {
	Enabled      bool          // Enable slow query logging
	Threshold    time.Duration // Threshold for slow queries (default: 1s)
	MaxEntries   int           // Max entries in memory buffer (default: 1000)
	LogToStdout  bool          // Also log to stdout
	IncludePlan  bool          // Include query plan info
	IncludeStack bool          // Include stack trace
	SampleRate   float64       // Sample rate for queries (1.0 = all, 0.1 = 10%)
}

// DefaultSlowQueryConfig returns default configuration
func DefaultSlowQueryConfig() *SlowQueryConfig {
	return &SlowQueryConfig{
		Enabled:     true,
		Threshold:   1 * time.Second,
		MaxEntries:  1000,
		LogToStdout: false,
		IncludePlan: true,
		SampleRate:  1.0,
	}
}

// SlowQueryEntry represents a logged slow query
type SlowQueryEntry struct {
	ID           string
	SQL          string
	Args         []interface{}
	Duration     time.Duration
	RowsAffected int64
	RowsReturned int
	Timestamp    time.Time

	// Profiling info
	ParseTime   time.Duration
	PlanTime    time.Duration
	ExecuteTime time.Duration

	// Metadata
	TableNames []string
	IndexNames []string
	PlanType   string

	// Context
	TransactionID uint64
	ConnectionID  int64
}

// QueryProfile contains detailed profiling information for a query
type QueryProfile struct {
	SQL          string
	StartTime    time.Time
	Phases       map[string]time.Duration
	CurrentPhase string
	phaseStart   time.Time
}

// SlowQueryLog manages slow query logging
type SlowQueryLog struct {
	config *SlowQueryConfig

	// Ring buffer for recent queries
	entries   *ring.Ring
	entriesMu sync.RWMutex

	// Statistics
	totalLogged  atomic.Uint64
	totalSkipped atomic.Uint64

	// Callback for external handling
	onSlowQuery func(*SlowQueryEntry)
}

// NewSlowQueryLog creates a new slow query log
func NewSlowQueryLog(config *SlowQueryConfig) *SlowQueryLog {
	if config == nil {
		config = DefaultSlowQueryConfig()
	}

	return &SlowQueryLog{
		config:  config,
		entries: ring.New(config.MaxEntries),
	}
}

// SetCallback sets a callback function for slow queries
func (sql *SlowQueryLog) SetCallback(cb func(*SlowQueryEntry)) {
	sql.onSlowQuery = cb
}

// ShouldSample returns true if this query should be sampled
func (sql *SlowQueryLog) ShouldSample() bool {
	if sql.config.SampleRate >= 1.0 {
		return true
	}
	if sql.config.SampleRate <= 0.0 {
		return false
	}
	// Simple sampling - not cryptographically secure but good enough
	return time.Now().UnixNano()%1000 < int64(sql.config.SampleRate*1000)
}

// Log logs a query if it exceeds the threshold
func (sql *SlowQueryLog) Log(sqlStr string, args []interface{}, duration time.Duration, rowsAffected int64, rowsReturned int) {
	if !sql.config.Enabled {
		return
	}

	if duration < sql.config.Threshold {
		return
	}

	if !sql.ShouldSample() {
		sql.totalSkipped.Add(1)
		return
	}

	entry := &SlowQueryEntry{
		ID:           fmt.Sprintf("sq_%d", time.Now().UnixNano()),
		SQL:          sqlStr,
		Args:         args,
		Duration:     duration,
		RowsAffected: rowsAffected,
		RowsReturned: rowsReturned,
		Timestamp:    time.Now(),
	}

	sql.entriesMu.Lock()
	sql.entries.Value = entry
	sql.entries = sql.entries.Next()
	sql.entriesMu.Unlock()

	sql.totalLogged.Add(1)

	// Call external handler if set
	if sql.onSlowQuery != nil {
		sql.onSlowQuery(entry)
	}

	// Log to stdout if enabled
	if sql.config.LogToStdout {
		fmt.Printf("[SLOW QUERY] %s | %v | %s\n", entry.Timestamp.Format(time.RFC3339), duration, sqlStr)
	}
}

// LogWithProfile logs a query with detailed profiling information
func (sql *SlowQueryLog) LogWithProfile(profile *QueryProfile, rowsAffected int64, rowsReturned int) {
	if !sql.config.Enabled {
		return
	}

	duration := time.Since(profile.StartTime)
	if duration < sql.config.Threshold {
		return
	}

	if !sql.ShouldSample() {
		sql.totalSkipped.Add(1)
		return
	}

	entry := &SlowQueryEntry{
		ID:           fmt.Sprintf("sq_%d", time.Now().UnixNano()),
		SQL:          profile.SQL,
		Duration:     duration,
		RowsAffected: rowsAffected,
		RowsReturned: rowsReturned,
		Timestamp:    time.Now(),
		ParseTime:    profile.Phases["parse"],
		PlanTime:     profile.Phases["plan"],
		ExecuteTime:  profile.Phases["execute"],
	}

	sql.entriesMu.Lock()
	sql.entries.Value = entry
	sql.entries = sql.entries.Next()
	sql.entriesMu.Unlock()

	sql.totalLogged.Add(1)

	if sql.onSlowQuery != nil {
		sql.onSlowQuery(entry)
	}

	if sql.config.LogToStdout {
		fmt.Printf("[SLOW QUERY] %s | Total: %v | Parse: %v | Plan: %v | Exec: %v | %s\n",
			entry.Timestamp.Format(time.RFC3339), duration, entry.ParseTime, entry.PlanTime, entry.ExecuteTime, profile.SQL)
	}
}

// GetEntries returns recent slow query entries
func (sql *SlowQueryLog) GetEntries(limit int) []*SlowQueryEntry {
	sql.entriesMu.RLock()
	defer sql.entriesMu.RUnlock()

	var entries []*SlowQueryEntry

	// Traverse ring buffer
	sql.entries.Do(func(v interface{}) {
		if v == nil {
			return
		}
		if entry, ok := v.(*SlowQueryEntry); ok {
			entries = append(entries, entry)
		}
	})

	// Return most recent first
	if len(entries) > limit {
		entries = entries[len(entries)-limit:]
	}

	// Reverse to get newest first
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}

	return entries
}

// GetTopSlowQueries returns the slowest queries
func (sql *SlowQueryLog) GetTopSlowQueries(n int) []*SlowQueryEntry {
	entries := sql.GetEntries(sql.config.MaxEntries)

	// Sort by duration (bubble sort for small n)
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[j].Duration > entries[i].Duration {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	if n > len(entries) {
		n = len(entries)
	}

	return entries[:n]
}

// GetStats returns slow query statistics
func (sql *SlowQueryLog) GetStats() SlowQueryStats {
	sql.entriesMu.RLock()
	count := 0
	sql.entries.Do(func(v interface{}) {
		if v != nil {
			count++
		}
	})
	sql.entriesMu.RUnlock()

	return SlowQueryStats{
		TotalLogged:  sql.totalLogged.Load(),
		TotalSkipped: sql.totalSkipped.Load(),
		BufferSize:   count,
		MaxBuffer:    sql.config.MaxEntries,
	}
}

// Clear clears all logged entries
func (sql *SlowQueryLog) Clear() {
	sql.entriesMu.Lock()
	defer sql.entriesMu.Unlock()

	sql.entries = ring.New(sql.config.MaxEntries)
	sql.totalLogged.Store(0)
	sql.totalSkipped.Store(0)
}

// SlowQueryStats holds slow query statistics
type SlowQueryStats struct {
	TotalLogged  uint64 `json:"total_logged"`
	TotalSkipped uint64 `json:"total_skipped"`
	BufferSize   int    `json:"buffer_size"`
	MaxBuffer    int    `json:"max_buffer"`
}

// NewQueryProfile creates a new query profile
func NewQueryProfile(sql string) *QueryProfile {
	return &QueryProfile{
		SQL:       sql,
		StartTime: time.Now(),
		Phases:    make(map[string]time.Duration),
	}
}

// StartPhase starts timing a phase
func (p *QueryProfile) StartPhase(phase string) {
	p.CurrentPhase = phase
	p.phaseStart = time.Now()
}

// EndPhase ends the current phase timing
func (p *QueryProfile) EndPhase() {
	if p.CurrentPhase != "" && !p.phaseStart.IsZero() {
		p.Phases[p.CurrentPhase] = time.Since(p.phaseStart)
		p.CurrentPhase = ""
	}
}

// GetPhase returns the duration for a phase
func (p *QueryProfile) GetPhase(phase string) time.Duration {
	return p.Phases[phase]
}

// TotalTime returns total elapsed time
func (p *QueryProfile) TotalTime() time.Duration {
	return time.Since(p.StartTime)
}

// SlowQueryAnalyzer provides analysis of slow queries
type SlowQueryAnalyzer struct {
	log *SlowQueryLog
}

// NewSlowQueryAnalyzer creates a new analyzer
func NewSlowQueryAnalyzer(log *SlowQueryLog) *SlowQueryAnalyzer {
	return &SlowQueryAnalyzer{log: log}
}

// AnalyzePatterns analyzes slow query patterns
func (a *SlowQueryAnalyzer) AnalyzePatterns() *QueryPatternReport {
	entries := a.log.GetEntries(a.log.config.MaxEntries)

	report := &QueryPatternReport{
		TotalQueries:  len(entries),
		PatternCounts: make(map[string]int),
		TableCounts:   make(map[string]int),
	}

	if len(entries) == 0 {
		return report
	}

	var totalDuration time.Duration
	var maxDuration time.Duration

	for _, entry := range entries {
		totalDuration += entry.Duration
		if entry.Duration > maxDuration {
			maxDuration = entry.Duration
		}

		// Extract pattern (simplified)
		pattern := extractQueryPattern(entry.SQL)
		report.PatternCounts[pattern]++

		// Count table references
		for _, table := range entry.TableNames {
			report.TableCounts[table]++
		}
	}

	report.AvgDuration = totalDuration / time.Duration(len(entries))
	report.MaxDuration = maxDuration

	return report
}

// QueryPatternReport contains analysis results
type QueryPatternReport struct {
	TotalQueries  int
	AvgDuration   time.Duration
	MaxDuration   time.Duration
	PatternCounts map[string]int
	TableCounts   map[string]int
}

// extractQueryPattern extracts a simplified pattern from SQL
func extractQueryPattern(sql string) string {
	// Very simplified pattern extraction
	// In production, this would use the parser
	if len(sql) > 50 {
		return sql[:50] + "..."
	}
	return sql
}
