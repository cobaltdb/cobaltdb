package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/catalog"
	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// AutoVacuumConfig configures the auto-vacuum system
type AutoVacuumConfig struct {
	Enabled                  bool          // Enable auto-vacuum
	VacuumThreshold          int           // Min dead tuples before vacuum (default: 50)
	VacuumScaleFactor        float64       // Fraction of table size (default: 0.2)
	AnalyzeThreshold         int           // Min rows changed before analyze (default: 50)
	AnalyzeScaleFactor       float64       // Fraction of table size for analyze (default: 0.1)
	AutoVacuumInterval       time.Duration // How often to check (default: 1 minute)
	VacuumCostLimit          int           // Cost limit per vacuum (default: 200)
	VacuumCostDelay          time.Duration // Delay between vacuum pages (default: 0)
	MaxWorkers               int           // Max parallel vacuum workers (default: 3)
	VacuumFreezeMinAge       int           // Min age before freezing (default: 50000000)
	VacuumFreezeTableAge     int           // Age to trigger aggressive freeze (default: 150000000)
	LogAutoVacuumMinDuration time.Duration // Log vacuums taking longer than this
	TrackCounts              bool          // Track tuple changes
}

// DefaultAutoVacuumConfig returns default configuration
func DefaultAutoVacuumConfig() *AutoVacuumConfig {
	return &AutoVacuumConfig{
		Enabled:                  true,
		VacuumThreshold:          50,
		VacuumScaleFactor:        0.2,
		AnalyzeThreshold:         50,
		AnalyzeScaleFactor:       0.1,
		AutoVacuumInterval:       1 * time.Minute,
		VacuumCostLimit:          200,
		VacuumCostDelay:          0,
		MaxWorkers:               3,
		VacuumFreezeMinAge:       50000000,
		VacuumFreezeTableAge:     150000000,
		LogAutoVacuumMinDuration: 250 * time.Millisecond,
		TrackCounts:              true,
	}
}

// TableStats tracks statistics for a table
type TableStats struct {
	TableName                 string
	TotalRows                 int64
	DeadTuples                int64
	LastVacuum                time.Time
	LastAnalyze               time.Time
	VacuumCount               uint64
	AnalyzeCount              uint64
	ModificationsSinceVacuum  int64
	ModificationsSinceAnalyze int64
	LiveTupleCount            int64
	DeadTupleCount            int64
	LastVacuumDuration        time.Duration
	LastAnalyzeDuration       time.Duration
	FrozenXID                 int64 // Oldest frozen transaction ID
	RelPages                  int64 // Estimated page count
	RelTuples                 int64 // Estimated tuple count
	AllVisiblePages           int64 // Pages with all tuples visible
	mu                        sync.RWMutex
}

// NeedsVacuum checks if table needs vacuuming
func (ts *TableStats) NeedsVacuum(config *AutoVacuumConfig) bool {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	threshold := int64(config.VacuumThreshold + int(config.VacuumScaleFactor*float64(ts.RelTuples)))
	return ts.DeadTuples > threshold
}

// NeedsAnalyze checks if table needs analyzing
func (ts *TableStats) NeedsAnalyze(config *AutoVacuumConfig) bool {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	threshold := int64(config.AnalyzeThreshold + int(config.AnalyzeScaleFactor*float64(ts.RelTuples)))
	return ts.ModificationsSinceAnalyze > threshold
}

// NeedsFreeze checks if table needs freezing
func (ts *TableStats) NeedsFreeze(config *AutoVacuumConfig, currentXID int64) bool {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	age := currentXID - ts.FrozenXID
	return age > int64(config.VacuumFreezeTableAge)
}

// UpdateDeadTuples updates dead tuple count
func (ts *TableStats) UpdateDeadTuples(count int64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.DeadTuples = count
}

// UpdateModifications updates modification count
func (ts *TableStats) UpdateModifications(count int64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.ModificationsSinceVacuum += count
	ts.ModificationsSinceAnalyze += count
}

// RecordVacuum records vacuum completion
func (ts *TableStats) RecordVacuum(duration time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.LastVacuum = time.Now()
	ts.LastVacuumDuration = duration
	ts.VacuumCount++
	ts.ModificationsSinceVacuum = 0
	ts.DeadTuples = 0
}

// RecordAnalyze records analyze completion
func (ts *TableStats) RecordAnalyze(duration time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.LastAnalyze = time.Now()
	ts.LastAnalyzeDuration = duration
	ts.AnalyzeCount++
	ts.ModificationsSinceAnalyze = 0
}

// AutoVacuum manages automatic vacuum operations
type AutoVacuum struct {
	config     *AutoVacuumConfig
	catalog    *catalog.Catalog
	stats      map[string]*TableStats
	workers    *vacuumWorkerPool
	mu         sync.RWMutex
	running    atomic.Bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	xidCounter atomic.Int64 // Transaction ID counter
}

// NewAutoVacuum creates a new auto-vacuum manager
func NewAutoVacuum(config *AutoVacuumConfig, cat *catalog.Catalog) *AutoVacuum {
	if config == nil {
		config = DefaultAutoVacuumConfig()
	}

	av := &AutoVacuum{
		config:  config,
		catalog: cat,
		stats:   make(map[string]*TableStats),
		stopCh:  make(chan struct{}),
	}

	av.workers = newVacuumWorkerPool(config.MaxWorkers)

	return av
}

// Start starts the auto-vacuum daemon
func (av *AutoVacuum) Start() {
	if !av.config.Enabled {
		return
	}

	if av.running.CompareAndSwap(false, true) {
		av.wg.Add(1)
		go av.mainLoop()
		logger.Default().Info("Auto-vacuum started")
	}
}

// Stop stops the auto-vacuum daemon
func (av *AutoVacuum) Stop() {
	if av.running.CompareAndSwap(true, false) {
		close(av.stopCh)
		av.wg.Wait()
		av.workers.stop()
		logger.Default().Info("Auto-vacuum stopped")
	}
}

// mainLoop runs the main vacuum checking loop
func (av *AutoVacuum) mainLoop() {
	defer av.wg.Done()

	ticker := time.NewTicker(av.config.AutoVacuumInterval)
	defer ticker.Stop()

	// Do initial scan
	av.checkTables()

	for {
		select {
		case <-av.stopCh:
			return
		case <-ticker.C:
			av.checkTables()
		}
	}
}

// checkTables checks all tables for vacuum/analyze needs
func (av *AutoVacuum) checkTables() {
	tables := av.getTableList()

	for _, tableName := range tables {
		select {
		case <-av.stopCh:
			return
		default:
		}

		stats := av.getTableStats(tableName)

		// Check if vacuum is needed
		if stats.NeedsVacuum(av.config) {
			av.workers.submit(vacuumJob{
				tableName: tableName,
				jobType:   "vacuum",
				stats:     stats,
			})
		} else if stats.NeedsAnalyze(av.config) {
			// Only analyze if we're not already vacuuming
			av.workers.submit(vacuumJob{
				tableName: tableName,
				jobType:   "analyze",
				stats:     stats,
			})
		}
	}
}

// getTableList returns list of tables to check
func (av *AutoVacuum) getTableList() []string {
	// In a real implementation, this would query the catalog
	// For now, return tracked tables
	av.mu.RLock()
	defer av.mu.RUnlock()

	tables := make([]string, 0, len(av.stats))
	for name := range av.stats {
		tables = append(tables, name)
	}
	return tables
}

// getTableStats gets or creates table statistics
func (av *AutoVacuum) getTableStats(tableName string) *TableStats {
	av.mu.Lock()
	defer av.mu.Unlock()

	if stats, exists := av.stats[tableName]; exists {
		return stats
	}

	stats := &TableStats{
		TableName: tableName,
	}
	av.stats[tableName] = stats
	return stats
}

// RegisterTable registers a table for auto-vacuum tracking
func (av *AutoVacuum) RegisterTable(tableName string, estimatedRows int64) {
	stats := av.getTableStats(tableName)
	stats.mu.Lock()
	stats.RelTuples = estimatedRows
	stats.mu.Unlock()
}

// UnregisterTable removes a table from tracking
func (av *AutoVacuum) UnregisterTable(tableName string) {
	av.mu.Lock()
	defer av.mu.Unlock()
	delete(av.stats, tableName)
}

// TrackModification records a table modification
func (av *AutoVacuum) TrackModification(tableName string, deleted int, updated int, inserted int) {
	if !av.config.TrackCounts {
		return
	}

	stats := av.getTableStats(tableName)
	totalChanges := int64(deleted + updated + inserted)
	stats.UpdateModifications(totalChanges)
	stats.UpdateDeadTuples(int64(deleted + updated))
}

// VacuumTable performs manual vacuum on a table
func (av *AutoVacuum) VacuumTable(ctx context.Context, tableName string) error {
	stats := av.getTableStats(tableName)

	start := time.Now()

	// Perform vacuum
	if err := av.performVacuum(ctx, tableName); err != nil {
		return fmt.Errorf("vacuum failed for %s: %w", tableName, err)
	}

	duration := time.Since(start)
	stats.RecordVacuum(duration)

	if duration > av.config.LogAutoVacuumMinDuration {
		logger.Default().Infof("Vacuumed %s in %v", tableName, duration)
	}

	return nil
}

// AnalyzeTable performs manual analyze on a table
func (av *AutoVacuum) AnalyzeTable(ctx context.Context, tableName string) error {
	stats := av.getTableStats(tableName)

	start := time.Now()

	// Perform analyze
	if err := av.performAnalyze(ctx, tableName); err != nil {
		return fmt.Errorf("analyze failed for %s: %w", tableName, err)
	}

	duration := time.Since(start)
	stats.RecordAnalyze(duration)

	if duration > av.config.LogAutoVacuumMinDuration {
		logger.Default().Infof("Analyzed %s in %v", tableName, duration)
	}

	return nil
}

// performVacuum performs the actual vacuum operation
func (av *AutoVacuum) performVacuum(ctx context.Context, tableName string) error {
	// In a real implementation, this would:
	// 1. Scan the table for dead tuples
	// 2. Remove dead tuples and compact pages
	// 3. Update visibility maps
	// 4. Freeze old tuples if needed

	// Simulate vacuum work
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Millisecond):
		// Vacuum completed
	}

	return nil
}

// performAnalyze performs the actual analyze operation
func (av *AutoVacuum) performAnalyze(ctx context.Context, tableName string) error {
	// In a real implementation, this would:
	// 1. Sample rows from the table
	// 2. Calculate column statistics (distinct values, min/max, etc.)
	// 3. Update pg_stats catalog

	// Simulate analyze work
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Millisecond):
		// Analyze completed
	}

	return nil
}

// GetStats returns statistics for all tracked tables
func (av *AutoVacuum) GetStats() map[string]*TableStats {
	av.mu.RLock()
	defer av.mu.RUnlock()

	result := make(map[string]*TableStats, len(av.stats))
	for name, stats := range av.stats {
		result[name] = stats
	}
	return result
}

// vacuumJob represents a vacuum or analyze job
type vacuumJob struct {
	tableName string
	jobType   string // "vacuum" or "analyze"
	stats     *TableStats
}

// vacuumWorkerPool manages vacuum workers
type vacuumWorkerPool struct {
	maxWorkers int
	jobs       chan vacuumJob
	wg         sync.WaitGroup
	stopCh     chan struct{}
	av         *AutoVacuum // Reference back to AutoVacuum for executing jobs
}

// newVacuumWorkerPool creates a new worker pool
func newVacuumWorkerPool(maxWorkers int) *vacuumWorkerPool {
	return &vacuumWorkerPool{
		maxWorkers: maxWorkers,
		jobs:       make(chan vacuumJob, maxWorkers*2),
		stopCh:     make(chan struct{}),
	}
}

// start starts the worker pool with a reference to AutoVacuum
func (vwp *vacuumWorkerPool) start(av *AutoVacuum) {
	vwp.av = av
	for i := 0; i < vwp.maxWorkers; i++ {
		vwp.wg.Add(1)
		go vwp.worker()
	}
}

// stop stops the worker pool
func (vwp *vacuumWorkerPool) stop() {
	close(vwp.stopCh)
	vwp.wg.Wait()
}

// submit submits a job to the pool
func (vwp *vacuumWorkerPool) submit(job vacuumJob) {
	select {
	case vwp.jobs <- job:
	case <-vwp.stopCh:
	}
}

// worker processes vacuum jobs
func (vwp *vacuumWorkerPool) worker() {
	defer vwp.wg.Done()

	for {
		select {
		case <-vwp.stopCh:
			return
		case job := <-vwp.jobs:
			vwp.processJob(job)
		}
	}
}

// processJob processes a single vacuum/analyze job
func (vwp *vacuumWorkerPool) processJob(job vacuumJob) {
	if vwp.av == nil {
		return
	}

	ctx := context.Background()

	switch job.jobType {
	case "vacuum":
		if err := vwp.av.VacuumTable(ctx, job.tableName); err != nil {
			logger.Default().Errorf("Auto-vacuum error for %s: %v", job.tableName, err)
		}
	case "analyze":
		if err := vwp.av.AnalyzeTable(ctx, job.tableName); err != nil {
			logger.Default().Errorf("Auto-analyze error for %s: %v", job.tableName, err)
		}
	}
}

// VacuumProgress tracks vacuum progress
type VacuumProgress struct {
	TableName        string
	Phase            string // "scanning", "vacuuming", "cleaning indexes"
	HeapBlksTotal    int64
	HeapBlksScanned  int64
	IndexVacuumCount int
	MaxDeadTuples    int64
	NumDeadTuples    int64
	StartTime        time.Time
}

// AutoVacuumMetrics contains auto-vacuum metrics
type AutoVacuumMetrics struct {
	VacuumCount    uint64
	AnalyzeCount   uint64
	VacuumErrors   uint64
	AnalyzeErrors  uint64
	CurrentWorkers int
	PendingJobs    int
}

// GetMetrics returns current auto-vacuum metrics
func (av *AutoVacuum) GetMetrics() AutoVacuumMetrics {
	return AutoVacuumMetrics{
		CurrentWorkers: av.config.MaxWorkers,
	}
}

// VacuumReport contains vacuum operation report
type VacuumReport struct {
	TableName         string
	Duration          time.Duration
	PagesScanned      int64
	PagesVacuumed     int64
	TuplesDeleted     int64
	TuplesFrozen      int64
	IndexPagesCleaned int64
}
