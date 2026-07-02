package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/advisor"
	"github.com/cobaltdb/cobaltdb/pkg/backup"
	"github.com/cobaltdb/cobaltdb/pkg/cache"
	"github.com/cobaltdb/cobaltdb/pkg/catalog"
	"github.com/cobaltdb/cobaltdb/pkg/metrics"
	"github.com/cobaltdb/cobaltdb/pkg/optimizer"
	"github.com/cobaltdb/cobaltdb/pkg/replication"
)

func (db *DB) GetMetricsCollector() *metrics.Collector {
	return db.metrics
}

// GetCatalog returns the underlying catalog instance.

func (db *DB) GetCatalog() *catalog.Catalog {
	return db.catalog
}

// DBStats holds database statistics
type DBStats struct {
	Path              string        `json:"path"`
	InMemory          bool          `json:"in_memory"`
	PageSize          int           `json:"page_size"`
	CacheSize         int           `json:"cache_size"`
	ActiveConnections int64         `json:"active_connections"`
	MaxConnections    int           `json:"max_connections"`
	Tables            int           `json:"tables"`
	Indexes           int           `json:"indexes"`
	DatabaseSize      int64         `json:"database_size_bytes"`
	Uptime            time.Duration `json:"uptime"`
	IsHealthy         bool          `json:"is_healthy"`
	LastCheckTime     time.Time     `json:"last_check_time"`
}

// Stats returns detailed database statistics

func (db *DB) Stats() (*DBStats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed.Load() {
		return nil, ErrDatabaseClosed
	}

	stats := &DBStats{
		Path:              db.path,
		InMemory:          db.options.CoreStorage.InMemory,
		PageSize:          db.options.CoreStorage.PageSize,
		CacheSize:         db.options.CoreStorage.CacheSize,
		ActiveConnections: db.activeConns.Load(),
		MaxConnections:    db.options.ConnectionPool.MaxConnections,
		LastCheckTime:     time.Now(),
		IsHealthy:         true,
	}

	// Get catalog stats
	if db.catalog != nil {
		stats.Tables = len(db.catalog.ListTables())
		// Count regular + FTS + JSON indexes
		stats.Indexes = len(db.catalog.ListFTSIndexes()) + len(db.catalog.ListJSONIndexes())
	}

	// Get backend size
	if db.backend != nil {
		stats.DatabaseSize = db.backend.Size()
	}

	return stats, nil
}

// HealthCheck performs a health check on the database

func (db *DB) HealthCheck() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed.Load() {
		return ErrDatabaseClosed
	}

	// Try a simple catalog operation to verify connectivity
	if db.catalog == nil {
		return fmt.Errorf("catalog not initialized")
	}

	// Ping the backend
	if db.backend == nil {
		return fmt.Errorf("backend not initialized")
	}

	return nil
}

// IsHealthy returns true if the database is healthy

func (db *DB) IsHealthy() bool {
	return db.HealthCheck() == nil
}

// GetDatabasePath returns the database file path (implements backup.Database)

func (db *DB) GetDatabasePath() string {
	return db.path
}

// GetWALPath returns the WAL directory path (implements backup.Database)

func (db *DB) GetWALPath() string {
	if db.wal != nil {
		return db.path + ".wal"
	}
	return ""
}

// Checkpoint performs a database checkpoint (implements backup.Database).
// When WAL is enabled, checkpoint uses flushMu.RLock so explicit transaction
// commits can proceed concurrently; WAL serialization is handled by w.mu in
// WAL.Checkpoint.  The no-WAL path keeps flushMu.Lock because there is no
// recovery log to replay changes that arrive after FlushTableTrees.

func (db *DB) Checkpoint() error {
	if db.closed.Load() {
		return ErrDatabaseClosed
	}

	// Skip checkpoint if a hot backup is active — backup holds backupMu exclusively
	// to ensure the file copy is point-in-time consistent.
	if !db.backupMu.TryLock() {
		return nil // backup in progress, skip this checkpoint
	}
	defer db.backupMu.Unlock()

	if db.wal != nil {
		db.flushMu.RLock()
		defer db.flushMu.RUnlock()
	} else {
		db.flushMu.Lock()
		defer db.flushMu.Unlock()
	}

	if db.catalog != nil {
		if err := db.catalog.FlushTableTrees(); err != nil {
			return fmt.Errorf("failed to flush table trees: %w", err)
		}
	}

	if db.wal != nil {
		return db.wal.Checkpoint(db.pool)
	}
	return db.pool.FlushDirty()
}

// BeginHotBackup starts a hot backup (implements backup.Database).
//
// Point-in-time consistency protocol:
//  1. backupMu is acquired (held until EndHotBackup) to block manual and
//     automatic checkpoint paths (DB.Checkpoint, runCheckpointJob).
//  2. The catalog and meta page are persisted, then a full checkpoint runs
//     (table trees flushed, WAL checkpointed / dirty pages flushed) so the
//     on-disk file is a complete, consistent image.
//  3. The buffer pool's dirty-page writers (background flusher and eviction
//     dirty writes) are paused so nothing mutates the database file while
//     the backup copies it. Writes continue in memory/WAL during the copy.
//
// EndHotBackup resumes flushing and releases backupMu.
func (db *DB) BeginHotBackup() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed.Load() {
		return ErrDatabaseClosed
	}

	// Acquire backupMu and hold it for the duration of the backup. Released
	// in EndHotBackup, or below on error so a failed Begin cannot wedge
	// every future checkpoint.
	db.backupMu.Lock()

	if err := db.hotBackupCheckpointLocked(); err != nil {
		db.backupMu.Unlock()
		return err
	}

	// Freeze the database file: no dirty-page writes from the background
	// flusher or eviction until EndHotBackup.
	db.pool.PauseBackgroundFlusher()
	db.hotBackupActive = true
	return nil
}

// hotBackupCheckpointLocked persists the catalog/meta page and flushes all
// dirty state to the database file. Caller must hold db.mu and db.backupMu.
func (db *DB) hotBackupCheckpointLocked() error {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	// Persist catalog metadata and root page ID before copying files
	if err := db.catalog.Save(); err != nil {
		return fmt.Errorf("failed to save catalog: %w", err)
	}
	if err := db.saveMetaPage(); err != nil {
		return fmt.Errorf("failed to save meta page: %w", err)
	}
	// Flush table trees and checkpoint so every committed page reaches the
	// database file before the copy starts. The backup manager's own
	// Checkpoint() call is a no-op while backupMu is held, so this is the
	// checkpoint that actually guarantees a consistent on-disk image.
	if err := db.catalog.FlushTableTrees(); err != nil {
		return fmt.Errorf("failed to flush table trees: %w", err)
	}
	if db.wal != nil {
		if err := db.wal.Checkpoint(db.pool); err != nil {
			return fmt.Errorf("failed to checkpoint WAL: %w", err)
		}
		return nil
	}
	return db.pool.FlushDirty()
}

// EndHotBackup ends a hot backup (implements backup.Database).
func (db *DB) EndHotBackup() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	// Resume flushing and release backupMu when a backup is active — even if
	// the database was closed mid-backup, so checkpoints are never wedged
	// permanently. Calling EndHotBackup without a matching BeginHotBackup is
	// a no-op (aside from the closed check).
	if db.hotBackupActive {
		db.hotBackupActive = false
		db.pool.ResumeBackgroundFlusher()
		db.backupMu.Unlock()
	}
	if db.closed.Load() {
		return ErrDatabaseClosed
	}
	return nil
}

// GetCurrentLSN returns the current log sequence number (implements backup.Database)

func (db *DB) GetCurrentLSN() uint64 {
	if db.wal != nil {
		return db.wal.LSN()
	}
	return 0
}

// Backup methods

// CreateBackup creates a new backup

func (db *DB) CreateBackup(ctx context.Context, backupType string) (*backup.Backup, error) {
	if db.closed.Load() {
		return nil, ErrDatabaseClosed
	}
	if db.backupMgr == nil {
		return nil, fmt.Errorf("backup manager not initialized")
	}
	var btype backup.Type
	switch backupType {
	case "full":
		btype = backup.TypeFull
	case "incremental":
		btype = backup.TypeIncremental
	default:
		btype = backup.TypeFull
	}
	return db.backupMgr.CreateBackup(ctx, btype)
}

// ListBackups returns a list of all backups

func (db *DB) ListBackups() []*backup.Backup {
	if db.backupMgr == nil {
		return nil
	}
	return db.backupMgr.ListBackups()
}

// GetBackup returns a specific backup by ID

func (db *DB) GetBackup(id string) *backup.Backup {
	if db.backupMgr == nil {
		return nil
	}
	return db.backupMgr.GetBackup(id)
}

// DeleteBackup deletes a backup by ID

func (db *DB) DeleteBackup(id string) error {
	if db.closed.Load() {
		return ErrDatabaseClosed
	}
	if db.backupMgr == nil {
		return fmt.Errorf("backup manager not initialized")
	}
	return db.backupMgr.DeleteBackup(id)
}

// GetBackupManager returns the backup manager

func (db *DB) GetBackupManager() *backup.Manager {
	return db.backupMgr
}

// Query Cache methods

// GetQueryCache returns the catalog's query cache (nil if not enabled).
func (db *DB) GetQueryCache() *cache.Cache {
	return db.catalog.GetQueryCache()
}

// GetIndexRecommendations returns missing index suggestions based on
// observed query patterns since the database was opened.

func (db *DB) GetIndexRecommendations() []*advisor.IndexRecommendation {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.indexAdvisor == nil {
		return nil
	}

	existing := db.catalog.ListIndexesByTable()
	return db.indexAdvisor.Recommendations(existing)
}

// ResetIndexAdvisor clears all recorded query patterns.

func (db *DB) ResetIndexAdvisor() {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.indexAdvisor != nil {
		db.indexAdvisor.Reset()
	}
}

// PlanCache methods

// GetPlanCacheStats returns query plan cache statistics
// Returns nil if plan cache is not enabled

func (db *DB) GetPlanCacheStats() *QueryPlanCacheStats {
	db.mu.RLock()
	planCache := db.planCache
	db.mu.RUnlock()

	if planCache == nil {
		return nil
	}
	stats := planCache.GetStats()
	return &stats
}

// ClearPlanCache clears all entries from the query plan cache

func (db *DB) ClearPlanCache() {
	db.mu.RLock()
	planCache := db.planCache
	db.mu.RUnlock()

	if planCache != nil {
		planCache.Clear()
	}
}

// EnablePlanCache enables the query plan cache with specified size limits

func (db *DB) EnablePlanCache(maxSize int64, maxEntries int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.planCache != nil {
		// Already enabled, just update settings
		return
	}

	if maxSize <= 0 {
		maxSize = 32 * 1024 * 1024 // 32MB default
	}
	if maxEntries <= 0 {
		maxEntries = 1000
	}
	db.planCache = NewQueryPlanCache(maxSize, maxEntries)
}

// DisablePlanCache disables and clears the query plan cache

func (db *DB) DisablePlanCache() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.planCache != nil {
		db.planCache.Clear()
		db.planCache = nil
	}
}

// IsPlanCacheEnabled returns true if plan cache is enabled

func (db *DB) IsPlanCacheEnabled() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.planCache != nil
}

// Optimizer methods

// GetOptimizer returns the query optimizer

func (db *DB) GetOptimizer() *optimizer.Optimizer {
	return db.optimizer
}

// UpdateTableStatistics updates statistics for a table

func (db *DB) UpdateTableStatistics(tableName string, stats *optimizer.TableStatistics) {
	if db.optimizer != nil {
		db.optimizer.UpdateStatistics(tableName, stats)
	}
}

// Replication methods

// GetReplicationManager returns the replication manager

func (db *DB) GetReplicationManager() *replication.Manager {
	return db.replicationMgr
}

// SearchVectorKNN performs a K-nearest neighbor search on a vector index

func (db *DB) SearchVectorKNN(indexName string, queryVector []float64, k int) ([]string, []float64, error) {
	if db.closed.Load() {
		return nil, nil, ErrDatabaseClosed
	}
	return db.catalog.SearchVectorKNN(indexName, queryVector, k)
}

// SearchVectorRange performs a range search on a vector index

func (db *DB) SearchVectorRange(indexName string, queryVector []float64, radius float64) ([]string, []float64, error) {
	if db.closed.Load() {
		return nil, nil, ErrDatabaseClosed
	}
	return db.catalog.SearchVectorRange(indexName, queryVector, radius)
}
