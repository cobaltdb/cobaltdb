package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/advisor"
	"github.com/cobaltdb/cobaltdb/pkg/audit"
	"github.com/cobaltdb/cobaltdb/pkg/backup"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/cache"
	"github.com/cobaltdb/cobaltdb/pkg/catalog"
	"github.com/cobaltdb/cobaltdb/pkg/fdw"
	"github.com/cobaltdb/cobaltdb/pkg/logger"
	"github.com/cobaltdb/cobaltdb/pkg/metrics"
	"github.com/cobaltdb/cobaltdb/pkg/optimizer"
	"github.com/cobaltdb/cobaltdb/pkg/replication"
	"github.com/cobaltdb/cobaltdb/pkg/scheduler"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

func DefaultOptions() *Options {
	return &Options{
		PageSize:            storage.PageSize,
		CacheSize:           1024, // 4MB cache
		InMemory:            false,
		WALEnabled:          BoolPtr(true),
		SyncMode:            SyncNormal,
		Logger:              logger.Default(),
		MaxConnections:      100, // Default max connections
		ConnectionTimeout:   30 * time.Second,
		QueryTimeout:        60 * time.Second,
		MaxStmtCacheSize:    1000, // Default max cached statements
		EnableAutoVacuum:    true,
		AutoVacuumInterval:  1 * time.Minute,
		AutoVacuumThreshold: 0.2, // 20% dead tuples triggers vacuum
		EnableAutoCheckpoint: true,
		CheckpointInterval:   5 * time.Minute,
		EnableScheduler:      true,
		AnalyzeInterval:      1 * time.Hour,
		SchedulerWorkers:     2,
		ParallelWorkers:      runtime.NumCPU(),
		ParallelThreshold:    1000,
	}
}

// Open opens or creates a database at the given path

func Open(path string, opts *Options) (*DB, error) {
	defaults := DefaultOptions()
	if opts == nil {
		opts = defaults
	} else {
		// Apply defaults for unspecified options
		if opts.PageSize == 0 {
			opts.PageSize = defaults.PageSize
		}
		if opts.CacheSize == 0 {
			opts.CacheSize = defaults.CacheSize
		}
		if opts.WALEnabled == nil {
			opts.WALEnabled = defaults.WALEnabled
		}
		if opts.SyncMode == 0 {
			opts.SyncMode = defaults.SyncMode
		}
		if opts.Logger == nil {
			opts.Logger = defaults.Logger
		}
	}

	// Setup logger
	log := opts.Logger
	if log == nil {
		log = logger.Default()
	}
	log = log.WithComponent("engine")

	var backend storage.Backend
	var err error

	if opts.InMemory || path == ":memory:" {
		log.Infof("Opening in-memory database")
		backend = storage.NewMemory()
	} else {
		log.Infof("Opening database at %s", path)
		// Ensure directory exists
		dir := filepath.Dir(path)
		if dir != "." && dir != "/" {
			if err := os.MkdirAll(dir, 0750); err != nil {
				return nil, fmt.Errorf("failed to create directory: %w", err)
			}
		}
		backend, err = storage.OpenDisk(path)
		if err != nil {
			log.Errorf("Failed to open database: %v", err)
			return nil, fmt.Errorf("failed to open database: %w", err)
		}

		// Wrap with encryption if encryption key is provided
		if opts.EncryptionConfig != nil && opts.EncryptionConfig.Enabled {
			log.Infof("Enabling encryption at rest")
			// Try to load existing salt for key derivation consistency
			if len(opts.EncryptionConfig.Salt) == 0 && path != ":memory:" {
				if salt, loadErr := storage.LoadSalt(path); loadErr == nil && salt != nil {
					opts.EncryptionConfig.Salt = salt
				}
			}
			backend, err = storage.NewEncryptedBackend(backend, opts.EncryptionConfig)
			if err != nil {
				backend.Close()
				return nil, fmt.Errorf("failed to setup encryption: %w", err)
			}
			// Persist salt for future opens
			if path != ":memory:" {
				if salt := backend.(*storage.EncryptedBackend).GetSalt(); salt != nil {
					if perr := storage.PersistSalt(path, salt); perr != nil {
						log.Warnf("failed to persist encryption salt: %v", perr)
					}
				}
			}
		} else if len(opts.EncryptionKey) > 0 {
			log.Infof("Enabling encryption at rest")
			encConfig := &storage.EncryptionConfig{
				Enabled:   true,
				Key:       opts.EncryptionKey,
				Algorithm: "aes-256-gcm",
				UseArgon2: true,
			}
			// Try to load existing salt for key derivation consistency
			if path != ":memory:" {
				if salt, loadErr := storage.LoadSalt(path); loadErr == nil && salt != nil {
					encConfig.Salt = salt
				}
			}
			backend, err = storage.NewEncryptedBackend(backend, encConfig)
			if err != nil {
				backend.Close()
				return nil, fmt.Errorf("failed to setup encryption: %w", err)
			}
			// Persist salt for future opens
			if path != ":memory:" {
				if salt := backend.(*storage.EncryptedBackend).GetSalt(); salt != nil {
					if perr := storage.PersistSalt(path, salt); perr != nil {
						log.Warnf("failed to persist encryption salt: %v", perr)
					}
				}
			}
		}

		// Wrap with page-level compression if configured
		if opts.CompressionConfig != nil && opts.CompressionConfig.Enabled {
			log.Infof("Enabling page-level compression")
			backend, err = storage.NewCompressedBackend(backend, opts.CompressionConfig)
			if err != nil {
				backend.Close()
				return nil, fmt.Errorf("failed to setup compression: %w", err)
			}
		}
	}

	// Initialize metrics collector
	collector := metrics.NewCollector(0) // Use default interval

	db := &DB{
		path:         path,
		backend:      backend,
		options:      opts,
		stmtCache:    make(map[string]*cachedStmt),
		stmtLRU:      newStmtLRUList(),
		metrics:      collector,
		shutdownCh:   make(chan struct{}),
		indexAdvisor: advisor.NewIndexAdvisor(),
	}

	// Initialize audit logger if configured
	if opts.AuditConfig != nil && opts.AuditConfig.Enabled {
		log.Infof("Initializing audit logging")
		auditLogger, err := audit.New(opts.AuditConfig, log)
		if err != nil {
			backend.Close()
			return nil, fmt.Errorf("failed to initialize audit logger: %w", err)
		}
		db.auditLogger = auditLogger
	}

	// Initialize RLS manager if enabled
	if opts.EnableRLS {
		log.Infof("Initializing row-level security")
		db.rlsManager = security.NewManager()
	}

	// Initialize connection semaphore if max connections is set
	if opts.MaxConnections > 0 {
		db.connSem = make(chan struct{}, opts.MaxConnections)
	}

	// Start metrics collection
	go collector.Start(context.Background())

	// Initialize buffer pool
	db.pool = storage.NewBufferPool(opts.CacheSize, backend)
	db.unregisterStorageStats = metrics.RegisterStorageMetricsProvider(func() metrics.StorageMetrics {
		stats := db.pool.Stats()
		return metrics.StorageMetrics{
			Capacity:      stats.Capacity,
			PageCount:     stats.PageCount,
			DirtyCount:    stats.DirtyCount,
			PinnedCount:   stats.PinnedCount,
			FreeCount:     stats.FreeCount,
			HitCount:      stats.HitCount,
			MissCount:     stats.MissCount,
			HitRatio:      stats.HitRatio,
			ReadCount:     stats.ReadCount,
			WriteCount:    stats.WriteCount,
			EvictionCount: stats.EvictionCount,
		}
	})

	// Start background dirty page flusher (5s interval) for disk-backed databases
	if db.path != ":memory:" {
		db.pool.StartBackgroundFlusher(5 * time.Second)
	}

	// Initialize or load database
	if err := db.initialize(); err != nil {
		collector.Stop() // Stop metrics goroutine to prevent leak
		if db.auditLogger != nil {
			db.auditLogger.Close()
		}
		if db.wal != nil {
			db.wal.Close()
		}
		backend.Close()
		return nil, err
	}

	// Start scheduler for maintenance jobs if enabled.
	// Auto-vacuum implies the scheduler must be active.
	if !db.options.InMemory && db.path != ":memory:" {
		if db.options.EnableScheduler || db.options.EnableAutoVacuum {
			db.startScheduler()
		}
	}

	return db, nil
}

// initialize initializes a new database or loads an existing one

func (db *DB) initialize() error {
	// Check if database exists
	if db.backend.Size() == 0 {
		// Create new database
		return db.createNew()
	}

	// Load existing database
	return db.loadExisting()
}

// createNew creates a new database

func (db *DB) createNew() error {
	// Create meta page with initial values
	metaPage := storage.NewPage(0, storage.PageTypeMeta)
	meta := storage.NewMetaPage()
	meta.Serialize(metaPage.Data)

	// Write initial meta page
	if _, err := db.backend.WriteAt(metaPage.Data, 0); err != nil {
		return fmt.Errorf("failed to write meta page: %w", err)
	}

	// Create root B+Tree for system catalog
	tree, err := btree.NewBTree(db.pool)
	if err != nil {
		return fmt.Errorf("failed to create catalog tree: %w", err)
	}
	db.rootTree = tree

	// Update meta page with actual root page ID
	meta.RootPageID = db.rootTree.RootPageID()
	meta.Serialize(metaPage.Data)
	if _, err := db.backend.WriteAt(metaPage.Data, 0); err != nil {
		return fmt.Errorf("failed to update meta page: %w", err)
	}

	// Initialize WAL for new databases when enabled, BEFORE creating the
	// catalog and transaction manager so they receive a non-nil WAL reference.
	if db.options.WALEnabled != nil && *db.options.WALEnabled && db.path != ":memory:" && db.wal == nil {
		walPath := db.path + ".wal"
		wal, err := storage.OpenWAL(walPath)
		if err != nil {
			return fmt.Errorf("failed to initialize WAL: %w", err)
		}
		db.wal = wal

		if encBackend, ok := db.backend.(*storage.EncryptedBackend); ok {
			wal.SetEncryptionCipher(encBackend.GetCipher())
		}

		db.pool.SetWAL(wal)

		switch db.options.SyncMode {
		case SyncNormal:
			wal.EnableGroupCommit(0, 5*time.Millisecond)
		case SyncOff:
			wal.EnableGroupCommit(0, 0)
		}
	}

	// Initialize catalog
	db.catalog = catalog.New(db.rootTree, db.pool, db.wal)
	db.catalog.SetParallelOptions(db.options.ParallelWorkers, db.options.ParallelThreshold)

	// Initialize FDW registry and register built-in wrappers
	fdwRegistry := fdw.NewRegistry()
	fdwRegistry.Register("csv", func() fdw.ForeignDataWrapper { return &fdw.CSVWrapper{} })
	db.catalog.SetFDWRegistry(fdwRegistry)

	// Enable RLS if configured
	if db.options.EnableRLS {
		db.catalog.EnableRLS()
	}

	// Initialize transaction manager
	db.txnMgr = txn.NewManager(db.pool, db.wal)
	db.catalog.SetTxnManager(db.txnMgr)
	db.catalog.EnableBufferedWrites()

	// Initialize query cache if enabled
	if db.options.EnableQueryCache {
		cacheConfig := &cache.Config{
			MaxSize:         db.options.QueryCacheSize,
			TTL:             db.options.QueryCacheTTL,
			Enabled:         true,
			CleanupInterval: 1 * time.Minute,
		}
		db.queryCache = cache.New(cacheConfig)
	}

	// Initialize query optimizer
	db.optimizer = optimizer.New(optimizer.DefaultConfig(), nil)

	// Initialize replication manager if configured
	if db.options.ReplicationRole != "" {
		// Parse role
		var role replication.Role
		switch db.options.ReplicationRole {
		case "master":
			role = replication.RoleMaster
		case "slave":
			role = replication.RoleSlave
		default:
			role = replication.RoleStandalone
		}

		// Parse mode
		var mode replication.ReplicationMode
		switch db.options.ReplicationMode {
		case "sync":
			mode = replication.ModeSync
		case "full_sync":
			mode = replication.ModeFullSync
		default:
			mode = replication.ModeAsync
		}

		replConfig := &replication.Config{
			Role:       role,
			Mode:       mode,
			ListenAddr: db.options.ReplicationListenAddr,
			MasterAddr: db.options.ReplicationMasterAddr,
			AuthToken:  db.options.ReplicationAuthToken,
			SSLCert:    db.options.ReplicationSSLCert,
			SSLKey:     db.options.ReplicationSSLKey,
			SSLCA:      db.options.ReplicationSSLCA,
			StateFile:  db.options.ReplicationStateFile,
		}
		db.replicationMgr = replication.NewManager(replConfig)
		db.configureReplicationCallbacks()
		if err := db.replicationMgr.Start(); err != nil {
			return fmt.Errorf("failed to start replication manager: %w", err)
		}
	}

	db.initializeBackupManager()

	// Initialize slow query log
	if db.options.EnableSlowQueryLog {
		threshold := db.options.SlowQueryThreshold
		if threshold == 0 {
			threshold = 1 * time.Second
		}
		maxEntries := db.options.SlowQueryMaxEntries
		if maxEntries == 0 {
			maxEntries = 1000
		}
		db.slowQueryLog = metrics.NewSlowQueryLog(true, threshold, maxEntries, db.options.SlowQueryLogFile)
		db.unregisterSlowQueryLog = metrics.RegisterSlowQueryLog(db.slowQueryLog)
	}

	return db.backend.Sync()
}

// saveMetaPage writes the current meta page to disk with updated root page ID

func (db *DB) saveMetaPage() error {
	metaPage := storage.NewPage(0, storage.PageTypeMeta)
	// Read existing meta page
	if _, err := db.backend.ReadAt(metaPage.Data, 0); err != nil {
		return fmt.Errorf("failed to read meta page: %w", err)
	}
	var meta storage.MetaPage
	if err := meta.Deserialize(metaPage.Data); err != nil {
		return fmt.Errorf("failed to deserialize meta page: %w", err)
	}
	// Update root page ID
	meta.RootPageID = db.rootTree.RootPageID()
	meta.Serialize(metaPage.Data)
	if _, err := db.backend.WriteAt(metaPage.Data, 0); err != nil {
		return fmt.Errorf("failed to write meta page: %w", err)
	}
	return nil
}

// loadExisting loads an existing database

func (db *DB) loadExisting() error {
	// Read meta page
	metaPage := storage.NewPage(0, storage.PageTypeMeta)
	if _, err := db.backend.ReadAt(metaPage.Data, 0); err != nil {
		return fmt.Errorf("failed to read meta page: %w", err)
	}

	var meta storage.MetaPage
	if err := meta.Deserialize(metaPage.Data); err != nil {
		return fmt.Errorf("failed to deserialize meta page: %w", err)
	}

	if err := meta.Validate(); err != nil {
		return fmt.Errorf("invalid database: %w", err)
	}

	// Open WAL if enabled
	if db.options.WALEnabled != nil && *db.options.WALEnabled && db.path != ":memory:" {
		walPath := db.path + ".wal"
		wal, err := storage.OpenWAL(walPath)
		if err != nil {
			return fmt.Errorf("failed to open WAL: %w", err)
		}
		db.wal = wal

		// If encryption is enabled, share the cipher with WAL for encrypted WAL records
		if encBackend, ok := db.backend.(*storage.EncryptedBackend); ok {
			wal.SetEncryptionCipher(encBackend.GetCipher())
		}

		db.pool.SetWAL(wal)

		// Enable group commit based on SyncMode
		switch db.options.SyncMode {
		case SyncNormal:
			wal.EnableGroupCommit(0, 5*time.Millisecond)
		case SyncOff:
			wal.EnableGroupCommit(0, 0)
		default: // SyncFull
			// immediate sync (default behavior)
		}

		// Recover from WAL if needed
		if wal.LSN() > wal.CheckpointLSN() {
			if err := wal.Recover(db.pool); err != nil {
				return fmt.Errorf("failed to recover from WAL: %w", err)
			}
		}
	}

	// Open root B+Tree
	db.rootTree = btree.OpenBTree(db.pool, meta.RootPageID)

	// Load catalog - schema and data are now stored in the B+Tree pages
	db.catalog = catalog.New(db.rootTree, db.pool, db.wal)
	db.catalog.SetParallelOptions(db.options.ParallelWorkers, db.options.ParallelThreshold)

	// Initialize FDW registry and register built-in wrappers
	fdwRegistry := fdw.NewRegistry()
	fdwRegistry.Register("csv", func() fdw.ForeignDataWrapper { return &fdw.CSVWrapper{} })
	db.catalog.SetFDWRegistry(fdwRegistry)

	// Enable RLS if configured
	if db.options.EnableRLS {
		db.catalog.EnableRLS()
	}

	// Load catalog metadata from the B+Tree
	if err := db.catalog.Load(); err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// Replay any logical WAL operations that were not flushed to pages before
	// the last crash.  This restores primary table data; indexes may need
	// rebuilding if they were updated after the WAL records were written.
	if db.wal != nil {
		if ops := db.wal.GetReplayOps(); len(ops) > 0 {
			if err := db.catalog.ReplayWALOps(ops); err != nil {
				return fmt.Errorf("failed to replay WAL operations: %w", err)
			}
		}
	}

	// Initialize transaction manager
	db.txnMgr = txn.NewManager(db.pool, db.wal)
	db.catalog.SetTxnManager(db.txnMgr)
	db.catalog.EnableBufferedWrites()

	// Initialize query cache if enabled
	if db.options.EnableQueryCache {
		cacheConfig := &cache.Config{
			MaxSize:         db.options.QueryCacheSize,
			TTL:             db.options.QueryCacheTTL,
			Enabled:         true,
			CleanupInterval: 1 * time.Minute,
		}
		db.queryCache = cache.New(cacheConfig)
	}

	// Initialize query optimizer
	db.optimizer = optimizer.New(optimizer.DefaultConfig(), nil)

	// Initialize replication manager if configured
	if db.options.ReplicationRole != "" {
		// Parse role
		var role replication.Role
		switch db.options.ReplicationRole {
		case "master":
			role = replication.RoleMaster
		case "slave":
			role = replication.RoleSlave
		default:
			role = replication.RoleStandalone
		}

		// Parse mode
		var mode replication.ReplicationMode
		switch db.options.ReplicationMode {
		case "sync":
			mode = replication.ModeSync
		case "full_sync":
			mode = replication.ModeFullSync
		default:
			mode = replication.ModeAsync
		}

		replConfig := &replication.Config{
			Role:       role,
			Mode:       mode,
			ListenAddr: db.options.ReplicationListenAddr,
			MasterAddr: db.options.ReplicationMasterAddr,
			AuthToken:  db.options.ReplicationAuthToken,
			SSLCert:    db.options.ReplicationSSLCert,
			SSLKey:     db.options.ReplicationSSLKey,
			SSLCA:      db.options.ReplicationSSLCA,
			StateFile:  db.options.ReplicationStateFile,
		}
		db.replicationMgr = replication.NewManager(replConfig)
		db.configureReplicationCallbacks()
		if err := db.replicationMgr.Start(); err != nil {
			return fmt.Errorf("failed to start replication manager: %w", err)
		}
	}

	db.initializeBackupManager()

	// Initialize slow query log
	if db.options.EnableSlowQueryLog {
		threshold := db.options.SlowQueryThreshold
		if threshold == 0 {
			threshold = 1 * time.Second
		}
		maxEntries := db.options.SlowQueryMaxEntries
		if maxEntries == 0 {
			maxEntries = 1000
		}
		db.slowQueryLog = metrics.NewSlowQueryLog(true, threshold, maxEntries, db.options.SlowQueryLogFile)
		db.unregisterSlowQueryLog = metrics.RegisterSlowQueryLog(db.slowQueryLog)
	}

	// Initialize query plan cache
	if db.options.EnablePlanCache {
		planCacheSize := db.options.PlanCacheSize
		if planCacheSize <= 0 {
			planCacheSize = 32 * 1024 * 1024 // 32MB default
		}
		planCacheEntries := db.options.PlanCacheEntries
		if planCacheEntries <= 0 {
			planCacheEntries = 1000
		}
		db.planCache = NewQueryPlanCache(planCacheSize, planCacheEntries)
	}

	return nil
}

func (db *DB) initializeBackupManager() {
	if db.options.BackupDir == "" && (db.options.InMemory || db.path == ":memory:") {
		db.backupMgr = nil
		return
	}

	backupConfig := &backup.Config{
		BackupDir:        db.options.BackupDir,
		RetentionPeriod:  db.options.BackupRetention,
		MaxBackups:       db.options.MaxBackups,
		CompressionLevel: db.options.BackupCompressionLevel,
	}
	if backupConfig.BackupDir == "" {
		backupConfig.BackupDir = "./backups"
	}
	db.backupMgr = backup.NewManager(backupConfig, db)
}

// Close closes the database
// Shutdown gracefully shuts down the database with a timeout

func (db *DB) Shutdown(ctx context.Context) error {
	db.shutdownOnce.Do(func() {
		close(db.shutdownCh)
	})

	// Wait for active connections to complete or timeout
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Timeout reached, force close
			return db.Close()
		case <-ticker.C:
			if db.activeConns.Load() == 0 {
				// All connections released
				return db.Close()
			}
		}
	}
}

// Close closes the database immediately

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return nil
	}

	db.closed.Store(true)

	// Signal shutdown
	select {
	case <-db.shutdownCh:
		// Already closed
	default:
		close(db.shutdownCh)
	}

	// Stop metrics collection
	if db.metrics != nil {
		db.metrics.Stop()
	}
	if db.unregisterSlowQueryLog != nil {
		db.unregisterSlowQueryLog()
	}
	if db.unregisterStorageStats != nil {
		db.unregisterStorageStats()
	}

	// Stop scheduler (vacuum, analyze, and other maintenance jobs)
	if db.scheduler != nil {
		db.scheduler.Stop()
	}

	var errs []error

	// Serialize all page-flush operations during close so that no concurrent
	// btree flush can race with BufferPool.FlushAll.
	db.flushMu.Lock()

	// Save catalog metadata to B+Tree (if not in-memory)
	if !db.options.InMemory && db.path != ":memory:" {
		if err := db.catalog.Save(); err != nil {
			errs = append(errs, fmt.Errorf("save catalog: %w", err))
		}

		// Update meta page with current root page ID
		if err := db.saveMetaPage(); err != nil {
			errs = append(errs, fmt.Errorf("save meta page: %w", err))
		}
	}

	// Perform WAL checkpoint before closing pool (checkpoint needs pool access)
	if db.wal != nil {
		if err := db.wal.Checkpoint(db.pool); err != nil {
			errs = append(errs, fmt.Errorf("checkpoint WAL: %w", err))
		}
	}

	// Flush buffer pool (after checkpoint)
	if err := db.pool.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close buffer pool: %w", err))
	}

	db.flushMu.Unlock()

	// Close audit logger
	if db.auditLogger != nil {
		if err := db.auditLogger.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close audit logger: %w", err))
		}
	}

	// Close replication manager
	if db.replicationMgr != nil {
		if err := db.replicationMgr.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("close replication manager: %w", err))
		}
	}

	// Close WAL
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close WAL: %w", err))
		}
	}

	// Close backend
	if err := db.backend.Close(); err != nil {
		errs = append(errs, fmt.Errorf("backend close: %w", err))
	}
	return errors.Join(errs...)
}

// startScheduler initializes and starts the job scheduler, registering
// default maintenance jobs (auto-vacuum, analyze).

func (db *DB) startScheduler() {
	workers := db.options.SchedulerWorkers
	if workers <= 0 {
		workers = 2
	}
	tick := db.options.SchedulerTickInterval
	if tick <= 0 {
		tick = 1 * time.Second
	}
	db.scheduler = scheduler.NewWithInterval(workers, db.options.Logger, tick)

	// Register auto-vacuum job
	if db.options.EnableAutoVacuum {
		interval := db.options.AutoVacuumInterval
		if interval <= 0 {
			interval = 1 * time.Minute
		}
		threshold := db.options.AutoVacuumThreshold
		if threshold <= 0 {
			threshold = 0.2
		}
		vacuumJob := &scheduler.Job{
			ID:       "auto-vacuum",
			Name:     "Auto Vacuum",
			Type:     scheduler.JobTypeVacuum,
			Interval: interval,
			Enabled:  true,
			Fn: func(ctx context.Context) error {
				return db.runAutoVacuumJob(threshold)
			},
		}
		if err := db.scheduler.Register(vacuumJob); err != nil {
			db.options.Logger.Warnf("Failed to register auto-vacuum job: %v", err)
		}
	}

	// Register analyze job
	analyzeInterval := db.options.AnalyzeInterval
	if analyzeInterval <= 0 {
		analyzeInterval = 1 * time.Hour
	}
	analyzeJob := &scheduler.Job{
		ID:       "auto-analyze",
		Name:     "Auto Analyze",
		Type:     scheduler.JobTypeAnalyze,
		Interval: analyzeInterval,
		Enabled:  true,
		Fn: func(ctx context.Context) error {
			return db.runAnalyzeJob()
		},
	}
	if err := db.scheduler.Register(analyzeJob); err != nil {
		db.options.Logger.Warnf("Failed to register auto-analyze job: %v", err)
	}

	// Register checkpoint job
	if db.options.EnableAutoCheckpoint {
		checkpointInterval := db.options.CheckpointInterval
		if checkpointInterval <= 0 {
			checkpointInterval = 5 * time.Minute
		}
		checkpointJob := &scheduler.Job{
			ID:       "auto-checkpoint",
			Name:     "Auto Checkpoint",
			Type:     scheduler.JobTypeCheckpoint,
			Interval: checkpointInterval,
			Enabled:  true,
			Fn: func(ctx context.Context) error {
				return db.runCheckpointJob()
			},
		}
		if err := db.scheduler.Register(checkpointJob); err != nil {
			db.options.Logger.Warnf("Failed to register auto-checkpoint job: %v", err)
		}
	}

	db.scheduler.Start()
}

// runAutoVacuumJob checks all tables and vacuums those exceeding the dead-tuple threshold.

func (db *DB) runAutoVacuumJob(threshold float64) error {
	tables := db.catalog.ListTablesNeedingVacuum(threshold)
	for _, tableName := range tables {
		if err := db.catalog.VacuumTable(tableName); err != nil {
			if db.options.Logger != nil {
				db.options.Logger.Warnf("AutoVacuum failed for table %s: %v", tableName, err)
			}
		} else {
			if db.options.Logger != nil {
				db.options.Logger.Infof("AutoVacuum completed for table %s", tableName)
			}
		}
	}
	return nil
}

// runAnalyzeJob runs ANALYZE on all tables to update query planner statistics.

func (db *DB) runAnalyzeJob() error {
	tables := db.catalog.ListTables()
	for _, tableName := range tables {
		if err := db.catalog.Analyze(tableName); err != nil {
			if db.options.Logger != nil {
				db.options.Logger.Warnf("AutoAnalyze failed for table %s: %v", tableName, err)
			}
		} else {
			if db.options.Logger != nil {
				db.options.Logger.Infof("AutoAnalyze completed for table %s", tableName)
			}
		}
	}
	return nil
}

// runCheckpointJob performs a WAL checkpoint to truncate the log and flush
// dirty pages.  Called by the scheduler; safe to run concurrently with reads
// and explicit transaction commits because DB.Checkpoint uses flushMu.RLock
// when WAL is enabled (WAL.Checkpoint serializes its own WAL append via w.mu).
func (db *DB) runCheckpointJob() error {
	if err := db.Checkpoint(); err != nil {
		if db.options.Logger != nil {
			db.options.Logger.Warnf("AutoCheckpoint failed: %v", err)
		}
		return err
	}
	if db.options.Logger != nil {
		db.options.Logger.Info("AutoCheckpoint completed")
	}
	return nil
}

// GetScheduler returns the job scheduler for advanced usage.
