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
		CoreStorage: CoreStorage{
			PageSize:   storage.PageSize,
			CacheSize:  1024, // 4MB cache
			InMemory:   false,
			WALEnabled: BoolPtr(true),
			SyncMode:   SyncNormal,
			Logger:     logger.Default(),
		},
		ConnectionPool: ConnectionPool{
			MaxConnections:    100,
			ConnectionTimeout: 30 * time.Second,
			QueryTimeout:      60 * time.Second,
		},
		Security: Security{
			MaxStmtCacheSize: 1000,
		},
		Maintenance: MaintenanceConfig{
			EnableAutoVacuum:     true,
			AutoVacuumInterval:   1 * time.Minute,
			AutoVacuumThreshold:  0.2,
			EnableAutoCheckpoint: true,
			CheckpointInterval:   5 * time.Minute,
		},
		Scheduler: SchedulerConfig{
			EnableScheduler: true,
			AnalyzeInterval: 1 * time.Hour,
			Workers:         2,
		},
		ParallelQuery: ParallelQueryConfig{
			Workers:   runtime.NumCPU(),
			Threshold: 1000,
		},
	}
}

func normalizeOptions(opts *Options) *Options {
	defaults := DefaultOptions()
	if opts == nil {
		return defaults
	}

	normalized := *opts
	if normalized.CoreStorage.PageSize == 0 {
		normalized.CoreStorage.PageSize = defaults.CoreStorage.PageSize
	}
	if normalized.CoreStorage.CacheSize == 0 {
		normalized.CoreStorage.CacheSize = defaults.CoreStorage.CacheSize
	}
	if normalized.CoreStorage.WALEnabled == nil {
		normalized.CoreStorage.WALEnabled = cloneBoolPtr(defaults.CoreStorage.WALEnabled)
	} else {
		normalized.CoreStorage.WALEnabled = cloneBoolPtr(normalized.CoreStorage.WALEnabled)
	}
	if normalized.CoreStorage.SyncMode == 0 {
		normalized.CoreStorage.SyncMode = defaults.CoreStorage.SyncMode
	}
	if normalized.CoreStorage.Logger == nil {
		normalized.CoreStorage.Logger = defaults.CoreStorage.Logger
	}
	if normalized.ConnectionPool.MaxConnections == 0 {
		normalized.ConnectionPool.MaxConnections = defaults.ConnectionPool.MaxConnections
	}
	if normalized.ConnectionPool.ConnectionTimeout == 0 {
		normalized.ConnectionPool.ConnectionTimeout = defaults.ConnectionPool.ConnectionTimeout
	}
	if normalized.ConnectionPool.QueryTimeout == 0 {
		normalized.ConnectionPool.QueryTimeout = defaults.ConnectionPool.QueryTimeout
	}
	if normalized.Security.MaxStmtCacheSize == 0 {
		normalized.Security.MaxStmtCacheSize = defaults.Security.MaxStmtCacheSize
	}
	if normalized.Maintenance.AutoVacuumInterval == 0 {
		normalized.Maintenance.AutoVacuumInterval = defaults.Maintenance.AutoVacuumInterval
	}
	if normalized.Maintenance.AutoVacuumThreshold == 0 {
		normalized.Maintenance.AutoVacuumThreshold = defaults.Maintenance.AutoVacuumThreshold
	}
	if normalized.Maintenance.CheckpointInterval == 0 {
		normalized.Maintenance.CheckpointInterval = defaults.Maintenance.CheckpointInterval
	}
	if normalized.Scheduler.AnalyzeInterval == 0 {
		normalized.Scheduler.AnalyzeInterval = defaults.Scheduler.AnalyzeInterval
	}
	if normalized.Scheduler.Workers == 0 {
		normalized.Scheduler.Workers = defaults.Scheduler.Workers
	}
	if normalized.ParallelQuery.Workers == 0 {
		normalized.ParallelQuery.Workers = defaults.ParallelQuery.Workers
	}
	if normalized.ParallelQuery.Threshold == 0 {
		normalized.ParallelQuery.Threshold = defaults.ParallelQuery.Threshold
	}
	if len(normalized.Security.EncryptionKey) > 0 {
		normalized.Security.EncryptionKey = append([]byte(nil), normalized.Security.EncryptionKey...)
	}
	if normalized.Security.EncryptionConfig != nil {
		encConfig := *normalized.Security.EncryptionConfig
		if len(encConfig.Key) > 0 {
			encConfig.Key = append([]byte(nil), encConfig.Key...)
		}
		if len(encConfig.Salt) > 0 {
			encConfig.Salt = append([]byte(nil), encConfig.Salt...)
		}
		normalized.Security.EncryptionConfig = &encConfig
	}
	if normalized.Security.AuditConfig != nil {
		auditConfig := *normalized.Security.AuditConfig
		if len(auditConfig.Events) > 0 {
			auditConfig.Events = append([]audit.EventType(nil), auditConfig.Events...)
		}
		if len(auditConfig.SensitiveFields) > 0 {
			auditConfig.SensitiveFields = append([]string(nil), auditConfig.SensitiveFields...)
		}
		if len(auditConfig.EncryptionKey) > 0 {
			auditConfig.EncryptionKey = append([]byte(nil), auditConfig.EncryptionKey...)
		}
		normalized.Security.AuditConfig = &auditConfig
	}
	if normalized.PageCompression.Config != nil {
		compressionConfig := *normalized.PageCompression.Config
		normalized.PageCompression.Config = &compressionConfig
	}
	return &normalized
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

// Open opens or creates a database at the given path

func Open(path string, opts *Options) (*DB, error) {
	opts = normalizeOptions(opts)
	if opts.CoreStorage.CacheSize <= 0 {
		return nil, fmt.Errorf("cache size must be positive: %d", opts.CoreStorage.CacheSize)
	}
	if opts.CoreStorage.PageSize != storage.PageSize {
		return nil, fmt.Errorf("page size %d is unsupported; expected %d", opts.CoreStorage.PageSize, storage.PageSize)
	}

	// Setup logger
	log := opts.CoreStorage.Logger
	if log == nil {
		log = logger.Default()
	}
	log = log.WithComponent("engine")

	var backend storage.Backend
	var err error

	if opts.CoreStorage.InMemory || path == ":memory:" {
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
		if opts.Security.EncryptionConfig != nil && opts.Security.EncryptionConfig.Enabled {
			log.Infof("Enabling encryption at rest")
			// Try to load existing salt for key derivation consistency
			if len(opts.Security.EncryptionConfig.Salt) == 0 && path != ":memory:" {
				if salt, loadErr := storage.LoadSalt(path); loadErr == nil && salt != nil {
					opts.Security.EncryptionConfig.Salt = salt
				}
			}
			backend, err = storage.NewEncryptedBackend(backend, opts.Security.EncryptionConfig)
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
		} else if len(opts.Security.EncryptionKey) > 0 {
			log.Infof("Enabling encryption at rest")
			encConfig := &storage.EncryptionConfig{
				Enabled:   true,
				Key:       opts.Security.EncryptionKey,
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
		if opts.PageCompression.Config != nil && opts.PageCompression.Config.Enabled {
			log.Infof("Enabling page-level compression")
			backend, err = storage.NewCompressedBackend(backend, opts.PageCompression.Config)
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
	if opts.Security.AuditConfig != nil && opts.Security.AuditConfig.Enabled {
		log.Infof("Initializing audit logging")
		auditLogger, err := audit.New(opts.Security.AuditConfig, log)
		if err != nil {
			backend.Close()
			return nil, fmt.Errorf("failed to initialize audit logger: %w", err)
		}
		db.auditLogger = auditLogger
	}

	// Initialize RLS manager if enabled
	if opts.Security.EnableRLS {
		log.Infof("Initializing row-level security")
		db.rlsManager = security.NewManager()
	}

	// Initialize connection limiter (atomic fast path; 0 = unlimited).
	if opts.ConnectionPool.MaxConnections > 0 {
		db.connLimit = int64(opts.ConnectionPool.MaxConnections)
	}

	// Start metrics collection
	go collector.Start(context.Background())

	// Initialize buffer pool
	db.pool, err = storage.NewBufferPoolWithError(opts.CoreStorage.CacheSize, backend)
	if err != nil {
		collector.Stop()
		if db.auditLogger != nil {
			err = errors.Join(err, db.auditLogger.Close())
		}
		err = errors.Join(err, backend.Close())
		return nil, fmt.Errorf("failed to initialize buffer pool: %w", err)
	}
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
	if !db.options.CoreStorage.InMemory && db.path != ":memory:" {
		if db.options.Scheduler.EnableScheduler || db.options.Maintenance.EnableAutoVacuum {
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
	if db.options.CoreStorage.WALEnabled != nil && *db.options.CoreStorage.WALEnabled && db.path != ":memory:" && db.wal == nil {
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

		switch db.options.CoreStorage.SyncMode {
		case SyncNormal:
			wal.EnableGroupCommit(0, 5*time.Millisecond)
		case SyncOff:
			wal.EnableGroupCommit(0, 0)
		}
	}

	// Initialize catalog
	db.catalog = catalog.New(db.rootTree, db.pool, db.wal)
	db.catalog.SetParallelOptions(db.options.ParallelQuery.Workers, db.options.ParallelQuery.Threshold)

	// Initialize FDW registry and register built-in wrappers
	fdwRegistry := fdw.NewRegistry()
	fdwRegistry.Register("csv", func() fdw.ForeignDataWrapper { return &fdw.CSVWrapper{} })
	db.catalog.SetFDWRegistry(fdwRegistry)

	// Enable RLS if configured
	if db.options.Security.EnableRLS {
		db.catalog.EnableRLS()
	}

	// Initialize transaction manager
	db.txnMgr = txn.NewManager(db.pool, db.wal)
	db.catalog.SetTxnManager(db.txnMgr)
	db.catalog.EnableBufferedWrites()

	// Initialize query cache if enabled
	if db.options.QueryCache.EnableQueryCache {
		cacheConfig := &cache.Config{
			MaxSize:         db.options.QueryCache.QueryCacheSize,
			TTL:             db.options.QueryCache.QueryCacheTTL,
			Enabled:         true,
			CleanupInterval: 1 * time.Minute,
		}
		db.queryCache = cache.New(cacheConfig)
	}

	// Initialize query optimizer
	db.optimizer = optimizer.New(optimizer.DefaultConfig(), nil)

	// Initialize replication manager if configured
	if db.options.Replication.Role != "" {
		// Parse role
		var role replication.Role
		switch db.options.Replication.Role {
		case "master":
			role = replication.RoleMaster
		case "slave":
			role = replication.RoleSlave
		default:
			role = replication.RoleStandalone
		}

		// Parse mode
		var mode replication.ReplicationMode
		switch db.options.Replication.Mode {
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
			ListenAddr: db.options.Replication.ListenAddr,
			MasterAddr: db.options.Replication.MasterAddr,
			AuthToken:  db.options.Replication.AuthToken,
			SSLCert:    db.options.Replication.SSLCert,
			SSLKey:     db.options.Replication.SSLKey,
			SSLCA:      db.options.Replication.SSLCA,
			StateFile:  db.options.Replication.StateFile,
		}
		db.replicationMgr = replication.NewManager(replConfig)
		db.configureReplicationCallbacks()
		if err := db.replicationMgr.Start(); err != nil {
			return fmt.Errorf("failed to start replication manager: %w", err)
		}
	}

	db.initializeBackupManager()

	// Initialize slow query log
	if db.options.SlowQueryLog.EnableSlowQueryLog {
		threshold := db.options.SlowQueryLog.Threshold
		if threshold == 0 {
			threshold = 1 * time.Second
		}
		maxEntries := db.options.SlowQueryLog.MaxEntries
		if maxEntries == 0 {
			maxEntries = 1000
		}
		db.slowQueryLog = metrics.NewSlowQueryLog(true, threshold, maxEntries, db.options.SlowQueryLog.LogFile)
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
	if db.options.CoreStorage.WALEnabled != nil && *db.options.CoreStorage.WALEnabled && db.path != ":memory:" {
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
		switch db.options.CoreStorage.SyncMode {
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
	rootTree, err := btree.OpenBTreeStrict(db.pool, meta.RootPageID)
	if err != nil {
		return fmt.Errorf("failed to open root B+Tree: %w", err)
	}
	db.rootTree = rootTree

	// Load catalog - schema and data are now stored in the B+Tree pages
	db.catalog = catalog.New(db.rootTree, db.pool, db.wal)
	db.catalog.SetParallelOptions(db.options.ParallelQuery.Workers, db.options.ParallelQuery.Threshold)

	// Initialize FDW registry and register built-in wrappers
	fdwRegistry := fdw.NewRegistry()
	fdwRegistry.Register("csv", func() fdw.ForeignDataWrapper { return &fdw.CSVWrapper{} })
	db.catalog.SetFDWRegistry(fdwRegistry)

	// Enable RLS if configured
	if db.options.Security.EnableRLS {
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
	if db.options.QueryCache.EnableQueryCache {
		cacheConfig := &cache.Config{
			MaxSize:         db.options.QueryCache.QueryCacheSize,
			TTL:             db.options.QueryCache.QueryCacheTTL,
			Enabled:         true,
			CleanupInterval: 1 * time.Minute,
		}
		db.queryCache = cache.New(cacheConfig)
	}

	// Initialize query optimizer
	db.optimizer = optimizer.New(optimizer.DefaultConfig(), nil)

	// Initialize replication manager if configured
	if db.options.Replication.Role != "" {
		// Parse role
		var role replication.Role
		switch db.options.Replication.Role {
		case "master":
			role = replication.RoleMaster
		case "slave":
			role = replication.RoleSlave
		default:
			role = replication.RoleStandalone
		}

		// Parse mode
		var mode replication.ReplicationMode
		switch db.options.Replication.Mode {
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
			ListenAddr: db.options.Replication.ListenAddr,
			MasterAddr: db.options.Replication.MasterAddr,
			AuthToken:  db.options.Replication.AuthToken,
			SSLCert:    db.options.Replication.SSLCert,
			SSLKey:     db.options.Replication.SSLKey,
			SSLCA:      db.options.Replication.SSLCA,
			StateFile:  db.options.Replication.StateFile,
		}
		db.replicationMgr = replication.NewManager(replConfig)
		db.configureReplicationCallbacks()
		if err := db.replicationMgr.Start(); err != nil {
			return fmt.Errorf("failed to start replication manager: %w", err)
		}
	}

	db.initializeBackupManager()

	// Initialize slow query log
	if db.options.SlowQueryLog.EnableSlowQueryLog {
		threshold := db.options.SlowQueryLog.Threshold
		if threshold == 0 {
			threshold = 1 * time.Second
		}
		maxEntries := db.options.SlowQueryLog.MaxEntries
		if maxEntries == 0 {
			maxEntries = 1000
		}
		db.slowQueryLog = metrics.NewSlowQueryLog(true, threshold, maxEntries, db.options.SlowQueryLog.LogFile)
		db.unregisterSlowQueryLog = metrics.RegisterSlowQueryLog(db.slowQueryLog)
	}

	// Initialize query plan cache
	if db.options.PlanCache.EnablePlanCache {
		planCacheSize := db.options.PlanCache.Size
		if planCacheSize <= 0 {
			planCacheSize = 32 * 1024 * 1024 // 32MB default
		}
		planCacheEntries := db.options.PlanCache.MaxEntries
		if planCacheEntries <= 0 {
			planCacheEntries = 1000
		}
		db.planCache = NewQueryPlanCache(planCacheSize, planCacheEntries)
	}

	return nil
}

func (db *DB) initializeBackupManager() {
	if db.options.Backup.Dir == "" && (db.options.CoreStorage.InMemory || db.path == ":memory:") {
		db.backupMgr = nil
		return
	}

	backupConfig := &backup.Config{
		BackupDir:        db.options.Backup.Dir,
		RetentionPeriod:  db.options.Backup.Retention,
		MaxBackups:       db.options.Backup.MaxBackups,
		CompressionLevel: db.options.Backup.CompressionLevel,
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
	if !db.options.CoreStorage.InMemory && db.path != ":memory:" {
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
	workers := db.options.Scheduler.Workers
	if workers <= 0 {
		workers = 2
	}
	tick := db.options.Scheduler.TickInterval
	if tick <= 0 {
		tick = 1 * time.Second
	}
	db.scheduler = scheduler.NewWithInterval(workers, db.options.CoreStorage.Logger, tick)

	// Register auto-vacuum job
	if db.options.Maintenance.EnableAutoVacuum {
		interval := db.options.Maintenance.AutoVacuumInterval
		if interval <= 0 {
			interval = 1 * time.Minute
		}
		threshold := db.options.Maintenance.AutoVacuumThreshold
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
			db.options.CoreStorage.Logger.Warnf("Failed to register auto-vacuum job: %v", err)
		}
	}

	// Register analyze job
	analyzeInterval := db.options.Scheduler.AnalyzeInterval
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
		db.options.CoreStorage.Logger.Warnf("Failed to register auto-analyze job: %v", err)
	}

	// Register checkpoint job
	if db.options.Maintenance.EnableAutoCheckpoint {
		checkpointInterval := db.options.Maintenance.CheckpointInterval
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
			db.options.CoreStorage.Logger.Warnf("Failed to register auto-checkpoint job: %v", err)
		}
	}

	db.scheduler.Start()
}

// runAutoVacuumJob checks all tables and vacuums those exceeding the dead-tuple threshold.

func (db *DB) runAutoVacuumJob(threshold float64) error {
	tables := db.catalog.ListTablesNeedingVacuum(threshold)
	for _, tableName := range tables {
		if err := db.catalog.VacuumTable(tableName); err != nil {
			if db.options.CoreStorage.Logger != nil {
				db.options.CoreStorage.Logger.Warnf("AutoVacuum failed for table %s: %v", tableName, err)
			}
		} else {
			if db.options.CoreStorage.Logger != nil {
				db.options.CoreStorage.Logger.Infof("AutoVacuum completed for table %s", tableName)
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
			if db.options.CoreStorage.Logger != nil {
				db.options.CoreStorage.Logger.Warnf("AutoAnalyze failed for table %s: %v", tableName, err)
			}
		} else {
			if db.options.CoreStorage.Logger != nil {
				db.options.CoreStorage.Logger.Infof("AutoAnalyze completed for table %s", tableName)
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
		if db.options.CoreStorage.Logger != nil {
			db.options.CoreStorage.Logger.Warnf("AutoCheckpoint failed: %v", err)
		}
		return err
	}
	if db.options.CoreStorage.Logger != nil {
		db.options.CoreStorage.Logger.Info("AutoCheckpoint completed")
	}
	return nil
}

// GetScheduler returns the job scheduler for advanced usage.
