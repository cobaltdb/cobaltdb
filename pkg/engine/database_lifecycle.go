package engine

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/advisor"
	"github.com/cobaltdb/cobaltdb/pkg/audit"
	"github.com/cobaltdb/cobaltdb/pkg/backup"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
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
	if normalized.InMemory {
		normalized.CoreStorage.InMemory = true
	}
	if normalized.PageSize > 0 && normalized.CoreStorage.PageSize == 0 {
		normalized.CoreStorage.PageSize = normalized.PageSize
	}
	if normalized.CacheSize > 0 && normalized.CoreStorage.CacheSize == 0 {
		normalized.CoreStorage.CacheSize = normalized.CacheSize
	}
	if normalized.WALEnabled != nil && normalized.CoreStorage.WALEnabled == nil {
		normalized.CoreStorage.WALEnabled = cloneBoolPtr(normalized.WALEnabled)
	}
	if normalized.SyncMode != 0 && normalized.CoreStorage.SyncMode == 0 {
		normalized.CoreStorage.SyncMode = normalized.SyncMode
	}
	if normalized.Logger != nil && normalized.CoreStorage.Logger == nil {
		normalized.CoreStorage.Logger = normalized.Logger
	}
	if normalized.MaxConnections > 0 && normalized.ConnectionPool.MaxConnections == 0 {
		normalized.ConnectionPool.MaxConnections = normalized.MaxConnections
	}
	if normalized.ConnectionTimeout > 0 && normalized.ConnectionPool.ConnectionTimeout == 0 {
		normalized.ConnectionPool.ConnectionTimeout = normalized.ConnectionTimeout
	}
	if normalized.QueryTimeout > 0 && normalized.ConnectionPool.QueryTimeout == 0 {
		normalized.ConnectionPool.QueryTimeout = normalized.QueryTimeout
	}
	if len(normalized.EncryptionKey) > 0 && len(normalized.Security.EncryptionKey) == 0 {
		normalized.Security.EncryptionKey = append([]byte(nil), normalized.EncryptionKey...)
	}
	if normalized.EncryptionConfig != nil && normalized.Security.EncryptionConfig == nil {
		normalized.Security.EncryptionConfig = normalized.EncryptionConfig
	}
	if normalized.EnableRLS {
		normalized.Security.EnableRLS = true
	}
	if normalized.AuditConfig != nil && normalized.Security.AuditConfig == nil {
		normalized.Security.AuditConfig = normalized.AuditConfig
	}
	if normalized.MaxStmtCacheSize > 0 && normalized.Security.MaxStmtCacheSize == 0 {
		normalized.Security.MaxStmtCacheSize = normalized.MaxStmtCacheSize
	}
	if normalized.StrictSQLParsing {
		normalized.Security.StrictSQLParsing = true
	}
	if normalized.EnableQueryCache {
		normalized.QueryCache.EnableQueryCache = true
	}
	if normalized.QueryCacheSize > 0 && normalized.QueryCache.QueryCacheSize == 0 {
		normalized.QueryCache.QueryCacheSize = normalized.QueryCacheSize
	}
	if normalized.QueryCacheTTL > 0 && normalized.QueryCache.QueryCacheTTL == 0 {
		normalized.QueryCache.QueryCacheTTL = normalized.QueryCacheTTL
	}
	if normalized.ReplicationRole != "" && normalized.Replication.Role == "" {
		normalized.Replication.Role = normalized.ReplicationRole
	}
	if normalized.ReplicationListenAddr != "" && normalized.Replication.ListenAddr == "" {
		normalized.Replication.ListenAddr = normalized.ReplicationListenAddr
	}
	if normalized.ReplicationMasterAddr != "" && normalized.Replication.MasterAddr == "" {
		normalized.Replication.MasterAddr = normalized.ReplicationMasterAddr
	}
	if normalized.ReplicationMode != "" && normalized.Replication.Mode == "" {
		normalized.Replication.Mode = normalized.ReplicationMode
	}
	if normalized.ReplicationAuthToken != "" && normalized.Replication.AuthToken == "" {
		normalized.Replication.AuthToken = normalized.ReplicationAuthToken
	}
	if normalized.ReplicationSSLCert != "" && normalized.Replication.SSLCert == "" {
		normalized.Replication.SSLCert = normalized.ReplicationSSLCert
	}
	if normalized.ReplicationSSLKey != "" && normalized.Replication.SSLKey == "" {
		normalized.Replication.SSLKey = normalized.ReplicationSSLKey
	}
	if normalized.ReplicationSSLCA != "" && normalized.Replication.SSLCA == "" {
		normalized.Replication.SSLCA = normalized.ReplicationSSLCA
	}
	if normalized.ReplicationStateFile != "" && normalized.Replication.StateFile == "" {
		normalized.Replication.StateFile = normalized.ReplicationStateFile
	}
	if normalized.BackupDir != "" && normalized.Backup.Dir == "" {
		normalized.Backup.Dir = normalized.BackupDir
	}
	if normalized.BackupRetention > 0 && normalized.Backup.Retention == 0 {
		normalized.Backup.Retention = normalized.BackupRetention
	}
	if normalized.MaxBackups > 0 && normalized.Backup.MaxBackups == 0 {
		normalized.Backup.MaxBackups = normalized.MaxBackups
	}
	if normalized.BackupCompressionLevel > 0 && normalized.Backup.CompressionLevel == 0 {
		normalized.Backup.CompressionLevel = normalized.BackupCompressionLevel
	}
	if normalized.EnableSlowQueryLog {
		normalized.SlowQueryLog.EnableSlowQueryLog = true
	}
	if normalized.SlowQueryThreshold > 0 && normalized.SlowQueryLog.Threshold == 0 {
		normalized.SlowQueryLog.Threshold = normalized.SlowQueryThreshold
	}
	if normalized.SlowQueryMaxEntries > 0 && normalized.SlowQueryLog.MaxEntries == 0 {
		normalized.SlowQueryLog.MaxEntries = normalized.SlowQueryMaxEntries
	}
	if normalized.SlowQueryLogFile != "" && normalized.SlowQueryLog.LogFile == "" {
		normalized.SlowQueryLog.LogFile = normalized.SlowQueryLogFile
	}
	if normalized.EnablePlanCache {
		normalized.PlanCache.EnablePlanCache = true
	}
	if normalized.PlanCacheSize > 0 && normalized.PlanCache.Size == 0 {
		normalized.PlanCache.Size = normalized.PlanCacheSize
	}
	if normalized.PlanCacheEntries > 0 && normalized.PlanCache.MaxEntries == 0 {
		normalized.PlanCache.MaxEntries = normalized.PlanCacheEntries
	}
	if normalized.EnableAutoVacuum {
		normalized.Maintenance.EnableAutoVacuum = true
	}
	if normalized.EnableScheduler {
		normalized.Scheduler.EnableScheduler = true
	}
	if normalized.CompressionConfig != nil && normalized.PageCompression.Config == nil {
		normalized.PageCompression.Config = normalized.CompressionConfig
	}
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
	if !normalized.Scheduler.EnableScheduler && normalized.Scheduler.TickInterval > 0 {
		normalized.Scheduler.EnableScheduler = true
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
	if err := validateOptions(opts); err != nil {
		return nil, err
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
		if err := prepareDatabaseParentDir(path); err != nil {
			return nil, err
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
				err = errors.Join(err, backend.Close())
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
				err = errors.Join(err, backend.Close())
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
				err = errors.Join(err, backend.Close())
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
			err = errors.Join(err, backend.Close())
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
			err = errors.Join(err, db.auditLogger.Close())
		}
		if db.wal != nil {
			err = errors.Join(err, db.wal.Close())
		}
		err = errors.Join(err, backend.Close())
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

func validateOptions(opts *Options) error {
	const maxEngineConnections = 100000

	if opts.CoreStorage.CacheSize <= 0 {
		return fmt.Errorf("cache size must be positive: %d", opts.CoreStorage.CacheSize)
	}
	if opts.CoreStorage.PageSize != storage.PageSize {
		return fmt.Errorf("page size %d is unsupported; expected %d", opts.CoreStorage.PageSize, storage.PageSize)
	}
	if opts.ConnectionPool.MaxConnections < 0 {
		return fmt.Errorf("max connections must be non-negative: %d", opts.ConnectionPool.MaxConnections)
	}
	if opts.ConnectionPool.MaxConnections > maxEngineConnections {
		return fmt.Errorf("max connections exceeds maximum (%d): %d", maxEngineConnections, opts.ConnectionPool.MaxConnections)
	}
	if opts.ConnectionPool.ConnectionTimeout < 0 {
		return fmt.Errorf("connection timeout must be non-negative: %s", opts.ConnectionPool.ConnectionTimeout)
	}
	if opts.ConnectionPool.QueryTimeout < 0 {
		return fmt.Errorf("query timeout must be non-negative: %s", opts.ConnectionPool.QueryTimeout)
	}
	if opts.Security.MaxStmtCacheSize < 0 {
		return fmt.Errorf("max statement cache size must be non-negative: %d", opts.Security.MaxStmtCacheSize)
	}
	if opts.QueryCache.QueryCacheSize < 0 {
		return fmt.Errorf("query cache size must be non-negative: %d", opts.QueryCache.QueryCacheSize)
	}
	if opts.QueryCache.QueryCacheTTL < 0 {
		return fmt.Errorf("query cache TTL must be non-negative: %s", opts.QueryCache.QueryCacheTTL)
	}
	if opts.Backup.Retention < 0 {
		return fmt.Errorf("backup retention must be non-negative: %s", opts.Backup.Retention)
	}
	if opts.Backup.MaxBackups < 0 {
		return fmt.Errorf("max backups must be non-negative: %d", opts.Backup.MaxBackups)
	}
	if opts.Backup.CompressionLevel < 0 || opts.Backup.CompressionLevel > gzip.BestCompression {
		return fmt.Errorf("backup compression level must be between 0 and %d: %d", gzip.BestCompression, opts.Backup.CompressionLevel)
	}
	if opts.SlowQueryLog.Threshold < 0 {
		return fmt.Errorf("slow query threshold must be non-negative: %s", opts.SlowQueryLog.Threshold)
	}
	if opts.SlowQueryLog.MaxEntries < 0 {
		return fmt.Errorf("slow query max entries must be non-negative: %d", opts.SlowQueryLog.MaxEntries)
	}
	if opts.PlanCache.Size < 0 {
		return fmt.Errorf("plan cache size must be non-negative: %d", opts.PlanCache.Size)
	}
	if opts.PlanCache.MaxEntries < 0 {
		return fmt.Errorf("plan cache max entries must be non-negative: %d", opts.PlanCache.MaxEntries)
	}
	if opts.Maintenance.AutoVacuumInterval < 0 {
		return fmt.Errorf("auto-vacuum interval must be non-negative: %s", opts.Maintenance.AutoVacuumInterval)
	}
	if opts.Maintenance.AutoVacuumThreshold < 0 || opts.Maintenance.AutoVacuumThreshold > 1 {
		return fmt.Errorf("auto-vacuum threshold must be between 0 and 1: %v", opts.Maintenance.AutoVacuumThreshold)
	}
	if opts.Maintenance.CheckpointInterval < 0 {
		return fmt.Errorf("checkpoint interval must be non-negative: %s", opts.Maintenance.CheckpointInterval)
	}
	if opts.Scheduler.AnalyzeInterval < 0 {
		return fmt.Errorf("scheduler analyze interval must be non-negative: %s", opts.Scheduler.AnalyzeInterval)
	}
	if opts.Scheduler.Workers < 0 {
		return fmt.Errorf("scheduler workers must be non-negative: %d", opts.Scheduler.Workers)
	}
	if opts.Scheduler.TickInterval < 0 {
		return fmt.Errorf("scheduler tick interval must be non-negative: %s", opts.Scheduler.TickInterval)
	}
	if opts.ParallelQuery.Workers < 0 {
		return fmt.Errorf("parallel query workers must be non-negative: %d", opts.ParallelQuery.Workers)
	}
	if opts.ParallelQuery.Threshold < 0 {
		return fmt.Errorf("parallel query threshold must be non-negative: %d", opts.ParallelQuery.Threshold)
	}
	return nil
}

func prepareDatabaseParentDir(path string) error {
	dir := filepath.Dir(filepath.Clean(path))
	if dir == "." || dir == "/" {
		return nil
	}
	if err := rejectDatabaseDirSymlinkPathComponents(dir); err != nil {
		return err
	}

	info, statErr := os.Lstat(dir)
	preexisting := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return fmt.Errorf("failed to stat database directory: %w", statErr)
	}
	if preexisting {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("database directory must not be a symlink: %s", dir)
		}
		if !info.IsDir() {
			return fmt.Errorf("database path parent must be a directory: %s", dir)
		}
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	if err := os.Chmod(dir, 0750); err != nil {
		return fmt.Errorf("failed to set database directory permissions: %w", err)
	}

	openedInfo, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("failed to stat database directory after create: %w", err)
	}
	if !openedInfo.IsDir() {
		return fmt.Errorf("database path parent must be a directory: %s", dir)
	}
	if preexisting && !os.SameFile(info, openedInfo) {
		return fmt.Errorf("database directory changed while opening: %s", dir)
	}

	return nil
}

func rejectDatabaseDirSymlinkPathComponents(path string) error {
	path = filepath.Clean(path)
	if path == "." || path == string(os.PathSeparator) {
		return nil
	}

	current := "."
	if filepath.IsAbs(path) {
		current = string(os.PathSeparator)
		path = strings.TrimPrefix(path, string(os.PathSeparator))
	}
	for _, part := range strings.Split(path, string(os.PathSeparator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to stat database directory component: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("database directory component must not be a symlink: %s", current)
		}
	}
	return nil
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
	if _, err := storage.WriteFullAt(db.backend, metaPage.Data, 0); err != nil {
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
	db.expandMetaPageCount(meta)
	meta.Serialize(metaPage.Data)
	if _, err := storage.WriteFullAt(db.backend, metaPage.Data, 0); err != nil {
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
		db.catalog.EnableQueryCacheWithLimits(db.options.QueryCache.QueryCacheSize, 0, db.options.QueryCache.QueryCacheTTL)
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
	if _, err := storage.ReadFullAt(db.backend, metaPage.Data, 0); err != nil {
		return fmt.Errorf("failed to read meta page: %w", err)
	}
	var meta storage.MetaPage
	if err := meta.Deserialize(metaPage.Data); err != nil {
		return fmt.Errorf("failed to deserialize meta page: %w", err)
	}
	// Update root page ID
	meta.RootPageID = db.rootTree.RootPageID()
	db.expandMetaPageCount(&meta)
	meta.Serialize(metaPage.Data)
	if _, err := storage.WriteFullAt(db.backend, metaPage.Data, 0); err != nil {
		return fmt.Errorf("failed to write meta page: %w", err)
	}
	return nil
}

func (db *DB) expandMetaPageCount(meta *storage.MetaPage) {
	if meta == nil {
		return
	}
	if db.pool != nil {
		if allocated := db.pool.AllocatedPageCount(); allocated > meta.PageCount {
			meta.PageCount = allocated
		}
	}
	if db.rootTree != nil {
		if rootPageCount := db.rootTree.RootPageID() + 1; rootPageCount > meta.PageCount {
			meta.PageCount = rootPageCount
		}
	}
}

// loadExisting loads an existing database

func (db *DB) loadExisting() error {
	// Read meta page
	metaPage := storage.NewPage(0, storage.PageTypeMeta)
	if _, err := storage.ReadFullAt(db.backend, metaPage.Data, 0); err != nil {
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
		db.catalog.EnableQueryCacheWithLimits(db.options.QueryCache.QueryCacheSize, 0, db.options.QueryCache.QueryCacheTTL)
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
		backupConfig.BackupDir = defaultBackupDirForDatabase(db.path)
	}
	db.backupMgr = backup.NewManager(backupConfig, db)
}

func defaultBackupDirForDatabase(path string) string {
	if strings.TrimSpace(path) == "" || path == ":memory:" {
		return "./backups"
	}
	return filepath.Clean(path) + ".backups"
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
