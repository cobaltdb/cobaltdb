package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/audit"
	"github.com/cobaltdb/cobaltdb/pkg/backup"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/cache"
	"github.com/cobaltdb/cobaltdb/pkg/catalog"
	"github.com/cobaltdb/cobaltdb/pkg/logger"
	"github.com/cobaltdb/cobaltdb/pkg/metrics"
	"github.com/cobaltdb/cobaltdb/pkg/optimizer"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/replication"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

var (
	ErrDatabaseClosed = errors.New("database is closed")
	ErrInvalidPath    = errors.New("invalid database path")
)

// DB represents a CobaltDB database instance
type DB struct {
	path     string
	backend  storage.Backend
	pool     *storage.BufferPool
	wal      *storage.WAL
	catalog  *catalog.Catalog
	txnMgr   *txn.Manager
	rootTree *btree.BTree
	mu       sync.RWMutex
	closed   bool
	options  *Options
	// Security components
	auditLogger *audit.Logger     // Audit logger
	rlsManager  *security.Manager // Row-level security manager
	// Prepared statement cache for performance (LRU via doubly-linked list)
	stmtCache map[string]*cachedStmt
	stmtMu    sync.RWMutex
	stmtLRU   *stmtLRUList  // O(1) eviction
	nextTxnID atomic.Uint64 // Auto-increment transaction ID counter
	// Metrics collector
	metrics *metrics.Collector
	// Connection management
	connSem      chan struct{} // Connection limit semaphore
	activeConns  atomic.Int64  // Active connection count
	shutdownCh   chan struct{} // Shutdown signal
	shutdownOnce sync.Once

	// Query Cache
	queryCache *cache.Cache

	// Query Optimizer
	optimizer *optimizer.Optimizer

	// Replication Manager
	replicationMgr *replication.Manager

	// Backup Manager
	backupMgr *backup.Manager

	// Slow Query Log
	slowQueryLog *metrics.SlowQueryLog

	// Query Plan Cache - caches parsed query statements
	planCache *QueryPlanCache
}

// Options contains database configuration options
type Options struct {
	PageSize          int
	CacheSize         int // number of pages
	InMemory          bool
	WALEnabled        bool
	SyncMode          SyncMode
	Logger            *logger.Logger            // Optional logger; if nil, uses default
	MaxConnections    int                       // Maximum concurrent connections (0 = unlimited)
	ConnectionTimeout time.Duration             // Timeout for acquiring a connection
	QueryTimeout      time.Duration             // Default query timeout (0 = no timeout)
	EncryptionKey     []byte                    // Encryption key for data at rest (nil = no encryption)
	EncryptionConfig  *storage.EncryptionConfig // Detailed encryption configuration
	AuditConfig       *audit.Config             // Audit logging configuration (nil = disabled)
	EnableRLS         bool                      // Enable Row-Level Security by default
	MaxStmtCacheSize  int                       // Maximum cached prepared statements (default: 1000)

	// Query Cache Options
	EnableQueryCache bool          // Enable query result caching
	QueryCacheSize   int64         // Max query cache size in bytes (default: 64MB)
	QueryCacheTTL    time.Duration // Query cache TTL (default: 5m)

	// Replication Options
	ReplicationRole           string        // "master", "slave", or "" (disabled)
	ReplicationListenAddr     string        // Master listen address for slaves
	ReplicationMasterAddr     string        // Slave: master address to connect
	ReplicationMode           string        // "async", "sync", "full_sync"
	ReplicationAuthToken      string        // Authentication token for replication
	ReplicationSSLCert        string        // SSL certificate file path
	ReplicationSSLKey         string        // SSL key file path
	ReplicationSSLCA          string        // SSL CA certificate path

	// Backup Options
	BackupDir               string        // Backup directory path
	BackupRetention         time.Duration // Backup retention period
	MaxBackups              int           // Maximum number of backups to keep
	BackupCompressionLevel  int           // Compression level (0-9, 0=disabled)

	// Slow Query Log Options
	EnableSlowQueryLog   bool          // Enable slow query logging
	SlowQueryThreshold   time.Duration // Threshold for slow queries (default: 1s)
	SlowQueryMaxEntries  int           // Max in-memory entries (default: 1000)
	SlowQueryLogFile     string        // Log file path (empty = memory only)

	// Query Plan Cache Options
	EnablePlanCache  bool  // Enable query plan caching
	PlanCacheSize    int64 // Max plan cache size in bytes (default: 32MB)
	PlanCacheEntries int   // Max number of cached plans (default: 1000)

}

// SyncMode controls when data is synced to disk
type SyncMode int

// cachedStmt represents a cached prepared statement with metadata
type cachedStmt struct {
	stmt     query.Statement
	lastUsed int64 // Unix timestamp for LRU
	useCount uint64
	sql      string        // key for reverse lookup
	elem     *stmtLRUEntry // pointer to LRU list element
}

// stmtLRUEntry is a node in the doubly-linked LRU list
type stmtLRUEntry struct {
	sql  string
	prev *stmtLRUEntry
	next *stmtLRUEntry
}

// stmtLRUList is a simple doubly-linked list for O(1) LRU eviction
type stmtLRUList struct {
	head *stmtLRUEntry // most recently used
	tail *stmtLRUEntry // least recently used
}

func newStmtLRUList() *stmtLRUList {
	return &stmtLRUList{}
}

func (l *stmtLRUList) pushFront(e *stmtLRUEntry) {
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	}
	l.head = e
	if l.tail == nil {
		l.tail = e
	}
}

func (l *stmtLRUList) moveToFront(e *stmtLRUEntry) {
	if l.head == e {
		return
	}
	l.remove(e)
	l.pushFront(e)
}

func (l *stmtLRUList) remove(e *stmtLRUEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (l *stmtLRUList) removeTail() *stmtLRUEntry {
	if l.tail == nil {
		return nil
	}
	e := l.tail
	l.remove(e)
	return e
}

const (
	SyncOff SyncMode = iota
	SyncNormal
	SyncFull
)

// DefaultOptions returns the default database options
func DefaultOptions() *Options {
	return &Options{
		PageSize:          storage.PageSize,
		CacheSize:         1024, // 4MB cache
		InMemory:          false,
		WALEnabled:        true,
		SyncMode:          SyncNormal,
		Logger:            logger.Default(),
		MaxConnections:    100, // Default max connections
		ConnectionTimeout: 30 * time.Second,
		QueryTimeout:      60 * time.Second,
		MaxStmtCacheSize:  1000, // Default max cached statements
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
		// InMemory and WALEnabled are booleans, use defaults if not explicitly set
		// SyncMode defaults to 0 which is SyncOff, but default is SyncNormal
		// We can't distinguish between unset and explicitly set to 0 for booleans and enums
		// So we use the default values if they appear to be zero values
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
			backend, err = storage.NewEncryptedBackend(backend, opts.EncryptionConfig)
			if err != nil {
				backend.Close()
				return nil, fmt.Errorf("failed to setup encryption: %w", err)
			}
		} else if len(opts.EncryptionKey) > 0 {
			log.Infof("Enabling encryption at rest")
			encConfig := &storage.EncryptionConfig{
				Enabled:   true,
				Key:       opts.EncryptionKey,
				Algorithm: "aes-256-gcm",
				UseArgon2: true,
			}
			backend, err = storage.NewEncryptedBackend(backend, encConfig)
			if err != nil {
				backend.Close()
				return nil, fmt.Errorf("failed to setup encryption: %w", err)
			}
		}
	}

	// Initialize metrics collector
	collector := metrics.NewCollector(0) // Use default interval

	db := &DB{
		path:       path,
		backend:    backend,
		options:    opts,
		stmtCache:  make(map[string]*cachedStmt),
		stmtLRU:    newStmtLRUList(),
		metrics:    collector,
		shutdownCh: make(chan struct{}),
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

	// Initialize catalog
	db.catalog = catalog.New(db.rootTree, db.pool, db.wal)

	// Enable RLS if configured
	if db.options.EnableRLS {
		db.catalog.EnableRLS()
	}

	// Initialize transaction manager
	db.txnMgr = txn.NewManager(db.pool, db.wal)

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
		}
		db.replicationMgr = replication.NewManager(replConfig)
		if err := db.replicationMgr.Start(); err != nil {
			return fmt.Errorf("failed to start replication manager: %w", err)
		}
	}

	// Initialize backup manager
	backupConfig := &backup.Config{
		BackupDir:        db.options.BackupDir,
		RetentionPeriod:  db.options.BackupRetention,
		MaxBackups:       db.options.MaxBackups,
		CompressionLevel: db.options.BackupCompressionLevel,
	}
	if backupConfig.BackupDir == "" {
		backupConfig.BackupDir = "./backups"
	}
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
	}

	db.backupMgr = backup.NewManager(backupConfig, db)

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
	if db.options.WALEnabled && db.path != ":memory:" {
		walPath := db.path + ".wal"
		wal, err := storage.OpenWAL(walPath)
		if err != nil {
			return fmt.Errorf("failed to open WAL: %w", err)
		}
		db.wal = wal
		db.pool.SetWAL(wal)

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

	// Enable RLS if configured
	if db.options.EnableRLS {
		db.catalog.EnableRLS()
	}

	// Load catalog metadata from the B+Tree
	if err := db.catalog.Load(); err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// Initialize transaction manager
	db.txnMgr = txn.NewManager(db.pool, db.wal)

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
		}
		db.replicationMgr = replication.NewManager(replConfig)
		if err := db.replicationMgr.Start(); err != nil {
			return fmt.Errorf("failed to start replication manager: %w", err)
		}
	}

	// Initialize backup manager
	backupConfig := &backup.Config{
		BackupDir:        db.options.BackupDir,
		RetentionPeriod:  db.options.BackupRetention,
		MaxBackups:       db.options.MaxBackups,
		CompressionLevel: db.options.BackupCompressionLevel,
	}
	if backupConfig.BackupDir == "" {
		backupConfig.BackupDir = "./backups"
	}
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
	}

	db.backupMgr = backup.NewManager(backupConfig, db)

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

	if db.closed {
		return nil
	}

	db.closed = true

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

	var errs []error

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

// getPreparedStatement returns a cached prepared statement or parses and caches it
func (db *DB) getPreparedStatement(sql string, args ...interface{}) (query.Statement, error) {
	// First check plan cache if enabled (more sophisticated caching with size limits)
	if db.planCache != nil {
		if entry, found := db.planCache.Get(sql, args); found {
			return entry.ParsedStmt, nil
		}
	}

	db.stmtMu.RLock()
	cached, exists := db.stmtCache[sql]
	db.stmtMu.RUnlock()

	if exists {
		// Move to front of LRU (most recently used)
		db.stmtMu.Lock()
		if c, ok := db.stmtCache[sql]; ok {
			c.lastUsed = time.Now().Unix()
			c.useCount++
			db.stmtLRU.moveToFront(c.elem)
		}
		db.stmtMu.Unlock()
		return cached.stmt, nil
	}

	// Parse and cache
	parsedStmt, err := query.Parse(sql)
	if err != nil {
		return nil, err
	}

	// Cache in plan cache if enabled
	if db.planCache != nil {
		db.planCache.Put(sql, args, parsedStmt)
	}

	// Cache the statement with O(1) LRU eviction
	db.stmtMu.Lock()
	maxCacheSize := db.options.MaxStmtCacheSize
	if maxCacheSize <= 0 {
		maxCacheSize = 1000
	}
	if len(db.stmtCache) >= maxCacheSize {
		db.evictLRUEntry()
	}
	entry := &stmtLRUEntry{sql: sql}
	cs := &cachedStmt{
		stmt:     parsedStmt,
		lastUsed: time.Now().Unix(),
		useCount: 1,
		sql:      sql,
		elem:     entry,
	}
	db.stmtCache[sql] = cs
	db.stmtLRU.pushFront(entry)
	db.stmtMu.Unlock()

	return parsedStmt, nil
}

// evictLRUEntry removes the least recently used entry from the cache
// Must be called with stmtMu.Lock() held
func (db *DB) evictLRUEntry() {
	tail := db.stmtLRU.removeTail()
	if tail != nil {
		delete(db.stmtCache, tail.sql)
	}
}

// acquireConnection acquires a connection slot with timeout
func (db *DB) acquireConnection(ctx context.Context) error {
	if db.connSem == nil {
		// No connection limit
		db.activeConns.Add(1)
		if db.metrics != nil {
			db.metrics.ConnectionAcquired()
		}
		return nil
	}

	timeout := db.options.ConnectionTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	// Create timeout context if not provided
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	select {
	case db.connSem <- struct{}{}:
		db.activeConns.Add(1)
		if db.metrics != nil {
			db.metrics.ConnectionAcquired()
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("connection timeout: %w", ctx.Err())
	case <-db.shutdownCh:
		return ErrDatabaseClosed
	}
}

// releaseConnection releases a connection slot
func (db *DB) releaseConnection() {
	if db.connSem != nil {
		select {
		case <-db.connSem:
		default:
			// Should not happen, but handle gracefully
		}
	}
	db.activeConns.Add(-1)
	if db.metrics != nil {
		db.metrics.ConnectionReleased()
	}
}

// Exec executes a SQL statement without returning rows
func (db *DB) Exec(ctx context.Context, sql string, args ...interface{}) (result Result, err error) {
	// Panic recovery for production safety - always log full stack trace
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			err = fmt.Errorf("internal error in Exec: %v", r)
			if db.options.Logger != nil {
				db.options.Logger.Errorf("PANIC in Exec: %v\n%s", r, stack)
			} else {
				fmt.Printf("PANIC in Exec: %v\n%s\n", r, stack)
			}
		}
	}()

	// Apply default query timeout if none set
	if db.options.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, db.options.QueryTimeout)
		defer cancel()
	}

	// Acquire connection
	if err := db.acquireConnection(ctx); err != nil {
		return Result{}, err
	}
	defer db.releaseConnection()

	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return Result{}, ErrDatabaseClosed
	}

	// Record metrics
	start := time.Now()
	if db.metrics != nil {
		defer func() {
			duration := time.Since(start)
			db.metrics.RecordQuery(duration, duration > 100*time.Millisecond)
		}()
	}

	// Slow query logging
	if db.slowQueryLog != nil {
		defer func() {
			duration := time.Since(start)
			db.slowQueryLog.Log(sql, duration, result.RowsAffected, 0)
		}()
	}

	// Try to use cached prepared statement
	stmt, err := db.getPreparedStatement(sql, args...)
	if err != nil {
		if db.metrics != nil {
			db.metrics.RecordError()
		}
		return Result{}, fmt.Errorf("parse error: %w", err)
	}

	// Execute statement
	return db.execute(ctx, stmt, args)
}

// Query executes a SQL query and returns rows
func (db *DB) Query(ctx context.Context, sql string, args ...interface{}) (rows *Rows, err error) {
	// Panic recovery for production safety - always log full stack trace
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			err = fmt.Errorf("internal error in Query: %v", r)
			if db.options.Logger != nil {
				db.options.Logger.Errorf("PANIC in Query: %v\n%s", r, stack)
			} else {
				fmt.Printf("PANIC in Query: %v\n%s\n", r, stack)
			}
		}
	}()
	// Apply default query timeout if none set
	if db.options.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, db.options.QueryTimeout)
		defer cancel()
	}

	// Acquire connection
	if err := db.acquireConnection(ctx); err != nil {
		return nil, err
	}
	defer db.releaseConnection()

	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDatabaseClosed
	}

	// Record metrics
	start := time.Now()
	if db.metrics != nil {
		defer func() {
			duration := time.Since(start)
			db.metrics.RecordQuery(duration, duration > 100*time.Millisecond)
		}()
	}

	// Slow query logging
	if db.slowQueryLog != nil {
		defer func() {
			duration := time.Since(start)
			db.slowQueryLog.Log(sql, duration, 0, 0)
		}()
	}

	// Try to use cached prepared statement
	stmt, err := db.getPreparedStatement(sql, args...)
	if err != nil {
		if db.metrics != nil {
			db.metrics.RecordError()
		}
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// Execute query
	return db.query(ctx, stmt, args)
}

// QueryRow executes a SQL query and returns a single row
func (db *DB) QueryRow(ctx context.Context, sql string, args ...interface{}) *Row {
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return &Row{err: err}
	}

	if !rows.Next() {
		rows.Close()
		return &Row{err: errors.New("no rows in result set")}
	}

	return &Row{rows: rows}
}

// Tables returns a list of all table names in the database
func (db *DB) Tables() []string {
	return db.catalog.ListTables()
}

// Path returns the database file path
func (db *DB) Path() string {
	return db.path
}

// TableSchema returns a human-readable schema for a table
func (db *DB) TableSchema(name string) (string, error) {
	table, err := db.catalog.GetTable(name)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", table.Name))
	for i, col := range table.Columns {
		sb.WriteString(fmt.Sprintf("  %s %s", col.Name, col.Type))
		if col.PrimaryKey {
			sb.WriteString(" PRIMARY KEY")
		}
		if col.AutoIncrement {
			sb.WriteString(" AUTOINCREMENT")
		}
		if col.NotNull {
			sb.WriteString(" NOT NULL")
		}
		if col.Unique {
			sb.WriteString(" UNIQUE")
		}
		if col.Default != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", col.Default))
		}
		if i < len(table.Columns)-1 {
			sb.WriteByte(',')
		}
		sb.WriteByte('\n')
	}
	sb.WriteString(");")
	return sb.String(), nil
}

// Begin starts a new transaction
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	return db.BeginWith(ctx, txn.DefaultOptions())
}

// BeginWith starts a new transaction with options
func (db *DB) BeginWith(ctx context.Context, opts *txn.Options) (*Tx, error) {
	// Acquire connection
	if err := db.acquireConnection(ctx); err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		db.releaseConnection()
		return nil, ErrDatabaseClosed
	}

	transaction := db.txnMgr.Begin(opts)

	// Begin transaction in catalog for WAL logging
	db.catalog.BeginTransaction(transaction.ID)

	return &Tx{
		db:  db,
		txn: transaction,
	}, nil
}

// auditUser extracts the username from context for audit logging.
func auditUser(ctx context.Context) string {
	if ctx != nil {
		if user, ok := ctx.Value("cobaltdb_user").(string); ok && user != "" {
			return user
		}
	}
	return "db_user"
}

// execute executes a statement
func (db *DB) execute(ctx context.Context, stmt query.Statement, args []interface{}) (result Result, err error) {
	start := time.Now()

	// Check for context cancellation
	if ctx != nil {
		select {
		case <-ctx.Done():
			return Result{}, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}
	}

	// Handle autocommit mode for write operations when WAL is enabled
	// Skip autocommit for transaction control statements (BEGIN/COMMIT/ROLLBACK)
	isTransactionControl := false
	switch stmt.(type) {
	case *query.BeginStmt, *query.CommitStmt, *query.RollbackStmt,
		*query.SavepointStmt, *query.ReleaseSavepointStmt:
		isTransactionControl = true
	}
	autocommit := db.wal != nil && !db.catalog.IsTransactionActive() && !isTransactionControl

	if autocommit {
		// Start a transaction for this operation
		db.catalog.BeginTransaction(db.nextTxnID.Add(1))
		defer func() {
			if err != nil {
				if rbErr := db.catalog.RollbackTransaction(); rbErr != nil {
					err = fmt.Errorf("%w; rollback failed: %v", err, rbErr)
				}
			} else {
				if cmtErr := db.catalog.CommitTransaction(); cmtErr != nil {
					err = fmt.Errorf("commit failed: %w", cmtErr)
				}
			}
		}()
	}

	switch s := stmt.(type) {
	case *query.CreateTableStmt:
		result, err := db.executeCreateTable(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CREATE_TABLE", audit.WithTable(s.Table))
		}
		if err == nil {
			db.replicateWrite("CREATE_TABLE", s.Table, nil)
		}
		return result, err
	case *query.InsertStmt:
		result, err := db.executeInsert(ctx, s, args)
		if db.auditLogger != nil {
			db.auditLogger.LogQuery(auditUser(ctx), "INSERT", time.Since(start), result.RowsAffected, err)
		}
		if err == nil {
			db.replicateWrite("INSERT", s.Table, args)
		}
		return result, err
	case *query.UpdateStmt:
		result, err := db.executeUpdate(ctx, s, args)
		if db.auditLogger != nil {
			db.auditLogger.LogQuery(auditUser(ctx), "UPDATE", time.Since(start), result.RowsAffected, err)
		}
		if err == nil {
			db.replicateWrite("UPDATE", s.Table, args)
		}
		return result, err
	case *query.DeleteStmt:
		result, err := db.executeDelete(ctx, s, args)
		if db.auditLogger != nil {
			db.auditLogger.LogQuery(auditUser(ctx), "DELETE", time.Since(start), result.RowsAffected, err)
		}
		if err == nil {
			db.replicateWrite("DELETE", s.Table, args)
		}
		return result, err
	case *query.DropTableStmt:
		result, err := db.executeDropTable(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "DROP_TABLE", audit.WithTable(s.Table))
		}
		if err == nil {
			db.replicateWrite("DROP_TABLE", s.Table, nil)
		}
		return result, err
	case *query.CreateIndexStmt:
		result, err := db.executeCreateIndex(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CREATE_INDEX", audit.WithTable(s.Table))
		}
		if err == nil {
			db.replicateWrite("CREATE_INDEX", s.Table, nil)
		}
		return result, err
	case *query.CreateViewStmt:
		result, err := db.executeCreateView(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CREATE_VIEW", audit.WithTable(s.Name))
		}
		if err == nil {
			db.replicateWrite("CREATE_VIEW", s.Name, nil)
		}
		return result, err
	case *query.DropViewStmt:
		result, err := db.executeDropView(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "DROP_VIEW", audit.WithTable(s.Name))
		}
		return result, err
	case *query.CreateTriggerStmt:
		result, err := db.executeCreateTrigger(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CREATE_TRIGGER", audit.WithTable(s.Table))
		}
		if err == nil {
			db.replicateWrite("CREATE_TRIGGER", s.Table, nil)
		}
		return result, err
	case *query.DropTriggerStmt:
		result, err := db.executeDropTrigger(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "DROP_TRIGGER")
		}
		if err == nil {
			db.replicateWrite("DROP_TRIGGER", "", nil)
		}
		return result, err
	case *query.CreateProcedureStmt:
		result, err := db.executeCreateProcedure(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CREATE_PROCEDURE")
		}
		if err == nil {
			db.replicateWrite("CREATE_PROCEDURE", "", nil)
		}
		return result, err
	case *query.DropProcedureStmt:
		result, err := db.executeDropProcedure(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "DROP_PROCEDURE")
		}
		if err == nil {
			db.replicateWrite("DROP_PROCEDURE", "", nil)
		}
		return result, err
	case *query.CreatePolicyStmt:
		result, err := db.executeCreatePolicy(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CREATE_POLICY", audit.WithTable(s.Table))
		}
		if err == nil {
			db.replicateWrite("CREATE_POLICY", s.Table, nil)
		}
		return result, err
	case *query.DropPolicyStmt:
		result, err := db.executeDropPolicy(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "DROP_POLICY")
		}
		if err == nil {
			db.replicateWrite("DROP_POLICY", "", nil)
		}
		return result, err
	case *query.CallProcedureStmt:
		result, err := db.executeCallProcedure(ctx, s, args)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CALL_PROCEDURE")
		}
		if err == nil {
			db.replicateWrite("CALL_PROCEDURE", "", args)
		}
		return result, err
	case *query.BeginStmt:
		if db.catalog.IsTransactionActive() {
			return Result{}, errors.New("transaction already in progress")
		}
		transaction := db.txnMgr.Begin(txn.DefaultOptions())
		if transaction == nil {
			return Result{}, errors.New("failed to begin transaction")
		}
		db.catalog.BeginTransaction(transaction.ID)
		return Result{}, nil
	case *query.CommitStmt:
		if !db.catalog.IsTransactionActive() {
			return Result{}, errors.New("no transaction in progress")
		}
		if err := db.catalog.FlushTableTrees(); err != nil {
			return Result{}, fmt.Errorf("failed to flush tables: %w", err)
		}
		if err := db.catalog.CommitTransaction(); err != nil {
			return Result{}, err
		}
		return Result{}, nil
	case *query.RollbackStmt:
		if !db.catalog.IsTransactionActive() {
			return Result{}, errors.New("no transaction in progress")
		}
		if s.ToSavepoint != "" {
			// ROLLBACK TO SAVEPOINT
			if err := db.catalog.RollbackToSavepoint(s.ToSavepoint); err != nil {
				return Result{}, err
			}
			return Result{}, nil
		}
		if err := db.catalog.RollbackTransaction(); err != nil {
			return Result{}, err
		}
		return Result{}, nil
	case *query.SavepointStmt:
		if !db.catalog.IsTransactionActive() {
			return Result{}, errors.New("SAVEPOINT can only be used within a transaction")
		}
		if err := db.catalog.Savepoint(s.Name); err != nil {
			return Result{}, err
		}
		return Result{}, nil
	case *query.ReleaseSavepointStmt:
		if !db.catalog.IsTransactionActive() {
			return Result{}, errors.New("RELEASE SAVEPOINT can only be used within a transaction")
		}
		if err := db.catalog.ReleaseSavepoint(s.Name); err != nil {
			return Result{}, err
		}
		return Result{}, nil
	case *query.VacuumStmt:
		result, err := db.executeVacuum(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventAdmin, auditUser(ctx), "VACUUM")
		}
		return result, err
	case *query.AnalyzeStmt:
		result, err := db.executeAnalyze(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventAdmin, auditUser(ctx), "ANALYZE")
		}
		return result, err
	case *query.CreateMaterializedViewStmt:
		result, err := db.executeCreateMaterializedView(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CREATE_MATERIALIZED_VIEW")
		}
		return result, err
	case *query.DropMaterializedViewStmt:
		result, err := db.executeDropMaterializedView(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "DROP_MATERIALIZED_VIEW")
		}
		return result, err
	case *query.RefreshMaterializedViewStmt:
		result, err := db.executeRefreshMaterializedView(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "REFRESH_MATERIALIZED_VIEW")
		}
		return result, err
	case *query.CreateFTSIndexStmt:
		result, err := db.executeCreateFTSIndex(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CREATE_FTS_INDEX")
		}
		return result, err
	case *query.CreateVectorIndexStmt:
		result, err := db.executeCreateVectorIndex(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CREATE_VECTOR_INDEX")
		}
		return result, err
	case *query.AlterTableStmt:
		result, err := db.executeAlterTable(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "ALTER_TABLE", audit.WithTable(s.Table))
		}
		return result, err
	case *query.SetVarStmt:
		// MySQL compatibility - accept SET commands silently
		return Result{}, nil
	case *query.UseStmt:
		// MySQL compatibility - accept USE commands silently (single-database)
		return Result{}, nil
	case *query.ShowTablesStmt, *query.ShowCreateTableStmt, *query.ShowColumnsStmt,
		*query.ShowDatabasesStmt, *query.DescribeStmt:
		// These are query-like statements that return rows — use Query() instead
		return Result{}, errors.New("use Query() instead of Exec() for SELECT/SHOW statements")
	case *query.DropIndexStmt:
		// Try FTS index first, then regular index
		if _, err := db.catalog.GetFTSIndex(s.Index); err == nil {
			if err := db.catalog.DropFTSIndex(s.Index); err != nil {
				return Result{}, err
			}
			if db.auditLogger != nil {
				db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "DROP_INDEX")
			}
			return Result{RowsAffected: 0}, nil
		}
		// Try regular index
		if err := db.catalog.DropIndex(s.Index); err != nil {
			return Result{}, err
		}
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "DROP_INDEX")
		}
		return Result{RowsAffected: 0}, nil
	default:
		return Result{}, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// query executes a query and returns rows
func (db *DB) query(ctx context.Context, stmt query.Statement, args []interface{}) (*Rows, error) {
	start := time.Now()

	// Check for context cancellation
	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}
	}

	switch s := stmt.(type) {
	case *query.SelectStmt:
		rows, err := db.executeSelect(ctx, s, args)
		if db.auditLogger != nil {
			var rowCount int64
			if rows != nil {
				rowCount = int64(len(rows.rows))
			}
			db.auditLogger.LogQuery(auditUser(ctx), "SELECT", time.Since(start), rowCount, err)
		}
		return rows, err
	case *query.UnionStmt:
		rows, err := db.executeUnion(ctx, s, args)
		if db.auditLogger != nil {
			var rowCount int64
			if rows != nil {
				rowCount = int64(len(rows.rows))
			}
			db.auditLogger.LogQuery(auditUser(ctx), "SELECT", time.Since(start), rowCount, err)
		}
		return rows, err
	case *query.SelectStmtWithCTE:
		rows, err := db.executeSelectWithCTE(ctx, s, args)
		if db.auditLogger != nil {
			var rowCount int64
			if rows != nil {
				rowCount = int64(len(rows.rows))
			}
			db.auditLogger.LogQuery(auditUser(ctx), "SELECT", time.Since(start), rowCount, err)
		}
		return rows, err
	case *query.ShowTablesStmt:
		return db.executeShowTablesQuery(ctx)
	case *query.ShowCreateTableStmt:
		return db.executeShowCreateTableQuery(ctx, s)
	case *query.ShowColumnsStmt:
		return db.executeShowColumnsQuery(ctx, s)
	case *query.ShowDatabasesStmt:
		return db.executeShowDatabasesQuery(ctx)
	case *query.DescribeStmt:
		return db.executeDescribeQuery(ctx, s)
	case *query.ExplainStmt:
		return db.executeExplainQuery(ctx, s)
	case *query.InsertStmt:
		if len(s.Returning) > 0 {
			return db.executeInsertReturning(ctx, s, args)
		}
		return nil, fmt.Errorf("not a query statement: %T", stmt)
	case *query.UpdateStmt:
		if len(s.Returning) > 0 {
			return db.executeUpdateReturning(ctx, s, args)
		}
		return nil, fmt.Errorf("not a query statement: %T", stmt)
	case *query.DeleteStmt:
		if len(s.Returning) > 0 {
			return db.executeDeleteReturning(ctx, s, args)
		}
		return nil, fmt.Errorf("not a query statement: %T", stmt)
	default:
		return nil, fmt.Errorf("not a query statement: %T", stmt)
	}
}

// executeCreateTable executes CREATE TABLE
func (db *DB) executeCreateTable(ctx context.Context, stmt *query.CreateTableStmt) (Result, error) {
	if err := db.catalog.CreateTable(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeInsert executes INSERT
func (db *DB) executeInsert(ctx context.Context, stmt *query.InsertStmt, args []interface{}) (Result, error) {
	lastInsertID, rowsAffected, err := db.catalog.Insert(ctx, stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
}

// executeUpdate executes UPDATE
func (db *DB) executeUpdate(ctx context.Context, stmt *query.UpdateStmt, args []interface{}) (Result, error) {
	lastInsertID, rowsAffected, err := db.catalog.Update(ctx, stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
}

// executeDelete executes DELETE
func (db *DB) executeDelete(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (Result, error) {
	lastInsertID, rowsAffected, err := db.catalog.Delete(ctx, stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
}

// executeInsertReturning executes INSERT with RETURNING clause
func (db *DB) executeInsertReturning(ctx context.Context, stmt *query.InsertStmt, args []interface{}) (*Rows, error) {
	_, _, err := db.catalog.Insert(ctx, stmt, args)
	if err != nil {
		return nil, err
	}

	// Get RETURNING results from catalog
	returningRows := db.catalog.GetLastReturningRows()
	returningCols := db.catalog.GetLastReturningColumns()

	if len(returningRows) == 0 {
		return &Rows{rows: nil, columns: returningCols}, nil
	}

	return &Rows{rows: returningRows, columns: returningCols}, nil
}

// executeUpdateReturning executes UPDATE with RETURNING clause
func (db *DB) executeUpdateReturning(ctx context.Context, stmt *query.UpdateStmt, args []interface{}) (*Rows, error) {
	_, _, err := db.catalog.Update(ctx, stmt, args)
	if err != nil {
		return nil, err
	}

	// Get RETURNING results from catalog
	returningRows := db.catalog.GetLastReturningRows()
	returningCols := db.catalog.GetLastReturningColumns()

	if len(returningRows) == 0 {
		return &Rows{rows: nil, columns: returningCols}, nil
	}

	return &Rows{rows: returningRows, columns: returningCols}, nil
}

// executeDeleteReturning executes DELETE with RETURNING clause
func (db *DB) executeDeleteReturning(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (*Rows, error) {
	_, _, err := db.catalog.Delete(ctx, stmt, args)
	if err != nil {
		return nil, err
	}

	// Get RETURNING results from catalog
	returningRows := db.catalog.GetLastReturningRows()
	returningCols := db.catalog.GetLastReturningColumns()

	if len(returningRows) == 0 {
		return &Rows{rows: nil, columns: returningCols}, nil
	}

	return &Rows{rows: returningRows, columns: returningCols}, nil
}

// executeAlterTable executes ALTER TABLE
func (db *DB) executeAlterTable(ctx context.Context, stmt *query.AlterTableStmt) (Result, error) {
	switch stmt.Action {
	case "ADD":
		if err := db.catalog.AlterTableAddColumn(stmt); err != nil {
			return Result{}, err
		}
	case "DROP":
		if err := db.catalog.AlterTableDropColumn(stmt); err != nil {
			return Result{}, err
		}
	case "RENAME_TABLE":
		if err := db.catalog.AlterTableRename(stmt); err != nil {
			return Result{}, err
		}
	case "RENAME_COLUMN":
		if err := db.catalog.AlterTableRenameColumn(stmt); err != nil {
			return Result{}, err
		}
	default:
		return Result{}, fmt.Errorf("unsupported ALTER TABLE action: %s", stmt.Action)
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropTable executes DROP TABLE
func (db *DB) executeDropTable(ctx context.Context, stmt *query.DropTableStmt) (Result, error) {
	if err := db.catalog.DropTable(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateIndex executes CREATE INDEX
func (db *DB) executeCreateIndex(ctx context.Context, stmt *query.CreateIndexStmt) (Result, error) {
	if err := db.catalog.CreateIndex(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateView executes CREATE VIEW
func (db *DB) executeCreateView(ctx context.Context, stmt *query.CreateViewStmt) (Result, error) {
	if err := db.catalog.CreateView(stmt.Name, stmt.Query); err != nil {
		if stmt.IfNotExists {
			return Result{RowsAffected: 0}, nil
		}
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropView executes DROP VIEW
func (db *DB) executeDropView(ctx context.Context, stmt *query.DropViewStmt) (Result, error) {
	if err := db.catalog.DropView(stmt.Name); err != nil {
		if stmt.IfExists {
			return Result{RowsAffected: 0}, nil
		}
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateTrigger executes CREATE TRIGGER
func (db *DB) executeCreateTrigger(ctx context.Context, stmt *query.CreateTriggerStmt) (Result, error) {
	if err := db.catalog.CreateTrigger(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropTrigger executes DROP TRIGGER
func (db *DB) executeDropTrigger(ctx context.Context, stmt *query.DropTriggerStmt) (Result, error) {
	if err := db.catalog.DropTrigger(stmt.Name); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateProcedure executes CREATE PROCEDURE
func (db *DB) executeCreateProcedure(ctx context.Context, stmt *query.CreateProcedureStmt) (Result, error) {
	if err := db.catalog.CreateProcedure(stmt); err != nil {
		if stmt.IfNotExists {
			return Result{}, nil // Silently succeed
		}
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropProcedure executes DROP PROCEDURE
func (db *DB) executeDropProcedure(ctx context.Context, stmt *query.DropProcedureStmt) (Result, error) {
	if err := db.catalog.DropProcedure(stmt.Name); err != nil {
		if stmt.IfExists {
			return Result{}, nil // Silently succeed
		}
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreatePolicy executes CREATE POLICY for row-level security
func (db *DB) executeCreatePolicy(ctx context.Context, stmt *query.CreatePolicyStmt) (Result, error) {
	// Check if RLS is enabled
	if !db.catalog.IsRLSEnabled() {
		return Result{}, errors.New("row-level security is not enabled for this database")
	}

	// Convert Event string to PolicyType
	var policyType security.PolicyType
	switch strings.ToUpper(stmt.Event) {
	case "ALL":
		policyType = security.PolicyAll
	case "SELECT":
		policyType = security.PolicySelect
	case "INSERT":
		policyType = security.PolicyInsert
	case "UPDATE":
		policyType = security.PolicyUpdate
	case "DELETE":
		policyType = security.PolicyDelete
	default:
		return Result{}, fmt.Errorf("invalid policy event: %s", stmt.Event)
	}

	// Convert Expression to string for storage
	usingExpr := ""
	if stmt.Using != nil {
		usingExpr = expressionToString(stmt.Using)
	}
	if usingExpr == "" {
		usingExpr = "TRUE" // Default to allowing all if no expression
	}

	// Create the policy
	policy := &security.Policy{
		Name:       stmt.Name,
		TableName:  stmt.Table,
		Type:       policyType,
		Expression: usingExpr,
		Users:      nil, // Could be extracted from ForRoles
		Roles:      stmt.ForRoles,
		Enabled:    true,
	}

	if err := db.catalog.CreateRLSPolicy(policy); err != nil {
		return Result{}, err
	}

	return Result{RowsAffected: 0}, nil
}

// expressionToString converts an expression to its SQL string representation
func expressionToString(expr query.Expression) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *query.Identifier:
		return e.Name
	case *query.QualifiedIdentifier:
		if e.Table != "" {
			var sb strings.Builder
			sb.Grow(len(e.Table) + 1 + len(e.Column))
			sb.WriteString(e.Table)
			sb.WriteByte('.')
			sb.WriteString(e.Column)
			return sb.String()
		}
		return e.Column
	case *query.StringLiteral:
		var sb strings.Builder
		sb.Grow(len(e.Value) + 2)
		sb.WriteByte('\'')
		sb.WriteString(e.Value)
		sb.WriteByte('\'')
		return sb.String()
	case *query.NumberLiteral:
		return e.Raw
	case *query.BooleanLiteral:
		if e.Value {
			return "TRUE"
		}
		return "FALSE"
	case *query.NullLiteral:
		return "NULL"
	case *query.BinaryExpr:
		left := expressionToString(e.Left)
		right := expressionToString(e.Right)
		op := tokenTypeToString(e.Operator)
		var sb strings.Builder
		sb.Grow(len(left) + 1 + len(op) + 1 + len(right))
		sb.WriteString(left)
		sb.WriteByte(' ')
		sb.WriteString(op)
		sb.WriteByte(' ')
		sb.WriteString(right)
		return sb.String()
	case *query.UnaryExpr:
		op := tokenTypeToString(e.Operator)
		operand := expressionToString(e.Expr)
		var sb strings.Builder
		sb.Grow(len(op) + 1 + len(operand))
		sb.WriteString(op)
		sb.WriteByte(' ')
		sb.WriteString(operand)
		return sb.String()
	case *query.FunctionCall:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = expressionToString(arg)
		}
		joined := strings.Join(args, ", ")
		var sb strings.Builder
		sb.Grow(len(e.Name) + 1 + len(joined) + 1)
		sb.WriteString(e.Name)
		sb.WriteByte('(')
		sb.WriteString(joined)
		sb.WriteByte(')')
		return sb.String()
	case *query.InExpr:
		exprStr := expressionToString(e.Expr)
		items := make([]string, len(e.List))
		for i, item := range e.List {
			items[i] = expressionToString(item)
		}
		joined := strings.Join(items, ", ")
		var sb strings.Builder
		sb.Grow(len(exprStr) + 5 + len(joined) + 1)
		sb.WriteString(exprStr)
		sb.WriteString(" IN (")
		sb.WriteString(joined)
		sb.WriteByte(')')
		return sb.String()
	case *query.LikeExpr:
		exprStr := expressionToString(e.Expr)
		patternStr := expressionToString(e.Pattern)
		var sb strings.Builder
		if e.Not {
			sb.Grow(len(exprStr) + 10 + len(patternStr))
			sb.WriteString(exprStr)
			sb.WriteString(" NOT LIKE ")
		} else {
			sb.Grow(len(exprStr) + 6 + len(patternStr))
			sb.WriteString(exprStr)
			sb.WriteString(" LIKE ")
		}
		sb.WriteString(patternStr)
		return sb.String()
	case *query.IsNullExpr:
		exprStr := expressionToString(e.Expr)
		var sb strings.Builder
		if e.Not {
			sb.Grow(len(exprStr) + 12)
			sb.WriteString(exprStr)
			sb.WriteString(" IS NOT NULL")
		} else {
			sb.Grow(len(exprStr) + 8)
			sb.WriteString(exprStr)
			sb.WriteString(" IS NULL")
		}
		return sb.String()
	default:
		return ""
	}
}

// tokenTypeToString converts a token type to its string representation
func tokenTypeToString(tok query.TokenType) string {
	switch tok {
	case query.TokenEq:
		return "="
	case query.TokenNeq:
		return "!="
	case query.TokenLt:
		return "<"
	case query.TokenGt:
		return ">"
	case query.TokenLte:
		return "<="
	case query.TokenGte:
		return ">="
	case query.TokenAnd:
		return "AND"
	case query.TokenOr:
		return "OR"
	case query.TokenNot:
		return "NOT"
	case query.TokenPlus:
		return "+"
	case query.TokenMinus:
		return "-"
	case query.TokenStar:
		return "*"
	case query.TokenSlash:
		return "/"
	default:
		return ""
	}
}

// executeDropPolicy executes DROP POLICY
func (db *DB) executeDropPolicy(ctx context.Context, stmt *query.DropPolicyStmt) (Result, error) {
	// Check if RLS is enabled
	if !db.catalog.IsRLSEnabled() {
		return Result{}, errors.New("row-level security is not enabled for this database")
	}

	tableName := stmt.Table
	if tableName == "" {
		// If no table specified, try to find the policy in all tables
		// This is a simplified implementation
		return Result{}, errors.New("table name required for DROP POLICY")
	}

	if err := db.catalog.DropRLSPolicy(tableName, stmt.Name); err != nil {
		if stmt.IfExists && err.Error() == "security policy not found" {
			return Result{RowsAffected: 0}, nil
		}
		return Result{}, err
	}

	return Result{RowsAffected: 0}, nil
}

// executeCallProcedure executes CALL procedure_name(params)
func (db *DB) executeCallProcedure(ctx context.Context, stmt *query.CallProcedureStmt, args []interface{}) (Result, error) {
	// Get the procedure from catalog
	proc, err := db.catalog.GetProcedure(stmt.Name)
	if err != nil {
		return Result{}, err
	}

	// Evaluate call arguments from SQL literals (e.g., CALL proc(1, 'hello'))
	callArgs := make([]interface{}, 0, len(stmt.Params))
	for _, paramExpr := range stmt.Params {
		val, err := catalog.EvalExpression(paramExpr, nil)
		if err != nil {
			return Result{}, fmt.Errorf("evaluating procedure argument: %w", err)
		}
		callArgs = append(callArgs, val)
	}

	// Merge: SQL literal args take precedence, then positional Go args fill remaining
	mergedArgs := callArgs
	if len(mergedArgs) == 0 && len(args) > 0 {
		mergedArgs = args
	}

	// Map procedure parameters to call arguments
	paramMap := make(map[string]interface{})
	for i, param := range proc.Params {
		if i < len(mergedArgs) {
			paramMap[param.Name] = mergedArgs[i]
		}
	}

	var totalRowsAffected int64
	for _, bodyStmt := range proc.Body {
		result, err := db.executeWithParams(ctx, bodyStmt, paramMap)
		if err != nil {
			return Result{}, err
		}
		totalRowsAffected += result.RowsAffected
	}

	return Result{RowsAffected: totalRowsAffected}, nil
}

// executeWithParams executes a statement with parameter substitution
func (db *DB) executeWithParams(ctx context.Context, stmt query.Statement, paramMap map[string]interface{}) (Result, error) {
	// Substitute parameters in the statement
	substitutedStmt := substituteParamsInStatement(stmt, paramMap)
	
	// Execute the statement (with no additional args since params are substituted)
	return db.execute(ctx, substitutedStmt, nil)
}

// substituteParamsInStatement replaces parameter references with literal values
func substituteParamsInStatement(stmt query.Statement, paramMap map[string]interface{}) query.Statement {
	switch s := stmt.(type) {
	case *query.InsertStmt:
		newStmt := *s
		newStmt.Values = substituteParamsInValues(s.Values, paramMap)
		return &newStmt
	case *query.UpdateStmt:
		newStmt := *s
		newStmt.Set = substituteParamsInSetClauses(s.Set, paramMap)
		if s.Where != nil {
			newStmt.Where = substituteParamsInExpr(s.Where, paramMap)
		}
		return &newStmt
	case *query.DeleteStmt:
		newStmt := *s
		if s.Where != nil {
			newStmt.Where = substituteParamsInExpr(s.Where, paramMap)
		}
		return &newStmt
	default:
		return stmt
	}
}

// substituteParamsInValues replaces params in VALUES clause
func substituteParamsInValues(values [][]query.Expression, paramMap map[string]interface{}) [][]query.Expression {
	result := make([][]query.Expression, len(values))
	for i, row := range values {
		result[i] = make([]query.Expression, len(row))
		for j, expr := range row {
			result[i][j] = substituteParamsInExpr(expr, paramMap)
		}
	}
	return result
}

// substituteParamsInSetClauses replaces params in SET clause
func substituteParamsInSetClauses(set []*query.SetClause, paramMap map[string]interface{}) []*query.SetClause {
	result := make([]*query.SetClause, len(set))
	for i, clause := range set {
		newClause := *clause
		newClause.Value = substituteParamsInExpr(clause.Value, paramMap)
		result[i] = &newClause
	}
	return result
}

// substituteParamsInExpr replaces parameter identifiers with literal values
func substituteParamsInExpr(expr query.Expression, paramMap map[string]interface{}) query.Expression {
	if expr == nil {
		return nil
	}
	
	switch e := expr.(type) {
	case *query.Identifier:
		if val, ok := paramMap[e.Name]; ok {
			switch v := val.(type) {
			case string:
				return &query.StringLiteral{Value: v}
			case int:
				return &query.NumberLiteral{Value: float64(v)}
			case int64:
				return &query.NumberLiteral{Value: float64(v)}
			case float64:
				return &query.NumberLiteral{Value: v}
			case bool:
				return &query.BooleanLiteral{Value: v}
			case nil:
				return &query.NullLiteral{}
			default:
				return &query.StringLiteral{Value: fmt.Sprintf("%v", v)}
			}
		}
		return expr
	case *query.BinaryExpr:
		return &query.BinaryExpr{
			Left:     substituteParamsInExpr(e.Left, paramMap),
			Operator: e.Operator,
			Right:    substituteParamsInExpr(e.Right, paramMap),
		}
	case *query.UnaryExpr:
		return &query.UnaryExpr{
			Operator: e.Operator,
			Expr:     substituteParamsInExpr(e.Expr, paramMap),
		}
	case *query.FunctionCall:
		newArgs := make([]query.Expression, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = substituteParamsInExpr(arg, paramMap)
		}
		return &query.FunctionCall{
			Name:     e.Name,
			Args:     newArgs,
			Distinct: e.Distinct,
		}
	default:
		return expr
	}
}

// executeSelect executes SELECT
// executeSelect executes SELECT
func (db *DB) executeSelect(ctx context.Context, stmt *query.SelectStmt, args []interface{}) (*Rows, error) {
	columns, rows, err := db.catalog.Select(stmt, args)
	if err != nil {
		return nil, err
	}
	return &Rows{
		columns: columns,
		rows:    rows,
		pos:     0,
	}, nil
}

// executeUnion executes a UNION/INTERSECT/EXCEPT query by running both sides and combining results
func (db *DB) executeUnion(ctx context.Context, stmt *query.UnionStmt, args []interface{}) (*Rows, error) {
	// Execute left side
	var leftRows *Rows
	var err error
	switch l := stmt.Left.(type) {
	case *query.SelectStmt:
		leftRows, err = db.executeSelect(ctx, l, args)
	case *query.UnionStmt:
		leftRows, err = db.executeUnion(ctx, l, args)
	default:
		return nil, fmt.Errorf("unsupported left side of set operation: %T", stmt.Left)
	}
	if err != nil {
		return nil, err
	}

	// Execute right side
	// Note: stmt.Right is always *SelectStmt by parser design (see query.UnionStmt).
	// Nested unions are represented via Left chaining, not Right nesting.
	rightRows, err := db.executeSelect(ctx, stmt.Right, args)
	if err != nil {
		return nil, err
	}

	// Use left side's column names
	columns := leftRows.columns

	// Validate column counts match
	if len(leftRows.columns) != len(rightRows.columns) {
		return nil, fmt.Errorf("each %s query must have the same number of columns: left has %d, right has %d",
			"set operation", len(leftRows.columns), len(rightRows.columns))
	}

	var combined [][]interface{}

	switch stmt.Op {
	case query.SetOpUnion:
		// Combine rows
		combined = make([][]interface{}, 0, len(leftRows.rows)+len(rightRows.rows))
		combined = append(combined, leftRows.rows...)
		combined = append(combined, rightRows.rows...)

		// If not UNION ALL, deduplicate
		if !stmt.All {
			seen := make(map[string]bool)
			var unique [][]interface{}
			for _, row := range combined {
				key := normalizeRowKey(row)
				if !seen[key] {
					seen[key] = true
					unique = append(unique, row)
				}
			}
			combined = unique
		}

	case query.SetOpIntersect:
		// INTERSECT: only rows that appear in both sides
		rightSet := make(map[string]int)
		for _, row := range rightRows.rows {
			key := normalizeRowKey(row)
			rightSet[key]++
		}

		if stmt.All {
			// INTERSECT ALL: preserve duplicates up to min count
			leftCount := make(map[string]int)
			leftByKey := make(map[string][][]interface{})
			for _, row := range leftRows.rows {
				key := normalizeRowKey(row)
				leftCount[key]++
				leftByKey[key] = append(leftByKey[key], row)
			}
			for key, lc := range leftCount {
				rc := rightSet[key]
				if rc > 0 {
					count := lc
					if rc < count {
						count = rc
					}
					for i := 0; i < count && i < len(leftByKey[key]); i++ {
						combined = append(combined, leftByKey[key][i])
					}
				}
			}
		} else {
			// INTERSECT: deduplicated intersection
			seen := make(map[string]bool)
			for _, row := range leftRows.rows {
				key := normalizeRowKey(row)
				if rightSet[key] > 0 && !seen[key] {
					seen[key] = true
					combined = append(combined, row)
				}
			}
		}

	case query.SetOpExcept:
		// EXCEPT: rows in left that are NOT in right
		rightSet := make(map[string]int)
		for _, row := range rightRows.rows {
			key := normalizeRowKey(row)
			rightSet[key]++
		}

		if stmt.All {
			// EXCEPT ALL: subtract right counts from left
			for _, row := range leftRows.rows {
				key := normalizeRowKey(row)
				if rightSet[key] > 0 {
					rightSet[key]--
				} else {
					combined = append(combined, row)
				}
			}
		} else {
			// EXCEPT: deduplicated difference
			seen := make(map[string]bool)
			for _, row := range leftRows.rows {
				key := normalizeRowKey(row)
				if rightSet[key] == 0 && !seen[key] {
					seen[key] = true
					combined = append(combined, row)
				}
			}
		}
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		db.applyUnionOrderBy(combined, columns, stmt.OrderBy)
	}

	// Apply OFFSET
	if stmt.Offset != nil {
		if num, ok := stmt.Offset.(*query.NumberLiteral); ok {
			offset := int(num.Value)
			if offset > 0 {
				if offset >= len(combined) {
					combined = nil
				} else {
					combined = combined[offset:]
				}
			}
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		if num, ok := stmt.Limit.(*query.NumberLiteral); ok {
			limit := int(num.Value)
			if limit >= 0 && limit <= len(combined) {
				combined = combined[:limit]
			}
		}
	}

	return &Rows{
		columns: columns,
		rows:    combined,
		pos:     0,
	}, nil
}

// normalizeRowKey creates a type-normalized string key for deduplication.
// Normalizes numeric types so int64(1) and float64(1.0) produce the same key.
func normalizeRowKey(row []interface{}) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, v := range row {
		if i > 0 {
			sb.WriteByte(' ')
		}
		if v == nil {
			sb.WriteString("<nil>")
			continue
		}
		switch val := v.(type) {
		case int:
			sb.WriteString(strconv.FormatInt(int64(val), 10))
		case int64:
			sb.WriteString(strconv.FormatInt(val, 10))
		case float64:
			// If it's a whole number, format as integer to match int types
			if val == float64(int64(val)) {
				sb.WriteString(strconv.FormatInt(int64(val), 10))
			} else {
				sb.WriteString(strconv.FormatFloat(val, 'g', -1, 64))
			}
		case string:
			sb.WriteString("S:")
			sb.WriteString(val)
		case bool:
			if val {
				sb.WriteString("true")
			} else {
				sb.WriteString("false")
			}
		default:
			fmt.Fprintf(&sb, "%v", val)
		}
	}
	sb.WriteByte(']')
	return sb.String()
}

// applyUnionOrderBy sorts union result rows
func (db *DB) applyUnionOrderBy(rows [][]interface{}, columns []string, orderBy []*query.OrderByExpr) {
	if len(rows) == 0 {
		return
	}

	sort.Slice(rows, func(i, j int) bool {
		for _, ob := range orderBy {
			colIdx := -1
			switch expr := ob.Expr.(type) {
			case *query.Identifier:
				for k, col := range columns {
					if strings.EqualFold(col, expr.Name) {
						colIdx = k
						break
					}
				}
			case *query.NumberLiteral:
				colIdx = int(expr.Value) - 1
			}
			if colIdx < 0 || colIdx >= len(rows[i]) || colIdx >= len(rows[j]) {
				continue
			}
			cmp := db.compareUnionValues(rows[i][colIdx], rows[j][colIdx])
			if cmp != 0 {
				if ob.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

// compareUnionValues compares two values for sorting
func (db *DB) compareUnionValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	// Try numeric comparison
	fa, errA := strconv.ParseFloat(sa, 64)
	fb, errB := strconv.ParseFloat(sb, 64)
	if errA == nil && errB == nil {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

// executeSelectWithCTE executes SELECT with CTEs
func (db *DB) executeSelectWithCTE(ctx context.Context, stmt *query.SelectStmtWithCTE, args []interface{}) (*Rows, error) {
	columns, rows, err := db.catalog.ExecuteCTE(stmt, args)
	if err != nil {
		return nil, err
	}
	return &Rows{
		columns: columns,
		rows:    rows,
		pos:     0,
	}, nil
}

// executeVacuum executes VACUUM
func (db *DB) executeVacuum(ctx context.Context, stmt *query.VacuumStmt) (Result, error) {
	if err := db.catalog.Vacuum(); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeAnalyze executes ANALYZE
func (db *DB) executeAnalyze(ctx context.Context, stmt *query.AnalyzeStmt) (Result, error) {
	if stmt.Table == "" {
		// Analyze all tables
		tables := db.catalog.ListTables()
		for _, tableName := range tables {
			if err := db.catalog.Analyze(tableName); err != nil {
				return Result{}, err
			}
		}
	} else {
		if err := db.catalog.Analyze(stmt.Table); err != nil {
			return Result{}, err
		}
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateMaterializedView executes CREATE MATERIALIZED VIEW
func (db *DB) executeCreateMaterializedView(ctx context.Context, stmt *query.CreateMaterializedViewStmt) (Result, error) {
	if err := db.catalog.CreateMaterializedView(stmt.Name, stmt.Query, stmt.IfNotExists); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropMaterializedView executes DROP MATERIALIZED VIEW
func (db *DB) executeDropMaterializedView(ctx context.Context, stmt *query.DropMaterializedViewStmt) (Result, error) {
	if err := db.catalog.DropMaterializedView(stmt.Name, stmt.IfExists); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeRefreshMaterializedView executes REFRESH MATERIALIZED VIEW
func (db *DB) executeRefreshMaterializedView(ctx context.Context, stmt *query.RefreshMaterializedViewStmt) (Result, error) {
	if err := db.catalog.RefreshMaterializedView(stmt.Name); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateFTSIndex executes CREATE FULLTEXT INDEX
func (db *DB) executeCreateFTSIndex(ctx context.Context, stmt *query.CreateFTSIndexStmt) (Result, error) {
	if err := db.catalog.CreateFTSIndex(stmt.Index, stmt.Table, stmt.Columns); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateVectorIndex executes CREATE VECTOR INDEX
func (db *DB) executeCreateVectorIndex(ctx context.Context, stmt *query.CreateVectorIndexStmt) (Result, error) {
	if err := db.catalog.CreateVectorIndex(stmt.Index, stmt.Table, stmt.Column); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeShowTablesQuery returns all table names as rows
func (db *DB) executeShowTablesQuery(ctx context.Context) (*Rows, error) {
	tables := db.catalog.ListTables()
	rows := make([][]interface{}, 0, len(tables))
	for _, t := range tables {
		rows = append(rows, []interface{}{t})
	}
	return &Rows{
		columns: []string{"Tables_in_database"},
		rows:    rows,
	}, nil
}

// executeShowCreateTableQuery returns the CREATE TABLE statement
func (db *DB) executeShowCreateTableQuery(ctx context.Context, stmt *query.ShowCreateTableStmt) (*Rows, error) {
	schema, err := db.TableSchema(stmt.Table)
	if err != nil {
		return nil, err
	}
	return &Rows{
		columns: []string{"Table", "Create Table"},
		rows:    [][]interface{}{{stmt.Table, schema}},
	}, nil
}

// executeShowColumnsQuery returns column information for a table
func (db *DB) executeShowColumnsQuery(ctx context.Context, stmt *query.ShowColumnsStmt) (*Rows, error) {
	table, err := db.catalog.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0, len(table.Columns))
	for _, col := range table.Columns {
		nullable := "YES"
		if col.NotNull || col.PrimaryKey {
			nullable = "NO"
		}
		key := ""
		if col.PrimaryKey {
			key = "PRI"
		} else if col.Unique {
			key = "UNI"
		}
		defVal := col.Default
		if defVal == "" {
			defVal = "NULL"
		}
		extra := ""
		if col.AutoIncrement {
			extra = "auto_increment"
		}
		rows = append(rows, []interface{}{col.Name, col.Type, nullable, key, defVal, extra})
	}
	return &Rows{
		columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
		rows:    rows,
	}, nil
}

// executeShowDatabasesQuery returns available databases
func (db *DB) executeShowDatabasesQuery(ctx context.Context) (*Rows, error) {
	return &Rows{
		columns: []string{"Database"},
		rows:    [][]interface{}{{"cobaltdb"}},
	}, nil
}

// executeDescribeQuery returns column info for a table (alias for SHOW COLUMNS)
func (db *DB) executeDescribeQuery(ctx context.Context, stmt *query.DescribeStmt) (*Rows, error) {
	return db.executeShowColumnsQuery(ctx, &query.ShowColumnsStmt{Table: stmt.Table})
}

// executeExplainQuery executes EXPLAIN and returns the query plan
func (db *DB) executeExplainQuery(ctx context.Context, stmt *query.ExplainStmt) (*Rows, error) {
	// Get the inner statement
	innerStmt := stmt.Statement

	var explanation string
	switch s := innerStmt.(type) {
	case *query.SelectStmt:
		explanation = fmt.Sprintf("SELECT FROM %s", s.From.Name)
		if s.Where != nil {
			explanation += " WITH WHERE CLAUSE"
		}
		if len(s.Joins) > 0 {
			explanation += fmt.Sprintf(" WITH %d JOIN(S)", len(s.Joins))
		}
		if len(s.GroupBy) > 0 {
			explanation += " WITH GROUP BY"
		}
		if len(s.OrderBy) > 0 {
			explanation += " WITH ORDER BY"
		}
		if s.Limit != nil {
			explanation += " WITH LIMIT"
		}
	default:
		explanation = fmt.Sprintf("EXPLAIN not supported for %T", innerStmt)
	}

	// Return as a single-row result
	columns := []string{"QUERY PLAN"}
	rows := [][]interface{}{{explanation}}

	return &Rows{
		columns: columns,
		rows:    rows,
		pos:     0,
	}, nil
}

// Result represents the result of an Exec operation
type Result struct {
	LastInsertID int64
	RowsAffected int64
}

// Rows represents query results
type Rows struct {
	columns []string
	rows    [][]interface{}
	pos     int
}

// Next advances to the next row
func (r *Rows) Next() bool {
	if r == nil {
		return false
	}
	r.pos++
	return r.pos <= len(r.rows)
}

// Scan copies column values into dest
func (r *Rows) Scan(dest ...interface{}) error {
	if r.pos == 0 || r.pos > len(r.rows) {
		return errors.New("no current row")
	}

	row := r.rows[r.pos-1]
	if len(dest) != len(row) {
		return errors.New("column count mismatch")
	}

	for i, d := range dest {
		if err := scanValue(row[i], d); err != nil {
			return err
		}
	}

	return nil
}

// Columns returns the column names
func (r *Rows) Columns() []string {
	return r.columns
}

// Close closes the rows
func (r *Rows) Close() error {
	return nil
}

// Row represents a single row result
type Row struct {
	rows *Rows
	err  error
}

// Scan copies column values into dest
func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	if r.rows == nil {
		return errors.New("no row available")
	}
	defer r.rows.Close()
	return r.rows.Scan(dest...)
}

// scanValue scans a value into a destination
func scanValue(src interface{}, dest interface{}) error {
	switch d := dest.(type) {
	case *interface{}:
		*d = src
	case *string:
		*d = fmt.Sprintf("%v", src)
	case *int:
		v, ok := src.(int64)
		if !ok {
			// Try float
			if f, ok := src.(float64); ok {
				*d = int(f)
				return nil
			}
			return fmt.Errorf("cannot scan %T into int", src)
		}
		*d = int(v)
	case *int64:
		v, ok := src.(int64)
		if !ok {
			if f, ok := src.(float64); ok {
				*d = int64(f)
				return nil
			}
			return fmt.Errorf("cannot scan %T into int64", src)
		}
		*d = v
	case *float64:
		v, ok := src.(float64)
		if !ok {
			return fmt.Errorf("cannot scan %T into float64", src)
		}
		*d = v
	case *bool:
		v, ok := src.(bool)
		if !ok {
			return fmt.Errorf("cannot scan %T into bool", src)
		}
		*d = v
	case *[]byte:
		v, ok := src.([]byte)
		if !ok {
			return fmt.Errorf("cannot scan %T into []byte", src)
		}
		*d = v
	default:
		return fmt.Errorf("unsupported scan destination: %T", dest)
	}
	return nil
}

// Tx represents a database transaction
type Tx struct {
	db   *DB
	txn  *txn.Transaction
	done atomic.Bool // prevents double commit/rollback and double connection release
}

// Exec executes a statement within the transaction
func (tx *Tx) Exec(ctx context.Context, sql string, args ...interface{}) (Result, error) {
	if tx.done.Load() {
		return Result{}, errors.New("transaction already completed")
	}

	tx.db.mu.RLock()
	defer tx.db.mu.RUnlock()

	if tx.db.closed {
		return Result{}, ErrDatabaseClosed
	}

	// Parse the statement
	stmt, err := tx.db.getPreparedStatement(sql, args...)
	if err != nil {
		return Result{}, fmt.Errorf("parse error: %w", err)
	}

	// Execute within transaction context
	return tx.db.execute(ctx, stmt, args)
}

// Query executes a query within the transaction.
// Changes made within this transaction are visible to subsequent queries.
// Uses the same internal execution path as Tx.Exec to ensure transaction isolation.
func (tx *Tx) Query(ctx context.Context, sql string, args ...interface{}) (*Rows, error) {
	if tx.done.Load() {
		return nil, errors.New("transaction already completed")
	}

	tx.db.mu.RLock()
	defer tx.db.mu.RUnlock()

	if tx.db.closed {
		return nil, ErrDatabaseClosed
	}

	stmt, err := tx.db.getPreparedStatement(sql, args...)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	return tx.db.query(ctx, stmt, args)
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if !tx.done.CompareAndSwap(false, true) {
		return errors.New("transaction already completed")
	}
	defer tx.db.releaseConnection()

	// Flush table B+Trees to buffer pool first
	if err := tx.db.catalog.FlushTableTrees(); err != nil {
		// Rollback catalog transaction to prevent it from staying active forever
		if rbErr := tx.db.catalog.RollbackTransaction(); rbErr != nil { /* already have primary error */
		}
		return fmt.Errorf("failed to flush tables: %w", err)
	}

	// Commit in catalog first (writes commit record to WAL)
	if err := tx.db.catalog.CommitTransaction(); err != nil {
		// Rollback catalog transaction to prevent it from staying active forever
		if rbErr := tx.db.catalog.RollbackTransaction(); rbErr != nil { /* already have primary error */
		}
		return fmt.Errorf("commit transaction failed: %w", err)
	}

	// Flush buffer pool to disk to ensure durability
	if err := tx.db.pool.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush buffer pool: %w", err)
	}

	return tx.txn.Commit()
}

// Rollback rolls back the transaction
func (tx *Tx) Rollback() error {
	if !tx.done.CompareAndSwap(false, true) {
		return errors.New("transaction already completed")
	}
	defer tx.db.releaseConnection()

	// Rollback in catalog first (writes rollback record to WAL)
	if err := tx.db.catalog.RollbackTransaction(); err != nil {
		return fmt.Errorf("rollback transaction failed: %w", err)
	}
	if tx.db.metrics != nil {
		tx.db.metrics.RecordTransaction(false)
	}
	return tx.txn.Rollback()
}

// GetMetrics returns a snapshot of all database metrics as JSON
func (db *DB) GetMetrics() ([]byte, error) {
	if db.metrics == nil {
		return nil, fmt.Errorf("metrics not enabled")
	}
	return db.metrics.SnapshotJSON()
}

// GetMetricsCollector returns the metrics collector for advanced usage
func (db *DB) GetMetricsCollector() *metrics.Collector {
	return db.metrics
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

	if db.closed {
		return nil, ErrDatabaseClosed
	}

	stats := &DBStats{
		Path:              db.path,
		InMemory:          db.options.InMemory,
		PageSize:          db.options.PageSize,
		CacheSize:         db.options.CacheSize,
		ActiveConnections: db.activeConns.Load(),
		MaxConnections:    db.options.MaxConnections,
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

	if db.closed {
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

// Checkpoint performs a database checkpoint (implements backup.Database)
func (db *DB) Checkpoint() error {
	if db.wal != nil {
		return db.wal.Checkpoint(db.pool)
	}
	return nil
}

// BeginHotBackup starts a hot backup (implements backup.Database)
func (db *DB) BeginHotBackup() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	// Disable checkpoints during backup
	return nil
}

// EndHotBackup ends a hot backup (implements backup.Database)
func (db *DB) EndHotBackup() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	// Re-enable checkpoints
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

// GetQueryCache returns the query cache
func (db *DB) GetQueryCache() *cache.Cache {
	return db.queryCache
}

// PlanCache methods

// GetPlanCacheStats returns query plan cache statistics
// Returns nil if plan cache is not enabled
func (db *DB) GetPlanCacheStats() *QueryPlanCacheStats {
	if db.planCache == nil {
		return nil
	}
	stats := db.planCache.GetStats()
	return &stats
}

// ClearPlanCache clears all entries from the query plan cache
func (db *DB) ClearPlanCache() {
	if db.planCache != nil {
		db.planCache.Clear()
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

// replicateWrite sends write operations to the replication manager
// This is called automatically after successful write operations (INSERT, UPDATE, DELETE, DDL)
func (db *DB) replicateWrite(operation string, table string, args []interface{}) {
	if db.replicationMgr == nil {
		return // Replication not enabled
	}

	// Serialize the write operation
	// Format: operation|table|args...
	var sb strings.Builder
	sb.WriteString(operation)
	sb.WriteByte('|')
	sb.WriteString(table)
	sb.WriteByte('|')
	for i, arg := range args {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%v", arg)
	}

	// Send to replication manager (async, non-blocking)
	// The replication manager handles buffering and sending to slaves
	_ = db.replicationMgr.ReplicateWALEntry([]byte(sb.String()))
}

// Replication methods

// GetReplicationManager returns the replication manager
func (db *DB) GetReplicationManager() *replication.Manager {
	return db.replicationMgr
}

// SearchVectorKNN performs a K-nearest neighbor search on a vector index
func (db *DB) SearchVectorKNN(indexName string, queryVector []float64, k int) ([]string, []float64, error) {
	if db.closed {
		return nil, nil, ErrDatabaseClosed
	}
	return db.catalog.SearchVectorKNN(indexName, queryVector, k)
}

// SearchVectorRange performs a range search on a vector index
func (db *DB) SearchVectorRange(indexName string, queryVector []float64, radius float64) ([]string, []float64, error) {
	if db.closed {
		return nil, nil, ErrDatabaseClosed
	}
	return db.catalog.SearchVectorRange(indexName, queryVector, radius)
}

