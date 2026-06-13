package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/replication"
	"github.com/cobaltdb/cobaltdb/pkg/scheduler"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

func toUpperFast(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			return strings.ToUpper(s)
		}
	}
	return s
}

var (
	ErrDatabaseClosed = errors.New("database is closed")
	ErrInvalidPath    = errors.New("invalid database path")
)

// PanicRecovery records the most recent panic recovered from a public query API.
type PanicRecovery struct {
	Operation string
	Value     string
	Stack     string
	At        time.Time
}

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
	closed   atomic.Bool
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
	connLimit    int64        // Maximum concurrent connections (0 = unlimited)
	connCount    atomic.Int64 // Current acquired connections (atomic semaphore)
	connWaitMu   sync.Mutex
	connWaiters  []chan struct{} // Blocked waiters when at limit
	activeConns  atomic.Int64    // Active connection count
	shutdownCh   chan struct{}   // Shutdown signal
	shutdownOnce sync.Once
	lastPanic    atomic.Value // stores PanicRecovery

	// Query Optimizer
	optimizer *optimizer.Optimizer

	// Replication Manager
	replicationMgr *replication.Manager

	// Backup Manager
	backupMgr *backup.Manager

	// Slow Query Log
	slowQueryLog           *metrics.SlowQueryLog
	unregisterSlowQueryLog func()
	unregisterStorageStats func()

	// Query Plan Cache - caches parsed query statements
	planCache *QueryPlanCache

	// flushMu coordinates page-flush operations. Explicit transaction commits
	// acquire an RLock so they can flush B-tree pages concurrently; checkpoint,
	// close, and backup paths acquire a Lock so that BufferPool.FlushAll never
	// runs concurrently with a btree flush.
	flushMu sync.RWMutex

	// JobScheduler manages periodic maintenance tasks (vacuum, analyze, etc.)
	scheduler *scheduler.Scheduler

	// IndexAdvisor analyzes queries and recommends missing indexes
	indexAdvisor *advisor.IndexAdvisor
}

// LastPanicRecovery returns the latest panic recovered from Exec or Query.
func (db *DB) LastPanicRecovery() *PanicRecovery {
	if db == nil {
		return nil
	}
	info, ok := db.lastPanic.Load().(PanicRecovery)
	if !ok {
		return nil
	}
	return &info
}

func (db *DB) recordRecoveredPanic(operation string, recovered interface{}, stack []byte) {
	info := PanicRecovery{
		Operation: operation,
		Value:     fmt.Sprint(recovered),
		Stack:     string(stack),
		At:        time.Now(),
	}
	db.lastPanic.Store(info)
	if db.options != nil && db.options.CoreStorage.Logger != nil {
		db.options.CoreStorage.Logger.Errorf("PANIC in %s: %v\n%s", operation, recovered, stack)
	}
}

// CoreStorage contains the fundamental storage engine parameters.
type CoreStorage struct {
	PageSize   int            // Database page size (must match storage.PageSize)
	CacheSize  int            // Number of cached pages
	InMemory   bool           // Run fully in-memory without persisting
	WALEnabled *bool          // Enable write-ahead logging (nil = default: true for disk)
	SyncMode   SyncMode       // Durability vs performance trade-off
	Logger     *logger.Logger // Optional custom logger (nil = default)
}

// ConnectionPool governs how concurrent database connections are managed.
type ConnectionPool struct {
	MaxConnections    int           // Maximum concurrent connections (0 = unlimited)
	ConnectionTimeout time.Duration // Timeout for acquiring a connection
	QueryTimeout      time.Duration // Default query timeout (0 = no timeout)
}

// Security governs encryption, auditing, and access control settings.
type Security struct {
	EncryptionKey    []byte                    // Encryption key for data at rest (nil = no encryption)
	EncryptionConfig *storage.EncryptionConfig // Detailed encryption configuration
	AuditConfig      *audit.Config             // Audit logging configuration (nil = disabled)
	EnableRLS        bool                      // Enable Row-Level Security by default
	MaxStmtCacheSize int                       // Maximum cached prepared statements (default: 1000)
	StrictSQLParsing bool                      // Reject trailing tokens after a parsed statement
}

// QueryCacheConfig governs the query result cache.
type QueryCacheConfig struct {
	EnableQueryCache bool          // Enable query result caching
	QueryCacheSize   int64         // Max query cache size in bytes (default: 64MB)
	QueryCacheTTL    time.Duration // Query cache TTL (default: 5m)
}

// ReplicationConfig governs the replication subsystem.
type ReplicationConfig struct {
	Role       string // "master", "slave", or "" (disabled)
	ListenAddr string // Master listen address for slaves to connect to
	MasterAddr string // Slave: master address to connect to
	Mode       string // "async", "sync", or "full_sync"
	AuthToken  string // Authentication token for replication
	SSLCert    string // SSL certificate file path
	SSLKey     string // SSL private key file path
	SSLCA      string // SSL CA certificate path
	StateFile  string // Slave resume state file path
}

// BackupConfig governs backup creation and retention.
type BackupConfig struct {
	Dir              string        // Backup directory path
	Retention        time.Duration // Backup retention period
	MaxBackups       int           // Maximum number of backups to retain
	CompressionLevel int           // Compression level (0-9, 0=disabled)
}

// SlowQueryLogConfig governs slow query logging.
type SlowQueryLogConfig struct {
	EnableSlowQueryLog bool          // Enable slow query logging
	Threshold          time.Duration // Threshold for slow queries (default: 1s)
	MaxEntries         int           // Max in-memory entries (default: 1000)
	LogFile            string        // Log file path (empty = memory only)
}

// PlanCacheConfig governs the query plan cache.
type PlanCacheConfig struct {
	EnablePlanCache bool  // Enable query plan caching
	Size            int64 // Max plan cache size in bytes (default: 32MB)
	MaxEntries      int   // Max number of cached plans (default: 1000)
}

// MaintenanceConfig governs auto-vacuum and checkpoint settings.
type MaintenanceConfig struct {
	EnableAutoVacuum     bool          // Enable automatic VACUUM (default: true for disk)
	AutoVacuumInterval   time.Duration // Interval between auto-vacuum checks (default: 1m)
	AutoVacuumThreshold  float64       // Dead tuple ratio to trigger vacuum (default: 0.2 = 20%)
	EnableAutoCheckpoint bool          // Enable automatic WAL checkpoint (default: true for disk)
	CheckpointInterval   time.Duration // Interval between checkpoints (default: 5m)
}

// SchedulerConfig governs the background job scheduler.
type SchedulerConfig struct {
	EnableScheduler bool          // Enable job scheduler (default: true for disk)
	AnalyzeInterval time.Duration // Interval for automatic ANALYZE (default: 1h)
	Workers         int           // Number of scheduler workers (default: 2)
	TickInterval    time.Duration // Dispatcher resolution (default: 1s)
}

// PageCompressionConfig holds page-level compression settings.
type PageCompressionConfig struct {
	Config *storage.CompressionConfig // nil = disabled
}

// ParallelQueryConfig governs parallel query execution.
type ParallelQueryConfig struct {
	Workers   int // Number of parallel query workers (0 = disabled, default: NumCPU)
	Threshold int // Min rows to trigger parallel execution (default: 1000)
}

// Options contains database configuration options
type Options struct {
	CoreStorage
	ConnectionPool
	Security

	// Deprecated: use CoreStorage.InMemory.
	InMemory bool
	// Deprecated: use CoreStorage.PageSize.
	PageSize int
	// Deprecated: use CoreStorage.CacheSize.
	CacheSize int
	// Deprecated: use CoreStorage.WALEnabled.
	WALEnabled *bool
	// Deprecated: use CoreStorage.SyncMode.
	SyncMode SyncMode
	// Deprecated: use CoreStorage.Logger.
	Logger *logger.Logger
	// Deprecated: use ConnectionPool.MaxConnections.
	MaxConnections int
	// Deprecated: use ConnectionPool.ConnectionTimeout.
	ConnectionTimeout time.Duration
	// Deprecated: use ConnectionPool.QueryTimeout.
	QueryTimeout time.Duration
	// Deprecated: use Security.EncryptionKey.
	EncryptionKey []byte
	// Deprecated: use Security.EncryptionConfig.
	EncryptionConfig *storage.EncryptionConfig
	// Deprecated: use Security.EnableRLS.
	EnableRLS bool
	// Deprecated: use Security.AuditConfig.
	AuditConfig *audit.Config
	// Deprecated: use Security.MaxStmtCacheSize.
	MaxStmtCacheSize int
	// Deprecated: use Security.StrictSQLParsing.
	StrictSQLParsing bool
	// Deprecated: use QueryCache.EnableQueryCache.
	EnableQueryCache bool
	// Deprecated: use QueryCache.QueryCacheSize.
	QueryCacheSize int64
	// Deprecated: use QueryCache.QueryCacheTTL.
	QueryCacheTTL time.Duration
	// Deprecated: use Replication.Role.
	ReplicationRole string
	// Deprecated: use Replication.ListenAddr.
	ReplicationListenAddr string
	// Deprecated: use Replication.MasterAddr.
	ReplicationMasterAddr string
	// Deprecated: use Replication.Mode.
	ReplicationMode string
	// Deprecated: use Replication.AuthToken.
	ReplicationAuthToken string
	// Deprecated: use Replication.SSLCert.
	ReplicationSSLCert string
	// Deprecated: use Replication.SSLKey.
	ReplicationSSLKey string
	// Deprecated: use Replication.SSLCA.
	ReplicationSSLCA string
	// Deprecated: use Replication.StateFile.
	ReplicationStateFile string
	// Deprecated: use Backup.Dir.
	BackupDir string
	// Deprecated: use Backup.Retention.
	BackupRetention time.Duration
	// Deprecated: use Backup.MaxBackups.
	MaxBackups int
	// Deprecated: use Backup.CompressionLevel.
	BackupCompressionLevel int
	// Deprecated: use SlowQueryLog.EnableSlowQueryLog.
	EnableSlowQueryLog bool
	// Deprecated: use SlowQueryLog.Threshold.
	SlowQueryThreshold time.Duration
	// Deprecated: use SlowQueryLog.MaxEntries.
	SlowQueryMaxEntries int
	// Deprecated: use SlowQueryLog.LogFile.
	SlowQueryLogFile string
	// Deprecated: use PlanCache.EnablePlanCache.
	EnablePlanCache bool
	// Deprecated: use PlanCache.Size.
	PlanCacheSize int64
	// Deprecated: use PlanCache.MaxEntries.
	PlanCacheEntries int
	// Deprecated: use Maintenance.EnableAutoVacuum.
	EnableAutoVacuum bool
	// Deprecated: use Scheduler.EnableScheduler.
	EnableScheduler bool
	// Deprecated: use PageCompression.Config.
	CompressionConfig *storage.CompressionConfig

	QueryCache      QueryCacheConfig
	Replication     ReplicationConfig
	Backup          BackupConfig
	SlowQueryLog    SlowQueryLogConfig
	PlanCache       PlanCacheConfig
	Maintenance     MaintenanceConfig
	Scheduler       SchedulerConfig
	PageCompression PageCompressionConfig
	ParallelQuery   ParallelQueryConfig
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

// BoolPtr returns a pointer to the given bool value. Useful for Options
// literals where *bool fields need an explicit value.
func BoolPtr(b bool) *bool {
	return &b
}

// DefaultOptions returns the default database options

func (db *DB) GetScheduler() *scheduler.Scheduler {
	return db.scheduler
}

// RegisterFDW registers a foreign data wrapper factory with the database.

func (db *DB) RegisterFDW(name string, factory func() fdw.ForeignDataWrapper) {
	db.catalog.GetFDWRegistry().Register(name, factory)
}

// getPreparedStatement returns a cached prepared statement or parses and caches it

func (db *DB) getPreparedStatement(sql string, args ...interface{}) (query.Statement, error) {
	// First check plan cache if enabled (more sophisticated caching with size limits)
	if db.planCache != nil {
		if entry, found := db.planCache.getShared(sql, args); found {
			return entry.ParsedStmt, nil
		}
	}

	db.stmtMu.RLock()
	cached, exists := db.stmtCache[sql]
	db.stmtMu.RUnlock()

	if exists {
		// Best-effort LRU update: if the lock is uncontended bump the stats,
		// otherwise skip rather than serialise every goroutine on stmtMu.
		if db.stmtMu.TryLock() {
			if c, ok := db.stmtCache[sql]; ok {
				c.lastUsed = time.Now().Unix()
				c.useCount++
				db.stmtLRU.moveToFront(c.elem)
			}
			db.stmtMu.Unlock()
		}
		return cached.stmt, nil
	}

	// Parse and cache
	parse := query.Parse
	if db.options.Security.StrictSQLParsing {
		parse = query.ParseStrict
	}
	parsedStmt, err := parse(sql)
	if err != nil {
		return nil, err
	}
	annotateDDLRawSQL(parsedStmt, sql)

	// Cache in plan cache if enabled
	if db.planCache != nil {
		if err := db.planCache.Put(sql, args, parsedStmt); err != nil {
			return nil, err
		}
	}

	// Cache the statement with O(1) LRU eviction
	db.stmtMu.Lock()
	if cached, exists := db.stmtCache[sql]; exists {
		cached.lastUsed = time.Now().Unix()
		cached.useCount++
		db.stmtLRU.moveToFront(cached.elem)
		db.stmtMu.Unlock()
		return cached.stmt, nil
	}
	maxCacheSize := db.options.Security.MaxStmtCacheSize
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

func annotateDDLRawSQL(stmt query.Statement, sql string) {
	normalized := strings.TrimSpace(sql)
	switch s := stmt.(type) {
	case *query.CreateViewStmt:
		s.RawSQL = normalized
	case *query.CreateMaterializedViewStmt:
		s.RawSQL = normalized
	case *query.CreateTriggerStmt:
		s.RawSQL = normalized
	case *query.CreateProcedureStmt:
		s.RawSQL = normalized
	}
}

// evictLRUEntry removes the least recently used entry from the cache
// Must be called with stmtMu.Lock() held

func (db *DB) evictLRUEntry() {
	tail := db.stmtLRU.removeTail()
	if tail != nil {
		delete(db.stmtCache, tail.sql)
	}
}

// acquireConnection acquires a connection slot with timeout.
// The fast path uses atomics to avoid channel/select overhead.
func (db *DB) acquireConnection(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("connection timeout: %w", err)
	}

	if db.connLimit <= 0 {
		// No connection limit
		db.activeConns.Add(1)
		if db.metrics != nil {
			db.metrics.ConnectionAcquired()
		}
		return nil
	}

	// Fast path: atomic increment if under limit.
	for {
		n := db.connCount.Load()
		if n >= db.connLimit {
			break
		}
		if db.connCount.CompareAndSwap(n, n+1) {
			db.activeConns.Add(1)
			if db.metrics != nil {
				db.metrics.ConnectionAcquired()
			}
			return nil
		}
	}

	// Slow path: block until a slot opens or context is cancelled.
	ch := make(chan struct{}, 1)
	db.connWaitMu.Lock()
	// Double-check under lock to prevent lost wakeups.
	if db.connCount.Load() < db.connLimit {
		db.connCount.Add(1)
		db.connWaitMu.Unlock()
		db.activeConns.Add(1)
		if db.metrics != nil {
			db.metrics.ConnectionAcquired()
		}
		return nil
	}
	db.connWaiters = append(db.connWaiters, ch)
	db.connWaitMu.Unlock()

	// Apply timeout if the caller context has no deadline.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		timeout := db.options.ConnectionPool.ConnectionTimeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	select {
	case <-ch:
		db.activeConns.Add(1)
		if db.metrics != nil {
			db.metrics.ConnectionAcquired()
		}
		return nil
	case <-ctx.Done():
		db.connWaitMu.Lock()
		for i, w := range db.connWaiters {
			if w == ch {
				db.connWaiters = append(db.connWaiters[:i], db.connWaiters[i+1:]...)
				break
			}
		}
		db.connWaitMu.Unlock()
		return fmt.Errorf("connection timeout: %w", ctx.Err())
	case <-db.shutdownCh:
		return ErrDatabaseClosed
	}
}

// releaseConnection releases a connection slot, waking a waiter if any.
func (db *DB) releaseConnection() {
	if db.connLimit > 0 {
		db.connWaitMu.Lock()
		if len(db.connWaiters) > 0 {
			ch := db.connWaiters[0]
			db.connWaiters = db.connWaiters[1:]
			db.connWaitMu.Unlock()
			ch <- struct{}{}
			db.activeConns.Add(-1)
			if db.metrics != nil {
				db.metrics.ConnectionReleased()
			}
			return
		}
		db.connWaitMu.Unlock()
		db.connCount.Add(-1)
	}
	db.activeConns.Add(-1)
	if db.metrics != nil {
		db.metrics.ConnectionReleased()
	}
}

// runStatement does the common setup for Exec and Query: panic recovery,
// query timeout, connection acquire, db closed check, and statement parsing.
// It returns the execution context, parsed statement, and a release-connection
// func; if err is non-nil the caller should return immediately.
func (db *DB) runStatement(ctx context.Context, methodName, sql string, args ...interface{}) (_ context.Context, _ query.Statement, start time.Time, release func(), err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Apply default query timeout only if the caller did not already set one.
	if db.options.ConnectionPool.QueryTimeout > 0 {
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, db.options.ConnectionPool.QueryTimeout)
			release = func() {
				cancel()
				db.releaseConnection()
			}
		} else {
			release = db.releaseConnection
		}
	} else {
		release = db.releaseConnection
	}

	// Acquire connection
	if acquireErr := db.acquireConnection(ctx); acquireErr != nil {
		return ctx, nil, time.Time{}, func() {}, acquireErr
	}

	var stmt query.Statement
	if err := func() error {
		db.mu.RLock()
		defer db.mu.RUnlock()
		if db.closed.Load() {
			return ErrDatabaseClosed
		}
		// Try to use cached prepared statement
		var parseErr error
		stmt, parseErr = db.getPreparedStatement(sql, args...)
		if parseErr != nil {
			return fmt.Errorf("parse error: %w", parseErr)
		}
		// Feed statement to index advisor for pattern analysis
		if db.indexAdvisor != nil {
			db.indexAdvisor.Analyze(stmt)
		}
		return nil
	}(); err != nil {
		release()
		return ctx, nil, time.Time{}, func() {}, err
	}

	start = time.Now()
	return ctx, stmt, start, release, nil
}

// Exec executes a SQL statement without returning rows

func (db *DB) Exec(ctx context.Context, sql string, args ...interface{}) (result Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			err = fmt.Errorf("internal error in Exec: %v", r)
			db.recordRecoveredPanic("Exec", r, stack)
		}
	}()

	runCtx, stmt, start, release, execErr := db.runStatement(ctx, "Exec", sql, args...)
	if execErr != nil {
		if errors.Is(execErr, ErrDatabaseClosed) {
			return Result{}, execErr
		}
		if db.metrics != nil {
			db.metrics.RecordError()
		}
		return Result{}, execErr
	}
	defer release()

	// Metrics
	if db.metrics != nil {
		defer func() {
			duration := time.Since(start)
			db.metrics.RecordQuery(duration, duration > 100*time.Millisecond)
		}()
	}

	// Slow query logging (Exec passes rows affected to Log)
	if db.slowQueryLog != nil {
		defer func() {
			db.slowQueryLog.Log(sql, time.Since(start), result.RowsAffected, 0)
		}()
	}

	return db.execute(runCtx, stmt, args)
}

// Query executes a SQL query and returns rows

func (db *DB) Query(ctx context.Context, sql string, args ...interface{}) (rows *Rows, err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			err = fmt.Errorf("internal error in Query: %v", r)
			db.recordRecoveredPanic("Query", r, stack)
		}
	}()

	runCtx, stmt, start, release, execErr := db.runStatement(ctx, "Query", sql, args...)
	if execErr != nil {
		if errors.Is(execErr, ErrDatabaseClosed) {
			return nil, execErr
		}
		if db.metrics != nil {
			db.metrics.RecordError()
		}
		return nil, execErr
	}
	defer release()

	// Metrics
	if db.metrics != nil {
		defer func() {
			duration := time.Since(start)
			db.metrics.RecordQuery(duration, duration > 100*time.Millisecond)
		}()
	}

	// Slow query logging (Query passes rowsAffected=0)
	if db.slowQueryLog != nil {
		defer func() {
			db.slowQueryLog.Log(sql, time.Since(start), 0, 0)
		}()
	}

	return db.query(runCtx, stmt, args)
}

// QueryRow executes a SQL query and returns a single row

func (db *DB) QueryRow(ctx context.Context, sql string, args ...interface{}) *Row {
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return &Row{err: err}
	}

	if !rows.Next() {
		if err := rows.Close(); err != nil {
			return &Row{err: err}
		}
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

// TableSchema returns a human-readable schema for a table.
func (db *DB) TableSchema(name string) (string, error) {
	return db.tableSchema(name, true, false)
}

// TableSchemaWithoutForeignKeys returns a table schema with foreign key
// constraints omitted, so dumps can restore data before validating FKs.
func (db *DB) TableSchemaWithoutForeignKeys(name string) (string, error) {
	return db.tableSchema(name, false, true)
}

func (db *DB) tableSchema(name string, includeForeignKeys, quoteIdentifiers bool) (string, error) {
	table, err := db.catalog.GetTable(name)
	if err != nil {
		return "", err
	}
	// A composite (multi-column) primary key is emitted as a table-level
	// constraint rather than per-column, so the dump restores correctly.
	compositePK := len(table.PrimaryKey) > 1

	var clauses []string
	for _, col := range table.Columns {
		line := fmt.Sprintf("  %s %s", schemaIdentifier(col.Name, quoteIdentifiers), schemaColumnType(col))
		if col.Collation != "" {
			line += fmt.Sprintf(" COLLATE %s", col.Collation)
		}
		if col.PrimaryKey && !compositePK {
			line += " PRIMARY KEY"
		}
		if col.AutoIncrement {
			line += " AUTOINCREMENT"
		}
		if col.NotNull {
			line += " NOT NULL"
		}
		if col.Unique {
			line += " UNIQUE"
		}
		if col.Default != "" {
			line += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		if col.CheckStr != "" {
			line += " "
			if col.CheckName != "" {
				line += fmt.Sprintf("CONSTRAINT %s ", schemaIdentifier(col.CheckName, quoteIdentifiers))
			}
			line += fmt.Sprintf("CHECK (%s)", schemaCheckExpr(col.CheckStr))
		}
		clauses = append(clauses, line)
	}
	if compositePK {
		clauses = append(clauses, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(schemaIdentifierList(table.PrimaryKey, quoteIdentifiers), ", ")))
	}
	for _, check := range table.Checks {
		clause := "  "
		if check.Name != "" {
			clause += fmt.Sprintf("CONSTRAINT %s ", schemaIdentifier(check.Name, quoteIdentifiers))
		}
		clause += fmt.Sprintf("CHECK (%s)", schemaCheckExpr(check.CheckStr))
		clauses = append(clauses, clause)
	}
	if includeForeignKeys {
		for _, fk := range table.ForeignKeys {
			clause := "  "
			if fk.Name != "" {
				clause += fmt.Sprintf("CONSTRAINT %s ", schemaIdentifier(fk.Name, quoteIdentifiers))
			}
			clause += fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s",
				strings.Join(schemaIdentifierList(fk.Columns, quoteIdentifiers), ", "),
				schemaIdentifier(fk.ReferencedTable, quoteIdentifiers))
			if len(fk.ReferencedColumns) > 0 {
				clause += fmt.Sprintf(" (%s)", strings.Join(schemaIdentifierList(fk.ReferencedColumns, quoteIdentifiers), ", "))
			}
			if fk.OnDelete != "" {
				clause += " ON DELETE " + fk.OnDelete
			}
			if fk.OnUpdate != "" {
				clause += " ON UPDATE " + fk.OnUpdate
			}
			clauses = append(clauses, clause)
		}
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", schemaIdentifier(table.Name, quoteIdentifiers)))
	sb.WriteString(strings.Join(clauses, ",\n"))
	sb.WriteString("\n);")
	return sb.String(), nil
}

func schemaColumnType(col catalog.ColumnDef) string {
	if strings.EqualFold(col.Type, "VECTOR") && col.Dimensions > 0 {
		return fmt.Sprintf("VECTOR(%d)", col.Dimensions)
	}
	return col.Type
}

func schemaIdentifier(name string, quote bool) string {
	if !quote {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func schemaIdentifierList(names []string, quote bool) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = schemaIdentifier(name, quote)
	}
	return out
}

func schemaCheckExpr(expr string) string {
	expr = strings.TrimSpace(expr)
	if len(expr) >= 2 && expr[0] == '(' && expr[len(expr)-1] == ')' {
		return strings.TrimSpace(expr[1 : len(expr)-1])
	}
	return expr
}

// TableForeignKeyRefs returns the distinct names of tables referenced by a
// table's foreign keys, used to order a dump so referenced tables come first.
func (db *DB) TableForeignKeyRefs(name string) []string {
	table, err := db.catalog.GetTable(name)
	if err != nil {
		return nil
	}
	seen := make(map[string]bool)
	var refs []string
	for _, fk := range table.ForeignKeys {
		if fk.ReferencedTable != "" && !seen[fk.ReferencedTable] && fk.ReferencedTable != name {
			seen[fk.ReferencedTable] = true
			refs = append(refs, fk.ReferencedTable)
		}
	}
	return refs
}

// TableForeignKeyRef describes a foreign key from a table to another table.
type TableForeignKeyRef struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          string
	OnUpdate          string
}

// TableForeignKeys returns foreign key definitions declared on a table.
func (db *DB) TableForeignKeys(name string) []TableForeignKeyRef {
	table, err := db.catalog.GetTable(name)
	if err != nil {
		return nil
	}
	refs := make([]TableForeignKeyRef, 0, len(table.ForeignKeys))
	for _, fk := range table.ForeignKeys {
		refs = append(refs, TableForeignKeyRef{
			Name:              fk.Name,
			Columns:           append([]string(nil), fk.Columns...),
			ReferencedTable:   fk.ReferencedTable,
			ReferencedColumns: append([]string(nil), fk.ReferencedColumns...),
			OnDelete:          fk.OnDelete,
			OnUpdate:          fk.OnUpdate,
		})
	}
	return refs
}

// TableSelfForeignKeyRefs returns self-referential foreign keys for a table.
// It is used by SQL dumps to emit self-referenced rows in restore-safe order.
func (db *DB) TableSelfForeignKeyRefs(name string) []TableForeignKeyRef {
	table, err := db.catalog.GetTable(name)
	if err != nil {
		return nil
	}
	var refs []TableForeignKeyRef
	for _, fk := range table.ForeignKeys {
		if !strings.EqualFold(fk.ReferencedTable, name) {
			continue
		}
		refColumns := fk.ReferencedColumns
		if len(refColumns) == 0 {
			refColumns = table.PrimaryKey
		}
		refs = append(refs, TableForeignKeyRef{
			Columns:           append([]string(nil), fk.Columns...),
			ReferencedTable:   fk.ReferencedTable,
			ReferencedColumns: append([]string(nil), refColumns...),
		})
	}
	return refs
}

// TableIndexDDL returns CREATE INDEX statements for a table's secondary indexes,
// used by the SQL dump so indexes survive a restore.
func (db *DB) TableIndexDDL(name string) []string {
	var ddl []string
	for _, idx := range db.catalog.GetTableIndexes(name) {
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		ddl = append(ddl, fmt.Sprintf("CREATE %sINDEX %s ON %s (%s);",
			unique,
			schemaIdentifier(idx.Name, true),
			schemaIdentifier(name, true),
			strings.Join(schemaIdentifierList(idx.Columns, true), ", ")))
	}
	return ddl
}

// FTSIndexDDL returns CREATE FULLTEXT INDEX statements for SQL dumps.
func (db *DB) FTSIndexDDL() []string {
	indexes := db.catalog.ListFTSIndexDefs()
	ddl := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		ddl = append(ddl, fmt.Sprintf("CREATE FULLTEXT INDEX %s ON %s (%s);",
			schemaIdentifier(idx.Name, true),
			schemaIdentifier(idx.TableName, true),
			strings.Join(schemaIdentifierList(idx.Columns, true), ", ")))
	}
	return ddl
}

// VectorIndexDDL returns CREATE VECTOR INDEX statements for SQL dumps.
func (db *DB) VectorIndexDDL() []string {
	indexes := db.catalog.ListVectorIndexDefs()
	ddl := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		ddl = append(ddl, fmt.Sprintf("CREATE VECTOR INDEX %s ON %s (%s);",
			schemaIdentifier(idx.Name, true),
			schemaIdentifier(idx.TableName, true),
			schemaIdentifier(idx.ColumnName, true)))
	}
	return ddl
}

// RLSPolicyDDL returns ALTER TABLE and CREATE POLICY statements for SQL dumps.
func (db *DB) RLSPolicyDDL() []string {
	tables := db.catalog.ListRLSEnabledTables()
	policies := db.catalog.ListRLSPolicies()
	if len(tables) == 0 && len(policies) == 0 {
		return nil
	}
	ddl := make([]string, 0, len(tables)+len(policies))
	enabledTables := make(map[string]bool)
	for _, tableName := range tables {
		tableKey := strings.ToLower(tableName)
		if enabledTables[tableKey] {
			continue
		}
		ddl = append(ddl, fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY;", schemaIdentifier(tableName, true)))
		enabledTables[tableKey] = true
	}
	for _, policy := range policies {
		tableKey := strings.ToLower(policy.TableName)
		if !enabledTables[tableKey] {
			ddl = append(ddl, fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY;", schemaIdentifier(policy.TableName, true)))
			enabledTables[tableKey] = true
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "CREATE POLICY %s ON %s",
			schemaIdentifier(policy.Name, true),
			schemaIdentifier(policy.TableName, true))
		if policy.Restrictive {
			sb.WriteString(" AS RESTRICTIVE")
		}
		fmt.Fprintf(&sb, " FOR %s", policy.Type.String())
		if len(policy.Roles) > 0 {
			sb.WriteString(" TO ")
			sb.WriteString(strings.Join(schemaIdentifierList(policy.Roles, true), ", "))
		}
		if strings.TrimSpace(policy.Expression) != "" {
			fmt.Fprintf(&sb, " USING (%s)", policy.Expression)
		}
		if strings.TrimSpace(policy.CheckExpression) != "" {
			fmt.Fprintf(&sb, " WITH CHECK (%s)", policy.CheckExpression)
		}
		sb.WriteString(";")
		ddl = append(ddl, sb.String())
	}
	return ddl
}

// ForeignTableDDL returns CREATE FOREIGN TABLE statements for SQL dumps.
func (db *DB) ForeignTableDDL() []string {
	foreignTables := db.catalog.ListForeignTables()
	sort.Slice(foreignTables, func(i, j int) bool {
		return strings.ToLower(foreignTables[i].TableName) < strings.ToLower(foreignTables[j].TableName)
	})
	ddl := make([]string, 0, len(foreignTables))
	for _, ft := range foreignTables {
		ddl = append(ddl, foreignTableDDL(ft))
	}
	return ddl
}

func foreignTableDDL(ft catalog.ForeignTableDef) string {
	columns := make([]string, len(ft.Columns))
	for i, col := range ft.Columns {
		line := fmt.Sprintf("%s %s", schemaIdentifier(col.Name, true), col.Type)
		if col.Collation != "" {
			line += fmt.Sprintf(" COLLATE %s", col.Collation)
		}
		if col.PrimaryKey {
			line += " PRIMARY KEY"
		}
		if col.AutoIncrement {
			line += " AUTOINCREMENT"
		}
		if col.NotNull {
			line += " NOT NULL"
		}
		if col.Unique {
			line += " UNIQUE"
		}
		if col.Default != "" {
			line += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		if col.CheckStr != "" {
			line += " "
			if col.CheckName != "" {
				line += fmt.Sprintf("CONSTRAINT %s ", schemaIdentifier(col.CheckName, true))
			}
			line += fmt.Sprintf("CHECK (%s)", schemaCheckExpr(col.CheckStr))
		}
		columns[i] = line
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "CREATE FOREIGN TABLE %s (\n  %s\n) WRAPPER %s",
		schemaIdentifier(ft.TableName, true),
		strings.Join(columns, ",\n  "),
		schemaStringLiteral(ft.Wrapper))
	if len(ft.Options) > 0 {
		keys := make([]string, 0, len(ft.Options))
		for key := range ft.Options {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		options := make([]string, len(keys))
		for i, key := range keys {
			options[i] = fmt.Sprintf("%s %s", schemaIdentifier(key, true), schemaStringLiteral(ft.Options[key]))
		}
		fmt.Fprintf(&sb, " OPTIONS (%s)", strings.Join(options, ", "))
	}
	sb.WriteString(";")
	return sb.String()
}

func schemaStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// ViewDDL returns persisted CREATE VIEW statements for SQL dumps.
func (db *DB) ViewDDL() []string {
	views := db.catalog.ListViewSQL()
	names := make([]string, 0, len(views))
	for name := range views {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	ddl := make([]string, 0, len(names))
	for _, name := range names {
		sql := strings.TrimSpace(views[name])
		if sql == "" {
			continue
		}
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		ddl = append(ddl, sql)
	}
	return ddl
}

// MaterializedViewDDL returns persisted CREATE MATERIALIZED VIEW statements for SQL dumps.
func (db *DB) MaterializedViewDDL() []string {
	views := db.catalog.ListMaterializedViewSQL()
	names := make([]string, 0, len(views))
	for name := range views {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	ddl := make([]string, 0, len(names))
	for _, name := range names {
		sql := strings.TrimSpace(views[name])
		if sql == "" {
			continue
		}
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		ddl = append(ddl, sql)
	}
	return ddl
}

// TriggerDDL returns persisted CREATE TRIGGER statements for SQL dumps.
func (db *DB) TriggerDDL() []string {
	triggers := db.catalog.ListTriggerSQL()
	names := make([]string, 0, len(triggers))
	for name := range triggers {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	ddl := make([]string, 0, len(names))
	for _, name := range names {
		sql := strings.TrimSpace(triggers[name])
		if sql == "" {
			continue
		}
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		ddl = append(ddl, sql)
	}
	return ddl
}

// ProcedureDDL returns persisted CREATE PROCEDURE statements for SQL dumps.
func (db *DB) ProcedureDDL() []string {
	procedures := db.catalog.ListProcedureSQL()
	names := make([]string, 0, len(procedures))
	for name := range procedures {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	ddl := make([]string, 0, len(names))
	for _, name := range names {
		sql := strings.TrimSpace(procedures[name])
		if sql == "" {
			continue
		}
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		ddl = append(ddl, sql)
	}
	return ddl
}

// Begin starts a new transaction

func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	return db.BeginWith(ctx, nil)
}

// BeginWith starts a new transaction with options

func (db *DB) BeginWith(ctx context.Context, opts *txn.Options) (*Tx, error) {
	// Acquire connection
	if err := db.acquireConnection(ctx); err != nil {
		return nil, err
	}

	if db.closed.Load() {
		db.releaseConnection()
		return nil, ErrDatabaseClosed
	}

	transaction := db.txnMgr.Begin(opts)

	// Begin transaction in catalog for WAL logging.
	// Pass the engine's manager transaction so the catalog shares the same
	// txn state for MVCC conflict detection instead of creating a duplicate.
	db.catalog.BeginTransactionWithTxn(transaction.ID, transaction)

	return acquireTx(db, transaction), nil
}

// auditUser extracts the username from context for audit logging.

func auditUser(ctx context.Context) string {
	if ctx == nil {
		return "db_user"
	}
	if user, ok := ctx.Value(security.RLSUserKey).(string); ok && user != "" {
		return user
	}
	if user, ok := ctx.Value("cobaltdb_user").(string); ok && user != "" {
		return user
	}
	return "db_user"
}

// dispatchDDL executes a DDL handler, then logs audit and replicates on success.
// isSchemaDDL reports whether a statement changes the catalog schema and so
// should trigger a schema flush to disk for crash durability.
func isSchemaDDL(stmt query.Statement) bool {
	switch stmt.(type) {
	case *query.CreateTableStmt, *query.CreateForeignTableStmt, *query.DropTableStmt,
		*query.CreateIndexStmt, *query.DropIndexStmt, *query.AlterTableStmt,
		*query.CreateViewStmt, *query.DropViewStmt:
		return true
	}
	return false
}

// persistSchema writes the catalog schema to the root B+Tree and flushes it to
// disk. Without this, schema created before the first checkpoint (clean Close or
// the background flusher) is lost on an unclean shutdown, leaving the database
// unopenable and its WAL data un-replayable. It is best-effort: a flush error is
// returned to the caller but does not roll back the already-committed DDL.
func (db *DB) persistSchema() error {
	if db.path == ":memory:" || db.catalog == nil || db.pool == nil {
		return nil
	}
	if err := db.catalog.Save(); err != nil {
		return err
	}
	return db.pool.FlushDirty()
}

func (db *DB) dispatchDDL(ctx context.Context, action, table string, handler func() (Result, error), opts ...audit.LogOption) (Result, error) {
	result, err := handler()
	if db.auditLogger != nil {
		db.auditLogger.Log(audit.EventDDL, auditUser(ctx), action, opts...)
	}
	if err == nil {
		if replErr := db.replicateWrite(action, table, nil); replErr != nil {
			return result, replErr
		}
	}
	return result, err
}

// execute executes a statement

func (db *DB) execute(ctx context.Context, stmt query.Statement, args []interface{}) (result Result, err error) {
	start := time.Now()

	// Flush the catalog schema to disk after a successful DDL so it survives an
	// unclean shutdown before the first checkpoint. Registered before the
	// autocommit defer below so it runs *after* the commit (defers are LIFO).
	if isSchemaDDL(stmt) {
		defer func() {
			if err == nil {
				if ferr := db.persistSchema(); ferr != nil {
					err = fmt.Errorf("persist schema: %w", ferr)
				}
			}
		}()
	}

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
		return db.dispatchDDL(ctx, "CREATE_TABLE", s.Table, func() (Result, error) { return db.executeCreateTable(ctx, s) }, audit.WithTable(s.Table))
	case *query.CreateForeignTableStmt:
		result, err := db.executeCreateForeignTable(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "CREATE_FOREIGN_TABLE", audit.WithTable(s.Table))
		}
		return result, err
	case *query.CreateCollectionStmt:
		return db.dispatchDDL(ctx, "CREATE_COLLECTION", s.Name, func() (Result, error) { return db.executeCreateCollection(ctx, s) }, audit.WithTable(s.Name))
	case *query.InsertStmt:
		explicitTxnActive := db.catalog.IsTransactionActive() && !autocommit
		result, err := db.executeInsert(ctx, s, args)
		if err != nil && s.ConflictAction == query.ConflictRollback && explicitTxnActive {
			if rbErr := db.catalog.RollbackTransaction(); rbErr != nil {
				err = fmt.Errorf("%w; rollback failed: %v", err, rbErr)
			}
		}
		if db.auditLogger != nil {
			db.auditLogger.LogQuery(auditUser(ctx), "INSERT", time.Since(start), result.RowsAffected, err)
		}
		if err == nil {
			if replErr := db.replicateWrite("INSERT", s.Table, args); replErr != nil {
				return result, replErr
			}
		}
		return result, err
	case *query.UpdateStmt:
		result, err := db.executeUpdate(ctx, s, args)
		if db.auditLogger != nil {
			db.auditLogger.LogQuery(auditUser(ctx), "UPDATE", time.Since(start), result.RowsAffected, err)
		}
		if err == nil {
			if replErr := db.replicateWrite("UPDATE", s.Table, args); replErr != nil {
				return result, replErr
			}
		}
		return result, err
	case *query.DeleteStmt:
		result, err := db.executeDelete(ctx, s, args)
		if db.auditLogger != nil {
			db.auditLogger.LogQuery(auditUser(ctx), "DELETE", time.Since(start), result.RowsAffected, err)
		}
		if err == nil {
			if replErr := db.replicateWrite("DELETE", s.Table, args); replErr != nil {
				return result, replErr
			}
		}
		return result, err
	case *query.DropTableStmt:
		return db.dispatchDDL(ctx, "DROP_TABLE", s.Table, func() (Result, error) { return db.executeDropTable(ctx, s) }, audit.WithTable(s.Table))
	case *query.DropCollectionStmt:
		return db.dispatchDDL(ctx, "DROP_COLLECTION", s.Name, func() (Result, error) { return db.executeDropCollection(ctx, s) }, audit.WithTable(s.Name))
	case *query.CreateIndexStmt:
		return db.dispatchDDL(ctx, "CREATE_INDEX", s.Table, func() (Result, error) { return db.executeCreateIndex(ctx, s) }, audit.WithTable(s.Table))
	case *query.CreateViewStmt:
		return db.dispatchDDL(ctx, "CREATE_VIEW", s.Name, func() (Result, error) { return db.executeCreateView(ctx, s) }, audit.WithTable(s.Name))
	case *query.DropViewStmt:
		result, err := db.executeDropView(ctx, s)
		if db.auditLogger != nil {
			db.auditLogger.Log(audit.EventDDL, auditUser(ctx), "DROP_VIEW", audit.WithTable(s.Name))
		}
		return result, err
	case *query.CreateTriggerStmt:
		return db.dispatchDDL(ctx, "CREATE_TRIGGER", s.Table, func() (Result, error) { return db.executeCreateTrigger(ctx, s) }, audit.WithTable(s.Table))
	case *query.DropTriggerStmt:
		return db.dispatchDDL(ctx, "DROP_TRIGGER", "", func() (Result, error) { return db.executeDropTrigger(ctx, s) })
	case *query.CreateProcedureStmt:
		return db.dispatchDDL(ctx, "CREATE_PROCEDURE", "", func() (Result, error) { return db.executeCreateProcedure(ctx, s) })
	case *query.DropProcedureStmt:
		return db.dispatchDDL(ctx, "DROP_PROCEDURE", "", func() (Result, error) { return db.executeDropProcedure(ctx, s) })
	case *query.CreatePolicyStmt:
		return db.dispatchDDL(ctx, "CREATE_POLICY", s.Table, func() (Result, error) { return db.executeCreatePolicy(ctx, s) }, audit.WithTable(s.Table))
	case *query.DropPolicyStmt:
		return db.dispatchDDL(ctx, "DROP_POLICY", "", func() (Result, error) { return db.executeDropPolicy(ctx, s) })
	case *query.CallProcedureStmt:
		return db.dispatchDDL(ctx, "CALL_PROCEDURE", "", func() (Result, error) { return db.executeCallProcedure(ctx, s, args) })
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
		if _, err := db.catalog.GetVectorIndex(s.Index); err == nil {
			if err := db.catalog.DropVectorIndex(s.Index); err != nil {
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
	case *query.ShowIndexStmt:
		return db.executeShowIndexQuery(ctx, s)
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
	case *query.CallProcedureStmt:
		return db.queryCallProcedure(ctx, s, args)
	default:
		return nil, fmt.Errorf("not a query statement: %T", stmt)
	}
}

// executeCreateTable executes CREATE TABLE

func (db *DB) executeCreateTable(ctx context.Context, stmt *query.CreateTableStmt) (Result, error) {
	if stmt.AsSelect != nil {
		return db.executeCreateTableAsSelect(ctx, stmt)
	}
	if stmt.IfNotExists {
		if _, err := db.catalog.GetTable(stmt.Table); err == nil {
			return Result{RowsAffected: 0}, nil
		}
	}
	cleanupOnError := func(primary error) error {
		if cleanupErr := db.catalog.CleanupFailedCreateTable(stmt.Table); cleanupErr != nil {
			return fmt.Errorf("%w; cleanup failed: %v", primary, cleanupErr)
		}
		return primary
	}
	if err := db.catalog.CreateTable(stmt); err != nil {
		return Result{}, err
	}
	// Named column-level UNIQUE constraints are represented as unique indexes so
	// ALTER TABLE ... DROP CONSTRAINT removes enforcement by dropping the index.
	for _, col := range stmt.Columns {
		if col.UniqueName == "" {
			continue
		}
		idx := &query.CreateIndexStmt{Index: col.UniqueName, Table: stmt.Table, Columns: []string{col.Name}, Unique: true}
		if err := db.catalog.CreateIndex(idx); err != nil {
			return Result{}, cleanupOnError(fmt.Errorf("creating unique constraint %s: %w", col.UniqueName, err))
		}
	}
	// Table-level UNIQUE (col, ...) constraints are enforced via unique indexes.
	for i, cols := range stmt.UniqueConstraints {
		idxName := fmt.Sprintf("%s_uniq_%d", stmt.Table, i)
		idx := &query.CreateIndexStmt{Index: idxName, Table: stmt.Table, Columns: cols, Unique: true, IfNotExists: true}
		if err := db.catalog.CreateIndex(idx); err != nil {
			return Result{}, cleanupOnError(fmt.Errorf("creating unique constraint index: %w", err))
		}
	}
	for _, constraint := range stmt.NamedUniqueConstraints {
		idx := &query.CreateIndexStmt{Index: constraint.Name, Table: stmt.Table, Columns: constraint.Columns, Unique: true}
		if err := db.catalog.CreateIndex(idx); err != nil {
			return Result{}, cleanupOnError(fmt.Errorf("creating unique constraint %s: %w", constraint.Name, err))
		}
	}
	return Result{RowsAffected: 0}, nil
}

func (db *DB) executeCreateCollection(ctx context.Context, stmt *query.CreateCollectionStmt) (Result, error) {
	if err := db.catalog.CreateCollection(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateTableAsSelect implements CREATE TABLE ... AS SELECT (CTAS):
// materialize the query, infer column types, create the table, insert the rows.
func (db *DB) executeCreateTableAsSelect(ctx context.Context, stmt *query.CreateTableStmt) (Result, error) {
	rows, err := db.query(ctx, stmt.AsSelect, nil)
	if err != nil {
		return Result{}, err
	}
	cols := rows.Columns()
	var data [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			_ = rows.Close()
			return Result{}, err
		}
		data = append(data, vals)
	}
	if err := rows.Close(); err != nil {
		return Result{}, err
	}

	colDefs := make([]*query.ColumnDef, len(cols))
	for i, name := range cols {
		colDefs[i] = &query.ColumnDef{Name: name, Type: inferCTASColumnType(data, i)}
	}
	createStmt := &query.CreateTableStmt{Table: stmt.Table, IfNotExists: stmt.IfNotExists, Columns: colDefs}
	if err := db.catalog.CreateTable(createStmt); err != nil {
		return Result{}, err
	}

	inserted := int64(0)
	for _, row := range data {
		valExprs := make([]query.Expression, len(row))
		for j, v := range row {
			valExprs[j] = valueToLiteralExpr(v)
		}
		ins := &query.InsertStmt{Table: stmt.Table, Values: [][]query.Expression{valExprs}}
		if _, n, err := db.catalog.Insert(ctx, ins, nil); err != nil {
			return Result{}, fmt.Errorf("CTAS insert: %w", err)
		} else {
			inserted += n
		}
	}
	return Result{RowsAffected: inserted}, nil
}

// inferCTASColumnType picks a column type for CTAS from the materialized values.
func inferCTASColumnType(data [][]interface{}, col int) query.TokenType {
	allInt, allNum, sawVal := true, true, false
	for _, row := range data {
		if col >= len(row) || row[col] == nil {
			continue
		}
		sawVal = true
		switch row[col].(type) {
		case int, int64:
		case float64:
			allInt = false
		default:
			allInt = false
			allNum = false
		}
	}
	switch {
	case !sawVal:
		return query.TokenText
	case allInt:
		return query.TokenInteger
	case allNum:
		return query.TokenReal
	default:
		return query.TokenText
	}
}

// valueToLiteralExpr wraps a Go value as the equivalent literal AST node.
func valueToLiteralExpr(v interface{}) query.Expression {
	switch val := v.(type) {
	case nil:
		return &query.NullLiteral{}
	case int:
		return &query.NumberLiteral{Value: float64(val)}
	case int64:
		return &query.NumberLiteral{Value: float64(val)}
	case float64:
		return &query.NumberLiteral{Value: val}
	case bool:
		return &query.BooleanLiteral{Value: val}
	case string:
		return &query.StringLiteral{Value: val}
	default:
		return &query.StringLiteral{Value: fmt.Sprintf("%v", val)}
	}
}

func (db *DB) executeCreateForeignTable(ctx context.Context, stmt *query.CreateForeignTableStmt) (Result, error) {
	if err := db.catalog.CreateForeignTable(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeInsert executes INSERT

func (db *DB) executeInsert(ctx context.Context, stmt *query.InsertStmt, args []interface{}) (Result, error) {
	if stmt.OnConflict != nil && stmt.OnConflict.DoUpdate != nil {
		return db.executeUpsert(ctx, stmt, args)
	}
	lastInsertID, rowsAffected, err := db.catalog.Insert(ctx, stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
}

// executeUpsert implements INSERT ... ON CONFLICT (...) DO UPDATE SET ... by
// attempting a per-row insert and, on a unique/primary-key conflict, applying
// the UPDATE assignments to the conflicting row. Safe under the catalog's
// single-writer model (the conflict check-then-act holds while we run).
func (db *DB) executeUpsert(ctx context.Context, stmt *query.InsertStmt, args []interface{}) (Result, error) {
	table, err := db.catalog.GetTable(stmt.Table)
	if err != nil {
		return Result{}, err
	}

	// Source rows come from VALUES, or from a materialized INSERT ... SELECT.
	valueRows := stmt.Values
	if stmt.Select != nil {
		rows, qerr := db.query(ctx, stmt.Select, args)
		if qerr != nil {
			return Result{}, qerr
		}
		cols := rows.Columns()
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if scanErr := rows.Scan(ptrs...); scanErr != nil {
				_ = rows.Close()
				return Result{}, scanErr
			}
			exprs := make([]query.Expression, len(vals))
			for j, v := range vals {
				exprs[j] = valueToLiteralExpr(v)
			}
			valueRows = append(valueRows, exprs)
		}
		if cerr := rows.Close(); cerr != nil {
			return Result{}, cerr
		}
	}

	// Resolve conflict target columns. Explicit ON CONFLICT targets use that
	// one target; MySQL-style ON DUPLICATE KEY UPDATE has no target, so try the
	// primary key followed by known UNIQUE constraints/indexes.
	conflictTargets := [][]string{stmt.OnConflict.Columns}
	if len(stmt.OnConflict.Columns) == 0 {
		conflictTargets = db.upsertConflictTargets(stmt.Table, table)
	}
	if len(conflictTargets) == 0 {
		return Result{}, fmt.Errorf("ON CONFLICT DO UPDATE requires a conflict target, primary key, or unique key")
	}

	// Map column name -> position within each inserted value row.
	colPos := make(map[string]int)
	if len(stmt.Columns) > 0 {
		for i, c := range stmt.Columns {
			colPos[strings.ToLower(c)] = i
		}
	} else {
		for i, c := range table.Columns {
			colPos[strings.ToLower(c.Name)] = i
		}
	}

	var result Result
	for _, row := range valueRows {
		single := &query.InsertStmt{Table: stmt.Table, Columns: stmt.Columns, Values: [][]query.Expression{row}}
		lastID, n, insErr := db.catalog.Insert(ctx, single, args)
		if insErr == nil {
			result.RowsAffected += n
			result.LastInsertID = lastID
			continue
		}
		if !isUniqueConflictError(insErr) {
			return Result{}, insErr
		}

		updated := false
		for _, conflictCols := range conflictTargets {
			// Build WHERE matching the conflict target to this row's values,
			// reusing the row's own value expressions directly.
			var where query.Expression
			missingTarget := ""
			for _, cc := range conflictCols {
				pos, ok := colPos[strings.ToLower(cc)]
				if !ok || pos >= len(row) {
					missingTarget = cc
					break
				}
				eq := &query.BinaryExpr{Left: &query.Identifier{Name: cc}, Operator: query.TokenEq, Right: row[pos]}
				if where == nil {
					where = eq
				} else {
					where = &query.BinaryExpr{Left: where, Operator: query.TokenAnd, Right: eq}
				}
			}
			if missingTarget != "" {
				if len(stmt.OnConflict.Columns) > 0 {
					return Result{}, fmt.Errorf("ON CONFLICT target column %q not present in inserted values", missingTarget)
				}
				continue
			}

			updateSet, substErr := substituteUpsertValuesInSetClauses(stmt.OnConflict.DoUpdate, colPos, row)
			if substErr != nil {
				return Result{}, substErr
			}
			upd := &query.UpdateStmt{Table: stmt.Table, Set: updateSet, Where: where}
			_, n, updErr := db.catalog.Update(ctx, upd, args)
			if updErr != nil {
				return Result{}, updErr
			}
			if n > 0 {
				result.RowsAffected += n
				updated = true
				break
			}
		}
		if !updated {
			return Result{}, insErr
		}
	}
	return result, nil
}

func (db *DB) upsertConflictTargets(tableName string, table *catalog.TableDef) [][]string {
	var targets [][]string
	seen := make(map[string]bool)
	add := func(cols []string) {
		if len(cols) == 0 {
			return
		}
		keyParts := make([]string, len(cols))
		for i, col := range cols {
			keyParts[i] = strings.ToLower(col)
		}
		key := strings.Join(keyParts, "\x00")
		if seen[key] {
			return
		}
		seen[key] = true
		targets = append(targets, append([]string(nil), cols...))
	}

	add(table.PrimaryKey)
	for _, col := range table.Columns {
		if col.Unique {
			add([]string{col.Name})
		}
	}
	for _, idx := range db.catalog.GetTableIndexes(tableName) {
		if idx.Unique {
			add(idx.Columns)
		}
	}
	return targets
}

func substituteUpsertValuesInSetClauses(set []*query.SetClause, colPos map[string]int, row []query.Expression) ([]*query.SetClause, error) {
	result := make([]*query.SetClause, len(set))
	for i, clause := range set {
		newClause := *clause
		value, err := substituteUpsertValuesExpr(clause.Value, colPos, row)
		if err != nil {
			return nil, err
		}
		newClause.Value = value
		result[i] = &newClause
	}
	return result, nil
}

func substituteUpsertValuesExpr(expr query.Expression, colPos map[string]int, row []query.Expression) (query.Expression, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *query.FunctionCall:
		if strings.EqualFold(e.Name, "VALUES") {
			if len(e.Args) != 1 {
				return nil, fmt.Errorf("VALUES() in ON DUPLICATE KEY UPDATE requires one column argument")
			}
			col, ok := upsertValuesColumnName(e.Args[0])
			if !ok {
				return nil, fmt.Errorf("VALUES() in ON DUPLICATE KEY UPDATE requires a column argument")
			}
			pos, ok := colPos[strings.ToLower(col)]
			if !ok || pos >= len(row) {
				return nil, fmt.Errorf("VALUES(%s) column not present in inserted values", col)
			}
			return query.CloneExpression(row[pos]), nil
		}
		args := make([]query.Expression, len(e.Args))
		for i, arg := range e.Args {
			v, err := substituteUpsertValuesExpr(arg, colPos, row)
			if err != nil {
				return nil, err
			}
			args[i] = v
		}
		orderBy, err := substituteUpsertValuesOrderBy(e.OrderBy, colPos, row)
		if err != nil {
			return nil, err
		}
		filter, err := substituteUpsertValuesExpr(e.Filter, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.FunctionCall{Name: e.Name, Args: args, Distinct: e.Distinct, OrderBy: orderBy, Filter: filter}, nil
	case *query.BinaryExpr:
		left, err := substituteUpsertValuesExpr(e.Left, colPos, row)
		if err != nil {
			return nil, err
		}
		right, err := substituteUpsertValuesExpr(e.Right, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.BinaryExpr{Left: left, Operator: e.Operator, Right: right}, nil
	case *query.UnaryExpr:
		v, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.UnaryExpr{Operator: e.Operator, Expr: v}, nil
	case *query.CaseExpr:
		newCase := &query.CaseExpr{}
		var err error
		if e.Expr != nil {
			newCase.Expr, err = substituteUpsertValuesExpr(e.Expr, colPos, row)
			if err != nil {
				return nil, err
			}
		}
		newCase.Whens = make([]*query.WhenClause, len(e.Whens))
		for i, when := range e.Whens {
			cond, err := substituteUpsertValuesExpr(when.Condition, colPos, row)
			if err != nil {
				return nil, err
			}
			res, err := substituteUpsertValuesExpr(when.Result, colPos, row)
			if err != nil {
				return nil, err
			}
			newCase.Whens[i] = &query.WhenClause{Condition: cond, Result: res}
		}
		if e.Else != nil {
			newCase.Else, err = substituteUpsertValuesExpr(e.Else, colPos, row)
			if err != nil {
				return nil, err
			}
		}
		return newCase, nil
	case *query.BetweenExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		lower, err := substituteUpsertValuesExpr(e.Lower, colPos, row)
		if err != nil {
			return nil, err
		}
		upper, err := substituteUpsertValuesExpr(e.Upper, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.BetweenExpr{Expr: ex, Lower: lower, Upper: upper, Not: e.Not}, nil
	case *query.InExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		list := make([]query.Expression, len(e.List))
		for i, item := range e.List {
			v, err := substituteUpsertValuesExpr(item, colPos, row)
			if err != nil {
				return nil, err
			}
			list[i] = v
		}
		return &query.InExpr{Expr: ex, List: list, Not: e.Not, Subquery: e.Subquery}, nil
	case *query.LikeExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		pattern, err := substituteUpsertValuesExpr(e.Pattern, colPos, row)
		if err != nil {
			return nil, err
		}
		escape, err := substituteUpsertValuesExpr(e.Escape, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.LikeExpr{Expr: ex, Pattern: pattern, Not: e.Not, Escape: escape}, nil
	case *query.IsNullExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.IsNullExpr{Expr: ex, Not: e.Not}, nil
	case *query.CastExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.CastExpr{Expr: ex, DataType: e.DataType}, nil
	case *query.AliasExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.AliasExpr{Expr: ex, Alias: e.Alias}, nil
	case *query.JSONPathExpr:
		col, err := substituteUpsertValuesExpr(e.Column, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.JSONPathExpr{Column: col, Path: e.Path, AsText: e.AsText}, nil
	case *query.JSONContainsExpr:
		col, err := substituteUpsertValuesExpr(e.Column, colPos, row)
		if err != nil {
			return nil, err
		}
		val, err := substituteUpsertValuesExpr(e.Value, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.JSONContainsExpr{Column: col, Value: val}, nil
	default:
		return query.CloneExpression(expr), nil
	}
}

func substituteUpsertValuesOrderBy(orderBy []*query.OrderByExpr, colPos map[string]int, row []query.Expression) ([]*query.OrderByExpr, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}
	out := make([]*query.OrderByExpr, len(orderBy))
	for i, ob := range orderBy {
		if ob == nil {
			continue
		}
		expr, err := substituteUpsertValuesExpr(ob.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		copied := *ob
		copied.Expr = expr
		out[i] = &copied
	}
	return out, nil
}

func upsertValuesColumnName(expr query.Expression) (string, bool) {
	switch e := expr.(type) {
	case *query.Identifier:
		return e.Name, true
	case *query.QualifiedIdentifier:
		return e.Column, true
	case *query.ColumnRef:
		return e.Column, e.Column != ""
	default:
		return "", false
	}
}

// isUniqueConflictError reports whether err is a primary-key/unique violation
// from the insert path (used to trigger ON CONFLICT DO UPDATE).
func isUniqueConflictError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "UNIQUE constraint failed") || strings.Contains(s, "duplicate primary key")
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
		if stmt.Column.UniqueName != "" {
			idx := &query.CreateIndexStmt{
				Index:   stmt.Column.UniqueName,
				Table:   stmt.Table,
				Columns: []string{stmt.Column.Name},
				Unique:  true,
			}
			if err := db.catalog.CreateIndex(idx); err != nil {
				return Result{}, fmt.Errorf("creating unique constraint %s: %w", stmt.Column.UniqueName, err)
			}
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
	case "ADD_CONSTRAINT":
		if strings.EqualFold(stmt.ConstraintType, "FOREIGN KEY") {
			if err := db.catalog.AlterTableAddForeignKeyConstraint(ctx, stmt); err != nil {
				return Result{}, err
			}
			break
		}
		if strings.EqualFold(stmt.ConstraintType, "CHECK") {
			if err := db.catalog.AlterTableAddCheckConstraint(stmt); err != nil {
				return Result{}, err
			}
			break
		}
		if !strings.EqualFold(stmt.ConstraintType, "UNIQUE") {
			return Result{}, fmt.Errorf("unsupported ALTER TABLE constraint type: %s", stmt.ConstraintType)
		}
		idx := &query.CreateIndexStmt{
			Index:   stmt.ConstraintName,
			Table:   stmt.Table,
			Columns: stmt.ConstraintColumns,
			Unique:  true,
		}
		if err := db.catalog.CreateIndex(idx); err != nil {
			return Result{}, err
		}
	case "DROP_CONSTRAINT":
		if err := db.catalog.DropTableConstraint(stmt.Table, stmt.ConstraintName); err != nil {
			return Result{}, err
		}
	case "ENABLE_RLS":
		if err := db.catalog.EnableRLSTable(stmt.Table); err != nil {
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

func (db *DB) executeDropCollection(ctx context.Context, stmt *query.DropCollectionStmt) (Result, error) {
	if err := db.catalog.DropCollection(stmt); err != nil {
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
	viewQuery, err := applyCreateViewColumnList(stmt)
	if err != nil {
		return Result{}, err
	}
	if stmt.OrReplace {
		var err error
		if stmt.Temporary {
			err = db.catalog.CreateOrReplaceTemporaryViewSQL(stmt.Name, viewQuery, stmt.RawSQL)
		} else {
			err = db.catalog.CreateOrReplaceViewSQL(stmt.Name, viewQuery, stmt.RawSQL)
		}
		if err != nil {
			return Result{}, err
		}
		return Result{RowsAffected: 0}, nil
	}
	if stmt.Temporary {
		err = db.catalog.CreateTemporaryViewSQL(stmt.Name, viewQuery, stmt.RawSQL)
	} else {
		err = db.catalog.CreateViewSQL(stmt.Name, viewQuery, stmt.RawSQL)
	}
	if err != nil {
		if stmt.IfNotExists {
			return Result{RowsAffected: 0}, nil
		}
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

func applyCreateViewColumnList(stmt *query.CreateViewStmt) (*query.SelectStmt, error) {
	if stmt == nil || len(stmt.Columns) == 0 {
		if stmt == nil {
			return nil, fmt.Errorf("nil CREATE VIEW statement")
		}
		return stmt.Query, nil
	}
	if stmt.Query == nil {
		return nil, fmt.Errorf("CREATE VIEW %s has no query", stmt.Name)
	}
	if len(stmt.Columns) != len(stmt.Query.Columns) {
		return nil, fmt.Errorf("view column list has %d columns but query returns %d columns", len(stmt.Columns), len(stmt.Query.Columns))
	}
	viewQuery := *stmt.Query
	viewQuery.Columns = make([]query.Expression, len(stmt.Query.Columns))
	for i, col := range stmt.Query.Columns {
		if alias, ok := col.(*query.AliasExpr); ok {
			viewQuery.Columns[i] = &query.AliasExpr{Expr: alias.Expr, Alias: stmt.Columns[i]}
			continue
		}
		viewQuery.Columns[i] = &query.AliasExpr{Expr: col, Alias: stmt.Columns[i]}
	}
	return &viewQuery, nil
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
	if err := db.catalog.CreateTriggerSQL(stmt, stmt.RawSQL); err != nil {
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
	if err := db.catalog.CreateProcedureSQL(stmt, stmt.RawSQL); err != nil {
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
	if _, err := db.catalog.GetTable(stmt.Table); err != nil {
		return Result{}, err
	}

	// Convert Event string to PolicyType
	var policyType security.PolicyType
	switch toUpperFast(stmt.Event) {
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
	checkExpr := ""
	if stmt.WithCheck != nil {
		checkExpr = expressionToString(stmt.WithCheck)
	}
	if usingExpr == "" {
		usingExpr = "TRUE" // Default to allowing all if no expression
	}

	// Create the policy
	policy := &security.Policy{
		Name:            stmt.Name,
		TableName:       stmt.Table,
		Type:            policyType,
		Expression:      usingExpr,
		CheckExpression: checkExpr,
		Restrictive:     !stmt.Permissive,
		Users:           nil, // Could be extracted from ForRoles
		Roles:           stmt.ForRoles,
		Enabled:         true,
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
	result, _, _, _, err := db.runCallProcedure(ctx, stmt, args, false)
	return result, err
}

func (db *DB) queryCallProcedure(ctx context.Context, stmt *query.CallProcedureStmt, args []interface{}) (*Rows, error) {
	_, resultRows, columns, values, err := db.runCallProcedure(ctx, stmt, args, true)
	if err != nil {
		return nil, err
	}
	if resultRows != nil {
		return resultRows, nil
	}
	if len(columns) == 0 {
		return &Rows{columns: []string{}, rows: [][]interface{}{}}, nil
	}
	return &Rows{columns: columns, rows: [][]interface{}{values}}, nil
}

func (db *DB) runCallProcedure(ctx context.Context, stmt *query.CallProcedureStmt, args []interface{}, captureResultRows bool) (Result, *Rows, []string, []interface{}, error) {
	// Get the procedure from catalog
	proc, err := db.catalog.GetProcedure(stmt.Name)
	if err != nil {
		return Result{}, nil, nil, nil, err
	}

	// Map procedure parameters to call arguments
	paramMap := make(map[string]interface{})
	paramDefs := make(map[string]*query.ParamDef)
	for _, param := range proc.Params {
		paramDefs[param.Name] = param
	}
	if err := evaluateProcedureCallArgs(stmt, proc, args, paramMap); err != nil {
		return Result{}, nil, nil, nil, err
	}

	var totalRowsAffected int64
	var resultRows *Rows
	for _, bodyStmt := range proc.Body {
		if setStmt, ok := bodyStmt.(*query.SetVarStmt); ok && isProcedureOutputParam(setStmt.Variable, paramDefs) {
			val, err := evalProcedureSetValue(setStmt.Value, paramMap)
			if err != nil {
				return Result{}, nil, nil, nil, fmt.Errorf("setting OUT parameter %s: %w", setStmt.Variable, err)
			}
			paramMap[strings.TrimSpace(setStmt.Variable)] = val
			continue
		}
		if captureResultRows && isProcedureResultStatement(bodyStmt) {
			substitutedStmt := substituteParamsInStatement(bodyStmt, paramMap)
			rows, err := db.query(ctx, substitutedStmt, nil)
			if err != nil {
				return Result{}, nil, nil, nil, err
			}
			if resultRows != nil {
				_ = resultRows.Close()
			}
			resultRows = rows
			continue
		}
		result, err := db.executeWithParams(ctx, bodyStmt, paramMap)
		if err != nil {
			if resultRows != nil {
				_ = resultRows.Close()
			}
			return Result{}, nil, nil, nil, err
		}
		totalRowsAffected += result.RowsAffected
	}

	outColumns, outValues := procedureOutputValues(proc.Params, paramMap)
	return Result{RowsAffected: totalRowsAffected}, resultRows, outColumns, outValues, nil
}

func evaluateProcedureCallArgs(stmt *query.CallProcedureStmt, proc *query.CreateProcedureStmt, args []interface{}, paramMap map[string]interface{}) error {
	callArgs := stmt.Args
	if len(callArgs) == 0 && len(stmt.Params) > 0 {
		callArgs = make([]query.CallArg, len(stmt.Params))
		for i, paramExpr := range stmt.Params {
			callArgs[i] = query.CallArg{Expr: paramExpr}
		}
	}
	if len(callArgs) == 0 && len(args) > 0 {
		if len(args) != len(proc.Params) {
			return fmt.Errorf("procedure %s expects %d arguments, got %d", proc.Name, len(proc.Params), len(args))
		}
		for i, param := range proc.Params {
			paramMap[param.Name] = args[i]
		}
		return nil
	}
	if len(callArgs) != len(proc.Params) {
		return fmt.Errorf("procedure %s expects %d arguments, got %d", proc.Name, len(proc.Params), len(callArgs))
	}

	procParamByName := make(map[string]*query.ParamDef, len(proc.Params))
	for _, param := range proc.Params {
		procParamByName[strings.ToLower(param.Name)] = param
	}
	nextPositional := 0
	for _, callArg := range callArgs {
		if callArg.Expr == nil {
			return fmt.Errorf("procedure %s has nil call argument", proc.Name)
		}
		val, err := catalog.EvalExpression(callArg.Expr, args)
		if err != nil {
			return fmt.Errorf("evaluating procedure argument: %w", err)
		}
		if callArg.Name == "" {
			if nextPositional >= len(proc.Params) {
				return fmt.Errorf("procedure %s expects %d arguments, got too many positional arguments", proc.Name, len(proc.Params))
			}
			target := proc.Params[nextPositional]
			if _, exists := paramMap[target.Name]; exists {
				return fmt.Errorf("procedure %s argument %s assigned more than once", proc.Name, target.Name)
			}
			paramMap[target.Name] = val
			nextPositional++
			continue
		}

		target := procParamByName[strings.ToLower(strings.TrimSpace(callArg.Name))]
		if target == nil {
			return fmt.Errorf("procedure %s has no parameter named %s", proc.Name, callArg.Name)
		}
		if _, exists := paramMap[target.Name]; exists {
			return fmt.Errorf("procedure %s argument %s assigned more than once", proc.Name, target.Name)
		}
		paramMap[target.Name] = val
	}
	for _, param := range proc.Params {
		if _, exists := paramMap[param.Name]; !exists {
			return fmt.Errorf("procedure %s missing argument %s", proc.Name, param.Name)
		}
	}
	return nil
}

func isProcedureResultStatement(stmt query.Statement) bool {
	switch stmt.(type) {
	case *query.SelectStmt, *query.UnionStmt, *query.SelectStmtWithCTE,
		*query.ShowTablesStmt, *query.ShowCreateTableStmt, *query.ShowColumnsStmt,
		*query.ShowDatabasesStmt, *query.DescribeStmt, *query.ExplainStmt:
		return true
	default:
		return false
	}
}

func isProcedureOutputParam(name string, params map[string]*query.ParamDef) bool {
	param := params[strings.TrimSpace(name)]
	if param == nil {
		return false
	}
	return param.Mode == query.TokenOut || param.Mode == query.TokenInout
}

func procedureOutputValues(params []*query.ParamDef, paramMap map[string]interface{}) ([]string, []interface{}) {
	var columns []string
	var values []interface{}
	for _, param := range params {
		if param == nil || (param.Mode != query.TokenOut && param.Mode != query.TokenInout) {
			continue
		}
		columns = append(columns, param.Name)
		values = append(values, paramMap[param.Name])
	}
	return columns, values
}

func evalProcedureSetValue(valueSQL string, paramMap map[string]interface{}) (interface{}, error) {
	valueSQL = strings.TrimSpace(valueSQL)
	if valueSQL == "" {
		return nil, errors.New("empty SET value")
	}
	stmt, err := query.Parse("SELECT " + valueSQL)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*query.SelectStmt)
	if !ok || len(selectStmt.Columns) == 0 {
		return nil, fmt.Errorf("SET value did not parse as a scalar expression")
	}
	expr := substituteParamsInExpr(selectStmt.Columns[0], paramMap)
	return catalog.EvalExpression(expr, nil)
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
	case *query.SelectStmt:
		return substituteParamsInSelectStmt(s, paramMap)
	default:
		return stmt
	}
}

func substituteParamsInSelectStmt(stmt *query.SelectStmt, paramMap map[string]interface{}) *query.SelectStmt {
	if stmt == nil {
		return nil
	}
	newStmt := *stmt
	newStmt.Columns = substituteParamsInExprs(stmt.Columns, paramMap)
	newStmt.Where = substituteParamsInExpr(stmt.Where, paramMap)
	newStmt.GroupBy = substituteParamsInExprs(stmt.GroupBy, paramMap)
	newStmt.Having = substituteParamsInExpr(stmt.Having, paramMap)
	newStmt.Limit = substituteParamsInExpr(stmt.Limit, paramMap)
	newStmt.Offset = substituteParamsInExpr(stmt.Offset, paramMap)
	if stmt.OrderBy != nil {
		newStmt.OrderBy = make([]*query.OrderByExpr, len(stmt.OrderBy))
		for i, order := range stmt.OrderBy {
			if order == nil {
				continue
			}
			copied := *order
			copied.Expr = substituteParamsInExpr(order.Expr, paramMap)
			newStmt.OrderBy[i] = &copied
		}
	}
	return &newStmt
}

func substituteParamsInExprs(exprs []query.Expression, paramMap map[string]interface{}) []query.Expression {
	if exprs == nil {
		return nil
	}
	result := make([]query.Expression, len(exprs))
	for i, expr := range exprs {
		result[i] = substituteParamsInExpr(expr, paramMap)
	}
	return result
}

func substituteParamsInOrderBy(orderBy []*query.OrderByExpr, paramMap map[string]interface{}) []*query.OrderByExpr {
	if len(orderBy) == 0 {
		return nil
	}
	result := make([]*query.OrderByExpr, len(orderBy))
	for i, ob := range orderBy {
		if ob == nil {
			continue
		}
		copied := *ob
		copied.Expr = substituteParamsInExpr(ob.Expr, paramMap)
		result[i] = &copied
	}
	return result
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
				return &query.StringLiteral{Value: catalog.ValueToStringKey(v)}
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
			OrderBy:  substituteParamsInOrderBy(e.OrderBy, paramMap),
			Filter:   substituteParamsInExpr(e.Filter, paramMap),
		}
	case *query.WindowExpr:
		newArgs := make([]query.Expression, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = substituteParamsInExpr(arg, paramMap)
		}
		partitionBy := make([]query.Expression, len(e.PartitionBy))
		for i, expr := range e.PartitionBy {
			partitionBy[i] = substituteParamsInExpr(expr, paramMap)
		}
		return &query.WindowExpr{
			Function:    e.Function,
			Args:        newArgs,
			Filter:      substituteParamsInExpr(e.Filter, paramMap),
			PartitionBy: partitionBy,
			OrderBy:     substituteParamsInOrderBy(e.OrderBy, paramMap),
			Frame:       e.Frame,
		}
	case *query.CaseExpr:
		newCase := &query.CaseExpr{}
		if e.Expr != nil {
			newCase.Expr = substituteParamsInExpr(e.Expr, paramMap)
		}
		newCase.Whens = make([]*query.WhenClause, len(e.Whens))
		for i, when := range e.Whens {
			newCase.Whens[i] = &query.WhenClause{
				Condition: substituteParamsInExpr(when.Condition, paramMap),
				Result:    substituteParamsInExpr(when.Result, paramMap),
			}
		}
		if e.Else != nil {
			newCase.Else = substituteParamsInExpr(e.Else, paramMap)
		}
		return newCase
	case *query.BetweenExpr:
		return &query.BetweenExpr{
			Expr:  substituteParamsInExpr(e.Expr, paramMap),
			Lower: substituteParamsInExpr(e.Lower, paramMap),
			Upper: substituteParamsInExpr(e.Upper, paramMap),
			Not:   e.Not,
		}
	case *query.InExpr:
		newList := make([]query.Expression, len(e.List))
		for i, item := range e.List {
			newList[i] = substituteParamsInExpr(item, paramMap)
		}
		return &query.InExpr{
			Expr:     substituteParamsInExpr(e.Expr, paramMap),
			List:     newList,
			Not:      e.Not,
			Subquery: e.Subquery,
		}
	case *query.IsNullExpr:
		return &query.IsNullExpr{
			Expr: substituteParamsInExpr(e.Expr, paramMap),
			Not:  e.Not,
		}
	case *query.CastExpr:
		return &query.CastExpr{
			Expr:     substituteParamsInExpr(e.Expr, paramMap),
			DataType: e.DataType,
		}
	case *query.LikeExpr:
		return &query.LikeExpr{
			Expr:    substituteParamsInExpr(e.Expr, paramMap),
			Pattern: substituteParamsInExpr(e.Pattern, paramMap),
			Not:     e.Not,
			Escape:  substituteParamsInExpr(e.Escape, paramMap),
		}
	case *query.AliasExpr:
		return &query.AliasExpr{
			Expr:  substituteParamsInExpr(e.Expr, paramMap),
			Alias: e.Alias,
		}
	default:
		return expr
	}
}

// executeSelect executes SELECT
// executeSelect executes SELECT

func (db *DB) executeSelect(ctx context.Context, stmt *query.SelectStmt, args []interface{}) (*Rows, error) {
	var columns []string
	var rows [][]interface{}
	var err error
	// Propagate the query context to the catalog only when row-level security is
	// enabled, so policies see the caller's user/roles. SelectWithContext holds
	// the exclusive lock to keep the shared RLS context per-query-safe; non-RLS
	// reads stay on the concurrent Select path.
	if db.catalog.IsRLSEnabled() {
		columns, rows, err = db.catalog.SelectWithContext(ctx, stmt, args)
	} else {
		columns, rows, err = db.catalog.Select(stmt, args)
	}
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
		case *string:
			if val != nil {
				sb.WriteString("S:")
				sb.WriteString(*val)
			}
		case catalog.StringBox:
			sb.WriteString("S:")
			sb.WriteString(val.String())
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

	// Fast path: direct numeric comparison without string conversion
	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case float64:
			af := float64(av)
			if af < bv {
				return -1
			}
			if af > bv {
				return 1
			}
			return 0
		}
	case float64:
		switch bv := b.(type) {
		case int64:
			bf := float64(bv)
			if av < bf {
				return -1
			}
			if av > bf {
				return 1
			}
			return 0
		case float64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
		if bv, ok := b.(*string); ok && bv != nil {
			if av < *bv {
				return -1
			}
			if av > *bv {
				return 1
			}
			return 0
		}
		if bv, ok := b.(catalog.StringBox); ok {
			if av < bv.String() {
				return -1
			}
			if av > bv.String() {
				return 1
			}
			return 0
		}
	case *string:
		if av == nil {
			return 1
		}
		if bv, ok := b.(string); ok {
			if *av < bv {
				return -1
			}
			if *av > bv {
				return 1
			}
			return 0
		}
		if bv, ok := b.(*string); ok {
			if bv == nil {
				return -1
			}
			if *av < *bv {
				return -1
			}
			if *av > *bv {
				return 1
			}
			return 0
		}
		if bv, ok := b.(catalog.StringBox); ok {
			if *av < bv.String() {
				return -1
			}
			if *av > bv.String() {
				return 1
			}
			return 0
		}
	case catalog.StringBox:
		if bv, ok := b.(string); ok {
			if av.String() < bv {
				return -1
			}
			if av.String() > bv {
				return 1
			}
			return 0
		}
		if bv, ok := b.(*string); ok && bv != nil {
			if av.String() < *bv {
				return -1
			}
			if av.String() > *bv {
				return 1
			}
			return 0
		}
		if bv, ok := b.(catalog.StringBox); ok {
			if av.String() < bv.String() {
				return -1
			}
			if av.String() > bv.String() {
				return 1
			}
			return 0
		}
	}

	// Fallback: type-aware string conversion to avoid fmt.Sprintf reflection
	sa := valueToStringForCompare(a)
	sb := valueToStringForCompare(b)
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

func valueToStringForCompare(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	switch val := v.(type) {
	case string:
		return val
	case *string:
		if val == nil {
			return "<nil>"
		}
		return *val
	case catalog.StringBox:
		return val.String()
	case []byte:
		return string(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return catalog.ValueToStringKey(val)
	}
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
	if err := db.catalog.CreateMaterializedViewSQL(stmt.Name, stmt.Query, stmt.IfNotExists, stmt.RawSQL); err != nil {
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

// executeShowIndexQuery returns index information for a table (SHOW INDEX).
func (db *DB) executeShowIndexQuery(ctx context.Context, stmt *query.ShowIndexStmt) (*Rows, error) {
	table, err := db.catalog.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}
	cols := []string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Null", "Index_type"}
	var rows [][]interface{}

	addIndexRows := func(keyName string, columns []string, nonUnique int64) {
		for i, c := range columns {
			nullable := "YES"
			for _, tc := range table.Columns {
				if strings.EqualFold(tc.Name, c) && (tc.NotNull || tc.PrimaryKey) {
					nullable = ""
				}
			}
			rows = append(rows, []interface{}{stmt.Table, nonUnique, keyName, int64(i + 1), c, nullable, "BTREE"})
		}
	}

	// PRIMARY KEY first, matching MySQL.
	if len(table.PrimaryKey) > 0 {
		addIndexRows("PRIMARY", table.PrimaryKey, 0)
	}
	for _, idx := range db.catalog.GetTableIndexes(stmt.Table) {
		nonUnique := int64(1)
		if idx.Unique {
			nonUnique = 0
		}
		addIndexRows(idx.Name, idx.Columns, nonUnique)
	}

	return &Rows{columns: cols, rows: rows}, nil
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
	innerStmt := stmt.Statement

	var plan *QueryPlan
	switch s := innerStmt.(type) {
	case *query.SelectStmt:
		plan = db.buildQueryPlan(s)
	case *query.InsertStmt:
		plan = db.buildInsertPlan(s)
	case *query.UpdateStmt:
		plan = db.buildUpdatePlan(s)
	case *query.DeleteStmt:
		plan = db.buildDeletePlan(s)
	default:
		columns := []string{"QUERY PLAN"}
		rows := [][]interface{}{{fmt.Sprintf("EXPLAIN not supported for %T", innerStmt)}}
		return &Rows{
			columns: columns,
			rows:    rows,
			pos:     0,
		}, nil
	}

	columns, rows := formatQueryPlan(plan)
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
	closed  bool
}

// Next advances to the next row

func (r *Rows) Next() bool {
	if r == nil || r.closed {
		return false
	}
	r.pos++
	return r.pos <= len(r.rows)
}

// Scan copies column values into dest

func (r *Rows) Scan(dest ...interface{}) error {
	if r == nil {
		return errors.New("rows is nil")
	}
	if r.closed {
		return errors.New("rows are closed")
	}
	if r.pos == 0 || r.pos > len(r.rows) {
		return errors.New("no current row")
	}

	row := r.rows[r.pos-1]
	if len(dest) != len(row) {
		return errors.New("column count mismatch")
	}

	for i, d := range dest {
		if di, ok := d.(*interface{}); ok {
			*di = cloneScannedValue(row[i])
			continue
		}
		if err := scanValue(row[i], d); err != nil {
			return err
		}
	}

	return nil
}

// Columns returns the column names

func (r *Rows) Columns() []string {
	if r == nil || r.closed || r.columns == nil {
		return nil
	}
	columns := make([]string, len(r.columns))
	copy(columns, r.columns)
	return columns
}

// ColumnTypeHints returns coarse SQL type names inferred from non-NULL row values.
func (r *Rows) ColumnTypeHints() []string {
	if r == nil || r.closed || len(r.columns) == 0 {
		return nil
	}
	hints := make([]string, len(r.columns))
	for _, row := range r.rows {
		for i, val := range row {
			if i >= len(hints) || hints[i] != "" {
				continue
			}
			hints[i] = rowValueTypeHint(val)
		}
	}
	return hints
}

func rowValueTypeHint(val interface{}) string {
	switch val.(type) {
	case nil:
		return ""
	case bool:
		return "BOOLEAN"
	case int, int8, int16, int32, uint, uint8, uint16, uint32:
		return "INTEGER"
	case int64, uint64:
		return "BIGINT"
	case float32:
		return "REAL"
	case float64:
		return "DOUBLE"
	case []byte:
		return "BLOB"
	case time.Time:
		return "DATETIME"
	default:
		return "TEXT"
	}
}

// Close closes the rows

func (r *Rows) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	r.columns = nil
	r.rows = nil
	r.pos = 0
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
		*d = cloneScannedValue(src)
	case *string:
		switch v := src.(type) {
		case string:
			*d = v
		case *string:
			if v != nil {
				*d = *v
			}
		case catalog.StringBox:
			*d = v.String()
		case []byte:
			*d = string(v)
		case int64:
			*d = strconv.FormatInt(v, 10)
		case int:
			*d = strconv.Itoa(v)
		case float64:
			*d = strconv.FormatFloat(v, 'f', -1, 64)
		case bool:
			if v {
				*d = "true"
			} else {
				*d = "false"
			}
		default:
			*d = catalog.ValueToStringKey(v)
		}
	case *int:
		v, ok := src.(int64)
		if !ok {
			// Try float
			if f, ok := src.(float64); ok {
				*d = int(f)
				return nil
			}
			// Try string
			if s, ok := src.(string); ok {
				if i, err := strconv.ParseInt(s, 10, 64); err == nil {
					*d = int(i)
					return nil
				}
			}
			// Try *string
			if ps, ok := src.(*string); ok && ps != nil {
				if i, err := strconv.ParseInt(*ps, 10, 64); err == nil {
					*d = int(i)
					return nil
				}
			}
			// Try StringBox
			if sb, ok := src.(catalog.StringBox); ok {
				if i, err := strconv.ParseInt(sb.String(), 10, 64); err == nil {
					*d = int(i)
					return nil
				}
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
			if s, ok := src.(string); ok {
				if i, err := strconv.ParseInt(s, 10, 64); err == nil {
					*d = i
					return nil
				}
			}
			if ps, ok := src.(*string); ok && ps != nil {
				if i, err := strconv.ParseInt(*ps, 10, 64); err == nil {
					*d = i
					return nil
				}
			}
			// Try StringBox
			if sb, ok := src.(catalog.StringBox); ok {
				if i, err := strconv.ParseInt(sb.String(), 10, 64); err == nil {
					*d = i
					return nil
				}
			}
			return fmt.Errorf("cannot scan %T into int64", src)
		}
		*d = v
	case *float64:
		v, ok := src.(float64)
		if !ok {
			if s, ok := src.(string); ok {
				if f, err := strconv.ParseFloat(s, 64); err == nil {
					*d = f
					return nil
				}
			}
			if ps, ok := src.(*string); ok && ps != nil {
				if f, err := strconv.ParseFloat(*ps, 64); err == nil {
					*d = f
					return nil
				}
			}
			if sb, ok := src.(catalog.StringBox); ok {
				if f, err := strconv.ParseFloat(sb.String(), 64); err == nil {
					*d = f
					return nil
				}
			}
			return fmt.Errorf("cannot scan %T into float64", src)
		}
		*d = v
	case *bool:
		v, ok := src.(bool)
		if !ok {
			if s, ok := src.(string); ok {
				if b, err := strconv.ParseBool(s); err == nil {
					*d = b
					return nil
				}
			}
			if ps, ok := src.(*string); ok && ps != nil {
				if b, err := strconv.ParseBool(*ps); err == nil {
					*d = b
					return nil
				}
			}
			if sb, ok := src.(catalog.StringBox); ok {
				if b, err := strconv.ParseBool(sb.String()); err == nil {
					*d = b
					return nil
				}
			}
			return fmt.Errorf("cannot scan %T into bool", src)
		}
		*d = v
	case *[]byte:
		v, ok := src.([]byte)
		if !ok {
			return fmt.Errorf("cannot scan %T into []byte", src)
		}
		*d = cloneScannedValue(v).([]byte)
	default:
		return fmt.Errorf("unsupported scan destination: %T", dest)
	}
	return nil
}

func cloneScannedValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case []byte:
		if typed == nil {
			return []byte(nil)
		}
		cloned := make([]byte, len(typed))
		copy(cloned, typed)
		return cloned
	case []interface{}:
		if typed == nil {
			return []interface{}(nil)
		}
		cloned := make([]interface{}, len(typed))
		for i, nested := range typed {
			cloned[i] = cloneScannedValue(nested)
		}
		return cloned
	case []string:
		if typed == nil {
			return []string(nil)
		}
		cloned := make([]string, len(typed))
		copy(cloned, typed)
		return cloned
	case []float64:
		if typed == nil {
			return []float64(nil)
		}
		cloned := make([]float64, len(typed))
		copy(cloned, typed)
		return cloned
	case map[string]interface{}:
		if typed == nil {
			return map[string]interface{}(nil)
		}
		cloned := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			cloned[key] = cloneScannedValue(nested)
		}
		return cloned
	case map[string]string:
		if typed == nil {
			return map[string]string(nil)
		}
		cloned := make(map[string]string, len(typed))
		for key, nested := range typed {
			cloned[key] = nested
		}
		return cloned
	default:
		return typed
	}
}

func acquireTx(db *DB, txn *txn.Transaction) *Tx {
	return &Tx{db: db, txn: txn}
}

func releaseTx(tx *Tx) {
	if tx == nil {
		return
	}
	tx.db = nil
	tx.txn = nil
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

	if tx.db.closed.Load() {
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

	if tx.db.closed.Load() {
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
	defer func() {
		releaseTx(tx)
	}()
	defer tx.db.releaseConnection()
	defer func() {
		if tx.txn != nil {
			tx.txn.Recycle()
		}
	}()

	// Concurrent explicit transactions apply buffered writes inside
	// CommitTransaction, which serializes on per-tree mutexes.
	// B-tree flushing is deferred to checkpoint/close; the B-tree
	// self-flushes before eviction when memory pressure requires it.
	tx.db.flushMu.RLock()
	defer tx.db.flushMu.RUnlock()

	// Commit in catalog (conflict detection, WAL write, apply buffered writes)
	if err := tx.db.catalog.CommitTransaction(); err != nil {
		// Rollback catalog transaction to prevent it from staying active forever
		if rbErr := tx.db.catalog.RollbackTransaction(); rbErr != nil {
			_ = rbErr // best-effort rollback; preserve primary error
		}
		return fmt.Errorf("commit transaction failed: %w", err)
	}

	// If the catalog already committed the shared manager transaction,
	// do not attempt to commit it again.
	if tx.txn.State != txn.TxnCommitted {
		if err := tx.txn.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// Rollback rolls back the transaction
func (tx *Tx) Rollback() error {
	if !tx.done.CompareAndSwap(false, true) {
		return errors.New("transaction already completed")
	}
	defer func() {
		releaseTx(tx)
	}()
	defer tx.db.releaseConnection()
	defer func() {
		if tx.txn != nil {
			tx.txn.Recycle()
		}
	}()

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
