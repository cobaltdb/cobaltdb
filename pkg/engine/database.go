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
	"github.com/cobaltdb/cobaltdb/pkg/util"
)

// toUpperFast delegates to util.ToUpperFast.
// Deprecated: use util.ToUpperFast directly.
func toUpperFast(s string) string { return util.ToUpperFast(s) }

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
	// backupMu serializes hot backup against concurrent checkpoints. Acquiring
	// backupMu in BeginHotBackup blocks both DB.Checkpoint and WAL auto-checkpoint.
	backupMu sync.Mutex
	// hotBackupActive (guarded by db.mu) records that BeginHotBackup holds
	// backupMu, so EndHotBackup only releases when a backup is in progress.
	hotBackupActive bool
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

	// Master-side replication capture state.
	// replCaptureMu serializes replication snapshot creation (write lock)
	// against the commit+capture window of write statements (read lock) so a
	// snapshot's LSN label exactly matches its contents. Lock ordering:
	// db.mu -> replCaptureMu -> flushMu.
	replCaptureMu sync.RWMutex
	// replMu guards the pending statement buffer used for explicit
	// transactions (statements are replicated on COMMIT, discarded on
	// ROLLBACK). A single buffer is sufficient under the engine's
	// single-writer transaction model.
	replMu         sync.Mutex
	replPending    [][]byte
	replSavepoints map[string]int

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

	// SyncTimeout bounds how long a sync/full_sync master write waits for
	// slave acknowledgements after commit (default 5s).
	SyncTimeout time.Duration
	// SyncStrict controls "sync" mode timeout behavior: when true the write
	// returns an error if no slave acknowledged within SyncTimeout; when
	// false (default) the write degrades to async with a logged warning.
	// "full_sync" mode always returns an error on timeout.
	SyncStrict bool
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
	EnableAutoVacuum    bool          // Enable automatic VACUUM (default: true for disk)
	AutoVacuumInterval  time.Duration // Interval between auto-vacuum checks (default: 1m)
	AutoVacuumThreshold float64       // Dead tuple ratio to trigger vacuum (default: 0.2 = 20%)
	// AutoVacuumRetention is the minimum age of a soft-deleted row before
	// AutoVacuum physically removes it. Set to 0 to disable temporal retention
	// protection (removes all dead rows). Default: 0 (disabled — this is the
	// pre-fix behavior; set to a duration like 1*time.Hour to protect recent
	// history needed by AS OF SYSTEM TIME queries).
	AutoVacuumRetention  time.Duration
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
	// Reject a query with fewer bind arguments than positional `?` placeholders.
	// Without this, an unbound placeholder evaluates to a fail-closed NULL and the
	// query returns silently wrong results (e.g. `WHERE v = ? OR v = ?` with one
	// arg matches nothing and reports no error) — a data-correctness hazard for
	// callers. Extra args remain tolerated (ignored), matching prior leniency.
	if n := query.CountPlaceholders(sql); n > len(args) {
		return nil, fmt.Errorf("statement has %d placeholder(s) but %d argument(s) were provided", n, len(args))
	}

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
		found := false
		for i, w := range db.connWaiters {
			if w == ch {
				db.connWaiters = append(db.connWaiters[:i], db.connWaiters[i+1:]...)
				found = true
				break
			}
		}
		db.connWaitMu.Unlock()
		if !found {
			// A releaseConnection already popped this waiter and handed it a
			// slot (the buffered channel send always succeeds, even though we
			// timed out). Reclaim the slot so connCount is not leaked — without
			// this, every timeout-racing-handoff permanently burns a slot and
			// the limiter eventually wedges. Every pop is followed by exactly
			// one send, so this receive cannot block forever.
			<-ch
			db.releaseHandedOffSlot()
		}
		return fmt.Errorf("connection timeout: %w", ctx.Err())
	case <-db.shutdownCh:
		return ErrDatabaseClosed
	}
}

// releaseHandedOffSlot returns a connection slot that releaseConnection handed
// to a waiter which then abandoned it (timeout). It passes the slot to the next
// waiter, or decrements connCount if none. It must NOT touch activeConns: the
// releasing side already decremented it and this waiter never incremented it.
func (db *DB) releaseHandedOffSlot() {
	db.connWaitMu.Lock()
	if len(db.connWaiters) > 0 {
		next := db.connWaiters[0]
		db.connWaiters = db.connWaiters[1:]
		db.connWaitMu.Unlock()
		next <- struct{}{} // buffered cap-1; hand the slot to the next waiter
		return
	}
	db.connWaitMu.Unlock()
	db.connCount.Add(-1)
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

	return db.execute(runCtx, sql, stmt, args)
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

	return db.query(runCtx, sql, stmt, args)
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

// AbortConnTransaction rolls back any transaction left open on the CALLING
// goroutine — e.g. when a server connection drops mid-transaction (after a wire
// BEGIN with no COMMIT/ROLLBACK). It is a no-op if no transaction is active.
// Transaction state is goroutine-local, so this must run on the connection's
// own handler goroutine. Without it, the abandoned transaction kept its locks
// (blocking the single writer) and pinned MVCC version pruning.
func (db *DB) AbortConnTransaction() {
	if db.closed.Load() || db.catalog == nil {
		return
	}
	if db.catalog.IsTransactionActive() {
		_ = db.catalog.RollbackTransaction()
		db.replTxnDiscard()
	}
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
	_ = table // retained for call-site clarity; replication now ships SQL text via execute()
	result, err := handler()
	if db.auditLogger != nil {
		db.auditLogger.Log(audit.EventDDL, auditUser(ctx), action, opts...)
	}
	return result, err
}

// execute executes a statement. sqlText is the original SQL of stmt and is
// used for statement-based replication; internal recursive executions (e.g.
// statements inside a stored procedure body) pass "" so only the outermost
// statement is replicated.

func (db *DB) execute(ctx context.Context, sqlText string, stmt query.Statement, args []interface{}) (result Result, err error) {
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

	// Statement-based replication capture. Registered BEFORE the autocommit
	// defer below (defers are LIFO) so it runs AFTER the implicit commit and
	// only ships statements that actually committed. The capture read-lock is
	// held across execution+commit+capture so a concurrent replication
	// snapshot cannot be labeled with an LSN that excludes a write already in
	// its data (which would make the slave double-apply that write).
	if sqlText != "" && isReplicatedWriteStmt(stmt) && db.replicationMasterManager() != nil {
		inExplicitTxn := !autocommit && db.catalog.IsTransactionActive()
		db.replCaptureMu.RLock()
		defer func() {
			if err != nil {
				db.replCaptureMu.RUnlock()
				return
			}
			needWait, repErr := db.replicateStatement(sqlText, args, inExplicitTxn)
			// Release the capture lock BEFORE the sync-mode ACK wait: waiting
			// while holding it can deadlock against a snapshot-sending slave
			// handler (it holds the slave connection mutex and needs the
			// capture write lock).
			db.replCaptureMu.RUnlock()
			if repErr == nil && needWait {
				if mgr := db.replicationMasterManager(); mgr != nil {
					repErr = db.replicationSyncWait(mgr)
				}
			}
			if repErr != nil {
				err = repErr
			}
		}()
	}

	if autocommit {
		// Hold flushMu.RLock across the whole autocommit transaction (statement
		// execution + the deferred CommitTransaction). This serializes the commit
		// against DB.Checkpoint (flushMu.Lock) so a concurrent checkpoint cannot
		// truncate the WAL between a commit's durable WAL append and its later
		// page-apply (tree.PutBatch), which would otherwise lose an acknowledged
		// write on a subsequent crash. Registered BEFORE the commit defer so, by
		// LIFO ordering, RUnlock runs AFTER the commit has applied its pages.
		db.flushMu.RLock()
		defer db.flushMu.RUnlock()

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
					// A failed commit (WAL record size limit, or a commit-time
					// conflict) returns before CommitTransaction clears the
					// goroutine-local txn state, leaving txnActive=true with the
					// failed statement's writes still buffered. Without this
					// rollback the next statement on the same connection would see
					// phantom uncommitted rows and run inside a leaked implicit
					// transaction. Roll back to restore atomicity.
					if rbErr := db.catalog.RollbackTransaction(); rbErr != nil {
						err = fmt.Errorf("%w; rollback after failed commit failed: %v", err, rbErr)
					}
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
		return result, err
	case *query.UpdateStmt:
		result, err := db.executeUpdate(ctx, s, args)
		if db.auditLogger != nil {
			db.auditLogger.LogQuery(auditUser(ctx), "UPDATE", time.Since(start), result.RowsAffected, err)
		}
		return result, err
	case *query.DeleteStmt:
		result, err := db.executeDelete(ctx, s, args)
		if db.auditLogger != nil {
			db.auditLogger.LogQuery(auditUser(ctx), "DELETE", time.Since(start), result.RowsAffected, err)
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
		// Pass the manager transaction into the catalog (as BeginWith does)
		// rather than calling BeginTransaction, which would begin a SECOND
		// manager transaction and orphan this one forever — leaking a pooled
		// Transaction and pinning MVCC pruneVersions' minActive so version-store
		// memory could never be reclaimed.
		db.catalog.BeginTransactionWithTxn(transaction.ID, transaction)
		db.replTxnDiscard() // defensive: start with an empty replication buffer
		return Result{}, nil
	case *query.CommitStmt:
		if !db.catalog.IsTransactionActive() {
			return Result{}, errors.New("no transaction in progress")
		}
		// Serialize the SQL COMMIT's flush+commit (WAL-append → page-apply)
		// against DB.Checkpoint (flushMu.Lock). Without this, a checkpoint could
		// truncate the WAL between the commit record's fsync and the buffered
		// writes' page-apply, losing acknowledged committed rows on a later crash
		// — the same durability window the autocommit path guards, reachable via
		// SQL BEGIN/COMMIT on the MySQL wire server and CLI. (The programmatic
		// Tx.Commit path already holds flushMu.RLock.) Released right after the
		// page-apply, BEFORE the sync-mode ACK wait, so a slow replica ACK does
		// not block checkpoints.
		db.flushMu.RLock()
		if err := db.catalog.FlushTableTrees(); err != nil {
			db.flushMu.RUnlock()
			return Result{}, fmt.Errorf("failed to flush tables: %w", err)
		}
		// Hold the replication capture lock across commit+entry-append so a
		// concurrent replication snapshot cannot observe the committed data
		// without the corresponding replication entries. The sync-mode ACK
		// wait runs after the lock is released (see replicateStatement).
		db.replCaptureMu.RLock()
		commitErr := db.catalog.CommitTransaction()
		var needWait bool
		var replErr error
		if commitErr == nil {
			needWait, replErr = db.replTxnFlush()
		}
		db.replCaptureMu.RUnlock()
		db.flushMu.RUnlock()
		if commitErr != nil {
			return Result{}, commitErr
		}
		if replErr == nil && needWait {
			if mgr := db.replicationMasterManager(); mgr != nil {
				replErr = db.replicationSyncWait(mgr)
			}
		}
		if replErr != nil {
			return Result{}, replErr
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
			db.replTxnRollbackToSavepoint(s.ToSavepoint)
			return Result{}, nil
		}
		if err := db.catalog.RollbackTransaction(); err != nil {
			return Result{}, err
		}
		db.replTxnDiscard()
		return Result{}, nil
	case *query.SavepointStmt:
		if !db.catalog.IsTransactionActive() {
			return Result{}, errors.New("SAVEPOINT can only be used within a transaction")
		}
		if err := db.catalog.Savepoint(s.Name); err != nil {
			return Result{}, err
		}
		db.replTxnMarkSavepoint(s.Name)
		return Result{}, nil
	case *query.ReleaseSavepointStmt:
		if !db.catalog.IsTransactionActive() {
			return Result{}, errors.New("RELEASE SAVEPOINT can only be used within a transaction")
		}
		if err := db.catalog.ReleaseSavepoint(s.Name); err != nil {
			return Result{}, err
		}
		db.replTxnReleaseSavepoint(s.Name)
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

// query executes a query and returns rows. sqlText is the original SQL of
// stmt, used for statement-based replication of write statements that return
// rows (INSERT/UPDATE/DELETE ... RETURNING, CALL); internal recursive
// executions pass "".

func (db *DB) query(ctx context.Context, sqlText string, stmt query.Statement, args []interface{}) (*Rows, error) {
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
			return db.queryWriteWithReplication(sqlText, args, func() (*Rows, error) {
				return db.executeInsertReturning(ctx, s, args)
			})
		}
		return nil, fmt.Errorf("not a query statement: %T", stmt)
	case *query.UpdateStmt:
		if len(s.Returning) > 0 {
			return db.queryWriteWithReplication(sqlText, args, func() (*Rows, error) {
				return db.executeUpdateReturning(ctx, s, args)
			})
		}
		return nil, fmt.Errorf("not a query statement: %T", stmt)
	case *query.DeleteStmt:
		if len(s.Returning) > 0 {
			return db.queryWriteWithReplication(sqlText, args, func() (*Rows, error) {
				return db.executeDeleteReturning(ctx, s, args)
			})
		}
		return nil, fmt.Errorf("not a query statement: %T", stmt)
	case *query.CallProcedureStmt:
		return db.queryWriteWithReplication(sqlText, args, func() (*Rows, error) {
			return db.queryCallProcedure(ctx, s, args)
		})
	default:
		return nil, fmt.Errorf("not a query statement: %T", stmt)
	}
}

// queryWriteWithReplication executes a write statement issued through the
// query path (RETURNING clauses, CALL) and replicates it on success. When a
// transaction is active the statement joins it and is buffered until COMMIT;
// otherwise it is shipped immediately under the capture lock.
func (db *DB) queryWriteWithReplication(sqlText string, args []interface{}, run func() (*Rows, error)) (*Rows, error) {
	if sqlText == "" || db.replicationMasterManager() == nil {
		return run()
	}

	inTxn := db.catalog.IsTransactionActive()
	var rows *Rows
	var needWait bool
	err := func() error {
		if !inTxn {
			db.replCaptureMu.RLock()
			defer db.replCaptureMu.RUnlock()
		}
		var runErr error
		rows, runErr = run()
		if runErr != nil {
			return runErr
		}
		var repErr error
		needWait, repErr = db.replicateStatement(sqlText, args, inTxn)
		return repErr
	}()
	if err != nil {
		return rows, err
	}
	// Sync-mode ACK wait outside the capture lock (see replicateStatement).
	if needWait {
		if mgr := db.replicationMasterManager(); mgr != nil {
			if repErr := db.replicationSyncWait(mgr); repErr != nil {
				return rows, repErr
			}
		}
	}
	return rows, nil
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
	rows, err := db.query(ctx, "", stmt.AsSelect, nil)
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
	if err := db.catalog.Vacuum(db.options.Maintenance.AutoVacuumRetention); err != nil {
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
