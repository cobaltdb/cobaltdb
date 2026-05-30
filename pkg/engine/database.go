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
	PageSize   int              // Database page size (must match storage.PageSize)
	CacheSize  int              // Number of cached pages
	InMemory   bool             // Run fully in-memory without persisting
	WALEnabled *bool            // Enable write-ahead logging (nil = default: true for disk)
	SyncMode   SyncMode         // Durability vs performance trade-off
	Logger     *logger.Logger   // Optional custom logger (nil = default)
}

// ConnectionPool governs how concurrent database connections are managed.
type ConnectionPool struct {
	MaxConnections    int           // Maximum concurrent connections (0 = unlimited)
	ConnectionTimeout time.Duration // Timeout for acquiring a connection
	QueryTimeout      time.Duration // Default query timeout (0 = no timeout)
}

// Security governs encryption, auditing, and access control settings.
type Security struct {
	EncryptionKey    []byte                     // Encryption key for data at rest (nil = no encryption)
	EncryptionConfig *storage.EncryptionConfig  // Detailed encryption configuration
	AuditConfig      *audit.Config             // Audit logging configuration (nil = disabled)
	EnableRLS        bool                      // Enable Row-Level Security by default
	MaxStmtCacheSize int                       // Maximum cached prepared statements (default: 1000)
	StrictSQLParsing bool                      // Reject trailing tokens after a parsed statement
}

// QueryCacheConfig governs the query result cache.
type QueryCacheConfig struct {
	EnableQueryCache bool          // Enable query result caching
	QueryCacheSize    int64         // Max query cache size in bytes (default: 64MB)
	QueryCacheTTL     time.Duration // Query cache TTL (default: 5m)
}

// ReplicationConfig governs the replication subsystem.
type ReplicationConfig struct {
	Role            string // "master", "slave", or "" (disabled)
	ListenAddr      string // Master listen address for slaves to connect to
	MasterAddr      string // Slave: master address to connect to
	Mode            string // "async", "sync", or "full_sync"
	AuthToken       string // Authentication token for replication
	SSLCert         string // SSL certificate file path
	SSLKey          string // SSL private key file path
	SSLCA           string // SSL CA certificate path
	StateFile       string // Slave resume state file path
}

// BackupConfig governs backup creation and retention.
type BackupConfig struct {
	Dir               string        // Backup directory path
	Retention         time.Duration // Backup retention period
	MaxBackups        int           // Maximum number of backups to retain
	CompressionLevel  int           // Compression level (0-9, 0=disabled)
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
	EnableAutoCheckpoint bool          // Enable automatic WAL checkpoint (default: true for disk)
	CheckpointInterval   time.Duration // Interval between checkpoints (default: 5m)
}

// SchedulerConfig governs the background job scheduler.
type SchedulerConfig struct {
	EnableScheduler    bool          // Enable job scheduler (default: true for disk)
	AnalyzeInterval    time.Duration // Interval for automatic ANALYZE (default: 1h)
	Workers            int           // Number of scheduler workers (default: 2)
	TickInterval       time.Duration // Dispatcher resolution (default: 1s)
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
	QueryCache      QueryCacheConfig
	Replication    ReplicationConfig
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
// It returns the parsed statement and a release-connection func; if err is
// non-nil the caller should return immediately.
func (db *DB) runStatement(ctx context.Context, methodName, sql string, args ...interface{}) (_ query.Statement, start time.Time, release func(), err error) {
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
		return nil, time.Time{}, func() {}, acquireErr
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
		return nil, time.Time{}, func() {}, err
	}

	start = time.Now()
	return stmt, start, release, nil
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

	stmt, start, release, execErr := db.runStatement(ctx, "Exec", sql, args...)
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

	return db.execute(ctx, stmt, args)
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

	stmt, start, release, execErr := db.runStatement(ctx, "Query", sql, args...)
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
	case *query.InsertStmt:
		result, err := db.executeInsert(ctx, s, args)
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

func (db *DB) executeCreateForeignTable(ctx context.Context, stmt *query.CreateForeignTableStmt) (Result, error) {
	if err := db.catalog.CreateForeignTable(stmt); err != nil {
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
	if err := db.catalog.CreateViewSQL(stmt.Name, stmt.Query, stmt.RawSQL); err != nil {
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

	// Evaluate call arguments from SQL literals/placeholders
	// (e.g., CALL proc(1, 'hello') or CALL proc(?, ?)).
	callArgs := make([]interface{}, 0, len(stmt.Params))
	for _, paramExpr := range stmt.Params {
		val, err := catalog.EvalExpression(paramExpr, args)
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
	if len(mergedArgs) != len(proc.Params) {
		return Result{}, fmt.Errorf("procedure %s expects %d arguments, got %d", proc.Name, len(proc.Params), len(mergedArgs))
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
	if r == nil || r.columns == nil {
		return nil
	}
	columns := make([]string, len(r.columns))
	copy(columns, r.columns)
	return columns
}

// ColumnTypeHints returns coarse SQL type names inferred from non-NULL row values.
func (r *Rows) ColumnTypeHints() []string {
	if r == nil || len(r.columns) == 0 {
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

// txPool recycles engine-level Tx wrappers to eliminate one heap allocation
// per explicit transaction.
var txPool sync.Pool

func acquireTx(db *DB, txn *txn.Transaction) *Tx {
	if v := txPool.Get(); v != nil {
		tx := v.(*Tx)
		tx.db = db
		tx.txn = txn
		tx.done.Store(false)
		return tx
	}
	return &Tx{db: db, txn: txn}
}

func releaseTx(tx *Tx) {
	if tx == nil {
		return
	}
	txPool.Put(tx)
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
