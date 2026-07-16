package engine

import (
	"context"
	"errors"
	"fmt"
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

const (
	SyncOff SyncMode = iota
	SyncNormal
	SyncFull
)

// BoolPtr returns a pointer to the given bool value. Useful for Options
// literals where *bool fields need an explicit value.
// DefaultOptions returns the default database options

// RegisterFDW registers a foreign data wrapper factory with the database.

// getPreparedStatement returns a cached prepared statement or parses and caches it

// evictLRUEntry removes the least recently used entry from the cache
// Must be called with stmtMu.Lock() held

// acquireConnection acquires a connection slot with timeout.
// The fast path uses atomics to avoid channel/select overhead.
// releaseHandedOffSlot returns a connection slot that releaseConnection handed
// to a waiter which then abandoned it (timeout). It passes the slot to the next
// waiter, or decrements connCount if none. It must NOT touch activeConns: the
// releasing side already decremented it and this waiter never incremented it.
// releaseConnection releases a connection slot, waking a waiter if any.
// runStatement does the common setup for Exec and Query: panic recovery,
// query timeout, connection acquire, db closed check, and statement parsing.
// It returns the execution context, parsed statement, and a release-connection
// func; if err is non-nil the caller should return immediately.
// Exec executes a SQL statement without returning rows

// Query executes a SQL query and returns rows

// QueryRow executes a SQL query and returns a single row

// Tables returns a list of all table names in the database

// Begin starts a new transaction

// BeginWith starts a new transaction with options

// AbortConnTransaction rolls back any transaction left open on the CALLING
// goroutine — e.g. when a server connection drops mid-transaction (after a wire
// BEGIN with no COMMIT/ROLLBACK). It is a no-op if no transaction is active.
// Transaction state is goroutine-local, so this must run on the connection's
// own handler goroutine. Without it, the abandoned transaction kept its locks
// (blocking the single writer) and pinned MVCC version pruning.
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

// executeCreateTableAsSelect implements CREATE TABLE ... AS SELECT (CTAS):
// materialize the query, infer column types, create the table, insert the rows.

// inferCTASColumnType picks a column type for CTAS from the materialized values.

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

// executeAlterTable executes ALTER TABLE

// executeDropTable executes DROP TABLE

// executeCreateIndex executes CREATE INDEX

// executeCreateView executes CREATE VIEW

// executeDropView executes DROP VIEW

// executeCreateTrigger executes CREATE TRIGGER

// executeDropTrigger executes DROP TRIGGER

// executeCreateProcedure executes CREATE PROCEDURE

// executeDropProcedure executes DROP PROCEDURE

// executeCreatePolicy executes CREATE POLICY for row-level security

// expressionToString converts an expression to its SQL string representation

// tokenTypeToString converts a token type to its string representation

// executeDropPolicy executes DROP POLICY

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

// executeAnalyze executes ANALYZE

// executeCreateMaterializedView executes CREATE MATERIALIZED VIEW

// executeDropMaterializedView executes DROP MATERIALIZED VIEW

// executeRefreshMaterializedView executes REFRESH MATERIALIZED VIEW

// executeCreateFTSIndex executes CREATE FULLTEXT INDEX

// executeCreateVectorIndex executes CREATE VECTOR INDEX

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
