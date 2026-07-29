package engine

import (
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/audit"
	"github.com/cobaltdb/cobaltdb/pkg/logger"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

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
	Role          string // "master", "slave", or "" (disabled)
	ListenAddr    string // Master listen address for slaves to connect to
	MasterAddr    string // Slave: master address to connect to
	Mode          string // "async", "sync", or "full_sync"
	AuthToken     string // Authentication token for replication
	SSLCert       string // TLS certificate: server certificate on master, optional client certificate on slave
	SSLKey        string // Private key matching SSLCert
	SSLCA         string // Client CA on master (mTLS), server root CA on slave
	SSLServerName string // Optional slave certificate name override
	StateFile     string // Slave resume state file path

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
	// Deprecated: use Replication.SSLServerName.
	ReplicationSSLServerName string
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

// SyncMode controls when data is synced to disk.
type SyncMode int

const (
	SyncOff SyncMode = iota
	SyncNormal
	SyncFull
)
