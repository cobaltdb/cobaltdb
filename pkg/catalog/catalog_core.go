// Package catalog provides table management and query execution for CobaltDB.
// It handles table creation, indexing, constraints, and SQL query processing.
//
// The Catalog is the central component that manages all database metadata
// and provides the query execution engine.
//
// Example usage:
//
//	tree := btree.NewBTree()
//	catalog := catalog.New(tree, nil, nil)
//
//	// Create a table
//	err := catalog.CreateTable("users", []catalog.Column{
//	    {Name: "id", Type: "INTEGER", PrimaryKey: true},
//	    {Name: "name", Type: "TEXT", NotNull: true},
//	})
//
//	// Execute a query
//	result, err := catalog.ExecuteQuery("SELECT * FROM users WHERE id = ?", 1)
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/cache"
	"github.com/cobaltdb/cobaltdb/pkg/fdw"
	"github.com/cobaltdb/cobaltdb/pkg/parallel"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrTableExists    = errors.New("table already exists")
	ErrTableNotFound  = errors.New("table not found")
	ErrColumnNotFound = errors.New("column not found")
	ErrIndexExists    = errors.New("index already exists")
	ErrIndexNotFound  = errors.New("index not found")
)

// TableDef represents a table definition
// PartitionInfo stores table partitioning configuration
type PartitionInfo struct {
	Type       query.PartitionType `json:"type"`
	Column     string              `json:"column"`
	NumParts   int                 `json:"num_parts"`
	Partitions []PartitionDef      `json:"partitions"`
}

// PartitionDef defines a single partition
type PartitionDef struct {
	Name     string `json:"name"`
	MinValue int64  `json:"min_value"`
	MaxValue int64  `json:"max_value"`
}

// TableDef holds the schema definition for a table or collection.
type TableDef struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"` // "table" or "collection"
	Columns     []ColumnDef     `json:"columns"`
	PrimaryKey  []string        `json:"primary_key"` // Supports composite PK
	CreatedAt   int64           `json:"created_at"`
	RootPageID  uint32          `json:"root_page_id"`
	ForeignKeys []ForeignKeyDef `json:"foreign_keys,omitempty"`
	Checks      []CheckDef      `json:"check_constraints,omitempty"`
	AutoIncSeq  int64           `json:"auto_inc_seq"`        // Per-table auto-increment counter
	Partition   *PartitionInfo  `json:"partition,omitempty"` // Table partitioning info
	Temporary   bool            `json:"-"`                   // Session-local table, not persisted
	// Performance: cache column indices (not persisted)
	columnIndices map[string]int `json:"-"`
}

type CheckDef struct {
	Name     string           `json:"name,omitempty"`
	CheckStr string           `json:"check_str"`
	Check    query.Expression `json:"-"`
}

// ForeignTableDef represents a foreign table definition
type ForeignTableDef struct {
	TableName string            `json:"table_name"`
	Columns   []ColumnDef       `json:"columns"`
	Wrapper   string            `json:"wrapper"`
	Options   map[string]string `json:"options"`
}

// ForeignKeyDef represents a foreign key constraint
type ForeignKeyDef struct {
	Name              string   `json:"name,omitempty"`
	Columns           []string `json:"columns"`
	ReferencedTable   string   `json:"referenced_table"`
	ReferencedColumns []string `json:"referenced_columns"`
	OnDelete          string   `json:"on_delete"` // NO ACTION, CASCADE, SET NULL, SET DEFAULT, RESTRICT
	OnUpdate          string   `json:"on_update"` // NO ACTION, CASCADE, SET NULL, SET DEFAULT, RESTRICT
}

// ColumnDef represents a column definition
type ColumnDef struct {
	Name          string           `json:"name"`
	Type          string           `json:"type"` // INTEGER, TEXT, REAL, BLOB, JSON, BOOLEAN, VECTOR
	NotNull       bool             `json:"not_null"`
	Unique        bool             `json:"unique"`
	PrimaryKey    bool             `json:"primary_key"`
	AutoIncrement bool             `json:"auto_increment"`
	Default       string           `json:"default,omitempty"`
	CheckStr      string           `json:"check_str,omitempty"`  // CHECK expression as SQL text (persisted)
	CheckName     string           `json:"check_name,omitempty"` // Optional CHECK constraint name
	Check         query.Expression `json:"-"`                    // Parsed CHECK expression (not persisted)
	defaultExpr   query.Expression `json:"-"`                    // Parsed DEFAULT expression (not persisted)
	sourceTbl     string           `json:"-"`                    // Source table name for JOIN column disambiguation
	Collation     string           `json:"collation,omitempty"`  // Optional column collation name
	Dimensions    int              `json:"dimensions,omitempty"` // For VECTOR type: number of dimensions
}

// IndexDef represents an index definition
// IndexStatus tracks the lifecycle of an index.
type IndexStatus int

const (
	IndexActive   IndexStatus = iota // fully built and usable
	IndexBuilding                    // metadata created, background population in progress
)

type IndexDef struct {
	Name       string      `json:"name"`
	TableName  string      `json:"table_name"`
	Columns    []string    `json:"columns"`
	Unique     bool        `json:"unique"`
	RootPageID uint32      `json:"root_page_id"`
	Status     IndexStatus `json:"status"`
	Temporary  bool        `json:"-"`
}

// selectColInfo holds information about selected columns in a query
type selectColInfo struct {
	name             string
	tableName        string // table name for JOINs
	index            int
	isAggregate      bool
	aggregateType    string           // COUNT, SUM, AVG, MIN, MAX
	aggregateCol     string           // column name for SUM, AVG, MIN, MAX
	aggregateExpr    query.Expression // full expression for SUM(expr), AVG(expr), etc.
	aggregateArgs    []query.Expression
	aggregateSep     string           // GROUP_CONCAT separator
	aggregateSepOK   bool             // true when GROUP_CONCAT separator was explicit
	aggregateSepExpr query.Expression // GROUP_CONCAT separator expression
	aggregateOrderBy []*query.OrderByExpr
	aggregateFilter  query.Expression
	isDistinct       bool              // for COUNT(DISTINCT col)
	isWindow         bool              // true for window functions
	windowExpr       *query.WindowExpr // window function expression
	hasEmbeddedAgg   bool              // true when expression (CASE, etc.) contains aggregate calls
	originalExpr     query.Expression  // the original expression for hasEmbeddedAgg columns

	// embeddedWindows holds window functions nested inside originalExpr (e.g.
	// SUM(x) OVER () + 1); they are computed then substituted during projection.
	embeddedWindows []*query.WindowExpr
}

// TableSnapshot holds all SELECT metadata needed to execute a scan without holding Catalog.mu.
// Captured while the lock is held, then used lock-free during the scan phase.
type TableSnapshot struct {
	Def          *TableDef       // table definition
	Columns      []selectColInfo // column info for projection
	ReturnCols   []string        // output column names
	IndexMatches []string        // row keys matching WHERE index lookup
	UseIndex     bool            // whether index lookup was used
	SchemaVer    uint64          // schema version at capture time (for cache invalidation)
}

// MaterializedViewDef represents a materialized view definition
type MaterializedViewDef struct {
	Name        string                   `json:"name"`
	Columns     []string                 `json:"columns"`
	Query       *query.SelectStmt        `json:"query"`
	Data        []map[string]interface{} `json:"data"` // Cached data
	LastRefresh time.Time                `json:"last_refresh"`
}

// StatsTableStats is an alias for table statistics (defined in stats.go)
type StatsTableStats = TableStats

// FTSIndexDef represents a full-text search index definition
type FTSIndexDef struct {
	Name      string   `json:"name"`
	TableName string   `json:"table_name"`
	Columns   []string `json:"columns"`
	// Inverted index: word -> list of row IDs
	Index map[string][]int64 `json:"index"`
}

// JSONIndexDef represents a JSON index definition (GIN-like)
type JSONIndexDef struct {
	Name      string             `json:"name"`
	TableName string             `json:"table_name"`
	Column    string             `json:"column"`              // JSON column name
	Path      string             `json:"path"`                // JSON path expression (e.g., "$.name")
	DataType  string             `json:"data_type"`           // indexed data type: "string", "number", "boolean"
	Index     map[string][]int64 `json:"index"`               // value -> list of row IDs (for string values)
	NumIndex  map[string][]int64 `json:"num_index,omitempty"` // for numeric values (string key to avoid precision issues)
}

// undoAction represents the type of undo operation
type undoAction int

const (
	undoInsert                 undoAction = iota // Undo an INSERT by deleting the key
	undoUpdate                                   // Undo an UPDATE by restoring the old value
	undoDelete                                   // Undo a DELETE by restoring the key/value
	undoCreateTable                              // Undo a CREATE TABLE by dropping the table
	undoDropTable                                // Undo a DROP TABLE by restoring the table
	undoCreateIndex                              // Undo a CREATE INDEX by dropping the index
	undoDropIndex                                // Undo a DROP INDEX by restoring the index
	undoCreateFTSIndex                           // Undo a CREATE FULLTEXT INDEX by dropping the index
	undoDropFTSIndex                             // Undo DROP INDEX for a full-text index by restoring it
	undoCreateVectorIndex                        // Undo a CREATE VECTOR INDEX by dropping the index
	undoDropVectorIndex                          // Undo DROP INDEX for a vector index by restoring it
	undoAlterAddColumn                           // Undo ALTER TABLE ADD COLUMN
	undoAlterDropColumn                          // Undo ALTER TABLE DROP COLUMN
	undoAlterRename                              // Undo ALTER TABLE RENAME
	undoAlterRenameColumn                        // Undo ALTER TABLE RENAME COLUMN
	undoAlterForeignKeys                         // Undo ALTER TABLE ADD/DROP FOREIGN KEY constraint
	undoAlterChecks                              // Undo ALTER TABLE ADD/DROP CHECK constraint
	undoAutoIncSeq                               // Undo AutoIncSeq change
	undoCreateView                               // Undo CREATE VIEW by dropping the view
	undoDropView                                 // Undo DROP VIEW by restoring the view
	undoCreateTrigger                            // Undo CREATE TRIGGER by dropping the trigger
	undoDropTrigger                              // Undo DROP TRIGGER by restoring the trigger
	undoCreateProcedure                          // Undo CREATE PROCEDURE by dropping the procedure
	undoDropProcedure                            // Undo DROP PROCEDURE by restoring the procedure
	undoCreateMaterializedView                   // Undo CREATE MATERIALIZED VIEW by dropping the view
	undoDropMaterializedView                     // Undo DROP MATERIALIZED VIEW by restoring the view
	undoCreateForeignTable                       // Undo CREATE FOREIGN TABLE by dropping the foreign table
	undoDropForeignTable                         // Undo DROP FOREIGN TABLE by restoring the foreign table
	undoEnableRLSTable                           // Undo ALTER TABLE ENABLE ROW LEVEL SECURITY
	undoCreateRLSPolicy                          // Undo CREATE POLICY by dropping the policy
	undoDropRLSPolicy                            // Undo DROP POLICY by restoring the policy
)

// indexUndoEntry records an index modification for rollback
type indexUndoEntry struct {
	indexName string
	key       []byte
	oldValue  []byte // nil means the key didn't exist (was added); non-nil means it had this value
	wasAdded  bool   // true = key was added (undo = delete), false = key was deleted (undo = put oldValue)
}

// undoEntry records the pre-change state for rollback
type undoEntry struct {
	action       undoAction
	tableName    string
	key          []byte
	oldValue     []byte // nil for INSERT undo (just delete the key)
	newKey       []byte // for undoUpdate that moved the row to a new PK key; delete it on undo
	indexChanges []indexUndoEntry
	// DDL undo fields
	tableDef       *TableDef                  // For undoDropTable: original table definition
	tableTree      btree.TreeStore            // For undoDropTable: original table B-tree
	tableIndexes   map[string]*IndexDef       // For undoDropTable: indexes
	tableIdxTrees  map[string]btree.TreeStore // For undoDropTable: index B-trees
	indexDef       *IndexDef                  // For undoDropIndex: original index definition
	indexTree      btree.TreeStore            // For undoDropIndex: original index B-tree
	indexName      string                     // For undoCreateIndex: index name to drop
	ftsIndexDef    *FTSIndexDef               // For undoDropFTSIndex: original full-text index definition
	vectorIndexDef *VectorIndexDef            // For undoDropVectorIndex: original vector index definition
	// ALTER TABLE undo fields
	oldColumns           []ColumnDef                 // For undoAlterAddColumn/undoAlterDropColumn: original columns
	oldForeignKeys       []ForeignKeyDef             // For undoAlterForeignKeys: original foreign keys
	oldChecks            []CheckDef                  // For undoAlterChecks: original check constraints
	oldPrimaryKeyColumns []string                    // For undoAlterRenameColumn: original PK name
	oldName              string                      // For undoAlterRename/undoAlterRenameColumn: original name
	newName              string                      // For undoAlterRename/undoAlterRenameColumn: new name
	oldRowData           []struct{ key, val []byte } // For undoAlterDropColumn: original row data
	droppedIndexes       map[string]*IndexDef        // For undoAlterDropColumn: dropped indexes
	droppedIdxTrees      map[string]btree.TreeStore  // For undoAlterDropColumn: dropped index trees
	oldAutoIncSeq        int64                       // For undoAutoIncSeq: previous AutoIncSeq value
	viewName             string                      // For view undo actions
	viewQuery            *query.SelectStmt           // For undoDropView: original view query
	viewSQL              string                      // For view undo actions
	viewTemporary        bool                        // For temporary view undo actions
	triggerName          string                      // For trigger undo actions
	triggerStmt          *query.CreateTriggerStmt    // For undoDropTrigger: original trigger
	triggerSQL           string                      // For trigger undo actions
	procedureName        string                      // For procedure undo actions
	procedureStmt        *query.CreateProcedureStmt  // For undoDropProcedure: original procedure
	procedureSQL         string                      // For procedure undo actions
	materializedViewName string                      // For materialized view undo actions
	materializedViewDef  *MaterializedViewDef        // For undoDropMaterializedView: original view
	materializedViewSQL  string                      // For materialized view undo actions
	foreignTableName     string                      // For foreign table undo actions
	foreignTableDef      *ForeignTableDef            // For undoDropForeignTable: original foreign table
	rlsTableName         string                      // For RLS undo actions
	rlsPolicyName        string                      // For RLS policy undo actions
	rlsPolicy            *security.Policy            // For undoDropRLSPolicy: original policy
	rlsPolicies          []*security.Policy          // For undoDropTable: original table policies
	rlsTableWasEnabled   bool                        // For undoEnableRLSTable: previous table state
}

// catalogTxnState holds per-transaction state for multi-transaction support.
type catalogTxnState struct {
	txnID           uint64
	txnActive       bool
	undoLog         []undoEntry
	savepoints      []savepointEntry
	managerTxn      interface{}                        // *txn.Transaction when txnManager bridge is active
	pendingWrites   []PendingWrite                     // buffered DML for commit-time application
	pendingWriteMap map[string]map[string]PendingWrite // table -> key -> latest write (O(1) lookup)
	readValues      map[txn.WriteKey][]byte            // key -> value at time of read (for MVCC validation)
	rowBuf          [8]interface{}                     // reused per-transaction scratch buffer for INSERT
	valueDataBuf    []byte                             // reused per-transaction buffer for encoded row values
	treeCache       map[string]btree.TreeStore         // cached tree references to avoid c.mu in commit
}

// getPendingWriteMap returns the pending-write map, building it lazily from
// the slice if it hasn't been materialised yet. This lets us skip map
// allocations for single-row autocommit transactions while still supporting
// read-your-writes.
func (ts *catalogTxnState) getPendingWriteMap() map[string]map[string]PendingWrite {
	if ts.pendingWriteMap == nil && len(ts.pendingWrites) > 0 {
		rebuildPendingWriteMap(ts)
	}
	return ts.pendingWriteMap
}

// PendingWrite buffers a DML operation for commit-time application when the
// txn.Manager bridge is active. This avoids holding Catalog.mu during DML.
type PendingWrite struct {
	TreeName     string
	Key          string
	Value        []byte
	IndexUpdates []PendingIndexUpdate
}

// PendingIndexUpdate buffers an index mutation for commit-time application.
type PendingIndexUpdate struct {
	IndexName string
	Key       string
	Value     []byte // nil for delete
	IsDelete  bool
}

// Catalog manages database schema metadata
type Catalog struct {
	mu                   sync.RWMutex
	tree                 btree.TreeStore
	tables               map[string]*TableDef
	foreignTables        map[string]*ForeignTableDef // Foreign table definitions
	indexes              map[string]*IndexDef
	indexTrees           map[string]btree.TreeStore // B+Trees for indexes
	pool                 *storage.BufferPool
	wal                  *storage.WAL
	tableTrees           map[string]btree.TreeStore            // Each table has its own B+Tree
	partitionTreeMu      sync.Mutex                            // protects lazy partition tree creation
	fdwRegistry          *fdw.Registry                         // FDW registry for foreign data wrappers
	views                map[string]*query.SelectStmt          // Views store their SELECT query
	viewSQL              map[string]string                     // Original CREATE VIEW SQL for persistence
	viewTemporary        map[string]bool                       // Session-local views, not persisted
	triggers             map[string]*query.CreateTriggerStmt   // Triggers store their definition
	triggerSQL           map[string]string                     // Original CREATE TRIGGER SQL for persistence
	triggerDepth         int                                   // current trigger recursion depth (guarded by c.mu)
	procedures           map[string]*query.CreateProcedureStmt // Procedures store their definition
	procedureSQL         map[string]string                     // Original CREATE PROCEDURE SQL for persistence
	materializedViews    map[string]*MaterializedViewDef       // Materialized views
	materializedViewSQL  map[string]string                     // Original CREATE MATERIALIZED VIEW SQL for persistence
	ftsIndexes           map[string]*FTSIndexDef               // Full-text search indexes
	jsonIndexes          map[string]*JSONIndexDef              // JSON indexes for fast JSON queries
	vectorIndexes        map[string]*VectorIndexDef            // Vector (HNSW) indexes for similarity search
	stats                map[string]*StatsTableStats           // Table statistics for ANALYZE
	cteResults           map[string]*cteResultSet              // Temporary CTE result cache for recursive CTEs
	keyCounter           int64                                 // For generating unique keys
	undoLog              []undoEntry                           // Undo log for transaction rollback (legacy)
	txnManager           interface{}                           // *txn.Manager bridge for MVCC multi-writer (nil = legacy single-writer mode)
	enableBufferedWrites bool                                  // Enable buffered DML (disabled by default until read-your-writes is fully implemented)
	savepoints           []savepointEntry                      // Stack of savepoints (legacy)
	rlsManager           *security.Manager                     // Row-level security manager
	enableRLS            bool                                  // Enable row-level security
	rlsPolicies          map[string]*security.Policy           // RLS policies: key = "table:policyName"
	queryCache           *cache.Cache                          // Query result cache (owned by pkg/cache)
	rlsCtx               context.Context                       // Context for RLS user/role extraction in SELECT
	lastReturningRows    [][]interface{}                       // Last RETURNING clause results
	lastReturningColumns []string                              // Column names for RETURNING results
	returningMu          sync.Mutex                            // protects lastReturningRows/lastReturningColumns

	// Dead tuple tracking for AutoVacuum
	deadTuples map[string]int64 // table name -> count of soft-deleted rows
	liveTuples map[string]int64 // table name -> count of live rows
	vacuumMu   sync.RWMutex     // protects deadTuples and liveTuples

	// Schema versioning and cache for lock-free metadata lookups.
	// schemaVersion increments on every DDL; schemaCache stores TableDefs
	// keyed by lower-case table name. Cache entries are invalidated when
	// schemaVersion changes, so stale reads are impossible.
	schemaVersion atomic.Uint64
	schemaCache   map[string]*TableDef
	schemaCacheMu sync.RWMutex

	// Parallel execution options
	parallelWorkers   int // 0 = disabled
	parallelThreshold int // min rows to trigger parallel

	// goroutineTxnShards maps goroutine ID -> txn state using 16 independently
	// locked shards. This eliminates the single-RWMutex bottleneck under high
	// concurrency while avoiding sync.Map's per-operation allocations.
	goroutineTxnShards [16]struct {
		mu sync.RWMutex
		m  map[uint64]*catalogTxnState
	}

	// commitMu shards the commit critical section by (table,key) hash so that
	// transactions touching disjoint rows can validate and write in parallel.
	commitMu [256]sync.Mutex

	// txnStatePool recycles per-transaction state structs to reduce GC pressure
	// from high-frequency Begin/Commit cycles.
	txnStatePool sync.Pool
}

func (c *Catalog) commitLockIdx(treeName string, key string) int {
	// FNV-1a hash: much better distribution than polynomial hash for
	// sequential zero-padded keys, eliminating commitMu shard collisions.
	h := uint32(2166136261)
	for i := 0; i < len(treeName); i++ {
		h ^= uint32(treeName[i])
		h *= 16777619
	}
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h) % len(c.commitMu)
}

// savepointEntry records a named savepoint with its undo log position
type savepointEntry struct {
	name            string
	undoPos         int // Position in undoLog at time of savepoint creation
	pendingWritePos int // Position in pendingWrites at time of savepoint creation (buffered mode)
}

// cteResultSet holds pre-computed results for recursive CTEs
type cteResultSet struct {
	columns []string
	rows    [][]interface{}
}

// Query cache types and functions moved to catalog_cache.go

// New creates a new Catalog backed by the given tree store, buffer pool, and WAL.
func New(tree btree.TreeStore, pool *storage.BufferPool, wal *storage.WAL) *Catalog {
	c := &Catalog{
		tree:                tree,
		tables:              make(map[string]*TableDef),
		foreignTables:       make(map[string]*ForeignTableDef),
		indexes:             make(map[string]*IndexDef),
		indexTrees:          make(map[string]btree.TreeStore),
		pool:                pool,
		wal:                 wal,
		tableTrees:          make(map[string]btree.TreeStore),
		fdwRegistry:         fdw.NewRegistry(),
		views:               make(map[string]*query.SelectStmt),
		viewSQL:             make(map[string]string),
		viewTemporary:       make(map[string]bool),
		triggers:            make(map[string]*query.CreateTriggerStmt),
		triggerSQL:          make(map[string]string),
		procedures:          make(map[string]*query.CreateProcedureStmt),
		procedureSQL:        make(map[string]string),
		materializedViews:   make(map[string]*MaterializedViewDef),
		materializedViewSQL: make(map[string]string),
		ftsIndexes:          make(map[string]*FTSIndexDef),
		jsonIndexes:         make(map[string]*JSONIndexDef),
		vectorIndexes:       make(map[string]*VectorIndexDef),
		stats:               make(map[string]*StatsTableStats),
		rlsPolicies:         make(map[string]*security.Policy),
		keyCounter:          0,
		queryCache:          nil, // Enabled lazily via EnableQueryCache()
		deadTuples:          make(map[string]int64),
		liveTuples:          make(map[string]int64),
	}
	for i := range c.goroutineTxnShards {
		c.goroutineTxnShards[i].m = make(map[uint64]*catalogTxnState)
	}
	return c
}

func (c *Catalog) SetWAL(wal *storage.WAL) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wal = wal
}

// SetParallelOptions configures parallel query execution.
func (c *Catalog) SetParallelOptions(workers, threshold int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.parallelWorkers = workers
	c.parallelThreshold = threshold
}

// ensureVacuumMaps lazily initializes dead/live tuple tracking maps.
// This allows tests that construct Catalog directly (not via New) to work.
func (c *Catalog) ensureVacuumMaps() {
	if c.deadTuples == nil {
		c.deadTuples = make(map[string]int64)
	}
	if c.liveTuples == nil {
		c.liveTuples = make(map[string]int64)
	}
}

// tryViewSelect attempts to resolve the query against a view. Returns handled=true
// if the FROM clause references a view and the query was fully resolved.
func (cat *Catalog) tryViewSelect(stmt *query.SelectStmt, args []interface{}) (bool, []string, [][]interface{}, error) {
	view, viewErr := cat.getViewLocked(stmt.From.Name)
	if viewErr != nil {
		return false, nil, nil, nil
	}

	// A view that carries its own ORDER BY / LIMIT / OFFSET must be evaluated as
	// a derived table (the "complex" path), not inlined: the merge path copies
	// only the OUTER query's ORDER BY/LIMIT, so inlining would silently drop the
	// view's own ordering and row limit (e.g. CREATE VIEW top3 AS ... ORDER BY x
	// DESC LIMIT 3 would return every row).
	viewIsComplex := view.Distinct || len(view.GroupBy) > 0 || view.Having != nil ||
		view.From == nil || len(view.OrderBy) > 0 || view.Limit != nil || view.Offset != nil
	if !viewIsComplex {
		for _, col := range view.Columns {
			actual := col
			if ae, ok := col.(*query.AliasExpr); ok {
				_ = ae
				viewIsComplex = true
				break
			}
			if fc, ok := actual.(*query.FunctionCall); ok {
				if strings.EqualFold(fc.Name, "COUNT") || strings.EqualFold(fc.Name, "SUM") || strings.EqualFold(fc.Name, "AVG") || strings.EqualFold(fc.Name, "MIN") || strings.EqualFold(fc.Name, "MAX") || strings.EqualFold(fc.Name, "GROUP_CONCAT") {
					viewIsComplex = true
					break
				}
			}
		}
	}

	if viewIsComplex {
		viewCols, viewRows, err := cat.selectLocked(view, args)
		if err != nil {
			return true, nil, nil, err
		}
		if len(stmt.Joins) == 0 {
			cols, rows, err := cat.applyOuterQuery(stmt, viewCols, viewRows, args)
			return true, cols, rows, err
		}
		viewResultName := toLowerFast(stmt.From.Name)
		if cat.cteResults == nil {
			cat.cteResults = make(map[string]*cteResultSet)
		}
		cat.cteResults[viewResultName] = &cteResultSet{columns: viewCols, rows: viewRows}
		return false, nil, nil, nil
	}

	var mergedJoins []*query.JoinClause
	mergedJoins = append(mergedJoins, view.Joins...)
	mergedJoins = append(mergedJoins, stmt.Joins...)
	mergedFrom := &query.TableRef{Name: view.From.Name, Alias: view.From.Alias, Subquery: view.From.Subquery, SubqueryStmt: view.From.SubqueryStmt}
	if stmt.From.Alias != "" {
		mergedFrom.Alias = stmt.From.Alias
	} else if mergedFrom.Alias == "" {
		mergedFrom.Alias = stmt.From.Name
	}
	mergedStmt := &query.SelectStmt{
		Distinct: stmt.Distinct,
		Columns:  stmt.Columns,
		From:     mergedFrom,
		Joins:    mergedJoins,
		GroupBy:  stmt.GroupBy,
		Having:   stmt.Having,
		OrderBy:  stmt.OrderBy,
		Limit:    stmt.Limit,
		Offset:   stmt.Offset,
	}
	if view.Where != nil && stmt.Where != nil {
		mergedStmt.Where = &query.BinaryExpr{
			Left:     view.Where,
			Operator: query.TokenAnd,
			Right:    stmt.Where,
		}
	} else if view.Where != nil {
		mergedStmt.Where = view.Where
	} else {
		mergedStmt.Where = stmt.Where
	}
	if len(stmt.Columns) > 0 {
		if _, isStar := stmt.Columns[0].(*query.StarExpr); isStar {
			mergedStmt.Columns = view.Columns
		}
	}
	cols, rows, err := cat.selectLocked(mergedStmt, args)
	return true, cols, rows, err
}

// resolveFromTable resolves the FROM clause table name to a TableDef by
// checking the catalog table registry, CTE results, and materialized views.
func (cat *Catalog) resolveFromTable(name string) (*TableDef, error) {
	table, err := cat.getTableLocked(name)
	if err == nil {
		return table, nil
	}

	// Check if it's a CTE result
	if cat.cteResults != nil {
		if cteRes, ok := cat.cteResults[toLowerFast(name)]; ok {
			table = &TableDef{Name: name}
			for _, colName := range cteRes.columns {
				table.Columns = append(table.Columns, ColumnDef{Name: colName, Type: "TEXT"})
			}
			return table, nil
		}
	}

	// Check for materialized view
	if mv, mvErr := cat.getMaterializedViewLocked(name); mvErr == nil {
		table = &TableDef{Name: name}
		table.Columns = materializedViewColumnDefs(mv)
		// Register as temporary CTE-like result for this query
		if cat.cteResults == nil {
			cat.cteResults = make(map[string]*cteResultSet)
		}
		cols := materializedViewColumnNames(mv)
		_, rows := materializedViewColumnsAndRows(mv)
		cat.cteResults[toLowerFast(name)] = &cteResultSet{columns: cols, rows: rows}
		return table, nil
	}

	return nil, fmt.Errorf("table '%s' not found", name)
}

// selectLocked executes a SELECT while assuming cat.mu is already held.
// It never releases the lock; for the outermost Select path that may unlock
// during the scan, use selectLockedInternal directly.
func (cat *Catalog) selectLocked(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	return cat.selectLockedInternal(stmt, args, false)
}

// selectUnlocked performs the table scan and post-processing for a SELECT.
// The catalog read lock may be released during the scan to allow concurrent
// writes. The caller is responsible for ensuring recursive calls (subqueries,
// JOIN resolution, views) do not unlock mid-execution.
//
// The canUnlock flag tells selectUnlocked whether the lock was released before
// entry. When canUnlock is false the lock is already held by the caller and
// must NOT be re-acquired. When canUnlock is true the lock was released and
// this function re-acquires it as needed per the postProcessUnlocked state.
//
// Lock contract on entry/exit (when canUnlock=true):
//   - scan error: lock is reacquired before return
//   - postProcessUnlocked=false (RLS enabled): lock held during post-process, held on exit
//   - postProcessUnlocked=true (RLS disabled): lock released during post-process, reacquired before exit
//
// When canUnlock=false, the lock is never touched by this function.
func (cat *Catalog) selectUnlocked(snap TableSnapshot, stmt *query.SelectStmt, args []interface{}, collectFullRows bool, queryTime time.Time, trees []btree.TreeStore, mvRows [][]interface{}, isMV bool, parallelWorkers int, parallelThreshold int, returnColumns []string, hiddenOrderByCols int, hasWindowFuncs bool, canUnlock bool) ([]string, [][]interface{}, error) {
	rows, windowFullRows, scanErr := cat.scanTableRowsWithSnapshot(snap, stmt, args, collectFullRows, queryTime, trees, mvRows, isMV, parallelWorkers, parallelThreshold)
	if scanErr != nil {
		if canUnlock {
			cat.mu.RLock()
		}
		return returnColumns, nil, scanErr
	}

	postProcessUnlocked := cat.canApplySelectPostProcessUnlocked()
	if canUnlock && !postProcessUnlocked {
		cat.mu.RLock()
	}

	returnColumns, rows = cat.applySelectPostProcess(applySelectPostProcessParams{
		rows:              rows,
		selectCols:        snap.Columns,
		stmt:              stmt,
		args:              args,
		returnColumns:     returnColumns,
		hiddenOrderByCols: hiddenOrderByCols,
		hasWindowFuncs:    hasWindowFuncs,
		windowFullRows:    windowFullRows,
		table:             snap.Def,
	})

	if canUnlock && postProcessUnlocked {
		cat.mu.RLock()
	}
	if rows == nil && returnColumns != nil {
		return returnColumns, nil, nil
	}

	return returnColumns, rows, nil
}

// selectLockedInternal is the core SELECT implementation. When canReleaseLock is
// true and the statement has no subqueries, the catalog read lock is released
// during the heavy table scan so concurrent writes can proceed. Recursive calls
// (subqueries, JOIN resolution, views) must pass canReleaseLock=false.
func (cat *Catalog) selectLockedInternal(stmt *query.SelectStmt, args []interface{}, canReleaseLock bool) ([]string, [][]interface{}, error) {
	if err := validateSelectBounds(cat, stmt, args); err != nil {
		return nil, nil, err
	}

	// Apply query optimization only when the query has something to optimize.
	// Skip the expensive allocator for simple SELECTs without WHERE/JOINs/etc.
	needsOptimize := stmt.Where != nil || len(stmt.Joins) > 0 || stmt.GroupBy != nil ||
		stmt.Having != nil || len(stmt.OrderBy) > 0 || stmt.Distinct ||
		len(stmt.Columns) == 0 || stmt.Limit != nil || stmt.Offset != nil ||
		stmt.AsOf != nil
	if needsOptimize {
		optimizer := query.NewQueryOptimizer()
		if optimizedStmt, err := optimizer.OptimizeSelect(stmt); err == nil && optimizedStmt != nil {
			stmt = optimizedStmt
		}
	}

	// Resolve positional references in GROUP BY and ORDER BY (e.g., GROUP BY 1, ORDER BY 2)
	stmt = resolvePositionalRefs(stmt)

	// Handle AS OF temporal queries
	queryTime := time.Now()
	if stmt.AsOf != nil {
		ts, err := cat.evaluateTemporalExpr(stmt.AsOf, args)
		if err != nil {
			return nil, nil, fmt.Errorf("AS OF expression error: %w", err)
		}
		queryTime = *ts
	}

	// Handle SELECT without FROM clause (scalar expressions)
	if stmt.From == nil {
		return cat.executeScalarSelect(stmt, args)
	}

	rlsNeedsBaseRows := cat.selectNeedsFullRowsForRLS(stmt.From.Name)

	// Fast path: SELECT COUNT(*) FROM table [WHERE ...] — skip row decoding
	if !rlsNeedsBaseRows {
		if cols, rows, ok, err := cat.tryCountStarFastPath(stmt, args, queryTime); err != nil {
			return nil, nil, err
		} else if ok {
			return cols, rows, nil
		}
	}

	// Fast path: SELECT SUM/AVG/MIN/MAX/COUNT(col) FROM table — streaming aggregates
	if !rlsNeedsBaseRows {
		if cols, rows, ok, err := cat.trySimpleAggregateFastPath(stmt, args); err != nil {
			return nil, nil, err
		} else if ok {
			return cols, rows, nil
		}
	}

	// Handle derived tables: FROM (SELECT ...) AS alias or FROM (SELECT ... UNION ...) AS alias
	if stmt.From.Subquery != nil || stmt.From.SubqueryStmt != nil {
		subCols, subRows, err := cat.executeDerivedTable(stmt.From, args)
		if err != nil {
			return nil, nil, fmt.Errorf("error in derived table '%s': %w", stmt.From.Alias, err)
		}
		if len(stmt.Joins) == 0 {
			return cat.applyOuterQuery(stmt, subCols, subRows, args)
		}
		// Derived table with JOINs: store as temporary CTE result so
		// executeSelectWithJoin can resolve it
		if cat.cteResults == nil {
			cat.cteResults = make(map[string]*cteResultSet)
		}
		dtName := toLowerFast(stmt.From.Alias)
		cat.cteResults[dtName] = &cteResultSet{columns: subCols, rows: subRows}
		defer delete(cat.cteResults, dtName)
		// Fall through to normal JOIN handling
	}

	// Check if it's a pre-computed CTE result (from recursive CTE execution)
	if cat.cteResults != nil {
		if cteRes, ok := cat.cteResults[toLowerFast(stmt.From.Name)]; ok {
			if result, handled := cat.handleCTEResult(stmt, args, cteRes); handled {
				return result.cols, result.rows, result.err
			}
			// CTE with JOINs: fall through to executeSelectWithJoin
		}
	}

	// Check if it's a view first
	if handled, cols, rows, err := cat.tryViewSelect(stmt, args); handled {
		return cols, rows, err
	}

	// Not a view - try to get as a table (or CTE result for JOIN queries)
	table, err := cat.resolveFromTable(stmt.From.Name)
	if err != nil {
		return nil, nil, err
	}

	mainTableRef := stmt.From.Name
	if stmt.From.Alias != "" {
		mainTableRef = stmt.From.Alias
	}

	selectCols, returnColumns, hasAggregates := cat.buildSelectColumnInfo(stmt, table, mainTableRef)

	// Handle JOINs if present
	if len(stmt.Joins) > 0 {
		// Check if we need GROUP BY or aggregates with JOIN
		if hasAggregates || len(stmt.GroupBy) > 0 {
			// Add hidden aggregates from HAVING and ORDER BY clauses that aren't in SELECT
			augSelectCols, hiddenHavingCount := addHiddenHavingAggregates(stmt.Having, selectCols, mainTableRef)
			augSelectCols, hiddenOrderByCount := addHiddenOrderByAggregates(stmt.OrderBy, augSelectCols, mainTableRef)
			hiddenCount := hiddenHavingCount + hiddenOrderByCount
			cols, rows, err := cat.executeSelectWithJoinAndGroupBy(stmt, args, augSelectCols, returnColumns)
			if err != nil {
				return nil, nil, err
			}
			// Apply ORDER BY for JOIN+GROUP BY results
			if len(stmt.OrderBy) > 0 {
				rows = cat.applyGroupByOrderBy(rows, augSelectCols, stmt.OrderBy)
			}
			// Apply OFFSET
			if stmt.Offset != nil {
				offsetVal, err := evaluateExpression(cat, nil, nil, stmt.Offset, args)
				if err == nil {
					if offset, ok := toInt(offsetVal); ok && offset > 0 {
						if offset >= len(rows) {
							rows = nil
						} else {
							rows = rows[offset:]
						}
					}
				}
			}
			// Apply LIMIT
			if stmt.Limit != nil {
				limitVal, err := evaluateExpression(cat, nil, nil, stmt.Limit, args)
				if err == nil {
					if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(rows) {
						rows = rows[:limit]
					}
				}
			}
			// Remove hidden columns from result rows
			if hiddenCount > 0 {
				visibleCount := len(augSelectCols) - hiddenCount
				for i, row := range rows {
					if len(row) > visibleCount {
						rows[i] = row[:visibleCount]
					}
				}
			}
			return cols, rows, nil
		}
		return cat.executeSelectWithJoin(stmt, args, selectCols)
	}

	// If we have aggregates or GROUP BY, handle them differently
	if hasAggregates || len(stmt.GroupBy) > 0 {
		// Add hidden aggregates from HAVING and ORDER BY clauses that aren't in SELECT
		augSelectCols, hiddenHavingCount := addHiddenHavingAggregates(stmt.Having, selectCols, mainTableRef)
		augSelectCols, hiddenOrderByCount := addHiddenOrderByAggregates(stmt.OrderBy, augSelectCols, mainTableRef)
		hiddenCount := hiddenHavingCount + hiddenOrderByCount
		cols, rows, err := cat.computeAggregatesWithGroupBy(table, stmt, args, augSelectCols, returnColumns)
		if err != nil {
			return nil, nil, err
		}
		// Remove hidden columns from result rows
		if hiddenCount > 0 {
			visibleCount := len(augSelectCols) - hiddenCount
			for i, row := range rows {
				if len(row) > visibleCount {
					rows[i] = row[:visibleCount]
				}
			}
		}
		return cols, rows, nil
	}

	// Add hidden ORDER BY columns not in SELECT list
	hiddenOrderByCols := 0
	if len(stmt.OrderBy) > 0 {
		selectCols, hiddenOrderByCols = addHiddenOrderByCols(stmt.OrderBy, selectCols, table)
	}

	// Detect window functions early so we can collect full rows for ORDER BY evaluation
	hasWindowFuncs := false
	for _, ci := range selectCols {
		if ci.isWindow || len(ci.embeddedWindows) > 0 {
			hasWindowFuncs = true
			break
		}
	}
	collectFullRows := hasWindowFuncs || rlsNeedsBaseRows
	// Prepare scan parameters while holding catalog lock.
	// Materialized views have no physical B-tree; resolveFromTable stores
	// their data in cteResults, so only suppress missing physical storage when
	// that materialized-view snapshot is present.
	var mvRows [][]interface{}
	var isMV bool
	trees, err := cat.getTableTreesForScanWithOptions(table, cat.buildFDWScanOptions(stmt, args))
	if err != nil {
		if cat.cteResults == nil {
			return nil, nil, err
		}
		cteRes, ok := cat.cteResults[toLowerFast(stmt.From.Name)]
		if !ok {
			return nil, nil, err
		}
		mvRows = cteRes.rows
		isMV = true
	}
	var indexMatches []string
	var useIndex bool
	if stmt.Where != nil {
		indexMatches, useIndex, err = cat.useIndexForQueryWithArgs(stmt.From.Name, stmt.Where, args)
		if err != nil {
			return nil, nil, err
		}
	}
	// For statements without subqueries, release the catalog lock during the
	// heavy scan so writes can proceed. Subqueries would recursively call
	// selectLocked, making lock release unsafe (non-reentrant RWMutex).
	// To enable lock release, we capture all metadata that the scan needs
	// while the lock is still held. This eliminates the lock-as-read-serialize bottleneck.
	tableSnapshot := TableSnapshot{
		Def:          table,
		Columns:      selectCols,
		ReturnCols:   returnColumns,
		IndexMatches: indexMatches,
		UseIndex:     useIndex,
		SchemaVer:    cat.schemaVersion.Load(),
	}
	if !isMV && len(trees) == 0 && cat.cteResults != nil {
		if cteRes, ok := cat.cteResults[toLowerFast(stmt.From.Name)]; ok {
			mvRows = cteRes.rows
			isMV = true
		}
	}
	canUnlock := canReleaseLock && !hasSubqueries(stmt)
	if canUnlock {
		cat.mu.RUnlock()
	}
	return cat.selectUnlocked(tableSnapshot, stmt, args, collectFullRows, queryTime, trees, mvRows, isMV, cat.parallelWorkers, cat.parallelThreshold, returnColumns, hiddenOrderByCols, hasWindowFuncs, canUnlock)
}

func (cat *Catalog) canApplySelectPostProcessUnlocked() bool {
	return !cat.enableRLS || cat.rlsManager == nil
}

func (cat *Catalog) selectNeedsFullRowsForRLS(tableName string) bool {
	if !cat.enableRLS || cat.rlsManager == nil || !cat.rlsManager.IsEnabled(tableName) {
		return false
	}
	rlsCtx := cat.rlsCtx
	if rlsCtx == nil {
		return false
	}
	user, _ := rlsContext(rlsCtx)
	return user != ""
}

// scanTableRowsWithSnapshot is a thin wrapper around scanTableRows that accepts
// a TableSnapshot instead of individual parameters. This enables the caller to
// capture all needed metadata while holding the lock, then release the lock
// before scanning.
func (cat *Catalog) scanTableRowsWithSnapshot(snap TableSnapshot, stmt *query.SelectStmt, args []interface{}, hasWindowFuncs bool, queryTime time.Time, trees []btree.TreeStore, mvRows [][]interface{}, isMV bool, parallelWorkers int, parallelThreshold int) ([][]interface{}, [][]interface{}, error) {
	return cat.scanTableRows(snap.Def, stmt, args, snap.Columns, hasWindowFuncs, queryTime, trees, snap.IndexMatches, snap.UseIndex, mvRows, isMV, parallelWorkers, parallelThreshold)
}

// scanTableRows reads rows from the table using index lookup, materialized view data, or full scan.
func (cat *Catalog) scanTableRows(table *TableDef, stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, hasWindowFuncs bool, queryTime time.Time, trees []btree.TreeStore, indexMatches []string, useIndex bool, mvRows [][]interface{}, isMV bool, parallelWorkers int, parallelThreshold int) ([][]interface{}, [][]interface{}, error) {
	var rows [][]interface{}
	var windowFullRows [][]interface{}

	if len(trees) == 0 && !isMV {
		return nil, nil, nil
	}

	// Compute early termination limit for LIMIT/OFFSET without ORDER BY/DISTINCT/window.
	earlyLimit := 0
	if stmt.Limit != nil && len(stmt.OrderBy) == 0 && !stmt.Distinct && !hasWindowFuncs {
		if limitVal, err := evaluateExpression(cat, nil, nil, stmt.Limit, args); err == nil {
			if limit, ok := toInt(limitVal); ok && limit > 0 {
				earlyLimit = int(limit)
				if stmt.Offset != nil {
					if offsetVal, err := evaluateExpression(cat, nil, nil, stmt.Offset, args); err == nil {
						if offset, ok := toInt(offsetVal); ok && offset > 0 {
							earlyLimit += int(offset)
						}
					}
				}
			}
		}
	}

	if useIndex {
		for _, pk := range indexMatches {
			var valueData []byte
			var found bool
			for _, tree := range trees {
				data, err := tree.Get([]byte(pk))
				if err == nil {
					valueData = data
					found = true
					break
				}
			}
			// Read-your-writes: pending writes override committed data.
			if ts := cat.getCurrentTxn(); ts != nil {
				if m, ok := ts.getPendingWriteMap()[table.Name]; ok {
					if pw, ok2 := m[pk]; ok2 {
						valueData = pw.Value
						found = true
					}
				}
			}
			if !found {
				continue
			}
			selectedRow, fullRow, ok, err := cat.filterAndProjectRow(valueData, table, stmt, selectCols, args, queryTime, hasWindowFuncs)
			if err != nil {
				return nil, nil, err
			}
			if !ok {
				continue
			}
			rows = append(rows, selectedRow)
			if hasWindowFuncs {
				fullRowCopy := make([]interface{}, len(fullRow))
				copy(fullRowCopy, fullRow)
				windowFullRows = append(windowFullRows, fullRowCopy)
			}
		}
	} else if isMV {
		for _, fullRow := range mvRows {
			if stmt.Where != nil {
				matched, err := evaluateWhere(cat, fullRow, table.Columns, stmt.Where, args)
				if err != nil || !matched {
					continue
				}
			}
			selectedRow := cat.projectSelectedRow(fullRow, selectCols, stmt, table, args, hasWindowFuncs)
			rows = append(rows, selectedRow)
			if hasWindowFuncs {
				fullRowCopy := make([]interface{}, len(fullRow))
				copy(fullRowCopy, fullRow)
				windowFullRows = append(windowFullRows, fullRowCopy)
			}
		}
	} else {
		// Fast path: single tree with no pending writes and no parallel execution.
		// Process rows directly from the iterator, skipping pairs/seen allocation.
		ts := cat.getCurrentTxn()
		hasPending := ts != nil
		if hasPending {
			if _, ok := ts.getPendingWriteMap()[table.Name]; !ok {
				hasPending = false
			}
		}
		canParallel := parallelWorkers > 0 &&
			len(stmt.OrderBy) == 0 &&
			!stmt.Distinct &&
			!hasWindowFuncs &&
			!hasSubqueries(stmt) &&
			stmt.Limit == nil &&
			stmt.Offset == nil

		if len(trees) == 1 && !hasPending {
			iter, err := trees[0].Scan(nil, nil)
			if err != nil {
				return nil, nil, fmt.Errorf("select: failed to scan table %s: %w", table.Name, err)
			}
			if cap(rows) == 0 {
				rows = make([][]interface{}, 0, trees[0].Size())
			}
			if hasWindowFuncs && cap(windowFullRows) == 0 {
				windowFullRows = make([][]interface{}, 0, trees[0].Size())
			}
			numCols := len(table.Columns)
			flatCap := int(trees[0].Size()) * numCols
			flatBuf := make([]interface{}, 0, flatCap)
			stringBuf := make([]string, flatCap)
			rowIdx := 0
			stringIdx := 0

			for iter.HasNext() {
				_, valueData, err := iter.NextString()
				if err != nil {
					iter.Close()
					return nil, nil, fmt.Errorf("select: failed to read table %s: %w", table.Name, err)
				}

				start := rowIdx * numCols
				end := start + numCols
				if end > cap(flatBuf) {
					grow := end - cap(flatBuf)
					if grow < numCols*16 {
						grow = numCols * 16
					}
					flatBuf = append(flatBuf, make([]interface{}, grow)...)
				}
				flatBuf = flatBuf[:end]
				row := flatBuf[start:end]

				vrow, sidx, ok := decodeVersionedRowFastEx(valueData, numCols, row, stringBuf, stringIdx)
				stringIdx = sidx
				if !ok {
					vrow, err = decodeVersionedRow(valueData, numCols)
					if err != nil {
						iter.Close()
						return nil, nil, fmt.Errorf("select: failed to decode row in table %s: %w", table.Name, err)
					}
				}
				if !vrow.Version.isVisibleAt(queryTime) {
					continue
				}
				fullRow := vrow.Data
				if stmt.Where != nil {
					matched, err := evaluateWhere(cat, fullRow, table.Columns, stmt.Where, args)
					if err != nil || !matched {
						continue
					}
				}
				selectedRow := cat.projectSelectedRow(fullRow, selectCols, stmt, table, args, hasWindowFuncs)
				rows = append(rows, selectedRow)
				if hasWindowFuncs {
					fullRowCopy := make([]interface{}, len(fullRow))
					copy(fullRowCopy, fullRow)
					windowFullRows = append(windowFullRows, fullRowCopy)
				}
				if earlyLimit > 0 && len(rows) >= earlyLimit {
					break
				}
				rowIdx++
			}
			iter.Close()
		} else {
			type kvPair struct {
				key   string
				value []byte
			}
			totalSize := 0
			for _, tree := range trees {
				totalSize += tree.Size()
			}
			pairs := make([]kvPair, 0, totalSize)
			seen := make(map[string]int, totalSize)

			for _, tree := range trees {
				iter, err := tree.Scan(nil, nil)
				if err != nil {
					return nil, nil, fmt.Errorf("select: failed to scan table %s: %w", table.Name, err)
				}
				for iter.HasNext() {
					k, valueData, err := iter.NextString()
					if err != nil {
						iter.Close()
						return nil, nil, fmt.Errorf("select: failed to read table %s: %w", table.Name, err)
					}
					pairs = append(pairs, kvPair{k, valueData})
					seen[k] = len(pairs) - 1
				}
				iter.Close()
			}

			// Read-your-writes: overlay buffered writes (INSERT, UPDATE, DELETE).
			if hasPending {
				if m, ok := ts.getPendingWriteMap()[table.Name]; ok {
					for _, pw := range m {
						k := string(pw.Key)
						if idx, ok := seen[k]; ok {
							pairs[idx].value = pw.Value
						} else {
							pairs = append(pairs, kvPair{k, pw.Value})
							seen[k] = len(pairs) - 1
						}
					}
				}
			}

			canParallel = canParallel && len(pairs) >= parallelThreshold

			if canParallel {
				values := make([][]byte, len(pairs))
				for i, p := range pairs {
					if _, err := decodeVersionedRow(p.value, len(table.Columns)); err != nil {
						return nil, nil, fmt.Errorf("select: failed to decode row in table %s: %w", table.Name, err)
					}
					values[i] = p.value
				}
				results := parallel.ParallelSelectRows(values, parallelWorkers, parallelThreshold,
					func(chunk [][]byte) [][]interface{} {
						chunkRows, _, _ := cat.processRowChunk(chunk, table, selectCols, stmt, args, queryTime, false)
						return chunkRows
					})
				rows = append(rows, results...)
			} else {
				if cap(rows) == 0 {
					rows = make([][]interface{}, 0, len(pairs))
				}
				if hasWindowFuncs && cap(windowFullRows) == 0 {
					windowFullRows = make([][]interface{}, 0, len(pairs))
				}
				for _, p := range pairs {
					selectedRow, fullRow, ok, err := cat.filterAndProjectRow(p.value, table, stmt, selectCols, args, queryTime, hasWindowFuncs)
					if err != nil {
						return nil, nil, err
					}
					if !ok {
						continue
					}
					rows = append(rows, selectedRow)
					if hasWindowFuncs {
						fullRowCopy := make([]interface{}, len(fullRow))
						copy(fullRowCopy, fullRow)
						windowFullRows = append(windowFullRows, fullRowCopy)
					}
					if earlyLimit > 0 && len(rows) >= earlyLimit {
						break
					}
				}
			}
		}
	}

	return rows, windowFullRows, nil
}

// filterAndProjectRow decodes a versioned row, checks visibility at queryTime,
// evaluates the WHERE clause, and projects the selected columns.
// Returns (selectedRow, fullRow, ok, err) where:
//   - selectedRow is the projected row to append to results
//   - fullRow is the decoded row data (for window function tracking)
//   - ok=true means the row passed visibility and WHERE filters and should be kept
//   - err is non-nil only for decode failures (not for invisible/unmatched rows)
//
// This consolidates the decode → visibility → WHERE → project pattern used
// across index scans, MV scans, and B-tree sequential scans.
func (cat *Catalog) filterAndProjectRow(valueData []byte, table *TableDef, stmt *query.SelectStmt, selectCols []selectColInfo, args []interface{}, queryTime time.Time, hasWindowFuncs bool) (selectedRow []interface{}, fullRow []interface{}, ok bool, err error) {
	vrow, err := decodeVersionedRow(valueData, len(table.Columns))
	if err != nil {
		return nil, nil, false, fmt.Errorf("select: failed to decode row in table %s: %w", table.Name, err)
	}
	if !vrow.Version.isVisibleAt(queryTime) {
		return nil, nil, false, nil
	}
	fullRow = vrow.Data
	if stmt.Where != nil {
		matched, err := evaluateWhere(cat, fullRow, table.Columns, stmt.Where, args)
		if err != nil || !matched {
			return nil, nil, false, nil
		}
	}
	selectedRow = cat.projectSelectedRow(fullRow, selectCols, stmt, table, args, hasWindowFuncs)
	return selectedRow, fullRow, true, nil
}

// getEffectiveTableData returns all live rows for a table as a map of key to
// versioned row data, merging committed B-tree data with pending buffered
// writes for read-your-writes visibility.
func (c *Catalog) getEffectiveTableData(table *TableDef) (map[string][]byte, error) {
	result := make(map[string][]byte)
	trees, _ := c.getTableTreesForScan(table)
	for _, tree := range trees {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return nil, fmt.Errorf("select: failed to scan join table %s: %w", table.Name, err)
		}
		for iter.HasNext() {
			k, valueData, err := iter.NextString()
			if err != nil {
				iter.Close()
				return nil, fmt.Errorf("select: failed to read join table %s: %w", table.Name, err)
			}
			if !bytesContainDeletedAt(valueData) {
				result[k] = valueData
				continue
			}
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				iter.Close()
				return nil, fmt.Errorf("select: failed to decode row in join table %s: %w", table.Name, err)
			}
			if vrow.Version.DeletedAt == 0 {
				result[k] = valueData
			}
		}
		iter.Close()
	}
	if ts := c.getCurrentTxn(); ts != nil {
		if m, ok := ts.pendingWriteMap[table.Name]; ok {
			for _, pw := range m {
				k := string(pw.Key)
				vrow, err := decodeVersionedRow(pw.Value, len(table.Columns))
				if err != nil {
					return nil, fmt.Errorf("select: failed to decode pending row in join table %s: %w", table.Name, err)
				}
				if vrow.Version.DeletedAt > 0 {
					delete(result, k)
				} else {
					result[k] = pw.Value
				}
			}
		}
	}
	return result, nil
}

func resolvePositionalRefs(stmt *query.SelectStmt) *query.SelectStmt {
	if stmt == nil {
		return stmt
	}

	modified := false

	// Resolve GROUP BY positional references
	var newGroupBy []query.Expression
	if len(stmt.GroupBy) > 0 {
		newGroupBy = make([]query.Expression, len(stmt.GroupBy))
		for i, gb := range stmt.GroupBy {
			if nl, ok := gb.(*query.NumberLiteral); ok {
				pos := int(nl.Value)
				if pos >= 1 && pos <= len(stmt.Columns) {
					// Replace with the SELECT column expression (unwrap alias if present)
					col := stmt.Columns[pos-1]
					if ae, ok := col.(*query.AliasExpr); ok {
						newGroupBy[i] = ae.Expr
					} else {
						newGroupBy[i] = col
					}
					modified = true
					continue
				}
			}
			newGroupBy[i] = gb
		}
	}

	// Resolve ORDER BY positional references
	var newOrderBy []*query.OrderByExpr
	if len(stmt.OrderBy) > 0 {
		newOrderBy = make([]*query.OrderByExpr, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			if nl, ok := ob.Expr.(*query.NumberLiteral); ok {
				pos := int(nl.Value)
				if pos >= 1 && pos <= len(stmt.Columns) {
					col := stmt.Columns[pos-1]
					var expr query.Expression
					if ae, ok := col.(*query.AliasExpr); ok {
						expr = ae.Expr
					} else {
						expr = col
					}
					newOrderBy[i] = &query.OrderByExpr{Expr: expr, Desc: ob.Desc, NullsFirst: ob.NullsFirst, NullsSpecified: ob.NullsSpecified}
					modified = true
					continue
				}
			}
			newOrderBy[i] = ob
		}
	}

	if !modified {
		return stmt
	}

	// Return a shallow copy with resolved references
	result := *stmt
	if newGroupBy != nil {
		result.GroupBy = newGroupBy
	}
	if newOrderBy != nil {
		result.OrderBy = newOrderBy
	}
	return &result
}

func resolveOuterRefsInQuery(subquery *query.SelectStmt, outerRow []interface{}, outerColumns []ColumnDef) *query.SelectStmt {
	if subquery == nil || outerRow == nil || len(outerColumns) == 0 {
		return subquery
	}

	// Build set of table names that are "inner" (part of the subquery itself)
	// When an alias exists, only register the alias as inner - the original table name
	// may refer to the outer query's table in correlated subqueries.
	innerTables := make(map[string]bool)
	if subquery.From != nil {
		if subquery.From.Alias != "" {
			innerTables[toLowerFast(subquery.From.Alias)] = true
		} else {
			innerTables[toLowerFast(subquery.From.Name)] = true
		}
	}
	for _, join := range subquery.Joins {
		if join.Table != nil {
			if join.Table.Alias != "" {
				innerTables[toLowerFast(join.Table.Alias)] = true
			} else {
				innerTables[toLowerFast(join.Table.Name)] = true
			}
		}
	}

	// Clone the subquery and resolve outer references
	result := *subquery // shallow copy

	// Resolve in SELECT columns
	if len(result.Columns) > 0 {
		newCols := make([]query.Expression, len(result.Columns))
		for i, col := range result.Columns {
			newCols[i] = resolveOuterRefsInExpr(col, outerRow, outerColumns, innerTables)
		}
		result.Columns = newCols
	}

	// Resolve in WHERE clause
	if result.Where != nil {
		result.Where = resolveOuterRefsInExpr(result.Where, outerRow, outerColumns, innerTables)
	}

	// Resolve in HAVING clause
	if result.Having != nil {
		result.Having = resolveOuterRefsInExpr(result.Having, outerRow, outerColumns, innerTables)
	}

	// Resolve in ORDER BY
	if len(result.OrderBy) > 0 {
		newOrderBy := make([]*query.OrderByExpr, len(result.OrderBy))
		for i, ob := range result.OrderBy {
			newOB := *ob
			newOB.Expr = resolveOuterRefsInExpr(newOB.Expr, outerRow, outerColumns, innerTables)
			newOrderBy[i] = &newOB
		}
		result.OrderBy = newOrderBy
	}

	// Resolve in GROUP BY
	if len(result.GroupBy) > 0 {
		newGroupBy := make([]query.Expression, len(result.GroupBy))
		for i, gb := range result.GroupBy {
			newGroupBy[i] = resolveOuterRefsInExpr(gb, outerRow, outerColumns, innerTables)
		}
		result.GroupBy = newGroupBy
	}

	// Resolve in join conditions
	if len(result.Joins) > 0 {
		newJoins := make([]*query.JoinClause, len(result.Joins))
		for i, join := range result.Joins {
			newJoin := *join
			if newJoin.Condition != nil {
				newJoin.Condition = resolveOuterRefsInExpr(newJoin.Condition, outerRow, outerColumns, innerTables)
			}
			newJoins[i] = &newJoin
		}
		result.Joins = newJoins
	}
	return &result
}

func resolveOuterRefsInExpr(expr query.Expression, outerRow []interface{}, outerColumns []ColumnDef, innerTables map[string]bool) query.Expression {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *query.QualifiedIdentifier:
		tableName := toLowerFast(e.Table)
		// If this table is NOT in the inner query's tables, it's an outer reference
		if !innerTables[tableName] {
			// Find the column in outer columns by matching sourceTbl and column name
			for i, col := range outerColumns {
				if strings.EqualFold(col.sourceTbl, e.Table) &&
					strings.EqualFold(col.Name, e.Column) {
					if i < len(outerRow) {
						return valueToExpr(outerRow[i])
					}
				}
			}
			// Fallback: match just column name (for unqualified outer columns)
			for i, col := range outerColumns {
				if strings.EqualFold(col.Name, e.Column) && i < len(outerRow) {
					return valueToExpr(outerRow[i])
				}
			}
		}
		return e
	case *query.BinaryExpr:
		left := resolveOuterRefsInExpr(e.Left, outerRow, outerColumns, innerTables)
		right := resolveOuterRefsInExpr(e.Right, outerRow, outerColumns, innerTables)
		if left != e.Left || right != e.Right {
			return &query.BinaryExpr{Left: left, Operator: e.Operator, Right: right}
		}
		return e
	case *query.UnaryExpr:
		inner := resolveOuterRefsInExpr(e.Expr, outerRow, outerColumns, innerTables)
		if inner != e.Expr {
			return &query.UnaryExpr{Operator: e.Operator, Expr: inner}
		}
		return e
	case *query.FunctionCall:
		newArgs := make([]query.Expression, len(e.Args))
		changed := false
		for i, arg := range e.Args {
			newArgs[i] = resolveOuterRefsInExpr(arg, outerRow, outerColumns, innerTables)
			if newArgs[i] != arg {
				changed = true
			}
		}
		newOrderBy := make([]*query.OrderByExpr, len(e.OrderBy))
		for i, ob := range e.OrderBy {
			if ob == nil {
				continue
			}
			copied := *ob
			copied.Expr = resolveOuterRefsInExpr(ob.Expr, outerRow, outerColumns, innerTables)
			if copied.Expr != ob.Expr {
				changed = true
			}
			newOrderBy[i] = &copied
		}
		newFilter := resolveOuterRefsInExpr(e.Filter, outerRow, outerColumns, innerTables)
		if newFilter != e.Filter {
			changed = true
		}
		if changed {
			return &query.FunctionCall{Name: e.Name, Args: newArgs, Distinct: e.Distinct, OrderBy: newOrderBy, Filter: newFilter}
		}
		return e
	case *query.InExpr:
		left := resolveOuterRefsInExpr(e.Expr, outerRow, outerColumns, innerTables)
		newList := make([]query.Expression, len(e.List))
		changed := left != e.Expr
		for i, v := range e.List {
			newList[i] = resolveOuterRefsInExpr(v, outerRow, outerColumns, innerTables)
			if newList[i] != v {
				changed = true
			}
		}
		if changed {
			return &query.InExpr{Expr: left, List: newList, Not: e.Not, Subquery: e.Subquery}
		}
		return e
	case *query.BetweenExpr:
		expr := resolveOuterRefsInExpr(e.Expr, outerRow, outerColumns, innerTables)
		lower := resolveOuterRefsInExpr(e.Lower, outerRow, outerColumns, innerTables)
		upper := resolveOuterRefsInExpr(e.Upper, outerRow, outerColumns, innerTables)
		if expr != e.Expr || lower != e.Lower || upper != e.Upper {
			return &query.BetweenExpr{Expr: expr, Lower: lower, Upper: upper, Not: e.Not}
		}
		return e
	case *query.IsNullExpr:
		inner := resolveOuterRefsInExpr(e.Expr, outerRow, outerColumns, innerTables)
		if inner != e.Expr {
			return &query.IsNullExpr{Expr: inner, Not: e.Not}
		}
		return e
	case *query.LikeExpr:
		expr := resolveOuterRefsInExpr(e.Expr, outerRow, outerColumns, innerTables)
		pattern := resolveOuterRefsInExpr(e.Pattern, outerRow, outerColumns, innerTables)
		if expr != e.Expr || pattern != e.Pattern {
			return &query.LikeExpr{Expr: expr, Pattern: pattern, Not: e.Not}
		}
		return e
	case *query.CaseExpr:
		caseExpr := resolveOuterRefsInExpr(e.Expr, outerRow, outerColumns, innerTables)
		newWhens := make([]*query.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			cond := resolveOuterRefsInExpr(w.Condition, outerRow, outerColumns, innerTables)
			result := resolveOuterRefsInExpr(w.Result, outerRow, outerColumns, innerTables)
			newWhens[i] = &query.WhenClause{Condition: cond, Result: result}
		}
		elseExpr := resolveOuterRefsInExpr(e.Else, outerRow, outerColumns, innerTables)
		return &query.CaseExpr{Expr: caseExpr, Whens: newWhens, Else: elseExpr}
	case *query.AliasExpr:
		inner := resolveOuterRefsInExpr(e.Expr, outerRow, outerColumns, innerTables)
		if inner != e.Expr {
			return &query.AliasExpr{Expr: inner, Alias: e.Alias}
		}
		return e
	default:
		return e
	}
}

func tokenTypeToColumnType(t query.TokenType) string {
	switch t {
	case query.TokenInteger:
		return "INTEGER"
	case query.TokenText:
		return "TEXT"
	case query.TokenReal:
		return "REAL"
	case query.TokenBlob:
		return "BLOB"
	case query.TokenBoolean:
		return "BOOLEAN"
	case query.TokenJSON:
		return "JSON"
	case query.TokenDate:
		return "DATE"
	case query.TokenTimestamp:
		return "TIMESTAMP"
	case query.TokenVector:
		return "VECTOR"
	default:
		return "TEXT"
	}
}

func (t *TableDef) buildColumnIndexCache() {
	t.columnIndices = make(map[string]int, len(t.Columns))
	for i, col := range t.Columns {
		t.columnIndices[toLowerFast(col.Name)] = i
	}
}

func (t *TableDef) GetColumnIndex(name string) int {
	if t.columnIndices == nil {
		t.buildColumnIndexCache()
	}
	// Fast path: exact case match (common when parsed SQL matches DDL).
	if idx, ok := t.columnIndices[name]; ok {
		return idx
	}
	// Fallback: case-insensitive lookup.
	if idx, ok := t.columnIndices[toLowerFast(name)]; ok {
		return idx
	}
	return -1
}

func (t *TableDef) isPrimaryKeyColumn(name string) bool {
	for _, pkCol := range t.PrimaryKey {
		if strings.EqualFold(pkCol, name) {
			return true
		}
	}
	return false
}

// getPartitionTreeName returns the partition tree name for a row value
// Returns empty string if table is not partitioned or no matching partition found
func (t *TableDef) getPartitionTreeName(partitionVal interface{}) string {
	if t.Partition == nil {
		return ""
	}

	// Convert partition value to int64
	var val int64
	switch v := partitionVal.(type) {
	case int64:
		val = v
	case int:
		val = int64(v)
	case float64:
		val = int64(v)
	case string:
		var err error
		val, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return ""
		}
	default:
		return ""
	}

	// Find matching partition based on partition type
	switch t.Partition.Type {
	case query.PartitionTypeRange:
		for _, p := range t.Partition.Partitions {
			if val >= p.MinValue && val < p.MaxValue {
				return t.Name + ":" + p.Name
			}
		}
	case query.PartitionTypeList:
		// For LIST partitions, we'd need value lists - simplified for now
		if len(t.Partition.Partitions) > 0 {
			// Hash the value to pick a partition
			idx := int(val) % len(t.Partition.Partitions)
			if idx < 0 {
				idx = -idx
			}
			return t.Name + ":" + t.Partition.Partitions[idx].Name
		}
	case query.PartitionTypeHash:
		if len(t.Partition.Partitions) > 0 {
			idx := int(val) % len(t.Partition.Partitions)
			if idx < 0 {
				idx = -idx
			}
			return t.Name + ":" + t.Partition.Partitions[idx].Name
		}
	}

	return ""
}

// getPartitionTreeNames returns all partition tree names for a partitioned table
// Used for SELECT to scan all partitions
func (t *TableDef) getPartitionTreeNames() []string {
	if t.Partition == nil {
		return []string{t.Name}
	}

	names := make([]string, len(t.Partition.Partitions))
	for i, p := range t.Partition.Partitions {
		names[i] = t.Name + ":" + p.Name
	}
	return names
}

// getTableTreesForScan returns all B-trees for scanning a table
// For partitioned tables, returns all partition trees; for non-partitioned, returns the single tree
func (c *Catalog) getTableTreesForScan(table *TableDef) ([]btree.TreeStore, error) {
	return c.getTableTreesForScanWithOptions(table, fdw.ScanOptions{})
}

func (c *Catalog) getTableTreesForScanWithOptions(table *TableDef, scanOptions fdw.ScanOptions) ([]btree.TreeStore, error) {
	// Foreign table: materialize FDW data into a temporary B-tree
	if table.Type == "foreign" {
		ft, ok := c.foreignTables[table.Name]
		if !ok {
			return nil, ErrTableNotFound
		}
		if c.fdwRegistry == nil {
			return nil, fmt.Errorf("fdw registry not initialized")
		}
		wrapper, ok := c.fdwRegistry.Get(ft.Wrapper)
		if !ok {
			return nil, fmt.Errorf("foreign data wrapper '%s' not found", ft.Wrapper)
		}
		if err := wrapper.Open(ft.Options); err != nil {
			return nil, fmt.Errorf("fdw open failed: %w", err)
		}
		defer wrapper.Close()
		cols := make([]string, len(ft.Columns))
		for i, col := range ft.Columns {
			cols[i] = col.Name
		}
		if len(scanOptions.Columns) == 0 {
			scanOptions.Columns = cols
		}
		maxMaterializedBytes, err := parseFDWMaterializedByteLimit(ft.Options)
		if err != nil {
			return nil, err
		}
		var materializedBytes int64

		tmpTree, err := btree.NewBTree(c.pool)
		if err != nil {
			return nil, err
		}

		putRow := func(rowIndex int, row []interface{}) error {
			row = expandFDWProjectedRow(row, cols, scanOptions.Columns)
			if maxMaterializedBytes > 0 {
				materializedBytes += estimateFDWRowBytes(row)
				if materializedBytes > maxMaterializedBytes {
					return fmt.Errorf("fdw materialized byte limit exceeded: max_materialized_bytes=%d", maxMaterializedBytes)
				}
			}
			key := []byte("fdw:" + strconv.Itoa(rowIndex))
			val, err := encodeVersionedRow(row, nil)
			if err != nil {
				return err
			}
			if err := tmpTree.Put(key, val); err != nil {
				return err
			}
			return nil
		}

		if streaming, ok := wrapper.(fdw.StreamingForeignDataWrapper); ok {
			cursor, err := streaming.OpenScan(ft.TableName, scanOptions)
			if err != nil {
				return nil, fmt.Errorf("fdw scan failed: %w", err)
			}
			rowIndex := 0
			for {
				row, err := cursor.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					_ = cursor.Close()
					return nil, fmt.Errorf("fdw scan failed: %w", err)
				}
				if err := putRow(rowIndex, row); err != nil {
					_ = cursor.Close()
					return nil, err
				}
				rowIndex++
			}
			if err := cursor.Close(); err != nil {
				return nil, fmt.Errorf("fdw cursor close failed: %w", err)
			}
			return []btree.TreeStore{tmpTree}, nil
		}

		rows, err := wrapper.Scan(ft.TableName, cols)
		if err != nil {
			return nil, fmt.Errorf("fdw scan failed: %w", err)
		}
		for i, row := range rows {
			if err := putRow(i, row); err != nil {
				return nil, err
			}
		}
		return []btree.TreeStore{tmpTree}, nil
	}

	if table.Partition == nil {
		tree, exists := c.tableTrees[table.Name]
		if !exists {
			return nil, ErrTableNotFound
		}
		return []btree.TreeStore{tree}, nil
	}

	// Partitioned table - collect all partition trees
	treeNames := table.getPartitionTreeNames()
	trees := make([]btree.TreeStore, 0, len(treeNames))
	for _, name := range treeNames {
		if tree, exists := c.tableTrees[name]; exists {
			trees = append(trees, tree)
		}
	}
	return trees, nil
}

func expandFDWProjectedRow(row []interface{}, allColumns []string, scanColumns []string) []interface{} {
	if len(scanColumns) == 0 || fdwScanColumnsMatchAllColumns(scanColumns, allColumns) {
		return row
	}
	expanded := make([]interface{}, len(allColumns))
	for i, col := range scanColumns {
		if i >= len(row) {
			break
		}
		for dst, allCol := range allColumns {
			if strings.EqualFold(allCol, col) {
				expanded[dst] = row[i]
				break
			}
		}
	}
	return expanded
}

func fdwScanColumnsMatchAllColumns(scanColumns []string, allColumns []string) bool {
	if len(scanColumns) != len(allColumns) {
		return false
	}
	for i := range allColumns {
		if !strings.EqualFold(scanColumns[i], allColumns[i]) {
			return false
		}
	}
	return true
}

func exprToSQL(expr query.Expression) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *query.NumberLiteral:
		return numberLiteralSQL(e)
	case *query.StringLiteral:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(e.Value, "'", "''"))
	case *query.BooleanLiteral:
		if e.Value {
			return "TRUE"
		}
		return "FALSE"
	case *query.NullLiteral:
		return "NULL"
	case *query.Identifier:
		return e.Name
	case *query.QualifiedIdentifier:
		return e.Table + "." + e.Column
	case *query.ColumnRef:
		if e.Table != "" {
			return e.Table + "." + e.Column
		}
		return e.Column
	case *query.StarExpr:
		if e.Table != "" {
			return e.Table + ".*"
		}
		return "*"
	case *query.AliasExpr:
		return exprToSQL(e.Expr) + " AS " + e.Alias
	case *query.BinaryExpr:
		left := exprToSQL(e.Left)
		right := exprToSQL(e.Right)
		op := ""
		switch e.Operator {
		case query.TokenEq:
			op = "="
		case query.TokenNeq:
			op = "!="
		case query.TokenLt:
			op = "<"
		case query.TokenGt:
			op = ">"
		case query.TokenLte:
			op = "<="
		case query.TokenGte:
			op = ">="
		case query.TokenAnd:
			op = "AND"
		case query.TokenOr:
			op = "OR"
		case query.TokenPlus:
			op = "+"
		case query.TokenMinus:
			op = "-"
		case query.TokenStar:
			op = "*"
		case query.TokenSlash:
			op = "/"
		case query.TokenPercent:
			op = "%"
		case query.TokenConcat:
			op = "||"
		case query.TokenLike:
			op = "LIKE"
		case query.TokenIn:
			op = "IN"
		case query.TokenIs:
			op = "IS"
		default:
			op = "?"
		}
		return fmt.Sprintf("(%s %s %s)", left, op, right)
	case *query.UnaryExpr:
		val := exprToSQL(e.Expr)
		if e.Operator == query.TokenNot {
			return fmt.Sprintf("NOT %s", val)
		}
		if e.Operator == query.TokenMinus {
			return fmt.Sprintf("-%s", val)
		}
		if e.Operator == query.TokenBitNot {
			return fmt.Sprintf("~%s", val)
		}
		return val
	case *query.FunctionCall:
		var args []string
		for _, arg := range e.Args {
			args = append(args, exprToSQL(arg))
		}
		return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))
	default:
		return fmt.Sprintf("%v", expr)
	}
}

func typeTaggedKey(v interface{}) string {
	if v == nil {
		return "\x01NULL\x01"
	}
	switch val := v.(type) {
	case int64:
		return "I:" + strconv.FormatInt(val, 10)
	case float64:
		if val == float64(int64(val)) && val >= -1e15 && val <= 1e15 {
			return "I:" + strconv.FormatInt(int64(val), 10)
		}
		return "F:" + strconv.FormatFloat(val, 'g', -1, 64)
	case bool:
		if val {
			return "B:1"
		}
		return "B:0"
	case []byte:
		// Convert []byte to string for consistent key generation
		return "S:" + string(val)
	case string:
		return "S:" + val
	default:
		return "S:" + ValueToStringKey(v)
	}
}

func buildCompositeIndexKey(table *TableDef, idxDef *IndexDef, row []interface{}) (string, bool) {
	if len(idxDef.Columns) == 0 {
		return "", false
	}
	if len(idxDef.Columns) == 1 {
		colIdx := table.GetColumnIndex(idxDef.Columns[0])
		if colIdx < 0 || colIdx >= len(row) || row[colIdx] == nil {
			return "", false
		}
		return typeTaggedKey(row[colIdx]), true
	}
	// Composite key: concatenate all column values
	var parts []string
	for _, col := range idxDef.Columns {
		colIdx := table.GetColumnIndex(col)
		if colIdx < 0 || colIdx >= len(row) || row[colIdx] == nil {
			return "", false
		}
		parts = append(parts, typeTaggedKey(row[colIdx]))
	}
	return strings.Join(parts, "\x00"), true
}

func encodeRow(exprs []query.Expression, args []interface{}) ([]byte, error) {
	values := make([]interface{}, 0, len(exprs))
	argIdx := 0

	for _, expr := range exprs {
		var val interface{}
		var err error

		switch e := expr.(type) {
		case *query.PlaceholderExpr:
			// Get value from args
			if e.Index < len(args) {
				val = args[e.Index]
			} else if argIdx < len(args) {
				// Also support positional placeholders
				val = args[argIdx]
				argIdx++
			} else {
				val = nil
			}
		case *query.StringLiteral:
			val = e.Value
		case *query.NumberLiteral:
			val = e.Value
		case *query.BooleanLiteral:
			val = e.Value
		case *query.NullLiteral:
			val = nil
		case *query.Identifier:
			val = e.Name
		default:
			val, err = EvalExpression(expr, args)
			if err != nil {
				return nil, err
			}
		}
		values = append(values, val)
	}

	// If we have remaining args that weren't used as placeholders, add them
	if argIdx < len(args) {
		for i := argIdx; i < len(args); i++ {
			values = append(values, args[i])
		}
	}

	return json.Marshal(values)
}

func decodeRow(data []byte, numCols int) ([]interface{}, error) {
	// Use decodeVersionedRow which handles both VersionedRow format and plain row format
	vrow, err := decodeVersionedRow(data, numCols)
	if err != nil {
		return nil, err
	}
	return vrow.Data, nil
}

// EvalExpression evaluates a query expression against the given argument list.
func EvalExpression(expr query.Expression, args []interface{}) (interface{}, error) {
	switch e := expr.(type) {
	case *query.StringLiteral:
		return e.Value, nil
	case *query.NumberLiteral:
		return e.Value, nil
	case *query.BooleanLiteral:
		return e.Value, nil
	case *query.NullLiteral:
		return nil, nil
	case *query.PlaceholderExpr:
		if e.Index < len(args) {
			return args[e.Index], nil
		}
		return nil, fmt.Errorf("placeholder index out of range")
	case *query.Identifier:
		return e.Name, nil // Return as string for now
	case *query.UnaryExpr:
		val, err := EvalExpression(e.Expr, args)
		if err != nil {
			return nil, err
		}
		if e.Operator == query.TokenMinus {
			if isIntegerType(val) {
				if f, ok := toFloat64(val); ok {
					return -int64(f), nil
				}
			}
			if f, ok := toFloat64(val); ok {
				return -f, nil
			}
		}
		if e.Operator == query.TokenNot {
			if val == nil {
				return nil, nil // NOT NULL = NULL per SQL three-valued logic
			}
			if b, ok := val.(bool); ok {
				return !b, nil
			}
		}
		return val, nil
	case *query.BinaryExpr:
		left, err := EvalExpression(e.Left, args)
		if err != nil {
			return nil, err
		}
		right, err := EvalExpression(e.Right, args)
		if err != nil {
			return nil, err
		}
		return evalBinaryExprValue(left, right, e.Operator)
	case *query.CaseExpr:
		if e.Expr != nil {
			// Simple CASE: CASE expr WHEN val THEN result
			baseVal, err := EvalExpression(e.Expr, args)
			if err != nil {
				return nil, err
			}
			// Per SQL standard, CASE NULL WHEN NULL is UNKNOWN (not true)
			if baseVal != nil {
				for _, when := range e.Whens {
					whenVal, err := EvalExpression(when.Condition, args)
					if err != nil {
						continue
					}
					if whenVal != nil && compareValues(baseVal, whenVal) == 0 {
						return EvalExpression(when.Result, args)
					}
				}
			}
		} else {
			// Searched CASE: CASE WHEN cond THEN result
			for _, when := range e.Whens {
				condVal, err := EvalExpression(when.Condition, args)
				if err != nil {
					continue
				}
				if toBool(condVal) {
					return EvalExpression(when.Result, args)
				}
			}
		}
		if e.Else != nil {
			return EvalExpression(e.Else, args)
		}
		return nil, nil
	case *query.CastExpr:
		val, err := EvalExpression(e.Expr, args)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		switch e.DataType {
		case query.TokenInteger:
			if f, ok := toFloat64(val); ok {
				return int64(f), nil
			}
			if s, ok := toString(val); ok {
				if i, err := strconv.ParseInt(s, 10, 64); err == nil {
					return i, nil
				}
			}
			return int64(0), nil
		case query.TokenReal:
			if f, ok := toFloat64(val); ok {
				return f, nil
			}
			if s, ok := toString(val); ok {
				if f, err := strconv.ParseFloat(s, 64); err == nil {
					return f, nil
				}
			}
			return float64(0), nil
		case query.TokenText:
			return ValueToStringKey(val), nil
		}
		return val, nil
	case *query.FunctionCall:
		evalArgs := make([]interface{}, len(e.Args))
		for i, arg := range e.Args {
			val, err := EvalExpression(arg, args)
			if err != nil {
				return nil, err
			}
			evalArgs[i] = val
		}
		return evalFunctionCallValue(toUpperFast(e.Name), evalArgs)
	case *query.AliasExpr:
		return EvalExpression(e.Expr, args)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func tokenize(text string) []string {
	// Simple tokenization - split on non-alphanumeric
	var words []string
	var current strings.Builder

	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			current.WriteRune(r)
		} else {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
		}
	}
	if current.Len() > 0 {
		words = append(words, current.String())
	}

	return words
}

func intersectSorted(a, b []int64) []int64 {
	var result []int64
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			result = append(result, a[i])
			i++
			j++
		}
	}

	return result
}

func catalogCompareValues(a, b interface{}) int {
	// Handle nil cases
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)
	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		}
		if aFloat > bFloat {
			return 1
		}
		return 0
	}

	// String comparison fallback
	aStr := ValueToStringKey(a)
	bStr := ValueToStringKey(b)
	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}

// evaluateTemporalExpr evaluates an AS OF temporal expression
// Returns the timestamp to query data as of that point in time
func (cat *Catalog) evaluateTemporalExpr(expr *query.TemporalExpr, args []interface{}) (*time.Time, error) {
	if expr == nil {
		return nil, nil
	}

	// Evaluate the timestamp expression
	val, err := EvalExpression(expr.Timestamp, args)
	if err != nil {
		return nil, err
	}

	var result time.Time

	if expr.IsSystem {
		// AS OF SYSTEM TIME 'expression'
		// Parse interval expression like '-1 hour', '-30 minutes', etc.
		switch v := val.(type) {
		case string:
			result = parseSystemTimeExpr(v)
		case time.Time:
			result = v
		default:
			result = time.Now()
		}
	} else {
		// AS OF 'timestamp'
		switch v := val.(type) {
		case string:
			// Try to parse as ISO 8601 or other common formats
			parsed, err := time.Parse(time.RFC3339, v)
			if err != nil {
				parsed, err = time.Parse("2006-01-02 15:04:05", v)
				if err != nil {
					parsed, err = time.Parse("2006-01-02", v)
					if err != nil {
						return nil, fmt.Errorf("cannot parse timestamp: %v", v)
					}
				}
			}
			result = parsed
		case time.Time:
			result = v
		default:
			return nil, fmt.Errorf("invalid timestamp type: %T", val)
		}
	}

	return &result, nil
}

// parseSystemTimeExpr parses expressions like "-1 hour", "-30 minutes", etc.
func parseSystemTimeExpr(expr string) time.Time {
	now := time.Now()

	// Simple parsing for common patterns
	expr = strings.TrimSpace(toLowerFast(expr))

	// Handle negative offset (past)
	if strings.HasPrefix(expr, "-") {
		expr = strings.TrimSpace(expr[1:])

		// Try to extract number and unit
		var num int
		var unit string
		if _, err := fmt.Sscanf(expr, "%d %s", &num, &unit); err != nil {
			return now
		}

		switch {
		case strings.HasPrefix(unit, "hour") || strings.HasPrefix(unit, "hr"):
			return now.Add(-time.Duration(num) * time.Hour)
		case strings.HasPrefix(unit, "minute") || strings.HasPrefix(unit, "min"):
			return now.Add(-time.Duration(num) * time.Minute)
		case strings.HasPrefix(unit, "second") || strings.HasPrefix(unit, "sec"):
			return now.Add(-time.Duration(num) * time.Second)
		case strings.HasPrefix(unit, "day"):
			return now.AddDate(0, 0, -num)
		}
	}

	// Handle positive offset (future) - less common but supported
	if strings.HasPrefix(expr, "+") {
		expr = strings.TrimSpace(expr[1:])
		var num int
		var unit string
		if _, err := fmt.Sscanf(expr, "%d %s", &num, &unit); err != nil {
			return now
		}

		switch {
		case strings.HasPrefix(unit, "hour") || strings.HasPrefix(unit, "hr"):
			return now.Add(time.Duration(num) * time.Hour)
		case strings.HasPrefix(unit, "minute") || strings.HasPrefix(unit, "min"):
			return now.Add(time.Duration(num) * time.Minute)
		}
	}

	// Default: try to parse as absolute timestamp
	if t, err := time.Parse(time.RFC3339, expr); err == nil {
		return t
	}
	if t, err := time.Parse("2006-01-02 15:04:05", expr); err == nil {
		return t
	}

	// Default to current time
	return now
}

// applyOffsetLimit applies OFFSET and LIMIT to a result set.
//
//nolint:unused // retained for coverage and future shared offset/limit path reuse.
func applyOffsetLimit(rows [][]interface{}, offsetExpr, limitExpr query.Expression, args []interface{}) [][]interface{} {
	result := rows

	if offsetExpr != nil {
		offsetVal, err := EvalExpression(offsetExpr, args)
		if err == nil {
			if off, ok := toInt(offsetVal); ok && off > 0 {
				if int(off) < len(result) {
					result = result[off:]
				} else {
					result = nil
				}
			}
		}
	}

	if limitExpr != nil && len(result) > 0 {
		limitVal, err := EvalExpression(limitExpr, args)
		if err == nil {
			if lim, ok := toInt(limitVal); ok && lim >= 0 && int(lim) < len(result) {
				result = result[:lim]
			}
		}
	}

	return result
}

// hasSubqueriesInExpr returns true if the expression contains any subquery.
func hasSubqueriesInExpr(expr query.Expression) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *query.SubqueryExpr, *query.ExistsExpr:
		return true
	case *query.InExpr:
		if e.Subquery != nil {
			return true
		}
		for _, item := range e.List {
			if hasSubqueriesInExpr(item) {
				return true
			}
		}
		return hasSubqueriesInExpr(e.Expr)
	case *query.BinaryExpr:
		return hasSubqueriesInExpr(e.Left) || hasSubqueriesInExpr(e.Right)
	case *query.UnaryExpr:
		return hasSubqueriesInExpr(e.Expr)
	case *query.FunctionCall:
		for _, arg := range e.Args {
			if hasSubqueriesInExpr(arg) {
				return true
			}
		}
		if hasSubqueriesInExpr(e.Filter) {
			return true
		}
		for _, ob := range e.OrderBy {
			if ob != nil && hasSubqueriesInExpr(ob.Expr) {
				return true
			}
		}
		return false
	case *query.WindowExpr:
		for _, arg := range e.Args {
			if hasSubqueriesInExpr(arg) {
				return true
			}
		}
		if hasSubqueriesInExpr(e.Filter) {
			return true
		}
		for _, partitionExpr := range e.PartitionBy {
			if hasSubqueriesInExpr(partitionExpr) {
				return true
			}
		}
		for _, ob := range e.OrderBy {
			if ob != nil && hasSubqueriesInExpr(ob.Expr) {
				return true
			}
		}
		return false
	case *query.CaseExpr:
		if hasSubqueriesInExpr(e.Expr) {
			return true
		}
		for _, w := range e.Whens {
			if hasSubqueriesInExpr(w.Condition) || hasSubqueriesInExpr(w.Result) {
				return true
			}
		}
		return hasSubqueriesInExpr(e.Else)
	case *query.CastExpr:
		return hasSubqueriesInExpr(e.Expr)
	case *query.BetweenExpr:
		return hasSubqueriesInExpr(e.Expr) || hasSubqueriesInExpr(e.Lower) || hasSubqueriesInExpr(e.Upper)
	case *query.LikeExpr:
		return hasSubqueriesInExpr(e.Expr) || hasSubqueriesInExpr(e.Pattern)
	case *query.IsNullExpr:
		return hasSubqueriesInExpr(e.Expr)
	case *query.AliasExpr:
		return hasSubqueriesInExpr(e.Expr)
	default:
		return false
	}
}

// hasSubqueries returns true if the SELECT statement contains any subquery expressions.
func hasSubqueries(stmt *query.SelectStmt) bool {
	if stmt.Where != nil && hasSubqueriesInExpr(stmt.Where) {
		return true
	}
	for _, col := range stmt.Columns {
		if hasSubqueriesInExpr(col) {
			return true
		}
	}
	for _, gb := range stmt.GroupBy {
		if hasSubqueriesInExpr(gb) {
			return true
		}
	}
	if stmt.Having != nil && hasSubqueriesInExpr(stmt.Having) {
		return true
	}
	for _, ob := range stmt.OrderBy {
		if hasSubqueriesInExpr(ob.Expr) {
			return true
		}
	}
	return false
}

// processRowChunk decodes, filters, and projects a chunk of raw row values.
// It returns the projected rows and optional full rows for window functions.
func (cat *Catalog) processRowChunk(
	values [][]byte,
	table *TableDef,
	selectCols []selectColInfo,
	stmt *query.SelectStmt,
	args []interface{},
	queryTime time.Time,
	hasWindowFuncs bool,
) ([][]interface{}, [][]interface{}, error) {
	var rows [][]interface{}
	var windowFullRows [][]interface{}
	for _, valueData := range values {
		vrow, err := decodeVersionedRow(valueData, len(table.Columns))
		if err != nil {
			return nil, nil, fmt.Errorf("select chunk: failed to decode row in table %s: %w", table.Name, err)
		}
		if !vrow.Version.isVisibleAt(queryTime) {
			continue
		}
		fullRow := vrow.Data
		if stmt.Where != nil {
			matched, err := evaluateWhere(cat, fullRow, table.Columns, stmt.Where, args)
			if err != nil || !matched {
				// Match the serial scan path (filterAndProjectRow): a WHERE-eval
				// error skips the row rather than failing the whole query, so
				// parallel and serial scans return the same result set
				// regardless of candidate-row count (which selects the path).
				continue
			}
		}
		var selectedRow []interface{}
		if isIdentityProjection(selectCols, len(fullRow)) {
			selectedRow = fullRow
		} else {
			selectedRow = make([]interface{}, len(selectCols))
			for i, ci := range selectCols {
				if ci.isWindow {
					continue
				}
				if ci.index >= 0 && ci.index < len(fullRow) {
					selectedRow[i] = fullRow[ci.index]
				} else if ci.index == -1 && !ci.isAggregate {
					if i < len(stmt.Columns) {
						val, err := evaluateExpression(cat, fullRow, table.Columns, stmt.Columns[i], args)
						if err == nil {
							selectedRow[i] = val
						}
					} else if len(ci.name) > 10 && ci.name[:10] == "__orderby_" {
						var obIdx int
						if _, err := fmt.Sscanf(ci.name, "__orderby_%d", &obIdx); err == nil && obIdx < len(stmt.OrderBy) {
							val, err := evaluateExpression(cat, fullRow, table.Columns, stmt.OrderBy[obIdx].Expr, args)
							if err == nil {
								selectedRow[i] = val
							}
						}
					}
				}
			}
		}
		rows = append(rows, selectedRow)
		if hasWindowFuncs {
			fullRowCopy := make([]interface{}, len(fullRow))
			copy(fullRowCopy, fullRow)
			windowFullRows = append(windowFullRows, fullRowCopy)
		}
	}
	return rows, windowFullRows, nil
}
