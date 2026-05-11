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
	"github.com/cobaltdb/cobaltdb/pkg/fdw"
	"github.com/cobaltdb/cobaltdb/pkg/parallel"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
	"strconv"
	"strings"
	"sync"
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
	AutoIncSeq  int64           `json:"auto_inc_seq"`        // Per-table auto-increment counter
	Partition   *PartitionInfo  `json:"partition,omitempty"` // Table partitioning info
	// Performance: cache column indices (not persisted)
	columnIndices map[string]int `json:"-"`
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
	Columns           []string `json:"columns"`
	ReferencedTable   string   `json:"referenced_table"`
	ReferencedColumns []string `json:"referenced_columns"`
	OnDelete          string   `json:"on_delete"` // NO ACTION, CASCADE, SET NULL, RESTRICT
	OnUpdate          string   `json:"on_update"` // NO ACTION, CASCADE, SET NULL, RESTRICT
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
	Check         query.Expression `json:"-"`                    // Parsed CHECK expression (not persisted)
	defaultExpr   query.Expression `json:"-"`                    // Parsed DEFAULT expression (not persisted)
	sourceTbl     string           `json:"-"`                    // Source table name for JOIN column disambiguation
	Dimensions    int              `json:"dimensions,omitempty"` // For VECTOR type: number of dimensions
}

// IndexDef represents an index definition
type IndexDef struct {
	Name       string   `json:"name"`
	TableName  string   `json:"table_name"`
	Columns    []string `json:"columns"`
	Unique     bool     `json:"unique"`
	RootPageID uint32   `json:"root_page_id"`
}

// selectColInfo holds information about selected columns in a query
type selectColInfo struct {
	name           string
	tableName      string // table name for JOINs
	index          int
	isAggregate    bool
	aggregateType  string            // COUNT, SUM, AVG, MIN, MAX
	aggregateCol   string            // column name for SUM, AVG, MIN, MAX
	aggregateExpr  query.Expression  // full expression for SUM(expr), AVG(expr), etc.
	isDistinct     bool              // for COUNT(DISTINCT col)
	isWindow       bool              // true for window functions
	windowExpr     *query.WindowExpr // window function expression
	hasEmbeddedAgg bool              // true when expression (CASE, etc.) contains aggregate calls
	originalExpr   query.Expression  // the original expression for hasEmbeddedAgg columns
}

// MaterializedViewDef represents a materialized view definition
type MaterializedViewDef struct {
	Name        string                   `json:"name"`
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
	undoInsert            undoAction = iota // Undo an INSERT by deleting the key
	undoUpdate                              // Undo an UPDATE by restoring the old value
	undoDelete                              // Undo a DELETE by restoring the key/value
	undoCreateTable                         // Undo a CREATE TABLE by dropping the table
	undoDropTable                           // Undo a DROP TABLE by restoring the table
	undoCreateIndex                         // Undo a CREATE INDEX by dropping the index
	undoDropIndex                           // Undo a DROP INDEX by restoring the index
	undoAlterAddColumn                      // Undo ALTER TABLE ADD COLUMN
	undoAlterDropColumn                     // Undo ALTER TABLE DROP COLUMN
	undoAlterRename                         // Undo ALTER TABLE RENAME
	undoAlterRenameColumn                   // Undo ALTER TABLE RENAME COLUMN
	undoAutoIncSeq                          // Undo AutoIncSeq change
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
	indexChanges []indexUndoEntry
	// DDL undo fields
	tableDef      *TableDef                  // For undoDropTable: original table definition
	tableTree     btree.TreeStore            // For undoDropTable: original table B-tree
	tableIndexes  map[string]*IndexDef       // For undoDropTable: indexes
	tableIdxTrees map[string]btree.TreeStore // For undoDropTable: index B-trees
	indexDef      *IndexDef                  // For undoDropIndex: original index definition
	indexTree     btree.TreeStore            // For undoDropIndex: original index B-tree
	indexName     string                     // For undoCreateIndex: index name to drop
	// ALTER TABLE undo fields
	oldColumns           []ColumnDef                 // For undoAlterAddColumn/undoAlterDropColumn: original columns
	oldPrimaryKeyColumns []string                    // For undoAlterRenameColumn: original PK name
	oldName              string                      // For undoAlterRename/undoAlterRenameColumn: original name
	newName              string                      // For undoAlterRename/undoAlterRenameColumn: new name
	oldRowData           []struct{ key, val []byte } // For undoAlterDropColumn: original row data
	droppedIndexes       map[string]*IndexDef        // For undoAlterDropColumn: dropped indexes
	droppedIdxTrees      map[string]btree.TreeStore  // For undoAlterDropColumn: dropped index trees
	oldAutoIncSeq        int64                       // For undoAutoIncSeq: previous AutoIncSeq value
}

// catalogTxnState holds per-transaction state for multi-transaction support.
type catalogTxnState struct {
	txnID             uint64
	txnActive         bool
	undoLog           []undoEntry
	savepoints        []savepointEntry
	managerTxn        interface{}    // *txn.Transaction when txnManager bridge is active
	pendingWrites     []PendingWrite // buffered DML for commit-time application
	pendingWriteMap   map[string]map[string]PendingWrite // table -> key -> latest write (O(1) lookup)
	readValues        map[txn.WriteKey][]byte // key -> value at time of read (for MVCC validation)
	rowBuf            [8]interface{} // reused per-transaction scratch buffer for INSERT
	valueDataBuf      []byte         // reused per-transaction buffer for encoded row values
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
	foreignTables        map[string]*ForeignTableDef           // Foreign table definitions
	indexes              map[string]*IndexDef
	indexTrees           map[string]btree.TreeStore // B+Trees for indexes
	pool                 *storage.BufferPool
	wal                  *storage.WAL
	tableTrees           map[string]btree.TreeStore               // Each table has its own B+Tree
	partitionTreeMu      sync.Mutex                               // protects lazy partition tree creation
	fdwRegistry          *fdw.Registry                         // FDW registry for foreign data wrappers
	views                map[string]*query.SelectStmt          // Views store their SELECT query
	triggers             map[string]*query.CreateTriggerStmt   // Triggers store their definition
	procedures           map[string]*query.CreateProcedureStmt // Procedures store their definition
	materializedViews    map[string]*MaterializedViewDef       // Materialized views
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
	queryCache           *QueryCache                           // Query result cache
	rlsCtx               context.Context                       // Context for RLS user/role extraction in SELECT
	lastReturningRows    [][]interface{} // Last RETURNING clause results
	lastReturningColumns []string        // Column names for RETURNING results
	returningMu          sync.Mutex      // protects lastReturningRows/lastReturningColumns

	// Dead tuple tracking for AutoVacuum
	deadTuples map[string]int64 // table name -> count of soft-deleted rows
	liveTuples map[string]int64 // table name -> count of live rows
	vacuumMu   sync.RWMutex     // protects deadTuples and liveTuples

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
	commitMu [64]sync.Mutex

	// txnStatePool recycles per-transaction state structs to reduce GC pressure
	// from high-frequency Begin/Commit cycles.
	txnStatePool sync.Pool
}

func (c *Catalog) commitLockIdx(treeName string, key string) int {
	h := uint32(0)
	for i := 0; i < len(treeName); i++ {
		h = h*31 + uint32(treeName[i])
	}
	for i := 0; i < len(key); i++ {
		h = h*31 + uint32(key[i])
	}
	return int(h) % len(c.commitMu)
}

// savepointEntry records a named savepoint with its undo log position
type savepointEntry struct {
	name           string
	undoPos        int // Position in undoLog at time of savepoint creation
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
		tree:              tree,
		tables:            make(map[string]*TableDef),
		foreignTables:     make(map[string]*ForeignTableDef),
		indexes:           make(map[string]*IndexDef),
		indexTrees:        make(map[string]btree.TreeStore),
		pool:              pool,
		wal:               wal,
		tableTrees:        make(map[string]btree.TreeStore),
		fdwRegistry:       fdw.NewRegistry(),
		views:             make(map[string]*query.SelectStmt),
		triggers:          make(map[string]*query.CreateTriggerStmt),
		procedures:        make(map[string]*query.CreateProcedureStmt),
		materializedViews: make(map[string]*MaterializedViewDef),
		ftsIndexes:        make(map[string]*FTSIndexDef),
		jsonIndexes:       make(map[string]*JSONIndexDef),
		vectorIndexes:     make(map[string]*VectorIndexDef),
		stats:             make(map[string]*StatsTableStats),
		rlsPolicies:       make(map[string]*security.Policy),
		keyCounter:        0,
		queryCache:        NewQueryCache(0, 0), // Disabled by default - enable with EnableQueryCache()
		deadTuples:        make(map[string]int64),
		liveTuples:        make(map[string]int64),
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

	viewIsComplex := view.Distinct || len(view.GroupBy) > 0 || view.Having != nil || view.From == nil
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
		if len(mv.Data) > 0 {
			for colName := range mv.Data[0] {
				table.Columns = append(table.Columns, ColumnDef{Name: colName, Type: "TEXT"})
			}
		}
		// Register as temporary CTE-like result for this query
		if cat.cteResults == nil {
			cat.cteResults = make(map[string]*cteResultSet)
		}
		cols := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			cols[i] = col.Name
		}
		rows := make([][]interface{}, len(mv.Data))
		for i, rowMap := range mv.Data {
			row := make([]interface{}, len(table.Columns))
			for j, col := range table.Columns {
				row[j] = rowMap[col.Name]
			}
			rows[i] = row
		}
		cat.cteResults[toLowerFast(name)] = &cteResultSet{columns: cols, rows: rows}
		return table, nil
	}

	return nil, fmt.Errorf("table '%s' not found", name)
}

func (cat *Catalog) selectLocked(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	// Apply query optimization
	optimizer := query.NewQueryOptimizer()
	optimizedStmt, err := optimizer.OptimizeSelect(stmt)
	if err == nil && optimizedStmt != nil {
		stmt = optimizedStmt
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

	// Fast path: SELECT COUNT(*) FROM table [WHERE ...] — skip row decoding
	if cols, rows, ok := cat.tryCountStarFastPath(stmt, args, queryTime); ok {
		return cols, rows, nil
	}

	// Fast path: SELECT SUM/AVG/MIN/MAX/COUNT(col) FROM table — streaming aggregates
	if cols, rows, ok := cat.trySimpleAggregateFastPath(stmt, args); ok {
		return cols, rows, nil
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
		if ci.isWindow {
			hasWindowFuncs = true
			break
		}
	}
	// Scan rows from table (index lookup, materialized view, or full table scan)
	rows, windowFullRows := cat.scanTableRows(table, stmt, args, selectCols, hasWindowFuncs, queryTime)


	returnColumns, rows = cat.applySelectPostProcess(applySelectPostProcessParams{
		rows:              rows,
		selectCols:        selectCols,
		stmt:              stmt,
		args:              args,
		returnColumns:     returnColumns,
		hiddenOrderByCols: hiddenOrderByCols,
		hasWindowFuncs:    hasWindowFuncs,
		windowFullRows:    windowFullRows,
		table:             table,
	})
	if rows == nil && returnColumns != nil {
		return returnColumns, nil, nil
	}

	return returnColumns, rows, nil
}


// scanTableRows reads rows from the table using index lookup, materialized view data, or full scan.
func (cat *Catalog) scanTableRows(table *TableDef, stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, hasWindowFuncs bool, queryTime time.Time) ([][]interface{}, [][]interface{}) {
	var rows [][]interface{}
	var windowFullRows [][]interface{}

	// Check if this is a materialized view with cached data
	var mvRows [][]interface{}
	var isMV bool
	if cteRes, ok := cat.cteResults[toLowerFast(stmt.From.Name)]; ok {
		mvRows = cteRes.rows
		isMV = true
		defer delete(cat.cteResults, toLowerFast(stmt.From.Name))
	}

	// Get all trees for scanning (handles partitioned tables)
	trees, err := cat.getTableTreesForScan(table)
	if err != nil && !isMV {
		return nil, nil
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

	// Try to use index for WHERE clause
	var useIndex bool
	var indexMatches map[string]bool
	if stmt.Where != nil {
		indexMatches, useIndex = cat.useIndexForQueryWithArgs(stmt.From.Name, stmt.Where, args)
	}

	if useIndex {
		for pk := range indexMatches {
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
				if m, ok := ts.pendingWriteMap[table.Name]; ok {
					if pw, ok2 := m[pk]; ok2 {
						valueData = pw.Value
						found = true
					}
				}
			}
			if !found {
				continue
			}
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				continue
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
			if _, ok := ts.pendingWriteMap[table.Name]; !ok {
				hasPending = false
			}
		}
		canParallel := cat.parallelWorkers > 0 &&
			len(stmt.OrderBy) == 0 &&
			!stmt.Distinct &&
			!hasWindowFuncs &&
			!hasSubqueries(stmt) &&
			stmt.Limit == nil &&
			stmt.Offset == nil

		if len(trees) == 1 && !hasPending && !canParallel {
			iter, err := trees[0].Scan(nil, nil)
			if err == nil {
				for iter.HasNext() {
					_, valueData, err := iter.NextString()
					if err != nil {
						break
					}
					vrow, err := decodeVersionedRow(valueData, len(table.Columns))
					if err != nil {
						continue
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
				}
				iter.Close()
			}
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
					continue
				}
				for iter.HasNext() {
					k, valueData, err := iter.NextString()
					if err != nil {
						break
					}
					pairs = append(pairs, kvPair{k, valueData})
					seen[k] = len(pairs) - 1
				}
				iter.Close()
			}

			// Read-your-writes: overlay buffered writes (INSERT, UPDATE, DELETE).
			if hasPending {
				if m, ok := ts.pendingWriteMap[table.Name]; ok {
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

			canParallel = canParallel && len(pairs) >= cat.parallelThreshold

			if canParallel {
				values := make([][]byte, len(pairs))
				for i, p := range pairs {
					values[i] = p.value
				}
				results := parallel.ParallelSelectRows(values, cat.parallelWorkers, cat.parallelThreshold,
					func(chunk [][]byte) [][]interface{} {
						chunkRows, _ := cat.processRowChunk(chunk, table, selectCols, stmt, args, queryTime, false)
						return chunkRows
					})
				rows = append(rows, results...)
			} else {
				for _, p := range pairs {
					vrow, err := decodeVersionedRow(p.value, len(table.Columns))
					if err != nil {
						continue
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
				}
			}
		}
	}

	return rows, windowFullRows
}

// getEffectiveTableData returns all live rows for a table as a map of key to
// versioned row data, merging committed B-tree data with pending buffered
// writes for read-your-writes visibility.
func (c *Catalog) getEffectiveTableData(table *TableDef) map[string][]byte {
	result := make(map[string][]byte)
	trees, _ := c.getTableTreesForScan(table)
	for _, tree := range trees {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			continue
		}
		for iter.HasNext() {
			k, valueData, err := iter.NextString()
			if err != nil {
				break
			}
			if !bytesContainDeletedAt(valueData) {
				result[k] = valueData
				continue
			}
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				continue
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
					continue
				}
				if vrow.Version.DeletedAt > 0 {
					delete(result, k)
				} else {
					result[k] = pw.Value
				}
			}
		}
	}
	return result
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
					newOrderBy[i] = &query.OrderByExpr{Expr: expr, Desc: ob.Desc}
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
		if changed {
			return &query.FunctionCall{Name: e.Name, Args: newArgs, Distinct: e.Distinct}
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
		cols := make([]string, len(ft.Columns))
		for i, col := range ft.Columns {
			cols[i] = col.Name
		}
		rows, err := wrapper.Scan(ft.TableName, cols)
		if err != nil {
			wrapper.Close()
			return nil, fmt.Errorf("fdw scan failed: %w", err)
		}
		wrapper.Close()

		tmpTree, err := btree.NewBTree(c.pool)
		if err != nil {
			return nil, err
		}
		for i, row := range rows {
			key := []byte("fdw:" + strconv.Itoa(i))
			val, err := encodeVersionedRow(row, nil)
			if err != nil {
				return nil, err
			}
			if err := tmpTree.Put(key, val); err != nil {
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

func exprToSQL(expr query.Expression) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *query.NumberLiteral:
		return fmt.Sprintf("%v", e.Value)
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
			if s, ok := val.(string); ok {
				if i, err := strconv.ParseInt(s, 10, 64); err == nil {
					return i, nil
				}
			}
			return int64(0), nil
		case query.TokenReal:
			if f, ok := toFloat64(val); ok {
				return f, nil
			}
			if s, ok := val.(string); ok {
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
) ([][]interface{}, [][]interface{}) {
	var rows [][]interface{}
	var windowFullRows [][]interface{}
	for _, valueData := range values {
		vrow, err := decodeVersionedRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}
		if !vrow.Version.isVisibleAt(queryTime) {
			continue
		}
		fullRow := vrow.Data
		if stmt.Where != nil {
			matched, err := evaluateWhere(cat, fullRow, table.Columns, stmt.Where, args)
			if err != nil {
				continue
			}
			if !matched {
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
	return rows, windowFullRows
}
