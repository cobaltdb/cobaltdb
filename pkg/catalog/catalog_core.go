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
//
package catalog

import (
	"context"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
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
	Name      string `json:"name"`
	MinValue  int64  `json:"min_value"`
	MaxValue  int64  `json:"max_value"`
}

type TableDef struct {
	Name        string              `json:"name"`
	Type        string              `json:"type"` // "table" or "collection"
	Columns     []ColumnDef         `json:"columns"`
	PrimaryKey  []string            `json:"primary_key"` // Supports composite PK
	CreatedAt   int64               `json:"created_at"`
	RootPageID  uint32              `json:"root_page_id"`
	ForeignKeys []ForeignKeyDef     `json:"foreign_keys,omitempty"`
	AutoIncSeq  int64               `json:"auto_inc_seq"` // Per-table auto-increment counter
	Partition   *PartitionInfo      `json:"partition,omitempty"` // Table partitioning info
	// Performance: cache column indices (not persisted)
	columnIndices map[string]int `json:"-"`
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
	CheckStr      string           `json:"check_str,omitempty"` // CHECK expression as SQL text (persisted)
	Check         query.Expression `json:"-"`                   // Parsed CHECK expression (not persisted)
	defaultExpr   query.Expression `json:"-"`                   // Parsed DEFAULT expression (not persisted)
	sourceTbl     string           `json:"-"`                   // Source table name for JOIN column disambiguation
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
	Name      string              `json:"name"`
	TableName string              `json:"table_name"`
	Column    string              `json:"column"`              // JSON column name
	Path      string              `json:"path"`                // JSON path expression (e.g., "$.name")
	DataType  string              `json:"data_type"`           // indexed data type: "string", "number", "boolean"
	Index     map[string][]int64  `json:"index"`               // value -> list of row IDs (for string values)
	NumIndex  map[string][]int64  `json:"num_index,omitempty"` // for numeric values (string key to avoid precision issues)
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
	tableDef      *TableDef               // For undoDropTable: original table definition
	tableTree     *btree.BTree            // For undoDropTable: original table B-tree
	tableIndexes  map[string]*IndexDef    // For undoDropTable: indexes
	tableIdxTrees map[string]*btree.BTree // For undoDropTable: index B-trees
	indexDef      *IndexDef               // For undoDropIndex: original index definition
	indexTree     *btree.BTree            // For undoDropIndex: original index B-tree
	indexName     string                  // For undoCreateIndex: index name to drop
	// ALTER TABLE undo fields
	oldColumns           []ColumnDef                 // For undoAlterAddColumn/undoAlterDropColumn: original columns
	oldPrimaryKeyColumns []string                    // For undoAlterRenameColumn: original PK name
	oldName              string                      // For undoAlterRename/undoAlterRenameColumn: original name
	newName              string                      // For undoAlterRename/undoAlterRenameColumn: new name
	oldRowData           []struct{ key, val []byte } // For undoAlterDropColumn: original row data
	droppedIndexes       map[string]*IndexDef        // For undoAlterDropColumn: dropped indexes
	droppedIdxTrees      map[string]*btree.BTree     // For undoAlterDropColumn: dropped index trees
	oldAutoIncSeq        int64                       // For undoAutoIncSeq: previous AutoIncSeq value
}

// Catalog manages database schema metadata
type Catalog struct {
	mu                sync.RWMutex
	tree              *btree.BTree
	tables            map[string]*TableDef
	indexes           map[string]*IndexDef
	indexTrees        map[string]*btree.BTree // B+Trees for indexes
	pool              *storage.BufferPool
	wal               *storage.WAL
	tableTrees        map[string]*btree.BTree               // Each table has its own B+Tree
	views             map[string]*query.SelectStmt          // Views store their SELECT query
	triggers          map[string]*query.CreateTriggerStmt   // Triggers store their definition
	procedures        map[string]*query.CreateProcedureStmt // Procedures store their definition
	materializedViews map[string]*MaterializedViewDef       // Materialized views
	ftsIndexes        map[string]*FTSIndexDef               // Full-text search indexes
	jsonIndexes       map[string]*JSONIndexDef              // JSON indexes for fast JSON queries
	vectorIndexes     map[string]*VectorIndexDef            // Vector (HNSW) indexes for similarity search
	stats             map[string]*StatsTableStats           // Table statistics for ANALYZE
	cteResults        map[string]*cteResultSet              // Temporary CTE result cache for recursive CTEs
	keyCounter        int64                                 // For generating unique keys
	txnID             uint64                                // Current transaction ID
	txnActive         bool                                  // Is a transaction active
	undoLog           []undoEntry                           // Undo log for transaction rollback
	savepoints        []savepointEntry                      // Stack of savepoints
	rlsManager           *security.Manager                     // Row-level security manager
	enableRLS            bool                                  // Enable row-level security
	rlsPolicies          map[string]*security.Policy           // RLS policies: key = "table:policyName"
	queryCache           *QueryCache                           // Query result cache
	rlsCtx               context.Context                       // Context for RLS user/role extraction in SELECT
	lastReturningRows    [][]interface{}                       // Last RETURNING clause results
	lastReturningColumns []string                              // Column names for RETURNING results
}

// savepointEntry records a named savepoint with its undo log position
type savepointEntry struct {
	name    string
	undoPos int // Position in undoLog at time of savepoint creation
}

// cteResultSet holds pre-computed results for recursive CTEs
type cteResultSet struct {
	columns []string
	rows    [][]interface{}
}

// QueryCacheEntry holds cached query results
type QueryCacheEntry struct {
	Columns   []string
	Rows      [][]interface{}
	Timestamp time.Time
	Tables    []string // Tables involved in the query (for invalidation)
}

// QueryCache manages cached query results
type QueryCache struct {
	entries   map[string]*QueryCacheEntry
	lru       *list.List
	lruMap    map[string]*list.Element
	maxSize   int
	ttl       time.Duration
	enabled   bool
	hitCount  atomic.Int64
	missCount atomic.Int64
	mu        sync.RWMutex
}

func NewQueryCache(maxSize int, ttl time.Duration) *QueryCache {
	return &QueryCache{
		entries: make(map[string]*QueryCacheEntry),
		lru:     list.New(),
		lruMap:  make(map[string]*list.Element),
		maxSize: maxSize,
		ttl:     ttl,
		enabled: maxSize > 0,
	}
}

func (qc *QueryCache) Get(key string) (*QueryCacheEntry, bool) {
	if !qc.enabled {
		return nil, false
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	entry, exists := qc.entries[key]
	if !exists {
		qc.missCount.Add(1)
		return nil, false
	}

	// Check if entry has expired
	if time.Since(entry.Timestamp) > qc.ttl {
		qc.missCount.Add(1)
		return nil, false
	}

	qc.promoteInLRU(key)
	qc.hitCount.Add(1)
	return entry, true
}

// promoteInLRU moves a key to the front of the LRU list (caller must hold at least RLock)
// Note: we upgrade to write lock for the LRU mutation
func (qc *QueryCache) promoteInLRU(key string) {
	if elem, ok := qc.lruMap[key]; ok {
		qc.lru.MoveToFront(elem)
	}
}

func (qc *QueryCache) Set(key string, columns []string, rows [][]interface{}, tables []string) {
	if !qc.enabled {
		return
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	// If key already exists, move to front
	if _, exists := qc.entries[key]; exists {
		if elem, ok := qc.lruMap[key]; ok {
			qc.lru.MoveToFront(elem)
		}
	} else {
		// Evict entries if cache is full
		for len(qc.entries) >= qc.maxSize {
			qc.evictOne()
		}
		elem := qc.lru.PushFront(key)
		qc.lruMap[key] = elem
	}

	qc.entries[key] = &QueryCacheEntry{
		Columns:   columns,
		Rows:      rows,
		Timestamp: time.Now(),
		Tables:    tables,
	}
}

func (qc *QueryCache) Invalidate(tableName string) {
	if !qc.enabled {
		return
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	tableLower := strings.ToLower(tableName)
	for key, entry := range qc.entries {
		for _, tbl := range entry.Tables {
			if strings.ToLower(tbl) == tableLower {
				if elem, ok := qc.lruMap[key]; ok {
					qc.lru.Remove(elem)
					delete(qc.lruMap, key)
				}
				delete(qc.entries, key)
				break
			}
		}
	}
}

func (qc *QueryCache) InvalidateAll() {
	if !qc.enabled {
		return
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	qc.entries = make(map[string]*QueryCacheEntry)
	qc.lru.Init()
	qc.lruMap = make(map[string]*list.Element)
}

func (qc *QueryCache) evictOne() {
	back := qc.lru.Back()
	if back != nil {
		key := back.Value.(string)
		qc.lru.Remove(back)
		delete(qc.lruMap, key)
		delete(qc.entries, key)
	}
}

func (qc *QueryCache) Stats() (hits, misses int64, size int) {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return qc.hitCount.Load(), qc.missCount.Load(), len(qc.entries)
}

func generateQueryKey(sql string, args []interface{}) string {
	// Use strings.Builder for efficient concatenation
	var builder strings.Builder
	builder.Grow(len(sql) + len(args)*16) // Pre-allocate estimated size
	builder.WriteString(sql)
	for _, arg := range args {
		builder.WriteByte('|')
		fmt.Fprint(&builder, arg)
	}
	return builder.String()
}

func isCacheableQuery(stmt *query.SelectStmt) bool {
	// Don't cache queries without a FROM clause (scalar queries might have functions like RANDOM())
	if stmt.From == nil {
		return false
	}

	// Don't cache queries with subqueries in SELECT (they might be non-deterministic)
	for _, col := range stmt.Columns {
		if containsSubquery(col) {
			return false
		}
	}

	// Don't cache queries with non-deterministic functions
	if containsNonDeterministicFunctions(stmt) {
		return false
	}

	return true
}

func containsSubquery(expr query.Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *query.SubqueryExpr, *query.ExistsExpr:
		return true
	case *query.AliasExpr:
		return containsSubquery(e.Expr)
	case *query.BinaryExpr:
		return containsSubquery(e.Left) || containsSubquery(e.Right)
	case *query.UnaryExpr:
		return containsSubquery(e.Expr)
	case *query.FunctionCall:
		for _, arg := range e.Args {
			if containsSubquery(arg) {
				return true
			}
		}
	}
	return false
}

func containsNonDeterministicFunctions(stmt *query.SelectStmt) bool {
	// Check columns
	for _, col := range stmt.Columns {
		if hasNonDeterministicFunction(col) {
			return true
		}
	}

	// Check WHERE clause
	if hasNonDeterministicFunction(stmt.Where) {
		return true
	}

	// Check ORDER BY
	for _, ob := range stmt.OrderBy {
		if hasNonDeterministicFunction(ob.Expr) {
			return true
		}
	}

	return false
}

func hasNonDeterministicFunction(expr query.Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *query.FunctionCall:
		fn := strings.ToUpper(e.Name)
		// List of non-deterministic functions
		nonDetFuncs := []string{"RANDOM", "RAND", "NOW", "CURRENT_TIMESTAMP", "UUID", "NEWID"}
		for _, ndf := range nonDetFuncs {
			if fn == ndf {
				return true
			}
		}
		// Check arguments recursively
		for _, arg := range e.Args {
			if hasNonDeterministicFunction(arg) {
				return true
			}
		}
	case *query.AliasExpr:
		return hasNonDeterministicFunction(e.Expr)
	case *query.BinaryExpr:
		return hasNonDeterministicFunction(e.Left) || hasNonDeterministicFunction(e.Right)
	case *query.UnaryExpr:
		return hasNonDeterministicFunction(e.Expr)
	}

	return false
}

func extractTablesFromQuery(stmt *query.SelectStmt) []string {
	tables := make(map[string]bool)

	if stmt.From != nil {
		tables[stmt.From.Name] = true
	}

	for _, join := range stmt.Joins {
		if join.Table != nil {
			tables[join.Table.Name] = true
		}
	}

	result := make([]string, 0, len(tables))
	for tbl := range tables {
		result = append(result, tbl)
	}
	return result
}

func queryToSQL(stmt *query.SelectStmt) string {
	// This is a simplified version - in production you'd want proper SQL generation
	// For caching purposes, we just need a consistent string representation
	var parts []string

	parts = append(parts, "SELECT")
	if stmt.Distinct {
		parts = append(parts, "DISTINCT")
	}

	// Columns
	colParts := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		colParts[i] = exprToString(col)
	}
	parts = append(parts, strings.Join(colParts, ", "))

	if stmt.From != nil {
		parts = append(parts, "FROM", stmt.From.Name)
	}

	return strings.Join(parts, " ")
}

func exprToString(expr query.Expression) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *query.Identifier:
		return e.Name
	case *query.StarExpr:
		return "*"
	case *query.StringLiteral:
		return fmt.Sprintf("'%s'", e.Value)
	case *query.NumberLiteral:
		return fmt.Sprintf("%v", e.Value)
	case *query.AliasExpr:
		return exprToString(e.Expr) + " AS " + e.Alias
	case *query.FunctionCall:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = exprToString(arg)
		}
		return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))
	default:
		return fmt.Sprintf("%T", expr)
	}
}

func New(tree *btree.BTree, pool *storage.BufferPool, wal *storage.WAL) *Catalog {
	return &Catalog{
		tree:              tree,
		tables:            make(map[string]*TableDef),
		indexes:           make(map[string]*IndexDef),
		indexTrees:        make(map[string]*btree.BTree),
		pool:              pool,
		wal:               wal,
		tableTrees:        make(map[string]*btree.BTree),
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
	}
}

func (c *Catalog) SetWAL(wal *storage.WAL) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wal = wal
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
		dtName := strings.ToLower(stmt.From.Alias)
		cat.cteResults[dtName] = &cteResultSet{columns: subCols, rows: subRows}
		defer delete(cat.cteResults, dtName)
		// Fall through to normal JOIN handling
	}

	// Check if it's a pre-computed CTE result (from recursive CTE execution)
	if cat.cteResults != nil {
		if cteRes, ok := cat.cteResults[strings.ToLower(stmt.From.Name)]; ok {
			if len(stmt.Joins) == 0 {
				// Check if outer query has window functions
				hasWindowFuncs := false
				for _, col := range stmt.Columns {
					actual := col
					if ae, ok := col.(*query.AliasExpr); ok {
						actual = ae.Expr
					}
					if _, ok := actual.(*query.WindowExpr); ok {
						hasWindowFuncs = true
						break
					}
				}
				if hasWindowFuncs {
					// Window functions need the full selectLocked pipeline.
					// Build a synthetic TableDef from CTE columns and process CTE rows
					// as if they were table rows.
					syntheticCols := make([]ColumnDef, len(cteRes.columns))
					for i, name := range cteRes.columns {
						syntheticCols[i] = ColumnDef{Name: name, Type: "TEXT"}
					}
					syntheticTable := &TableDef{
						Name:    stmt.From.Name,
						Columns: syntheticCols,
					}

					// Build selectColInfo for the outer query
					selectCols := make([]selectColInfo, len(stmt.Columns))
					returnColumns := make([]string, len(stmt.Columns))
					for i, col := range stmt.Columns {
						aliasName := ""
						actual := col
						if ae, ok := col.(*query.AliasExpr); ok {
							aliasName = ae.Alias
							actual = ae.Expr
						}
						switch c := actual.(type) {
						case *query.WindowExpr:
							selectCols[i] = selectColInfo{
								name:       aliasName,
								isWindow:   true,
								windowExpr: c,
							}
							if aliasName != "" {
								returnColumns[i] = aliasName
							} else {
								returnColumns[i] = c.Function
							}
						case *query.Identifier:
							selectCols[i] = selectColInfo{name: c.Name}
							if aliasName != "" {
								returnColumns[i] = aliasName
							} else {
								returnColumns[i] = c.Name
							}
						default:
							selectCols[i] = selectColInfo{name: aliasName}
							if aliasName != "" {
								returnColumns[i] = aliasName
							} else {
								returnColumns[i] = fmt.Sprintf("col%d", i)
							}
						}
					}

					// Filter rows with WHERE
					var filteredRows [][]interface{}
					for _, row := range cteRes.rows {
						if stmt.Where != nil {
							matched, err := evaluateWhere(cat, row, syntheticCols, stmt.Where, args)
							if err != nil || !matched {
								continue
							}
						}
						filteredRows = append(filteredRows, row)
					}

					// Project columns
					var projectedRows [][]interface{}
					for _, row := range filteredRows {
						projRow := make([]interface{}, len(selectCols))
						for i, ci := range selectCols {
							if ci.isWindow {
								projRow[i] = nil // placeholder for window function
							} else {
								// Find column in CTE result
								for j, name := range cteRes.columns {
									colName := ci.name
									if colName == "" {
										continue
									}
									if strings.EqualFold(name, colName) && j < len(row) {
										projRow[i] = row[j]
										break
									}
								}
							}
						}
						projectedRows = append(projectedRows, projRow)
					}

					// Evaluate window functions
					projectedRows = cat.evaluateWindowFunctions(projectedRows, selectCols, syntheticTable, stmt, args, filteredRows)

					// Apply ORDER BY
					if len(stmt.OrderBy) > 0 {
						sort.SliceStable(projectedRows, func(a, b int) bool {
							for _, ob := range stmt.OrderBy {
								va, _ := evaluateExpression(cat, projectedRows[a], syntheticCols, ob.Expr, args)
								vb, _ := evaluateExpression(cat, projectedRows[b], syntheticCols, ob.Expr, args)
								// Try to resolve from projected columns
								if va == nil {
									for j, ci := range selectCols {
										if id, ok := ob.Expr.(*query.Identifier); ok && strings.EqualFold(ci.name, id.Name) {
											va = projectedRows[a][j]
											vb = projectedRows[b][j]
											break
										}
									}
								}
								cmp := compareValues(va, vb)
								if cmp == 0 {
									continue
								}
								if ob.Desc {
									return cmp > 0
								}
								return cmp < 0
							}
							return false
						})
					}

					// Apply LIMIT/OFFSET
					if stmt.Offset != nil {
						offsetVal, err := evaluateExpression(cat, nil, nil, stmt.Offset, args)
						if err == nil {
							if off, ok := toInt(offsetVal); ok && off > 0 {
								if int(off) < len(projectedRows) {
									projectedRows = projectedRows[off:]
								} else {
									projectedRows = nil
								}
							}
						}
					}
					if stmt.Limit != nil {
						limitVal, err := evaluateExpression(cat, nil, nil, stmt.Limit, args)
						if err == nil {
							if lim, ok := toInt(limitVal); ok && lim >= 0 && int(lim) < len(projectedRows) {
								projectedRows = projectedRows[:lim]
							}
						}
					}

					return returnColumns, projectedRows, nil
				}
				// No window functions: use applyOuterQuery for simple CTE access
				return cat.applyOuterQuery(stmt, cteRes.columns, cteRes.rows, args)
			}
			// CTE with JOINs: register CTE as a temporary table definition
			// so executeSelectWithJoin can find it via getTableLocked
			// (executeSelectWithJoin already handles cteResults for both main and join tables)
			// Fall through - executeSelectWithJoin will pick it up from cteResults
		}
	}

	// Check if it's a view first
	view, viewErr := cat.getViewLocked(stmt.From.Name)
	if viewErr == nil {
		// Check if view is complex (has GROUP BY, HAVING, DISTINCT, aggregates, no FROM, or aliased columns)
		viewIsComplex := view.Distinct || len(view.GroupBy) > 0 || view.Having != nil || view.From == nil
		if !viewIsComplex {
			for _, col := range view.Columns {
				actual := col
				if ae, ok := col.(*query.AliasExpr); ok {
					// View has aliased columns - outer query may reference aliases
					// which don't exist in the underlying table, so treat as complex
					_ = ae
					viewIsComplex = true
					break
				}
				if fc, ok := actual.(*query.FunctionCall); ok {
					fn := strings.ToUpper(fc.Name)
					if fn == "COUNT" || fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX" || fn == "GROUP_CONCAT" {
						viewIsComplex = true
						break
					}
				}
			}
		}

		if viewIsComplex {
			// For complex views: execute the view first, then apply outer query
			viewCols, viewRows, err := cat.selectLocked(view, args)
			if err != nil {
				return nil, nil, err
			}
			if len(stmt.Joins) == 0 {
				// No JOINs: use applyOuterQuery for simple complex-view access
				return cat.applyOuterQuery(stmt, viewCols, viewRows, args)
			}
			// Complex view with JOINs: store results as temporary CTE result
			// so the JOIN handling path can resolve it
			viewResultName := strings.ToLower(stmt.From.Name)
			if cat.cteResults == nil {
				cat.cteResults = make(map[string]*cteResultSet)
			}
			cat.cteResults[viewResultName] = &cteResultSet{columns: viewCols, rows: viewRows}
			defer delete(cat.cteResults, viewResultName)
			// Skip simple view inlining - fall through to table lookup which will
			// find the CTE result we just stored
		} else {
			// Simple view: inline the view's FROM/WHERE with the outer query
			// Merge JOINs: view's JOINs first, then outer query's JOINs
			var mergedJoins []*query.JoinClause
			mergedJoins = append(mergedJoins, view.Joins...)
			mergedJoins = append(mergedJoins, stmt.Joins...)
			// Create a copy of view.From to preserve the outer query's alias
			mergedFrom := &query.TableRef{Name: view.From.Name, Alias: view.From.Alias, Subquery: view.From.Subquery, SubqueryStmt: view.From.SubqueryStmt}
			// Preserve the outer query's alias for the view name so references like ap.col still work
			if stmt.From.Alias != "" {
				mergedFrom.Alias = stmt.From.Alias
			} else if mergedFrom.Alias == "" {
				// Use the view name as alias so references like view_name.col still work
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
			// Merge WHERE clauses: both view and outer query conditions must hold
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
			// If outer query uses SELECT *, use view's columns instead
			if len(stmt.Columns) > 0 {
				if _, isStar := stmt.Columns[0].(*query.StarExpr); isStar {
					mergedStmt.Columns = view.Columns
				}
			}
			return cat.selectLocked(mergedStmt, args)
		}
	}

	// Not a view - try to get as a table (or CTE result for JOIN queries)
	table, err := cat.getTableLocked(stmt.From.Name)
	if err != nil {
		// Check if it's a CTE result that has JOINs
		var foundInCTE bool
		if cat.cteResults != nil {
			if cteRes, ok := cat.cteResults[strings.ToLower(stmt.From.Name)]; ok {
				// Create a synthetic table definition from the CTE result
				table = &TableDef{
					Name: stmt.From.Name,
				}
				for _, colName := range cteRes.columns {
					table.Columns = append(table.Columns, ColumnDef{Name: colName, Type: "TEXT"})
				}
				foundInCTE = true
			}
		}
		// Check for materialized view if not found in CTE
		if !foundInCTE {
			if mv, mvErr := cat.getMaterializedViewLocked(stmt.From.Name); mvErr == nil {
			// It's a materialized view - create a synthetic table from its data
			table = &TableDef{
				Name: stmt.From.Name,
			}
			// Extract column names from the materialized view data
			if len(mv.Data) > 0 {
				for colName := range mv.Data[0] {
					table.Columns = append(table.Columns, ColumnDef{Name: colName, Type: "TEXT"})
				}
				// Also need to store the MV data for scanning - use a special marker
				// We'll handle MV data in selectLocked similar to CTE results
			}
			// Register as temporary CTE-like result for this query
			if cat.cteResults == nil {
				cat.cteResults = make(map[string]*cteResultSet)
			}
			cols := make([]string, len(table.Columns))
			for i, col := range table.Columns {
				cols[i] = col.Name
			}
			// Convert map data to rows
			rows := make([][]interface{}, len(mv.Data))
			for i, rowMap := range mv.Data {
				row := make([]interface{}, len(table.Columns))
				for j, col := range table.Columns {
					row[j] = rowMap[col.Name]
				}
				rows[i] = row
			}
			cat.cteResults[strings.ToLower(stmt.From.Name)] = &cteResultSet{columns: cols, rows: rows}
				// MV data will be cleaned up after row scanning
			} else {
				return nil, nil, err
			}
		}
	}

	// Get column names and their indices in the table (optimized with cache)
	var selectCols []selectColInfo
	var hasAggregates bool

	// Determine main table alias for consistent naming in selectCols
	mainTableRef := stmt.From.Name
	if stmt.From.Alias != "" {
		mainTableRef = stmt.From.Alias
	}

	for _, col := range stmt.Columns {
		// Unwrap AliasExpr to get the underlying expression and alias name
		aliasName := ""
		actualCol := col
		if ae, ok := col.(*query.AliasExpr); ok {
			aliasName = ae.Alias
			actualCol = ae.Expr
		}
		switch c := actualCol.(type) {
		case *query.Identifier:
			// Check if this is a dotted identifier like "table.column"
			if dotIdx := strings.IndexByte(c.Name, '.'); dotIdx > 0 && dotIdx < len(c.Name)-1 {
				// Treat as QualifiedIdentifier
				qi := &query.QualifiedIdentifier{Table: c.Name[:dotIdx], Column: c.Name[dotIdx+1:]}
				colName := qi.Column
				targetTable := qi.Table

				mainTableAlias := stmt.From.Name
				if stmt.From.Alias != "" {
					mainTableAlias = stmt.From.Alias
				}

				if targetTable == stmt.From.Name || targetTable == stmt.From.Alias {
					if idx := table.GetColumnIndex(colName); idx >= 0 {
						displayName := colName
						if aliasName != "" {
							displayName = aliasName
						}
						selectCols = append(selectCols, selectColInfo{name: displayName, tableName: mainTableAlias, index: idx})
					}
				} else {
					for _, join := range stmt.Joins {
						joinAlias := join.Table.Name
						if join.Table.Alias != "" {
							joinAlias = join.Table.Alias
						}
						if joinAlias == targetTable || join.Table.Name == targetTable {
							joinTable, err := cat.getTableLocked(join.Table.Name)
							if err == nil {
								if idx := joinTable.GetColumnIndex(colName); idx >= 0 {
									displayName := colName
									if aliasName != "" {
										displayName = aliasName
									}
									selectCols = append(selectCols, selectColInfo{name: displayName, tableName: joinAlias, index: idx})
								}
							}
							break
						}
					}
				}
			} else {
				// Use cached column index
				if idx := table.GetColumnIndex(c.Name); idx >= 0 {
					displayName := c.Name
					if aliasName != "" {
						displayName = aliasName
					}
					selectCols = append(selectCols, selectColInfo{name: displayName, tableName: mainTableRef, index: idx})
				}
			}
		case *query.QualifiedIdentifier:
			// Handle qualified identifiers like "u.name" or "o.id"
			// Find which table this column belongs to
			targetTable := c.Table
			colName := c.Column

			// Determine main table alias for consistent naming
			mainTableAlias := stmt.From.Name
			if stmt.From.Alias != "" {
				mainTableAlias = stmt.From.Alias
			}

			// Check if it's the main table
			if targetTable == stmt.From.Name || targetTable == stmt.From.Alias {
				if idx := table.GetColumnIndex(colName); idx >= 0 {
					selectCols = append(selectCols, selectColInfo{name: colName, tableName: mainTableAlias, index: idx})
				}
			} else {
				// Check joined tables
				for _, join := range stmt.Joins {
					joinAlias := join.Table.Name
					if join.Table.Alias != "" {
						joinAlias = join.Table.Alias
					}
					if joinAlias == targetTable || join.Table.Name == targetTable {
						joinTable, err := cat.getTableLocked(join.Table.Name)
						if err == nil {
							if idx := joinTable.GetColumnIndex(colName); idx >= 0 {
								selectCols = append(selectCols, selectColInfo{name: colName, tableName: joinAlias, index: idx})
							}
						}
						break
					}
				}
			}
		case *query.StarExpr:
			// SELECT * - get all columns from main table
			for i, tc := range table.Columns {
				selectCols = append(selectCols, selectColInfo{name: tc.Name, tableName: mainTableRef, index: i})
			}
			// Also include columns from joined tables
			for _, join := range stmt.Joins {
				joinAlias := join.Table.Name
				if join.Table.Alias != "" {
					joinAlias = join.Table.Alias
				}
				joinTable, err := cat.getTableLocked(join.Table.Name)
				if err == nil {
					for i, tc := range joinTable.Columns {
						selectCols = append(selectCols, selectColInfo{name: tc.Name, tableName: joinAlias, index: i})
					}
				}
			}
		case *query.FunctionCall:
			// Handle aggregate functions: COUNT, SUM, AVG, MIN, MAX
			funcName := strings.ToUpper(c.Name)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" || funcName == "GROUP_CONCAT" {
				hasAggregates = true
				colName := "*" // Default for COUNT(*)
				aggTableName := mainTableRef
				var aggExpr query.Expression // non-nil when arg is an expression (not simple column)
				if len(c.Args) > 0 {
					switch arg := c.Args[0].(type) {
					case *query.Identifier:
						colName = arg.Name
					case *query.QualifiedIdentifier:
						colName = arg.Column
						// Find which table this belongs to
						if arg.Table == stmt.From.Name || arg.Table == stmt.From.Alias {
							aggTableName = mainTableRef
						} else {
							// Check joined tables
							for _, join := range stmt.Joins {
								joinAlias := join.Table.Name
								if join.Table.Alias != "" {
									joinAlias = join.Table.Alias
								}
								if joinAlias == arg.Table || join.Table.Name == arg.Table {
									aggTableName = joinAlias
									break
								}
							}
						}
					case *query.StarExpr:
						colName = "*"
					default:
						// Expression argument (e.g., SUM(quantity * price))
						colName = fmt.Sprintf("%v", arg)
						aggExpr = c.Args[0]
					}
				}
				displayName := c.Name + "(" + colName + ")"
				if c.Distinct {
					displayName = c.Name + "(DISTINCT " + colName + ")"
				}
				selectCols = append(selectCols, selectColInfo{
					name:          displayName,
					tableName:     aggTableName,
					index:         -1,
					isAggregate:   true,
					aggregateType: funcName,
					aggregateCol:  colName,
					aggregateExpr: aggExpr,
					isDistinct:    c.Distinct,
				})
			} else {
				// Scalar function (COALESCE, LENGTH, UPPER, etc.)
				// Check if the function contains aggregate sub-expressions
				var embeddedAggs []*query.FunctionCall
				collectAggregatesFromExpr(actualCol, &embeddedAggs)
				if len(embeddedAggs) > 0 {
					hasAggregates = true
				}
				selectCols = append(selectCols, selectColInfo{
					name:           c.Name + "()",
					tableName:      mainTableRef,
					index:          -1, // Will be evaluated per row
					hasEmbeddedAgg: len(embeddedAggs) > 0,
					originalExpr:   actualCol,
				})
			}
		case *query.WindowExpr:
			// Window function (ROW_NUMBER, RANK, etc.)
			displayName := c.Function + "()"
			if aliasName != "" {
				displayName = aliasName
			}
			selectCols = append(selectCols, selectColInfo{
				name:       displayName,
				tableName:  mainTableRef,
				index:      -1,
				isWindow:   true,
				windowExpr: c,
			})
		default:
			// Handle arbitrary expressions (CASE, CAST, arithmetic, etc.)
			// Check if expression contains aggregate function calls
			var embeddedAggs []*query.FunctionCall
			collectAggregatesFromExpr(actualCol, &embeddedAggs)
			exprName := "expr"
			if aliasName != "" {
				exprName = aliasName
			}
			if len(embeddedAggs) > 0 {
				hasAggregates = true
			}
			selectCols = append(selectCols, selectColInfo{
				name:           exprName,
				tableName:      mainTableRef,
				index:          -1, // Will be evaluated per row
				hasEmbeddedAgg: len(embeddedAggs) > 0,
				originalExpr:   actualCol,
			})
		}
		// Apply alias to the last added selectColInfo entry
		if aliasName != "" && len(selectCols) > 0 {
			selectCols[len(selectCols)-1].name = aliasName
		}
	}

	// Extract column names for return
	returnColumns := make([]string, len(selectCols))
	for i, ci := range selectCols {
		returnColumns[i] = ci.name
	}

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

	// Read all rows from B+Tree
	var rows [][]interface{}
	var windowFullRows [][]interface{} // full table rows for window function ORDER BY evaluation
	// Check if this is a materialized view with cached data
	var mvRows [][]interface{}
	var isMV bool
	if cteRes, ok := cat.cteResults[strings.ToLower(stmt.From.Name)]; ok {
		// This is MV data stored as a CTE result
		mvRows = cteRes.rows
		isMV = true
		// Clean up the MV data entry after we're done
		defer delete(cat.cteResults, strings.ToLower(stmt.From.Name))
	}

	// Get all trees for scanning (handles partitioned tables)
	trees, err := cat.getTableTreesForScan(table)
	if err != nil && !isMV {
		return returnColumns, rows, nil
	}

	// Compute early termination limit for LIMIT/OFFSET without ORDER BY/DISTINCT/window.
	// When no reordering is needed, we can stop scanning once we have offset+limit rows.
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

	// If using index, directly fetch matching rows instead of full scan
	// For partitioned tables, we need to check all partition trees
	if useIndex {
		for pk := range indexMatches {
			// Try to find the row in any partition tree
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
			if !found {
				continue // Row not found
			}

			// Decode full row with version info
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}

			// Apply AS OF temporal filtering
			if !vrow.Version.isVisibleAt(queryTime) {
				continue // Skip row that wasn't visible at query time
			}

			fullRow := vrow.Data

			// Apply WHERE clause if present (for additional conditions)
			if stmt.Where != nil {
				matched, err := evaluateWhere(cat, fullRow, table.Columns, stmt.Where, args)
				if err != nil {
					continue // Skip row on error
				}
				if !matched {
					continue // Skip row that doesn't match WHERE condition
				}
			}

			// Extract only selected columns
			selectedRow := make([]interface{}, len(selectCols))
			for i, ci := range selectCols {
				if ci.isWindow {
					// Window functions are evaluated after all rows are collected
					continue
				}
				if ci.index >= 0 && ci.index < len(fullRow) {
					// Regular column
					selectedRow[i] = fullRow[ci.index]
				} else if ci.index == -1 && !ci.isAggregate {
					// Scalar function or expression - evaluate it
					if i < len(stmt.Columns) {
						if expr, ok := stmt.Columns[i].(query.Expression); ok {
							val, err := evaluateExpression(cat, fullRow, table.Columns, expr, args)
							if err == nil {
								selectedRow[i] = val
							}
						}
					} else if strings.HasPrefix(ci.name, "__orderby_") {
						// Hidden ORDER BY expression column - extract index from name
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
			rows = append(rows, selectedRow)
			if hasWindowFuncs {
				fullRowCopy := make([]interface{}, len(fullRow))
				copy(fullRowCopy, fullRow)
				windowFullRows = append(windowFullRows, fullRowCopy)
			}
		}
	} else if isMV {
		// Materialized view data scan (rows already loaded)
		for _, fullRow := range mvRows {
			// Apply WHERE clause if present
			if stmt.Where != nil {
				matched, err := evaluateWhere(cat, fullRow, table.Columns, stmt.Where, args)
				if err != nil {
					continue // Skip row on error
				}
				if !matched {
					continue // Skip row that doesn't match WHERE condition
				}
			}

			// Extract only selected columns
			selectedRow := make([]interface{}, len(selectCols))
			for i, ci := range selectCols {
				if ci.isWindow {
					continue
				}
				if ci.index >= 0 && ci.index < len(fullRow) {
					selectedRow[i] = fullRow[ci.index]
				} else if ci.index == -1 && !ci.isAggregate {
					if i < len(stmt.Columns) {
						if expr, ok := stmt.Columns[i].(query.Expression); ok {
							val, err := evaluateExpression(cat, fullRow, table.Columns, expr, args)
							if err == nil {
								selectedRow[i] = val
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
	} else if !isMV {
		// Full table scan when no index is available
		// For partitioned tables, scan all partition trees
		for _, tree := range trees {
			iter, err := tree.Scan(nil, nil)
			if err != nil {
				return returnColumns, nil, fmt.Errorf("failed to scan table: %w", err)
			}

			for iter.HasNext() {
				_, valueData, err := iter.Next()
				if err != nil {
					break
				}

				// Decode full row with version info
				vrow, err := decodeVersionedRow(valueData, len(table.Columns))
				if err != nil {
					continue
				}

				// Apply AS OF temporal filtering
				if !vrow.Version.isVisibleAt(queryTime) {
					continue // Skip row that wasn't visible at query time
				}

				fullRow := vrow.Data

				// Apply WHERE clause if present
				if stmt.Where != nil {
					matched, err := evaluateWhere(cat, fullRow, table.Columns, stmt.Where, args)
					if err != nil {
						continue // Skip row on error
					}
					if !matched {
						continue // Skip row that doesn't match WHERE condition
					}
				}

				// Extract only selected columns
				selectedRow := make([]interface{}, len(selectCols))
				for i, ci := range selectCols {
					if ci.isWindow {
						// Window functions are evaluated after all rows are collected
						continue
					}
					if ci.index >= 0 && ci.index < len(fullRow) {
						// Regular column
						selectedRow[i] = fullRow[ci.index]
					} else if ci.index == -1 && !ci.isAggregate {
						// Scalar function or expression - evaluate it
						if i < len(stmt.Columns) {
							if expr, ok := stmt.Columns[i].(query.Expression); ok {
								val, err := evaluateExpression(cat, fullRow, table.Columns, expr, args)
								if err == nil {
									selectedRow[i] = val
								}
							}
						} else if strings.HasPrefix(ci.name, "__orderby_") {
							// Hidden ORDER BY expression column - extract index from name
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
				rows = append(rows, selectedRow)
				if hasWindowFuncs {
					fullRowCopy := make([]interface{}, len(fullRow))
					copy(fullRowCopy, fullRow)
					windowFullRows = append(windowFullRows, fullRowCopy)
				}

				// Early termination: if no ORDER BY, DISTINCT, or window functions,
				// we can stop scanning once we have enough rows for OFFSET+LIMIT.
				if earlyLimit > 0 && len(rows) >= earlyLimit {
					break
				}
			}
			iter.Close()
		}
	}

	// Evaluate window functions if present (hasWindowFuncs was computed above)
	if hasWindowFuncs {
		rows = cat.evaluateWindowFunctions(rows, selectCols, table, stmt, args, windowFullRows)
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		rows = cat.applyOrderBy(rows, selectCols, stmt.OrderBy)
	}

	// Apply DISTINCT before stripping hidden ORDER BY columns
	// This preserves sort order from ORDER BY while deduplicating visible columns
	if stmt.Distinct {
		// For DISTINCT, only compare visible columns (not hidden ORDER BY cols)
		visibleCols := len(selectCols)
		if hiddenOrderByCols > 0 {
			visibleCols = len(selectCols) - hiddenOrderByCols
		}
		seen := make(map[string]bool)
		var distinctRows [][]interface{}
		for _, row := range rows {
			visibleSlice := row
			if visibleCols < len(row) {
				visibleSlice = row[:visibleCols]
			}
			key := rowKeyForDedup(visibleSlice)
			if !seen[key] {
				seen[key] = true
				distinctRows = append(distinctRows, row)
			}
		}
		rows = distinctRows
	}

	// Strip hidden ORDER BY columns after sorting and distinct
	if hiddenOrderByCols > 0 {
		rows = stripHiddenCols(rows, len(selectCols), hiddenOrderByCols)
		selectCols = selectCols[:len(selectCols)-hiddenOrderByCols]
	}

	// Apply OFFSET if present
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

	// Apply LIMIT if present
	if stmt.Limit != nil {
		limitVal, err := evaluateExpression(cat, nil, nil, stmt.Limit, args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(rows) {
				rows = rows[:limit]
			}
		}
	}

	// Apply Row-Level Security filtering
	if cat.enableRLS && cat.rlsManager != nil && stmt.From != nil {
		rlsCtx := cat.rlsCtx
		if rlsCtx == nil {
			rlsCtx = context.Background()
		}
		user, _ := rlsCtx.Value("cobaltdb_user").(string)
		roles, _ := rlsCtx.Value("cobaltdb_roles").([]string)
		if user != "" {
			cols, filteredRows, rlsErr := cat.applyRLSFilterInternal(rlsCtx, stmt.From.Name, returnColumns, rows, user, roles)
			if rlsErr != nil {
				return nil, nil, fmt.Errorf("RLS filter failed: %w", rlsErr)
			}
			returnColumns = cols
			rows = filteredRows
		}
	}

	return returnColumns, rows, nil
}

func (cat *Catalog) applyOuterQuery(stmt *query.SelectStmt, viewCols []string, viewRows [][]interface{}, args []interface{}) ([]string, [][]interface{}, error) {
	// Build column definitions from view result columns
	columns := make([]ColumnDef, len(viewCols))
	for i, name := range viewCols {
		columns[i] = ColumnDef{Name: name}
	}

	// Check if outer query has aggregates
	hasAggregates := false
	for _, col := range stmt.Columns {
		actual := col
		if ae, ok := col.(*query.AliasExpr); ok {
			actual = ae.Expr
		}
		if fc, ok := actual.(*query.FunctionCall); ok {
			fn := strings.ToUpper(fc.Name)
			if fn == "COUNT" || fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX" || fn == "GROUP_CONCAT" {
				hasAggregates = true
				break
			}
		}
	}

	// Apply WHERE clause
	var filteredRows [][]interface{}
	if stmt.Where != nil {
		for _, row := range viewRows {
			matched, err := evaluateWhere(cat, row, columns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
			filteredRows = append(filteredRows, row)
		}
	} else {
		filteredRows = viewRows
	}

	// Handle aggregates or GROUP BY on the view result
	if hasAggregates || len(stmt.GroupBy) > 0 {
		// Build return column names
		returnCols := make([]string, len(stmt.Columns))
		for i, col := range stmt.Columns {
			aliasName := ""
			actual := col
			if ae, ok := col.(*query.AliasExpr); ok {
				aliasName = ae.Alias
				actual = ae.Expr
			}
			if aliasName != "" {
				returnCols[i] = aliasName
			} else if fc, ok := actual.(*query.FunctionCall); ok {
				fn := strings.ToUpper(fc.Name)
				if len(fc.Args) > 0 {
					returnCols[i] = fn + "()"
				} else {
					returnCols[i] = fn + "(*)"
				}
			} else if id, ok := actual.(*query.Identifier); ok {
				returnCols[i] = id.Name
			} else {
				returnCols[i] = fmt.Sprintf("col%d", i)
			}
		}

		// Group rows if GROUP BY is present
		type rowGroup struct {
			key  string
			rows [][]interface{}
		}
		var groups []rowGroup

		if len(stmt.GroupBy) > 0 {
			groupMap := make(map[string]int)
			for _, row := range filteredRows {
				var keyParts []string
				for _, gb := range stmt.GroupBy {
					val, err := evaluateExpression(cat, row, columns, gb, args)
					if err == nil {
						keyParts = append(keyParts, fmt.Sprintf("%v", val))
					} else {
						keyParts = append(keyParts, "<nil>")
					}
				}
				key := strings.Join(keyParts, "|")
				if idx, exists := groupMap[key]; exists {
					groups[idx].rows = append(groups[idx].rows, row)
				} else {
					groupMap[key] = len(groups)
					groups = append(groups, rowGroup{key: key, rows: [][]interface{}{row}})
				}
			}
		} else {
			// Single group: all rows
			groups = []rowGroup{{key: "", rows: filteredRows}}
		}

		// Compute aggregates for each group
		var resultRows [][]interface{}
		for _, group := range groups {
			resultRow := make([]interface{}, len(stmt.Columns))
			for i, col := range stmt.Columns {
				actual := col
				if ae, ok := col.(*query.AliasExpr); ok {
					actual = ae.Expr
				}
				if fc, ok := actual.(*query.FunctionCall); ok {
					fn := strings.ToUpper(fc.Name)
					resultRow[i] = cat.computeViewAggregate(fn, fc, group.rows, columns, args)
				} else {
					// Non-aggregate column: use value from first row in group
					if len(group.rows) > 0 {
						val, err := evaluateExpression(cat, group.rows[0], columns, actual, args)
						if err == nil {
							resultRow[i] = val
						}
					}
				}
			}
			resultRows = append(resultRows, resultRow)
		}

		// Build result columns for HAVING/ORDER BY evaluation
		resultColumns := make([]ColumnDef, len(returnCols))
		for i, name := range returnCols {
			resultColumns[i] = ColumnDef{Name: name}
		}

		// Apply HAVING filter
		if stmt.Having != nil {
			var havingRows [][]interface{}
			for _, row := range resultRows {
				ok, err := evaluateWhere(cat, row, resultColumns, stmt.Having, args)
				if err == nil && ok {
					havingRows = append(havingRows, row)
				}
			}
			resultRows = havingRows
		}

		// Apply ORDER BY
		if len(stmt.OrderBy) > 0 && len(resultRows) > 1 {
			sort.SliceStable(resultRows, func(i, j int) bool {
				for _, ob := range stmt.OrderBy {
					vi, _ := evaluateExpression(cat, resultRows[i], resultColumns, ob.Expr, args)
					vj, _ := evaluateExpression(cat, resultRows[j], resultColumns, ob.Expr, args)
					cmp := compareValues(vi, vj)
					if ob.Desc {
						cmp = -cmp
					}
					if cmp != 0 {
						return cmp < 0
					}
				}
				return false
			})
		}

		// Apply LIMIT/OFFSET
		if stmt.Offset != nil {
			offsetVal, err := EvalExpression(stmt.Offset, args)
			if err == nil {
				if f, ok := toFloat64(offsetVal); ok {
					offset := int(f)
					if offset >= len(resultRows) {
						resultRows = nil
					} else if offset > 0 {
						resultRows = resultRows[offset:]
					}
				}
			}
		}
		if stmt.Limit != nil {
			limitVal, err := EvalExpression(stmt.Limit, args)
			if err == nil {
				if f, ok := toFloat64(limitVal); ok {
					limit := int(f)
					if limit >= 0 && limit <= len(resultRows) {
						resultRows = resultRows[:limit]
					}
				}
			}
		}

		return returnCols, resultRows, nil
	}

	// Project columns
	var returnCols []string
	var resultRows [][]interface{}

	// Build column mapping
	type colMapping struct {
		name    string
		viewIdx int // -1 if needs evaluation
	}
	var mappings []colMapping

	for _, col := range stmt.Columns {
		aliasName := ""
		actual := col
		if ae, ok := col.(*query.AliasExpr); ok {
			aliasName = ae.Alias
			actual = ae.Expr
		}
		switch c := actual.(type) {
		case *query.StarExpr:
			for j, name := range viewCols {
				mappings = append(mappings, colMapping{name: name, viewIdx: j})
			}
		case *query.Identifier:
			found := false
			for j, name := range viewCols {
				if strings.EqualFold(name, c.Name) {
					displayName := name
					if aliasName != "" {
						displayName = aliasName
					}
					mappings = append(mappings, colMapping{name: displayName, viewIdx: j})
					found = true
					break
				}
			}
			if !found {
				mappings = append(mappings, colMapping{name: c.Name, viewIdx: -1})
			}
		default:
			name := "expr"
			if aliasName != "" {
				name = aliasName
			}
			mappings = append(mappings, colMapping{name: name, viewIdx: -1})
		}
	}

	returnCols = make([]string, len(mappings))
	for i, m := range mappings {
		returnCols[i] = m.name
	}

	for _, row := range filteredRows {
		resultRow := make([]interface{}, len(mappings))
		for i, m := range mappings {
			if m.viewIdx >= 0 && m.viewIdx < len(row) {
				resultRow[i] = row[m.viewIdx]
			} else {
				// Evaluate expression against view row
				val, err := evaluateExpression(cat, row, columns, stmt.Columns[i], args)
				if err == nil {
					resultRow[i] = val
				}
			}
		}
		resultRows = append(resultRows, resultRow)
	}

	// Build selectColInfo for ORDER BY
	selectCols := make([]selectColInfo, len(mappings))
	for i, m := range mappings {
		selectCols[i] = selectColInfo{name: m.name, index: i}
	}

	// Add hidden ORDER BY columns from view that aren't in the outer SELECT
	hiddenViewOrderByCols := 0
	if len(stmt.OrderBy) > 0 {
		for _, ob := range stmt.OrderBy {
			if ident, ok := ob.Expr.(*query.Identifier); ok {
				// Check if this column is already in selectCols
				found := false
				for _, sc := range selectCols {
					if strings.EqualFold(sc.name, ident.Name) {
						found = true
						break
					}
				}
				if !found {
					// Check if it's a view column
					for j, vc := range viewCols {
						if strings.EqualFold(vc, ident.Name) {
							// Add as hidden column and append values from view rows
							selectCols = append(selectCols, selectColInfo{name: vc, index: len(mappings) + hiddenViewOrderByCols})
							for k := range resultRows {
								if j < len(filteredRows[k]) {
									resultRows[k] = append(resultRows[k], filteredRows[k][j])
								}
							}
							hiddenViewOrderByCols++
							break
						}
					}
				}
			}
		}
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		resultRows = cat.applyOrderBy(resultRows, selectCols, stmt.OrderBy)
	}

	// Strip hidden ORDER BY columns
	if hiddenViewOrderByCols > 0 {
		visibleCount := len(mappings)
		for i, row := range resultRows {
			if len(row) > visibleCount {
				resultRows[i] = row[:visibleCount]
			}
		}
	}

	// Apply DISTINCT
	if stmt.Distinct {
		resultRows = cat.applyDistinct(resultRows)
	}

	// Apply OFFSET
	if stmt.Offset != nil {
		offsetVal, err := evaluateExpression(cat, nil, nil, stmt.Offset, args)
		if err == nil {
			if offset, ok := toInt(offsetVal); ok && offset > 0 {
				if offset >= len(resultRows) {
					resultRows = nil
				} else {
					resultRows = resultRows[offset:]
				}
			}
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		limitVal, err := evaluateExpression(cat, nil, nil, stmt.Limit, args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(resultRows) {
				resultRows = resultRows[:limit]
			}
		}
	}

	return returnCols, resultRows, nil
}

func (cat *Catalog) computeViewAggregate(fn string, fc *query.FunctionCall, rows [][]interface{}, columns []ColumnDef, args []interface{}) interface{} {
	switch fn {
	case "COUNT":
		if len(fc.Args) > 0 {
			if _, ok := fc.Args[0].(*query.StarExpr); ok {
				return int64(len(rows))
			}
			// COUNT(col) - count non-null
			count := int64(0)
			for _, row := range rows {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					count++
				}
			}
			return count
		}
		return int64(len(rows))
	case "SUM":
		sum := float64(0)
		hasVal := false
		for _, row := range rows {
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					if f, ok := toFloat64(val); ok {
						sum += f
						hasVal = true
					}
				}
			}
		}
		if hasVal {
			return sum
		}
		return nil
	case "AVG":
		sum := float64(0)
		count := 0
		for _, row := range rows {
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					if f, ok := toFloat64(val); ok {
						sum += f
						count++
					}
				}
			}
		}
		if count > 0 {
			return sum / float64(count)
		}
		return nil
	case "MIN":
		var minVal interface{}
		for _, row := range rows {
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					if minVal == nil || compareValues(val, minVal) < 0 {
						minVal = val
					}
				}
			}
		}
		return minVal
	case "MAX":
		var maxVal interface{}
		for _, row := range rows {
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					if maxVal == nil || compareValues(val, maxVal) > 0 {
						maxVal = val
					}
				}
			}
		}
		return maxVal
	case "GROUP_CONCAT":
		var parts []string
		for _, row := range rows {
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					parts = append(parts, fmt.Sprintf("%v", val))
				}
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, ",")
		}
		return nil
	}
	return nil
}



func addHiddenOrderByCols(orderBy []*query.OrderByExpr, selectCols []selectColInfo, table *TableDef) ([]selectColInfo, int) {
	if len(orderBy) == 0 || table == nil {
		return selectCols, 0
	}

	added := 0
	for obIdx, ob := range orderBy {
		switch expr := ob.Expr.(type) {
		case *query.Identifier:
			nameLower := strings.ToLower(expr.Name)
			found := false
			for _, ci := range selectCols {
				if strings.ToLower(ci.name) == nameLower {
					found = true
					break
				}
			}
			if !found {
				colIdx := table.GetColumnIndex(expr.Name)
				if colIdx >= 0 {
					selectCols = append(selectCols, selectColInfo{
						name:  expr.Name,
						index: colIdx,
					})
					added++
				}
			}
		case *query.NumberLiteral:
			// Positional ORDER BY - always in selectCols
		default:
			// Expression - add as hidden column to be evaluated per-row
			// Use obIdx so applyOrderBy can match by ORDER BY position
			exprName := fmt.Sprintf("__orderby_%d", obIdx)
			selectCols = append(selectCols, selectColInfo{
				name:  exprName,
				index: -1, // Will be evaluated per row
			})
			added++
		}
	}
	return selectCols, added
}

// tryCountStarFastPath detects SELECT COUNT(*) FROM table and counts rows
// without decoding row data. Returns (cols, rows, true) if fast path used.
func (cat *Catalog) tryCountStarFastPath(stmt *query.SelectStmt, args []interface{}, queryTime time.Time) ([]string, [][]interface{}, bool) {
	// Only works for: single COUNT(*), single table, no JOINs, no GROUP BY, no subquery
	if len(stmt.Columns) != 1 || len(stmt.Joins) > 0 || len(stmt.GroupBy) > 0 || stmt.Having != nil {
		return nil, nil, false
	}
	if stmt.From.Subquery != nil || stmt.From.SubqueryStmt != nil {
		return nil, nil, false
	}

	// Check if it's COUNT(*) or COUNT(*) with alias
	col := stmt.Columns[0]
	if ae, ok := col.(*query.AliasExpr); ok {
		col = ae.Expr
	}
	fc, ok := col.(*query.FunctionCall)
	if !ok || !strings.EqualFold(fc.Name, "COUNT") || len(fc.Args) != 1 {
		return nil, nil, false
	}
	if _, isStar := fc.Args[0].(*query.StarExpr); !isStar {
		return nil, nil, false
	}

	// Get table and tree
	table, err := cat.getTableLocked(stmt.From.Name)
	if err != nil {
		return nil, nil, false
	}
	// Skip fast path for partitioned tables (data spread across partition trees)
	if table.Partition != nil {
		return nil, nil, false
	}
	tree, exists := cat.tableTrees[stmt.From.Name]
	if !exists {
		return nil, nil, false
	}

	// Count rows by iterating B-tree keys
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return nil, nil, false
	}

	count := int64(0)
	hasWhere := stmt.Where != nil
	hasTemporalQuery := stmt.AsOf != nil

	for iter.HasNext() {
		key, valueData, err := iter.Next()
		if err != nil || key == nil {
			break
		}

		if !hasWhere && !hasTemporalQuery {
			// Fast path: no WHERE, no AS OF — skip all decoding, just count keys
			// Soft-deleted rows still have B-tree entries, so we need a minimal check
			// But for non-temporal queries, all live rows are visible
			if len(valueData) > 0 && valueData[0] == '{' {
				// Check if row has non-zero DeletedAt via quick byte scan
				if !bytesContainDeletedAt(valueData) {
					count++
					continue
				}
			}
			// Fallback to full decode for edge cases
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			if vrow.Version.DeletedAt == 0 {
				count++
			}
		} else if hasWhere {
			// WHERE clause requires row data
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			if !vrow.Version.isVisibleAt(queryTime) {
				continue
			}
			matched, err := evaluateWhere(cat, vrow.Data, table.Columns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
			count++
		} else {
			// AS OF temporal query — need version check
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			if !vrow.Version.isVisibleAt(queryTime) {
				continue
			}
			count++
		}
	}
	iter.Close()

	colName := "COUNT(*)"
	if ae, ok := stmt.Columns[0].(*query.AliasExpr); ok {
		colName = ae.Alias
	}

	return []string{colName}, [][]interface{}{{count}}, true
}

// bytesContainDeletedAt quickly checks if JSON data has a non-zero deleted_at.
// Returns true if "deleted_at" appears with a non-zero value (soft-deleted row).
// This avoids full JSON unmarshal for COUNT(*) fast path.
// trySimpleAggregateFastPath handles SELECT with only simple aggregates
// (SUM, AVG, MIN, MAX, COUNT) on a single table without GROUP BY/JOIN/subquery.
// Computes aggregates in a single streaming pass.
func (cat *Catalog) trySimpleAggregateFastPath(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, bool) {
	// Requirements: no GROUP BY, no HAVING, no JOINs, no subquery, no ORDER BY, no LIMIT
	if len(stmt.GroupBy) > 0 || stmt.Having != nil || len(stmt.Joins) > 0 {
		return nil, nil, false
	}
	if stmt.From == nil || stmt.From.Subquery != nil || stmt.From.SubqueryStmt != nil {
		return nil, nil, false
	}
	if stmt.AsOf != nil || stmt.Limit != nil || len(stmt.OrderBy) > 0 {
		return nil, nil, false
	}

	// All columns must be simple aggregates (no DISTINCT, no expression args)
	type aggSpec struct {
		funcName string // SUM, AVG, MIN, MAX, COUNT
		colName  string // column name or "*"
		colIdx   int    // column index in table
		alias    string // result column name
		isDistinct bool
	}
	var specs []aggSpec

	for _, col := range stmt.Columns {
		actual := col
		alias := ""
		if ae, ok := col.(*query.AliasExpr); ok {
			alias = ae.Alias
			actual = ae.Expr
		}
		fc, ok := actual.(*query.FunctionCall)
		if !ok || len(fc.Args) != 1 {
			return nil, nil, false
		}
		funcName := strings.ToUpper(fc.Name)
		if funcName != "SUM" && funcName != "AVG" && funcName != "MIN" && funcName != "MAX" && funcName != "COUNT" {
			return nil, nil, false
		}

		// DISTINCT aggregates are complex — fall back to normal path
		if fc.Distinct {
			return nil, nil, false
		}

		colName := "*"
		if _, isStar := fc.Args[0].(*query.StarExpr); !isStar {
			if id, ok := fc.Args[0].(*query.Identifier); ok {
				colName = id.Name
			} else {
				return nil, nil, false // expression arg, too complex
			}
		}
		if alias == "" {
			alias = funcName + "(" + colName + ")"
		}
		specs = append(specs, aggSpec{funcName: funcName, colName: colName, alias: alias})
	}

	if len(specs) == 0 {
		return nil, nil, false
	}

	// Get table
	table, err := cat.getTableLocked(stmt.From.Name)
	if err != nil {
		return nil, nil, false
	}
	if table.Partition != nil {
		return nil, nil, false
	}
	tree, exists := cat.tableTrees[stmt.From.Name]
	if !exists {
		return nil, nil, false
	}

	// Resolve column indices
	for i := range specs {
		if specs[i].colName != "*" {
			specs[i].colIdx = table.GetColumnIndex(specs[i].colName)
			if specs[i].colIdx < 0 {
				return nil, nil, false
			}
		}
	}

	// Streaming aggregate state
	type aggState struct {
		count  int64
		sum    float64
		hasVal bool
		genVal interface{} // for MIN/MAX on non-numeric types (strings)
	}
	states := make([]aggState, len(specs))

	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return nil, nil, false
	}

	// Use byte-level fast path for SUM/AVG without WHERE (skip full JSON decode)
	canUseByteFastPath := stmt.Where == nil
	if canUseByteFastPath {
		for _, spec := range specs {
			if spec.funcName != "SUM" && spec.funcName != "AVG" && !(spec.funcName == "COUNT" && spec.colName == "*") {
				canUseByteFastPath = false
				break
			}
		}
	}

	for iter.HasNext() {
		_, valueData, err := iter.Next()
		if err != nil {
			break
		}

		if canUseByteFastPath && len(valueData) > 0 && valueData[0] == '{' {
			if bytesContainDeletedAt(valueData) {
				continue
			}
			// Try byte-level extraction; fall back to full decode on failure
			allOK := true
			for i, spec := range specs {
				if spec.funcName == "COUNT" {
					states[i].count++
					continue
				}
				if fval, ok := extractColumnFloat64(valueData, spec.colIdx); ok {
					states[i].sum += fval
					states[i].count++
					states[i].hasVal = true
				} else {
					allOK = false
					break
				}
			}
			if allOK {
				continue
			}
			// Byte extraction failed for this row — undo partial updates and fall through
			for i, spec := range specs {
				if spec.funcName == "COUNT" {
					states[i].count--
				}
			}
		}

		vrow, err := decodeVersionedRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}
		if vrow.Version.DeletedAt > 0 {
			continue
		}
		row := vrow.Data

		// Apply WHERE
		if stmt.Where != nil {
			matched, err := evaluateWhere(cat, row, table.Columns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
		}

		// Update aggregates
		for i, spec := range specs {
			if spec.funcName == "COUNT" {
				if spec.colName == "*" {
					states[i].count++
				} else if spec.colIdx < len(row) && row[spec.colIdx] != nil {
					states[i].count++
				}
				continue
			}

			if spec.colIdx >= len(row) || row[spec.colIdx] == nil {
				continue
			}
			val := row[spec.colIdx]

			switch spec.funcName {
			case "SUM":
				if fval, ok := toFloat64Safe(val); ok {
					states[i].sum += fval
					states[i].hasVal = true
				}
			case "AVG":
				if fval, ok := toFloat64Safe(val); ok {
					states[i].sum += fval
					states[i].count++
					states[i].hasVal = true
				}
			case "MIN":
				if !states[i].hasVal {
					states[i].genVal = val
					states[i].hasVal = true
				} else if compareValues(val, states[i].genVal) < 0 {
					states[i].genVal = val
				}
			case "MAX":
				if !states[i].hasVal {
					states[i].genVal = val
					states[i].hasVal = true
				} else if compareValues(val, states[i].genVal) > 0 {
					states[i].genVal = val
				}
			}
		}
	}
	iter.Close()

	// Build result
	colNames := make([]string, len(specs))
	resultRow := make([]interface{}, len(specs))
	for i, spec := range specs {
		colNames[i] = spec.alias
		switch spec.funcName {
		case "COUNT":
			resultRow[i] = states[i].count
		case "SUM":
			if states[i].hasVal {
				resultRow[i] = states[i].sum
			}
		case "AVG":
			if states[i].hasVal && states[i].count > 0 {
				resultRow[i] = states[i].sum / float64(states[i].count)
			}
		case "MIN", "MAX":
			if states[i].hasVal {
				resultRow[i] = states[i].genVal
			}
		}
	}

	return colNames, [][]interface{}{resultRow}, true
}

// toFloat64Safe converts a value to float64, returning (value, true) or (0, false)
func toFloat64Safe(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	}
	return 0, false
}

func bytesContainDeletedAt(data []byte) bool {
	// Look for "deleted_at": followed by a non-zero digit
	needle := []byte(`"deleted_at":`)
	for i := 0; i <= len(data)-len(needle)-1; i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			if data[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			// Check the value after the colon
			pos := i + len(needle)
			// Skip whitespace
			for pos < len(data) && (data[pos] == ' ' || data[pos] == '\t') {
				pos++
			}
			// If value is 0, row is NOT deleted
			if pos < len(data) && data[pos] == '0' {
				return false
			}
			// Non-zero value means row IS deleted
			return true
		}
	}
	// No deleted_at field found — treat as not deleted (legacy format)
	return false
}

func stripHiddenCols(rows [][]interface{}, totalCols int, hiddenCount int) [][]interface{} {
	if hiddenCount <= 0 {
		return rows
	}
	visibleCount := totalCols - hiddenCount
	for i, row := range rows {
		if len(row) > visibleCount {
			rows[i] = row[:visibleCount]
		}
	}
	return rows
}

func rowKeyForDedup(vals []interface{}) string {
	var b strings.Builder
	for i, val := range vals {
		if i > 0 {
			b.WriteByte(0) // null byte separator
		}
		if val == nil {
			b.WriteString("\x01NULL\x01") // tagged NULL marker
		} else {
			b.WriteString(fmt.Sprintf("V:%v", val))
		}
	}
	return b.String()
}

func addHiddenHavingAggregates(having query.Expression, selectCols []selectColInfo, mainTableRef string) ([]selectColInfo, int) {
	if having == nil {
		return selectCols, 0
	}
	// Collect all aggregate function calls from HAVING
	var aggCalls []*query.FunctionCall
	collectAggregatesFromExpr(having, &aggCalls)

	added := 0
	for _, fc := range aggCalls {
		funcName := strings.ToUpper(fc.Name)
		colName := "*"
		aggTableName := mainTableRef
		var aggExpr query.Expression
		if len(fc.Args) > 0 {
			switch arg := fc.Args[0].(type) {
			case *query.Identifier:
				colName = arg.Name
			case *query.QualifiedIdentifier:
				colName = arg.Column
				aggTableName = arg.Table
			default:
				aggExpr = fc.Args[0]
			}
		}
		// Check if this aggregate is already in selectCols
		found := false
		for _, sc := range selectCols {
			if sc.isAggregate && strings.EqualFold(sc.aggregateType, funcName) && strings.EqualFold(sc.aggregateCol, colName) {
				found = true
				break
			}
		}
		if !found {
			displayName := funcName + "(" + colName + ")"
			if fc.Distinct {
				displayName = funcName + "(DISTINCT " + colName + ")"
			}
			selectCols = append(selectCols, selectColInfo{
				name:          displayName,
				tableName:     aggTableName,
				index:         -1,
				isAggregate:   true,
				aggregateType: funcName,
				aggregateCol:  colName,
				aggregateExpr: aggExpr,
				isDistinct:    fc.Distinct,
			})
			added++
		}
	}
	return selectCols, added
}

func addHiddenOrderByAggregates(orderBy []*query.OrderByExpr, selectCols []selectColInfo, mainTableRef string) ([]selectColInfo, int) {
	if len(orderBy) == 0 {
		return selectCols, 0
	}
	var aggCalls []*query.FunctionCall
	for _, ob := range orderBy {
		collectAggregatesFromExpr(ob.Expr, &aggCalls)
	}
	added := 0
	for _, fc := range aggCalls {
		funcName := strings.ToUpper(fc.Name)
		colName := "*"
		aggTableName := mainTableRef
		var aggExpr query.Expression
		if len(fc.Args) > 0 {
			switch arg := fc.Args[0].(type) {
			case *query.Identifier:
				colName = arg.Name
			case *query.QualifiedIdentifier:
				colName = arg.Column
				aggTableName = arg.Table
			default:
				aggExpr = fc.Args[0]
			}
		}
		// Check if this aggregate is already in selectCols
		found := false
		for _, sc := range selectCols {
			if sc.isAggregate && strings.EqualFold(sc.aggregateType, funcName) && strings.EqualFold(sc.aggregateCol, colName) {
				found = true
				break
			}
		}
		if !found {
			displayName := funcName + "(" + colName + ")"
			if fc.Distinct {
				displayName = funcName + "(DISTINCT " + colName + ")"
			}
			selectCols = append(selectCols, selectColInfo{
				name:          displayName,
				tableName:     aggTableName,
				index:         -1,
				isAggregate:   true,
				aggregateType: funcName,
				aggregateCol:  colName,
				aggregateExpr: aggExpr,
				isDistinct:    fc.Distinct,
			})
			added++
		}
	}
	return selectCols, added
}

func collectAggregatesFromExpr(expr query.Expression, result *[]*query.FunctionCall) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *query.FunctionCall:
		funcName := strings.ToUpper(e.Name)
		if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" || funcName == "GROUP_CONCAT" {
			*result = append(*result, e)
		}
		for _, arg := range e.Args {
			collectAggregatesFromExpr(arg, result)
		}
	case *query.BinaryExpr:
		collectAggregatesFromExpr(e.Left, result)
		collectAggregatesFromExpr(e.Right, result)
	case *query.UnaryExpr:
		collectAggregatesFromExpr(e.Expr, result)
	case *query.AliasExpr:
		collectAggregatesFromExpr(e.Expr, result)
	case *query.BetweenExpr:
		collectAggregatesFromExpr(e.Expr, result)
		collectAggregatesFromExpr(e.Lower, result)
		collectAggregatesFromExpr(e.Upper, result)
	case *query.InExpr:
		collectAggregatesFromExpr(e.Expr, result)
		for _, v := range e.List {
			collectAggregatesFromExpr(v, result)
		}
	case *query.IsNullExpr:
		collectAggregatesFromExpr(e.Expr, result)
	case *query.CaseExpr:
		if e.Expr != nil {
			collectAggregatesFromExpr(e.Expr, result)
		}
		for _, w := range e.Whens {
			collectAggregatesFromExpr(w.Condition, result)
			collectAggregatesFromExpr(w.Result, result)
		}
		if e.Else != nil {
			collectAggregatesFromExpr(e.Else, result)
		}
	case *query.LikeExpr:
		collectAggregatesFromExpr(e.Expr, result)
		collectAggregatesFromExpr(e.Pattern, result)
	}
}

func evaluateHaving(c *Catalog, row []interface{}, selectCols []selectColInfo, columns []ColumnDef, having query.Expression, args []interface{}) (bool, error) {
	if having == nil {
		return true, nil
	}

	// For HAVING, we need to handle aggregate functions specially
	// The aggregate results are already in the row at the indices matching selectCols
	// We need to transform the HAVING expression to use indices from the row

	// First, simplify the HAVING expression by replacing aggregate calls with their values
	havingExpr := resolveAggregateInExpr(having, selectCols, row)

	// Build columns from selectCols if not provided
	evalCols := columns
	if evalCols == nil {
		evalCols = make([]ColumnDef, len(selectCols))
		for i, sc := range selectCols {
			evalCols[i] = ColumnDef{Name: sc.name}
			if sc.tableName != "" {
				evalCols[i].sourceTbl = sc.tableName
			}
		}
	}

	// Now evaluate the simplified expression
	result, err := evaluateExpression(c, row, evalCols, havingExpr, args)
	if err != nil {
		return false, err
	}

	if result == nil {
		return false, nil
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case int, int64, float64:
		switch n := v.(type) {
		case int:
			return n != 0, nil
		case int64:
			return n != 0, nil
		case float64:
			return n != 0, nil
		}
	}
	return true, nil
}

func resolveAggregateInExpr(expr query.Expression, selectCols []selectColInfo, row []interface{}) query.Expression {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *query.BinaryExpr:
		return &query.BinaryExpr{
			Left:     resolveAggregateInExpr(e.Left, selectCols, row),
			Operator: e.Operator,
			Right:    resolveAggregateInExpr(e.Right, selectCols, row),
		}
	case *query.FunctionCall:
		// Check if this is an aggregate function
		funcName := strings.ToUpper(e.Name)
		if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" || funcName == "GROUP_CONCAT" {
			// Find the column name for this aggregate
			colName := "*"
			hasExprArg := false
			if len(e.Args) > 0 {
				if ident, ok := e.Args[0].(*query.Identifier); ok {
					colName = ident.Name
				} else if qi, ok := e.Args[0].(*query.QualifiedIdentifier); ok {
					colName = qi.Column
				} else if _, ok := e.Args[0].(*query.StarExpr); ok {
					colName = "*"
				} else {
					// Expression argument like SUM(quantity * price)
					colName = fmt.Sprintf("%v", e.Args[0])
					hasExprArg = true
				}
			}
			aggSignature := funcName + "(" + colName + ")"
			isDistinct := e.Distinct
			if isDistinct {
				aggSignature = funcName + "(DISTINCT " + colName + ")"
			}

			// Find the index in selectCols - match by aggregate type/col or by name/alias
			for i, sc := range selectCols {
				if !sc.isAggregate || i >= len(row) {
					continue
				}
				// Match by aggregate function signature
				scSignature := sc.aggregateType + "(" + sc.aggregateCol + ")"
				if sc.isDistinct {
					scSignature = sc.aggregateType + "(DISTINCT " + sc.aggregateCol + ")"
				}
				if strings.EqualFold(aggSignature, scSignature) || sc.name == aggSignature {
					return valueToLiteral(row[i])
				}
				// For expression args, match by aggregate type when both have expressions
				if hasExprArg && sc.aggregateExpr != nil && strings.EqualFold(funcName, sc.aggregateType) {
					return valueToLiteral(row[i])
				}
			}
		}
		return e
	case *query.Identifier:
		// Try to find identifier in selectCols (works for both aggregate aliases and regular columns)
		for i, sc := range selectCols {
			if strings.EqualFold(sc.name, e.Name) && i < len(row) {
				return valueToLiteral(row[i])
			}
		}
		return e
	case *query.UnaryExpr:
		return &query.UnaryExpr{
			Operator: e.Operator,
			Expr:     resolveAggregateInExpr(e.Expr, selectCols, row),
		}
	case *query.BetweenExpr:
		return &query.BetweenExpr{
			Expr:  resolveAggregateInExpr(e.Expr, selectCols, row),
			Lower: resolveAggregateInExpr(e.Lower, selectCols, row),
			Upper: resolveAggregateInExpr(e.Upper, selectCols, row),
			Not:   e.Not,
		}
	case *query.InExpr:
		resolved := &query.InExpr{
			Expr:     resolveAggregateInExpr(e.Expr, selectCols, row),
			Not:      e.Not,
			Subquery: e.Subquery,
		}
		if len(e.List) > 0 {
			resolved.List = make([]query.Expression, len(e.List))
			for i, v := range e.List {
				resolved.List[i] = resolveAggregateInExpr(v, selectCols, row)
			}
		}
		return resolved
	case *query.CaseExpr:
		resolved := &query.CaseExpr{}
		if e.Expr != nil {
			resolved.Expr = resolveAggregateInExpr(e.Expr, selectCols, row)
		}
		for _, w := range e.Whens {
			resolved.Whens = append(resolved.Whens, &query.WhenClause{
				Condition: resolveAggregateInExpr(w.Condition, selectCols, row),
				Result:    resolveAggregateInExpr(w.Result, selectCols, row),
			})
		}
		if e.Else != nil {
			resolved.Else = resolveAggregateInExpr(e.Else, selectCols, row)
		}
		return resolved
	case *query.IsNullExpr:
		return &query.IsNullExpr{
			Expr: resolveAggregateInExpr(e.Expr, selectCols, row),
			Not:  e.Not,
		}
	default:
		return e
	}
}

func replaceAggregatesInExpr(expr query.Expression, aggResults map[*query.FunctionCall]interface{}) query.Expression {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *query.FunctionCall:
		if val, ok := aggResults[e]; ok {
			return valueToLiteral(val)
		}
		// Recurse into function arguments (for COALESCE(SUM(x), 0) etc.)
		newArgs := make([]query.Expression, len(e.Args))
		changed := false
		for i, arg := range e.Args {
			newArgs[i] = replaceAggregatesInExpr(arg, aggResults)
			if newArgs[i] != arg {
				changed = true
			}
		}
		if changed {
			return &query.FunctionCall{
				Name:     e.Name,
				Args:     newArgs,
				Distinct: e.Distinct,
			}
		}
		return e
	case *query.CaseExpr:
		newCase := &query.CaseExpr{}
		if e.Expr != nil {
			newCase.Expr = replaceAggregatesInExpr(e.Expr, aggResults)
		}
		for _, w := range e.Whens {
			newCase.Whens = append(newCase.Whens, &query.WhenClause{
				Condition: replaceAggregatesInExpr(w.Condition, aggResults),
				Result:    replaceAggregatesInExpr(w.Result, aggResults),
			})
		}
		if e.Else != nil {
			newCase.Else = replaceAggregatesInExpr(e.Else, aggResults)
		}
		return newCase
	case *query.BinaryExpr:
		return &query.BinaryExpr{
			Left:     replaceAggregatesInExpr(e.Left, aggResults),
			Operator: e.Operator,
			Right:    replaceAggregatesInExpr(e.Right, aggResults),
		}
	case *query.UnaryExpr:
		return &query.UnaryExpr{
			Operator: e.Operator,
			Expr:     replaceAggregatesInExpr(e.Expr, aggResults),
		}
	case *query.AliasExpr:
		return &query.AliasExpr{
			Expr:  replaceAggregatesInExpr(e.Expr, aggResults),
			Alias: e.Alias,
		}
	default:
		return e
	}
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
			innerTables[strings.ToLower(subquery.From.Alias)] = true
		} else {
			innerTables[strings.ToLower(subquery.From.Name)] = true
		}
	}
	for _, join := range subquery.Joins {
		if join.Table != nil {
			if join.Table.Alias != "" {
				innerTables[strings.ToLower(join.Table.Alias)] = true
			} else {
				innerTables[strings.ToLower(join.Table.Name)] = true
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
		tableName := strings.ToLower(e.Table)
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

func evaluateWhere(c *Catalog, row []interface{}, columns []ColumnDef, where query.Expression, args []interface{}) (bool, error) {
	if where == nil {
		return true, nil
	}

	// Evaluate the expression
	result, err := evaluateExpression(c, row, columns, where, args)
	if err != nil {
		return false, err
	}

	// Convert result to boolean
	// Note: result can be nil for IS NULL expressions - this is handled below

	switch v := result.(type) {
	case bool:
		return v, nil
	case nil:
		// For IS NULL expressions, nil result means the value is null
		// but we need to check if this was from an IS NULL expression
		// If the where expression is IsNullExpr, nil result should be treated as false
		// because evaluateIsNull returns a bool, not nil
		return false, nil
	case int, int64, float64:
		// Non-zero numbers are truthy
		switch n := v.(type) {
		case int:
			return n != 0, nil
		case int64:
			return n != 0, nil
		case float64:
			return n != 0, nil
		}
	case string:
		return v != "", nil
	}

	return false, nil
}

func isIntegerType(v interface{}) bool {
	switch v.(type) {
	case int:
		return true
	case int64:
		return true
	case float64:
		// JSON numbers are decoded as float64, check if it's a whole number
		f := v.(float64)
		return f == float64(int64(f)) && f >= -1e15 && f <= 1e15
	}
	return false
}

func addValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot add non-numeric values")
	}
	result := aNum + bNum
	if isIntegerType(a) && isIntegerType(b) {
		return int64(result), nil
	}
	return result, nil
}

func subtractValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot subtract non-numeric values")
	}
	result := aNum - bNum
	if isIntegerType(a) && isIntegerType(b) {
		return int64(result), nil
	}
	return result, nil
}

func multiplyValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot multiply non-numeric values")
	}
	result := aNum * bNum
	if isIntegerType(a) && isIntegerType(b) {
		return int64(result), nil
	}
	return result, nil
}

func divideValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot divide non-numeric values")
	}
	if bNum == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	return aNum / bNum, nil
}

func moduloValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot compute modulo of non-numeric values")
	}
	if bNum == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	// Use integer modulo if both are ints
	_, aIsInt := a.(int)
	_, bIsInt := b.(int)
	_, aIsInt64 := a.(int64)
	_, bIsInt64 := b.(int64)
	if (aIsInt || aIsInt64) && (bIsInt || bIsInt64) {
		return int64(aNum) % int64(bNum), nil
	}
	return math.Mod(aNum, bNum), nil
}

func evaluateLike(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.LikeExpr, args []interface{}) (interface{}, error) {
	left, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	pattern, err := evaluateExpression(c, row, columns, expr.Pattern, args)
	if err != nil {
		return false, err
	}

	// Handle NULL - SQL three-valued logic: NULL in LIKE → NULL (unknown)
	if left == nil || pattern == nil {
		return nil, nil
	}

	leftStr, ok := left.(string)
	if !ok {
		leftStr = fmt.Sprintf("%v", left)
	}

	patternStr, ok := pattern.(string)
	if !ok {
		patternStr = fmt.Sprintf("%v", pattern)
	}

	// Handle ESCAPE character
	escapeChar := byte(0)
	if expr.Escape != nil {
		escVal, err := evaluateExpression(c, row, columns, expr.Escape, args)
		if err == nil && escVal != nil {
			escStr := fmt.Sprintf("%v", escVal)
			if len(escStr) == 1 {
				escapeChar = escStr[0]
			}
		}
	}

	var matched bool
	if escapeChar != 0 {
		matched = matchLikeSimple(leftStr, patternStr, escapeChar)
	} else {
		matched = matchLikeSimple(leftStr, patternStr)
	}

	// Handle NOT LIKE
	if expr.Not {
		return !matched, nil
	}
	return matched, nil
}

func evaluateIsNull(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.IsNullExpr, args []interface{}) (interface{}, error) {
	val, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	isNull := val == nil
	if expr.Not {
		return !isNull, nil
	}
	return isNull, nil
}

func matchLikeSimple(s, pattern string, escapeChar ...byte) bool {
	if pattern == "" {
		return s == ""
	}

	var esc byte
	if len(escapeChar) > 0 {
		esc = escapeChar[0]
	}

	// Convert both strings to lower case for case-insensitive matching
	s = strings.ToLower(s)
	pattern = strings.ToLower(pattern)
	if esc >= 'A' && esc <= 'Z' {
		esc = esc + 32 // lowercase the escape char too
	}

	sIdx := 0
	pIdx := 0

	for sIdx < len(s) && pIdx < len(pattern) {
		char := pattern[pIdx]

		// Handle escape character
		if esc != 0 && char == esc && pIdx+1 < len(pattern) {
			pIdx++ // skip escape char
			// Next char is literal
			if sIdx < len(s) && s[sIdx] == pattern[pIdx] {
				sIdx++
				pIdx++
				continue
			}
			return false
		}

		// Handle %
		if char == '%' {
			// Skip consecutive %
			for pIdx < len(pattern) && pattern[pIdx] == '%' {
				pIdx++
			}
			if pIdx >= len(pattern) {
				return true
			}
			// Try matching remaining pattern at each position
			for sIdx <= len(s) {
				if matchLikeSimple(s[sIdx:], pattern[pIdx:], escapeChar...) {
					return true
				}
				if sIdx >= len(s) {
					break
				}
				sIdx++
			}
			return false
		}

		// Handle _
		if char == '_' {
			sIdx++
			pIdx++
			continue
		}

		// Literal match
		if sIdx < len(s) && s[sIdx] == char {
			sIdx++
			pIdx++
			continue
		}

		return false
	}

	// Skip any trailing % in pattern
	for pIdx < len(pattern) && pattern[pIdx] == '%' {
		pIdx++
	}

	return sIdx == len(s) && pIdx == len(pattern)
}

func evaluateIn(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.InExpr, args []interface{}) (interface{}, error) {
	left, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	// SQL three-valued logic: if left is NULL, IN/NOT IN returns NULL (unknown)
	if left == nil {
		return nil, nil
	}

	// Handle subquery: IN (SELECT ...)
	if expr.Subquery != nil {
		subq := resolveOuterRefsInQuery(expr.Subquery, row, columns)
		_, subqueryRows, err := c.selectLocked(subq, args)
		if err != nil {
			return false, err
		}
		found := false
		hasNull := false
		for _, subRow := range subqueryRows {
			if len(subRow) > 0 {
				if subRow[0] == nil {
					hasNull = true
				} else if compareValues(left, subRow[0]) == 0 {
					found = true
					break
				}
			}
		}
		if found {
			if expr.Not {
				return false, nil
			}
			return true, nil
		}
		// SQL three-valued logic: NOT IN with NULLs in list and no match → NULL (unknown)
		if hasNull {
			return nil, nil
		}
		if expr.Not {
			return true, nil
		}
		return false, nil
	}

	// Evaluate all values in the list
	var listValues []interface{}
	for _, item := range expr.List {
		val, err := evaluateExpression(c, row, columns, item, args)
		if err != nil {
			return false, err
		}
		listValues = append(listValues, val)
	}

	// Check if left is in list (with three-valued NULL logic)
	found := false
	hasNull := false
	for _, v := range listValues {
		if v == nil {
			hasNull = true
		} else if compareValues(left, v) == 0 {
			found = true
			break
		}
	}

	if found {
		if expr.Not {
			return false, nil
		}
		return true, nil
	}
	// SQL three-valued logic: IN/NOT IN with NULLs in list and no match → NULL (unknown)
	if hasNull {
		return nil, nil
	}
	if expr.Not {
		return true, nil
	}
	return false, nil
}

func evaluateCastExpr(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.CastExpr, args []interface{}) (interface{}, error) {
	val, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	switch expr.DataType {
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
		return fmt.Sprintf("%v", val), nil
	case query.TokenBoolean:
		if b, ok := val.(bool); ok {
			return b, nil
		}
		if f, ok := toFloat64(val); ok {
			return f != 0, nil
		}
		if s, ok := val.(string); ok {
			return strings.ToLower(s) == "true" || s == "1", nil
		}
		return false, nil
	}
	return val, nil
}

func evaluateBetween(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.BetweenExpr, args []interface{}) (interface{}, error) {
	exprVal, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	lowerVal, err := evaluateExpression(c, row, columns, expr.Lower, args)
	if err != nil {
		return false, err
	}

	upperVal, err := evaluateExpression(c, row, columns, expr.Upper, args)
	if err != nil {
		return false, err
	}

	// Handle NULL - SQL three-valued logic: NULL in BETWEEN → NULL (unknown)
	if exprVal == nil || lowerVal == nil || upperVal == nil {
		return nil, nil
	}

	// Check: lower <= expr <= upper
	lowCmp := compareValues(exprVal, lowerVal)
	highCmp := compareValues(exprVal, upperVal)

	result := lowCmp >= 0 && highCmp <= 0

	// Handle NOT BETWEEN
	if expr.Not {
		return !result, nil
	}
	return result, nil
}

func evaluateJSONFunction(funcName string, args []interface{}) (interface{}, error) {
	switch funcName {
	case "JSON_EXTRACT":
		if len(args) < 2 {
			return nil, fmt.Errorf("JSON_EXTRACT requires 2 arguments")
		}
		// Handle various argument types
		var jsonData string
		var path string

		// First arg - could be string or other
		switch v := args[0].(type) {
		case string:
			jsonData = v
		default:
			if args[0] != nil {
				jsonData = fmt.Sprintf("%v", args[0])
			}
		}

		// Second arg - could be string or other
		switch v := args[1].(type) {
		case string:
			path = v
		default:
			if args[1] != nil {
				path = fmt.Sprintf("%v", args[1])
			}
		}

		return JSONExtract(jsonData, path)

	case "JSON_SET":
		if len(args) < 3 {
			return nil, fmt.Errorf("JSON_SET requires 3 arguments")
		}
		jsonData, _ := args[0].(string)
		path, _ := args[1].(string)
		value, _ := args[2].(string)
		// Convert value to JSON string if it's not already
		if value != "" && args[2] != nil {
			// The value should already be a string representation
		}
		return JSONSet(jsonData, path, value)

	case "JSON_REMOVE":
		if len(args) < 2 {
			return nil, fmt.Errorf("JSON_REMOVE requires 2 arguments")
		}
		jsonData, _ := args[0].(string)
		path, _ := args[1].(string)
		return JSONRemove(jsonData, path)

	case "JSON_VALID":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_VALID requires 1 argument")
		}
		if args[0] == nil {
			return false, nil
		}
		str, ok := args[0].(string)
		if !ok {
			return false, nil
		}
		return IsValidJSON(str), nil

	case "JSON_ARRAY_LENGTH":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_ARRAY_LENGTH requires 1 argument")
		}
		if args[0] == nil {
			return 0, nil
		}
		jsonData, ok := args[0].(string)
		if !ok {
			return 0, nil
		}
		length, err := JSONArrayLength(jsonData)
		if err != nil {
			return nil, err
		}
		return float64(length), nil

	case "JSON_TYPE":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_TYPE requires at least 1 argument")
		}
		if args[0] == nil {
			return "null", nil
		}
		jsonData, ok := args[0].(string)
		if !ok {
			return "unknown", nil
		}
		var path string
		if len(args) > 1 {
			path, _ = args[1].(string)
		}
		return JSONType(jsonData, path)

	case "JSON_KEYS":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_KEYS requires at least 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		jsonData, ok := args[0].(string)
		if !ok {
			return nil, nil
		}
		return JSONKeys(jsonData)

	case "JSON_PRETTY":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_PRETTY requires 1 argument")
		}
		if args[0] == nil {
			return "", nil
		}
		jsonData, ok := args[0].(string)
		if !ok {
			return nil, nil
		}
		return JSONPretty(jsonData)

	case "JSON_MINIFY":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_MINIFY requires 1 argument")
		}
		if args[0] == nil {
			return "", nil
		}
		jsonData, ok := args[0].(string)
		if !ok {
			return nil, nil
		}
		return JSONMinify(jsonData)

	case "JSON_MERGE":
		if len(args) < 2 {
			return nil, fmt.Errorf("JSON_MERGE requires 2 arguments")
		}
		json1, _ := args[0].(string)
		json2, _ := args[1].(string)
		return JSONMerge(json1, json2)

	case "JSON_QUOTE":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_QUOTE requires 1 argument")
		}
		if args[0] == nil {
			return "null", nil
		}
		str, ok := args[0].(string)
		if !ok {
			return nil, nil
		}
		return JSONQuote(str), nil

	case "JSON_UNQUOTE":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_UNQUOTE requires 1 argument")
		}
		if args[0] == nil {
			return "", nil
		}
		str, ok := args[0].(string)
		if !ok {
			return nil, nil
		}
		return JSONUnquote(str)

	case "REGEXP_MATCH":
		if len(args) < 2 {
			return nil, fmt.Errorf("REGEXP_MATCH requires 2 arguments")
		}
		str, _ := args[0].(string)
		pattern, _ := args[1].(string)
		if str == "" || pattern == "" {
			return false, nil
		}
		return RegexMatch(str, pattern)

	case "REGEXP_REPLACE":
		if len(args) < 3 {
			return nil, fmt.Errorf("REGEXP_REPLACE requires 3 arguments")
		}
		str, _ := args[0].(string)
		pattern, _ := args[1].(string)
		replacement, _ := args[2].(string)
		if str == "" || pattern == "" {
			return str, nil
		}
		return RegexReplace(str, pattern, replacement)

	case "REGEXP_EXTRACT":
		if len(args) < 2 {
			return nil, fmt.Errorf("REGEXP_EXTRACT requires 2 arguments")
		}
		str, _ := args[0].(string)
		pattern, _ := args[1].(string)
		if str == "" || pattern == "" {
			return []string{}, nil
		}
		return RegexExtract(str, pattern)

	default:
		return nil, fmt.Errorf("unknown function: %s", funcName)
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
		t.columnIndices[strings.ToLower(col.Name)] = i
	}
}

func (t *TableDef) GetColumnIndex(name string) int {
	if t.columnIndices == nil {
		t.buildColumnIndexCache()
	}
	if idx, ok := t.columnIndices[strings.ToLower(name)]; ok {
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
func (c *Catalog) getTableTreesForScan(table *TableDef) ([]*btree.BTree, error) {
	if table.Partition == nil {
		tree, exists := c.tableTrees[table.Name]
		if !exists {
			return nil, ErrTableNotFound
		}
		return []*btree.BTree{tree}, nil
	}

	// Partitioned table - collect all partition trees
	treeNames := table.getPartitionTreeNames()
	trees := make([]*btree.BTree, 0, len(treeNames))
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
		return "S:" + fmt.Sprintf("%v", v)
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
		// NULL propagation: NULL op anything = NULL (except AND/OR)
		if left == nil || right == nil {
			switch e.Operator {
			case query.TokenAnd:
				if left == nil && right == nil {
					return nil, nil
				}
				if left == nil {
					if toBool(right) {
						return nil, nil // NULL AND true = NULL
					}
					return false, nil // NULL AND false = false
				}
				if toBool(left) {
					return nil, nil // true AND NULL = NULL
				}
				return false, nil // false AND NULL = false
			case query.TokenOr:
				if left == nil && right == nil {
					return nil, nil
				}
				if left == nil {
					if toBool(right) {
						return true, nil // NULL OR true = true
					}
					return nil, nil // NULL OR false = NULL
				}
				if toBool(left) {
					return true, nil // true OR NULL = true
				}
				return nil, nil // false OR NULL = NULL
			case query.TokenConcat:
				// Concat with NULL returns NULL in standard SQL
				return nil, nil
			default:
				return nil, nil
			}
		}
		// Handle arithmetic in value expressions
		lf, lok := toFloat64(left)
		rf, rok := toFloat64(right)
		if lok && rok {
			bothInt := isIntegerType(left) && isIntegerType(right)
			switch e.Operator {
			case query.TokenPlus:
				if bothInt {
					return int64(lf) + int64(rf), nil
				}
				return lf + rf, nil
			case query.TokenMinus:
				if bothInt {
					return int64(lf) - int64(rf), nil
				}
				return lf - rf, nil
			case query.TokenStar:
				if bothInt {
					return int64(lf) * int64(rf), nil
				}
				return lf * rf, nil
			case query.TokenSlash:
				if rf != 0 {
					return lf / rf, nil
				}
				return nil, fmt.Errorf("division by zero")
			case query.TokenPercent:
				if rf != 0 {
					return int64(lf) % int64(rf), nil
				}
				return nil, fmt.Errorf("division by zero")
			}
		}
		// Comparison operators
		switch e.Operator {
		case query.TokenEq:
			return compareValues(left, right) == 0, nil
		case query.TokenNeq:
			return compareValues(left, right) != 0, nil
		case query.TokenLt:
			return compareValues(left, right) < 0, nil
		case query.TokenGt:
			return compareValues(left, right) > 0, nil
		case query.TokenLte:
			return compareValues(left, right) <= 0, nil
		case query.TokenGte:
			return compareValues(left, right) >= 0, nil
		case query.TokenAnd:
			return toBool(left) && toBool(right), nil
		case query.TokenOr:
			return toBool(left) || toBool(right), nil
		case query.TokenConcat:
			return fmt.Sprintf("%v%v", left, right), nil
		}
		return nil, fmt.Errorf("unsupported binary operator in value expression")
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
			return fmt.Sprintf("%v", val), nil
		}
		return val, nil
	case *query.FunctionCall:
		// Evaluate function arguments
		evalArgs := make([]interface{}, len(e.Args))
		for i, arg := range e.Args {
			val, err := EvalExpression(arg, args)
			if err != nil {
				return nil, err
			}
			evalArgs[i] = val
		}
		funcName := strings.ToUpper(e.Name)
		switch funcName {
		case "COALESCE":
			for _, a := range evalArgs {
				if a != nil {
					return a, nil
				}
			}
			return nil, nil
		case "NULLIF":
			if len(evalArgs) == 2 && compareValues(evalArgs[0], evalArgs[1]) == 0 {
				return nil, nil
			}
			if len(evalArgs) >= 1 {
				return evalArgs[0], nil
			}
			return nil, nil
		case "IIF":
			if len(evalArgs) == 3 {
				if toBool(evalArgs[0]) {
					return evalArgs[1], nil
				}
				return evalArgs[2], nil
			}
			return nil, nil
		case "ABS":
			if len(evalArgs) == 1 {
				if f, ok := toFloat64(evalArgs[0]); ok {
					if f < 0 {
						return -f, nil
					}
					return f, nil
				}
			}
			return nil, nil
		case "UPPER":
			if len(evalArgs) == 1 && evalArgs[0] != nil {
				return strings.ToUpper(fmt.Sprintf("%v", evalArgs[0])), nil
			}
			return nil, nil
		case "LOWER":
			if len(evalArgs) == 1 && evalArgs[0] != nil {
				return strings.ToLower(fmt.Sprintf("%v", evalArgs[0])), nil
			}
			return nil, nil
		case "LENGTH":
			if len(evalArgs) == 1 && evalArgs[0] != nil {
				return len(fmt.Sprintf("%v", evalArgs[0])), nil
			}
			return nil, nil
		case "CONCAT":
			var sb strings.Builder
			for _, a := range evalArgs {
				if a != nil {
					sb.WriteString(fmt.Sprintf("%v", a))
					if sb.Len() > maxStringResultLen {
						return nil, fmt.Errorf("CONCAT result exceeds maximum length")
					}
				}
			}
			return sb.String(), nil
		case "IFNULL":
			if len(evalArgs) >= 2 {
				if evalArgs[0] != nil {
					return evalArgs[0], nil
				}
				return evalArgs[1], nil
			}
			return nil, nil
		case "TRIM":
			if len(evalArgs) >= 1 && evalArgs[0] != nil {
				return strings.TrimSpace(fmt.Sprintf("%v", evalArgs[0])), nil
			}
			return nil, nil
		case "LTRIM":
			if len(evalArgs) >= 1 && evalArgs[0] != nil {
				return strings.TrimLeft(fmt.Sprintf("%v", evalArgs[0]), " \t\n\r"), nil
			}
			return nil, nil
		case "RTRIM":
			if len(evalArgs) >= 1 && evalArgs[0] != nil {
				return strings.TrimRight(fmt.Sprintf("%v", evalArgs[0]), " \t\n\r"), nil
			}
			return nil, nil
		case "SUBSTR", "SUBSTRING":
			if len(evalArgs) < 2 {
				return nil, nil
			}
			if evalArgs[0] == nil || evalArgs[1] == nil {
				return nil, nil
			}
			str := fmt.Sprintf("%v", evalArgs[0])
			start, _ := toFloat64(evalArgs[1])
			startInt := int(start) - 1
			if startInt < 0 {
				startInt = 0
			}
			if startInt >= len(str) {
				return "", nil
			}
			if len(evalArgs) >= 3 && evalArgs[2] != nil {
				length, _ := toFloat64(evalArgs[2])
				lengthInt := int(length)
				if lengthInt < 0 {
					return "", nil
				}
				if startInt+lengthInt > len(str) {
					lengthInt = len(str) - startInt
				}
				return str[startInt : startInt+lengthInt], nil
			}
			return str[startInt:], nil
		case "REPLACE":
			if len(evalArgs) < 3 || evalArgs[0] == nil || evalArgs[1] == nil || evalArgs[2] == nil {
				return nil, nil
			}
			str := fmt.Sprintf("%v", evalArgs[0])
			old := fmt.Sprintf("%v", evalArgs[1])
			if old == "" {
				return str, nil
			}
			newStr := fmt.Sprintf("%v", evalArgs[2])
			result := strings.ReplaceAll(str, old, newStr)
			if len(result) > maxStringResultLen {
				return nil, fmt.Errorf("REPLACE result exceeds maximum length")
			}
			return result, nil
		case "INSTR":
			if len(evalArgs) >= 2 && evalArgs[0] != nil && evalArgs[1] != nil {
				str := fmt.Sprintf("%v", evalArgs[0])
				substr := fmt.Sprintf("%v", evalArgs[1])
				idx := strings.Index(str, substr)
				if idx < 0 {
					return int64(0), nil
				}
				return int64(idx + 1), nil // 1-based
			}
			return nil, nil
		case "ROUND":
			if len(evalArgs) >= 1 && evalArgs[0] != nil {
				f, ok := toFloat64(evalArgs[0])
				if !ok {
					return nil, nil
				}
				decimals := 0
				if len(evalArgs) >= 2 {
					d, _ := toFloat64(evalArgs[1])
					decimals = int(d)
				}
				pow := math.Pow(10, float64(decimals))
				return math.Round(f*pow) / pow, nil
			}
			return nil, nil
		case "FLOOR":
			if len(evalArgs) >= 1 && evalArgs[0] != nil {
				if f, ok := toFloat64(evalArgs[0]); ok {
					return math.Floor(f), nil
				}
			}
			return nil, nil
		case "CEIL", "CEILING":
			if len(evalArgs) >= 1 && evalArgs[0] != nil {
				if f, ok := toFloat64(evalArgs[0]); ok {
					return math.Ceil(f), nil
				}
			}
			return nil, nil
		case "TYPEOF":
			if len(evalArgs) < 1 || evalArgs[0] == nil {
				return "null", nil
			}
			switch evalArgs[0].(type) {
			case int, int64:
				return "integer", nil
			case float64:
				f := evalArgs[0].(float64)
				if f == float64(int64(f)) {
					return "integer", nil
				}
				return "real", nil
			case string:
				return "text", nil
			case bool:
				return "integer", nil
			default:
				return "text", nil
			}
		case "MIN":
			if len(evalArgs) >= 2 {
				min := evalArgs[0]
				for _, a := range evalArgs[1:] {
					if a != nil && (min == nil || compareValues(a, min) < 0) {
						min = a
					}
				}
				return min, nil
			}
			if len(evalArgs) == 1 {
				return evalArgs[0], nil
			}
			return nil, nil
		case "MAX":
			if len(evalArgs) >= 2 {
				max := evalArgs[0]
				for _, a := range evalArgs[1:] {
					if a != nil && (max == nil || compareValues(a, max) > 0) {
						max = a
					}
				}
				return max, nil
			}
			if len(evalArgs) == 1 {
				return evalArgs[0], nil
			}
			return nil, nil
		case "REVERSE":
			if len(evalArgs) >= 1 && evalArgs[0] != nil {
				str := fmt.Sprintf("%v", evalArgs[0])
				runes := []rune(str)
				for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
					runes[i], runes[j] = runes[j], runes[i]
				}
				return string(runes), nil
			}
			return nil, nil
		case "REPEAT":
			if len(evalArgs) >= 2 && evalArgs[0] != nil && evalArgs[1] != nil {
				str := fmt.Sprintf("%v", evalArgs[0])
				n, _ := toFloat64(evalArgs[1])
				if int(n) <= 0 {
					return "", nil
				}
				if int(n)*len(str) > maxStringResultLen {
					return nil, fmt.Errorf("REPEAT result exceeds maximum length")
				}
				return strings.Repeat(str, int(n)), nil
			}
			return nil, nil
		default:
			return nil, fmt.Errorf("unsupported function in value expression: %s", funcName)
		}
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
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
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
	expr = strings.TrimSpace(strings.ToLower(expr))

	// Handle negative offset (past)
	if strings.HasPrefix(expr, "-") {
		expr = strings.TrimSpace(expr[1:])

		// Try to extract number and unit
		var num int
		var unit string
		fmt.Sscanf(expr, "%d %s", &num, &unit)

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
		fmt.Sscanf(expr, "%d %s", &num, &unit)

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

// applyOffsetLimit applies OFFSET and LIMIT to a result set
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
