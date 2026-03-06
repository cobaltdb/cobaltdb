package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
type TableDef struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"` // "table" or "collection"
	Columns     []ColumnDef     `json:"columns"`
	PrimaryKey  string          `json:"primary_key"`
	CreatedAt   int64           `json:"created_at"`
	RootPageID  uint32          `json:"root_page_id"`
	ForeignKeys []ForeignKeyDef `json:"foreign_keys,omitempty"`
	AutoIncSeq  int64           `json:"auto_inc_seq"` // Per-table auto-increment counter
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
	Type          string           `json:"type"` // INTEGER, TEXT, REAL, BLOB, JSON, BOOLEAN
	NotNull       bool             `json:"not_null"`
	Unique        bool             `json:"unique"`
	PrimaryKey    bool             `json:"primary_key"`
	AutoIncrement bool             `json:"auto_increment"`
	Default       string           `json:"default,omitempty"`
	CheckStr      string           `json:"check_str,omitempty"` // CHECK expression as SQL text (persisted)
	Check         query.Expression `json:"-"`                   // Parsed CHECK expression (not persisted)
	defaultExpr   query.Expression `json:"-"`                   // Parsed DEFAULT expression (not persisted)
	sourceTbl     string           `json:"-"`                   // Source table name for JOIN column disambiguation
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
	name          string
	tableName     string // table name for JOINs
	index         int
	isAggregate   bool
	aggregateType string // COUNT, SUM, AVG, MIN, MAX
	aggregateCol  string // column name for SUM, AVG, MIN, MAX
	aggregateExpr query.Expression // full expression for SUM(expr), AVG(expr), etc.
	isDistinct    bool             // for COUNT(DISTINCT col)
	isWindow      bool             // true for window functions
	windowExpr    *query.WindowExpr // window function expression
	hasEmbeddedAgg bool             // true when expression (CASE, etc.) contains aggregate calls
	originalExpr   query.Expression // the original expression for hasEmbeddedAgg columns
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
	Name      string `json:"name"`
	TableName string `json:"table_name"`
	Column    string `json:"column"`          // JSON column name
	Path      string `json:"path"`            // JSON path expression (e.g., "$.name")
	DataType  string `json:"data_type"`       // indexed data type: "string", "number", "boolean"
	Index     map[string][]int64 `json:"index"` // value -> list of row IDs (for string values)
	NumIndex  map[float64][]int64 `json:"num_index,omitempty"` // for numeric values
}

// undoAction represents the type of undo operation
type undoAction int

const (
	undoInsert      undoAction = iota // Undo an INSERT by deleting the key
	undoUpdate                        // Undo an UPDATE by restoring the old value
	undoDelete                        // Undo a DELETE by restoring the key/value
	undoCreateTable                   // Undo a CREATE TABLE by dropping the table
	undoDropTable                     // Undo a DROP TABLE by restoring the table
	undoCreateIndex                   // Undo a CREATE INDEX by dropping the index
	undoDropIndex                     // Undo a DROP INDEX by restoring the index
	undoAlterAddColumn                // Undo ALTER TABLE ADD COLUMN
	undoAlterDropColumn               // Undo ALTER TABLE DROP COLUMN
	undoAlterRename                   // Undo ALTER TABLE RENAME
	undoAlterRenameColumn             // Undo ALTER TABLE RENAME COLUMN
	undoAutoIncSeq                    // Undo AutoIncSeq change
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
	tableDef      *TableDef                     // For undoDropTable: original table definition
	tableTree     *btree.BTree                  // For undoDropTable: original table B-tree
	tableIndexes  map[string]*IndexDef          // For undoDropTable: indexes
	tableIdxTrees map[string]*btree.BTree       // For undoDropTable: index B-trees
	indexDef      *IndexDef                     // For undoDropIndex: original index definition
	indexTree     *btree.BTree                  // For undoDropIndex: original index B-tree
	indexName     string                        // For undoCreateIndex: index name to drop
	// ALTER TABLE undo fields
	oldColumns    []ColumnDef                  // For undoAlterAddColumn/undoAlterDropColumn: original columns
	oldPrimaryKey string                       // For undoAlterRenameColumn: original PK name
	oldName       string                       // For undoAlterRename/undoAlterRenameColumn: original name
	newName       string                       // For undoAlterRename/undoAlterRenameColumn: new name
	oldRowData    []struct{ key, val []byte }  // For undoAlterDropColumn: original row data
	droppedIndexes map[string]*IndexDef        // For undoAlterDropColumn: dropped indexes
	droppedIdxTrees map[string]*btree.BTree    // For undoAlterDropColumn: dropped index trees
	oldAutoIncSeq int64                        // For undoAutoIncSeq: previous AutoIncSeq value
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
	stats             map[string]*StatsTableStats           // Table statistics for ANALYZE
	cteResults        map[string]*cteResultSet              // Temporary CTE result cache for recursive CTEs
	keyCounter        int64                                 // For generating unique keys
	txnID             uint64                                // Current transaction ID
	txnActive         bool                                  // Is a transaction active
	undoLog           []undoEntry                           // Undo log for transaction rollback
	savepoints        []savepointEntry                     // Stack of savepoints
	rlsManager        *security.Manager                     // Row-level security manager
	enableRLS         bool                                  // Enable row-level security
	rlsPolicies       map[string]*security.Policy           // RLS policies: key = "table:policyName"
}

// savepointEntry records a named savepoint with its undo log position
type savepointEntry struct {
	name     string
	undoPos  int // Position in undoLog at time of savepoint creation
}

// cteResultSet holds pre-computed results for recursive CTEs
type cteResultSet struct {
	columns []string
	rows    [][]interface{}
}

// New creates a new catalog
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
		stats:             make(map[string]*StatsTableStats),
		rlsPolicies:       make(map[string]*security.Policy),
		keyCounter:        0,
	}
}

// EnableRLS enables row-level security and initializes the RLS manager
func (c *Catalog) EnableRLS() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enableRLS = true
	c.rlsManager = security.NewManager()
}

// GetRLSManager returns the RLS manager
func (c *Catalog) GetRLSManager() *security.Manager {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rlsManager
}

// IsRLSEnabled checks if RLS is enabled
func (c *Catalog) IsRLSEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.enableRLS
}

// CreateRLSPolicy creates a row-level security policy
func (c *Catalog) CreateRLSPolicy(policy *security.Policy) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.enableRLS {
		return errors.New("row-level security is not enabled")
	}

	return c.rlsManager.CreatePolicy(policy)
}

// DropRLSPolicy drops a row-level security policy
func (c *Catalog) DropRLSPolicy(tableName, policyName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.enableRLS {
		return errors.New("row-level security is not enabled")
	}

	return c.rlsManager.DropPolicy(tableName, policyName)
}

// SetWAL sets the WAL for the catalog
func (c *Catalog) SetWAL(wal *storage.WAL) {
	c.wal = wal
}

// BeginTransaction begins a new transaction
func (c *Catalog) BeginTransaction(txnID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txnID = txnID
	c.txnActive = true
	c.undoLog = nil        // Clear undo log for new transaction
	c.savepoints = nil     // Clear savepoints
}

// CommitTransaction commits the current transaction
func (c *Catalog) CommitTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.wal != nil && c.txnActive {
		// Write commit record to WAL
		record := &storage.WALRecord{
			TxnID: c.txnID,
			Type:  storage.WALCommit,
		}
		if err := c.wal.Append(record); err != nil {
			return err
		}
	}
	c.txnActive = false
	c.undoLog = nil    // Discard undo log on successful commit
	c.savepoints = nil // Clear savepoints
	return nil
}

// FlushTableTrees flushes all table B+Trees to disk
func (c *Catalog) FlushTableTrees() error {
	for tableName, tree := range c.tableTrees {
		if err := tree.Flush(); err != nil {
			return fmt.Errorf("failed to flush table %s: %w", tableName, err)
		}
	}
	return nil
}

// RollbackTransaction rolls back the current transaction
func (c *Catalog) RollbackTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.wal != nil && c.txnActive {
		// Write rollback record to WAL
		record := &storage.WALRecord{
			TxnID: c.txnID,
			Type:  storage.WALRollback,
		}
		if err := c.wal.Append(record); err != nil {
			return err
		}
	}

	// Replay undo log in reverse to restore pre-transaction state
	var rollbackErr error
	for i := len(c.undoLog) - 1; i >= 0; i-- {
		entry := c.undoLog[i]
		tree := c.tableTrees[entry.tableName] // May be nil for DDL undo entries
		switch entry.action {
		case undoInsert:
			// Undo an INSERT: delete the key that was inserted
			if tree != nil {
				if err := tree.Delete(entry.key); err != nil {
					rollbackErr = fmt.Errorf("rollback failed undoing insert: %w", err)
				}
			}
		case undoUpdate:
			// Undo an UPDATE: restore the old value
			if tree != nil {
				if err := tree.Put(entry.key, entry.oldValue); err != nil {
					rollbackErr = fmt.Errorf("rollback failed undoing update: %w", err)
				}
			}
		case undoDelete:
			// Undo a DELETE: restore the deleted key/value
			if tree != nil {
				if err := tree.Put(entry.key, entry.oldValue); err != nil {
					rollbackErr = fmt.Errorf("rollback failed undoing delete: %w", err)
				}
			}
		case undoCreateTable:
			// Undo a CREATE TABLE: remove the table
			delete(c.tables, entry.tableName)
			delete(c.tableTrees, entry.tableName)
			delete(c.stats, entry.tableName)
			// Remove indexes created for this table
			for idxName, idxDef := range c.indexes {
				if idxDef.TableName == entry.tableName {
					delete(c.indexes, idxName)
					delete(c.indexTrees, idxName)
				}
			}
			// Remove from catalog tree
			if c.tree != nil {
				if err := c.tree.Delete([]byte("tbl:" + entry.tableName)); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback failed removing table %s from catalog: %w", entry.tableName, err)
					}
				}
			}
		case undoDropTable:
			// Undo a DROP TABLE: restore the table
			if entry.tableDef != nil {
				c.tables[entry.tableName] = entry.tableDef
				entry.tableDef.buildColumnIndexCache()
			}
			if entry.tableTree != nil {
				c.tableTrees[entry.tableName] = entry.tableTree
			}
			for idxName, idxDef := range entry.tableIndexes {
				c.indexes[idxName] = idxDef
			}
			for idxName, idxTree := range entry.tableIdxTrees {
				c.indexTrees[idxName] = idxTree
			}
			// Restore catalog tree entry
			if entry.tableDef != nil {
				if err := c.storeTableDef(entry.tableDef); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback failed restoring table def %s: %w", entry.tableName, err)
					}
				}
			}
		case undoCreateIndex:
			// Undo a CREATE INDEX: drop the index
			delete(c.indexes, entry.indexName)
			delete(c.indexTrees, entry.indexName)
			if c.tree != nil {
				if err := c.tree.Delete([]byte("idx:" + entry.indexName)); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback failed dropping index %s: %w", entry.indexName, err)
					}
				}
			}
		case undoDropIndex:
			// Undo a DROP INDEX: restore the index
			if entry.indexDef != nil {
				c.indexes[entry.indexName] = entry.indexDef
			}
			if entry.indexTree != nil {
				c.indexTrees[entry.indexName] = entry.indexTree
			}
			if entry.indexDef != nil {
				if err := c.storeIndexDef(entry.indexDef); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback failed restoring index def %s: %w", entry.indexName, err)
					}
				}
			}
		case undoAutoIncSeq:
			// Undo AutoIncSeq change: restore old value
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.AutoIncSeq = entry.oldAutoIncSeq
			}
		case undoAlterAddColumn:
			// Undo ALTER TABLE ADD COLUMN: restore original columns
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.Columns = entry.oldColumns
				tbl.buildColumnIndexCache()
				_ = c.storeTableDef(tbl)
			}
		case undoAlterDropColumn:
			// Undo ALTER TABLE DROP COLUMN: restore original columns and row data
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.Columns = entry.oldColumns
				tbl.buildColumnIndexCache()
				// Restore original row data
				if tree, treeExists := c.tableTrees[entry.tableName]; treeExists {
					for _, rd := range entry.oldRowData {
						if err := tree.Put(rd.key, rd.val); err != nil {
							if rollbackErr == nil {
								rollbackErr = fmt.Errorf("rollback failed restoring row data for %s: %w", entry.tableName, err)
							}
						}
					}
				}
				// Restore dropped indexes
				for idxName, idxDef := range entry.droppedIndexes {
					c.indexes[idxName] = idxDef
				}
				for idxName, idxTree := range entry.droppedIdxTrees {
					c.indexTrees[idxName] = idxTree
				}
				if err := c.storeTableDef(tbl); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback failed storing table def %s: %w", entry.tableName, err)
					}
				}
			}
		case undoAlterRename:
			// Undo ALTER TABLE RENAME: swap names back
			if tbl, exists := c.tables[entry.newName]; exists {
				delete(c.tables, entry.newName)
				c.tables[entry.oldName] = tbl
				if tree, treeExists := c.tableTrees[entry.newName]; treeExists {
					delete(c.tableTrees, entry.newName)
					c.tableTrees[entry.oldName] = tree
				}
				// Restore index table references
				for _, idxDef := range c.indexes {
					if idxDef.TableName == entry.newName {
						idxDef.TableName = entry.oldName
					}
				}
				// Restore stats
				if stats, sExists := c.stats[entry.newName]; sExists {
					delete(c.stats, entry.newName)
					c.stats[entry.oldName] = stats
				}
			}
		case undoAlterRenameColumn:
			// Undo ALTER TABLE RENAME COLUMN: restore old column name
			if tbl, exists := c.tables[entry.tableName]; exists {
				for i, col := range tbl.Columns {
					if strings.EqualFold(col.Name, entry.newName) {
						tbl.Columns[i].Name = entry.oldName
						break
					}
				}
				// Restore PK reference if changed
				if strings.EqualFold(tbl.PrimaryKey, entry.newName) {
					tbl.PrimaryKey = entry.oldPrimaryKey
				}
				tbl.buildColumnIndexCache()
				// Restore index column references
				for _, idxDef := range c.indexes {
					if idxDef.TableName == entry.tableName {
						for i, idxCol := range idxDef.Columns {
							if strings.EqualFold(idxCol, entry.newName) {
								idxDef.Columns[i] = entry.oldName
							}
						}
					}
				}
				_ = c.storeTableDef(tbl)
			}
		}

		// Reverse index changes in reverse order
		for j := len(entry.indexChanges) - 1; j >= 0; j-- {
			idxChange := entry.indexChanges[j]
			idxTree, exists := c.indexTrees[idxChange.indexName]
			if !exists {
				continue
			}
			if idxChange.wasAdded {
				// Index key was added -> undo by deleting it
				if err := idxTree.Delete(idxChange.key); err != nil {
					rollbackErr = fmt.Errorf("rollback failed undoing index add: %w", err)
				}
			} else {
				// Index key was deleted -> undo by restoring it
				if err := idxTree.Put(idxChange.key, idxChange.oldValue); err != nil {
					rollbackErr = fmt.Errorf("rollback failed undoing index delete: %w", err)
				}
			}
		}
	}
	if rollbackErr != nil {
		c.undoLog = nil
		c.txnActive = false
		c.savepoints = nil
		return rollbackErr
	}

	c.undoLog = nil
	c.txnActive = false
	c.savepoints = nil
	return nil
}

// IsTransactionActive returns true if a transaction is active
func (c *Catalog) IsTransactionActive() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.txnActive
}

// Savepoint creates a named savepoint at the current position in the undo log
func (c *Catalog) Savepoint(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.txnActive {
		return fmt.Errorf("SAVEPOINT can only be used within a transaction")
	}
	c.savepoints = append(c.savepoints, savepointEntry{
		name:    name,
		undoPos: len(c.undoLog),
	})
	return nil
}

// RollbackToSavepoint rolls back to the named savepoint, undoing changes after it
func (c *Catalog) RollbackToSavepoint(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.txnActive {
		return fmt.Errorf("ROLLBACK TO SAVEPOINT can only be used within a transaction")
	}

	// Find the savepoint
	spIdx := -1
	for i := len(c.savepoints) - 1; i >= 0; i-- {
		if strings.EqualFold(c.savepoints[i].name, name) {
			spIdx = i
			break
		}
	}
	if spIdx < 0 {
		return fmt.Errorf("savepoint '%s' does not exist", name)
	}

	undoPos := c.savepoints[spIdx].undoPos

	// Replay undo entries from the end back to the savepoint position
	var rollbackErr error
	for i := len(c.undoLog) - 1; i >= undoPos; i-- {
		entry := c.undoLog[i]
		tree := c.tableTrees[entry.tableName]
		switch entry.action {
		case undoInsert:
			if tree != nil {
				if err := tree.Delete(entry.key); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed undoing insert: %w", err)
					}
				}
			}
		case undoUpdate:
			if tree != nil {
				if err := tree.Put(entry.key, entry.oldValue); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed undoing update: %w", err)
					}
				}
			}
		case undoDelete:
			if tree != nil {
				if err := tree.Put(entry.key, entry.oldValue); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed undoing delete: %w", err)
					}
				}
			}
		case undoCreateTable:
			delete(c.tables, entry.tableName)
			delete(c.tableTrees, entry.tableName)
			delete(c.stats, entry.tableName)
			for idxName, idxDef := range c.indexes {
				if idxDef.TableName == entry.tableName {
					delete(c.indexes, idxName)
					delete(c.indexTrees, idxName)
				}
			}
			if c.tree != nil {
				if err := c.tree.Delete([]byte("tbl:" + entry.tableName)); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed removing table %s from catalog: %w", entry.tableName, err)
					}
				}
			}
		case undoDropTable:
			if entry.tableDef != nil {
				c.tables[entry.tableName] = entry.tableDef
				entry.tableDef.buildColumnIndexCache()
			}
			if entry.tableTree != nil {
				c.tableTrees[entry.tableName] = entry.tableTree
			}
			for idxName, idxDef := range entry.tableIndexes {
				c.indexes[idxName] = idxDef
			}
			for idxName, idxTree := range entry.tableIdxTrees {
				c.indexTrees[idxName] = idxTree
			}
			if entry.tableDef != nil {
				if err := c.storeTableDef(entry.tableDef); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed restoring table def %s: %w", entry.tableName, err)
					}
				}
			}
		case undoCreateIndex:
			delete(c.indexes, entry.indexName)
			delete(c.indexTrees, entry.indexName)
			if c.tree != nil {
				if err := c.tree.Delete([]byte("idx:" + entry.indexName)); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed dropping index %s: %w", entry.indexName, err)
					}
				}
			}
		case undoDropIndex:
			if entry.indexDef != nil {
				c.indexes[entry.indexName] = entry.indexDef
			}
			if entry.indexTree != nil {
				c.indexTrees[entry.indexName] = entry.indexTree
			}
		case undoAutoIncSeq:
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.AutoIncSeq = entry.oldAutoIncSeq
			}
		case undoAlterAddColumn:
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.Columns = entry.oldColumns
				tbl.buildColumnIndexCache()
				if err := c.storeTableDef(tbl); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed storing table def %s: %w", entry.tableName, err)
					}
				}
			}
		case undoAlterDropColumn:
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.Columns = entry.oldColumns
				tbl.buildColumnIndexCache()
				if t, e := c.tableTrees[entry.tableName]; e {
					for _, rd := range entry.oldRowData {
						if err := t.Put(rd.key, rd.val); err != nil {
							if rollbackErr == nil {
								rollbackErr = fmt.Errorf("rollback to savepoint failed restoring row data for %s: %w", entry.tableName, err)
							}
						}
					}
				}
				for idxName, idxDef := range entry.droppedIndexes {
					c.indexes[idxName] = idxDef
				}
				for idxName, idxTree := range entry.droppedIdxTrees {
					c.indexTrees[idxName] = idxTree
				}
				if err := c.storeTableDef(tbl); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed storing table def %s: %w", entry.tableName, err)
					}
				}
			}
		}

		// Reverse index changes
		for j := len(entry.indexChanges) - 1; j >= 0; j-- {
			idxChange := entry.indexChanges[j]
			idxTree, exists := c.indexTrees[idxChange.indexName]
			if !exists {
				continue
			}
			if idxChange.wasAdded {
				if err := idxTree.Delete(idxChange.key); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed deleting from index %s: %w", idxChange.indexName, err)
					}
				}
			} else {
				if err := idxTree.Put(idxChange.key, idxChange.oldValue); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed putting to index %s: %w", idxChange.indexName, err)
					}
				}
			}
		}
	}

	// Truncate undo log to savepoint position
	c.undoLog = c.undoLog[:undoPos]
	// Remove savepoints after this one (but keep the current savepoint)
	c.savepoints = c.savepoints[:spIdx+1]
	return rollbackErr
}

// ReleaseSavepoint releases a named savepoint (removes it but keeps changes)
func (c *Catalog) ReleaseSavepoint(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.txnActive {
		return fmt.Errorf("RELEASE SAVEPOINT can only be used within a transaction")
	}

	// Find and remove the savepoint
	for i := len(c.savepoints) - 1; i >= 0; i-- {
		if strings.EqualFold(c.savepoints[i].name, name) {
			// Remove this savepoint and all savepoints created after it
			c.savepoints = c.savepoints[:i]
			return nil
		}
	}
	return fmt.Errorf("savepoint '%s' does not exist", name)
}

// TxnID returns the current transaction ID
func (c *Catalog) TxnID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.txnID
}

// CreateTable creates a new table
func (c *Catalog) CreateTable(stmt *query.CreateTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tables[stmt.Table]; exists {
		if stmt.IfNotExists {
			return nil // Table already exists, silently succeed
		}
		return ErrTableExists
	}

	// Create new B+Tree for the table's data
	tree, err := btree.NewBTree(c.pool)
	if err != nil {
		return err
	}

	tableDef := &TableDef{
		Name:        stmt.Table,
		Type:        "table",
		Columns:     make([]ColumnDef, len(stmt.Columns)),
		CreatedAt:   time.Now().UnixNano(),
		RootPageID:  tree.RootPageID(),
		ForeignKeys: make([]ForeignKeyDef, len(stmt.ForeignKeys)),
	}

	for i, col := range stmt.Columns {
		tableDef.Columns[i] = ColumnDef{
			Name:          col.Name,
			Type:          tokenTypeToColumnType(col.Type),
			NotNull:       col.NotNull,
			Unique:        col.Unique,
			PrimaryKey:    col.PrimaryKey,
			AutoIncrement: col.AutoIncrement,
			Default:       exprToSQL(col.Default),
			CheckStr:      exprToSQL(col.Check),
			Check:         col.Check,
			defaultExpr:   col.Default,
		}
		if col.PrimaryKey {
			tableDef.PrimaryKey = col.Name
			tableDef.Columns[i].NotNull = true // PRIMARY KEY implies NOT NULL
		}
	}

	// Copy foreign key definitions
	for i, fk := range stmt.ForeignKeys {
		tableDef.ForeignKeys[i] = ForeignKeyDef{
			Columns:           fk.Columns,
			ReferencedTable:   fk.ReferencedTable,
			ReferencedColumns: fk.ReferencedColumns,
			OnDelete:          fk.OnDelete,
			OnUpdate:          fk.OnUpdate,
		}
	}

	c.tables[stmt.Table] = tableDef
	c.tableTrees[stmt.Table] = tree // Store the tree for data operations

	// Build column index cache for performance
	tableDef.buildColumnIndexCache()

	// Record DDL undo entry for transaction rollback
	if c.txnActive {
		c.undoLog = append(c.undoLog, undoEntry{
			action:    undoCreateTable,
			tableName: stmt.Table,
		})
	}

	// Store table definition in catalog tree
	return c.storeTableDef(tableDef)
}

// storeTableDef stores a table definition in the catalog tree
func (c *Catalog) storeTableDef(table *TableDef) error {
	key := []byte("tbl:" + table.Name)
	data, err := json.Marshal(table)
	if err != nil {
		return err
	}

	if c.tree != nil {
		return c.tree.Put(key, data)
	}
	return nil
}

// DropTable drops a table
func (c *Catalog) DropTable(stmt *query.DropTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !stmt.IfExists {
		if _, exists := c.tables[stmt.Table]; !exists {
			return ErrTableNotFound
		}
	}

	// Check if table actually exists before trying to delete
	tableDef, exists := c.tables[stmt.Table]

	// Record DDL undo entry for transaction rollback before deleting
	if c.txnActive && exists {
		entry := undoEntry{
			action:        undoDropTable,
			tableName:     stmt.Table,
			tableDef:      tableDef,
			tableTree:     c.tableTrees[stmt.Table],
			tableIndexes:  make(map[string]*IndexDef),
			tableIdxTrees: make(map[string]*btree.BTree),
		}
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName == stmt.Table {
				entry.tableIndexes[idxName] = idxDef
				if tree, ok := c.indexTrees[idxName]; ok {
					entry.tableIdxTrees[idxName] = tree
				}
			}
		}
		c.undoLog = append(c.undoLog, entry)
	}

	// Clean up table data B-tree
	delete(c.tableTrees, stmt.Table)

	// Clean up indexes associated with this table
	if tableDef != nil {
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName == stmt.Table {
				delete(c.indexes, idxName)
				delete(c.indexTrees, idxName)
			}
		}
	}

	// Clean up views that reference this table (triggers, FTS indexes, stats)
	delete(c.stats, stmt.Table)
	delete(c.tables, stmt.Table)

	// Remove from catalog tree only if the table existed
	if c.tree != nil && exists {
		key := []byte("tbl:" + stmt.Table)
		return c.tree.Delete(key)
	}
	return nil
}

// AlterTableAddColumn adds a new column to an existing table
func (c *Catalog) AlterTableAddColumn(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, exists := c.tables[stmt.Table]
	if !exists {
		return ErrTableNotFound
	}

	// Check if column already exists
	for _, col := range table.Columns {
		if col.Name == stmt.Column.Name {
			return fmt.Errorf("column %s already exists in table %s", stmt.Column.Name, stmt.Table)
		}
	}

	// Save undo entry before modification
	if c.txnActive {
		oldCols := make([]ColumnDef, len(table.Columns))
		copy(oldCols, table.Columns)
		c.undoLog = append(c.undoLog, undoEntry{
			action:     undoAlterAddColumn,
			tableName:  stmt.Table,
			oldColumns: oldCols,
		})
	}

	newCol := ColumnDef{
		Name:          stmt.Column.Name,
		Type:          tokenTypeToColumnType(stmt.Column.Type),
		NotNull:       stmt.Column.NotNull,
		Unique:        stmt.Column.Unique,
		PrimaryKey:    stmt.Column.PrimaryKey,
		AutoIncrement: stmt.Column.AutoIncrement,
		Default:       exprToSQL(stmt.Column.Default),
		CheckStr:      exprToSQL(stmt.Column.Check),
		Check:         stmt.Column.Check,
		defaultExpr:   stmt.Column.Default,
	}

	table.Columns = append(table.Columns, newCol)
	table.buildColumnIndexCache()

	// Backfill existing rows with the default value for the new column
	tree, treeExists := c.tableTrees[stmt.Table]
	if treeExists {
		// Compute default value
		var defaultVal interface{}
		if newCol.defaultExpr != nil {
			defaultVal, _ = evaluateExpression(c, nil, nil, newCol.defaultExpr, nil)
		}

		// Scan all rows and append the default value
		iter, _ := tree.Scan(nil, nil)
		defer iter.Close()
		type rowUpdate struct {
			key  []byte
			data []byte
		}
		var updates []rowUpdate
		for iter.HasNext() {
			key, valueData, err := iter.Next()
			if err != nil {
				break
			}
			var values []interface{}
			if err := json.Unmarshal(valueData, &values); err != nil {
				continue
			}
			// Only update rows that are missing the new column
			if len(values) < len(table.Columns) {
				for len(values) < len(table.Columns) {
					values = append(values, defaultVal)
				}
				newData, err := json.Marshal(values)
				if err != nil {
					continue
				}
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				updates = append(updates, rowUpdate{key: keyCopy, data: newData})
			}
		}

		// Apply updates
		for _, u := range updates {
			tree.Put(u.key, u.data)
		}
	}

	// Store updated table definition
	return c.storeTableDef(table)
}

// AlterTableDropColumn drops a column from a table
func (c *Catalog) AlterTableDropColumn(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, exists := c.tables[stmt.Table]
	if !exists {
		return ErrTableNotFound
	}

	colName := stmt.NewName // Column name to drop stored in NewName
	colIdx := -1
	for i, col := range table.Columns {
		if strings.EqualFold(col.Name, colName) {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return fmt.Errorf("column '%s' does not exist in table '%s'", colName, stmt.Table)
	}

	// Cannot drop primary key column
	if strings.EqualFold(table.Columns[colIdx].Name, table.PrimaryKey) {
		return fmt.Errorf("cannot drop PRIMARY KEY column '%s'", colName)
	}

	// Save undo entry before modification
	if c.txnActive {
		oldCols := make([]ColumnDef, len(table.Columns))
		copy(oldCols, table.Columns)
		entry := undoEntry{
			action:          undoAlterDropColumn,
			tableName:       stmt.Table,
			oldColumns:      oldCols,
			droppedIndexes:  make(map[string]*IndexDef),
			droppedIdxTrees: make(map[string]*btree.BTree),
		}
		// Save original row data before modification
		if tree, treeExists := c.tableTrees[stmt.Table]; treeExists {
			iter, _ := tree.Scan(nil, nil)
			defer iter.Close()
			for iter.HasNext() {
				key, val, err := iter.Next()
				if err != nil {
					break
				}
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				valCopy := make([]byte, len(val))
				copy(valCopy, val)
				entry.oldRowData = append(entry.oldRowData, struct{ key, val []byte }{keyCopy, valCopy})
			}
		}
		// Save indexes that will be dropped
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName == stmt.Table {
				for _, idxCol := range idxDef.Columns {
					if strings.EqualFold(idxCol, colName) {
						entry.droppedIndexes[idxName] = idxDef
						if idxTree, ok := c.indexTrees[idxName]; ok {
							entry.droppedIdxTrees[idxName] = idxTree
						}
						break
					}
				}
			}
		}
		c.undoLog = append(c.undoLog, entry)
	}

	// Remove column from definition
	table.Columns = append(table.Columns[:colIdx], table.Columns[colIdx+1:]...)
	table.buildColumnIndexCache()

	// Update all existing rows - remove the dropped column's data
	tree, exists := c.tableTrees[stmt.Table]
	if exists {
		var updates []struct {
			key []byte
			val []byte
		}
		iter, _ := tree.Scan(nil, nil)
		defer iter.Close()
		for iter.HasNext() {
			key, valueData, err := iter.Next()
			if err != nil {
				break
			}
			row, err := decodeRow(valueData, colIdx+1+len(table.Columns))
			if err != nil {
				continue
			}
			if colIdx < len(row) {
				row = append(row[:colIdx], row[colIdx+1:]...)
			}
			newData, err := json.Marshal(row)
			if err != nil {
				continue
			}
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			updates = append(updates, struct {
				key []byte
				val []byte
			}{keyCopy, newData})
		}
		for _, u := range updates {
			if err := tree.Put(u.key, u.val); err != nil {
				return fmt.Errorf("failed to update row after column drop: %w", err)
			}
		}
	}

	// Drop any indexes on the dropped column
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName == stmt.Table {
			for _, idxCol := range idxDef.Columns {
				if strings.EqualFold(idxCol, colName) {
					delete(c.indexes, idxName)
					delete(c.indexTrees, idxName)
					break
				}
			}
		}
	}

	return c.storeTableDef(table)
}

// AlterTableRename renames a table
func (c *Catalog) AlterTableRename(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, exists := c.tables[stmt.Table]
	if !exists {
		return ErrTableNotFound
	}

	if _, exists := c.tables[stmt.NewName]; exists {
		return fmt.Errorf("table '%s' already exists", stmt.NewName)
	}

	// Save undo entry before modification
	if c.txnActive {
		c.undoLog = append(c.undoLog, undoEntry{
			action:    undoAlterRename,
			tableName: stmt.Table,
			oldName:   stmt.Table,
			newName:   stmt.NewName,
		})
	}

	// Update table name in all maps
	delete(c.tables, stmt.Table)
	c.tables[stmt.NewName] = table

	if tree, exists := c.tableTrees[stmt.Table]; exists {
		delete(c.tableTrees, stmt.Table)
		c.tableTrees[stmt.NewName] = tree
	}

	// Update index references
	for _, idxDef := range c.indexes {
		if idxDef.TableName == stmt.Table {
			idxDef.TableName = stmt.NewName
		}
	}

	// Update stats
	if stats, exists := c.stats[stmt.Table]; exists {
		delete(c.stats, stmt.Table)
		c.stats[stmt.NewName] = stats
	}

	return nil
}

// AlterTableRenameColumn renames a column in a table
func (c *Catalog) AlterTableRenameColumn(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, exists := c.tables[stmt.Table]
	if !exists {
		return ErrTableNotFound
	}

	found := false
	for i, col := range table.Columns {
		if strings.EqualFold(col.Name, stmt.OldName) {
			// Save undo entry before modification
			if c.txnActive {
				c.undoLog = append(c.undoLog, undoEntry{
					action:        undoAlterRenameColumn,
					tableName:     stmt.Table,
					oldName:       stmt.OldName,
					newName:       stmt.NewName,
					oldPrimaryKey: table.PrimaryKey,
				})
			}
			table.Columns[i].Name = stmt.NewName
			found = true
			// Update primary key reference if needed
			if strings.EqualFold(table.PrimaryKey, stmt.OldName) {
				table.PrimaryKey = stmt.NewName
			}
			break
		}
	}
	if !found {
		return fmt.Errorf("column '%s' does not exist in table '%s'", stmt.OldName, stmt.Table)
	}

	table.buildColumnIndexCache()

	// Update index column references
	for _, idxDef := range c.indexes {
		if idxDef.TableName == stmt.Table {
			for i, idxCol := range idxDef.Columns {
				if strings.EqualFold(idxCol, stmt.OldName) {
					idxDef.Columns[i] = stmt.NewName
				}
			}
		}
	}

	return c.storeTableDef(table)
}

// GetTable retrieves a table definition
func (c *Catalog) GetTable(name string) (*TableDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTableLocked(name)
}

// getTableLocked is the internal version without locking (caller must hold lock)
func (c *Catalog) getTableLocked(name string) (*TableDef, error) {
	table, exists := c.tables[name]
	if !exists {
		return nil, ErrTableNotFound
	}
	return table, nil
}

// CreateView creates a new view
func (c *Catalog) CreateView(name string, query *query.SelectStmt) error {
	if _, exists := c.views[name]; exists {
		return ErrTableExists
	}
	if _, exists := c.tables[name]; exists {
		return ErrTableExists
	}
	c.views[name] = query
	return nil
}

// GetView retrieves a view definition
func (c *Catalog) GetView(name string) (*query.SelectStmt, error) {
	view, exists := c.views[name]
	if !exists {
		return nil, ErrTableNotFound
	}
	return view, nil
}

// DropView drops a view
func (c *Catalog) DropView(name string) error {
	if _, exists := c.views[name]; !exists {
		return ErrTableNotFound
	}
	delete(c.views, name)
	return nil
}

// HasTableOrView checks if a table or view exists
func (c *Catalog) HasTableOrView(name string) bool {
	_, tableExists := c.tables[name]
	_, viewExists := c.views[name]
	return tableExists || viewExists
}

// CreateTrigger creates a new trigger
func (c *Catalog) CreateTrigger(stmt *query.CreateTriggerStmt) error {
	// Check if table exists
	if _, err := c.getTableLocked(stmt.Table); err != nil {
		return err
	}

	if _, exists := c.triggers[stmt.Name]; exists {
		return fmt.Errorf("trigger %s already exists", stmt.Name)
	}
	c.triggers[stmt.Name] = stmt
	return nil
}

// GetTrigger retrieves a trigger definition
func (c *Catalog) GetTrigger(name string) (*query.CreateTriggerStmt, error) {
	trigger, exists := c.triggers[name]
	if !exists {
		return nil, fmt.Errorf("trigger %s not found", name)
	}
	return trigger, nil
}

// DropTrigger drops a trigger
func (c *Catalog) DropTrigger(name string) error {
	if _, exists := c.triggers[name]; !exists {
		return fmt.Errorf("trigger %s not found", name)
	}
	delete(c.triggers, name)
	return nil
}

// GetTriggersForTable retrieves all triggers for a table
func (c *Catalog) GetTriggersForTable(tableName string, event string) []*query.CreateTriggerStmt {
	var result []*query.CreateTriggerStmt
	for _, trigger := range c.triggers {
		if trigger.Table == tableName && (event == "" || trigger.Event == event) {
			result = append(result, trigger)
		}
	}
	return result
}

// executeTriggers executes all triggers for a given table and event.
// newRow is used for INSERT/UPDATE triggers (NEW.col), oldRow for DELETE/UPDATE triggers (OLD.col).
func (c *Catalog) executeTriggers(tableName string, event string, timing string, newRow []interface{}, oldRow []interface{}, columns []ColumnDef) error {
	triggers := c.GetTriggersForTable(tableName, event)
	for _, trigger := range triggers {
		if trigger.Time != timing {
			continue
		}
		if len(trigger.Body) == 0 {
			continue
		}

		// Evaluate WHEN condition if present
		if trigger.Condition != nil {
			resolvedCond := c.resolveTriggerExpr(trigger.Condition, newRow, oldRow, columns)
			result, err := evaluateExpression(c, nil, nil, resolvedCond, nil)
			if err != nil {
				continue // Condition evaluation error - skip trigger
			}
			if result == nil {
				continue // NULL condition - skip trigger
			}
			if b, ok := result.(bool); ok && !b {
				continue // false condition - skip trigger
			}
			// For numeric results, 0 = false
			if f, ok := toFloat64(result); ok && f == 0 {
				continue
			}
		}

		// Execute each statement in the trigger body
		for _, bodyStmt := range trigger.Body {
			// Substitute NEW.col and OLD.col references with actual values
			resolved := c.resolveTriggerRefs(bodyStmt, newRow, oldRow, columns)
			// Execute the resolved statement
			if err := c.executeTriggerStatement(resolved); err != nil {
				return fmt.Errorf("trigger %s: %w", trigger.Name, err)
			}
		}
	}
	return nil
}

// executeTriggerStatement executes a single statement from a trigger body
func (c *Catalog) executeTriggerStatement(stmt query.Statement) error {
	switch s := stmt.(type) {
	case *query.InsertStmt:
		_, _, err := c.insertLocked(s, nil)
		return err
	case *query.UpdateStmt:
		_, _, err := c.updateLocked(s, nil)
		return err
	case *query.DeleteStmt:
		_, _, err := c.deleteLocked(s, nil)
		return err
	default:
		return fmt.Errorf("unsupported trigger statement type: %T", stmt)
	}
}

// resolveTriggerRefs replaces NEW.col and OLD.col QualifiedIdentifier references
// in a statement with their actual literal values.
func (c *Catalog) resolveTriggerRefs(stmt query.Statement, newRow []interface{}, oldRow []interface{}, columns []ColumnDef) query.Statement {
	switch s := stmt.(type) {
	case *query.InsertStmt:
		resolved := *s
		resolved.Values = make([][]query.Expression, len(s.Values))
		for i, row := range s.Values {
			resolved.Values[i] = make([]query.Expression, len(row))
			for j, expr := range row {
				resolved.Values[i][j] = c.resolveTriggerExpr(expr, newRow, oldRow, columns)
			}
		}
		return &resolved
	case *query.UpdateStmt:
		resolved := *s
		resolved.Set = make([]*query.SetClause, len(s.Set))
		for i, sc := range s.Set {
			newSc := *sc
			newSc.Value = c.resolveTriggerExpr(sc.Value, newRow, oldRow, columns)
			resolved.Set[i] = &newSc
		}
		if s.Where != nil {
			resolved.Where = c.resolveTriggerExpr(s.Where, newRow, oldRow, columns)
		}
		return &resolved
	case *query.DeleteStmt:
		resolved := *s
		if s.Where != nil {
			resolved.Where = c.resolveTriggerExpr(s.Where, newRow, oldRow, columns)
		}
		return &resolved
	}
	return stmt
}

// resolveTriggerExpr replaces NEW.col and OLD.col references in an expression
func (c *Catalog) resolveTriggerExpr(expr query.Expression, newRow []interface{}, oldRow []interface{}, columns []ColumnDef) query.Expression {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *query.QualifiedIdentifier:
		tbl := strings.ToUpper(e.Table)
		if tbl == "NEW" && newRow != nil {
			for i, col := range columns {
				if strings.EqualFold(col.Name, e.Column) && i < len(newRow) {
					return valueToLiteral(newRow[i])
				}
			}
		} else if tbl == "OLD" && oldRow != nil {
			for i, col := range columns {
				if strings.EqualFold(col.Name, e.Column) && i < len(oldRow) {
					return valueToLiteral(oldRow[i])
				}
			}
		}
		return e
	case *query.BinaryExpr:
		return &query.BinaryExpr{
			Left:     c.resolveTriggerExpr(e.Left, newRow, oldRow, columns),
			Operator: e.Operator,
			Right:    c.resolveTriggerExpr(e.Right, newRow, oldRow, columns),
		}
	case *query.UnaryExpr:
		return &query.UnaryExpr{
			Operator: e.Operator,
			Expr:     c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
		}
	case *query.FunctionCall:
		newArgs := make([]query.Expression, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = c.resolveTriggerExpr(arg, newRow, oldRow, columns)
		}
		return &query.FunctionCall{Name: e.Name, Args: newArgs, Distinct: e.Distinct}
	case *query.CaseExpr:
		newCase := &query.CaseExpr{}
		if e.Expr != nil {
			newCase.Expr = c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns)
		}
		newCase.Whens = make([]*query.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			newCase.Whens[i] = &query.WhenClause{
				Condition: c.resolveTriggerExpr(w.Condition, newRow, oldRow, columns),
				Result:    c.resolveTriggerExpr(w.Result, newRow, oldRow, columns),
			}
		}
		if e.Else != nil {
			newCase.Else = c.resolveTriggerExpr(e.Else, newRow, oldRow, columns)
		}
		return newCase
	case *query.BetweenExpr:
		return &query.BetweenExpr{
			Expr:  c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
			Lower: c.resolveTriggerExpr(e.Lower, newRow, oldRow, columns),
			Upper: c.resolveTriggerExpr(e.Upper, newRow, oldRow, columns),
			Not:   e.Not,
		}
	case *query.InExpr:
		newList := make([]query.Expression, len(e.List))
		for i, v := range e.List {
			newList[i] = c.resolveTriggerExpr(v, newRow, oldRow, columns)
		}
		return &query.InExpr{
			Expr:     c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
			List:     newList,
			Not:      e.Not,
			Subquery: e.Subquery,
		}
	case *query.IsNullExpr:
		return &query.IsNullExpr{
			Expr: c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
			Not:  e.Not,
		}
	case *query.CastExpr:
		return &query.CastExpr{
			Expr:     c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
			DataType: e.DataType,
		}
	case *query.LikeExpr:
		return &query.LikeExpr{
			Expr:    c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
			Pattern: c.resolveTriggerExpr(e.Pattern, newRow, oldRow, columns),
			Not:     e.Not,
		}
	}
	return expr
}

// CreateProcedure creates a new stored procedure
func (c *Catalog) CreateProcedure(stmt *query.CreateProcedureStmt) error {
	if _, exists := c.procedures[stmt.Name]; exists {
		return fmt.Errorf("procedure %s already exists", stmt.Name)
	}
	c.procedures[stmt.Name] = stmt
	return nil
}

// GetProcedure retrieves a procedure definition
func (c *Catalog) GetProcedure(name string) (*query.CreateProcedureStmt, error) {
	proc, exists := c.procedures[name]
	if !exists {
		return nil, fmt.Errorf("procedure %s not found", name)
	}
	return proc, nil
}

// DropProcedure drops a procedure
func (c *Catalog) DropProcedure(name string) error {
	if _, exists := c.procedures[name]; !exists {
		return fmt.Errorf("procedure %s not found", name)
	}
	delete(c.procedures, name)
	return nil
}

// Insert inserts rows into a table
func (c *Catalog) Insert(stmt *query.InsertStmt, args []interface{}) (int64, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.insertLocked(stmt, args)
}

func (c *Catalog) insertLocked(stmt *query.InsertStmt, args []interface{}) (int64, int64, error) {
	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	tree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	// Determine column mapping
	// If columns are specified in INSERT, use them; otherwise use all table columns
	var insertColumns []string
	if len(stmt.Columns) > 0 {
		// Validate that all specified column names exist in the table
		for _, colName := range stmt.Columns {
			if table.GetColumnIndex(colName) < 0 {
				return 0, 0, fmt.Errorf("column '%s' does not exist in table '%s'", colName, stmt.Table)
			}
		}
		insertColumns = stmt.Columns
	} else {
		// Use all columns from table definition
		for _, col := range table.Columns {
			insertColumns = append(insertColumns, col.Name)
		}
	}

	// Insert each row
	rowsAffected := int64(0)
	autoIncValue := int64(0)

	// Pre-calculate insert column indices for performance
	insertColIndices := make([]int, len(insertColumns))
	for i, colName := range insertColumns {
		insertColIndices[i] = table.GetColumnIndex(colName)
	}

	// Handle INSERT...SELECT: execute SELECT and convert to value rows
	valueRows := stmt.Values
	if stmt.Select != nil {
		selectCols, selectRows, err := c.selectLocked(stmt.Select, args)
		if err != nil {
			return 0, 0, fmt.Errorf("INSERT...SELECT failed: %w", err)
		}
		// Validate column count matches
		if len(selectCols) != len(insertColumns) {
			return 0, 0, fmt.Errorf("INSERT...SELECT column count mismatch: INSERT has %d columns, SELECT returns %d columns", len(insertColumns), len(selectCols))
		}
		// Convert select results to expression rows
		valueRows = make([][]query.Expression, len(selectRows))
		for i, row := range selectRows {
			exprRow := make([]query.Expression, len(row))
			for j, val := range row {
				switch v := val.(type) {
				case nil:
					exprRow[j] = &query.NullLiteral{}
				case string:
					exprRow[j] = &query.StringLiteral{Value: v}
				case float64:
					exprRow[j] = &query.NumberLiteral{Value: v}
				case int64:
					exprRow[j] = &query.NumberLiteral{Value: float64(v), Raw: fmt.Sprintf("%d", v)}
				case int:
					exprRow[j] = &query.NumberLiteral{Value: float64(v), Raw: fmt.Sprintf("%d", v)}
				case bool:
					exprRow[j] = &query.BooleanLiteral{Value: v}
				default:
					exprRow[j] = &query.StringLiteral{Value: fmt.Sprintf("%v", v)}
				}
			}
			valueRows[i] = exprRow
		}
	}

	// Save AutoIncSeq before insert loop for rollback
	savedAutoIncSeq := table.AutoIncSeq
	if c.txnActive {
		c.undoLog = append(c.undoLog, undoEntry{
			action:        undoAutoIncSeq,
			tableName:     stmt.Table,
			oldAutoIncSeq: savedAutoIncSeq,
		})
	}

	// Track insertions for statement-level atomicity (undo on partial failure)
	type stmtInsert struct {
		key       []byte
		idxKeys   []struct{ idxName string; key []byte }
	}
	var stmtInserts []stmtInsert
	var insertedRows [][]interface{} // Track rows for trigger execution
	var insertErr error

	for _, valueRow := range valueRows {
		// Validate value count matches column count
		if len(valueRow) != len(insertColumns) {
			// Allow one fewer value if there is exactly one AUTO_INCREMENT column
			autoIncCount := 0
			for _, col := range table.Columns {
				if col.AutoIncrement {
					autoIncCount++
				}
			}
			if !(autoIncCount > 0 && len(valueRow) == len(insertColumns)-autoIncCount) {
				return 0, 0, fmt.Errorf("INSERT has %d columns but %d values", len(insertColumns), len(valueRow))
			}
		}

		// Generate unique key (use auto-increment if primary key exists)
		var key string
		hasPrimaryKey := false
		for i, colName := range insertColumns {
			if colName == table.PrimaryKey {
				hasPrimaryKey = true
				// Get primary key value from valueRow if provided
				if i < len(valueRow) {
					if numLit, ok := valueRow[i].(*query.NumberLiteral); ok {
						pkVal := int64(numLit.Value)
						key = fmt.Sprintf("%020d", pkVal)
						// Keep auto-inc counter ahead of explicit values
						if pkVal > table.AutoIncSeq {
							table.AutoIncSeq = pkVal
						}
					} else {
						// Non-numeric primary key (TEXT, etc.)
						val, err := evaluateExpression(c, nil, nil, valueRow[i], args)
						if err == nil && val != nil {
							if strVal, ok := val.(string); ok {
								key = "S:" + strVal // Prefix to distinguish from numeric keys
							} else if fVal, ok := toFloat64(val); ok {
								pkVal := int64(fVal)
								key = fmt.Sprintf("%020d", pkVal)
								if pkVal > table.AutoIncSeq {
									table.AutoIncSeq = pkVal
								}
							}
						}
					}
				}
			}
		}

		if !hasPrimaryKey || key == "" {
			// Generate auto-increment key (per-table counter)
			table.AutoIncSeq++
			autoIncValue = table.AutoIncSeq
			key = fmt.Sprintf("%020d", autoIncValue)
		}

		// Build full row with all columns
		rowValues := make([]interface{}, len(table.Columns))
		colSet := make([]bool, len(table.Columns)) // Track which columns were explicitly set

		// Map provided values to their columns using pre-calculated indices
		for colIdx, tableColIdx := range insertColIndices {
			if colIdx < len(valueRow) && tableColIdx >= 0 {
				val, err := evaluateExpression(c, nil, nil, valueRow[colIdx], args)
				if err != nil {
					rowValues[tableColIdx] = nil
				} else {
					rowValues[tableColIdx] = val
				}
				colSet[tableColIdx] = true // Mark this column as explicitly set
			}
		}

		// Fill remaining columns with defaults (only for columns not explicitly set)
		for i, col := range table.Columns {
			if !colSet[i] {
				// Auto-increment columns get the generated key value
				if col.AutoIncrement {
					rowValues[i] = float64(autoIncValue)
					continue
				}
				// Try to use DEFAULT expression first
				if col.defaultExpr != nil {
					defVal, err := EvalExpression(col.defaultExpr, args)
					if err == nil {
						rowValues[i] = defVal
						continue
					}
				}
				// SQL standard: omitted columns without DEFAULT get NULL
				rowValues[i] = nil
			}
		}

		// For INTEGER PRIMARY KEY columns, NULL means auto-increment
		for i, col := range table.Columns {
			if col.PrimaryKey && rowValues[i] == nil && autoIncValue > 0 {
				rowValues[i] = float64(autoIncValue)
			}
		}

		// Check NOT NULL constraints before inserting
		for i, col := range table.Columns {
			if col.NotNull && !col.AutoIncrement && rowValues[i] == nil {
				insertErr = fmt.Errorf("NOT NULL constraint failed: column '%s' cannot be null", col.Name)
				break
			}
		}
		if insertErr != nil {
			break
		}

		// Check UNIQUE constraints before inserting
		skipRow := false
		for i, col := range table.Columns {
			if col.Unique && rowValues[i] != nil {
				// Check if a row with this unique value already exists
				iter, _ := tree.Scan(nil, nil)
				var duplicateKey []byte
				for iter.HasNext() {
					k, existingData, err := iter.Next()
					if err != nil {
						break
					}
					var existingRow []interface{}
					if err := json.Unmarshal(existingData, &existingRow); err != nil {
						continue
					}
					if len(existingRow) > i && compareValues(rowValues[i], existingRow[i]) == 0 {
						duplicateKey = k
						break
					}
				}
				iter.Close()
				if duplicateKey != nil {
					if stmt.ConflictAction == query.ConflictIgnore {
						skipRow = true
						break
					} else if stmt.ConflictAction == query.ConflictReplace {
						// Clean up index entries for the row being replaced
						oldData, getErr := tree.Get(duplicateKey)
						if getErr == nil {
							oldRow, decErr := decodeRow(oldData, len(table.Columns))
							if decErr == nil {
								for idxName, idxTree := range c.indexTrees {
									idxDef := c.indexes[idxName]
									if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
										oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
										if ok {
											idxTree.Delete([]byte(oldIdxKey))
										}
									}
								}
							}
						}
						tree.Delete(duplicateKey)
					} else {
						insertErr = fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
						break
					}
				}
			}
		}
		if insertErr != nil {
			break
		}
		if skipRow {
			continue
		}

		// Check CHECK constraints before inserting
		for _, col := range table.Columns {
			if col.Check != nil {
				result, err := evaluateExpression(c, rowValues, table.Columns, col.Check, args)
				if err != nil {
					insertErr = fmt.Errorf("CHECK constraint failed: %v", err)
					break
				}
				// Per SQL standard, NULL (unknown) passes CHECK constraint; only explicit false fails
				if result != nil {
					if resultBool, ok := result.(bool); ok && !resultBool {
						insertErr = fmt.Errorf("CHECK constraint failed for column: %s", col.Name)
						break
					}
				}
			}
		}
		if insertErr != nil {
			break
		}

		// Check FOREIGN KEY constraints before inserting
		for _, fk := range table.ForeignKeys {
			// Get the value(s) for the foreign key column(s)
			for i, colName := range fk.Columns {
				colIdx := table.GetColumnIndex(colName)
				if colIdx < 0 || colIdx >= len(rowValues) {
					continue
				}
				fkValue := rowValues[colIdx]
				if fkValue == nil {
					continue // NULL values skip FK check
				}

				// Check if referenced row exists
				refTable, err := c.getTableLocked(fk.ReferencedTable)
				if err != nil {
					insertErr = fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
					break
				}

				refColIdx := 0
				if len(fk.ReferencedColumns) > i {
					refColIdx = refTable.GetColumnIndex(fk.ReferencedColumns[i])
				} else if len(refTable.Columns) > 0 {
					// Default to first column
					refColIdx = 0
				}

				refTree, exists := c.tableTrees[fk.ReferencedTable]
				if !exists {
					insertErr = fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
					break
				}

				// Search for matching row
				found := false
				refIter, _ := refTree.Scan(nil, nil)
				for refIter.HasNext() {
					_, refData, err := refIter.Next()
					if err != nil {
						break
					}
					var refRow []interface{}
					if err := json.Unmarshal(refData, &refRow); err != nil {
						continue
					}
					if refColIdx < len(refRow) && compareValues(fkValue, refRow[refColIdx]) == 0 {
						found = true
						break
					}
				}
				refIter.Close()

				if !found {
					insertErr = fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValue, fk.ReferencedTable)
					break
				}
			}
			if insertErr != nil {
				break
			}
		}
		if insertErr != nil {
			break
		}

		// Encode row
		valueData, err := json.Marshal(rowValues)
		if err != nil {
			insertErr = err
			break
		}

		// Log to WAL before applying change
		if c.wal != nil && c.txnActive {
			// For INSERT, we log the key and value
			// Format: key (null-terminated) + value
			walData := append([]byte(key), 0)
			walData = append(walData, valueData...)
			record := &storage.WALRecord{
				TxnID: c.txnID,
				Type:  storage.WALInsert,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				insertErr = err
				break
			}
		}

		// Enforce PRIMARY KEY uniqueness - check if key already exists
		if _, err := tree.Get([]byte(key)); err == nil {
			if stmt.ConflictAction == query.ConflictIgnore {
				continue // Skip this row
			} else if stmt.ConflictAction == query.ConflictReplace {
				// Clean up index entries for the row being replaced (PK conflict)
				oldData, getErr := tree.Get([]byte(key))
				if getErr == nil {
					oldRow, decErr := decodeRow(oldData, len(table.Columns))
					if decErr == nil {
						for idxName, idxTree := range c.indexTrees {
							idxDef := c.indexes[idxName]
							if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
								oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
								if ok {
									idxTree.Delete([]byte(oldIdxKey))
								}
							}
						}
					}
				}
				// Delete existing row before replacing
				tree.Delete([]byte(key))
			} else {
				insertErr = fmt.Errorf("UNIQUE constraint failed: duplicate primary key value")
				break
			}
		}

		// Store in B+Tree
		if err := tree.Put([]byte(key), valueData); err != nil {
			insertErr = fmt.Errorf("failed to store row: %w", err)
			break
		}

		// Update indexes and track changes for undo
		var idxChanges []indexUndoEntry
		for idxName, idxTree := range c.indexTrees {
			idxDef := c.indexes[idxName]
			if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
				indexKey, ok := buildCompositeIndexKey(table, idxDef, rowValues)
				if ok {
					// Enforce UNIQUE constraint
					if idxDef.Unique {
						if oldPKData, err := idxTree.Get([]byte(indexKey)); err == nil {
							if stmt.ConflictAction == query.ConflictIgnore {
								// Delete the already-stored row from the main table
								if err := tree.Delete([]byte(key)); err != nil {
									// Continue with conflict handling, but note the error
									_ = err
								}
								// Undo any index entries already added in this loop iteration
								for _, undo := range idxChanges {
									if undo.wasAdded {
										if idxTree2, ok := c.indexTrees[undo.indexName]; ok {
											if err := idxTree2.Delete(undo.key); err != nil {
												_ = err
											}
										}
									}
								}
								skipRow = true
								break
							} else if stmt.ConflictAction == query.ConflictReplace {
								// Delete the old row that conflicts on this unique index
								oldPK := string(oldPKData)
								if oldPK != key { // Only if it's a different row
									oldRowData, getErr := tree.Get([]byte(oldPK))
									if getErr == nil {
										oldRow, decErr := decodeRow(oldRowData, len(table.Columns))
										if decErr == nil {
											// Clean up all index entries for the old row
											for otherIdxName, otherIdxTree := range c.indexTrees {
												otherIdxDef := c.indexes[otherIdxName]
												if otherIdxDef.TableName == stmt.Table && len(otherIdxDef.Columns) > 0 {
													oldIdxKey, ok := buildCompositeIndexKey(table, otherIdxDef, oldRow)
													if ok {
														otherIdxTree.Delete([]byte(oldIdxKey))
													}
												}
											}
										}
									}
									tree.Delete([]byte(oldPK))
								}
							} else {
								insertErr = fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", indexKey, idxName)
								break
							}
						}
					}
					if err := idxTree.Put([]byte(indexKey), []byte(key)); err != nil {
						insertErr = fmt.Errorf("failed to update index %s: %w", idxName, err)
						break
					}
					if c.txnActive {
						idxChanges = append(idxChanges, indexUndoEntry{
							indexName: idxName,
							key:       []byte(indexKey),
							wasAdded:  true,
						})
					}
				}
			}
		}
		if skipRow {
			continue
		}
		if insertErr != nil {
			// Row was stored but index failed - delete the row
			if err := tree.Delete([]byte(key)); err != nil {
				// Best effort cleanup failed, continue with original error
				_ = err
			}
			break
		}

		// Record undo log entry for rollback (after applying change)
		if c.txnActive {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, []byte(key))
			c.undoLog = append(c.undoLog, undoEntry{
				action:       undoInsert,
				tableName:    stmt.Table,
				key:          keyCopy,
				indexChanges: idxChanges,
			})
		}

		// Track for statement-level atomicity
		si := stmtInsert{key: []byte(key)}
		for _, ic := range idxChanges {
			si.idxKeys = append(si.idxKeys, struct{ idxName string; key []byte }{ic.indexName, ic.key})
		}
		stmtInserts = append(stmtInserts, si)

		// Save row for trigger execution
		rowCopy := make([]interface{}, len(rowValues))
		copy(rowCopy, rowValues)
		insertedRows = append(insertedRows, rowCopy)

		rowsAffected++
	}

	// Statement-level atomicity: undo all inserts on error (outside explicit transactions)
	if insertErr != nil && !c.txnActive {
		for i := len(stmtInserts) - 1; i >= 0; i-- {
			si := stmtInserts[i]
			if err := tree.Delete(si.key); err != nil {
				// Best effort cleanup failed
				_ = err
			}
			for _, ik := range si.idxKeys {
				if idxTree, exists := c.indexTrees[ik.idxName]; exists {
					if err := idxTree.Delete(ik.key); err != nil {
						// Best effort cleanup failed
						_ = err
					}
				}
			}
		}
		table.AutoIncSeq = savedAutoIncSeq
		return 0, 0, insertErr
	}
	if insertErr != nil {
		// Inside explicit transaction - undo log handles cleanup on ROLLBACK
		// But remove the undo entries for this failed statement's successful rows
		// since the caller will see an error and may want statement-level atomicity
		// Undo the successful rows immediately for statement atomicity
		for i := len(stmtInserts) - 1; i >= 0; i-- {
			si := stmtInserts[i]
			_ = tree.Delete(si.key)
			for _, ik := range si.idxKeys {
				if idxTree, exists := c.indexTrees[ik.idxName]; exists {
					_ = idxTree.Delete(ik.key)
				}
			}
		}
		// Remove the undo log entries we added for this statement
		// (the AutoIncSeq entry + one per successful row)
		undoToRemove := 1 + len(stmtInserts) // 1 for AutoIncSeq + N for rows
		if len(c.undoLog) >= undoToRemove {
			c.undoLog = c.undoLog[:len(c.undoLog)-undoToRemove]
		}
		table.AutoIncSeq = savedAutoIncSeq
		return 0, 0, insertErr
	}

	// Execute AFTER INSERT triggers for each inserted row
	for _, insertedRow := range insertedRows {
		_ = c.executeTriggers(stmt.Table, "INSERT", "AFTER", insertedRow, nil, table.Columns)
	}

	return autoIncValue, rowsAffected, nil
}

// Update updates rows in a table
func (c *Catalog) Update(stmt *query.UpdateStmt, args []interface{}) (int64, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updateLocked(stmt, args)
}

func (c *Catalog) updateLocked(stmt *query.UpdateStmt, args []interface{}) (int64, int64, error) {
	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	tree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	rowsAffected := int64(0)
	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	// Collect entries to update (need old row for index cleanup)
	type updateEntry struct {
		key    []byte
		oldRow []interface{}
		newRow []interface{}
	}
	var entries []updateEntry

	// Pre-calculate column indices for SET clauses
	setColumnIndices := make([]int, len(stmt.Set))
	for i, setClause := range stmt.Set {
		setColumnIndices[i] = table.GetColumnIndex(setClause.Column)
		if setColumnIndices[i] < 0 {
			return 0, 0, fmt.Errorf("column '%s' not found in table '%s'", setClause.Column, stmt.Table)
		}
	}

	for iter.HasNext() {
		key, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode row
		row, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present
		if stmt.Where != nil {
			matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
			if err != nil {
				return 0, rowsAffected, fmt.Errorf("WHERE evaluation error: %w", err)
			}
			if !matched {
				continue // Skip row that doesn't match WHERE condition
			}
		}

		// Make a copy of the row to update
		updatedRow := make([]interface{}, len(row))
		copy(updatedRow, row)

		// Update fields - use pre-calculated column indices
		for i, setClause := range stmt.Set {
			colIdx := setColumnIndices[i]
			if colIdx >= 0 {
				newVal, err := evaluateExpression(c, row, table.Columns, setClause.Value, args)
				if err != nil {
					return 0, rowsAffected, fmt.Errorf("failed to evaluate SET expression for column '%s': %w", setClause.Column, err)
				}
				updatedRow[colIdx] = newVal
			}
		}

		// Make copies since iterator may reuse buffers
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)

		// Check UNIQUE constraints before updating
		for i, col := range table.Columns {
			if col.Unique && updatedRow[i] != nil {
				// Check if another row (not this one) has the same unique value
				checkIter, _ := tree.Scan(nil, nil)
				duplicate := false
				for checkIter.HasNext() {
					checkKey, existingData, err := checkIter.Next()
					if err != nil {
						break
					}
					// Skip the current row being updated
					if string(checkKey) == string(key) {
						continue
					}
					var existingRow []interface{}
					if err := json.Unmarshal(existingData, &existingRow); err != nil {
						continue
					}
					if len(existingRow) > i && compareValues(updatedRow[i], existingRow[i]) == 0 {
						duplicate = true
						break
					}
				}
				checkIter.Close()
				if duplicate {
					return 0, rowsAffected, fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
				}
			}
		}

		// Check UNIQUE INDEX constraints before updating
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName == stmt.Table && idxDef.Unique && len(idxDef.Columns) > 0 {
				newIdxKey, newOk := buildCompositeIndexKey(table, idxDef, updatedRow)
				if !newOk {
					continue
				}
				oldIdxKey, _ := buildCompositeIndexKey(table, idxDef, row)
				if newIdxKey == oldIdxKey {
					continue // Value unchanged, no conflict possible
				}
				if idxTree, exists := c.indexTrees[idxName]; exists {
					if _, err := idxTree.Get([]byte(newIdxKey)); err == nil {
						return 0, rowsAffected, fmt.Errorf("UNIQUE constraint failed: duplicate value in index %s", idxName)
					}
				}
			}
		}

		// Check NOT NULL constraints before updating
		for i, col := range table.Columns {
			if col.NotNull && i < len(updatedRow) && updatedRow[i] == nil {
				return 0, rowsAffected, fmt.Errorf("NOT NULL constraint failed: column '%s' cannot be null", col.Name)
			}
		}

		// Check CHECK constraints before updating
		for _, col := range table.Columns {
			if col.Check != nil {
				result, err := evaluateExpression(c, updatedRow, table.Columns, col.Check, args)
				if err != nil {
					return 0, rowsAffected, fmt.Errorf("CHECK constraint failed: %v", err)
				}
				// Per SQL standard, NULL (unknown) passes CHECK constraint; only explicit false fails
				if result != nil {
					if resultBool, ok := result.(bool); ok && !resultBool {
						return 0, rowsAffected, fmt.Errorf("CHECK constraint failed for column: %s", col.Name)
					}
				}
			}
		}

		// Check FOREIGN KEY constraints on updated columns
		for _, fk := range table.ForeignKeys {
			for i, colName := range fk.Columns {
				colIdx := table.GetColumnIndex(colName)
				if colIdx < 0 || colIdx >= len(updatedRow) {
					continue
				}
				fkValue := updatedRow[colIdx]
				if fkValue == nil {
					continue // NULL values skip FK check
				}
				// Only check if the FK column value actually changed
				if colIdx < len(row) && compareValues(fkValue, row[colIdx]) == 0 {
					continue // Value didn't change, skip check
				}
				// Check if referenced row exists
				refTable, err := c.getTableLocked(fk.ReferencedTable)
				if err != nil {
					return 0, rowsAffected, fmt.Errorf("FOREIGN KEY constraint failed: referenced table '%s' not found", fk.ReferencedTable)
				}
				refColIdx := 0
				if len(fk.ReferencedColumns) > i {
					refColIdx = refTable.GetColumnIndex(fk.ReferencedColumns[i])
				}
				refTree, exists := c.tableTrees[fk.ReferencedTable]
				if !exists {
					return 0, rowsAffected, fmt.Errorf("FOREIGN KEY constraint failed: referenced table '%s' not found", fk.ReferencedTable)
				}
				found := false
				refIter, _ := refTree.Scan(nil, nil)
				for refIter.HasNext() {
					_, refData, err := refIter.Next()
					if err != nil {
						break
					}
					var refRow []interface{}
					if err := json.Unmarshal(refData, &refRow); err != nil {
						continue
					}
					if refColIdx < len(refRow) && compareValues(fkValue, refRow[refColIdx]) == 0 {
						found = true
						break
					}
				}
				refIter.Close()
				if !found {
					return 0, rowsAffected, fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValue, fk.ReferencedTable)
				}
			}
		}

		entries = append(entries, updateEntry{
			key:    keyCopy,
			oldRow: row,
			newRow: updatedRow,
		})
		rowsAffected++
	}

	// Apply updates
	pkColIdx := table.GetColumnIndex(table.PrimaryKey)
	// Foreign key enforcer for CASCADE/RESTRICT/SET NULL actions on PK changes
	fke := NewForeignKeyEnforcer(c)
	for _, entry := range entries {
		oldKey := entry.key

		// Re-encode row
		newValueData, err := json.Marshal(entry.newRow)
		if err != nil {
			continue
		}

		// Check if PRIMARY KEY was changed - need to delete old key and insert new one
		newKey := oldKey
		pkChanged := false
		if pkColIdx >= 0 && pkColIdx < len(entry.newRow) && pkColIdx < len(entry.oldRow) {
			if compareValues(entry.oldRow[pkColIdx], entry.newRow[pkColIdx]) != 0 {
				pkChanged = true
				// Generate new key from the updated PK value
				pkVal := entry.newRow[pkColIdx]
				if strVal, ok := pkVal.(string); ok {
					newKey = []byte("S:" + strVal)
				} else if fVal, ok := toFloat64(pkVal); ok {
					newKey = []byte(fmt.Sprintf("%020d", int64(fVal)))
				}
				// Check if the new PK already exists (duplicate PK violation)
				if existingData, err := tree.Get(newKey); err == nil && existingData != nil {
					return 0, 0, fmt.Errorf("PRIMARY KEY constraint failed: duplicate key '%v'", pkVal)
				}
			}
		}

		// Enforce foreign key ON UPDATE actions (CASCADE, SET NULL, RESTRICT)
		if pkChanged && pkColIdx >= 0 {
			if fkErr := fke.OnUpdate(context.Background(), stmt.Table, entry.oldRow[pkColIdx], entry.newRow[pkColIdx]); fkErr != nil {
				return 0, 0, fmt.Errorf("foreign key constraint: %w", fkErr)
			}
		}

		// Log to WAL before applying change
		if c.wal != nil && c.txnActive {
			if pkChanged {
				// Log delete of old key
				deleteRecord := &storage.WALRecord{
					TxnID: c.txnID,
					Type:  storage.WALDelete,
					Data:  oldKey,
				}
				if err := c.wal.Append(deleteRecord); err != nil {
					return 0, rowsAffected, err
				}
				// Log insert of new key
				walData := append(newKey, 0)
				walData = append(walData, newValueData...)
				insertRecord := &storage.WALRecord{
					TxnID: c.txnID,
					Type:  storage.WALInsert,
					Data:  walData,
				}
				if err := c.wal.Append(insertRecord); err != nil {
					return 0, rowsAffected, err
				}
			} else {
				// For UPDATE without PK change, log the key and new value
				walData := append([]byte(oldKey), 0)
				walData = append(walData, newValueData...)
				record := &storage.WALRecord{
					TxnID: c.txnID,
					Type:  storage.WALUpdate,
					Data:  walData,
				}
				if err := c.wal.Append(record); err != nil {
					return 0, rowsAffected, err
				}
			}
		}

		if pkChanged {
			// Delete old key and insert new key
			if err := tree.Delete(oldKey); err != nil {
				// Continue with update, but note the error
				_ = err
			}
			if err := tree.Put(newKey, newValueData); err != nil {
				return 0, rowsAffected, fmt.Errorf("failed to update row with new key: %w", err)
			}
			// Update auto-increment counter if needed
			if fVal, ok := toFloat64(entry.newRow[pkColIdx]); ok {
				pkVal := int64(fVal)
				if pkVal > table.AutoIncSeq {
					table.AutoIncSeq = pkVal
				}
			}
		} else {
			if err := tree.Put(oldKey, newValueData); err != nil {
				return 0, rowsAffected, fmt.Errorf("failed to update row: %w", err)
			}
		}

		// Update indexes: remove old entries and add new ones, track for undo
		var idxChanges []indexUndoEntry
		for idxName, idxTree := range c.indexTrees {
			idxDef := c.indexes[idxName]
			if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
				// Remove old index entry
				oldIndexKey, oldOk := buildCompositeIndexKey(table, idxDef, entry.oldRow)
				if oldOk {
					oldIdxVal, getErr := idxTree.Get([]byte(oldIndexKey))
					_ = idxTree.Delete([]byte(oldIndexKey))
					if c.txnActive && getErr == nil {
						idxChanges = append(idxChanges, indexUndoEntry{
							indexName: idxName,
							key:       []byte(oldIndexKey),
							oldValue:  oldIdxVal,
							wasAdded:  false, // was deleted
						})
					}
				}
				// Add new index entry
				newIndexKey, newOk := buildCompositeIndexKey(table, idxDef, entry.newRow)
				if newOk {
					// Enforce UNIQUE constraint (skip if value unchanged)
					if idxDef.Unique && newIndexKey != oldIndexKey {
						if _, err := idxTree.Get([]byte(newIndexKey)); err == nil {
							return 0, rowsAffected, fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", newIndexKey, idxName)
						}
					}
					if err := idxTree.Put([]byte(newIndexKey), newKey); err != nil {
						return 0, rowsAffected, fmt.Errorf("failed to update index %s: %w", idxName, err)
					}
					if c.txnActive {
						idxChanges = append(idxChanges, indexUndoEntry{
							indexName: idxName,
							key:       []byte(newIndexKey),
							wasAdded:  true,
						})
					}
				}
			}
		}

		// Record undo log entry for rollback (after applying change)
		if c.txnActive {
			oldValueData, marshalErr := json.Marshal(entry.oldRow)
			if marshalErr == nil {
				keyCopy := make([]byte, len(oldKey))
				copy(keyCopy, oldKey)
				c.undoLog = append(c.undoLog, undoEntry{
					action:       undoUpdate,
					tableName:    stmt.Table,
					key:          keyCopy,
					oldValue:     oldValueData,
					indexChanges: idxChanges,
				})
			}
		}
	}

	// Execute AFTER UPDATE triggers (per-row)
	for _, entry := range entries {
		_ = c.executeTriggers(stmt.Table, "UPDATE", "AFTER", entry.newRow, entry.oldRow, table.Columns)
	}

	return 0, rowsAffected, nil
}

// Delete deletes rows from a table
func (c *Catalog) Delete(stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleteLocked(stmt, args)
}

func (c *Catalog) deleteLocked(stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	tree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	rowsAffected := int64(0)
	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	// Collect keys and row data to delete (need row data for index cleanup)
	type deleteEntry struct {
		key   []byte
		value []byte
	}
	var entries []deleteEntry
	for iter.HasNext() {
		key, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode row
		row, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present
		if stmt.Where != nil {
			matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
			if err != nil {
				return 0, rowsAffected, fmt.Errorf("WHERE evaluation error: %w", err)
			}
			if !matched {
				continue // Skip row that doesn't match WHERE condition
			}
		}

		// Make copies of key and value since iterator may reuse buffers
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		valueCopy := make([]byte, len(valueData))
		copy(valueCopy, valueData)

		entries = append(entries, deleteEntry{key: keyCopy, value: valueCopy})
		rowsAffected++
	}

	// Foreign key enforcer for CASCADE/RESTRICT actions
	fke := NewForeignKeyEnforcer(c)

	// Delete collected entries
	for _, entry := range entries {
		key := entry.key

		// Enforce foreign key ON DELETE actions (CASCADE, SET NULL, RESTRICT)
		// Extract primary key value from the row for FK lookup
		row, err := decodeRow(entry.value, len(table.Columns))
		if err == nil {
			pkColIdx := table.GetColumnIndex(table.PrimaryKey)
			if pkColIdx >= 0 && pkColIdx < len(row) && row[pkColIdx] != nil {
				if fkErr := fke.OnDelete(context.Background(), stmt.Table, row[pkColIdx]); fkErr != nil {
					return 0, 0, fmt.Errorf("foreign key constraint: %w", fkErr)
				}
			}
		}

		// Remove from indexes first (before deleting the row), track for undo
		var idxChanges []indexUndoEntry
		if row == nil {
			row, err = decodeRow(entry.value, len(table.Columns))
		}
		if err == nil {
			for idxName, idxTree := range c.indexTrees {
				idxDef := c.indexes[idxName]
				if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
					indexKey, ok := buildCompositeIndexKey(table, idxDef, row)
					if ok {
						oldIdxVal, getErr := idxTree.Get([]byte(indexKey))
						idxTree.Delete([]byte(indexKey))
						if c.txnActive && getErr == nil {
							idxChanges = append(idxChanges, indexUndoEntry{
								indexName: idxName,
								key:       []byte(indexKey),
								oldValue:  oldIdxVal,
								wasAdded:  false, // was deleted
							})
						}
					}
				}
			}
		}

		// Log to WAL before applying change
		if c.wal != nil && c.txnActive {
			walData := append([]byte(key), 0)
			record := &storage.WALRecord{
				TxnID: c.txnID,
				Type:  storage.WALDelete,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				return 0, rowsAffected, err
			}
		}

		// Record undo log entry for rollback
		if c.txnActive {
			keyCopy2 := make([]byte, len(key))
			copy(keyCopy2, key)
			valueCopy2 := make([]byte, len(entry.value))
			copy(valueCopy2, entry.value)
			c.undoLog = append(c.undoLog, undoEntry{
				action:       undoDelete,
				tableName:    stmt.Table,
				key:          keyCopy2,
				oldValue:     valueCopy2,
				indexChanges: idxChanges,
			})
		}

		if err := tree.Delete(key); err != nil {
			return 0, rowsAffected, fmt.Errorf("failed to delete row: %w", err)
		}

		// Execute AFTER DELETE trigger per-row
		_ = c.executeTriggers(stmt.Table, "DELETE", "AFTER", nil, row, table.Columns)
	}

	return 0, rowsAffected, nil
}

// Select queries rows from a table or view
func (cat *Catalog) Select(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	cat.mu.RLock()
	defer cat.mu.RUnlock()
	return cat.selectLocked(stmt, args)
}

// selectLocked executes a SELECT without acquiring the lock.
// Use this from within methods that already hold the lock (Update, Delete, etc.)
// to avoid deadlocks when evaluating subqueries.
func (cat *Catalog) selectLocked(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	// Resolve positional references in GROUP BY and ORDER BY (e.g., GROUP BY 1, ORDER BY 2)
	stmt = resolvePositionalRefs(stmt)

	// Handle SELECT without FROM clause (scalar expressions)
	if stmt.From == nil {
		return cat.executeScalarSelect(stmt, args)
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
	view, viewErr := cat.GetView(stmt.From.Name)
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
			if len(stmt.Columns) == 1 {
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
		if cat.cteResults != nil {
			if cteRes, ok := cat.cteResults[strings.ToLower(stmt.From.Name)]; ok {
				// Create a synthetic table definition from the CTE result
				table = &TableDef{
					Name: stmt.From.Name,
				}
				for _, colName := range cteRes.columns {
					table.Columns = append(table.Columns, ColumnDef{Name: colName, Type: "TEXT"})
				}
			} else {
				return nil, nil, err
			}
		} else {
			return nil, nil, err
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
					name:            c.Name + "()",
					tableName:       mainTableRef,
					index:           -1, // Will be evaluated per row
					hasEmbeddedAgg:  len(embeddedAggs) > 0,
					originalExpr:    actualCol,
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
				name:            exprName,
				tableName:       mainTableRef,
				index:           -1, // Will be evaluated per row
				hasEmbeddedAgg:  len(embeddedAggs) > 0,
				originalExpr:    actualCol,
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
	tree, exists := cat.tableTrees[stmt.From.Name]
	if !exists {
		return returnColumns, rows, nil
	}

	// Try to use index for WHERE clause
	var useIndex bool
	var indexMatches map[string]bool
	if stmt.Where != nil {
		indexMatches, useIndex = cat.useIndexForQueryWithArgs(stmt.From.Name, stmt.Where, args)
	}

	// If using index, directly fetch matching rows instead of full scan
	if useIndex {
		for pk := range indexMatches {
			valueData, err := tree.Get([]byte(pk))
			if err != nil {
				continue // Row not found
			}

			// Decode full row
			fullRow, err := decodeRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}

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
	} else {
		// Full table scan when no index is available
		iter, _ := tree.Scan(nil, nil)
		defer iter.Close()

		for iter.HasNext() {
			_, valueData, err := iter.Next()
			if err != nil {
				break
			}

			// Decode full row
			fullRow, err := decodeRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}

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

	return returnColumns, rows, nil
}

// applyOuterQuery applies an outer query's projections, WHERE, ORDER BY, etc.
// to already-fetched rows from a view or CTE result.
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

// computeViewAggregate computes a single aggregate value for a group of rows
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

// executeScalarSelect handles SELECT without FROM clause (scalar expressions)
func (c *Catalog) executeScalarSelect(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	// SELECT without FROM - evaluate each expression
	var returnColumns []string
	var rows [][]interface{}

	// Handle each column in the SELECT clause
	if len(stmt.Columns) == 0 {
		return nil, nil, errors.New("no columns specified")
	}

	// Check if this is a simple expression or contains aggregates/window functions
	hasAggregate := false
	hasWindowFunc := false

	for _, col := range stmt.Columns {
		actual := col
		if ae, ok := col.(*query.AliasExpr); ok {
			actual = ae.Expr
		}
		if fc, ok := actual.(*query.FunctionCall); ok {
			funcName := strings.ToUpper(fc.Name)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" || funcName == "GROUP_CONCAT" {
				hasAggregate = true
			}
		}
		if _, ok := actual.(*query.WindowExpr); ok {
			hasWindowFunc = true
		}
	}

	if hasAggregate {
		// Aggregate without FROM - compute single aggregate result
		return c.executeScalarAggregate(stmt, args)
	}

	if hasWindowFunc {
		// Window function without FROM - need to handle specially
		return nil, nil, errors.New("window functions require FROM clause")
	}

	// Check WHERE clause (e.g., SELECT 1 WHERE 1 = 1)
	if stmt.Where != nil {
		matched, err := evaluateWhere(c, nil, nil, stmt.Where, args)
		if err != nil {
			return nil, nil, err
		}
		if !matched {
			// WHERE is false — return empty result with column names
			for i, col := range stmt.Columns {
				colName := fmt.Sprintf("column_%d", i)
				if ae, ok := col.(*query.AliasExpr); ok {
					colName = ae.Alias
				} else if ident, ok := col.(*query.Identifier); ok {
					colName = ident.Name
				} else if fc, ok := col.(*query.FunctionCall); ok {
					colName = fc.Name
				}
				returnColumns = append(returnColumns, colName)
			}
			return returnColumns, nil, nil
		}
	}

	// Simple scalar expression - evaluate each column expression once
	row := make([]interface{}, len(stmt.Columns))
	for i, col := range stmt.Columns {
		val, err := evaluateExpression(c, nil, nil, col, args)
		if err != nil {
			return nil, nil, err
		}

		// Build column name
		colName := fmt.Sprintf("column_%d", i)
		if ae, ok := col.(*query.AliasExpr); ok {
			colName = ae.Alias
		} else if ident, ok := col.(*query.Identifier); ok {
			colName = ident.Name
		} else if fc, ok := col.(*query.FunctionCall); ok {
			colName = fc.Name
		}
		returnColumns = append(returnColumns, colName)
		row[i] = val
	}

	rows = append(rows, row)

	// Handle DISTINCT
	if stmt.Distinct {
		rows = c.applyDistinct(rows)
	}

	// Handle ORDER BY (for scalar expressions, just sort the single row)
	if len(stmt.OrderBy) > 0 {
		// No ordering needed for single row
	}

	// Handle LIMIT
	if stmt.Limit != nil {
		limitVal, err := evaluateExpression(c, nil, nil, stmt.Limit, args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(rows) {
				rows = rows[:limit]
			}
		}
	}

	return returnColumns, rows, nil
}

// executeScalarAggregate handles aggregate functions without FROM
func (c *Catalog) executeScalarAggregate(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	var returnColumns []string

	// Evaluate each column as an aggregate
	row := make([]interface{}, len(stmt.Columns))
	for i, col := range stmt.Columns {
		fc, ok := col.(*query.FunctionCall)
		if !ok {
			return nil, nil, errors.New("aggregate functions required in this context")
		}

		funcName := strings.ToUpper(fc.Name)
		var colName string
		var result interface{}

		switch funcName {
		case "COUNT":
			colName = "COUNT(*)"
			// COUNT(*) without FROM is always 1
			result = float64(1)
		case "SUM":
			colName = "SUM"
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(c, nil, nil, fc.Args[0], args)
				if err == nil {
					if f, ok := toFloat64(val); ok {
						result = f
					}
				}
			}
		case "AVG":
			colName = "AVG"
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(c, nil, nil, fc.Args[0], args)
				if err == nil {
					if f, ok := toFloat64(val); ok {
						result = f
					}
				}
			}
		case "MIN":
			colName = "MIN"
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(c, nil, nil, fc.Args[0], args)
				if err == nil {
					result = val
				}
			}
		case "MAX":
			colName = "MAX"
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(c, nil, nil, fc.Args[0], args)
				if err == nil {
					result = val
				}
			}
		default:
			colName = fc.Name
			result = nil
		}

		returnColumns = append(returnColumns, colName)
		row[i] = result
	}

	return returnColumns, [][]interface{}{row}, nil
}

// executeSelectWithJoin handles SELECT with JOIN clauses
func (c *Catalog) executeSelectWithJoin(stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo) ([]string, [][]interface{}, error) {
	var mainTableCols []ColumnDef
	var intermediateRows [][]interface{}

	// Check if main table is a CTE result
	if c.cteResults != nil {
		if cteRes, ok := c.cteResults[strings.ToLower(stmt.From.Name)]; ok {
			mainTableCols = make([]ColumnDef, len(cteRes.columns))
			for i, col := range cteRes.columns {
				mainTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
			}
			intermediateRows = make([][]interface{}, len(cteRes.rows))
			copy(intermediateRows, cteRes.rows)
		}
	}

	if mainTableCols == nil {
		// Get the main table
		mainTable, err := c.getTableLocked(stmt.From.Name)
		if err != nil {
			return nil, nil, err
		}

		mainTree, exists := c.tableTrees[stmt.From.Name]
		if !exists {
			return nil, [][]interface{}{}, nil
		}

		mainTableCols = mainTable.Columns

		// Build initial intermediate rows from main table (full column data)
		mainIter, _ := mainTree.Scan(nil, nil)
		for mainIter.HasNext() {
			_, data, err := mainIter.Next()
			if err != nil {
				break
			}
			row, err := decodeRow(data, len(mainTable.Columns))
			if err != nil {
				continue
			}
			intermediateRows = append(intermediateRows, row)
		}
		mainIter.Close()
	}

	// Track combined columns and table offsets for projection
	combinedColumns := make([]ColumnDef, len(mainTableCols))
	copy(combinedColumns, mainTableCols)
	// Set source table name for column disambiguation in JOINs
	mainAlias := stmt.From.Name
	if stmt.From.Alias != "" {
		mainAlias = stmt.From.Alias
	}
	for i := range combinedColumns {
		combinedColumns[i].sourceTbl = mainAlias
	}

	type tableOffset struct {
		name   string
		offset int
		count  int
	}
	tableOffsets := []tableOffset{{
		name:   mainAlias,
		offset: 0,
		count:  len(mainTableCols),
	}}

	// Chain through each JOIN
	for _, join := range stmt.Joins {
		var joinTableCols []ColumnDef
		var joinRows [][]interface{}

		// Check if join table is a derived table (subquery or UNION)
		if join.Table.Subquery != nil || join.Table.SubqueryStmt != nil {
			subCols, subRows, err := c.executeDerivedTable(join.Table, args)
			if err == nil {
				joinTableCols = make([]ColumnDef, len(subCols))
				for i, col := range subCols {
					joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
				}
				joinRows = subRows
			}
		}

		// Check if join table is a CTE result
		if joinTableCols == nil && c.cteResults != nil {
			if cteRes, ok := c.cteResults[strings.ToLower(join.Table.Name)]; ok {
				// Use CTE result rows
				joinTableCols = make([]ColumnDef, len(cteRes.columns))
				for i, col := range cteRes.columns {
					joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
				}
				joinRows = cteRes.rows
			}
		}

		if joinTableCols == nil {
			// Check if it's a view (CTE registered as view)
			if viewDef, viewErr := c.GetView(join.Table.Name); viewErr == nil {
				viewCols, viewRows, viewExecErr := c.selectLocked(viewDef, args)
				if viewExecErr == nil {
					joinTableCols = make([]ColumnDef, len(viewCols))
					for i, col := range viewCols {
						joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
					}
					joinRows = viewRows
				}
			}
		}

		if joinTableCols == nil {
			// Normal table lookup
			joinTable, err := c.getTableLocked(join.Table.Name)
			if err != nil {
				continue
			}

			joinTree, exists := c.tableTrees[join.Table.Name]
			if !exists {
				continue
			}

			joinTableCols = joinTable.Columns
			// Scan join table rows
			joinIter, _ := joinTree.Scan(nil, nil)
			for joinIter.HasNext() {
				_, data, err := joinIter.Next()
				if err != nil {
					break
				}
				row, err := decodeRow(data, len(joinTable.Columns))
				if err != nil {
					continue
				}
				joinRows = append(joinRows, row)
			}
			joinIter.Close()
		}

		isLeftJoin := join.Type == query.TokenLeft || join.Type == query.TokenFull
		isRightJoin := join.Type == query.TokenRight || join.Type == query.TokenFull
		isCrossJoin := join.Type == query.TokenCross

		// Build combined columns for evaluating ON condition
		newCombinedColumns := make([]ColumnDef, len(combinedColumns)+len(joinTableCols))
		copy(newCombinedColumns, combinedColumns)
		copy(newCombinedColumns[len(combinedColumns):], joinTableCols)
		// Set source table name for join table columns
		joinAlias := join.Table.Name
		if join.Table.Alias != "" {
			joinAlias = join.Table.Alias
		}
		for i := len(combinedColumns); i < len(newCombinedColumns); i++ {
			newCombinedColumns[i].sourceTbl = joinAlias
		}

		var newIntermediate [][]interface{}

		// joinRows is already populated above (from CTE result or B-tree scan)
		rightRows := joinRows

		if isCrossJoin {
			// CROSS JOIN: Cartesian product
			for _, leftRow := range intermediateRows {
				for _, joinRow := range rightRows {
					combined := make([]interface{}, len(leftRow)+len(joinRow))
					copy(combined, leftRow)
					copy(combined[len(leftRow):], joinRow)

					if join.Condition != nil {
						ok, err := evaluateWhere(c, combined, newCombinedColumns, join.Condition, args)
						if err != nil || !ok {
							continue
						}
					}
					newIntermediate = append(newIntermediate, combined)
				}
			}
		} else {
			// Track which right rows were matched (for RIGHT/FULL OUTER JOIN)
			rightMatched := make([]bool, len(rightRows))

			// Iterate through left side rows
			for _, leftRow := range intermediateRows {
				matched := false

				for ri, joinRow := range rightRows {
					combined := make([]interface{}, len(leftRow)+len(joinRow))
					copy(combined, leftRow)
					copy(combined[len(leftRow):], joinRow)

					if join.Condition != nil {
						ok, err := evaluateWhere(c, combined, newCombinedColumns, join.Condition, args)
						if err != nil || !ok {
							continue
						}
					}
					matched = true
					rightMatched[ri] = true
					newIntermediate = append(newIntermediate, combined)
				}

				if isLeftJoin && !matched {
					// NULLs for join table columns
					combined := make([]interface{}, len(leftRow)+len(joinTableCols))
					copy(combined, leftRow)
					newIntermediate = append(newIntermediate, combined)
				}
			}

			// For RIGHT/FULL OUTER JOIN: add unmatched right rows with NULL left side
			if isRightJoin {
				for ri, joinRow := range rightRows {
					if !rightMatched[ri] {
						combined := make([]interface{}, len(combinedColumns)+len(joinTableCols))
						copy(combined[len(combinedColumns):], joinRow)
						newIntermediate = append(newIntermediate, combined)
					}
				}
			}
		}

		intermediateRows = newIntermediate
		combinedColumns = newCombinedColumns
		tableOffsets = append(tableOffsets, tableOffset{
			name:   joinAlias,
			offset: tableOffsets[len(tableOffsets)-1].offset + tableOffsets[len(tableOffsets)-1].count,
			count:  len(joinTableCols),
		})
	}

	// Apply WHERE clause to joined rows
	if stmt.Where != nil {
		var filteredRows [][]interface{}
		for _, row := range intermediateRows {
			matched, err := evaluateWhere(c, row, combinedColumns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
			filteredRows = append(filteredRows, row)
		}
		intermediateRows = filteredRows
	}

	// Add hidden ORDER BY columns from joined tables not already in selectCols
	hiddenOrderByCols := 0
	if len(stmt.OrderBy) > 0 {
		for _, ob := range stmt.OrderBy {
			var colName, tblName string
			switch expr := ob.Expr.(type) {
			case *query.QualifiedIdentifier:
				colName = expr.Column
				tblName = expr.Table
			case *query.Identifier:
				if dotIdx := strings.IndexByte(expr.Name, '.'); dotIdx > 0 && dotIdx < len(expr.Name)-1 {
					tblName = expr.Name[:dotIdx]
					colName = expr.Name[dotIdx+1:]
				} else {
					colName = expr.Name
				}
			default:
				continue
			}
			// Check if already in selectCols
			found := false
			colLower := strings.ToLower(colName)
			tblLower := strings.ToLower(tblName)
			for _, ci := range selectCols {
				if strings.ToLower(ci.name) == colLower {
					if tblName == "" || strings.ToLower(ci.tableName) == tblLower {
						found = true
						break
					}
				}
			}
			if !found {
				// Find the column in the appropriate table
				if tblName != "" {
					for _, to := range tableOffsets {
						if strings.ToLower(to.name) == tblLower {
							// Find column index in that table
							for _, col := range combinedColumns {
								if strings.ToLower(col.Name) == colLower && strings.ToLower(col.sourceTbl) == tblLower {
									// Get raw index within the table
									rawIdx := -1
									// Look up the table to get column index
									tblDef, tErr := c.getTableLocked(to.name)
									if tErr == nil {
										rawIdx = tblDef.GetColumnIndex(colName)
									}
									if rawIdx < 0 {
										// Try finding by iterating through the table's known columns
										for ci, cc := range combinedColumns[to.offset : to.offset+to.count] {
											if strings.ToLower(cc.Name) == colLower {
												rawIdx = ci
												break
											}
										}
									}
									if rawIdx >= 0 {
										selectCols = append(selectCols, selectColInfo{name: colName, tableName: to.name, index: rawIdx})
										hiddenOrderByCols++
									}
									break
								}
							}
							break
						}
					}
				} else {
					// No table qualifier - search main table columns
					for ci, cc := range mainTableCols {
						if strings.EqualFold(cc.Name, colName) {
							selectCols = append(selectCols, selectColInfo{name: colName, tableName: mainAlias, index: ci})
							hiddenOrderByCols++
							break
						}
					}
				}
			}
		}
	}

	// Project selectCols from intermediate rows
	var resultRows [][]interface{}
	for _, row := range intermediateRows {
		projected := make([]interface{}, 0, len(selectCols))
		for i, ci := range selectCols {
			if ci.index == -1 && !ci.isAggregate {
				// Scalar function - evaluate it against the combined row
				if i < len(stmt.Columns) {
					if expr, ok := stmt.Columns[i].(query.Expression); ok {
						val, err := evaluateExpression(c, row, combinedColumns, expr, args)
						if err == nil {
							projected = append(projected, val)
							continue
						}
					}
				}
				projected = append(projected, nil)
				continue
			}
			if ci.index < 0 {
				// Aggregate - append nil placeholder
				projected = append(projected, nil)
				continue
			}
			found := false
			for _, to := range tableOffsets {
				if ci.tableName == to.name || (ci.tableName == "" && to.offset == 0) {
					colIdx := to.offset + ci.index
					if colIdx >= 0 && colIdx < len(row) {
						projected = append(projected, row[colIdx])
					} else {
						projected = append(projected, nil)
					}
					found = true
					break
				}
			}
			if !found {
				projected = append(projected, nil)
			}
		}
		resultRows = append(resultRows, projected)
	}

	// Evaluate window functions on projected rows
	hasWindowFuncs := false
	for _, ci := range selectCols {
		if ci.isWindow {
			hasWindowFuncs = true
			break
		}
	}
	if hasWindowFuncs {
		resultRows = c.evaluateWindowFunctions(resultRows, selectCols, nil, stmt, args, nil)
	}

	// Build return columns
	visibleCols := len(selectCols) - hiddenOrderByCols
	returnColumns := make([]string, visibleCols)
	for i := 0; i < visibleCols; i++ {
		returnColumns[i] = selectCols[i].name
	}

	// Apply ORDER BY to projected rows
	if len(stmt.OrderBy) > 0 {
		resultRows = c.applyOrderBy(resultRows, selectCols, stmt.OrderBy)
	}

	// Strip hidden ORDER BY columns
	if hiddenOrderByCols > 0 {
		resultRows = stripHiddenCols(resultRows, len(selectCols), hiddenOrderByCols)
		selectCols = selectCols[:visibleCols]
	}

	// Apply DISTINCT
	if stmt.Distinct {
		resultRows = c.applyDistinct(resultRows)
	}

	// Apply OFFSET
	if stmt.Offset != nil {
		offsetVal, err := evaluateExpression(c, nil, nil, stmt.Offset, args)
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
		limitVal, err := evaluateExpression(c, nil, nil, stmt.Limit, args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(resultRows) {
				resultRows = resultRows[:limit]
			}
		}
	}

	return returnColumns, resultRows, nil
}

// executeSelectWithJoinAndGroupBy handles SELECT with JOIN and GROUP BY / aggregates
func (c *Catalog) executeSelectWithJoinAndGroupBy(stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, returnColumns []string) ([]string, [][]interface{}, error) {
	var mainTableCols []ColumnDef
	var intermediateRows [][]interface{}

	// Check if main table is a CTE result or derived table
	if stmt.From.Subquery != nil || stmt.From.SubqueryStmt != nil {
		// Derived table as main table
		subCols, subRows, err := c.executeDerivedTable(stmt.From, args)
		if err != nil {
			return nil, nil, err
		}
		mainTableCols = make([]ColumnDef, len(subCols))
		for i, col := range subCols {
			mainTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
		}
		intermediateRows = make([][]interface{}, len(subRows))
		copy(intermediateRows, subRows)
	} else if c.cteResults != nil {
		if cteRes, ok := c.cteResults[strings.ToLower(stmt.From.Name)]; ok {
			mainTableCols = make([]ColumnDef, len(cteRes.columns))
			for i, col := range cteRes.columns {
				mainTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
			}
			intermediateRows = make([][]interface{}, len(cteRes.rows))
			copy(intermediateRows, cteRes.rows)
		}
	}

	if mainTableCols == nil {
		// Get main table for column info
		mainTable, err := c.getTableLocked(stmt.From.Name)
		if err != nil {
			return nil, nil, err
		}

		mainTree, exists := c.tableTrees[stmt.From.Name]
		if !exists {
			return returnColumns, [][]interface{}{}, nil
		}

		mainTableCols = mainTable.Columns

		// Build initial intermediate rows from main table (full column data)
		mainIter, _ := mainTree.Scan(nil, nil)
		for mainIter.HasNext() {
			_, data, err := mainIter.Next()
			if err != nil {
				break
			}
			row, err := decodeRow(data, len(mainTable.Columns))
			if err != nil {
				continue
			}
			intermediateRows = append(intermediateRows, row)
		}
		mainIter.Close()
	}

	// Track combined columns from all tables
	allColumns := make([]ColumnDef, len(mainTableCols))
	copy(allColumns, mainTableCols)
	// Set source table name for column disambiguation in JOINs
	mainAlias := stmt.From.Name
	if stmt.From.Alias != "" {
		mainAlias = stmt.From.Alias
	}
	for i := range allColumns {
		allColumns[i].sourceTbl = mainAlias
	}

	// Chain through each JOIN to build full combined rows
	for _, join := range stmt.Joins {
		var joinTableCols []ColumnDef
		var rightRows [][]interface{}

		// Check if join table is a derived table
		if join.Table.Subquery != nil || join.Table.SubqueryStmt != nil {
			subCols, subRows, err := c.executeDerivedTable(join.Table, args)
			if err == nil {
				joinTableCols = make([]ColumnDef, len(subCols))
				for i, col := range subCols {
					joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
				}
				rightRows = subRows
			}
		}

		// Check if join table is a CTE result
		if joinTableCols == nil && c.cteResults != nil {
			if cteRes, ok := c.cteResults[strings.ToLower(join.Table.Name)]; ok {
				joinTableCols = make([]ColumnDef, len(cteRes.columns))
				for i, col := range cteRes.columns {
					joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
				}
				rightRows = cteRes.rows
			}
		}

		// Check if join table is a view (CTE registered as view)
		if joinTableCols == nil {
			if viewDef, viewErr := c.GetView(join.Table.Name); viewErr == nil {
				viewCols, viewRows, viewExecErr := c.selectLocked(viewDef, args)
				if viewExecErr == nil {
					joinTableCols = make([]ColumnDef, len(viewCols))
					for i, col := range viewCols {
						joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
					}
					rightRows = viewRows
				}
			}
		}

		if joinTableCols == nil {
			// Normal table lookup
			joinTable, err := c.getTableLocked(join.Table.Name)
			if err != nil {
				continue
			}
			joinTree, exists := c.tableTrees[join.Table.Name]
			if !exists {
				continue
			}
			joinTableCols = joinTable.Columns

			// Collect right side rows
			joinIter, _ := joinTree.Scan(nil, nil)
			for joinIter.HasNext() {
				_, data, err := joinIter.Next()
				if err != nil {
					break
				}
				joinRow, err := decodeRow(data, len(joinTable.Columns))
				if err != nil {
					continue
				}
				rightRows = append(rightRows, joinRow)
			}
			joinIter.Close()
		}

		isLeftJoin := join.Type == query.TokenLeft || join.Type == query.TokenFull
		isRightJoin := join.Type == query.TokenRight || join.Type == query.TokenFull
		isCrossJoin := join.Type == query.TokenCross

		newAllColumns := make([]ColumnDef, len(allColumns)+len(joinTableCols))
		copy(newAllColumns, allColumns)
		copy(newAllColumns[len(allColumns):], joinTableCols)
		// Set source table name for join table columns
		joinAlias := join.Table.Name
		if join.Table.Alias != "" {
			joinAlias = join.Table.Alias
		}
		for i := len(allColumns); i < len(newAllColumns); i++ {
			newAllColumns[i].sourceTbl = joinAlias
		}

		var newIntermediate [][]interface{}

		if isCrossJoin {
			// CROSS JOIN: Cartesian product - include ALL combinations
			for _, leftRow := range intermediateRows {
				for _, joinRow := range rightRows {
					combined := make([]interface{}, len(leftRow)+len(joinRow))
					copy(combined, leftRow)
					copy(combined[len(leftRow):], joinRow)
					newIntermediate = append(newIntermediate, combined)
				}
			}
		} else {
			rightMatched := make([]bool, len(rightRows))

			for _, leftRow := range intermediateRows {
				matched := false

				for ri, joinRow := range rightRows {
					combined := make([]interface{}, len(leftRow)+len(joinRow))
					copy(combined, leftRow)
					copy(combined[len(leftRow):], joinRow)

					if join.Condition != nil {
						ok, err := evaluateWhere(c, combined, newAllColumns, join.Condition, args)
						if err != nil || !ok {
							continue
						}
					}
					matched = true
					rightMatched[ri] = true
					newIntermediate = append(newIntermediate, combined)
				}

				if isLeftJoin && !matched {
					combined := make([]interface{}, len(leftRow)+len(joinTableCols))
					copy(combined, leftRow)
					newIntermediate = append(newIntermediate, combined)
				}
			}

			// For RIGHT/FULL OUTER JOIN: add unmatched right rows
			if isRightJoin {
				for ri, joinRow := range rightRows {
					if !rightMatched[ri] {
						combined := make([]interface{}, len(allColumns)+len(joinTableCols))
						copy(combined[len(allColumns):], joinRow)
						newIntermediate = append(newIntermediate, combined)
					}
				}
			}
		}

		intermediateRows = newIntermediate
		allColumns = newAllColumns
	}

	// Apply WHERE clause to joined rows before GROUP BY
	if stmt.Where != nil {
		var filteredRows [][]interface{}
		for _, row := range intermediateRows {
			matched, err := evaluateWhere(c, row, allColumns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
			filteredRows = append(filteredRows, row)
		}
		intermediateRows = filteredRows
	}

	// joinedRows now contains properly filtered and chained results
	joinedRows := intermediateRows

	// Parse GROUP BY column indices (relative to combined row) and expressions
	type joinGroupBySpec struct {
		index int              // >=0 for simple column, -1 for expression
		expr  query.Expression // non-nil for expression GROUP BY
	}
	joinGroupBySpecs := make([]joinGroupBySpec, len(stmt.GroupBy))
	for i, gb := range stmt.GroupBy {
		switch g := gb.(type) {
		case *query.Identifier:
			found := false
			// Check for dotted identifier like "table.column"
			if dotIdx := strings.IndexByte(g.Name, '.'); dotIdx > 0 && dotIdx < len(g.Name)-1 {
				tblName := g.Name[:dotIdx]
				colName := g.Name[dotIdx+1:]
				for j, col := range allColumns {
					if strings.EqualFold(col.Name, colName) && strings.EqualFold(col.sourceTbl, tblName) {
						joinGroupBySpecs[i] = joinGroupBySpec{index: j}
						found = true
						break
					}
				}
			}
			// Find column index in combined columns (bare name)
			if !found {
				for j, col := range allColumns {
					if col.Name == g.Name {
						joinGroupBySpecs[i] = joinGroupBySpec{index: j}
						found = true
						break
					}
				}
			}
			if !found {
				// Check if it's a SELECT alias
				for _, col := range stmt.Columns {
					if ae, ok := col.(*query.AliasExpr); ok && strings.EqualFold(ae.Alias, g.Name) {
						// Found alias - resolve to underlying column
						if innerIdent, ok := ae.Expr.(*query.Identifier); ok {
							for j, col := range allColumns {
								if col.Name == innerIdent.Name {
									joinGroupBySpecs[i] = joinGroupBySpec{index: j}
									found = true
									break
								}
							}
						} else if qi, ok := ae.Expr.(*query.QualifiedIdentifier); ok {
							for j, col := range allColumns {
								if col.Name == qi.Column {
									joinGroupBySpecs[i] = joinGroupBySpec{index: j}
									found = true
									break
								}
							}
						}
						if !found {
							joinGroupBySpecs[i] = joinGroupBySpec{index: -1, expr: ae.Expr}
							found = true
						}
						break
					}
				}
			}
			if !found {
				joinGroupBySpecs[i] = joinGroupBySpec{index: -1, expr: gb}
			}
		case *query.QualifiedIdentifier:
			targetTable := g.Table
			colName := g.Column
			found := false

			if targetTable == stmt.From.Name || targetTable == stmt.From.Alias {
				for j, col := range mainTableCols {
					if col.Name == colName {
						joinGroupBySpecs[i] = joinGroupBySpec{index: j}
						found = true
						break
					}
				}
			} else {
				offset := len(mainTableCols)
				for _, join := range stmt.Joins {
					joinTableName := join.Table.Name
					if join.Table.Alias != "" {
						joinTableName = join.Table.Alias
					}
					if joinTableName == targetTable {
						jt, err := c.getTableLocked(join.Table.Name)
						if err == nil {
							for j, col := range jt.Columns {
								if col.Name == colName {
									joinGroupBySpecs[i] = joinGroupBySpec{index: offset + j}
									found = true
									break
								}
							}
						}
						break
					}
					jt, _ := c.getTableLocked(join.Table.Name)
					if jt != nil {
						offset += len(jt.Columns)
					}
				}
			}
			if !found {
				joinGroupBySpecs[i] = joinGroupBySpec{index: -1, expr: gb}
			}
		default:
			// Expression GROUP BY (CASE, CAST, etc.)
			joinGroupBySpecs[i] = joinGroupBySpec{index: -1, expr: gb}
		}
	}

	// Group the joined rows
	groups := make(map[string][][]interface{})
	groupOrder := []string{}
	for _, row := range joinedRows {
		var groupKey strings.Builder
		for i, spec := range joinGroupBySpecs {
			if i > 0 {
				groupKey.WriteString("|")
			}
			if spec.index >= 0 && spec.index < len(row) {
				groupKey.WriteString(fmt.Sprintf("%v", row[spec.index]))
			} else if spec.expr != nil {
				val, err := evaluateExpression(c, row, allColumns, spec.expr, args)
				if err == nil {
					groupKey.WriteString(fmt.Sprintf("%v", val))
				}
			}
		}
		key := groupKey.String()
		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], row)
	}

	// Compute aggregates for each group
	var resultRows [][]interface{}

	for _, gk := range groupOrder {
		groupRows := groups[gk]
		if len(groupRows) == 0 {
			continue
		}
		resultRow := make([]interface{}, len(selectCols))

		for i, ci := range selectCols {
			if ci.isAggregate {
				// Collect values for this aggregate
				var values []interface{}
				for _, row := range groupRows {
					if ci.aggregateCol == "*" && ci.aggregateExpr == nil {
						values = append(values, int64(1))
					} else if ci.aggregateExpr != nil {
						// Expression argument (e.g., SUM(quantity * price))
						v, err := evaluateExpression(c, row, allColumns, ci.aggregateExpr, args)
						if err == nil {
							values = append(values, v)
						}
					} else {
						// Find column index for aggregate column
						// Need to consider which table the column belongs to
						colIdx := -1

						// First, determine which table the aggregate column belongs to
						targetTable := ci.tableName

						// Check if the target table matches the main table (by name or alias)
						mainAlias := stmt.From.Name
						if stmt.From.Alias != "" {
							mainAlias = stmt.From.Alias
						}
						if targetTable == "" || strings.EqualFold(targetTable, mainAlias) || strings.EqualFold(targetTable, stmt.From.Name) {
							// Column is in main table
							for j, col := range mainTableCols {
								if strings.EqualFold(col.Name, ci.aggregateCol) {
									colIdx = j
									break
								}
							}
						} else {
							// Column is in a joined table - find the correct table and offset
							offset := len(mainTableCols)
							for _, join := range stmt.Joins {
								joinTable, err := c.getTableLocked(join.Table.Name)
								if err != nil {
									continue
								}
								joinAlias := join.Table.Name
								if join.Table.Alias != "" {
									joinAlias = join.Table.Alias
								}
								if strings.EqualFold(joinAlias, targetTable) || strings.EqualFold(join.Table.Name, targetTable) {
									// Found the right table, look for column
									for j, col := range joinTable.Columns {
										if strings.EqualFold(col.Name, ci.aggregateCol) {
											colIdx = offset + j
											break
										}
									}
									break
								}
								offset += len(joinTable.Columns)
							}
						}

						if colIdx >= 0 && colIdx < len(row) {
							values = append(values, row[colIdx])
						}
					}
				}

				// Compute aggregate
				switch ci.aggregateType {
				case "COUNT":
					if ci.aggregateCol == "*" && !ci.isDistinct {
						resultRow[i] = int64(len(groupRows))
					} else if ci.isDistinct {
						// Count distinct non-null values
						seen := make(map[string]bool)
						for _, v := range values {
							if v != nil {
								seen[fmt.Sprintf("%v", v)] = true
							}
						}
						resultRow[i] = int64(len(seen))
					} else {
						count := int64(0)
						for _, v := range values {
							if v != nil {
								count++
							}
						}
						resultRow[i] = count
					}
				case "SUM":
					var sum float64
					hasVal := false
					for _, v := range values {
						if v != nil {
							if f, ok := toFloat64(v); ok {
								sum += f
								hasVal = true
							}
						}
					}
					if hasVal {
						resultRow[i] = sum
					} else {
						resultRow[i] = nil
					}
				case "AVG":
					var sum float64
					var count int64
					for _, v := range values {
						if v != nil {
							if f, ok := toFloat64(v); ok {
								sum += f
								count++
							}
						}
					}
					if count > 0 {
						resultRow[i] = sum / float64(count)
					} else {
						resultRow[i] = nil
					}
				case "MIN":
					var minVal interface{}
					for _, v := range values {
						if v != nil {
							if minVal == nil || compareValues(v, minVal) < 0 {
								minVal = v
							}
						}
					}
					resultRow[i] = minVal
				case "MAX":
					var maxVal interface{}
					for _, v := range values {
						if v != nil {
							if maxVal == nil || compareValues(v, maxVal) > 0 {
								maxVal = v
							}
						}
					}
					resultRow[i] = maxVal
				case "GROUP_CONCAT":
					var parts []string
					for _, v := range values {
						if v != nil {
							parts = append(parts, fmt.Sprintf("%v", v))
						}
					}
					if len(parts) > 0 {
						resultRow[i] = strings.Join(parts, ",")
					} else {
						resultRow[i] = nil
					}
				}
			} else {
				// Non-aggregate column - get value from first row
				colIdx := -1
				// Try table-qualified match first
				if ci.tableName != "" {
					for j, col := range allColumns {
						if strings.EqualFold(col.Name, ci.name) && strings.EqualFold(col.sourceTbl, ci.tableName) {
							colIdx = j
							break
						}
					}
				}
				// Fallback to name-only match
				if colIdx < 0 {
					for j, col := range allColumns {
						if col.Name == ci.name {
							colIdx = j
							break
						}
					}
				}
				if ci.hasEmbeddedAgg && len(groupRows) > 0 {
					// Expression with embedded aggregates in JOIN context
					val, err := c.evaluateExprWithGroupAggregatesJoin(ci.originalExpr, groupRows, allColumns, args)
					if err == nil {
						resultRow[i] = val
					}
				} else if colIdx >= 0 && len(groupRows) > 0 && colIdx < len(groupRows[0]) {
					resultRow[i] = groupRows[0][colIdx]
				} else if colIdx == -1 && len(groupRows) > 0 {
					// Expression column (CASE, CAST, etc.) - evaluate it
					if i < len(stmt.Columns) {
						expr := stmt.Columns[i]
						if ae, ok := expr.(*query.AliasExpr); ok {
							expr = ae.Expr
						}
						val, err := evaluateExpression(c, groupRows[0], allColumns, expr, args)
						if err == nil {
							resultRow[i] = val
						}
					}
				}
			}
		}

		resultRows = append(resultRows, resultRow)
	}

	// Apply HAVING clause to results
	if stmt.Having != nil {
		var filtered [][]interface{}
		for _, row := range resultRows {
			havingMatched, err := evaluateHaving(c, row, selectCols, nil, stmt.Having, args)
			if err == nil && havingMatched {
				filtered = append(filtered, row)
			}
		}
		resultRows = filtered
	}

	return returnColumns, resultRows, nil
}

// toInt converts a value to int
func toInt(v interface{}) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	default:
		return 0, false
	}
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

// addHiddenOrderByCols adds ORDER BY columns/expressions not present in selectCols as hidden columns.
// Returns augmented selectCols and count of hidden columns added.
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

// stripHiddenCols removes the last hiddenCount columns from each row
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

// applyOrderBy sorts rows based on ORDER BY clause
func (c *Catalog) applyOrderBy(rows [][]interface{}, selectCols []selectColInfo, orderBy []*query.OrderByExpr) [][]interface{} {
	if len(rows) == 0 || len(orderBy) == 0 {
		return rows
	}

	// Build sort key function
	sorted := make([][]interface{}, len(rows))
	copy(sorted, rows)

	sort.Slice(sorted, func(i, j int) bool {
		for obIdx, ob := range orderBy {
			// Find column index by matching expression to selectCols
			colIdx := -1
			switch expr := ob.Expr.(type) {
			case *query.Identifier:
				// Check for dotted identifier like "table.column"
				if dotIdx := strings.IndexByte(expr.Name, '.'); dotIdx > 0 && dotIdx < len(expr.Name)-1 {
					tblLower := strings.ToLower(expr.Name[:dotIdx])
					colNameLower := strings.ToLower(expr.Name[dotIdx+1:])
					for idx, ci := range selectCols {
						if strings.ToLower(ci.name) == colNameLower && strings.ToLower(ci.tableName) == tblLower {
							colIdx = idx
							break
						}
					}
					if colIdx < 0 {
						for idx, ci := range selectCols {
							if strings.ToLower(ci.name) == colNameLower {
								colIdx = idx
								break
							}
						}
					}
				} else {
					nameLower := strings.ToLower(expr.Name)
					for idx, ci := range selectCols {
						if strings.ToLower(ci.name) == nameLower {
							colIdx = idx
							break
						}
					}
				}
			case *query.QualifiedIdentifier:
				colLower := strings.ToLower(expr.Column)
				tblLower := strings.ToLower(expr.Table)
				// First try exact match with table name
				for idx, ci := range selectCols {
					if strings.ToLower(ci.name) == colLower && strings.ToLower(ci.tableName) == tblLower {
						colIdx = idx
						break
					}
				}
				// Fallback to column-name-only match
				if colIdx < 0 {
					for idx, ci := range selectCols {
						if strings.ToLower(ci.name) == colLower {
							colIdx = idx
							break
						}
					}
				}
			case *query.NumberLiteral:
				// ORDER BY 1, 2, 3 (column position)
				if pos, ok := toFloat64(expr.Value); ok {
					colIdx = int(pos) - 1 // 1-based to 0-based
				}
			default:
				// Expression ORDER BY (e.g., ORDER BY price * quantity)
				// Match to the correct hidden ORDER BY column by index
				targetName := fmt.Sprintf("__orderby_%d", obIdx)
				for idx, ci := range selectCols {
					if ci.name == targetName {
						colIdx = idx
						break
					}
				}
			}
			if colIdx < 0 || colIdx >= len(sorted[i]) || colIdx >= len(sorted[j]) {
				continue
			}

			cmp := compareValues(sorted[i][colIdx], sorted[j][colIdx])
			if cmp != 0 {
				if ob.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return sorted
}

// applyDistinct removes duplicate rows
// evaluateWindowFunctions evaluates window functions (ROW_NUMBER, RANK, DENSE_RANK, etc.)
// Window functions operate on the full result set after WHERE but before ORDER BY.
// fullRows contains the original full table rows (all columns) for evaluating ORDER BY/PARTITION BY
// expressions that reference columns not in the SELECT list.
func (c *Catalog) evaluateWindowFunctions(rows [][]interface{}, selectCols []selectColInfo, table *TableDef, stmt *query.SelectStmt, args []interface{}, fullRows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return rows
	}

	for colIdx, ci := range selectCols {
		if !ci.isWindow || ci.windowExpr == nil {
			continue
		}

		we := ci.windowExpr

		// Build partition groups
		type partitionEntry struct {
			originalIdx int
			row         []interface{}
			fullRow     []interface{} // full table row for evaluating non-SELECT columns
		}
		partitions := make(map[string][]partitionEntry)
		partitionOrder := []string{} // preserve order of first appearance

		for i, row := range rows {
			var fRow []interface{}
			if i < len(fullRows) {
				fRow = fullRows[i]
			}
			// Compute partition key
			partKey := ""
			if len(we.PartitionBy) > 0 {
				var keyParts []string
				for _, pExpr := range we.PartitionBy {
					val := c.evalWindowExprOnRow(pExpr, row, selectCols, table, args, fRow)
					keyParts = append(keyParts, fmt.Sprintf("%v", val))
				}
				partKey = strings.Join(keyParts, "|")
			}

			if _, exists := partitions[partKey]; !exists {
				partitionOrder = append(partitionOrder, partKey)
			}
			partitions[partKey] = append(partitions[partKey], partitionEntry{originalIdx: i, row: row, fullRow: fRow})
		}

		// Process each partition
		for _, pk := range partitionOrder {
			entries := partitions[pk]

			// Sort within partition by window ORDER BY
			if len(we.OrderBy) > 0 {
				sort.SliceStable(entries, func(a, b int) bool {
					for _, ob := range we.OrderBy {
						va := c.evalWindowExprOnRow(ob.Expr, entries[a].row, selectCols, table, args, entries[a].fullRow)
						vb := c.evalWindowExprOnRow(ob.Expr, entries[b].row, selectCols, table, args, entries[b].fullRow)
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

			// Compute window function values
			switch we.Function {
			case "ROW_NUMBER":
				for i, entry := range entries {
					rows[entry.originalIdx][colIdx] = int64(i + 1)
				}

			case "RANK":
				rank := 1
				for i, entry := range entries {
					if i > 0 {
						// Check if ORDER BY values changed from previous row
						changed := false
						for _, ob := range we.OrderBy {
							va := c.evalWindowExprOnRow(ob.Expr, entries[i-1].row, selectCols, table, args, entries[i-1].fullRow)
							vb := c.evalWindowExprOnRow(ob.Expr, entry.row, selectCols, table, args, entry.fullRow)
							if compareValues(va, vb) != 0 {
								changed = true
								break
							}
						}
						if changed {
							rank = i + 1
						}
					}
					rows[entry.originalIdx][colIdx] = int64(rank)
				}

			case "DENSE_RANK":
				denseRank := 1
				for i, entry := range entries {
					if i > 0 {
						changed := false
						for _, ob := range we.OrderBy {
							va := c.evalWindowExprOnRow(ob.Expr, entries[i-1].row, selectCols, table, args, entries[i-1].fullRow)
							vb := c.evalWindowExprOnRow(ob.Expr, entry.row, selectCols, table, args, entry.fullRow)
							if compareValues(va, vb) != 0 {
								changed = true
								break
							}
						}
						if changed {
							denseRank++
						}
					}
					rows[entry.originalIdx][colIdx] = int64(denseRank)
				}

			case "LAG":
				offset := 1
				if len(we.Args) >= 2 {
					if num, ok := we.Args[1].(*query.NumberLiteral); ok {
						offset = int(num.Value)
					}
				}
				var defaultVal interface{}
				if len(we.Args) >= 3 {
					defaultVal = c.evalWindowExprOnRow(we.Args[2], entries[0].row, selectCols, table, args, entries[0].fullRow)
				}
				for i, entry := range entries {
					if i-offset >= 0 {
						if len(we.Args) > 0 {
							rows[entry.originalIdx][colIdx] = c.evalWindowExprOnRow(we.Args[0], entries[i-offset].row, selectCols, table, args, entries[i-offset].fullRow)
						}
					} else {
						rows[entry.originalIdx][colIdx] = defaultVal
					}
				}

			case "LEAD":
				offset := 1
				if len(we.Args) >= 2 {
					if num, ok := we.Args[1].(*query.NumberLiteral); ok {
						offset = int(num.Value)
					}
				}
				var defaultVal interface{}
				if len(we.Args) >= 3 {
					defaultVal = c.evalWindowExprOnRow(we.Args[2], entries[0].row, selectCols, table, args, entries[0].fullRow)
				}
				for i, entry := range entries {
					if i+offset < len(entries) {
						if len(we.Args) > 0 {
							rows[entry.originalIdx][colIdx] = c.evalWindowExprOnRow(we.Args[0], entries[i+offset].row, selectCols, table, args, entries[i+offset].fullRow)
						}
					} else {
						rows[entry.originalIdx][colIdx] = defaultVal
					}
				}

			case "FIRST_VALUE":
				if len(entries) > 0 && len(we.Args) > 0 {
					firstVal := c.evalWindowExprOnRow(we.Args[0], entries[0].row, selectCols, table, args, entries[0].fullRow)
					for _, entry := range entries {
						rows[entry.originalIdx][colIdx] = firstVal
					}
				}

			case "LAST_VALUE":
				if len(entries) > 0 && len(we.Args) > 0 {
					lastVal := c.evalWindowExprOnRow(we.Args[0], entries[len(entries)-1].row, selectCols, table, args, entries[len(entries)-1].fullRow)
					for _, entry := range entries {
						rows[entry.originalIdx][colIdx] = lastVal
					}
				}

			case "NTH_VALUE":
				if len(we.Args) >= 2 {
					if num, ok := we.Args[1].(*query.NumberLiteral); ok {
						n := int(num.Value)
						if n >= 1 && n <= len(entries) {
							nthVal := c.evalWindowExprOnRow(we.Args[0], entries[n-1].row, selectCols, table, args, entries[n-1].fullRow)
							for _, entry := range entries {
								rows[entry.originalIdx][colIdx] = nthVal
							}
						}
					}
				}

			case "COUNT":
				if len(we.OrderBy) > 0 {
					// Running COUNT with ORDER BY
					if len(we.Args) > 0 {
						if _, isStar := we.Args[0].(*query.StarExpr); isStar {
							for i, entry := range entries {
								rows[entry.originalIdx][colIdx] = int64(i + 1)
							}
						} else {
							count := int64(0)
							for _, entry := range entries {
								val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
								if val != nil {
									count++
								}
								rows[entry.originalIdx][colIdx] = count
							}
						}
					} else {
						for i, entry := range entries {
							rows[entry.originalIdx][colIdx] = int64(i + 1)
						}
					}
				} else if len(we.Args) > 0 {
					if _, isStar := we.Args[0].(*query.StarExpr); isStar {
						count := int64(len(entries))
						for _, entry := range entries {
							rows[entry.originalIdx][colIdx] = count
						}
					} else {
						count := int64(0)
						for _, entry := range entries {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if val != nil {
								count++
							}
						}
						for _, entry := range entries {
							rows[entry.originalIdx][colIdx] = count
						}
					}
				} else {
					count := int64(len(entries))
					for _, entry := range entries {
						rows[entry.originalIdx][colIdx] = count
					}
				}

			case "SUM":
				if len(we.Args) > 0 {
					if len(we.OrderBy) > 0 {
						// Running SUM with ORDER BY
						sum := 0.0
						hasVal := false
						for _, entry := range entries {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if val != nil {
								if v, ok := toFloat64(val); ok {
									sum += v
									hasVal = true
								}
							}
							if hasVal {
								rows[entry.originalIdx][colIdx] = sum
							} else {
								rows[entry.originalIdx][colIdx] = nil
							}
						}
					} else {
						// Partition-wide SUM
						sum := 0.0
						hasVal := false
						for _, entry := range entries {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if val != nil {
								if v, ok := toFloat64(val); ok {
									sum += v
									hasVal = true
								}
							}
						}
						for _, entry := range entries {
							if hasVal {
								rows[entry.originalIdx][colIdx] = sum
							} else {
								rows[entry.originalIdx][colIdx] = nil
							}
						}
					}
				}

			case "AVG":
				if len(we.Args) > 0 && len(entries) > 0 {
					if len(we.OrderBy) > 0 {
						// Running AVG with ORDER BY
						sum := 0.0
						count := 0
						for _, entry := range entries {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if val != nil {
								if v, ok := toFloat64(val); ok {
									sum += v
									count++
								}
							}
							if count > 0 {
								rows[entry.originalIdx][colIdx] = sum / float64(count)
							} else {
								rows[entry.originalIdx][colIdx] = nil
							}
						}
					} else {
						// Partition-wide AVG
						sum := 0.0
						count := 0
						for _, entry := range entries {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if val != nil {
								if v, ok := toFloat64(val); ok {
									sum += v
									count++
								}
							}
						}
						if count > 0 {
							avg := sum / float64(count)
							for _, entry := range entries {
								rows[entry.originalIdx][colIdx] = avg
							}
						} else {
							for _, entry := range entries {
								rows[entry.originalIdx][colIdx] = nil
							}
						}
					}
				}

			case "MIN":
				if len(we.Args) > 0 && len(entries) > 0 {
					if len(we.OrderBy) > 0 {
						// Running MIN with ORDER BY
						var minVal interface{}
						for _, entry := range entries {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if val != nil && (minVal == nil || compareValues(val, minVal) < 0) {
								minVal = val
							}
							rows[entry.originalIdx][colIdx] = minVal
						}
					} else {
						// Partition-wide MIN
						minVal := c.evalWindowExprOnRow(we.Args[0], entries[0].row, selectCols, table, args, entries[0].fullRow)
						for _, entry := range entries[1:] {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if compareValues(val, minVal) < 0 {
								minVal = val
							}
						}
						for _, entry := range entries {
							rows[entry.originalIdx][colIdx] = minVal
						}
					}
				}

			case "MAX":
				if len(we.Args) > 0 && len(entries) > 0 {
					if len(we.OrderBy) > 0 {
						// Running MAX with ORDER BY
						var maxVal interface{}
						for _, entry := range entries {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if val != nil && (maxVal == nil || compareValues(val, maxVal) > 0) {
								maxVal = val
							}
							rows[entry.originalIdx][colIdx] = maxVal
						}
					} else {
						// Partition-wide MAX
						maxVal := c.evalWindowExprOnRow(we.Args[0], entries[0].row, selectCols, table, args, entries[0].fullRow)
						for _, entry := range entries[1:] {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if compareValues(val, maxVal) > 0 {
								maxVal = val
							}
						}
						for _, entry := range entries {
							rows[entry.originalIdx][colIdx] = maxVal
						}
					}
				}
			}
		}
	}

	return rows
}

// evalWindowExprOnRow evaluates an expression against the projected row values.
// Optional fullRow parameter provides the original full table row for resolving
// columns not in the SELECT list (e.g., ORDER BY price when price is not selected).
func (c *Catalog) evalWindowExprOnRow(expr query.Expression, row []interface{}, selectCols []selectColInfo, table *TableDef, args []interface{}, fullRowOpt ...[]interface{}) interface{} {
	// First try to match against projected columns by name
	switch e := expr.(type) {
	case *query.Identifier:
		// Look in selectCols first
		for i, ci := range selectCols {
			if strings.EqualFold(ci.name, e.Name) && i < len(row) {
				return row[i]
			}
		}
		// Look in table columns using the full row if available
		if table != nil {
			// Try full row first (has all table columns)
			if len(fullRowOpt) > 0 && fullRowOpt[0] != nil {
				if idx := table.GetColumnIndex(e.Name); idx >= 0 && idx < len(fullRowOpt[0]) {
					return fullRowOpt[0][idx]
				}
			}
			// Fallback to projected row
			if idx := table.GetColumnIndex(e.Name); idx >= 0 && idx < len(row) {
				return row[idx]
			}
		}
	case *query.QualifiedIdentifier:
		// First try exact match with table name
		for i, ci := range selectCols {
			if strings.EqualFold(ci.name, e.Column) && strings.EqualFold(ci.tableName, e.Table) && i < len(row) {
				return row[i]
			}
		}
		// Fallback to column-name-only match
		for i, ci := range selectCols {
			if strings.EqualFold(ci.name, e.Column) && i < len(row) {
				return row[i]
			}
		}
		// Try full row for qualified identifiers too
		if table != nil && len(fullRowOpt) > 0 && fullRowOpt[0] != nil {
			if idx := table.GetColumnIndex(e.Column); idx >= 0 && idx < len(fullRowOpt[0]) {
				return fullRowOpt[0][idx]
			}
		}
	case *query.NumberLiteral:
		return e.Value
	case *query.StringLiteral:
		return e.Value
	}

	// Fallback: try evaluateExpression with full row if available
	if table != nil && len(fullRowOpt) > 0 && fullRowOpt[0] != nil {
		val, err := evaluateExpression(c, fullRowOpt[0], table.Columns, expr, args)
		if err == nil {
			return val
		}
	}
	// Final fallback: try evaluateExpression with projected row
	if table != nil {
		val, err := evaluateExpression(c, row, table.Columns, expr, args)
		if err == nil {
			return val
		}
	}
	return nil
}

// rowKeyForDedup creates a dedup-safe key for a row (or subset of row values).
// It properly distinguishes NULL from the string "nil"/"<nil>" by using type-tagged encoding.
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

func (c *Catalog) applyDistinct(rows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return rows
	}

	seen := make(map[string]bool)
	var result [][]interface{}

	for _, row := range rows {
		key := rowKeyForDedup(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// computeAggregates computes aggregate functions for a SELECT query
func (c *Catalog) computeAggregates(table *TableDef, stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, returnColumns []string) ([]string, [][]interface{}, error) {
	tree, exists := c.tableTrees[stmt.From.Name]
	if !exists {
		// Return zeros/nulls for aggregates on empty table
		return c.computeAggregateResult(selectCols, returnColumns, 0, nil)
	}

	// Read all rows and collect values for aggregates
	var filteredRows [][]interface{}
	var aggregateValues [][]interface{} // [column][row]

	// Initialize aggregate value storage
	for range selectCols {
		aggregateValues = append(aggregateValues, nil)
	}

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	for iter.HasNext() {
		_, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode full row
		fullRow, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present
		if stmt.Where != nil {
			matched, err := evaluateWhere(c, fullRow, table.Columns, stmt.Where, args)
			if err != nil {
				continue
			}
			if !matched {
				continue
			}
		}

		filteredRows = append(filteredRows, fullRow)

		// Collect values for aggregate columns
		for i, ci := range selectCols {
			if ci.isAggregate {
				var val interface{}
				if ci.aggregateCol == "*" && ci.aggregateExpr == nil {
					// COUNT(*): just count rows
					val = int64(1)
				} else if ci.aggregateExpr != nil {
					// Expression argument (e.g., SUM(quantity * price))
					v, err := evaluateExpression(c, fullRow, table.Columns, ci.aggregateExpr, args)
					if err == nil {
						val = v
					}
				} else {
					// Find column index for aggregate column
					colIdx := table.GetColumnIndex(ci.aggregateCol)
					if colIdx >= 0 && colIdx < len(fullRow) {
						val = fullRow[colIdx]
					}
				}
				aggregateValues[i] = append(aggregateValues[i], val)
			}
		}
	}

	// Compute aggregate results
	return c.computeAggregateResult(selectCols, returnColumns, len(filteredRows), aggregateValues)
}

// computeAggregateResult calculates the final aggregate values
func (c *Catalog) computeAggregateResult(selectCols []selectColInfo, returnColumns []string, rowCount int, aggregateValues [][]interface{}) ([]string, [][]interface{}, error) {
	resultRow := make([]interface{}, len(selectCols))

	for i, ci := range selectCols {
		if !ci.isAggregate {
			continue
		}

		switch ci.aggregateType {
		case "COUNT":
			if ci.aggregateCol == "*" && !ci.isDistinct {
				resultRow[i] = int64(rowCount)
			} else if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				if ci.isDistinct {
					// Count distinct non-null values
					seen := make(map[string]bool)
					for _, v := range aggregateValues[i] {
						if v != nil {
							key := fmt.Sprintf("%v", v)
							seen[key] = true
						}
					}
					resultRow[i] = int64(len(seen))
				} else {
					// Count non-null values
					count := int64(0)
					for _, v := range aggregateValues[i] {
						if v != nil {
							count++
						}
					}
					resultRow[i] = count
				}
			} else {
				resultRow[i] = int64(0)
			}

		case "SUM":
			if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				var sum float64
				hasVal := false
				for _, v := range aggregateValues[i] {
					if v != nil {
						if f, ok := toFloat64(v); ok {
							sum += f
							hasVal = true
						}
					}
				}
				if hasVal {
					resultRow[i] = sum
				} else {
					resultRow[i] = nil
				}
			} else {
				resultRow[i] = nil
			}

		case "AVG":
			if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				var sum float64
				var count int64
				for _, v := range aggregateValues[i] {
					if v != nil {
						if f, ok := toFloat64(v); ok {
							sum += f
							count++
						}
					}
				}
				if count > 0 {
					resultRow[i] = sum / float64(count)
				} else {
					resultRow[i] = nil
				}
			} else {
				resultRow[i] = nil
			}

		case "MIN":
			if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				var minVal interface{}
				for _, v := range aggregateValues[i] {
					if v != nil {
						if minVal == nil || compareValues(v, minVal) < 0 {
							minVal = v
						}
					}
				}
				resultRow[i] = minVal
			} else {
				resultRow[i] = nil
			}

		case "MAX":
			if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				var maxVal interface{}
				for _, v := range aggregateValues[i] {
					if v != nil {
						if maxVal == nil || compareValues(v, maxVal) > 0 {
							maxVal = v
						}
					}
				}
				resultRow[i] = maxVal
			} else {
				resultRow[i] = nil
			}

		case "GROUP_CONCAT":
			if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				var parts []string
				for _, v := range aggregateValues[i] {
					if v != nil {
						parts = append(parts, fmt.Sprintf("%v", v))
					}
				}
				if len(parts) > 0 {
					resultRow[i] = strings.Join(parts, ",")
				} else {
					resultRow[i] = nil
				}
			} else {
				resultRow[i] = nil
			}
		}
	}

	return returnColumns, [][]interface{}{resultRow}, nil
}

// addHiddenHavingAggregates adds any aggregate functions from HAVING clause that aren't already in selectCols.
// This is needed so that HAVING can reference aggregates not in the SELECT list (e.g., HAVING AVG(rating) >= 4).
// Returns the augmented selectCols and the number of hidden columns added.
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

// addHiddenOrderByAggregates adds any aggregate functions from ORDER BY clause that aren't already in selectCols.
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

// collectAggregatesFromExpr walks an expression tree and collects all aggregate FunctionCall nodes
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

// computeAggregatesWithGroupBy handles GROUP BY queries with aggregates
func (c *Catalog) computeAggregatesWithGroupBy(table *TableDef, stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, returnColumns []string) ([]string, [][]interface{}, error) {
	tree, exists := c.tableTrees[stmt.From.Name]
	if !exists {
		// Return empty result for GROUP BY on non-existent table
		return returnColumns, [][]interface{}{}, nil
	}

	// Parse GROUP BY column indices (for simple column refs) and expressions
	type groupBySpec struct {
		index int              // >=0 for simple column, -1 for expression
		expr  query.Expression // non-nil for expression GROUP BY
	}
	groupBySpecs := make([]groupBySpec, len(stmt.GroupBy))
	for i, gb := range stmt.GroupBy {
		if ident, ok := gb.(*query.Identifier); ok {
			idx := table.GetColumnIndex(ident.Name)
			if idx >= 0 {
				groupBySpecs[i] = groupBySpec{index: idx}
			} else {
				// Check if it's a SELECT alias
				resolved := false
				for j, col := range stmt.Columns {
					if ae, ok := col.(*query.AliasExpr); ok && strings.EqualFold(ae.Alias, ident.Name) {
						// Found alias - use the underlying expression
						if innerIdent, ok := ae.Expr.(*query.Identifier); ok {
							if ci := table.GetColumnIndex(innerIdent.Name); ci >= 0 {
								groupBySpecs[i] = groupBySpec{index: ci}
								resolved = true
								break
							}
						}
						// Expression alias (CASE, function, etc.)
						groupBySpecs[i] = groupBySpec{index: -1, expr: ae.Expr}
						resolved = true
						break
					}
					// Also check selectCols name for non-alias columns
					if j < len(selectCols) && strings.EqualFold(selectCols[j].name, ident.Name) && selectCols[j].index >= 0 {
						groupBySpecs[i] = groupBySpec{index: selectCols[j].index}
						resolved = true
						break
					}
				}
				if !resolved {
					groupBySpecs[i] = groupBySpec{index: -1, expr: gb}
				}
			}
		} else {
			groupBySpecs[i] = groupBySpec{index: -1, expr: gb}
		}
	}

	// Group rows by GROUP BY columns
	// key is string representation of group values, value is slice of rows
	groups := make(map[string][][]interface{})
	groupOrder := []string{} // preserve insertion order

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	for iter.HasNext() {
		_, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode full row
		fullRow, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present (filters rows before grouping)
		if stmt.Where != nil {
			matched, err := evaluateWhere(c, fullRow, table.Columns, stmt.Where, args)
			if err != nil {
				continue
			}
			if !matched {
				continue
			}
		}

		// Build group key
		var groupKey strings.Builder
		for i, spec := range groupBySpecs {
			if i > 0 {
				groupKey.WriteString("|")
			}
			if spec.index >= 0 && spec.index < len(fullRow) {
				groupKey.WriteString(fmt.Sprintf("%v", fullRow[spec.index]))
			} else if spec.expr != nil {
				val, err := evaluateExpression(c, fullRow, table.Columns, spec.expr, args)
				if err == nil {
					groupKey.WriteString(fmt.Sprintf("%v", val))
				}
			}
		}

		// Add row to appropriate group
		key := groupKey.String()
		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], fullRow)
	}

	// Compute aggregates for each group
	var resultRows [][]interface{}

	// If no groups (empty table) and no GROUP BY clause, we still need to return
	// a single row with aggregate results (e.g., COUNT(*) = 0)
	if len(groups) == 0 && len(stmt.GroupBy) == 0 && len(selectCols) > 0 {
		resultRow := make([]interface{}, len(selectCols))
		hasAggregate := false
		for i, ci := range selectCols {
			if ci.isAggregate {
				hasAggregate = true
				switch ci.aggregateType {
				case "COUNT":
					resultRow[i] = int64(0)
				case "SUM", "AVG", "MIN", "MAX":
					resultRow[i] = nil // NULL for empty set
				}
			} else if ci.hasEmbeddedAgg {
				hasAggregate = true
				// Evaluate expression with empty group (aggregates return 0/NULL)
				val, err := c.evaluateExprWithGroupAggregates(ci.originalExpr, nil, table, args)
				if err == nil {
					resultRow[i] = val
				}
			}
		}
		if hasAggregate {
			// Apply HAVING clause even for empty-table aggregate
			if stmt.Having != nil {
				havingMatched, err := evaluateHaving(c, resultRow, selectCols, table.Columns, stmt.Having, args)
				if err == nil && havingMatched {
					resultRows = append(resultRows, resultRow)
				}
			} else {
				resultRows = append(resultRows, resultRow)
			}
		}
	}

	for _, gk := range groupOrder {
		groupRows := groups[gk]
		resultRow := make([]interface{}, len(selectCols))

		for i, ci := range selectCols {
			if ci.isAggregate {
				// Collect values for this aggregate
				var values []interface{}
				for _, row := range groupRows {
					if ci.aggregateCol == "*" && ci.aggregateExpr == nil {
						// COUNT(*): just count rows
						values = append(values, int64(1))
					} else if ci.aggregateExpr != nil {
						// Expression argument (e.g., SUM(quantity * price))
						v, err := evaluateExpression(c, row, table.Columns, ci.aggregateExpr, args)
						if err == nil {
							values = append(values, v)
						}
					} else {
						colIdx := table.GetColumnIndex(ci.aggregateCol)
						if colIdx >= 0 && colIdx < len(row) {
							values = append(values, row[colIdx])
						}
					}
				}

				// Compute aggregate
				switch ci.aggregateType {
				case "COUNT":
					if ci.aggregateCol == "*" && !ci.isDistinct {
						resultRow[i] = int64(len(groupRows))
					} else if ci.isDistinct {
						// Count distinct non-null values
						seen := make(map[string]bool)
						for _, v := range values {
							if v != nil {
								seen[fmt.Sprintf("%v", v)] = true
							}
						}
						resultRow[i] = int64(len(seen))
					} else {
						count := int64(0)
						for _, v := range values {
							if v != nil {
								count++
							}
						}
						resultRow[i] = count
					}
				case "SUM":
					var sum float64
					hasVal := false
					for _, v := range values {
						if v != nil {
							if f, ok := toFloat64(v); ok {
								sum += f
								hasVal = true
							}
						}
					}
					if hasVal {
						resultRow[i] = sum
					} else {
						resultRow[i] = nil
					}
				case "AVG":
					var sum float64
					var count int64
					for _, v := range values {
						if v != nil {
							if f, ok := toFloat64(v); ok {
								sum += f
								count++
							}
						}
					}
					if count > 0 {
						resultRow[i] = sum / float64(count)
					} else {
						resultRow[i] = nil
					}
				case "MIN":
					var minVal interface{}
					for _, v := range values {
						if v != nil {
							if minVal == nil || compareValues(v, minVal) < 0 {
								minVal = v
							}
						}
					}
					resultRow[i] = minVal
				case "MAX":
					var maxVal interface{}
					for _, v := range values {
						if v != nil {
							if maxVal == nil || compareValues(v, maxVal) > 0 {
								maxVal = v
							}
						}
					}
					resultRow[i] = maxVal
				case "GROUP_CONCAT":
					var parts []string
					for _, v := range values {
						if v != nil {
							parts = append(parts, fmt.Sprintf("%v", v))
						}
					}
					if len(parts) > 0 {
						resultRow[i] = strings.Join(parts, ",")
					} else {
						resultRow[i] = nil
					}
				}
			} else {
				// Non-aggregate column - get value from first row in group
				if ci.hasEmbeddedAgg && len(groupRows) > 0 {
					// Expression (CASE, etc.) with embedded aggregates
					// Pre-compute aggregates over the group, then evaluate expression
					val, err := c.evaluateExprWithGroupAggregates(ci.originalExpr, groupRows, table, args)
					if err == nil {
						resultRow[i] = val
					}
				} else if ci.index >= 0 && len(groupRows) > 0 && ci.index < len(groupRows[0]) {
					resultRow[i] = groupRows[0][ci.index]
				} else if ci.index == -1 && len(groupRows) > 0 {
					// Expression column (CASE, CAST, etc.) - evaluate it
					if i < len(stmt.Columns) {
						expr := stmt.Columns[i]
						if ae, ok := expr.(*query.AliasExpr); ok {
							expr = ae.Expr
						}
						val, err := evaluateExpression(c, groupRows[0], table.Columns, expr, args)
						if err == nil {
							resultRow[i] = val
						}
					}
				}
			}
		}

		// Apply HAVING clause if present
		if stmt.Having != nil {
			// Create a temporary row with column values for evaluation
			// We need to create a virtual row that has the right column structure
			havingMatched, err := evaluateHaving(c, resultRow, selectCols, table.Columns, stmt.Having, args)
			if err != nil || !havingMatched {
				continue
			}
		}

		resultRows = append(resultRows, resultRow)
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		resultRows = c.applyGroupByOrderBy(resultRows, selectCols, stmt.OrderBy)
	}

	// Apply DISTINCT if present
	if stmt.Distinct {
		resultRows = c.applyDistinct(resultRows)
	}

	// Apply OFFSET if present
	if stmt.Offset != nil {
		offsetVal, err := evaluateExpression(c, nil, nil, stmt.Offset, args)
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

	// Apply LIMIT if present
	if stmt.Limit != nil {
		limitVal, err := evaluateExpression(c, nil, nil, stmt.Limit, args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(resultRows) {
				resultRows = resultRows[:limit]
			}
		}
	}

	return returnColumns, resultRows, nil
}

// evaluateHaving evaluates a HAVING clause against a group result row
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

// resolveAggregateInExpr replaces aggregate function calls with their computed values
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

// valueToLiteral converts a Go value to the appropriate AST literal expression
func valueToLiteral(v interface{}) query.Expression {
	if v == nil {
		return &query.NullLiteral{}
	}
	switch val := v.(type) {
	case string:
		return &query.StringLiteral{Value: val}
	case bool:
		return &query.BooleanLiteral{Value: val}
	default:
		_ = val
		return &query.NumberLiteral{Value: toNumber(v)}
	}
}

// toNumber converts a value to a number for comparison
func toNumber(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case string:
		n, _ := strconv.ParseFloat(val, 64)
		return n
	default:
		return 0
	}
}

// toBool converts a value to a boolean
func toBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int:
		return val != 0
	case int64:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != ""
	default:
		return false
	}
}

// toBoolNullable converts a value to boolean with NULL awareness
// Returns (value, isNil) for SQL three-valued logic
func toBoolNullable(v interface{}) (bool, bool) {
	if v == nil {
		return false, true
	}
	return toBool(v), false
}

// evaluateExprWithGroupAggregates evaluates an expression (e.g. CASE WHEN SUM(x)>100 THEN 'high' ELSE 'low' END)
// by first computing any aggregate functions over the group rows, then substituting their values.
func (c *Catalog) evaluateExprWithGroupAggregates(expr query.Expression, groupRows [][]interface{}, table *TableDef, args []interface{}) (interface{}, error) {
	// Collect all aggregate function calls from the expression
	var aggCalls []*query.FunctionCall
	collectAggregatesFromExpr(expr, &aggCalls)

	// Compute each aggregate over the group rows
	aggResults := make(map[*query.FunctionCall]interface{})
	for _, fc := range aggCalls {
		funcName := strings.ToUpper(fc.Name)
		var values []interface{}
		for _, row := range groupRows {
			if len(fc.Args) == 0 || isStarArg(fc.Args[0]) {
				values = append(values, int64(1))
			} else {
				v, err := evaluateExpression(c, row, table.Columns, fc.Args[0], args)
				if err == nil {
					values = append(values, v)
				}
			}
		}

		switch funcName {
		case "COUNT":
			if len(fc.Args) == 0 || isStarArg(fc.Args[0]) {
				aggResults[fc] = int64(len(groupRows))
			} else {
				count := int64(0)
				for _, v := range values {
					if v != nil {
						count++
					}
				}
				aggResults[fc] = count
			}
		case "SUM":
			var sum float64
			hasVal := false
			for _, v := range values {
				if v != nil {
					if f, ok := toFloat64(v); ok {
						sum += f
						hasVal = true
					}
				}
			}
			if hasVal {
				aggResults[fc] = sum
			} else {
				aggResults[fc] = nil
			}
		case "AVG":
			var sum float64
			var count int64
			for _, v := range values {
				if v != nil {
					if f, ok := toFloat64(v); ok {
						sum += f
						count++
					}
				}
			}
			if count > 0 {
				aggResults[fc] = sum / float64(count)
			} else {
				aggResults[fc] = nil
			}
		case "MIN":
			var minVal interface{}
			for _, v := range values {
				if v != nil {
					if minVal == nil || compareValues(v, minVal) < 0 {
						minVal = v
					}
				}
			}
			aggResults[fc] = minVal
		case "MAX":
			var maxVal interface{}
			for _, v := range values {
				if v != nil {
					if maxVal == nil || compareValues(v, maxVal) > 0 {
						maxVal = v
					}
				}
			}
			aggResults[fc] = maxVal
		}
	}

	// Replace aggregate calls in expression with their computed values, then evaluate
	replaced := replaceAggregatesInExpr(expr, aggResults)
	var baseRow []interface{}
	var baseCols []ColumnDef
	if len(groupRows) > 0 {
		baseRow = groupRows[0]
		baseCols = table.Columns
	}
	return evaluateExpression(c, baseRow, baseCols, replaced, args)
}

// evaluateExprWithGroupAggregatesJoin is like evaluateExprWithGroupAggregates but for JOIN context
// where columns come from multiple tables combined into allColumns.
func (c *Catalog) evaluateExprWithGroupAggregatesJoin(expr query.Expression, groupRows [][]interface{}, allColumns []ColumnDef, args []interface{}) (interface{}, error) {
	var aggCalls []*query.FunctionCall
	collectAggregatesFromExpr(expr, &aggCalls)

	aggResults := make(map[*query.FunctionCall]interface{})
	for _, fc := range aggCalls {
		funcName := strings.ToUpper(fc.Name)
		var values []interface{}
		for _, row := range groupRows {
			if len(fc.Args) == 0 || isStarArg(fc.Args[0]) {
				values = append(values, int64(1))
			} else {
				v, err := evaluateExpression(c, row, allColumns, fc.Args[0], args)
				if err == nil {
					values = append(values, v)
				}
			}
		}

		switch funcName {
		case "COUNT":
			if len(fc.Args) == 0 || isStarArg(fc.Args[0]) {
				aggResults[fc] = int64(len(groupRows))
			} else {
				count := int64(0)
				for _, v := range values {
					if v != nil {
						count++
					}
				}
				aggResults[fc] = count
			}
		case "SUM":
			var sum float64
			hasVal := false
			for _, v := range values {
				if v != nil {
					if f, ok := toFloat64(v); ok {
						sum += f
						hasVal = true
					}
				}
			}
			if hasVal {
				aggResults[fc] = sum
			} else {
				aggResults[fc] = nil
			}
		case "AVG":
			var sum float64
			var count int64
			for _, v := range values {
				if v != nil {
					if f, ok := toFloat64(v); ok {
						sum += f
						count++
					}
				}
			}
			if count > 0 {
				aggResults[fc] = sum / float64(count)
			} else {
				aggResults[fc] = nil
			}
		case "MIN":
			var minVal interface{}
			for _, v := range values {
				if v != nil {
					if minVal == nil || compareValues(v, minVal) < 0 {
						minVal = v
					}
				}
			}
			aggResults[fc] = minVal
		case "MAX":
			var maxVal interface{}
			for _, v := range values {
				if v != nil {
					if maxVal == nil || compareValues(v, maxVal) > 0 {
						maxVal = v
					}
				}
			}
			aggResults[fc] = maxVal
		}
	}

	replaced := replaceAggregatesInExpr(expr, aggResults)
	var baseRow []interface{}
	if len(groupRows) > 0 {
		baseRow = groupRows[0]
	}
	return evaluateExpression(c, baseRow, allColumns, replaced, args)
}

// isStarArg checks if an expression is a StarExpr (for COUNT(*))
func isStarArg(e query.Expression) bool {
	_, ok := e.(*query.StarExpr)
	return ok
}

// replaceAggregatesInExpr replaces FunctionCall nodes that are aggregates with their pre-computed literal values
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

// applyGroupByOrderBy applies ORDER BY to GROUP BY results
func (c *Catalog) applyGroupByOrderBy(rows [][]interface{}, selectCols []selectColInfo, orderBy []*query.OrderByExpr) [][]interface{} {
	if len(rows) == 0 || len(orderBy) == 0 {
		return rows
	}

	// Build a map from column name to selectCols index
	nameToIndex := make(map[string]int)
	for i, ci := range selectCols {
		nameToIndex[strings.ToUpper(ci.name)] = i
		// For aggregates, also add the signature format
		if ci.isAggregate {
			sig := strings.ToUpper(ci.aggregateType + "(" + ci.aggregateCol + ")")
			nameToIndex[sig] = i
		}
	}

	sorted := make([][]interface{}, len(rows))
	copy(sorted, rows)

	sort.Slice(sorted, func(i, j int) bool {
		for _, ob := range orderBy {
			// Get the column name from the ORDER BY expression
			var colName string
			var exprArgMatch query.Expression // for expression-arg aggregates
			if ident, ok := ob.Expr.(*query.Identifier); ok {
				colName = ident.Name
			} else if fn, ok := ob.Expr.(*query.FunctionCall); ok {
				// Handle aggregate in ORDER BY
				colName = fn.Name + "("
				if len(fn.Args) > 0 {
					switch arg := fn.Args[0].(type) {
					case *query.Identifier:
						colName += arg.Name + ")"
					case *query.QualifiedIdentifier:
						colName += arg.Column + ")"
					case *query.StarExpr:
						colName += "*)"
					default:
						// Expression argument (e.g., SUM(price * quantity))
						// Use "*)" for name matching but also store for direct comparison
						colName += "*)"
						exprArgMatch = fn.Args[0]
					}
				} else {
					colName += "*)"
				}
			} else if qi, ok := ob.Expr.(*query.QualifiedIdentifier); ok {
				colName = qi.Column
			} else if nl, ok := ob.Expr.(*query.NumberLiteral); ok {
				// Positional ORDER BY (ORDER BY 1, 2, etc.)
				pos := int(nl.Value) - 1 // 1-based to 0-based
				if pos >= 0 && pos < len(selectCols) {
					vi := sorted[i][pos]
					vj := sorted[j][pos]
					if vi == nil && vj == nil {
						continue
					}
					if vi == nil {
						return !ob.Desc
					}
					if vj == nil {
						return ob.Desc
					}
					viF, viNum := toFloat64(vi)
					vjF, vjNum := toFloat64(vj)
					if viNum && vjNum {
						if viF < vjF {
							return !ob.Desc
						} else if viF > vjF {
							return ob.Desc
						}
						continue
					}
					viS := fmt.Sprintf("%v", vi)
					vjS := fmt.Sprintf("%v", vj)
					if viS < vjS {
						return !ob.Desc
					} else if viS > vjS {
						return ob.Desc
					}
				}
				continue
			}

			idx, ok := nameToIndex[strings.ToUpper(colName)]
			if !ok {
				continue
			}

			// For expression-arg aggregates, verify we found the right one
			// (multiple aggregates could share "SUM(*)" name but have different expressions)
			if exprArgMatch != nil {
				// Try to find exact match by aggregateExpr
				foundExact := false
				for k, ci := range selectCols {
					if ci.isAggregate && ci.aggregateExpr == exprArgMatch {
						idx = k
						foundExact = true
						break
					}
				}
				if !foundExact {
					// Fallback: use the name-matched index
				}
			}

			// Compare values
			vi := sorted[i][idx]
			vj := sorted[j][idx]

			// Handle nil values
			if vi == nil && vj == nil {
				continue
			}
			if vi == nil {
				return !ob.Desc
			}
			if vj == nil {
				return ob.Desc
			}

			// Compare based on type
			viF, viNum := toFloat64(vi)
			vjF, vjNum := toFloat64(vj)
			if viNum && vjNum {
				if viF < vjF {
					return !ob.Desc
				} else if viF > vjF {
					return ob.Desc
				}
				continue
			}
			switch vi.(type) {
			case string:
				viS := vi.(string)
				vjS := vj.(string)
				if viS < vjS {
					return !ob.Desc
				} else if viS > vjS {
					return ob.Desc
				}
			}
		}
		return false
	})

	return sorted
}

// resolvePositionalRefs resolves positional references (numeric literals) in GROUP BY and ORDER BY.
// For example, GROUP BY 1 becomes GROUP BY <first_select_column_expression>.
// ORDER BY 2 DESC becomes ORDER BY <second_select_column_expression> DESC.
// This follows SQL standard behavior where integers in GROUP BY/ORDER BY reference
// SELECT column positions (1-based).
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

// resolveOuterRefsInQuery resolves outer column references in a correlated subquery.
// It replaces QualifiedIdentifier and Identifier references that refer to outer row columns
// with their actual literal values, enabling correlated subqueries.
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

// valueToExpr converts a Go value to an AST literal expression
func valueToExpr(val interface{}) query.Expression {
	if val == nil {
		return &query.NullLiteral{}
	}
	switch v := val.(type) {
	case string:
		return &query.StringLiteral{Value: v}
	case float64:
		return &query.NumberLiteral{Value: v}
	case int:
		return &query.NumberLiteral{Value: float64(v)}
	case int64:
		return &query.NumberLiteral{Value: float64(v)}
	case bool:
		if v {
			return &query.NumberLiteral{Value: 1}
		}
		return &query.NumberLiteral{Value: 0}
	default:
		return &query.StringLiteral{Value: fmt.Sprintf("%v", v)}
	}
}

// resolveOuterRefsInExpr walks an expression tree and replaces outer column references with literals
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

// evaluateWhere evaluates a WHERE clause against a row
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

// evaluateExpression evaluates an expression against a row
func evaluateExpression(c *Catalog, row []interface{}, columns []ColumnDef, expr query.Expression, args []interface{}) (interface{}, error) {
	switch e := expr.(type) {
	case *query.BinaryExpr:
		return evaluateBinaryExpr(c, row, columns, e, args)
	case *query.Identifier:
		// Check if this is a dotted identifier like "table.column"
		if dotIdx := strings.IndexByte(e.Name, '.'); dotIdx > 0 && dotIdx < len(e.Name)-1 {
			// Treat as QualifiedIdentifier
			return evaluateExpression(c, row, columns, &query.QualifiedIdentifier{
				Table:  e.Name[:dotIdx],
				Column: e.Name[dotIdx+1:],
			}, args)
		}
		// Find column value (case-insensitive)
		nameLower := strings.ToLower(e.Name)
		for i, col := range columns {
			if strings.ToLower(col.Name) == nameLower && i < len(row) {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column not found: %s", e.Name)
	case *query.PlaceholderExpr:
		if e.Index < len(args) {
			return args[e.Index], nil
		}
		return nil, fmt.Errorf("placeholder index out of range")
	case *query.StringLiteral:
		return e.Value, nil
	case *query.NumberLiteral:
		return e.Value, nil
	case *query.BooleanLiteral:
		return e.Value, nil
	case *query.NullLiteral:
		return nil, nil
	case *query.QualifiedIdentifier:
		// table.column format - prefer exact table match using sourceTbl (case-insensitive)
		colLower := strings.ToLower(e.Column)
		tblLower := strings.ToLower(e.Table)
		for i, col := range columns {
			if strings.ToLower(col.Name) == colLower && strings.ToLower(col.sourceTbl) == tblLower && i < len(row) {
				return row[i], nil
			}
		}
		// Fallback: match by column name only (for non-JOIN contexts)
		for i, col := range columns {
			if strings.ToLower(col.Name) == colLower && i < len(row) {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column not found: %s.%s", e.Table, e.Column)
	case *query.LikeExpr:
		return evaluateLike(c, row, columns, e, args)
	case *query.InExpr:
		return evaluateIn(c, row, columns, e, args)
	case *query.BetweenExpr:
		return evaluateBetween(c, row, columns, e, args)
	case *query.IsNullExpr:
		return evaluateIsNull(c, row, columns, e, args)
	case *query.FunctionCall:
		return evaluateFunctionCall(c, row, columns, e, args)
	case *query.AliasExpr:
		// Unwrap alias and evaluate the underlying expression
		return evaluateExpression(c, row, columns, e.Expr, args)
	case *query.CaseExpr:
		return evaluateCaseExpr(c, row, columns, e, args)
	case *query.CastExpr:
		return evaluateCastExpr(c, row, columns, e, args)
	case *query.SubqueryExpr:
		// Scalar subquery: execute and return first column of first row
		// Support correlated subqueries by resolving outer references
		subq := resolveOuterRefsInQuery(e.Query, row, columns)
		cols, rows, err := c.selectLocked(subq, args)
		if err != nil {
			return nil, err
		}
		_ = cols
		if len(rows) == 0 || len(rows[0]) == 0 {
			return nil, nil
		}
		if len(rows) > 1 {
			return nil, fmt.Errorf("scalar subquery returned %d rows instead of 1", len(rows))
		}
		return rows[0][0], nil
	case *query.ExistsExpr:
		// Support correlated subqueries by resolving outer references
		subq := resolveOuterRefsInQuery(e.Subquery, row, columns)
		_, rows, err := c.selectLocked(subq, args)
		if err != nil {
			return nil, err
		}
		exists := len(rows) > 0
		if e.Not {
			return !exists, nil
		}
		return exists, nil
	case *query.UnaryExpr:
		val, err := evaluateExpression(c, row, columns, e.Expr, args)
		if err != nil {
			return nil, err
		}
		switch e.Operator {
		case query.TokenMinus:
			if f, ok := toFloat64(val); ok {
				if _, isInt := val.(int); isInt {
					return int(-f), nil
				}
				if _, isInt64 := val.(int64); isInt64 {
					return int64(-f), nil
				}
				return -f, nil
			}
			return nil, fmt.Errorf("cannot negate non-numeric value")
		case query.TokenNot:
			if val == nil {
				return nil, nil // NOT NULL = NULL per SQL three-valued logic
			}
			return !toBool(val), nil
		}
		return val, nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evaluateBinaryExpr evaluates a binary expression
func evaluateBinaryExpr(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.BinaryExpr, args []interface{}) (interface{}, error) {
	left, err := evaluateExpression(c, row, columns, expr.Left, args)
	if err != nil {
		return nil, err
	}

	right, err := evaluateExpression(c, row, columns, expr.Right, args)
	if err != nil {
		return nil, err
	}

	// Handle logical operators first (they have special NULL semantics per SQL standard)
	// SQL three-valued logic: NULL AND false = false, NULL OR true = true,
	// NULL AND true = NULL, NULL OR false = NULL
	switch expr.Operator {
	case query.TokenAnd:
		leftBool, leftIsNil := toBoolNullable(left)
		rightBool, rightIsNil := toBoolNullable(right)
		if (!leftIsNil && !leftBool) || (!rightIsNil && !rightBool) {
			return false, nil // false AND anything = false
		}
		if leftIsNil || rightIsNil {
			return nil, nil // NULL AND true = NULL
		}
		return leftBool && rightBool, nil
	case query.TokenOr:
		leftBool, leftIsNil := toBoolNullable(left)
		rightBool, rightIsNil := toBoolNullable(right)
		if (!leftIsNil && leftBool) || (!rightIsNil && rightBool) {
			return true, nil // true OR anything = true
		}
		if leftIsNil || rightIsNil {
			return nil, nil // NULL OR false = NULL
		}
		return leftBool || rightBool, nil
	}

	// Handle NULL comparisons (for non-logical operators)
	if left == nil || right == nil {
		switch expr.Operator {
		case query.TokenIs:
			// IS NULL - true if both are nil
			// IS NOT NULL - true if either is not nil
			if rightVal, ok := right.(bool); ok {
				if rightVal {
					return left == nil, nil
				}
				return left != nil, nil
			}
		case query.TokenEq:
			// SQL standard: NULL = anything (including NULL) is NULL (unknown)
			return nil, nil
		case query.TokenNeq:
			// SQL standard: NULL != anything (including NULL) is NULL (unknown)
			return nil, nil
		}
		return nil, nil // NULL comparison returns NULL per SQL standard
	}

	// Handle arithmetic operators (+, -, *, /)
	switch expr.Operator {
	case query.TokenPlus:
		return addValues(left, right)
	case query.TokenMinus:
		return subtractValues(left, right)
	case query.TokenStar:
		return multiplyValues(left, right)
	case query.TokenSlash:
		return divideValues(left, right)
	case query.TokenPercent:
		return moduloValues(left, right)
	case query.TokenConcat:
		return fmt.Sprintf("%v%v", left, right), nil
	}

	// Compare based on operator
	switch expr.Operator {
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
	default:
		return false, fmt.Errorf("unsupported operator: %v", expr.Operator)
	}
}

// compareValues compares two values
func compareValues(a, b interface{}) int {
	// Handle NULLs: NULLs sort last (after all non-NULL values)
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1 // NULL sorts after non-NULL
	}
	if b == nil {
		return -1 // non-NULL sorts before NULL
	}

	// Handle numeric types
	aNum, aIsNum := toFloat64(a)
	bNum, bIsNum := toFloat64(b)
	if aIsNum && bIsNum {
		if aNum < bNum {
			return -1
		}
		if aNum > bNum {
			return 1
		}
		return 0
	}

	// Handle strings
	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
	if aIsStr && bIsStr {
		if aStr < bStr {
			return -1
		}
		if aStr > bStr {
			return 1
		}
		return 0
	}

	// Fallback to string comparison
	return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
}

// addValues adds two numeric values
// isIntegerType checks if a value is an integer type (int, int64, or float64 with whole value from JSON)
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

// subtractValues subtracts two numeric values
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

// multiplyValues multiplies two numeric values
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

// divideValues divides two numeric values
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

// moduloValues computes the modulo of two numeric values
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

// evaluateLike evaluates a LIKE expression (column LIKE pattern)
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

// evaluateIsNull evaluates IS NULL / IS NOT NULL expression
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

// matchLikeSimple implements SQL LIKE matching (case-insensitive)
// Supports: % (any sequence), _ (single character), escape character
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

// evaluateIn evaluates an IN expression (column IN (1, 2, 3))
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

// evaluateCaseExpr evaluates a CASE expression
func evaluateCaseExpr(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.CaseExpr, args []interface{}) (interface{}, error) {
	if expr.Expr != nil {
		// Simple CASE: CASE expr WHEN val1 THEN result1 WHEN val2 THEN result2 ELSE default END
		baseVal, err := evaluateExpression(c, row, columns, expr.Expr, args)
		if err != nil {
			return nil, err
		}
		// Per SQL standard, CASE NULL WHEN NULL is UNKNOWN (not true)
		// If base value is NULL, skip all WHEN comparisons and fall through to ELSE
		if baseVal != nil {
			for _, when := range expr.Whens {
				whenVal, err := evaluateExpression(c, row, columns, when.Condition, args)
				if err != nil {
					return nil, err
				}
				if whenVal != nil && compareValues(baseVal, whenVal) == 0 {
					return evaluateExpression(c, row, columns, when.Result, args)
				}
			}
		}
	} else {
		// Searched CASE: CASE WHEN cond1 THEN result1 WHEN cond2 THEN result2 ELSE default END
		for _, when := range expr.Whens {
			condVal, err := evaluateExpression(c, row, columns, when.Condition, args)
			if err != nil {
				return nil, err
			}
			if toBool(condVal) {
				return evaluateExpression(c, row, columns, when.Result, args)
			}
		}
	}
	if expr.Else != nil {
		return evaluateExpression(c, row, columns, expr.Else, args)
	}
	return nil, nil
}

// evaluateCastExpr evaluates a CAST expression
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

// evaluateBetween evaluates a BETWEEN expression (column BETWEEN 1 AND 10)
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

// evaluateFunctionCall evaluates scalar functions
func evaluateFunctionCall(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.FunctionCall, args []interface{}) (interface{}, error) {
	funcName := strings.ToUpper(expr.Name)

	// Short-circuit evaluation for COALESCE/IFNULL - evaluate lazily
	if funcName == "COALESCE" || funcName == "IFNULL" {
		for _, arg := range expr.Args {
			val, err := evaluateExpression(c, row, columns, arg, args)
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
		}
		return nil, nil
	}

	// Evaluate arguments first (eager for all other functions)
	evalArgs := make([]interface{}, len(expr.Args))
	for i, arg := range expr.Args {
		val, err := evaluateExpression(c, row, columns, arg, args)
		if err != nil {
			return nil, err
		}
		evalArgs[i] = val
	}

	switch funcName {
	case "LENGTH", "LEN":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("LENGTH requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		return float64(len(str)), nil

	case "UPPER":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("UPPER requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		return strings.ToUpper(str), nil

	case "LOWER":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("LOWER requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		return strings.ToLower(str), nil

	case "TRIM", "LTRIM", "RTRIM":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("%s requires at least 1 argument", funcName)
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		// Optional second arg: characters to trim (default: whitespace)
		trimChars := " \t\n\r"
		if len(evalArgs) >= 2 && evalArgs[1] != nil {
			trimChars = fmt.Sprintf("%v", evalArgs[1])
		}
		switch funcName {
		case "LTRIM":
			return strings.TrimLeft(str, trimChars), nil
		case "RTRIM":
			return strings.TrimRight(str, trimChars), nil
		default:
			return strings.Trim(str, trimChars), nil
		}

	case "SUBSTR", "SUBSTRING":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("SUBSTR requires at least 2 arguments")
		}
		// SQL standard: any NULL argument returns NULL
		if evalArgs[0] == nil || evalArgs[1] == nil {
			return nil, nil
		}
		if len(evalArgs) >= 3 && evalArgs[2] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		start, _ := toFloat64(evalArgs[1])
		startInt := int(start) - 1 // SQL SUBSTR is 1-indexed
		if startInt < 0 {
			startInt = 0
		}
		if startInt >= len(str) {
			return "", nil
		}
		if len(evalArgs) >= 3 {
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

	case "CONCAT":
		var result strings.Builder
		for _, arg := range evalArgs {
			if arg != nil {
				result.WriteString(fmt.Sprintf("%v", arg))
			}
		}
		return result.String(), nil

	case "ABS":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("ABS requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			if f < 0 {
				return -f, nil
			}
			return f, nil
		}
		return evalArgs[0], nil

	case "ROUND":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("ROUND requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		f, ok := toFloat64(evalArgs[0])
		if !ok {
			return evalArgs[0], nil
		}
		precision := 0
		if len(evalArgs) >= 2 {
			if p, ok := toFloat64(evalArgs[1]); ok {
				precision = int(p)
			}
		}
		divisor := 1.0
		for i := 0; i < precision; i++ {
			divisor *= 10
		}
		result := math.Round(f*divisor) / divisor
		if precision == 0 {
			return float64(int64(result)), nil
		}
		return result, nil

	case "FLOOR":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("FLOOR requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return math.Floor(f), nil
		}
		return evalArgs[0], nil

	case "CEIL", "CEILING":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("CEIL requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return math.Ceil(f), nil
		}
		return evalArgs[0], nil

	case "COALESCE", "IFNULL":
		// Handled above with short-circuit evaluation
		return nil, nil

	case "NULLIF":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("NULLIF requires 2 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil {
			return evalArgs[0], nil
		}
		if compareValues(evalArgs[0], evalArgs[1]) == 0 {
			return nil, nil
		}
		return evalArgs[0], nil

	case "REPLACE":
		if len(evalArgs) < 3 {
			return nil, fmt.Errorf("REPLACE requires 3 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil || evalArgs[2] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		old, ok2 := evalArgs[1].(string)
		if !ok2 {
			old = fmt.Sprintf("%v", evalArgs[1])
		}
		if old == "" {
			return str, nil
		}
		newStr, ok3 := evalArgs[2].(string)
		if !ok3 {
			newStr = fmt.Sprintf("%v", evalArgs[2])
		}
		return strings.ReplaceAll(str, old, newStr), nil

	case "INSTR":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("INSTR requires 2 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil {
			return nil, nil // SQL standard: NULL input returns NULL
		}
		haystack, ok := evalArgs[0].(string)
		if !ok {
			haystack = fmt.Sprintf("%v", evalArgs[0])
		}
		needle, ok := evalArgs[1].(string)
		if !ok {
			needle = fmt.Sprintf("%v", evalArgs[1])
		}
		idx := strings.Index(haystack, needle)
		if idx < 0 {
			return float64(0), nil
		}
		return float64(idx + 1), nil

	case "PRINTF":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("PRINTF requires at least 1 argument")
		}
		format, ok := evalArgs[0].(string)
		if !ok {
			format = fmt.Sprintf("%v", evalArgs[0])
		}
		// Simple printf implementation
		var result strings.Builder
		argIndex := 1
		i := 0
		for i < len(format) {
			if format[i] == '%' && i+1 < len(format) {
				nextChar := format[i+1]
				switch nextChar {
				case 's':
					if argIndex < len(evalArgs) {
						result.WriteString(fmt.Sprintf("%v", evalArgs[argIndex]))
						argIndex++
					}
					i += 2
				case 'd', 'i':
					if argIndex < len(evalArgs) {
						if f, ok := toFloat64(evalArgs[argIndex]); ok {
							result.WriteString(fmt.Sprintf("%d", int64(f)))
						}
						argIndex++
					}
					i += 2
				case 'f':
					if argIndex < len(evalArgs) {
						if f, ok := toFloat64(evalArgs[argIndex]); ok {
							result.WriteString(fmt.Sprintf("%f", f))
						}
						argIndex++
					}
					i += 2
				default:
					result.WriteByte(format[i])
					i++
				}
			} else {
				result.WriteByte(format[i])
				i++
			}
		}
		return result.String(), nil

	case "DATE", "TIME", "DATETIME":
		// Simple date/time functions - return current time for now
		// Full implementation would require time parsing
		if len(evalArgs) < 1 {
			return nil, nil
		}
		return evalArgs[0], nil

	case "NOW", "CURRENT_TIMESTAMP", "CURRENT_TIME", "CURRENT_DATE":
		// Return current timestamp
		now := time.Now()
		return now.Format("2006-01-02 15:04:05"), nil

	case "STRFTIME":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("STRFTIME requires 2 arguments")
		}
		// Simple strftime - just return the input for now
		if evalArgs[1] == nil {
			return nil, nil
		}
		return fmt.Sprintf("%v", evalArgs[1]), nil

	case "CAST":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("CAST requires 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		targetType, ok := evalArgs[1].(string)
		if !ok {
			targetType = strings.ToUpper(fmt.Sprintf("%v", evalArgs[1]))
		}
		switch targetType {
		case "INTEGER", "INT":
			if f, ok := toFloat64(evalArgs[0]); ok {
				return int64(f), nil
			}
			if s, ok := evalArgs[0].(string); ok {
				var i int64
				fmt.Sscanf(s, "%d", &i)
				return i, nil
			}
			if b, ok := evalArgs[0].(bool); ok {
				if b {
					return int64(1), nil
				}
				return int64(0), nil
			}
		case "REAL", "FLOAT":
			if f, ok := toFloat64(evalArgs[0]); ok {
				return f, nil
			}
			if s, ok := evalArgs[0].(string); ok {
				var f float64
				fmt.Sscanf(s, "%f", &f)
				return f, nil
			}
		case "TEXT", "STRING":
			return fmt.Sprintf("%v", evalArgs[0]), nil
		case "BOOLEAN", "BOOL":
			if b, ok := evalArgs[0].(bool); ok {
				return b, nil
			}
			if f, ok := toFloat64(evalArgs[0]); ok {
				return f != 0, nil
			}
			if s, ok := evalArgs[0].(string); ok {
				return strings.ToLower(s) == "true" || s == "1", nil
			}
		}
		return evalArgs[0], nil

	case "CONCAT_WS":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("CONCAT_WS requires at least 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		separator := fmt.Sprintf("%v", evalArgs[0])
		var parts []string
		for _, arg := range evalArgs[1:] {
			if arg != nil {
				parts = append(parts, fmt.Sprintf("%v", arg))
			}
		}
		return strings.Join(parts, separator), nil

	case "GROUP_CONCAT":
		// GROUP_CONCAT is handled in aggregate path; scalar fallback just returns the value
		if len(evalArgs) >= 1 && evalArgs[0] != nil {
			return fmt.Sprintf("%v", evalArgs[0]), nil
		}
		return nil, nil

	case "REVERSE":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("REVERSE requires 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		runes := []rune(str)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil

	case "REPEAT":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("REPEAT requires 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		count, _ := toFloat64(evalArgs[1])
		if count <= 0 {
			return "", nil
		}
		return strings.Repeat(str, int(count)), nil

	case "LEFT":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("LEFT requires 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		n, _ := toFloat64(evalArgs[1])
		ni := int(n)
		if ni <= 0 {
			return "", nil
		}
		if ni >= len(str) {
			return str, nil
		}
		return str[:ni], nil

	case "RIGHT":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("RIGHT requires 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		n, _ := toFloat64(evalArgs[1])
		ni := int(n)
		if ni <= 0 {
			return "", nil
		}
		if ni >= len(str) {
			return str, nil
		}
		return str[len(str)-ni:], nil

	case "LPAD":
		if len(evalArgs) < 3 {
			return nil, fmt.Errorf("LPAD requires 3 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		targetLen, _ := toFloat64(evalArgs[1])
		pad := fmt.Sprintf("%v", evalArgs[2])
		ti := int(targetLen)
		if len(str) >= ti {
			return str[:ti], nil
		}
		for len(str) < ti {
			str = pad + str
		}
		return str[len(str)-ti:], nil

	case "RPAD":
		if len(evalArgs) < 3 {
			return nil, fmt.Errorf("RPAD requires 3 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		targetLen, _ := toFloat64(evalArgs[1])
		pad := fmt.Sprintf("%v", evalArgs[2])
		ti := int(targetLen)
		if len(str) >= ti {
			return str[:ti], nil
		}
		for len(str) < ti {
			str = str + pad
		}
		return str[:ti], nil

	case "HEX":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("HEX requires 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return fmt.Sprintf("%X", int64(f)), nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		return fmt.Sprintf("%X", []byte(str)), nil

	case "TYPEOF":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("TYPEOF requires 1 argument")
		}
		if evalArgs[0] == nil {
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

	case "IIF":
		if len(evalArgs) < 3 {
			return nil, fmt.Errorf("IIF requires 3 arguments")
		}
		cond := evalArgs[0]
		truthy := false
		if b, ok := cond.(bool); ok {
			truthy = b
		} else if f, ok := toFloat64(cond); ok {
			truthy = f != 0
		} else if cond != nil {
			truthy = true
		}
		if truthy {
			return evalArgs[1], nil
		}
		return evalArgs[2], nil

	case "RANDOM":
		return float64(time.Now().UnixNano() % 1000000), nil

	case "UNICODE":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("UNICODE requires 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		if len(str) == 0 {
			return nil, nil
		}
		return float64([]rune(str)[0]), nil

	case "CHAR":
		var result strings.Builder
		for _, arg := range evalArgs {
			if arg != nil {
				if f, ok := toFloat64(arg); ok {
					result.WriteRune(rune(int(f)))
				}
			}
		}
		return result.String(), nil

	case "ZEROBLOB":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("ZEROBLOB requires 1 argument")
		}
		n, _ := toFloat64(evalArgs[0])
		return strings.Repeat("\x00", int(n)), nil

	case "QUOTE":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("QUOTE requires 1 argument")
		}
		if evalArgs[0] == nil {
			return "NULL", nil
		}
		if s, ok := evalArgs[0].(string); ok {
			return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
		}
		return fmt.Sprintf("%v", evalArgs[0]), nil

	case "GLOB":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("GLOB requires 2 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil {
			return nil, nil
		}
		pattern := fmt.Sprintf("%v", evalArgs[0])
		str := fmt.Sprintf("%v", evalArgs[1])
		// Simple glob: * matches any, ? matches single char
		regexPattern := "^" + strings.ReplaceAll(strings.ReplaceAll(
			regexp.QuoteMeta(pattern), `\*`, ".*"), `\?`, ".") + "$"
		matched, _ := regexp.MatchString(regexPattern, str)
		return matched, nil

	default:
		// Check for JSON functions
		return evaluateJSONFunction(funcName, evalArgs)
	}
}

// evaluateJSONFunction evaluates JSON-related functions
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

// toFloat64 converts a value to float64
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case bool:
		if n {
			return 1, true
		}
		return 0, true
	case string:
		// Try to parse string as number (SQLite-compatible behavior)
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// tokenTypeToColumnType converts a query token type to a column type string
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
	default:
		return "TEXT"
	}
}

// buildColumnIndexCache builds a cache of column name to index mappings
func (t *TableDef) buildColumnIndexCache() {
	t.columnIndices = make(map[string]int, len(t.Columns))
	for i, col := range t.Columns {
		t.columnIndices[strings.ToLower(col.Name)] = i
	}
}

// GetColumnIndex returns the index of a column by name (case-insensitive), -1 if not found
func (t *TableDef) GetColumnIndex(name string) int {
	if t.columnIndices == nil {
		t.buildColumnIndexCache()
	}
	if idx, ok := t.columnIndices[strings.ToLower(name)]; ok {
		return idx
	}
	return -1
}

// Helper methods for future implementation
func (c *Catalog) ListTables() []string {
	tables := make([]string, 0, len(c.tables))
	for name := range c.tables {
		tables = append(tables, name)
	}
	return tables
}

// Save saves catalog metadata to the B+Tree
// Note: Table data is already persisted via the buffer pool
func (c *Catalog) Save() error {
	// Save table definitions to catalog tree
	for _, tableDef := range c.tables {
		if err := c.storeTableDef(tableDef); err != nil {
			return fmt.Errorf("failed to save table definition %s: %w", tableDef.Name, err)
		}
	}

	// Flush the catalog B+Tree's in-memory data to its page
	if c.tree != nil {
		if err := c.tree.Flush(); err != nil {
			return fmt.Errorf("failed to flush catalog tree: %w", err)
		}
	}

	// Flush all table B+Trees to their pages
	if err := c.FlushTableTrees(); err != nil {
		return fmt.Errorf("failed to flush table trees: %w", err)
	}

	// Flush buffer pool to ensure all pages are written to disk
	if c.pool != nil {
		if err := c.pool.FlushAll(); err != nil {
			return fmt.Errorf("failed to flush buffer pool: %w", err)
		}
	}

	return nil
}

// Load loads catalog metadata from the B+Tree
func (c *Catalog) Load() error {
	if c.tree == nil {
		return nil
	}

	// Load table definitions from catalog tree
	// Table data is loaded on-demand via the buffer pool
	iter, err := c.tree.Scan([]byte("tbl:"), []byte("tbl;"))
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Valid() {
		key, value, err := iter.Next()
		if err != nil {
			break
		}

		// Parse key to get table name
		keyStr := string(key)
		if !strings.HasPrefix(keyStr, "tbl:") {
			continue
		}
		tableName := strings.TrimPrefix(keyStr, "tbl:")

		// Unmarshal table definition
		var tableDef TableDef
		if err := json.Unmarshal(value, &tableDef); err != nil {
			continue
		}

		// Restore DEFAULT and CHECK expressions from persisted strings
		for i := range tableDef.Columns {
			if tableDef.Columns[i].Default != "" && tableDef.Columns[i].defaultExpr == nil {
				if parsed, err := query.ParseExpression(tableDef.Columns[i].Default); err == nil {
					tableDef.Columns[i].defaultExpr = parsed
				}
			}
			if tableDef.Columns[i].CheckStr != "" && tableDef.Columns[i].Check == nil {
				if parsed, err := query.ParseExpression(tableDef.Columns[i].CheckStr); err == nil {
					tableDef.Columns[i].Check = parsed
				}
			}
		}

		c.tables[tableName] = &tableDef

		// Create or open B+Tree for the table
		if tableDef.RootPageID != 0 {
			c.tableTrees[tableName] = btree.OpenBTree(c.pool, tableDef.RootPageID)
		} else {
			tree, err := btree.NewBTree(c.pool)
			if err != nil {
				continue
			}
			tableDef.RootPageID = tree.RootPageID()
			c.tableTrees[tableName] = tree
		}

		// Build column index cache
		tableDef.buildColumnIndexCache()
	}

	return nil
}

// Deprecated: SaveData is no longer needed - data is persisted via B+Tree pages
func (c *Catalog) SaveData(dir string) error {
	return c.Save()
}

// Deprecated: LoadSchema is no longer needed - schema is loaded from B+Tree
func (c *Catalog) LoadSchema(dir string) error {
	return nil
}

// Deprecated: LoadData is no longer needed - data is loaded on-demand from B+Tree
func (c *Catalog) LoadData(dir string) error {
	return nil
}

func (c *Catalog) CreateIndex(stmt *query.CreateIndexStmt) error {
	if !stmt.IfNotExists {
		if _, exists := c.indexes[stmt.Index]; exists {
			return ErrIndexExists
		}
	}

	// Verify table exists
	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return err
	}

	// Verify all index columns exist in the table
	for _, colName := range stmt.Columns {
		if table.GetColumnIndex(colName) < 0 {
			return fmt.Errorf("column '%s' not found in table '%s'", colName, stmt.Table)
		}
	}

	// Create B+Tree for the index
	indexTree, err := btree.NewBTree(c.pool)
	if err != nil {
		return err
	}

	indexDef := &IndexDef{
		Name:       stmt.Index,
		TableName:  stmt.Table,
		Columns:    stmt.Columns,
		Unique:     stmt.Unique,
		RootPageID: indexTree.RootPageID(),
	}

	c.indexes[stmt.Index] = indexDef
	c.indexTrees[stmt.Index] = indexTree

	// Populate index with existing data from the table
	tree, exists := c.tableTrees[stmt.Table]
	if exists {
		iter, _ := tree.Scan(nil, nil)
		defer iter.Close()
		for iter.HasNext() {
			key, valueData, _ := iter.Next()
			row, err := decodeRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			indexKey, ok := buildCompositeIndexKey(table, indexDef, row)
			if ok {
				indexTree.Put([]byte(indexKey), key)
			}
		}
	}

	// Record DDL undo entry for transaction rollback
	if c.txnActive {
		c.undoLog = append(c.undoLog, undoEntry{
			action:    undoCreateIndex,
			indexName: stmt.Index,
		})
	}

	return c.storeIndexDef(indexDef)
}

func (c *Catalog) storeIndexDef(index *IndexDef) error {
	key := []byte("idx:" + index.Name)
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}

	if c.tree != nil {
		return c.tree.Put(key, data)
	}
	return nil
}

// exprToSQL converts an expression back to SQL text for persistence
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

func (c *Catalog) GetIndex(name string) (*IndexDef, error) {
	index, exists := c.indexes[name]
	if !exists {
		return nil, ErrIndexNotFound
	}
	return index, nil
}

// findUsableIndex finds an index that can be used for a WHERE clause
// Returns the index name, column name, and the value to search for
func (c *Catalog) findUsableIndex(tableName string, where query.Expression) (string, string, interface{}) {
	return c.findUsableIndexWithArgs(tableName, where, nil)
}

func (c *Catalog) findUsableIndexWithArgs(tableName string, where query.Expression, args []interface{}) (string, string, interface{}) {
	if where == nil {
		return "", "", nil
	}

	switch expr := where.(type) {
	case *query.BinaryExpr:
		// Recurse into AND conditions to find an indexed column
		if expr.Operator == query.TokenAnd {
			if name, col, val := c.findUsableIndexWithArgs(tableName, expr.Left, args); name != "" {
				return name, col, val
			}
			return c.findUsableIndexWithArgs(tableName, expr.Right, args)
		}

		if expr.Operator == query.TokenEq {
			// Check if left side is a column identifier
			if ident, ok := expr.Left.(*query.Identifier); ok {
				colName := ident.Name
				// Check if there's an index on this column
				for idxName, idxDef := range c.indexes {
					if idxDef.TableName == tableName && len(idxDef.Columns) > 0 && idxDef.Columns[0] == colName {
						// Get the value to search for
						searchVal := c.extractLiteralValue(expr.Right, args)
						if searchVal == nil {
							continue
						}
						return idxName, colName, searchVal
					}
				}
			}
			// Also check right side: value = column
			if ident, ok := expr.Right.(*query.Identifier); ok {
				colName := ident.Name
				for idxName, idxDef := range c.indexes {
					if idxDef.TableName == tableName && len(idxDef.Columns) > 0 && idxDef.Columns[0] == colName {
						searchVal := c.extractLiteralValue(expr.Left, args)
						if searchVal == nil {
							continue
						}
						return idxName, colName, searchVal
					}
				}
			}
		}
	}
	return "", "", nil
}

// buildCompositeIndexKey builds an index key from multiple columns.
// For single-column indexes, returns fmt.Sprintf("%v", row[colIdx]).
// For composite indexes, returns "col1val\x00col2val\x00..." with null-byte separators.
func buildCompositeIndexKey(table *TableDef, idxDef *IndexDef, row []interface{}) (string, bool) {
	if len(idxDef.Columns) == 0 {
		return "", false
	}
	if len(idxDef.Columns) == 1 {
		colIdx := table.GetColumnIndex(idxDef.Columns[0])
		if colIdx < 0 || colIdx >= len(row) || row[colIdx] == nil {
			return "", false
		}
		return fmt.Sprintf("%v", row[colIdx]), true
	}
	// Composite key: concatenate all column values
	var parts []string
	for _, col := range idxDef.Columns {
		colIdx := table.GetColumnIndex(col)
		if colIdx < 0 || colIdx >= len(row) || row[colIdx] == nil {
			return "", false
		}
		parts = append(parts, fmt.Sprintf("%v", row[colIdx]))
	}
	return strings.Join(parts, "\x00"), true
}

// extractLiteralValue extracts a concrete value from an expression, resolving placeholders with args
func (c *Catalog) extractLiteralValue(expr query.Expression, args []interface{}) interface{} {
	switch v := expr.(type) {
	case *query.NumberLiteral:
		return v.Value
	case *query.StringLiteral:
		return v.Value
	case *query.BooleanLiteral:
		return v.Value
	case *query.PlaceholderExpr:
		if args != nil && v.Index < len(args) {
			return args[v.Index]
		}
		return nil
	default:
		return nil
	}
}

// useIndexForQuery checks if an index can be used and returns matching primary keys
func (c *Catalog) useIndexForQuery(tableName string, where query.Expression) (map[string]bool, bool) {
	return c.useIndexForQueryWithArgs(tableName, where, nil)
}

func (c *Catalog) useIndexForQueryWithArgs(tableName string, where query.Expression, args []interface{}) (map[string]bool, bool) {
	idxName, _, searchVal := c.findUsableIndexWithArgs(tableName, where, args)
	if idxName == "" || searchVal == nil {
		return nil, false
	}

	// Only use index optimization for UNIQUE indexes.
	// Non-unique indexes store one PK per key value in the BTree,
	// so multiple rows with the same index value would be missed.
	// Fall back to full table scan for non-unique indexes.
	idxDef, idxExists := c.indexes[idxName]
	if !idxExists || !idxDef.Unique {
		return nil, false
	}

	indexTree, exists := c.indexTrees[idxName]
	if !exists {
		return nil, false
	}

	indexKey := fmt.Sprintf("%v", searchVal)
	result := make(map[string]bool)

	pkData, err := indexTree.Get([]byte(indexKey))
	if err != nil {
		// No matching rows
		return result, true
	}
	result[string(pkData)] = true
	return result, true
}

func (c *Catalog) DropIndex(name string) error {
	idxDef, exists := c.indexes[name]
	if !exists {
		return ErrIndexNotFound
	}

	// Record DDL undo entry for transaction rollback
	if c.txnActive {
		c.undoLog = append(c.undoLog, undoEntry{
			action:    undoDropIndex,
			indexName: name,
			indexDef:  idxDef,
			indexTree: c.indexTrees[name],
		})
	}

	delete(c.indexes, name)
	delete(c.indexTrees, name)

	if c.tree != nil {
		key := []byte("idx:" + name)
		return c.tree.Delete(key)
	}
	return nil
}

// encodeRow encodes a row of expressions to bytes
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

// decodeRow decodes bytes to a row of values
func decodeRow(data []byte, numCols int) ([]interface{}, error) {
	var values []interface{}
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	// Restore integer types lost by JSON unmarshaling (JSON decodes all numbers as float64)
	for i, v := range values {
		if f, ok := v.(float64); ok {
			if f == float64(int64(f)) && f >= -1e15 && f <= 1e15 {
				values[i] = int64(f)
			}
		}
	}
	// Pad row to match current column count (handles ALTER TABLE ADD COLUMN)
	for len(values) < numCols {
		values = append(values, nil)
	}
	return values, nil
}

// fastEncodeRow encodes a row using a simple binary format (faster than JSON)
// Format: [type1][len1][data1][type2][len2][data2]...
// Types: 0=nill, 1=int64, 2=float64, 3=string, 4=bool
func fastEncodeRow(values []interface{}) ([]byte, error) {
	if len(values) == 0 {
		return []byte{0}, nil // empty marker
	}

	var buf []byte
	for _, v := range values {
		switch val := v.(type) {
		case nil:
			buf = append(buf, 0) // type: nil
		case int:
			buf = append(buf, 1) // type: int
			buf = append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24), byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
		case int64:
			buf = append(buf, 1) // type: int64
			buf = append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24), byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
		case float64:
			buf = append(buf, 2) // type: float64
			bits := uint64(math.Float64bits(val))
			buf = append(buf, byte(bits), byte(bits>>8), byte(bits>>16), byte(bits>>24), byte(bits>>32), byte(bits>>40), byte(bits>>48), byte(bits>>56))
		case string:
			buf = append(buf, 3) // type: string
			buf = append(buf, byte(len(val)), byte(len(val)>>8))
			buf = append(buf, val...)
		case bool:
			buf = append(buf, 4) // type: bool
			if val {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		default:
			// Fallback to JSON for unknown types
			j, err := json.Marshal(val)
			if err != nil {
				buf = append(buf, 0) // nil as fallback
			} else {
				buf = append(buf, 3) // treat as string
				buf = append(buf, byte(len(j)), byte(len(j)>>8))
				buf = append(buf, j...)
			}
		}
	}
	return buf, nil
}

// fastDecodeRow decodes a row from binary format
func fastDecodeRow(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return []interface{}{}, nil
	}

	var values []interface{}
	i := 0
	for i < len(data) {
		typ := data[i]
		i++
		switch typ {
		case 0: // nil
			values = append(values, nil)
		case 1: // int64
			if i+8 > len(data) {
				return nil, fmt.Errorf("invalid data: expected int64")
			}
			var v int64
			v = int64(data[i]) | int64(data[i+1])<<8 | int64(data[i+2])<<16 | int64(data[i+3])<<24 |
				int64(data[i+4])<<32 | int64(data[i+5])<<40 | int64(data[i+6])<<48 | int64(data[i+7])<<56
			values = append(values, v)
			i += 8
		case 2: // float64
			if i+8 > len(data) {
				return nil, fmt.Errorf("invalid data: expected float64")
			}
			bits := uint64(data[i]) | uint64(data[i+1])<<8 | uint64(data[i+2])<<16 | uint64(data[i+3])<<24 |
				uint64(data[i+4])<<32 | uint64(data[i+5])<<40 | uint64(data[i+6])<<48 | uint64(data[i+7])<<56
			values = append(values, math.Float64frombits(bits))
			i += 8
		case 3: // string
			if i+2 > len(data) {
				return nil, fmt.Errorf("invalid data: expected string length")
			}
			length := int(data[i]) | int(data[i+1])<<8
			i += 2
			if i+length > len(data) {
				return nil, fmt.Errorf("invalid data: expected string")
			}
			values = append(values, string(data[i:i+length]))
			i += length
		case 4: // bool
			if i >= len(data) {
				return nil, fmt.Errorf("invalid data: expected bool")
			}
			values = append(values, data[i] != 0)
			i++
		default:
			return nil, fmt.Errorf("unknown type: %d", typ)
		}
	}
	return values, nil
}

// serializePK converts a primary key value to the byte key format used in the BTree.
// It tries the value as-is, then with "S:" prefix for string keys (matching Insert format).
func (c *Catalog) serializePK(pkValue interface{}, tree *btree.BTree) []byte {
	switch val := pkValue.(type) {
	case string:
		// Try direct string key first
		key := []byte(val)
		if tree != nil {
			if _, err := tree.Get(key); err == nil {
				return key
			}
		}
		// Try with "S:" prefix (Insert format for text PKs)
		if !strings.HasPrefix(val, "S:") {
			sKey := []byte("S:" + val)
			if tree != nil {
				if _, err := tree.Get(sKey); err == nil {
					return sKey
				}
			}
		}
		return key // Default to direct string key
	case int:
		return []byte(fmt.Sprintf("%020d", int64(val)))
	case int64:
		return []byte(fmt.Sprintf("%020d", val))
	case float64:
		return []byte(fmt.Sprintf("%020d", int64(val)))
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
}

// GetRow retrieves a single row by its primary key
func (c *Catalog) GetRow(tableName string, pkValue interface{}) (map[string]interface{}, error) {
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return nil, err
	}

	tree, exists := c.tableTrees[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s has no data", tableName)
	}

	// Serialize the primary key using the same format as Insert
	key := c.serializePK(pkValue, tree)

	// Get the row from BTree
	data, err := tree.Get(key)
	if err != nil {
		return nil, err
	}

	// Decode the row (Insert uses json.Marshal, so use decodeRow for consistency)
	values, err := decodeRow(data, len(table.Columns))
	if err != nil {
		return nil, err
	}

	// Convert to map
	row := make(map[string]interface{})
	for i, col := range table.Columns {
		if i < len(values) {
			row[col.Name] = values[i]
		}
	}

	return row, nil
}

// UpdateRow updates a single row by its primary key
func (c *Catalog) UpdateRow(tableName string, pkValue interface{}, row map[string]interface{}) error {
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	tree, exists := c.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s has no data", tableName)
	}

	// Serialize the primary key using the same format as Insert
	key := c.serializePK(pkValue, tree)

	// Convert row map to values slice
	values := make([]interface{}, len(table.Columns))
	for i, col := range table.Columns {
		if val, exists := row[col.Name]; exists {
			values[i] = val
		} else {
			values[i] = nil
		}
	}

	// Encode the row (Insert uses json.Marshal, so use same format for consistency)
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	// Update in BTree
	return tree.Put(key, data)
}

// DeleteRow deletes a single row by its primary key
func (c *Catalog) DeleteRow(tableName string, pkValue interface{}) error {
	tree, exists := c.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s has no data", tableName)
	}

	// Serialize the primary key using the same format as Insert
	key := c.serializePK(pkValue, tree)

	// Get old value for undo log and index cleanup
	oldData, err := tree.Get(key)
	if err != nil {
		return tree.Delete(key) // Key might not exist in expected format, try anyway
	}

	table := c.tables[tableName]

	// Clean up index entries
	var idxChanges []indexUndoEntry
	if table != nil {
		oldRow, decErr := decodeRow(oldData, len(table.Columns))
		if decErr == nil {
			for idxName, idxTree := range c.indexTrees {
				idxDef := c.indexes[idxName]
				if idxDef != nil && idxDef.TableName == tableName && len(idxDef.Columns) > 0 {
					oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
					if ok {
						if c.txnActive {
							// Save old index value for undo
							oldIdxVal, _ := idxTree.Get([]byte(oldIdxKey))
							idxChanges = append(idxChanges, indexUndoEntry{
								indexName: idxName,
								key:       []byte(oldIdxKey),
								oldValue:  oldIdxVal,
								wasAdded:  false,
							})
						}
						_ = idxTree.Delete([]byte(oldIdxKey))
					}
				}
			}
		}
	}

	// Record undo log entry for rollback
	if c.txnActive {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		oldCopy := make([]byte, len(oldData))
		copy(oldCopy, oldData)
		c.undoLog = append(c.undoLog, undoEntry{
			action:       undoDelete,
			tableName:    tableName,
			key:          keyCopy,
			oldValue:     oldCopy,
			indexChanges: idxChanges,
		})
	}

	// Cascade FK enforcement before deleting
	if table != nil && len(table.ForeignKeys) >= 0 {
		fke := NewForeignKeyEnforcer(c)
		pkColIdx := table.GetColumnIndex(table.PrimaryKey)
		if pkColIdx >= 0 {
			oldRow, decErr := decodeRow(oldData, len(table.Columns))
			if decErr == nil && pkColIdx < len(oldRow) && oldRow[pkColIdx] != nil {
				if fkErr := fke.OnDelete(context.Background(), tableName, oldRow[pkColIdx]); fkErr != nil {
					return fmt.Errorf("cascade delete: %w", fkErr)
				}
			}
		}
	}

	// Delete from BTree
	return tree.Delete(key)
}

// evalExpression evaluates an expression to a value
// EvalExpression evaluates an expression to a value
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
			return strings.ReplaceAll(str, old, newStr), nil
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

// ==================== MATERIALIZED VIEWS ====================

// CreateMaterializedView creates a new materialized view
func (c *Catalog) CreateMaterializedView(name string, selectStmt *query.SelectStmt) error {
	if _, exists := c.materializedViews[name]; exists {
		return fmt.Errorf("materialized view %s already exists", name)
	}

	// Execute the query to get initial data
	columns, rows, err := c.Select(selectStmt, nil)
	if err != nil {
		return fmt.Errorf("failed to execute materialized view query: %w", err)
	}

	// Convert rows to map format
	data := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		rowMap := make(map[string]interface{})
		for j, col := range columns {
			if j < len(row) {
				rowMap[col] = row[j]
			}
		}
		data[i] = rowMap
	}

	c.materializedViews[name] = &MaterializedViewDef{
		Name:        name,
		Query:       selectStmt,
		Data:        data,
		LastRefresh: time.Now(),
	}

	return nil
}

// DropMaterializedView drops a materialized view
func (c *Catalog) DropMaterializedView(name string) error {
	if _, exists := c.materializedViews[name]; !exists {
		return fmt.Errorf("materialized view %s not found", name)
	}

	delete(c.materializedViews, name)
	return nil
}

// RefreshMaterializedView refreshes a materialized view's data
func (c *Catalog) RefreshMaterializedView(name string) error {
	mv, exists := c.materializedViews[name]
	if !exists {
		return fmt.Errorf("materialized view %s not found", name)
	}

	// Re-execute the query
	columns, rows, err := c.Select(mv.Query, nil)
	if err != nil {
		return fmt.Errorf("failed to refresh materialized view: %w", err)
	}

	// Convert rows to map format
	data := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		rowMap := make(map[string]interface{})
		for j, col := range columns {
			if j < len(row) {
				rowMap[col] = row[j]
			}
		}
		data[i] = rowMap
	}

	mv.Data = data
	mv.LastRefresh = time.Now()

	return nil
}

// GetMaterializedView returns a materialized view's data
func (c *Catalog) GetMaterializedView(name string) (*MaterializedViewDef, error) {
	mv, exists := c.materializedViews[name]
	if !exists {
		return nil, fmt.Errorf("materialized view %s not found", name)
	}
	return mv, nil
}

// ListMaterializedViews returns all materialized view names
func (c *Catalog) ListMaterializedViews() []string {
	names := make([]string, 0, len(c.materializedViews))
	for name := range c.materializedViews {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ==================== FULL-TEXT SEARCH ====================

// CreateFTSIndex creates a full-text search index on a table
func (c *Catalog) CreateFTSIndex(name, tableName string, columns []string) error {
	if _, exists := c.ftsIndexes[name]; exists {
		return fmt.Errorf("FTS index %s already exists", name)
	}

	// Verify table exists
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	// Verify columns exist
	for _, col := range columns {
		if table.GetColumnIndex(col) == -1 {
			return fmt.Errorf("column %s not found in table %s", col, tableName)
		}
	}

	ftsIndex := &FTSIndexDef{
		Name:      name,
		TableName: tableName,
		Columns:   columns,
		Index:     make(map[string][]int64),
	}

	// Build the index from existing data
	tree, exists := c.tableTrees[tableName]
	if exists {
		iter, err := tree.Scan(nil, nil)
		if err == nil {
			defer iter.Close()
			for iter.HasNext() {
				key, value, _ := iter.Next()
				if key == nil || len(value) == 0 {
					break
				}
				var row map[string]interface{}
				// value is []byte, no need for type assertion
				if err := json.Unmarshal(value, &row); err != nil {
					continue
				}
				c.indexRowForFTS(ftsIndex, row, key)
			}
		}
	}

	c.ftsIndexes[name] = ftsIndex
	return nil
}

// indexRowForFTS indexes a single row for FTS
func (c *Catalog) indexRowForFTS(ftsIndex *FTSIndexDef, row map[string]interface{}, key []byte) {
	// Extract row ID from key - use a simple hash of the key
	rowID := int64(0)
	for _, b := range key {
		rowID = rowID*31 + int64(b)
	}
	if rowID < 0 {
		rowID = -rowID
	}

	// Index each column
	for _, col := range ftsIndex.Columns {
		if val, exists := row[col]; exists && val != nil {
			text := fmt.Sprintf("%v", val)
			words := tokenize(text)
			for _, word := range words {
				word = strings.ToLower(word)
				ftsIndex.Index[word] = append(ftsIndex.Index[word], rowID)
			}
		}
	}
}

// tokenize splits text into words
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

// DropFTSIndex drops a full-text search index
func (c *Catalog) DropFTSIndex(name string) error {
	if _, exists := c.ftsIndexes[name]; !exists {
		return fmt.Errorf("FTS index %s not found", name)
	}

	delete(c.ftsIndexes, name)
	return nil
}

// SearchFTS performs a full-text search
func (c *Catalog) SearchFTS(indexName string, query string) ([]int64, error) {
	ftsIndex, exists := c.ftsIndexes[indexName]
	if !exists {
		return nil, fmt.Errorf("FTS index %s not found", indexName)
	}

	words := tokenize(query)
	if len(words) == 0 {
		return []int64{}, nil
	}

	// Find rows matching all words (AND logic)
	var result []int64
	first := true

	for _, word := range words {
		word = strings.ToLower(word)
		rows, exists := ftsIndex.Index[word]
		if !exists {
			return []int64{}, nil // Word not found, no matches
		}

		if first {
			result = append([]int64{}, rows...)
			first = false
		} else {
			// Intersection
			result = intersectSorted(result, rows)
			if len(result) == 0 {
				return []int64{}, nil
			}
		}
	}

	return result, nil
}

// intersectSorted returns the intersection of two sorted int64 slices
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

// GetFTSIndex returns an FTS index definition
func (c *Catalog) GetFTSIndex(name string) (*FTSIndexDef, error) {
	ftsIndex, exists := c.ftsIndexes[name]
	if !exists {
		return nil, fmt.Errorf("FTS index %s not found", name)
	}
	return ftsIndex, nil
}

// ListFTSIndexes returns all FTS index names
func (c *Catalog) ListFTSIndexes() []string {
	names := make([]string, 0, len(c.ftsIndexes))
	for name := range c.ftsIndexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ==================== VACUUM ====================

// Vacuum reclaims storage space and defragments the database
func (c *Catalog) Vacuum() error {
	// In a real implementation, this would:
	// 1. Rebuild all B-trees to eliminate fragmentation
	// 2. Remove deleted entries
	// 3. Compact storage

	// For now, we'll do a simple compaction of table trees using Scan
	for name, tree := range c.tableTrees {
		// Use Scan to get all entries
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			continue
		}

		// Collect all entries
		type entry struct {
			key   []byte
			value []byte
		}
		var entries []entry
		for iter.HasNext() {
			key, value, _ := iter.Next()
			if key == nil {
				break
			}
			entries = append(entries, entry{key: key, value: value})
		}
		iter.Close()

		if len(entries) == 0 {
			continue
		}

		// Create a new tree and copy data
		newTree, err := btree.NewBTree(c.pool)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if err := newTree.Put(e.key, e.value); err != nil {
				// Handle error during Put
				continue
			}
		}

		c.tableTrees[name] = newTree
	}

	// Compact index trees
	for name, tree := range c.indexTrees {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			continue
		}

		type entry struct {
			key   []byte
			value []byte
		}
		var entries []entry
		for iter.HasNext() {
			key, value, _ := iter.Next()
			if key == nil {
				break
			}
			entries = append(entries, entry{key: key, value: value})
		}
		iter.Close()

		if len(entries) == 0 {
			continue
		}

		newTree, err := btree.NewBTree(c.pool)
		if err != nil {
			continue
		}
		for _, e := range entries {
			newTree.Put(e.key, e.value)
		}

		c.indexTrees[name] = newTree
	}

	return nil
}

// ==================== ANALYZE ====================

// Analyze collects statistics for a table
func (c *Catalog) Analyze(tableName string) error {
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	tree, exists := c.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s has no data", tableName)
	}

	// Use Scan to iterate over all entries
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	var rowCount int64
	// Analyze each column - first pass: collect all values
	columnValues := make(map[string][]interface{})
	nullCounts := make(map[string]int64)

	for iter.HasNext() {
		_, value, _ := iter.Next()
		if value == nil {
			break
		}
		rowCount++
		var rowSlice []interface{}
		if err := json.Unmarshal(value, &rowSlice); err != nil {
			continue
		}
		for i, col := range table.Columns {
			if i >= len(rowSlice) || rowSlice[i] == nil {
				nullCounts[col.Name]++
			} else {
				columnValues[col.Name] = append(columnValues[col.Name], rowSlice[i])
			}
		}
	}

	stats := &StatsTableStats{
		TableName:    tableName,
		RowCount:     uint64(rowCount),
		ColumnStats:  make(map[string]*ColumnStats),
		LastAnalyzed: time.Now(),
	}

	// Analyze each column
	for _, col := range table.Columns {
		values := columnValues[col.Name]
		colStats := &ColumnStats{
			ColumnName: col.Name,
		}

		// Count distinct values
		valueSet := make(map[string]bool)
		for _, val := range values {
			valueSet[fmt.Sprintf("%v", val)] = true
		}

		colStats.DistinctCount = uint64(len(valueSet))
		colStats.NullCount = uint64(nullCounts[col.Name])

		// Find min/max
		if len(values) > 0 {
			minVal := values[0]
			maxVal := values[0]
			for _, val := range values[1:] {
				if catalogCompareValues(val, minVal) < 0 {
					minVal = val
				}
				if catalogCompareValues(val, maxVal) > 0 {
					maxVal = val
				}
			}
			colStats.MinValue = minVal
			colStats.MaxValue = maxVal
		}

		stats.ColumnStats[col.Name] = colStats
	}

	c.stats[tableName] = stats
	return nil
}

// catalogCompareValues compares two values for min/max tracking
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

// GetTableStats returns statistics for a table
func (c *Catalog) GetTableStats(tableName string) (*StatsTableStats, error) {
	stats, exists := c.stats[tableName]
	if !exists {
		return nil, fmt.Errorf("no statistics for table %s", tableName)
	}
	return stats, nil
}

// ==================== CTE (Common Table Expressions) ====================

// ExecuteCTE executes a SELECT statement with CTEs
func (c *Catalog) ExecuteCTE(stmt *query.SelectStmtWithCTE, args []interface{}) ([]string, [][]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Store original views temporarily
	originalViews := make(map[string]*query.SelectStmt)

	// Initialize CTE results map if needed
	if c.cteResults == nil {
		c.cteResults = make(map[string]*cteResultSet)
	}
	// Track which CTE results we create so we can clean up
	var createdCTEResults []string

	// Register CTEs as temporary views or execute recursive CTEs
	for _, cte := range stmt.CTEs {
		cteName := strings.ToLower(cte.Name)

		// Check for duplicates
		if _, exists := originalViews[cte.Name]; exists {
			for name, view := range originalViews {
				c.views[name] = view
			}
			return nil, nil, fmt.Errorf("duplicate CTE name: %s", cte.Name)
		}

		// Save original view if exists
		if orig, exists := c.views[cte.Name]; exists {
			originalViews[cte.Name] = orig
		}

		// Check if this is a recursive CTE with UNION ALL
		if stmt.IsRecursive {
			if unionStmt, ok := cte.Query.(*query.UnionStmt); ok {
				// Execute recursive CTE
				err := c.executeRecursiveCTE(cte.Name, cteName, cte.Columns, unionStmt, args)
				if err != nil {
					// Clean up on error
					for _, name := range createdCTEResults {
						delete(c.cteResults, name)
					}
					return nil, nil, fmt.Errorf("recursive CTE %s: %w", cte.Name, err)
				}
				createdCTEResults = append(createdCTEResults, cteName)
				continue
			}
		}

		// Non-recursive CTE: register as a view or execute as union
		if selectQuery, ok := cte.Query.(*query.SelectStmt); ok {
			c.views[cte.Name] = selectQuery
			// Materialize CTE results so subsequent CTEs can reference them
			if len(stmt.CTEs) > 1 {
				cols, rows, err := c.selectLocked(selectQuery, args)
				if err == nil {
					c.cteResults[cteName] = &cteResultSet{columns: cols, rows: rows}
					createdCTEResults = append(createdCTEResults, cteName)
				}
			}
		} else if unionQuery, ok := cte.Query.(*query.UnionStmt); ok {
			// Execute UNION query and store results in cteResults
			cols, rows, err := c.executeCTEUnion(unionQuery, args)
			if err != nil {
				// Clean up on error
				for _, name := range createdCTEResults {
					delete(c.cteResults, name)
				}
				return nil, nil, fmt.Errorf("CTE %s: %w", cte.Name, err)
			}
			if len(cols) == 0 && len(cte.Columns) > 0 {
				cols = cte.Columns
			}
			c.cteResults[cteName] = &cteResultSet{
				columns: cols,
				rows:    rows,
			}
			createdCTEResults = append(createdCTEResults, cteName)
		}
	}

	// Execute the main query (already holding lock)
	columns, rows, err := c.selectLocked(stmt.Select, args)

	// Restore original views and clean up CTE results
	for _, cte := range stmt.CTEs {
		name := cte.Name
		if orig, exists := originalViews[name]; exists {
			c.views[name] = orig
		} else {
			delete(c.views, name)
		}
	}
	for _, name := range createdCTEResults {
		delete(c.cteResults, name)
	}

	return columns, rows, err
}

// executeDerivedTable executes a derived table subquery, handling both SelectStmt and UnionStmt
func (c *Catalog) executeDerivedTable(ref *query.TableRef, args []interface{}) ([]string, [][]interface{}, error) {
	if ref.SubqueryStmt != nil {
		switch s := ref.SubqueryStmt.(type) {
		case *query.UnionStmt:
			return c.executeCTEUnion(s, args)
		case *query.SelectStmt:
			return c.selectLocked(s, args)
		default:
			return nil, nil, fmt.Errorf("unsupported derived table statement type: %T", ref.SubqueryStmt)
		}
	}
	if ref.Subquery != nil {
		return c.selectLocked(ref.Subquery, args)
	}
	return nil, nil, fmt.Errorf("derived table has no subquery")
}

// executeCTEUnion executes a UNION query within a CTE context
func (c *Catalog) executeCTEUnion(stmt *query.UnionStmt, args []interface{}) ([]string, [][]interface{}, error) {
	// Execute left side
	var leftCols []string
	var leftRows [][]interface{}
	var err error
	switch l := stmt.Left.(type) {
	case *query.SelectStmt:
		leftCols, leftRows, err = c.selectLocked(l, args)
	case *query.UnionStmt:
		leftCols, leftRows, err = c.executeCTEUnion(l, args)
	default:
		return nil, nil, fmt.Errorf("unsupported left side of UNION in CTE: %T", stmt.Left)
	}
	if err != nil {
		return nil, nil, err
	}

	// Execute right side
	var rightRows [][]interface{}
	rightCols, rightRows, err := c.selectLocked(stmt.Right, args)
	if err != nil {
		return nil, nil, err
	}
	_ = rightCols

	// Combine results based on set operation type
	var allRows [][]interface{}

	switch stmt.Op {
	case query.SetOpIntersect:
		// INTERSECT - only rows that appear in both sides
		rightSet := make(map[string]bool)
		for _, row := range rightRows {
			rightSet[rowKeyForDedup(row)] = true
		}
		seen := make(map[string]bool)
		for _, row := range leftRows {
			key := rowKeyForDedup(row)
			if rightSet[key] {
				if stmt.All || !seen[key] {
					seen[key] = true
					allRows = append(allRows, row)
				}
			}
		}
	case query.SetOpExcept:
		// EXCEPT - rows in left but not in right
		rightSet := make(map[string]bool)
		for _, row := range rightRows {
			rightSet[rowKeyForDedup(row)] = true
		}
		seen := make(map[string]bool)
		for _, row := range leftRows {
			key := rowKeyForDedup(row)
			if !rightSet[key] {
				if stmt.All || !seen[key] {
					seen[key] = true
					allRows = append(allRows, row)
				}
			}
		}
	default:
		// UNION / UNION ALL
		if stmt.All {
			allRows = make([][]interface{}, 0, len(leftRows)+len(rightRows))
			allRows = append(allRows, leftRows...)
			allRows = append(allRows, rightRows...)
		} else {
			seen := make(map[string]bool)
			allRows = make([][]interface{}, 0, len(leftRows)+len(rightRows))
			for _, row := range leftRows {
				key := rowKeyForDedup(row)
				if !seen[key] {
					seen[key] = true
					allRows = append(allRows, row)
				}
			}
			for _, row := range rightRows {
				key := rowKeyForDedup(row)
				if !seen[key] {
					seen[key] = true
					allRows = append(allRows, row)
				}
			}
		}
	}

	return leftCols, allRows, nil
}

// executeRecursiveCTE executes a recursive CTE and stores results in cteResults
func (c *Catalog) executeRecursiveCTE(name string, nameLower string, cteColumns []string, unionStmt *query.UnionStmt, args []interface{}) error {
	const maxDepth = 1000

	// The left side is the anchor member
	anchorStmt, ok := unionStmt.Left.(*query.SelectStmt)
	if !ok {
		return fmt.Errorf("recursive CTE anchor must be a SELECT statement")
	}

	// The right side is the recursive member
	recursiveStmt := unionStmt.Right

	// Step 1: Execute anchor member
	anchorCols, anchorRows, err := c.selectLocked(anchorStmt, args)
	if err != nil {
		return fmt.Errorf("anchor member: %w", err)
	}

	// Use CTE-defined column names if provided, otherwise use anchor's column names
	cteCols := anchorCols
	if len(cteColumns) > 0 && len(cteColumns) <= len(anchorCols) {
		cteCols = make([]string, len(anchorCols))
		copy(cteCols, anchorCols)
		for i, col := range cteColumns {
			cteCols[i] = col
		}
	}

	// Step 2: Iteratively execute recursive member
	// Accumulate all results
	allRows := make([][]interface{}, len(anchorRows))
	copy(allRows, anchorRows)

	// Working table: rows from the last iteration
	workingRows := anchorRows

	for depth := 0; depth < maxDepth; depth++ {
		if len(workingRows) == 0 {
			break
		}

		// Store current working set as CTE result so recursive member can reference it
		c.cteResults[nameLower] = &cteResultSet{
			columns: cteCols,
			rows:    workingRows,
		}

		// Execute recursive member
		_, newRows, err := c.selectLocked(recursiveStmt, args)
		if err != nil {
			return fmt.Errorf("recursive member (depth %d): %w", depth, err)
		}

		if len(newRows) == 0 {
			break
		}

		allRows = append(allRows, newRows...)
		workingRows = newRows
	}

	// Store final accumulated results
	c.cteResults[nameLower] = &cteResultSet{
		columns: cteCols,
		rows:    allRows,
	}

	return nil
}

// ==================== JSON Index Functions ====================

// CreateJSONIndex creates a new JSON index for fast JSON queries
func (c *Catalog) CreateJSONIndex(name, tableName, column, path, dataType string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.jsonIndexes[name]; exists {
		return fmt.Errorf("JSON index '%s' already exists", name)
	}

	// Check if table exists
	table, exists := c.tables[tableName]
	if !exists {
		return ErrTableNotFound
	}

	// Check if column exists and is JSON type
	colExists := false
	for _, col := range table.Columns {
		if col.Name == column {
			colExists = true
			if col.Type != "JSON" {
				return fmt.Errorf("column '%s' is not JSON type", column)
			}
			break
		}
	}
	if !colExists {
		return ErrColumnNotFound
	}

	// Create the index
	jsonIndex := &JSONIndexDef{
		Name:      name,
		TableName: tableName,
		Column:    column,
		Path:      path,
		DataType:  dataType,
		Index:     make(map[string][]int64),
		NumIndex:  make(map[float64][]int64),
	}

	// Index existing data
	if err := c.buildJSONIndex(jsonIndex); err != nil {
		return fmt.Errorf("failed to build JSON index: %w", err)
	}

	c.jsonIndexes[name] = jsonIndex
	return nil
}

// buildJSONIndex indexes existing data for a JSON index
func (c *Catalog) buildJSONIndex(idx *JSONIndexDef) error {
	tree, exists := c.tableTrees[idx.TableName]
	if !exists {
		return nil
	}

	iter, _ := tree.Scan(nil, nil)
	rowNum := int64(0)

	for iter.HasNext() {
		_, valueData, err := iter.Next()
		if err != nil {
			continue
		}

		var values []interface{}
		if err := json.Unmarshal(valueData, &values); err != nil {
			continue
		}

		// Extract JSON value using path
		jsonVal := c.extractJSONValue(values, idx.Column, idx.Path)
		if jsonVal != nil {
			c.indexJSONValue(idx, jsonVal, rowNum)
		}
		rowNum++
	}

	iter.Close()
	return nil
}

// extractJSONValue extracts a value from JSON using a path
func (c *Catalog) extractJSONValue(row []interface{}, column, path string) interface{} {
	// Simple implementation: find JSON column and extract path
	// TODO: Implement full JSON path resolution
	for _, val := range row {
		if jsonMap, ok := val.(map[string]interface{}); ok {
			// Simple $.key path
			if len(path) > 2 && path[0] == '$' && path[1] == '.' {
				key := path[2:]
				if v, exists := jsonMap[key]; exists {
					return v
				}
			}
		}
	}
	return nil
}

// indexJSONValue adds a value to the JSON index
func (c *Catalog) indexJSONValue(idx *JSONIndexDef, value interface{}, rowNum int64) {
	switch v := value.(type) {
	case string:
		idx.Index[v] = append(idx.Index[v], rowNum)
	case float64:
		idx.NumIndex[v] = append(idx.NumIndex[v], rowNum)
	case int:
		idx.NumIndex[float64(v)] = append(idx.NumIndex[float64(v)], rowNum)
	case bool:
		strVal := fmt.Sprintf("%t", v)
		idx.Index[strVal] = append(idx.Index[strVal], rowNum)
	}
}

// DropJSONIndex drops a JSON index
func (c *Catalog) DropJSONIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.jsonIndexes[name]; !exists {
		return fmt.Errorf("JSON index '%s' not found", name)
	}

	delete(c.jsonIndexes, name)
	return nil
}

// QueryJSONIndex queries the JSON index for rows matching a value
func (c *Catalog) QueryJSONIndex(indexName string, value interface{}) ([]int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	idx, exists := c.jsonIndexes[indexName]
	if !exists {
		return nil, fmt.Errorf("JSON index '%s' not found", indexName)
	}

	switch v := value.(type) {
	case string:
		return idx.Index[v], nil
	case float64:
		return idx.NumIndex[v], nil
	case int:
		return idx.NumIndex[float64(v)], nil
	default:
		return nil, fmt.Errorf("unsupported value type for JSON index query")
	}
}

// GetJSONIndex returns a JSON index definition
func (c *Catalog) GetJSONIndex(name string) (*JSONIndexDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	idx, exists := c.jsonIndexes[name]
	if !exists {
		return nil, fmt.Errorf("JSON index '%s' not found", name)
	}
	return idx, nil
}

// ListJSONIndexes returns all JSON index names
func (c *Catalog) ListJSONIndexes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.jsonIndexes))
	for name := range c.jsonIndexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ApplyRLSFilter applies row-level security filtering to rows
// Returns only rows that pass the RLS policies for the given user
func (c *Catalog) ApplyRLSFilter(tableName string, columns []string, rows [][]interface{}, user string, roles []string) ([]string, [][]interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.enableRLS || c.rlsManager == nil {
		return columns, rows, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return columns, rows, nil
	}

	// Convert rows to map format for RLS evaluation
	mapRows := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		mapRow := make(map[string]interface{})
		for j, col := range columns {
			if j < len(row) {
				mapRow[col] = row[j]
			}
		}
		mapRows[i] = mapRow
	}

	// Apply RLS filtering
	filtered, err := c.rlsManager.FilterRows(context.Background(), tableName, security.PolicySelect, mapRows, user, roles)
	if err != nil {
		return nil, nil, err
	}

	// Convert back to row format
	filteredRows := make([][]interface{}, len(filtered))
	for i, mapRow := range filtered {
		row := make([]interface{}, len(columns))
		for j, col := range columns {
			row[j] = mapRow[col]
		}
		filteredRows[i] = row
	}

	return columns, filteredRows, nil
}

// CheckRLSForInsert checks if a row can be inserted based on RLS policies
func (c *Catalog) CheckRLSForInsert(tableName string, row map[string]interface{}, user string, roles []string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return true, nil
	}

	return c.rlsManager.CheckAccess(context.Background(), tableName, security.PolicyInsert, row, user, roles)
}

// CheckRLSForUpdate checks if a row can be updated based on RLS policies
func (c *Catalog) CheckRLSForUpdate(tableName string, row map[string]interface{}, user string, roles []string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return true, nil
	}

	return c.rlsManager.CheckAccess(context.Background(), tableName, security.PolicyUpdate, row, user, roles)
}

// CheckRLSForDelete checks if a row can be deleted based on RLS policies
func (c *Catalog) CheckRLSForDelete(tableName string, row map[string]interface{}, user string, roles []string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return true, nil
	}

	return c.rlsManager.CheckAccess(context.Background(), tableName, security.PolicyDelete, row, user, roles)
}
