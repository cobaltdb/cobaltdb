package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/catalog"
	"github.com/cobaltdb/cobaltdb/pkg/query"
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
	// Prepared statement cache for performance
	stmtCache   map[string]query.Statement
	stmtMu      sync.RWMutex
	nextTxnID   atomic.Uint64 // Auto-increment transaction ID counter
}

// Options contains database configuration options
type Options struct {
	PageSize   int
	CacheSize  int // number of pages
	InMemory   bool
	WALEnabled bool
	SyncMode   SyncMode
}

// SyncMode controls when data is synced to disk
type SyncMode int

const (
	SyncOff SyncMode = iota
	SyncNormal
	SyncFull
)

// DefaultOptions returns the default database options
func DefaultOptions() *Options {
	return &Options{
		PageSize:   storage.PageSize,
		CacheSize:  1024, // 4MB cache
		InMemory:   false,
		WALEnabled: true,
		SyncMode:   SyncNormal,
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
	}

	var backend storage.Backend
	var err error

	if opts.InMemory || path == ":memory:" {
		backend = storage.NewMemory()
	} else {
		// Ensure directory exists
		dir := filepath.Dir(path)
		if dir != "." && dir != "/" {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory: %w", err)
			}
		}
		backend, err = storage.OpenDisk(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open database: %w", err)
		}
	}

	db := &DB{
		path:      path,
		backend:   backend,
		options:   opts,
		stmtCache: make(map[string]query.Statement),
	}

	// Initialize buffer pool
	db.pool = storage.NewBufferPool(opts.CacheSize, backend)

	// Initialize or load database
	if err := db.initialize(); err != nil {
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

	// Initialize transaction manager
	db.txnMgr = txn.NewManager(db.pool, db.wal)

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

	// Load catalog metadata from the B+Tree
	if err := db.catalog.Load(); err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// Initialize transaction manager
	db.txnMgr = txn.NewManager(db.pool, db.wal)

	return nil
}

// Close closes the database
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	db.closed = true

	// Save catalog metadata to B+Tree (if not in-memory)
	if !db.options.InMemory && db.path != ":memory:" {
		if err := db.catalog.Save(); err != nil {
			return fmt.Errorf("failed to save catalog: %w", err)
		}

		// Update meta page with current root page ID
		if err := db.saveMetaPage(); err != nil {
			return fmt.Errorf("failed to save meta page: %w", err)
		}
	}

	// Flush buffer pool
	if err := db.pool.Close(); err != nil {
		return err
	}

	// Perform WAL checkpoint if enabled
	if db.wal != nil {
		if err := db.wal.Checkpoint(db.pool); err != nil {
			return fmt.Errorf("failed to checkpoint WAL: %w", err)
		}
	}

	// Close WAL
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			return err
		}
	}

	// Close backend
	return db.backend.Close()
}

// getPreparedStatement returns a cached prepared statement or parses and caches it
func (db *DB) getPreparedStatement(sql string) (query.Statement, error) {
	db.stmtMu.RLock()
	stmt, exists := db.stmtCache[sql]
	db.stmtMu.RUnlock()

	if exists {
		return stmt, nil
	}

	// Parse and cache
	parsedStmt, err := query.Parse(sql)
	if err != nil {
		return nil, err
	}

	// Cache the statement (limit cache size to prevent memory issues)
	db.stmtMu.Lock()
	if len(db.stmtCache) < 1000 {
		db.stmtCache[sql] = parsedStmt
	}
	db.stmtMu.Unlock()

	return parsedStmt, nil
}

// Exec executes a SQL statement without returning rows
func (db *DB) Exec(ctx context.Context, sql string, args ...interface{}) (Result, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return Result{}, ErrDatabaseClosed
	}

	// Try to use cached prepared statement
	stmt, err := db.getPreparedStatement(sql)
	if err != nil {
		return Result{}, fmt.Errorf("parse error: %w", err)
	}

	// Execute statement
	return db.execute(ctx, stmt, args)
}

// Query executes a SQL query and returns rows
func (db *DB) Query(ctx context.Context, sql string, args ...interface{}) (*Rows, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDatabaseClosed
	}

	// Try to use cached prepared statement
	stmt, err := db.getPreparedStatement(sql)
	if err != nil {
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

// TableSchema returns a human-readable schema for a table
func (db *DB) TableSchema(name string) (string, error) {
	table, err := db.catalog.GetTable(name)
	if err != nil {
		return "", err
	}
	var result string
	result = fmt.Sprintf("CREATE TABLE %s (\n", table.Name)
	for i, col := range table.Columns {
		result += fmt.Sprintf("  %s %s", col.Name, col.Type)
		if col.PrimaryKey {
			result += " PRIMARY KEY"
		}
		if col.AutoIncrement {
			result += " AUTOINCREMENT"
		}
		if col.NotNull {
			result += " NOT NULL"
		}
		if col.Unique {
			result += " UNIQUE"
		}
		if col.Default != "" {
			result += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		if i < len(table.Columns)-1 {
			result += ","
		}
		result += "\n"
	}
	result += ");"
	return result, nil
}

// Begin starts a new transaction
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	return db.BeginWith(ctx, txn.DefaultOptions())
}

// BeginWith starts a new transaction with options
func (db *DB) BeginWith(ctx context.Context, opts *txn.Options) (*Tx, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
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

// execute executes a statement
func (db *DB) execute(ctx context.Context, stmt query.Statement, args []interface{}) (result Result, err error) {
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
				db.catalog.RollbackTransaction()
			} else {
				db.catalog.CommitTransaction()
			}
		}()
	}

	switch s := stmt.(type) {
	case *query.CreateTableStmt:
		return db.executeCreateTable(ctx, s)
	case *query.InsertStmt:
		return db.executeInsert(ctx, s, args)
	case *query.UpdateStmt:
		return db.executeUpdate(ctx, s, args)
	case *query.DeleteStmt:
		return db.executeDelete(ctx, s, args)
	case *query.DropTableStmt:
		return db.executeDropTable(ctx, s)
	case *query.CreateIndexStmt:
		return db.executeCreateIndex(ctx, s)
	case *query.CreateViewStmt:
		return db.executeCreateView(ctx, s)
	case *query.DropViewStmt:
		return db.executeDropView(ctx, s)
	case *query.CreateTriggerStmt:
		return db.executeCreateTrigger(ctx, s)
	case *query.DropTriggerStmt:
		return db.executeDropTrigger(ctx, s)
	case *query.CreateProcedureStmt:
		return db.executeCreateProcedure(ctx, s)
	case *query.DropProcedureStmt:
		return db.executeDropProcedure(ctx, s)
	case *query.CallProcedureStmt:
		return db.executeCallProcedure(ctx, s, args)
	case *query.BeginStmt:
		if db.catalog.IsTransactionActive() {
			return Result{}, errors.New("transaction already in progress")
		}
		transaction := db.txnMgr.Begin(txn.DefaultOptions())
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
		return db.executeVacuum(ctx, s)
	case *query.AnalyzeStmt:
		return db.executeAnalyze(ctx, s)
	case *query.CreateMaterializedViewStmt:
		return db.executeCreateMaterializedView(ctx, s)
	case *query.DropMaterializedViewStmt:
		return db.executeDropMaterializedView(ctx, s)
	case *query.RefreshMaterializedViewStmt:
		return db.executeRefreshMaterializedView(ctx, s)
	case *query.CreateFTSIndexStmt:
		return db.executeCreateFTSIndex(ctx, s)
	case *query.AlterTableStmt:
		return db.executeAlterTable(ctx, s)
	case *query.SetVarStmt:
		// MySQL compatibility - accept SET commands silently
		return Result{}, nil
	case *query.UseStmt:
		// MySQL compatibility - accept USE commands silently (single-database)
		return Result{}, nil
	case *query.ShowTablesStmt, *query.ShowCreateTableStmt, *query.ShowColumnsStmt,
		*query.ShowDatabasesStmt, *query.DescribeStmt:
		// These are query-like statements but may come through Exec
		return Result{}, nil
	case *query.DropIndexStmt:
		// Try FTS index first, then regular index
		if _, err := db.catalog.GetFTSIndex(s.Index); err == nil {
			if err := db.catalog.DropFTSIndex(s.Index); err != nil {
				return Result{}, err
			}
			return Result{RowsAffected: 0}, nil
		}
		// Try regular index
		if err := db.catalog.DropIndex(s.Index); err != nil {
			return Result{}, err
		}
		return Result{RowsAffected: 0}, nil
	default:
		return Result{}, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// query executes a query and returns rows
func (db *DB) query(ctx context.Context, stmt query.Statement, args []interface{}) (*Rows, error) {
	switch s := stmt.(type) {
	case *query.SelectStmt:
		return db.executeSelect(ctx, s, args)
	case *query.UnionStmt:
		return db.executeUnion(ctx, s, args)
	case *query.SelectStmtWithCTE:
		return db.executeSelectWithCTE(ctx, s, args)
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
	lastInsertID, rowsAffected, err := db.catalog.Insert(stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
}

// executeUpdate executes UPDATE
func (db *DB) executeUpdate(ctx context.Context, stmt *query.UpdateStmt, args []interface{}) (Result, error) {
	lastInsertID, rowsAffected, err := db.catalog.Update(stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
}

// executeDelete executes DELETE
func (db *DB) executeDelete(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (Result, error) {
	lastInsertID, rowsAffected, err := db.catalog.Delete(stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
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
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropProcedure executes DROP PROCEDURE
func (db *DB) executeDropProcedure(ctx context.Context, stmt *query.DropProcedureStmt) (Result, error) {
	if err := db.catalog.DropProcedure(stmt.Name); err != nil {
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

	// Execute each statement in the procedure body
	// Map call arguments to procedure parameters
	execArgs := args

	var totalRowsAffected int64
	for _, bodyStmt := range proc.Body {
		// Execute the statement
		result, err := db.execute(ctx, bodyStmt, execArgs)
		if err != nil {
			return Result{}, err
		}
		totalRowsAffected += result.RowsAffected
	}

	return Result{RowsAffected: totalRowsAffected}, nil
}

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
	if err := db.catalog.CreateMaterializedView(stmt.Name, stmt.Query); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropMaterializedView executes DROP MATERIALIZED VIEW
func (db *DB) executeDropMaterializedView(ctx context.Context, stmt *query.DropMaterializedViewStmt) (Result, error) {
	if err := db.catalog.DropMaterializedView(stmt.Name); err != nil {
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
	db  *DB
	txn *txn.Transaction
}

// Exec executes a statement within the transaction
func (tx *Tx) Exec(ctx context.Context, sql string, args ...interface{}) (Result, error) {
	tx.db.mu.RLock()
	defer tx.db.mu.RUnlock()

	if tx.db.closed {
		return Result{}, ErrDatabaseClosed
	}

	// Parse the statement
	stmt, err := tx.db.getPreparedStatement(sql)
	if err != nil {
		return Result{}, fmt.Errorf("parse error: %w", err)
	}

	// Execute within transaction context
	return tx.db.execute(ctx, stmt, args)
}

// Query executes a query within the transaction
// Changes made within this transaction are visible to subsequent queries
func (tx *Tx) Query(ctx context.Context, sql string, args ...interface{}) (*Rows, error) {
	return tx.db.Query(ctx, sql, args...)
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	// Flush table B+Trees to buffer pool first
	if err := tx.db.catalog.FlushTableTrees(); err != nil {
		return fmt.Errorf("failed to flush tables: %w", err)
	}

	// Commit in catalog first (writes commit record to WAL)
	if err := tx.db.catalog.CommitTransaction(); err != nil {
		return err
	}

	// Flush buffer pool to disk to ensure durability
	if err := tx.db.pool.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush buffer pool: %w", err)
	}

	return tx.txn.Commit()
}

// Rollback rolls back the transaction
func (tx *Tx) Rollback() error {
	// Rollback in catalog first (writes rollback record to WAL)
	if err := tx.db.catalog.RollbackTransaction(); err != nil {
		return err
	}
	return tx.txn.Rollback()
}
