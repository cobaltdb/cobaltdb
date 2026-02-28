package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

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
	path       string
	backend    storage.Backend
	pool       *storage.BufferPool
	wal        *storage.WAL
	catalog    *catalog.Catalog
	txnMgr     *txn.Manager
	rootTree   *btree.BTree
	mu         sync.RWMutex
	closed     bool
	options    *Options
}

// Options contains database configuration options
type Options struct {
	PageSize     int
	CacheSize    int // number of pages
	InMemory     bool
	WALEnabled   bool
	SyncMode     SyncMode
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
	if opts == nil {
		opts = DefaultOptions()
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
		path:    path,
		backend: backend,
		options: opts,
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
	// Create meta page
	metaPage := storage.NewPage(0, storage.PageTypeMeta)
	meta := storage.NewMetaPage()
	meta.Serialize(metaPage.Data)

	// Write meta page
	if _, err := db.backend.WriteAt(metaPage.Data, 0); err != nil {
		return fmt.Errorf("failed to write meta page: %w", err)
	}

	// Create root B+Tree for system catalog
	tree, err := btree.NewBTree(db.pool)
	if err != nil {
		return fmt.Errorf("failed to create catalog tree: %w", err)
	}
	db.rootTree = tree

	// Initialize catalog
	db.catalog = catalog.New(db.rootTree, db.pool)

	// Initialize transaction manager
	db.txnMgr = txn.NewManager(db.pool, db.wal)

	return db.backend.Sync()
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

	// Load catalog
	db.catalog = catalog.New(db.rootTree, db.pool)

	// Initialize transaction manager
	db.txnMgr = txn.NewManager(db.pool, db.wal)

	// Load table data from disk
	if db.path != ":memory:" {
		dataDir := db.path + ".data"

		// First load schema
		if err := db.catalog.LoadSchema(dataDir); err != nil {
			return fmt.Errorf("failed to load schema: %w", err)
		}

		// Then load data
		if err := db.catalog.LoadData(dataDir); err != nil {
			return fmt.Errorf("failed to load data: %w", err)
		}
	}

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

	// Save catalog data to disk (if not in-memory)
	if !db.options.InMemory && db.path != ":memory:" {
		dataDir := db.path + ".data"
		if err := db.catalog.SaveData(dataDir); err != nil {
			return fmt.Errorf("failed to save data: %w", err)
		}
	}

	// Flush buffer pool
	if err := db.pool.Close(); err != nil {
		return err
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

// Exec executes a SQL statement without returning rows
func (db *DB) Exec(ctx context.Context, sql string, args ...interface{}) (Result, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return Result{}, ErrDatabaseClosed
	}

	// Parse SQL
	stmt, err := query.Parse(sql)
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

	// Parse SQL
	stmt, err := query.Parse(sql)
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
		return &Row{err: errors.New("no rows in result set")}
	}

	return &Row{rows: rows}
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
	return &Tx{
		db:  db,
		txn: transaction,
	}, nil
}

// execute executes a statement
func (db *DB) execute(ctx context.Context, stmt query.Statement, args []interface{}) (Result, error) {
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
	case *query.BeginStmt:
		return Result{}, errors.New("use Begin() method to start a transaction")
	case *query.CommitStmt:
		return Result{}, errors.New("use Commit() method to commit a transaction")
	case *query.RollbackStmt:
		return Result{}, errors.New("use Rollback() method to rollback a transaction")
	default:
		return Result{}, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// query executes a query and returns rows
func (db *DB) query(ctx context.Context, stmt query.Statement, args []interface{}) (*Rows, error) {
	switch s := stmt.(type) {
	case *query.SelectStmt:
		return db.executeSelect(ctx, s, args)
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

// executeDropTable executes DROP TABLE
func (db *DB) executeDropTable(ctx context.Context, stmt *query.DropTableStmt) (Result, error) {
	if err := db.catalog.DropTable(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
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
	// TODO: implement transaction-scoped execution
	return tx.db.Exec(ctx, sql, args...)
}

// Query executes a query within the transaction
func (tx *Tx) Query(ctx context.Context, sql string, args ...interface{}) (*Rows, error) {
	// TODO: implement transaction-scoped query
	return tx.db.Query(ctx, sql, args...)
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	return tx.txn.Commit()
}

// Rollback rolls back the transaction
func (tx *Tx) Rollback() error {
	return tx.txn.Rollback()
}
