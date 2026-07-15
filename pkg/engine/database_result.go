package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/catalog"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

// Result holds the result of a DML statement execution
type Result struct {
	LastInsertID int64
	RowsAffected int64
}

// Rows represents query results
type Rows struct {
	columns []string
	rows    [][]interface{}
	pos     int
	closed  bool
}

// Next advances to the next row

func (r *Rows) Next() bool {
	if r == nil || r.closed {
		return false
	}
	r.pos++
	return r.pos <= len(r.rows)
}

// Scan copies column values into dest

func (r *Rows) Scan(dest ...interface{}) error {
	if r == nil {
		return errors.New("rows is nil")
	}
	if r.closed {
		return errors.New("rows are closed")
	}
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
	if r == nil || r.closed || r.columns == nil {
		return nil
	}
	columns := make([]string, len(r.columns))
	copy(columns, r.columns)
	return columns
}

// ColumnTypeHints returns coarse SQL type names inferred from non-NULL row values.
func (r *Rows) ColumnTypeHints() []string {
	if r == nil || r.closed || len(r.columns) == 0 {
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
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	r.columns = nil
	r.rows = nil
	r.pos = 0
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
	case catalog.StringBox:
		return typed.String()
	case *string:
		if typed == nil {
			return nil
		}
		return *typed
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

func acquireTx(db *DB, txn *txn.Transaction) *Tx {
	return &Tx{db: db, txn: txn}
}

func releaseTx(tx *Tx) {
	if tx == nil {
		return
	}
	tx.db = nil
	tx.txn = nil
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
	return tx.db.execute(ctx, sql, stmt, args)
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

	return tx.db.query(ctx, sql, stmt, args)
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

	// Hold the replication capture lock across commit+entry-append so a
	// concurrent replication snapshot cannot observe the committed data
	// without the corresponding replication entries. The sync-mode ACK wait
	// runs after all locks are released. Lock order: replCaptureMu -> flushMu.
	needWait, err := func() (bool, error) {
		tx.db.replCaptureMu.RLock()
		defer tx.db.replCaptureMu.RUnlock()

		// Concurrent explicit transactions apply buffered writes inside
		// CommitTransaction, which serializes on per-tree mutexes.
		// B-tree flushing is deferred to checkpoint/close; the B-tree
		// self-flushes before eviction when memory pressure requires it.
		tx.db.flushMu.RLock()
		defer tx.db.flushMu.RUnlock()

		// Commit in catalog (conflict detection, WAL write, apply buffered writes)
		if err := tx.db.catalog.CommitTransaction(); err != nil {
			if rbErr := tx.db.catalog.RollbackTransaction(); rbErr != nil {
				_ = rbErr
			}
			tx.db.replTxnDiscard()
			return false, fmt.Errorf("commit transaction failed: %w", err)
		}

		if tx.txn.State != txn.TxnCommitted {
			if err := tx.txn.Commit(); err != nil {
				return false, err
			}
		}

		return tx.db.replTxnFlush()
	}()
	if err != nil {
		return err
	}

	if needWait {
		if mgr := tx.db.replicationMasterManager(); mgr != nil {
			return tx.db.replicationSyncWait(mgr)
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

	tx.db.replTxnDiscard()

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
