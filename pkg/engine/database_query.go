package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/fdw"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/scheduler"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

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
	// Reject a query with fewer bind arguments than positional `?` placeholders.
	// Without this, an unbound placeholder evaluates to a fail-closed NULL and the
	// query returns silently wrong results (e.g. `WHERE v = ? OR v = ?` with one
	// arg matches nothing and reports no error) — a data-correctness hazard for
	// callers. Extra args remain tolerated (ignored), matching prior leniency.
	if n := query.CountPlaceholders(sql); n > len(args) {
		return nil, fmt.Errorf("statement has %d placeholder(s) but %d argument(s) were provided", n, len(args))
	}

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

func (db *DB) acquireConnection(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("connection timeout: %w", err)
	}

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
		found := false
		for i, w := range db.connWaiters {
			if w == ch {
				db.connWaiters = append(db.connWaiters[:i], db.connWaiters[i+1:]...)
				found = true
				break
			}
		}
		db.connWaitMu.Unlock()
		if !found {
			// A releaseConnection already popped this waiter and handed it a
			// slot (the buffered channel send always succeeds, even though we
			// timed out). Reclaim the slot so connCount is not leaked — without
			// this, every timeout-racing-handoff permanently burns a slot and
			// the limiter eventually wedges. Every pop is followed by exactly
			// one send, so this receive cannot block forever.
			<-ch
			db.releaseHandedOffSlot()
		}
		return fmt.Errorf("connection timeout: %w", ctx.Err())
	case <-db.shutdownCh:
		return ErrDatabaseClosed
	}
}

// releaseHandedOffSlot returns a connection slot that releaseConnection handed
// to a waiter which then abandoned it (timeout). It passes the slot to the next
// waiter, or decrements connCount if none. It must NOT touch activeConns: the
// releasing side already decremented it and this waiter never incremented it.
func (db *DB) releaseHandedOffSlot() {
	db.connWaitMu.Lock()
	if len(db.connWaiters) > 0 {
		next := db.connWaiters[0]
		db.connWaiters = db.connWaiters[1:]
		db.connWaitMu.Unlock()
		next <- struct{}{} // buffered cap-1; hand the slot to the next waiter
		return
	}
	db.connWaitMu.Unlock()
	db.connCount.Add(-1)
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
// It returns the execution context, parsed statement, and a release-connection
// func; if err is non-nil the caller should return immediately.
func (db *DB) runStatement(ctx context.Context, methodName, sql string, args ...interface{}) (_ context.Context, _ query.Statement, start time.Time, release func(), err error) {
	if ctx == nil {
		ctx = context.Background()
	}
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
		return ctx, nil, time.Time{}, func() {}, acquireErr
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
		return ctx, nil, time.Time{}, func() {}, err
	}

	start = time.Now()
	return ctx, stmt, start, release, nil
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

	runCtx, stmt, start, release, execErr := db.runStatement(ctx, "Exec", sql, args...)
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

	return db.execute(runCtx, sql, stmt, args)
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

	runCtx, stmt, start, release, execErr := db.runStatement(ctx, "Query", sql, args...)
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

	return db.query(runCtx, sql, stmt, args)
}

// QueryRow executes a SQL query and returns a single row

func (db *DB) QueryRow(ctx context.Context, sql string, args ...interface{}) *Row {
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return &Row{err: err}
	}

	if !rows.Next() {
		if err := rows.Close(); err != nil {
			return &Row{err: err}
		}
		return &Row{err: errors.New("no rows in result set")}
	}

	return &Row{rows: rows}
}

// Tables returns a list of all table names in the database

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

// AbortConnTransaction rolls back any transaction left open on the CALLING
// goroutine — e.g. when a server connection drops mid-transaction (after a wire
// BEGIN with no COMMIT/ROLLBACK). It is a no-op if no transaction is active.
// Transaction state is goroutine-local, so this must run on the connection's
// own handler goroutine. Without it, the abandoned transaction kept its locks
// (blocking the single writer) and pinned MVCC version pruning.
func (db *DB) AbortConnTransaction() {
	if db.closed.Load() || db.catalog == nil {
		return
	}
	if db.catalog.IsTransactionActive() {
		_ = db.catalog.RollbackTransaction()
		db.replTxnDiscard()
	}
}

// auditUser extracts the username from context for audit logging.
