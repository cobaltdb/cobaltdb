package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// QueryExecutor wraps query execution with timeout and cancellation support
type QueryExecutor struct {
	db *DB

	// Active queries tracking
	activeQueries map[string]*ActiveQuery
	queriesMu     sync.RWMutex

	// Global query timeout (overrides DB option if set)
	defaultTimeout time.Duration

	// Metrics
	timeoutCount   atomic.Uint64
	cancelCount    atomic.Uint64
	completedCount atomic.Uint64
}

// ActiveQuery represents a currently executing query
type ActiveQuery struct {
	ID        string
	SQL       string
	StartTime time.Time
	Timeout   time.Duration
	cancel    context.CancelFunc
}

// QueryStats contains query execution statistics
type QueryStats struct {
	ActiveQueries  int
	TimeoutCount   uint64
	CancelCount    uint64
	CompletedCount uint64
	DefaultTimeout time.Duration
}

// NewQueryExecutor creates a new query executor with timeout support
func NewQueryExecutor(db *DB) *QueryExecutor {
	return &QueryExecutor{
		db:             db,
		activeQueries:  make(map[string]*ActiveQuery),
		defaultTimeout: db.options.QueryTimeout,
	}
}

// ExecuteWithTimeout executes a query with timeout support
func (qe *QueryExecutor) ExecuteWithTimeout(ctx context.Context, sql string, args []interface{}, timeout time.Duration) (Result, error) {
	// Create timeout context if timeout is specified
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	} else if qe.defaultTimeout > 0 {
		// Use default timeout from DB options
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, qe.defaultTimeout)
		defer cancel()
	}

	// Track the query
	queryID := qe.trackQuery(sql, timeout)
	defer qe.untrackQuery(queryID)

	// Execute with context
	result, err := qe.db.Exec(ctx, sql, args...)

	// Check for timeout
	if ctx.Err() == context.DeadlineExceeded {
		qe.timeoutCount.Add(1)
		return Result{}, fmt.Errorf("query timeout: %s exceeded %v", sql, timeout)
	}

	if err == nil {
		qe.completedCount.Add(1)
	}

	return result, err
}

// QueryWithTimeout executes a query with timeout support
func (qe *QueryExecutor) QueryWithTimeout(ctx context.Context, sql string, args []interface{}, timeout time.Duration) (*Rows, error) {
	// Create timeout context if timeout is specified
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	} else if qe.defaultTimeout > 0 {
		// Use default timeout from DB options
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, qe.defaultTimeout)
		defer cancel()
	}

	// Track the query
	queryID := qe.trackQuery(sql, timeout)
	defer qe.untrackQuery(queryID)

	// Execute with context
	rows, err := qe.db.Query(ctx, sql, args...)

	// Check for timeout
	if ctx.Err() == context.DeadlineExceeded {
		qe.timeoutCount.Add(1)
		return nil, fmt.Errorf("query timeout: %s exceeded %v", sql, timeout)
	}

	if err == nil {
		qe.completedCount.Add(1)
	}

	return rows, err
}

// trackQuery adds a query to the active queries map
func (qe *QueryExecutor) trackQuery(sql string, timeout time.Duration) string {
	queryID := fmt.Sprintf("q_%d", time.Now().UnixNano())

	qe.queriesMu.Lock()
	defer qe.queriesMu.Unlock()

	qe.activeQueries[queryID] = &ActiveQuery{
		ID:        queryID,
		SQL:       sql,
		StartTime: time.Now(),
		Timeout:   timeout,
	}

	return queryID
}

// untrackQuery removes a query from the active queries map
func (qe *QueryExecutor) untrackQuery(queryID string) {
	qe.queriesMu.Lock()
	defer qe.queriesMu.Unlock()

	delete(qe.activeQueries, queryID)
}

// CancelQuery cancels a running query by ID
func (qe *QueryExecutor) CancelQuery(queryID string) error {
	qe.queriesMu.Lock()
	query, exists := qe.activeQueries[queryID]
	qe.queriesMu.Unlock()

	if !exists {
		return fmt.Errorf("query %s not found", queryID)
	}

	if query.cancel != nil {
		query.cancel()
		qe.cancelCount.Add(1)
	}

	return nil
}

// CancelAllQueries cancels all running queries
func (qe *QueryExecutor) CancelAllQueries() int {
	qe.queriesMu.RLock()
	queries := make([]*ActiveQuery, 0, len(qe.activeQueries))
	for _, q := range qe.activeQueries {
		queries = append(queries, q)
	}
	qe.queriesMu.RUnlock()

	count := 0
	for _, q := range queries {
		if q.cancel != nil {
			q.cancel()
			count++
		}
	}

	qe.cancelCount.Add(uint64(count))
	return count
}

// GetActiveQueries returns a list of currently running queries
func (qe *QueryExecutor) GetActiveQueries() []*ActiveQuery {
	qe.queriesMu.RLock()
	defer qe.queriesMu.RUnlock()

	result := make([]*ActiveQuery, 0, len(qe.activeQueries))
	for _, q := range qe.activeQueries {
		result = append(result, &ActiveQuery{
			ID:        q.ID,
			SQL:       q.SQL,
			StartTime: q.StartTime,
			Timeout:   q.Timeout,
		})
	}

	return result
}

// GetLongRunningQueries returns queries that have been running longer than the specified duration
func (qe *QueryExecutor) GetLongRunningQueries(threshold time.Duration) []*ActiveQuery {
	qe.queriesMu.RLock()
	defer qe.queriesMu.RUnlock()

	now := time.Now()
	result := make([]*ActiveQuery, 0)

	for _, q := range qe.activeQueries {
		if now.Sub(q.StartTime) > threshold {
			result = append(result, &ActiveQuery{
				ID:        q.ID,
				SQL:       q.SQL,
				StartTime: q.StartTime,
				Timeout:   q.Timeout,
			})
		}
	}

	return result
}

// Stats returns query execution statistics
func (qe *QueryExecutor) Stats() QueryStats {
	qe.queriesMu.RLock()
	activeCount := len(qe.activeQueries)
	qe.queriesMu.RUnlock()

	return QueryStats{
		ActiveQueries:  activeCount,
		TimeoutCount:   qe.timeoutCount.Load(),
		CancelCount:    qe.cancelCount.Load(),
		CompletedCount: qe.completedCount.Load(),
		DefaultTimeout: qe.defaultTimeout,
	}
}

// SetDefaultTimeout sets the default query timeout
func (qe *QueryExecutor) SetDefaultTimeout(timeout time.Duration) {
	qe.defaultTimeout = timeout
}

// ExecOption is a functional option for query execution
type ExecOption func(*execConfig)

type execConfig struct {
	timeout time.Duration
	context context.Context
}

// WithTimeout sets a timeout for the query execution
func WithTimeout(timeout time.Duration) ExecOption {
	return func(c *execConfig) {
		c.timeout = timeout
	}
}

// WithContext sets a context for the query execution
func WithContext(ctx context.Context) ExecOption {
	return func(c *execConfig) {
		c.context = ctx
	}
}

// ExecWithOptions executes a query with the given options
func (db *DB) ExecWithOptions(sql string, args []interface{}, opts ...ExecOption) (Result, error) {
	cfg := &execConfig{
		context: context.Background(),
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// Apply timeout if specified
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		cfg.context, cancel = context.WithTimeout(cfg.context, cfg.timeout)
		defer cancel()
	} else if db.options.QueryTimeout > 0 {
		// Use default timeout from DB options
		var cancel context.CancelFunc
		cfg.context, cancel = context.WithTimeout(cfg.context, db.options.QueryTimeout)
		defer cancel()
	}

	return db.Exec(cfg.context, sql, args...)
}

// QueryWithOptions executes a query with the given options
func (db *DB) QueryWithOptions(sql string, args []interface{}, opts ...ExecOption) (*Rows, error) {
	cfg := &execConfig{
		context: context.Background(),
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// Apply timeout if specified
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		cfg.context, cancel = context.WithTimeout(cfg.context, cfg.timeout)
		defer cancel()
	} else if db.options.QueryTimeout > 0 {
		// Use default timeout from DB options
		var cancel context.CancelFunc
		cfg.context, cancel = context.WithTimeout(cfg.context, db.options.QueryTimeout)
		defer cancel()
	}

	return db.Query(cfg.context, sql, args...)
}

// QueryRowWithOptions executes a query with the given options
func (db *DB) QueryRowWithOptions(sql string, args []interface{}, opts ...ExecOption) *Row {
	cfg := &execConfig{
		context: context.Background(),
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// Apply timeout if specified
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		cfg.context, cancel = context.WithTimeout(cfg.context, cfg.timeout)
		defer cancel()
	} else if db.options.QueryTimeout > 0 {
		// Use default timeout from DB options
		var cancel context.CancelFunc
		cfg.context, cancel = context.WithTimeout(cfg.context, db.options.QueryTimeout)
		defer cancel()
	}

	return db.QueryRow(cfg.context, sql, args...)
}
