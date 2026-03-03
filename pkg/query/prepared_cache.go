package query

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// PreparedStatement represents a cached prepared statement
type PreparedStatement struct {
	ID          string
	SQL         string
	Stmt        Statement
	ParamCount  int
	CreatedAt   time.Time
	LastUsedAt  time.Time
	UseCount    uint64
	AvgExecTime time.Duration
}

// PreparedCache manages cached prepared statements
type PreparedCache struct {
	statements map[string]*PreparedStatement
	mu         sync.RWMutex
	maxSize    int
	ttl        time.Duration
}

// NewPreparedCache creates a new prepared statement cache
func NewPreparedCache(maxSize int, ttl time.Duration) *PreparedCache {
	if maxSize <= 0 {
		maxSize = 100
	}
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}

	cache := &PreparedCache{
		statements: make(map[string]*PreparedStatement),
		maxSize:    maxSize,
		ttl:        ttl,
	}

	// Start cleanup goroutine
	go cache.cleanupLoop()

	return cache
}

// GenerateID generates a unique ID for a SQL statement
func (pc *PreparedCache) GenerateID(sql string) string {
	hash := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(hash[:8])
}

// Get retrieves a prepared statement from cache
func (pc *PreparedCache) Get(id string) (*PreparedStatement, bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	stmt, exists := pc.statements[id]
	if !exists {
		return nil, false
	}

	// Check if expired
	if time.Since(stmt.LastUsedAt) > pc.ttl {
		delete(pc.statements, id)
		return nil, false
	}

	// Update LastUsedAt for LRU tracking
	stmt.LastUsedAt = time.Now()

	return stmt, true
}

// GetBySQL retrieves a prepared statement by its SQL text
func (pc *PreparedCache) GetBySQL(sql string) (*PreparedStatement, bool) {
	id := pc.GenerateID(sql)
	return pc.Get(id)
}

// Put adds a prepared statement to cache
func (pc *PreparedCache) Put(sql string, stmt Statement, paramCount int) *PreparedStatement {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Check if cache is full
	if len(pc.statements) >= pc.maxSize {
		pc.evictLRU()
	}

	id := pc.GenerateID(sql)
	prepared := &PreparedStatement{
		ID:         id,
		SQL:        sql,
		Stmt:       stmt,
		ParamCount: paramCount,
		CreatedAt:  time.Now(),
		LastUsedAt: time.Now(),
	}

	pc.statements[id] = prepared
	return prepared
}

// UpdateStats updates execution statistics for a prepared statement
func (pc *PreparedCache) UpdateStats(id string, execTime time.Duration) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	stmt, exists := pc.statements[id]
	if !exists {
		return
	}

	stmt.LastUsedAt = time.Now()
	stmt.UseCount++

	// Update average execution time using exponential moving average
	if stmt.AvgExecTime == 0 {
		stmt.AvgExecTime = execTime
	} else {
		stmt.AvgExecTime = (stmt.AvgExecTime*9 + execTime) / 10
	}
}

// Remove removes a prepared statement from cache
func (pc *PreparedCache) Remove(id string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	delete(pc.statements, id)
}

// Clear clears all cached statements
func (pc *PreparedCache) Clear() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.statements = make(map[string]*PreparedStatement)
}

// Size returns the number of cached statements
func (pc *PreparedCache) Size() int {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	return len(pc.statements)
}

// GetAll returns all cached statements
func (pc *PreparedCache) GetAll() []*PreparedStatement {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	result := make([]*PreparedStatement, 0, len(pc.statements))
	for _, stmt := range pc.statements {
		result = append(result, stmt)
	}
	return result
}

// evictLRU removes the least recently used statement
func (pc *PreparedCache) evictLRU() {
	var oldestID string
	var oldestTime time.Time

	for id, stmt := range pc.statements {
		if oldestID == "" || stmt.LastUsedAt.Before(oldestTime) {
			oldestID = id
			oldestTime = stmt.LastUsedAt
		}
	}

	if oldestID != "" {
		delete(pc.statements, oldestID)
	}
}

// cleanupLoop periodically removes expired statements
func (pc *PreparedCache) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		pc.cleanup()
	}
}

// cleanup removes expired statements
func (pc *PreparedCache) cleanup() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	now := time.Now()
	for id, stmt := range pc.statements {
		if now.Sub(stmt.LastUsedAt) > pc.ttl {
			delete(pc.statements, id)
		}
	}
}

// PreparedCacheStats contains cache statistics
type PreparedCacheStats struct {
	Size           int
	HitCount       uint64
	MissCount      uint64
	EvictionCount  uint64
	TotalExecTime  time.Duration
	AvgExecTime    time.Duration
}

// Stats returns cache statistics
func (pc *PreparedCache) Stats() PreparedCacheStats {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	var totalExecTime time.Duration
	var totalUseCount uint64

	for _, stmt := range pc.statements {
		totalExecTime += stmt.AvgExecTime * time.Duration(stmt.UseCount)
		totalUseCount += stmt.UseCount
	}

	avgExecTime := time.Duration(0)
	if totalUseCount > 0 {
		avgExecTime = totalExecTime / time.Duration(totalUseCount)
	}

	return PreparedCacheStats{
		Size:          len(pc.statements),
		TotalExecTime: totalExecTime,
		AvgExecTime:   avgExecTime,
	}
}

// PreparedStatementExecutor executes prepared statements with caching
type PreparedStatementExecutor struct {
	cache  *PreparedCache
	parser *Parser
}

// NewPreparedStatementExecutor creates a new executor
func NewPreparedStatementExecutor(cache *PreparedCache) *PreparedStatementExecutor {
	if cache == nil {
		cache = NewPreparedCache(100, 30*time.Minute)
	}

	return &PreparedStatementExecutor{
		cache: cache,
	}
}

// Prepare prepares a SQL statement (with caching)
func (pse *PreparedStatementExecutor) Prepare(sql string) (*PreparedStatement, error) {
	// Check cache first
	if stmt, found := pse.cache.GetBySQL(sql); found {
		return stmt, nil
	}

	// Parse the SQL
	// Tokenize first
	tokens, err := Tokenize(sql)
	if err != nil {
		return nil, fmt.Errorf("tokenize error: %w", err)
	}
	parser := NewParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// Count parameters
	paramCount := countParameters(stmt)

	// Add to cache
	prepared := pse.cache.Put(sql, stmt, paramCount)
	return prepared, nil
}

// Execute executes a prepared statement with the given arguments
func (pse *PreparedStatementExecutor) Execute(ctx context.Context, id string, args []interface{}) (Statement, error) {
	stmt, found := pse.cache.Get(id)
	if !found {
		return nil, fmt.Errorf("prepared statement not found: %s", id)
	}

	start := time.Now()
	defer func() {
		pse.cache.UpdateStats(id, time.Since(start))
	}()

	// Return the cached statement
	// The actual execution would be done by the caller
	return stmt.Stmt, nil
}

// ExecuteSQL prepares and executes a SQL statement in one call
func (pse *PreparedStatementExecutor) ExecuteSQL(ctx context.Context, sql string, args []interface{}) (Statement, error) {
	prepared, err := pse.Prepare(sql)
	if err != nil {
		return nil, err
	}

	return pse.Execute(ctx, prepared.ID, args)
}

// countParameters counts the number of parameters in a statement
func countParameters(stmt Statement) int {
	switch s := stmt.(type) {
	case *SelectStmt:
		return countExprParameters(s.Where)
	case *InsertStmt:
		count := 0
		for _, row := range s.Values {
			for _, expr := range row {
				count += countExprParameters(expr)
			}
		}
		return count
	case *UpdateStmt:
		count := 0
		for _, set := range s.Set {
			count += countExprParameters(set.Value)
		}
		count += countExprParameters(s.Where)
		return count
	case *DeleteStmt:
		return countExprParameters(s.Where)
	default:
		return 0
	}
}

// countExprParameters counts parameters in an expression
func countExprParameters(expr Expression) int {
	if expr == nil {
		return 0
	}

	switch e := expr.(type) {
	case *PlaceholderExpr:
		return 1
	case *BinaryExpr:
		return countExprParameters(e.Left) + countExprParameters(e.Right)
	case *UnaryExpr:
		return countExprParameters(e.Expr)
	case *FunctionCall:
		count := 0
		for _, arg := range e.Args {
			count += countExprParameters(arg)
		}
		return count
	case *InExpr:
		count := countExprParameters(e.Expr)
		for _, val := range e.List {
			count += countExprParameters(val)
		}
		return count
	case *BetweenExpr:
		return countExprParameters(e.Expr) + countExprParameters(e.Lower) + countExprParameters(e.Upper)
	case *CaseExpr:
		count := countExprParameters(e.Expr)
		for _, when := range e.Whens {
			count += countExprParameters(when.Condition)
			count += countExprParameters(when.Result)
		}
		count += countExprParameters(e.Else)
		return count
	default:
		return 0
	}
}

// Invalidate removes a statement from cache
func (pse *PreparedStatementExecutor) Invalidate(id string) {
	pse.cache.Remove(id)
}

// InvalidateAll clears the cache
func (pse *PreparedStatementExecutor) InvalidateAll() {
	pse.cache.Clear()
}

// CacheStats returns cache statistics
func (pse *PreparedStatementExecutor) CacheStats() PreparedCacheStats {
	return pse.cache.Stats()
}
