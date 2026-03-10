package query

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
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
	stopCh     chan struct{}
	stopOnce   sync.Once
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
		stopCh:     make(chan struct{}),
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

	for {
		select {
		case <-ticker.C:
			pc.cleanup()
		case <-pc.stopCh:
			return
		}
	}
}

// Close stops the background cleanup goroutine.
func (pc *PreparedCache) Close() {
	pc.stopOnce.Do(func() {
		close(pc.stopCh)
	})
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
	Size          int
	HitCount      uint64
	MissCount     uint64
	EvictionCount uint64
	TotalExecTime time.Duration
	AvgExecTime   time.Duration
}

// Stats returns cache statistics
func (pc *PreparedCache) Stats() PreparedCacheStats {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	var totalExecTime time.Duration
	var totalUseCount uint64

	for _, stmt := range pc.statements {
		useCount := stmt.UseCount
		if useCount > uint64(math.MaxInt64) {
			useCount = uint64(math.MaxInt64)
		}
		totalExecTime += stmt.AvgExecTime * time.Duration(useCount)
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
