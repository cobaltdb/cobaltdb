package engine

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// CacheEntry represents a cached query result
type CacheEntry struct {
	Key       string
	SQL       string
	Args      []interface{}
	Columns   []string
	Rows      [][]interface{}
	Tables    []string // Tables this query depends on
	CreatedAt time.Time
	ExpiresAt time.Time
	HitCount  atomic.Uint64
	Size      int64 // Estimated memory size
}

// IsExpired checks if the cache entry has expired
func (e *CacheEntry) IsExpired() bool {
	return time.Now().After(e.ExpiresAt)
}

// CacheStats contains cache statistics
type CacheStats struct {
	Hits          uint64  `json:"hits"`
	Misses        uint64  `json:"misses"`
	Evictions     uint64  `json:"evictions"`
	Invalidations uint64  `json:"invalidations"`
	Entries       int     `json:"entries"`
	Size          int64   `json:"size"`
	MaxSize       int64   `json:"max_size"`
	HitRate       float64 `json:"hit_rate"`
}

// CacheConfig configures the query cache
type CacheConfig struct {
	Enabled         bool
	MaxSize         int64         // Max total size in bytes
	MaxEntries      int           // Max number of entries
	DefaultTTL      time.Duration // Default TTL for entries
	MaxEntrySize    int64         // Max size for a single entry
	CleanupInterval time.Duration // How often to run cleanup
}

// DefaultCacheConfig returns default cache configuration
func DefaultCacheConfig() *CacheConfig {
	return &CacheConfig{
		Enabled:         true,
		MaxSize:         100 * 1024 * 1024, // 100MB
		MaxEntries:      10000,
		DefaultTTL:      5 * time.Minute,
		MaxEntrySize:    10 * 1024 * 1024, // 10MB per entry
		CleanupInterval: 1 * time.Minute,
	}
}

// QueryCache provides query result caching
type QueryCache struct {
	mu         sync.RWMutex
	config     *CacheConfig
	entries    map[string]*list.Element       // Cache key -> list element
	lru        *list.List                     // LRU list
	tableIndex map[string]map[string]struct{} // table name -> set of cache keys

	// Statistics
	hits          atomic.Uint64
	misses        atomic.Uint64
	evictions     atomic.Uint64
	invalidations atomic.Uint64
	currentSize   atomic.Int64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewQueryCache creates a new query cache
func NewQueryCache(config *CacheConfig) *QueryCache {
	if config == nil {
		config = DefaultCacheConfig()
	}

	qc := &QueryCache{
		config:     config,
		entries:    make(map[string]*list.Element),
		lru:        list.New(),
		tableIndex: make(map[string]map[string]struct{}),
		stopCh:     make(chan struct{}),
	}

	if config.Enabled {
		qc.wg.Add(1)
		go qc.cleanupLoop()
	}

	return qc
}

// Close stops the cache cleanup goroutine
func (qc *QueryCache) Close() {
	if qc.config.Enabled {
		close(qc.stopCh)
		qc.wg.Wait()
	}
}

// Get retrieves a cached result
func (qc *QueryCache) Get(sql string, args []interface{}) ([]string, [][]interface{}, bool) {
	if !qc.config.Enabled {
		return nil, nil, false
	}

	key := qc.generateKey(sql, args)

	qc.mu.RLock()
	elem, exists := qc.entries[key]
	qc.mu.RUnlock()

	if !exists {
		qc.misses.Add(1)
		return nil, nil, false
	}

	entry := elem.Value.(*CacheEntry)

	// Check expiration
	if entry.IsExpired() {
		qc.mu.Lock()
		// Double-check after acquiring write lock
		if elem2, stillExists := qc.entries[key]; stillExists {
			entry2 := elem2.Value.(*CacheEntry)
			if entry2.IsExpired() {
				qc.removeEntry(elem2)
			}
		}
		qc.mu.Unlock()
		qc.misses.Add(1)
		return nil, nil, false
	}

	// Move to front (most recently used)
	qc.mu.Lock()
	qc.lru.MoveToFront(elem)
	qc.mu.Unlock()

	entry.HitCount.Add(1)
	qc.hits.Add(1)

	// Return copies of the data
	return qc.copyColumns(entry.Columns), qc.copyRows(entry.Rows), true
}

// Set caches a query result
func (qc *QueryCache) Set(sql string, args []interface{}, columns []string, rows [][]interface{}, tables []string, ttl time.Duration) error {
	if !qc.config.Enabled {
		return nil
	}

	// Check if entry would be too large
	entrySize := qc.estimateDataSize(columns, rows)
	if entrySize > qc.config.MaxEntrySize {
		return fmt.Errorf("result too large to cache: %d bytes", entrySize)
	}

	if ttl <= 0 {
		ttl = qc.config.DefaultTTL
	}

	key := qc.generateKey(sql, args)

	entry := &CacheEntry{
		Key:       key,
		SQL:       sql,
		Args:      qc.copyArgs(args),
		Columns:   qc.copyColumns(columns),
		Rows:      qc.copyRows(rows),
		Tables:    tables,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(ttl),
		Size:      entrySize,
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	// Check if key already exists
	if elem, exists := qc.entries[key]; exists {
		// Update existing entry
		oldEntry := elem.Value.(*CacheEntry)
		qc.currentSize.Add(-oldEntry.Size)
		elem.Value = entry
		qc.lru.MoveToFront(elem)
		qc.currentSize.Add(entrySize)
		return nil
	}

	// Make room if necessary
	for (qc.currentSize.Load()+entrySize > qc.config.MaxSize || len(qc.entries) >= qc.config.MaxEntries) && qc.lru.Len() > 0 {
		qc.evictLRU()
	}

	// Add new entry
	elem := qc.lru.PushFront(entry)
	qc.entries[key] = elem
	qc.currentSize.Add(entrySize)

	// Update table index
	for _, table := range tables {
		table = strings.ToLower(table)
		if qc.tableIndex[table] == nil {
			qc.tableIndex[table] = make(map[string]struct{})
		}
		qc.tableIndex[table][key] = struct{}{}
	}

	return nil
}

// Invalidate removes entries that depend on the given table
func (qc *QueryCache) Invalidate(table string) {
	if !qc.config.Enabled {
		return
	}

	table = strings.ToLower(table)

	qc.mu.Lock()
	defer qc.mu.Unlock()

	keys, exists := qc.tableIndex[table]
	if !exists {
		return
	}

	for key := range keys {
		if elem, exists := qc.entries[key]; exists {
			qc.removeEntry(elem)
			qc.invalidations.Add(1)
		}
	}

	delete(qc.tableIndex, table)
}

// InvalidateAll clears the entire cache
func (qc *QueryCache) InvalidateAll() {
	if !qc.config.Enabled {
		return
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	qc.entries = make(map[string]*list.Element)
	qc.lru.Init()
	qc.tableIndex = make(map[string]map[string]struct{})
	qc.currentSize.Store(0)
}

// Stats returns cache statistics
func (qc *QueryCache) Stats() CacheStats {
	hits := qc.hits.Load()
	misses := qc.misses.Load()
	total := hits + misses

	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	qc.mu.RLock()
	entries := len(qc.entries)
	qc.mu.RUnlock()

	return CacheStats{
		Hits:          hits,
		Misses:        misses,
		Evictions:     qc.evictions.Load(),
		Invalidations: qc.invalidations.Load(),
		Entries:       entries,
		Size:          qc.currentSize.Load(),
		MaxSize:       qc.config.MaxSize,
		HitRate:       hitRate,
	}
}

// generateKey creates a cache key from SQL and args
func (qc *QueryCache) generateKey(sql string, args []interface{}) string {
	// Normalize SQL (remove extra whitespace)
	normalized := qc.normalizeSQL(sql)

	// Create hash
	h := sha256.New()
	h.Write([]byte(normalized))
	h.Write([]byte("|"))

	// Add args to hash
	for _, arg := range args {
		h.Write([]byte(fmt.Sprintf("%v|", arg)))
	}

	return hex.EncodeToString(h.Sum(nil))
}

// normalizeSQL normalizes SQL for consistent cache keys
func (qc *QueryCache) normalizeSQL(sql string) string {
	// Convert to lower case
	sql = strings.ToLower(sql)

	// Remove extra whitespace
	fields := strings.Fields(sql)
	return strings.Join(fields, " ")
}

// removeEntry removes an entry from the cache
func (qc *QueryCache) removeEntry(elem *list.Element) {
	entry := elem.Value.(*CacheEntry)

	qc.lru.Remove(elem)
	delete(qc.entries, entry.Key)
	qc.currentSize.Add(-entry.Size)

	// Remove from table index
	for _, table := range entry.Tables {
		table = strings.ToLower(table)
		if keys, exists := qc.tableIndex[table]; exists {
			delete(keys, entry.Key)
			if len(keys) == 0 {
				delete(qc.tableIndex, table)
			}
		}
	}
}

// evictLRU removes the least recently used entry
func (qc *QueryCache) evictLRU() {
	elem := qc.lru.Back()
	if elem == nil {
		return
	}

	qc.removeEntry(elem)
	qc.evictions.Add(1)
}

// cleanupLoop periodically cleans up expired entries
func (qc *QueryCache) cleanupLoop() {
	defer qc.wg.Done()

	ticker := time.NewTicker(qc.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-qc.stopCh:
			return
		case <-ticker.C:
			qc.cleanupExpired()
		}
	}
}

// cleanupExpired removes all expired entries
func (qc *QueryCache) cleanupExpired() {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	now := time.Now()
	var toRemove []*list.Element

	for elem := qc.lru.Back(); elem != nil; elem = elem.Prev() {
		entry := elem.Value.(*CacheEntry)
		if now.After(entry.ExpiresAt) {
			toRemove = append(toRemove, elem)
		}
	}

	for _, elem := range toRemove {
		qc.removeEntry(elem)
	}
}

// copyColumns creates a copy of column names
func (qc *QueryCache) copyColumns(cols []string) []string {
	if cols == nil {
		return nil
	}
	result := make([]string, len(cols))
	copy(result, cols)
	return result
}

// copyRows creates a deep copy of rows
func (qc *QueryCache) copyRows(rows [][]interface{}) [][]interface{} {
	if rows == nil {
		return nil
	}
	result := make([][]interface{}, len(rows))
	for i, row := range rows {
		newRow := make([]interface{}, len(row))
		copy(newRow, row)
		result[i] = newRow
	}
	return result
}

// copyArgs creates a copy of args
func (qc *QueryCache) copyArgs(args []interface{}) []interface{} {
	if args == nil {
		return nil
	}
	result := make([]interface{}, len(args))
	copy(result, args)
	return result
}

// estimateDataSize estimates the memory size of column and row data
func (qc *QueryCache) estimateDataSize(columns []string, rows [][]interface{}) int64 {
	var size int64

	// Estimate column names size
	for _, col := range columns {
		size += int64(len(col))
	}

	// Estimate row data size (rough approximation)
	for _, row := range rows {
		for _, val := range row {
			size += qc.estimateValueSize(val)
		}
	}

	return size
}

// estimateValueSize estimates the size of a single value
func (qc *QueryCache) estimateValueSize(v interface{}) int64 {
	switch val := v.(type) {
	case nil:
		return 1
	case bool:
		return 1
	case int, int32, int64:
		return 8
	case float32:
		return 4
	case float64:
		return 8
	case string:
		return int64(len(val))
	case []byte:
		return int64(len(val))
	case time.Time:
		return 24
	default:
		return 8 // Conservative estimate
	}
}

// CacheableQuery checks if a query should be cached
// Generally SELECT queries without side effects are cacheable
func (qc *QueryCache) CacheableQuery(sql string) bool {
	if !qc.config.Enabled {
		return false
	}

	sql = strings.ToLower(strings.TrimSpace(sql))

	// Only cache SELECT statements
	if !strings.HasPrefix(sql, "select") {
		return false
	}

	// Don't cache queries with non-deterministic functions
	nonDeterministic := []string{"now()", "random()", "uuid()", "current_timestamp", "rand("}
	for _, fn := range nonDeterministic {
		if strings.Contains(sql, fn) {
			return false
		}
	}

	// Don't cache queries with FOR UPDATE
	if strings.Contains(sql, "for update") {
		return false
	}

	return true
}

// ExtractTables extracts table names from a SQL statement (simplified version)
// In production, this would use the SQL parser
func (qc *QueryCache) ExtractTables(sql string) []string {
	sql = strings.ToLower(sql)
	tables := make(map[string]bool)

	// Very simple extraction - just looks for common patterns
	// In production, use proper SQL parsing
	words := strings.Fields(sql)
	for i, word := range words {
		if word == "from" && i+1 < len(words) {
			tableName := strings.TrimSuffix(words[i+1], ",")
			if !strings.Contains(tableName, "(") {
				tables[tableName] = true
			}
		}
		if word == "join" && i+1 < len(words) {
			tableName := strings.TrimSuffix(words[i+1], ",")
			if !strings.Contains(tableName, "(") {
				tables[tableName] = true
			}
		}
	}

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}
	return result
}

// CachedQueryExecutor wraps query execution with caching
type CachedQueryExecutor struct {
	cache *QueryCache
	db    *DB
}

// NewCachedQueryExecutor creates a new caching query executor
func NewCachedQueryExecutor(db *DB, cache *QueryCache) *CachedQueryExecutor {
	return &CachedQueryExecutor{
		cache: cache,
		db:    db,
	}
}

// Query executes a SELECT query with caching support
func (cqe *CachedQueryExecutor) Query(ctx context.Context, sql string, args ...interface{}) (*Rows, error) {
	if !cqe.cache.config.Enabled || !cqe.cache.CacheableQuery(sql) {
		return cqe.db.Query(ctx, sql, args...)
	}

	// Try to get from cache
	if columns, rows, found := cqe.cache.Get(sql, args); found {
		return &Rows{
			columns: columns,
			rows:    rows,
			pos:     0,
		}, nil
	}

	// Execute the query
	rows, err := cqe.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	// Extract tables for cache invalidation
	tables := cqe.cache.ExtractTables(sql)

	// Cache the result
	cqe.cache.Set(sql, args, rows.columns, rows.rows, tables, 0)

	return rows, nil
}

// InvalidateTable invalidates cache entries for a specific table
func (cqe *CachedQueryExecutor) InvalidateTable(table string) {
	cqe.cache.Invalidate(table)
}
