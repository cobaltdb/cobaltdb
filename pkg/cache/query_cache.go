// Package cache provides query result caching for CobaltDB
package cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Entry represents a cached query result
type Entry struct {
	Key        string
	SQL        string
	Args       []interface{}
	Columns    []string
	Rows       [][]interface{}
	Size       int64
	CreatedAt  time.Time
	AccessedAt time.Time
	HitCount   int64
	TableDeps  []string // Tables this query depends on
}

// Config holds cache configuration
type Config struct {
	// MaxSize is the maximum cache size in bytes
	MaxSize int64
	// MaxEntries is the maximum number of cached entries (0 = unlimited)
	MaxEntries int
	// TTL is the default time-to-live for cache entries
	TTL time.Duration
	// CleanupInterval is how often to clean expired entries
	CleanupInterval time.Duration
	// Enabled enables/disables caching
	Enabled bool
}

// DefaultConfig returns default cache configuration
func DefaultConfig() *Config {
	return &Config{
		MaxSize:         64 * 1024 * 1024, // 64MB
		MaxEntries:      10000,
		TTL:             5 * time.Minute,
		CleanupInterval: 1 * time.Minute,
		Enabled:         true,
	}
}

// Cache manages query result caching
type Cache struct {
	config *Config

	// Cache storage
	mu      sync.RWMutex
	entries map[string]*Entry
	lruList *list.List // LRU eviction list
	elemMap map[string]*list.Element

	// Current size tracking
	currentSize int64

	// Metrics
	hits   uint64
	misses uint64
	evicts uint64

	// Cleanup
	stopCh    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	// Table dependency tracking for invalidation
	tableDeps map[string]map[string]struct{} // table -> set of query keys
	depMu     sync.RWMutex
}

// New creates a new query cache
func New(config *Config) *Cache {
	if config == nil {
		config = DefaultConfig()
	} else {
		config = normalizeConfig(config)
	}

	c := &Cache{
		config:    config,
		entries:   make(map[string]*Entry),
		lruList:   list.New(),
		elemMap:   make(map[string]*list.Element),
		tableDeps: make(map[string]map[string]struct{}),
		stopCh:    make(chan struct{}),
	}

	// Start cleanup goroutine
	if config.Enabled {
		c.wg.Add(1)
		go c.cleanupLoop()
	}

	return c
}

func normalizeConfig(config *Config) *Config {
	normalized := *config
	if normalized.CleanupInterval <= 0 {
		normalized.CleanupInterval = DefaultConfig().CleanupInterval
	}
	return &normalized
}

// Close shuts down the cache
func (c *Cache) Close() {
	if c.config.Enabled {
		c.closeOnce.Do(func() { close(c.stopCh) })
		c.wg.Wait()
	}
}

// Get retrieves a cached result
func (c *Cache) Get(sql string, args []interface{}) (*Entry, bool) {
	if !c.config.Enabled {
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}

	key := generateKey(sql, args)

	c.mu.Lock()
	entry, exists := c.entries[key]
	if !exists {
		c.mu.Unlock()
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}

	// Check if expired
	if c.config.TTL > 0 && time.Since(entry.CreatedAt) > c.config.TTL {
		c.deleteLocked(key)
		c.mu.Unlock()
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}

	// Update access time and hit count
	entry.AccessedAt = time.Now()
	entry.HitCount++

	// Move to front of LRU list
	if elem, ok := c.elemMap[key]; ok {
		c.lruList.MoveToFront(elem)
	}
	entryCopy := cloneEntry(entry)
	c.mu.Unlock()

	atomic.AddUint64(&c.hits, 1)
	return entryCopy, true
}

// Set stores a result in the cache
func (c *Cache) Set(sql string, args []interface{}, columns []string, rows [][]interface{}, tableDeps []string) {
	if !c.config.Enabled {
		return
	}

	key := generateKey(sql, args)

	// Calculate entry size
	size := estimateSize(columns, rows)

	// Check if entry is too large
	if c.config.MaxSize > 0 && size > c.config.MaxSize/10 { // Don't cache if > 10% of max size
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove old entry before capacity checks so replacing an existing key
	// does not evict an unrelated cached result.
	if oldEntry, exists := c.entries[key]; exists {
		if oldElem, ok := c.elemMap[key]; ok {
			c.lruList.Remove(oldElem)
			delete(c.elemMap, key)
		}
		delete(c.entries, key)
		atomic.AddInt64(&c.currentSize, -oldEntry.Size)
		c.untrackTableDeps(key, oldEntry.TableDeps)
	}

	// Make room if needed
	for c.config.MaxSize > 0 && atomic.LoadInt64(&c.currentSize)+size > c.config.MaxSize {
		if !c.evictLRU() {
			break // Can't evict anything
		}
	}

	entry := &Entry{
		Key:        key,
		SQL:        sql,
		Args:       cloneValues(args),
		Columns:    cloneStrings(columns),
		Rows:       cloneRows(rows),
		Size:       size,
		CreatedAt:  time.Now(),
		AccessedAt: time.Now(),
		HitCount:   0,
		TableDeps:  cloneStrings(tableDeps),
	}

	// Check max entries limit
	for c.config.MaxEntries > 0 && len(c.entries) >= c.config.MaxEntries {
		if !c.evictLRU() {
			break
		}
	}

	// Add new entry
	c.entries[key] = entry
	elem := c.lruList.PushFront(entry)
	c.elemMap[key] = elem
	atomic.AddInt64(&c.currentSize, size)

	// Track table dependencies
	c.trackTableDeps(key, tableDeps)
}

// Delete removes an entry from the cache
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteLocked(key)
}

func (c *Cache) deleteLocked(key string) {
	if entry, exists := c.entries[key]; exists {
		if elem, ok := c.elemMap[key]; ok {
			c.lruList.Remove(elem)
			delete(c.elemMap, key)
		}
		delete(c.entries, key)
		atomic.AddInt64(&c.currentSize, -entry.Size)

		// Remove table dependencies
		c.untrackTableDeps(key, entry.TableDeps)
	}
}

// InvalidateTable removes all entries that depend on a specific table
func (c *Cache) InvalidateTable(table string) {
	c.depMu.RLock()
	deps, exists := c.tableDeps[table]
	if !exists {
		c.depMu.RUnlock()
		return
	}

	// Copy keys to avoid deadlock
	keys := make([]string, 0, len(deps))
	for key := range deps {
		keys = append(keys, key)
	}
	c.depMu.RUnlock()

	// Delete all dependent entries
	for _, key := range keys {
		c.Delete(key)
	}
}

// InvalidateAll clears the entire cache
func (c *Cache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*Entry)
	c.lruList.Init()
	c.elemMap = make(map[string]*list.Element)
	atomic.StoreInt64(&c.currentSize, 0)

	c.depMu.Lock()
	c.tableDeps = make(map[string]map[string]struct{})
	c.depMu.Unlock()
}

// Stats returns cache statistics
func (c *Cache) Stats() Stats {
	c.mu.RLock()
	entryCount := len(c.entries)
	c.mu.RUnlock()

	return Stats{
		Hits:        atomic.LoadUint64(&c.hits),
		Misses:      atomic.LoadUint64(&c.misses),
		Evictions:   atomic.LoadUint64(&c.evicts),
		EntryCount:  entryCount,
		CurrentSize: atomic.LoadInt64(&c.currentSize),
		MaxSize:     c.config.MaxSize,
		HitRate:     c.calculateHitRate(),
	}
}

// Stats holds cache statistics
type Stats struct {
	Hits        uint64  `json:"hits"`
	Misses      uint64  `json:"misses"`
	Evictions   uint64  `json:"evictions"`
	EntryCount  int     `json:"entry_count"`
	CurrentSize int64   `json:"current_size"`
	MaxSize     int64   `json:"max_size"`
	HitRate     float64 `json:"hit_rate"`
}

// evictLRU removes the least recently used entry.
// The caller must hold c.mu.
func (c *Cache) evictLRU() bool {
	elem := c.lruList.Back()
	if elem == nil {
		return false
	}

	entry, ok := elem.Value.(*Entry)
	if !ok {
		return false
	}

	c.lruList.Remove(elem)
	delete(c.elemMap, entry.Key)
	delete(c.entries, entry.Key)
	atomic.AddInt64(&c.currentSize, -entry.Size)
	atomic.AddUint64(&c.evicts, 1)

	// Remove table dependencies
	c.untrackTableDeps(entry.Key, entry.TableDeps)

	return true
}

// trackTableDeps tracks which tables a query depends on
func (c *Cache) trackTableDeps(key string, tables []string) {
	c.depMu.Lock()
	defer c.depMu.Unlock()

	for _, table := range tables {
		if _, exists := c.tableDeps[table]; !exists {
			c.tableDeps[table] = make(map[string]struct{})
		}
		c.tableDeps[table][key] = struct{}{}
	}
}

// untrackTableDeps removes table dependency tracking
func (c *Cache) untrackTableDeps(key string, tables []string) {
	c.depMu.Lock()
	defer c.depMu.Unlock()

	for _, table := range tables {
		if deps, exists := c.tableDeps[table]; exists {
			delete(deps, key)
			if len(deps) == 0 {
				delete(c.tableDeps, table)
			}
		}
	}
}

// cleanupLoop periodically cleans expired entries
func (c *Cache) cleanupLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.cleanupExpired()
		}
	}
}

// cleanupExpired removes expired entries
func (c *Cache) cleanupExpired() {
	now := time.Now()
	var toDelete []string

	c.mu.RLock()
	for key, entry := range c.entries {
		if c.config.TTL > 0 && now.Sub(entry.CreatedAt) > c.config.TTL {
			toDelete = append(toDelete, key)
		}
	}
	c.mu.RUnlock()

	for _, key := range toDelete {
		c.Delete(key)
	}
}

// calculateHitRate returns the cache hit rate
func (c *Cache) calculateHitRate() float64 {
	hits := atomic.LoadUint64(&c.hits)
	misses := atomic.LoadUint64(&c.misses)
	total := hits + misses

	if total == 0 {
		return 0.0
	}

	return float64(hits) / float64(total) * 100.0
}

func cloneEntry(entry *Entry) *Entry {
	if entry == nil {
		return nil
	}
	copy := *entry
	copy.Args = cloneValues(entry.Args)
	copy.Columns = cloneStrings(entry.Columns)
	copy.Rows = cloneRows(entry.Rows)
	copy.TableDeps = cloneStrings(entry.TableDeps)
	return &copy
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func cloneValues(values []interface{}) []interface{} {
	if values == nil {
		return nil
	}
	cloned := make([]interface{}, len(values))
	for i, value := range values {
		cloned[i] = cloneValue(value)
	}
	return cloned
}

func cloneRows(rows [][]interface{}) [][]interface{} {
	if rows == nil {
		return nil
	}
	cloned := make([][]interface{}, len(rows))
	for i, row := range rows {
		cloned[i] = cloneValues(row)
	}
	return cloned
}

func cloneValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case []byte:
		if typed == nil {
			return []byte(nil)
		}
		cloned := make([]byte, len(typed))
		copy(cloned, typed)
		return cloned
	case []interface{}:
		return cloneValues(typed)
	case []string:
		return cloneStrings(typed)
	case map[string]interface{}:
		cloned := make(map[string]interface{}, len(typed))
		for key, mapValue := range typed {
			cloned[key] = cloneValue(mapValue)
		}
		return cloned
	case map[string]string:
		return cloneStringMap(typed)
	default:
		return typed
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

// sha256Pool recycles SHA256 hashers to avoid allocation per cache key.
var sha256Pool = sync.Pool{
	New: func() interface{} {
		return sha256.New()
	},
}

// generateKey creates a cache key from SQL and args.
// Uses a pooled hasher and stack buffers to minimize allocations.
func generateKey(sql string, args []interface{}) string {
	h := sha256Pool.Get().(hash.Hash)
	h.Reset()

	h.Write([]byte(sql))
	for _, arg := range args {
		h.Write([]byte("|"))
		h.Write([]byte(argToString(arg)))
	}

	var sumBuf [32]byte
	sum := h.Sum(sumBuf[:0])
	sha256Pool.Put(h)

	var hexBuf [64]byte
	hex.Encode(hexBuf[:], sum)
	return string(hexBuf[:])
}

func argToString(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// estimateSize estimates the memory size of cached data
func estimateSize(columns []string, rows [][]interface{}) int64 {
	size := int64(0)

	// Column names
	for _, col := range columns {
		size += int64(len(col))
	}

	// Rows (rough estimate)
	for _, row := range rows {
		for _, val := range row {
			switch v := val.(type) {
			case string:
				size += int64(len(v))
			case []byte:
				size += int64(len(v))
			case nil:
				size += 8
			default:
				size += 16 // rough estimate for other types
			}
		}
	}

	// Overhead for cache entry structure
	size += 256

	return size
}
