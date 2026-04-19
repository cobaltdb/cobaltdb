// Package cache provides query result caching for CobaltDB
package cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	mu       sync.RWMutex
	entries  map[string]*Entry
	lruList  *list.List // LRU eviction list
	elemMap  map[string]*list.Element

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

	c.mu.RLock()
	entry, exists := c.entries[key]
	c.mu.RUnlock()

	if !exists {
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}

	// Check if expired
	if time.Since(entry.CreatedAt) > c.config.TTL {
		c.Delete(key)
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}

	// Update access time and hit count
	c.mu.Lock()
	entry.AccessedAt = time.Now()
	entry.HitCount++

	// Move to front of LRU list
	if elem, ok := c.elemMap[key]; ok {
		c.lruList.MoveToFront(elem)
	}
	c.mu.Unlock()

	atomic.AddUint64(&c.hits, 1)
	return entry, true
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
	if size > c.config.MaxSize/10 { // Don't cache if > 10% of max size
		return
	}

	// Make room if needed
	for atomic.LoadInt64(&c.currentSize)+size > c.config.MaxSize {
		if !c.evictLRU() {
			break // Can't evict anything
		}
	}

	entry := &Entry{
		Key:        key,
		SQL:        sql,
		Args:       args,
		Columns:    columns,
		Rows:       rows,
		Size:       size,
		CreatedAt:  time.Now(),
		AccessedAt: time.Now(),
		HitCount:   0,
		TableDeps:  tableDeps,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove old entry if exists
	if oldElem, exists := c.elemMap[key]; exists {
		if oldEntry, ok := oldElem.Value.(*Entry); ok {
			atomic.AddInt64(&c.currentSize, -oldEntry.Size)
		}
		c.lruList.Remove(oldElem)
	}

	// Check max entries limit
	if c.config.MaxEntries > 0 && len(c.entries) >= c.config.MaxEntries {
		c.evictLRU()
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

// evictLRU removes the least recently used entry
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
		if now.Sub(entry.CreatedAt) > c.config.TTL {
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

// generateKey creates a cache key from SQL and args
func generateKey(sql string, args []interface{}) string {
	h := sha256.New()
	h.Write([]byte(sql))
	for _, arg := range args {
		h.Write([]byte(fmt.Sprintf("|%v", arg)))
	}
	return hex.EncodeToString(h.Sum(nil))
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
