package engine

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// QueryPlanCacheEntry represents a cached query plan
type QueryPlanCacheEntry struct {
	SQL          string
	ParsedStmt   query.Statement
	CreatedAt    time.Time
	LastAccessed time.Time
	AccessCount  uint64
	Size         int64 // Estimated size in bytes
}

// QueryPlanCache provides LRU caching for query plans
type QueryPlanCache struct {
	mu            sync.RWMutex
	entries       map[string]*list.Element // Hash -> List Element
	lruList       *list.List
	maxSize       int64
	currentSize   int64
	maxEntries    int

	// Statistics
	hits          uint64
	misses        uint64
	evictions     uint64
	invalidations uint64
}

// QueryPlanCacheStats contains cache statistics
type QueryPlanCacheStats struct {
	Size          int
	CurrentBytes  int64
	MaxBytes      int64
	HitRate       float64
	Hits          uint64
	Misses        uint64
	Evictions     uint64
	Invalidations uint64
}

// NewQueryPlanCache creates a new query plan cache
func NewQueryPlanCache(maxSize int64, maxEntries int) *QueryPlanCache {
	if maxSize <= 0 {
		maxSize = 64 * 1024 * 1024 // 64MB default
	}
	if maxEntries <= 0 {
		maxEntries = 1000
	}

	return &QueryPlanCache{
		entries:    make(map[string]*list.Element),
		lruList:    list.New(),
		maxSize:    maxSize,
		maxEntries: maxEntries,
	}
}

// Get retrieves a cached query plan
func (c *QueryPlanCache) Get(sql string, args []interface{}) (*QueryPlanCacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := c.hashQuery(sql, args)
	elem, found := c.entries[hash]
	if !found {
		c.misses++
		return nil, false
	}

	entry := elem.Value.(*QueryPlanCacheEntry)
	entry.LastAccessed = time.Now()
	entry.AccessCount++

	// Move to front (most recently used)
	c.lruList.MoveToFront(elem)
	c.hits++

	return entry, true
}

// Put adds a query plan to the cache
func (c *QueryPlanCache) Put(sql string, args []interface{}, stmt query.Statement) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := c.hashQuery(sql, args)

	// Check if entry already exists
	if elem, exists := c.entries[hash]; exists {
		// Update existing entry
		oldEntry := elem.Value.(*QueryPlanCacheEntry)
		c.currentSize -= oldEntry.Size

		newEntry := c.createEntry(sql, stmt)
		elem.Value = newEntry
		c.lruList.MoveToFront(elem)
		c.currentSize += newEntry.Size
		return nil
	}

	// Evict entries if necessary
	entrySize := c.estimateEntrySize(sql, stmt)
	for (c.currentSize+entrySize > c.maxSize || len(c.entries) >= c.maxEntries) && c.lruList.Len() > 0 {
		c.evictLRU()
	}

	// Create and add new entry
	entry := c.createEntry(sql, stmt)
	elem := c.lruList.PushFront(entry)
	c.entries[hash] = elem
	c.currentSize += entry.Size

	return nil
}

// Invalidate removes a query plan from the cache
func (c *QueryPlanCache) Invalidate(sql string, args []interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := c.hashQuery(sql, args)
	if elem, exists := c.entries[hash]; exists {
		entry := elem.Value.(*QueryPlanCacheEntry)
		c.currentSize -= entry.Size
		delete(c.entries, hash)
		c.lruList.Remove(elem)
		c.invalidations++
	}
}

// Clear removes all entries from the cache
func (c *QueryPlanCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*list.Element)
	c.lruList = list.New()
	c.currentSize = 0
	c.invalidations += uint64(len(c.entries))
}

// GetStats returns cache statistics
func (c *QueryPlanCache) GetStats() QueryPlanCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hitRate := 0.0
	total := c.hits + c.misses
	if total > 0 {
		hitRate = float64(c.hits) / float64(total) * 100
	}

	return QueryPlanCacheStats{
		Size:          len(c.entries),
		CurrentBytes:  c.currentSize,
		MaxBytes:      c.maxSize,
		HitRate:       hitRate,
		Hits:          c.hits,
		Misses:        c.misses,
		Evictions:     c.evictions,
		Invalidations: c.invalidations,
	}
}

// GetTopQueries returns the most frequently accessed queries
func (c *QueryPlanCache) GetTopQueries(n int) []QueryPlanCacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if n <= 0 {
		n = 10
	}

	// Collect all entries
	allEntries := make([]QueryPlanCacheEntry, 0, len(c.entries))
	for elem := c.lruList.Front(); elem != nil; elem = elem.Next() {
		entry := *elem.Value.(*QueryPlanCacheEntry)
		allEntries = append(allEntries, entry)
	}

	// Sort by access count (simple bubble sort for small n)
	for i := 0; i < len(allEntries) && i < n; i++ {
		for j := i + 1; j < len(allEntries); j++ {
			if allEntries[j].AccessCount > allEntries[i].AccessCount {
				allEntries[i], allEntries[j] = allEntries[j], allEntries[i]
			}
		}
	}

	if len(allEntries) > n {
		return allEntries[:n]
	}
	return allEntries
}

// WarmCache pre-populates the cache with common queries
func (c *QueryPlanCache) WarmCache(queries []string) error {
	for _, sql := range queries {
		// Parse the query
		stmt, err := query.Parse(sql)
		if err != nil {
			continue // Skip invalid queries
		}

		// Add to cache
		c.Put(sql, nil, stmt)
	}
	return nil
}

// evictLRU removes the least recently used entry
func (c *QueryPlanCache) evictLRU() {
	elem := c.lruList.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*QueryPlanCacheEntry)
	hash := c.hashQuery(entry.SQL, nil)
	delete(c.entries, hash)
	c.lruList.Remove(elem)
	c.currentSize -= entry.Size
	c.evictions++
}

// createEntry creates a new cache entry
func (c *QueryPlanCache) createEntry(sql string, stmt query.Statement) *QueryPlanCacheEntry {
	now := time.Now()
	return &QueryPlanCacheEntry{
		SQL:          sql,
		ParsedStmt:   stmt,
		CreatedAt:    now,
		LastAccessed: now,
		AccessCount:  1,
		Size:         c.estimateEntrySize(sql, stmt),
	}
}

// hashQuery creates a hash for the query and args
func (c *QueryPlanCache) hashQuery(sql string, args []interface{}) string {
	h := sha256.New()
	h.Write([]byte(sql))
	if len(args) > 0 {
		// Include arg types in hash (not values for security)
		for _, arg := range args {
			h.Write([]byte(fmt.Sprintf("%T", arg)))
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// estimateEntrySize estimates the memory size of a cache entry
func (c *QueryPlanCache) estimateEntrySize(sql string, stmt query.Statement) int64 {
	// Rough estimation
	size := int64(len(sql))
	if stmt != nil {
		size += 512 // Base statement overhead
	}
	return size
}

// ResetStats resets cache statistics
func (c *QueryPlanCache) ResetStats() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.hits = 0
	c.misses = 0
	c.evictions = 0
	c.invalidations = 0
}
