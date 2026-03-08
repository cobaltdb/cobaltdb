package engine

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// QueryPlanCacheConfig configures the query plan cache
type QueryPlanCacheConfig struct {
	Enabled         bool          // Whether plan caching is enabled
	MaxSize         int           // Maximum number of plans to cache (default: 1000)
	TTL             time.Duration // Time-to-live for cached plans (default: 1 hour)
	InvalidateOnDDL bool          // Invalidate plans on DDL changes (default: true)
}

// DefaultQueryPlanCacheConfig returns default configuration
func DefaultQueryPlanCacheConfig() *QueryPlanCacheConfig {
	return &QueryPlanCacheConfig{
		Enabled:         true,
		MaxSize:         1000,
		TTL:             1 * time.Hour,
		InvalidateOnDDL: true,
	}
}

// ExecutionPlan represents a cached query execution plan
type ExecutionPlan struct {
	SQL           string
	StmtType      string
	TableNames    []string
	IndexHints    []string
	EstimatedCost int
	CreatedAt     time.Time
	LastUsed      time.Time
	UseCount      uint64
}

// IsStale returns true if the plan has expired
func (ep *ExecutionPlan) IsStale(ttl time.Duration) bool {
	return time.Since(ep.LastUsed) > ttl
}

// lruEntry represents an entry in the LRU list
type lruEntry struct {
	sql  string
	plan *ExecutionPlan
}

// QueryPlanCache caches query execution plans using O(1) LRU eviction
type QueryPlanCache struct {
	config     *QueryPlanCacheConfig
	plans      map[string]*list.Element // SQL -> list element (for O(1) lookup)
	lruList    *list.List               // Doubly-linked list for O(1) LRU
	mu         sync.RWMutex
	hitCount   uint64
	missCount  uint64
	evictCount uint64
}

// NewQueryPlanCache creates a new query plan cache
func NewQueryPlanCache(config *QueryPlanCacheConfig) *QueryPlanCache {
	if config == nil {
		config = DefaultQueryPlanCacheConfig()
	}
	return &QueryPlanCache{
		config:  config,
		plans:   make(map[string]*list.Element),
		lruList: list.New(),
	}
}

// Get retrieves a cached execution plan
func (c *QueryPlanCache) Get(sql string) (*ExecutionPlan, bool) {
	if !c.config.Enabled {
		return nil, false
	}

	c.mu.RLock()
	element, exists := c.plans[sql]
	c.mu.RUnlock()

	if !exists {
		atomic.AddUint64(&c.missCount, 1)
		return nil, false
	}

	entry := element.Value.(*lruEntry)
	plan := entry.plan

	// Check if stale
	if plan.IsStale(c.config.TTL) {
		c.mu.Lock()
		// Double-check after acquiring lock
		if element, exists := c.plans[sql]; exists {
			entry := element.Value.(*lruEntry)
			if entry.plan.IsStale(c.config.TTL) {
				c.removeElement(element)
				atomic.AddUint64(&c.evictCount, 1)
			}
		}
		c.mu.Unlock()
		atomic.AddUint64(&c.missCount, 1)
		return nil, false
	}

	// Update stats and move to front (O(1))
	c.mu.Lock()
	plan.LastUsed = time.Now()
	plan.UseCount++
	c.lruList.MoveToFront(element)
	c.mu.Unlock()

	atomic.AddUint64(&c.hitCount, 1)
	return plan, true
}

// Put adds or updates a cached execution plan
func (c *QueryPlanCache) Put(sql string, plan *ExecutionPlan) {
	if !c.config.Enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Update plan metadata
	plan.SQL = sql
	plan.LastUsed = time.Now()

	// Check if this is an update
	if element, exists := c.plans[sql]; exists {
		// Update existing entry
		entry := element.Value.(*lruEntry)
		entry.plan = plan
		c.lruList.MoveToFront(element)
		return
	}

	// Check if we need to evict (before adding new)
	if len(c.plans) >= c.config.MaxSize {
		c.evictLRU()
	}

	// Add new entry at front
	entry := &lruEntry{sql: sql, plan: plan}
	element := c.lruList.PushFront(entry)
	c.plans[sql] = element
}

// Invalidate removes a cached plan
func (c *QueryPlanCache) Invalidate(sql string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, exists := c.plans[sql]; exists {
		c.removeElement(element)
	}
}

// InvalidateByTable invalidates all plans that reference a given table
func (c *QueryPlanCache) InvalidateByTable(tableName string) {
	if !c.config.InvalidateOnDDL {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var toInvalidate []string
	for sql, element := range c.plans {
		entry := element.Value.(*lruEntry)
		for _, t := range entry.plan.TableNames {
			if t == tableName {
				toInvalidate = append(toInvalidate, sql)
				break
			}
		}
	}

	for _, sql := range toInvalidate {
		if element, exists := c.plans[sql]; exists {
			c.removeElement(element)
		}
	}
}

// InvalidateAll clears the entire cache
func (c *QueryPlanCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.plans = make(map[string]*list.Element)
	c.lruList.Init()
}

// removeElement removes an element from the cache (must hold c.mu)
func (c *QueryPlanCache) removeElement(element *list.Element) {
	entry := element.Value.(*lruEntry)
	delete(c.plans, entry.sql)
	c.lruList.Remove(element)
}

// evictLRU evicts the least recently used item (must hold c.mu)
func (c *QueryPlanCache) evictLRU() {
	element := c.lruList.Back()
	if element == nil {
		return
	}

	c.removeElement(element)
	atomic.AddUint64(&c.evictCount, 1)
}

// Stats returns cache statistics
func (c *QueryPlanCache) Stats() QueryPlanCacheStats {
	c.mu.RLock()
	size := len(c.plans)
	c.mu.RUnlock()

	hits := atomic.LoadUint64(&c.hitCount)
	misses := atomic.LoadUint64(&c.missCount)
	total := hits + misses

	hitRatio := float64(0)
	if total > 0 {
		hitRatio = float64(hits) / float64(total)
	}

	return QueryPlanCacheStats{
		Size:       size,
		MaxSize:    c.config.MaxSize,
		HitCount:   hits,
		MissCount:  misses,
		HitRatio:   hitRatio,
		EvictCount: atomic.LoadUint64(&c.evictCount),
		Enabled:    c.config.Enabled,
	}
}

// QueryPlanCacheStats holds cache statistics
type QueryPlanCacheStats struct {
	Size       int     `json:"size"`
	MaxSize    int     `json:"max_size"`
	HitCount   uint64  `json:"hit_count"`
	MissCount  uint64  `json:"miss_count"`
	HitRatio   float64 `json:"hit_ratio"`
	EvictCount uint64  `json:"evict_count"`
	Enabled    bool    `json:"enabled"`
}

// CreatePlan creates an execution plan for a statement
func CreatePlan(stmt query.Statement, sql string) *ExecutionPlan {
	plan := &ExecutionPlan{
		SQL:       sql,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
		UseCount:  1,
	}

	switch s := stmt.(type) {
	case *query.SelectStmt:
		plan.StmtType = "SELECT"
		if s.From != nil {
			plan.TableNames = []string{s.From.Name}
		}
	case *query.InsertStmt:
		plan.StmtType = "INSERT"
		plan.TableNames = []string{s.Table}
	case *query.UpdateStmt:
		plan.StmtType = "UPDATE"
		plan.TableNames = []string{s.Table}
	case *query.DeleteStmt:
		plan.StmtType = "DELETE"
		plan.TableNames = []string{s.Table}
	default:
		plan.StmtType = "OTHER"
	}

	return plan
}
