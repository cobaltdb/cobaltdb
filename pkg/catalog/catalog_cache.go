package catalog

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// QueryCacheEntry holds cached query results (used by old catalog.QueryCache, kept for catalog_cache_test.go)
type QueryCacheEntry struct {
	Columns   []string
	Rows      [][]interface{}
	Timestamp time.Time
	Tables    []string
}

// QueryCache manages cached query results.
// Deprecated: use pkg/cache.Cache instead. This struct is retained only
// for catalog_cache_test.go which tests the old cache isolation behavior.
type QueryCache struct {
	entries   map[string]*QueryCacheEntry
	lru       *list.List
	lruMap    map[string]*list.Element
	maxSize   int
	ttl       time.Duration
	enabled   bool
	hitCount  atomic.Int64
	missCount atomic.Int64
	mu        sync.RWMutex
}

// NewQueryCache creates a new LRU query result cache.
// Deprecated: use cache.New instead.
func NewQueryCache(maxSize int, ttl time.Duration) *QueryCache {
	return &QueryCache{
		entries: make(map[string]*QueryCacheEntry),
		lru:     list.New(),
		lruMap:  make(map[string]*list.Element),
		maxSize: maxSize,
		ttl:     ttl,
		enabled: maxSize > 0,
	}
}

func (qc *QueryCache) Get(key string) (*QueryCacheEntry, bool) {
	if !qc.enabled {
		return nil, false
	}

	qc.mu.RLock()
	entry, exists := qc.entries[key]
	if !exists {
		qc.mu.RUnlock()
		qc.missCount.Add(1)
		return nil, false
	}

	expired := qc.ttl > 0 && time.Since(entry.Timestamp) > qc.ttl
	entryCopy := cloneQueryCacheEntry(entry)
	qc.mu.RUnlock()

	if expired {
		qc.missCount.Add(1)
		return nil, false
	}

	qc.hitCount.Add(1)
	qc.mu.Lock()
	qc.promoteInLRU(key)
	qc.mu.Unlock()

	return entryCopy, true
}

func (qc *QueryCache) promoteInLRU(key string) {
	if elem, ok := qc.lruMap[key]; ok {
		qc.lru.MoveToFront(elem)
	}
}

func (qc *QueryCache) Set(key string, columns []string, rows [][]interface{}, tables []string) {
	if !qc.enabled {
		return
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	if _, exists := qc.entries[key]; exists {
		if elem, ok := qc.lruMap[key]; ok {
			qc.lru.MoveToFront(elem)
		}
	} else {
		for len(qc.entries) >= qc.maxSize {
			qc.evictOne()
		}
		elem := qc.lru.PushFront(key)
		qc.lruMap[key] = elem
	}

	qc.entries[key] = &QueryCacheEntry{
		Columns:   cloneStringSlice(columns),
		Rows:      cloneInterfaceRows(rows),
		Timestamp: time.Now(),
		Tables:    cloneStringSlice(tables),
	}
}

func (qc *QueryCache) Invalidate(tableName string) {
	if !qc.enabled {
		return
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	for key, entry := range qc.entries {
		for _, tbl := range entry.Tables {
			if strings.EqualFold(tbl, tableName) {
				if elem, ok := qc.lruMap[key]; ok {
					qc.lru.Remove(elem)
					delete(qc.lruMap, key)
				}
				delete(qc.entries, key)
				break
			}
		}
	}
}

func (qc *QueryCache) InvalidateAll() {
	if !qc.enabled {
		return
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	qc.entries = make(map[string]*QueryCacheEntry)
	qc.lru.Init()
	qc.lruMap = make(map[string]*list.Element)
}

func (qc *QueryCache) evictOne() {
	back := qc.lru.Back()
	if back != nil {
		key := back.Value.(string)
		qc.lru.Remove(back)
		delete(qc.lruMap, key)
		delete(qc.entries, key)
	}
}

func (qc *QueryCache) Stats() (hits, misses int64, size int) {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return qc.hitCount.Load(), qc.missCount.Load(), len(qc.entries)
}

func cloneQueryCacheEntry(entry *QueryCacheEntry) *QueryCacheEntry {
	if entry == nil {
		return nil
	}
	cloned := *entry
	cloned.Columns = cloneStringSlice(entry.Columns)
	cloned.Rows = cloneInterfaceRows(entry.Rows)
	cloned.Tables = cloneStringSlice(entry.Tables)
	return &cloned
}

// generateQueryKey builds a cache key from a SQL string and query arguments.
// Deprecated: use query.GenerateQueryKey.
func generateQueryKey(sql string, args []interface{}) string {
	return query.GenerateQueryKey(sql, args)
}

// isCacheableQuery returns true if the SELECT statement is safe to cache.
// Deprecated: use query.IsCacheableQuery.
func isCacheableQuery(stmt *query.SelectStmt) bool {
	return query.IsCacheableQuery(stmt)
}

// extractTablesFromQuery returns the set of table names referenced by a SELECT.
// Deprecated: use query.ExtractTablesFromQuery.
func extractTablesFromQuery(stmt *query.SelectStmt) []string {
	return query.ExtractTablesFromQuery(stmt)
}

// queryToSQL produces a rough SQL string from a SELECT statement.
// Deprecated: use query.QueryToSQL.
func queryToSQL(stmt *query.SelectStmt) string {
	return query.QueryToSQL(stmt)
}

// exprToString is kept for backward compatibility only.
// Deprecated: use query.ExprToString instead.
func exprToString(expr query.Expression) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *query.Identifier:
		return e.Name
	case *query.StarExpr:
		return "*"
	case *query.StringLiteral:
		return fmt.Sprintf("'%s'", e.Value)
	case *query.NumberLiteral:
		return fmt.Sprintf("%v", e.Value)
	case *query.AliasExpr:
		return exprToString(e.Expr) + " AS " + e.Alias
	case *query.FunctionCall:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = exprToString(arg)
		}
		return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))
	default:
		return fmt.Sprintf("%T", expr)
	}
}

// containsSubquery is exported for test coverage; delegates to query package.
func containsSubquery(expr query.Expression) bool { return query.ContainsSubquery(expr) }

// hasNonDeterministicFunction is exported for test coverage; delegates to query package.
func hasNonDeterministicFunction(expr query.Expression) bool { return query.HasNonDeterministicFunction(expr) }

// containsNonDeterministicFunctions is exported for test coverage; delegates to query package.
func containsNonDeterministicFunctions(stmt *query.SelectStmt) bool { return query.ContainsNonDeterministicFunctions(stmt) }