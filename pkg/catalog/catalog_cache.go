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
func generateQueryKey(sql string, args []interface{}) string {
	var builder strings.Builder
	builder.Grow(len(sql) + len(args)*16)
	builder.WriteString(sql)
	for _, arg := range args {
		builder.WriteByte('|')
		fmt.Fprint(&builder, arg)
	}
	return builder.String()
}

// isCacheableQuery returns true if the SELECT statement is safe to cache.
// Queries without a FROM clause, with subqueries in SELECT, or with
// non-deterministic functions are not cached.
func isCacheableQuery(stmt *query.SelectStmt) bool {
	if stmt.From == nil {
		return false
	}
	for _, col := range stmt.Columns {
		if containsSubquery(col) {
			return false
		}
	}
	if containsNonDeterministicFunctions(stmt) {
		return false
	}
	return true
}

func containsSubquery(expr query.Expression) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *query.SubqueryExpr, *query.ExistsExpr:
		return true
	case *query.AliasExpr:
		return containsSubquery(e.Expr)
	case *query.BinaryExpr:
		return containsSubquery(e.Left) || containsSubquery(e.Right)
	case *query.UnaryExpr:
		return containsSubquery(e.Expr)
	case *query.FunctionCall:
		for _, arg := range e.Args {
			if containsSubquery(arg) {
				return true
			}
		}
	}
	return false
}

func containsNonDeterministicFunctions(stmt *query.SelectStmt) bool {
	for _, col := range stmt.Columns {
		if hasNonDeterministicFunction(col) {
			return true
		}
	}
	if hasNonDeterministicFunction(stmt.Where) {
		return true
	}
	for _, ob := range stmt.OrderBy {
		if hasNonDeterministicFunction(ob.Expr) {
			return true
		}
	}
	return false
}

func hasNonDeterministicFunction(expr query.Expression) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *query.FunctionCall:
		nonDetFuncs := []string{"RANDOM", "RAND", "NOW", "CURRENT_TIMESTAMP", "UUID", "NEWID"}
		for _, ndf := range nonDetFuncs {
			if strings.EqualFold(e.Name, ndf) {
				return true
			}
		}
		for _, arg := range e.Args {
			if hasNonDeterministicFunction(arg) {
				return true
			}
		}
	case *query.AliasExpr:
		return hasNonDeterministicFunction(e.Expr)
	case *query.BinaryExpr:
		return hasNonDeterministicFunction(e.Left) || hasNonDeterministicFunction(e.Right)
	case *query.UnaryExpr:
		return hasNonDeterministicFunction(e.Expr)
	}
	return false
}

// extractTablesFromQuery returns the set of table names referenced by a SELECT.
func extractTablesFromQuery(stmt *query.SelectStmt) []string {
	tables := make(map[string]bool)
	if stmt.From != nil {
		tables[stmt.From.Name] = true
	}
	for _, join := range stmt.Joins {
		if join.Table != nil {
			tables[join.Table.Name] = true
		}
	}
	result := make([]string, 0, len(tables))
	for tbl := range tables {
		result = append(result, tbl)
	}
	return result
}

// queryToSQL produces a rough SQL string from a SELECT statement.
// This is used for cache key generation and is not a full serializer.
func queryToSQL(stmt *query.SelectStmt) string {
	var parts []string
	parts = append(parts, "SELECT")
	if stmt.Distinct {
		parts = append(parts, "DISTINCT")
	}
	colParts := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		colParts[i] = exprToString(col)
	}
	parts = append(parts, strings.Join(colParts, ", "))
	if stmt.From != nil {
		parts = append(parts, "FROM", stmt.From.Name)
	}
	return strings.Join(parts, " ")
}

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