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

// QueryCacheEntry holds cached query results
type QueryCacheEntry struct {
	Columns   []string
	Rows      [][]interface{}
	Timestamp time.Time
	Tables    []string // Tables involved in the query (for invalidation)
}

// QueryCacheStore defines the interface for query result caching.
type QueryCacheStore interface {
	Get(key string) (*QueryCacheEntry, bool)
	Set(key string, columns []string, rows [][]interface{}, tables []string)
	Invalidate(tableName string)
	InvalidateAll()
	Stats() (hits, misses int64, size int)
}

// QueryCache manages cached query results
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

// NewQueryCache creates a new LRU query result cache with the given maximum size and TTL.
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

	// Check if entry has expired. A non-positive TTL means entries do not
	// expire by age.
	expired := qc.ttl > 0 && time.Since(entry.Timestamp) > qc.ttl
	entryCopy := cloneQueryCacheEntry(entry)
	qc.mu.RUnlock()

	if expired {
		qc.missCount.Add(1)
		return nil, false
	}

	qc.hitCount.Add(1)

	// Promote in LRU under write lock
	qc.mu.Lock()
	qc.promoteInLRU(key)
	qc.mu.Unlock()

	return entryCopy, true
}

// promoteInLRU moves a key to the front of the LRU list (caller must hold at least RLock)
// Note: we upgrade to write lock for the LRU mutation
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

	// If key already exists, move to front
	if _, exists := qc.entries[key]; exists {
		if elem, ok := qc.lruMap[key]; ok {
			qc.lru.MoveToFront(elem)
		}
	} else {
		// Evict entries if cache is full
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

func cloneStringSlice(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func cloneInterfaceRows(rows [][]interface{}) [][]interface{} {
	if rows == nil {
		return nil
	}
	cloned := make([][]interface{}, len(rows))
	for i, row := range rows {
		cloned[i] = cloneInterfaceSlice(row)
	}
	return cloned
}

func cloneInterfaceSlice(values []interface{}) []interface{} {
	if values == nil {
		return nil
	}
	cloned := make([]interface{}, len(values))
	for i, value := range values {
		cloned[i] = cloneInterfaceValue(value)
	}
	return cloned
}

func cloneInterfaceValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case []byte:
		if typed == nil {
			return []byte(nil)
		}
		cloned := make([]byte, len(typed))
		copy(cloned, typed)
		return cloned
	case []interface{}:
		return cloneInterfaceSlice(typed)
	case map[string]interface{}:
		cloned := make(map[string]interface{}, len(typed))
		for key, mapValue := range typed {
			cloned[key] = cloneInterfaceValue(mapValue)
		}
		return cloned
	default:
		return typed
	}
}

func generateQueryKey(sql string, args []interface{}) string {
	// Use strings.Builder for efficient concatenation
	var builder strings.Builder
	builder.Grow(len(sql) + len(args)*16) // Pre-allocate estimated size
	builder.WriteString(sql)
	for _, arg := range args {
		builder.WriteByte('|')
		fmt.Fprint(&builder, arg)
	}
	return builder.String()
}

func isCacheableQuery(stmt *query.SelectStmt) bool {
	// Don't cache queries without a FROM clause (scalar queries might have functions like RANDOM())
	if stmt.From == nil {
		return false
	}

	// Don't cache queries with subqueries in SELECT (they might be non-deterministic)
	for _, col := range stmt.Columns {
		if containsSubquery(col) {
			return false
		}
	}

	// Don't cache queries with non-deterministic functions
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
	// Check columns
	for _, col := range stmt.Columns {
		if hasNonDeterministicFunction(col) {
			return true
		}
	}

	// Check WHERE clause
	if hasNonDeterministicFunction(stmt.Where) {
		return true
	}

	// Check ORDER BY
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
		// List of non-deterministic functions
		nonDetFuncs := []string{"RANDOM", "RAND", "NOW", "CURRENT_TIMESTAMP", "UUID", "NEWID"}
		for _, ndf := range nonDetFuncs {
			if strings.EqualFold(e.Name, ndf) {
				return true
			}
		}
		// Check arguments recursively
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

func queryToSQL(stmt *query.SelectStmt) string {
	// This is a simplified version - in production you'd want proper SQL generation
	// For caching purposes, we just need a consistent string representation
	var parts []string

	parts = append(parts, "SELECT")
	if stmt.Distinct {
		parts = append(parts, "DISTINCT")
	}

	// Columns
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
