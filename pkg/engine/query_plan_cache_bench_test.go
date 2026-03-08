package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// BenchmarkQueryPlanCacheGet benchmarks cache retrieval with O(1) LRU
func BenchmarkQueryPlanCacheGet(b *testing.B) {
	config := &QueryPlanCacheConfig{
		Enabled:         true,
		MaxSize:         1000,
		TTL:             1 * time.Hour,
		InvalidateOnDDL: true,
	}
	cache := NewQueryPlanCache(config)

	// Populate cache
	for i := 0; i < 1000; i++ {
		sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i)
		plan := &ExecutionPlan{
			SQL:        sql,
			StmtType:   "SELECT",
			TableNames: []string{"users"},
			LastUsed:   time.Now(),
		}
		cache.Put(sql, plan)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i%1000)
			cache.Get(sql)
			i++
		}
	})
}

// BenchmarkQueryPlanCachePut benchmarks cache insertion with O(1) LRU
func BenchmarkQueryPlanCachePut(b *testing.B) {
	config := &QueryPlanCacheConfig{
		Enabled:         true,
		MaxSize:         1000,
		TTL:             1 * time.Hour,
		InvalidateOnDDL: true,
	}
	cache := NewQueryPlanCache(config)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i)
			plan := &ExecutionPlan{
				SQL:        sql,
				StmtType:   "SELECT",
				TableNames: []string{"users"},
				LastUsed:   time.Now(),
			}
			cache.Put(sql, plan)
			i++
		}
	})
}

// BenchmarkQueryPlanCacheMixed benchmarks mixed get/put operations
func BenchmarkQueryPlanCacheMixed(b *testing.B) {
	config := &QueryPlanCacheConfig{
		Enabled:         true,
		MaxSize:         1000,
		TTL:             1 * time.Hour,
		InvalidateOnDDL: true,
	}
	cache := NewQueryPlanCache(config)

	// Populate cache
	for i := 0; i < 500; i++ {
		sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i)
		plan := &ExecutionPlan{
			SQL:        sql,
			StmtType:   "SELECT",
			TableNames: []string{"users"},
			LastUsed:   time.Now(),
		}
		cache.Put(sql, plan)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				// 50% reads
				sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i%500)
				cache.Get(sql)
			} else {
				// 50% writes
				sql := fmt.Sprintf("SELECT * FROM orders WHERE id = %d", i)
				plan := &ExecutionPlan{
					SQL:        sql,
					StmtType:   "SELECT",
					TableNames: []string{"orders"},
					LastUsed:   time.Now(),
				}
				cache.Put(sql, plan)
			}
			i++
		}
	})
}

// BenchmarkQueryPlanCacheInvalidateByTable benchmarks table-based invalidation
func BenchmarkQueryPlanCacheInvalidateByTable(b *testing.B) {
	config := &QueryPlanCacheConfig{
		Enabled:         true,
		MaxSize:         1000,
		TTL:             1 * time.Hour,
		InvalidateOnDDL: true,
	}

	for i := 0; i < b.N; i++ {
		cache := NewQueryPlanCache(config)

		// Populate cache with mixed tables
		for j := 0; j < 1000; j++ {
			table := "users"
			if j%2 == 0 {
				table = "orders"
			}
			sql := fmt.Sprintf("SELECT * FROM %s WHERE id = %d", table, j)
			plan := &ExecutionPlan{
				SQL:        sql,
				StmtType:   "SELECT",
				TableNames: []string{table},
				LastUsed:   time.Now(),
			}
			cache.Put(sql, plan)
		}

		// Invalidate all plans for "users" table
		cache.InvalidateByTable("users")
	}
}

// BenchmarkCreatePlan benchmarks plan creation
func BenchmarkCreatePlan(b *testing.B) {
	sql := "SELECT * FROM users WHERE id = 1"
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CreatePlan(stmt, sql)
	}
}

// BenchmarkQueryPlanCacheWithHighHitRate benchmarks high hit rate scenario
func BenchmarkQueryPlanCacheWithHighHitRate(b *testing.B) {
	config := &QueryPlanCacheConfig{
		Enabled:         true,
		MaxSize:         1000,
		TTL:             1 * time.Hour,
		InvalidateOnDDL: true,
	}
	cache := NewQueryPlanCache(config)

	// Populate with 100 queries
	queries := make([]string, 100)
	for i := 0; i < 100; i++ {
		sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i)
		queries[i] = sql
		plan := &ExecutionPlan{
			SQL:        sql,
			StmtType:   "SELECT",
			TableNames: []string{"users"},
			LastUsed:   time.Now(),
		}
		cache.Put(sql, plan)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 99% hit rate - always query the same 10 plans
		sql := queries[i%10]
		cache.Get(sql)
	}
}

// BenchmarkQueryPlanCacheWithLowHitRate benchmarks low hit rate scenario
func BenchmarkQueryPlanCacheWithLowHitRate(b *testing.B) {
	config := &QueryPlanCacheConfig{
		Enabled:         true,
		MaxSize:         1000,
		TTL:             1 * time.Hour,
		InvalidateOnDDL: true,
	}
	cache := NewQueryPlanCache(config)

	// Populate with 1000 queries
	queries := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i)
		queries[i] = sql
		plan := &ExecutionPlan{
			SQL:        sql,
			StmtType:   "SELECT",
			TableNames: []string{"users"},
			LastUsed:   time.Now(),
		}
		cache.Put(sql, plan)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Low hit rate - query random plans
		sql := queries[i%1000]
		cache.Get(sql)
	}
}

// BenchmarkQueryPlanCacheEviction benchmarks eviction behavior
func BenchmarkQueryPlanCacheEviction(b *testing.B) {
	config := &QueryPlanCacheConfig{
		Enabled:         true,
		MaxSize:         100,
		TTL:             1 * time.Hour,
		InvalidateOnDDL: true,
	}

	for i := 0; i < b.N; i++ {
		cache := NewQueryPlanCache(config)

		// Insert more than MaxSize to trigger eviction
		for j := 0; j < 200; j++ {
			sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", j)
			plan := &ExecutionPlan{
				SQL:        sql,
				StmtType:   "SELECT",
				TableNames: []string{"users"},
				LastUsed:   time.Now(),
			}
			cache.Put(sql, plan)
		}
	}
}

// BenchmarkStats benchmarks statistics retrieval
func BenchmarkQueryPlanCacheStats(b *testing.B) {
	config := &QueryPlanCacheConfig{
		Enabled:         true,
		MaxSize:         1000,
		TTL:             1 * time.Hour,
		InvalidateOnDDL: true,
	}
	cache := NewQueryPlanCache(config)

	// Populate cache
	for i := 0; i < 1000; i++ {
		sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i)
		plan := &ExecutionPlan{
			SQL:        sql,
			StmtType:   "SELECT",
			TableNames: []string{"users"},
			LastUsed:   time.Now(),
		}
		cache.Put(sql, plan)
	}

	// Do some operations to generate stats
	for i := 0; i < 5000; i++ {
		sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i%1000)
		cache.Get(sql)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Stats()
	}
}
