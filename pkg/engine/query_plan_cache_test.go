package engine

import (
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestQueryPlanCacheBasic(t *testing.T) {
	config := &QueryPlanCacheConfig{
		Enabled: true,
		MaxSize: 100,
		TTL:     1 * time.Hour,
	}
	cache := NewQueryPlanCache(config)

	// Create a plan
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
	}
	plan := CreatePlan(stmt, "SELECT * FROM users")

	// Put in cache
	cache.Put("SELECT * FROM users", plan)

	// Get from cache
	cached, found := cache.Get("SELECT * FROM users")
	if !found {
		t.Error("Expected to find cached plan")
	}
	if cached.SQL != "SELECT * FROM users" {
		t.Errorf("Expected SQL 'SELECT * FROM users', got '%s'", cached.SQL)
	}
	if cached.StmtType != "SELECT" {
		t.Errorf("Expected StmtType 'SELECT', got '%s'", cached.StmtType)
	}
}

func TestQueryPlanCacheMiss(t *testing.T) {
	config := &QueryPlanCacheConfig{
		Enabled: true,
		MaxSize: 100,
	}
	cache := NewQueryPlanCache(config)

	// Try to get non-existent plan
	_, found := cache.Get("SELECT * FROM nonexistent")
	if found {
		t.Error("Expected cache miss for non-existent plan")
	}

	stats := cache.Stats()
	if stats.MissCount != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.MissCount)
	}
}

func TestQueryPlanCacheDisabled(t *testing.T) {
	config := &QueryPlanCacheConfig{
		Enabled: false,
		MaxSize: 100,
	}
	cache := NewQueryPlanCache(config)

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
	}
	plan := CreatePlan(stmt, "SELECT * FROM users")
	cache.Put("SELECT * FROM users", plan)

	_, found := cache.Get("SELECT * FROM users")
	if found {
		t.Error("Expected cache to be disabled")
	}
}

func TestQueryPlanCacheExpiration(t *testing.T) {
	config := &QueryPlanCacheConfig{
		Enabled: true,
		MaxSize: 100,
		TTL:     50 * time.Millisecond,
	}
	cache := NewQueryPlanCache(config)

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
	}
	plan := CreatePlan(stmt, "SELECT * FROM users")
	cache.Put("SELECT * FROM users", plan)

	// Should find immediately
	_, found := cache.Get("SELECT * FROM users")
	if !found {
		t.Error("Expected to find plan before expiration")
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should not find after expiration
	_, found = cache.Get("SELECT * FROM users")
	if found {
		t.Error("Expected plan to be expired")
	}
}

func TestQueryPlanCacheEviction(t *testing.T) {
	config := &QueryPlanCacheConfig{
		Enabled: true,
		MaxSize: 3,
		TTL:     1 * time.Hour,
	}
	cache := NewQueryPlanCache(config)

	// Add 3 plans
	for i := 0; i < 3; i++ {
		stmt := &query.SelectStmt{
			From: &query.TableRef{Name: "table"},
		}
		sql := "SELECT * FROM table WHERE id = " + string(rune('0'+i))
		plan := CreatePlan(stmt, sql)
		cache.Put(sql, plan)
	}

	// Access the first one to make it recently used
	cache.Get("SELECT * FROM table WHERE id = 0")

	// Add a 4th plan - should evict the least recently used (id=1)
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "table"},
	}
	plan := CreatePlan(stmt, "SELECT * FROM table WHERE id = 3")
	cache.Put("SELECT * FROM table WHERE id = 3", plan)

	// First one should still be there (recently used)
	_, found := cache.Get("SELECT * FROM table WHERE id = 0")
	if !found {
		t.Error("Expected recently used plan to still be cached")
	}

	// Evicted one should be gone
	_, found = cache.Get("SELECT * FROM table WHERE id = 1")
	if found {
		t.Error("Expected LRU plan to be evicted")
	}
}

func TestQueryPlanCacheInvalidate(t *testing.T) {
	config := &QueryPlanCacheConfig{
		Enabled: true,
		MaxSize: 100,
	}
	cache := NewQueryPlanCache(config)

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
	}
	plan := CreatePlan(stmt, "SELECT * FROM users")
	cache.Put("SELECT * FROM users", plan)

	// Invalidate
	cache.Invalidate("SELECT * FROM users")

	_, found := cache.Get("SELECT * FROM users")
	if found {
		t.Error("Expected plan to be invalidated")
	}
}

func TestQueryPlanCacheInvalidateByTable(t *testing.T) {
	config := &QueryPlanCacheConfig{
		Enabled:         true,
		MaxSize:         100,
		InvalidateOnDDL: true,
	}
	cache := NewQueryPlanCache(config)

	// Add plans for different tables
	stmt1 := &query.SelectStmt{From: &query.TableRef{Name: "users"}}
	cache.Put("SELECT * FROM users", CreatePlan(stmt1, "SELECT * FROM users"))

	stmt2 := &query.SelectStmt{From: &query.TableRef{Name: "orders"}}
	cache.Put("SELECT * FROM orders", CreatePlan(stmt2, "SELECT * FROM orders"))

	// Invalidate by table
	cache.InvalidateByTable("users")

	// Users plan should be gone
	_, found := cache.Get("SELECT * FROM users")
	if found {
		t.Error("Expected users plan to be invalidated")
	}

	// Orders plan should still be there
	_, found = cache.Get("SELECT * FROM orders")
	if !found {
		t.Error("Expected orders plan to still be cached")
	}
}

func TestQueryPlanCacheStats(t *testing.T) {
	config := &QueryPlanCacheConfig{
		Enabled: true,
		MaxSize: 100,
	}
	cache := NewQueryPlanCache(config)

	// Add some plans
	for i := 0; i < 5; i++ {
		stmt := &query.SelectStmt{From: &query.TableRef{Name: "users"}}
		cache.Put("SELECT * FROM users WHERE id = ?", CreatePlan(stmt, "SELECT * FROM users WHERE id = ?"))
	}

	// Get some plans (creates new ones each time since we're using the same SQL)
	for i := 0; i < 3; i++ {
		cache.Get("SELECT * FROM users WHERE id = ?")
	}

	stats := cache.Stats()
	if stats.Size != 1 {
		t.Errorf("Expected size 1 (same SQL), got %d", stats.Size)
	}
	if stats.HitCount != 3 {
		t.Errorf("Expected 3 hits, got %d", stats.HitCount)
	}
}

func TestCreatePlan(t *testing.T) {
	tests := []struct {
		stmt     query.Statement
		expected string
	}{
		{&query.SelectStmt{From: &query.TableRef{Name: "users"}}, "SELECT"},
		{&query.InsertStmt{Table: "users"}, "INSERT"},
		{&query.UpdateStmt{Table: "users"}, "UPDATE"},
		{&query.DeleteStmt{Table: "users"}, "DELETE"},
	}

	for _, tc := range tests {
		plan := CreatePlan(tc.stmt, "test")
		if plan.StmtType != tc.expected {
			t.Errorf("Expected StmtType '%s', got '%s'", tc.expected, plan.StmtType)
		}
	}
}

func BenchmarkQueryPlanCacheGetPut(b *testing.B) {
	config := &QueryPlanCacheConfig{
		Enabled: true,
		MaxSize: 1000,
		TTL:     1 * time.Hour,
	}
	cache := NewQueryPlanCache(config)

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
	}
	plan := CreatePlan(stmt, "SELECT * FROM users")
	cache.Put("SELECT * FROM users", plan)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				cache.Get("SELECT * FROM users")
			} else {
				cache.Put("SELECT * FROM users", plan)
			}
			i++
		}
	})
}
