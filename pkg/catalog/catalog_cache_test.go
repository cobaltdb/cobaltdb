package catalog

import (
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestQueryCache(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Enable query cache
	catalog.EnableQueryCache(100, 5*time.Minute)

	// Create test table
	err := catalog.CreateTable(&query.CreateTableStmt{
		Table: "cache_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, _, err = catalog.Insert(&query.InsertStmt{
		Table:   "cache_test",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}},
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "Charlie"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// First query - should be a cache miss
	cols1, rows1, err := catalog.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "cache_test"},
	}, nil)
	if err != nil {
		t.Fatalf("First SELECT failed: %v", err)
	}
	if len(rows1) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows1))
	}

	// Check cache stats - should have 0 hits, 1 miss
	hits, misses, size := catalog.GetQueryCacheStats()
	if hits != 0 {
		t.Errorf("Expected 0 hits after first query, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss after first query, got %d", misses)
	}
	if size != 1 {
		t.Errorf("Expected cache size 1, got %d", size)
	}

	// Second identical query - should be a cache hit
	cols2, rows2, err := catalog.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "cache_test"},
	}, nil)
	if err != nil {
		t.Fatalf("Second SELECT failed: %v", err)
	}
	if len(rows2) != 3 {
		t.Errorf("Expected 3 rows on second query, got %d", len(rows2))
	}

	// Verify column names match
	for i, col := range cols1 {
		if cols2[i] != col {
			t.Errorf("Column names don't match: %s vs %s", col, cols2[i])
		}
	}

	// Check cache stats - should have 1 hit, 1 miss
	hits, misses, size = catalog.GetQueryCacheStats()
	if hits != 1 {
		t.Errorf("Expected 1 hit after second query, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss after second query, got %d", misses)
	}

	t.Logf("Cache stats: hits=%d, misses=%d, size=%d", hits, misses, size)
}

func TestQueryCacheInvalidation(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Enable query cache
	catalog.EnableQueryCache(100, 5*time.Minute)

	// Create test table
	err := catalog.CreateTable(&query.CreateTableStmt{
		Table: "cache_inval",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, _, err = catalog.Insert(&query.InsertStmt{
		Table:   "cache_inval",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 100}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query to populate cache
	_, rows1, err := catalog.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "cache_inval"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(rows1) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows1))
	}
	t.Logf("First query val=%v (type=%T)", rows1[0][0], rows1[0][0])

	// Update the row
	_, _, err = catalog.Update(&query.UpdateStmt{
		Table: "cache_inval",
		Set:   []*query.SetClause{{Column: "val", Value: &query.NumberLiteral{Value: 200}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Query again - should see updated value (cache invalidated)
	_, rows2, err := catalog.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "cache_inval"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Fatalf("SELECT after UPDATE failed: %v", err)
	}
	if len(rows2) != 1 {
		t.Fatalf("Expected 1 row after update, got %d", len(rows2))
	}

	// Check that value was updated (should be 200 after update)
	// Value could be int64 or float64 depending on storage
	var val2 float64
	switch v := rows2[0][0].(type) {
	case float64:
		val2 = v
	case int64:
		val2 = float64(v)
	case int:
		val2 = float64(v)
	default:
		t.Fatalf("Unexpected type for val: %T", rows2[0][0])
	}
	if val2 != 200 {
		t.Errorf("Expected val=200 after update, got %v", rows2[0][0])
	}

	t.Log("Cache invalidation works correctly")
}

func TestQueryCacheNonCacheable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Enable query cache
	catalog.EnableQueryCache(100, 5*time.Minute)

	// Create test table
	err := catalog.CreateTable(&query.CreateTableStmt{
		Table: "cache_nc",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Query with subquery - should not be cached
	_, _, err = catalog.Select(&query.SelectStmt{
		Columns: []query.Expression{
			&query.SubqueryExpr{
				Query: &query.SelectStmt{
					Columns: []query.Expression{&query.Identifier{Name: "id"}},
					From:    &query.TableRef{Name: "cache_nc"},
				},
			},
		},
		From: &query.TableRef{Name: "cache_nc"},
	}, nil)
	if err != nil {
		t.Fatalf("SELECT with subquery failed: %v", err)
	}

	// Query without FROM - should not be cached
	_, _, err = catalog.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.NumberLiteral{Value: 1}},
	}, nil)
	if err != nil {
		t.Fatalf("SELECT without FROM failed: %v", err)
	}

	// Check cache stats - should be empty
	hits, misses, size := catalog.GetQueryCacheStats()
	if size != 0 {
		t.Errorf("Expected cache size 0 for non-cacheable queries, got %d", size)
	}

	t.Logf("Non-cacheable queries: hits=%d, misses=%d, size=%d", hits, misses, size)
}

func TestQueryCacheTTL(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Enable query cache with very short TTL
	catalog.EnableQueryCache(100, 100*time.Millisecond)

	// Create test table
	err := catalog.CreateTable(&query.CreateTableStmt{
		Table: "cache_ttl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, _, err = catalog.Insert(&query.InsertStmt{
		Table:   "cache_ttl",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// First query
	_, _, err = catalog.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "cache_ttl"},
	}, nil)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	// Wait for TTL to expire
	time.Sleep(200 * time.Millisecond)

	// Second query - should be a cache miss (entry expired)
	_, _, err = catalog.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "cache_ttl"},
	}, nil)
	if err != nil {
		t.Fatalf("SELECT after TTL failed: %v", err)
	}

	// Should have 0 hits (expired entry doesn't count as hit)
	hits, misses, size := catalog.GetQueryCacheStats()
	if hits != 0 {
		t.Errorf("Expected 0 hits after TTL expiration, got %d", hits)
	}

	t.Logf("TTL test: hits=%d, misses=%d, size=%d", hits, misses, size)
}
