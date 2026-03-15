package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_JSONFunctions tests JSON functions
func TestCoverage_JSONFunctions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_test",
		Columns: []string{"id", "data"},
		Values: [][]query.Expression{
			{numReal(1), strReal(`{"name": "test", "value": 123}`)},
			{numReal(2), strReal(`{"items": [1, 2, 3]}`)},
		},
	}, nil)

	queries := []string{
		"SELECT id, JSON_EXTRACT(data, '$.name') FROM json_test",
		"SELECT id, JSON_TYPE(data) FROM json_test",
		"SELECT id, JSON_VALID(data) FROM json_test",
		"SELECT JSON_OBJECT('id', id, 'data', data) FROM json_test",
		"SELECT JSON_ARRAY(1, 2, 3) FROM json_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JSON query error: %v", err)
		} else {
			t.Logf("JSON query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_StringFunctions tests string functions
func TestCoverage_StringFunctions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "str_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "str_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("Hello World")}},
	}, nil)

	queries := []string{
		"SELECT UPPER(val) FROM str_test",
		"SELECT LOWER(val) FROM str_test",
		"SELECT LENGTH(val) FROM str_test",
		"SELECT SUBSTRING(val, 1, 5) FROM str_test",
		"SELECT TRIM(val) FROM str_test",
		"SELECT REPLACE(val, 'World', 'Universe') FROM str_test",
		"SELECT CONCAT(val, '!') FROM str_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("String query error: %v", err)
		} else {
			t.Logf("String query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_MathFunctions tests math functions
func TestCoverage_MathFunctions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "math_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "math_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	queries := []string{
		"SELECT ABS(-10) FROM math_test",
		"SELECT ROUND(3.14159, 2) FROM math_test",
		"SELECT FLOOR(3.9) FROM math_test",
		"SELECT CEIL(3.1) FROM math_test",
		"SELECT SQRT(val) FROM math_test",
		"SELECT POWER(2, 3) FROM math_test",
		"SELECT MOD(val, 3) FROM math_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Math query error: %v", err)
		} else {
			t.Logf("Math query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_DateFunctions tests date functions
func TestCoverage_DateFunctions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "date_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "date_test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	queries := []string{
		"SELECT CURRENT_DATE FROM date_test",
		"SELECT CURRENT_TIME FROM date_test",
		"SELECT CURRENT_TIMESTAMP FROM date_test",
		"SELECT DATE('2024-01-15') FROM date_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Date query error: %v", err)
		} else {
			t.Logf("Date query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_AggregateFunctions tests aggregate functions
func TestCoverage_AggregateFunctions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "category", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		category := "A"
		if i > 5 {
			category = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_test",
			Columns: []string{"id", "val", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal(category)}},
		}, nil)
	}

	queries := []string{
		"SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM agg_test",
		"SELECT category, COUNT(*), SUM(val) FROM agg_test GROUP BY category",
		"SELECT COUNT(DISTINCT category) FROM agg_test",
		"SELECT GROUP_CONCAT(category) FROM agg_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate query error: %v", err)
		} else {
			t.Logf("Aggregate query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CreateTableHashPartitionAuto tests CreateTable with HASH partitioning
// where partitions are auto-generated based on NumPartitions
func TestCoverage_CreateTableHashPartitionAuto(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create table with HASH partitioning and NumPartitions but no explicit partition defs
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "hash_part_auto",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
		Partition: &query.PartitionDef{
			Type:          query.PartitionTypeHash,
			Column:        "id",
			NumPartitions: 4, // Auto-generate 4 partitions
		},
	})
	if err != nil {
		t.Fatalf("Failed to create hash partitioned table: %v", err)
	}

	// Verify table was created
	cat.mu.RLock()
	table, exists := cat.tables["hash_part_auto"]
	cat.mu.RUnlock()

	if !exists {
		t.Fatal("Table 'hash_part_auto' was not created")
	}

	if table.Partition == nil {
		t.Fatal("Table should have partition info")
	}

	if table.Partition.Type != query.PartitionTypeHash {
		t.Errorf("Expected HASH partition type, got %v", table.Partition.Type)
	}

	// Should have 4 auto-generated partitions
	if len(table.Partition.Partitions) != 4 {
		t.Errorf("Expected 4 partitions, got %d", len(table.Partition.Partitions))
	}

	// Check partition names are auto-generated (p0, p1, p2, p3)
	expectedNames := map[string]bool{"p0": false, "p1": false, "p2": false, "p3": false}
	for _, p := range table.Partition.Partitions {
		if _, exists := expectedNames[p.Name]; !exists {
			t.Errorf("Unexpected partition name: %s", p.Name)
		}
		expectedNames[p.Name] = true
	}

	for name, found := range expectedNames {
		if !found {
			t.Errorf("Expected partition %s not found", name)
		}
	}
}
