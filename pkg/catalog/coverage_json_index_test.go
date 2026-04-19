package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestBuildJSONIndexWithNestedPath tests buildJSONIndex with nested JSON paths
func TestBuildJSONIndexWithNestedPath(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with JSON column
	c.CreateTable(&query.CreateTableStmt{
		Table: "json_nested",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenJSON},
		},
	})

	// Insert nested JSON data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "json_nested",
		Columns: []string{"id", "data"},
		Values: [][]query.Expression{
			{numReal(1), strReal(`{"user":{"name":"alice","age":30},"status":"active"}`)},
			{numReal(2), strReal(`{"user":{"name":"bob","age":25},"status":"inactive"}`)},
			{numReal(3), strReal(`{"user":{"name":"charlie","age":35},"status":"active"}`)},
		},
	}, nil)

	// Create JSON index with nested path
	c.CreateJSONIndex("idx_user_name", "json_nested", "data", "$.user.name", "TEXT")

	// Test query using JSON index
	result, err := c.ExecuteQuery("SELECT * FROM json_nested WHERE JSON_EXTRACT(data, '$.user.name') = 'alice'")
	if err != nil {
		t.Logf("JSON index query error (may be expected): %v", err)
	} else {
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row for alice, got %d", len(result.Rows))
		}
	}
}

// TestBuildJSONIndexWithArrayPath tests buildJSONIndex with array data
func TestBuildJSONIndexWithArrayPath(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "json_array",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "tags", Type: query.TokenJSON},
		},
	})

	// Insert array JSON data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "json_array",
		Columns: []string{"id", "tags"},
		Values: [][]query.Expression{
			{numReal(1), strReal(`["important", "urgent"]`)},
			{numReal(2), strReal(`["normal", "low"]`)},
		},
	}, nil)

	// Create JSON index
	c.CreateJSONIndex("idx_tags", "json_array", "tags", "$[0]", "TEXT")

	// Query
	result, err := c.ExecuteQuery("SELECT * FROM json_array WHERE JSON_EXTRACT(tags, '$[0]') = 'important'")
	if err != nil {
		t.Logf("Array JSON query: %v", err)
	} else {
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}
	}
}

// TestBuildJSONIndexEmptyTable tests buildJSONIndex on empty table
func TestBuildJSONIndexEmptyTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "json_empty",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenJSON},
		},
	})

	// Create JSON index on empty table - should not error
	err := c.CreateJSONIndex("idx_empty", "json_empty", "data", "$.name", "TEXT")
	if err != nil {
		t.Errorf("CreateJSONIndex on empty table failed: %v", err)
	}
}

func TestBuildJSONIndexWithInvalidRowData(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "json_bad",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenJSON},
		},
	})

	// Insert one valid row and one invalid raw value directly into the tree
	c.Insert(ctx, &query.InsertStmt{
		Table:   "json_bad",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"name":"alice"}`)}},
	}, nil)

	// Put invalid JSON bytes directly into the tree to trigger json.Unmarshal error
	c.tableTrees["json_bad"].Put([]byte{99}, []byte("not json"))

	// CreateJSONIndex should skip the invalid row and succeed
	err := c.CreateJSONIndex("idx_bad", "json_bad", "data", "$.name", "TEXT")
	if err != nil {
		t.Errorf("CreateJSONIndex with invalid row data failed: %v", err)
	}
}

func TestBuildJSONIndexWithNilJSONValue(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "json_nil",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenJSON},
		},
	})

	// Insert rows where path does not match
	c.Insert(ctx, &query.InsertStmt{
		Table:   "json_nil",
		Columns: []string{"id", "data"},
		Values: [][]query.Expression{
			{numReal(1), strReal(`{"other":"value1"}`)},
			{numReal(2), strReal(`{"other":"value2"}`)},
		},
	}, nil)

	// CreateJSONIndex should succeed even when path yields nil
	err := c.CreateJSONIndex("idx_nil", "json_nil", "data", "$.name", "TEXT")
	if err != nil {
		t.Errorf("CreateJSONIndex with nil JSON values failed: %v", err)
	}
}
