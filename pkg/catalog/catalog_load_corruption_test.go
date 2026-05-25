package catalog

import (
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestLoadReturnsCorruptTableMetadataError(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	defer pool.Close()

	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	if err := catalogTree.Put([]byte("tbl:corrupt_users"), []byte("not valid json")); err != nil {
		t.Fatalf("Put corrupt metadata: %v", err)
	}

	c := New(catalogTree, pool, nil)
	err = c.Load()
	if err == nil || !strings.Contains(err.Error(), "corrupt_users") {
		t.Fatalf("expected corrupt table metadata error, got %v", err)
	}
	if _, exists := c.tables["corrupt_users"]; exists {
		t.Fatal("corrupt table should not be loaded after Load failure")
	}
}

func TestLoadReturnsCorruptIndexMetadataError(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	defer pool.Close()

	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	if err := catalogTree.Put([]byte("idx:corrupt_email_idx"), []byte("not valid json")); err != nil {
		t.Fatalf("Put corrupt index metadata: %v", err)
	}

	c := New(catalogTree, pool, nil)
	err = c.Load()
	if err == nil || !strings.Contains(err.Error(), "corrupt_email_idx") {
		t.Fatalf("expected corrupt index metadata error, got %v", err)
	}
	if _, exists := c.indexes["corrupt_email_idx"]; exists {
		t.Fatal("corrupt index should not be loaded after Load failure")
	}
}

func TestLoadReturnsCorruptForeignTableMetadataError(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	defer pool.Close()

	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	if err := catalogTree.Put([]byte("ft:remote_orders"), []byte("not valid json")); err != nil {
		t.Fatalf("Put corrupt foreign table metadata: %v", err)
	}

	c := New(catalogTree, pool, nil)
	err = c.Load()
	if err == nil || !strings.Contains(err.Error(), "remote_orders") {
		t.Fatalf("expected corrupt foreign table metadata error, got %v", err)
	}
	if _, exists := c.foreignTables["remote_orders"]; exists {
		t.Fatal("corrupt foreign table should not be loaded after Load failure")
	}
}

func TestLoadReturnsCorruptSQLObjectMetadataError(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "view", key: "view:broken_view", want: "broken_view"},
		{name: "trigger", key: "trg:broken_trigger", want: "broken_trigger"},
		{name: "procedure", key: "proc:broken_procedure", want: "broken_procedure"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := storage.NewBufferPool(1024, storage.NewMemory())
			defer pool.Close()

			catalogTree, err := btree.NewBTree(pool)
			if err != nil {
				t.Fatalf("NewBTree: %v", err)
			}
			if err := catalogTree.Put([]byte(tt.key), []byte("not valid json")); err != nil {
				t.Fatalf("Put corrupt SQL object metadata: %v", err)
			}

			c := New(catalogTree, pool, nil)
			err = c.Load()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected corrupt SQL object metadata error containing %q, got %v", tt.want, err)
			}
		})
	}
}

func TestLoadReturnsCorruptMaterializedViewMetadataError(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	defer pool.Close()

	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	if err := catalogTree.Put([]byte("mv:broken_mv"), []byte("not valid json")); err != nil {
		t.Fatalf("Put corrupt materialized view metadata: %v", err)
	}

	c := New(catalogTree, pool, nil)
	err = c.Load()
	if err == nil || !strings.Contains(err.Error(), "broken_mv") {
		t.Fatalf("expected corrupt materialized view metadata error, got %v", err)
	}
	if _, exists := c.materializedViews["broken_mv"]; exists {
		t.Fatal("corrupt materialized view should not be loaded after Load failure")
	}
}

func TestLoadReturnsCorruptVectorIndexMetadataError(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	defer pool.Close()

	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	if err := catalogTree.Put([]byte("vec:broken_vec_idx"), []byte("not valid json")); err != nil {
		t.Fatalf("Put corrupt vector index metadata: %v", err)
	}

	c := New(catalogTree, pool, nil)
	err = c.Load()
	if err == nil || !strings.Contains(err.Error(), "broken_vec_idx") {
		t.Fatalf("expected corrupt vector index metadata error, got %v", err)
	}
	if _, exists := c.vectorIndexes["broken_vec_idx"]; exists {
		t.Fatal("corrupt vector index should not be loaded after Load failure")
	}
}
