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
