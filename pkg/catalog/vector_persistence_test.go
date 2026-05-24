package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestVectorIndexMetadataPersistsOnCreateAndDrop(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(64, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("new catalog tree: %v", err)
	}

	c := New(tree, pool, nil)
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "docs",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if err := c.CreateVectorIndex("idx_docs_embedding", "docs", "embedding"); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	assertVectorMetadataPresent(t, tree, true)

	if err := c.DropVectorIndex("idx_docs_embedding"); err != nil {
		t.Fatalf("drop vector index: %v", err)
	}
	assertVectorMetadataPresent(t, tree, false)
}

func assertVectorMetadataPresent(t *testing.T, tree btree.TreeStore, want bool) {
	t.Helper()
	iter, err := tree.Scan([]byte("vec:"), []byte("vec;"))
	if err != nil {
		t.Fatalf("scan vector metadata: %v", err)
	}
	defer iter.Close()

	got := iter.HasNext()
	if got != want {
		t.Fatalf("vector metadata presence = %v, want %v", got, want)
	}
}
