package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogMore(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestGetPartitionTreeNames101 tests getPartitionTreeNames
func TestGetPartitionTreeNames101(t *testing.T) {
	// Non-partitioned table
	table := &TableDef{
		Name:      "test",
		Partition: nil,
	}
	names := table.getPartitionTreeNames()
	if len(names) != 1 || names[0] != "test" {
		t.Errorf("Expected ['test'], got %v", names)
	}

	// Partitioned table
	table2 := &TableDef{
		Name: "test2",
		Partition: &PartitionInfo{
			Partitions: []PartitionDef{
				{Name: "p1"},
				{Name: "p2"},
				{Name: "p3"},
				},
		},
	}
	names2 := table2.getPartitionTreeNames()
	if len(names2) != 3 {
		t.Fatalf("Expected 3 partition names, got %d", len(names2))
	}
	expected := []string{"test2:p1", "test2:p2", "test2:p3"}
	for i, exp := range expected {
		if names2[i] != exp {
			t.Errorf("Expected %s at index %d, got %s", exp, i, names2[i])
		}
	}
}

// TestGetTableTreesForScan101 tests getTableTreesForScan
func TestGetTableTreesForScan101(t *testing.T) {
	c := newTestCatalogMore(t)

	// Create a table first
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get table
	table := c.tables["test"]

	// Get trees for non-partitioned table
	trees, err := c.getTableTreesForScan(table)
	if err != nil {
		t.Fatalf("getTableTreesForScan failed: %v", err)
	}
	if len(trees) != 1 {
		t.Errorf("Expected 1 tree, got %d", len(trees))
	}

	// Test with non-existent table
	fakeTable := &TableDef{Name: "nonexistent"}
	_, err = c.getTableTreesForScan(fakeTable)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}
