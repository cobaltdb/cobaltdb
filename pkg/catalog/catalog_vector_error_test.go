package catalog

import (
	"errors"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

type scanFailTree struct {
	btree.TreeStore
	err error
}

func (t *scanFailTree) Scan(startKey, endKey []byte) (btree.TreeIterator, error) {
	return nil, t.err
}

func TestCreateVectorIndexReturnsScanFailure(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "vec_scan_fail",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	c.tableTrees["vec_scan_fail"] = &scanFailTree{
		TreeStore: c.tableTrees["vec_scan_fail"],
		err:       errors.New("scan failed"),
	}

	err := c.CreateVectorIndex("idx_vec_scan_fail", "vec_scan_fail", "embedding")
	if err == nil || !strings.Contains(err.Error(), "scan failed") {
		t.Fatalf("expected vector index scan failure, got %v", err)
	}
	if _, ok := c.vectorIndexes["idx_vec_scan_fail"]; ok {
		t.Fatal("vector index should not be registered after scan failure")
	}
}
