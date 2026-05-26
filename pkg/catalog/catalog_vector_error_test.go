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

func TestUpdateVectorIndexesForInsertReturnsMissingColumnError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "vec_stale_insert",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c.vectorIndexes["idx_vec_stale_insert"] = &VectorIndexDef{
		Name:       "idx_vec_stale_insert",
		TableName:  "vec_stale_insert",
		ColumnName: "missing_embedding",
		Dimensions: 3,
		IndexType:  "hnsw",
		HNSW:       NewHNSWIndex("idx_vec_stale_insert", "vec_stale_insert", "missing_embedding", 3),
	}

	err := c.updateVectorIndexesForInsert("vec_stale_insert", []interface{}{int64(1), []interface{}{1.0, 0.0, 0.0}}, "00000000000000000001")
	if err == nil || !strings.Contains(err.Error(), "missing_embedding") || !strings.Contains(err.Error(), "idx_vec_stale_insert") {
		t.Fatalf("expected stale vector insert index error, got %v", err)
	}
}

func TestUpdateVectorIndexesForUpdateReturnsMissingColumnError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "vec_stale_update",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c.vectorIndexes["idx_vec_stale_update"] = &VectorIndexDef{
		Name:       "idx_vec_stale_update",
		TableName:  "vec_stale_update",
		ColumnName: "missing_embedding",
		Dimensions: 3,
		IndexType:  "hnsw",
		HNSW:       NewHNSWIndex("idx_vec_stale_update", "vec_stale_update", "missing_embedding", 3),
	}

	err := c.updateVectorIndexesForUpdate("vec_stale_update", []interface{}{int64(1), []interface{}{1.0, 0.0, 0.0}}, "00000000000000000001")
	if err == nil || !strings.Contains(err.Error(), "missing_embedding") || !strings.Contains(err.Error(), "idx_vec_stale_update") {
		t.Fatalf("expected stale vector update index error, got %v", err)
	}
}
