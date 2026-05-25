package catalog

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestCreateIndexReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE idx_corrupt_row (id INTEGER PRIMARY KEY, email TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO idx_corrupt_row (id, email) VALUES (1, 'a@example.com')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["idx_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	_, err := c.ExecuteQuery("CREATE INDEX idx_corrupt_email ON idx_corrupt_row(email)")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "idx_corrupt_row") {
		t.Fatalf("expected corrupt row index error, got %v", err)
	}
	if _, exists := c.indexes["idx_corrupt_email"]; exists {
		t.Fatal("failed index should not remain registered")
	}
}

func TestCreateJSONIndexReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE json_idx_corrupt_row (id INTEGER PRIMARY KEY, data JSON)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery(`INSERT INTO json_idx_corrupt_row (id, data) VALUES (1, '{"name":"alice"}')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["json_idx_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	err := c.CreateJSONIndex("json_idx_corrupt_name", "json_idx_corrupt_row", "data", "$.name", "TEXT")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "json_idx_corrupt_row") {
		t.Fatalf("expected corrupt row JSON index error, got %v", err)
	}
	if _, exists := c.jsonIndexes["json_idx_corrupt_name"]; exists {
		t.Fatal("failed JSON index should not remain registered")
	}
}

func TestCreateJSONIndexIndexesVersionedRows(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE json_idx_versioned (id INTEGER PRIMARY KEY, data JSON)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery(`INSERT INTO json_idx_versioned (id, data) VALUES (1, '{"name":"alice","age":30}')`); err != nil {
		t.Fatalf("insert alice: %v", err)
	}
	if _, err := c.ExecuteQuery(`INSERT INTO json_idx_versioned (id, data) VALUES (2, '{"name":"bob","age":25}')`); err != nil {
		t.Fatalf("insert bob: %v", err)
	}

	if err := c.CreateJSONIndex("json_idx_versioned_name", "json_idx_versioned", "data", "$.name", "TEXT"); err != nil {
		t.Fatalf("create JSON index: %v", err)
	}
	rows, err := c.QueryJSONIndex("json_idx_versioned_name", "alice")
	if err != nil {
		t.Fatalf("query JSON index: %v", err)
	}
	if len(rows) != 1 || rows[0] != 0 {
		t.Fatalf("expected alice at row 0, got %v", rows)
	}
}

func TestCreateVectorIndexReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "vec_idx_corrupt_row",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["vec_idx_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	err := c.CreateVectorIndex("vec_idx_corrupt_embedding", "vec_idx_corrupt_row", "embedding")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "vec_idx_corrupt_embedding") {
		t.Fatalf("expected corrupt row vector index error, got %v", err)
	}
	if _, exists := c.vectorIndexes["vec_idx_corrupt_embedding"]; exists {
		t.Fatal("failed vector index should not remain registered")
	}
}

func TestCreateFTSIndexReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE fts_idx_corrupt_row (id INTEGER PRIMARY KEY, body TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["fts_idx_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	err := c.CreateFTSIndex("fts_idx_corrupt_body", "fts_idx_corrupt_row", []string{"body"})
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "fts_idx_corrupt_body") {
		t.Fatalf("expected corrupt row FTS index error, got %v", err)
	}
	if _, exists := c.ftsIndexes["fts_idx_corrupt_body"]; exists {
		t.Fatal("failed FTS index should not remain registered")
	}
}
