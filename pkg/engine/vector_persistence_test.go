package engine

import (
	"context"
	"path/filepath"
	"testing"
)

func TestVectorIndexPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vectors.db")
	opts := DefaultOptions()
	opts.EnableScheduler = false
	opts.EnableAutoCheckpoint = false
	opts.EnableAutoVacuum = false

	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	if _, err := db.Exec(ctx, `CREATE TABLE docs (
		id INTEGER PRIMARY KEY,
		name TEXT,
		embedding VECTOR(3)
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO docs (id, name, embedding) VALUES
		(1, 'x-axis', [1.0, 0.0, 0.0]),
		(2, 'y-axis', [0.0, 1.0, 0.0]),
		(3, 'z-axis', [0.0, 0.0, 1.0]),
		(4, 'diagonal', [0.7, 0.7, 0.0])
	`); err != nil {
		t.Fatalf("insert vectors: %v", err)
	}
	if _, err := db.Exec(ctx, `CREATE VECTOR INDEX idx_docs_embedding ON docs (embedding)`); err != nil {
		t.Fatalf("create vector index: %v", err)
	}

	beforeKey := requireVectorIndexReady(t, db, 4)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := Open(path, opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	afterKey := requireVectorIndexReady(t, reopened, 4)
	if afterKey != beforeKey {
		t.Fatalf("nearest vector key changed after reopen: before=%q after=%q", beforeKey, afterKey)
	}

	if err := reopened.catalog.DropVectorIndex("idx_docs_embedding"); err != nil {
		t.Fatalf("drop vector index: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close after drop: %v", err)
	}

	afterDrop, err := Open(path, opts)
	if err != nil {
		t.Fatalf("reopen after drop: %v", err)
	}
	defer afterDrop.Close()
	if _, err := afterDrop.catalog.GetVectorIndex("idx_docs_embedding"); err == nil {
		t.Fatal("vector index was resurrected after drop and reopen")
	}
}

func requireVectorIndexReady(t *testing.T, db *DB, wantNodes int) string {
	t.Helper()

	idx, err := db.catalog.GetVectorIndex("idx_docs_embedding")
	if err != nil {
		t.Fatalf("get vector index: %v", err)
	}
	if idx.HNSW == nil {
		t.Fatal("vector index has nil HNSW")
	}
	if len(idx.HNSW.Nodes) != wantNodes {
		t.Fatalf("vector index node count = %d, want %d", len(idx.HNSW.Nodes), wantNodes)
	}
	if idx.HNSW.EntryPoint == nil {
		t.Fatal("vector index entry point was not rebuilt")
	}

	keys, _, err := idx.HNSW.SearchKNN([]float64{1.0, 0.0, 0.0}, 1)
	if err != nil {
		t.Fatalf("vector search: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("vector search returned %d keys, want 1", len(keys))
	}
	return keys[0]
}
