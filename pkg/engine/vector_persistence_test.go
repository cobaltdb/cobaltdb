package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func TestVectorIndexPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vectors.db")
	opts := DefaultOptions()
	opts.Scheduler.EnableScheduler = false
	opts.Maintenance.EnableAutoCheckpoint = false
	opts.Maintenance.EnableAutoVacuum = false

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

func TestVectorIndexLargeRebuildAndBackupRestore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "vectors-large.db")
	opts := DefaultOptions()
	opts.Backup.Dir = filepath.Join(dir, "backups")
	opts.Backup.CompressionLevel = 0
	opts.Scheduler.EnableScheduler = false
	opts.Maintenance.EnableAutoCheckpoint = false
	opts.Maintenance.EnableAutoVacuum = false

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
	for i := 1; i <= 512; i++ {
		sql := fmt.Sprintf(
			"INSERT INTO docs (id, name, embedding) VALUES (%d, 'doc-%03d', [0.0, %.1f, %.1f])",
			i, i, float64(i), float64(i%17),
		)
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("insert vector %d: %v", i, err)
		}
	}
	if _, err := db.Exec(ctx, `CREATE VECTOR INDEX idx_docs_embedding ON docs (embedding)`); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	if _, err := db.Exec(ctx, `UPDATE docs SET embedding = [1.0, 0.0, 0.0] WHERE id = 128`); err != nil {
		t.Fatalf("update vector: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM docs WHERE id = 255`); err != nil {
		t.Fatalf("delete vector: %v", err)
	}

	wantKey := fmt.Sprintf("%020d", 128)
	requireVectorIndexSearchHit(t, db, "idx_docs_embedding", 511, []float64{1.0, 0.0, 0.0}, wantKey)

	full, err := db.CreateBackup(ctx, "full")
	if err != nil {
		t.Fatalf("create vector backup: %v", err)
	}
	restorePath := filepath.Join(dir, "restore", "vectors-restored.db")
	if err := db.GetBackupManager().Restore(ctx, full.ID, restorePath); err != nil {
		t.Fatalf("restore vector backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	reopened, err := Open(path, opts)
	if err != nil {
		t.Fatalf("reopen source: %v", err)
	}
	requireVectorIndexSearchHit(t, reopened, "idx_docs_embedding", 511, []float64{1.0, 0.0, 0.0}, wantKey)
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened source: %v", err)
	}

	restored, err := Open(restorePath, opts)
	if err != nil {
		t.Fatalf("open restored vector db: %v", err)
	}
	defer restored.Close()
	requireVectorIndexSearchHit(t, restored, "idx_docs_embedding", 511, []float64{1.0, 0.0, 0.0}, wantKey)
	assertScalar(t, restored, "SELECT COUNT(*) FROM docs", int64(511))
	assertScalar(t, restored, "SELECT name FROM docs WHERE id = 128", "doc-128")
}

func TestVectorIndexThousandPlusMixedDMLReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "vectors-mixed.db")
	opts := DefaultOptions()
	opts.Scheduler.EnableScheduler = false
	opts.Maintenance.EnableAutoCheckpoint = false
	opts.Maintenance.EnableAutoVacuum = false

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
	for i := 1; i <= 1024; i++ {
		sql := fmt.Sprintf(
			"INSERT INTO docs (id, name, embedding) VALUES (%d, 'doc-%04d', [%.1f, %.1f, %.1f])",
			i, i, float64(i%31), float64(i%17), float64(i%7),
		)
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("insert vector %d: %v", i, err)
		}
	}
	if _, err := db.Exec(ctx, `CREATE VECTOR INDEX idx_docs_embedding ON docs (embedding)`); err != nil {
		t.Fatalf("create vector index: %v", err)
	}

	for i := 25; i <= 500; i += 25 {
		sql := fmt.Sprintf("UPDATE docs SET embedding = [9.0, %.1f, 1.0] WHERE id = %d", float64(i/25), i)
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("update vector %d: %v", i, err)
		}
	}
	for _, id := range []int{17, 64, 128, 255, 511, 777, 999} {
		if _, err := db.Exec(ctx, fmt.Sprintf("DELETE FROM docs WHERE id = %d", id)); err != nil {
			t.Fatalf("delete vector %d: %v", id, err)
		}
	}

	wantNodes := 1024 - 7
	wantKey := fmt.Sprintf("%020d", 500)
	requireVectorIndexSearchHit(t, db, "idx_docs_embedding", wantNodes, []float64{9.0, 20.0, 1.0}, wantKey)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	for cycle := 1; cycle <= 2; cycle++ {
		reopened, err := Open(path, opts)
		if err != nil {
			t.Fatalf("reopen cycle %d: %v", cycle, err)
		}
		requireVectorIndexSearchHit(t, reopened, "idx_docs_embedding", wantNodes, []float64{9.0, 20.0, 1.0}, wantKey)
		assertScalar(t, reopened, "SELECT COUNT(*) FROM docs", int64(wantNodes))
		assertScalar(t, reopened, "SELECT name FROM docs WHERE id = 500", "doc-0500")
		if err := reopened.Close(); err != nil {
			t.Fatalf("close cycle %d: %v", cycle, err)
		}
	}
}

func requireVectorIndexReady(t *testing.T, db *DB, wantNodes int) string {
	t.Helper()
	return requireVectorIndexSearchHit(t, db, "idx_docs_embedding", wantNodes, []float64{1.0, 0.0, 0.0}, "")
}

func requireVectorIndexSearchHit(t *testing.T, db *DB, indexName string, wantNodes int, query []float64, wantKey string) string {
	t.Helper()

	idx, err := db.catalog.GetVectorIndex(indexName)
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

	if wantKey != "" {
		node, ok := idx.HNSW.Nodes[wantKey]
		if !ok {
			t.Fatalf("vector index does not contain key %q", wantKey)
		}
		if len(node.Vector) != len(query) {
			t.Fatalf("vector key %q dimensions = %d, want %d", wantKey, len(node.Vector), len(query))
		}
		for i := range query {
			if node.Vector[i] != query[i] {
				t.Fatalf("vector key %q value[%d] = %v, want %v", wantKey, i, node.Vector[i], query[i])
			}
		}
	}

	k := 1
	keys, _, err := idx.HNSW.SearchKNN(query, k)
	if err != nil {
		t.Fatalf("vector search: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("vector search returned no keys")
	}
	return keys[0]
}
