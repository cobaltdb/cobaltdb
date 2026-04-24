package integration

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestPageCompressionRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "compressed.db")

	ctx := context.Background()
	opts := &engine.Options{
		InMemory: false,
		CompressionConfig: &storage.CompressionConfig{
			Enabled:  true,
			Level:    storage.CompressionLevelFast,
			MinRatio: 1.0,
		},
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE comp_test (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert compressible data
	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ctx, `INSERT INTO comp_test VALUES (?, ?)`, i, "repeated text data for compression")
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Query back
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM comp_test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 100 {
			t.Fatalf("Expected 100 rows, got %d", count)
		}
	}
}

func TestPageCompressionDisabled(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "uncompressed.db")

	ctx := context.Background()
	opts := &engine.Options{
		InMemory: false,
		CompressionConfig: &storage.CompressionConfig{
			Enabled: false,
		},
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE no_comp (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO no_comp VALUES (1), (2), (3)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM no_comp`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 3 {
			t.Fatalf("Expected 3 rows, got %d", count)
		}
	}
}
