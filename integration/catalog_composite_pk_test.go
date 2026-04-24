package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestCompositePrimaryKey verifies that a multi-column PRIMARY KEY enforces
// uniqueness over the tuple of values, not just the last PK column.
func TestCompositePrimaryKey(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE memberships (
		tenant_id INTEGER,
		user_id   INTEGER,
		role      TEXT,
		PRIMARY KEY (tenant_id, user_id)
	)`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Distinct tuples — all should succeed.
	inserts := [][]interface{}{
		{1, 100, "admin"},
		{1, 101, "member"},
		{2, 100, "owner"}, // same user_id as (1,100) but different tenant — OK
	}
	for i, row := range inserts {
		if _, err := db.Exec(ctx, `INSERT INTO memberships VALUES (?, ?, ?)`, row...); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Duplicate composite tuple (1,100) — must fail.
	if _, err := db.Exec(ctx, `INSERT INTO memberships VALUES (?, ?, ?)`, 1, 100, "other"); err == nil {
		t.Fatal("expected duplicate composite PK insert to fail")
	}

	// Verify row count: 3 unique rows survived.
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM memberships`)
	if err != nil {
		t.Fatalf("query count: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected COUNT row")
	}
	var count int
	if err := rows.Scan(&count); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
}

// TestCompositePrimaryKeyMixedTypes covers a composite PK with int + string
// components to exercise formatKeyComponent's type dispatch.
func TestCompositePrimaryKeyMixedTypes(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE tags (
		post_id INTEGER,
		tag     TEXT,
		PRIMARY KEY (post_id, tag)
	)`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := db.Exec(ctx, `INSERT INTO tags VALUES (1, 'go')`); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO tags VALUES (1, 'db')`); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO tags VALUES (2, 'go')`); err != nil {
		t.Fatalf("insert 3: %v", err)
	}
	// Duplicate (1,'go') — must fail.
	if _, err := db.Exec(ctx, `INSERT INTO tags VALUES (1, 'go')`); err == nil {
		t.Fatal("expected duplicate (1,'go') to fail")
	}
}
