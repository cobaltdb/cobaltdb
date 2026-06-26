package engine

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

// TestBlobRoundTripThroughEngine verifies a BLOB value with embedded NUL/high/
// quote bytes round-trips byte-for-byte through INSERT/SELECT, survives a
// concurrent DELETE of another row (the soft-delete re-encode path), and
// persists across a disk reopen.
func TestBlobRoundTripThroughEngine(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "blob.db")
	ctx := context.Background()

	blob := []byte{0x00, 0xFF, 0x41, '"', '\\', 0x80, 0x7f, 0x00}
	other := []byte{1, 2, 3}

	db, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE b (id INTEGER PRIMARY KEY, data BLOB)")
	if _, err := db.Exec(ctx, "INSERT INTO b (id, data) VALUES (?, ?)", 1, blob); err != nil {
		t.Fatalf("insert blob: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO b (id, data) VALUES (?, ?)", 2, other); err != nil {
		t.Fatalf("insert other: %v", err)
	}

	readBlob := func(d *DB, id int) []byte {
		var v interface{}
		if err := d.QueryRow(ctx, "SELECT data FROM b WHERE id = "+itoa(id)).Scan(&v); err != nil {
			t.Fatalf("scan id=%d: %v", id, err)
		}
		switch b := v.(type) {
		case []byte:
			return b
		case string:
			return []byte(b)
		default:
			t.Fatalf("id=%d decoded as %T, want []byte", id, v)
			return nil
		}
	}

	if got := readBlob(db, 1); !bytes.Equal(got, blob) {
		t.Fatalf("blob round-trip: got %v, want %v", got, blob)
	}

	// Soft-delete row 2 (re-encodes a binary row via the delete path).
	mustExec(t, db, "DELETE FROM b WHERE id = 2")
	if got := readBlob(db, 1); !bytes.Equal(got, blob) {
		t.Fatalf("blob corrupted after deleting another row: got %v, want %v", got, blob)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and verify persistence.
	db2, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if got := readBlob(db2, 1); !bytes.Equal(got, blob) {
		t.Fatalf("blob not persisted: got %v, want %v", got, blob)
	}
}
