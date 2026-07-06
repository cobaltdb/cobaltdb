package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func openMemDB(t *testing.T) *engine.DB {
	t.Helper()
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

// Regression: a dump of a TEXT value or binary BLOB containing a NUL byte must
// round-trip. Previously quoteSQLStringLiteral did not escape NUL, so the
// lexer's `for l.ch != 0` string scan terminated early and the restore failed
// with "unterminated string literal" — while the dump step reported success,
// silently producing an unrestorable backup.
func TestDumpRestoreNulByteRoundTrip(t *testing.T) {
	ctx := context.Background()

	t.Run("text_with_nul", func(t *testing.T) {
		src := openMemDB(t)
		defer src.Close()
		if _, err := src.Exec(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		val := "abc\x00def"
		if _, err := src.Exec(ctx, `INSERT INTO t VALUES (1, ?)`, val); err != nil {
			t.Fatalf("insert: %v", err)
		}
		dumpPath := filepath.Join(t.TempDir(), "d.sql")
		if err := dumpDatabase(src, dumpPath); err != nil {
			t.Fatalf("dump: %v", err)
		}
		dst := openMemDB(t)
		defer dst.Close()
		if err := restoreDatabase(dst, dumpPath); err != nil {
			t.Fatalf("restore: %v", err)
		}
		var got interface{}
		rows, err := dst.Query(ctx, `SELECT v FROM t WHERE id=1`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		for rows.Next() {
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("scan: %v", err)
			}
		}
		rows.Close()
		if fmt.Sprintf("%v", got) != val {
			t.Fatalf("TEXT-with-NUL mismatch: got %q want %q", fmt.Sprintf("%v", got), val)
		}
	})

	t.Run("blob_with_nul", func(t *testing.T) {
		src := openMemDB(t)
		defer src.Close()
		if _, err := src.Exec(ctx, `CREATE TABLE b (id INTEGER PRIMARY KEY, v BLOB)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		blob := []byte{0x00, 0x01, 0xff, 0xfe, 'A', 0x00, 0x80}
		if _, err := src.Exec(ctx, `INSERT INTO b VALUES (1, ?)`, blob); err != nil {
			t.Fatalf("insert: %v", err)
		}
		dumpPath := filepath.Join(t.TempDir(), "d.sql")
		if err := dumpDatabase(src, dumpPath); err != nil {
			t.Fatalf("dump: %v", err)
		}
		dst := openMemDB(t)
		defer dst.Close()
		if err := restoreDatabase(dst, dumpPath); err != nil {
			t.Fatalf("restore: %v", err)
		}
		var got interface{}
		rows, err := dst.Query(ctx, `SELECT v FROM b WHERE id=1`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		for rows.Next() {
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("scan: %v", err)
			}
		}
		rows.Close()
		gotBytes, ok := got.([]byte)
		if !ok {
			gotBytes = []byte(fmt.Sprintf("%v", got))
		}
		if string(gotBytes) != string(blob) {
			t.Fatalf("BLOB-with-NUL mismatch: got %v want %v", gotBytes, blob)
		}
	})
}

// Unit-level guard on the escaper itself.
func TestQuoteSQLStringLiteralEscapesNul(t *testing.T) {
	got := quoteSQLStringLiteral("a\x00b")
	want := `'a\0b'`
	if got != want {
		t.Fatalf("quoteSQLStringLiteral(NUL) = %q, want %q", got, want)
	}
}
