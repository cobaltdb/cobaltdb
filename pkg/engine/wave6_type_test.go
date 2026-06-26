package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func scalarString(t *testing.T, db *DB, sql string) interface{} {
	t.Helper()
	var v interface{}
	if err := db.QueryRow(context.Background(), sql).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return v
}

// TestIntegerArithmeticPrecision verifies integer +, -, *, % stay in the int64
// domain (no float64 precision loss above 2^53).
func TestIntegerArithmeticPrecision(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (a BIGINT, b BIGINT)")
	mustExec(t, db, "INSERT INTO t VALUES (1000000007, 1000000007)")

	var prod int64
	if err := db.QueryRow(ctx, "SELECT a * b FROM t").Scan(&prod); err != nil {
		t.Fatalf("scan product: %v", err)
	}
	if prod != 1000000014000000049 {
		t.Fatalf("a*b = %d, want 1000000014000000049 (float64 precision loss)", prod)
	}

	mustExec(t, db, "CREATE TABLE t2 (x BIGINT)")
	mustExec(t, db, "INSERT INTO t2 VALUES (9007199254740993)")
	var sum int64
	if err := db.QueryRow(ctx, "SELECT x + 0 FROM t2").Scan(&sum); err != nil {
		t.Fatalf("scan sum: %v", err)
	}
	if sum != 9007199254740993 {
		t.Fatalf("x+0 = %d, want 9007199254740993", sum)
	}
}

// TestSubstrNegativeStart verifies SUBSTR with a negative position counts from
// the end of the string (MySQL/SQLite semantics).
func TestSubstrNegativeStart(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	cases := map[string]string{
		"SELECT SUBSTR('foobarbar', -4)":    "rbar",
		"SELECT SUBSTR('Hello', -2)":        "lo",
		"SELECT SUBSTR('foobarbar', -4, 3)": "rba",
		"SELECT SUBSTR('Hello', 2)":         "ello", // positive unchanged
	}
	for sql, want := range cases {
		got := scalarString(t, db, sql)
		if gs, _ := got.(string); gs != want {
			t.Errorf("%s = %q, want %q", sql, got, want)
		}
	}
}

// TestConcatNullReturnsNull verifies CONCAT returns NULL if any argument is NULL
// (MySQL semantics).
func TestConcatNullReturnsNull(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	if v := scalarString(t, db, "SELECT CONCAT('a', NULL, 'b')"); v != nil {
		t.Fatalf("CONCAT('a', NULL, 'b') = %v, want NULL", v)
	}
	if v := scalarString(t, db, "SELECT CONCAT('a', 'b')"); v != "ab" {
		t.Fatalf("CONCAT('a','b') = %v, want 'ab'", v)
	}
}

// TestEncryptionWithCompressionRejected verifies that enabling encryption at
// rest together with page compression is rejected at open time (the two
// backends have incompatible I/O contracts and would silently corrupt
// compressible writes).
func TestEncryptionWithCompressionRejected(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/enc_comp.db"
	_, err := Open(path, &Options{
		Security:        Security{EncryptionKey: []byte("a-test-encryption-key-32-bytes!!")},
		PageCompression: PageCompressionConfig{Config: &storage.CompressionConfig{Enabled: true, MinRatio: 0.9}},
	})
	if err == nil {
		t.Fatal("expected Open to reject encryption + compression together")
	}
	if !strings.Contains(err.Error(), "compression") || !strings.Contains(err.Error(), "encryption") {
		t.Fatalf("error should mention both encryption and compression, got: %v", err)
	}
}
