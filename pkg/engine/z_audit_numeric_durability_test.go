package engine

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func numScalar(t *testing.T, db *DB, query string) interface{} {
	t.Helper()
	row := db.QueryRow(context.Background(), query)
	var got interface{}
	if err := row.Scan(&got); err != nil {
		t.Fatalf("scan %q: %v", query, err)
	}
	return got
}

// TestFailedCommitIsAtomic is a regression test for an atomicity bug: when an
// autocommit statement's commit fails (here triggered by exceeding the WAL
// record size limit), the failed statement's buffered write must NOT remain
// visible to subsequent statements on the same connection, and must not persist.
// Previously CommitTransaction returned the error before clearing the
// goroutine-local txn state, so the phantom row stayed live (COUNT reflected
// data that never persisted) inside a leaked implicit transaction.
func TestFailedCommitIsAtomic(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "atomic.db")
	ctx := context.Background()
	db, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'small')")

	// A value large enough that the JSON-encoded WAL record exceeds the uint16
	// payload limit; the commit must fail cleanly.
	big := strings.Repeat("Q", 100000)
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (2, ?)", big); err == nil {
		t.Fatal("expected oversized insert to fail at commit")
	}

	// The failed insert must not be visible on this connection.
	if got := numScalar(t, db, "SELECT COUNT(*) FROM t"); got != int64(1) {
		t.Fatalf("after failed commit COUNT = %v, want 1 (phantom uncommitted row leaked)", got)
	}
	// The connection must not be stuck in a leaked implicit transaction.
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (3, 'ok')"); err != nil {
		t.Fatalf("insert after failed commit failed (leaked txn): %v", err)
	}
	if got := numScalar(t, db, "SELECT v FROM t WHERE id = 1"); got != "small" {
		t.Fatalf("row 1 corrupted: %v", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Persisted state must match: rows 1 and 3 only.
	db2, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if got := numScalar(t, db2, "SELECT COUNT(*) FROM t"); got != int64(2) {
		t.Fatalf("after reopen COUNT = %v, want 2", got)
	}
}

// TestInt64BoundaryLiteralPrecision is a regression test for silent precision
// loss of boundary integer literals. MinInt64 (-9223372036854775808) has a
// magnitude that overflows a positive int64, so the parser previously stored it
// as an approximate float64. It must round-trip exactly as int64, including
// across a disk reopen.
func TestInt64BoundaryLiteralPrecision(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "int.db")
	ctx := context.Background()
	db, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	vals := []int64{math.MaxInt64, math.MinInt64, math.MaxInt64 - 1, 9007199254740993, -9007199254740993}
	for i, v := range vals {
		if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, v)); err != nil {
			t.Fatalf("insert %d: %v", v, err)
		}
	}
	verify := func(d *DB, tag string) {
		for i, v := range vals {
			got := numScalar(t, d, fmt.Sprintf("SELECT v FROM t WHERE id = %d", i))
			gi, ok := got.(int64)
			if !ok {
				t.Errorf("[%s] v=%d decoded as %T (%v), want int64", tag, v, got, got)
				continue
			}
			if gi != v {
				t.Errorf("[%s] int64 roundtrip: got %d, want %d", tag, gi, v)
			}
		}
	}
	verify(db, "before")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	verify(db2, "reopen")
}

// TestFloatBoundaryRoundTrip verifies exact float64 round-trip (including tiny
// and huge magnitudes) through insert, select, and a disk reopen.
func TestFloatBoundaryRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "flt.db")
	ctx := context.Background()
	db, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v REAL)")
	vals := []float64{0.1, 0.2, 0.3, math.SmallestNonzeroFloat64, math.MaxFloat64, 1e-300, 1e308, math.Pi, 123456789.123456789}
	for i, v := range vals {
		if _, err := db.Exec(ctx, "INSERT INTO t VALUES (?, ?)", i, v); err != nil {
			t.Fatalf("insert %v: %v", v, err)
		}
	}
	verify := func(d *DB, tag string) {
		for i, v := range vals {
			got := numScalar(t, d, fmt.Sprintf("SELECT v FROM t WHERE id = %d", i))
			gf, ok := got.(float64)
			if !ok {
				if gi, ok2 := got.(int64); ok2 {
					gf, ok = float64(gi), true
				}
			}
			if !ok {
				t.Errorf("[%s] v=%v decoded as %T", tag, v, got)
				continue
			}
			if gf != v {
				t.Errorf("[%s] float roundtrip: got %x, want %x", tag, math.Float64bits(gf), math.Float64bits(v))
			}
		}
	}
	verify(db, "before")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	verify(db2, "reopen")
}

// TestLargeTextRoundTrip verifies TEXT values up to (but under) the WAL record
// size limit round-trip byte-for-byte through a disk reopen.
func TestLargeTextRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "txt.db")
	ctx := context.Background()
	db, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	// Stay below the ~64KB uint16 WAL payload limit (JSON overhead included).
	sizes := []int{1000, 4096, 8192, 16384, 32768, 60000}
	texts := map[int]string{}
	for i, sz := range sizes {
		s := strings.Repeat("abcXYZ123", sz/9+1)[:sz]
		texts[i] = s
		if _, err := db.Exec(ctx, "INSERT INTO t VALUES (?, ?)", i, s); err != nil {
			t.Fatalf("insert size %d: %v", sz, err)
		}
	}
	verify := func(d *DB, tag string) {
		for i, sz := range sizes {
			got := numScalar(t, d, fmt.Sprintf("SELECT v FROM t WHERE id = %d", i))
			gs, ok := got.(string)
			if !ok {
				if b, ok2 := got.([]byte); ok2 {
					gs, ok = string(b), true
				}
			}
			if !ok {
				t.Errorf("[%s] size %d decoded as %T", tag, sz, got)
				continue
			}
			if gs != texts[i] {
				t.Errorf("[%s] size %d text corrupted: got len %d, want %d", tag, sz, len(gs), sz)
			}
		}
	}
	verify(db, "before")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	verify(db2, "reopen")
}

// TestCrashRecoverySyncModes verifies that committed rows recovered after an
// unclean process exit are never corrupted under SyncNormal and SyncOff.
func TestCrashRecoverySyncModes(t *testing.T) {
	if os.Getenv("COBALTDB_SYNC_CRASH_HELPER") == "1" {
		runSyncModeCrashWriter(t)
		os.Exit(0)
	}
	opts := func(m SyncMode) *Options {
		return &Options{
			CoreStorage: CoreStorage{CacheSize: 128, WALEnabled: BoolPtr(true), SyncMode: m},
			Maintenance: MaintenanceConfig{EnableAutoCheckpoint: false, EnableAutoVacuum: false},
			Scheduler:   SchedulerConfig{EnableScheduler: false},
		}
	}
	for _, mode := range []struct {
		name string
		m    SyncMode
	}{{"normal", SyncNormal}, {"off", SyncOff}} {
		t.Run(mode.name, func(t *testing.T) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "s.db")
			db, err := Open(dbPath, opts(mode.m))
			if err != nil {
				t.Fatal(err)
			}
			mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoverySyncModes")
			cmd.Env = append(os.Environ(),
				"COBALTDB_SYNC_CRASH_HELPER=1",
				"COBALTDB_SYNC_CRASH_DB="+dbPath,
				"COBALTDB_SYNC_CRASH_MODE="+mode.name,
			)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("crash helper failed: %v\n%s", err, out)
			}
			rec, err := Open(dbPath, opts(mode.m))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer rec.Close()
			// Whatever survived must be internally consistent (v == id*10).
			rows := queryRows(t, rec, "SELECT id, v FROM t ORDER BY id")
			for _, r := range rows {
				id, _ := r[0].(int64)
				v, _ := r[1].(int64)
				if v != id*10 {
					t.Errorf("[%s] corrupted recovered row id=%d v=%d, want %d", mode.name, id, v, id*10)
				}
			}
		})
	}
}

func runSyncModeCrashWriter(t *testing.T) {
	t.Helper()
	dbPath := os.Getenv("COBALTDB_SYNC_CRASH_DB")
	var m SyncMode = SyncNormal
	if os.Getenv("COBALTDB_SYNC_CRASH_MODE") == "off" {
		m = SyncOff
	}
	db, err := Open(dbPath, &Options{
		CoreStorage: CoreStorage{CacheSize: 128, WALEnabled: BoolPtr(true), SyncMode: m},
		Maintenance: MaintenanceConfig{EnableAutoCheckpoint: false, EnableAutoVacuum: false},
		Scheduler:   SchedulerConfig{EnableScheduler: false},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 1; i <= 20; i++ {
		if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10)); err != nil {
			t.Fatal(err)
		}
	}
	// crash: no Close/Checkpoint
}

// TestEncryptionWrongKeyRejected verifies encrypted data cannot be read with a
// wrong key and round-trips with the correct key across a reopen.
func TestEncryptionWrongKeyRejected(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enc.db")
	ctx := context.Background()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	opts := func(k []byte) *Options {
		return &Options{
			CoreStorage: CoreStorage{CacheSize: 128, WALEnabled: BoolPtr(true), SyncMode: SyncFull},
			Security:    Security{EncryptionKey: k},
			Maintenance: MaintenanceConfig{EnableAutoCheckpoint: false, EnableAutoVacuum: false},
			Scheduler:   SchedulerConfig{EnableScheduler: false},
		}
	}
	db, err := Open(dbPath, opts(key))
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'secret-data')")
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dbPath, opts(key))
	if err != nil {
		t.Fatalf("reopen with correct key: %v", err)
	}
	if got := numScalar(t, db2, "SELECT v FROM t WHERE id = 1"); got != "secret-data" {
		t.Errorf("correct key data = %v, want secret-data", got)
	}
	db2.Close()

	wrong := make([]byte, 32)
	for i := range wrong {
		wrong[i] = 0xAA
	}
	db3, err := Open(dbPath, opts(wrong))
	if err != nil {
		// Acceptable: authentication failure on open.
		return
	}
	defer db3.Close()
	row := db3.QueryRow(ctx, "SELECT v FROM t WHERE id = 1")
	var v interface{}
	if err := row.Scan(&v); err == nil {
		if s, _ := v.(string); s == "secret-data" {
			t.Errorf("SECURITY: wrong key returned plaintext %q", s)
		}
	}
}
