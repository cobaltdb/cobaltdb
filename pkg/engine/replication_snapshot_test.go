package engine

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

type oversizedSnapshotBackend struct{}

func (oversizedSnapshotBackend) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (oversizedSnapshotBackend) WriteAt(buf []byte, offset int64) (int, error) {
	return len(buf), nil
}
func (oversizedSnapshotBackend) Sync() error          { return nil }
func (oversizedSnapshotBackend) Size() int64          { return maxEngineReplicationSnapshotSize + 1 }
func (oversizedSnapshotBackend) Truncate(int64) error { return nil }
func (oversizedSnapshotBackend) Close() error         { return nil }

type shortWriteEngineBackend struct {
	storage.Backend
	limit int
}

func (b *shortWriteEngineBackend) WriteAt(buf []byte, offset int64) (int, error) {
	if b.limit >= 0 && len(buf) > b.limit {
		return b.Backend.WriteAt(buf[:b.limit], offset)
	}
	return b.Backend.WriteAt(buf, offset)
}

type shortReadEngineBackend struct {
	storage.Backend
	limit int
}

func (b *shortReadEngineBackend) ReadAt(buf []byte, offset int64) (int, error) {
	if b.limit >= 0 && len(buf) > b.limit {
		return b.Backend.ReadAt(buf[:b.limit], offset)
	}
	return b.Backend.ReadAt(buf, offset)
}

func TestReplicationSnapshotRoundTripReloadsCatalog(t *testing.T) {
	ctx := context.Background()

	master, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open master: %v", err)
	}
	defer master.Close()

	if _, err := master.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := master.Exec(ctx, "INSERT INTO users VALUES (1, 'ada')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	snapshot, _, err := master.createReplicationSnapshot()
	if err != nil {
		t.Fatalf("createReplicationSnapshot: %v", err)
	}
	if len(snapshot) == 0 {
		t.Fatal("expected non-empty snapshot")
	}

	slave, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open slave: %v", err)
	}
	defer slave.Close()

	if err := slave.applyReplicationSnapshot(snapshot, 12); err != nil {
		t.Fatalf("applyReplicationSnapshot: %v", err)
	}

	var name string
	if err := slave.QueryRow(ctx, "SELECT name FROM users WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("query restored snapshot: %v", err)
	}
	if name != "ada" {
		t.Fatalf("expected restored name ada, got %q", name)
	}
}

func TestCreateReplicationSnapshotRejectsShortBackendRead(t *testing.T) {
	ctx := context.Background()

	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	db.backend = &shortReadEngineBackend{Backend: db.backend, limit: storage.PageSize - 1}

	_, _, err = db.createReplicationSnapshot()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("createReplicationSnapshot short read error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func TestApplyReplicationSnapshotRejectsShortBackendWrite(t *testing.T) {
	ctx := context.Background()

	master, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open master: %v", err)
	}
	defer master.Close()

	if _, err := master.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := master.Exec(ctx, "INSERT INTO users VALUES (1, 'ada')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	snapshot, lsn, err := master.createReplicationSnapshot()
	if err != nil {
		t.Fatalf("createReplicationSnapshot: %v", err)
	}
	// In-memory databases have no WAL, so LSN is 0. With a real WAL-backed
	// database, this would be non-zero after the checkpoint.
	if lsn != 0 {
		t.Logf("snapshot LSN=%d (non-zero indicates WAL-backed DB)", lsn)
	}

	slave, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open slave: %v", err)
	}
	defer slave.Close()
	slave.backend = &shortWriteEngineBackend{Backend: storage.NewMemory(), limit: storage.PageSize - 1}

	err = slave.applyReplicationSnapshot(snapshot, 12)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("applyReplicationSnapshot short write error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestApplyReplicationSnapshotRejectsInvalidPayloadBeforeDestructiveWrite(t *testing.T) {
	ctx := context.Background()

	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users VALUES (1, 'ada')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	err = db.applyReplicationSnapshot([]byte("bad snapshot"), 12)
	if err == nil || !strings.Contains(err.Error(), "invalid replication snapshot: too small") {
		t.Fatalf("expected invalid snapshot error, got %v", err)
	}

	var name string
	if err := db.QueryRow(ctx, "SELECT name FROM users WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("query existing row after rejected snapshot: %v", err)
	}
	if name != "ada" {
		t.Fatalf("expected existing name ada, got %q", name)
	}
}

func TestSaveMetaPageRejectsShortBackendRead(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.backend = &shortReadEngineBackend{Backend: db.backend, limit: storage.PageSize - 1}
	err = db.saveMetaPage()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("saveMetaPage short read error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func TestSaveMetaPageRejectsShortBackendWrite(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.backend = &shortWriteEngineBackend{Backend: db.backend, limit: storage.PageSize - 1}
	err = db.saveMetaPage()
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("saveMetaPage short write error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestCreateReplicationSnapshotRejectsOversizedBackend(t *testing.T) {
	ctx := context.Background()

	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	db.backend = oversizedSnapshotBackend{}
	_, _, err = db.createReplicationSnapshot()
	if err == nil || !strings.Contains(err.Error(), "replication snapshot too large") {
		t.Fatalf("expected oversized snapshot error, got %v", err)
	}
}

func TestConfigureReplicationCallbacksWiresEngineSnapshots(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "master.db")

	db, err := Open(dbPath, &Options{
		CoreStorage: CoreStorage{CacheSize: 256},
		Replication: ReplicationConfig{
			Role:       "master",
			ListenAddr: "127.0.0.1:0",
			StateFile:  filepath.Join(dir, "replication-state.json"),
		},
	})
	if err != nil {
		t.Fatalf("open replicated db: %v", err)
	}
	defer db.Close()

	mgr := db.GetReplicationManager()
	if mgr == nil {
		t.Fatal("expected replication manager")
	}
	if mgr.OnSnapshot == nil {
		t.Fatal("expected OnSnapshot callback")
	}
	if mgr.OnApplySnapshot == nil {
		t.Fatal("expected OnApplySnapshot callback")
	}
}

func TestAppendSnapshotCheckpointReturnsWALError(t *testing.T) {
	wal, err := storage.OpenWAL(filepath.Join(t.TempDir(), "snapshot.wal"))
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}

	db := &DB{wal: wal}
	err = db.appendSnapshotCheckpointLocked(12)
	if !errors.Is(err, storage.ErrWALClosed) {
		t.Fatalf("expected ErrWALClosed, got %v", err)
	}
}
