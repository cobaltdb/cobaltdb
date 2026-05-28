package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestReplicationSnapshotRoundTripReloadsCatalog(t *testing.T) {
	ctx := context.Background()

	master, err := Open(":memory:", &Options{InMemory: true, CacheSize: 256})
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

	snapshot, err := master.createReplicationSnapshot()
	if err != nil {
		t.Fatalf("createReplicationSnapshot: %v", err)
	}
	if len(snapshot) == 0 {
		t.Fatal("expected non-empty snapshot")
	}

	slave, err := Open(":memory:", &Options{InMemory: true, CacheSize: 256})
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

func TestConfigureReplicationCallbacksWiresEngineSnapshots(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "master.db")

	db, err := Open(dbPath, &Options{
		CacheSize:             256,
		ReplicationRole:       "master",
		ReplicationListenAddr: "127.0.0.1:0",
		ReplicationStateFile:  filepath.Join(dir, "replication-state.json"),
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
