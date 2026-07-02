package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestHotBackupPointInTimeConsistentUnderWrites races sequential writes
// against a hot backup and verifies the restored copy (a) opens cleanly and
// (b) contains a consistent, gap-free prefix of the writes. Before the fix,
// the buffer pool's background flusher and eviction writes kept mutating the
// database file while the backup copied it, producing torn images.
func TestHotBackupPointInTimeConsistentUnderWrites(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "hotbackup.db")

	db, err := Open(dbPath, &Options{
		CoreStorage: CoreStorage{CacheSize: 64}, // small cache to force eviction pressure
		Backup:      BackupConfig{Dir: filepath.Join(dir, "backups")},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Seed some rows so the backup has real content.
	const seed = 50
	for i := 1; i <= seed; i++ {
		if _, err := db.Exec(ctx, "INSERT INTO items (id, payload) VALUES (?, ?)",
			i, fmt.Sprintf("payload-%04d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i)); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}

	// Writer goroutine: keeps inserting sequential ids while the backup runs.
	var nextID atomic.Int64
	nextID.Store(seed)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			id := nextID.Add(1)
			if _, err := db.Exec(ctx, "INSERT INTO items (id, payload) VALUES (?, ?)",
				id, fmt.Sprintf("payload-%04d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", id)); err != nil {
				// The database stays writable during backup; an error here is a bug.
				t.Errorf("racing insert %d: %v", id, err)
				return
			}
		}
	}()

	// Let the writer get going, then take the hot backup.
	time.Sleep(50 * time.Millisecond)
	bk, err := db.CreateBackup(ctx, "full")
	if err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("create backup: %v", err)
	}

	// Keep writing a bit longer, then stop.
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	if t.Failed() {
		return
	}

	// Post-backup sanity: checkpoints must be unblocked again after EndHotBackup.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after backup: %v", err)
	}

	// Restore to a fresh path and open it.
	restorePath := filepath.Join(dir, "restored.db")
	if err := db.GetBackupManager().Restore(ctx, bk.ID, restorePath); err != nil {
		t.Fatalf("restore: %v", err)
	}

	restored, err := Open(restorePath, nil)
	if err != nil {
		t.Fatalf("restored copy failed to open (backup not consistent): %v", err)
	}
	defer restored.Close()

	rows, err := restored.Query(ctx, "SELECT id FROM items ORDER BY id")
	if err != nil {
		t.Fatalf("query restored copy: %v", err)
	}
	ids := make(map[int64]bool)
	var maxID int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids[id] = true
		if id > maxID {
			maxID = id
		}
	}
	rows.Close()

	// The restored database must contain at least everything that was
	// durably committed before the backup began...
	if maxID < seed {
		t.Fatalf("restored copy lost seeded rows: max id %d < %d", maxID, seed)
	}
	// ...and must be a gap-free prefix of the sequential writes: every id in
	// [1, maxID] present exactly once. A torn copy shows up as holes.
	for i := int64(1); i <= maxID; i++ {
		if !ids[i] {
			t.Fatalf("restored copy is not a consistent prefix: id %d missing (max id %d)", i, maxID)
		}
	}

	// The live database must still contain all rows written during the race.
	row := db.QueryRow(ctx, "SELECT COUNT(*) FROM items")
	var count int64
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count live rows: %v", err)
	}
	if count != nextID.Load() {
		t.Fatalf("live database lost rows: count %d, expected %d", count, nextID.Load())
	}
}
