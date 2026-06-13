package engine

import (
	"fmt"
	"os"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/catalog"
	"github.com/cobaltdb/cobaltdb/pkg/fdw"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

const maxEngineReplicationSnapshotSize = 1 << 30 // 1 GiB

func (db *DB) configureReplicationCallbacks() {
	if db.replicationMgr == nil {
		return
	}

	db.replicationMgr.OnSnapshot = db.createReplicationSnapshot
	db.replicationMgr.OnApplySnapshot = db.applyReplicationSnapshot
}

func (db *DB) createReplicationSnapshot() ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return nil, ErrDatabaseClosed
	}
	if db.catalog == nil || db.backend == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	if err := validateReplicationSnapshotSize(db.backend.Size()); err != nil {
		return nil, err
	}

	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	if err := db.catalog.Save(); err != nil {
		return nil, fmt.Errorf("failed to save catalog: %w", err)
	}
	if err := db.saveMetaPage(); err != nil {
		return nil, fmt.Errorf("failed to save meta page: %w", err)
	}
	if db.wal != nil {
		if err := db.wal.Checkpoint(db.pool); err != nil {
			return nil, fmt.Errorf("failed to checkpoint snapshot: %w", err)
		}
	} else if err := db.backend.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync snapshot: %w", err)
	}

	size := db.backend.Size()
	if err := validateReplicationSnapshotSize(size); err != nil {
		return nil, err
	}

	data := make([]byte, size)
	for offset := int64(0); offset < size; {
		end := offset + storage.PageSize
		if end > size {
			end = size
		}

		n, err := storage.ReadFullAt(db.backend, data[offset:end], offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read snapshot: %w", err)
		}
		offset += int64(n)
	}

	return data, nil
}

func validateReplicationSnapshotSize(size int64) error {
	if size < 0 {
		return fmt.Errorf("invalid backend size: %d", size)
	}
	if size > maxEngineReplicationSnapshotSize {
		return fmt.Errorf("replication snapshot too large: %d bytes", size)
	}
	return nil
}

func validateReplicationSnapshotPayload(data []byte) error {
	if err := validateReplicationSnapshotSize(int64(len(data))); err != nil {
		return err
	}
	if len(data) < storage.PageSize {
		return fmt.Errorf("invalid replication snapshot: too small (%d bytes)", len(data))
	}

	var meta storage.MetaPage
	if err := meta.Deserialize(data[:storage.PageSize]); err != nil {
		return fmt.Errorf("failed to deserialize snapshot meta page: %w", err)
	}
	if err := meta.Validate(); err != nil {
		return fmt.Errorf("invalid snapshot database: %w", err)
	}
	return nil
}

func (db *DB) applyReplicationSnapshot(data []byte, lsn uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrDatabaseClosed
	}
	if db.backend == nil {
		return fmt.Errorf("backend not initialized")
	}
	if err := validateReplicationSnapshotPayload(data); err != nil {
		return err
	}

	if db.catalog.GetQueryCache() != nil {
		db.catalog.GetQueryCache().InvalidateAll()
	}
	if db.planCache != nil {
		db.planCache.Clear()
	}

	if err := db.resetWALForSnapshotLocked(); err != nil {
		return err
	}
	if err := db.backend.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate snapshot target: %w", err)
	}

	for offset := int64(0); offset < int64(len(data)); {
		end := offset + storage.PageSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		n, err := storage.WriteFullAt(db.backend, data[offset:end], offset)
		if err != nil {
			return fmt.Errorf("failed to write snapshot: %w", err)
		}
		offset += int64(n)
	}
	if err := db.backend.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot target: %w", err)
	}

	if err := db.reloadSnapshotStateLocked(); err != nil {
		return err
	}
	if err := db.appendSnapshotCheckpointLocked(lsn); err != nil {
		return err
	}

	return nil
}

func (db *DB) appendSnapshotCheckpointLocked(lsn uint64) error {
	if db.wal == nil {
		return nil
	}
	if err := db.wal.AppendWithoutSync(&storage.WALRecord{
		Type: storage.WALCheckpoint,
		LSN:  lsn,
	}); err != nil {
		return fmt.Errorf("failed to append snapshot WAL checkpoint: %w", err)
	}
	if err := db.wal.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot WAL checkpoint: %w", err)
	}
	return nil
}

func (db *DB) resetWALForSnapshotLocked() error {
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			return fmt.Errorf("failed to close WAL before snapshot apply: %w", err)
		}
		db.wal = nil
	}

	if db.options.CoreStorage.WALEnabled == nil || !*db.options.CoreStorage.WALEnabled || db.path == ":memory:" {
		return nil
	}

	walPath := db.path + ".wal"
	if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove stale WAL: %w", err)
	}

	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL after snapshot apply: %w", err)
	}
	if encBackend, ok := db.backend.(*storage.EncryptedBackend); ok {
		wal.SetEncryptionCipher(encBackend.GetCipher())
	}
	switch db.options.CoreStorage.SyncMode {
	case SyncNormal:
		wal.EnableGroupCommit(0, 5*time.Millisecond)
	case SyncOff:
		wal.EnableGroupCommit(0, 0)
	}
	db.wal = wal
	return nil
}

func (db *DB) reloadSnapshotStateLocked() error {
	metaPage := storage.NewPage(0, storage.PageTypeMeta)
	if _, err := storage.ReadFullAt(db.backend, metaPage.Data, 0); err != nil {
		return fmt.Errorf("failed to read snapshot meta page: %w", err)
	}

	var meta storage.MetaPage
	if err := meta.Deserialize(metaPage.Data); err != nil {
		return fmt.Errorf("failed to deserialize snapshot meta page: %w", err)
	}
	if err := meta.Validate(); err != nil {
		return fmt.Errorf("invalid snapshot database: %w", err)
	}

	// Re-open the buffer pool with the new root so btree operations use the
	// correct pages after the snapshot was written.
	db.pool = storage.NewBufferPool(db.options.CoreStorage.CacheSize, db.backend)
	if db.wal != nil {
		db.pool.SetWAL(db.wal)
	}

	rootTree, err := btree.OpenBTreeStrict(db.pool, meta.RootPageID)
	if err != nil {
		return fmt.Errorf("failed to open snapshot root B+Tree: %w", err)
	}
	db.rootTree = rootTree
	db.catalog = catalog.New(db.rootTree, db.pool, db.wal)
	db.catalog.SetParallelOptions(db.options.ParallelQuery.Workers, db.options.ParallelQuery.Threshold)

	fdwRegistry := fdw.NewRegistry()
	fdwRegistry.Register("csv", func() fdw.ForeignDataWrapper { return &fdw.CSVWrapper{} })
	db.catalog.SetFDWRegistry(fdwRegistry)
	if db.options.Security.EnableRLS {
		db.catalog.EnableRLS()
	}
	if err := db.catalog.Load(); err != nil {
		return fmt.Errorf("failed to load snapshot catalog: %w", err)
	}

	db.txnMgr = txn.NewManager(db.pool, db.wal)
	if db.options.QueryCache.EnableQueryCache {
		db.catalog.EnableQueryCacheWithLimits(db.options.QueryCache.QueryCacheSize, 0, db.options.QueryCache.QueryCacheTTL)
	}

	return nil
}
