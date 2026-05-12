package catalog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

func (c *Catalog) EnableRLS() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enableRLS = true
	c.rlsManager = security.NewManager()
}

func (c *Catalog) GetRLSManager() *security.Manager {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rlsManager
}

func (c *Catalog) IsRLSEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.enableRLS
}

func (c *Catalog) EnableQueryCache(maxSize int, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryCache = NewQueryCache(maxSize, ttl)
}

func (c *Catalog) DisableQueryCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryCache = NewQueryCache(0, 0)
}

func (c *Catalog) GetQueryCacheStats() (hits, misses int64, size int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.queryCache == nil {
		return 0, 0, 0
	}
	return c.queryCache.Stats()
}

func (c *Catalog) invalidateQueryCache(tableName string) {
	if c.queryCache != nil {
		c.queryCache.Invalidate(tableName)
	}
}

func (c *Catalog) CreateRLSPolicy(policy *security.Policy) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	if !c.enableRLS {
		return errors.New("row-level security is not enabled")
	}

	return c.rlsManager.CreatePolicy(policy)
}

func (c *Catalog) DropRLSPolicy(tableName, policyName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	if !c.enableRLS {
		return errors.New("row-level security is not enabled")
	}

	return c.rlsManager.DropPolicy(tableName, policyName)
}

// getCurrentTxn returns the active transaction state for the current transaction.
// If no current transaction is set, it falls back to the legacy single-transaction fields.
func (c *Catalog) getCurrentTxn() *catalogTxnState {
	return c.getGoroutineTxnState()
}

func goroutineTxnShardIdx(gid int64) int {
	return int(uint64(gid) & 15)
}

func (c *Catalog) registerGoroutineTxn(ts *catalogTxnState) {
	shard := &c.goroutineTxnShards[goroutineTxnShardIdx(goroutineID())]
	shard.mu.Lock()
	if shard.m == nil {
		shard.m = make(map[uint64]*catalogTxnState)
	}
	shard.m[uint64(goroutineID())] = ts
	shard.mu.Unlock()
}

func (c *Catalog) unregisterGoroutineTxn() {
	shard := &c.goroutineTxnShards[goroutineTxnShardIdx(goroutineID())]
	shard.mu.Lock()
	delete(shard.m, uint64(goroutineID()))
	shard.mu.Unlock()
}

func (c *Catalog) getGoroutineTxnState() *catalogTxnState {
	shard := &c.goroutineTxnShards[goroutineTxnShardIdx(goroutineID())]
	shard.mu.RLock()
	ts := shard.m[uint64(goroutineID())]
	shard.mu.RUnlock()
	return ts
}

// getTxnState acquires a catalogTxnState from the pool or allocates a new one.
func (c *Catalog) getTxnState() *catalogTxnState {
	if v := c.txnStatePool.Get(); v != nil {
		return v.(*catalogTxnState)
	}
	return &catalogTxnState{}
}

// putTxnState resets and returns a catalogTxnState to the pool for reuse.
func (c *Catalog) putTxnState(ts *catalogTxnState) {
	ts.txnID = 0
	ts.txnActive = false
	ts.undoLog = ts.undoLog[:0]
	ts.savepoints = ts.savepoints[:0]
	ts.managerTxn = nil
	ts.pendingWrites = ts.pendingWrites[:0]
	ts.pendingWriteMap = nil
	for k := range ts.readValues {
		delete(ts.readValues, k)
	}
	for i := range ts.rowBuf {
		ts.rowBuf[i] = nil
	}
	ts.valueDataBuf = ts.valueDataBuf[:0]
	c.txnStatePool.Put(ts)
}

// getCurrentTxnUndoLog returns the undo log for the current transaction.
func (c *Catalog) getCurrentTxnUndoLog() []undoEntry {
	if ts := c.getCurrentTxn(); ts != nil {
		return ts.undoLog
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.undoLog
}

// getCurrentTxnSavepoints returns the savepoints for the current transaction.
func (c *Catalog) getCurrentTxnSavepoints() []savepointEntry {
	if ts := c.getCurrentTxn(); ts != nil {
		return ts.savepoints
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.savepoints
}

// isCurrentTxnActive returns true if the current goroutine has an active
// transaction.  It resolves via the goroutine-to-txn map so it is safe under
// concurrent transactions (unlike the legacy c.txnActive bool).
func (c *Catalog) isCurrentTxnActive() bool {
	if ts := c.getCurrentTxn(); ts != nil {
		return ts.txnActive
	}
	return false
}

// getCurrentTxnID returns the ID of the current goroutine's active transaction,
// or 0 if none is active.  Safe under concurrency because it resolves via the
// goroutine-to-txn map instead of the legacy shared c.txnID field.
func (c *Catalog) getCurrentTxnID() uint64 {
	if ts := c.getCurrentTxn(); ts != nil {
		return ts.txnID
	}
	return 0
}

// appendUndoEntry appends an undo entry to the current transaction's undo log.
func (c *Catalog) appendUndoEntry(entry undoEntry) {
	if ts := c.getCurrentTxn(); ts != nil {
		ts.undoLog = append(ts.undoLog, entry)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.undoLog = append(c.undoLog, entry)
}

// truncateUndoLog truncates the current transaction's undo log to the given length.
func (c *Catalog) truncateUndoLog(length int) {
	if ts := c.getCurrentTxn(); ts != nil {
		ts.undoLog = ts.undoLog[:length]
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.undoLog = c.undoLog[:length]
}

// setCurrentTxnSavepoints sets the savepoints for the current transaction.
func (c *Catalog) setCurrentTxnSavepoints(sps []savepointEntry) {
	if ts := c.getCurrentTxn(); ts != nil {
		ts.savepoints = sps
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.savepoints = sps
}

func (c *Catalog) BeginTransaction(txnID uint64) {
	// Create the Manager transaction outside of c.mu.
	var managerTxn interface{}
	if mgr, ok := c.txnManager.(*txn.Manager); ok && mgr != nil {
		managerTxn = mgr.Begin(txn.DefaultOptions())
	}
	c.beginTransactionLocked(txnID, managerTxn)
}

// BeginTransactionWithTxn begins a catalog transaction using an existing
// manager transaction (e.g. one created by the engine). This avoids creating
// a redundant second transaction when the engine and catalog share the same
// txn.Manager.
func (c *Catalog) BeginTransactionWithTxn(txnID uint64, managerTxn interface{}) {
	c.beginTransactionLocked(txnID, managerTxn)
}

func (c *Catalog) beginTransactionLocked(txnID uint64, managerTxn interface{}) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cs := c.getTxnState()
	cs.txnID = txnID
	cs.txnActive = true
	cs.managerTxn = managerTxn
	// Register goroutine-local txn state so concurrent writers (including autocommit)
	// each see their own transaction state via getCurrentTxn() without a second map lookup.
	c.registerGoroutineTxn(cs)
}

func (c *Catalog) CommitTransaction() error {
	ts := c.getCurrentTxn()

	// When a txn.Manager bridge is active, commit through the Manager first.
	// This performs conflict detection and updates the version store.
	if ts != nil {
		if mt, ok := ts.managerTxn.(*txn.Transaction); ok && mt != nil {
			// Fast path: single-row transaction with no index updates.
			// Bypass all batch-map allocations for the common case.
			if len(ts.pendingWrites) == 1 && len(ts.pendingWrites[0].IndexUpdates) == 0 {
				pw := ts.pendingWrites[0]
				shard := c.commitLockIdx(pw.TreeName, pw.Key)
				c.commitMu[shard].Lock()
				defer c.commitMu[shard].Unlock()

				// Validate reads
				if len(ts.readValues) > 0 {
					c.mu.RLock()
					for wk, originalValue := range ts.readValues {
						tree, exists := c.tableTrees[wk.TreeName]
						if !exists {
							continue
						}
						var currentValue []byte
						if bt, ok := tree.(*btree.BTree); ok {
							currentValue, _ = bt.GetString(wk.Key)
						} else {
							currentValue, _ = tree.Get([]byte(wk.Key))
						}
						if !bytes.Equal(originalValue, currentValue) {
							c.mu.RUnlock()
							return txn.ErrConflict
						}
					}
					c.mu.RUnlock()
				}

				if err := mt.Commit(); err != nil {
					return fmt.Errorf("txn manager commit: %w", err)
				}

				c.mu.RLock()
				tree, exists := c.tableTrees[pw.TreeName]
				c.mu.RUnlock()
				if !exists {
					return fmt.Errorf("partition tree %s not found", pw.TreeName)
				}
				var putErr error
				if bt, ok := tree.(*btree.BTree); ok {
					putErr = bt.PutString(pw.Key, pw.Value)
				} else {
					putErr = tree.Put([]byte(pw.Key), pw.Value)
				}
				if putErr != nil {
					return fmt.Errorf("failed to apply buffered write to %s: %w", pw.TreeName, putErr)
				}
				ts.pendingWrites = ts.pendingWrites[:0]
				ts.pendingWriteMap = nil
			} else {
				// General batch path for multi-row or index-updating transactions.
				numPW := len(ts.pendingWrites)
				tableKeys := make(map[string][][]byte, numPW)
				tableVals := make(map[string][][]byte, numPW)
				idxPuts := make(map[string][][]byte, numPW)
				idxPutVals := make(map[string][][]byte, numPW)
				idxDels := make(map[string][][]byte, numPW)
				shardSet := make(map[int]struct{}, numPW*2)
				for _, pw := range ts.pendingWrites {
					tableKeys[pw.TreeName] = append(tableKeys[pw.TreeName], []byte(pw.Key))
					tableVals[pw.TreeName] = append(tableVals[pw.TreeName], pw.Value)
					shardSet[c.commitLockIdx(pw.TreeName, pw.Key)] = struct{}{}
					for _, idx := range pw.IndexUpdates {
						if idx.IsDelete {
							idxDels[idx.IndexName] = append(idxDels[idx.IndexName], []byte(idx.Key))
						} else {
							idxPuts[idx.IndexName] = append(idxPuts[idx.IndexName], []byte(idx.Key))
							idxPutVals[idx.IndexName] = append(idxPutVals[idx.IndexName], idx.Value)
						}
						shardSet[c.commitLockIdx(idx.IndexName, idx.Key)] = struct{}{}
					}
				}
				for wk := range ts.readValues {
					shardSet[c.commitLockIdx(wk.TreeName, wk.Key)] = struct{}{}
				}

				// Snapshot tree references under a brief RLock before locking commitMu.
				c.mu.RLock()
				tableTrees := make(map[string]btree.TreeStore, len(tableKeys))
				for name := range tableKeys {
					if tree, ok := c.tableTrees[name]; ok {
						tableTrees[name] = tree
					}
				}
				indexTrees := make(map[string]btree.TreeStore, len(idxPuts)+len(idxDels))
				for name := range idxPuts {
					if tree, ok := c.indexTrees[name]; ok {
						indexTrees[name] = tree
					}
				}
				for name := range idxDels {
					if tree, ok := c.indexTrees[name]; ok {
						indexTrees[name] = tree
					}
				}
				c.mu.RUnlock()

				// Lock all touched shards in sorted order, validate, commit through
				// the Manager, apply writes, then unlock.
				if err := func() error {
					sortedShards := make([]int, 0, len(shardSet))
					for s := range shardSet {
						sortedShards = append(sortedShards, s)
					}
					sort.Ints(sortedShards)
					for _, s := range sortedShards {
						c.commitMu[s].Lock()
					}
					defer func() {
						for i := len(sortedShards) - 1; i >= 0; i-- {
							c.commitMu[sortedShards[i]].Unlock()
						}
					}()

					// Validate reads and commit through the Manager while holding locks.
					if len(ts.readValues) > 0 {
						c.mu.RLock()
						for wk, originalValue := range ts.readValues {
							tree, exists := c.tableTrees[wk.TreeName]
							if !exists {
								continue
							}
							var currentValue []byte
							if bt, ok := tree.(*btree.BTree); ok {
								currentValue, _ = bt.GetString(wk.Key)
							} else {
								currentValue, _ = tree.Get([]byte(wk.Key))
							}
							if !bytes.Equal(originalValue, currentValue) {
								c.mu.RUnlock()
								return txn.ErrConflict
							}
						}
						c.mu.RUnlock()
					}

					if err := mt.Commit(); err != nil {
						return fmt.Errorf("txn manager commit: %w", err)
					}

					// Apply writes while still holding locks.
					for name, keys := range tableKeys {
						tree, exists := tableTrees[name]
						if !exists {
							return fmt.Errorf("partition tree %s not found", name)
						}
						if err := tree.PutBatch(keys, tableVals[name]); err != nil {
							return fmt.Errorf("failed to apply buffered writes to %s: %w", name, err)
						}
					}
					for name, keys := range idxPuts {
						if tree, ok := indexTrees[name]; ok {
							_ = tree.PutBatch(keys, idxPutVals[name])
						}
					}
					for name, keys := range idxDels {
						if tree, ok := indexTrees[name]; ok {
							_ = tree.DeleteBatch(keys)
						}
					}
					ts.pendingWrites = ts.pendingWrites[:0]
					ts.pendingWriteMap = nil
					return nil
				}(); err != nil {
					return err
				}
			}
		}
	}

	// Legacy path: write WAL commit record when no Manager handled it.
	managerHandledCommit := false
	if ts != nil && ts.managerTxn != nil {
		if _, ok := ts.managerTxn.(*txn.Transaction); ok {
			managerHandledCommit = true
		}
	}
	if c.wal != nil && ts != nil && ts.txnActive && !managerHandledCommit {
		record := &storage.WALRecord{
			TxnID: ts.txnID,
			Type:  storage.WALCommit,
		}
		if err := c.wal.Append(record); err != nil {
			return err
		}
	}

	if ts == nil {
		c.undoLog = nil
		c.savepoints = nil
	}
	if ts != nil {
		ts.txnActive = false
	}
	c.unregisterGoroutineTxn()
	if ts != nil {
		c.putTxnState(ts)
	}
	return nil
}

func (c *Catalog) FlushTableTrees() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.flushTableTreesLocked()
}

// flushTableTreesLocked is the lock-free internal version. Must be called with mu held.
func (c *Catalog) flushTableTreesLocked() error {
	type item struct {
		name string
		tree btree.TreeStore
	}
	items := make([]item, 0, len(c.tableTrees))
	for name, tree := range c.tableTrees {
		items = append(items, item{name, tree})
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(items) {
		workers = len(items)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(items))
	sem := make(chan struct{}, workers)

	for _, it := range items {
		wg.Add(1)
		sem <- struct{}{}
		go func(name string, tree btree.TreeStore) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := tree.Flush(); err != nil {
				errChan <- fmt.Errorf("failed to flush table %s: %w", name, err)
			}
		}(it.name, it.tree)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		return err
	}
	return nil
}

func (c *Catalog) RollbackTransaction() error {
	// Rollback the Manager transaction first if present.
	ts := c.getCurrentTxn()
	if ts != nil {
		if mt, ok := ts.managerTxn.(*txn.Transaction); ok && mt != nil {
			_ = mt.Rollback() // best-effort; continue with catalog rollback
		}
		// Discard buffered writes.
		ts.pendingWrites = nil
	}

	if c.wal != nil && ts != nil && ts.txnActive {
		record := &storage.WALRecord{
			TxnID: ts.txnID,
			Type:  storage.WALRollback,
		}
		if err := c.wal.Append(record); err != nil {
			return err
		}
	}

	undoLog := c.getCurrentTxnUndoLog()
	if len(undoLog) > 0 {
		// Determine if the undo log contains any DDL entries.
		hasDDL := false
		for _, e := range undoLog {
			if isDDLUndo(e.action) {
				hasDDL = true
				break
			}
		}

		var rollbackErr error
		if !hasDDL {
			// Snapshot tree and table-def references under a brief RLock,
			// then replay DML undos without holding the catalog lock.
			c.mu.RLock()
			tableTrees := make(map[string]btree.TreeStore, len(undoLog))
			tableDefs := make(map[string]*TableDef, len(undoLog))
			indexTrees := make(map[string]btree.TreeStore)
			for _, e := range undoLog {
				if e.tableName != "" {
					tableTrees[e.tableName] = c.tableTrees[e.tableName]
					if td, ok := c.tables[e.tableName]; ok {
						tableDefs[e.tableName] = td
					}
				}
				for _, ic := range e.indexChanges {
					indexTrees[ic.indexName] = c.indexTrees[ic.indexName]
				}
			}
			c.mu.RUnlock()

			for i := len(undoLog) - 1; i >= 0; i-- {
				entry := undoLog[i]
				if err := applyDMLUndoEntry(entry, tableTrees, tableDefs, "rollback"); err != nil && rollbackErr == nil {
					rollbackErr = err
				}
				rollbackErr = reverseIndexChangesWithMaps(entry, indexTrees, "rollback", rollbackErr)
			}
		} else {
			c.mu.Lock()
			rollbackErr = c.replayUndoLog(len(undoLog)-1, 0, "rollback")
			c.mu.Unlock()
		}

		if ts == nil {
			c.mu.Lock()
			c.undoLog = nil
			c.savepoints = nil
			c.mu.Unlock()
		}
		if rollbackErr != nil {
			if ts != nil {
				ts.txnActive = false
			}
			c.unregisterGoroutineTxn()
			if ts != nil {
				c.putTxnState(ts)
			}
			return rollbackErr
		}
	}

	if ts == nil {
		// Legacy single-transaction path: clear shared undo state.
		c.mu.Lock()
		c.undoLog = nil
		c.savepoints = nil
		c.mu.Unlock()
	}

	if ts != nil {
		ts.txnActive = false
	}
	c.unregisterGoroutineTxn()
	if ts != nil {
		c.putTxnState(ts)
	}
	return nil
}

// replayUndoLog replays undo log entries in reverse from startIdx down to endIdx (inclusive).
func (c *Catalog) replayUndoLog(startIdx, endIdx int, errorPrefix string) error {
	undoLog := c.getCurrentTxnUndoLog()
	if startIdx < 0 || len(undoLog) == 0 {
		return nil
	}
	var rollbackErr error
	for i := startIdx; i >= endIdx; i-- {
		entry := undoLog[i]
		if err := c.applyUndoEntry(entry, errorPrefix); err != nil && rollbackErr == nil {
			rollbackErr = err
		}
		rollbackErr = c.reverseIndexChanges(entry, errorPrefix, rollbackErr)
	}
	return rollbackErr
}

func (c *Catalog) applyUndoEntry(entry undoEntry, errorPrefix string) error {
	tree := c.tableTrees[entry.tableName]
	switch entry.action {
	case undoInsert:
		if tree != nil {
			if err := tree.Delete(entry.key); err != nil {
				return fmt.Errorf("%s undoing insert: %w", errorPrefix, err)
			}
		}
	case undoUpdate:
		if tree != nil {
			if err := tree.Put(entry.key, entry.oldValue); err != nil {
				return fmt.Errorf("%s undoing update: %w", errorPrefix, err)
			}
		}
	case undoDelete:
		if tree != nil {
			if err := tree.Put(entry.key, entry.oldValue); err != nil {
				return fmt.Errorf("%s undoing delete: %w", errorPrefix, err)
			}
		}
	case undoCreateTable:
		return c.undoCreateTableEntry(entry, errorPrefix)
	case undoDropTable:
		return c.undoDropTableEntry(entry, errorPrefix)
	case undoCreateIndex:
		return c.undoCreateIndexEntry(entry, errorPrefix)
	case undoDropIndex:
		return c.undoDropIndexEntry(entry, errorPrefix)
	case undoAutoIncSeq:
		if tbl, exists := c.tables[entry.tableName]; exists {
			atomic.StoreInt64(&tbl.AutoIncSeq, entry.oldAutoIncSeq)
		}
	case undoAlterAddColumn:
		if tbl, exists := c.tables[entry.tableName]; exists {
			tbl.Columns = entry.oldColumns
			tbl.buildColumnIndexCache()
			_ = c.storeTableDef(tbl)
		}
	case undoAlterDropColumn:
		return c.undoAlterDropColumnEntry(entry, errorPrefix)
	case undoAlterRename:
		c.undoAlterRenameEntry(entry)
	case undoAlterRenameColumn:
		c.undoAlterRenameColumnEntry(entry)
	}
	return nil
}

func (c *Catalog) undoCreateTableEntry(entry undoEntry, errorPrefix string) error {
	delete(c.tables, entry.tableName)
	delete(c.tableTrees, entry.tableName)
	delete(c.stats, entry.tableName)
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName == entry.tableName {
			delete(c.indexes, idxName)
			delete(c.indexTrees, idxName)
		}
	}
	if c.tree != nil {
		if err := c.tree.Delete([]byte("tbl:" + entry.tableName)); err != nil {
			return fmt.Errorf("%s removing table %s: %w", errorPrefix, entry.tableName, err)
		}
	}
	return nil
}

func (c *Catalog) undoDropTableEntry(entry undoEntry, errorPrefix string) error {
	if entry.tableDef != nil {
		c.tables[entry.tableName] = entry.tableDef
		entry.tableDef.buildColumnIndexCache()
	}
	if entry.tableTree != nil {
		c.tableTrees[entry.tableName] = entry.tableTree
	}
	for idxName, idxDef := range entry.tableIndexes {
		c.indexes[idxName] = idxDef
	}
	for idxName, idxTree := range entry.tableIdxTrees {
		c.indexTrees[idxName] = idxTree
	}
	if entry.tableDef != nil {
		if err := c.storeTableDef(entry.tableDef); err != nil {
			return fmt.Errorf("%s restoring table def %s: %w", errorPrefix, entry.tableName, err)
		}
	}
	return nil
}

func (c *Catalog) undoCreateIndexEntry(entry undoEntry, errorPrefix string) error {
	delete(c.indexes, entry.indexName)
	delete(c.indexTrees, entry.indexName)
	if c.tree != nil {
		if err := c.tree.Delete([]byte("idx:" + entry.indexName)); err != nil {
			return fmt.Errorf("%s dropping index %s: %w", errorPrefix, entry.indexName, err)
		}
	}
	return nil
}

func (c *Catalog) undoDropIndexEntry(entry undoEntry, errorPrefix string) error {
	if entry.indexDef != nil {
		c.indexes[entry.indexName] = entry.indexDef
	}
	if entry.indexTree != nil {
		c.indexTrees[entry.indexName] = entry.indexTree
	}
	if entry.indexDef != nil {
		if err := c.storeIndexDef(entry.indexDef); err != nil {
			return fmt.Errorf("%s restoring index def %s: %w", errorPrefix, entry.indexName, err)
		}
	}
	return nil
}

func (c *Catalog) undoAlterDropColumnEntry(entry undoEntry, errorPrefix string) error {
	tbl, exists := c.tables[entry.tableName]
	if !exists {
		return nil
	}
	tbl.Columns = entry.oldColumns
	tbl.buildColumnIndexCache()
	if t, e := c.tableTrees[entry.tableName]; e {
		for _, rd := range entry.oldRowData {
			if err := t.Put(rd.key, rd.val); err != nil {
				return fmt.Errorf("%s restoring row data for %s: %w", errorPrefix, entry.tableName, err)
			}
		}
	}
	for idxName, idxDef := range entry.droppedIndexes {
		c.indexes[idxName] = idxDef
	}
	for idxName, idxTree := range entry.droppedIdxTrees {
		c.indexTrees[idxName] = idxTree
	}
	if err := c.storeTableDef(tbl); err != nil {
		return fmt.Errorf("%s storing table def %s: %w", errorPrefix, entry.tableName, err)
	}
	return nil
}

func (c *Catalog) undoAlterRenameEntry(entry undoEntry) {
	tbl, exists := c.tables[entry.newName]
	if !exists {
		return
	}
	delete(c.tables, entry.newName)
	c.tables[entry.oldName] = tbl
	tbl.Name = entry.oldName
	if tree, treeExists := c.tableTrees[entry.newName]; treeExists {
		delete(c.tableTrees, entry.newName)
		c.tableTrees[entry.oldName] = tree
	}
	for _, idxDef := range c.indexes {
		if idxDef.TableName == entry.newName {
			idxDef.TableName = entry.oldName
		}
	}
	if stats, sExists := c.stats[entry.newName]; sExists {
		delete(c.stats, entry.newName)
		c.stats[entry.oldName] = stats
	}
}

func (c *Catalog) undoAlterRenameColumnEntry(entry undoEntry) {
	tbl, exists := c.tables[entry.tableName]
	if !exists {
		return
	}
	for i, col := range tbl.Columns {
		if strings.EqualFold(col.Name, entry.newName) {
			tbl.Columns[i].Name = entry.oldName
			break
		}
	}
	for i, pkCol := range tbl.PrimaryKey {
		if strings.EqualFold(pkCol, entry.newName) {
			tbl.PrimaryKey[i] = entry.oldName
		}
	}
	tbl.buildColumnIndexCache()
	for _, idxDef := range c.indexes {
		if idxDef.TableName == entry.tableName {
			for i, idxCol := range idxDef.Columns {
				if strings.EqualFold(idxCol, entry.newName) {
					idxDef.Columns[i] = entry.oldName
				}
			}
		}
	}
	_ = c.storeTableDef(tbl)
}

func (c *Catalog) reverseIndexChanges(entry undoEntry, errorPrefix string, rollbackErr error) error {
	for j := len(entry.indexChanges) - 1; j >= 0; j-- {
		idxChange := entry.indexChanges[j]
		idxTree, exists := c.indexTrees[idxChange.indexName]
		if !exists {
			continue
		}
		var err error
		if idxChange.wasAdded {
			err = idxTree.Delete(idxChange.key)
		} else {
			err = idxTree.Put(idxChange.key, idxChange.oldValue)
		}
		if err != nil && rollbackErr == nil {
			rollbackErr = fmt.Errorf("%s undoing index change: %w", errorPrefix, err)
		}
	}
	return rollbackErr
}

// isDDLUndo reports whether an undo action modifies catalog metadata maps.
func isDDLUndo(a undoAction) bool {
	switch a {
	case undoCreateTable, undoDropTable, undoCreateIndex, undoDropIndex,
		undoAlterAddColumn, undoAlterDropColumn, undoAlterRename, undoAlterRenameColumn:
		return true
	default:
		return false
	}
}

// applyDMLUndoEntry replays a single DML undo entry using snapshotted trees.
// It must NOT be called with DDL entries.
func applyDMLUndoEntry(entry undoEntry, tableTrees map[string]btree.TreeStore, tableDefs map[string]*TableDef, errorPrefix string) error {
	switch entry.action {
	case undoInsert:
		if tree := tableTrees[entry.tableName]; tree != nil {
			if err := tree.Delete(entry.key); err != nil {
				return fmt.Errorf("%s undoing insert: %w", errorPrefix, err)
			}
		}
	case undoUpdate:
		if tree := tableTrees[entry.tableName]; tree != nil {
			if err := tree.Put(entry.key, entry.oldValue); err != nil {
				return fmt.Errorf("%s undoing update: %w", errorPrefix, err)
			}
		}
	case undoDelete:
		if tree := tableTrees[entry.tableName]; tree != nil {
			if err := tree.Put(entry.key, entry.oldValue); err != nil {
				return fmt.Errorf("%s undoing delete: %w", errorPrefix, err)
			}
		}
	case undoAutoIncSeq:
		if tbl := tableDefs[entry.tableName]; tbl != nil {
			atomic.StoreInt64(&tbl.AutoIncSeq, entry.oldAutoIncSeq)
		}
	}
	return nil
}

// reverseIndexChangesWithMaps replays index changes using snapshotted index trees.
func reverseIndexChangesWithMaps(entry undoEntry, indexTrees map[string]btree.TreeStore, errorPrefix string, rollbackErr error) error {
	for j := len(entry.indexChanges) - 1; j >= 0; j-- {
		idxChange := entry.indexChanges[j]
		idxTree := indexTrees[idxChange.indexName]
		if idxTree == nil {
			continue
		}
		var err error
		if idxChange.wasAdded {
			err = idxTree.Delete(idxChange.key)
		} else {
			err = idxTree.Put(idxChange.key, idxChange.oldValue)
		}
		if err != nil && rollbackErr == nil {
			rollbackErr = fmt.Errorf("%s undoing index change: %w", errorPrefix, err)
		}
	}
	return rollbackErr
}

func (c *Catalog) IsTransactionActive() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isCurrentTxnActive()
}

func (c *Catalog) Savepoint(name string) error {
	if !c.isCurrentTxnActive() {
		return fmt.Errorf("SAVEPOINT can only be used within a transaction")
	}
	pwPos := 0
	if ts := c.getCurrentTxn(); ts != nil {
		pwPos = len(ts.pendingWrites)
	}
	sps := append(c.getCurrentTxnSavepoints(), savepointEntry{
		name:            name,
		undoPos:         len(c.getCurrentTxnUndoLog()),
		pendingWritePos: pwPos,
	})
	c.setCurrentTxnSavepoints(sps)
	return nil
}

func (c *Catalog) RollbackToSavepoint(name string) error {
	if !c.isCurrentTxnActive() {
		return fmt.Errorf("ROLLBACK TO SAVEPOINT can only be used within a transaction")
	}

	// Find the savepoint
	sps := c.getCurrentTxnSavepoints()
	spIdx := -1
	for i := len(sps) - 1; i >= 0; i-- {
		if strings.EqualFold(sps[i].name, name) {
			spIdx = i
			break
		}
	}
	if spIdx < 0 {
		return fmt.Errorf("savepoint '%s' does not exist", name)
	}

	undoPos := sps[spIdx].undoPos
	pwPos := sps[spIdx].pendingWritePos

	undoLog := c.getCurrentTxnUndoLog()
	if undoPos >= 0 && undoPos < len(undoLog) {
		// Determine if affected undo entries contain DDL.
		hasDDL := false
		for i := len(undoLog) - 1; i >= undoPos; i-- {
			if isDDLUndo(undoLog[i].action) {
				hasDDL = true
				break
			}
		}

		var rollbackErr error
		if !hasDDL {
			// Snapshot tree and table-def references under a brief RLock,
			// then replay DML undos without holding the catalog lock.
			c.mu.RLock()
			tableTrees := make(map[string]btree.TreeStore)
			tableDefs := make(map[string]*TableDef)
			indexTrees := make(map[string]btree.TreeStore)
			for i := len(undoLog) - 1; i >= undoPos; i-- {
				e := undoLog[i]
				if e.tableName != "" {
					tableTrees[e.tableName] = c.tableTrees[e.tableName]
					if td, ok := c.tables[e.tableName]; ok {
						tableDefs[e.tableName] = td
					}
				}
				for _, ic := range e.indexChanges {
					indexTrees[ic.indexName] = c.indexTrees[ic.indexName]
				}
			}
			c.mu.RUnlock()

			for i := len(undoLog) - 1; i >= undoPos; i-- {
				entry := undoLog[i]
				if err := applyDMLUndoEntry(entry, tableTrees, tableDefs, "rollback to savepoint"); err != nil && rollbackErr == nil {
					rollbackErr = err
				}
				rollbackErr = reverseIndexChangesWithMaps(entry, indexTrees, "rollback to savepoint", rollbackErr)
			}
		} else {
			c.mu.Lock()
			rollbackErr = c.replayUndoLog(len(undoLog)-1, undoPos, "rollback to savepoint")
			c.mu.Unlock()
		}

		if rollbackErr != nil {
			c.truncateUndoLog(undoPos)
			if ts := c.getCurrentTxn(); ts != nil {
				ts.pendingWrites = ts.pendingWrites[:pwPos]
				rebuildPendingWriteMap(ts)
			}
			c.setCurrentTxnSavepoints(sps[:spIdx+1])
			return rollbackErr
		}
	}

	// Truncate undo log to savepoint position
	c.truncateUndoLog(undoPos)
	// Truncate pending writes to savepoint position (buffered mode)
	if ts := c.getCurrentTxn(); ts != nil {
		ts.pendingWrites = ts.pendingWrites[:pwPos]
		rebuildPendingWriteMap(ts)
	}
	// Remove savepoints after this one (but keep the current savepoint)
	c.setCurrentTxnSavepoints(sps[:spIdx+1])
	return nil
}

func (c *Catalog) ReleaseSavepoint(name string) error {
	if !c.isCurrentTxnActive() {
		return fmt.Errorf("RELEASE SAVEPOINT can only be used within a transaction")
	}

	// Find and remove the savepoint.  All savepoint state lives in the
	// per-transaction struct (ts.savepoints), so no catalog lock is needed.
	sps := c.getCurrentTxnSavepoints()
	for i := len(sps) - 1; i >= 0; i-- {
		if strings.EqualFold(sps[i].name, name) {
			// Remove this savepoint and all savepoints created after it
			c.setCurrentTxnSavepoints(sps[:i])
			return nil
		}
	}
	return fmt.Errorf("savepoint '%s' does not exist", name)
}

func (c *Catalog) TxnID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getCurrentTxnID()
}

// SetTxnManager connects the Catalog to a txn.Manager for MVCC multi-writer
// support. When set, DML operations buffer writes in the Manager transaction
// and apply them at commit time instead of mutating B-trees directly.
func (c *Catalog) SetTxnManager(mgr interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txnManager = mgr
}

// hasTxnManager returns true when the Catalog is wired to a txn.Manager.
func (c *Catalog) hasTxnManager() bool {
	return c.txnManager != nil
}

// getCurrentManagerTxn returns the txn.Manager Transaction for the current
// catalog transaction, or nil if none exists.
func (c *Catalog) getCurrentManagerTxn() interface{} {
	if ts := c.getCurrentTxn(); ts != nil {
		return ts.managerTxn
	}
	return nil
}

// recordManagerRead records a read version and value for the given key in the
// current txn.Manager transaction's ReadSet. This enables conflict detection
// for read-modify-write cycles under SnapshotIsolation.
func (c *Catalog) recordManagerRead(treeName string, key string, valueData []byte) {
	// Snapshot the value we read for commit-time validation.
	if ts := c.getCurrentTxn(); ts != nil {
		if ts.readValues == nil {
			ts.readValues = make(map[txn.WriteKey][]byte)
		}
		var valCopy []byte
		if len(valueData) > 0 {
			valCopy = make([]byte, len(valueData))
			copy(valCopy, valueData)
		}
		var wk2 txn.WriteKey
		wk2.TreeName = treeName
		wk2.Key = key
		ts.readValues[wk2] = valCopy
	}

	if !c.isBufferedMode() {
		return
	}

	mt, ok := c.getCurrentManagerTxn().(*txn.Transaction)
	if !ok || mt == nil {
		return
	}
	mgr, ok := c.txnManager.(*txn.Manager)
	if !ok || mgr == nil {
		return
	}
	ver := mgr.GetCurrentVersion(treeName, key)
	mt.SetReadVersion(treeName, key, ver)
}

// isBufferedMode returns true when the current transaction should buffer
// writes instead of mutating B-trees directly.
func (c *Catalog) isBufferedMode() bool {
	return c.enableBufferedWrites && c.hasTxnManager() && c.getCurrentManagerTxn() != nil && c.isCurrentTxnActive()
}

// EnableBufferedWrites turns on the buffered DML path for MVCC multi-writer.
// Read-your-writes is fully implemented in scan, select, insert, update,
// delete, and fast-path execution.
func (c *Catalog) EnableBufferedWrites() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enableBufferedWrites = true
}

// getCurrentTxnPendingWrites returns the pending write buffer for the current
// transaction, initializing it if necessary.
func (c *Catalog) getCurrentTxnPendingWrites() []PendingWrite {
	if ts := c.getCurrentTxn(); ts != nil {
		return ts.pendingWrites
	}
	return nil
}

// appendPendingWrite adds a buffered write to the current transaction.
func (c *Catalog) appendPendingWrite(pw PendingWrite) {
	if ts := c.getCurrentTxn(); ts != nil {
		ts.pendingWrites = append(ts.pendingWrites, pw)
		// Only build the map index when we have multiple pending writes.
		// Single-row autocommit transactions never need O(1) key lookup,
		// so this saves two heap allocations per INSERT.
		if len(ts.pendingWrites) > 1 {
			if ts.pendingWriteMap == nil {
				rebuildPendingWriteMap(ts)
			}
			if ts.pendingWriteMap[pw.TreeName] == nil {
				ts.pendingWriteMap[pw.TreeName] = make(map[string]PendingWrite)
			}
			ts.pendingWriteMap[pw.TreeName][pw.Key] = pw
		}
	}
}

// clearPendingWrites discards all buffered writes for the current transaction.
func (c *Catalog) clearPendingWrites() {
	if ts := c.getCurrentTxn(); ts != nil {
		ts.pendingWrites = nil
		ts.pendingWriteMap = nil
	}
}

// rebuildPendingWriteMap rebuilds the map index from the pendingWrites slice.
// Call this after rolling back or truncating pending writes.
func rebuildPendingWriteMap(ts *catalogTxnState) {
	if len(ts.pendingWrites) == 0 {
		ts.pendingWriteMap = nil
		return
	}
	ts.pendingWriteMap = make(map[string]map[string]PendingWrite, len(ts.pendingWrites))
	for _, pw := range ts.pendingWrites {
		if ts.pendingWriteMap[pw.TreeName] == nil {
			ts.pendingWriteMap[pw.TreeName] = make(map[string]PendingWrite)
		}
		ts.pendingWriteMap[pw.TreeName][pw.Key] = pw
	}
}

// keyInPendingWrites reports whether the given table/key already exists in the
// current transaction's pending write buffer.
func (c *Catalog) keyInPendingWrites(tableName string, key string) bool {
	ts := c.getCurrentTxn()
	if ts == nil || len(ts.pendingWrites) == 0 {
		return false
	}
	if m, ok := ts.getPendingWriteMap()[tableName]; ok {
		_, exists := m[key]
		return exists
	}
	for _, pw := range ts.pendingWrites {
		if pw.TreeName == tableName && pw.Key == key {
			return true
		}
	}
	return false
}

// indexKeyInPendingWrites reports whether the given index key already exists in
// the current transaction's pending write buffer (for unique constraint checks).
func (c *Catalog) indexKeyInPendingWrites(indexName string, key string) bool {
	ts := c.getCurrentTxn()
	if ts == nil || len(ts.pendingWrites) == 0 {
		return false
	}
	for _, pw := range ts.pendingWrites {
		for _, idx := range pw.IndexUpdates {
			if idx.IndexName == indexName && idx.Key == key && !idx.IsDelete {
				return true
			}
		}
	}
	return false
}

// ReplayWALOps replays logical WAL operations (from txn.Manager commit) into
// the primary B-trees.  This is called during database open after the catalog
// has been loaded.  It restores committed data that may not have been flushed
// to pages before a crash.  After replay, all indexes for affected tables are
// rebuilt so that index trees stay consistent with the recovered table data.
func (c *Catalog) ReplayWALOps(ops []storage.WALReplayOp) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	affectedTables := make(map[string]struct{})

	for _, op := range ops {
		switch op.Type {
		case storage.WALInsert, storage.WALUpdate:
			if len(op.Data) < 4 {
				continue
			}
			keyLen := binary.LittleEndian.Uint32(op.Data[0:4])
			if int(4+keyLen) > len(op.Data) {
				continue
			}
			key := string(op.Data[4 : 4+keyLen])
			value := op.Data[4+keyLen:]

			// Manager format: "tableName:rowKey"
			parts := strings.SplitN(key, ":", 2)
			if len(parts) != 2 {
				// Legacy format without table prefix — cannot route.
				continue
			}
			tree, exists := c.tableTrees[parts[0]]
			if !exists {
				continue
			}
			_ = tree.Put([]byte(parts[1]), value)
			affectedTables[parts[0]] = struct{}{}

		case storage.WALDelete:
			if len(op.Data) < 4 {
				continue
			}
			keyLen := binary.LittleEndian.Uint32(op.Data[0:4])
			if int(4+keyLen) > len(op.Data) {
				continue
			}
			key := string(op.Data[4 : 4+keyLen])
			parts := strings.SplitN(key, ":", 2)
			if len(parts) != 2 {
				continue
			}
			tree, exists := c.tableTrees[parts[0]]
			if !exists {
				continue
			}
			_ = tree.Delete([]byte(parts[1]))
			affectedTables[parts[0]] = struct{}{}
		}
	}

	for tableName := range affectedTables {
		if err := c.rebuildTableIndexesLocked(tableName); err != nil {
			return fmt.Errorf("failed to rebuild indexes for %s: %w", tableName, err)
		}
	}
	return nil
}
