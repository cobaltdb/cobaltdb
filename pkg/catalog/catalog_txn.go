package catalog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/cache"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

func (c *Catalog) EnableRLS() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enableRLS = true
	if c.rlsManager == nil {
		c.rlsManager = security.NewManager()
	}
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
	c.enableQueryCache(cache.DefaultConfig().MaxSize, maxSize, ttl)
}

func (c *Catalog) EnableQueryCacheWithLimits(maxBytes int64, maxEntries int, ttl time.Duration) {
	defaults := cache.DefaultConfig()
	if maxBytes <= 0 {
		maxBytes = defaults.MaxSize
	}
	if maxEntries <= 0 {
		maxEntries = defaults.MaxEntries
	}
	c.enableQueryCache(maxBytes, maxEntries, ttl)
}

func (c *Catalog) enableQueryCache(maxBytes int64, maxEntries int, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryCache = cache.New(&cache.Config{
		MaxEntries:      maxEntries,
		MaxSize:         maxBytes,
		TTL:             ttl,
		Enabled:         maxBytes > 0 && maxEntries > 0,
		CleanupInterval: 1 * time.Minute,
	})
}

func (c *Catalog) DisableQueryCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queryCache != nil {
		c.queryCache.Close()
	}
	c.queryCache = nil
}

// GetQueryCache returns the catalog's query cache (nil if not enabled).
func (c *Catalog) GetQueryCache() *cache.Cache {
	return c.queryCache
}

func (c *Catalog) GetQueryCacheStats() (hits, misses int64, size int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.queryCache == nil {
		return 0, 0, 0
	}
	stats := c.queryCache.Stats()
	return clampUint64ToInt64(stats.Hits), clampUint64ToInt64(stats.Misses), stats.EntryCount
}

func clampUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

func (c *Catalog) invalidateQueryCache(tableName string) {
	if c.queryCache != nil {
		c.queryCache.InvalidateTable(tableName)
	}
}

func (c *Catalog) CreateRLSPolicy(policy *security.Policy) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	if !c.enableRLS {
		return errors.New("row-level security is not enabled")
	}
	if policy == nil {
		return security.ErrInvalidPolicy
	}

	if c.rlsManager == nil {
		c.rlsManager = security.NewManager()
	}
	if err := c.rlsManager.CreatePolicy(policy); err != nil {
		return err
	}
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:        undoCreateRLSPolicy,
			rlsTableName:  policy.TableName,
			rlsPolicyName: policy.Name,
		})
	}
	return nil
}

func (c *Catalog) DropRLSPolicy(tableName, policyName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	if !c.enableRLS {
		return errors.New("row-level security is not enabled")
	}

	if c.rlsManager == nil {
		return security.ErrPolicyNotFound
	}
	dropped, err := c.rlsManager.GetPolicy(tableName, policyName)
	if err != nil {
		return err
	}
	if err := c.rlsManager.DropPolicy(tableName, policyName); err != nil {
		return err
	}
	if err := c.deleteCatalogDef("rlsp:" + strings.ToLower(tableName) + ":" + strings.ToLower(policyName)); err != nil {
		_ = c.rlsManager.CreatePolicy(dropped)
		return fmt.Errorf("failed to delete RLS policy metadata %s on %s: %w", policyName, tableName, err)
	}
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:        undoDropRLSPolicy,
			rlsTableName:  tableName,
			rlsPolicyName: policyName,
			rlsPolicy:     dropped,
		})
	}
	return nil
}

func (c *Catalog) EnableRLSTable(tableName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	if !c.hasTableLocked(tableName) {
		return fmt.Errorf("table %s does not exist", tableName)
	}
	if c.rlsManager == nil {
		c.rlsManager = security.NewManager()
	}
	wasEnabled := c.enableRLS && c.rlsManager.IsEnabled(tableName)
	c.enableRLS = true
	c.rlsManager.EnableTable(tableName)
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:             undoEnableRLSTable,
			rlsTableName:       tableName,
			rlsTableWasEnabled: wasEnabled,
		})
	}
	return nil
}

// getCurrentTxn returns the active transaction state for the current transaction.
// If no current transaction is set, it falls back to the legacy single-transaction fields.
func (c *Catalog) getCurrentTxn() *catalogTxnState {
	return c.getGoroutineTxnState()
}

func goroutineTxnShardIdx(key uint64) int {
	return int(key & 15) // #nosec G115 - masked shard index is in [0, 15].
}

func goroutineTxnKey(gid int64) uint64 {
	if gid < 0 {
		return uint64(-(gid + 1)) + 1 // #nosec G115 - adjusted before conversion to handle MinInt64.
	}
	return uint64(gid) // #nosec G115 - negative values handled above.
}

func (c *Catalog) registerGoroutineTxn(ts *catalogTxnState) {
	key := goroutineTxnKey(goroutineID())
	shard := &c.goroutineTxnShards[goroutineTxnShardIdx(key)]
	shard.mu.Lock()
	if shard.m == nil {
		shard.m = make(map[uint64]*catalogTxnState)
	}
	shard.m[key] = ts
	shard.mu.Unlock()
}

func (c *Catalog) unregisterGoroutineTxn() {
	key := goroutineTxnKey(goroutineID())
	shard := &c.goroutineTxnShards[goroutineTxnShardIdx(key)]
	shard.mu.Lock()
	delete(shard.m, key)
	shard.mu.Unlock()
}

func (c *Catalog) getGoroutineTxnState() *catalogTxnState {
	key := goroutineTxnKey(goroutineID())
	shard := &c.goroutineTxnShards[goroutineTxnShardIdx(key)]
	shard.mu.RLock()
	ts := shard.m[key]
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
	for k := range ts.treeCache {
		delete(ts.treeCache, k)
	}
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
	// No c.mu needed: getTxnState() uses sync.Pool and registerGoroutineTxn
	// uses per-shard mutexes. Removing this RLock eliminates a major source
	// of contention for autocommit workloads where every statement begins a
	// new catalog transaction.
	cs := c.getTxnState()
	cs.txnID = txnID
	cs.txnActive = true
	cs.managerTxn = managerTxn
	// Register goroutine-local txn state so concurrent writers (including autocommit)
	// each see their own transaction state via getCurrentTxn() without a second map lookup.
	c.registerGoroutineTxn(cs)
}

// pendingIdxOp is the net effect of one transaction's buffered updates on a
// single index key, used to collapse a delete+re-insert (or insert+delete) of
// the same key to its last operation before applying at commit.
type pendingIdxOp struct {
	isDelete bool
	value    []byte
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

				// Validate reads using cached tree references (no c.mu needed).
				if len(ts.readValues) > 0 {
					for wk, originalValue := range ts.readValues {
						tree, exists := ts.treeCache[wk.TreeName]
						if !exists || tree == nil {
							continue
						}
						currentValue, err := readCommitValidationValue(tree, wk.TreeName, wk.Key)
						if err != nil {
							c.commitMu[shard].Unlock()
							return err
						}
						if !bytes.Equal(originalValue, currentValue) {
							c.commitMu[shard].Unlock()
							return txn.ErrConflict
						}
					}
				}

				if err := mt.Commit(); err != nil {
					c.commitMu[shard].Unlock()
					return fmt.Errorf("txn manager commit: %w", err)
				}

				tree, exists := ts.treeCache[pw.TreeName]
				if !exists || tree == nil {
					c.commitMu[shard].Unlock()
					return fmt.Errorf("partition tree %s not found", pw.TreeName)
				}
				var putErr error
				if bt, ok := tree.(*btree.BTree); ok {
					putErr = bt.PutString(pw.Key, pw.Value)
				} else {
					putErr = tree.Put([]byte(pw.Key), pw.Value)
				}
				if putErr != nil {
					c.commitMu[shard].Unlock()
					return fmt.Errorf("failed to apply buffered write to %s: %w", pw.TreeName, putErr)
				}
				c.commitMu[shard].Unlock()
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
				// Collapse repeated ops on the same index key to their net effect
				// (last op in pendingWrites order wins), so a delete+re-insert of a
				// unique key ends with the entry present and an insert+delete with it
				// absent; the same key must not land in both idxPuts and idxDels.
				idxNet := make(map[string]map[string]pendingIdxOp, numPW)
				shardSet := make(map[int]struct{}, numPW*2)
				for _, pw := range ts.pendingWrites {
					tableKeys[pw.TreeName] = append(tableKeys[pw.TreeName], []byte(pw.Key))
					tableVals[pw.TreeName] = append(tableVals[pw.TreeName], pw.Value)
					shardSet[c.commitLockIdx(pw.TreeName, pw.Key)] = struct{}{}
					for _, idx := range pw.IndexUpdates {
						m := idxNet[idx.IndexName]
						if m == nil {
							m = make(map[string]pendingIdxOp)
							idxNet[idx.IndexName] = m
						}
						m[idx.Key] = pendingIdxOp{isDelete: idx.IsDelete, value: idx.Value}
						shardSet[c.commitLockIdx(idx.IndexName, idx.Key)] = struct{}{}
					}
				}
				for idxName, m := range idxNet {
					for k, op := range m {
						if op.isDelete {
							idxDels[idxName] = append(idxDels[idxName], []byte(k))
						} else {
							idxPuts[idxName] = append(idxPuts[idxName], []byte(k))
							idxPutVals[idxName] = append(idxPutVals[idxName], op.value)
						}
					}
				}
				for wk := range ts.readValues {
					shardSet[c.commitLockIdx(wk.TreeName, wk.Key)] = struct{}{}
				}

				// Use cached tree references (no c.mu needed).
				tableTrees := make(map[string]btree.TreeStore, len(tableKeys))
				for name := range tableKeys {
					if tree, ok := ts.treeCache[name]; ok && tree != nil {
						tableTrees[name] = tree
					}
				}
				indexTrees := make(map[string]btree.TreeStore, len(idxPuts)+len(idxDels))
				for name := range idxPuts {
					if tree, ok := ts.treeCache[name]; ok && tree != nil {
						indexTrees[name] = tree
					}
				}
				for name := range idxDels {
					if tree, ok := ts.treeCache[name]; ok && tree != nil {
						indexTrees[name] = tree
					}
				}

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
						for wk, originalValue := range ts.readValues {
							tree, exists := ts.treeCache[wk.TreeName]
							if !exists || tree == nil {
								continue
							}
							currentValue, err := readCommitValidationValue(tree, wk.TreeName, wk.Key)
							if err != nil {
								return err
							}
							if !bytes.Equal(originalValue, currentValue) {
								return txn.ErrConflict
							}
						}
					}

					for name := range tableKeys {
						if _, exists := tableTrees[name]; !exists {
							return fmt.Errorf("partition tree %s not found", name)
						}
					}
					for name := range idxPuts {
						if _, exists := indexTrees[name]; !exists {
							return fmt.Errorf("index tree %s not found", name)
						}
					}
					for name := range idxDels {
						if _, exists := indexTrees[name]; !exists {
							return fmt.Errorf("index tree %s not found", name)
						}
					}

					if err := mt.Commit(); err != nil {
						return fmt.Errorf("txn manager commit: %w", err)
					}

					// Apply writes while still holding locks.
					for name, keys := range tableKeys {
						tree := tableTrees[name]
						if err := tree.PutBatch(keys, tableVals[name]); err != nil {
							return fmt.Errorf("failed to apply buffered writes to %s: %w", name, err)
						}
					}
					for name, keys := range idxPuts {
						tree := indexTrees[name]
						if err := tree.PutBatch(keys, idxPutVals[name]); err != nil {
							return fmt.Errorf("failed to apply buffered index writes to %s: %w", name, err)
						}
					}
					for name, keys := range idxDels {
						tree := indexTrees[name]
						if err := tree.DeleteBatch(keys); err != nil {
							return fmt.Errorf("failed to apply buffered index deletes to %s: %w", name, err)
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

func readCommitValidationValue(tree btree.TreeStore, treeName, key string) ([]byte, error) {
	var (
		currentValue []byte
		err          error
	)
	if bt, ok := tree.(*btree.BTree); ok {
		currentValue, err = bt.GetString(key)
	} else {
		currentValue, err = tree.Get([]byte(key))
	}
	if err == nil {
		return currentValue, nil
	}
	if errors.Is(err, btree.ErrKeyNotFound) {
		return nil, nil
	}
	return nil, fmt.Errorf("failed to validate read for %s/%s: %w", treeName, key, err)
}

func (c *Catalog) FlushTableTrees() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.flushTableTreesLocked()
}

// flushTableTreesLocked is the lock-free internal version. Must be called with mu held.
func (c *Catalog) flushTableTreesLocked() error {
	return flushTreeStoreMapLocked("table", c.tableTrees)
}

// flushIndexTreesLocked is the lock-free internal version. Must be called with mu held.
func (c *Catalog) flushIndexTreesLocked() error {
	return flushTreeStoreMapLocked("index", c.indexTrees)
}

func flushTreeStoreMapLocked(kind string, trees map[string]btree.TreeStore) error {
	type item struct {
		name string
		tree btree.TreeStore
	}
	items := make([]item, 0, len(trees))
	for name, tree := range trees {
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
				errChan <- fmt.Errorf("failed to flush %s %s: %w", kind, name, err)
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
	case undoCreateFTSIndex:
		delete(c.ftsIndexes, entry.indexName)
	case undoDropFTSIndex:
		if entry.ftsIndexDef != nil {
			c.ftsIndexes[entry.indexName] = cloneFTSIndexDef(entry.ftsIndexDef)
		}
	case undoCreateVectorIndex:
		delete(c.vectorIndexes, entry.indexName)
		if c.tree != nil {
			if err := c.tree.Delete([]byte("vec:" + entry.indexName)); err != nil {
				return fmt.Errorf("%s dropping vector index %s: %w", errorPrefix, entry.indexName, err)
			}
		}
	case undoDropVectorIndex:
		if entry.vectorIndexDef != nil {
			cloned := cloneVectorIndexDef(entry.vectorIndexDef)
			c.vectorIndexes[entry.indexName] = cloned
			if err := c.storeVectorIndexDef(cloned); err != nil {
				return fmt.Errorf("%s restoring vector index def %s: %w", errorPrefix, entry.indexName, err)
			}
		}
	case undoAutoIncSeq:
		if tbl, exists := c.tables[entry.tableName]; exists {
			atomic.StoreInt64(&tbl.AutoIncSeq, entry.oldAutoIncSeq)
		}
	case undoAlterAddColumn:
		if tbl, exists := c.tables[entry.tableName]; exists {
			tbl.Columns = entry.oldColumns
			tbl.buildColumnIndexCache()
			if err := c.storeTableDef(tbl); err != nil {
				return fmt.Errorf("%s storing table def %s after add column undo: %w", errorPrefix, entry.tableName, err)
			}
		}
	case undoAlterDropColumn:
		return c.undoAlterDropColumnEntry(entry, errorPrefix)
	case undoAlterRename:
		return c.undoAlterRenameEntry(entry, errorPrefix)
	case undoAlterRenameColumn:
		return c.undoAlterRenameColumnEntry(entry, errorPrefix)
	case undoAlterForeignKeys:
		if tbl, exists := c.tables[entry.tableName]; exists {
			tbl.ForeignKeys = cloneForeignKeys(entry.oldForeignKeys)
			if err := c.storeTableDef(tbl); err != nil {
				return fmt.Errorf("%s storing table def %s after foreign key undo: %w", errorPrefix, entry.tableName, err)
			}
		}
	case undoAlterChecks:
		if tbl, exists := c.tables[entry.tableName]; exists {
			tbl.Checks = cloneCheckDefs(entry.oldChecks)
			if err := c.storeTableDef(tbl); err != nil {
				return fmt.Errorf("%s storing table def %s after check constraint undo: %w", errorPrefix, entry.tableName, err)
			}
		}
	case undoCreateView:
		return c.undoCreateViewEntry(entry, errorPrefix)
	case undoDropView:
		return c.undoDropViewEntry(entry, errorPrefix)
	case undoCreateTrigger:
		return c.undoCreateTriggerEntry(entry, errorPrefix)
	case undoDropTrigger:
		return c.undoDropTriggerEntry(entry, errorPrefix)
	case undoCreateProcedure:
		return c.undoCreateProcedureEntry(entry, errorPrefix)
	case undoDropProcedure:
		return c.undoDropProcedureEntry(entry, errorPrefix)
	case undoCreateMaterializedView:
		return c.undoCreateMaterializedViewEntry(entry, errorPrefix)
	case undoDropMaterializedView:
		return c.undoDropMaterializedViewEntry(entry, errorPrefix)
	case undoCreateForeignTable:
		return c.undoCreateForeignTableEntry(entry, errorPrefix)
	case undoDropForeignTable:
		return c.undoDropForeignTableEntry(entry, errorPrefix)
	case undoEnableRLSTable:
		if c.rlsManager != nil && !entry.rlsTableWasEnabled {
			c.rlsManager.DisableTable(entry.rlsTableName)
			if err := c.deleteCatalogDef("rlst:" + strings.ToLower(entry.rlsTableName)); err != nil {
				return fmt.Errorf("%s removing RLS table metadata %s: %w", errorPrefix, entry.rlsTableName, err)
			}
		}
	case undoCreateRLSPolicy:
		if c.rlsManager != nil {
			if err := c.rlsManager.DropPolicy(entry.rlsTableName, entry.rlsPolicyName); err != nil && !errors.Is(err, security.ErrPolicyNotFound) {
				return fmt.Errorf("%s dropping RLS policy %s on %s: %w", errorPrefix, entry.rlsPolicyName, entry.rlsTableName, err)
			}
			if err := c.deleteCatalogDef("rlsp:" + strings.ToLower(entry.rlsTableName) + ":" + strings.ToLower(entry.rlsPolicyName)); err != nil {
				return fmt.Errorf("%s removing RLS policy metadata %s on %s: %w", errorPrefix, entry.rlsPolicyName, entry.rlsTableName, err)
			}
		}
	case undoDropRLSPolicy:
		if c.rlsManager == nil {
			c.rlsManager = security.NewManager()
		}
		if entry.rlsPolicy != nil {
			if err := c.rlsManager.CreatePolicy(entry.rlsPolicy); err != nil && !errors.Is(err, security.ErrPolicyAlreadyExists) {
				return fmt.Errorf("%s restoring RLS policy %s on %s: %w", errorPrefix, entry.rlsPolicyName, entry.rlsTableName, err)
			}
			if err := c.storeRLSEnabledTable(entry.rlsPolicy.TableName); err != nil {
				return fmt.Errorf("%s restoring RLS table metadata %s: %w", errorPrefix, entry.rlsPolicy.TableName, err)
			}
			if err := c.storeRLSPolicyDef(entry.rlsPolicy); err != nil {
				return fmt.Errorf("%s restoring RLS policy metadata %s on %s: %w", errorPrefix, entry.rlsPolicyName, entry.rlsTableName, err)
			}
		}
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
	for idxName, idxDef := range entry.tableIndexes {
		if idxDef == nil {
			continue
		}
		if err := c.storeIndexDef(idxDef); err != nil {
			return fmt.Errorf("%s restoring index def %s: %w", errorPrefix, idxName, err)
		}
	}
	if entry.rlsTableWasEnabled {
		if c.rlsManager == nil {
			c.rlsManager = security.NewManager()
		}
		c.enableRLS = true
		c.rlsManager.EnableTable(entry.tableName)
		if err := c.storeRLSEnabledTable(entry.tableName); err != nil {
			return fmt.Errorf("%s restoring RLS table metadata %s: %w", errorPrefix, entry.tableName, err)
		}
	}
	for _, policy := range entry.rlsPolicies {
		if policy == nil {
			continue
		}
		if c.rlsManager == nil {
			c.rlsManager = security.NewManager()
		}
		c.enableRLS = true
		if err := c.rlsManager.CreatePolicy(policy); err != nil && !errors.Is(err, security.ErrPolicyAlreadyExists) {
			return fmt.Errorf("%s restoring RLS policy %s on %s: %w", errorPrefix, policy.Name, policy.TableName, err)
		}
		if err := c.storeRLSPolicyDef(policy); err != nil {
			return fmt.Errorf("%s restoring RLS policy metadata %s on %s: %w", errorPrefix, policy.Name, policy.TableName, err)
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
	for idxName, idxDef := range entry.droppedIndexes {
		if idxDef == nil {
			continue
		}
		if err := c.storeIndexDef(idxDef); err != nil {
			return fmt.Errorf("%s restoring dropped index def %s: %w", errorPrefix, idxName, err)
		}
	}
	return nil
}

func (c *Catalog) undoAlterRenameEntry(entry undoEntry, errorPrefix string) error {
	tbl, exists := c.tables[entry.newName]
	if !exists {
		return nil
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
	changedFKTables := make(map[string]*TableDef)
	for tableName, tbl := range c.tables {
		for i := range tbl.ForeignKeys {
			if strings.EqualFold(tbl.ForeignKeys[i].ReferencedTable, entry.newName) {
				tbl.ForeignKeys[i].ReferencedTable = entry.oldName
				changedFKTables[tableName] = tbl
			}
		}
	}
	if stats, sExists := c.stats[entry.newName]; sExists {
		delete(c.stats, entry.newName)
		c.stats[entry.oldName] = stats
	}
	if c.tree != nil {
		if err := c.tree.Delete([]byte("tbl:" + entry.newName)); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
			return fmt.Errorf("%s removing renamed table def %s: %w", errorPrefix, entry.newName, err)
		}
		if err := c.storeTableDef(tbl); err != nil {
			return fmt.Errorf("%s restoring renamed table def %s: %w", errorPrefix, entry.oldName, err)
		}
	}
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName == entry.oldName {
			if err := c.storeIndexDef(idxDef); err != nil {
				return fmt.Errorf("%s storing index def %s after rename table undo: %w", errorPrefix, idxName, err)
			}
		}
	}
	for tableName, tbl := range changedFKTables {
		if tableName == entry.oldName {
			continue
		}
		if err := c.storeTableDef(tbl); err != nil {
			return fmt.Errorf("%s storing foreign key metadata for table %s after rename table undo: %w", errorPrefix, tableName, err)
		}
	}
	return nil
}

func (c *Catalog) undoAlterRenameColumnEntry(entry undoEntry, errorPrefix string) error {
	tbl, exists := c.tables[entry.tableName]
	if !exists {
		return nil
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
	renameCheckColumnReferences(tbl, entry.newName, entry.oldName)
	for _, idxDef := range c.indexes {
		if idxDef.TableName == entry.tableName {
			for i, idxCol := range idxDef.Columns {
				if strings.EqualFold(idxCol, entry.newName) {
					idxDef.Columns[i] = entry.oldName
				}
			}
		}
	}
	changedFKTables := make(map[string]*TableDef)
	if renameForeignKeyColumns(tbl.ForeignKeys, entry.newName, entry.oldName) {
		changedFKTables[entry.tableName] = tbl
	}
	for tableName, table := range c.tables {
		if renameReferencedForeignKeyColumns(table.ForeignKeys, entry.tableName, entry.newName, entry.oldName) {
			changedFKTables[tableName] = table
		}
	}
	if err := c.storeTableDef(tbl); err != nil {
		return fmt.Errorf("%s storing table def %s after rename column undo: %w", errorPrefix, entry.tableName, err)
	}
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName == entry.tableName {
			if err := c.storeIndexDef(idxDef); err != nil {
				return fmt.Errorf("%s storing index def %s after rename column undo: %w", errorPrefix, idxName, err)
			}
		}
	}
	for tableName, table := range changedFKTables {
		if tableName == entry.tableName {
			continue
		}
		if err := c.storeTableDef(table); err != nil {
			return fmt.Errorf("%s storing foreign key metadata for table %s after rename column undo: %w", errorPrefix, tableName, err)
		}
	}
	return nil
}

func (c *Catalog) undoCreateViewEntry(entry undoEntry, errorPrefix string) error {
	delete(c.views, entry.viewName)
	delete(c.viewSQL, entry.viewName)
	delete(c.viewTemporary, entry.viewName)
	if c.tree != nil {
		if err := c.tree.Delete([]byte("view:" + entry.viewName)); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
			return fmt.Errorf("%s removing view %s: %w", errorPrefix, entry.viewName, err)
		}
	}
	return nil
}

func (c *Catalog) undoDropViewEntry(entry undoEntry, errorPrefix string) error {
	if c.views == nil {
		c.views = make(map[string]*query.SelectStmt)
	}
	if c.viewSQL == nil {
		c.viewSQL = make(map[string]string)
	}
	c.views[entry.viewName] = entry.viewQuery
	c.viewSQL[entry.viewName] = entry.viewSQL
	if c.viewTemporary == nil {
		c.viewTemporary = make(map[string]bool)
	}
	c.viewTemporary[entry.viewName] = entry.viewTemporary
	if strings.TrimSpace(c.viewSQL[entry.viewName]) == "" {
		c.viewSQL[entry.viewName] = createViewSQL(entry.viewName, entry.viewQuery)
	}
	if c.tree != nil && !entry.viewTemporary {
		if err := c.storeViewDef(entry.viewName, c.viewSQL[entry.viewName]); err != nil {
			return fmt.Errorf("%s restoring view %s: %w", errorPrefix, entry.viewName, err)
		}
	}
	return nil
}

func (c *Catalog) undoCreateTriggerEntry(entry undoEntry, errorPrefix string) error {
	delete(c.triggers, entry.triggerName)
	delete(c.triggerSQL, entry.triggerName)
	if c.tree != nil {
		if err := c.tree.Delete([]byte("trg:" + entry.triggerName)); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
			return fmt.Errorf("%s removing trigger %s: %w", errorPrefix, entry.triggerName, err)
		}
	}
	return nil
}

func (c *Catalog) undoDropTriggerEntry(entry undoEntry, errorPrefix string) error {
	if c.triggers == nil {
		c.triggers = make(map[string]*query.CreateTriggerStmt)
	}
	if c.triggerSQL == nil {
		c.triggerSQL = make(map[string]string)
	}
	c.triggers[entry.triggerName] = entry.triggerStmt
	c.triggerSQL[entry.triggerName] = entry.triggerSQL
	if strings.TrimSpace(c.triggerSQL[entry.triggerName]) == "" {
		c.triggerSQL[entry.triggerName] = createTriggerSQL(entry.triggerStmt)
	}
	if entry.triggerStmt != nil {
		entry.triggerStmt.RawSQL = c.triggerSQL[entry.triggerName]
	}
	if c.tree != nil {
		if err := c.storeTriggerDef(entry.triggerName, c.triggerSQL[entry.triggerName]); err != nil {
			return fmt.Errorf("%s restoring trigger %s: %w", errorPrefix, entry.triggerName, err)
		}
	}
	return nil
}

func (c *Catalog) undoCreateProcedureEntry(entry undoEntry, errorPrefix string) error {
	delete(c.procedures, entry.procedureName)
	delete(c.procedureSQL, entry.procedureName)
	if c.tree != nil {
		if err := c.tree.Delete([]byte("proc:" + entry.procedureName)); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
			return fmt.Errorf("%s removing procedure %s: %w", errorPrefix, entry.procedureName, err)
		}
	}
	return nil
}

func (c *Catalog) undoDropProcedureEntry(entry undoEntry, errorPrefix string) error {
	if c.procedures == nil {
		c.procedures = make(map[string]*query.CreateProcedureStmt)
	}
	if c.procedureSQL == nil {
		c.procedureSQL = make(map[string]string)
	}
	c.procedures[entry.procedureName] = entry.procedureStmt
	c.procedureSQL[entry.procedureName] = entry.procedureSQL
	if strings.TrimSpace(c.procedureSQL[entry.procedureName]) == "" {
		c.procedureSQL[entry.procedureName] = createProcedureSQL(entry.procedureStmt)
	}
	if entry.procedureStmt != nil {
		entry.procedureStmt.RawSQL = c.procedureSQL[entry.procedureName]
	}
	if c.tree != nil {
		if err := c.storeProcedureDef(entry.procedureName, c.procedureSQL[entry.procedureName]); err != nil {
			return fmt.Errorf("%s restoring procedure %s: %w", errorPrefix, entry.procedureName, err)
		}
	}
	return nil
}

func (c *Catalog) undoCreateMaterializedViewEntry(entry undoEntry, errorPrefix string) error {
	delete(c.materializedViews, entry.materializedViewName)
	delete(c.materializedViewSQL, entry.materializedViewName)
	if c.cteResults != nil {
		delete(c.cteResults, toLowerFast(entry.materializedViewName))
	}
	if c.tree != nil {
		if err := c.tree.Delete([]byte("mv:" + entry.materializedViewName)); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
			return fmt.Errorf("%s removing materialized view %s: %w", errorPrefix, entry.materializedViewName, err)
		}
	}
	return nil
}

func (c *Catalog) undoDropMaterializedViewEntry(entry undoEntry, errorPrefix string) error {
	if c.materializedViews == nil {
		c.materializedViews = make(map[string]*MaterializedViewDef)
	}
	if c.materializedViewSQL == nil {
		c.materializedViewSQL = make(map[string]string)
	}
	c.materializedViews[entry.materializedViewName] = cloneMaterializedViewDef(entry.materializedViewDef)
	c.materializedViewSQL[entry.materializedViewName] = entry.materializedViewSQL
	if strings.TrimSpace(c.materializedViewSQL[entry.materializedViewName]) == "" && entry.materializedViewDef != nil {
		c.materializedViewSQL[entry.materializedViewName] = createMaterializedViewSQL(entry.materializedViewName, entry.materializedViewDef.Query)
	}
	if c.cteResults != nil {
		delete(c.cteResults, toLowerFast(entry.materializedViewName))
	}
	if c.tree != nil {
		if err := c.storeMaterializedViewDef(
			entry.materializedViewName,
			c.materializedViewSQL[entry.materializedViewName],
			c.materializedViews[entry.materializedViewName],
		); err != nil {
			return fmt.Errorf("%s restoring materialized view %s: %w", errorPrefix, entry.materializedViewName, err)
		}
	}
	return nil
}

func (c *Catalog) undoCreateForeignTableEntry(entry undoEntry, errorPrefix string) error {
	delete(c.foreignTables, entry.foreignTableName)
	if c.tree != nil {
		if err := c.tree.Delete([]byte("ft:" + entry.foreignTableName)); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
			return fmt.Errorf("%s removing foreign table %s: %w", errorPrefix, entry.foreignTableName, err)
		}
	}
	return nil
}

func (c *Catalog) undoDropForeignTableEntry(entry undoEntry, errorPrefix string) error {
	if c.foreignTables == nil {
		c.foreignTables = make(map[string]*ForeignTableDef)
	}
	c.foreignTables[entry.foreignTableName] = cloneForeignTableDef(entry.foreignTableDef)
	if c.tree != nil && entry.foreignTableDef != nil {
		if err := c.storeForeignTableDef(entry.foreignTableDef); err != nil {
			return fmt.Errorf("%s restoring foreign table %s: %w", errorPrefix, entry.foreignTableName, err)
		}
	}
	return nil
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
		undoCreateFTSIndex, undoDropFTSIndex, undoCreateVectorIndex, undoDropVectorIndex,
		undoAlterAddColumn, undoAlterDropColumn, undoAlterRename, undoAlterRenameColumn, undoAlterForeignKeys, undoAlterChecks,
		undoCreateView, undoDropView, undoCreateTrigger, undoDropTrigger,
		undoCreateProcedure, undoDropProcedure,
		undoCreateMaterializedView, undoDropMaterializedView,
		undoCreateForeignTable, undoDropForeignTable,
		undoEnableRLSTable, undoCreateRLSPolicy, undoDropRLSPolicy:
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
		var wk2 txn.WriteKey
		wk2.TreeName = treeName
		wk2.Key = key
		ts.readValues[wk2] = valueData
		// Cache tree reference so CommitTransaction doesn't need c.mu.
		// Caller must hold c.mu for read.
		if ts.treeCache == nil {
			ts.treeCache = make(map[string]btree.TreeStore, 4)
		}
		if ts.treeCache[treeName] == nil {
			ts.treeCache[treeName] = c.tableTrees[treeName]
		}
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

// recordManagerReadTs is the ts-cached variant of recordManagerRead.
func (c *Catalog) recordManagerReadTs(ts *catalogTxnState, treeName string, key string, valueData []byte) {
	if ts == nil {
		return
	}
	if ts.readValues == nil {
		ts.readValues = make(map[txn.WriteKey][]byte)
	}
	var wk2 txn.WriteKey
	wk2.TreeName = treeName
	wk2.Key = key
	ts.readValues[wk2] = valueData
	if ts.treeCache == nil {
		ts.treeCache = make(map[string]btree.TreeStore, 4)
	}
	if ts.treeCache[treeName] == nil {
		ts.treeCache[treeName] = c.tableTrees[treeName]
	}

	if !c.enableBufferedWrites || c.txnManager == nil || ts.managerTxn == nil || !ts.txnActive {
		return
	}
	mt, ok := ts.managerTxn.(*txn.Transaction)
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

// appendPendingWriteTs is the ts-cached variant of appendPendingWrite.
func (c *Catalog) appendPendingWriteTs(ts *catalogTxnState, pw PendingWrite) {
	if ts == nil {
		return
	}
	ts.pendingWrites = append(ts.pendingWrites, pw)
	if len(ts.pendingWrites) > 1 {
		if ts.pendingWriteMap == nil {
			rebuildPendingWriteMap(ts)
		}
		if ts.pendingWriteMap[pw.TreeName] == nil {
			ts.pendingWriteMap[pw.TreeName] = make(map[string]PendingWrite)
		}
		ts.pendingWriteMap[pw.TreeName][pw.Key] = pw
	}
	if ts.treeCache == nil {
		ts.treeCache = make(map[string]btree.TreeStore, 4)
	}
	if ts.treeCache[pw.TreeName] == nil {
		ts.treeCache[pw.TreeName] = c.tableTrees[pw.TreeName]
	}
	for _, idx := range pw.IndexUpdates {
		if ts.treeCache[idx.IndexName] == nil {
			ts.treeCache[idx.IndexName] = c.indexTrees[idx.IndexName]
		}
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
	// A pending soft-delete frees the key for re-insert, so it must not count as
	// a live conflicting pending write.
	pendingDeleted := func(value []byte) bool {
		numCols := 0
		if tbl := c.tables[tableName]; tbl != nil {
			numCols = len(tbl.Columns)
		}
		vrow, err := decodeVersionedRow(value, numCols)
		return err == nil && vrow.Version.DeletedAt > 0
	}
	if m, ok := ts.getPendingWriteMap()[tableName]; ok {
		pw, exists := m[key]
		if !exists {
			return false
		}
		return !pendingDeleted(pw.Value)
	}
	for _, pw := range ts.pendingWrites {
		if pw.TreeName == tableName && pw.Key == key {
			return !pendingDeleted(pw.Value)
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

// indexKeyPendingState reports the net effect of this txn's buffered index
// updates on the given index key: +1 if the last pending op is an insert, -1 if
// it is a delete, 0 if there is no pending op. Walking in append order makes the
// last op win, so a unique-index slot freed by a pending delete can be reused
// while one taken by a pending insert is still rejected (read-your-writes).
func (c *Catalog) indexKeyPendingState(indexName string, key string) int {
	ts := c.getCurrentTxn()
	if ts == nil || len(ts.pendingWrites) == 0 {
		return 0
	}
	state := 0
	for _, pw := range ts.pendingWrites {
		for _, idx := range pw.IndexUpdates {
			if idx.IndexName == indexName && idx.Key == key {
				if idx.IsDelete {
					state = -1
				} else {
					state = 1
				}
			}
		}
	}
	return state
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
		case storage.WALInsert, storage.WALUpdate, storage.WALUpdateCommit:
			key, value, err := parseReplayWALKeyValue(op.Data)
			if err != nil {
				return fmt.Errorf("invalid WAL replay data for txn %d type %v: %w", op.TxnID, op.Type, err)
			}

			tableName, rowKey, err := parseReplayWALKey(key)
			if err != nil {
				return fmt.Errorf("invalid WAL replay key for txn %d: %w", op.TxnID, err)
			}
			tree, exists := c.tableTrees[tableName]
			if !exists {
				continue
			}
			if err := tree.Put([]byte(rowKey), value); err != nil {
				return fmt.Errorf("failed to replay WAL %v for %s: %w", op.Type, key, err)
			}
			affectedTables[tableName] = struct{}{}

		case storage.WALDelete:
			key, _, err := parseReplayWALKeyValue(op.Data)
			if err != nil {
				return fmt.Errorf("invalid WAL replay delete data for txn %d: %w", op.TxnID, err)
			}
			tableName, rowKey, err := parseReplayWALKey(key)
			if err != nil {
				return fmt.Errorf("invalid WAL replay delete key for txn %d: %w", op.TxnID, err)
			}
			tree, exists := c.tableTrees[tableName]
			if !exists {
				continue
			}
			if err := tree.Delete([]byte(rowKey)); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
				return fmt.Errorf("failed to replay WAL delete for %s: %w", key, err)
			}
			affectedTables[tableName] = struct{}{}
		default:
			return fmt.Errorf("invalid WAL replay record type %v for txn %d", op.Type, op.TxnID)
		}
	}

	for tableName := range affectedTables {
		if err := c.rebuildTableIndexesLocked(tableName); err != nil {
			return fmt.Errorf("failed to rebuild indexes for %s: %w", tableName, err)
		}
	}
	return nil
}

func parseReplayWALKeyValue(data []byte) (string, []byte, error) {
	if len(data) < 4 {
		return "", nil, fmt.Errorf("record too short: %d bytes", len(data))
	}
	keyLen := binary.LittleEndian.Uint32(data[0:4])
	keyEnd := uint64(4) + uint64(keyLen)
	if keyEnd > uint64(len(data)) {
		return "", nil, fmt.Errorf("key length %d exceeds record size %d", keyLen, len(data))
	}
	keyEndInt := int(keyEnd)
	return string(data[4:keyEndInt]), data[keyEndInt:], nil
}

const maxCatalogLogicalWALDataBytes = 1<<16 - 1 // storage WAL payload length is uint16

func encodeLogicalWALData(treeName string, key []byte, value []byte) ([]byte, error) {
	if treeName == "" {
		return nil, fmt.Errorf("WAL tree name cannot be empty")
	}
	if len(key) == 0 {
		return nil, fmt.Errorf("WAL row key cannot be empty")
	}
	keyLen := len(treeName) + 1 + len(key)
	if keyLen > maxCatalogLogicalWALDataBytes-4 {
		return nil, fmt.Errorf("WAL record data size exceeds maximum (%d bytes): key length %d",
			maxCatalogLogicalWALDataBytes, keyLen)
	}
	if len(value) > maxCatalogLogicalWALDataBytes-4-keyLen {
		return nil, fmt.Errorf("WAL record data size exceeds maximum (%d bytes): key length %d, value length %d",
			maxCatalogLogicalWALDataBytes, keyLen, len(value))
	}
	data := make([]byte, 4+keyLen+len(value))
	binary.LittleEndian.PutUint32(data[:4], uint32(keyLen)) // #nosec G115 - keyLen range checked above.
	copy(data[4:], treeName)
	data[4+len(treeName)] = ':'
	copy(data[4+len(treeName)+1:], key)
	copy(data[4+keyLen:], value)
	return data, nil
}

func parseReplayWALKey(key string) (string, string, error) {
	// Manager format: "tableName:rowKey".
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("missing table prefix")
	}
	if parts[0] == "" {
		return "", "", fmt.Errorf("empty table name")
	}
	if parts[1] == "" {
		return "", "", fmt.Errorf("empty row key")
	}
	return parts[0], parts[1], nil
}
