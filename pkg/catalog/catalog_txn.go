package catalog

import (
	"errors"
	"fmt"

	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
	"strings"
	"time"
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

	if !c.enableRLS {
		return errors.New("row-level security is not enabled")
	}

	return c.rlsManager.CreatePolicy(policy)
}

func (c *Catalog) DropRLSPolicy(tableName, policyName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.enableRLS {
		return errors.New("row-level security is not enabled")
	}

	return c.rlsManager.DropPolicy(tableName, policyName)
}

// getCurrentTxn returns the active transaction state for the current transaction.
// If no current transaction is set, it falls back to the legacy single-transaction fields.
func (c *Catalog) getCurrentTxn() *catalogTxnState {
	if c.currentTxnID != 0 {
		if ts, ok := c.activeTxns[c.currentTxnID]; ok {
			return ts
		}
	}
	return nil
}

// getCurrentTxnUndoLog returns the undo log for the current transaction.
func (c *Catalog) getCurrentTxnUndoLog() []undoEntry {
	if ts := c.getCurrentTxn(); ts != nil {
		return ts.undoLog
	}
	return c.undoLog
}

// getCurrentTxnSavepoints returns the savepoints for the current transaction.
func (c *Catalog) getCurrentTxnSavepoints() []savepointEntry {
	if ts := c.getCurrentTxn(); ts != nil {
		return ts.savepoints
	}
	return c.savepoints
}

// isCurrentTxnActive returns true if the current transaction is active.
func (c *Catalog) isCurrentTxnActive() bool {
	if ts := c.getCurrentTxn(); ts != nil {
		return ts.txnActive
	}
	return c.txnActive
}

// getCurrentTxnID returns the ID of the current transaction.
func (c *Catalog) getCurrentTxnID() uint64 {
	if ts := c.getCurrentTxn(); ts != nil {
		return ts.txnID
	}
	return c.txnID
}

// appendUndoEntry appends an undo entry to the current transaction's undo log.
func (c *Catalog) appendUndoEntry(entry undoEntry) {
	if ts := c.getCurrentTxn(); ts != nil {
		ts.undoLog = append(ts.undoLog, entry)
		return
	}
	c.undoLog = append(c.undoLog, entry)
}

// truncateUndoLog truncates the current transaction's undo log to the given length.
func (c *Catalog) truncateUndoLog(length int) {
	if ts := c.getCurrentTxn(); ts != nil {
		ts.undoLog = ts.undoLog[:length]
		return
	}
	c.undoLog = c.undoLog[:length]
}

// setCurrentTxnSavepoints sets the savepoints for the current transaction.
func (c *Catalog) setCurrentTxnSavepoints(sps []savepointEntry) {
	if ts := c.getCurrentTxn(); ts != nil {
		ts.savepoints = sps
		return
	}
	c.savepoints = sps
}

func (c *Catalog) BeginTransaction(txnID uint64) {
	// Create the Manager transaction outside of c.mu so we can decide
	// whether to acquire bufferedTxnMu BEFORE c.mu. This ordering is
	// required to avoid deadlock with CommitTransaction/RollbackTransaction
	// which release bufferedTxnMu while holding c.mu.
	var managerTxn interface{}
	if mgr, ok := c.txnManager.(*txn.Manager); ok && mgr != nil {
		managerTxn = mgr.Begin(txn.DefaultOptions())
	}
	willBuffer := c.enableBufferedWrites && managerTxn != nil
	if willBuffer {
		// If the current transaction already holds bufferedTxnMu, allow
		// nested begin without reacquiring (prevents self-deadlock).
		c.mu.Lock()
		alreadyHolds := c.bufferedTxnOwner != 0 && c.bufferedTxnOwner == c.currentTxnID
		c.mu.Unlock()
		if !alreadyHolds {
			c.bufferedTxnMu.Lock()
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.txnID = txnID
	c.txnActive = true
	c.undoLog = nil    // Clear undo log for new transaction
	c.savepoints = nil // Clear savepoints
	c.currentTxnID = txnID
	if c.activeTxns == nil {
		c.activeTxns = make(map[uint64]*catalogTxnState)
	}
	cs := &catalogTxnState{
		txnID:     txnID,
		txnActive: true,
	}
	cs.managerTxn = managerTxn
	// Serialize buffered write transactions to prevent lost updates.
	// Reads can still proceed via c.mu.RLock, but only one buffered write
	// transaction runs at a time, preserving single-writer semantics.
	if willBuffer {
		if c.bufferedTxnOwner == 0 {
			c.bufferedTxnOwner = txnID
			cs.holdsBufferedLock = true
		}
	}
	c.activeTxns[txnID] = cs
}

func (c *Catalog) CommitTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// When a txn.Manager bridge is active, commit through the Manager first.
	// This performs conflict detection and updates the version store.
	if ts := c.getCurrentTxn(); ts != nil {
		if mt, ok := ts.managerTxn.(*txn.Transaction); ok && mt != nil {
			if err := mt.Commit(); err != nil {
				return fmt.Errorf("txn manager commit: %w", err)
			}
			// Apply any buffered writes to B-trees after Manager commit succeeds.
			if err := c.applyPendingWritesLocked(ts); err != nil {
				return fmt.Errorf("apply buffered writes: %w", err)
			}
		}
	}

	if c.wal != nil && c.txnActive {
		// Write commit record to WAL
		record := &storage.WALRecord{
			TxnID: c.txnID,
			Type:  storage.WALCommit,
		}
		if err := c.wal.Append(record); err != nil {
			return err
		}
	}
	c.txnActive = false
	c.undoLog = nil    // Discard undo log on successful commit
	c.savepoints = nil // Clear savepoints
	if c.currentTxnID != 0 {
		if ts := c.activeTxns[c.currentTxnID]; ts != nil && ts.holdsBufferedLock {
			c.bufferedTxnOwner = 0
			c.bufferedTxnMu.Unlock()
		}
		delete(c.activeTxns, c.currentTxnID)
		c.currentTxnID = 0
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
	for tableName, tree := range c.tableTrees {
		if err := tree.Flush(); err != nil {
			return fmt.Errorf("failed to flush table %s: %w", tableName, err)
		}
	}
	return nil
}

func (c *Catalog) RollbackTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Rollback the Manager transaction first if present.
	if ts := c.getCurrentTxn(); ts != nil {
		if mt, ok := ts.managerTxn.(*txn.Transaction); ok && mt != nil {
			_ = mt.Rollback() // best-effort; continue with catalog rollback
		}
		// Discard buffered writes.
		ts.pendingWrites = nil
	}

	if c.wal != nil && c.txnActive {
		record := &storage.WALRecord{
			TxnID: c.txnID,
			Type:  storage.WALRollback,
		}
		if err := c.wal.Append(record); err != nil {
			return err
		}
	}

	undoLog := c.getCurrentTxnUndoLog()
	if err := c.replayUndoLog(len(undoLog)-1, 0, "rollback"); err != nil {
		c.txnActive = false
		c.undoLog = nil
		c.savepoints = nil
		if c.currentTxnID != 0 {
			if ts := c.activeTxns[c.currentTxnID]; ts != nil && ts.holdsBufferedLock {
				c.bufferedTxnMu.Unlock()
			}
			delete(c.activeTxns, c.currentTxnID)
			c.currentTxnID = 0
		}
		return err
	}

	c.txnActive = false
	c.undoLog = nil
	c.savepoints = nil
	if c.currentTxnID != 0 {
		if ts := c.activeTxns[c.currentTxnID]; ts != nil && ts.holdsBufferedLock {
			c.bufferedTxnOwner = 0
			c.bufferedTxnMu.Unlock()
		}
		delete(c.activeTxns, c.currentTxnID)
		c.currentTxnID = 0
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
			tbl.AutoIncSeq = entry.oldAutoIncSeq
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


func (c *Catalog) IsTransactionActive() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isCurrentTxnActive()
}

func (c *Catalog) Savepoint(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()
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

	if err := c.replayUndoLog(len(c.getCurrentTxnUndoLog())-1, undoPos, "rollback to savepoint"); err != nil {
		c.truncateUndoLog(undoPos)
		if ts := c.getCurrentTxn(); ts != nil {
			ts.pendingWrites = ts.pendingWrites[:pwPos]
		}
		c.setCurrentTxnSavepoints(sps[:spIdx+1])
		return err
	}

	// Truncate undo log to savepoint position
	c.truncateUndoLog(undoPos)
	// Truncate pending writes to savepoint position (buffered mode)
	if ts := c.getCurrentTxn(); ts != nil {
		ts.pendingWrites = ts.pendingWrites[:pwPos]
	}
	// Remove savepoints after this one (but keep the current savepoint)
	c.setCurrentTxnSavepoints(sps[:spIdx+1])
	return nil
}

func (c *Catalog) ReleaseSavepoint(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.isCurrentTxnActive() {
		return fmt.Errorf("RELEASE SAVEPOINT can only be used within a transaction")
	}

	// Find and remove the savepoint
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

// isBufferedMode returns true when the current transaction should buffer
// writes instead of mutating B-trees directly.
func (c *Catalog) isBufferedMode() bool {
	return c.enableBufferedWrites && c.hasTxnManager() && c.getCurrentManagerTxn() != nil && c.isCurrentTxnActive()
}

// EnableBufferedWrites turns on the buffered DML path for MVCC multi-writer.
// This is opt-in until read-your-writes support is fully implemented.
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
	}
}

// clearPendingWrites discards all buffered writes for the current transaction.
func (c *Catalog) clearPendingWrites() {
	if ts := c.getCurrentTxn(); ts != nil {
		ts.pendingWrites = nil
	}
}

// applyPendingWritesLocked applies buffered DML writes to B-trees at commit time.
// Must be called with c.mu held.
func (c *Catalog) applyPendingWritesLocked(ts *catalogTxnState) error {
	for _, pw := range ts.pendingWrites {
		tree, exists := c.tableTrees[pw.TreeName]
		if !exists {
			return fmt.Errorf("partition tree %s not found", pw.TreeName)
		}
		if err := tree.Put(pw.Key, pw.Value); err != nil {
			return fmt.Errorf("failed to apply buffered write to %s: %w", pw.TreeName, err)
		}
		for _, idx := range pw.IndexUpdates {
			idxTree, exists := c.indexTrees[idx.IndexName]
			if !exists {
				continue
			}
			if idx.IsDelete {
				_ = idxTree.Delete(idx.Key)
			} else {
				_ = idxTree.Put(idx.Key, idx.Value)
			}
		}
	}
	ts.pendingWrites = nil
	return nil
}

// keyInPendingWrites reports whether the given table/key already exists in the
// current transaction's pending write buffer.
func (c *Catalog) keyInPendingWrites(tableName string, key []byte) bool {
	if ts := c.getCurrentTxn(); ts != nil {
		for _, pw := range ts.pendingWrites {
			if pw.TreeName == tableName && string(pw.Key) == string(key) {
				return true
			}
		}
	}
	return false
}

// indexKeyInPendingWrites reports whether the given index key already exists in
// the current transaction's pending write buffer (for unique constraint checks).
func (c *Catalog) indexKeyInPendingWrites(indexName string, key []byte) bool {
	if ts := c.getCurrentTxn(); ts != nil {
		for _, pw := range ts.pendingWrites {
			for _, idx := range pw.IndexUpdates {
				if idx.IndexName == indexName && string(idx.Key) == string(key) && !idx.IsDelete {
					return true
				}
			}
		}
	}
	return false
}
