package catalog

import (
	"errors"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
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

func (c *Catalog) BeginTransaction(txnID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txnID = txnID
	c.txnActive = true
	c.undoLog = nil    // Clear undo log for new transaction
	c.savepoints = nil // Clear savepoints
}

func (c *Catalog) CommitTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	if c.wal != nil && c.txnActive {
		record := &storage.WALRecord{
			TxnID: c.txnID,
			Type:  storage.WALRollback,
		}
		if err := c.wal.Append(record); err != nil {
			return err
		}
	}

	if err := c.replayUndoLog(len(c.undoLog)-1, 0, "rollback"); err != nil {
		c.undoLog = nil
		c.txnActive = false
		c.savepoints = nil
		return err
	}

	c.undoLog = nil
	c.txnActive = false
	c.savepoints = nil
	return nil
}


// replayUndoLog replays undo log entries in reverse from startIdx down to endIdx (inclusive).
func (c *Catalog) replayUndoLog(startIdx, endIdx int, errorPrefix string) error {
	if startIdx < 0 || len(c.undoLog) == 0 {
		return nil
	}
	var rollbackErr error
	for i := startIdx; i >= endIdx; i-- {
		entry := c.undoLog[i]
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
	return c.txnActive
}

func (c *Catalog) Savepoint(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.txnActive {
		return fmt.Errorf("SAVEPOINT can only be used within a transaction")
	}
	c.savepoints = append(c.savepoints, savepointEntry{
		name:    name,
		undoPos: len(c.undoLog),
	})
	return nil
}

func (c *Catalog) RollbackToSavepoint(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.txnActive {
		return fmt.Errorf("ROLLBACK TO SAVEPOINT can only be used within a transaction")
	}

	// Find the savepoint
	spIdx := -1
	for i := len(c.savepoints) - 1; i >= 0; i-- {
		if strings.EqualFold(c.savepoints[i].name, name) {
			spIdx = i
			break
		}
	}
	if spIdx < 0 {
		return fmt.Errorf("savepoint '%s' does not exist", name)
	}

	undoPos := c.savepoints[spIdx].undoPos

	if err := c.replayUndoLog(len(c.undoLog)-1, undoPos, "rollback to savepoint"); err != nil {
		c.undoLog = c.undoLog[:undoPos]
		c.savepoints = c.savepoints[:spIdx+1]
		return err
	}

	// Truncate undo log to savepoint position
	c.undoLog = c.undoLog[:undoPos]
	// Remove savepoints after this one (but keep the current savepoint)
	c.savepoints = c.savepoints[:spIdx+1]
	return nil
}

func (c *Catalog) ReleaseSavepoint(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.txnActive {
		return fmt.Errorf("RELEASE SAVEPOINT can only be used within a transaction")
	}

	// Find and remove the savepoint
	for i := len(c.savepoints) - 1; i >= 0; i-- {
		if strings.EqualFold(c.savepoints[i].name, name) {
			// Remove this savepoint and all savepoints created after it
			c.savepoints = c.savepoints[:i]
			return nil
		}
	}
	return fmt.Errorf("savepoint '%s' does not exist", name)
}

func (c *Catalog) TxnID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.txnID
}
