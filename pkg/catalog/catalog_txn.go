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
		// Write rollback record to WAL
		record := &storage.WALRecord{
			TxnID: c.txnID,
			Type:  storage.WALRollback,
		}
		if err := c.wal.Append(record); err != nil {
			return err
		}
	}

	// Replay undo log in reverse to restore pre-transaction state
	var rollbackErr error
	for i := len(c.undoLog) - 1; i >= 0; i-- {
		entry := c.undoLog[i]
		tree := c.tableTrees[entry.tableName] // May be nil for DDL undo entries
		switch entry.action {
		case undoInsert:
			// Undo an INSERT: delete the key that was inserted
			if tree != nil {
				if err := tree.Delete(entry.key); err != nil {
					rollbackErr = fmt.Errorf("rollback failed undoing insert: %w", err)
				}
			}
		case undoUpdate:
			// Undo an UPDATE: restore the old value
			if tree != nil {
				if err := tree.Put(entry.key, entry.oldValue); err != nil {
					rollbackErr = fmt.Errorf("rollback failed undoing update: %w", err)
				}
			}
		case undoDelete:
			// Undo a DELETE: restore the deleted key/value
			if tree != nil {
				if err := tree.Put(entry.key, entry.oldValue); err != nil {
					rollbackErr = fmt.Errorf("rollback failed undoing delete: %w", err)
				}
			}
		case undoCreateTable:
			// Undo a CREATE TABLE: remove the table
			delete(c.tables, entry.tableName)
			delete(c.tableTrees, entry.tableName)
			delete(c.stats, entry.tableName)
			// Remove indexes created for this table
			for idxName, idxDef := range c.indexes {
				if idxDef.TableName == entry.tableName {
					delete(c.indexes, idxName)
					delete(c.indexTrees, idxName)
				}
			}
			// Remove from catalog tree
			if c.tree != nil {
				if err := c.tree.Delete([]byte("tbl:" + entry.tableName)); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback failed removing table %s from catalog: %w", entry.tableName, err)
					}
				}
			}
		case undoDropTable:
			// Undo a DROP TABLE: restore the table
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
			// Restore catalog tree entry
			if entry.tableDef != nil {
				if err := c.storeTableDef(entry.tableDef); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback failed restoring table def %s: %w", entry.tableName, err)
					}
				}
			}
		case undoCreateIndex:
			// Undo a CREATE INDEX: drop the index
			delete(c.indexes, entry.indexName)
			delete(c.indexTrees, entry.indexName)
			if c.tree != nil {
				if err := c.tree.Delete([]byte("idx:" + entry.indexName)); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback failed dropping index %s: %w", entry.indexName, err)
					}
				}
			}
		case undoDropIndex:
			// Undo a DROP INDEX: restore the index
			if entry.indexDef != nil {
				c.indexes[entry.indexName] = entry.indexDef
			}
			if entry.indexTree != nil {
				c.indexTrees[entry.indexName] = entry.indexTree
			}
			if entry.indexDef != nil {
				if err := c.storeIndexDef(entry.indexDef); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback failed restoring index def %s: %w", entry.indexName, err)
					}
				}
			}
		case undoAutoIncSeq:
			// Undo AutoIncSeq change: restore old value
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.AutoIncSeq = entry.oldAutoIncSeq
			}
		case undoAlterAddColumn:
			// Undo ALTER TABLE ADD COLUMN: restore original columns
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.Columns = entry.oldColumns
				tbl.buildColumnIndexCache()
				_ = c.storeTableDef(tbl)
			}
		case undoAlterDropColumn:
			// Undo ALTER TABLE DROP COLUMN: restore original columns and row data
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.Columns = entry.oldColumns
				tbl.buildColumnIndexCache()
				// Restore original row data
				if tree, treeExists := c.tableTrees[entry.tableName]; treeExists {
					for _, rd := range entry.oldRowData {
						if err := tree.Put(rd.key, rd.val); err != nil {
							if rollbackErr == nil {
								rollbackErr = fmt.Errorf("rollback failed restoring row data for %s: %w", entry.tableName, err)
							}
						}
					}
				}
				// Restore dropped indexes
				for idxName, idxDef := range entry.droppedIndexes {
					c.indexes[idxName] = idxDef
				}
				for idxName, idxTree := range entry.droppedIdxTrees {
					c.indexTrees[idxName] = idxTree
				}
				if err := c.storeTableDef(tbl); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback failed storing table def %s: %w", entry.tableName, err)
					}
				}
			}
		case undoAlterRename:
			// Undo ALTER TABLE RENAME: swap names back
			if tbl, exists := c.tables[entry.newName]; exists {
				delete(c.tables, entry.newName)
				c.tables[entry.oldName] = tbl
				tbl.Name = entry.oldName // Restore table name
				if tree, treeExists := c.tableTrees[entry.newName]; treeExists {
					delete(c.tableTrees, entry.newName)
					c.tableTrees[entry.oldName] = tree
				}
				// Restore index table references
				for _, idxDef := range c.indexes {
					if idxDef.TableName == entry.newName {
						idxDef.TableName = entry.oldName
					}
				}
				// Restore stats
				if stats, sExists := c.stats[entry.newName]; sExists {
					delete(c.stats, entry.newName)
					c.stats[entry.oldName] = stats
				}
			}
		case undoAlterRenameColumn:
			// Undo ALTER TABLE RENAME COLUMN: restore old column name
			if tbl, exists := c.tables[entry.tableName]; exists {
				for i, col := range tbl.Columns {
					if strings.EqualFold(col.Name, entry.newName) {
						tbl.Columns[i].Name = entry.oldName
						break
					}
				}
				// Restore PK reference if changed
				// Update PK references if needed
				for i, pkCol := range tbl.PrimaryKey {
					if strings.EqualFold(pkCol, entry.newName) {
						tbl.PrimaryKey[i] = entry.oldName
					}
				}
				tbl.buildColumnIndexCache()
				// Restore index column references
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
		}

		// Reverse index changes in reverse order
		for j := len(entry.indexChanges) - 1; j >= 0; j-- {
			idxChange := entry.indexChanges[j]
			idxTree, exists := c.indexTrees[idxChange.indexName]
			if !exists {
				continue
			}
			if idxChange.wasAdded {
				// Index key was added -> undo by deleting it
				if err := idxTree.Delete(idxChange.key); err != nil {
					rollbackErr = fmt.Errorf("rollback failed undoing index add: %w", err)
				}
			} else {
				// Index key was deleted -> undo by restoring it
				if err := idxTree.Put(idxChange.key, idxChange.oldValue); err != nil {
					rollbackErr = fmt.Errorf("rollback failed undoing index delete: %w", err)
				}
			}
		}
	}
	if rollbackErr != nil {
		c.undoLog = nil
		c.txnActive = false
		c.savepoints = nil
		return rollbackErr
	}

	c.undoLog = nil
	c.txnActive = false
	c.savepoints = nil
	return nil
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

	// Replay undo entries from the end back to the savepoint position
	var rollbackErr error
	for i := len(c.undoLog) - 1; i >= undoPos; i-- {
		entry := c.undoLog[i]
		tree := c.tableTrees[entry.tableName]
		switch entry.action {
		case undoInsert:
			if tree != nil {
				if err := tree.Delete(entry.key); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed undoing insert: %w", err)
					}
				}
			}
		case undoUpdate:
			if tree != nil {
				if err := tree.Put(entry.key, entry.oldValue); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed undoing update: %w", err)
					}
				}
			}
		case undoDelete:
			if tree != nil {
				if err := tree.Put(entry.key, entry.oldValue); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed undoing delete: %w", err)
					}
				}
			}
		case undoCreateTable:
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
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed removing table %s from catalog: %w", entry.tableName, err)
					}
				}
			}
		case undoDropTable:
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
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed restoring table def %s: %w", entry.tableName, err)
					}
				}
			}
		case undoCreateIndex:
			delete(c.indexes, entry.indexName)
			delete(c.indexTrees, entry.indexName)
			if c.tree != nil {
				if err := c.tree.Delete([]byte("idx:" + entry.indexName)); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed dropping index %s: %w", entry.indexName, err)
					}
				}
			}
		case undoDropIndex:
			if entry.indexDef != nil {
				c.indexes[entry.indexName] = entry.indexDef
			}
			if entry.indexTree != nil {
				c.indexTrees[entry.indexName] = entry.indexTree
			}
		case undoAutoIncSeq:
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.AutoIncSeq = entry.oldAutoIncSeq
			}
		case undoAlterAddColumn:
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.Columns = entry.oldColumns
				tbl.buildColumnIndexCache()
				if err := c.storeTableDef(tbl); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed storing table def %s: %w", entry.tableName, err)
					}
				}
			}
		case undoAlterDropColumn:
			if tbl, exists := c.tables[entry.tableName]; exists {
				tbl.Columns = entry.oldColumns
				tbl.buildColumnIndexCache()
				if t, e := c.tableTrees[entry.tableName]; e {
					for _, rd := range entry.oldRowData {
						if err := t.Put(rd.key, rd.val); err != nil {
							if rollbackErr == nil {
								rollbackErr = fmt.Errorf("rollback to savepoint failed restoring row data for %s: %w", entry.tableName, err)
							}
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
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed storing table def %s: %w", entry.tableName, err)
					}
				}
			}
		case undoAlterRename:
			// Undo ALTER TABLE RENAME: swap names back
			if tbl, exists := c.tables[entry.newName]; exists {
				delete(c.tables, entry.newName)
				c.tables[entry.oldName] = tbl
				tbl.Name = entry.oldName // Restore table name
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
		case undoAlterRenameColumn:
			// Undo ALTER TABLE RENAME COLUMN: restore old column name
			if tbl, exists := c.tables[entry.tableName]; exists {
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
		}

		// Reverse index changes
		for j := len(entry.indexChanges) - 1; j >= 0; j-- {
			idxChange := entry.indexChanges[j]
			idxTree, exists := c.indexTrees[idxChange.indexName]
			if !exists {
				continue
			}
			if idxChange.wasAdded {
				if err := idxTree.Delete(idxChange.key); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed deleting from index %s: %w", idxChange.indexName, err)
					}
				}
			} else {
				if err := idxTree.Put(idxChange.key, idxChange.oldValue); err != nil {
					if rollbackErr == nil {
						rollbackErr = fmt.Errorf("rollback to savepoint failed putting to index %s: %w", idxChange.indexName, err)
					}
				}
			}
		}
	}

	// Truncate undo log to savepoint position
	c.undoLog = c.undoLog[:undoPos]
	// Remove savepoints after this one (but keep the current savepoint)
	c.savepoints = c.savepoints[:spIdx+1]
	return rollbackErr
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
