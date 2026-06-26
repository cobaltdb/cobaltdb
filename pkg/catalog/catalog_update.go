package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

// updateEntry tracks a row to be updated
type updateEntry struct {
	key      []byte
	oldRow   []interface{}
	newRow   []interface{}
	treeName string // which partition tree this entry came from
}

// updateSnapshot holds all Catalog metadata needed for the buffered UPDATE scan.
// It is built under a brief Catalog.mu RLock and then used without the lock.
type updateSnapshot struct {
	table       *TableDef
	trees       []btree.TreeStore
	treeNames   []string
	indexedRows []string
	useIndex    bool
	indexes     []indexSnapshot
	fkRefs      map[string]fkSnapshot
	triggers    []*query.CreateTriggerStmt
}

func (c *Catalog) Update(ctx context.Context, stmt *query.UpdateStmt, args []interface{}) (int64, int64, error) {
	// Fast path: resolve table metadata from schema cache without lock.
	table, ver, cacheHit := c.getCachedTable(stmt.Table)
	if !cacheHit {
		c.mu.RLock()
		var err error
		table, err = c.getTableLocked(stmt.Table)
		if err != nil && err != ErrTableNotFound {
			c.mu.RUnlock()
			return 0, 0, err
		}
		if table != nil {
			ver = c.schemaVersion.Load()
			c.putCachedTable(stmt.Table, table)
		}
		c.mu.RUnlock()
	}

	c.mu.RLock()
	if table != nil && c.schemaVersion.Load() != ver {
		var err error
		table, err = c.getTableLocked(stmt.Table)
		if err != nil && err != ErrTableNotFound {
			c.mu.RUnlock()
			return 0, 0, err
		}
	}

	// Check for INSTEAD OF UPDATE trigger first (for views)
	if trig := c.findInsteadOfTrigger(stmt.Table, "UPDATE"); trig != nil {
		defer c.mu.RUnlock()
		return c.executeInsteadOfUpdateTrigger(ctx, trig, stmt, args)
	}

	if table == nil {
		var err error
		table, err = c.getTableLocked(stmt.Table)
		if err != nil {
			c.mu.RUnlock()
			return 0, 0, err
		}
	}
	if table.Type == "foreign" {
		c.mu.RUnlock()
		return 0, 0, fmt.Errorf("cannot update foreign table '%s'", stmt.Table)
	}

	// Handle UPDATE with JOIN or target alias. The join path models the target
	// table as a SELECT source, which lets qualified alias references resolve.
	if stmt.Alias != "" || stmt.From != nil || len(stmt.Joins) > 0 {
		defer c.mu.RUnlock()
		return c.updateWithJoinLocked(ctx, stmt, args)
	}

	useBuffer := c.isBufferedMode() && table.Partition == nil
	if useBuffer {
		if c.hasVectorIndexForTableLocked(stmt.Table) {
			useBuffer = false
		}
		for _, setClause := range stmt.Set {
			if table.isPrimaryKeyColumn(setClause.Column) {
				useBuffer = false
				break
			}
		}
	}
	ts := c.getCurrentTxn()

	if !useBuffer {
		defer c.mu.RUnlock()
		return c.updateLocked(ctx, stmt, args, table)
	}

	// Buffered path: snapshot and release lock for scan.
	var snap updateSnapshot
	if err := c.buildUpdateSnapshot(&snap, table, stmt, args); err != nil {
		c.mu.RUnlock()
		return 0, 0, err
	}
	c.mu.RUnlock()

	entries, rowsAffected, err := c.scanUpdateEntries(ctx, stmt, args, &snap, ts)
	if err != nil {
		return 0, rowsAffected, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	pendingWriteStartPos := 0
	if ts != nil {
		pendingWriteStartPos = len(ts.pendingWrites)
	}
	for _, entry := range entries {
		if trigErr := c.executeTriggersList(ctx, snap.triggers, "UPDATE", "BEFORE", entry.newRow, entry.oldRow, table.Columns); trigErr != nil {
			if ts != nil {
				ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
				rebuildPendingWriteMap(ts)
			}
			return 0, rowsAffected, fmt.Errorf("BEFORE UPDATE trigger failed: %w", trigErr)
		}
	}

	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, entry := range entries {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, entry.newRow, table, args)
			if err != nil {
				if ts != nil {
					ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
					rebuildPendingWriteMap(ts)
				}
				return 0, rowsAffected, fmt.Errorf("RETURNING clause failed: %w", err)
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}

	if err := c.bufferUpdateEntries(table, stmt, entries, ts); err != nil {
		if ts != nil {
			ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
			rebuildPendingWriteMap(ts)
		}
		return 0, rowsAffected, err
	}
	// Execute AFTER UPDATE triggers (per-row)
	for _, entry := range entries {
		if trigErr := c.executeTriggersList(ctx, snap.triggers, "UPDATE", "AFTER", entry.newRow, entry.oldRow, table.Columns); trigErr != nil {
			if ts != nil {
				ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
				rebuildPendingWriteMap(ts)
			}
			return 0, rowsAffected, fmt.Errorf("AFTER UPDATE trigger failed: %w", trigErr)
		}
	}

	c.invalidateQueryCache(stmt.Table)

	c.setLastReturning(returningRows, returningCols)

	return 0, rowsAffected, nil
}

func (c *Catalog) buildUpdateSnapshot(snap *updateSnapshot, table *TableDef, stmt *query.UpdateStmt, args []interface{}) error {
	snap.table = table
	trees, err := c.getTableTreesForScan(table)
	if err != nil {
		return err
	}
	snap.trees = trees
	snap.treeNames = append(snap.treeNames[:0], stmt.Table)
	if table.Partition != nil {
		snap.treeNames = append(snap.treeNames[:0], table.getPartitionTreeNames()...)
	}
	if stmt.Where != nil {
		indexedRows, useIndex, err := c.useIndexForQueryWithArgs(stmt.Table, stmt.Where, args)
		if err != nil {
			return err
		}
		snap.indexedRows, snap.useIndex = indexedRows, useIndex
	} else {
		snap.indexedRows = snap.indexedRows[:0]
		snap.useIndex = false
	}
	snap.indexes = snap.indexes[:0]
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName == table.Name {
			snap.indexes = append(snap.indexes, indexSnapshot{
				name: idxName,
				def:  idxDef,
				tree: c.indexTrees[idxName],
			})
		}
	}
	if len(table.ForeignKeys) > 0 {
		if snap.fkRefs == nil {
			snap.fkRefs = make(map[string]fkSnapshot, len(table.ForeignKeys))
		} else {
			clear(snap.fkRefs)
		}
		for _, fk := range table.ForeignKeys {
			if _, ok := snap.fkRefs[fk.ReferencedTable]; ok {
				continue
			}
			refTable, err := c.getTableLocked(fk.ReferencedTable)
			if err != nil {
				continue
			}
			refTree, exists := c.tableTrees[fk.ReferencedTable]
			if !exists {
				continue
			}
			snap.fkRefs[fk.ReferencedTable] = fkSnapshot{table: refTable, tree: refTree}
		}
	} else if snap.fkRefs != nil {
		clear(snap.fkRefs)
	}
	snap.triggers = c.getTriggersForTableLocked(table.Name, "UPDATE")
	return nil
}

func (c *Catalog) processUpdateRowDataSnapshot(ctx context.Context, table *TableDef, tree btree.TreeStore, treeName string, key []byte, row []interface{},
	stmt *query.UpdateStmt, args []interface{}, setColumnIndices []int, entries *[]updateEntry, rowsAffected *int64, snap *updateSnapshot, ts *catalogTxnState) error {

	// Apply Row-Level Security check for UPDATE
	if allowed, rlsErr := c.checkRowAccessLocked(ctx, stmt.Table, table.Columns, row, security.PolicyUpdate); rlsErr != nil || !allowed {
		return nil
	}

	// Make a copy of the row to update
	updatedRow := make([]interface{}, len(row))
	copy(updatedRow, row)

	// Update fields - use pre-calculated column indices
	for i, setClause := range stmt.Set {
		colIdx := setColumnIndices[i]
		if colIdx >= 0 {
			newVal, err := evaluateExpression(c, row, table.Columns, setClause.Value, args)
			if err != nil {
				return fmt.Errorf("failed to evaluate SET expression for column '%s': %w", setClause.Column, err)
			}
			updatedRow[colIdx] = newVal
		}
	}
	if allowed, rlsErr := c.checkRowCheckLocked(ctx, stmt.Table, table.Columns, updatedRow, security.PolicyUpdate); rlsErr != nil {
		return fmt.Errorf("RLS WITH CHECK failed for UPDATE: %w", rlsErr)
	} else if !allowed {
		return nil
	}

	// Check all constraints before applying the update. *entries holds the rows
	// already staged by this statement so within-statement unique collisions are caught.
	if err := c.checkConstraintsForUpdate(table, tree, key, row, updatedRow, snap, ts, args, treeName, *entries); err != nil {
		return err
	}

	*entries = append(*entries, updateEntry{
		key:      key,
		oldRow:   row,
		newRow:   updatedRow,
		treeName: treeName,
	})
	*rowsAffected++
	return nil
}

// hasUpdateUniqueConflict reports whether assigning newVal to the unique column
// at colIdx for the row keyed selfKey would duplicate the value held by another
// live row. It honors read-your-writes so the UPDATE statement and transaction
// see a consistent view:
//
//   - collected: rows already updated earlier in this same statement. Their new
//     values are the freshest and override everything else.
//   - pendingKeys: buffered writes from earlier statements in the same txn. They
//     override the committed tree.
//   - the committed tree, skipping any key superseded above (and skipping selfKey).
//
// This catches duplicates introduced purely among rows mutated in one statement
// or one transaction — which a committed-tree-only scan misses, silently
// breaking the UNIQUE invariant.
func (c *Catalog) hasUpdateUniqueConflict(tree btree.TreeStore, table *TableDef, colIdx int,
	newVal interface{}, selfKey string, pendingKeys map[string]PendingWrite, collected []updateEntry) (bool, error) {

	numCols := len(table.Columns)
	overridden := make(map[string]struct{}, len(collected))
	for i := range collected {
		ek := string(collected[i].key)
		overridden[ek] = struct{}{}
		if ek == selfKey {
			continue
		}
		nr := collected[i].newRow
		if colIdx < len(nr) && nr[colIdx] != nil && compareValues(newVal, nr[colIdx]) == 0 {
			return true, nil
		}
	}
	for k, pw := range pendingKeys {
		if k == selfKey {
			continue
		}
		if _, ok := overridden[k]; ok {
			continue
		}
		vrow, live, err := decodeLiveRow(pw.Value, numCols)
		if err != nil {
			return false, fmt.Errorf("failed to decode pending row during UNIQUE check on table %s: %w", table.Name, err)
		}
		if !live {
			continue
		}
		if colIdx < len(vrow) && vrow[colIdx] != nil && compareValues(newVal, vrow[colIdx]) == 0 {
			return true, nil
		}
	}
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return false, fmt.Errorf("failed to scan table for UNIQUE check: %w", err)
	}
	defer iter.Close()
	for iter.HasNext() {
		k, existingData, err := iter.Next()
		if err != nil {
			return false, fmt.Errorf("failed to read row during UNIQUE check on table %s: %w", table.Name, err)
		}
		ks := string(k)
		if ks == selfKey {
			continue
		}
		if _, ok := overridden[ks]; ok {
			continue
		}
		if _, ok := pendingKeys[ks]; ok {
			continue
		}
		vrow, live, err := decodeLiveRow(existingData, numCols)
		if err != nil {
			return false, fmt.Errorf("failed to decode row during UNIQUE check on table %s: %w", table.Name, err)
		}
		if !live {
			continue
		}
		if colIdx < len(vrow) && vrow[colIdx] != nil && compareValues(newVal, vrow[colIdx]) == 0 {
			return true, nil
		}
	}
	return false, nil
}

// checkConstraintsForUpdate validates UNIQUE, NOT NULL, CHECK, and FK constraints
// for a row update. Returns nil on success, or a descriptive error on constraint failure.
// collected holds the rows already staged by this UPDATE statement so that two rows
// driven to the same unique value within one statement (or txn) are rejected.
func (c *Catalog) checkConstraintsForUpdate(table *TableDef, tree btree.TreeStore, key []byte, oldRow, newRow []interface{}, snap *updateSnapshot, ts *catalogTxnState, args []interface{}, treeName string, collected []updateEntry) error {
	applySelfReferentialUpdateCascades(table, oldRow, newRow)

	// Check UNIQUE constraints on table columns
	var pendingKeys map[string]PendingWrite
	if ts != nil {
		pendingKeys = ts.getPendingWriteMap()[treeName]
	}
	for i, col := range table.Columns {
		if col.Unique && newRow[i] != nil {
			conflict, err := c.hasUpdateUniqueConflict(tree, table, i, newRow[i], string(key), pendingKeys, collected)
			if err != nil {
				return err
			}
			if conflict {
				return fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
			}
		}
	}

	// Check UNIQUE INDEX constraints using the snapshot index list
	for _, idx := range snap.indexes {
		if !idx.def.Unique || len(idx.def.Columns) == 0 {
			continue
		}
		newIdxKey, newOk := buildCompositeIndexKey(table, idx.def, newRow)
		if !newOk {
			continue
		}
		oldIdxKey, _ := buildCompositeIndexKey(table, idx.def, oldRow)
		if newIdxKey == oldIdxKey {
			continue
		}
		if idx.tree != nil {
			if _, err := idx.tree.Get([]byte(newIdxKey)); err == nil {
				return fmt.Errorf("UNIQUE constraint failed: duplicate value in index %s", idx.name)
			}
		}
	}

	// Check NOT NULL + CHECK via the shared non-FK validator. The FK check
	// below uses snap.fkRefs (snapshot-aware, lock-free); the shared
	// validateRowAgainstConstraints uses the lock-based live FK table, so
	// we can't reuse it here.
	if err := c.validateRowNonFKConstraints(table, newRow, args); err != nil {
		return err
	}

	// Check FOREIGN KEY constraints using the snapshot FK references
	for _, fk := range table.ForeignKeys {
		if !foreignKeyColumnsChanged(table, fk, oldRow, newRow) {
			continue
		}
		fkValues, skip := foreignKeyValuesForRow(table, fk, newRow)
		if skip {
			continue
		}
		refSnap, ok := snap.fkRefs[fk.ReferencedTable]
		if !ok || refSnap.tree == nil {
			return fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
		}
		var pendingParents map[string]PendingWrite
		if ts != nil {
			pendingParents = ts.getPendingWriteMap()[fk.ReferencedTable]
		}
		refColumns := referencedColumnsForTable(refSnap.table, fk)
		found := selfUpdatedRowSatisfiesForeignKey(table, fk, newRow, fkValues)
		if !found {
			var err error
			found, err = referencedRowExistsSnapshot(refSnap.table, refSnap.tree, pendingParents, refColumns, fkValues)
			if err != nil {
				return fmt.Errorf("FOREIGN KEY constraint failed: failed to scan referenced table %s: %w", fk.ReferencedTable, err)
			}
		}
		if !found {
			return fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValues, fk.ReferencedTable)
		}
	}

	return nil
}

func (c *Catalog) processUpdateRowSnapshot(ctx context.Context, table *TableDef, tree btree.TreeStore, treeName string, key []byte, valueData []byte,
	stmt *query.UpdateStmt, args []interface{}, setColumnIndices []int, entries *[]updateEntry, rowsAffected *int64, snap *updateSnapshot, ts *catalogTxnState) error {

	row, live, err := decodeLiveRow(valueData, len(table.Columns))
	if err != nil {
		return fmt.Errorf("update: failed to decode row in table %s: %w", table.Name, err)
	}
	if !live {
		return nil
	}
	if stmt.Where != nil {
		matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
		if err != nil {
			return fmt.Errorf("WHERE evaluation error: %w", err)
		}
		if !matched {
			return nil
		}
	}
	return c.processUpdateRowDataSnapshot(ctx, table, tree, treeName, key, row, stmt, args, setColumnIndices, entries, rowsAffected, snap, ts)
}

func (c *Catalog) scanUpdateEntries(ctx context.Context, stmt *query.UpdateStmt, args []interface{}, snap *updateSnapshot, ts *catalogTxnState) ([]updateEntry, int64, error) {
	table := snap.table
	trees := snap.trees
	treeNames := snap.treeNames
	indexedRows := snap.indexedRows
	useIndex := snap.useIndex
	useBuffer := c.isBufferedMode() && table.Partition == nil

	// Pre-calculate column indices for SET clauses
	setColumnIndices := make([]int, len(stmt.Set))
	for i, setClause := range stmt.Set {
		setColumnIndices[i] = table.GetColumnIndex(setClause.Column)
		if setColumnIndices[i] < 0 {
			return nil, 0, fmt.Errorf("column '%s' not found in table '%s'", setClause.Column, stmt.Table)
		}
	}

	var entries []updateEntry
	rowsAffected := int64(0)

	for treeIdx, tree := range trees {
		treeName := treeNames[treeIdx]

		var pendingKeys map[string]PendingWrite
		if ts != nil {
			pendingKeys = ts.getPendingWriteMap()[treeName]
		}

		if useIndex {
			for _, pkStr := range indexedRows {
				key := []byte(pkStr)
				valueData, err := tree.Get(key)
				found := err == nil && valueData != nil
				if pwValue, ok := pendingKeys[pkStr]; ok {
					valueData = pwValue.Value
					found = true
				} else if found && useBuffer {
					c.recordManagerRead(treeName, pkStr, valueData)
				}
				if !found {
					continue
				}
				if err := c.processUpdateRowSnapshot(ctx, table, tree, treeName, key, valueData, stmt, args, setColumnIndices, &entries, &rowsAffected, snap, ts); err != nil {
					return entries, rowsAffected, err
				}
			}
			continue
		}

		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return entries, 0, fmt.Errorf("failed to scan table for UPDATE: %w", err)
		}
		seenPending := make(map[string]bool)

		for iter.HasNext() {
			key, valueData, err := iter.Next()
			if err != nil {
				iter.Close()
				return entries, rowsAffected, fmt.Errorf("failed to read table for UPDATE: %w", err)
			}
			k := string(key)
			fromPending := false
			if pwValue, ok := pendingKeys[k]; ok {
				valueData = pwValue.Value
				seenPending[k] = true
				fromPending = true
			}

			row, live, err := decodeLiveRow(valueData, len(table.Columns))
			if err != nil {
				iter.Close()
				return entries, rowsAffected, fmt.Errorf("update: failed to decode row in table %s: %w", table.Name, err)
			}
			if !live {
				continue
			}

			if stmt.Where != nil {
				matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
				if err != nil {
					return entries, rowsAffected, fmt.Errorf("WHERE evaluation error: %w", err)
				}
				if !matched {
					continue
				}
			}

			if !fromPending && useBuffer {
				c.recordManagerRead(treeName, string(key), valueData)
			}

			if err := c.processUpdateRowDataSnapshot(ctx, table, tree, treeName, key, row, stmt, args, setColumnIndices, &entries, &rowsAffected, snap, ts); err != nil {
				return entries, rowsAffected, err
			}
		}
		iter.Close()

		var pendingKeyList []string
		for k := range pendingKeys {
			if !seenPending[k] {
				pendingKeyList = append(pendingKeyList, k)
			}
		}
		sort.Strings(pendingKeyList)
		if pendingKeys != nil {
			for _, k := range pendingKeyList {
				key := []byte(k)
				valueData := pendingKeys[k].Value
				row, live, err := decodeLiveRow(valueData, len(table.Columns))
				if err != nil {
					return entries, rowsAffected, fmt.Errorf("update: failed to decode pending row in table %s: %w", table.Name, err)
				}
				if !live {
					continue
				}
				if stmt.Where != nil {
					matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
					if err != nil || !matched {
						continue
					}
				}
				if err := c.processUpdateRowDataSnapshot(ctx, table, tree, treeName, key, row, stmt, args, setColumnIndices, &entries, &rowsAffected, snap, ts); err != nil {
					return entries, rowsAffected, err
				}
			}
		}
	}

	return entries, rowsAffected, nil
}

func (c *Catalog) updateLocked(ctx context.Context, stmt *query.UpdateStmt, args []interface{}, tableArg ...*TableDef) (int64, int64, error) {
	// Check for INSTEAD OF UPDATE trigger first (for views)
	if trig := c.findInsteadOfTrigger(stmt.Table, "UPDATE"); trig != nil {
		return c.executeInsteadOfUpdateTrigger(ctx, trig, stmt, args)
	}

	var table *TableDef
	if len(tableArg) > 0 && tableArg[0] != nil {
		table = tableArg[0]
	} else {
		var err error
		table, err = c.getTableLocked(stmt.Table)
		if err != nil {
			return 0, 0, err
		}
	}
	if table.Type == "foreign" {
		return 0, 0, fmt.Errorf("cannot update foreign table '%s'", stmt.Table)
	}

	// Handle UPDATE with JOIN or target alias. The join path models the target
	// table as a SELECT source, which lets qualified alias references resolve.
	if stmt.Alias != "" || stmt.From != nil || len(stmt.Joins) > 0 {
		return c.updateWithJoinLocked(ctx, stmt, args)
	}

	// Determine if we can use buffered writes for this update.
	useBuffer := c.isBufferedMode() && table.Partition == nil
	if useBuffer {
		if c.hasVectorIndexForTableLocked(stmt.Table) {
			useBuffer = false
		}
		for _, setClause := range stmt.Set {
			if table.isPrimaryKeyColumn(setClause.Column) {
				useBuffer = false
				break
			}
		}
	}

	// Cache transaction state to avoid repeated goroutine-shard lookups.
	ts := c.getCurrentTxn()
	txnActive := ts != nil && ts.txnActive

	// Get all trees for scanning (handles partitioned tables)
	trees, err := c.getTableTreesForScan(table)
	if err != nil {
		return 0, 0, err
	}

	// Get tree names for later lookup during update
	treeNames := []string{stmt.Table}
	if table.Partition != nil {
		treeNames = table.getPartitionTreeNames()
	}

	// Pre-calculate column indices for SET clauses
	setColumnIndices := make([]int, len(stmt.Set))
	for i, setClause := range stmt.Set {
		setColumnIndices[i] = table.GetColumnIndex(setClause.Column)
		if setColumnIndices[i] < 0 {
			return 0, 0, fmt.Errorf("column '%s' not found in table '%s'", setClause.Column, stmt.Table)
		}
	}

	// Check if we can use an index for the WHERE clause
	var indexedRows []string
	useIndex := false
	if stmt.Where != nil {
		var idxErr error
		indexedRows, useIndex, idxErr = c.useIndexForQueryWithArgs(stmt.Table, stmt.Where, args)
		if idxErr != nil {
			return 0, 0, idxErr
		}
	}

	// Phase 1: resolve which rows match the WHERE clause and build
	// the per-row update entries. This is the heaviest phase in
	// updateLocked: it walks all partition trees, overlays pending
	// writes, decodes rows, evaluates WHERE, and produces
	// (entries, rowsAffected).
	entries, rowsAffected, err := c.resolveUpdateTargetRows(
		ctx, stmt, table, trees, treeNames, indexedRows, useIndex,
		setColumnIndices, ts, useBuffer, args,
	)
	if err != nil {
		return 0, rowsAffected, err
	}

	// Track pending-write start position for statement-level rollback
	// in buffered mode. Both phase 2 and phase 3 may need to roll
	// pending writes back to this marker.
	pendingWriteStartPos := 0
	if ts != nil {
		pendingWriteStartPos = len(ts.pendingWrites)
	}

	// Phase 2: validate constraints — execute BEFORE UPDATE triggers
	// and evaluate the RETURNING projection. Both run before the
	// mutations are committed so a trigger failure or RETURNING
	// error can short-circuit cleanly with a pending-write rollback.
	returningRows, returningCols, err := c.validateUpdateConstraints(
		ctx, stmt, table, entries, rowsAffected, ts, pendingWriteStartPos, args,
	)
	if err != nil {
		return 0, rowsAffected, err
	}

	// Phase 3: apply the collected updates to the storage layer,
	// execute AFTER UPDATE triggers, invalidate the query cache, and
	// publish the RETURNING result for the executor.
	if err := c.applyUpdateIndexes(
		ctx, stmt, table, entries, ts, txnActive, useBuffer,
		pendingWriteStartPos, returningRows, returningCols,
	); err != nil {
		return 0, rowsAffected, err
	}

	return 0, rowsAffected, nil
}

// resolveUpdateTargetRows is phase 1 of updateLocked: it walks every
// partition tree, overlays pending writes (so buffered-mode updates see
// their own prior mutations), evaluates the WHERE clause, and assembles
// the per-row updateEntry slice. It also counts rowsAffected.
//
// Two scan paths are supported:
//   - Index path: when the WHERE clause is index-eligible, the helper
//     restricts itself to indexedRows and overlays each row's pending
//     write before re-checking WHERE (the index may return a superset
//     for composite prefix matches).
//   - Full-scan path: a TreeIterator walks the entire B-tree. Pending
//     writes that were not visited during the scan are processed in
//     deterministic key order afterwards so buffered-mode updates
//     correctly pick up prior inserts/updates from the same txn.
//
// Returns the entries slice and the running rowsAffected count. Any
// error short-circuits the caller with the current count.
func (c *Catalog) resolveUpdateTargetRows(
	ctx context.Context,
	stmt *query.UpdateStmt,
	table *TableDef,
	trees []btree.TreeStore,
	treeNames []string,
	indexedRows []string,
	useIndex bool,
	setColumnIndices []int,
	ts *catalogTxnState,
	useBuffer bool,
	args []interface{},
) ([]updateEntry, int64, error) {
	var entries []updateEntry
	rowsAffected := int64(0)

	for treeIdx, tree := range trees {
		treeName := treeNames[treeIdx]

		// Collect pending writes for this tree to overlay in scan.
		var pendingKeys map[string]PendingWrite
		if ts != nil {
			pendingKeys = ts.getPendingWriteMap()[treeName]
		}

		// If we have indexed rows, only process those specific rows
		if useIndex {
			for _, pkStr := range indexedRows {
				key := []byte(pkStr)
				valueData, err := tree.Get(key)
				found := err == nil && valueData != nil
				// Overlay pending write
				if pwValue, ok := pendingKeys[pkStr]; ok {
					valueData = pwValue.Value
					found = true
				} else if found && useBuffer {
					c.recordManagerRead(treeName, pkStr, valueData)
				}
				if !found {
					continue
				}
				if err := c.processUpdateRow(ctx, table, tree, treeName, key, valueData, stmt, args, setColumnIndices, &entries, &rowsAffected); err != nil {
					return nil, rowsAffected, err
				}
			}
			continue
		}

		// Full table scan path
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan table for UPDATE: %w", err)
		}
		seenPending := make(map[string]bool)

		for iter.HasNext() {
			key, valueData, err := iter.Next()
			if err != nil {
				iter.Close()
				return nil, rowsAffected, fmt.Errorf("failed to read table for UPDATE: %w", err)
			}
			k := string(key)
			fromPending := false
			if pwValue, ok := pendingKeys[k]; ok {
				valueData = pwValue.Value
				seenPending[k] = true
				fromPending = true
			}

			// Decode row with version info
			row, live, err := decodeLiveRow(valueData, len(table.Columns))
			if err != nil {
				iter.Close()
				return nil, rowsAffected, fmt.Errorf("update: failed to decode row in table %s: %w", table.Name, err)
			}
			// Skip soft-deleted rows (decodeLiveRow already filters; if !live, continue)
			if !live {
				continue
			}

			// Apply WHERE clause if present
			if stmt.Where != nil {
				matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
				if err != nil {
					return nil, rowsAffected, fmt.Errorf("WHERE evaluation error: %w", err)
				}
				if !matched {
					continue // Skip row that doesn't match WHERE condition
				}
			}

			if !fromPending && useBuffer {
				c.recordManagerRead(treeName, string(key), valueData)
			}

			if err := c.processUpdateRowData(ctx, table, tree, treeName, key, row, stmt, args, setColumnIndices, &entries, &rowsAffected); err != nil {
				return nil, rowsAffected, err
			}
		}
		iter.Close()

		// Process pending inserts/updates not present in B-tree
		var pendingKeyList []string
		for k := range pendingKeys {
			if !seenPending[k] {
				pendingKeyList = append(pendingKeyList, k)
			}
		}
		sort.Strings(pendingKeyList)
		if pendingKeys != nil {
			for _, k := range pendingKeyList {
				key := []byte(k)
				valueData := pendingKeys[k].Value
				row, live, err := decodeLiveRow(valueData, len(table.Columns))
				if err != nil {
					return nil, rowsAffected, fmt.Errorf("update: failed to decode pending row in table %s: %w", table.Name, err)
				}
				if !live {
					continue
				}
				if stmt.Where != nil {
					matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
					if err != nil || !matched {
						continue
					}
				}
				if err := c.processUpdateRowData(ctx, table, tree, treeName, key, row, stmt, args, setColumnIndices, &entries, &rowsAffected); err != nil {
					return nil, rowsAffected, err
				}
			}
		}
	}
	return entries, rowsAffected, nil
}

// validateUpdateConstraints is phase 2 of updateLocked: it runs the
// BEFORE UPDATE triggers and evaluates the RETURNING projection. Both
// run before the mutations are committed, so a trigger failure or
// RETURNING error can short-circuit with a pending-write rollback to
// the marker captured at the start of the UPDATE.
//
// BEFORE triggers see entry.newRow so they can reject the planned
// mutation. RETURNING runs only when rowsAffected > 0 — projecting
// from an empty entry list is a no-op (we still return an empty
// projection and let the caller publish it).
//
// On error, the helper rolls back pending writes to pendingWriteStartPos
// before returning. The caller decides whether to propagate the error
// or continue with another path.
func (c *Catalog) validateUpdateConstraints(
	ctx context.Context,
	stmt *query.UpdateStmt,
	table *TableDef,
	entries []updateEntry,
	rowsAffected int64,
	ts *catalogTxnState,
	pendingWriteStartPos int,
	args []interface{},
) ([][]interface{}, []string, error) {
	for _, entry := range entries {
		if trigErr := c.executeTriggers(ctx, stmt.Table, "UPDATE", "BEFORE", entry.newRow, entry.oldRow, table.Columns); trigErr != nil {
			if ts != nil {
				ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
				rebuildPendingWriteMap(ts)
			}
			return nil, nil, fmt.Errorf("BEFORE UPDATE trigger failed: %w", trigErr)
		}
	}

	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, entry := range entries {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, entry.newRow, table, args)
			if err != nil {
				if ts != nil {
					ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
					rebuildPendingWriteMap(ts)
				}
				return nil, nil, fmt.Errorf("RETURNING clause failed: %w", err)
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}
	return returningRows, returningCols, nil
}

// applyUpdateIndexes is phase 3 of updateLocked: it dispatches the
// collected entries to either the buffered path (bufferUpdateEntries)
// or the direct path (applyUpdateEntries), runs the AFTER UPDATE
// triggers, invalidates the query cache for the affected table, and
// publishes the RETURNING result for the executor.
//
// The buffered path uses pending-write rollback on trigger failure,
// while the direct path replays the undo log (applyUpdateEntries owns
// its own rollback). The helper centralises that asymmetry so
// updateLocked is left with a single linear call sequence.
func (c *Catalog) applyUpdateIndexes(
	ctx context.Context,
	stmt *query.UpdateStmt,
	table *TableDef,
	entries []updateEntry,
	ts *catalogTxnState,
	txnActive bool,
	useBuffer bool,
	pendingWriteStartPos int,
	returningRows [][]interface{},
	returningCols []string,
) error {
	// Apply collected updates
	if useBuffer {
		if err := c.bufferUpdateEntries(table, stmt, entries, ts); err != nil {
			if ts != nil {
				ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
				rebuildPendingWriteMap(ts)
			}
			return err
		}
	} else {
		if err := c.applyUpdateEntries(ctx, table, stmt, entries, ts, txnActive); err != nil {
			return err
		}
	}
	// Execute AFTER UPDATE triggers (per-row)
	for _, entry := range entries {
		if trigErr := c.executeTriggers(ctx, stmt.Table, "UPDATE", "AFTER", entry.newRow, entry.oldRow, table.Columns); trigErr != nil {
			if useBuffer {
				if ts != nil {
					ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
					rebuildPendingWriteMap(ts)
				}
			} else if rbErr := c.rollbackAppliedUpdateEntries(table, stmt.Table, entries); rbErr != nil {
				return fmt.Errorf("AFTER UPDATE trigger failed: %w; rollback failed: %v", trigErr, rbErr)
			}
			return fmt.Errorf("AFTER UPDATE trigger failed: %w", trigErr)
		}
	}

	// Invalidate query cache for the affected table
	c.invalidateQueryCache(stmt.Table)

	// Store returning rows for retrieval
	c.setLastReturning(returningRows, returningCols)

	return nil
}

func (c *Catalog) updateWithJoinLocked(ctx context.Context, stmt *query.UpdateStmt, args []interface{}) (int64, int64, error) {
	targetTable, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	targetTree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	// Build a SELECT statement from the UPDATE to execute the join. Select
	// target columns AND columns from every source table referenced in
	// FROM/JOIN so that SET expressions can reference source-table columns
	// (e.g. UPDATE t SET x = s.y FROM s WHERE t.id = s.id).
	var selectColumns []query.Expression
	var joinedColDefs []ColumnDef
	targetAlias := stmt.Table
	if stmt.Alias != "" {
		targetAlias = stmt.Alias
	}
	for _, col := range targetTable.Columns {
		selectColumns = append(selectColumns, &query.QualifiedIdentifier{Table: targetAlias, Column: col.Name})
		cd := col
		cd.sourceTbl = targetAlias
		joinedColDefs = append(joinedColDefs, cd)
	}

	// Collect source table references (FROM + JOINs).
	var sourceRefs []*query.TableRef
	if stmt.From != nil {
		sourceRefs = append(sourceRefs, stmt.From)
	}
	for _, j := range stmt.Joins {
		if j != nil && j.Table != nil {
			sourceRefs = append(sourceRefs, j.Table)
		}
	}
	for _, tref := range sourceRefs {
		srcTable, srcErr := c.getTableLocked(tref.Name)
		if srcErr != nil {
			continue // Source may be a subquery/CTE we cannot introspect here
		}
		tableAlias := tref.Name
		if tref.Alias != "" {
			tableAlias = tref.Alias
		}
		for _, col := range srcTable.Columns {
			selectColumns = append(selectColumns, &query.QualifiedIdentifier{Table: tableAlias, Column: col.Name})
			cd := col
			cd.sourceTbl = tableAlias
			joinedColDefs = append(joinedColDefs, cd)
		}
	}

	selectStmt := &query.SelectStmt{
		Columns: selectColumns,
		From:    &query.TableRef{Name: stmt.Table, Alias: stmt.Alias},
		Joins:   stmt.Joins,
		Where:   stmt.Where,
	}

	// If FROM clause exists, use it as the main table and add target as first join
	if stmt.From != nil {
		selectStmt.From = stmt.From
		// Add target table as first join with no condition
		selectStmt.Joins = append([]*query.JoinClause{{
			Type:  query.TokenJoin,
			Table: &query.TableRef{Name: stmt.Table, Alias: stmt.Alias},
		}}, stmt.Joins...)
	}

	// Execute the join to find matching rows
	_, resultRows, err := c.selectLocked(selectStmt, args)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to execute UPDATE join: %w", err)
	}

	// Find primary key column index in the target table
	pkColIdx := 0
	if len(targetTable.PrimaryKey) > 0 {
		pkColIdx = targetTable.GetColumnIndex(targetTable.PrimaryKey[0])
		if pkColIdx < 0 {
			pkColIdx = 0
		}
	}

	// Map each target row's key to the first joined row that matched it, so
	// SET expressions referencing source columns can still resolve. If two
	// source rows match the same target row, SQL's UPDATE semantics allow
	// picking either — we pick the first, matching most engines.
	keyToJoinedRow := make(map[string][]interface{})
	keyOrder := make([]string, 0)
	for _, row := range resultRows {
		if pkColIdx < len(row) && row[pkColIdx] != nil {
			key := c.serializePK(row[pkColIdx], targetTree)
			k := string(key)
			if _, seen := keyToJoinedRow[k]; !seen {
				keyToJoinedRow[k] = row
				keyOrder = append(keyOrder, k)
			}
		}
	}

	if len(keyOrder) == 0 {
		return 0, 0, nil // No rows to update
	}

	// Now update each row
	rowsAffected := int64(0)

	var entries []joinUpdateEntry

	// Pre-calculate column indices for SET clauses
	setColumnIndices := make([]int, len(stmt.Set))
	for i, setClause := range stmt.Set {
		setColumnIndices[i] = targetTable.GetColumnIndex(setClause.Column)
		if setColumnIndices[i] < 0 {
			return 0, 0, fmt.Errorf("column '%s' not found in table '%s'", setClause.Column, stmt.Table)
		}
	}

	// Iterate over keys to update (deterministic: iteration order of keyOrder
	// mirrors match order, which matches existing behavior).
	for _, keyStr := range keyOrder {
		key := []byte(keyStr)
		valueData, err := targetTree.Get(key)
		if err != nil {
			continue // Row may have been deleted
		}

		row, err := decodeRow(valueData, len(targetTable.Columns))
		if err != nil {
			return 0, rowsAffected, fmt.Errorf("update join: failed to decode row in table %s: %w", targetTable.Name, err)
		}

		// Make a copy of the row to update
		updatedRow := make([]interface{}, len(row))
		copy(updatedRow, row)

		// Use the joined row for SET evaluation so SET expressions can
		// reference source-table columns. The target portion of the joined
		// row may be stale (selectLocked may have projected through several
		// layers), but it came from the same selectLocked snapshot we used
		// to locate this key, so it's consistent with that snapshot. The
		// authoritative target-column values for indexing/encoding come
		// from `row` fetched above.
		evalRow := keyToJoinedRow[keyStr]
		evalCols := joinedColDefs
		if len(evalRow) != len(evalCols) {
			// Fallback to target-only evaluation if the shapes disagree
			// (e.g. subquery projection rewrote the column list).
			evalRow = row
			evalCols = targetTable.Columns
		}

		// Apply SET clauses
		for i, setClause := range stmt.Set {
			colIdx := setColumnIndices[i]
			newVal, err := evaluateExpression(c, evalRow, evalCols, setClause.Value, args)
			if err != nil {
				return 0, rowsAffected, fmt.Errorf("failed to evaluate SET expression: %w", err)
			}
			updatedRow[colIdx] = newVal
		}

		// Check RLS policy for UPDATE on this specific row
		if allowed, rlsErr := c.checkRowAccessLocked(ctx, stmt.Table, targetTable.Columns, row, security.PolicyUpdate); rlsErr != nil {
			return 0, rowsAffected, fmt.Errorf("RLS check failed: %w", rlsErr)
		} else if !allowed {
			continue
		}
		if allowed, rlsErr := c.checkRowCheckLocked(ctx, stmt.Table, targetTable.Columns, updatedRow, security.PolicyUpdate); rlsErr != nil {
			return 0, rowsAffected, fmt.Errorf("RLS WITH CHECK failed for UPDATE: %w", rlsErr)
		} else if !allowed {
			continue
		}

		// Check constraints (simplified - full checks in actual implementation)
		for i, col := range targetTable.Columns {
			if col.NotNull && i < len(updatedRow) && updatedRow[i] == nil {
				return 0, rowsAffected, fmt.Errorf("NOT NULL constraint failed: %s", col.Name)
			}
		}

		entries = append(entries, joinUpdateEntry{
			key:    key,
			oldRow: row,
			newRow: updatedRow,
		})
		rowsAffected++
	}

	for _, entry := range entries {
		if trigErr := c.executeTriggers(ctx, stmt.Table, "UPDATE", "BEFORE", entry.newRow, entry.oldRow, targetTable.Columns); trigErr != nil {
			return 0, rowsAffected, fmt.Errorf("BEFORE UPDATE trigger failed: %w", trigErr)
		}
	}

	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, entry := range entries {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, entry.newRow, targetTable, args)
			if err != nil {
				return 0, rowsAffected, fmt.Errorf("RETURNING clause failed: %w", err)
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}

	// Apply all updates
	if err := c.applyJoinUpdateEntries(stmt.Table, targetTable, targetTree, entries); err != nil {
		if rbErr := c.rollbackAppliedJoinUpdateEntries(stmt.Table, targetTable, targetTree, entries); rbErr != nil {
			return 0, rowsAffected, fmt.Errorf("%w; rollback failed: %v", err, rbErr)
		}
		return 0, rowsAffected, err
	}

	for _, entry := range entries {
		if trigErr := c.executeTriggers(ctx, stmt.Table, "UPDATE", "AFTER", entry.newRow, entry.oldRow, targetTable.Columns); trigErr != nil {
			if rbErr := c.rollbackAppliedJoinUpdateEntries(stmt.Table, targetTable, targetTree, entries); rbErr != nil {
				return 0, rowsAffected, fmt.Errorf("AFTER UPDATE trigger failed: %w; rollback failed: %v", trigErr, rbErr)
			}
			return 0, rowsAffected, fmt.Errorf("AFTER UPDATE trigger failed: %w", trigErr)
		}
	}

	// Store returning rows for retrieval
	c.setLastReturning(returningRows, returningCols)

	return int64(len(entries)), rowsAffected, nil
}

// joinUpdateEntry is a local entry type used by updateWithJoinLocked to track
// pending updates discovered through a JOIN.
type joinUpdateEntry struct {
	key    []byte
	oldRow []interface{}
	newRow []interface{}
}

// applyJoinUpdateEntries writes back updated rows, updates indexes, and records
// undo log entries for a JOIN-based UPDATE.
func (c *Catalog) applyJoinUpdateEntries(tableName string, table *TableDef, tree btree.TreeStore, entries []joinUpdateEntry) error {
	ts := c.getCurrentTxn()
	txnActive := ts != nil && ts.txnActive
	for _, entry := range entries {
		newValue, err := encodeRow(nil, entry.newRow)
		if err != nil {
			return err
		}
		if c.wal != nil && txnActive {
			walData, err := encodeLogicalWALData(tableName, entry.key, newValue)
			if err != nil {
				return fmt.Errorf("failed to encode WAL update record: %w", err)
			}
			record := &storage.WALRecord{
				TxnID: ts.txnID,
				Type:  storage.WALUpdate,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				return fmt.Errorf("failed to append WAL record: %w", err)
			}
		}
		if err := tree.Put(entry.key, newValue); err != nil {
			return err
		}

		// Update indexes
		var deletedIndexEntries []deletedIndexEntry
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName != tableName || len(idxDef.Columns) == 0 {
				continue
			}
			idxTree, exists := c.indexTrees[idxName]
			if !exists {
				continue
			}
			deletedEntry, err := deleteIndexEntryForRowTracked(idxName, table, idxDef, idxTree, entry.oldRow, entry.key)
			if err != nil {
				if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
					return fmt.Errorf("failed to delete old index entry: %w; failed to restore deleted index entries: %v", err, restoreErr)
				}
				return fmt.Errorf("failed to delete old index entry: %w", err)
			}
			if deletedEntry != nil {
				deletedIndexEntries = append(deletedIndexEntries, *deletedEntry)
			}

			newIdxKey, newOk := buildCompositeIndexKey(table, idxDef, entry.newRow)
			if !newOk || newIdxKey == "" {
				continue
			}
			var newStorageKey []byte
			if idxDef.Unique {
				newStorageKey = []byte(newIdxKey)
			} else {
				newStorageKey = []byte(newIdxKey + "\x00" + string(entry.key))
			}
			if err := idxTree.Put(newStorageKey, entry.key); err != nil {
				if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
					return fmt.Errorf("failed to insert new index entry: %w; failed to restore deleted index entries: %v", err, restoreErr)
				}
				return fmt.Errorf("failed to insert new index entry: %w", err)
			}
		}

		// Record undo log for rollback
		if txnActive {
			oldValueData, marshalErr := json.Marshal(entry.oldRow)
			if marshalErr == nil {
				keyCopy := make([]byte, len(entry.key))
				copy(keyCopy, entry.key)
				c.appendUndoEntry(undoEntry{
					action:    undoUpdate,
					tableName: tableName,
					key:       keyCopy,
					oldValue:  oldValueData,
				})
			}
		}
	}
	return nil
}

func (c *Catalog) rollbackAppliedJoinUpdateEntries(tableName string, table *TableDef, tree btree.TreeStore, entries []joinUpdateEntry) error {
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		oldValue, err := encodeRow(nil, entry.oldRow)
		if err != nil {
			return err
		}
		if err := tree.Put(entry.key, oldValue); err != nil {
			return err
		}
	}
	return c.rebuildTableIndexesLocked(tableName)
}

func (c *Catalog) deleteWithUsingLocked(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	targetTable, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	targetTree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	// Build a SELECT statement to execute the join
	// Select all columns from target table to get the primary key
	var columns []query.Expression
	targetAlias := stmt.Table
	if stmt.Alias != "" {
		targetAlias = stmt.Alias
	}
	for _, col := range targetTable.Columns {
		columns = append(columns, &query.QualifiedIdentifier{Table: targetAlias, Column: col.Name})
	}

	selectStmt := &query.SelectStmt{
		Columns: columns,
		From:    &query.TableRef{Name: stmt.Table, Alias: stmt.Alias},
		Where:   stmt.Where,
	}

	// Add USING tables as joins
	for _, usingTable := range stmt.Using {
		selectStmt.Joins = append(selectStmt.Joins, &query.JoinClause{
			Type:  query.TokenJoin,
			Table: usingTable,
		})
	}

	// Execute the join to find matching rows
	_, resultRows, err := c.selectLocked(selectStmt, args)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to execute DELETE USING: %w", err)
	}

	// Find primary key column index
	pkIdx := 0
	if len(targetTable.PrimaryKey) > 0 {
		pkIdx = targetTable.GetColumnIndex(targetTable.PrimaryKey[0])
		if pkIdx < 0 {
			pkIdx = 0
		}
	}

	// Collect keys of rows to delete by serializing the PK value
	keysToDelete := make(map[string]struct{})
	for _, row := range resultRows {
		if pkIdx < len(row) && row[pkIdx] != nil {
			key := c.serializePK(row[pkIdx], targetTree)
			keysToDelete[string(key)] = struct{}{}
		}
	}

	if len(keysToDelete) == 0 {
		return 0, 0, nil // No rows to delete
	}

	// Now delete each row
	rowsAffected := int64(0)
	var entries []joinDelEntry

	// Foreign key enforcer for CASCADE/RESTRICT actions
	fke := NewForeignKeyEnforcer(c)

	// Collect entries to delete
	for keyStr := range keysToDelete {
		key := []byte(keyStr)
		valueData, err := targetTree.Get(key)
		if err != nil {
			continue // Row may have been deleted
		}

		row, version, live, err := decodeLiveRowFull(valueData, len(targetTable.Columns))
		if err != nil {
			return 0, rowsAffected, fmt.Errorf("delete using: failed to decode row in table %s: %w", targetTable.Name, err)
		}
		// Skip already deleted rows
		if !live {
			continue
		}

		// Check RLS policy for DELETE on this row
		if allowed, rlsErr := c.checkRowAccessLocked(ctx, stmt.Table, targetTable.Columns, row, security.PolicyDelete); rlsErr != nil {
			return 0, 0, fmt.Errorf("RLS check failed: %w", rlsErr)
		} else if !allowed {
			continue
		}

		// Enforce foreign key ON DELETE actions
		if fkErr := fke.OnDeleteRow(ctx, stmt.Table, row); fkErr != nil {
			return 0, 0, fmt.Errorf("foreign key constraint: %w", fkErr)
		}

		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		valueCopy := make([]byte, len(valueData))
		copy(valueCopy, valueData)

		entries = append(entries, joinDelEntry{
			key:     keyCopy,
			value:   valueCopy,
			row:     row,
			version: version,
		})
		rowsAffected++
	}

	for _, entry := range entries {
		if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "BEFORE", nil, entry.row, targetTable.Columns); trigErr != nil {
			return 0, rowsAffected, fmt.Errorf("BEFORE DELETE trigger failed: %w", trigErr)
		}
	}

	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, entry := range entries {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, entry.row, targetTable, args)
			if err != nil {
				return 0, rowsAffected, fmt.Errorf("RETURNING clause failed: %w", err)
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}

	// Soft delete collected entries
	if err := c.softDeleteJoinEntries(stmt.Table, targetTable, targetTree, entries); err != nil {
		if rbErr := c.rollbackAppliedJoinDeleteEntries(stmt.Table, targetTree, entries); rbErr != nil {
			return 0, rowsAffected, fmt.Errorf("%w; rollback failed: %v", err, rbErr)
		}
		return 0, rowsAffected, err
	}

	for _, entry := range entries {
		if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "AFTER", nil, entry.row, targetTable.Columns); trigErr != nil {
			if rbErr := c.rollbackAppliedJoinDeleteEntries(stmt.Table, targetTree, entries); rbErr != nil {
				return 0, rowsAffected, fmt.Errorf("AFTER DELETE trigger failed: %w; rollback failed: %v", trigErr, rbErr)
			}
			return 0, rowsAffected, fmt.Errorf("AFTER DELETE trigger failed: %w", trigErr)
		}
	}

	// Store returning rows for retrieval
	c.setLastReturning(returningRows, returningCols)

	return int64(len(entries)), rowsAffected, nil
}

// processUpdateRow processes a single row update from index lookup (valueData is raw bytes)

// joinDelEntry is a local entry type used by deleteWithUsingLocked to track
// pending deletions discovered through a JOIN.
type joinDelEntry struct {
	key     []byte
	value   []byte
	row     []interface{}
	version RowVersion // captured at scan time; avoids re-decode in softDeleteJoinEntries
}

func (c *Catalog) rollbackAppliedJoinDeleteEntries(tableName string, tree btree.TreeStore, entries []joinDelEntry) error {
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if err := tree.Put(entry.key, entry.value); err != nil {
			return err
		}
	}
	return c.rebuildTableIndexesLocked(tableName)
}

// softDeleteJoinEntries performs soft deletion of collected entries from a
// USING-based DELETE: updates indexes, writes WAL records, and marks rows deleted.
func (c *Catalog) softDeleteJoinEntries(tableName string, table *TableDef, tree btree.TreeStore, entries []joinDelEntry) error {
	ts := c.getCurrentTxn()
	txnActive := ts != nil && ts.txnActive
	for _, entry := range entries {
		// Remove from indexes first (before soft deleting the row), tracking
		// for rollback. Uses entry.row which was decoded and visibility-checked
		// during scan in deleteWithUsingLocked.
		var deletedIndexEntries []deletedIndexEntry
		for idxName, idxTree := range c.indexTrees {
			idxDef := c.indexes[idxName]
			if idxDef.TableName == tableName && len(idxDef.Columns) > 0 {
				deletedEntry, err := deleteIndexEntryForRowTracked(idxName, table, idxDef, idxTree, entry.row, entry.key)
				if err != nil {
					if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
						return fmt.Errorf("failed to delete index entry: %w; failed to restore deleted index entries: %v", err, restoreErr)
					}
					return fmt.Errorf("failed to delete index entry: %w", err)
				}
				if deletedEntry != nil {
					deletedIndexEntries = append(deletedIndexEntries, *deletedEntry)
				}
			}
		}

		// Soft delete: use entry.version (captured at scan time) for re-encode.
		vrow := VersionedRow{Data: entry.row, Version: entry.version}
		vrow.Version.markDeleted(time.Now())

		// Re-encode and store
		deletedValueData, err := json.Marshal(vrow)
		if err != nil {
			if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
				return fmt.Errorf("failed to encode deleted row: %w; failed to restore deleted index entries: %v", err, restoreErr)
			}
			return fmt.Errorf("failed to encode deleted row: %w", err)
		}

		// Log to WAL before applying change
		if c.wal != nil && txnActive {
			walData, err := encodeLogicalWALData(tableName, entry.key, nil)
			if err != nil {
				if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
					return fmt.Errorf("failed to encode WAL delete record: %w; failed to restore deleted index entries: %v", err, restoreErr)
				}
				return fmt.Errorf("failed to encode WAL delete record: %w", err)
			}
			record := &storage.WALRecord{
				TxnID: ts.txnID,
				Type:  storage.WALDelete,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
					return fmt.Errorf("failed to append WAL record: %w; failed to restore deleted index entries: %v", err, restoreErr)
				}
				return fmt.Errorf("failed to append WAL record: %w", err)
			}
		}

		if err := tree.Put(entry.key, deletedValueData); err != nil {
			if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
				return fmt.Errorf("failed to soft delete row: %w; failed to restore deleted index entries: %v", err, restoreErr)
			}
			return fmt.Errorf("failed to soft delete row: %w", err)
		}
	}
	return nil
}
func (c *Catalog) processUpdateRow(ctx context.Context, table *TableDef, tree btree.TreeStore, treeName string, key []byte, valueData []byte,
	stmt *query.UpdateStmt, args []interface{}, setColumnIndices []int, entries *[]updateEntry, rowsAffected *int64) error {
	row, live, err := decodeLiveRow(valueData, len(table.Columns))
	if err != nil {
		return fmt.Errorf("update: failed to decode row in table %s: %w", table.Name, err)
	}
	if !live {
		return nil // Skip soft-deleted rows
	}
	// Index may return superset of matching rows (e.g. composite index prefix match),
	// so always re-check WHERE clause to ensure correctness.
	if stmt.Where != nil {
		matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
		if err != nil {
			return fmt.Errorf("WHERE evaluation error: %w", err)
		}
		if !matched {
			return nil
		}
	}
	return c.processUpdateRowData(ctx, table, tree, treeName, key, row, stmt, args, setColumnIndices, entries, rowsAffected)
}

// processUpdateRowData processes a single row update from scan path (row is already decoded)
func (c *Catalog) processUpdateRowData(ctx context.Context, table *TableDef, tree btree.TreeStore, treeName string, key []byte, row []interface{},
	stmt *query.UpdateStmt, args []interface{}, setColumnIndices []int, entries *[]updateEntry, rowsAffected *int64) error {

	// Apply Row-Level Security check for UPDATE
	if allowed, rlsErr := c.checkRowAccessLocked(ctx, stmt.Table, table.Columns, row, security.PolicyUpdate); rlsErr != nil || !allowed {
		return nil
	}

	// Make a copy of the row to update
	updatedRow := make([]interface{}, len(row))
	copy(updatedRow, row)

	// Update fields - use pre-calculated column indices
	for i, setClause := range stmt.Set {
		colIdx := setColumnIndices[i]
		if colIdx >= 0 {
			newVal, err := evaluateExpression(c, row, table.Columns, setClause.Value, args)
			if err != nil {
				return fmt.Errorf("failed to evaluate SET expression for column '%s': %w", setClause.Column, err)
			}
			updatedRow[colIdx] = newVal
		}
	}
	if allowed, rlsErr := c.checkRowCheckLocked(ctx, stmt.Table, table.Columns, updatedRow, security.PolicyUpdate); rlsErr != nil {
		return fmt.Errorf("RLS WITH CHECK failed for UPDATE: %w", rlsErr)
	} else if !allowed {
		return nil
	}

	// Check UNIQUE constraints before updating. *entries holds the rows already
	// staged by this statement so two rows driven to the same unique value within
	// one statement are rejected instead of silently committing a duplicate.
	applySelfReferentialUpdateCascades(table, row, updatedRow)

	for i, col := range table.Columns {
		if col.Unique && updatedRow[i] != nil {
			conflict, err := c.hasUpdateUniqueConflict(tree, table, i, updatedRow[i], string(key), nil, *entries)
			if err != nil {
				return err
			}
			if conflict {
				return fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
			}
		}
	}

	// Check UNIQUE INDEX constraints before updating
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName == stmt.Table && idxDef.Unique && len(idxDef.Columns) > 0 {
			newIdxKey, newOk := buildCompositeIndexKey(table, idxDef, updatedRow)
			if !newOk {
				continue
			}
			oldIdxKey, _ := buildCompositeIndexKey(table, idxDef, row)
			if newIdxKey == oldIdxKey {
				continue // Value unchanged, no conflict possible
			}
			if idxTree, exists := c.indexTrees[idxName]; exists {
				if _, err := idxTree.Get([]byte(newIdxKey)); err == nil {
					return fmt.Errorf("UNIQUE constraint failed: duplicate value in index %s", idxName)
				}
			}
		}
	}

	// Check NOT NULL + CHECK constraints before updating. The FK check
	// below uses the ForeignKeyEnforcer (which can short-circuit on
	// unchanged columns), so we use the non-FK variant here.
	if err := c.validateRowNonFKConstraints(table, updatedRow, args); err != nil {
		return err
	}

	// Check FOREIGN KEY constraints on updated columns
	fke := NewForeignKeyEnforcer(c)
	for _, fk := range table.ForeignKeys {
		if !foreignKeyColumnsChanged(table, fk, row, updatedRow) {
			continue
		}
		fkValues, skip := foreignKeyValuesForRow(table, fk, updatedRow)
		if skip {
			continue
		}
		found := selfUpdatedRowSatisfiesForeignKey(table, fk, updatedRow, fkValues)
		if !found {
			var err error
			found, err = fke.referencedRowExists(fk.ReferencedTable, fk.ReferencedColumns, fkValues)
			if err != nil {
				return fmt.Errorf("FOREIGN KEY constraint failed: failed to decode referenced row in table %s: %w", fk.ReferencedTable, err)
			}
		}
		if !found {
			return fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValues, fk.ReferencedTable)
		}
	}

	*entries = append(*entries, updateEntry{
		key:      key,
		oldRow:   row,
		newRow:   updatedRow,
		treeName: treeName,
	})
	*rowsAffected++
	return nil
}

func (c *Catalog) UpdateRow(tableName string, pkValue interface{}, row map[string]interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updateRowLocked(tableName, pkValue, row)
}

func applySelfReferentialUpdateCascades(table *TableDef, oldRow, newRow []interface{}) {
	if table == nil {
		return
	}
	for _, fk := range table.ForeignKeys {
		if fk.OnUpdate != "CASCADE" || fk.ReferencedTable != table.Name {
			continue
		}
		refColumns := referencedColumnsForTable(table, fk)
		if len(refColumns) != len(fk.Columns) {
			continue
		}
		oldRefValues, ok := rowColumnValues(table, oldRow, refColumns)
		if !ok || valuesContainNil(oldRefValues) {
			continue
		}
		newRefValues, ok := rowColumnValues(table, newRow, refColumns)
		if !ok || valuesSliceCompareEqual(oldRefValues, newRefValues) {
			continue
		}
		oldLocalValues, ok := rowColumnValues(table, oldRow, fk.Columns)
		if !ok || !valuesSliceCompareEqual(oldLocalValues, oldRefValues) {
			continue
		}
		for i, colName := range fk.Columns {
			colIdx := table.GetColumnIndex(colName)
			if colIdx >= 0 && colIdx < len(newRow) {
				newRow[colIdx] = newRefValues[i]
			}
		}
	}
}

func selfUpdatedRowSatisfiesForeignKey(table *TableDef, fk ForeignKeyDef, row []interface{}, fkValues []interface{}) bool {
	if table == nil || fk.ReferencedTable != table.Name {
		return false
	}
	refColumns := referencedColumnsForTable(table, fk)
	refValues, ok := rowColumnValues(table, row, refColumns)
	return ok && valuesSliceCompareEqual(refValues, fkValues)
}

func rowColumnValues(table *TableDef, row []interface{}, columns []string) ([]interface{}, bool) {
	if len(columns) == 0 {
		return nil, false
	}
	values := make([]interface{}, len(columns))
	for i, colName := range columns {
		colIdx := table.GetColumnIndex(colName)
		if colIdx < 0 || colIdx >= len(row) {
			return nil, false
		}
		values[i] = row[colIdx]
	}
	return values, true
}

func valuesContainNil(values []interface{}) bool {
	for _, value := range values {
		if value == nil {
			return true
		}
	}
	return false
}

func valuesSliceCompareEqual(left, right []interface{}) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if compareValues(left[i], right[i]) != 0 {
			return false
		}
	}
	return true
}

// updateRowLocked is the lock-free internal version. Must be called with mu held.
func (c *Catalog) updateRowLocked(tableName string, pkValue interface{}, row map[string]interface{}) error {
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	tree, exists := c.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s has no data", tableName)
	}

	// Serialize the primary key using the same format as Insert
	key := c.serializePK(pkValue, tree)

	// Convert row map to values slice
	values := make([]interface{}, len(table.Columns))
	for i, col := range table.Columns {
		if val, exists := row[col.Name]; exists {
			values[i] = val
		} else {
			values[i] = nil
		}
	}

	// Encode the row (Insert uses json.Marshal, so use same format for consistency)
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	// Update in BTree
	return tree.Put(key, data)
}

// applyUpdateEntries applies collected update entries: re-encodes rows, handles
// PK changes, enforces FK constraints, logs to WAL, updates indexes, and records
// undo log entries.
func (c *Catalog) applyUpdateEntries(ctx context.Context, table *TableDef, stmt *query.UpdateStmt, entries []updateEntry, ts *catalogTxnState, txnActive bool) error {
	pkColIdx := -1
	if len(table.PrimaryKey) > 0 {
		pkColIdx = table.GetColumnIndex(table.PrimaryKey[0])
	}
	fke := NewForeignKeyEnforcer(c)
	untrackReferenceRows := fke.trackUpdatingReferenceRows(stmt.Table, entries)
	defer untrackReferenceRows()
	appliedEntries := make([]updateEntry, 0, len(entries))
	undoStart := 0
	if txnActive {
		undoStart = len(c.getCurrentTxnUndoLog())
	}
	rollbackApplied := func(cause error, current *updateEntry) error {
		if txnActive {
			undoEnd := len(c.getCurrentTxnUndoLog()) - 1
			if undoEnd >= undoStart {
				if rbErr := c.replayUndoLog(undoEnd, undoStart, "statement rollback"); rbErr != nil {
					c.truncateUndoLog(undoStart)
					return fmt.Errorf("%w; rollback failed: %v", cause, rbErr)
				}
			}
			c.truncateUndoLog(undoStart)
		}
		toRollback := appliedEntries
		if txnActive {
			toRollback = nil
		}
		if current != nil {
			toRollback = append(append([]updateEntry{}, toRollback...), *current)
		}
		if len(toRollback) == 0 {
			if rbErr := fke.rollbackAppliedUpdates(); rbErr != nil {
				return fmt.Errorf("%w; rollback failed: %v", cause, rbErr)
			}
			return cause
		}
		if rbErr := c.rollbackAppliedUpdateEntries(table, stmt.Table, toRollback); rbErr != nil {
			return fmt.Errorf("%w; rollback failed: %v", cause, rbErr)
		}
		if rbErr := fke.rollbackAppliedUpdates(); rbErr != nil {
			return fmt.Errorf("%w; rollback failed: %v", cause, rbErr)
		}
		return cause
	}

	for i := range entries {
		entry := &entries[i]
		oldKey := entry.key

		// Re-encode row with new timestamp
		newValueData, err := encodeVersionedRow(entry.newRow, nil)
		if err != nil {
			return rollbackApplied(fmt.Errorf("failed to encode updated row: %w", err), nil)
		}

		// Get the tree for this entry
		updateTree, exists := c.tableTrees[entry.treeName]
		if !exists {
			return rollbackApplied(fmt.Errorf("partition tree %s not found", entry.treeName), nil)
		}

		// Check if PRIMARY KEY was changed
		newKey := oldKey
		pkChanged := false
		if pkColIdx >= 0 && pkColIdx < len(entry.newRow) && pkColIdx < len(entry.oldRow) {
			if compareValues(entry.oldRow[pkColIdx], entry.newRow[pkColIdx]) != 0 {
				pkChanged = true
				pkVal := entry.newRow[pkColIdx]
				if strVal, ok := toString(pkVal); ok {
					newKey = []byte("S:" + strVal)
				} else if fVal, ok := toFloat64(pkVal); ok {
					newKey = []byte(formatKey(int64(fVal)))
				}
				if existingData, err := updateTree.Get(newKey); err == nil && existingData != nil {
					return rollbackApplied(fmt.Errorf("PRIMARY KEY constraint failed: duplicate key '%v'", pkVal), nil)
				}
			}
		}

		// Enforce foreign key ON UPDATE actions for any referenced column, not
		// only primary-key columns.
		if fkErr := fke.OnUpdateRow(ctx, stmt.Table, entry.oldRow, entry.newRow); fkErr != nil {
			return rollbackApplied(fmt.Errorf("foreign key constraint: %w", fkErr), nil)
		}

		idxChanges, directErr := c.applyUpdateEntryDirect(
			table, stmt, entry, oldKey, newKey, pkChanged, pkColIdx, ts, txnActive, newValueData,
		)
		if directErr != nil {
			return rollbackApplied(directErr, entry)
		}

		// Record undo log entry for rollback
		if txnActive {
			oldValueData, marshalErr := json.Marshal(entry.oldRow)
			if marshalErr != nil {
				return rollbackApplied(fmt.Errorf("failed to encode undo log for row: %w", marshalErr), entry)
			}
			keyCopy := make([]byte, len(oldKey))
			copy(keyCopy, oldKey)
			// If the UPDATE changed the primary key, the row was moved to newKey
			// in the B-tree (old key deleted, new key written). Record newKey so
			// txn rollback can delete the orphaned new-key row before restoring
			// the old-key row; otherwise rollback leaves a duplicate.
			var newKeyCopy []byte
			if pkChanged && string(oldKey) != string(newKey) {
				newKeyCopy = make([]byte, len(newKey))
				copy(newKeyCopy, newKey)
			}
			c.appendUndoEntry(undoEntry{
				action:       undoUpdate,
				tableName:    stmt.Table,
				key:          keyCopy,
				oldValue:     oldValueData,
				newKey:       newKeyCopy,
				indexChanges: idxChanges,
			})
		}
		appliedEntries = append(appliedEntries, *entry)
	}
	return nil
}

// applyUpdateEntryDirect is the per-row extraction of the legacy direct
// update path. The caller has already computed the B-tree tree, detected
// a PK change (if any), and produced the encoded new value. This helper
// owns: WAL append, B-tree mutation, secondary-index update with
// rollback-on-failure, and vector-index update.
//
// The trickiest concern is the secondary-index update: each index must
// be deleted from the old position and inserted at the new position,
// with a UNIQUE check on the new key for unique indexes. The helper
// builds the idxChanges slice that the caller records in the undo
// log entry, since undo-log recording is per-row and belongs at the
// caller level. Errors from any of these sub-steps propagate up so
// the caller's rollbackApplied closure can replay the undo log and
// undo the FK tracker.
//
// Returns:
//   - idxChanges: the per-index undo entries for this row, suitable
//     for the undoLog.indexChanges field.
//   - err: any unrecoverable error. Caller breaks the loop and rolls
//     back the statement.
func (c *Catalog) applyUpdateEntryDirect(
	table *TableDef,
	stmt *query.UpdateStmt,
	entry *updateEntry,
	oldKey, newKey []byte,
	pkChanged bool,
	pkColIdx int,
	ts *catalogTxnState,
	txnActive bool,
	newValueData []byte,
) ([]indexUndoEntry, error) {
	// Log to WAL before applying change.
	if c.wal != nil && txnActive {
		if pkChanged {
			deleteData, err := encodeLogicalWALData(entry.treeName, oldKey, nil)
			if err != nil {
				return nil, err
			}
			deleteRecord := &storage.WALRecord{
				TxnID: ts.txnID,
				Type:  storage.WALDelete,
				Data:  deleteData,
			}
			if err := c.wal.Append(deleteRecord); err != nil {
				return nil, err
			}
			walData, err := encodeLogicalWALData(entry.treeName, newKey, newValueData)
			if err != nil {
				return nil, err
			}
			insertRecord := &storage.WALRecord{
				TxnID: ts.txnID,
				Type:  storage.WALInsert,
				Data:  walData,
			}
			if err := c.wal.Append(insertRecord); err != nil {
				return nil, err
			}
		} else {
			walData, err := encodeLogicalWALData(entry.treeName, oldKey, newValueData)
			if err != nil {
				return nil, err
			}
			record := &storage.WALRecord{
				TxnID: ts.txnID,
				Type:  storage.WALUpdate,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				return nil, err
			}
		}
	}

	// B-tree mutation: delete + put on PK change, plain put otherwise.
	updateTree, exists := c.tableTrees[entry.treeName]
	if !exists {
		return nil, fmt.Errorf("partition tree %s not found", entry.treeName)
	}
	if pkChanged {
		if err := updateTree.Delete(oldKey); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
			return nil, fmt.Errorf("failed to delete old row key during update: %w", err)
		}
		if err := updateTree.Put(newKey, newValueData); err != nil {
			return nil, fmt.Errorf("failed to update row with new key: %w", err)
		}
		// Keep the auto-increment sequence at least as large as the
		// new PK so future INSERTs that don't supply a PK value don't
		// collide with the one we just used.
		if pkColIdx >= 0 && pkColIdx < len(entry.newRow) {
			if fVal, ok := toFloat64(entry.newRow[pkColIdx]); ok {
				pkVal := int64(fVal)
				if pkVal > atomic.LoadInt64(&table.AutoIncSeq) {
					atomic.StoreInt64(&table.AutoIncSeq, pkVal)
				}
			}
		}
	} else {
		if err := updateTree.Put(oldKey, newValueData); err != nil {
			return nil, fmt.Errorf("failed to update row: %w", err)
		}
	}

	// Update indexes: remove old entries and add new ones, track for undo.
	var idxChanges []indexUndoEntry
	for idxName, idxTree := range c.indexTrees {
		idxDef := c.indexes[idxName]
		if idxDef.TableName != stmt.Table || len(idxDef.Columns) == 0 {
			continue
		}
		oldIndexKey, oldOk := buildCompositeIndexKey(table, idxDef, entry.oldRow)
		if oldOk {
			var idxStorageKey []byte
			if idxDef.Unique {
				idxStorageKey = []byte(oldIndexKey)
			} else {
				idxStorageKey = []byte(oldIndexKey + "\x00" + string(entry.key))
			}
			oldIdxVal, getErr := idxTree.Get(idxStorageKey)
			if err := idxTree.Delete(idxStorageKey); err != nil {
				return nil, fmt.Errorf("failed to delete from index %s: %w", idxName, err)
			}
			if txnActive && getErr == nil {
				idxChanges = append(idxChanges, indexUndoEntry{
					indexName: idxName,
					key:       idxStorageKey,
					oldValue:  oldIdxVal,
					wasAdded:  false,
				})
			}
		}
		newIndexKey, newOk := buildCompositeIndexKey(table, idxDef, entry.newRow)
		if newOk {
			var idxStorageKey []byte
			if idxDef.Unique {
				idxStorageKey = []byte(newIndexKey)
				if newIndexKey != oldIndexKey {
					if _, err := idxTree.Get(idxStorageKey); err == nil {
						return nil, fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", newIndexKey, idxName)
					}
				}
			} else {
				idxStorageKey = []byte(newIndexKey + "\x00" + string(newKey))
			}
			if err := idxTree.Put(idxStorageKey, newKey); err != nil {
				return nil, fmt.Errorf("failed to update index %s: %w", idxName, err)
			}
			if txnActive {
				idxChanges = append(idxChanges, indexUndoEntry{
					indexName: idxName,
					key:       idxStorageKey,
					wasAdded:  true,
				})
			}
		}
	}

	// Update vector indexes. Vector-index errors propagate so the
	// caller can roll back the FK tracker.
	if err := c.updateVectorIndexesForUpdate(stmt.Table, entry.newRow, string(entry.key)); err != nil {
		return nil, err
	}

	return idxChanges, nil
}

func (c *Catalog) rollbackAppliedUpdateEntries(table *TableDef, tableName string, entries []updateEntry) error {
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		updateTree, exists := c.tableTrees[entry.treeName]
		if !exists {
			return fmt.Errorf("partition tree %s not found", entry.treeName)
		}

		if newKey, ok := buildCompositePK(table, entry.newRow); ok && newKey != string(entry.key) {
			if err := updateTree.Delete([]byte(newKey)); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
				return fmt.Errorf("delete updated key %s: %w", newKey, err)
			}
		}

		oldValueData, err := encodeVersionedRow(entry.oldRow, nil)
		if err != nil {
			return fmt.Errorf("encode old row: %w", err)
		}
		if err := updateTree.Put(entry.key, oldValueData); err != nil {
			return fmt.Errorf("restore row: %w", err)
		}
	}
	return c.rebuildTableIndexesLocked(tableName)
}

// bufferUpdateEntry is the per-row extraction of the buffered update path.
// The caller handles FK enforcement; this helper owns: row encoding,
// secondary-index update key building, pending-write recording, and
// managerTxn write tracking.
//
// Unlike applyUpdateEntryDirect (which writes B-tree + WAL immediately),
// buffered mode records the mutations in ts.pendingWrites for commit-time
// application. Errors from UNIQUE constraint checks propagate to the caller
// so the pending-write slice can be truncated.
func (c *Catalog) bufferUpdateEntry(table *TableDef, stmt *query.UpdateStmt, entry *updateEntry, ts *catalogTxnState) ([]byte, []PendingIndexUpdate, error) {
	newValueData, err := encodeVersionedRow(entry.newRow, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode updated row: %w", err)
	}

	var idxUpdates []PendingIndexUpdate
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName != stmt.Table || len(idxDef.Columns) == 0 {
			continue
		}
		oldIndexKey, oldOk := buildCompositeIndexKey(table, idxDef, entry.oldRow)
		newIndexKey, newOk := buildCompositeIndexKey(table, idxDef, entry.newRow)

		if oldOk && oldIndexKey != "" {
			var oldIdxStorageKey []byte
			if idxDef.Unique {
				oldIdxStorageKey = []byte(oldIndexKey)
			} else {
				oldIdxStorageKey = []byte(oldIndexKey + "\x00" + string(entry.key))
			}
			idxUpdates = append(idxUpdates, PendingIndexUpdate{
				IndexName: idxName,
				Key:       string(oldIdxStorageKey),
				IsDelete:  true,
			})
		}

		if newOk && newIndexKey != "" {
			if idxDef.Unique && newIndexKey != oldIndexKey {
				if idxTree, exists := c.indexTrees[idxName]; exists {
					if _, err := idxTree.Get([]byte(newIndexKey)); err == nil {
						return nil, nil, fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", newIndexKey, idxName)
					}
					if c.indexKeyInPendingWrites(idxName, newIndexKey) {
						return nil, nil, fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", newIndexKey, idxName)
					}
				}
			}
			var newIdxStorageKey []byte
			if idxDef.Unique {
				newIdxStorageKey = []byte(newIndexKey)
			} else {
				newIdxStorageKey = []byte(newIndexKey + "\x00" + string(entry.key))
			}
			idxUpdates = append(idxUpdates, PendingIndexUpdate{
				IndexName: idxName,
				Key:       string(newIdxStorageKey),
				Value:     []byte(entry.key),
			})
		}
	}

	c.appendPendingWriteTs(ts, PendingWrite{
		TreeName:     stmt.Table,
		Key:          string(entry.key),
		Value:        newValueData,
		IndexUpdates: idxUpdates,
	})
	if ts != nil {
		if mt, ok := ts.managerTxn.(*txn.Transaction); ok && mt != nil {
			mt.SetWrite(stmt.Table, string(entry.key), newValueData)
		}
	}

	return newValueData, idxUpdates, nil
}

// bufferUpdateEntries buffers updated rows and their index mutations for
// commit-time application in MVCC buffered mode.
func (c *Catalog) bufferUpdateEntries(table *TableDef, stmt *query.UpdateStmt, entries []updateEntry, ts *catalogTxnState) error {
	fke := NewForeignKeyEnforcer(c)
	untrackReferenceRows := fke.trackUpdatingReferenceRows(stmt.Table, entries)
	defer untrackReferenceRows()
	for _, entry := range entries {
		if fkErr := fke.OnUpdateRow(context.Background(), stmt.Table, entry.oldRow, entry.newRow); fkErr != nil {
			return fmt.Errorf("foreign key constraint: %w", fkErr)
		}
		if _, _, err := c.bufferUpdateEntry(table, stmt, &entry, ts); err != nil {
			return err
		}
	}
	return nil
}
