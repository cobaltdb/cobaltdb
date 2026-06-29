package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

var (
	ErrForeignKeyViolation = errors.New("foreign key constraint violation")
	ErrReferencedRowExists = errors.New("cannot delete or update: referenced row exists")
)

// ForeignKeyEnforcer handles foreign key constraint enforcement
type ForeignKeyEnforcer struct {
	catalog          *Catalog
	appliedUpdates   []updateEntry
	appliedDeletes   []deleteEntry
	deletingRows     map[string]struct{}
	updatingRows     map[string]struct{}
	referenceChanges map[string][]referenceChange
}

// NewForeignKeyEnforcer creates a new foreign key enforcer
func NewForeignKeyEnforcer(catalog *Catalog) *ForeignKeyEnforcer {
	return &ForeignKeyEnforcer{catalog: catalog}
}

type referenceChange struct {
	oldRow  []interface{}
	newRow  []interface{}
	deleted bool
}

// OnDelete handles foreign key actions when a row is deleted from the referenced table.
// pkValues accepts one value per primary key column (variadic for backward compatibility).
func (fke *ForeignKeyEnforcer) OnDelete(ctx context.Context, tableName string, pkValues ...interface{}) error {
	if len(pkValues) == 0 {
		return nil
	}
	return fke.applyDeleteActions(ctx, tableName, pkValues)
}

func (fke *ForeignKeyEnforcer) OnDeleteRow(ctx context.Context, tableName string, oldRow []interface{}) error {
	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil || table == nil {
		return nil
	}
	referencingTables := fke.findReferencingTables(tableName)
	for _, refInfo := range referencingTables {
		fk := refInfo.ForeignKey
		refColumns := fke.referencedColumnsForFK(fk, table)
		values, ok := fke.rowValuesForColumns(table, oldRow, refColumns)
		if !ok || fke.hasNull(values) {
			continue
		}
		if err := fke.applyDeleteAction(ctx, refInfo.TableName, fk, values); err != nil {
			return err
		}
	}
	return nil
}

func (fke *ForeignKeyEnforcer) applyDeleteActions(ctx context.Context, tableName string, referencedValues []interface{}) error {
	for _, refInfo := range fke.findReferencingTables(tableName) {
		if err := fke.applyDeleteAction(ctx, refInfo.TableName, refInfo.ForeignKey, referencedValues); err != nil {
			return err
		}
	}
	return nil
}

func (fke *ForeignKeyEnforcer) applyDeleteAction(ctx context.Context, referencingTable string, fk ForeignKeyDef, referencedValues []interface{}) error {
	rows, err := fke.findReferencingRows(referencingTable, fk, referencedValues)
	if err != nil {
		return err
	}

	switch fk.OnDelete {
	case "CASCADE":
		for _, row := range rows {
			if row.pending || fke.shouldBufferAction() {
				if err := fke.pendingDeleteRow(ctx, referencingTable, row); err != nil {
					return fmt.Errorf("cascade delete failed: %w", err)
				}
				continue
			}
			if err := fke.deleteRow(ctx, referencingTable, row.rowKey); err != nil {
				return fmt.Errorf("cascade delete failed: %w", err)
			}
		}
	case "SET NULL":
		for _, row := range rows {
			if row.pending || fke.shouldBufferAction() {
				if err := fke.pendingUpdateForeignKey(ctx, referencingTable, row, fk.Columns, nil); err != nil {
					return fmt.Errorf("set null failed: %w", err)
				}
				continue
			}
			if err := fke.setNull(ctx, referencingTable, row.rowKey, fk.Columns); err != nil {
				return fmt.Errorf("set null failed: %w", err)
			}
		}
	case "SET DEFAULT":
		defaultValues, err := fke.defaultValuesForColumns(referencingTable, fk.Columns)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			referencesChanging, err := fke.defaultReferencesChangingRow(fk, defaultValues)
			if err != nil {
				return err
			}
			if referencesChanging {
				return fmt.Errorf("%w: SET DEFAULT would reference a row deleted or updated by this statement", ErrForeignKeyViolation)
			}
		}
		for _, row := range rows {
			if row.pending || fke.shouldBufferAction() {
				if err := fke.pendingUpdateForeignKey(ctx, referencingTable, row, fk.Columns, defaultValues, true); err != nil {
					return fmt.Errorf("set default failed: %w", err)
				}
				continue
			}
			if err := fke.updateForeignKey(ctx, referencingTable, row.rowKey, fk.Columns, defaultValues, true); err != nil {
				return fmt.Errorf("set default failed: %w", err)
			}
		}
	case "RESTRICT", "NO ACTION":
		if len(rows) > 0 {
			return ErrReferencedRowExists
		}
	default:
		if len(rows) > 0 {
			return ErrReferencedRowExists
		}
	}
	return nil
}

// OnUpdate handles foreign key actions when a primary key is updated in the referenced table.
// oldPkValues and newPkValues must contain one value per primary key column.
func (fke *ForeignKeyEnforcer) OnUpdate(ctx context.Context, tableName string, oldPkValues []interface{}, newPkValues []interface{}) error {
	if len(oldPkValues) == 0 || len(newPkValues) == 0 {
		return nil
	}
	return fke.applyUpdateActions(ctx, tableName, oldPkValues, newPkValues)
}

func (fke *ForeignKeyEnforcer) OnUpdateRow(ctx context.Context, tableName string, oldRow, newRow []interface{}) error {
	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil || table == nil {
		return nil
	}
	for _, refInfo := range fke.findReferencingTables(tableName) {
		fk := refInfo.ForeignKey
		refColumns := fke.referencedColumnsForFK(fk, table)
		oldValues, ok := fke.rowValuesForColumns(table, oldRow, refColumns)
		if !ok || fke.hasNull(oldValues) {
			continue
		}
		newValues, ok := fke.rowValuesForColumns(table, newRow, refColumns)
		if !ok || fke.valuesSliceEqual(oldValues, newValues) {
			continue
		}
		if err := fke.applyUpdateAction(ctx, refInfo.TableName, fk, oldValues, newValues); err != nil {
			return err
		}
	}
	return nil
}

func (fke *ForeignKeyEnforcer) applyUpdateActions(ctx context.Context, tableName string, oldValues []interface{}, newValues []interface{}) error {
	for _, refInfo := range fke.findReferencingTables(tableName) {
		if err := fke.applyUpdateAction(ctx, refInfo.TableName, refInfo.ForeignKey, oldValues, newValues); err != nil {
			return err
		}
	}
	return nil
}

func (fke *ForeignKeyEnforcer) applyUpdateAction(ctx context.Context, referencingTable string, fk ForeignKeyDef, oldValues []interface{}, newValues []interface{}) error {
	rows, err := fke.findReferencingRows(referencingTable, fk, oldValues)
	if err != nil {
		return err
	}

	switch fk.OnUpdate {
	case "CASCADE":
		for _, row := range rows {
			if row.pending || fke.shouldBufferAction() {
				if err := fke.pendingUpdateForeignKey(ctx, referencingTable, row, fk.Columns, newValues); err != nil {
					return fmt.Errorf("cascade update failed: %w", err)
				}
				continue
			}
			if err := fke.updateForeignKey(ctx, referencingTable, row.rowKey, fk.Columns, newValues); err != nil {
				return fmt.Errorf("cascade update failed: %w", err)
			}
		}
	case "SET NULL":
		for _, row := range rows {
			if row.pending || fke.shouldBufferAction() {
				if err := fke.pendingUpdateForeignKey(ctx, referencingTable, row, fk.Columns, nil); err != nil {
					return fmt.Errorf("set null failed: %w", err)
				}
				continue
			}
			if err := fke.setNull(ctx, referencingTable, row.rowKey, fk.Columns); err != nil {
				return fmt.Errorf("set null failed: %w", err)
			}
		}
	case "SET DEFAULT":
		defaultValues, err := fke.defaultValuesForColumns(referencingTable, fk.Columns)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			referencesChanging, err := fke.defaultReferencesChangingRow(fk, defaultValues)
			if err != nil {
				return err
			}
			if referencesChanging {
				return fmt.Errorf("%w: SET DEFAULT would reference a row deleted or updated by this statement", ErrForeignKeyViolation)
			}
		}
		for _, row := range rows {
			if row.pending || fke.shouldBufferAction() {
				if err := fke.pendingUpdateForeignKey(ctx, referencingTable, row, fk.Columns, defaultValues, true); err != nil {
					return fmt.Errorf("set default failed: %w", err)
				}
				continue
			}
			if err := fke.updateForeignKey(ctx, referencingTable, row.rowKey, fk.Columns, defaultValues, true); err != nil {
				return fmt.Errorf("set default failed: %w", err)
			}
		}
	case "RESTRICT", "NO ACTION":
		if len(rows) > 0 {
			return ErrReferencedRowExists
		}
	default:
		if len(rows) > 0 {
			return ErrReferencedRowExists
		}
	}
	return nil
}

func (fke *ForeignKeyEnforcer) shouldBufferAction() bool {
	ts := fke.catalog.getCurrentTxn()
	return ts != nil && ts.txnActive && fke.catalog.isBufferedMode()
}

func (fke *ForeignKeyEnforcer) referencedColumnsForFK(fk ForeignKeyDef, referencedTable *TableDef) []string {
	if len(fk.ReferencedColumns) > 0 {
		return fk.ReferencedColumns
	}
	if referencedTable == nil {
		return nil
	}
	return referencedTable.PrimaryKey
}

func (fke *ForeignKeyEnforcer) rowValuesForColumns(table *TableDef, row []interface{}, columns []string) ([]interface{}, bool) {
	if table == nil || len(columns) == 0 {
		return nil, false
	}
	values := make([]interface{}, len(columns))
	for i, col := range columns {
		idx := table.GetColumnIndex(col)
		if idx < 0 || idx >= len(row) {
			return nil, false
		}
		values[i] = row[idx]
	}
	return values, true
}

func (fke *ForeignKeyEnforcer) hasNull(values []interface{}) bool {
	for _, val := range values {
		if val == nil {
			return true
		}
	}
	return false
}

func (fke *ForeignKeyEnforcer) valuesSliceEqual(left, right []interface{}) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !fke.valuesEqual(left[i], right[i]) {
			return false
		}
	}
	return true
}

func (fke *ForeignKeyEnforcer) defaultValuesForColumns(tableName string, columns []string) ([]interface{}, error) {
	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil {
		return nil, err
	}
	values := make([]interface{}, len(columns))
	for i, colName := range columns {
		colIdx := table.GetColumnIndex(colName)
		if colIdx < 0 || colIdx >= len(table.Columns) {
			return nil, fmt.Errorf("foreign key column %s not found in table %s", colName, tableName)
		}
		col := table.Columns[colIdx]
		if col.defaultExpr == nil {
			values[i] = nil
			continue
		}
		val, err := EvalExpression(col.defaultExpr, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate DEFAULT for %s.%s: %w", tableName, colName, err)
		}
		values[i] = val
	}
	return values, nil
}

func (fke *ForeignKeyEnforcer) defaultReferencesChangingRow(fk ForeignKeyDef, defaultValues []interface{}) (bool, error) {
	if len(defaultValues) == 0 || fke.hasNull(defaultValues) || len(fke.referenceChanges) == 0 {
		return false, nil
	}
	table, err := fke.catalog.getTableLocked(fk.ReferencedTable)
	if err != nil {
		return false, err
	}
	refColumns := fke.referencedColumnsForFK(fk, table)
	changes := fke.referenceChanges[fk.ReferencedTable]
	for _, change := range changes {
		oldValues, ok := fke.rowValuesForColumns(table, change.oldRow, refColumns)
		if !ok || !fke.valuesSliceEqual(oldValues, defaultValues) {
			continue
		}
		if change.deleted {
			return true, nil
		}
		newValues, ok := fke.rowValuesForColumns(table, change.newRow, refColumns)
		if !ok || !fke.valuesSliceEqual(oldValues, newValues) {
			return true, nil
		}
	}
	return false, nil
}

func (fke *ForeignKeyEnforcer) trackDeletingReferenceRows(tableName string, entries []deleteEntry) func() {
	if len(entries) == 0 {
		return func() {}
	}
	if fke.referenceChanges == nil {
		fke.referenceChanges = make(map[string][]referenceChange)
	}
	start := len(fke.referenceChanges[tableName])
	for _, entry := range entries {
		row := entry.row
		if row == nil && entry.value != nil {
			if table, err := fke.catalog.getTableLocked(tableName); err == nil {
				if decoded, decErr := decodeRow(entry.value, len(table.Columns)); decErr == nil {
					row = decoded
				}
			}
		}
		if row == nil {
			continue
		}
		fke.referenceChanges[tableName] = append(fke.referenceChanges[tableName], referenceChange{
			oldRow:  append([]interface{}(nil), row...),
			deleted: true,
		})
	}
	return func() {
		fke.referenceChanges[tableName] = fke.referenceChanges[tableName][:start]
	}
}

func (fke *ForeignKeyEnforcer) trackUpdatingReferenceRows(tableName string, entries []updateEntry) func() {
	if len(entries) == 0 {
		return func() {}
	}
	if fke.referenceChanges == nil {
		fke.referenceChanges = make(map[string][]referenceChange)
	}
	start := len(fke.referenceChanges[tableName])
	for _, entry := range entries {
		fke.referenceChanges[tableName] = append(fke.referenceChanges[tableName], referenceChange{
			oldRow: append([]interface{}(nil), entry.oldRow...),
			newRow: append([]interface{}(nil), entry.newRow...),
		})
	}
	return func() {
		fke.referenceChanges[tableName] = fke.referenceChanges[tableName][:start]
	}
}

// ValidateInsert validates that foreign key values reference existing rows
// row is a map from column name to value
func (fke *ForeignKeyEnforcer) ValidateInsert(ctx context.Context, tableName string, row map[string]interface{}) error {
	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil {
		return err
	}

	for _, fk := range table.ForeignKeys {
		// Get the foreign key column values from the row
		fkValues := make([]interface{}, len(fk.Columns))
		for i, col := range fk.Columns {
			val, exists := row[col]
			if !exists {
				return fmt.Errorf("foreign key column %s not found in row", col)
			}
			fkValues[i] = val
		}

		// Check if any value is NULL (foreign keys allow NULL unless NOT NULL)
		hasNull := false
		for _, val := range fkValues {
			if val == nil {
				hasNull = true
				break
			}
		}
		if hasNull {
			continue // NULL values are allowed in foreign keys
		}

		// Check if the referenced row exists
		exists, err := fke.referencedRowExists(fk.ReferencedTable, fk.ReferencedColumns, fkValues)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: %s.%v references non-existent row in %s.%v",
				ErrForeignKeyViolation, tableName, fk.Columns, fk.ReferencedTable, fk.ReferencedColumns)
		}
	}

	return nil
}

// ValidateUpdate validates foreign key constraints on update
func (fke *ForeignKeyEnforcer) ValidateUpdate(ctx context.Context, tableName string, oldRow, newRow map[string]interface{}) error {
	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil {
		return err
	}

	for _, fk := range table.ForeignKeys {
		// Check if any foreign key column was changed
		fkChanged := false
		for _, col := range fk.Columns {
			oldVal, oldExists := oldRow[col]
			newVal, newExists := newRow[col]
			if !oldExists || !newExists || oldVal != newVal {
				fkChanged = true
				break
			}
		}

		if !fkChanged {
			continue // Foreign key values didn't change
		}

		// Get the new foreign key column values
		fkValues := make([]interface{}, len(fk.Columns))
		for i, col := range fk.Columns {
			val, exists := newRow[col]
			if !exists {
				return fmt.Errorf("foreign key column %s not found in row", col)
			}
			fkValues[i] = val
		}

		// Check if any value is NULL
		hasNull := false
		for _, val := range fkValues {
			if val == nil {
				hasNull = true
				break
			}
		}
		if hasNull {
			continue
		}

		// Check if the referenced row exists
		exists, err := fke.referencedRowExists(fk.ReferencedTable, fk.ReferencedColumns, fkValues)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: %s.%v references non-existent row in %s.%v",
				ErrForeignKeyViolation, tableName, fk.Columns, fk.ReferencedTable, fk.ReferencedColumns)
		}
	}

	return nil
}

// referencingTableInfo holds information about a table that references another table
type referencingTableInfo struct {
	TableName  string
	ForeignKey ForeignKeyDef
}

type referencingRowMatch struct {
	key     string
	rowKey  interface{}
	row     []interface{}
	pending bool
}

// findReferencingTables finds all tables that have foreign keys referencing the given table
func (fke *ForeignKeyEnforcer) findReferencingTables(tableName string) []referencingTableInfo {
	var result []referencingTableInfo

	for name, table := range fke.catalog.tables {
		for _, fk := range table.ForeignKeys {
			if fk.ReferencedTable == tableName {
				result = append(result, referencingTableInfo{
					TableName:  name,
					ForeignKey: fk,
				})
			}
		}
	}

	return result
}

// findReferencingRows finds all rows in a table that reference the given primary key values.
// pkValues must contain one value for each referenced column (in order).
// Returns matching row keys and whether the match is currently pending.
func (fke *ForeignKeyEnforcer) findReferencingRows(tableName string, fk ForeignKeyDef, pkValues []interface{}) ([]referencingRowMatch, error) {
	var result []referencingRowMatch

	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil {
		return nil, err
	}

	// Get column indices for foreign key columns
	fkColIndices := make([]int, len(fk.Columns))
	for i, col := range fk.Columns {
		idx := table.GetColumnIndex(col)
		if idx < 0 {
			return nil, fmt.Errorf("foreign key column %s not found", col)
		}
		fkColIndices[i] = idx
	}

	// Ensure pkValues length matches fk columns length
	if len(pkValues) != len(fk.Columns) {
		return nil, fmt.Errorf("pkValues length (%d) does not match fk columns length (%d)", len(pkValues), len(fk.Columns))
	}

	rowMatches := func(row []interface{}) bool {
		for i, colIdx := range fkColIndices {
			if colIdx >= len(row) || !fke.valuesEqual(row[colIdx], pkValues[i]) {
				return false
			}
		}
		return true
	}

	// Same-txn pending writes for the referencing table override the committed
	// state (read-your-writes): a child inserted earlier in this txn must be seen
	// (else deleting/updating the parent would leave a dangling FK at commit), and
	// a committed child soft-deleted in this txn must not be counted.
	var pending map[string]PendingWrite
	if ts := fke.catalog.getCurrentTxn(); ts != nil {
		pending = ts.getPendingWriteMap()[tableName]
	}
	for k, pw := range pending {
		pendingRow, live, decErr := decodeLiveRow(pw.Value, len(table.Columns))
		if decErr != nil {
			return nil, decErr
		}
		if !live {
			continue
		}
		if rowMatches(pendingRow) {
			result = append(result, referencingRowMatch{
				key:     k,
				rowKey:  fke.deserializeValue([]byte(k)),
				row:     append([]interface{}(nil), pendingRow...),
				pending: true,
			})
		}
	}

	// Get the table's BTree
	tree, exists := fke.catalog.tableTrees[tableName]
	if !exists {
		return result, nil // Table has no committed data
	}

	// Fallback: full table scan, skipping rows superseded by a pending write
	// (already accounted for above).
	iter, err := tree.Scan([]byte{}, []byte{0xFF})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.HasNext() {
		key, value, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if _, overridden := pending[string(key)]; overridden {
			continue
		}

		row, live, decErr := decodeLiveRow(value, len(table.Columns))
		if decErr != nil {
			return nil, decErr
		}
		if !live {
			continue
		}

		if rowMatches(row) {
			result = append(result, referencingRowMatch{
				key:    string(key),
				rowKey: fke.deserializeValue(key),
				row:    append([]interface{}(nil), row...),
			})
		}
	}

	return result, nil
}

func (fke *ForeignKeyEnforcer) pendingDeleteRow(ctx context.Context, tableName string, match referencingRowMatch) error {
	ts := fke.catalog.getCurrentTxn()
	if ts == nil {
		return fmt.Errorf("no active transaction for pending delete")
	}
	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil {
		return err
	}
	actionKey := fke.actionRowKey(tableName, match.key)
	if fke.isDeleting(actionKey) {
		return nil
	}
	fke.markDeleting(actionKey)
	defer fke.unmarkDeleting(actionKey)
	untrackReferenceRows := fke.trackDeletingReferenceRows(tableName, []deleteEntry{{row: match.row}})
	defer untrackReferenceRows()
	if err := fke.OnDeleteRow(ctx, tableName, match.row); err != nil {
		return err
	}
	now := time.Now().Unix()
	valueData, err := json.Marshal(VersionedRow{
		Data: match.row,
		Version: RowVersion{
			CreatedAt: now,
			DeletedAt: now,
		},
	})
	if err != nil {
		return err
	}
	idxUpdates := fke.pendingIndexDeletesForRow(table, tableName, match.key, match.row)
	fke.appendPendingActionWrite(ts, tableName, match.key, valueData, idxUpdates)
	return nil
}

func (fke *ForeignKeyEnforcer) pendingUpdateForeignKey(ctx context.Context, tableName string, match referencingRowMatch, columns []string, newValues []interface{}, validateLocalFK ...bool) error {
	ts := fke.catalog.getCurrentTxn()
	if ts == nil {
		return fmt.Errorf("no active transaction for pending update")
	}
	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil {
		return err
	}
	actionKey := fke.actionRowKey(tableName, match.key)
	if fke.isUpdating(actionKey) {
		return nil
	}
	fke.markUpdating(actionKey)
	defer fke.unmarkUpdating(actionKey)
	oldRow := append([]interface{}(nil), match.row...)
	newRow := append([]interface{}(nil), match.row...)
	for i, col := range columns {
		colIdx := table.GetColumnIndex(col)
		if colIdx < 0 || colIdx >= len(newRow) {
			continue
		}
		if newValues == nil || i >= len(newValues) {
			newRow[colIdx] = nil
			continue
		}
		newRow[colIdx] = newValues[i]
	}
	if err := fke.validateActionRow(tableName, match.key, table, newRow); err != nil {
		return err
	}
	if len(validateLocalFK) > 0 && validateLocalFK[0] {
		if err := fke.catalog.checkForeignKeyConstraints(table, newRow, ts); err != nil {
			return err
		}
	}
	if err := fke.OnUpdateRow(ctx, tableName, oldRow, newRow); err != nil {
		return err
	}
	valueData, err := encodeVersionedRow(newRow, nil)
	if err != nil {
		return err
	}
	idxUpdates := fke.pendingIndexUpdatesForRowChange(table, tableName, match.key, match.row, newRow)
	fke.appendPendingActionWrite(ts, tableName, match.key, valueData, idxUpdates)
	return nil
}

func (fke *ForeignKeyEnforcer) pendingIndexDeletesForRow(table *TableDef, tableName, key string, oldRow []interface{}) []PendingIndexUpdate {
	var updates []PendingIndexUpdate
	for idxName, idxDef := range fke.catalog.indexes {
		if idxDef == nil || idxDef.TableName != tableName || len(idxDef.Columns) == 0 {
			continue
		}
		oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
		if !ok {
			continue
		}
		idxStorageKey := oldIdxKey
		if !idxDef.Unique {
			idxStorageKey = oldIdxKey + "\x00" + key
		}
		updates = append(updates, PendingIndexUpdate{IndexName: idxName, Key: idxStorageKey, IsDelete: true})
	}
	return updates
}

func (fke *ForeignKeyEnforcer) pendingIndexUpdatesForRowChange(table *TableDef, tableName, key string, oldRow, newRow []interface{}) []PendingIndexUpdate {
	updates := fke.pendingIndexDeletesForRow(table, tableName, key, oldRow)
	for idxName, idxDef := range fke.catalog.indexes {
		if idxDef == nil || idxDef.TableName != tableName || len(idxDef.Columns) == 0 {
			continue
		}
		newIdxKey, ok := buildCompositeIndexKey(table, idxDef, newRow)
		if !ok {
			continue
		}
		idxStorageKey := newIdxKey
		if !idxDef.Unique {
			idxStorageKey = newIdxKey + "\x00" + key
		}
		updates = append(updates, PendingIndexUpdate{IndexName: idxName, Key: idxStorageKey, Value: []byte(key)})
	}
	return updates
}

func (fke *ForeignKeyEnforcer) appendPendingActionWrite(ts *catalogTxnState, tableName, key string, value []byte, idxUpdates []PendingIndexUpdate) {
	fke.catalog.appendPendingWriteTs(ts, PendingWrite{
		TreeName:     tableName,
		Key:          key,
		Value:        value,
		IndexUpdates: idxUpdates,
	})
	if mt, ok := ts.managerTxn.(*txn.Transaction); ok && mt != nil {
		mt.SetWrite(tableName, key, value)
	}
}

// referencedRowExists checks if a row exists in the referenced table
func (fke *ForeignKeyEnforcer) referencedRowExists(tableName string, columns []string, values []interface{}) (bool, error) {
	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil {
		return false, nil
	}
	if len(columns) == 0 {
		columns = table.PrimaryKey
	}
	if len(columns) != len(values) {
		return false, fmt.Errorf("referenced column count (%d) does not match value count (%d)", len(columns), len(values))
	}
	colIndices := make([]int, len(columns))
	for i, col := range columns {
		idx := table.GetColumnIndex(col)
		if idx < 0 {
			return false, nil
		}
		colIndices[i] = idx
	}

	tree, exists := fke.catalog.tableTrees[tableName]
	if !exists {
		return false, nil // Table has no data
	}

	rowMatches := func(row []interface{}) bool {
		for i, colIdx := range colIndices {
			if colIdx >= len(row) || !fke.valuesEqual(row[colIdx], values[i]) {
				return false
			}
		}
		return true
	}

	var pending map[string]PendingWrite
	if ts := fke.catalog.getCurrentTxn(); ts != nil {
		pending = ts.getPendingWriteMap()[tableName]
		for _, pw := range pending {
			pendingRow, live, decErr := decodeLiveRow(pw.Value, len(table.Columns))
			if decErr != nil {
				return false, decErr
			}
			if !live {
				continue
			}
			if rowMatches(pendingRow) {
				return true, nil
			}
		}
	}

	iter, err := tree.Scan([]byte{}, []byte{0xFF})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	for iter.HasNext() {
		key, value, err := iter.Next()
		if err != nil {
			return false, err
		}
		if _, overridden := pending[string(key)]; overridden {
			continue
		}
		vrow, decErr := decodeVersionedRow(value, len(table.Columns))
		if decErr != nil {
			row, rowErr := decodeRow(value, len(table.Columns))
			if rowErr != nil {
				return false, decErr
			}
			if rowMatches(row) {
				return true, nil
			}
			continue
		}
		if vrow.Version.DeletedAt > 0 {
			continue
		}
		if rowMatches(vrow.Data) {
			return true, nil
		}
	}

	return false, nil
}

// setNull sets specified columns to NULL in a row
func (fke *ForeignKeyEnforcer) setNull(ctx context.Context, tableName string, rowKey interface{}, columns []string) error {
	// Get the current row data
	rowData, err := fke.getRowSlice(tableName, rowKey)
	if err != nil {
		return err
	}

	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil {
		return err
	}
	actionKey := fke.actionRowKey(tableName, string(fke.serializeValue(rowKey)))
	if fke.isUpdating(actionKey) {
		return nil
	}
	fke.markUpdating(actionKey)
	defer fke.unmarkUpdating(actionKey)
	oldRow := append([]interface{}(nil), rowData...)

	// Set columns to NULL by index
	for _, col := range columns {
		colIdx := table.GetColumnIndex(col)
		if colIdx >= 0 && colIdx < len(rowData) {
			rowData[colIdx] = nil
		}
	}
	if err := fke.validateActionRow(tableName, string(fke.serializeValue(rowKey)), table, rowData); err != nil {
		return err
	}
	if err := fke.OnUpdateRow(ctx, tableName, oldRow, rowData); err != nil {
		return err
	}

	// Update the row
	return fke.updateRowSlice(tableName, rowKey, rowData)
}

// updateForeignKey updates foreign key columns with new values.
// newValues must contain one value per column (for composite keys).
func (fke *ForeignKeyEnforcer) updateForeignKey(ctx context.Context, tableName string, rowKey interface{}, columns []string, newValues []interface{}, validateLocalFK ...bool) error {
	// Get the current row data
	rowData, err := fke.getRowSlice(tableName, rowKey)
	if err != nil {
		return err
	}

	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil {
		return err
	}
	actionKey := fke.actionRowKey(tableName, string(fke.serializeValue(rowKey)))
	if fke.isUpdating(actionKey) {
		return nil
	}
	fke.markUpdating(actionKey)
	defer fke.unmarkUpdating(actionKey)
	oldRow := append([]interface{}(nil), rowData...)

	// Update foreign key columns by index
	for i, col := range columns {
		colIdx := table.GetColumnIndex(col)
		if colIdx >= 0 && colIdx < len(rowData) {
			if i < len(newValues) {
				rowData[colIdx] = newValues[i]
			}
		}
	}
	if err := fke.validateActionRow(tableName, string(fke.serializeValue(rowKey)), table, rowData); err != nil {
		return err
	}
	if len(validateLocalFK) > 0 && validateLocalFK[0] {
		if err := fke.catalog.checkForeignKeyConstraints(table, rowData, fke.catalog.getCurrentTxn()); err != nil {
			return err
		}
	}
	if err := fke.OnUpdateRow(ctx, tableName, oldRow, rowData); err != nil {
		return err
	}

	// Update the row
	return fke.updateRowSlice(tableName, rowKey, rowData)
}

func (fke *ForeignKeyEnforcer) validateActionRow(tableName, selfKey string, table *TableDef, row []interface{}) error {
	for i, col := range table.Columns {
		if col.NotNull && i < len(row) && row[i] == nil {
			return fmt.Errorf("NOT NULL constraint failed: column '%s' cannot be null", col.Name)
		}
	}
	if err := fke.catalog.checkRowConstraints(table, row, nil); err != nil {
		return err
	}
	if err := fke.validateActionUniqueConstraints(tableName, selfKey, table, row); err != nil {
		return err
	}
	return nil
}

func (fke *ForeignKeyEnforcer) validateActionUniqueConstraints(tableName, selfKey string, table *TableDef, row []interface{}) error {
	pending := map[string]PendingWrite(nil)
	if ts := fke.catalog.getCurrentTxn(); ts != nil {
		pending = ts.getPendingWriteMap()[tableName]
	}

	for i, col := range table.Columns {
		if !col.Unique || i >= len(row) || row[i] == nil {
			continue
		}
		conflict, err := fke.actionRowConflicts(tableName, selfKey, table, pending, func(existing []interface{}) bool {
			return i < len(existing) && existing[i] != nil && compareValues(row[i], existing[i]) == 0
		})
		if err != nil {
			return err
		}
		if conflict {
			return fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
		}
	}

	for idxName, idxDef := range fke.catalog.indexes {
		if idxDef == nil || idxDef.TableName != tableName || !idxDef.Unique || len(idxDef.Columns) == 0 {
			continue
		}
		newIdxKey, ok := buildCompositeIndexKey(table, idxDef, row)
		if !ok {
			continue
		}
		conflict, err := fke.actionRowConflicts(tableName, selfKey, table, pending, func(existing []interface{}) bool {
			existingKey, ok := buildCompositeIndexKey(table, idxDef, existing)
			return ok && existingKey == newIdxKey
		})
		if err != nil {
			return err
		}
		if conflict {
			return fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", newIdxKey, idxName)
		}
	}

	return nil
}

func (fke *ForeignKeyEnforcer) actionRowConflicts(tableName, selfKey string, table *TableDef, pending map[string]PendingWrite, matches func([]interface{}) bool) (bool, error) {
	for key, pw := range pending {
		if key == selfKey {
			continue
		}
		row, deleted, err := decodeActionConstraintRow(pw.Value, len(table.Columns))
		if err != nil {
			return false, fmt.Errorf("failed to decode pending row during FK action UNIQUE check on table %s: %w", tableName, err)
		}
		if !deleted && matches(row) {
			return true, nil
		}
	}

	tree, exists := fke.catalog.tableTrees[tableName]
	if !exists {
		return false, fmt.Errorf("table %s not found", tableName)
	}
	iter, err := tree.Scan([]byte{}, []byte{0xFF})
	if err != nil {
		return false, fmt.Errorf("failed to scan table for FK action UNIQUE check: %w", err)
	}
	defer iter.Close()
	for iter.HasNext() {
		key, value, err := iter.Next()
		if err != nil {
			return false, fmt.Errorf("failed to read row during FK action UNIQUE check on table %s: %w", tableName, err)
		}
		keyStr := string(key)
		if keyStr == selfKey {
			continue
		}
		if _, overridden := pending[keyStr]; overridden {
			continue
		}
		row, deleted, err := decodeActionConstraintRow(value, len(table.Columns))
		if err != nil {
			return false, fmt.Errorf("failed to decode row during FK action UNIQUE check on table %s: %w", tableName, err)
		}
		if !deleted && matches(row) {
			return true, nil
		}
	}
	return false, nil
}

func decodeActionConstraintRow(data []byte, numCols int) ([]interface{}, bool, error) {
	row, live, err := decodeLiveRow(data, numCols)
	if err == nil {
		return row, !live, nil
	}
	plainRow, rowErr := decodeRow(data, numCols)
	if rowErr != nil {
		return nil, false, err
	}
	return plainRow, false, nil
}

// getRowSlice retrieves a row as a slice
func (fke *ForeignKeyEnforcer) getRowSlice(tableName string, rowKey interface{}) ([]interface{}, error) {
	tree, exists := fke.catalog.tableTrees[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	key := fke.serializeValue(rowKey)
	value, err := tree.Get(key)
	if err != nil {
		return nil, err
	}

	table := fke.catalog.tables[tableName]
	numCols := 0
	if table != nil {
		numCols = len(table.Columns)
	}
	row, decErr := decodeRow(value, numCols)
	if decErr != nil {
		return nil, decErr
	}

	return row, nil
}

// updateRowSlice updates a row from a slice with index maintenance and undo logging
func (fke *ForeignKeyEnforcer) updateRowSlice(tableName string, rowKey interface{}, rowData []interface{}) error {
	tree, exists := fke.catalog.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	ts := fke.catalog.getCurrentTxn()
	txnActive := ts != nil && ts.txnActive

	key := fke.serializeValue(rowKey)

	// Get old value for undo log and index cleanup
	oldData, getErr := tree.Get(key)

	table := fke.catalog.tables[tableName]

	// Update indexes: remove old entries, add new ones
	var idxChanges []indexUndoEntry
	if table != nil && getErr == nil {
		oldRow, decErr := decodeRow(oldData, len(table.Columns))
		if decErr == nil {
			fke.appliedUpdates = append(fke.appliedUpdates, updateEntry{
				key:      append([]byte(nil), key...),
				oldRow:   append([]interface{}(nil), oldRow...),
				newRow:   append([]interface{}(nil), rowData...),
				treeName: tableName,
			})
			for idxName, idxTree := range fke.catalog.indexTrees {
				idxDef := fke.catalog.indexes[idxName]
				if idxDef != nil && idxDef.TableName == tableName && len(idxDef.Columns) > 0 {
					oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
					if ok {
						oldIdxStorageKey := []byte(oldIdxKey)
						if !idxDef.Unique {
							oldIdxStorageKey = []byte(oldIdxKey + "\x00" + string(key))
						}
						var oldIdxVal []byte
						if txnActive {
							oldIdxVal, _ = idxTree.Get(oldIdxStorageKey)
						}
						if err := idxTree.Delete(oldIdxStorageKey); err != nil {
							if idxDef.Unique || !errors.Is(err, btree.ErrKeyNotFound) {
								return fmt.Errorf("index cleanup failed: %w", err)
							}
							legacyIdxStorageKey := []byte(oldIdxKey)
							legacyIdxVal, legacyGetErr := idxTree.Get(legacyIdxStorageKey)
							if legacyErr := idxTree.Delete(legacyIdxStorageKey); legacyErr != nil {
								return fmt.Errorf("index cleanup failed: %w", err)
							}
							oldIdxStorageKey = legacyIdxStorageKey
							if txnActive && legacyGetErr == nil {
								oldIdxVal = legacyIdxVal
							}
						}
						if txnActive {
							idxChanges = append(idxChanges, indexUndoEntry{
								indexName: idxName,
								key:       oldIdxStorageKey,
								oldValue:  oldIdxVal,
								wasAdded:  false,
							})
						}
					}
					newIdxKey, ok := buildCompositeIndexKey(table, idxDef, rowData)
					if ok {
						newIdxStorageKey := []byte(newIdxKey)
						if !idxDef.Unique {
							newIdxStorageKey = []byte(newIdxKey + "\x00" + string(key))
						}
						if err := idxTree.Put(newIdxStorageKey, key); err != nil {
							return fmt.Errorf("index update failed: %w", err)
						}
						if txnActive {
							idxChanges = append(idxChanges, indexUndoEntry{
								indexName: idxName,
								key:       newIdxStorageKey,
								wasAdded:  true,
							})
						}
					}
				}
			}
		}
	}

	// Record undo log entry for rollback
	if txnActive && getErr == nil {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		oldCopy := make([]byte, len(oldData))
		copy(oldCopy, oldData)
		fke.catalog.appendUndoEntry(undoEntry{
			action:       undoUpdate,
			tableName:    tableName,
			key:          keyCopy,
			oldValue:     oldCopy,
			indexChanges: idxChanges,
		})
	}

	value, err := encodeVersionedRow(rowData, nil)
	if err != nil {
		return err
	}

	if fke.catalog.wal != nil && txnActive {
		walData, err := encodeLogicalWALData(tableName, key, value)
		if err != nil {
			return fmt.Errorf("failed to encode WAL update record: %w", err)
		}
		record := &storage.WALRecord{
			TxnID: ts.txnID,
			Type:  storage.WALUpdate,
			Data:  walData,
		}
		if err := fke.catalog.wal.Append(record); err != nil {
			return fmt.Errorf("failed to append WAL record: %w", err)
		}
	}

	if err := tree.Put(key, value); err != nil {
		return err
	}

	// Invalidate query cache for the affected child table
	fke.catalog.invalidateQueryCache(tableName)

	return nil
}

func (fke *ForeignKeyEnforcer) rollbackAppliedUpdates() error {
	if len(fke.appliedUpdates) == 0 {
		return nil
	}
	touched := make(map[string]struct{}, len(fke.appliedUpdates))
	for i := len(fke.appliedUpdates) - 1; i >= 0; i-- {
		entry := fke.appliedUpdates[i]
		tree, exists := fke.catalog.tableTrees[entry.treeName]
		if !exists {
			return fmt.Errorf("partition tree %s not found", entry.treeName)
		}
		oldValueData, err := encodeVersionedRow(entry.oldRow, nil)
		if err != nil {
			return fmt.Errorf("encode old FK action row: %w", err)
		}
		if err := tree.Put(entry.key, oldValueData); err != nil {
			return fmt.Errorf("restore FK action row: %w", err)
		}
		touched[entry.treeName] = struct{}{}
	}
	for tableName := range touched {
		if err := fke.catalog.rebuildTableIndexesLocked(tableName); err != nil {
			return err
		}
	}
	fke.appliedUpdates = nil
	return nil
}

func (fke *ForeignKeyEnforcer) deleteRow(ctx context.Context, tableName string, rowKey interface{}) error {
	tree, exists := fke.catalog.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s has no data", tableName)
	}
	ts := fke.catalog.getCurrentTxn()
	txnActive := ts != nil && ts.txnActive

	key := fke.catalog.serializePK(rowKey, tree)
	oldData, err := tree.Get(key)
	if err != nil {
		return nil
	}

	table := fke.catalog.tables[tableName]
	if table == nil {
		return fmt.Errorf("table %s not found", tableName)
	}
	vrow, err := decodeVersionedRow(oldData, len(table.Columns))
	if err != nil {
		return err
	}
	oldRow := append([]interface{}(nil), vrow.Data...)

	actionKey := fke.actionRowKey(tableName, string(key))
	if fke.isDeleting(actionKey) {
		return nil
	}
	fke.markDeleting(actionKey)
	defer fke.unmarkDeleting(actionKey)
	untrackReferenceRows := fke.trackDeletingReferenceRows(tableName, []deleteEntry{{row: oldRow}})
	defer untrackReferenceRows()
	if err := fke.OnDeleteRow(ctx, tableName, oldRow); err != nil {
		return err
	}

	var idxChanges []indexUndoEntry
	for idxName, idxTree := range fke.catalog.indexTrees {
		idxDef := fke.catalog.indexes[idxName]
		if idxDef == nil || idxDef.TableName != tableName || len(idxDef.Columns) == 0 {
			continue
		}
		oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
		if !ok {
			continue
		}
		idxStorageKey := []byte(oldIdxKey)
		if !idxDef.Unique {
			idxStorageKey = []byte(oldIdxKey + "\x00" + string(key))
		}
		oldIdxVal, getErr := idxTree.Get(idxStorageKey)
		if err := idxTree.Delete(idxStorageKey); err != nil {
			if rbErr := fke.catalog.rebuildTableIndexesLocked(tableName); rbErr != nil {
				return fmt.Errorf("failed to delete from index %s: %w; index rebuild failed: %v", idxName, err, rbErr)
			}
			return fmt.Errorf("failed to delete from index %s: %w", idxName, err)
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

	fke.appliedDeletes = append(fke.appliedDeletes, deleteEntry{
		key:      append([]byte(nil), key...),
		value:    append([]byte(nil), oldData...),
		row:      oldRow,
		treeName: tableName,
	})

	if txnActive {
		fke.catalog.appendUndoEntry(undoEntry{
			action:       undoDelete,
			tableName:    tableName,
			key:          append([]byte(nil), key...),
			oldValue:     append([]byte(nil), oldData...),
			indexChanges: idxChanges,
		})
	}

	vrow.Version.markDeleted(time.Now())
	deletedValueData, err := json.Marshal(vrow)
	if err != nil {
		return fmt.Errorf("failed to encode deleted row: %w", err)
	}
	if fke.catalog.wal != nil && txnActive {
		walData, err := encodeLogicalWALData(tableName, key, nil)
		if err != nil {
			return fmt.Errorf("failed to encode WAL delete record: %w", err)
		}
		record := &storage.WALRecord{
			TxnID: ts.txnID,
			Type:  storage.WALDelete,
			Data:  walData,
		}
		if err := fke.catalog.wal.Append(record); err != nil {
			return fmt.Errorf("failed to append WAL record: %w", err)
		}
	}
	return tree.Put(key, deletedValueData)
}

func (fke *ForeignKeyEnforcer) rollbackAppliedDeletes() error {
	if len(fke.appliedDeletes) == 0 {
		return nil
	}
	touched := make(map[string]struct{}, len(fke.appliedDeletes))
	for i := len(fke.appliedDeletes) - 1; i >= 0; i-- {
		entry := fke.appliedDeletes[i]
		tree, exists := fke.catalog.tableTrees[entry.treeName]
		if !exists {
			return fmt.Errorf("partition tree %s not found", entry.treeName)
		}
		if err := tree.Put(entry.key, entry.value); err != nil {
			return fmt.Errorf("restore FK cascade deleted row: %w", err)
		}
		touched[entry.treeName] = struct{}{}
	}
	for tableName := range touched {
		if err := fke.catalog.rebuildTableIndexesLocked(tableName); err != nil {
			return err
		}
	}
	fke.appliedDeletes = nil
	return nil
}

func (fke *ForeignKeyEnforcer) actionRowKey(tableName, key string) string {
	return tableName + "\x00" + key
}

func (fke *ForeignKeyEnforcer) isDeleting(actionKey string) bool {
	_, ok := fke.deletingRows[actionKey]
	return ok
}

func (fke *ForeignKeyEnforcer) markDeleting(actionKey string) {
	if fke.deletingRows == nil {
		fke.deletingRows = make(map[string]struct{})
	}
	fke.deletingRows[actionKey] = struct{}{}
}

func (fke *ForeignKeyEnforcer) unmarkDeleting(actionKey string) {
	delete(fke.deletingRows, actionKey)
}

func (fke *ForeignKeyEnforcer) isUpdating(actionKey string) bool {
	_, ok := fke.updatingRows[actionKey]
	return ok
}

func (fke *ForeignKeyEnforcer) markUpdating(actionKey string) {
	if fke.updatingRows == nil {
		fke.updatingRows = make(map[string]struct{})
	}
	fke.updatingRows[actionKey] = struct{}{}
}

func (fke *ForeignKeyEnforcer) unmarkUpdating(actionKey string) {
	delete(fke.updatingRows, actionKey)
}

// valuesEqual compares two values with type normalization
// Handles JSON unmarshaling types (float64 for numbers) vs Go types (int, int64)
func (fke *ForeignKeyEnforcer) valuesEqual(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Fast path for identical types — compare as int64 when both are ints,
	// avoiding float64 coercion that loses precision above 2^53.
	if ai, aOK := toInt64(a); aOK {
		if bi, bOK := toInt64(b); bOK {
			return ai == bi
		}
	}

	// Normalize both values to float64 for numeric comparison
	var aFloat, bFloat float64
	aIsNum := false
	bIsNum := false

	switch v := a.(type) {
	case int:
		aFloat = float64(v)
		aIsNum = true
	case int8:
		aFloat = float64(v)
		aIsNum = true
	case int16:
		aFloat = float64(v)
		aIsNum = true
	case int32:
		aFloat = float64(v)
		aIsNum = true
	case int64:
		aFloat = float64(v)
		aIsNum = true
	case uint:
		aFloat = float64(v)
		aIsNum = true
	case uint8:
		aFloat = float64(v)
		aIsNum = true
	case uint16:
		aFloat = float64(v)
		aIsNum = true
	case uint32:
		aFloat = float64(v)
		aIsNum = true
	case uint64:
		aFloat = float64(v)
		aIsNum = true
	case float32:
		aFloat = float64(v)
		aIsNum = true
	case float64:
		aFloat = v
		aIsNum = true
	}

	switch v := b.(type) {
	case int:
		bFloat = float64(v)
		bIsNum = true
	case int8:
		bFloat = float64(v)
		bIsNum = true
	case int16:
		bFloat = float64(v)
		bIsNum = true
	case int32:
		bFloat = float64(v)
		bIsNum = true
	case int64:
		bFloat = float64(v)
		bIsNum = true
	case uint:
		bFloat = float64(v)
		bIsNum = true
	case uint8:
		bFloat = float64(v)
		bIsNum = true
	case uint16:
		bFloat = float64(v)
		bIsNum = true
	case uint32:
		bFloat = float64(v)
		bIsNum = true
	case uint64:
		bFloat = float64(v)
		bIsNum = true
	case float32:
		bFloat = float64(v)
		bIsNum = true
	case float64:
		bFloat = v
		bIsNum = true
	}

	// If both are numeric, compare as floats
	if aIsNum && bIsNum {
		return aFloat == bFloat
	}

	// Otherwise, use standard equality
	return a == b
}

// serializeValue serializes a value for use as a BTree key
// Uses the same format as catalog.Insert: zero-padded 20-digit for integers
func (fke *ForeignKeyEnforcer) serializeValue(v interface{}) []byte {
	switch val := v.(type) {
	case string:
		return []byte("S:" + val)
	case int:
		return padIntKey(int64(val))
	case int64:
		return padIntKey(val)
	case float64:
		return padIntKey(int64(val))
	case []byte:
		return val
	case nil:
		return []byte("NULL")
	default:
		return []byte(ValueToStringKey(val))
	}
}

// padIntKey formats an int64 as a zero-padded 20-digit key without fmt.Sprintf.
func padIntKey(v int64) []byte {
	s := strconv.FormatInt(v, 10)
	if len(s) >= 20 {
		return []byte(s)
	}
	buf := make([]byte, 20)
	for i := range buf {
		buf[i] = '0'
	}
	copy(buf[20-len(s):], s)
	return buf
}

// deserializeValue deserializes a value from bytes
func (fke *ForeignKeyEnforcer) deserializeValue(data []byte) interface{} {
	str := string(data)

	// Handle "S:" prefix for string primary keys
	if len(str) > 2 && str[:2] == "S:" {
		return str[2:]
	}

	// Try to parse as int first (handles zero-padded format)
	var intVal int64
	if _, err := fmt.Sscanf(str, "%d", &intVal); err == nil {
		return int(intVal)
	}

	// Try float
	var floatVal float64
	if _, err := fmt.Sscanf(str, "%f", &floatVal); err == nil {
		return floatVal
	}

	// Return as string
	return str
}

// serializeCompositeKey serializes multiple values into a composite key
func (fke *ForeignKeyEnforcer) serializeCompositeKey(values []interface{}) []byte {
	var parts [][]byte
	for _, v := range values {
		parts = append(parts, fke.serializeValue(v))
	}

	// Join with a delimiter
	var result []byte
	for i, part := range parts {
		if i > 0 {
			result = append(result, 0x00) // Null byte delimiter
		}
		result = append(result, part...)
	}
	return result
}

// CheckForeignKeyConstraints checks all foreign key constraints for a table
// This can be used to validate the entire database after bulk operations
func (fke *ForeignKeyEnforcer) CheckForeignKeyConstraints(ctx context.Context, tableName string) error {
	// Get the table's BTree
	tree, exists := fke.catalog.tableTrees[tableName]
	if !exists {
		return nil // Table has no data
	}

	table, err := fke.catalog.getTableLocked(tableName)
	if err != nil {
		return err
	}

	// Check each row
	iter, err := tree.Scan([]byte{}, []byte{0xFF})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.HasNext() {
		_, value, err := iter.Next()
		if err != nil {
			return err
		}

		// Parse the row as a slice
		rowSlice, decErr := decodeRow(value, len(table.Columns))
		if decErr != nil {
			return decErr
		}

		// Convert to map for validation
		rowMap := make(map[string]interface{})
		for i, col := range table.Columns {
			if i < len(rowSlice) {
				rowMap[col.Name] = rowSlice[i]
			}
		}

		// Validate foreign keys
		if err := fke.ValidateInsert(ctx, tableName, rowMap); err != nil {
			return err
		}
	}

	return nil
}
