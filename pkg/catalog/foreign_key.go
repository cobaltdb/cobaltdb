package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrForeignKeyViolation = errors.New("foreign key constraint violation")
	ErrReferencedRowExists = errors.New("cannot delete or update: referenced row exists")
)

// ForeignKeyEnforcer handles foreign key constraint enforcement
type ForeignKeyEnforcer struct {
	catalog *Catalog
}

// NewForeignKeyEnforcer creates a new foreign key enforcer
func NewForeignKeyEnforcer(catalog *Catalog) *ForeignKeyEnforcer {
	return &ForeignKeyEnforcer{catalog: catalog}
}

// OnDelete handles foreign key actions when a row is deleted from the referenced table
func (fke *ForeignKeyEnforcer) OnDelete(ctx context.Context, tableName string, pkValue interface{}) error {
	// Find all tables that reference this table
	referencingTables := fke.findReferencingTables(tableName)

	for _, refInfo := range referencingTables {
		fk := refInfo.ForeignKey
		referencingTable := refInfo.TableName

		// Find rows in referencing table that reference the deleted row
		rows, err := fke.findReferencingRows(referencingTable, fk, pkValue)
		if err != nil {
			return err
		}

		// Apply the ON DELETE action
		switch fk.OnDelete {
		case "CASCADE":
			// Delete all referencing rows
			for _, rowKey := range rows {
				if err := fke.catalog.DeleteRow(referencingTable, rowKey); err != nil {
					return fmt.Errorf("cascade delete failed: %w", err)
				}
			}
		case "SET NULL":
			// Set foreign key columns to NULL
			for _, rowKey := range rows {
				if err := fke.setNull(ctx, referencingTable, rowKey, fk.Columns); err != nil {
					return fmt.Errorf("set null failed: %w", err)
				}
			}
		case "RESTRICT", "NO ACTION":
			// Prevent deletion if referencing rows exist
			if len(rows) > 0 {
				return ErrReferencedRowExists
			}
		default:
			// Default behavior is RESTRICT
			if len(rows) > 0 {
				return ErrReferencedRowExists
			}
		}
	}

	return nil
}

// OnUpdate handles foreign key actions when a primary key is updated in the referenced table
func (fke *ForeignKeyEnforcer) OnUpdate(ctx context.Context, tableName string, oldPkValue, newPkValue interface{}) error {
	// Find all tables that reference this table
	referencingTables := fke.findReferencingTables(tableName)

	for _, refInfo := range referencingTables {
		fk := refInfo.ForeignKey
		referencingTable := refInfo.TableName

		// Find rows in referencing table that reference the updated row
		rows, err := fke.findReferencingRows(referencingTable, fk, oldPkValue)
		if err != nil {
			return err
		}

		// Apply the ON UPDATE action
		switch fk.OnUpdate {
		case "CASCADE":
			// Update all referencing rows with the new value
			for _, rowKey := range rows {
				if err := fke.updateForeignKey(ctx, referencingTable, rowKey, fk.Columns, newPkValue); err != nil {
					return fmt.Errorf("cascade update failed: %w", err)
				}
			}
		case "SET NULL":
			// Set foreign key columns to NULL
			for _, rowKey := range rows {
				if err := fke.setNull(ctx, referencingTable, rowKey, fk.Columns); err != nil {
					return fmt.Errorf("set null failed: %w", err)
				}
			}
		case "RESTRICT", "NO ACTION":
			// Prevent update if referencing rows exist
			if len(rows) > 0 {
				return ErrReferencedRowExists
			}
		default:
			// Default behavior is RESTRICT
			if len(rows) > 0 {
				return ErrReferencedRowExists
			}
		}
	}

	return nil
}

// ValidateInsert validates that foreign key values reference existing rows
// row is a map from column name to value
func (fke *ForeignKeyEnforcer) ValidateInsert(ctx context.Context, tableName string, row map[string]interface{}) error {
	table, err := fke.catalog.GetTable(tableName)
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
	table, err := fke.catalog.GetTable(tableName)
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

// findReferencingRows finds all rows in a table that reference a specific value
// Returns the primary keys of matching rows
func (fke *ForeignKeyEnforcer) findReferencingRows(tableName string, fk ForeignKeyDef, pkValue interface{}) ([]interface{}, error) {
	var result []interface{}

	table, err := fke.catalog.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	// Get the table's BTree
	tree, exists := fke.catalog.tableTrees[tableName]
	if !exists {
		return result, nil // Table has no data
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

	// Iterate through all rows and find matching ones
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

		// Parse the row as a slice
		var row []interface{}
		if err := json.Unmarshal(value, &row); err != nil {
			return nil, err
		}

		// Check if this row references the deleted row
		matches := true
		for _, colIdx := range fkColIndices {
			if colIdx >= len(row) {
				matches = false
				break
			}

			rowVal := row[colIdx]
			expectedVal := pkValue

			if !fke.valuesEqual(rowVal, expectedVal) {
				matches = false
				break
			}
		}

		if matches {
			// Use the key as the primary key
			pk := fke.deserializeValue(key)
			result = append(result, pk)
		}
	}

	return result, nil
}

// referencedRowExists checks if a row exists in the referenced table
func (fke *ForeignKeyEnforcer) referencedRowExists(tableName string, columns []string, values []interface{}) (bool, error) {
	// Get the table's BTree
	tree, exists := fke.catalog.tableTrees[tableName]
	if !exists {
		return false, nil // Table has no data
	}

	// Build the key for lookup
	// For single column primary key, use the value directly
	// For composite keys, we'd need to serialize all values
	var key []byte
	if len(columns) == 1 {
		key = fke.serializeValue(values[0])
	} else {
		// Composite key - serialize all values
		key = fke.serializeCompositeKey(values)
	}

	// Check if the row exists
	_, err := tree.Get(key)
	if err != nil {
		// Check if error is key not found
		if err.Error() == "key not found" {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// setNull sets specified columns to NULL in a row
func (fke *ForeignKeyEnforcer) setNull(ctx context.Context, tableName string, rowKey interface{}, columns []string) error {
	// Get the current row data
	rowData, err := fke.getRowSlice(tableName, rowKey)
	if err != nil {
		return err
	}

	table, err := fke.catalog.GetTable(tableName)
	if err != nil {
		return err
	}

	// Set columns to NULL by index
	for _, col := range columns {
		colIdx := table.GetColumnIndex(col)
		if colIdx >= 0 && colIdx < len(rowData) {
			rowData[colIdx] = nil
		}
	}

	// Update the row
	return fke.updateRowSlice(tableName, rowKey, rowData)
}

// updateForeignKey updates foreign key columns with a new value
func (fke *ForeignKeyEnforcer) updateForeignKey(ctx context.Context, tableName string, rowKey interface{}, columns []string, newValue interface{}) error {
	// Get the current row data
	rowData, err := fke.getRowSlice(tableName, rowKey)
	if err != nil {
		return err
	}

	table, err := fke.catalog.GetTable(tableName)
	if err != nil {
		return err
	}

	// Update foreign key columns by index
	for _, col := range columns {
		colIdx := table.GetColumnIndex(col)
		if colIdx >= 0 && colIdx < len(rowData) {
			rowData[colIdx] = newValue
		}
	}

	// Update the row
	return fke.updateRowSlice(tableName, rowKey, rowData)
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

	var row []interface{}
	if err := json.Unmarshal(value, &row); err != nil {
		return nil, err
	}

	return row, nil
}

// updateRowSlice updates a row from a slice
func (fke *ForeignKeyEnforcer) updateRowSlice(tableName string, rowKey interface{}, rowData []interface{}) error {
	tree, exists := fke.catalog.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	key := fke.serializeValue(rowKey)
	value, err := json.Marshal(rowData)
	if err != nil {
		return err
	}

	tree.Put(key, value)
	return nil
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
		return []byte(val)
	case int:
		return []byte(fmt.Sprintf("%020d", int64(val)))
	case int64:
		return []byte(fmt.Sprintf("%020d", val))
	case float64:
		return []byte(fmt.Sprintf("%f", val))
	case []byte:
		return val
	case nil:
		return []byte("NULL")
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
}

// deserializeValue deserializes a value from bytes
func (fke *ForeignKeyEnforcer) deserializeValue(data []byte) interface{} {
	str := string(data)

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

	table, err := fke.catalog.GetTable(tableName)
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
		var rowSlice []interface{}
		if err := json.Unmarshal(value, &rowSlice); err != nil {
			return err
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
