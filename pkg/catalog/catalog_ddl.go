package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"regexp"
	"strings"
	"time"
)

// validIdentifierName checks if a table or column name is valid
// Names must start with a letter or underscore, contain only alphanumeric characters and underscores,
// and not exceed 64 characters (SQL standard limit)
var validIdentifierName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)

// stripQuotes removes surrounding double quotes, backticks, or square brackets from an identifier
func stripQuotes(name string) string {
	if len(name) >= 2 {
		if (name[0] == '"' && name[len(name)-1] == '"') ||
			(name[0] == '`' && name[len(name)-1] == '`') ||
			(name[0] == '[' && name[len(name)-1] == ']') {
			return name[1 : len(name)-1]
		}
	}
	return name
}

// validateTableName validates a table name
func validateTableName(name string) error {
	if name == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	stripped := stripQuotes(name)
	if !validIdentifierName.MatchString(stripped) {
		return fmt.Errorf("invalid table name %q: must start with letter or underscore, contain only alphanumeric characters and underscores, and be 1-64 characters", stripped)
	}
	if name == stripped && isReservedWord(name) {
		return fmt.Errorf("table name %q is a reserved word", name)
	}
	return nil
}

// validateColumnName validates a column name
func validateColumnName(name string) error {
	if name == "" {
		return fmt.Errorf("column name cannot be empty")
	}
	stripped := stripQuotes(name)
	if !validIdentifierName.MatchString(stripped) {
		return fmt.Errorf("invalid column name %q: must start with letter or underscore, contain only alphanumeric characters and underscores, and be 1-64 characters", stripped)
	}
	if name == stripped && isReservedWord(name) {
		return fmt.Errorf("column name %q is a reserved word", name)
	}
	return nil
}

var reservedWords = map[string]bool{
	"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true,
	"CREATE": true, "DROP": true, "ALTER": true, "TABLE": true,
	"INDEX": true, "VIEW": true, "TRIGGER": true, "PROCEDURE": true,
	"FROM": true, "WHERE": true, "JOIN": true, "ON": true, "AND": true,
	"OR": true, "NOT": true, "IN": true, "BETWEEN": true, "LIKE": true,
	"NULL": true, "TRUE": true, "FALSE": true, "DEFAULT": true,
	"PRIMARY": true, "FOREIGN": true, "REFERENCES": true,
	"UNIQUE": true, "CHECK": true, "CONSTRAINT": true,
	"COLUMN": true, "RENAME": true, "TO": true, "AS": true,
	"ORDER": true, "BY": true, "GROUP": true, "HAVING": true,
	"LIMIT": true, "OFFSET": true, "UNION": true, "INTERSECT": true,
	"EXCEPT": true, "ALL": true, "DISTINCT": true,
	"CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
	"CAST": true, "INTO": true, "VALUES": true,
	"INNER": true, "LEFT": true, "RIGHT": true, "OUTER": true,
	"CROSS": true, "NATURAL": true, "USING": true,
	"EXISTS": true, "ROLLBACK": true, "COMMIT": true, "TRANSACTION": true,
	"IS": true, "INTEGER": true, "TEXT": true, "REAL": true, "BLOB": true,
	"BOOLEAN": true, "JSON": true, "RETURNING": true,
}

func isReservedWord(name string) bool {
	return reservedWords[strings.ToUpper(name)]
}

func (c *Catalog) CreateTable(stmt *query.CreateTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate table name
	if err := validateTableName(stmt.Table); err != nil {
		return err
	}

	if _, exists := c.tables[stmt.Table]; exists {
		if stmt.IfNotExists {
			return nil // Table already exists, silently succeed
		}
		return ErrTableExists
	}
	if _, exists := c.foreignTables[stmt.Table]; exists {
		if stmt.IfNotExists {
			return nil
		}
		return ErrTableExists
	}

	// Create new B+Tree for the table's data
	tree, err := btree.NewBTree(c.pool)
	if err != nil {
		return err
	}

	tableDef := &TableDef{
		Name:        stmt.Table,
		Type:        "table",
		Columns:     make([]ColumnDef, len(stmt.Columns)),
		CreatedAt:   time.Now().UnixNano(),
		RootPageID:  tree.RootPageID(),
		ForeignKeys: make([]ForeignKeyDef, len(stmt.ForeignKeys)),
	}

	// Handle partitioning if specified
	if stmt.Partition != nil {
		partitionInfo := &PartitionInfo{
			Type:   stmt.Partition.Type,
			Column: stmt.Partition.Column,
		}
		if stmt.Partition.NumPartitions > 0 {
			partitionInfo.NumParts = stmt.Partition.NumPartitions
		}
		// Convert SinglePartition to PartitionDef
		// For RANGE partitioning with LESS THAN, the value is the max bound
		var prevMax int64 = -9223372036854775808 // Min int64 for first partition
		for _, sp := range stmt.Partition.Partitions {
			pd := PartitionDef{Name: sp.Name}
			// Extract max value from Values expressions (for RANGE LESS THAN)
			if len(sp.Values) >= 1 {
				if nl, ok := sp.Values[0].(*query.NumberLiteral); ok {
					pd.MaxValue = int64(nl.Value)
					pd.MinValue = prevMax
					prevMax = pd.MaxValue
				}
			}
			partitionInfo.Partitions = append(partitionInfo.Partitions, pd)
		}

		// For HASH/LIST/KEY partitioning with NumPartitions but no explicit definitions,
		// auto-generate partition names
		if len(partitionInfo.Partitions) == 0 && partitionInfo.NumParts > 0 {
			for i := 0; i < partitionInfo.NumParts; i++ {
				pd := PartitionDef{
					Name: fmt.Sprintf("p%d", i),
				}
				partitionInfo.Partitions = append(partitionInfo.Partitions, pd)
			}
		}

		tableDef.Partition = partitionInfo

		// Create partition trees immediately
		for _, pd := range partitionInfo.Partitions {
			partTreeName := stmt.Table + ":" + pd.Name
			partTree, err := btree.NewBTree(c.pool)
			if err != nil {
				return fmt.Errorf("failed to create partition tree for %s: %w", partTreeName, err)
			}
			c.tableTrees[partTreeName] = partTree
		}
	}

	for i, col := range stmt.Columns {
		tableDef.Columns[i] = ColumnDef{
			Name:          col.Name,
			Type:          tokenTypeToColumnType(col.Type),
			NotNull:       col.NotNull,
			Unique:        col.Unique,
			PrimaryKey:    col.PrimaryKey,
			AutoIncrement: col.AutoIncrement,
			Default:       exprToSQL(col.Default),
			CheckStr:      exprToSQL(col.Check),
			Check:         col.Check,
			defaultExpr:   col.Default,
			Dimensions:    col.Dimensions,
		}
		if col.PrimaryKey {
			tableDef.PrimaryKey = append(tableDef.PrimaryKey, col.Name)
			tableDef.Columns[i].NotNull = true // PRIMARY KEY implies NOT NULL
		}
	}

	// Handle table-level PRIMARY KEY (for composite PK).
	// Dedupe: a column may appear in both col.PrimaryKey and stmt.PrimaryKey
	// when ASTs are constructed by hand (and harmlessly in some parser
	// edge cases). Without dedup, ["id"] + ["id"] becomes a bogus composite.
	if len(stmt.PrimaryKey) > 0 {
		seen := make(map[string]bool, len(tableDef.PrimaryKey))
		for _, pk := range tableDef.PrimaryKey {
			seen[strings.ToLower(pk)] = true
		}
		for _, pkCol := range stmt.PrimaryKey {
			if !seen[strings.ToLower(pkCol)] {
				tableDef.PrimaryKey = append(tableDef.PrimaryKey, pkCol)
				seen[strings.ToLower(pkCol)] = true
			}
			// Mark the column as PK/NOT NULL regardless of whether it was a
			// dup, so the column flag stays in sync with the PK list.
			for i, col := range tableDef.Columns {
				if strings.EqualFold(col.Name, pkCol) {
					tableDef.Columns[i].NotNull = true
					tableDef.Columns[i].PrimaryKey = true
					break
				}
			}
		}
	}

	// Copy foreign key definitions
	for i, fk := range stmt.ForeignKeys {
		tableDef.ForeignKeys[i] = ForeignKeyDef{
			Columns:           fk.Columns,
			ReferencedTable:   fk.ReferencedTable,
			ReferencedColumns: fk.ReferencedColumns,
			OnDelete:          fk.OnDelete,
			OnUpdate:          fk.OnUpdate,
		}
	}

	c.tables[stmt.Table] = tableDef
	c.tableTrees[stmt.Table] = tree // Store the tree for data operations

	// Build column index cache for performance
	tableDef.buildColumnIndexCache()

	// Record DDL undo entry for transaction rollback
	if c.txnActive {
		c.undoLog = append(c.undoLog, undoEntry{
			action:    undoCreateTable,
			tableName: stmt.Table,
		})
	}

	// Store table definition in catalog tree
	return c.storeTableDef(tableDef)
}

func (c *Catalog) storeTableDef(table *TableDef) error {
	key := []byte("tbl:" + table.Name)
	data, err := json.Marshal(table)
	if err != nil {
		return err
	}

	if c.tree != nil {
		return c.tree.Put(key, data)
	}
	return nil
}

func (c *Catalog) DropTable(stmt *query.DropTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !stmt.IfExists {
		if _, exists := c.tables[stmt.Table]; !exists {
			if _, fexists := c.foreignTables[stmt.Table]; !fexists {
				return ErrTableNotFound
			}
		}
	}

	// Drop foreign table if present
	if _, fexists := c.foreignTables[stmt.Table]; fexists {
		delete(c.foreignTables, stmt.Table)
		return nil
	}

	// Check if table actually exists before trying to delete
	tableDef, exists := c.tables[stmt.Table]

	// Record DDL undo entry for transaction rollback before deleting
	if c.txnActive && exists {
		entry := undoEntry{
			action:        undoDropTable,
			tableName:     stmt.Table,
			tableDef:      tableDef,
			tableTree:     c.tableTrees[stmt.Table],
			tableIndexes:  make(map[string]*IndexDef),
			tableIdxTrees: make(map[string]*btree.BTree),
		}
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName == stmt.Table {
				entry.tableIndexes[idxName] = idxDef
				if tree, ok := c.indexTrees[idxName]; ok {
					entry.tableIdxTrees[idxName] = tree
				}
			}
		}
		c.undoLog = append(c.undoLog, entry)
	}

	// Clean up table data B-tree
	delete(c.tableTrees, stmt.Table)

	// Clean up indexes associated with this table
	if tableDef != nil {
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName == stmt.Table {
				delete(c.indexes, idxName)
				delete(c.indexTrees, idxName)
			}
		}
	}

	// Clean up views that reference this table (triggers, FTS indexes, stats)
	delete(c.stats, stmt.Table)
	delete(c.tables, stmt.Table)

	// Remove from catalog tree only if the table existed
	if c.tree != nil && exists {
		key := []byte("tbl:" + stmt.Table)
		return c.tree.Delete(key)
	}
	return nil
}

func (c *Catalog) AlterTableAddColumn(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate table name
	if err := validateTableName(stmt.Table); err != nil {
		return err
	}

	// Validate column name
	if err := validateColumnName(stmt.Column.Name); err != nil {
		return err
	}

	table, exists := c.tables[stmt.Table]
	if !exists {
		return ErrTableNotFound
	}

	// Check if column already exists
	for _, col := range table.Columns {
		if col.Name == stmt.Column.Name {
			return fmt.Errorf("column %s already exists in table %s", stmt.Column.Name, stmt.Table)
		}
	}

	// Save undo entry before modification
	if c.txnActive {
		oldCols := make([]ColumnDef, len(table.Columns))
		copy(oldCols, table.Columns)
		c.undoLog = append(c.undoLog, undoEntry{
			action:     undoAlterAddColumn,
			tableName:  stmt.Table,
			oldColumns: oldCols,
		})
	}

	newCol := ColumnDef{
		Name:          stmt.Column.Name,
		Type:          tokenTypeToColumnType(stmt.Column.Type),
		NotNull:       stmt.Column.NotNull,
		Unique:        stmt.Column.Unique,
		PrimaryKey:    stmt.Column.PrimaryKey,
		AutoIncrement: stmt.Column.AutoIncrement,
		Default:       exprToSQL(stmt.Column.Default),
		CheckStr:      exprToSQL(stmt.Column.Check),
		Check:         stmt.Column.Check,
		defaultExpr:   stmt.Column.Default,
		Dimensions:    stmt.Column.Dimensions,
	}

	table.Columns = append(table.Columns, newCol)
	table.buildColumnIndexCache()

	// Backfill existing rows with the default value for the new column
	tree, treeExists := c.tableTrees[stmt.Table]
	if treeExists {
		// Compute default value
		var defaultVal interface{}
		if newCol.defaultExpr != nil {
			defaultVal, _ = evaluateExpression(c, nil, nil, newCol.defaultExpr, nil)
		}

		// Remember the old column count before adding the new column
		oldColCount := len(table.Columns) - 1

		// Scan all rows and append the default value
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("failed to scan table for ALTER TABLE: %w", err)
		}
		defer iter.Close()
		type rowUpdate struct {
			key  []byte
			data []byte
		}
		var updates []rowUpdate
		for iter.HasNext() {
			key, valueData, err := iter.Next()
			if err != nil {
				break
			}
			// Use oldColCount to decode to avoid automatic padding with nil
			vrow, err := decodeVersionedRow(valueData, oldColCount)
			if err != nil {
				continue
			}
			values := vrow.Data
			// Only update rows that are missing the new column
			if len(values) <= oldColCount {
				for len(values) < len(table.Columns) {
					values = append(values, defaultVal)
				}
				// Update the VersionedRow and re-encode
				vrow.Data = values
				newData, err := json.Marshal(vrow)
				if err != nil {
					continue
				}
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				updates = append(updates, rowUpdate{key: keyCopy, data: newData})
			}
		}

		// Apply updates
		for _, u := range updates {
			if err := tree.Put(u.key, u.data); err != nil {
				return fmt.Errorf("failed to update row during ALTER TABLE backfill: %w", err)
			}
		}
	}

	// Store updated table definition
	return c.storeTableDef(table)
}

func (c *Catalog) AlterTableDropColumn(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate table name
	if err := validateTableName(stmt.Table); err != nil {
		return err
	}

	table, exists := c.tables[stmt.Table]
	if !exists {
		return ErrTableNotFound
	}

	// Column name to drop can be in Column.Name (from parser) or NewName (legacy/tests)
	colName := stmt.Column.Name
	if colName == "" {
		colName = stmt.NewName
	}

	// Validate column name
	if err := validateColumnName(colName); err != nil {
		return err
	}
	colIdx := -1
	for i, col := range table.Columns {
		if strings.EqualFold(col.Name, colName) {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return fmt.Errorf("column '%s' does not exist in table '%s'", colName, stmt.Table)
	}

	// Save original column count before modification
	originalColCount := len(table.Columns)

	// Cannot drop primary key column
	if table.isPrimaryKeyColumn(table.Columns[colIdx].Name) {
		return fmt.Errorf("cannot drop PRIMARY KEY column '%s'", colName)
	}

	// Save undo entry before modification
	if c.txnActive {
		oldCols := make([]ColumnDef, len(table.Columns))
		copy(oldCols, table.Columns)
		entry := undoEntry{
			action:          undoAlterDropColumn,
			tableName:       stmt.Table,
			oldColumns:      oldCols,
			droppedIndexes:  make(map[string]*IndexDef),
			droppedIdxTrees: make(map[string]*btree.BTree),
		}
		// Save original row data before modification
		if tree, treeExists := c.tableTrees[stmt.Table]; treeExists {
			iter, err := tree.Scan(nil, nil)
			if err != nil {
				return fmt.Errorf("failed to scan table for ALTER TABLE DROP COLUMN: %w", err)
			}
			defer iter.Close()
			for iter.HasNext() {
				key, val, err := iter.Next()
				if err != nil {
					break
				}
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				valCopy := make([]byte, len(val))
				copy(valCopy, val)
				entry.oldRowData = append(entry.oldRowData, struct{ key, val []byte }{keyCopy, valCopy})
			}
		}
		// Save indexes that will be dropped
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName == stmt.Table {
				for _, idxCol := range idxDef.Columns {
					if strings.EqualFold(idxCol, colName) {
						entry.droppedIndexes[idxName] = idxDef
						if idxTree, ok := c.indexTrees[idxName]; ok {
							entry.droppedIdxTrees[idxName] = idxTree
						}
						break
					}
				}
			}
		}
		c.undoLog = append(c.undoLog, entry)
	}

	// Remove column from definition
	table.Columns = append(table.Columns[:colIdx], table.Columns[colIdx+1:]...)
	table.buildColumnIndexCache()

	// Update all existing rows - remove the dropped column's data
	tree, exists := c.tableTrees[stmt.Table]
	if exists {
		var updates []struct {
			key []byte
			val []byte
		}
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("failed to scan table for ALTER TABLE DROP COLUMN backfill: %w", err)
		}
		defer iter.Close()
		for iter.HasNext() {
			key, valueData, err := iter.Next()
			if err != nil {
				break
			}
			row, err := decodeRow(valueData, originalColCount)
			if err != nil {
				continue
			}
			if colIdx < len(row) {
				row = append(row[:colIdx], row[colIdx+1:]...)
			}
			newData, err := json.Marshal(row)
			if err != nil {
				continue
			}
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			updates = append(updates, struct {
				key []byte
				val []byte
			}{keyCopy, newData})
		}
		for _, u := range updates {
			if err := tree.Put(u.key, u.val); err != nil {
				return fmt.Errorf("failed to update row after column drop: %w", err)
			}
		}
	}

	// Drop any indexes on the dropped column
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName == stmt.Table {
			for _, idxCol := range idxDef.Columns {
				if strings.EqualFold(idxCol, colName) {
					delete(c.indexes, idxName)
					delete(c.indexTrees, idxName)
					break
				}
			}
		}
	}

	return c.storeTableDef(table)
}

func (c *Catalog) AlterTableRename(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate old table name
	if err := validateTableName(stmt.Table); err != nil {
		return err
	}

	// Validate new table name
	if err := validateTableName(stmt.NewName); err != nil {
		return err
	}

	table, exists := c.tables[stmt.Table]
	if !exists {
		return ErrTableNotFound
	}

	if _, exists := c.tables[stmt.NewName]; exists {
		return fmt.Errorf("table '%s' already exists", stmt.NewName)
	}

	// Save undo entry before modification
	if c.txnActive {
		c.undoLog = append(c.undoLog, undoEntry{
			action:    undoAlterRename,
			tableName: stmt.Table,
			oldName:   stmt.Table,
			newName:   stmt.NewName,
		})
	}

	// Update table name in all maps
	delete(c.tables, stmt.Table)
	c.tables[stmt.NewName] = table

	if tree, exists := c.tableTrees[stmt.Table]; exists {
		delete(c.tableTrees, stmt.Table)
		c.tableTrees[stmt.NewName] = tree
	}

	// Update index references
	for _, idxDef := range c.indexes {
		if idxDef.TableName == stmt.Table {
			idxDef.TableName = stmt.NewName
		}
	}

	// Update stats
	if stats, exists := c.stats[stmt.Table]; exists {
		delete(c.stats, stmt.Table)
		c.stats[stmt.NewName] = stats
	}

	table.Name = stmt.NewName

	// Persist renamed table to catalog B-tree
	if c.tree != nil {
		// Best-effort delete of old entry (may not exist if table was only in-memory)
		_ = c.tree.Delete([]byte("tbl:" + stmt.Table))
	}
	if err := c.storeTableDef(table); err != nil {
		return fmt.Errorf("failed to persist renamed table: %w", err)
	}

	return nil
}

func (c *Catalog) AlterTableRenameColumn(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate table name
	if err := validateTableName(stmt.Table); err != nil {
		return err
	}

	// Validate old column name
	if err := validateColumnName(stmt.OldName); err != nil {
		return err
	}

	// Validate new column name
	if err := validateColumnName(stmt.NewName); err != nil {
		return err
	}

	table, exists := c.tables[stmt.Table]
	if !exists {
		return ErrTableNotFound
	}

	found := false
	for i, col := range table.Columns {
		if strings.EqualFold(col.Name, stmt.OldName) {
			// Save undo entry before modification
			if c.txnActive {
				c.undoLog = append(c.undoLog, undoEntry{
					action:               undoAlterRenameColumn,
					tableName:            stmt.Table,
					oldName:              stmt.OldName,
					newName:              stmt.NewName,
					oldPrimaryKeyColumns: append([]string{}, table.PrimaryKey...),
				})
			}
			table.Columns[i].Name = stmt.NewName
			found = true
			// Update primary key reference if needed
			// Update PK column names if needed
			for i, pkCol := range table.PrimaryKey {
				if strings.EqualFold(pkCol, stmt.OldName) {
					table.PrimaryKey[i] = stmt.NewName
				}
			}
			break
		}
	}
	if !found {
		return fmt.Errorf("column '%s' does not exist in table '%s'", stmt.OldName, stmt.Table)
	}

	table.buildColumnIndexCache()

	// Update index column references
	for _, idxDef := range c.indexes {
		if idxDef.TableName == stmt.Table {
			for i, idxCol := range idxDef.Columns {
				if strings.EqualFold(idxCol, stmt.OldName) {
					idxDef.Columns[i] = stmt.NewName
				}
			}
		}
	}

	return c.storeTableDef(table)
}

func (c *Catalog) GetTable(name string) (*TableDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTableLocked(name)
}

func (c *Catalog) getTableLocked(name string) (*TableDef, error) {
	table, exists := c.tables[name]
	if !exists {
		// Check if it's a foreign table - return a synthetic TableDef
		if ft, ok := c.foreignTables[name]; ok {
			synthetic := &TableDef{
				Name:    ft.TableName,
				Type:    "foreign",
				Columns: ft.Columns,
			}
			return synthetic, nil
		}
		return nil, ErrTableNotFound
	}
	return table, nil
}

func (c *Catalog) CreateView(name string, query *query.SelectStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.views[name]; exists {
		return ErrTableExists
	}
	if _, exists := c.tables[name]; exists {
		return ErrTableExists
	}
	c.views[name] = query
	return nil
}

func (c *Catalog) GetView(name string) (*query.SelectStmt, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getViewLocked(name)
}

// getViewLocked is the lock-free internal version. Must be called with mu held.
func (c *Catalog) getViewLocked(name string) (*query.SelectStmt, error) {
	view, exists := c.views[name]
	if !exists {
		return nil, ErrTableNotFound
	}
	return view, nil
}

func (c *Catalog) DropView(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.views[name]; !exists {
		return ErrTableNotFound
	}
	delete(c.views, name)
	return nil
}

func (c *Catalog) HasTableOrView(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, tableExists := c.tables[name]
	_, viewExists := c.views[name]
	return tableExists || viewExists
}

func (c *Catalog) CreateTrigger(stmt *query.CreateTriggerStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Check if table or view exists
	_, tableExists := c.tables[stmt.Table]
	_, viewExists := c.views[stmt.Table]
	if !tableExists && !viewExists {
		return fmt.Errorf("table or view not found: %s", stmt.Table)
	}

	if _, exists := c.triggers[stmt.Name]; exists {
		return fmt.Errorf("trigger %s already exists", stmt.Name)
	}
	c.triggers[stmt.Name] = stmt
	return nil
}

func (c *Catalog) GetTrigger(name string) (*query.CreateTriggerStmt, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	trigger, exists := c.triggers[name]
	if !exists {
		return nil, fmt.Errorf("trigger %s not found", name)
	}
	return trigger, nil
}

func (c *Catalog) DropTrigger(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.triggers[name]; !exists {
		return fmt.Errorf("trigger %s not found", name)
	}
	delete(c.triggers, name)
	return nil
}

func (c *Catalog) GetTriggersForTable(tableName string, event string) []*query.CreateTriggerStmt {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTriggersForTableLocked(tableName, event)
}

// getTriggersForTableLocked is the lock-free internal version. Must be called with mu held.
func (c *Catalog) getTriggersForTableLocked(tableName string, event string) []*query.CreateTriggerStmt {
	var result []*query.CreateTriggerStmt
	for _, trigger := range c.triggers {
		if trigger.Table == tableName && (event == "" || trigger.Event == event) {
			result = append(result, trigger)
		}
	}
	return result
}

func (c *Catalog) executeTriggers(ctx context.Context, tableName string, event string, timing string, newRow []interface{}, oldRow []interface{}, columns []ColumnDef) error {
	triggers := c.getTriggersForTableLocked(tableName, event)
	for _, trigger := range triggers {
		if trigger.Time != timing {
			continue
		}
		if len(trigger.Body) == 0 {
			continue
		}

		// Evaluate WHEN condition if present
		if trigger.Condition != nil {
			resolvedCond := c.resolveTriggerExpr(trigger.Condition, newRow, oldRow, columns)
			result, err := evaluateExpression(c, nil, nil, resolvedCond, nil)
			if err != nil {
				continue // Condition evaluation error - skip trigger
			}
			if result == nil {
				continue // NULL condition - skip trigger
			}
			if b, ok := result.(bool); ok && !b {
				continue // false condition - skip trigger
			}
			// For numeric results, 0 = false
			if f, ok := toFloat64(result); ok && f == 0 {
				continue
			}
		}

		// Execute each statement in the trigger body
		for _, bodyStmt := range trigger.Body {
			// Substitute NEW.col and OLD.col references with actual values
			resolved := c.resolveTriggerRefs(bodyStmt, newRow, oldRow, columns)
			// Execute the resolved statement
			if err := c.executeTriggerStatement(ctx, resolved); err != nil {
				return fmt.Errorf("trigger %s: %w", trigger.Name, err)
			}
		}
	}
	return nil
}

func (c *Catalog) executeTriggerStatement(ctx context.Context, stmt query.Statement) error {
	switch s := stmt.(type) {
	case *query.InsertStmt:
		_, _, err := c.insertLocked(ctx, s, nil)
		return err
	case *query.UpdateStmt:
		_, _, err := c.updateLocked(ctx, s, nil)
		return err
	case *query.DeleteStmt:
		_, _, err := c.deleteLocked(ctx, s, nil)
		return err
	default:
		return fmt.Errorf("unsupported trigger statement type: %T", stmt)
	}
}

func (c *Catalog) resolveTriggerRefs(stmt query.Statement, newRow []interface{}, oldRow []interface{}, columns []ColumnDef) query.Statement {
	switch s := stmt.(type) {
	case *query.InsertStmt:
		resolved := *s
		resolved.Values = make([][]query.Expression, len(s.Values))
		for i, row := range s.Values {
			resolved.Values[i] = make([]query.Expression, len(row))
			for j, expr := range row {
				resolved.Values[i][j] = c.resolveTriggerExpr(expr, newRow, oldRow, columns)
			}
		}
		return &resolved
	case *query.UpdateStmt:
		resolved := *s
		resolved.Set = make([]*query.SetClause, len(s.Set))
		for i, sc := range s.Set {
			newSc := *sc
			newSc.Value = c.resolveTriggerExpr(sc.Value, newRow, oldRow, columns)
			resolved.Set[i] = &newSc
		}
		if s.Where != nil {
			resolved.Where = c.resolveTriggerExpr(s.Where, newRow, oldRow, columns)
		}
		return &resolved
	case *query.DeleteStmt:
		resolved := *s
		if s.Where != nil {
			resolved.Where = c.resolveTriggerExpr(s.Where, newRow, oldRow, columns)
		}
		return &resolved
	}
	return stmt
}

func (c *Catalog) resolveTriggerExpr(expr query.Expression, newRow []interface{}, oldRow []interface{}, columns []ColumnDef) query.Expression {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *query.QualifiedIdentifier:
		tbl := strings.ToUpper(e.Table)
		if tbl == "NEW" && newRow != nil {
			for i, col := range columns {
				if strings.EqualFold(col.Name, e.Column) && i < len(newRow) {
					return valueToLiteral(newRow[i])
				}
			}
		} else if tbl == "OLD" && oldRow != nil {
			for i, col := range columns {
				if strings.EqualFold(col.Name, e.Column) && i < len(oldRow) {
					return valueToLiteral(oldRow[i])
				}
			}
		}
		return e
	case *query.BinaryExpr:
		return &query.BinaryExpr{
			Left:     c.resolveTriggerExpr(e.Left, newRow, oldRow, columns),
			Operator: e.Operator,
			Right:    c.resolveTriggerExpr(e.Right, newRow, oldRow, columns),
		}
	case *query.UnaryExpr:
		return &query.UnaryExpr{
			Operator: e.Operator,
			Expr:     c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
		}
	case *query.FunctionCall:
		newArgs := make([]query.Expression, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = c.resolveTriggerExpr(arg, newRow, oldRow, columns)
		}
		return &query.FunctionCall{Name: e.Name, Args: newArgs, Distinct: e.Distinct}
	case *query.CaseExpr:
		newCase := &query.CaseExpr{}
		if e.Expr != nil {
			newCase.Expr = c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns)
		}
		newCase.Whens = make([]*query.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			newCase.Whens[i] = &query.WhenClause{
				Condition: c.resolveTriggerExpr(w.Condition, newRow, oldRow, columns),
				Result:    c.resolveTriggerExpr(w.Result, newRow, oldRow, columns),
			}
		}
		if e.Else != nil {
			newCase.Else = c.resolveTriggerExpr(e.Else, newRow, oldRow, columns)
		}
		return newCase
	case *query.BetweenExpr:
		return &query.BetweenExpr{
			Expr:  c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
			Lower: c.resolveTriggerExpr(e.Lower, newRow, oldRow, columns),
			Upper: c.resolveTriggerExpr(e.Upper, newRow, oldRow, columns),
			Not:   e.Not,
		}
	case *query.InExpr:
		newList := make([]query.Expression, len(e.List))
		for i, v := range e.List {
			newList[i] = c.resolveTriggerExpr(v, newRow, oldRow, columns)
		}
		return &query.InExpr{
			Expr:     c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
			List:     newList,
			Not:      e.Not,
			Subquery: e.Subquery,
		}
	case *query.IsNullExpr:
		return &query.IsNullExpr{
			Expr: c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
			Not:  e.Not,
		}
	case *query.CastExpr:
		return &query.CastExpr{
			Expr:     c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
			DataType: e.DataType,
		}
	case *query.LikeExpr:
		return &query.LikeExpr{
			Expr:    c.resolveTriggerExpr(e.Expr, newRow, oldRow, columns),
			Pattern: c.resolveTriggerExpr(e.Pattern, newRow, oldRow, columns),
			Not:     e.Not,
		}
	}
	return expr
}

func (c *Catalog) CreateProcedure(stmt *query.CreateProcedureStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.procedures[stmt.Name]; exists {
		return fmt.Errorf("procedure %s already exists", stmt.Name)
	}
	c.procedures[stmt.Name] = stmt
	return nil
}

func (c *Catalog) GetProcedure(name string) (*query.CreateProcedureStmt, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	proc, exists := c.procedures[name]
	if !exists {
		return nil, fmt.Errorf("procedure %s not found", name)
	}
	return proc, nil
}

func (c *Catalog) DropProcedure(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.procedures[name]; !exists {
		return fmt.Errorf("procedure %s not found", name)
	}
	delete(c.procedures, name)
	return nil
}

func (c *Catalog) GetTableStats(tableName string) (*StatsTableStats, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	stats, exists := c.stats[tableName]
	if !exists {
		return nil, fmt.Errorf("no statistics for table %s", tableName)
	}
	return stats, nil
}

// findInsteadOfTrigger finds an INSTEAD OF trigger for a table/view and event
func (c *Catalog) findInsteadOfTrigger(tableName string, event string) *query.CreateTriggerStmt {
	for _, trigger := range c.triggers {
		if trigger.Table == tableName && trigger.Event == event && trigger.Time == "INSTEAD OF" {
			return trigger
		}
	}
	return nil
}

// getColumnsForTableOrView returns column definitions for a table or view
func (c *Catalog) getColumnsForTableOrView(name string) []ColumnDef {
	// Try table first
	if table, err := c.getTableLocked(name); err == nil {
		return table.Columns
	}
	// Try view
	if view, err := c.getViewLocked(name); err == nil {
		// Get column names from view's SELECT
		cols := make([]ColumnDef, len(view.Columns))
		for i, col := range view.Columns {
			colName := ""
			switch c := col.(type) {
			case *query.Identifier:
				colName = c.Name
			case *query.AliasExpr:
				if c.Alias != "" {
					colName = c.Alias
				} else if id, ok := c.Expr.(*query.Identifier); ok {
					colName = id.Name
				}
			case *query.QualifiedIdentifier:
				colName = c.Column
			}
			if colName == "" {
				colName = fmt.Sprintf("column_%d", i)
			}
			cols[i] = ColumnDef{Name: colName, Type: "TEXT"}
		}
		return cols
	}
	return nil
}

// executeInsteadOfTrigger executes an INSTEAD OF trigger for an INSERT statement
func (c *Catalog) executeInsteadOfTrigger(ctx context.Context, trigger *query.CreateTriggerStmt, stmt *query.InsertStmt, args []interface{}) (int64, int64, error) {
	// For each row being inserted, execute the trigger body
	rowsAffected := int64(0)

	// Get columns for resolving NEW. references
	columns := c.getColumnsForTableOrView(stmt.Table)

	valueRows := stmt.Values
	if stmt.Select != nil {
		// Handle INSERT...SELECT
		_, selectRows, err := c.selectLocked(stmt.Select, args)
		if err != nil {
			return 0, 0, fmt.Errorf("INSERT...SELECT failed: %w", err)
		}
		// Convert select results to expression rows
		valueRows = make([][]query.Expression, len(selectRows))
		for i, row := range selectRows {
			exprRow := make([]query.Expression, len(row))
			for j, val := range row {
				switch v := val.(type) {
				case nil:
					exprRow[j] = &query.NullLiteral{}
				case string:
					exprRow[j] = &query.StringLiteral{Value: v}
				case float64:
					exprRow[j] = &query.NumberLiteral{Value: v}
				case int64:
					exprRow[j] = &query.NumberLiteral{Value: float64(v), Raw: fmt.Sprintf("%d", v)}
				case int:
					exprRow[j] = &query.NumberLiteral{Value: float64(v), Raw: fmt.Sprintf("%d", v)}
				case bool:
					exprRow[j] = &query.BooleanLiteral{Value: v}
				default:
					exprRow[j] = &query.StringLiteral{Value: fmt.Sprintf("%v", v)}
				}
			}
			valueRows[i] = exprRow
		}
	}

	for _, valueRow := range valueRows {
		// Build the NEW row from the insert values
		newRow := make([]interface{}, len(valueRow))
		for i, expr := range valueRow {
			val, _ := evaluateExpression(c, nil, nil, expr, args)
			newRow[i] = val
		}

		// Execute trigger body
		for _, bodyStmt := range trigger.Body {
			// Resolve NEW. references
			resolved := c.resolveTriggerRefs(bodyStmt, newRow, nil, columns)
			if err := c.executeTriggerStatement(ctx, resolved); err != nil {
				return 0, 0, fmt.Errorf("INSTEAD OF INSERT trigger failed: %w", err)
			}
		}
		rowsAffected++
	}

	return 0, rowsAffected, nil
}

// executeInsteadOfUpdateTrigger executes an INSTEAD OF UPDATE trigger
func (c *Catalog) executeInsteadOfUpdateTrigger(ctx context.Context, trigger *query.CreateTriggerStmt, stmt *query.UpdateStmt, args []interface{}) (int64, int64, error) {
	rowsAffected := int64(0)

	// Get columns for the view/table
	columns := c.getColumnsForTableOrView(stmt.Table)

	// Get rows from the view/table
	var rows [][]interface{}
	if view, err := c.getViewLocked(stmt.Table); err == nil {
		// It's a view - execute the view's SELECT
		_, viewRows, err := c.selectLocked(view, args)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to execute view for INSTEAD OF UPDATE: %w", err)
		}
		rows = viewRows
	} else {
		// It's a table - scan it
		tree, exists := c.tableTrees[stmt.Table]
		if !exists {
			return 0, 0, fmt.Errorf("table not found: %s", stmt.Table)
		}
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to scan for INSTEAD OF UPDATE: %w", err)
		}
		defer iter.Close()
		for iter.HasNext() {
			_, valueData, err := iter.Next()
			if err != nil {
				break
			}
			row, err := decodeRow(valueData, len(columns))
			if err != nil {
				continue
			}
			rows = append(rows, row)
		}
	}

	// Process each row
	for _, row := range rows {
		// Check WHERE clause
		if stmt.Where != nil {
			matched, err := evaluateWhere(c, row, columns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
		}

		// Build NEW row with updated values
		newRow := make([]interface{}, len(row))
		copy(newRow, row)
		for _, setClause := range stmt.Set {
			for i, col := range columns {
				if strings.EqualFold(col.Name, setClause.Column) && i < len(newRow) {
					val, _ := evaluateExpression(c, row, columns, setClause.Value, args)
					newRow[i] = val
					break
				}
			}
		}

		// Execute trigger body
		for _, bodyStmt := range trigger.Body {
			resolved := c.resolveTriggerRefs(bodyStmt, newRow, row, columns)
			if err := c.executeTriggerStatement(ctx, resolved); err != nil {
				return 0, 0, fmt.Errorf("INSTEAD OF UPDATE trigger failed: %w", err)
			}
		}
		rowsAffected++
	}

	return 0, rowsAffected, nil
}

// executeInsteadOfDeleteTrigger executes an INSTEAD OF DELETE trigger
func (c *Catalog) executeInsteadOfDeleteTrigger(ctx context.Context, trigger *query.CreateTriggerStmt, stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	rowsAffected := int64(0)

	// Get columns for the view/table
	columns := c.getColumnsForTableOrView(stmt.Table)

	// Get rows from the view/table
	var rows [][]interface{}
	if view, err := c.getViewLocked(stmt.Table); err == nil {
		// It's a view - execute the view's SELECT
		_, viewRows, err := c.selectLocked(view, args)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to execute view for INSTEAD OF DELETE: %w", err)
		}
		rows = viewRows
	} else {
		// It's a table - scan it
		tree, exists := c.tableTrees[stmt.Table]
		if !exists {
			return 0, 0, fmt.Errorf("table not found: %s", stmt.Table)
		}
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to scan for INSTEAD OF DELETE: %w", err)
		}
		defer iter.Close()
		for iter.HasNext() {
			_, valueData, err := iter.Next()
			if err != nil {
				break
			}
			row, err := decodeRow(valueData, len(columns))
			if err != nil {
				continue
			}
			rows = append(rows, row)
		}
	}

	// Process each row
	for _, row := range rows {
		// Check WHERE clause
		if stmt.Where != nil {
			matched, err := evaluateWhere(c, row, columns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
		}

		// Execute trigger body with OLD row
		for _, bodyStmt := range trigger.Body {
			resolved := c.resolveTriggerRefs(bodyStmt, nil, row, columns)
			if err := c.executeTriggerStatement(ctx, resolved); err != nil {
				return 0, 0, fmt.Errorf("INSTEAD OF DELETE trigger failed: %w", err)
			}
		}
		rowsAffected++
	}

	return 0, rowsAffected, nil
}
