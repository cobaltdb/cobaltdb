package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
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
	return reservedWords[toUpperFast(name)]
}

// isMaxValuePartitionBound reports whether a RANGE partition bound expression is
// the MAXVALUE sentinel (`VALUES LESS THAN (MAXVALUE)`), which the parser emits
// as a bare identifier rather than a numeric literal.
func isMaxValuePartitionBound(expr query.Expression) bool {
	switch e := expr.(type) {
	case *query.Identifier:
		return strings.EqualFold(e.Name, "MAXVALUE")
	case *query.ColumnRef:
		return strings.EqualFold(e.Column, "MAXVALUE")
	}
	return false
}

func (c *Catalog) CreateTable(stmt *query.CreateTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

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
		Checks:      make([]CheckDef, len(stmt.CheckConstraints)),
		Temporary:   stmt.Temporary,
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
				} else if isMaxValuePartitionBound(sp.Values[0]) {
					// VALUES LESS THAN (MAXVALUE) is the catch-all upper bound;
					// parsed as a bare identifier, not a number. Without this it
					// kept MinValue=MaxValue=0 (a dead partition) and every value
					// above the previous bound was rejected at INSERT time.
					pd.MaxValue = math.MaxInt64
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
			CheckName:     col.CheckName,
			Check:         col.Check,
			defaultExpr:   col.Default,
			Collation:     col.Collation,
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
			seen[toLowerFast(pk)] = true
		}
		for _, pkCol := range stmt.PrimaryKey {
			if !seen[toLowerFast(pkCol)] {
				tableDef.PrimaryKey = append(tableDef.PrimaryKey, pkCol)
				seen[toLowerFast(pkCol)] = true
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

	// Build column index cache before validating constraints that resolve column names.
	tableDef.buildColumnIndexCache()

	// Copy and validate foreign key definitions
	for i, fk := range stmt.ForeignKeys {
		normalizedFK, err := c.validateForeignKeyDefLocked(tableDef, fk, true)
		if err != nil {
			return err
		}
		tableDef.ForeignKeys[i] = normalizedFK
	}
	for i, check := range stmt.CheckConstraints {
		tableDef.Checks[i] = CheckDef{
			Name:     check.Name,
			CheckStr: exprToSQL(check.Expr),
			Check:    check.Expr,
		}
	}

	c.tables[stmt.Table] = tableDef
	c.tableTrees[stmt.Table] = tree // Store the tree for data operations

	// Store table definition in catalog tree before exposing the create as
	// successful. If persistence fails, remove in-memory metadata so callers
	// do not observe a table that failed CREATE.
	if err := c.storeTableDef(tableDef); err != nil {
		delete(c.tables, stmt.Table)
		delete(c.tableTrees, stmt.Table)
		for treeName := range c.tableTrees {
			if strings.HasPrefix(treeName, stmt.Table+":") {
				delete(c.tableTrees, treeName)
			}
		}
		return err
	}

	// Record DDL undo entry for transaction rollback after persistence succeeds.
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:    undoCreateTable,
			tableName: stmt.Table,
		})
	}

	return nil
}

func (c *Catalog) CreateCollection(stmt *query.CreateCollectionStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	if err := validateTableName(stmt.Name); err != nil {
		return err
	}
	if _, exists := c.tables[stmt.Name]; exists {
		if stmt.IfNotExists {
			return nil
		}
		return ErrTableExists
	}
	if _, exists := c.foreignTables[stmt.Name]; exists {
		if stmt.IfNotExists {
			return nil
		}
		return ErrTableExists
	}

	tree, err := btree.NewBTree(c.pool)
	if err != nil {
		return err
	}
	tableDef := &TableDef{
		Name:       stmt.Name,
		Type:       "collection",
		Columns:    []ColumnDef{},
		CreatedAt:  time.Now().UnixNano(),
		RootPageID: tree.RootPageID(),
	}
	c.tables[stmt.Name] = tableDef
	c.tableTrees[stmt.Name] = tree
	if err := c.storeTableDef(tableDef); err != nil {
		delete(c.tables, stmt.Name)
		delete(c.tableTrees, stmt.Name)
		return err
	}
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:    undoCreateTable,
			tableName: stmt.Name,
		})
	}
	return nil
}

func (c *Catalog) DropCollection(stmt *query.DropCollectionStmt) error {
	c.mu.RLock()
	tableDef, exists := c.tables[stmt.Name]
	c.mu.RUnlock()
	if !exists {
		if stmt.IfExists {
			return nil
		}
		return ErrTableNotFound
	}
	if tableDef.Type != "collection" {
		return fmt.Errorf("%s is not a collection", stmt.Name)
	}
	return c.DropTable(&query.DropTableStmt{Table: stmt.Name, IfExists: stmt.IfExists})
}

func (c *Catalog) storeTableDef(table *TableDef) error {
	if table.Temporary {
		return nil
	}
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

func cloneForeignKeys(fks []ForeignKeyDef) []ForeignKeyDef {
	if len(fks) == 0 {
		return nil
	}
	out := make([]ForeignKeyDef, len(fks))
	copy(out, fks)
	for i := range out {
		out[i].Columns = append([]string(nil), fks[i].Columns...)
		out[i].ReferencedColumns = append([]string(nil), fks[i].ReferencedColumns...)
	}
	return out
}

func queryForeignKeyToCatalog(fk *query.ForeignKeyDef) ForeignKeyDef {
	if fk == nil {
		return ForeignKeyDef{}
	}
	return ForeignKeyDef{
		Name:              fk.Name,
		Columns:           append([]string(nil), fk.Columns...),
		ReferencedTable:   fk.ReferencedTable,
		ReferencedColumns: append([]string(nil), fk.ReferencedColumns...),
		OnDelete:          fk.OnDelete,
		OnUpdate:          fk.OnUpdate,
	}
}

func (c *Catalog) validateForeignKeyDefLocked(table *TableDef, fk *query.ForeignKeyDef, allowUnresolvedReference bool) (ForeignKeyDef, error) {
	if fk == nil {
		return ForeignKeyDef{}, fmt.Errorf("missing FOREIGN KEY definition")
	}
	if len(fk.Columns) == 0 {
		return ForeignKeyDef{}, fmt.Errorf("foreign key must include at least one column")
	}
	if fk.ReferencedTable == "" {
		return ForeignKeyDef{}, fmt.Errorf("foreign key referenced table is required")
	}
	seenLocal := make(map[string]struct{}, len(fk.Columns))
	for _, col := range fk.Columns {
		if _, exists := seenLocal[strings.ToLower(col)]; exists {
			return ForeignKeyDef{}, fmt.Errorf("duplicate foreign key column '%s'", col)
		}
		seenLocal[strings.ToLower(col)] = struct{}{}
		if table.GetColumnIndex(col) < 0 {
			return ForeignKeyDef{}, fmt.Errorf("foreign key column '%s' not found in table '%s'", col, table.Name)
		}
	}

	refTable := table
	if !strings.EqualFold(fk.ReferencedTable, table.Name) {
		var err error
		refTable, err = c.getTableLocked(fk.ReferencedTable)
		if err != nil {
			if allowUnresolvedReference {
				if len(fk.ReferencedColumns) > 0 && len(fk.ReferencedColumns) != len(fk.Columns) {
					return ForeignKeyDef{}, fmt.Errorf("foreign key column count mismatch: %d referencing columns, %d referenced columns", len(fk.Columns), len(fk.ReferencedColumns))
				}
				return queryForeignKeyToCatalog(fk), nil
			}
			return ForeignKeyDef{}, fmt.Errorf("FOREIGN KEY constraint failed: referenced table '%s' not found", fk.ReferencedTable)
		}
	}
	refColumns := append([]string(nil), fk.ReferencedColumns...)
	if len(refColumns) == 0 {
		refColumns = append([]string(nil), refTable.PrimaryKey...)
		if len(refColumns) == 0 {
			return ForeignKeyDef{}, fmt.Errorf("foreign key referenced columns omitted but table '%s' has no PRIMARY KEY", fk.ReferencedTable)
		}
	}
	if len(refColumns) != len(fk.Columns) {
		return ForeignKeyDef{}, fmt.Errorf("foreign key column count mismatch: %d referencing columns, %d referenced columns", len(fk.Columns), len(refColumns))
	}
	seenRef := make(map[string]struct{}, len(refColumns))
	for _, col := range refColumns {
		if _, exists := seenRef[strings.ToLower(col)]; exists {
			return ForeignKeyDef{}, fmt.Errorf("duplicate foreign key referenced column '%s'", col)
		}
		seenRef[strings.ToLower(col)] = struct{}{}
		if refTable.GetColumnIndex(col) < 0 {
			return ForeignKeyDef{}, fmt.Errorf("foreign key referenced column '%s' not found in table '%s'", col, fk.ReferencedTable)
		}
	}

	out := queryForeignKeyToCatalog(fk)
	out.Columns = append([]string(nil), fk.Columns...)
	out.ReferencedColumns = refColumns
	return out, nil
}

func (c *Catalog) AlterTableAddForeignKeyConstraint(ctx context.Context, stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return err
	}
	if stmt.ForeignKey == nil {
		return fmt.Errorf("missing FOREIGN KEY definition")
	}
	if stmt.ConstraintName == "" {
		return fmt.Errorf("foreign key constraint name is required")
	}
	for _, existing := range table.ForeignKeys {
		if strings.EqualFold(existing.Name, stmt.ConstraintName) {
			return fmt.Errorf("constraint %s already exists", stmt.ConstraintName)
		}
	}
	normalizedFK, err := c.validateForeignKeyDefLocked(table, stmt.ForeignKey, false)
	if err != nil {
		return err
	}
	normalizedFK.Name = stmt.ConstraintName

	oldFKs := cloneForeignKeys(table.ForeignKeys)
	table.ForeignKeys = append(table.ForeignKeys, normalizedFK)
	if err := NewForeignKeyEnforcer(c).CheckForeignKeyConstraints(ctx, stmt.Table); err != nil {
		table.ForeignKeys = oldFKs
		return err
	}
	if err := c.storeTableDef(table); err != nil {
		table.ForeignKeys = oldFKs
		return err
	}
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:         undoAlterForeignKeys,
			tableName:      stmt.Table,
			oldForeignKeys: oldFKs,
		})
	}
	return nil
}

func (c *Catalog) AlterTableAddCheckConstraint(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return err
	}
	if stmt.ConstraintName == "" {
		return fmt.Errorf("check constraint name is required")
	}
	if stmt.ConstraintCheck == nil {
		return fmt.Errorf("missing CHECK constraint expression")
	}
	for _, existing := range table.Checks {
		if strings.EqualFold(existing.Name, stmt.ConstraintName) {
			return fmt.Errorf("constraint %s already exists", stmt.ConstraintName)
		}
	}

	oldChecks := cloneCheckDefs(table.Checks)
	table.Checks = append(table.Checks, CheckDef{
		Name:     stmt.ConstraintName,
		CheckStr: exprToSQL(stmt.ConstraintCheck),
		Check:    stmt.ConstraintCheck,
	})
	if err := c.validateCheckConstraintsLocked(table); err != nil {
		table.Checks = oldChecks
		return err
	}
	if err := c.storeTableDef(table); err != nil {
		table.Checks = oldChecks
		return err
	}
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:    undoAlterChecks,
			tableName: stmt.Table,
			oldChecks: oldChecks,
		})
	}
	return nil
}

func (c *Catalog) validateCheckConstraintsLocked(table *TableDef) error {
	tree := c.tableTrees[table.Name]
	if tree != nil {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.HasNext() {
			_, value, err := iter.Next()
			if err != nil {
				return err
			}
			row, err := decodeRow(value, len(table.Columns))
			if err != nil {
				return err
			}
			if err := c.checkRowConstraints(table, row, nil); err != nil {
				return err
			}
		}
	}
	if ts := c.getCurrentTxn(); ts != nil {
		if pending := ts.getPendingWriteMap()[table.Name]; len(pending) > 0 {
			for _, pw := range pending {
				pendingRow, live, err := decodeLiveRow(pw.Value, len(table.Columns))
				if err != nil {
					return err
				}
				if !live {
					continue
				}
				if err := c.checkRowConstraints(table, pendingRow, nil); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *Catalog) DropTableConstraint(tableName, constraintName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}
	for i, fk := range table.ForeignKeys {
		if strings.EqualFold(fk.Name, constraintName) {
			oldFKs := cloneForeignKeys(table.ForeignKeys)
			table.ForeignKeys = append(table.ForeignKeys[:i], table.ForeignKeys[i+1:]...)
			if err := c.storeTableDef(table); err != nil {
				table.ForeignKeys = oldFKs
				return err
			}
			if c.isCurrentTxnActive() {
				c.appendUndoEntry(undoEntry{
					action:         undoAlterForeignKeys,
					tableName:      tableName,
					oldForeignKeys: oldFKs,
				})
			}
			return nil
		}
	}
	for i, check := range table.Checks {
		if strings.EqualFold(check.Name, constraintName) {
			oldChecks := cloneCheckDefs(table.Checks)
			table.Checks = append(table.Checks[:i], table.Checks[i+1:]...)
			if err := c.storeTableDef(table); err != nil {
				table.Checks = oldChecks
				return err
			}
			if c.isCurrentTxnActive() {
				c.appendUndoEntry(undoEntry{
					action:    undoAlterChecks,
					tableName: tableName,
					oldChecks: oldChecks,
				})
			}
			return nil
		}
	}

	idxDef, exists := c.indexes[constraintName]
	if !exists {
		return ErrIndexNotFound
	}
	if idxDef.TableName != tableName {
		return fmt.Errorf("constraint %s not found on table %s", constraintName, tableName)
	}
	if !idxDef.Unique {
		return fmt.Errorf("constraint %s is not a UNIQUE constraint", constraintName)
	}
	if err := c.deleteCatalogDef("idx:" + constraintName); err != nil {
		return fmt.Errorf("failed to delete constraint metadata %s: %w", constraintName, err)
	}
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:    undoDropIndex,
			indexName: constraintName,
			indexDef:  idxDef,
			indexTree: c.indexTrees[constraintName],
		})
	}
	delete(c.indexes, constraintName)
	delete(c.indexTrees, constraintName)
	return nil
}

func (c *Catalog) DropTable(stmt *query.DropTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	if !stmt.IfExists {
		if _, exists := c.tables[stmt.Table]; !exists {
			if _, fexists := c.foreignTables[stmt.Table]; !fexists {
				return ErrTableNotFound
			}
		}
	}

	if err := c.ensureTableNotReferencedByForeignKeyLocked(stmt.Table); err != nil {
		return err
	}

	// Drop foreign table if present
	if ft, fexists := c.foreignTables[stmt.Table]; fexists {
		if err := c.deleteCatalogDef("ft:" + stmt.Table); err != nil {
			return fmt.Errorf("failed to delete foreign table metadata %s: %w", stmt.Table, err)
		}
		if c.isCurrentTxnActive() {
			c.appendUndoEntry(undoEntry{
				action:           undoDropForeignTable,
				foreignTableName: stmt.Table,
				foreignTableDef:  cloneForeignTableDef(ft),
			})
		}
		delete(c.foreignTables, stmt.Table)
		return nil
	}

	// Check if table actually exists before trying to delete
	tableDef, exists := c.tables[stmt.Table]
	tableIndexes := make(map[string]*IndexDef)
	tableIdxTrees := make(map[string]btree.TreeStore)
	var tableRLSPolicies []*security.Policy
	tableRLSWasEnabled := false
	if exists {
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName == stmt.Table {
				tableIndexes[idxName] = idxDef
				if tree, ok := c.indexTrees[idxName]; ok {
					tableIdxTrees[idxName] = tree
				}
			}
		}
		if c.enableRLS && c.rlsManager != nil {
			tableRLSWasEnabled = c.rlsManager.IsEnabled(stmt.Table)
			tableRLSPolicies = c.rlsManager.GetTablePolicies(stmt.Table)
		}
		var deletedIndexes []string
		for idxName, idxDef := range tableIndexes {
			if idxDef.Temporary {
				continue
			}
			if err := c.deleteCatalogDef("idx:" + idxName); err != nil {
				return fmt.Errorf("failed to delete index metadata %s for dropped table %s: %w", idxName, stmt.Table, err)
			}
			deletedIndexes = append(deletedIndexes, idxName)
		}
		for _, policy := range tableRLSPolicies {
			key := "rlsp:" + strings.ToLower(policy.TableName) + ":" + strings.ToLower(policy.Name)
			if err := c.deleteCatalogDef(key); err != nil {
				for _, idxName := range deletedIndexes {
					if restoreErr := c.storeIndexDef(tableIndexes[idxName]); restoreErr != nil {
						return fmt.Errorf("failed to delete RLS policy metadata %s for dropped table %s: %w; restoring index metadata %s failed: %v", policy.Name, stmt.Table, err, idxName, restoreErr)
					}
				}
				for _, restorePolicy := range tableRLSPolicies {
					if restoreErr := c.storeRLSPolicyDef(restorePolicy); restoreErr != nil {
						return fmt.Errorf("failed to delete RLS policy metadata %s for dropped table %s: %w; restoring RLS policy metadata %s failed: %v", policy.Name, stmt.Table, err, restorePolicy.Name, restoreErr)
					}
				}
				return fmt.Errorf("failed to delete RLS policy metadata %s for dropped table %s: %w", policy.Name, stmt.Table, err)
			}
		}
		if tableRLSWasEnabled {
			if err := c.deleteCatalogDef("rlst:" + strings.ToLower(stmt.Table)); err != nil {
				for _, idxName := range deletedIndexes {
					if restoreErr := c.storeIndexDef(tableIndexes[idxName]); restoreErr != nil {
						return fmt.Errorf("failed to delete RLS table metadata %s: %w; restoring index metadata %s failed: %v", stmt.Table, err, idxName, restoreErr)
					}
				}
				for _, policy := range tableRLSPolicies {
					if restoreErr := c.storeRLSPolicyDef(policy); restoreErr != nil {
						return fmt.Errorf("failed to delete RLS table metadata %s: %w; restoring RLS policy metadata %s failed: %v", stmt.Table, err, policy.Name, restoreErr)
					}
				}
				return fmt.Errorf("failed to delete RLS table metadata %s: %w", stmt.Table, err)
			}
		}
		if err := c.deleteCatalogDef("tbl:" + stmt.Table); err != nil {
			for _, idxName := range deletedIndexes {
				if restoreErr := c.storeIndexDef(tableIndexes[idxName]); restoreErr != nil {
					return fmt.Errorf("failed to delete table metadata %s: %w; restoring index metadata %s failed: %v", stmt.Table, err, idxName, restoreErr)
				}
			}
			for _, policy := range tableRLSPolicies {
				if restoreErr := c.storeRLSPolicyDef(policy); restoreErr != nil {
					return fmt.Errorf("failed to delete table metadata %s: %w; restoring RLS policy metadata %s failed: %v", stmt.Table, err, policy.Name, restoreErr)
				}
			}
			if tableRLSWasEnabled {
				if restoreErr := c.storeRLSEnabledTable(stmt.Table); restoreErr != nil {
					return fmt.Errorf("failed to delete table metadata %s: %w; restoring RLS table metadata failed: %v", stmt.Table, err, restoreErr)
				}
			}
			return fmt.Errorf("failed to delete table metadata %s: %w", stmt.Table, err)
		}
	}

	// Record DDL undo entry for transaction rollback before deleting
	if c.isCurrentTxnActive() && exists {
		entry := undoEntry{
			action:             undoDropTable,
			tableName:          stmt.Table,
			tableDef:           tableDef,
			tableTree:          c.tableTrees[stmt.Table],
			tableIndexes:       make(map[string]*IndexDef),
			tableIdxTrees:      make(map[string]btree.TreeStore),
			rlsPolicies:        tableRLSPolicies,
			rlsTableName:       stmt.Table,
			rlsTableWasEnabled: tableRLSWasEnabled,
		}
		for idxName, idxDef := range tableIndexes {
			entry.tableIndexes[idxName] = idxDef
		}
		for idxName, idxTree := range tableIdxTrees {
			entry.tableIdxTrees[idxName] = idxTree
		}
		c.appendUndoEntry(entry)
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
	if c.rlsManager != nil {
		for _, policy := range tableRLSPolicies {
			_ = c.rlsManager.DropPolicy(policy.TableName, policy.Name)
		}
		if tableRLSWasEnabled {
			c.rlsManager.DisableTable(stmt.Table)
		}
	}

	// Clean up views that reference this table (triggers, FTS indexes, stats)
	delete(c.stats, stmt.Table)
	delete(c.tables, stmt.Table)

	return nil
}

func (c *Catalog) ensureTableNotReferencedByForeignKeyLocked(tableName string) error {
	for refTableName, table := range c.tables {
		if strings.EqualFold(refTableName, tableName) {
			continue
		}
		for _, fk := range table.ForeignKeys {
			if strings.EqualFold(fk.ReferencedTable, tableName) {
				constraint := fk.Name
				if constraint == "" {
					constraint = "<unnamed>"
				}
				return fmt.Errorf("cannot drop table %s: referenced by foreign key %s on table %s", tableName, constraint, refTableName)
			}
		}
	}
	return nil
}

func (c *Catalog) ensureColumnNotUsedByForeignKeyLocked(tableName, colName string) error {
	table, exists := c.tables[tableName]
	if !exists {
		return ErrTableNotFound
	}
	for _, fk := range table.ForeignKeys {
		for _, fkCol := range fk.Columns {
			if strings.EqualFold(fkCol, colName) {
				constraint := fk.Name
				if constraint == "" {
					constraint = "<unnamed>"
				}
				return fmt.Errorf("cannot drop column %s.%s: used by foreign key %s", tableName, colName, constraint)
			}
		}
	}
	for refTableName, refTable := range c.tables {
		for _, fk := range refTable.ForeignKeys {
			if !strings.EqualFold(fk.ReferencedTable, tableName) {
				continue
			}
			for _, refCol := range fk.ReferencedColumns {
				if strings.EqualFold(refCol, colName) {
					constraint := fk.Name
					if constraint == "" {
						constraint = "<unnamed>"
					}
					return fmt.Errorf("cannot drop column %s.%s: referenced by foreign key %s on table %s", tableName, colName, constraint, refTableName)
				}
			}
		}
	}
	return nil
}

func renameForeignKeyColumns(fks []ForeignKeyDef, oldName, newName string) bool {
	changed := false
	for i := range fks {
		for j, col := range fks[i].Columns {
			if strings.EqualFold(col, oldName) {
				fks[i].Columns[j] = newName
				changed = true
			}
		}
	}
	return changed
}

func renameReferencedForeignKeyColumns(fks []ForeignKeyDef, referencedTable, oldName, newName string) bool {
	changed := false
	for i := range fks {
		if !strings.EqualFold(fks[i].ReferencedTable, referencedTable) {
			continue
		}
		for j, col := range fks[i].ReferencedColumns {
			if strings.EqualFold(col, oldName) {
				fks[i].ReferencedColumns[j] = newName
				changed = true
			}
		}
	}
	return changed
}

type checkColumnRefVisitor struct {
	oldName string
	newName string
	rename  bool
	changed bool
	found   bool
}

func (v *checkColumnRefVisitor) matchName(name string) bool {
	if strings.EqualFold(name, v.oldName) {
		return true
	}
	if dot := strings.LastIndexByte(name, '.'); dot >= 0 && dot < len(name)-1 {
		return strings.EqualFold(name[dot+1:], v.oldName)
	}
	return false
}

func (v *checkColumnRefVisitor) renameName(name string) string {
	if strings.EqualFold(name, v.oldName) {
		return v.newName
	}
	if dot := strings.LastIndexByte(name, '.'); dot >= 0 && dot < len(name)-1 && strings.EqualFold(name[dot+1:], v.oldName) {
		return name[:dot+1] + v.newName
	}
	return name
}

func (v *checkColumnRefVisitor) VisitIdentifier(expr *query.Identifier, ctx interface{}) interface{} {
	if v.matchName(expr.Name) {
		v.found = true
		if v.rename {
			expr.Name = v.renameName(expr.Name)
			v.changed = true
		}
	}
	return expr
}

func (v *checkColumnRefVisitor) VisitQualifiedIdentifier(expr *query.QualifiedIdentifier, ctx interface{}) interface{} {
	if strings.EqualFold(expr.Column, v.oldName) {
		v.found = true
		if v.rename {
			expr.Column = v.newName
			v.changed = true
		}
	}
	return expr
}

func (v *checkColumnRefVisitor) VisitColumnRef(expr *query.ColumnRef, ctx interface{}) interface{} {
	if strings.EqualFold(expr.Column, v.oldName) {
		v.found = true
		if v.rename {
			expr.Column = v.newName
			v.changed = true
		}
	}
	return expr
}

func (v *checkColumnRefVisitor) VisitBinaryExpr(expr *query.BinaryExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitUnaryExpr(expr *query.UnaryExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitFunctionCall(expr *query.FunctionCall, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitStringLiteral(expr *query.StringLiteral, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitNumberLiteral(expr *query.NumberLiteral, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitBooleanLiteral(expr *query.BooleanLiteral, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitNullLiteral(expr *query.NullLiteral, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitVectorLiteral(expr *query.VectorLiteral, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitPlaceholder(expr *query.PlaceholderExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitInExpr(expr *query.InExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitBetweenExpr(expr *query.BetweenExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitLikeExpr(expr *query.LikeExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitIsNullExpr(expr *query.IsNullExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitCastExpr(expr *query.CastExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitCaseExpr(expr *query.CaseExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitSubqueryExpr(expr *query.SubqueryExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitExistsExpr(expr *query.ExistsExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitStarExpr(expr *query.StarExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitJSONPathExpr(expr *query.JSONPathExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitJSONContainsExpr(expr *query.JSONContainsExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitAliasExpr(expr *query.AliasExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitMatchExpr(expr *query.MatchExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitWindowExpr(expr *query.WindowExpr, ctx interface{}) interface{} {
	return expr
}
func (v *checkColumnRefVisitor) VisitWindowSpec(expr *query.WindowSpec, ctx interface{}) interface{} {
	return expr
}

func checkExpressionReferencesColumn(expr query.Expression, colName string) bool {
	visitor := &checkColumnRefVisitor{oldName: colName}
	query.Walk(expr, visitor, nil)
	return visitor.found
}

func renameExpressionColumnReferences(expr query.Expression, oldName, newName string) bool {
	visitor := &checkColumnRefVisitor{oldName: oldName, newName: newName, rename: true}
	query.Walk(expr, visitor, nil)
	return visitor.changed
}

func checkConstraintLabel(name string, fallback string) string {
	if name != "" {
		return name
	}
	return fallback
}

func ensureColumnNotUsedByCheck(table *TableDef, colName string, dropColIdx int) error {
	for i, col := range table.Columns {
		if i == dropColIdx || col.Check == nil {
			continue
		}
		if checkExpressionReferencesColumn(col.Check, colName) {
			return fmt.Errorf("cannot drop column %s.%s: referenced by CHECK constraint %s", table.Name, colName, checkConstraintLabel(col.CheckName, col.Name))
		}
	}
	for i, check := range table.Checks {
		if check.Check == nil {
			continue
		}
		if checkExpressionReferencesColumn(check.Check, colName) {
			return fmt.Errorf("cannot drop column %s.%s: referenced by CHECK constraint %s", table.Name, colName, checkConstraintLabel(check.Name, fmt.Sprintf("#%d", i+1)))
		}
	}
	return nil
}

func renameCheckColumnReferences(table *TableDef, oldName, newName string) bool {
	changed := false
	for i := range table.Columns {
		if table.Columns[i].Check == nil {
			continue
		}
		if renameExpressionColumnReferences(table.Columns[i].Check, oldName, newName) {
			table.Columns[i].CheckStr = exprToSQL(table.Columns[i].Check)
			changed = true
		}
	}
	for i := range table.Checks {
		if table.Checks[i].Check == nil {
			continue
		}
		if renameExpressionColumnReferences(table.Checks[i].Check, oldName, newName) {
			table.Checks[i].CheckStr = exprToSQL(table.Checks[i].Check)
			changed = true
		}
	}
	return changed
}

// CleanupFailedCreateTable removes table/index metadata created by a CREATE
// TABLE statement whose follow-up constraint/index creation failed. It does not
// record undo entries because the statement itself never succeeded.
func (c *Catalog) CleanupFailedCreateTable(tableName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	var indexNames []string
	for idxName, idxDef := range c.indexes {
		if idxDef != nil && idxDef.TableName == tableName {
			indexNames = append(indexNames, idxName)
		}
	}
	for _, idxName := range indexNames {
		if err := c.deleteCatalogDef("idx:" + idxName); err != nil {
			return fmt.Errorf("failed to delete index metadata %s during CREATE TABLE cleanup: %w", idxName, err)
		}
	}
	if _, exists := c.tables[tableName]; exists {
		if err := c.deleteCatalogDef("tbl:" + tableName); err != nil {
			return fmt.Errorf("failed to delete table metadata %s during CREATE TABLE cleanup: %w", tableName, err)
		}
	}

	delete(c.tableTrees, tableName)
	for treeName := range c.tableTrees {
		if strings.HasPrefix(treeName, tableName+":") {
			delete(c.tableTrees, treeName)
		}
	}
	for _, idxName := range indexNames {
		delete(c.indexes, idxName)
		delete(c.indexTrees, idxName)
	}
	delete(c.stats, tableName)
	delete(c.tables, tableName)
	c.pruneFailedCreateTableUndoLocked(tableName, indexNames)
	return nil
}

func (c *Catalog) pruneFailedCreateTableUndoLocked(tableName string, indexNames []string) {
	indexSet := make(map[string]struct{}, len(indexNames))
	for _, idxName := range indexNames {
		indexSet[idxName] = struct{}{}
	}
	prune := func(undoLog []undoEntry) ([]undoEntry, []int) {
		if len(undoLog) == 0 {
			return undoLog, nil
		}
		out := undoLog[:0]
		var removed []int
		for pos, entry := range undoLog {
			remove := false
			switch entry.action {
			case undoCreateTable:
				remove = entry.tableName == tableName
			case undoCreateIndex:
				_, remove = indexSet[entry.indexName]
			}
			if remove {
				removed = append(removed, pos)
				continue
			}
			out = append(out, entry)
		}
		return out, removed
	}
	adjustSavepoints := func(savepoints []savepointEntry, removed []int) {
		if len(removed) == 0 {
			return
		}
		for i := range savepoints {
			shift := 0
			for _, pos := range removed {
				if pos < savepoints[i].undoPos {
					shift++
				}
			}
			savepoints[i].undoPos -= shift
			if savepoints[i].undoPos < 0 {
				savepoints[i].undoPos = 0
			}
		}
	}
	if ts := c.getCurrentTxn(); ts != nil {
		var removed []int
		ts.undoLog, removed = prune(ts.undoLog)
		adjustSavepoints(ts.savepoints, removed)
		return
	}
	var removed []int
	c.undoLog, removed = prune(c.undoLog)
	adjustSavepoints(c.savepoints, removed)
}

func (c *Catalog) AlterTableAddColumn(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

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

	newCol := ColumnDef{
		Name:          stmt.Column.Name,
		Type:          tokenTypeToColumnType(stmt.Column.Type),
		NotNull:       stmt.Column.NotNull,
		Unique:        stmt.Column.Unique,
		PrimaryKey:    stmt.Column.PrimaryKey,
		AutoIncrement: stmt.Column.AutoIncrement,
		Default:       exprToSQL(stmt.Column.Default),
		CheckStr:      exprToSQL(stmt.Column.Check),
		CheckName:     stmt.Column.CheckName,
		Check:         stmt.Column.Check,
		defaultExpr:   stmt.Column.Default,
		Dimensions:    stmt.Column.Dimensions,
	}

	// Backfill existing rows with the default value for the new column
	tree, treeExists := c.tableTrees[stmt.Table]
	type rowUpdate struct {
		key  []byte
		data []byte
	}
	var updates []rowUpdate
	if treeExists {
		// Compute default value
		var defaultVal interface{}
		if newCol.defaultExpr != nil {
			defaultVal, _ = evaluateExpression(c, nil, nil, newCol.defaultExpr, nil)
		}

		// Remember the old column count before adding the new column
		oldColCount := len(table.Columns)
		newColCount := oldColCount + 1

		// Scan all rows and append the default value
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("failed to scan table for ALTER TABLE: %w", err)
		}
		defer iter.Close()
		for iter.HasNext() {
			key, valueData, err := iter.Next()
			if err != nil {
				return fmt.Errorf("failed to read row during ALTER TABLE backfill: %w", err)
			}
			// Use oldColCount to decode to avoid automatic padding with nil
			vrow, err := decodeVersionedRow(valueData, oldColCount)
			if err != nil {
				return fmt.Errorf("failed to decode row in table %s during ALTER TABLE ADD COLUMN: %w", stmt.Table, err)
			}
			values := vrow.Data
			// Only update rows that are missing the new column
			if len(values) <= oldColCount {
				for len(values) < newColCount {
					values = append(values, defaultVal)
				}
				// Update the VersionedRow and re-encode (binary-safe).
				vrow.Data = values
				newData, err := encodeVersionedRowFull(vrow.Data, vrow.Version)
				if err != nil {
					return fmt.Errorf("failed to encode row during ALTER TABLE ADD COLUMN: %w", err)
				}
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				updates = append(updates, rowUpdate{key: keyCopy, data: newData})
			}
		}
	}

	// Save undo entry before modification
	if c.isCurrentTxnActive() {
		oldCols := make([]ColumnDef, len(table.Columns))
		copy(oldCols, table.Columns)
		c.appendUndoEntry(undoEntry{
			action:     undoAlterAddColumn,
			tableName:  stmt.Table,
			oldColumns: oldCols,
		})
	}

	table.Columns = append(table.Columns, newCol)
	table.buildColumnIndexCache()

	if treeExists {
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
	defer c.invalidateSchemaCache()

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
	if err := c.ensureColumnNotUsedByForeignKeyLocked(stmt.Table, colName); err != nil {
		return err
	}
	if err := ensureColumnNotUsedByCheck(table, colName, colIdx); err != nil {
		return err
	}

	dropIndexes := make(map[string]*IndexDef)
	dropIdxTrees := make(map[string]btree.TreeStore)
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName != stmt.Table {
			continue
		}
		for _, idxCol := range idxDef.Columns {
			if strings.EqualFold(idxCol, colName) {
				dropIndexes[idxName] = idxDef
				if idxTree, ok := c.indexTrees[idxName]; ok {
					dropIdxTrees[idxName] = idxTree
				}
				break
			}
		}
	}
	var undo *undoEntry
	if c.isCurrentTxnActive() {
		oldCols := make([]ColumnDef, len(table.Columns))
		copy(oldCols, table.Columns)
		entry := undoEntry{
			action:          undoAlterDropColumn,
			tableName:       stmt.Table,
			oldColumns:      oldCols,
			droppedIndexes:  make(map[string]*IndexDef),
			droppedIdxTrees: make(map[string]btree.TreeStore),
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
					return fmt.Errorf("failed to read row while saving ALTER TABLE DROP COLUMN undo data: %w", err)
				}
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				valCopy := make([]byte, len(val))
				copy(valCopy, val)
				entry.oldRowData = append(entry.oldRowData, struct{ key, val []byte }{keyCopy, valCopy})
			}
		}
		for idxName, idxDef := range dropIndexes {
			entry.droppedIndexes[idxName] = idxDef
		}
		for idxName, idxTree := range dropIdxTrees {
			entry.droppedIdxTrees[idxName] = idxTree
		}
		undo = &entry
	}

	// Prepare all row updates before mutating the table definition so corrupt
	// row failures leave the in-memory schema unchanged.
	tree, exists := c.tableTrees[stmt.Table]
	var updates []struct {
		key []byte
		val []byte
	}
	if exists {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("failed to scan table for ALTER TABLE DROP COLUMN backfill: %w", err)
		}
		defer iter.Close()
		for iter.HasNext() {
			key, valueData, err := iter.Next()
			if err != nil {
				return fmt.Errorf("failed to read row during ALTER TABLE DROP COLUMN backfill: %w", err)
			}
			// Decode through the versioned path and re-encode the VersionedRow
			// (mirroring ADD COLUMN). Marshalling the bare data array instead
			// discarded created_at/deleted_at: soft-deleted tombstones came back
			// live (resurrected rows, index desync) and live rows lost their
			// CreatedAt (broken AS OF temporal queries).
			vrow, err := decodeVersionedRow(valueData, originalColCount)
			if err != nil {
				return fmt.Errorf("failed to decode row in table %s during ALTER TABLE DROP COLUMN: %w", stmt.Table, err)
			}
			row := vrow.Data
			if colIdx < len(row) {
				row = append(row[:colIdx], row[colIdx+1:]...)
			}
			vrow.Data = row
			newData, err := encodeVersionedRowFull(vrow.Data, vrow.Version)
			if err != nil {
				return fmt.Errorf("failed to encode row during ALTER TABLE DROP COLUMN: %w", err)
			}
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			updates = append(updates, struct {
				key []byte
				val []byte
			}{keyCopy, newData})
		}
	}

	var deletedIndexNames []string
	for idxName, idxDef := range dropIndexes {
		if idxDef.Temporary {
			continue
		}
		if err := c.deleteCatalogDef("idx:" + idxName); err != nil {
			return fmt.Errorf("failed to delete index metadata %s for dropped column %s.%s: %w", idxName, stmt.Table, colName, err)
		}
		deletedIndexNames = append(deletedIndexNames, idxName)
	}
	restoreDroppedIndexMetadata := func(primary error) error {
		for _, idxName := range deletedIndexNames {
			if restoreErr := c.storeIndexDef(dropIndexes[idxName]); restoreErr != nil {
				return fmt.Errorf("%w; restoring index metadata %s failed: %v", primary, idxName, restoreErr)
			}
		}
		return primary
	}
	if undo != nil {
		c.appendUndoEntry(*undo)
	}

	// Remove column from definition
	table.Columns = append(table.Columns[:colIdx], table.Columns[colIdx+1:]...)
	table.buildColumnIndexCache()

	// Update all existing rows - remove the dropped column's data
	if exists {
		for _, u := range updates {
			if err := tree.Put(u.key, u.val); err != nil {
				return restoreDroppedIndexMetadata(fmt.Errorf("failed to update row after column drop: %w", err))
			}
		}
	}

	// Drop any indexes on the dropped column
	for idxName := range dropIndexes {
		delete(c.indexes, idxName)
		delete(c.indexTrees, idxName)
	}

	if err := c.storeTableDef(table); err != nil {
		return restoreDroppedIndexMetadata(err)
	}
	return nil
}

func (c *Catalog) AlterTableRename(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

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

	if err := c.deleteCatalogDef("tbl:" + stmt.Table); err != nil {
		return fmt.Errorf("failed to delete renamed table metadata %s: %w", stmt.Table, err)
	}

	// Save undo entry before modification
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
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
	var changedIndexes []*IndexDef
	for _, idxDef := range c.indexes {
		if idxDef.TableName == stmt.Table {
			idxDef.TableName = stmt.NewName
			changedIndexes = append(changedIndexes, idxDef)
		}
	}

	changedFKTables := make(map[string]*TableDef)
	for tableName, tbl := range c.tables {
		for i := range tbl.ForeignKeys {
			if strings.EqualFold(tbl.ForeignKeys[i].ReferencedTable, stmt.Table) {
				tbl.ForeignKeys[i].ReferencedTable = stmt.NewName
				changedFKTables[tableName] = tbl
			}
		}
	}

	// Update stats
	if stats, exists := c.stats[stmt.Table]; exists {
		delete(c.stats, stmt.Table)
		c.stats[stmt.NewName] = stats
	}

	table.Name = stmt.NewName

	if err := c.storeTableDef(table); err != nil {
		return fmt.Errorf("failed to persist renamed table: %w", err)
	}
	for _, idxDef := range changedIndexes {
		if err := c.storeIndexDef(idxDef); err != nil {
			return fmt.Errorf("failed to store index metadata %s after renaming table %s to %s: %w", idxDef.Name, stmt.Table, stmt.NewName, err)
		}
	}
	for tableName, tbl := range changedFKTables {
		if tableName == stmt.NewName {
			continue
		}
		if err := c.storeTableDef(tbl); err != nil {
			return fmt.Errorf("failed to store foreign key metadata for table %s after renaming referenced table %s to %s: %w", tableName, stmt.Table, stmt.NewName, err)
		}
	}

	return nil
}

func (c *Catalog) AlterTableRenameColumn(stmt *query.AlterTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

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
			if c.isCurrentTxnActive() {
				c.appendUndoEntry(undoEntry{
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
	renameCheckColumnReferences(table, stmt.OldName, stmt.NewName)

	// Update index column references
	var changedIndexes []*IndexDef
	for _, idxDef := range c.indexes {
		if idxDef.TableName == stmt.Table {
			for i, idxCol := range idxDef.Columns {
				if strings.EqualFold(idxCol, stmt.OldName) {
					idxDef.Columns[i] = stmt.NewName
					changedIndexes = append(changedIndexes, idxDef)
				}
			}
		}
	}
	changedFKTables := make(map[string]*TableDef)
	if renameForeignKeyColumns(table.ForeignKeys, stmt.OldName, stmt.NewName) {
		changedFKTables[stmt.Table] = table
	}
	for tableName, tbl := range c.tables {
		if renameReferencedForeignKeyColumns(tbl.ForeignKeys, stmt.Table, stmt.OldName, stmt.NewName) {
			changedFKTables[tableName] = tbl
		}
	}

	if err := c.storeTableDef(table); err != nil {
		return err
	}
	for _, idxDef := range changedIndexes {
		if err := c.storeIndexDef(idxDef); err != nil {
			return fmt.Errorf("failed to store index metadata %s after renaming column %s.%s: %w", idxDef.Name, stmt.Table, stmt.OldName, err)
		}
	}
	for tableName, tbl := range changedFKTables {
		if tableName == stmt.Table {
			continue
		}
		if err := c.storeTableDef(tbl); err != nil {
			return fmt.Errorf("failed to store foreign key metadata for table %s after renaming column %s.%s: %w", tableName, stmt.Table, stmt.OldName, err)
		}
	}
	return nil
}

func (c *Catalog) GetTable(name string) (*TableDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	table, err := c.getTableLocked(name)
	if err != nil {
		return nil, err
	}
	return cloneTableDef(table), nil
}

// GetTableIndexes returns copies of the secondary indexes defined on a table,
// used by SHOW INDEX.
func (c *Catalog) GetTableIndexes(table string) []IndexDef {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []IndexDef
	for _, idx := range c.indexes {
		if idx != nil && strings.EqualFold(idx.TableName, table) {
			out = append(out, *idx)
		}
	}
	return out
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

// getCachedTable returns a TableDef from the schema cache without acquiring
// Catalog.mu. The caller must verify the entry is still valid by comparing
// the returned version with c.schemaVersion.
func (c *Catalog) getCachedTable(name string) (*TableDef, uint64, bool) {
	c.schemaCacheMu.RLock()
	def, ok := c.schemaCache[name]
	ver := c.schemaVersion.Load()
	c.schemaCacheMu.RUnlock()
	if !ok {
		return nil, 0, false
	}
	return def, ver, true
}

// putCachedTable stores a TableDef in the schema cache. Must be called while
// holding Catalog.mu (or after a DDL operation) so the version is consistent.
func (c *Catalog) putCachedTable(name string, def *TableDef) {
	c.schemaCacheMu.Lock()
	if c.schemaCache == nil {
		c.schemaCache = make(map[string]*TableDef)
	}
	c.schemaCache[name] = def
	c.schemaCacheMu.Unlock()
}

// invalidateSchemaCache clears all cached entries and bumps schemaVersion.
// Call after any DDL operation.
func (c *Catalog) invalidateSchemaCache() {
	c.schemaVersion.Add(1)
	c.schemaCacheMu.Lock()
	c.schemaCache = make(map[string]*TableDef)
	c.schemaCacheMu.Unlock()
	// DDL changes the schema, so any cached query results may now be stale or
	// reference a dropped/altered table. The query result cache only tracked DML
	// invalidation; DDL must flush it too (otherwise a SELECT could serve rows
	// from a table that was just DROPped or ALTERed). DDL is rare, so flush all.
	if c.queryCache != nil {
		c.queryCache.InvalidateAll()
	}
}

func (c *Catalog) CreateView(name string, query *query.SelectStmt) error {
	return c.CreateViewSQL(name, query, "")
}

func (c *Catalog) CreateViewSQL(name string, viewQuery *query.SelectStmt, sql string) error {
	return c.createViewSQL(name, viewQuery, sql, false)
}

func (c *Catalog) CreateTemporaryViewSQL(name string, viewQuery *query.SelectStmt, sql string) error {
	return c.createViewSQL(name, viewQuery, sql, true)
}

func (c *Catalog) createViewSQL(name string, viewQuery *query.SelectStmt, sql string, temporary bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	if _, exists := c.views[name]; exists {
		return ErrTableExists
	}
	if _, exists := c.tables[name]; exists {
		return ErrTableExists
	}
	if c.viewSQL == nil {
		c.viewSQL = make(map[string]string)
	}
	if c.viewTemporary == nil {
		c.viewTemporary = make(map[string]bool)
	}
	c.views[name] = viewQuery
	if strings.TrimSpace(sql) == "" {
		sql = createViewSQL(name, viewQuery)
	}
	c.viewSQL[name] = strings.TrimSpace(sql)
	c.viewTemporary[name] = temporary
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:        undoCreateView,
			viewName:      name,
			viewSQL:       c.viewSQL[name],
			viewTemporary: temporary,
		})
	}
	return nil
}

func (c *Catalog) CreateOrReplaceViewSQL(name string, viewQuery *query.SelectStmt, sql string) error {
	return c.createOrReplaceViewSQL(name, viewQuery, sql, false)
}

func (c *Catalog) CreateOrReplaceTemporaryViewSQL(name string, viewQuery *query.SelectStmt, sql string) error {
	return c.createOrReplaceViewSQL(name, viewQuery, sql, true)
}

func (c *Catalog) createOrReplaceViewSQL(name string, viewQuery *query.SelectStmt, sql string, temporary bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	if _, exists := c.tables[name]; exists {
		return ErrTableExists
	}
	if c.viewSQL == nil {
		c.viewSQL = make(map[string]string)
	}
	if c.viewTemporary == nil {
		c.viewTemporary = make(map[string]bool)
	}
	oldQuery, existed := c.views[name]
	oldSQL := c.viewSQL[name]
	oldTemporary := c.viewTemporary[name]
	if strings.TrimSpace(sql) == "" {
		sql = createViewSQL(name, viewQuery)
	}
	c.views[name] = viewQuery
	c.viewSQL[name] = strings.TrimSpace(sql)
	c.viewTemporary[name] = temporary
	if temporary && existed && !oldTemporary {
		if err := c.deleteCatalogDef("view:" + name); err != nil {
			return fmt.Errorf("failed to delete replaced view metadata %s: %w", name, err)
		}
	}
	if c.isCurrentTxnActive() {
		if existed {
			c.appendUndoEntry(undoEntry{
				action:        undoDropView,
				viewName:      name,
				viewQuery:     cloneSelectStmt(oldQuery),
				viewSQL:       oldSQL,
				viewTemporary: oldTemporary,
			})
		} else {
			c.appendUndoEntry(undoEntry{
				action:        undoCreateView,
				viewName:      name,
				viewSQL:       c.viewSQL[name],
				viewTemporary: temporary,
			})
		}
	}
	return nil
}

func (c *Catalog) GetView(name string) (*query.SelectStmt, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getViewLocked(name)
}

// ListViewSQL returns persisted CREATE VIEW statements keyed by view name.
func (c *Catalog) ListViewSQL() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(map[string]string, len(c.viewSQL))
	for name, sql := range c.viewSQL {
		if c.viewTemporary[name] {
			continue
		}
		out[name] = sql
	}
	return out
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
	defer c.invalidateSchemaCache()
	if _, exists := c.views[name]; !exists {
		return ErrTableNotFound
	}
	temporary := c.viewTemporary[name]
	if !temporary {
		if err := c.deleteCatalogDef("view:" + name); err != nil {
			return fmt.Errorf("failed to delete view metadata %s: %w", name, err)
		}
	}
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:        undoDropView,
			viewName:      name,
			viewQuery:     cloneSelectStmt(c.views[name]),
			viewSQL:       c.viewSQL[name],
			viewTemporary: temporary,
		})
	}
	delete(c.views, name)
	delete(c.viewSQL, name)
	delete(c.viewTemporary, name)
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
	return c.CreateTriggerSQL(stmt, "")
}

func (c *Catalog) CreateTriggerSQL(stmt *query.CreateTriggerStmt, sql string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	// Check if table or view exists
	_, tableExists := c.tables[stmt.Table]
	_, viewExists := c.views[stmt.Table]
	if !tableExists && !viewExists {
		return fmt.Errorf("table or view not found: %s", stmt.Table)
	}

	if _, exists := c.triggers[stmt.Name]; exists {
		return fmt.Errorf("trigger %s already exists", stmt.Name)
	}
	if c.triggerSQL == nil {
		c.triggerSQL = make(map[string]string)
	}
	if strings.TrimSpace(sql) == "" {
		sql = createTriggerSQL(stmt)
	}
	stmt.RawSQL = strings.TrimSpace(sql)
	c.triggers[stmt.Name] = stmt
	c.triggerSQL[stmt.Name] = stmt.RawSQL
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:      undoCreateTrigger,
			triggerName: stmt.Name,
			triggerSQL:  stmt.RawSQL,
		})
	}
	return nil
}

func (c *Catalog) GetTrigger(name string) (*query.CreateTriggerStmt, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	trigger, exists := c.triggers[name]
	if !exists {
		return nil, fmt.Errorf("trigger %s not found", name)
	}
	return cloneCreateTriggerStmt(trigger), nil
}

// ListTriggerSQL returns persisted CREATE TRIGGER statements keyed by trigger name.
func (c *Catalog) ListTriggerSQL() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(map[string]string, len(c.triggerSQL))
	for name, sql := range c.triggerSQL {
		out[name] = sql
	}
	return out
}

func (c *Catalog) DropTrigger(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	if _, exists := c.triggers[name]; !exists {
		return fmt.Errorf("trigger %s not found", name)
	}
	if err := c.deleteCatalogDef("trg:" + name); err != nil {
		return fmt.Errorf("failed to delete trigger metadata %s: %w", name, err)
	}
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:      undoDropTrigger,
			triggerName: name,
			triggerStmt: cloneCreateTriggerStmt(c.triggers[name]),
			triggerSQL:  c.triggerSQL[name],
		})
	}
	delete(c.triggers, name)
	delete(c.triggerSQL, name)
	return nil
}

func (c *Catalog) GetTriggersForTable(tableName string, event string) []*query.CreateTriggerStmt {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTriggersForTableLocked(tableName, event)
}

// maxTriggerRecursionDepth bounds cascading/recursive trigger execution so a
// self-referential or mutually-recursive trigger returns an error instead of
// overflowing the goroutine stack (a fatal, unrecoverable crash). Matches
// SQLite's default recursive-trigger limit.
const maxTriggerRecursionDepth = 1000

// getTriggersForTableLocked is the lock-free internal version. Must be called with mu held.
func (c *Catalog) getTriggersForTableLocked(tableName string, event string) []*query.CreateTriggerStmt {
	var result []*query.CreateTriggerStmt
	for _, trigger := range c.triggers {
		if trigger.Table == tableName && (event == "" || trigger.Event == event) {
			result = append(result, cloneCreateTriggerStmt(trigger))
		}
	}
	// Go map iteration is randomized; sort by trigger name so multiple triggers
	// on the same table/event fire in a deterministic order (run-to-run stable).
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
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

		if err := c.executeTriggerBody(ctx, trigger.Name, trigger.Body, newRow, oldRow, columns); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) executeTriggerBody(ctx context.Context, triggerName string, body []query.Statement, newRow []interface{}, oldRow []interface{}, columns []ColumnDef) error {
	// Bound recursion: a trigger body re-enters insert/update/deleteLocked,
	// which fire triggers again. Without this, a self-referential or
	// mutually-recursive trigger overflows the stack (fatal crash). Guarded by
	// c.mu, which is held throughout trigger execution.
	if c.triggerDepth >= maxTriggerRecursionDepth {
		return fmt.Errorf("trigger recursion depth exceeded (%d) executing trigger %s", maxTriggerRecursionDepth, triggerName)
	}
	c.triggerDepth++
	defer func() { c.triggerDepth-- }()

	ts := c.getCurrentTxn()
	createdTxn := false
	if ts == nil {
		ts = c.getTxnState()
		ts.txnActive = true
		c.registerGoroutineTxn(ts)
		createdTxn = true
	}

	undoStart := len(ts.undoLog)
	pendingStart := len(ts.pendingWrites)
	finish := func() {
		if createdTxn {
			ts.txnActive = false
			c.unregisterGoroutineTxn()
			c.putTxnState(ts)
		}
	}

	for _, bodyStmt := range body {
		resolved := c.resolveTriggerRefs(bodyStmt, newRow, oldRow, columns)
		if err := c.executeTriggerStatement(ctx, resolved); err != nil {
			rollbackErr := c.rollbackTriggerBodyEffects(ts, undoStart, pendingStart)
			finish()
			if rollbackErr != nil {
				return fmt.Errorf("trigger %s: %w; rollback failed: %v", triggerName, err, rollbackErr)
			}
			return fmt.Errorf("trigger %s: %w", triggerName, err)
		}
	}

	if createdTxn {
		ts.undoLog = ts.undoLog[:0]
		ts.pendingWrites = ts.pendingWrites[:0]
	}
	finish()
	return nil
}

func (c *Catalog) rollbackTriggerBodyEffects(ts *catalogTxnState, undoStart int, pendingStart int) error {
	var rollbackErr error
	for i := len(ts.undoLog) - 1; i >= undoStart; i-- {
		entry := ts.undoLog[i]
		if err := c.applyUndoEntry(entry, "trigger rollback"); err != nil && rollbackErr == nil {
			rollbackErr = err
		}
		rollbackErr = c.reverseIndexChanges(entry, "trigger rollback", rollbackErr)
	}
	ts.undoLog = ts.undoLog[:undoStart]
	ts.pendingWrites = ts.pendingWrites[:pendingStart]
	rebuildPendingWriteMap(ts)
	return rollbackErr
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
		tbl := toUpperFast(e.Table)
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
		newOrderBy := make([]*query.OrderByExpr, len(e.OrderBy))
		for i, ob := range e.OrderBy {
			if ob == nil {
				continue
			}
			copied := *ob
			copied.Expr = c.resolveTriggerExpr(ob.Expr, newRow, oldRow, columns)
			newOrderBy[i] = &copied
		}
		return &query.FunctionCall{Name: e.Name, Args: newArgs, Distinct: e.Distinct, OrderBy: newOrderBy, Filter: c.resolveTriggerExpr(e.Filter, newRow, oldRow, columns)}
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
	return c.CreateProcedureSQL(stmt, "")
}

func (c *Catalog) CreateProcedureSQL(stmt *query.CreateProcedureStmt, sql string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	if _, exists := c.procedures[stmt.Name]; exists {
		return fmt.Errorf("procedure %s already exists", stmt.Name)
	}
	if c.procedureSQL == nil {
		c.procedureSQL = make(map[string]string)
	}
	if strings.TrimSpace(sql) == "" {
		sql = createProcedureSQL(stmt)
	}
	stmt.RawSQL = strings.TrimSpace(sql)
	c.procedures[stmt.Name] = stmt
	c.procedureSQL[stmt.Name] = stmt.RawSQL
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:        undoCreateProcedure,
			procedureName: stmt.Name,
			procedureSQL:  stmt.RawSQL,
		})
	}
	return nil
}

func (c *Catalog) GetProcedure(name string) (*query.CreateProcedureStmt, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	proc, exists := c.procedures[name]
	if !exists {
		return nil, fmt.Errorf("procedure %s not found", name)
	}
	return cloneCreateProcedureStmt(proc), nil
}

// ListProcedureSQL returns persisted CREATE PROCEDURE statements keyed by procedure name.
func (c *Catalog) ListProcedureSQL() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(map[string]string, len(c.procedureSQL))
	for name, sql := range c.procedureSQL {
		out[name] = sql
	}
	return out
}

func (c *Catalog) DropProcedure(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	if _, exists := c.procedures[name]; !exists {
		return fmt.Errorf("procedure %s not found", name)
	}
	if err := c.deleteCatalogDef("proc:" + name); err != nil {
		return fmt.Errorf("failed to delete procedure metadata %s: %w", name, err)
	}
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:        undoDropProcedure,
			procedureName: name,
			procedureStmt: cloneCreateProcedureStmt(c.procedures[name]),
			procedureSQL:  c.procedureSQL[name],
		})
	}
	delete(c.procedures, name)
	delete(c.procedureSQL, name)
	return nil
}

func (c *Catalog) GetTableStats(tableName string) (*StatsTableStats, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	stats, exists := c.stats[tableName]
	if !exists {
		return nil, fmt.Errorf("no statistics for table %s", tableName)
	}
	return cloneTableStats(stats), nil
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
				colName = "column_" + strconv.Itoa(i)
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
					exprRow[j] = &query.NumberLiteral{Value: float64(v), Raw: strconv.FormatInt(int64(v), 10)}
				case int:
					exprRow[j] = &query.NumberLiteral{Value: float64(v), Raw: strconv.FormatInt(int64(v), 10)}
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

		if err := c.executeTriggerBody(ctx, trigger.Name, trigger.Body, newRow, nil, columns); err != nil {
			return 0, 0, fmt.Errorf("INSTEAD OF INSERT trigger failed: %w", err)
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
				return 0, 0, fmt.Errorf("failed to read row for INSTEAD OF UPDATE: %w", err)
			}
			row, err := decodeRow(valueData, len(columns))
			if err != nil {
				return 0, 0, fmt.Errorf("failed to decode row for INSTEAD OF UPDATE on %s: %w", stmt.Table, err)
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

		if err := c.executeTriggerBody(ctx, trigger.Name, trigger.Body, newRow, row, columns); err != nil {
			return 0, 0, fmt.Errorf("INSTEAD OF UPDATE trigger failed: %w", err)
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
				return 0, 0, fmt.Errorf("failed to read row for INSTEAD OF DELETE: %w", err)
			}
			row, err := decodeRow(valueData, len(columns))
			if err != nil {
				return 0, 0, fmt.Errorf("failed to decode row for INSTEAD OF DELETE on %s: %w", stmt.Table, err)
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

		if err := c.executeTriggerBody(ctx, trigger.Name, trigger.Body, nil, row, columns); err != nil {
			return 0, 0, fmt.Errorf("INSTEAD OF DELETE trigger failed: %w", err)
		}
		rowsAffected++
	}

	return 0, rowsAffected, nil
}
