package catalog

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

var (
	ErrTableExists    = errors.New("table already exists")
	ErrTableNotFound  = errors.New("table not found")
	ErrColumnNotFound = errors.New("column not found")
	ErrIndexExists    = errors.New("index already exists")
	ErrIndexNotFound  = errors.New("index not found")
)

// TableDef represents a table definition
type TableDef struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"` // "table" or "collection"
	Columns     []ColumnDef    `json:"columns"`
	PrimaryKey  string         `json:"primary_key"`
	CreatedAt   int64          `json:"created_at"`
	RootPageID  uint32         `json:"root_page_id"`
	ForeignKeys []ForeignKeyDef `json:"foreign_keys,omitempty"`
	// Performance: cache column indices (not persisted)
	columnIndices map[string]int `json:"-"`
}

// ForeignKeyDef represents a foreign key constraint
type ForeignKeyDef struct {
	Columns       []string `json:"columns"`
	ReferencedTable string `json:"referenced_table"`
	ReferencedColumns []string `json:"referenced_columns"`
	OnDelete      string `json:"on_delete"` // NO ACTION, CASCADE, SET NULL, RESTRICT
	OnUpdate      string `json:"on_update"` // NO ACTION, CASCADE, SET NULL, RESTRICT
}

// ColumnDef represents a column definition
type ColumnDef struct {
	Name         string           `json:"name"`
	Type         string           `json:"type"` // INTEGER, TEXT, REAL, BLOB, JSON, BOOLEAN
	NotNull      bool             `json:"not_null"`
	Unique       bool             `json:"unique"`
	PrimaryKey   bool             `json:"primary_key"`
	AutoIncrement bool             `json:"auto_increment"`
	Default      string           `json:"default,omitempty"`
	Check        query.Expression `json:"check,omitempty"`
}

// IndexDef represents an index definition
type IndexDef struct {
	Name       string   `json:"name"`
	TableName  string   `json:"table_name"`
	Columns    []string `json:"columns"`
	Unique     bool     `json:"unique"`
	RootPageID uint32   `json:"root_page_id"`
}

// selectColInfo holds information about selected columns in a query
type selectColInfo struct {
	name          string
	tableName     string // table name for JOINs
	index         int
	isAggregate   bool
	aggregateType string // COUNT, SUM, AVG, MIN, MAX
	aggregateCol  string // column name for SUM, AVG, MIN, MAX
}

// Catalog manages database schema metadata
type Catalog struct {
	tree        *btree.BTree
	tables      map[string]*TableDef
	indexes     map[string]*IndexDef
	indexTrees  map[string]*btree.BTree // B+Trees for indexes
	pool        *storage.BufferPool
	wal         *storage.WAL
	tableTrees  map[string]*btree.BTree // Each table has its own B+Tree
	tableData   map[string]map[string][]byte // Temporary: simple in-memory storage
	views       map[string]*query.SelectStmt // Views store their SELECT query
	triggers    map[string]*query.CreateTriggerStmt // Triggers store their definition
	procedures  map[string]*query.CreateProcedureStmt // Procedures store their definition
	keyCounter  int64 // For generating unique keys
	txnID       uint64 // Current transaction ID
	txnActive   bool   // Is a transaction active
}

// New creates a new catalog
func New(tree *btree.BTree, pool *storage.BufferPool, wal *storage.WAL) *Catalog {
	return &Catalog{
		tree:       tree,
		tables:     make(map[string]*TableDef),
		indexes:    make(map[string]*IndexDef),
		indexTrees: make(map[string]*btree.BTree),
		pool:       pool,
		wal:        wal,
		tableTrees: make(map[string]*btree.BTree),
		tableData:  make(map[string]map[string][]byte),
		views:      make(map[string]*query.SelectStmt),
		triggers:   make(map[string]*query.CreateTriggerStmt),
		procedures: make(map[string]*query.CreateProcedureStmt),
		keyCounter: 0,
	}
}

// SetWAL sets the WAL for the catalog
func (c *Catalog) SetWAL(wal *storage.WAL) {
	c.wal = wal
}

// BeginTransaction begins a new transaction
func (c *Catalog) BeginTransaction(txnID uint64) {
	c.txnID = txnID
	c.txnActive = true
}

// CommitTransaction commits the current transaction
func (c *Catalog) CommitTransaction() error {
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
	return nil
}

// RollbackTransaction rolls back the current transaction
func (c *Catalog) RollbackTransaction() error {
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
	c.txnActive = false
	return nil
}

// IsTransactionActive returns true if a transaction is active
func (c *Catalog) IsTransactionActive() bool {
	return c.txnActive
}

// TxnID returns the current transaction ID
func (c *Catalog) TxnID() uint64 {
	return c.txnID
}

// CreateTable creates a new table
func (c *Catalog) CreateTable(stmt *query.CreateTableStmt) error {
	if !stmt.IfNotExists {
		if _, exists := c.tables[stmt.Table]; exists {
			return ErrTableExists
		}
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
		CreatedAt:   0, // TODO: use current timestamp
		RootPageID:  tree.RootPageID(),
		ForeignKeys: make([]ForeignKeyDef, len(stmt.ForeignKeys)),
	}

	for i, col := range stmt.Columns {
		tableDef.Columns[i] = ColumnDef{
			Name:          col.Name,
			Type:          tokenTypeToColumnType(col.Type),
			NotNull:       col.NotNull,
			Unique:        col.Unique,
			PrimaryKey:    col.PrimaryKey,
			AutoIncrement: col.AutoIncrement,
			Check:         col.Check,
		}
		if col.PrimaryKey {
			tableDef.PrimaryKey = col.Name
		}
	}

	// Copy foreign key definitions
	for i, fk := range stmt.ForeignKeys {
		tableDef.ForeignKeys[i] = ForeignKeyDef{
			Columns:          fk.Columns,
			ReferencedTable:  fk.ReferencedTable,
			ReferencedColumns: fk.ReferencedColumns,
			OnDelete:        fk.OnDelete,
			OnUpdate:        fk.OnUpdate,
		}
	}

	c.tables[stmt.Table] = tableDef
	c.tableTrees[stmt.Table] = tree // Store the tree for data operations
	c.tableData[stmt.Table] = make(map[string][]byte) // Initialize data storage

	// Build column index cache for performance
	tableDef.buildColumnIndexCache()

	// Store table definition in catalog tree
	return c.storeTableDef(tableDef)
}

// storeTableDef stores a table definition in the catalog tree
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

// DropTable drops a table
func (c *Catalog) DropTable(stmt *query.DropTableStmt) error {
	if !stmt.IfExists {
		if _, exists := c.tables[stmt.Table]; !exists {
			return ErrTableNotFound
		}
	}

	delete(c.tables, stmt.Table)

	// Remove from catalog tree
	if c.tree != nil {
		key := []byte("tbl:" + stmt.Table)
		return c.tree.Delete(key)
	}
	return nil
}

// GetTable retrieves a table definition
func (c *Catalog) GetTable(name string) (*TableDef, error) {
	table, exists := c.tables[name]
	if !exists {
		return nil, ErrTableNotFound
	}
	return table, nil
}

// CreateView creates a new view
func (c *Catalog) CreateView(name string, query *query.SelectStmt) error {
	if _, exists := c.views[name]; exists {
		return ErrTableExists
	}
	if _, exists := c.tables[name]; exists {
		return ErrTableExists
	}
	c.views[name] = query
	return nil
}

// GetView retrieves a view definition
func (c *Catalog) GetView(name string) (*query.SelectStmt, error) {
	view, exists := c.views[name]
	if !exists {
		return nil, ErrTableNotFound
	}
	return view, nil
}

// DropView drops a view
func (c *Catalog) DropView(name string) error {
	if _, exists := c.views[name]; !exists {
		return ErrTableNotFound
	}
	delete(c.views, name)
	return nil
}

// HasTableOrView checks if a table or view exists
func (c *Catalog) HasTableOrView(name string) bool {
	_, tableExists := c.tables[name]
	_, viewExists := c.views[name]
	return tableExists || viewExists
}

// CreateTrigger creates a new trigger
func (c *Catalog) CreateTrigger(stmt *query.CreateTriggerStmt) error {
	if _, exists := c.triggers[stmt.Name]; exists {
		return fmt.Errorf("trigger %s already exists", stmt.Name)
	}
	c.triggers[stmt.Name] = stmt
	return nil
}

// GetTrigger retrieves a trigger definition
func (c *Catalog) GetTrigger(name string) (*query.CreateTriggerStmt, error) {
	trigger, exists := c.triggers[name]
	if !exists {
		return nil, fmt.Errorf("trigger %s not found", name)
	}
	return trigger, nil
}

// DropTrigger drops a trigger
func (c *Catalog) DropTrigger(name string) error {
	if _, exists := c.triggers[name]; !exists {
		return fmt.Errorf("trigger %s not found", name)
	}
	delete(c.triggers, name)
	return nil
}

// GetTriggersForTable retrieves all triggers for a table
func (c *Catalog) GetTriggersForTable(tableName string, event string) []*query.CreateTriggerStmt {
	var result []*query.CreateTriggerStmt
	for _, trigger := range c.triggers {
		if trigger.Table == tableName && (event == "" || trigger.Event == event) {
			result = append(result, trigger)
		}
	}
	return result
}

// executeTriggers executes all triggers for a given table and event
func (c *Catalog) executeTriggers(tableName string, event string, timing string, row []interface{}, columns []ColumnDef) error {
	triggers := c.GetTriggersForTable(tableName, event)
	for _, trigger := range triggers {
		// Check timing (BEFORE or AFTER)
		if trigger.Time != timing {
			continue
		}
		// Execute trigger body (stored statements)
		// For now, triggers are a framework - execution would require
		// a full statement execution engine
		// This is a placeholder for actual trigger execution
		_ = row
		_ = columns
	}
	return nil
}

// CreateProcedure creates a new stored procedure
func (c *Catalog) CreateProcedure(stmt *query.CreateProcedureStmt) error {
	if _, exists := c.procedures[stmt.Name]; exists {
		return fmt.Errorf("procedure %s already exists", stmt.Name)
	}
	c.procedures[stmt.Name] = stmt
	return nil
}

// GetProcedure retrieves a procedure definition
func (c *Catalog) GetProcedure(name string) (*query.CreateProcedureStmt, error) {
	proc, exists := c.procedures[name]
	if !exists {
		return nil, fmt.Errorf("procedure %s not found", name)
	}
	return proc, nil
}

// DropProcedure drops a procedure
func (c *Catalog) DropProcedure(name string) error {
	if _, exists := c.procedures[name]; !exists {
		return fmt.Errorf("procedure %s not found", name)
	}
	delete(c.procedures, name)
	return nil
}

// Insert inserts rows into a table
func (c *Catalog) Insert(stmt *query.InsertStmt, args []interface{}) (int64, int64, error) {
	table, err := c.GetTable(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	tree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	// Determine column mapping
	// If columns are specified in INSERT, use them; otherwise use all table columns
	var insertColumns []string
	if len(stmt.Columns) > 0 {
		insertColumns = stmt.Columns
	} else {
		// Use all columns from table definition
		for _, col := range table.Columns {
			insertColumns = append(insertColumns, col.Name)
		}
	}

	// Insert each row
	rowsAffected := int64(0)
	autoIncValue := int64(0)

	// Pre-calculate insert column indices for performance
	insertColIndices := make([]int, len(insertColumns))
	for i, colName := range insertColumns {
		insertColIndices[i] = table.GetColumnIndex(colName)
	}

	for _, valueRow := range stmt.Values {
		// Generate unique key (use auto-increment if primary key exists)
		var key string
		hasPrimaryKey := false
		for i, colName := range insertColumns {
			if colName == table.PrimaryKey {
				hasPrimaryKey = true
				// Get primary key value from valueRow if provided
				if i < len(valueRow) {
					if numLit, ok := valueRow[i].(*query.NumberLiteral); ok {
						key = fmt.Sprintf("%020d", int64(numLit.Value))
					}
				}
			}
		}

		if !hasPrimaryKey || key == "" {
			// Generate auto-increment key
			autoIncValue = atomic.AddInt64(&c.keyCounter, 1)
			key = fmt.Sprintf("%020d", autoIncValue)
		}

		// Build full row with all columns
		rowValues := make([]interface{}, len(table.Columns))

		// Map provided values to their columns using pre-calculated indices
		for colIdx, tableColIdx := range insertColIndices {
			if colIdx < len(valueRow) && tableColIdx >= 0 {
				val, err := EvalExpression(valueRow[colIdx], args)
				if err != nil {
					rowValues[tableColIdx] = nil
				} else {
					rowValues[tableColIdx] = val
				}
			}
		}

		// Fill remaining columns with defaults
		for i, col := range table.Columns {
			if rowValues[i] == nil {
				// Set default values based on column type
				switch col.Type {
				case "INTEGER":
					rowValues[i] = int64(0)
				case "TEXT":
					rowValues[i] = ""
				case "REAL":
					rowValues[i] = float64(0)
				case "BOOLEAN":
					rowValues[i] = false
				default:
					rowValues[i] = nil
				}
			}
		}

		// Check UNIQUE constraints before inserting
		for i, col := range table.Columns {
			if col.Unique && rowValues[i] != nil {
				// Check if a row with this unique value already exists
				iter, _ := tree.Scan(nil, nil)
				for iter.HasNext() {
					_, existingData, err := iter.Next()
					if err != nil {
						break
					}
					var existingRow []interface{}
					if err := json.Unmarshal(existingData, &existingRow); err != nil {
						continue
					}
					if len(existingRow) > i && compareValues(rowValues[i], existingRow[i]) == 0 {
						iter.Close()
						return 0, rowsAffected, fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
					}
				}
				iter.Close()
			}
		}

		// Check CHECK constraints before inserting
		for _, col := range table.Columns {
			if col.Check != nil {
				result, err := evaluateExpression(c, rowValues, table.Columns, col.Check, args)
				if err != nil {
					return 0, rowsAffected, fmt.Errorf("CHECK constraint failed: %v", err)
				}
				if resultBool, ok := result.(bool); !ok || !resultBool {
					return 0, rowsAffected, fmt.Errorf("CHECK constraint failed for column: %s", col.Name)
				}
			}
		}

		// Check FOREIGN KEY constraints before inserting
		for _, fk := range table.ForeignKeys {
			// Get the value(s) for the foreign key column(s)
			for i, colName := range fk.Columns {
				colIdx := table.GetColumnIndex(colName)
				if colIdx < 0 || colIdx >= len(rowValues) {
					continue
				}
				fkValue := rowValues[colIdx]
				if fkValue == nil {
					continue // NULL values skip FK check
				}

				// Check if referenced row exists
				refTable, err := c.GetTable(fk.ReferencedTable)
				if err != nil {
					return 0, rowsAffected, fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
				}

				refColIdx := 0
				if len(fk.ReferencedColumns) > i {
					refColIdx = refTable.GetColumnIndex(fk.ReferencedColumns[i])
				} else if len(refTable.Columns) > 0 {
					// Default to first column
					refColIdx = 0
				}

				refTree, exists := c.tableTrees[fk.ReferencedTable]
				if !exists {
					return 0, rowsAffected, fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
				}

				// Search for matching row
				found := false
				refIter, _ := refTree.Scan(nil, nil)
				for refIter.HasNext() {
					_, refData, err := refIter.Next()
					if err != nil {
						break
					}
					var refRow []interface{}
					if err := json.Unmarshal(refData, &refRow); err != nil {
						continue
					}
					if refColIdx < len(refRow) && compareValues(fkValue, refRow[refColIdx]) == 0 {
						found = true
						break
					}
				}
				refIter.Close()

				if !found {
					return 0, rowsAffected, fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValue, fk.ReferencedTable)
				}
			}
		}

		// Encode row
		valueData, err := json.Marshal(rowValues)
		if err != nil {
			return 0, rowsAffected, err
		}

		// Log to WAL before applying change
		if c.wal != nil && c.txnActive {
			// For INSERT, we log the key and value
			// Format: key (null-terminated) + value
			walData := append([]byte(key), 0)
			walData = append(walData, valueData...)
			record := &storage.WALRecord{
				TxnID: c.txnID,
				Type:  storage.WALInsert,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				return 0, rowsAffected, err
			}
		}

		// Store in B+Tree
		tree.Put([]byte(key), valueData)

		// Update indexes
		for idxName, idxTree := range c.indexTrees {
			idxDef := c.indexes[idxName]
			if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
				colIdx := table.GetColumnIndex(idxDef.Columns[0])
				if colIdx >= 0 && colIdx < len(rowValues) {
					indexKey := fmt.Sprintf("%v", rowValues[colIdx])
					idxTree.Put([]byte(indexKey), []byte(key))
				}
			}
		}

		rowsAffected++
	}

	// Execute AFTER INSERT triggers
	_ = c.executeTriggers(stmt.Table, "INSERT", "AFTER", nil, table.Columns)

	return 0, rowsAffected, nil
}

// Update updates rows in a table
func (c *Catalog) Update(stmt *query.UpdateStmt, args []interface{}) (int64, int64, error) {
	table, err := c.GetTable(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	tree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	rowsAffected := int64(0)
	iter, _ := tree.Scan(nil, nil)

	// Collect keys to update
	var keys [][]byte
	var values [][]byte

	// Pre-calculate column indices for SET clauses
	setColumnIndices := make([]int, len(stmt.Set))
	for i, setClause := range stmt.Set {
		setColumnIndices[i] = table.GetColumnIndex(setClause.Column)
	}

	for iter.HasNext() {
		key, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode row
		row, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present
		if stmt.Where != nil {
			matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
			if err != nil {
				continue
			}
			if !matched {
				continue // Skip row that doesn't match WHERE condition
			}
		}

		// Make a copy of the row to update
		updatedRow := make([]interface{}, len(row))
		copy(updatedRow, row)

		// Update fields - use pre-calculated column indices
		for i, setClause := range stmt.Set {
			colIdx := setColumnIndices[i]
			if colIdx >= 0 {
				newVal, err := EvalExpression(setClause.Value, args)
				if err != nil {
					continue
				}
				updatedRow[colIdx] = newVal
			}
		}

		// Check UNIQUE constraints before updating
		for i, col := range table.Columns {
			if col.Unique && updatedRow[i] != nil {
				// Check if another row (not this one) has the same unique value
				checkIter, _ := tree.Scan(nil, nil)
				for checkIter.HasNext() {
					checkKey, existingData, err := checkIter.Next()
					if err != nil {
						break
					}
					// Skip the current row being updated
					if string(checkKey) == string(key) {
						continue
					}
					var existingRow []interface{}
					if err := json.Unmarshal(existingData, &existingRow); err != nil {
						continue
					}
					if len(existingRow) > i && compareValues(updatedRow[i], existingRow[i]) == 0 {
						checkIter.Close()
						return 0, rowsAffected, fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
					}
				}
				checkIter.Close()
			}
		}

		// Check CHECK constraints before updating
		for _, col := range table.Columns {
			if col.Check != nil {
				result, err := evaluateExpression(c, updatedRow, table.Columns, col.Check, args)
				if err != nil {
					return 0, rowsAffected, fmt.Errorf("CHECK constraint failed: %v", err)
				}
				if resultBool, ok := result.(bool); !ok || !resultBool {
					return 0, rowsAffected, fmt.Errorf("CHECK constraint failed for column: %s", col.Name)
				}
			}
		}

		// Re-encode row
		newValueData, err := json.Marshal(updatedRow)
		if err != nil {
			continue
		}

		keys = append(keys, key)
		values = append(values, newValueData)
		rowsAffected++
	}
	iter.Close()

	// Apply updates
	for i, key := range keys {
		// Log to WAL before applying change
		if c.wal != nil && c.txnActive {
			// For UPDATE, we log the key and new value
			// Format: key (null-terminated) + value
			walData := append([]byte(key), 0)
			walData = append(walData, values[i]...)
			record := &storage.WALRecord{
				TxnID: c.txnID,
				Type:  storage.WALUpdate,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				return 0, rowsAffected, err
			}
		}

		tree.Put(key, values[i])

		// Update indexes with new values
		// Decode the updated row to get new column values
		updatedRow, err := decodeRow(values[i], len(table.Columns))
		if err == nil {
			for idxName, idxTree := range c.indexTrees {
				idxDef := c.indexes[idxName]
				if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
					colIdx := table.GetColumnIndex(idxDef.Columns[0])
					if colIdx >= 0 && colIdx < len(updatedRow) {
						indexKey := fmt.Sprintf("%v", updatedRow[colIdx])
						idxTree.Put([]byte(indexKey), key)
					}
				}
			}
		}
	}

	// Execute AFTER UPDATE triggers
	_ = c.executeTriggers(stmt.Table, "UPDATE", "AFTER", nil, table.Columns)

	return 0, rowsAffected, nil
}

// Delete deletes rows from a table
func (c *Catalog) Delete(stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	table, err := c.GetTable(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	tree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	rowsAffected := int64(0)
	iter, _ := tree.Scan(nil, nil)

	// Collect keys to delete
	var keys [][]byte
	for iter.HasNext() {
		key, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode row
		row, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present
		if stmt.Where != nil {
			matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
			if err != nil {
				continue
			}
			if !matched {
				continue // Skip row that doesn't match WHERE condition
			}
		}

		keys = append(keys, key)
		rowsAffected++
	}
	iter.Close()

	// Delete collected keys
	for _, key := range keys {
		// Log to WAL before applying change
		if c.wal != nil && c.txnActive {
			// For DELETE, we log the key being deleted
			// Format: key (null-terminated)
			walData := append([]byte(key), 0)
			record := &storage.WALRecord{
				TxnID: c.txnID,
				Type:  storage.WALDelete,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				return 0, rowsAffected, err
			}
		}

		tree.Delete(key)
	}

	// Execute AFTER DELETE triggers
	_ = c.executeTriggers(stmt.Table, "DELETE", "AFTER", nil, table.Columns)

	return 0, rowsAffected, nil
}

// Select queries rows from a table or view
func (c *Catalog) Select(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	if stmt.From == nil {
		return nil, nil, errors.New("no table specified")
	}

	// Check if it's a view first
	view, viewErr := c.GetView(stmt.From.Name)
	if viewErr == nil {
		// It's a view - execute the view's query
		return c.Select(view, args)
	}

	// Not a view - try to get as a table
	table, err := c.GetTable(stmt.From.Name)
	if err != nil {
		return nil, nil, err
	}

	// Get column names and their indices in the table (optimized with cache)
	var selectCols []selectColInfo
	var hasAggregates bool

	for _, col := range stmt.Columns {
		switch c := col.(type) {
		case *query.Identifier:
			// Use cached column index
			if idx := table.GetColumnIndex(c.Name); idx >= 0 {
				selectCols = append(selectCols, selectColInfo{name: c.Name, tableName: stmt.From.Name, index: idx})
			}
		case *query.StarExpr:
			// SELECT * - get all columns from table
			for i, tc := range table.Columns {
				selectCols = append(selectCols, selectColInfo{name: tc.Name, tableName: stmt.From.Name, index: i})
			}
		case *query.FunctionCall:
			// Handle aggregate functions: COUNT, SUM, AVG, MIN, MAX
			funcName := strings.ToUpper(c.Name)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" {
				hasAggregates = true
				colName := "*" // Default for COUNT(*)
				if len(c.Args) > 0 {
					if ident, ok := c.Args[0].(*query.Identifier); ok {
						colName = ident.Name
					}
				}
				selectCols = append(selectCols, selectColInfo{
					name:          c.Name + "(" + colName + ")",
					tableName:     stmt.From.Name,
					index:         -1,
					isAggregate:   true,
					aggregateType: funcName,
					aggregateCol:  colName,
				})
			}
		}
	}

	// Handle JOINs if present
	if len(stmt.Joins) > 0 {
		return c.executeSelectWithJoin(stmt, args, selectCols)
	}

	// Extract column names for return
	returnColumns := make([]string, len(selectCols))
	for i, ci := range selectCols {
		returnColumns[i] = ci.name
	}

	// If we have aggregates or GROUP BY, handle them differently
	if hasAggregates || len(stmt.GroupBy) > 0 {
		return c.computeAggregatesWithGroupBy(table, stmt, args, selectCols, returnColumns)
	}

	// Read all rows from B+Tree
	var rows [][]interface{}
	tree, exists := c.tableTrees[stmt.From.Name]
	if !exists {
		return returnColumns, rows, nil
	}

	// Try to use index for WHERE clause
	var useIndex bool
	var indexMatches map[string]bool
	if stmt.Where != nil {
		indexMatches, useIndex = c.useIndexForQuery(stmt.From.Name, stmt.Where)
	}

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		key, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// If using index, only fetch matching rows
		if useIndex {
			if !indexMatches[string(key)] {
				continue
			}
		}

		count++

		// Decode full row
		fullRow, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present
		if stmt.Where != nil {
			matched, err := evaluateWhere(c,fullRow, table.Columns, stmt.Where, args)
			if err != nil {
				continue // Skip row on error
			}
			if !matched {
				continue // Skip row that doesn't match WHERE condition
			}
		}

		// Extract only selected columns
		selectedRow := make([]interface{}, len(selectCols))
		for i, ci := range selectCols {
			if ci.index < len(fullRow) {
				selectedRow[i] = fullRow[ci.index]
			}
		}
		rows = append(rows, selectedRow)
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		rows = c.applyOrderBy(rows, selectCols, stmt.OrderBy)
	}

	// Apply DISTINCT if present
	if stmt.Distinct {
		rows = c.applyDistinct(rows)
	}

	// Apply OFFSET if present
	if stmt.Offset != nil {
		offsetVal, err := evaluateExpression(c, nil, nil, stmt.Offset, args)
		if err == nil {
			if offset, ok := toInt(offsetVal); ok && offset > 0 && offset < len(rows) {
				rows = rows[offset:]
			}
		}
	}

	// Apply LIMIT if present
	if stmt.Limit != nil {
		limitVal, err := evaluateExpression(c, nil, nil, stmt.Limit, args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && limit < len(rows) {
				rows = rows[:limit]
			}
		}
	}

	return returnColumns, rows, nil
}

// executeSelectWithJoin handles SELECT with JOIN clauses
func (c *Catalog) executeSelectWithJoin(stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo) ([]string, [][]interface{}, error) {
	// Support INNER, LEFT, RIGHT JOIN with ON clause
	// Get the main table
	mainTable, err := c.GetTable(stmt.From.Name)
	if err != nil {
		return nil, nil, err
	}

	mainTree, exists := c.tableTrees[stmt.From.Name]
	if !exists {
		return nil, [][]interface{}{}, nil
	}

	// Process each JOIN
	var resultRows [][]interface{}

	for _, join := range stmt.Joins {
		// Get the joined table
		joinTable, err := c.GetTable(join.Table.Name)
		if err != nil {
			continue
		}

		joinTree, exists := c.tableTrees[join.Table.Name]
		if !exists {
			continue
		}

		// Determine if this is a LEFT, RIGHT, or INNER join
		isLeftJoin := join.Type == query.TokenLeft
		isRightJoin := join.Type == query.TokenRight

		// For LEFT/RIGHT JOIN, we need to track matched rows
		matchedRightRows := make(map[int]bool)

		if isLeftJoin {
			// LEFT JOIN: first collect all matching rows
			mainIter, _ := mainTree.Scan(nil, nil)
			defer mainIter.Close()

			for mainIter.HasNext() {
				_, mainValueData, err := mainIter.Next()
				if err != nil {
					break
				}

				mainRow, err := decodeRow(mainValueData, len(mainTable.Columns))
				if err != nil {
					continue
				}

				joinIter, _ := joinTree.Scan(nil, nil)
				defer joinIter.Close()
				matched := false

				for joinIter.HasNext() {
					_, joinValueData, err := joinIter.Next()
					if err != nil {
						break
					}

					joinRow, err := decodeRow(joinValueData, len(joinTable.Columns))
					if err != nil {
						continue
					}

					// Check join condition
					if join.Condition != nil {
						combinedRow := append(mainRow, joinRow...)
						matched, err = evaluateWhere(c,combinedRow, append(mainTable.Columns, joinTable.Columns...), join.Condition, args)
						if err != nil || !matched {
							continue
						}
					} else {
						matched = true
					}

					// Create combined row
					combinedResult := make([]interface{}, 0)
					for _, ci := range selectCols {
						if ci.tableName == "" || ci.tableName == stmt.From.Name {
							if ci.index >= 0 && ci.index < len(mainRow) {
								combinedResult = append(combinedResult, mainRow[ci.index])
							} else {
								combinedResult = append(combinedResult, nil)
							}
						} else if ci.tableName == join.Table.Name {
							if ci.index >= 0 && ci.index < len(joinRow) {
								combinedResult = append(combinedResult, joinRow[ci.index])
							} else {
								combinedResult = append(combinedResult, nil)
							}
						}
					}
					resultRows = append(resultRows, combinedResult)
				}

				// If no match, add row with NULLs for join table columns
				if !matched {
					combinedResult := make([]interface{}, 0)
					for _, ci := range selectCols {
						if ci.tableName == "" || ci.tableName == stmt.From.Name {
							if ci.index >= 0 && ci.index < len(mainRow) {
								combinedResult = append(combinedResult, mainRow[ci.index])
							} else {
								combinedResult = append(combinedResult, nil)
							}
						} else if ci.tableName == join.Table.Name {
							// NULL for unmatched join columns
							combinedResult = append(combinedResult, nil)
						}
					}
					resultRows = append(resultRows, combinedResult)
				}
			}
		} else if isRightJoin {
			// RIGHT JOIN: collect all right table rows first
			joinIter, _ := joinTree.Scan(nil, nil)
			defer joinIter.Close()

			// Build a map of all left rows for lookup
			var mainRows [][]interface{}
			mainIter, _ := mainTree.Scan(nil, nil)
			defer mainIter.Close()

			for mainIter.HasNext() {
				_, mainValueData, err := mainIter.Next()
				if err != nil {
					break
				}
				mainRow, err := decodeRow(mainValueData, len(mainTable.Columns))
				if err != nil {
					continue
				}
				mainRows = append(mainRows, mainRow)
			}

			// Process right table rows
			rightRowIndex := 0
			for joinIter.HasNext() {
				_, joinValueData, err := joinIter.Next()
				if err != nil {
					break
				}

				joinRow, err := decodeRow(joinValueData, len(joinTable.Columns))
				if err != nil {
					continue
				}

				matched := false

				// Check against each main row
				for _, mainRow := range mainRows {
					if join.Condition != nil {
						combinedRow := append(mainRow, joinRow...)
						matched, err = evaluateWhere(c,combinedRow, append(mainTable.Columns, joinTable.Columns...), join.Condition, args)
						if err != nil || !matched {
							continue
						}
					} else {
						matched = true
					}

					// Create combined row
					combinedResult := make([]interface{}, 0)
					for _, ci := range selectCols {
						if ci.tableName == "" || ci.tableName == stmt.From.Name {
							if ci.index >= 0 && ci.index < len(mainRow) {
								combinedResult = append(combinedResult, mainRow[ci.index])
							} else {
								combinedResult = append(combinedResult, nil)
							}
						} else if ci.tableName == join.Table.Name {
							if ci.index >= 0 && ci.index < len(joinRow) {
								combinedResult = append(combinedResult, joinRow[ci.index])
							} else {
								combinedResult = append(combinedResult, nil)
							}
						}
					}
					resultRows = append(resultRows, combinedResult)
				}

				// If no match, add row with NULLs for main table columns
				if !matched {
					combinedResult := make([]interface{}, 0)
					for _, ci := range selectCols {
						if ci.tableName == "" || ci.tableName == stmt.From.Name {
							// NULL for unmatched main columns
							combinedResult = append(combinedResult, nil)
						} else if ci.tableName == join.Table.Name {
							if ci.index >= 0 && ci.index < len(joinRow) {
								combinedResult = append(combinedResult, joinRow[ci.index])
							} else {
								combinedResult = append(combinedResult, nil)
							}
						}
					}
					resultRows = append(resultRows, combinedResult)
				}

				matchedRightRows[rightRowIndex] = matched
				rightRowIndex++
			}
		} else {
			// INNER JOIN (default behavior)
			mainIter, _ := mainTree.Scan(nil, nil)
			defer mainIter.Close()

			for mainIter.HasNext() {
				_, mainValueData, err := mainIter.Next()
				if err != nil {
					break
				}

				mainRow, err := decodeRow(mainValueData, len(mainTable.Columns))
				if err != nil {
					continue
				}

				// Scan the joined table
				joinIter, _ := joinTree.Scan(nil, nil)
				defer joinIter.Close()

				for joinIter.HasNext() {
					_, joinValueData, err := joinIter.Next()
					if err != nil {
						break
					}

					joinRow, err := decodeRow(joinValueData, len(joinTable.Columns))
					if err != nil {
						continue
					}

					// Check join condition if present
					if join.Condition != nil {
						// Combine rows for evaluation
						combinedRow := append(mainRow, joinRow...)
						matched, err := evaluateWhere(c,combinedRow, append(mainTable.Columns, joinTable.Columns...), join.Condition, args)
						if err != nil || !matched {
							continue
						}
					}

					// Create combined row
					combinedResult := make([]interface{}, 0)
					for _, ci := range selectCols {
						// Determine which table the column belongs to
						if ci.tableName == "" || ci.tableName == stmt.From.Name {
							if ci.index >= 0 && ci.index < len(mainRow) {
								combinedResult = append(combinedResult, mainRow[ci.index])
							} else {
								combinedResult = append(combinedResult, nil)
							}
						} else if ci.tableName == join.Table.Name {
							if ci.index >= 0 && ci.index < len(joinRow) {
								combinedResult = append(combinedResult, joinRow[ci.index])
							} else {
								combinedResult = append(combinedResult, nil)
							}
						}
					}
					resultRows = append(resultRows, combinedResult)
				}
			}
		}
	}

	// Build return columns
	returnColumns := make([]string, len(selectCols))
	for i, ci := range selectCols {
		returnColumns[i] = ci.name
	}

	return returnColumns, resultRows, nil
}

// toInt converts a value to int
func toInt(v interface{}) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	default:
		return 0, false
	}
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

// applyOrderBy sorts rows based on ORDER BY clause
func (c *Catalog) applyOrderBy(rows [][]interface{}, selectCols []selectColInfo, orderBy []*query.OrderByExpr) [][]interface{} {
	if len(rows) == 0 || len(orderBy) == 0 {
		return rows
	}

	// Build sort key function
	sorted := make([][]interface{}, len(rows))
	copy(sorted, rows)

	sort.Slice(sorted, func(i, j int) bool {
		for _, ob := range orderBy {
			// Find column index
			colIdx := -1
			for idx, ci := range selectCols {
				if ident, ok := ob.Expr.(*query.Identifier); ok {
					if ci.name == ident.Name {
						colIdx = idx
						break
					}
				}
			}
			if colIdx < 0 || colIdx >= len(sorted[i]) || colIdx >= len(sorted[j]) {
				continue
			}

			cmp := compareValues(sorted[i][colIdx], sorted[j][colIdx])
			if cmp != 0 {
				if ob.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return sorted
}

// applyDistinct removes duplicate rows
func (c *Catalog) applyDistinct(rows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return rows
	}

	seen := make(map[string]bool)
	var result [][]interface{}

	for _, row := range rows {
		// Create a string key for the row
		var key strings.Builder
		for _, val := range row {
			key.WriteString(fmt.Sprintf("%v|", val))
		}
		if !seen[key.String()] {
			seen[key.String()] = true
			result = append(result, row)
		}
	}

	return result
}

// computeAggregates computes aggregate functions for a SELECT query
func (c *Catalog) computeAggregates(table *TableDef, stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, returnColumns []string) ([]string, [][]interface{}, error) {
	tree, exists := c.tableTrees[stmt.From.Name]
	if !exists {
		// Return zeros/nulls for aggregates on empty table
		return c.computeAggregateResult(selectCols, returnColumns, 0, nil)
	}

	// Read all rows and collect values for aggregates
	var filteredRows [][]interface{}
	var aggregateValues [][]interface{} // [column][row]

	// Initialize aggregate value storage
	for range selectCols {
		aggregateValues = append(aggregateValues, nil)
	}

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	for iter.HasNext() {
		_, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode full row
		fullRow, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present
		if stmt.Where != nil {
			matched, err := evaluateWhere(c,fullRow, table.Columns, stmt.Where, args)
			if err != nil {
				continue
			}
			if !matched {
				continue
			}
		}

		filteredRows = append(filteredRows, fullRow)

		// Collect values for aggregate columns
		for i, ci := range selectCols {
			if ci.isAggregate {
				var val interface{}
				if ci.aggregateCol == "*" {
					// COUNT(*): just count rows
					val = int64(1)
				} else {
					// Find column index for aggregate column
					colIdx := table.GetColumnIndex(ci.aggregateCol)
					if colIdx >= 0 && colIdx < len(fullRow) {
						val = fullRow[colIdx]
					}
				}
				aggregateValues[i] = append(aggregateValues[i], val)
			}
		}
	}

	// Compute aggregate results
	return c.computeAggregateResult(selectCols, returnColumns, len(filteredRows), aggregateValues)
}

// computeAggregateResult calculates the final aggregate values
func (c *Catalog) computeAggregateResult(selectCols []selectColInfo, returnColumns []string, rowCount int, aggregateValues [][]interface{}) ([]string, [][]interface{}, error) {
	resultRow := make([]interface{}, len(selectCols))

	for i, ci := range selectCols {
		if !ci.isAggregate {
			continue
		}

		switch ci.aggregateType {
		case "COUNT":
			if ci.aggregateCol == "*" {
				resultRow[i] = int64(rowCount)
			} else if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				// Count non-null values
				count := int64(0)
				for _, v := range aggregateValues[i] {
					if v != nil {
						count++
					}
				}
				resultRow[i] = count
			} else {
				resultRow[i] = int64(0)
			}

		case "SUM":
			if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				var sum float64
				for _, v := range aggregateValues[i] {
					if v != nil {
						if f, ok := toFloat64(v); ok {
							sum += f
						}
					}
				}
				resultRow[i] = sum
			} else {
				resultRow[i] = nil
			}

		case "AVG":
			if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				var sum float64
				var count int64
				for _, v := range aggregateValues[i] {
					if v != nil {
						if f, ok := toFloat64(v); ok {
							sum += f
						}
						count++
					}
				}
				if count > 0 {
					resultRow[i] = sum / float64(count)
				} else {
					resultRow[i] = nil
				}
			} else {
				resultRow[i] = nil
			}

		case "MIN":
			if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				var min *float64
				for _, v := range aggregateValues[i] {
					if v != nil {
						if fv, ok := toFloat64(v); ok {
							if min == nil || fv < *min {
								min = &fv
							}
						}
					}
				}
				if min != nil {
					resultRow[i] = *min
				} else {
					resultRow[i] = nil
				}
			} else {
				resultRow[i] = nil
			}

		case "MAX":
			if aggregateValues != nil && len(aggregateValues[i]) > 0 {
				var max *float64
				for _, v := range aggregateValues[i] {
					if v != nil {
						if fv, ok := toFloat64(v); ok {
							if max == nil || fv > *max {
								max = &fv
							}
						}
					}
				}
				if max != nil {
					resultRow[i] = *max
				} else {
					resultRow[i] = nil
				}
			} else {
				resultRow[i] = nil
			}
		}
	}

	return returnColumns, [][]interface{}{resultRow}, nil
}

// computeAggregatesWithGroupBy handles GROUP BY queries with aggregates
func (c *Catalog) computeAggregatesWithGroupBy(table *TableDef, stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, returnColumns []string) ([]string, [][]interface{}, error) {
	tree, exists := c.tableTrees[stmt.From.Name]
	if !exists {
		// Return empty result for GROUP BY on non-existent table
		return returnColumns, [][]interface{}{}, nil
	}

	// Parse GROUP BY column indices
	groupByIndices := make([]int, len(stmt.GroupBy))
	for i, gb := range stmt.GroupBy {
		if ident, ok := gb.(*query.Identifier); ok {
			groupByIndices[i] = table.GetColumnIndex(ident.Name)
		}
	}

	// Group rows by GROUP BY columns
	// key is string representation of group values, value is slice of rows
	groups := make(map[string][][]interface{})

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	for iter.HasNext() {
		_, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode full row
		fullRow, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present (filters rows before grouping)
		if stmt.Where != nil {
			matched, err := evaluateWhere(c,fullRow, table.Columns, stmt.Where, args)
			if err != nil {
				continue
			}
			if !matched {
				continue
			}
		}

		// Build group key
		var groupKey strings.Builder
		for i, idx := range groupByIndices {
			if i > 0 {
				groupKey.WriteString("|")
			}
			if idx >= 0 && idx < len(fullRow) {
				groupKey.WriteString(fmt.Sprintf("%v", fullRow[idx]))
			}
		}

		// Add row to appropriate group
		groups[groupKey.String()] = append(groups[groupKey.String()], fullRow)
	}

	// Compute aggregates for each group
	var resultRows [][]interface{}

	for _, groupRows := range groups {
		resultRow := make([]interface{}, len(selectCols))

		for i, ci := range selectCols {
			if ci.isAggregate {
				// Collect values for this aggregate
				var values []interface{}
				for _, row := range groupRows {
					if ci.aggregateCol == "*" {
						// COUNT(*): just count rows
						values = append(values, int64(1))
					} else {
						colIdx := table.GetColumnIndex(ci.aggregateCol)
						if colIdx >= 0 && colIdx < len(row) {
							values = append(values, row[colIdx])
						}
					}
				}

				// Compute aggregate
				switch ci.aggregateType {
				case "COUNT":
					if ci.aggregateCol == "*" {
						resultRow[i] = int64(len(groupRows))
					} else {
						count := int64(0)
						for _, v := range values {
							if v != nil {
								count++
							}
						}
						resultRow[i] = count
					}
				case "SUM":
					var sum float64
					for _, v := range values {
						if v != nil {
							if f, ok := toFloat64(v); ok {
								sum += f
							}
						}
					}
					resultRow[i] = sum
				case "AVG":
					var sum float64
					var count int64
					for _, v := range values {
						if v != nil {
							if f, ok := toFloat64(v); ok {
								sum += f
								count++
							}
						}
					}
					if count > 0 {
						resultRow[i] = sum / float64(count)
					} else {
						resultRow[i] = nil
					}
				case "MIN":
					var min *float64
					for _, v := range values {
						if v != nil {
							if fv, ok := toFloat64(v); ok {
								if min == nil || fv < *min {
									min = &fv
								}
							}
						}
					}
					if min != nil {
						resultRow[i] = *min
					} else {
						resultRow[i] = nil
					}
				case "MAX":
					var max *float64
					for _, v := range values {
						if v != nil {
							if fv, ok := toFloat64(v); ok {
								if max == nil || fv > *max {
									max = &fv
								}
							}
						}
					}
					if max != nil {
						resultRow[i] = *max
					} else {
						resultRow[i] = nil
					}
				}
			} else {
				// Non-aggregate column - get value from first row in group
				// Find the column index from selectCols
				colIdx := -1
				for _, sc := range selectCols {
					if !sc.isAggregate && sc.name == ci.name {
						colIdx = sc.index
						break
					}
				}
				if colIdx >= 0 && len(groupRows) > 0 && colIdx < len(groupRows[0]) {
					resultRow[i] = groupRows[0][colIdx]
				}
			}
		}

		// Apply HAVING clause if present
		if stmt.Having != nil {
			// Create a temporary row with column values for evaluation
			// We need to create a virtual row that has the right column structure
			havingMatched, err := evaluateHaving(c, resultRow, selectCols, table.Columns, stmt.Having, args)
			if err != nil || !havingMatched {
				continue
			}
		}

		resultRows = append(resultRows, resultRow)
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		resultRows = c.applyGroupByOrderBy(resultRows, selectCols, stmt.OrderBy)
	}

	// Apply DISTINCT if present
	if stmt.Distinct {
		resultRows = c.applyDistinct(resultRows)
	}

	// Apply OFFSET if present
	if stmt.Offset != nil {
		offsetVal, err := evaluateExpression(c, nil, nil, stmt.Offset, args)
		if err == nil {
			if offset, ok := toInt(offsetVal); ok && offset > 0 && offset < len(resultRows) {
				resultRows = resultRows[offset:]
			}
		}
	}

	// Apply LIMIT if present
	if stmt.Limit != nil {
		limitVal, err := evaluateExpression(c, nil, nil, stmt.Limit, args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && limit < len(resultRows) {
				resultRows = resultRows[:limit]
			}
		}
	}

	return returnColumns, resultRows, nil
}

// evaluateHaving evaluates a HAVING clause against a group result row
func evaluateHaving(c *Catalog, row []interface{}, selectCols []selectColInfo, columns []ColumnDef, having query.Expression, args []interface{}) (bool, error) {
	if having == nil {
		return true, nil
	}

	// For HAVING, we need to handle aggregate functions specially
	// The aggregate results are already in the row at the indices matching selectCols
	// We need to transform the HAVING expression to use indices from the row

	// First, simplify the HAVING expression by replacing aggregate calls with their values
	havingExpr := resolveAggregateInExpr(having, selectCols, row)

	// Now evaluate the simplified expression
	result, err := evaluateExpression(c, row, nil, havingExpr, args)
	if err != nil {
		return false, err
	}

	if result == nil {
		return false, nil
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case int, int64, float64:
		switch n := v.(type) {
		case int:
			return n != 0, nil
		case int64:
			return n != 0, nil
		case float64:
			return n != 0, nil
		}
	}
	return true, nil
}

// resolveAggregateInExpr replaces aggregate function calls with their computed values
func resolveAggregateInExpr(expr query.Expression, selectCols []selectColInfo, row []interface{}) query.Expression {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *query.BinaryExpr:
		return &query.BinaryExpr{
			Left:     resolveAggregateInExpr(e.Left, selectCols, row),
			Operator: e.Operator,
			Right:    resolveAggregateInExpr(e.Right, selectCols, row),
		}
	case *query.FunctionCall:
		// Check if this is an aggregate function
		funcName := strings.ToUpper(e.Name)
		if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" {
			// Find the column name for this aggregate
			colName := "*"
			if len(e.Args) > 0 {
				if ident, ok := e.Args[0].(*query.Identifier); ok {
					colName = ident.Name
				}
			}
			aggName := e.Name + "(" + colName + ")"

			// Find the index in selectCols
			for i, sc := range selectCols {
				if sc.isAggregate && sc.name == aggName {
					// Return a placeholder that evaluates to the value at this index
					if i < len(row) {
						// Return a literal with the actual value
						return &query.NumberLiteral{Value: toNumber(row[i])}
					}
				}
			}
		}
		return e
	case *query.Identifier:
		// For non-aggregate identifiers, try to find them in selectCols
		for i, sc := range selectCols {
			if !sc.isAggregate && sc.name == e.Name {
				if i < len(row) {
					return &query.NumberLiteral{Value: toNumber(row[i])}
				}
			}
		}
		return e
	default:
		return e
	}
}

// toNumber converts a value to a number for comparison
func toNumber(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	default:
		return 0
	}
}

// applyGroupByOrderBy applies ORDER BY to GROUP BY results
func (c *Catalog) applyGroupByOrderBy(rows [][]interface{}, selectCols []selectColInfo, orderBy []*query.OrderByExpr) [][]interface{} {
	if len(rows) == 0 || len(orderBy) == 0 {
		return rows
	}

	// Build a map from column name to selectCols index
	nameToIndex := make(map[string]int)
	for i, ci := range selectCols {
		nameToIndex[ci.name] = i
	}

	sorted := make([][]interface{}, len(rows))
	copy(sorted, rows)

	sort.Slice(sorted, func(i, j int) bool {
		for _, ob := range orderBy {
			// Get the column name from the ORDER BY expression
			var colName string
			if ident, ok := ob.Expr.(*query.Identifier); ok {
				colName = ident.Name
			} else if fn, ok := ob.Expr.(*query.FunctionCall); ok {
				// Handle aggregate in ORDER BY
				colName = fn.Name + "("
				if len(fn.Args) > 0 {
					if argIdent, ok := fn.Args[0].(*query.Identifier); ok {
						colName += argIdent.Name + ")"
					}
				}
			}

			idx, ok := nameToIndex[colName]
			if !ok {
				continue
			}

			// Compare values
			vi := sorted[i][idx]
			vj := sorted[j][idx]

			// Handle nil values
			if vi == nil && vj == nil {
				continue
			}
			if vi == nil {
				return !ob.Desc
			}
			if vj == nil {
				return ob.Desc
			}

			// Compare based on type
			switch vi.(type) {
			case int, int64:
				vi64, _ := toInt64(vi)
				vj64, _ := toInt64(vj)
				if vi64 < vj64 {
					return !ob.Desc
				} else if vi64 > vj64 {
					return ob.Desc
				}
			case float64:
				viF := vi.(float64)
				vjF := vj.(float64)
				if viF < vjF {
					return !ob.Desc
				} else if viF > vjF {
					return ob.Desc
				}
			case string:
				viS := vi.(string)
				vjS := vj.(string)
				if viS < vjS {
					return !ob.Desc
				} else if viS > vjS {
					return ob.Desc
				}
			}
		}
		return false
	})

	return sorted
}

// evaluateWhere evaluates a WHERE clause against a row
func evaluateWhere(c *Catalog, row []interface{}, columns []ColumnDef, where query.Expression, args []interface{}) (bool, error) {
	if where == nil {
		return true, nil
	}

	// Evaluate the expression
	result, err := evaluateExpression(c, row, columns, where, args)
	if err != nil {
		return false, err
	}

	// Convert result to boolean
	if result == nil {
		return false, nil
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case int, int64, float64:
		// Non-zero numbers are truthy
		switch n := v.(type) {
		case int:
			return n != 0, nil
		case int64:
			return n != 0, nil
		case float64:
			return n != 0, nil
		}
	case string:
		return v != "", nil
	}

	return false, nil
}

// evaluateExpression evaluates an expression against a row
func evaluateExpression(c *Catalog, row []interface{}, columns []ColumnDef, expr query.Expression, args []interface{}) (interface{}, error) {
	switch e := expr.(type) {
	case *query.BinaryExpr:
		return evaluateBinaryExpr(c, row, columns, e, args)
	case *query.Identifier:
		// Find column value
		for i, col := range columns {
			if col.Name == e.Name && i < len(row) {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column not found: %s", e.Name)
	case *query.PlaceholderExpr:
		if e.Index < len(args) {
			return args[e.Index], nil
		}
		return nil, fmt.Errorf("placeholder index out of range")
	case *query.StringLiteral:
		return e.Value, nil
	case *query.NumberLiteral:
		return e.Value, nil
	case *query.BooleanLiteral:
		return e.Value, nil
	case *query.NullLiteral:
		return nil, nil
	case *query.QualifiedIdentifier:
		// table.column format
		for i, col := range columns {
			if col.Name == e.Column && i < len(row) {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column not found: %s.%s", e.Table, e.Column)
	case *query.LikeExpr:
		return evaluateLike(c, row, columns, e, args)
	case *query.InExpr:
		return evaluateIn(c, row, columns, e, args)
	case *query.BetweenExpr:
		return evaluateBetween(c, row, columns, e, args)
	case *query.FunctionCall:
		return evaluateFunctionCall(c, row, columns, e, args)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evaluateBinaryExpr evaluates a binary expression
func evaluateBinaryExpr(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.BinaryExpr, args []interface{}) (interface{}, error) {
	left, err := evaluateExpression(c, row, columns, expr.Left, args)
	if err != nil {
		return nil, err
	}

	right, err := evaluateExpression(c, row, columns, expr.Right, args)
	if err != nil {
		return nil, err
	}

	// Handle NULL comparisons
	if left == nil || right == nil {
		switch expr.Operator {
		case query.TokenIs:
			// IS NULL - true if both are nil
			// IS NOT NULL - true if either is not nil
			if rightVal, ok := right.(bool); ok {
				if rightVal {
					return left == nil, nil
				}
				return left != nil, nil
			}
		case query.TokenEq:
			return left == right, nil
		case query.TokenNeq:
			return left != right, nil
		}
		return false, nil
	}

	// Compare based on operator
	switch expr.Operator {
	case query.TokenEq:
		return compareValues(left, right) == 0, nil
	case query.TokenNeq:
		return compareValues(left, right) != 0, nil
	case query.TokenLt:
		return compareValues(left, right) < 0, nil
	case query.TokenGt:
		return compareValues(left, right) > 0, nil
	case query.TokenLte:
		return compareValues(left, right) <= 0, nil
	case query.TokenGte:
		return compareValues(left, right) >= 0, nil
	default:
		return false, fmt.Errorf("unsupported operator: %v", expr.Operator)
	}
}

// compareValues compares two values
func compareValues(a, b interface{}) int {
	// Handle numeric types
	aNum, aIsNum := toFloat64(a)
	bNum, bIsNum := toFloat64(b)
	if aIsNum && bIsNum {
		if aNum < bNum {
			return -1
		}
		if aNum > bNum {
			return 1
		}
		return 0
	}

	// Handle strings
	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
	if aIsStr && bIsStr {
		if aStr < bStr {
			return -1
		}
		if aStr > bStr {
			return 1
		}
		return 0
	}

	// Fallback to string comparison
	return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
}

// evaluateLike evaluates a LIKE expression (column LIKE pattern)
func evaluateLike(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.LikeExpr, args []interface{}) (bool, error) {
	left, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	pattern, err := evaluateExpression(c, row, columns, expr.Pattern, args)
	if err != nil {
		return false, err
	}

	// Handle NULL
	if left == nil || pattern == nil {
		return false, nil
	}

	leftStr, ok := left.(string)
	if !ok {
		leftStr = fmt.Sprintf("%v", left)
	}

	patternStr, ok := pattern.(string)
	if !ok {
		patternStr = fmt.Sprintf("%v", pattern)
	}

	// Simple LIKE implementation
	matched := matchLikeSimple(leftStr, patternStr)

	// Handle NOT LIKE
	if expr.Not {
		return !matched, nil
	}
	return matched, nil
}

// matchLikeSimple implements simple SQL LIKE matching
// Supports: % (any sequence), _ (single character)
func matchLikeSimple(s, pattern string) bool {
	if pattern == "" {
		return s == ""
	}

	// Convert pattern to regex-like matching
	sIdx := 0
	pIdx := 0

	for sIdx < len(s) && pIdx < len(pattern) {
		char := pattern[pIdx]

		// Handle %
		if char == '%' {
			// Skip consecutive %
			for pIdx < len(pattern) && pattern[pIdx] == '%' {
				pIdx++
			}
			if pIdx >= len(pattern) {
				// Trailing % matches rest
				return true
			}
			// Try matching remaining pattern at each position
			for sIdx < len(s) {
				if matchLikeSimple(s[sIdx:], pattern[pIdx:]) {
					return true
				}
				sIdx++
			}
			return false
		}

		// Handle _
		if char == '_' {
			sIdx++
			pIdx++
			continue
		}

		// Literal match
		if sIdx < len(s) && s[sIdx] == char {
			sIdx++
			pIdx++
			continue
		}

		return false
	}

	// All of s should be consumed
	// Skip any trailing % in pattern
	for pIdx < len(pattern) && pattern[pIdx] == '%' {
		pIdx++
	}

	return sIdx == len(s) && pIdx == len(pattern)
}

// evaluateIn evaluates an IN expression (column IN (1, 2, 3))
func evaluateIn(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.InExpr, args []interface{}) (bool, error) {
	left, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	// Handle subquery: IN (SELECT ...)
	if expr.Subquery != nil {
		_, subqueryRows, err := c.Select(expr.Subquery, args)
		if err != nil {
			return false, err
		}
		// Check if left value is in any of the subquery results
		for _, subRow := range subqueryRows {
			if len(subRow) > 0 && compareValues(left, subRow[0]) == 0 {
				if !expr.Not {
					return true, nil
				}
			}
		}
		if expr.Not {
			return true, nil // NOT IN - true if no match
		}
		return false, nil
	}

	// Evaluate all values in the list
	var listValues []interface{}
	for _, item := range expr.List {
		val, err := evaluateExpression(c, row, columns, item, args)
		if err != nil {
			return false, err
		}
		listValues = append(listValues, val)
	}

	// Check if left is in list
	inList := false
	for _, v := range listValues {
		if compareValues(left, v) == 0 {
			inList = true
			break
		}
	}

	// Handle NOT IN
	if expr.Not {
		return !inList, nil
	}
	return inList, nil
}

// evaluateBetween evaluates a BETWEEN expression (column BETWEEN 1 AND 10)
func evaluateBetween(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.BetweenExpr, args []interface{}) (bool, error) {
	exprVal, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	lowerVal, err := evaluateExpression(c, row, columns, expr.Lower, args)
	if err != nil {
		return false, err
	}

	upperVal, err := evaluateExpression(c, row, columns, expr.Upper, args)
	if err != nil {
		return false, err
	}

	// Handle NULL
	if exprVal == nil || lowerVal == nil || upperVal == nil {
		return false, nil
	}

	// Check: lower <= expr <= upper
	lowCmp := compareValues(exprVal, lowerVal)
	highCmp := compareValues(exprVal, upperVal)

	result := lowCmp >= 0 && highCmp <= 0

	// Handle NOT BETWEEN
	if expr.Not {
		return !result, nil
	}
	return result, nil
}

// evaluateFunctionCall evaluates scalar functions
func evaluateFunctionCall(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.FunctionCall, args []interface{}) (interface{}, error) {
	funcName := strings.ToUpper(expr.Name)

	// Evaluate arguments first
	evalArgs := make([]interface{}, len(expr.Args))
	for i, arg := range expr.Args {
		val, err := evaluateExpression(c, row, columns, arg, args)
		if err != nil {
			return nil, err
		}
		evalArgs[i] = val
	}

	switch funcName {
	case "LENGTH", "LEN":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("LENGTH requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		return float64(len(str)), nil

	case "UPPER":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("UPPER requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		return strings.ToUpper(str), nil

	case "LOWER":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("LOWER requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		return strings.ToLower(str), nil

	case "TRIM", "LTRIM", "RTRIM":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("%s requires at least 1 argument", funcName)
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		str = strings.TrimSpace(str)
		if funcName == "LTRIM" {
			str = strings.TrimLeft(str, " \t\n\r")
		} else if funcName == "RTRIM" {
			str = strings.TrimRight(str, " \t\n\r")
		}
		return str, nil

	case "SUBSTR", "SUBSTRING":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("SUBSTR requires at least 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		start, _ := toFloat64(evalArgs[1])
		startInt := int(start)
		if startInt < 0 {
			startInt = len(str) + startInt
		}
		if startInt < 0 {
			startInt = 0
		}
		if startInt >= len(str) {
			return "", nil
		}
		if len(evalArgs) >= 3 {
			length, _ := toFloat64(evalArgs[2])
			lengthInt := int(length)
			if startInt+lengthInt > len(str) {
				lengthInt = len(str) - startInt
			}
			return str[startInt : startInt+lengthInt], nil
		}
		return str[startInt:], nil

	case "CONCAT":
		var result strings.Builder
		for _, arg := range evalArgs {
			if arg != nil {
				result.WriteString(fmt.Sprintf("%v", arg))
			}
		}
		return result.String(), nil

	case "ABS":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("ABS requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			if f < 0 {
				return -f, nil
			}
			return f, nil
		}
		return evalArgs[0], nil

	case "ROUND":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("ROUND requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		f, ok := toFloat64(evalArgs[0])
		if !ok {
			return evalArgs[0], nil
		}
		precision := 0
		if len(evalArgs) >= 2 {
			if p, ok := toFloat64(evalArgs[1]); ok {
				precision = int(p)
			}
		}
		divisor := 1.0
		for i := 0; i < precision; i++ {
			divisor *= 10
		}
		result := math.Round(f*divisor) / divisor
		if precision == 0 {
			return float64(int64(result)), nil
		}
		return result, nil

	case "FLOOR":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("FLOOR requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return math.Floor(f), nil
		}
		return evalArgs[0], nil

	case "CEIL", "CEILING":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("CEIL requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return math.Ceil(f), nil
		}
		return evalArgs[0], nil

	case "COALESCE", "IFNULL":
		for _, arg := range evalArgs {
			if arg != nil {
				return arg, nil
			}
		}
		return nil, nil

	case "NULLIF":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("NULLIF requires 2 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil {
			return evalArgs[0], nil
		}
		if compareValues(evalArgs[0], evalArgs[1]) == 0 {
			return nil, nil
		}
		return evalArgs[0], nil

	case "REPLACE":
		if len(evalArgs) < 3 {
			return nil, fmt.Errorf("REPLACE requires 3 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil || evalArgs[2] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		old, _ := evalArgs[1].(string)
		if old == "" {
			return str, nil
		}
		new, _ := evalArgs[2].(string)
		return strings.ReplaceAll(str, old, new), nil

	case "INSTR":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("INSTR requires 2 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil {
			return float64(0), nil
		}
		haystack, ok := evalArgs[0].(string)
		if !ok {
			haystack = fmt.Sprintf("%v", evalArgs[0])
		}
		needle, ok := evalArgs[1].(string)
		if !ok {
			needle = fmt.Sprintf("%v", evalArgs[1])
		}
		idx := strings.Index(haystack, needle)
		if idx < 0 {
			return float64(0), nil
		}
		return float64(idx + 1), nil

	case "PRINTF":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("PRINTF requires at least 1 argument")
		}
		format, ok := evalArgs[0].(string)
		if !ok {
			format = fmt.Sprintf("%v", evalArgs[0])
		}
		// Simple printf implementation
		var result strings.Builder
		argIndex := 1
		i := 0
		for i < len(format) {
			if format[i] == '%' && i+1 < len(format) {
				nextChar := format[i+1]
				switch nextChar {
				case 's':
					if argIndex < len(evalArgs) {
						result.WriteString(fmt.Sprintf("%v", evalArgs[argIndex]))
						argIndex++
					}
					i += 2
				case 'd', 'i':
					if argIndex < len(evalArgs) {
						if f, ok := toFloat64(evalArgs[argIndex]); ok {
							result.WriteString(fmt.Sprintf("%d", int64(f)))
						}
						argIndex++
					}
					i += 2
				case 'f':
					if argIndex < len(evalArgs) {
						if f, ok := toFloat64(evalArgs[argIndex]); ok {
							result.WriteString(fmt.Sprintf("%f", f))
						}
						argIndex++
					}
					i += 2
				default:
					result.WriteByte(format[i])
					i++
				}
			} else {
				result.WriteByte(format[i])
				i++
			}
		}
		return result.String(), nil

	case "DATE", "TIME", "DATETIME":
		// Simple date/time functions - return current time for now
		// Full implementation would require time parsing
		if len(evalArgs) < 1 {
			return nil, nil
		}
		return evalArgs[0], nil

	case "STRFTIME":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("STRFTIME requires 2 arguments")
		}
		// Simple strftime - just return the input for now
		if evalArgs[1] == nil {
			return nil, nil
		}
		return fmt.Sprintf("%v", evalArgs[1]), nil

	case "CAST":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("CAST requires 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		targetType, ok := evalArgs[1].(string)
		if !ok {
			targetType = strings.ToUpper(fmt.Sprintf("%v", evalArgs[1]))
		}
		switch targetType {
		case "INTEGER", "INT":
			if f, ok := toFloat64(evalArgs[0]); ok {
				return float64(int64(f)), nil
			}
			if s, ok := evalArgs[0].(string); ok {
				var i int64
				fmt.Sscanf(s, "%d", &i)
				return float64(i), nil
			}
		case "REAL", "FLOAT":
			if f, ok := toFloat64(evalArgs[0]); ok {
				return f, nil
			}
		case "TEXT", "STRING":
			return fmt.Sprintf("%v", evalArgs[0]), nil
		case "BOOLEAN", "BOOL":
			if b, ok := evalArgs[0].(bool); ok {
				return b, nil
			}
			if s, ok := evalArgs[0].(string); ok {
				return strings.ToLower(s) == "true", nil
			}
		}
		return evalArgs[0], nil

	default:
		// Check for JSON functions
		return evaluateJSONFunction(funcName, evalArgs)
	}
}

// evaluateJSONFunction evaluates JSON-related functions
func evaluateJSONFunction(funcName string, args []interface{}) (interface{}, error) {
	switch funcName {
	case "JSON_EXTRACT":
		if len(args) < 2 {
			return nil, fmt.Errorf("JSON_EXTRACT requires 2 arguments")
		}
		// Simple JSON path extraction - just return the value for now
		return args[0], nil

	case "JSON_SET":
		if len(args) < 3 {
			return nil, fmt.Errorf("JSON_SET requires 3 arguments")
		}
		return args[2], nil

	case "JSON_VALID":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_VALID requires 1 argument")
		}
		if args[0] == nil {
			return false, nil
		}
		_, ok := args[0].(string)
		return ok, nil

	case "JSON_ARRAY_LENGTH":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_ARRAY_LENGTH requires 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		return float64(0), nil

	case "JSON_TYPE":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_TYPE requires 1 argument")
		}
		if args[0] == nil {
			return "null", nil
		}
		switch args[0].(type) {
		case string:
			return "string", nil
		case float64:
			return "number", nil
		case bool:
			return "boolean", nil
		default:
			return "unknown", nil
		}

	default:
		return nil, fmt.Errorf("unknown function: %s", funcName)
	}
}

// toFloat64 converts a value to float64
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// tokenTypeToColumnType converts a query token type to a column type string
func tokenTypeToColumnType(t query.TokenType) string {
	switch t {
	case query.TokenInteger:
		return "INTEGER"
	case query.TokenText:
		return "TEXT"
	case query.TokenReal:
		return "REAL"
	case query.TokenBlob:
		return "BLOB"
	case query.TokenBoolean:
		return "BOOLEAN"
	case query.TokenJSON:
		return "JSON"
	case query.TokenDate:
		return "DATE"
	case query.TokenTimestamp:
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

// buildColumnIndexCache builds a cache of column name to index mappings
func (t *TableDef) buildColumnIndexCache() {
	t.columnIndices = make(map[string]int, len(t.Columns))
	for i, col := range t.Columns {
		t.columnIndices[col.Name] = i
	}
}

// GetColumnIndex returns the index of a column by name, -1 if not found
func (t *TableDef) GetColumnIndex(name string) int {
	if t.columnIndices == nil {
		t.buildColumnIndexCache()
	}
	if idx, ok := t.columnIndices[name]; ok {
		return idx
	}
	return -1
}

// Helper methods for future implementation
func (c *Catalog) ListTables() []string {
	tables := make([]string, 0, len(c.tables))
	for name := range c.tables {
		tables = append(tables, name)
	}
	return tables
}

// SaveData saves all table data to disk
func (c *Catalog) SaveData(dir string) error {
	// Create directory if not exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Save catalog schema (table definitions)
	schema := map[string]interface{}{
		"tables": c.tables,
	}
	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}
	if err := os.WriteFile(dir+"/schema.json", schemaBytes, 0644); err != nil {
		return fmt.Errorf("failed to write schema: %w", err)
	}

	// Save each table's data
	for tableName, tree := range c.tableTrees {
		// Get all key-value pairs from B+Tree
		var keys []string
		var values [][]byte

		iter, _ := tree.Scan(nil, nil)
		for iter.HasNext() {
			key, value, _ := iter.Next()
			keys = append(keys, string(key))
			values = append(values, value)
		}
		iter.Close()

		if len(keys) == 0 {
			continue // Skip empty tables
		}

		// Save to JSON file
		data := map[string]interface{}{
			"keys":   keys,
			"values": values,
		}

		dataBytes, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal table %s: %w", tableName, err)
		}

		filename := fmt.Sprintf("%s/%s.json", dir, tableName)
		if err := os.WriteFile(filename, dataBytes, 0644); err != nil {
			return fmt.Errorf("failed to write table %s: %w", tableName, err)
		}
	}

	return nil
}

// LoadSchema loads the catalog schema from disk
func (c *Catalog) LoadSchema(dir string) error {
	schemaFile := dir + "/schema.json"
	if _, err := os.Stat(schemaFile); os.IsNotExist(err) {
		return nil // No schema to load
	}

	schemaBytes, err := os.ReadFile(schemaFile)
	if err != nil {
		return fmt.Errorf("failed to read schema: %w", err)
	}

	var schema map[string]interface{}
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		return fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	// Load tables
	if tablesData, ok := schema["tables"].(map[string]interface{}); ok {
		for name, data := range tablesData {
			// Marshal back to get proper type
			tableBytes, _ := json.Marshal(data)
			var tableDef TableDef
			json.Unmarshal(tableBytes, &tableDef)
			c.tables[name] = &tableDef

			// Create B+Tree for the table
			tree, _ := btree.NewBTree(c.pool)
			c.tableTrees[name] = tree
		}
	}

	return nil
}

// LoadData loads all table data from disk
func (c *Catalog) LoadData(dir string) error {
	// Check if directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// No data to load
		return nil
	}

	// Read all JSON files in directory
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") || file.Name() == "schema.json" {
			continue
		}

		tableName := strings.TrimSuffix(file.Name(), ".json")

		// Check if table exists
		if _, err := c.GetTable(tableName); err != nil {
			continue // Skip non-existent tables
		}

		// Read file
		dataBytes, err := os.ReadFile(fmt.Sprintf("%s/%s", dir, file.Name()))
		if err != nil {
			return fmt.Errorf("failed to read table %s: %w", tableName, err)
		}

		// Unmarshal
		var data map[string]interface{}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return fmt.Errorf("failed to unmarshal table %s: %w", tableName, err)
		}

		// Get tree
		tree, exists := c.tableTrees[tableName]
		if !exists {
			continue
		}

		// Load keys and values
		keysData := data["keys"].([]interface{})
		valuesData := data["values"].([]interface{})

		for i, keyData := range keysData {
			key := []byte(keyData.(string))

			// Decode base64 value
			encodedValue := valuesData[i].(string)
			decodedValue, err := base64.StdEncoding.DecodeString(encodedValue)
			if err != nil {
				// Try as plain string if not base64
				decodedValue = []byte(encodedValue)
			}

			tree.Put(key, decodedValue)
		}

		// Verify data was loaded
	}

	return nil
}

func (c *Catalog) CreateIndex(stmt *query.CreateIndexStmt) error {
	if !stmt.IfNotExists {
		if _, exists := c.indexes[stmt.Index]; exists {
			return ErrIndexExists
		}
	}

	// Verify table exists
	table, err := c.GetTable(stmt.Table)
	if err != nil {
		return err
	}

	// Create B+Tree for the index
	indexTree, err := btree.NewBTree(c.pool)
	if err != nil {
		return err
	}

	indexDef := &IndexDef{
		Name:       stmt.Index,
		TableName:  stmt.Table,
		Columns:    stmt.Columns,
		Unique:     stmt.Unique,
		RootPageID: indexTree.RootPageID(),
	}

	c.indexes[stmt.Index] = indexDef
	c.indexTrees[stmt.Index] = indexTree

	// Populate index with existing data from the table
	tree, exists := c.tableTrees[stmt.Table]
	if exists {
		tableColIdx := table.GetColumnIndex(stmt.Columns[0])
		if tableColIdx >= 0 {
			iter, _ := tree.Scan(nil, nil)
			for iter.HasNext() {
				key, valueData, _ := iter.Next()
				row, err := decodeRow(valueData, len(table.Columns))
				if err != nil {
					continue
				}
				if tableColIdx < len(row) {
					// Index key is the column value, value is the primary key
					indexKey := fmt.Sprintf("%v", row[tableColIdx])
					indexTree.Put([]byte(indexKey), key)
				}
			}
			iter.Close()
		}
	}

	return c.storeIndexDef(indexDef)
}

func (c *Catalog) storeIndexDef(index *IndexDef) error {
	key := []byte("idx:" + index.Name)
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}

	if c.tree != nil {
		return c.tree.Put(key, data)
	}
	return nil
}

func (c *Catalog) GetIndex(name string) (*IndexDef, error) {
	index, exists := c.indexes[name]
	if !exists {
		return nil, ErrIndexNotFound
	}
	return index, nil
}

// findUsableIndex finds an index that can be used for a WHERE clause
// Returns the index name, column name, and the value to search for
func (c *Catalog) findUsableIndex(tableName string, where query.Expression) (string, string, interface{}) {
	if where == nil {
		return "", "", nil
	}

	// Check for simple equality condition: column = value
	switch expr := where.(type) {
	case *query.BinaryExpr:
		if expr.Operator == query.TokenEq {
			// Check if left side is a column identifier
		 if ident, ok := expr.Left.(*query.Identifier); ok {
				colName := ident.Name
				// Check if there's an index on this column
				for idxName, idxDef := range c.indexes {
					if idxDef.TableName == tableName && len(idxDef.Columns) > 0 && idxDef.Columns[0] == colName {
						// Get the value to search for
						var searchVal interface{}
						switch v := expr.Right.(type) {
						case *query.NumberLiteral:
							searchVal = v.Value
						case *query.StringLiteral:
							searchVal = v.Value
						case *query.PlaceholderExpr:
							// Can't determine value at parse time
							return "", "", nil
						default:
							return "", "", nil
						}
						return idxName, colName, searchVal
					}
				}
			}
		}
	}
	return "", "", nil
}

// useIndexForQuery checks if an index can be used and returns matching primary keys
func (c *Catalog) useIndexForQuery(tableName string, where query.Expression) (map[string]bool, bool) {
	idxName, _, searchVal := c.findUsableIndex(tableName, where)
	if idxName == "" || searchVal == nil {
		return nil, false
	}

	indexTree, exists := c.indexTrees[idxName]
	if !exists {
		return nil, false
	}

	// Look up the index
	indexKey := fmt.Sprintf("%v", searchVal)
	pkData, err := indexTree.Get([]byte(indexKey))
	if err != nil {
		// No matching rows
		return map[string]bool{}, true
	}

	// Return the primary key(s) found
	result := make(map[string]bool)
	result[string(pkData)] = true
	return result, true
}

func (c *Catalog) DropIndex(name string) error {
	if _, exists := c.indexes[name]; !exists {
		return ErrIndexNotFound
	}

	delete(c.indexes, name)

	if c.tree != nil {
		key := []byte("idx:" + name)
		return c.tree.Delete(key)
	}
	return nil
}

// Load loads the catalog from the tree
func (c *Catalog) Load() error {
	if c.tree == nil {
		return nil
	}

	// Iterate over all catalog entries
	iter, err := c.tree.Scan([]byte("tbl:"), []byte("tbl:~"))
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		key, value, err := iter.Next()
		if err != nil {
			break
		}

		var tableDef TableDef
		if err := json.Unmarshal(value, &tableDef); err != nil {
			return fmt.Errorf("failed to unmarshal table definition: %w", err)
		}

		c.tables[string(key[4:])] = &tableDef
	}

	// Load indexes
	iter, err = c.tree.Scan([]byte("idx:"), []byte("idx:~"))
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		key, value, err := iter.Next()
		if err != nil {
			break
		}

		var indexDef IndexDef
		if err := json.Unmarshal(value, &indexDef); err != nil {
			return fmt.Errorf("failed to unmarshal index definition: %w", err)
		}

		c.indexes[string(key[4:])] = &indexDef
	}

	return nil
}

// encodeRow encodes a row of expressions to bytes
func encodeRow(exprs []query.Expression, args []interface{}) ([]byte, error) {
	values := make([]interface{}, 0, len(exprs))
	argIdx := 0

	for _, expr := range exprs {
		var val interface{}
		var err error

		switch e := expr.(type) {
		case *query.PlaceholderExpr:
			// Get value from args
			if e.Index < len(args) {
				val = args[e.Index]
			} else if argIdx < len(args) {
				// Also support positional placeholders
				val = args[argIdx]
				argIdx++
			} else {
				val = nil
			}
		case *query.StringLiteral:
			val = e.Value
		case *query.NumberLiteral:
			val = e.Value
		case *query.BooleanLiteral:
			val = e.Value
		case *query.NullLiteral:
			val = nil
		case *query.Identifier:
			val = e.Name
		default:
			val, err = EvalExpression(expr, args)
			if err != nil {
				return nil, err
			}
		}
		values = append(values, val)
	}

	// If we have remaining args that weren't used as placeholders, add them
	if argIdx < len(args) {
		for i := argIdx; i < len(args); i++ {
			values = append(values, args[i])
		}
	}

	return json.Marshal(values)
}

// decodeRow decodes bytes to a row of values
func decodeRow(data []byte, numCols int) ([]interface{}, error) {
	var values []interface{}
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	return values, nil
}

// fastEncodeRow encodes a row using a simple binary format (faster than JSON)
// Format: [type1][len1][data1][type2][len2][data2]...
// Types: 0=nill, 1=int64, 2=float64, 3=string, 4=bool
func fastEncodeRow(values []interface{}) ([]byte, error) {
	if len(values) == 0 {
		return []byte{0}, nil // empty marker
	}

	var buf []byte
	for _, v := range values {
		switch val := v.(type) {
		case nil:
			buf = append(buf, 0) // type: nil
		case int:
			buf = append(buf, 1) // type: int
			buf = append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24), byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
		case int64:
			buf = append(buf, 1) // type: int64
			buf = append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24), byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
		case float64:
			buf = append(buf, 2) // type: float64
			bits := uint64(math.Float64bits(val))
			buf = append(buf, byte(bits), byte(bits>>8), byte(bits>>16), byte(bits>>24), byte(bits>>32), byte(bits>>40), byte(bits>>48), byte(bits>>56))
		case string:
			buf = append(buf, 3) // type: string
			buf = append(buf, byte(len(val)), byte(len(val)>>8))
			buf = append(buf, val...)
		case bool:
			buf = append(buf, 4) // type: bool
			if val {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		default:
			// Fallback to JSON for unknown types
			j, err := json.Marshal(val)
			if err != nil {
				buf = append(buf, 0) // nil as fallback
			} else {
				buf = append(buf, 3) // treat as string
				buf = append(buf, byte(len(j)), byte(len(j)>>8))
				buf = append(buf, j...)
			}
		}
	}
	return buf, nil
}

// fastDecodeRow decodes a row from binary format
func fastDecodeRow(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return []interface{}{}, nil
	}

	var values []interface{}
	i := 0
	for i < len(data) {
		typ := data[i]
		i++
		switch typ {
		case 0: // nil
			values = append(values, nil)
		case 1: // int64
			if i+8 > len(data) {
				return nil, fmt.Errorf("invalid data: expected int64")
			}
			var v int64
			v = int64(data[i]) | int64(data[i+1])<<8 | int64(data[i+2])<<16 | int64(data[i+3])<<24 |
				int64(data[i+4])<<32 | int64(data[i+5])<<40 | int64(data[i+6])<<48 | int64(data[i+7])<<56
			values = append(values, v)
			i += 8
		case 2: // float64
			if i+8 > len(data) {
				return nil, fmt.Errorf("invalid data: expected float64")
			}
			bits := uint64(data[i]) | uint64(data[i+1])<<8 | uint64(data[i+2])<<16 | uint64(data[i+3])<<24 |
				uint64(data[i+4])<<32 | uint64(data[i+5])<<40 | uint64(data[i+6])<<48 | uint64(data[i+7])<<56
			values = append(values, math.Float64frombits(bits))
			i += 8
		case 3: // string
			if i+2 > len(data) {
				return nil, fmt.Errorf("invalid data: expected string length")
			}
			length := int(data[i]) | int(data[i+1])<<8
			i += 2
			if i+length > len(data) {
				return nil, fmt.Errorf("invalid data: expected string")
			}
			values = append(values, string(data[i:i+length]))
			i += length
		case 4: // bool
			if i >= len(data) {
				return nil, fmt.Errorf("invalid data: expected bool")
			}
			values = append(values, data[i] != 0)
			i++
		default:
			return nil, fmt.Errorf("unknown type: %d", typ)
		}
	}
	return values, nil
}

// evalExpression evaluates an expression to a value
// EvalExpression evaluates an expression to a value
func EvalExpression(expr query.Expression, args []interface{}) (interface{}, error) {
	switch e := expr.(type) {
	case *query.StringLiteral:
		return e.Value, nil
	case *query.NumberLiteral:
		return e.Value, nil
	case *query.BooleanLiteral:
		return e.Value, nil
	case *query.NullLiteral:
		return nil, nil
	case *query.PlaceholderExpr:
		if e.Index < len(args) {
			return args[e.Index], nil
		}
		return nil, fmt.Errorf("placeholder index out of range")
	case *query.Identifier:
		return e.Name, nil // Return as string for now
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}
