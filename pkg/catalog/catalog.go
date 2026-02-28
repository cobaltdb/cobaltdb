package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
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
	Name       string      `json:"name"`
	Type       string      `json:"type"` // "table" or "collection"
	Columns    []ColumnDef `json:"columns"`
	PrimaryKey string      `json:"primary_key"`
	CreatedAt  int64       `json:"created_at"`
	RootPageID uint32      `json:"root_page_id"`
}

// ColumnDef represents a column definition
type ColumnDef struct {
	Name         string `json:"name"`
	Type         string `json:"type"` // INTEGER, TEXT, REAL, BLOB, JSON, BOOLEAN
	NotNull      bool   `json:"not_null"`
	Unique       bool   `json:"unique"`
	PrimaryKey   bool   `json:"primary_key"`
	AutoIncrement bool  `json:"auto_increment"`
	Default      string `json:"default,omitempty"`
}

// IndexDef represents an index definition
type IndexDef struct {
	Name       string   `json:"name"`
	TableName  string   `json:"table_name"`
	Columns    []string `json:"columns"`
	Unique     bool     `json:"unique"`
	RootPageID uint32   `json:"root_page_id"`
}

// Catalog manages database schema metadata
type Catalog struct {
	tree        *btree.BTree
	tables      map[string]*TableDef
	indexes     map[string]*IndexDef
	pool        *storage.BufferPool
	tableTrees  map[string]*btree.BTree // Each table has its own B+Tree
	tableData   map[string]map[string][]byte // Temporary: simple in-memory storage
	keyCounter  int64 // For generating unique keys
}

// New creates a new catalog
func New(tree *btree.BTree, pool *storage.BufferPool) *Catalog {
	return &Catalog{
		tree:       tree,
		tables:     make(map[string]*TableDef),
		indexes:    make(map[string]*IndexDef),
		pool:       pool,
		tableTrees: make(map[string]*btree.BTree),
		tableData:  make(map[string]map[string][]byte),
		keyCounter: 0,
	}
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
		Name:       stmt.Table,
		Type:       "table",
		Columns:    make([]ColumnDef, len(stmt.Columns)),
		CreatedAt:  0, // TODO: use current timestamp
		RootPageID: tree.RootPageID(),
	}

	for i, col := range stmt.Columns {
		tableDef.Columns[i] = ColumnDef{
			Name:         col.Name,
			Type:         tokenTypeToColumnType(col.Type),
			NotNull:      col.NotNull,
			Unique:       col.Unique,
			PrimaryKey:   col.PrimaryKey,
			AutoIncrement: col.AutoIncrement,
		}
		if col.PrimaryKey {
			tableDef.PrimaryKey = col.Name
		}
	}

	c.tables[stmt.Table] = tableDef
	c.tableTrees[stmt.Table] = tree // Store the tree for data operations
	c.tableData[stmt.Table] = make(map[string][]byte) // Initialize data storage

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

		// Map provided values to their columns
		for colIdx, colName := range insertColumns {
			if colIdx < len(valueRow) {
				// Find matching column in table
				for tableColIdx, tableCol := range table.Columns {
					if tableCol.Name == colName {
						val, err := EvalExpression(valueRow[colIdx], args)
						if err != nil {
							rowValues[tableColIdx] = nil
						} else {
							rowValues[tableColIdx] = val
						}
						break
					}
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

		// Encode row
		valueData, err := json.Marshal(rowValues)
		if err != nil {
			return 0, rowsAffected, err
		}

		// Store in B+Tree
		tree.Put([]byte(key), valueData)
		rowsAffected++
	}

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

	// TODO: Implement WHERE clause filtering
	// For now, update all rows

	rowsAffected := int64(0)
	iter, _ := tree.Scan(nil, nil)

	// Collect keys to update
	var keys [][]byte
	var values [][]byte
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
		_ = row // TODO: use row for WHERE clause filtering

		// Update fields
		for _, setClause := range stmt.Set {
			newVal, err := EvalExpression(setClause.Value, args)
			if err != nil {
				continue
			}
			// Update row (simplified - just append new values for now)
			_ = newVal
		}

		// Re-encode row
		newValueData, err := encodeRow([]query.Expression{}, args)
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
		tree.Put(key, values[i])
	}

	return 0, rowsAffected, nil
}

// Delete deletes rows from a table
func (c *Catalog) Delete(stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	_, err := c.GetTable(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	tree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	// TODO: Implement WHERE clause filtering
	// For now, delete all rows

	rowsAffected := int64(0)
	iter, _ := tree.Scan(nil, nil)

	// Collect keys to delete
	var keys [][]byte
	for iter.HasNext() {
		key, _, err := iter.Next()
		if err != nil {
			break
		}
		keys = append(keys, key)
		rowsAffected++
	}
	iter.Close()

	// Delete collected keys
	for _, key := range keys {
		tree.Delete(key)
	}

	return 0, rowsAffected, nil
}

// Select queries rows from a table
func (c *Catalog) Select(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	if stmt.From == nil {
		return nil, nil, errors.New("no table specified")
	}

	table, err := c.GetTable(stmt.From.Name)
	if err != nil {
		return nil, nil, err
	}

	// Get column names and their indices in the table
	type colInfo struct {
		name  string
		index int
	}
	var selectCols []colInfo

	for _, col := range stmt.Columns {
		switch c := col.(type) {
		case *query.Identifier:
			// Find column index in table
			for i, tc := range table.Columns {
				if tc.Name == c.Name {
					selectCols = append(selectCols, colInfo{name: c.Name, index: i})
					break
				}
			}
		case *query.StarExpr:
			// SELECT * - get all columns from table
			for i, tc := range table.Columns {
				selectCols = append(selectCols, colInfo{name: tc.Name, index: i})
			}
		}
	}

	// Extract column names for return
	returnColumns := make([]string, len(selectCols))
	for i, ci := range selectCols {
		returnColumns[i] = ci.name
	}

	// Read all rows from B+Tree
	var rows [][]interface{}
	tree, exists := c.tableTrees[stmt.From.Name]
	if !exists {
		return returnColumns, rows, nil
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
			matched, err := evaluateWhere(fullRow, table.Columns, stmt.Where, args)
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

	return returnColumns, rows, nil
}

// evaluateWhere evaluates a WHERE clause against a row
func evaluateWhere(row []interface{}, columns []ColumnDef, where query.Expression, args []interface{}) (bool, error) {
	if where == nil {
		return true, nil
	}

	// Evaluate the expression
	result, err := evaluateExpression(row, columns, where, args)
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
func evaluateExpression(row []interface{}, columns []ColumnDef, expr query.Expression, args []interface{}) (interface{}, error) {
	switch e := expr.(type) {
	case *query.BinaryExpr:
		return evaluateBinaryExpr(row, columns, e, args)
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
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evaluateBinaryExpr evaluates a binary expression
func evaluateBinaryExpr(row []interface{}, columns []ColumnDef, expr *query.BinaryExpr, args []interface{}) (interface{}, error) {
	left, err := evaluateExpression(row, columns, expr.Left, args)
	if err != nil {
		return nil, err
	}

	right, err := evaluateExpression(row, columns, expr.Right, args)
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
	default:
		return "TEXT"
	}
}

// Helper methods for future implementation
func (c *Catalog) ListTables() []string {
	tables := make([]string, 0, len(c.tables))
	for name := range c.tables {
		tables = append(tables, name)
	}
	return tables
}

func (c *Catalog) CreateIndex(stmt *query.CreateIndexStmt) error {
	if !stmt.IfNotExists {
		if _, exists := c.indexes[stmt.Index]; exists {
			return ErrIndexExists
		}
	}

	// Verify table exists
	if _, err := c.GetTable(stmt.Table); err != nil {
		return err
	}

	// For now, just store the index definition without creating B+Tree
	// TODO: Create B+Tree when pool is available
	indexDef := &IndexDef{
		Name:       stmt.Index,
		TableName:  stmt.Table,
		Columns:    stmt.Columns,
		Unique:     stmt.Unique,
		RootPageID: 0, // Will be set when B+Tree is created
	}

	c.indexes[stmt.Index] = indexDef

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
