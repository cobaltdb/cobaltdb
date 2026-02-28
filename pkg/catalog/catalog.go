package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
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

	// Insert each row
	rowsAffected := int64(0)
	for _, valueRow := range stmt.Values {
		// Generate unique key using atomic counter + timestamp
		keyNum := atomic.AddInt64(&c.keyCounter, 1)
		key := fmt.Sprintf("%020d", keyNum) // Fixed width for proper sorting

		// Encode values
		valueData, err := encodeRow(valueRow, args)
		if err != nil {
			return 0, rowsAffected, err
		}

		// Store in B+Tree
		tree.Put([]byte(key), valueData)
		rowsAffected++
	}

	_ = table // Table def used for validation
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
			newVal, err := evalExpression(setClause.Value, args)
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

	// Get column names
	columns := make([]string, 0)
	for _, col := range stmt.Columns {
		switch c := col.(type) {
		case *query.Identifier:
			columns = append(columns, c.Name)
		case *query.StarExpr:
			// SELECT * - get all columns from table
			for _, tc := range table.Columns {
				columns = append(columns, tc.Name)
			}
		default:
			columns = append(columns, fmt.Sprintf("col%d", len(columns)))
		}
	}

	// Read all rows from B+Tree
	var rows [][]interface{}
	tree, exists := c.tableTrees[stmt.From.Name]
	if !exists {
		return columns, rows, nil
	}

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	for iter.HasNext() {
		_, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode row
		row, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}
		rows = append(rows, row)
	}

	return columns, rows, nil
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
	values := make([]interface{}, len(exprs))
	for i, expr := range exprs {
		val, err := evalExpression(expr, args)
		if err != nil {
			return nil, err
		}
		values[i] = val
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
func evalExpression(expr query.Expression, args []interface{}) (interface{}, error) {
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
