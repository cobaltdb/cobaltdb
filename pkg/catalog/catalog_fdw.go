package catalog

import (
	"fmt"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/fdw"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// CreateForeignTable creates a new foreign table backed by an FDW.
func (c *Catalog) CreateForeignTable(stmt *query.CreateForeignTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	if _, exists := c.tables[stmt.Table]; exists {
		return ErrTableExists
	}
	if _, exists := c.foreignTables[stmt.Table]; exists {
		return ErrTableExists
	}

	// Validate wrapper exists
	if c.fdwRegistry != nil {
		if !c.fdwRegistry.Has(stmt.Wrapper) {
			return fmt.Errorf("foreign data wrapper '%s' not found", stmt.Wrapper)
		}
	}

	cols := make([]ColumnDef, len(stmt.Columns))
	for i, col := range stmt.Columns {
		cols[i] = ColumnDef{
			Name:          col.Name,
			Type:          query.TokenTypeString(col.Type),
			NotNull:       col.NotNull,
			Unique:        col.Unique,
			PrimaryKey:    col.PrimaryKey,
			AutoIncrement: col.AutoIncrement,
		}
	}

	c.foreignTables[stmt.Table] = &ForeignTableDef{
		TableName: stmt.Table,
		Columns:   cols,
		Wrapper:   stmt.Wrapper,
		Options:   stmt.Options,
	}
	return nil
}

// GetForeignTable returns the foreign table definition.
func (c *Catalog) GetForeignTable(name string) (*ForeignTableDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ft, exists := c.foreignTables[name]
	if !exists {
		return nil, ErrTableNotFound
	}
	return ft, nil
}

// DropForeignTable drops a foreign table.
func (c *Catalog) DropForeignTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	if _, exists := c.foreignTables[name]; !exists {
		return ErrTableNotFound
	}
	delete(c.foreignTables, name)
	return nil
}

// IsForeignTable returns true if the named table is a foreign table.
func (c *Catalog) IsForeignTable(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.foreignTables[name]
	return ok
}

// SetFDWRegistry sets the FDW registry on the catalog.
func (c *Catalog) SetFDWRegistry(registry *fdw.Registry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fdwRegistry = registry
}

// GetFDWRegistry returns the FDW registry.
func (c *Catalog) GetFDWRegistry() *fdw.Registry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.fdwRegistry
}

func (c *Catalog) buildFDWScanOptions(stmt *query.SelectStmt, args []interface{}) fdw.ScanOptions {
	if stmt == nil || stmt.From == nil {
		return fdw.ScanOptions{}
	}
	return fdw.ScanOptions{
		Predicates: collectFDWPredicates(stmt.Where, stmt.From, args),
	}
}

func collectFDWPredicates(expr query.Expression, from *query.TableRef, args []interface{}) []fdw.Predicate {
	if expr == nil || from == nil {
		return nil
	}
	if bin, ok := expr.(*query.BinaryExpr); ok && bin.Operator == query.TokenAnd {
		left := collectFDWPredicates(bin.Left, from, args)
		right := collectFDWPredicates(bin.Right, from, args)
		if len(left) == 0 {
			return right
		}
		return append(left, right...)
	}
	predicate, ok := extractFDWPredicate(expr, from, args)
	if !ok {
		return nil
	}
	return []fdw.Predicate{predicate}
}

func extractFDWPredicate(expr query.Expression, from *query.TableRef, args []interface{}) (fdw.Predicate, bool) {
	bin, ok := expr.(*query.BinaryExpr)
	if !ok || !isFDWPushdownOperator(bin.Operator) {
		return fdw.Predicate{}, false
	}
	if column, ok := fdwPredicateColumn(bin.Left, from); ok {
		value, ok := fdwPredicateValue(bin.Right, args)
		if !ok {
			return fdw.Predicate{}, false
		}
		return fdw.Predicate{Column: column, Operator: query.TokenTypeString(bin.Operator), Value: value}, true
	}
	if column, ok := fdwPredicateColumn(bin.Right, from); ok {
		value, ok := fdwPredicateValue(bin.Left, args)
		if !ok {
			return fdw.Predicate{}, false
		}
		return fdw.Predicate{Column: column, Operator: reverseFDWOperator(bin.Operator), Value: value}, true
	}
	return fdw.Predicate{}, false
}

func isFDWPushdownOperator(op query.TokenType) bool {
	switch op {
	case query.TokenEq, query.TokenNeq, query.TokenLt, query.TokenGt, query.TokenLte, query.TokenGte:
		return true
	default:
		return false
	}
}

func fdwPredicateColumn(expr query.Expression, from *query.TableRef) (string, bool) {
	switch e := expr.(type) {
	case *query.Identifier:
		return e.Name, e.Name != ""
	case *query.ColumnRef:
		if !fdwQualifierMatches(e.Table, from) {
			return "", false
		}
		return e.Column, e.Column != "" && e.Column != "*"
	case *query.QualifiedIdentifier:
		if !fdwQualifierMatches(e.Table, from) {
			return "", false
		}
		return e.Column, e.Column != ""
	default:
		return "", false
	}
}

func fdwQualifierMatches(qualifier string, from *query.TableRef) bool {
	if qualifier == "" {
		return true
	}
	if strings.EqualFold(qualifier, from.Name) {
		return true
	}
	return from.Alias != "" && strings.EqualFold(qualifier, from.Alias)
}

func fdwPredicateValue(expr query.Expression, args []interface{}) (interface{}, bool) {
	switch e := expr.(type) {
	case *query.StringLiteral:
		return e.Value, true
	case *query.NumberLiteral:
		return e.Value, true
	case *query.BooleanLiteral:
		return e.Value, true
	case *query.NullLiteral:
		return nil, true
	case *query.PlaceholderExpr:
		if e.Index < 0 || e.Index >= len(args) {
			return nil, false
		}
		return args[e.Index], true
	default:
		return nil, false
	}
}

func reverseFDWOperator(op query.TokenType) string {
	switch op {
	case query.TokenLt:
		return query.TokenTypeString(query.TokenGt)
	case query.TokenGt:
		return query.TokenTypeString(query.TokenLt)
	case query.TokenLte:
		return query.TokenTypeString(query.TokenGte)
	case query.TokenGte:
		return query.TokenTypeString(query.TokenLte)
	default:
		return query.TokenTypeString(op)
	}
}
