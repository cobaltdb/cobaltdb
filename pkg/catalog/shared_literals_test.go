package catalog

import (
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func numReal(v float64) *query.NumberLiteral { return &query.NumberLiteral{Value: v} }
func strReal(s string) *query.StringLiteral  { return &query.StringLiteral{Value: s} }

func createCoverageTestTable(t *testing.T, cat *Catalog, name string, cols []*query.ColumnDef) {
	t.Helper()
	stmt := &query.CreateTableStmt{
		Table:   name,
		Columns: cols,
	}
	if err := cat.CreateTable(stmt); err != nil {
		t.Fatalf("CreateTable(%s) failed: %v", name, err)
	}
}

//lint:ignore U1000 retained for generated catalog coverage tests.
func colReal(name string) *query.QualifiedIdentifier {
	// Parse "table.column" format
	if dotIdx := strings.IndexByte(name, '.'); dotIdx > 0 && dotIdx < len(name)-1 {
		return &query.QualifiedIdentifier{Table: name[:dotIdx], Column: name[dotIdx+1:]}
	}
	return &query.QualifiedIdentifier{Column: name}
}

func mustParseSelect(sql string) *query.SelectStmt {
	parsed, err := query.Parse(sql)
	if err != nil {
		panic(err)
	}
	// Handle both SelectStmt and UnionStmt
	if sel, ok := parsed.(*query.SelectStmt); ok {
		return sel
	}
	if union, ok := parsed.(*query.UnionStmt); ok {
		// Extract the left SelectStmt from Union
		if left, ok := union.Left.(*query.SelectStmt); ok {
			return left
		}
	}
	panic("parsed statement is not a SELECT")
}
