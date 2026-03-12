package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Save/Load roundtrip with real table data
// ============================================================

func TestCoverage_SaveLoadRoundtrip(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "roundtrip",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "status", Type: query.TokenText},
			{Name: "score", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert via Insert method
	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "roundtrip",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "done"},
			&query.NumberLiteral{Value: 10},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := cat.Save(); err != nil {
		t.Fatal(err)
	}

	// Load from same tree
	cat2 := New(tree, pool, nil)
	if err := cat2.Load(); err != nil {
		t.Fatal(err)
	}

	tables := cat2.ListTables()
	found := false
	for _, tbl := range tables {
		if tbl == "roundtrip" {
			found = true
			break
		}
	}
	if !found {
		t.Error("table 'roundtrip' not found after Load")
	}
	pool.Close()
}

// ============================================================
// RLS-enabled insert/update/delete checks
// ============================================================

func TestCoverage_RLS_CheckOperations(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cat.EnableRLS()

	createCoverageTestTable(t, cat, "rls_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "owner", Type: query.TokenText},
	})

	ctx := context.Background()
	row := map[string]interface{}{"id": 1, "owner": "alice"}

	allowed, err := cat.checkRLSForInsertInternal(ctx, "rls_t", row, "alice", nil)
	if err != nil {
		t.Errorf("checkRLSForInsert: %v", err)
	}
	if !allowed {
		t.Error("expected insert allowed")
	}

	allowed, err = cat.checkRLSForUpdateInternal(ctx, "rls_t", row, "alice", nil)
	if err != nil {
		t.Errorf("checkRLSForUpdate: %v", err)
	}
	if !allowed {
		t.Error("expected update allowed")
	}

	allowed, err = cat.checkRLSForDeleteInternal(ctx, "rls_t", row, "alice", nil)
	if err != nil {
		t.Errorf("checkRLSForDelete: %v", err)
	}
	if !allowed {
		t.Error("expected delete allowed")
	}

	cols := []string{"id", "owner"}
	rows := [][]interface{}{{1, "alice"}, {2, "bob"}}
	filteredCols, filteredRows, err := cat.applyRLSFilterInternal(ctx, "rls_t", cols, rows, "alice", nil)
	if err != nil {
		t.Errorf("applyRLSFilter: %v", err)
	}
	if len(filteredCols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(filteredCols))
	}
	if len(filteredRows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(filteredRows))
	}
}

// ============================================================
// resolveOuterRefsInQuery
// ============================================================

func TestCoverage_resolveOuterRefsInQuery(t *testing.T) {
	result := resolveOuterRefsInQuery(nil, nil, nil)
	if result != nil {
		t.Error("expected nil for nil subquery")
	}

	sub := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "t1"},
	}
	result = resolveOuterRefsInQuery(sub, nil, nil)
	if result != sub {
		t.Error("expected same subquery when outerRow is nil")
	}

	result = resolveOuterRefsInQuery(sub, []interface{}{1}, nil)
	if result != sub {
		t.Error("expected same subquery when outerColumns is empty")
	}

	outerCols := []ColumnDef{
		{Name: "outer_id", sourceTbl: "outer_t"},
	}
	subWithRef := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "inner_t"},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Table: "outer_t", Column: "outer_id"},
			Operator: query.TokenEq,
			Right:    &query.Identifier{Name: "id"},
		},
	}
	result = resolveOuterRefsInQuery(subWithRef, []interface{}{42}, outerCols)
	if result == nil {
		t.Error("expected non-nil result")
	}

	subWithJoin := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "inner_t", Alias: "it"},
		Joins: []*query.JoinClause{
			{Table: &query.TableRef{Name: "other_t", Alias: "ot"}},
		},
	}
	result = resolveOuterRefsInQuery(subWithJoin, []interface{}{1}, outerCols)
	if result == nil {
		t.Error("expected non-nil result with joins")
	}
}

// ============================================================
// resolveAggregateInExpr
// ============================================================

func TestCoverage_resolveAggregateInExpr(t *testing.T) {
	result := resolveAggregateInExpr(nil, nil, nil)
	if result != nil {
		t.Error("expected nil")
	}

	selectCols := []selectColInfo{
		{name: "total", isAggregate: true, aggregateType: "SUM", aggregateCol: "amount"},
		{name: "cnt", isAggregate: true, aggregateType: "COUNT", aggregateCol: "*"},
		{name: "name", isAggregate: false},
	}
	row := []interface{}{100.0, int64(5), "test"}

	// BinaryExpr
	binExpr := &query.BinaryExpr{
		Left:     &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
		Operator: query.TokenGt,
		Right:    &query.NumberLiteral{Value: 50},
	}
	result = resolveAggregateInExpr(binExpr, selectCols, row)
	if result == nil {
		t.Error("expected non-nil for BinaryExpr")
	}

	// COUNT(*)
	countExpr := &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}
	result = resolveAggregateInExpr(countExpr, selectCols, row)
	if result == nil {
		t.Error("expected non-nil for COUNT(*)")
	}

	// Identifier
	result = resolveAggregateInExpr(&query.Identifier{Name: "name"}, selectCols, row)
	if result == nil {
		t.Error("expected non-nil for Identifier")
	}

	// UnaryExpr
	result = resolveAggregateInExpr(&query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.Identifier{Name: "total"},
	}, selectCols, row)
	if result == nil {
		t.Error("expected non-nil for UnaryExpr")
	}

	// BetweenExpr
	result = resolveAggregateInExpr(&query.BetweenExpr{
		Expr:  &query.Identifier{Name: "total"},
		Lower: &query.NumberLiteral{Value: 0},
		Upper: &query.NumberLiteral{Value: 200},
	}, selectCols, row)
	if result == nil {
		t.Error("expected non-nil for BetweenExpr")
	}

	// InExpr
	result = resolveAggregateInExpr(&query.InExpr{
		Expr: &query.Identifier{Name: "total"},
		List: []query.Expression{
			&query.NumberLiteral{Value: 100},
		},
	}, selectCols, row)
	if result == nil {
		t.Error("expected non-nil for InExpr")
	}

	// QualifiedIdentifier arg
	result = resolveAggregateInExpr(&query.FunctionCall{
		Name: "SUM",
		Args: []query.Expression{&query.QualifiedIdentifier{Table: "t", Column: "amount"}},
	}, selectCols, row)
	if result == nil {
		t.Error("expected non-nil for SUM with QualifiedIdentifier")
	}

	// DISTINCT
	distCols := []selectColInfo{
		{name: "dcnt", isAggregate: true, aggregateType: "COUNT", aggregateCol: "amount", isDistinct: true},
	}
	result = resolveAggregateInExpr(&query.FunctionCall{
		Name: "COUNT", Distinct: true,
		Args: []query.Expression{&query.Identifier{Name: "amount"}},
	}, distCols, []interface{}{int64(3)})
	if result == nil {
		t.Error("expected non-nil for COUNT DISTINCT")
	}

	// Expression arg (SUM(qty * price))
	exprArgCols := []selectColInfo{
		{name: "revenue", isAggregate: true, aggregateType: "SUM", aggregateCol: "unknown",
			aggregateExpr: &query.BinaryExpr{
				Left: &query.Identifier{Name: "qty"}, Operator: query.TokenStar,
				Right: &query.Identifier{Name: "price"},
			}},
	}
	result = resolveAggregateInExpr(&query.FunctionCall{
		Name: "SUM",
		Args: []query.Expression{&query.BinaryExpr{
			Left: &query.Identifier{Name: "qty"}, Operator: query.TokenStar,
			Right: &query.Identifier{Name: "price"},
		}},
	}, exprArgCols, []interface{}{500.0})
	if result == nil {
		t.Error("expected non-nil for SUM with expression arg")
	}
}

// ============================================================
// evaluateHaving numeric results
// ============================================================

func TestCoverage_evaluateHaving_NumericResults(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	selectCols := []selectColInfo{
		{name: "cnt", isAggregate: true, aggregateType: "COUNT", aggregateCol: "*"},
	}

	got, err := evaluateHaving(cat, []interface{}{int64(5)}, selectCols, nil, &query.Identifier{Name: "cnt"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Error("expected true for cnt=5")
	}

	got, err = evaluateHaving(cat, []interface{}{int64(0)}, selectCols, nil, &query.Identifier{Name: "cnt"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Error("expected false for cnt=0")
	}

	got, err = evaluateHaving(cat, []interface{}{nil}, selectCols, nil, &query.Identifier{Name: "cnt"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Error("expected false for nil")
	}

	floatCols := []selectColInfo{
		{name: "avg_val", isAggregate: true, aggregateType: "AVG", aggregateCol: "score"},
	}

	got, err = evaluateHaving(cat, []interface{}{float64(3.5)}, floatCols, nil, &query.Identifier{Name: "avg_val"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Error("expected true for avg=3.5")
	}

	got, err = evaluateHaving(cat, []interface{}{float64(0)}, floatCols, nil, &query.Identifier{Name: "avg_val"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Error("expected false for avg=0")
	}
}

// ============================================================
// ExecuteQuery helper
// ============================================================

func TestCoverage_ExecuteQuery(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// ExecuteQuery is a stub that returns empty result
	// Just verify it doesn't panic
	result, err := cat.ExecuteQuery("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// ============================================================
// isCacheableQuery
// ============================================================

func TestCoverage_isCacheableQuery(t *testing.T) {
	// SELECT with FROM is cacheable
	if !isCacheableQuery(&query.SelectStmt{From: &query.TableRef{Name: "t"}}) {
		t.Error("expected SELECT with FROM to be cacheable")
	}
	// SELECT without FROM is not cacheable
	if isCacheableQuery(&query.SelectStmt{}) {
		t.Error("expected SELECT without FROM to not be cacheable")
	}
}

// ============================================================
// valueToLiteral
// ============================================================

func TestCoverage_valueToLiteral(t *testing.T) {
	if _, ok := valueToLiteral(nil).(*query.NullLiteral); !ok {
		t.Error("expected NullLiteral for nil")
	}
	if nl, ok := valueToLiteral(int64(42)).(*query.NumberLiteral); !ok || nl.Value != 42 {
		t.Error("expected NumberLiteral(42)")
	}
	if nl, ok := valueToLiteral(float64(3.14)).(*query.NumberLiteral); !ok || nl.Value != 3.14 {
		t.Error("expected NumberLiteral(3.14)")
	}
	if sl, ok := valueToLiteral("hello").(*query.StringLiteral); !ok || sl.Value != "hello" {
		t.Error("expected StringLiteral(hello)")
	}
	if bl, ok := valueToLiteral(true).(*query.BooleanLiteral); !ok || !bl.Value {
		t.Error("expected BooleanLiteral(true)")
	}
	if nl, ok := valueToLiteral(42).(*query.NumberLiteral); !ok || nl.Value != 42 {
		t.Error("expected NumberLiteral(42) for int")
	}
}
