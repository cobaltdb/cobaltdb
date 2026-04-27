package optimizer

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func benchmarkOptimizerSetup() *Optimizer {
	stats := &Statistics{
		TableStats: map[string]*TableStatistics{
			"users": {
				TableName: "users",
				RowCount:  100000,
				IndexStats: map[string]*IndexStatistics{
					"idx_id":        {IndexName: "idx_id", Columns: []string{"id"}, IsUnique: true, Selectivity: 0.00001},
					"idx_name":      {IndexName: "idx_name", Columns: []string{"name"}, IsUnique: false, Selectivity: 0.1},
					"idx_composite": {IndexName: "idx_composite", Columns: []string{"name", "email"}, IsUnique: false, Selectivity: 0.05},
				},
			},
			"orders": {
				TableName: "orders",
				RowCount:  500000,
				IndexStats: map[string]*IndexStatistics{
					"idx_user_id": {IndexName: "idx_user_id", Columns: []string{"user_id"}, IsUnique: false, Selectivity: 0.01},
					"idx_date":    {IndexName: "idx_date", Columns: []string{"created_at"}, IsUnique: false, Selectivity: 0.2},
				},
			},
			"products": {
				TableName: "products",
				RowCount:  10000,
				IndexStats: map[string]*IndexStatistics{
					"idx_sku": {IndexName: "idx_sku", Columns: []string{"sku"}, IsUnique: true, Selectivity: 0.0001},
				},
			},
			"categories": {
				TableName: "categories",
				RowCount:  100,
			},
		},
	}
	return New(DefaultConfig(), stats)
}

// BenchmarkOptimizeSimple benchmarks optimization of a simple SELECT
func BenchmarkOptimizeSimple(b *testing.B) {
	opt := benchmarkOptimizerSetup()
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Operator: query.TokenEq,
			Left:     &query.Identifier{Name: "id"},
			Right:    &query.NumberLiteral{Value: 42},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opt.Optimize(stmt)
	}
}

// BenchmarkOptimizeWithJoins benchmarks optimization with multiple joins
func BenchmarkOptimizeWithJoins(b *testing.B) {
	opt := benchmarkOptimizerSetup()
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Joins: []*query.JoinClause{
			{Type: query.TokenCross, Table: &query.TableRef{Name: "orders"}},
			{Type: query.TokenInner, Table: &query.TableRef{Name: "products"}},
			{Type: query.TokenLeft, Table: &query.TableRef{Name: "categories"}},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opt.Optimize(stmt)
	}
}

// BenchmarkOptimizeWithManyJoins benchmarks reordering with 6 joins (max default)
func BenchmarkOptimizeWithManyJoins(b *testing.B) {
	opt := benchmarkOptimizerSetup()
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Joins: []*query.JoinClause{
			{Type: query.TokenCross, Table: &query.TableRef{Name: "orders"}},
			{Type: query.TokenInner, Table: &query.TableRef{Name: "products"}},
			{Type: query.TokenLeft, Table: &query.TableRef{Name: "categories"}},
			{Type: query.TokenInner, Table: &query.TableRef{Name: "reviews"}},
			{Type: query.TokenCross, Table: &query.TableRef{Name: "shipments"}},
			{Type: query.TokenLeft, Table: &query.TableRef{Name: "invoices"}},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opt.Optimize(stmt)
	}
}

// BenchmarkSelectBestIndex benchmarks index selection
func BenchmarkSelectBestIndex(b *testing.B) {
	opt := benchmarkOptimizerSetup()
	where := &query.BinaryExpr{
		Operator: query.TokenEq,
		Left:     &query.Identifier{Name: "name"},
		Right:    &query.StringLiteral{Value: "Alice"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opt.SelectBestIndex("users", where)
	}
}

// BenchmarkSelectBestIndexComposite benchmarks index selection with composite index
func BenchmarkSelectBestIndexComposite(b *testing.B) {
	opt := benchmarkOptimizerSetup()
	where := &query.BinaryExpr{
		Operator: query.TokenAnd,
		Left: &query.BinaryExpr{
			Operator: query.TokenEq,
			Left:     &query.Identifier{Name: "name"},
			Right:    &query.StringLiteral{Value: "Alice"},
		},
		Right: &query.BinaryExpr{
			Operator: query.TokenEq,
			Left:     &query.Identifier{Name: "email"},
			Right:    &query.StringLiteral{Value: "alice@example.com"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opt.SelectBestIndex("users", where)
	}
}

// BenchmarkScoreIndex benchmarks the index scoring function
func BenchmarkScoreIndex(b *testing.B) {
	opt := benchmarkOptimizerSetup()
	columns := []string{"name", "email"}
	indexStats := &IndexStatistics{
		IndexName:   "idx_composite",
		Columns:     []string{"name", "email"},
		IsUnique:    false,
		Selectivity: 0.05,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opt.scoreIndex(columns, indexStats)
	}
}

// BenchmarkExtractColumnReferences benchmarks column reference extraction
func BenchmarkExtractColumnReferences(b *testing.B) {
	opt := benchmarkOptimizerSetup()
	expr := &query.BinaryExpr{
		Operator: query.TokenAnd,
		Left: &query.BinaryExpr{
			Operator: query.TokenEq,
			Left:     &query.Identifier{Name: "name"},
			Right:    &query.StringLiteral{Value: "Alice"},
		},
		Right: &query.BinaryExpr{
			Operator: query.TokenGt,
			Left:     &query.Identifier{Name: "age"},
			Right:    &query.NumberLiteral{Value: 18},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opt.extractColumnReferences(expr)
	}
}
