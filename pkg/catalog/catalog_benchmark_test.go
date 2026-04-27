package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func setupBenchCatalog(b *testing.B) *Catalog {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	wal, err := storage.OpenWAL(b.TempDir() + "/test.wal")
	if err != nil {
		b.Skip("WAL not available for benchmark")
	}
	tree, err := btree.NewBTree(pool)
	if err != nil {
		b.Fatal(err)
	}
	cat := New(tree, pool, wal)
	return cat
}

func setupBenchCatalogTable(b *testing.B, cat *Catalog, numRows int) {
	err := cat.CreateTable(
		&query.CreateTableStmt{
			Table: "bench_users",
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "name", Type: query.TokenText},
				{Name: "age", Type: query.TokenInteger},
				{Name: "email", Type: query.TokenText},
			},
		})
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < numRows; i++ {
		_, _, err := cat.Insert(ctx, &query.InsertStmt{
			Table:   "bench_users",
			Columns: []string{"id", "name", "age", "email"},
			Values: [][]query.Expression{
				{
					&query.NumberLiteral{Value: float64(i)},
					&query.StringLiteral{Value: fmt.Sprintf("user-%d", i)},
					&query.NumberLiteral{Value: float64(i % 100)},
					&query.StringLiteral{Value: fmt.Sprintf("user-%d@example.com", i)},
				},
			},
		}, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCatalogCreateTable benchmarks table creation
func BenchmarkCatalogCreateTable(b *testing.B) {
	cat := setupBenchCatalog(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.CreateTable(&query.CreateTableStmt{
			Table: fmt.Sprintf("temp_table_%d", i),
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "name", Type: query.TokenText},
			},
		})
	}
}

// BenchmarkCatalogInsert benchmarks single-row INSERT
func BenchmarkCatalogInsert(b *testing.B) {
	cat := setupBenchCatalog(b)
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "bench_insert",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "bench_insert",
			Columns: []string{"id", "name"},
			Values: [][]query.Expression{
				{
					&query.NumberLiteral{Value: float64(i)},
					&query.StringLiteral{Value: fmt.Sprintf("name-%d", i)},
				},
			},
		}, nil)
	}
}

// BenchmarkCatalogSelect benchmarks simple SELECT
func BenchmarkCatalogSelect(b *testing.B) {
	cat := setupBenchCatalog(b)
	setupBenchCatalogTable(b, cat, 1000)

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "bench_users"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkCatalogSelectWhere benchmarks SELECT with WHERE clause
func BenchmarkCatalogSelectWhere(b *testing.B) {
	cat := setupBenchCatalog(b)
	setupBenchCatalogTable(b, cat, 1000)

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "bench_users"},
		Where: &query.BinaryExpr{
			Operator: query.TokenEq,
			Left:     &query.Identifier{Name: "age"},
			Right:    &query.NumberLiteral{Value: 42},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkCatalogSelectIndexed benchmarks SELECT with primary key lookup
func BenchmarkCatalogSelectIndexed(b *testing.B) {
	cat := setupBenchCatalog(b)
	setupBenchCatalogTable(b, cat, 1000)

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "bench_users"},
		Where: &query.BinaryExpr{
			Operator: query.TokenEq,
			Left:     &query.Identifier{Name: "id"},
			Right:    &query.NumberLiteral{Value: 42},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkCatalogUpdate benchmarks UPDATE
func BenchmarkCatalogUpdate(b *testing.B) {
	cat := setupBenchCatalog(b)
	setupBenchCatalogTable(b, cat, 1000)

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Update(ctx, &query.UpdateStmt{
			Table: "bench_users",
			Set: []*query.SetClause{
				{Column: "age", Value: &query.NumberLiteral{Value: float64(i % 200)}},
			},
			Where: &query.BinaryExpr{
				Operator: query.TokenEq,
				Left:     &query.Identifier{Name: "id"},
				Right:    &query.NumberLiteral{Value: float64(i % 1000)},
			},
		}, nil)
	}
}

// BenchmarkCatalogDelete benchmarks DELETE
func BenchmarkCatalogDelete(b *testing.B) {
	cat := setupBenchCatalog(b)
	setupBenchCatalogTable(b, cat, 1000)

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Delete(ctx, &query.DeleteStmt{
			Table: "bench_users",
			Where: &query.BinaryExpr{
				Operator: query.TokenEq,
				Left:     &query.Identifier{Name: "id"},
				Right:    &query.NumberLiteral{Value: float64(i % 1000)},
			},
		}, nil)
	}
}

// BenchmarkCatalogSelectJoin benchmarks SELECT with JOIN
func BenchmarkCatalogSelectJoin(b *testing.B) {
	cat := setupBenchCatalog(b)

	// Create customers table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "customers",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	// Create orders table
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
			{Name: "total", Type: query.TokenReal},
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "customers",
			Columns: []string{"id", "name"},
			Values: [][]query.Expression{
				{
					&query.NumberLiteral{Value: float64(i)},
					&query.StringLiteral{Value: fmt.Sprintf("customer-%d", i)},
				},
			},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "orders",
			Columns: []string{"id", "user_id", "total"},
			Values: [][]query.Expression{
				{
					&query.NumberLiteral{Value: float64(i)},
					&query.NumberLiteral{Value: float64(i)},
					&query.NumberLiteral{Value: float64(i) * 1.5},
				},
			},
		}, nil)
	}

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "customers"},
		Joins: []*query.JoinClause{
			{
				Type:  query.TokenInner,
				Table: &query.TableRef{Name: "orders"},
				Condition: &query.BinaryExpr{
					Operator: query.TokenEq,
					Left:     &query.QualifiedIdentifier{Table: "customers", Column: "id"},
					Right:    &query.QualifiedIdentifier{Table: "orders", Column: "user_id"},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkCatalogSelectGroupBy benchmarks SELECT with GROUP BY
func BenchmarkCatalogSelectGroupBy(b *testing.B) {
	cat := setupBenchCatalog(b)
	setupBenchCatalogTable(b, cat, 1000)

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "bench_users"},
		Columns: []query.Expression{
			&query.Identifier{Name: "age"},
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
		},
		GroupBy: []query.Expression{&query.Identifier{Name: "age"}},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkCatalogSelectAggregate benchmarks aggregate functions
func BenchmarkCatalogSelectAggregate(b *testing.B) {
	cat := setupBenchCatalog(b)
	setupBenchCatalogTable(b, cat, 1000)

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "bench_users"},
		Columns: []query.Expression{
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "age"}}},
			&query.FunctionCall{Name: "AVG", Args: []query.Expression{&query.Identifier{Name: "age"}}},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkCatalogSelectOrderBy benchmarks SELECT with ORDER BY
func BenchmarkCatalogSelectOrderBy(b *testing.B) {
	cat := setupBenchCatalog(b)
	setupBenchCatalogTable(b, cat, 1000)

	stmt := &query.SelectStmt{
		From:    &query.TableRef{Name: "bench_users"},
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "name"}},
		OrderBy: []*query.OrderByExpr{{Expr: &query.Identifier{Name: "age"}}},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkCatalogSelectLimit benchmarks SELECT with LIMIT
func BenchmarkCatalogSelectLimit(b *testing.B) {
	cat := setupBenchCatalog(b)
	setupBenchCatalogTable(b, cat, 1000)

	stmt := &query.SelectStmt{
		From:    &query.TableRef{Name: "bench_users"},
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "name"}},
		Limit:   &query.NumberLiteral{Value: 10},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkCatalogExecuteQuery benchmarks the high-level ExecuteQuery API
func BenchmarkCatalogExecuteQuery(b *testing.B) {
	cat := setupBenchCatalog(b)
	setupBenchCatalogTable(b, cat, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.ExecuteQuery(`SELECT id, name FROM bench_users WHERE age = 42`)
	}
}
