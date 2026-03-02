package catalog

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// BenchmarkInsert benchmarks single row inserts
func BenchmarkInsert(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "bench_insert",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "value", Type: query.TokenReal},
		},
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Insert(&query.InsertStmt{
			Table:   "bench_insert",
			Columns: []string{"id", "name", "value"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: fmt.Sprintf("name_%d", i)},
				&query.NumberLiteral{Value: float64(i) * 1.5},
			}},
		}, nil)
	}
}

// BenchmarkSelect benchmarks SELECT queries
func BenchmarkSelect(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "bench_select",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "value", Type: query.TokenReal},
		},
	})

	// Insert data
	for i := 0; i < 10000; i++ {
		cat.Insert(&query.InsertStmt{
			Table:   "bench_select",
			Columns: []string{"id", "name", "value"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: fmt.Sprintf("name_%d", i)},
				&query.NumberLiteral{Value: float64(i) * 1.5},
			}},
		}, nil)
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
			&query.Identifier{Name: "value"},
		},
		From: &query.TableRef{Name: "bench_select"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 5000},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkSelectAll benchmarks SELECT * queries
func BenchmarkSelectAll(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "bench_select_all",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "value", Type: query.TokenReal},
		},
	})

	// Insert data
	for i := 0; i < 1000; i++ {
		cat.Insert(&query.InsertStmt{
			Table:   "bench_select_all",
			Columns: []string{"id", "name", "value"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: fmt.Sprintf("name_%d", i)},
				&query.NumberLiteral{Value: float64(i) * 1.5},
			}},
		}, nil)
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "bench_select_all"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkUpdate benchmarks UPDATE queries
func BenchmarkUpdate(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "bench_update",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenReal},
		},
	})

	// Insert data
	for i := 0; i < 10000; i++ {
		cat.Insert(&query.InsertStmt{
			Table:   "bench_update",
			Columns: []string{"id", "value"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i) * 1.5},
			}},
		}, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Update(&query.UpdateStmt{
			Table: "bench_update",
			Set: []*query.SetClause{
				{Column: "value", Value: &query.NumberLiteral{Value: float64(i) * 2.0}},
			},
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: float64(i % 10000)},
			},
		}, nil)
	}
}

// BenchmarkDelete benchmarks DELETE queries
func BenchmarkDelete(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "bench_delete",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenReal},
		},
	})

	// Insert data
	for i := 0; i < b.N; i++ {
		cat.Insert(&query.InsertStmt{
			Table:   "bench_delete",
			Columns: []string{"id", "value"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i) * 1.5},
			}},
		}, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Delete(&query.DeleteStmt{
			Table: "bench_delete",
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: float64(i)},
			},
		}, nil)
	}
}

// BenchmarkAggregateCount benchmarks COUNT aggregate
func BenchmarkAggregateCount(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "bench_agg",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenReal},
		},
	})

	// Insert data
	for i := 0; i < 10000; i++ {
		cat.Insert(&query.InsertStmt{
			Table:   "bench_agg",
			Columns: []string{"id", "value"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i) * 1.5},
			}},
		}, nil)
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
		},
		From: &query.TableRef{Name: "bench_agg"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkAggregateSum benchmarks SUM aggregate
func BenchmarkAggregateSum(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "bench_sum",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenReal},
		},
	})

	// Insert data
	for i := 0; i < 10000; i++ {
		cat.Insert(&query.InsertStmt{
			Table:   "bench_sum",
			Columns: []string{"id", "value"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i) * 1.5},
			}},
		}, nil)
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "value"}}},
		},
		From: &query.TableRef{Name: "bench_sum"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkOrderBy benchmarks ORDER BY queries
func BenchmarkOrderBy(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "bench_order",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenReal},
		},
	})

	// Insert data
	for i := 0; i < 10000; i++ {
		cat.Insert(&query.InsertStmt{
			Table:   "bench_order",
			Columns: []string{"id", "value"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i) * 1.5},
			}},
		}, nil)
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "value"}},
		From:    &query.TableRef{Name: "bench_order"},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.Identifier{Name: "value"}, Desc: true},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkGroupBy benchmarks GROUP BY queries
func BenchmarkGroupBy(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "bench_group",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "value", Type: query.TokenReal},
		},
	})

	// Insert data
	categories := []string{"A", "B", "C", "D", "E"}
	for i := 0; i < 10000; i++ {
		cat.Insert(&query.InsertStmt{
			Table:   "bench_group",
			Columns: []string{"id", "category", "value"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: categories[i%5]},
				&query.NumberLiteral{Value: float64(i) * 1.5},
			}},
		}, nil)
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "value"}}},
		},
		From:    &query.TableRef{Name: "bench_group"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.Select(stmt, nil)
	}
}

// BenchmarkJSONExtract benchmarks JSON extraction
func BenchmarkJSONExtract(b *testing.B) {
	json := `{"users": [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}, {"name": "Bob", "age": 35}]}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		JSONExtract(json, "$.users[0].name")
	}
}

// BenchmarkJSONSet benchmarks JSON modification
func BenchmarkJSONSet(b *testing.B) {
	json := `{"users": [{"name": "John", "age": 30}]}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		JSONSet(json, "$.users[0].age", "31")
	}
}
