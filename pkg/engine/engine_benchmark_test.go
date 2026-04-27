package engine

import (
	"context"
	"fmt"
	"testing"
)

func setupBenchDB(b *testing.B) *DB {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 2048,
	})
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func setupBenchTable(b *testing.B, db *DB, numRows int) {
	ctx := context.Background()
	_, err := db.Exec(ctx, `CREATE TABLE bench_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)`)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < numRows; i++ {
		_, err := db.Exec(ctx, `INSERT INTO bench_users (id, name, age, email) VALUES (?, ?, ?, ?)`,
			i, fmt.Sprintf("user-%d", i), i%100, fmt.Sprintf("user-%d@example.com", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkExecInsert benchmarks single-row INSERT statements
func BenchmarkExecInsert(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()
	_, err := db.Exec(ctx, `CREATE TABLE bench_insert (id INTEGER, name TEXT)`)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec(ctx, `INSERT INTO bench_insert (id, name) VALUES (?, ?)`, i, fmt.Sprintf("name-%d", i))
	}
}

// BenchmarkExecSelect benchmarks simple SELECT execution
func BenchmarkExecSelect(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, `SELECT id, name, age, email FROM bench_users WHERE age = ?`, i%100)
		if rows != nil {
			rows.Close()
		}
	}
}

// BenchmarkExecSelectIndexed benchmarks SELECT with primary key lookup
func BenchmarkExecSelectIndexed(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, `SELECT id, name, age, email FROM bench_users WHERE id = ?`, i%1000)
		if rows != nil {
			rows.Close()
		}
	}
}

// BenchmarkExecUpdate benchmarks UPDATE statements
func BenchmarkExecUpdate(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec(ctx, `UPDATE bench_users SET age = ? WHERE id = ?`, i%200, i%1000)
	}
}

// BenchmarkExecDelete benchmarks DELETE + INSERT cycle to maintain row count
func BenchmarkExecDelete(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec(ctx, `DELETE FROM bench_users WHERE id = ?`, i%1000)
		db.Exec(ctx, `INSERT INTO bench_users (id, name, age, email) VALUES (?, ?, ?, ?)`,
			i%1000, fmt.Sprintf("user-%d", i), i%100, fmt.Sprintf("user-%d@example.com", i))
	}
}

// BenchmarkTransactionCommit benchmarks BEGIN/COMMIT overhead
func BenchmarkTransactionCommit(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 100)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin(ctx)
		tx.Exec(ctx, `INSERT INTO bench_users (id, name, age, email) VALUES (?, ?, ?, ?)`,
			1000+i, fmt.Sprintf("tx-%d", i), i%100, fmt.Sprintf("tx-%d@example.com", i))
		tx.Commit()
	}
}

// BenchmarkTransactionRollback benchmarks BEGIN/ROLLBACK overhead
func BenchmarkTransactionRollback(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 100)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin(ctx)
		tx.Exec(ctx, `INSERT INTO bench_users (id, name, age, email) VALUES (?, ?, ?, ?)`,
			1000+i, fmt.Sprintf("tx-%d", i), i%100, fmt.Sprintf("tx-%d@example.com", i))
		tx.Rollback()
	}
}

// BenchmarkQueryRow benchmarks QueryRow for single-value lookup
func BenchmarkQueryRow(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := db.QueryRow(ctx, `SELECT name FROM bench_users WHERE id = ?`, i%1000)
		var name string
		row.Scan(&name)
	}
}

// BenchmarkExecSelectOrderBy benchmarks SELECT with ORDER BY
func BenchmarkExecSelectOrderBy(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, `SELECT id, name FROM bench_users ORDER BY age`)
		if rows != nil {
			rows.Close()
		}
	}
}

// BenchmarkExecSelectLimit benchmarks SELECT with LIMIT
func BenchmarkExecSelectLimit(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, `SELECT id, name FROM bench_users LIMIT 10`)
		if rows != nil {
			rows.Close()
		}
	}
}

// BenchmarkExecSelectJoin benchmarks SELECT with JOIN
func BenchmarkExecSelectJoin(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, `CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)`)
	if err != nil {
		b.Fatal(err)
	}
	_, err = db.Exec(ctx, `CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		_, err := db.Exec(ctx, `INSERT INTO customers (id, name) VALUES (?, ?)`, i, fmt.Sprintf("customer-%d", i))
		if err != nil {
			b.Fatal(err)
		}
		_, err = db.Exec(ctx, `INSERT INTO orders (id, user_id, total) VALUES (?, ?, ?)`, i, i, float64(i)*1.5)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, `SELECT customers.name, orders.total FROM customers JOIN orders ON customers.id = orders.user_id`)
		if rows != nil {
			rows.Close()
		}
	}
}

// BenchmarkExecSelectGroupBy benchmarks SELECT with GROUP BY
func BenchmarkExecSelectGroupBy(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, `SELECT age, COUNT(*) FROM bench_users GROUP BY age`)
		if rows != nil {
			rows.Close()
		}
	}
}

// BenchmarkExecSelectAggregate benchmarks aggregate functions
func BenchmarkExecSelectAggregate(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, `SELECT COUNT(*), SUM(age), AVG(age), MAX(age), MIN(age) FROM bench_users`)
		if rows != nil {
			rows.Close()
		}
	}
}

// BenchmarkPreparedStatement benchmarks repeated execution of the same query
func BenchmarkPreparedStatement(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	setupBenchTable(b, db, 1000)
	ctx := context.Background()

	sql := `SELECT name FROM bench_users WHERE id = ?`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, sql, i%1000)
		if rows != nil {
			rows.Close()
		}
	}
}

// BenchmarkExecCreateTable benchmarks CREATE TABLE
func BenchmarkExecCreateTable(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec(ctx, fmt.Sprintf(`CREATE TABLE temp_table_%d (id INTEGER PRIMARY KEY, name TEXT)`, i))
	}
}

// BenchmarkExecDropTable benchmarks DROP TABLE
func BenchmarkExecDropTable(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		db.Exec(ctx, fmt.Sprintf(`CREATE TABLE drop_table_%d (id INTEGER PRIMARY KEY, name TEXT)`, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec(ctx, fmt.Sprintf(`DROP TABLE drop_table_%d`, i))
	}
}
