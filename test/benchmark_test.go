package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func BenchmarkInsert(b *testing.B) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 10 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE bench (id INTEGER, value TEXT)`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec(ctx, `INSERT INTO bench (id, value) VALUES (?, ?)`, i, fmt.Sprintf("value-%d", i))
	}
	b.StopTimer()
}

func BenchmarkInsertBatch(b *testing.B) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 10 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE bench_batch (id INTEGER, value TEXT)`)

	batchSize := 100
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin(ctx)
		for j := 0; j < batchSize; j++ {
			tx.Exec(ctx, `INSERT INTO bench_batch (id, value) VALUES (?, ?)`,
				i*batchSize+j, fmt.Sprintf("value-%d", i*batchSize+j))
		}
		tx.Commit()
	}
	b.StopTimer()
}

func BenchmarkSelect(b *testing.B) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 10 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE bench_select (id INTEGER, value TEXT)`)

	// Insert test data
	numRows := 10000
	for i := 0; i < numRows; i++ {
		db.Exec(ctx, `INSERT INTO bench_select (id, value) VALUES (?, ?)`, i, fmt.Sprintf("value-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, `SELECT id, value FROM bench_select`)
		rows.Close()
	}
	b.StopTimer()
}

func BenchmarkSelectWithScan(b *testing.B) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 10 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE bench_scan (id INTEGER, value TEXT)`)

	// Insert test data
	numRows := 1000
	for i := 0; i < numRows; i++ {
		db.Exec(ctx, `INSERT INTO bench_scan (id, value) VALUES (?, ?)`, i, fmt.Sprintf("value-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, `SELECT id, value FROM bench_scan`)
		for rows.Next() {
			var id interface{}
			var value interface{}
			rows.Scan(&id, &value)
		}
		rows.Close()
	}
	b.StopTimer()
}

func BenchmarkCreateTable(b *testing.B) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tableName := fmt.Sprintf("table_%d", i)
		db.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, value TEXT)`, tableName))
	}
	b.StopTimer()
}

func BenchmarkTransaction(b *testing.B) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 10 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE bench_tx (id INTEGER, value TEXT)`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin(ctx)
		tx.Exec(ctx, `INSERT INTO bench_tx (id, value) VALUES (?, ?)`, i, fmt.Sprintf("value-%d", i))
		tx.Commit()
	}
	b.StopTimer()
}

func BenchmarkConcurrentInsert(b *testing.B) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 10 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE bench_concurrent (id INTEGER, value TEXT)`)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			db.Exec(ctx, `INSERT INTO bench_concurrent (id, value) VALUES (?, ?)`, i, fmt.Sprintf("value-%d", i))
			i++
		}
	})
	b.StopTimer()
}

func BenchmarkConcurrentRead(b *testing.B) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 10 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE bench_read (id INTEGER, value TEXT)`)

	// Insert test data
	for i := 0; i < 1000; i++ {
		db.Exec(ctx, `INSERT INTO bench_read (id, value) VALUES (?, ?)`, i, fmt.Sprintf("value-%d", i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rows, _ := db.Query(ctx, `SELECT id, value FROM bench_read`)
			rows.Close()
		}
	})
}
