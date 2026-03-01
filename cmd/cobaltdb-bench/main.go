package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

var (
	flagHelp       bool
	flagInMemory   bool
	flagPath       string
	flagRows       int
	flagBenchmarks string
)

func init() {
	flag.BoolVar(&flagHelp, "help", false, "Show help")
	flag.BoolVar(&flagHelp, "h", false, "Show help (short)")
	flag.BoolVar(&flagInMemory, "memory", true, "Use in-memory database")
	flag.StringVar(&flagPath, "path", ":memory:", "Database path")
	flag.IntVar(&flagRows, "rows", 10000, "Number of rows for benchmarks")
	flag.StringVar(&flagBenchmarks, "bench", "all", "Benchmarks to run: all, insert, select, update, delete, transaction")
}

func main() {
	flag.Parse()

	if flagHelp {
		printHelp()
		os.Exit(0)
	}

	runBenchmarks()
}

func printHelp() {
	fmt.Print(`
CobaltDB Benchmark Tool v1.0

Usage:
  cobaltdb-bench [options]

Options:
  -h, -help           Show this help message
  -memory             Use in-memory database (default: true)
  -path <path>        Database file path
  -rows <n>           Number of rows (default: 10000)
  -bench <name>       Benchmark to run: all, insert, select, update, delete, transaction

Examples:
  cobaltdb-bench
  cobaltdb-bench -rows 50000
  cobaltdb-bench -bench insert
`)
}

func runBenchmarks() {
	fmt.Printf("CobaltDB Benchmark Tool\n")
	fmt.Printf("========================\n")
	fmt.Printf("Rows: %d\n", flagRows)
	fmt.Printf("Mode: %s\n", func() string {
		if flagInMemory {
			return "in-memory"
		}
		return "disk"
	}())
	fmt.Println()

	ctx := context.Background()

	db, err := engine.Open(flagPath, &engine.Options{
		InMemory: flagInMemory,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Run selected benchmarks
	switch flagBenchmarks {
	case "all":
		runAllBenchmarks(db, ctx)
	case "insert":
		runInsertBenchmark(db, ctx)
	case "select":
		runSelectBenchmark(db, ctx)
	case "update":
		runUpdateBenchmark(db, ctx)
	case "delete":
		runDeleteBenchmark(db, ctx)
	case "transaction":
		runTransactionBenchmark(db, ctx)
	default:
		fmt.Printf("Unknown benchmark: %s\n", flagBenchmarks)
	}
}

func runAllBenchmarks(db *engine.DB, ctx context.Context) {
	runInsertBenchmark(db, ctx)
	runSelectBenchmark(db, ctx)
	runUpdateBenchmark(db, ctx)
	runDeleteBenchmark(db, ctx)
	runTransactionBenchmark(db, ctx)
}

func runInsertBenchmark(db *engine.DB, ctx context.Context) {
	fmt.Println("=== INSERT Benchmark ===")

	// Setup
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_insert")
	db.Exec(ctx, "CREATE TABLE bench_insert (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

	// Benchmark
	start := time.Now()
	for i := 0; i < flagRows; i++ {
		db.Exec(ctx, "INSERT INTO bench_insert (name, age) VALUES (?, ?)", fmt.Sprintf("user-%d", i), i%100)
	}
	elapsed := time.Since(start)

	ops := float64(flagRows) / elapsed.Seconds()
	fmt.Printf("Time: %v\n", elapsed)
	fmt.Printf("Ops/sec: %.2f\n", ops)
	fmt.Printf("Avg time/op: %.2f ns\n", float64(elapsed.Nanoseconds())/float64(flagRows))
	fmt.Println()
}

func runSelectBenchmark(db *engine.DB, ctx context.Context) {
	fmt.Println("=== SELECT Benchmark ===")

	// Setup
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_select")
	db.Exec(ctx, "CREATE TABLE bench_select (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	for i := 0; i < flagRows; i++ {
		db.Exec(ctx, "INSERT INTO bench_select (name, age) VALUES (?, ?)", fmt.Sprintf("user-%d", i), i%100)
	}

	// Benchmark - Full scan
	start := time.Now()
	for i := 0; i < 100; i++ {
		rows, _ := db.Query(ctx, "SELECT * FROM bench_select")
		rows.Close()
	}
	elapsed := time.Since(start)

	ops := float64(100) / elapsed.Seconds()
	fmt.Printf("Time: %v\n", elapsed)
	fmt.Printf("Ops/sec: %.2f\n", ops)
	fmt.Println()

	// With WHERE
	fmt.Println("=== SELECT with WHERE ===")
	start = time.Now()
	for i := 0; i < 100; i++ {
		rows, _ := db.Query(ctx, "SELECT * FROM bench_select WHERE age > ?", 50)
		rows.Close()
	}
	elapsed = time.Since(start)

	ops = float64(100) / elapsed.Seconds()
	fmt.Printf("Time: %v\n", elapsed)
	fmt.Printf("Ops/sec: %.2f\n", ops)
	fmt.Println()
}

func runUpdateBenchmark(db *engine.DB, ctx context.Context) {
	fmt.Println("=== UPDATE Benchmark ===")

	// Setup
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_update")
	db.Exec(ctx, "CREATE TABLE bench_update (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	for i := 0; i < flagRows; i++ {
		db.Exec(ctx, "INSERT INTO bench_update (name, age) VALUES (?, ?)", fmt.Sprintf("user-%d", i), i%100)
	}

	// Single row update
	start := time.Now()
	for i := 0; i < 1000; i++ {
		db.Exec(ctx, "UPDATE bench_update SET age = ? WHERE id = ?", i+1000, i)
	}
	elapsed := time.Since(start)

	ops := float64(1000) / elapsed.Seconds()
	fmt.Printf("Single row - Time: %v\n", elapsed)
	fmt.Printf("Ops/sec: %.2f\n", ops)
	fmt.Println()

	// Multi-row update
	start = time.Now()
	db.Exec(ctx, "UPDATE bench_update SET age = ? WHERE age < ?", 999, 50)
	elapsed = time.Since(start)

	fmt.Printf("Multi row (all age < 50) - Time: %v\n", elapsed)
	fmt.Println()
}

func runDeleteBenchmark(db *engine.DB, ctx context.Context) {
	fmt.Println("=== DELETE Benchmark ===")

	// Setup
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_delete")
	db.Exec(ctx, "CREATE TABLE bench_delete (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	for i := 0; i < flagRows; i++ {
		db.Exec(ctx, "INSERT INTO bench_delete (name, age) VALUES (?, ?)", fmt.Sprintf("user-%d", i), i%100)
	}

	// Single row delete
	start := time.Now()
	for i := 0; i < 1000; i++ {
		db.Exec(ctx, "DELETE FROM bench_delete WHERE id = ?", i)
		db.Exec(ctx, "INSERT INTO bench_delete (id, name, age) VALUES (?, ?, ?)", i, fmt.Sprintf("user-%d", i), i%100)
	}
	elapsed := time.Since(start)

	ops := float64(1000) / elapsed.Seconds()
	fmt.Printf("Single row - Time: %v\n", elapsed)
	fmt.Printf("Ops/sec: %.2f\n", ops)
	fmt.Println()

	// Multi-row delete
	start = time.Now()
	db.Exec(ctx, "DELETE FROM bench_delete WHERE age < ?", 50)
	elapsed = time.Since(start)

	fmt.Printf("Multi row (all age < 50) - Time: %v\n", elapsed)
	fmt.Println()
}

func runTransactionBenchmark(db *engine.DB, ctx context.Context) {
	fmt.Println("=== TRANSACTION Benchmark ===")

	// Setup
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_tx")
	db.Exec(ctx, "CREATE TABLE bench_tx (id INTEGER PRIMARY KEY, name TEXT)")

	// Single statement in transaction
	start := time.Now()
	for i := 0; i < 1000; i++ {
		tx, _ := db.Begin(ctx)
		tx.Exec(ctx, "INSERT INTO bench_tx (name) VALUES (?)", fmt.Sprintf("user-%d", i))
		tx.Commit()
	}
	elapsed := time.Since(start)

	ops := float64(1000) / elapsed.Seconds()
	fmt.Printf("Auto-commit - Time: %v\n", elapsed)
	fmt.Printf("Ops/sec: %.2f\n", ops)
	fmt.Println()

	// Batch insert in transaction
	db.Exec(ctx, "DELETE FROM bench_tx")
	start = time.Now()
	tx, _ := db.Begin(ctx)
	for i := 0; i < 1000; i++ {
		tx.Exec(ctx, "INSERT INTO bench_tx (name) VALUES (?)", fmt.Sprintf("user-%d", i))
	}
	tx.Commit()
	elapsed = time.Since(start)

	fmt.Printf("Batch (1000 rows) - Time: %v\n", elapsed)
	fmt.Println()
}
