package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// FullBenchmarkSuite runs comprehensive benchmarks across all database operations
func BenchmarkFullSuite(b *testing.B) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 100 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Run all benchmark categories
	b.Run("Insert", func(b *testing.B) {
		benchmarkInsertSuite(b, db, ctx)
	})
	b.Run("Select", func(b *testing.B) {
		benchmarkSelectSuite(b, db, ctx)
	})
	b.Run("Update", func(b *testing.B) {
		benchmarkUpdateSuite(b, db, ctx)
	})
	b.Run("Delete", func(b *testing.B) {
		benchmarkDeleteSuite(b, db, ctx)
	})
	b.Run("Join", func(b *testing.B) {
		benchmarkJoinSuite(b, db, ctx)
	})
	b.Run("Transaction", func(b *testing.B) {
		benchmarkTransactionSuite(b, db, ctx)
	})
	b.Run("JSON", func(b *testing.B) {
		benchmarkJSONSuite(b, db, ctx)
	})
	b.Run("Index", func(b *testing.B) {
		benchmarkIndexSuite(b, db, ctx)
	})
	b.Run("Aggregation", func(b *testing.B) {
		benchmarkAggregationSuite(b, db, ctx)
	})
	b.Run("Window", func(b *testing.B) {
		benchmarkWindowSuite(b, db, ctx)
	})
	b.Run("CTE", func(b *testing.B) {
		benchmarkCTESuite(b, db, ctx)
	})
	b.Run("Concurrent", func(b *testing.B) {
		benchmarkConcurrentSuite(b, db, ctx)
	})
}

// ============================================================================
// INSERT Benchmarks
// ============================================================================
func benchmarkInsertSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	b.Run("Simple", func(b *testing.B) {
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_insert_simple")
		db.Exec(ctx, "CREATE TABLE bench_insert_simple (id INTEGER PRIMARY KEY, name TEXT)")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Exec(ctx, "INSERT INTO bench_insert_simple (name) VALUES (?)", fmt.Sprintf("user-%d", i))
		}
	})

	b.Run("Batch_100", func(b *testing.B) {
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_insert_batch")
		db.Exec(ctx, "CREATE TABLE bench_insert_batch (id INTEGER PRIMARY KEY, name TEXT)")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, _ := db.Begin(ctx)
			for j := 0; j < 100; j++ {
				tx.Exec(ctx, "INSERT INTO bench_insert_batch (name) VALUES (?)", fmt.Sprintf("user-%d-%d", i, j))
			}
			tx.Commit()
		}
	})

	b.Run("Batch_1000", func(b *testing.B) {
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_insert_batch_1k")
		db.Exec(ctx, "CREATE TABLE bench_insert_batch_1k (id INTEGER PRIMARY KEY, name TEXT)")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, _ := db.Begin(ctx)
			for j := 0; j < 1000; j++ {
				tx.Exec(ctx, "INSERT INTO bench_insert_batch_1k (name) VALUES (?)", fmt.Sprintf("user-%d-%d", i, j))
			}
			tx.Commit()
		}
	})

	b.Run("WithIndex", func(b *testing.B) {
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_insert_idx")
		db.Exec(ctx, "CREATE TABLE bench_insert_idx (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Exec(ctx, "INSERT INTO bench_insert_idx (email) VALUES (?)", fmt.Sprintf("user%d@test.com", i))
		}
	})
}

// ============================================================================
// SELECT Benchmarks
// ============================================================================
func benchmarkSelectSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	// Setup test data
	setupTable := func(name string, rows int) {
		db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
		db.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, salary REAL)", name))
		for i := 0; i < rows; i++ {
			db.Exec(ctx, fmt.Sprintf("INSERT INTO %s (name, age, salary) VALUES (?, ?, ?)", name),
				fmt.Sprintf("user-%d", i), i%100, float64(i)*1000.5)
		}
	}

	b.Run("PointLookup_1K", func(b *testing.B) {
		setupTable("bench_select_point_1k", 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_select_point_1k WHERE id = ?", i%1000+1)
			rows.Close()
		}
	})

	b.Run("PointLookup_10K", func(b *testing.B) {
		setupTable("bench_select_point_10k", 10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_select_point_10k WHERE id = ?", i%10000+1)
			rows.Close()
		}
	})

	b.Run("FullScan_1K", func(b *testing.B) {
		setupTable("bench_select_scan_1k", 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_select_scan_1k")
			count := 0
			for rows.Next() {
				count++
			}
			rows.Close()
		}
	})

	b.Run("FullScan_10K", func(b *testing.B) {
		setupTable("bench_select_scan_10k", 10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_select_scan_10k")
			count := 0
			for rows.Next() {
				count++
			}
			rows.Close()
		}
	})

	b.Run("WhereClause", func(b *testing.B) {
		setupTable("bench_select_where", 10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_select_where WHERE age > ?", 50)
			rows.Close()
		}
	})

	b.Run("OrderBy", func(b *testing.B) {
		setupTable("bench_select_order", 10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_select_order ORDER BY salary DESC")
			rows.Close()
		}
	})

	b.Run("LimitOffset", func(b *testing.B) {
		setupTable("bench_select_limit", 10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_select_limit LIMIT 100 OFFSET 1000")
			rows.Close()
		}
	})
}

// ============================================================================
// UPDATE Benchmarks
// ============================================================================
func benchmarkUpdateSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	setupTable := func(name string, rows int) {
		db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
		db.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", name))
		for i := 0; i < rows; i++ {
			db.Exec(ctx, fmt.Sprintf("INSERT INTO %s (name, age) VALUES (?, ?)", name),
				fmt.Sprintf("user-%d", i), i%100)
		}
	}

	b.Run("SingleRow", func(b *testing.B) {
		setupTable("bench_update_single", 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Exec(ctx, "UPDATE bench_update_single SET age = ? WHERE id = ?", i%100, i%1000+1)
		}
	})

	b.Run("ManyRows", func(b *testing.B) {
		setupTable("bench_update_many", 10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Exec(ctx, "UPDATE bench_update_many SET age = ? WHERE age < ?", 999, 50)
		}
	})

	b.Run("WithTransaction", func(b *testing.B) {
		setupTable("bench_update_tx", 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, _ := db.Begin(ctx)
			for j := 0; j < 100; j++ {
				tx.Exec(ctx, "UPDATE bench_update_tx SET age = ? WHERE id = ?", j, j+1)
			}
			tx.Commit()
		}
	})
}

// ============================================================================
// DELETE Benchmarks
// ============================================================================
func benchmarkDeleteSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	b.Run("SingleRow", func(b *testing.B) {
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_delete_single")
		db.Exec(ctx, "CREATE TABLE bench_delete_single (id INTEGER PRIMARY KEY, name TEXT)")
		for i := 0; i < 10000; i++ {
			db.Exec(ctx, "INSERT INTO bench_delete_single (name) VALUES (?)", fmt.Sprintf("user-%d", i))
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Exec(ctx, "DELETE FROM bench_delete_single WHERE id = ?", i%10000+1)
			db.Exec(ctx, "INSERT INTO bench_delete_single (id, name) VALUES (?, ?)", i%10000+1, fmt.Sprintf("user-%d", i))
		}
	})

	b.Run("ManyRows", func(b *testing.B) {
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_delete_many")
		db.Exec(ctx, "CREATE TABLE bench_delete_many (id INTEGER PRIMARY KEY, age INTEGER)")
		for i := 0; i < 10000; i++ {
			db.Exec(ctx, "INSERT INTO bench_delete_many (age) VALUES (?)", i%100)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Exec(ctx, "DELETE FROM bench_delete_many WHERE age < ?", 50)
			// Re-populate for next iteration
			if i < b.N-1 {
				for j := 0; j < 5000; j++ {
					db.Exec(ctx, "INSERT INTO bench_delete_many (age) VALUES (?)", j%50)
				}
			}
		}
	})
}

// ============================================================================
// JOIN Benchmarks
// ============================================================================
func benchmarkJoinSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	setupJoinTables := func(ordersRows, customersRows int) {
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_orders")
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_customers")
		db.Exec(ctx, "CREATE TABLE bench_customers (id INTEGER PRIMARY KEY, name TEXT)")
		db.Exec(ctx, "CREATE TABLE bench_orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")

		for i := 0; i < customersRows; i++ {
			db.Exec(ctx, "INSERT INTO bench_customers (name) VALUES (?)", fmt.Sprintf("customer-%d", i))
		}
		for i := 0; i < ordersRows; i++ {
			db.Exec(ctx, "INSERT INTO bench_orders (customer_id, amount) VALUES (?, ?)",
				i%customersRows+1, float64(i)*10.5)
		}
	}

	b.Run("InnerJoin_1K", func(b *testing.B) {
		setupJoinTables(1000, 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_orders o JOIN bench_customers c ON o.customer_id = c.id")
			rows.Close()
		}
	})

	b.Run("InnerJoin_10K", func(b *testing.B) {
		setupJoinTables(10000, 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_orders o JOIN bench_customers c ON o.customer_id = c.id")
			rows.Close()
		}
	})

	b.Run("LeftJoin", func(b *testing.B) {
		setupJoinTables(10000, 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_customers c LEFT JOIN bench_orders o ON c.id = o.customer_id")
			rows.Close()
		}
	})

	b.Run("ThreeWayJoin", func(b *testing.B) {
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_a")
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_b")
		db.Exec(ctx, "DROP TABLE IF EXISTS bench_c")
		db.Exec(ctx, "CREATE TABLE bench_a (id INTEGER PRIMARY KEY)")
		db.Exec(ctx, "CREATE TABLE bench_b (id INTEGER PRIMARY KEY, a_id INTEGER)")
		db.Exec(ctx, "CREATE TABLE bench_c (id INTEGER PRIMARY KEY, b_id INTEGER)")

		for i := 0; i < 1000; i++ {
			db.Exec(ctx, "INSERT INTO bench_a VALUES (?)", i)
			db.Exec(ctx, "INSERT INTO bench_b VALUES (?, ?)", i, i)
			db.Exec(ctx, "INSERT INTO bench_c VALUES (?, ?)", i, i)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, `SELECT * FROM bench_a
				JOIN bench_b ON bench_a.id = bench_b.a_id
				JOIN bench_c ON bench_b.id = bench_c.b_id`)
			rows.Close()
		}
	})
}

// ============================================================================
// Transaction Benchmarks
// ============================================================================
func benchmarkTransactionSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_tx")
	db.Exec(ctx, "CREATE TABLE bench_tx (id INTEGER PRIMARY KEY, name TEXT)")

	b.Run("SingleStatement", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx, _ := db.Begin(ctx)
			tx.Exec(ctx, "INSERT INTO bench_tx (name) VALUES (?)", fmt.Sprintf("user-%d", i))
			tx.Commit()
		}
	})

	b.Run("Batch_100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx, _ := db.Begin(ctx)
			for j := 0; j < 100; j++ {
				tx.Exec(ctx, "INSERT INTO bench_tx (name) VALUES (?)", fmt.Sprintf("user-%d-%d", i, j))
			}
			tx.Commit()
		}
	})

	b.Run("Batch_1000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx, _ := db.Begin(ctx)
			for j := 0; j < 1000; j++ {
				tx.Exec(ctx, "INSERT INTO bench_tx (name) VALUES (?)", fmt.Sprintf("user-%d-%d", i, j))
			}
			tx.Commit()
		}
	})

	b.Run("Rollback", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx, _ := db.Begin(ctx)
			for j := 0; j < 100; j++ {
				tx.Exec(ctx, "INSERT INTO bench_tx (name) VALUES (?)", fmt.Sprintf("temp-%d-%d", i, j))
			}
			tx.Rollback()
		}
	})
}

// ============================================================================
// JSON Benchmarks
// ============================================================================
func benchmarkJSONSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_json")
	db.Exec(ctx, "CREATE TABLE bench_json (id INTEGER PRIMARY KEY, data JSON)")

	// Insert test data
	for i := 0; i < 10000; i++ {
		jsonData := fmt.Sprintf(`{"id": %d, "name": "user-%d", "active": true, "tags": ["tag1", "tag2"]}`, i, i)
		db.Exec(ctx, "INSERT INTO bench_json (data) VALUES (?)", jsonData)
	}

	b.Run("Insert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			jsonData := fmt.Sprintf(`{"id": %d, "name": "user-%d"}`, i, i)
			db.Exec(ctx, "INSERT INTO bench_json (data) VALUES (?)", jsonData)
		}
	})

	b.Run("Extract", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT JSON_EXTRACT(data, '$.name') FROM bench_json WHERE id = ?", i%10000+1)
			rows.Close()
		}
	})

	b.Run("Where", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_json WHERE JSON_EXTRACT(data, '$.active') = true")
			rows.Close()
		}
	})
}

// ============================================================================
// Index Benchmarks
// ============================================================================
func benchmarkIndexSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_index_no")
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_index_yes")
	db.Exec(ctx, "CREATE TABLE bench_index_no (id INTEGER PRIMARY KEY, email TEXT)")
	db.Exec(ctx, "CREATE TABLE bench_index_yes (id INTEGER PRIMARY KEY, email TEXT)")
	db.Exec(ctx, "CREATE INDEX idx_email ON bench_index_yes(email)")

	// Insert same data to both tables
	for i := 0; i < 10000; i++ {
		email := fmt.Sprintf("user%d@example.com", i)
		db.Exec(ctx, "INSERT INTO bench_index_no (email) VALUES (?)", email)
		db.Exec(ctx, "INSERT INTO bench_index_yes (email) VALUES (?)", email)
	}

	b.Run("NoIndex", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_index_no WHERE email = ?", fmt.Sprintf("user%d@example.com", i%10000))
			rows.Close()
		}
	})

	b.Run("WithIndex", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT * FROM bench_index_yes WHERE email = ?", fmt.Sprintf("user%d@example.com", i%10000))
			rows.Close()
		}
	})
}

// ============================================================================
// Aggregation Benchmarks
// ============================================================================
func benchmarkAggregationSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_agg")
	db.Exec(ctx, "CREATE TABLE bench_agg (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")

	for i := 0; i < 10000; i++ {
		dept := fmt.Sprintf("dept-%d", i%10)
		db.Exec(ctx, "INSERT INTO bench_agg (dept, salary) VALUES (?, ?)", dept, i*100)
	}

	b.Run("Count", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT COUNT(*) FROM bench_agg")
			rows.Close()
		}
	})

	b.Run("Sum", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT SUM(salary) FROM bench_agg")
			rows.Close()
		}
	})

	b.Run("Avg", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT AVG(salary) FROM bench_agg")
			rows.Close()
		}
	})

	b.Run("GroupBy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT dept, COUNT(*), AVG(salary) FROM bench_agg GROUP BY dept")
			rows.Close()
		}
	})

	b.Run("Having", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT dept FROM bench_agg GROUP BY dept HAVING COUNT(*) > 500")
			rows.Close()
		}
	})
}

// ============================================================================
// Window Function Benchmarks
// ============================================================================
func benchmarkWindowSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_window")
	db.Exec(ctx, "CREATE TABLE bench_window (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")

	for i := 0; i < 10000; i++ {
		dept := fmt.Sprintf("dept-%d", i%10)
		db.Exec(ctx, "INSERT INTO bench_window (dept, salary) VALUES (?, ?)", dept, i*100)
	}

	b.Run("RowNumber", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT id, ROW_NUMBER() OVER (ORDER BY salary) FROM bench_window")
			rows.Close()
		}
	})

	b.Run("Rank", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT id, RANK() OVER (ORDER BY salary) FROM bench_window")
			rows.Close()
		}
	})

	b.Run("Partition", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) FROM bench_window")
			rows.Close()
		}
	})

	b.Run("SumOver", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, "SELECT id, SUM(salary) OVER (PARTITION BY dept) FROM bench_window")
			rows.Close()
		}
	})
}

// ============================================================================
// CTE Benchmarks
// ============================================================================
func benchmarkCTESuite(b *testing.B, db *engine.DB, ctx context.Context) {
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_cte")
	db.Exec(ctx, "CREATE TABLE bench_cte (id INTEGER PRIMARY KEY, manager_id INTEGER, name TEXT)")

	// Insert hierarchy data
	for i := 0; i < 1000; i++ {
		managerID := 0
		if i > 0 {
			managerID = (i-1)/10 + 1
		}
		db.Exec(ctx, "INSERT INTO bench_cte (manager_id, name) VALUES (?, ?)", managerID, fmt.Sprintf("emp-%d", i))
	}

	b.Run("SimpleCTE", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, `
				WITH managers AS (SELECT * FROM bench_cte WHERE manager_id = 0)
				SELECT * FROM managers`)
			rows.Close()
		}
	})

	b.Run("RecursiveCTE", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := db.Query(ctx, `
				WITH RECURSIVE subordinates AS (
					SELECT * FROM bench_cte WHERE id = 1
					UNION ALL
					SELECT e.* FROM bench_cte e JOIN subordinates s ON e.manager_id = s.id
				)
				SELECT * FROM subordinates`)
			rows.Close()
		}
	})
}

// ============================================================================
// Concurrent Benchmarks
// ============================================================================
func benchmarkConcurrentSuite(b *testing.B, db *engine.DB, ctx context.Context) {
	db.Exec(ctx, "DROP TABLE IF EXISTS bench_concurrent")
	db.Exec(ctx, "CREATE TABLE bench_concurrent (id INTEGER PRIMARY KEY, counter INTEGER)")
	db.Exec(ctx, "INSERT INTO bench_concurrent VALUES (1, 0)")

	b.Run("ParallelRead_10", func(b *testing.B) {
		b.SetParallelism(10)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				rows, _ := db.Query(ctx, "SELECT * FROM bench_concurrent")
				rows.Close()
			}
		})
	})

	b.Run("ParallelRead_100", func(b *testing.B) {
		b.SetParallelism(100)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				rows, _ := db.Query(ctx, "SELECT * FROM bench_concurrent")
				rows.Close()
			}
		})
	})

	b.Run("ParallelWrite_10", func(b *testing.B) {
		b.SetParallelism(10)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				db.Exec(ctx, "INSERT INTO bench_concurrent (counter) VALUES (?)", i)
				i++
			}
		})
	})
}

// ============================================================================
// Standalone Benchmark Functions
// ============================================================================

func BenchmarkFullInsert1K(b *testing.B) {
	benchmarkFullInsert(b, 1000)
}

func BenchmarkFullInsert10K(b *testing.B) {
	benchmarkFullInsert(b, 10000)
}

func BenchmarkFullInsert50K(b *testing.B) {
	benchmarkFullInsert(b, 50000)
}

func benchmarkFullInsert(b *testing.B, rows int) {
	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true})
	defer db.Close()

	db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS bench_insert_%d", rows))
	db.Exec(ctx, fmt.Sprintf("CREATE TABLE bench_insert_%d (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", rows))

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin(ctx)
		for j := 0; j < rows; j++ {
			tx.Exec(ctx, fmt.Sprintf("INSERT INTO bench_insert_%d (name, age) VALUES (?, ?)", rows),
				fmt.Sprintf("user-%d", j), j%100)
		}
		tx.Commit()
	}
	elapsed := time.Since(start)

	opsPerSec := float64(rows*b.N) / elapsed.Seconds()
	b.ReportMetric(opsPerSec, "ops/sec")
}

func BenchmarkFullSelect1K(b *testing.B) {
	benchmarkFullSelect(b, 1000)
}

func BenchmarkFullSelect10K(b *testing.B) {
	benchmarkFullSelect(b, 10000)
}

func BenchmarkFullSelect50K(b *testing.B) {
	benchmarkFullSelect(b, 50000)
}

func benchmarkFullSelect(b *testing.B, rows int) {
	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true})
	defer db.Close()

	db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS bench_select_%d", rows))
	db.Exec(ctx, fmt.Sprintf("CREATE TABLE bench_select_%d (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", rows))

	for i := 0; i < rows; i++ {
		db.Exec(ctx, fmt.Sprintf("INSERT INTO bench_select_%d (name, age) VALUES (?, ?)", rows),
			fmt.Sprintf("user-%d", i), i%100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := db.Query(ctx, fmt.Sprintf("SELECT * FROM bench_select_%d", rows))
		count := 0
		for result.Next() {
			count++
		}
		result.Close()
	}
}

// BenchmarkComplexQuery tests complex multi-operation queries
func BenchmarkComplexQuery(b *testing.B) {
	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true})
	defer db.Close()

	// Setup schema
	db.Exec(ctx, `
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			product_id INTEGER,
			quantity INTEGER,
			price REAL,
			order_date DATE
		)
	`)
	db.Exec(ctx, `
		CREATE TABLE customers (
			id INTEGER PRIMARY KEY,
			name TEXT,
			region TEXT
		)
	`)
	db.Exec(ctx, `
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT,
			category TEXT
		)
	`)

	// Insert test data
	for i := 0; i < 1000; i++ {
		db.Exec(ctx, "INSERT INTO customers (name, region) VALUES (?, ?)",
			fmt.Sprintf("Customer-%d", i), fmt.Sprintf("Region-%d", i%10))
	}
	for i := 0; i < 100; i++ {
		db.Exec(ctx, "INSERT INTO products (name, category) VALUES (?, ?)",
			fmt.Sprintf("Product-%d", i), fmt.Sprintf("Category-%d", i%5))
	}
	for i := 0; i < 10000; i++ {
		db.Exec(ctx, "INSERT INTO orders (customer_id, product_id, quantity, price, order_date) VALUES (?, ?, ?, ?, ?)",
			i%1000+1, i%100+1, i%10+1, float64(i)*10.5, "2024-01-01")
	}

	// Complex query with JOINs, GROUP BY, and HAVING
	query := `
		SELECT
			c.region,
			p.category,
			COUNT(*) as order_count,
			SUM(o.quantity * o.price) as total_revenue,
			AVG(o.quantity) as avg_quantity
		FROM orders o
		JOIN customers c ON o.customer_id = c.id
		JOIN products p ON o.product_id = p.id
		GROUP BY c.region, p.category
		HAVING total_revenue > 10000
		ORDER BY total_revenue DESC
	`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, query)
		for rows.Next() {
		}
		rows.Close()
	}
}
