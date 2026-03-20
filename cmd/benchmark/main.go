package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// BenchmarkResult represents a single benchmark result
type BenchmarkResult struct {
	Name        string        `json:"name"`
	Category    string        `json:"category"`
	Operations  int           `json:"operations"`
	Duration    time.Duration `json:"duration"`
	OpsPerSec   float64       `json:"ops_per_sec"`
	LatencyAvg  time.Duration `json:"latency_avg"`
	LatencyMin  time.Duration `json:"latency_min"`
	LatencyMax  time.Duration `json:"latency_max"`
	MemoryUsage uint64        `json:"memory_usage"`
	Success     bool          `json:"success"`
	Error       string        `json:"error,omitempty"`
}

// BenchmarkSuite contains all benchmark results
type BenchmarkSuite struct {
	Timestamp   time.Time         `json:"timestamp"`
	GoVersion   string            `json:"go_version"`
	OS          string            `json:"os"`
	Arch        string            `json:"arch"`
	CPUs        int               `json:"cpus"`
	Results     []BenchmarkResult `json:"results"`
	TotalTests  int               `json:"total_tests"`
	PassedTests int               `json:"passed_tests"`
	FailedTests int               `json:"failed_tests"`
}

var suite BenchmarkSuite

// Configuration for realistic benchmark scenarios - OPTIMIZED for reasonable runtime
const (
	// Data sizes - balanced for realistic yet fast testing
	SmallDataSet  = 500   // 500 rows
	MediumDataSet = 2000  // 2K rows
	LargeDataSet  = 10000 // 10K rows

	// Benchmark iterations
	IterationsFast     = 100 // For very fast operations (point lookups)
	IterationsNormal   = 50  // For normal operations
	IterationsSlow     = 20  // For slower operations (writes, complex queries)
	IterationsVerySlow = 10  // For very slow operations (JOINs, VACUUM)
)

func main() {
	log.Println("🚀 CobaltDB Comprehensive Benchmark Suite")
	log.Println("=========================================")
	log.Println("📊 Realistic data sizes: 500-10K rows")
	log.Println("⏱️  Measuring actual performance characteristics")
	log.Println("")

	suite = BenchmarkSuite{
		Timestamp: time.Now(),
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		CPUs:      runtime.NumCPU(),
		Results:   []BenchmarkResult{},
	}

	// Run all benchmarks
	runDDLBenchmarks()
	runDMLBenchmarks()
	runQueryBenchmarks()
	runTransactionBenchmarks()
	runJSONBenchmarks()
	runAdvancedFeatureBenchmarks()
	runConcurrencyBenchmarks()
	runServerBenchmarks()

	// Generate reports
	generateMarkdownReport()
	generateHTMLReport()
	generateJSONReport()

	// Print summary
	printSummary()
}

func runDDLBenchmarks() {
	log.Println("\n📊 Running DDL Benchmarks...")

	ctx := context.Background()

	// CREATE TABLE benchmark
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	tableCounter := 0
	result := benchmarkFunc("CREATE TABLE", "DDL", IterationsNormal, func() error {
		tableCounter++
		_, err := db.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE users_%d (
				id INTEGER PRIMARY KEY,
				username TEXT NOT NULL UNIQUE,
				email TEXT NOT NULL,
				created_at INTEGER,
				active BOOLEAN DEFAULT 1
			)`, tableCounter))
		return err
	})
	suite.Results = append(suite.Results, result)
	db.Close()

	// CREATE INDEX benchmark
	db, _ = engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	db.Exec(ctx, "CREATE TABLE idx_test (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, city TEXT)")
	for i := 0; i < MediumDataSet; i++ {
		db.Exec(ctx, "INSERT INTO idx_test VALUES (?, ?, ?, ?, ?)",
			i, fmt.Sprintf("user_%d", i), fmt.Sprintf("user%d@example.com", i), 18+(i%50), fmt.Sprintf("City_%d", i%100))
	}

	indexCounter := 0
	result = benchmarkFunc("CREATE INDEX", "DDL", IterationsSlow, func() error {
		indexCounter++
		_, err := db.Exec(ctx, fmt.Sprintf("CREATE INDEX idx_age_%d ON idx_test (age)", indexCounter))
		return err
	})
	suite.Results = append(suite.Results, result)
	db.Close()

	// CREATE VIEW benchmark
	db, _ = engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	db.Exec(ctx, "CREATE TABLE view_base (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
	for i := 0; i < SmallDataSet; i++ {
		db.Exec(ctx, "INSERT INTO view_base VALUES (?, ?, ?)", i, fmt.Sprintf("name_%d", i), i*10)
	}

	viewCounter := 0
	result = benchmarkFunc("CREATE VIEW", "DDL", IterationsNormal, func() error {
		viewCounter++
		_, err := db.Exec(ctx, fmt.Sprintf(
			"CREATE VIEW v_active_%d AS SELECT * FROM view_base WHERE value > 100", viewCounter))
		return err
	})
	suite.Results = append(suite.Results, result)
	db.Close()
}

func runDMLBenchmarks() {
	log.Println("\n📊 Running DML Benchmarks...")

	ctx := context.Background()

	// INSERT Single benchmark
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	db.Exec(ctx, `CREATE TABLE insert_test (
		id INTEGER PRIMARY KEY,
		username TEXT,
		email TEXT,
		age INTEGER,
		score REAL,
		data TEXT
	)`)

	insertID := 0
	result := benchmarkFunc("INSERT Single", "DML", IterationsNormal, func() error {
		insertID++
		_, err := db.Exec(ctx,
			"INSERT INTO insert_test VALUES (?, ?, ?, ?, ?, ?)",
			insertID,
			fmt.Sprintf("user_%d", insertID),
			fmt.Sprintf("user%d@example.com", insertID),
			20+(insertID%50),
			float64(insertID%1000)/10.0,
			strings.Repeat("x", 100),
		)
		return err
	})
	suite.Results = append(suite.Results, result)
	db.Close()

	// INSERT Batch benchmark
	db, _ = engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	db.Exec(ctx, `CREATE TABLE batch_test (id INTEGER PRIMARY KEY, data TEXT)`)

	batchID := 0
	result = benchmarkFunc("INSERT Batch (50 rows)", "DML", IterationsSlow, func() error {
		batchID++
		for i := 0; i < 50; i++ {
			_, err := db.Exec(ctx, "INSERT INTO batch_test VALUES (?, ?)",
				batchID*50+i, strings.Repeat("data", 50))
			if err != nil {
				return err
			}
		}
		return nil
	})
	suite.Results = append(suite.Results, result)
	db.Close()

	// UPDATE benchmark
	db, _ = engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	db.Exec(ctx, `CREATE TABLE update_test (
		id INTEGER PRIMARY KEY,
		name TEXT,
		counter INTEGER,
		updated_at INTEGER
	)`)
	db.Exec(ctx, "CREATE INDEX idx_update_counter ON update_test (counter)")

	for i := 0; i < MediumDataSet; i++ {
		db.Exec(ctx, "INSERT INTO update_test VALUES (?, ?, ?, ?)",
			i, fmt.Sprintf("name_%d", i), i%100, time.Now().Unix())
	}

	updateCounter := 0
	result = benchmarkFunc("UPDATE with Index", "DML", IterationsSlow, func() error {
		updateCounter++
		_, err := db.Exec(ctx,
			"UPDATE update_test SET counter = counter + 1, updated_at = ? WHERE id = ?",
			time.Now().Unix(), updateCounter%MediumDataSet)
		return err
	})
	suite.Results = append(suite.Results, result)
	db.Close()

	// DELETE benchmark
	db, _ = engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	db.Exec(ctx, "CREATE TABLE delete_test (id INTEGER PRIMARY KEY, data TEXT)")
	db.Exec(ctx, "CREATE INDEX idx_delete_id ON delete_test (id)")

	for i := 0; i < MediumDataSet; i++ {
		db.Exec(ctx, "INSERT INTO delete_test VALUES (?, ?)", i, fmt.Sprintf("data_%d", i))
	}

	deleteCounter := MediumDataSet
	result = benchmarkFunc("DELETE with Index", "DML", IterationsSlow, func() error {
		deleteCounter++
		db.Exec(ctx, "INSERT INTO delete_test VALUES (?, ?)", deleteCounter, "temp_data")
		_, err := db.Exec(ctx, "DELETE FROM delete_test WHERE id = ?", deleteCounter)
		return err
	})
	suite.Results = append(suite.Results, result)
	db.Close()
}

func runQueryBenchmarks() {
	log.Println("\n📊 Running Query Benchmarks...")

	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	defer db.Close()

	// Setup tables
	db.Exec(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT,
		age INTEGER,
		city TEXT,
		score REAL
	)`)
	db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		user_id INTEGER,
		amount REAL,
		status TEXT,
		created_at INTEGER
	)`)

	// Create indexes
	db.Exec(ctx, "CREATE INDEX idx_users_city ON users (city)")
	db.Exec(ctx, "CREATE INDEX idx_users_age ON users (age)")
	db.Exec(ctx, "CREATE INDEX idx_orders_user ON orders (user_id)")
	db.Exec(ctx, "CREATE INDEX idx_orders_status ON orders (status)")

	// Insert test data
	cities := []string{"Istanbul", "Ankara", "Izmir", "Bursa", "Antalya", "Adana", "Konya"}
	log.Println("  Populating users table (10K rows)...")
	for i := 0; i < LargeDataSet; i++ {
		db.Exec(ctx, "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?)",
			i,
			fmt.Sprintf("User_%d", i),
			fmt.Sprintf("user%d@example.com", i),
			18+(i%60),
			cities[i%len(cities)],
			float64(i%10000)/100.0)
	}

	// Insert orders (5 per user = 50K orders)
	log.Println("  Populating orders table (50K rows)...")
	statuses := []string{"pending", "completed", "cancelled", "processing"}
	for i := 0; i < LargeDataSet*5; i++ {
		db.Exec(ctx, "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
			i,
			i%LargeDataSet,
			float64(i%1000)+0.99,
			statuses[i%len(statuses)],
			time.Now().Unix()-int64(i%86400))
	}

	// Simple SELECT by PK
	result := benchmarkFunc("SELECT Point Lookup (PK)", "Query", IterationsFast, func() error {
		rows, err := db.Query(ctx, "SELECT * FROM users WHERE id = ?", time.Now().UnixNano()%LargeDataSet)
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// SELECT with indexed filter
	result = benchmarkFunc("SELECT with WHERE (Index)", "Query", IterationsNormal, func() error {
		rows, err := db.Query(ctx, "SELECT * FROM users WHERE city = ? LIMIT 50", cities[time.Now().Second()%len(cities)])
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// SELECT with range scan
	result = benchmarkFunc("SELECT Range Scan", "Query", IterationsNormal, func() error {
		rows, err := db.Query(ctx, "SELECT * FROM users WHERE age BETWEEN ? AND ? LIMIT 50", 25, 35)
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// SELECT with ORDER BY (indexed)
	result = benchmarkFunc("SELECT with ORDER BY (Index)", "Query", IterationsNormal, func() error {
		rows, err := db.Query(ctx, "SELECT * FROM users ORDER BY age DESC LIMIT 50")
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// SELECT with ORDER BY (no index - filesort)
	result = benchmarkFunc("SELECT with ORDER BY (No Index)", "Query", IterationsSlow, func() error {
		rows, err := db.Query(ctx, "SELECT * FROM users ORDER BY score DESC LIMIT 50")
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// Simple JOIN with index
	result = benchmarkFunc("SELECT Simple JOIN (Indexed)", "Query", IterationsNormal, func() error {
		rows, err := db.Query(ctx, `
			SELECT u.name, o.amount
			FROM users u
			JOIN orders o ON u.id = o.user_id
			WHERE u.id = ?
			LIMIT 5`, time.Now().UnixNano()%LargeDataSet)
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// Complex JOIN with aggregation
	result = benchmarkFunc("SELECT Complex JOIN + Aggregate", "Query", IterationsVerySlow, func() error {
		rows, err := db.Query(ctx, `
			SELECT u.city, COUNT(o.id) as order_count, SUM(o.amount) as total
			FROM users u
			JOIN orders o ON u.id = o.user_id
			WHERE o.status = 'completed'
			GROUP BY u.city
			LIMIT 10`)
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// SELECT with Aggregation only
	result = benchmarkFunc("SELECT Aggregation Only", "Query", IterationsNormal, func() error {
		rows, err := db.Query(ctx, "SELECT COUNT(*), AVG(age), MAX(score) FROM users")
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// Full table scan
	result = benchmarkFunc("SELECT Full Table Scan", "Query", IterationsVerySlow, func() error {
		rows, err := db.Query(ctx, "SELECT * FROM users WHERE email LIKE '%@example.com%'")
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)
}

func runTransactionBenchmarks() {
	log.Println("\n📊 Running Transaction Benchmarks...")

	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE txn_test (id INTEGER PRIMARY KEY, value INTEGER, name TEXT)")
	db.Exec(ctx, "CREATE INDEX idx_txn_value ON txn_test (value)")

	for i := 0; i < MediumDataSet; i++ {
		db.Exec(ctx, "INSERT INTO txn_test VALUES (?, ?, ?)", i, i, fmt.Sprintf("name_%d", i))
	}

	// Single transaction with multiple operations
	result := benchmarkFunc("Transaction (10 ops)", "Transaction", IterationsSlow, func() error {
		tx, err := db.Begin(ctx)
		if err != nil {
			return err
		}
		startID := time.Now().UnixNano() % MediumDataSet
		for i := 0; i < 10; i++ {
			_, err := tx.Exec(ctx, "UPDATE txn_test SET value = value + 1 WHERE id = ?", (startID+int64(i))%MediumDataSet)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
		return tx.Commit()
	})
	suite.Results = append(suite.Results, result)

	// Read-only transaction
	result = benchmarkFunc("Read-Only Transaction", "Transaction", IterationsNormal, func() error {
		tx, err := db.Begin(ctx)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, "SELECT * FROM txn_test WHERE value > ? LIMIT 50", time.Now().Unix()%100)
		if err != nil {
			tx.Rollback()
			return err
		}
		rows.Close()
		return tx.Commit()
	})
	suite.Results = append(suite.Results, result)
}

func runJSONBenchmarks() {
	log.Println("\n📊 Running JSON Benchmarks...")

	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE json_test (id INTEGER PRIMARY KEY, data JSON)")

	jsonTemplate := `{"id": %d, "user": {"name": "User_%d", "email": "user%d@example.com", "active": true}, "metadata": {"score": %.2f, "tags": ["tag1", "tag2", "tag3"], "created": "%s"}}`

	log.Println("  Populating JSON data (5K rows)...")
	for i := 0; i < MediumDataSet; i++ {
		jsonData := fmt.Sprintf(jsonTemplate, i, i, i, float64(i%1000)/10.0, time.Now().Format(time.RFC3339))
		db.Exec(ctx, "INSERT INTO json_test VALUES (?, ?)", i, jsonData)
	}

	// JSON Insert
	insertID := MediumDataSet
	result := benchmarkFunc("JSON Insert", "JSON", IterationsNormal, func() error {
		insertID++
		jsonData := fmt.Sprintf(jsonTemplate, insertID, insertID, insertID, 50.0, time.Now().Format(time.RFC3339))
		_, err := db.Exec(ctx, "INSERT INTO json_test VALUES (?, ?)", insertID, jsonData)
		return err
	})
	suite.Results = append(suite.Results, result)

	// JSON_EXTRACT
	result = benchmarkFunc("JSON_EXTRACT", "JSON", IterationsNormal, func() error {
		rows, err := db.Query(ctx,
			"SELECT JSON_EXTRACT(data, '$.user.name') FROM json_test WHERE id = ?",
			time.Now().UnixNano()%MediumDataSet)
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// JSON_EXTRACT nested
	result = benchmarkFunc("JSON_EXTRACT Nested", "JSON", IterationsNormal, func() error {
		rows, err := db.Query(ctx,
			"SELECT JSON_EXTRACT(data, '$.metadata.score') FROM json_test WHERE id = ?",
			time.Now().UnixNano()%MediumDataSet)
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)
}

func runAdvancedFeatureBenchmarks() {
	log.Println("\n📊 Running Advanced Feature Benchmarks...")

	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE advanced_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, category TEXT)")
	db.Exec(ctx, "CREATE INDEX idx_advanced_category ON advanced_test (category)")

	log.Println("  Populating advanced test data (5K rows)...")
	categories := []string{"A", "B", "C", "D", "E"}
	for i := 0; i < MediumDataSet; i++ {
		db.Exec(ctx, "INSERT INTO advanced_test VALUES (?, ?, ?, ?)",
			i, fmt.Sprintf("name_%d", i), i%1000, categories[i%len(categories)])
	}

	// VACUUM benchmark
	result := benchmarkFunc("VACUUM", "Maintenance", IterationsVerySlow, func() error {
		_, err := db.Exec(ctx, "VACUUM")
		return err
	})
	suite.Results = append(suite.Results, result)

	// ANALYZE benchmark
	result = benchmarkFunc("ANALYZE", "Maintenance", IterationsSlow, func() error {
		_, err := db.Exec(ctx, "ANALYZE advanced_test")
		return err
	})
	suite.Results = append(suite.Results, result)

	// CREATE MATERIALIZED VIEW
	mvCounter := 0
	result = benchmarkFunc("CREATE MATERIALIZED VIEW", "Advanced", IterationsSlow, func() error {
		mvCounter++
		_, err := db.Exec(ctx, fmt.Sprintf(
			"CREATE MATERIALIZED VIEW mv_%d AS SELECT category, COUNT(*) as cnt, AVG(value) as avg_val FROM advanced_test GROUP BY category",
			mvCounter))
		return err
	})
	suite.Results = append(suite.Results, result)

	// REFRESH MATERIALIZED VIEW
	db.Exec(ctx, "CREATE MATERIALIZED VIEW mv_refresh AS SELECT * FROM advanced_test WHERE value > 500")
	result = benchmarkFunc("REFRESH MATERIALIZED VIEW", "Advanced", IterationsSlow, func() error {
		_, err := db.Exec(ctx, "REFRESH MATERIALIZED VIEW mv_refresh")
		return err
	})
	suite.Results = append(suite.Results, result)

	// CTE Query
	result = benchmarkFunc("CTE Query", "Advanced", IterationsNormal, func() error {
		rows, err := db.Query(ctx, `
			WITH category_stats AS (
				SELECT category, COUNT(*) as cnt, AVG(value) as avg_val
				FROM advanced_test
				GROUP BY category
			)
			SELECT * FROM category_stats WHERE cnt > 100`)
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// Window Functions
	result = benchmarkFunc("Window Functions", "Advanced", IterationsNormal, func() error {
		rows, err := db.Query(ctx, `
			SELECT id, name, category, value,
				ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rn,
				SUM(value) OVER (PARTITION BY category) as cat_total,
				AVG(value) OVER (PARTITION BY category) as cat_avg
			FROM advanced_test
			WHERE id < 1000`)
		if err != nil {
			return err
		}
		rows.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)
}

func runConcurrencyBenchmarks() {
	log.Println("\n📊 Running Concurrency Benchmarks...")

	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 4096})
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, counter INTEGER, data TEXT)")
	db.Exec(ctx, "CREATE INDEX idx_concurrent_id ON concurrent_test (id)")

	for i := 0; i < SmallDataSet; i++ {
		db.Exec(ctx, "INSERT INTO concurrent_test VALUES (?, ?, ?)", i, 0, fmt.Sprintf("data_%d", i))
	}

	// Concurrent reads
	result := benchmarkFunc("Concurrent Reads (10 goroutines)", "Concurrency", IterationsSlow, func() error {
		var wg sync.WaitGroup
		errChan := make(chan error, 10)

		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					rows, err := db.Query(ctx, "SELECT * FROM concurrent_test WHERE id = ?", (goroutineID*50+i)%SmallDataSet)
					if err != nil {
						errChan <- err
						return
					}
					rows.Close()
				}
			}(g)
		}

		wg.Wait()
		close(errChan)

		for err := range errChan {
			if err != nil {
				return err
			}
		}
		return nil
	})
	suite.Results = append(suite.Results, result)

	// Concurrent writes
	result = benchmarkFunc("Concurrent Writes (5 goroutines)", "Concurrency", IterationsSlow, func() error {
		var wg sync.WaitGroup
		errChan := make(chan error, 5)

		for g := 0; g < 5; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					_, err := db.Exec(ctx,
						"UPDATE concurrent_test SET counter = counter + 1 WHERE id = ?",
						(goroutineID*20+i)%SmallDataSet)
					if err != nil {
						errChan <- err
						return
					}
				}
			}(g)
		}

		wg.Wait()
		close(errChan)

		for err := range errChan {
			if err != nil {
				return err
			}
		}
		return nil
	})
	suite.Results = append(suite.Results, result)
}

func runServerBenchmarks() {
	log.Println("\n📊 Running Server Mode Benchmarks...")

	// Server startup benchmark
	result := benchmarkFunc("Server Startup", "Server", IterationsNormal, func() error {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
		if err != nil {
			return err
		}
		db.Close()
		return nil
	})
	suite.Results = append(suite.Results, result)

	// Connection benchmark with file-based database
	tempDir := os.TempDir()
	result = benchmarkFunc("Database Open/Close (File)", "Server", IterationsNormal, func() error {
		dbPath := filepath.Join(tempDir, fmt.Sprintf("cobalt_test_%d.db", time.Now().UnixNano()))
		db, err := engine.Open(dbPath, &engine.Options{InMemory: false, CacheSize: 1024})
		if err != nil {
			return err
		}
		db.Close()
		os.Remove(dbPath)
		return nil
	})
	suite.Results = append(suite.Results, result)
}

func benchmarkFunc(name, category string, operations int, fn func() error) BenchmarkResult {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	latencies := make([]time.Duration, 0, operations)
	start := time.Now()
	var minLatency, maxLatency time.Duration
	success := true
	var errMsg string

	for i := 0; i < operations; i++ {
		opStart := time.Now()
		err := fn()
		opDuration := time.Since(opStart)

		if err != nil {
			success = false
			errMsg = err.Error()
			break
		}

		latencies = append(latencies, opDuration)
		if minLatency == 0 || opDuration < minLatency {
			minLatency = opDuration
		}
		if opDuration > maxLatency {
			maxLatency = opDuration
		}
	}

	duration := time.Since(start)
	runtime.ReadMemStats(&m2)

	var avgLatency time.Duration
	if len(latencies) > 0 {
		var total time.Duration
		for _, l := range latencies {
			total += l
		}
		avgLatency = total / time.Duration(len(latencies))
	}

	// Calculate ops/sec
	var opsPerSec float64
	if duration > 0 {
		if success {
			opsPerSec = float64(operations) / duration.Seconds()
		} else {
			opsPerSec = float64(len(latencies)) / duration.Seconds()
		}
	} else {
		if avgLatency > 0 {
			opsPerSec = 1.0 / avgLatency.Seconds()
		} else if len(latencies) > 0 && minLatency > 0 {
			opsPerSec = 1.0 / minLatency.Seconds()
		} else {
			opsPerSec = 0
		}
	}

	result := BenchmarkResult{
		Name:        name,
		Category:    category,
		Operations:  operations,
		Duration:    duration,
		OpsPerSec:   opsPerSec,
		LatencyAvg:  avgLatency,
		LatencyMin:  minLatency,
		LatencyMax:  maxLatency,
		MemoryUsage: m2.TotalAlloc - m1.TotalAlloc,
		Success:     success,
		Error:       errMsg,
	}

	suite.TotalTests++
	if success {
		suite.PassedTests++
	} else {
		suite.FailedTests++
	}

	log.Printf("  %s: %.0f ops/sec (avg: %v)", name, opsPerSec, avgLatency)

	return result
}

func generateMarkdownReport() {
	filename := "benchmark_report.md"
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create markdown report: %v", err)
		return
	}
	defer f.Close()

	fmt.Fprintf(f, "# CobaltDB Benchmark Report\n\n")
	fmt.Fprintf(f, "**Generated:** %s\n\n", suite.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(f, "## System Information\n\n")
	fmt.Fprintf(f, "- **Go Version:** %s\n", suite.GoVersion)
	fmt.Fprintf(f, "- **OS:** %s\n", suite.OS)
	fmt.Fprintf(f, "- **Architecture:** %s\n", suite.Arch)
	fmt.Fprintf(f, "- **CPUs:** %d\n\n", suite.CPUs)

	fmt.Fprintf(f, "## Summary\n\n")
	fmt.Fprintf(f, "| Metric | Value |\n")
	fmt.Fprintf(f, "|--------|-------|\n")
	fmt.Fprintf(f, "| Total Tests | %d |\n", suite.TotalTests)
	fmt.Fprintf(f, "| Passed | %d |\n", suite.PassedTests)
	fmt.Fprintf(f, "| Failed | %d |\n", suite.FailedTests)
	fmt.Fprintf(f, "| Success Rate | %.1f%% |\n\n", float64(suite.PassedTests)/float64(suite.TotalTests)*100)

	// Group by category with consistent ordering
	categoryOrder := []string{"DDL", "DML", "Query", "Transaction", "JSON", "Advanced", "Maintenance", "Concurrency", "Server"}
	categories := make(map[string][]BenchmarkResult)
	for _, r := range suite.Results {
		categories[r.Category] = append(categories[r.Category], r)
	}

	for _, cat := range categoryOrder {
		results, ok := categories[cat]
		if !ok {
			continue
		}
		fmt.Fprintf(f, "## %s\n\n", cat)
		fmt.Fprintf(f, "| Operation | Ops/Sec | Avg Latency | Min Latency | Max Latency | Memory | Status |\n")
		fmt.Fprintf(f, "|-----------|---------|-------------|-------------|-------------|--------|--------|\n")

		for _, r := range results {
			status := "✅"
			if !r.Success {
				status = "❌"
			}
			fmt.Fprintf(f, "| %s | %.0f | %s | %s | %s | %s | %s |\n",
				r.Name,
				r.OpsPerSec,
				r.LatencyAvg,
				r.LatencyMin,
				r.LatencyMax,
				formatBytes(r.MemoryUsage),
				status)
		}
		fmt.Fprintf(f, "\n")
	}

	fmt.Fprintf(f, "## Notes\n\n")
	fmt.Fprintf(f, "- All benchmarks run on in-memory database (`:memory:`)\n")
	fmt.Fprintf(f, "- Data sizes: Small (500 rows), Medium (2K rows), Large (10K rows)\n")
	fmt.Fprintf(f, "- Latency measurements include Go runtime overhead\n")
	fmt.Fprintf(f, "- Memory usage is approximate (measured via runtime.ReadMemStats)\n")
	fmt.Fprintf(f, "- Concurrent benchmarks use goroutines\n")

	log.Printf("✅ Markdown report generated: %s", filename)
}

func generateHTMLReport() {
	filename := "benchmark_report.html"
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create HTML report: %v", err)
		return
	}
	defer f.Close()

	tmpl := `<!DOCTYPE html>
<html>
<head>
	<title>CobaltDB Benchmark Report</title>
	<style>
		body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; background: #f5f5f5; }
		.container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
		h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
		h2 { color: #34495e; margin-top: 30px; }
		.info { background: #ecf0f1; padding: 15px; border-radius: 5px; margin: 20px 0; }
		.info p { margin: 5px 0; }
		table { width: 100%; border-collapse: collapse; margin: 20px 0; }
		th { background: #3498db; color: white; padding: 12px; text-align: left; }
		td { padding: 10px; border-bottom: 1px solid #ddd; }
		tr:hover { background: #f5f5f5; }
		.success { color: #27ae60; font-weight: bold; }
		.failure { color: #e74c3c; font-weight: bold; }
		.metric { display: inline-block; background: #3498db; color: white; padding: 15px 25px; margin: 10px; border-radius: 5px; }
		.metric-value { font-size: 24px; font-weight: bold; }
		.metric-label { font-size: 12px; opacity: 0.9; }
		.warning { background: #fff3cd; padding: 10px; border-radius: 5px; margin: 10px 0; }
	</style>
</head>
<body>
	<div class="container">
		<h1>CobaltDB Benchmark Report</h1>
		<p><strong>Generated:</strong> {{.Timestamp.Format "2006-01-02 15:04:05"}}</p>

		<div class="info">
			<p><strong>Go Version:</strong> {{.GoVersion}}</p>
			<p><strong>OS:</strong> {{.OS}}</p>
			<p><strong>Architecture:</strong> {{.Arch}}</p>
			<p><strong>CPUs:</strong> {{.CPUs}}</p>
		</div>

		<h2>Summary</h2>
		<div>
			<div class="metric">
				<div class="metric-value">{{.TotalTests}}</div>
				<div class="metric-label">Total Tests</div>
			</div>
			<div class="metric">
				<div class="metric-value">{{.PassedTests}}</div>
				<div class="metric-label">Passed</div>
			</div>
			<div class="metric">
				<div class="metric-value">{{.FailedTests}}</div>
				<div class="metric-label">Failed</div>
			</div>
			<div class="metric">
				<div class="metric-value">{{printf "%.1f" (div .PassedTests .TotalTests)}}%</div>
				<div class="metric-label">Success Rate</div>
			</div>
		</div>

		<div class="warning">
			<strong>Note:</strong> Benchmarks use realistic data sizes: Small (500 rows), Medium (2K rows), Large (10K rows)
		</div>

		{{range $cat, $results := groupByCategory .Results}}
		<h2>{{$cat}}</h2>
		<table>
			<thead>
				<tr>
					<th>Operation</th>
					<th>Ops/Sec</th>
					<th>Avg Latency</th>
					<th>Min Latency</th>
					<th>Max Latency</th>
					<th>Memory</th>
					<th>Status</th>
				</tr>
			</thead>
			<tbody>
				{{range $results}}
				<tr>
					<td>{{.Name}}</td>
					<td>{{printf "%.0f" .OpsPerSec}}</td>
					<td>{{.LatencyAvg}}</td>
					<td>{{.LatencyMin}}</td>
					<td>{{.LatencyMax}}</td>
					<td>{{formatBytes .MemoryUsage}}</td>
					<td>{{if .Success}}<span class="success">✅</span>{{else}}<span class="failure">❌</span>{{end}}</td>
				</tr>
				{{end}}
			</tbody>
		</table>
		{{end}}

		<h2>Notes</h2>
		<ul>
			<li>All benchmarks run on in-memory database (<code>:memory:</code>)</li>
			<li>Data sizes: Small (500 rows), Medium (2K rows), Large (10K rows)</li>
			<li>Latency measurements include Go runtime overhead</li>
			<li>Memory usage is approximate (measured via runtime.ReadMemStats)</li>
			<li>Concurrent benchmarks use goroutines</li>
		</ul>
	</div>
</body>
</html>`

	funcMap := template.FuncMap{
		"formatBytes": formatBytes,
		"div": func(a, b int) float64 {
			if b == 0 {
				return 0
			}
			return float64(a) / float64(b) * 100
		},
		"groupByCategory": func(results []BenchmarkResult) map[string][]BenchmarkResult {
			cats := make(map[string][]BenchmarkResult)
			order := []string{"DDL", "DML", "Query", "Transaction", "JSON", "Advanced", "Maintenance", "Concurrency", "Server"}
			for _, cat := range order {
				for _, r := range results {
					if r.Category == cat {
						cats[cat] = append(cats[cat], r)
					}
				}
			}
			return cats
		},
	}

	t := template.Must(template.New("report").Funcs(funcMap).Parse(tmpl))
	if err := t.Execute(f, suite); err != nil {
		log.Printf("Failed to execute template: %v", err)
		return
	}

	log.Printf("✅ HTML report generated: %s", filename)
}

func generateJSONReport() {
	filename := "benchmark_report.json"
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create JSON report: %v", err)
		return
	}
	defer f.Close()

	type cleanResult struct {
		Name        string  `json:"name"`
		Category    string  `json:"category"`
		Operations  int     `json:"operations"`
		Duration    string  `json:"duration"`
		OpsPerSec   float64 `json:"ops_per_sec"`
		LatencyAvg  string  `json:"latency_avg"`
		LatencyMin  string  `json:"latency_min"`
		LatencyMax  string  `json:"latency_max"`
		MemoryUsage uint64  `json:"memory_usage"`
		Success     bool    `json:"success"`
		Error       string  `json:"error,omitempty"`
	}

	type cleanSuite struct {
		Timestamp   string        `json:"timestamp"`
		GoVersion   string        `json:"go_version"`
		OS          string        `json:"os"`
		Arch        string        `json:"arch"`
		NumCPU      int           `json:"num_cpu"`
		TotalTests  int           `json:"total_tests"`
		PassedTests int           `json:"passed_tests"`
		FailedTests int           `json:"failed_tests"`
		Results     []cleanResult `json:"results"`
	}

	clean := cleanSuite{
		Timestamp:   suite.Timestamp.Format(time.RFC3339),
		GoVersion:   suite.GoVersion,
		OS:          suite.OS,
		Arch:        suite.Arch,
		NumCPU:      suite.CPUs,
		TotalTests:  suite.TotalTests,
		PassedTests: suite.PassedTests,
		FailedTests: suite.FailedTests,
		Results:     make([]cleanResult, len(suite.Results)),
	}

	for i, r := range suite.Results {
		opsPerSec := r.OpsPerSec
		if math.IsInf(opsPerSec, 0) || math.IsNaN(opsPerSec) {
			opsPerSec = 0
		}
		clean.Results[i] = cleanResult{
			Name:        r.Name,
			Category:    r.Category,
			Operations:  r.Operations,
			Duration:    r.Duration.String(),
			OpsPerSec:   opsPerSec,
			LatencyAvg:  r.LatencyAvg.String(),
			LatencyMin:  r.LatencyMin.String(),
			LatencyMax:  r.LatencyMax.String(),
			MemoryUsage: r.MemoryUsage,
			Success:     r.Success,
			Error:       r.Error,
		}
	}

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(clean); err != nil {
		log.Printf("Failed to encode JSON: %v", err)
		return
	}

	log.Printf("✅ JSON report generated: %s", filename)
}

func printSummary() {
	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("BENCHMARK SUMMARY")
	log.Println(strings.Repeat("=", 60))
	log.Printf("Total Tests:  %d", suite.TotalTests)
	log.Printf("Passed:       %d ✅", suite.PassedTests)
	log.Printf("Failed:       %d ❌", suite.FailedTests)
	log.Printf("Success Rate: %.1f%%", float64(suite.PassedTests)/float64(suite.TotalTests)*100)
	log.Println("")
	log.Println("Data Sizes Used:")
	log.Println("  - Small:  500 rows")
	log.Println("  - Medium: 2,000 rows")
	log.Println("  - Large:  10,000 rows")
	log.Println("")
	log.Println("Generated Reports:")
	log.Println("  - benchmark_report.md (Markdown)")
	log.Println("  - benchmark_report.html (HTML)")
	log.Println("  - benchmark_report.json (JSON)")
	log.Println("")
	log.Println("Benchmark complete!")
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
