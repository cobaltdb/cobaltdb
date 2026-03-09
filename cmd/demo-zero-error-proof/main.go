// CobaltDB - SIFIR HATA KANITI - Her İşlem Detaylı Log
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

var (
	stats struct {
		Total   int64
		Success int64
		Failed  int64
	}
	db  *engine.DB
	ctx = context.Background()
)

func main() {
	fmt.Println("╔═══════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║              COBALTDB - DETAYLI HATA KONTROLÜ                             ║")
	fmt.Println("║                    (Her İşlem Ayrı Loglanıyor)                            ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	var err error
	db, err = engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		fmt.Printf("❌ VERİTABANI AÇILAMADI: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	fmt.Println("✅ Veritabanı açıldı")

	start := time.Now()

	// TESTLER
	testDDL()
	testInsert()
	testSelect()
	testUpdate()
	testDelete()
	testJoin()
	testTransaction()
	testJSON()
	testConcurrent()

	duration := time.Since(start)

	// FİNAL RAPOR
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                         FİNAL RAPOR                                       ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  TOPLAM İŞLEM:  %5d                                                    ║\n", stats.Total)
	fmt.Printf("║  ✅ BAŞARILI:    %5d                                                    ║\n", stats.Success)
	fmt.Printf("║  ❌ BAŞARISIZ:   %5d                                                    ║\n", stats.Failed)
	fmt.Printf("║  SÜRE:           %v                                                      ║\n", duration.Round(time.Millisecond))
	fmt.Println("╚═══════════════════════════════════════════════════════════════════════════╝")

	if stats.Failed == 0 {
		fmt.Println()
		fmt.Println("🎉 GERÇEKTEN SIFIR HATA! TÜM İŞLEMLER BAŞARILI!")
		fmt.Println()
	} else {
		fmt.Printf("\n⚠️  %d HATA VAR\n", stats.Failed)
	}
}

func execute(name, sql string, args ...interface{}) bool {
	atomic.AddInt64(&stats.Total, 1)
	_, err := db.Exec(ctx, sql, args...)
	if err != nil {
		atomic.AddInt64(&stats.Failed, 1)
		fmt.Printf("  ❌ EXEC FAIL: %-40s | Error: %v\n", name, err)
		return false
	}
	atomic.AddInt64(&stats.Success, 1)
	fmt.Printf("  ✅ EXEC OK:   %-40s\n", name)
	return true
}

func query(name, sql string, args ...interface{}) bool {
	atomic.AddInt64(&stats.Total, 1)
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		atomic.AddInt64(&stats.Failed, 1)
		fmt.Printf("  ❌ QUERY FAIL: %-40s | Error: %v\n", name, err)
		return false
	}
	rows.Close()
	atomic.AddInt64(&stats.Success, 1)
	fmt.Printf("  ✅ QUERY OK:   %-40s\n", name)
	return true
}

func testDDL() {
	fmt.Println("▶ DDL TESTLERİ (Tablo/Index/View)")

	// DROP
	execute("DROP TABLE IF EXISTS users", "DROP TABLE IF EXISTS users")
	execute("DROP TABLE IF EXISTS products", "DROP TABLE IF EXISTS products")
	execute("DROP TABLE IF EXISTS orders", "DROP TABLE IF EXISTS orders")
	execute("DROP VIEW IF EXISTS v_users", "DROP VIEW IF EXISTS v_users")

	// CREATE TABLE
	execute("CREATE TABLE users", `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT,
		username TEXT,
		age INTEGER,
		balance REAL,
		is_active BOOLEAN,
		profile JSON,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`)

	execute("CREATE TABLE products", `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT,
		price REAL,
		stock INTEGER,
		tags JSON
	)`)

	execute("CREATE TABLE orders", `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		user_id INTEGER,
		product_id INTEGER,
		quantity INTEGER,
		total REAL,
		status TEXT
	)`)

	// CREATE INDEX
	execute("CREATE INDEX idx_users_email", "CREATE INDEX idx_users_email ON users(email)")
	execute("CREATE INDEX idx_users_age", "CREATE INDEX idx_users_age ON users(age)")
	execute("CREATE INDEX idx_products_price", "CREATE INDEX idx_products_price ON products(price)")

	// CREATE VIEW
	execute("CREATE VIEW v_users", "CREATE VIEW v_users AS SELECT * FROM users WHERE is_active = true")

	fmt.Println()
}

func testInsert() {
	fmt.Println("▶ INSERT TESTLERİ")

	// Basic inserts
	execute("INSERT user 1", "INSERT INTO users (email, username, age, balance, is_active) VALUES (?, ?, ?, ?, ?)",
		"user1@test.com", "user1", 25, 1000.50, true)
	execute("INSERT user 2", "INSERT INTO users (email, username, age, balance, is_active) VALUES (?, ?, ?, ?, ?)",
		"user2@test.com", "user2", 30, 2500.00, true)
	execute("INSERT user 3", "INSERT INTO users (email, username, age, balance, is_active) VALUES (?, ?, ?, ?, ?)",
		"user3@test.com", "user3", 35, 500.00, false)

	// JSON insert
	execute("INSERT user with JSON", "INSERT INTO users (email, username, profile) VALUES (?, ?, ?)",
		"json@test.com", "jsonuser", `{"bio": "Hello", "theme": "dark"}`)

	// Products
	execute("INSERT product 1", "INSERT INTO products (name, price, stock, tags) VALUES (?, ?, ?, ?)",
		"Laptop", 999.99, 10, `["electronics", "computers"]`)
	execute("INSERT product 2", "INSERT INTO products (name, price, stock, tags) VALUES (?, ?, ?, ?)",
		"Phone", 599.99, 20, `["electronics", "mobile"]`)
	execute("INSERT product 3", "INSERT INTO products (name, price, stock, tags) VALUES (?, ?, ?, ?)",
		"Book", 19.99, 100, `["books", "fiction"]`)

	// Orders
	execute("INSERT order 1", "INSERT INTO orders (user_id, product_id, quantity, total, status) VALUES (?, ?, ?, ?, ?)",
		1, 1, 1, 999.99, "completed")
	execute("INSERT order 2", "INSERT INTO orders (user_id, product_id, quantity, total, status) VALUES (?, ?, ?, ?, ?)",
		2, 2, 2, 1199.98, "pending")
	execute("INSERT order 3", "INSERT INTO orders (user_id, product_id, quantity, total, status) VALUES (?, ?, ?, ?, ?)",
		1, 3, 5, 99.95, "completed")

	fmt.Println()
}

func testSelect() {
	fmt.Println("▶ SELECT TESTLERİ")

	query("SELECT * FROM users", "SELECT * FROM users")
	query("SELECT with WHERE", "SELECT * FROM users WHERE age > 25")
	query("SELECT with AND", "SELECT * FROM users WHERE age > 25 AND is_active = true")
	query("SELECT with OR", "SELECT * FROM users WHERE age < 30 OR balance > 2000")
	query("SELECT with IN", "SELECT * FROM users WHERE age IN (25, 30, 35)")
	query("SELECT with BETWEEN", "SELECT * FROM products WHERE price BETWEEN 100 AND 1000")
	query("SELECT with LIKE prefix", "SELECT * FROM users WHERE username LIKE 'user%'")
	query("SELECT with ORDER BY", "SELECT * FROM users ORDER BY age DESC")
	query("SELECT with LIMIT", "SELECT * FROM users LIMIT 2")
	query("SELECT with LIMIT OFFSET", "SELECT * FROM users LIMIT 2 OFFSET 1")
	query("SELECT COUNT(*)", "SELECT COUNT(*) FROM users")
	query("SELECT SUM", "SELECT SUM(balance) FROM users")
	query("SELECT AVG", "SELECT AVG(age) FROM users")
	query("SELECT MIN/MAX", "SELECT MIN(price), MAX(price) FROM products")
	query("SELECT GROUP BY", "SELECT is_active, COUNT(*) FROM users GROUP BY is_active")
	query("SELECT HAVING", "SELECT status, SUM(total) FROM orders GROUP BY status HAVING SUM(total) > 100")
	query("SELECT DISTINCT", "SELECT DISTINCT age FROM users")
	query("SELECT with alias", "SELECT id AS uid, email AS mail FROM users LIMIT 1")
	query("SELECT from VIEW", "SELECT * FROM v_users")
	query("SELECT COALESCE", "SELECT COALESCE(NULL, 'fallback')")
	query("SELECT CASE", "SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END")

	fmt.Println()
}

func testUpdate() {
	fmt.Println("▶ UPDATE TESTLERİ")

	execute("UPDATE balance", "UPDATE users SET balance = balance + 100 WHERE id = 1")
	execute("UPDATE status", "UPDATE users SET is_active = false WHERE id = 3")
	execute("UPDATE price", "UPDATE products SET price = price * 0.9 WHERE id = 1")
	execute("UPDATE stock", "UPDATE products SET stock = stock - 1 WHERE id = 1 AND stock > 0")
	execute("UPDATE order status", "UPDATE orders SET status = 'completed' WHERE id = 2")

	fmt.Println()
}

func testDelete() {
	fmt.Println("▶ DELETE TESTLERİ")

	// Önce silinecek veri ekle
	execute("INSERT temp user", "INSERT INTO users (email, username) VALUES (?, ?)", "temp@test.com", "temp")
	execute("DELETE temp user", "DELETE FROM users WHERE email = 'temp@test.com'")

	fmt.Println()
}

func testJoin() {
	fmt.Println("▶ JOIN TESTLERİ")

	query("INNER JOIN", "SELECT u.username, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id")
	query("LEFT JOIN", "SELECT u.username, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id")
	query("Multiple JOIN", "SELECT u.username, p.name, o.quantity FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id")
	query("JOIN with WHERE", "SELECT u.username, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 500")

	fmt.Println()
}

func testTransaction() {
	fmt.Println("▶ TRANSACTION TESTLERİ")

	// Commit
	tx1, _ := db.Begin(ctx)
	_, err1 := tx1.Exec(ctx, "INSERT INTO users (email, username) VALUES (?, ?)", "tx1@test.com", "tx1")
	_, err2 := tx1.Exec(ctx, "INSERT INTO users (email, username) VALUES (?, ?)", "tx2@test.com", "tx2")
	err3 := tx1.Commit()

	atomic.AddInt64(&stats.Total, 3)
	if err1 != nil || err2 != nil || err3 != nil {
		atomic.AddInt64(&stats.Failed, 1)
		fmt.Printf("  ❌ TRANSACTION Commit: FAILED\n")
	} else {
		atomic.AddInt64(&stats.Success, 1)
		fmt.Printf("  ✅ TRANSACTION Commit: OK\n")
	}

	// Rollback
	tx2, _ := db.Begin(ctx)
	tx2.Exec(ctx, "INSERT INTO users (email, username) VALUES (?, ?)", "rollback@test.com", "rollback")
	err4 := tx2.Rollback()

	atomic.AddInt64(&stats.Total, 1)
	if err4 != nil {
		atomic.AddInt64(&stats.Failed, 1)
		fmt.Printf("  ❌ TRANSACTION Rollback: FAILED\n")
	} else {
		atomic.AddInt64(&stats.Success, 1)
		fmt.Printf("  ✅ TRANSACTION Rollback: OK\n")
	}

	fmt.Println()
}

func testJSON() {
	fmt.Println("▶ JSON TESTLERİ")

	query("JSON_EXTRACT", "SELECT JSON_EXTRACT(profile, '$.bio') FROM users WHERE profile IS NOT NULL")
	query("JSON with WHERE", "SELECT * FROM users WHERE JSON_EXTRACT(profile, '$.theme') = 'dark'")
	query("JSON_ARRAY_LENGTH", "SELECT JSON_ARRAY_LENGTH(tags) FROM products WHERE tags IS NOT NULL")

	execute("JSON_SET", "UPDATE users SET profile = JSON_SET(profile, '$.updated', true) WHERE profile IS NOT NULL")

	fmt.Println()
}

func testConcurrent() {
	fmt.Println("▶ CONCURRENT TEST (10 Worker)")

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			tx, _ := db.Begin(ctx)

			// SELECT
			_, err1 := tx.Query(ctx, "SELECT * FROM users WHERE id = ?", (id%3)+1)
			// UPDATE
			_, err2 := tx.Exec(ctx, "UPDATE users SET balance = balance + 1 WHERE id = ?", (id%3)+1)

			if err1 != nil || err2 != nil {
				tx.Rollback()
				errors <- fmt.Errorf("worker %d failed", id)
			} else {
				tx.Commit()
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	atomic.AddInt64(&stats.Total, 10)
	errorCount := 0
	for err := range errors {
		if err != nil {
			errorCount++
		}
	}

	if errorCount > 0 {
		atomic.AddInt64(&stats.Failed, int64(errorCount))
		fmt.Printf("  ❌ Concurrent: %d workers failed\n", errorCount)
	} else {
		atomic.AddInt64(&stats.Success, 10)
		fmt.Printf("  ✅ Concurrent: 10/10 workers OK\n")
	}

	fmt.Println()
}
