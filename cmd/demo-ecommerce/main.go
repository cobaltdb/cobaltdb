// CobaltDB E-Commerce Demo - HIZLI VERSİYON (Düşük Bellek Kullanımı)
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorCyan   = "\033[36m"
	ColorBold   = "\033[1m"
)

var stats struct {
	insertOps int64
	selectOps int64
	updateOps int64
	txnOps    int64
	errorOps  int64
}

func main() {
	printBanner()

	ctx := context.Background()
	startTime := time.Now()

	// Veritabanına bağlan
	fmt.Printf("%s[1/4]%s Veritabanına bağlanıyor...\n", ColorCyan, ColorReset)
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		fmt.Printf("%sHATA:%s %v\n", ColorRed, ColorReset, err)
		os.Exit(1)
	}
	defer db.Close()
	fmt.Printf("%s✓%s Bağlantı başarılı!\n\n", ColorGreen, ColorReset)

	// Schema oluştur
	fmt.Printf("%s[2/4]%s Schema oluşturuluyor...\n", ColorCyan, ColorReset)
	createSchema(ctx, db)
	fmt.Printf("%s✓%s 6 tablo oluşturuldu!\n\n", ColorGreen, ColorReset)

	// Veri ekle
	fmt.Printf("%s[3/4]%s Veri popülasyonu...\n", ColorCyan, ColorReset)
	populateData(ctx, db)
	fmt.Printf("%s✓%s Toplam %s%d%s kayıt!\n\n", ColorGreen, ColorReset, ColorBold,
		atomic.LoadInt64(&stats.insertOps), ColorReset)

	// Senaryolar
	fmt.Printf("%s[4/4]%s E-Ticaret Senaryoları...\n\n", ColorCyan, ColorReset)
	runScenarios(ctx, db)

	// Rapor
	printReport(time.Since(startTime))
}

func printBanner() {
	fmt.Println()
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║          🛒  COBALTDB E-TİCARET DEMO  🛒                       ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
}

func createSchema(ctx context.Context, db *engine.DB) {
	tables := []string{
		"CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT, name TEXT, city TEXT)",
		"CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, stock INTEGER)",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL, status TEXT)",
		"CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, qty INTEGER)",
		"CREATE TABLE reviews (id INTEGER PRIMARY KEY, product_id INTEGER, rating INTEGER, comment TEXT)",
	}

	for _, sql := range tables {
		db.Exec(ctx, sql)
	}
}

func populateData(ctx context.Context, db *engine.DB) {
	// Kategoriler
	tx, _ := db.Begin(ctx)
	for _, cat := range []string{"Elektronik", "Moda", "Ev", "Kitap", "Spor", "Oyuncak", "Kozmetik", "Süpermarket"} {
		tx.Exec(ctx, "INSERT INTO categories (name) VALUES (?)", cat)
		atomic.AddInt64(&stats.insertOps, 1)
	}
	tx.Commit()

	// Müşteriler - tek tek ama hızlı
	fmt.Print("  → Müşteriler (5,000)... ")
	for i := 0; i < 5000; i++ {
		db.Exec(ctx, "INSERT INTO customers (email, name, city) VALUES (?, ?, ?)",
			fmt.Sprintf("musteri%d@test.com", i),
			fmt.Sprintf("Müşteri %d", i),
			[]string{"İstanbul", "Ankara", "İzmir"}[i%3])
		atomic.AddInt64(&stats.insertOps, 1)
		if i%500 == 0 {
			printProgress(i, 5000)
		}
	}
	printProgress(5000, 5000)
	fmt.Printf(" %s✓%s\n", ColorGreen, ColorReset)

	// Ürünler
	fmt.Print("  → Ürünler (10,000)... ")
	for i := 0; i < 10000; i++ {
		db.Exec(ctx, "INSERT INTO products (name, price, stock) VALUES (?, ?, ?)",
			fmt.Sprintf("Ürün %d", i),
			10.0+float64(rand.Intn(990)),
			rand.Intn(1000))
		atomic.AddInt64(&stats.insertOps, 1)
		if i%1000 == 0 {
			printProgress(i, 10000)
		}
	}
	printProgress(10000, 10000)
	fmt.Printf(" %s✓%s\n", ColorGreen, ColorReset)

	// Siparişler
	fmt.Print("  → Siparişler (20,000)... ")
	statuses := []string{"pending", "shipped", "delivered", "cancelled"}
	for i := 0; i < 20000; i++ {
		db.Exec(ctx, "INSERT INTO orders (customer_id, total, status) VALUES (?, ?, ?)",
			rand.Intn(5000)+1,
			float64(rand.Intn(1000)),
			statuses[rand.Intn(len(statuses))])
		atomic.AddInt64(&stats.insertOps, 1)

		// Her siparişe 1-2 ürün
		for j := 0; j < rand.Intn(2)+1; j++ {
			db.Exec(ctx, "INSERT INTO order_items (order_id, product_id, qty) VALUES (?, ?, ?)",
				i+1, rand.Intn(10000)+1, rand.Intn(3)+1)
			atomic.AddInt64(&stats.insertOps, 1)
		}

		if i%2000 == 0 {
			printProgress(i, 20000)
		}
	}
	printProgress(20000, 20000)
	fmt.Printf(" %s✓%s\n", ColorGreen, ColorReset)

	// İncelemeler
	fmt.Print("  → İncelemeler (5,000)... ")
	for i := 0; i < 5000; i++ {
		db.Exec(ctx, "INSERT INTO reviews (product_id, rating, comment) VALUES (?, ?, ?)",
			rand.Intn(10000)+1, rand.Intn(5)+1, "Güzel ürün")
		atomic.AddInt64(&stats.insertOps, 1)
	}
	fmt.Printf("%s✓%s\n", ColorGreen, ColorReset)
}

func printProgress(current, total int) {
	width := 30
	pct := (current * 100) / total
	filled := (current * width) / total
	bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	fmt.Printf("\r[%s] %3d%%", bar, pct)
}

func runScenarios(ctx context.Context, db *engine.DB) {
	scenarios := []struct {
		name string
		fn   func(context.Context, *engine.DB)
	}{
		{"Satış Raporu", scenarioSalesReport},
		{"En Çok Satanlar", scenarioTopProducts},
		{"Şehir Analizi", scenarioCityAnalysis},
		{"Stok Güncelleme", scenarioUpdateStock},
		{"Concurrent Test", scenarioConcurrent},
	}

	for i, s := range scenarios {
		fmt.Printf("%sSenaryo %d:%s %s\n", ColorYellow, i+1, ColorReset, s.name)
		start := time.Now()
		s.fn(ctx, db)
		fmt.Printf("  %s✓%s %v\n\n", ColorGreen, ColorReset, time.Since(start))
	}
}

func scenarioSalesReport(ctx context.Context, db *engine.DB) {
	rows, _ := db.Query(ctx, "SELECT status, COUNT(*), SUM(total) FROM orders GROUP BY status")
	if rows != nil {
		rows.Close()
	}
	atomic.AddInt64(&stats.selectOps, 1)
}

func scenarioTopProducts(ctx context.Context, db *engine.DB) {
	rows, _ := db.Query(ctx, `SELECT p.name, COUNT(oi.id)
		FROM products p JOIN order_items oi ON p.id = oi.product_id
		GROUP BY p.id ORDER BY COUNT(oi.id) DESC LIMIT 10`)
	if rows != nil {
		rows.Close()
	}
	atomic.AddInt64(&stats.selectOps, 1)
}

func scenarioCityAnalysis(ctx context.Context, db *engine.DB) {
	rows, _ := db.Query(ctx, `SELECT city, COUNT(*), COUNT(DISTINCT o.id)
		FROM customers c LEFT JOIN orders o ON c.id = o.customer_id
		GROUP BY city`)
	if rows != nil {
		rows.Close()
	}
	atomic.AddInt64(&stats.selectOps, 1)
}

func scenarioUpdateStock(ctx context.Context, db *engine.DB) {
	db.Exec(ctx, "UPDATE products SET stock = stock - 1 WHERE stock > 0 AND id <= 100")
	atomic.AddInt64(&stats.updateOps, 1)
}

func scenarioConcurrent(ctx context.Context, db *engine.DB) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tx, _ := db.Begin(ctx)
			tx.Query(ctx, "SELECT * FROM products WHERE id = ?", rand.Intn(10000)+1)
			tx.Exec(ctx, "UPDATE products SET stock = stock - 1 WHERE id = ?", rand.Intn(100)+1)
			tx.Commit()
			atomic.AddInt64(&stats.txnOps, 1)
		}(i)
	}
	wg.Wait()
}

func printReport(duration time.Duration) {
	total := atomic.LoadInt64(&stats.insertOps) + atomic.LoadInt64(&stats.selectOps) +
		atomic.LoadInt64(&stats.updateOps) + atomic.LoadInt64(&stats.txnOps)

	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    📊 SONUÇ RAPORU 📊                          ║")
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  ⏱️  Süre:           %-42s ║\n", duration.Round(time.Millisecond))
	fmt.Printf("║  📝 INSERT:          %-42d ║\n", atomic.LoadInt64(&stats.insertOps))
	fmt.Printf("║  🔍 SELECT:          %-42d ║\n", atomic.LoadInt64(&stats.selectOps))
	fmt.Printf("║  🔄 UPDATE:          %-42d ║\n", atomic.LoadInt64(&stats.updateOps))
	fmt.Printf("║  💾 TRANSACTION:     %-42d ║\n", atomic.LoadInt64(&stats.txnOps))
	fmt.Printf("║  📈 Toplam:          %-42d ║\n", total)
	fmt.Printf("║  ⚡ İşlem/sn:        %-42.2f ║\n", float64(total)/duration.Seconds())
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println(ColorGreen + "✅ E-TİCARET DEMO BAŞARIYLA TAMAMLANDI!" + ColorReset)
	fmt.Println()
}
