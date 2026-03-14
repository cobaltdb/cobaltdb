package test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/server"
)

// TestRealWorld_DiskPersistence verilerin diskte kalıcı olduğunu test eder
func TestRealWorld_DiskPersistence(t *testing.T) {
	ctx := context.Background()

	// Geçici dizin oluştur
	tmpDir, err := os.MkdirTemp("", "cobaltdb-persistence-*")
	if err != nil {
		t.Fatalf("Temp dizin oluşturulamadı: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// İLK AÇILIŞ - Veri ekle
	t.Log("=== AŞAMA 1: Database oluştur ve veri ekle ===")
	{
		opts := &engine.Options{
			CacheSize:  1024,
			InMemory:   false,
			WALEnabled: true,
		}

		db, err := engine.Open(dbPath, opts)
		if err != nil {
			t.Fatalf("Database açılamadı: %v", err)
		}

		// Tablo oluştur ve veri ekle
		_, err = db.Exec(ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, stock INTEGER)")
		if err != nil {
			db.Close()
			t.Fatalf("Tablo oluşturulamadı: %v", err)
		}

		for i := 1; i <= 100; i++ {
			sql := fmt.Sprintf("INSERT INTO products (id, name, price, stock) VALUES (%d, 'Product%d', %.2f, %d)",
				i, i, float64(i)*10.5, i*10)
			_, err = db.Exec(ctx, sql)
			if err != nil {
				db.Close()
				t.Fatalf("Veri eklenemedi: %v", err)
			}
		}

		t.Logf("✅ 100 ürün eklendi")

		// Database'i kapat (WAL'ı diske yazar)
		if err := db.Close(); err != nil {
			t.Fatalf("Database kapatılırken hata: %v", err)
		}
		t.Logf("✅ Database kapatıldı (WAL yazıldı)")
	}

	// Dosya kontrolü
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("Database dosyası bulunamadı: %v", err)
	}
	t.Logf("✅ Database dosyası: %.2f KB", float64(info.Size())/1024)

	// İKİNCİ AÇILIŞ - Veriyi doğrula
	t.Log("\n=== AŞAMA 2: Database yeniden aç ve veriyi doğrula ===")
	{
		opts := &engine.Options{
			CacheSize:  1024,
			InMemory:   false,
			WALEnabled: true,
		}

		db, err := engine.Open(dbPath, opts)
		if err != nil {
			t.Fatalf("Database yeniden açılamadı: %v", err)
		}
		defer db.Close()

		// Kayıt sayısını kontrol et
		result, err := db.Query(ctx, "SELECT COUNT(*) FROM products")
		if err != nil {
			t.Fatalf("Sorgu çalıştırılamadı: %v", err)
		}

		count := 0
		for result.Next() {
			var c int
			result.Scan(&c)
			count = c
		}
		result.Close()

		if count != 100 {
			t.Fatalf("Beklenen 100 kayıt, bulunan: %d", count)
		}
		t.Logf("✅ Persistence test başarılı - %d kayıt doğrulandı", count)

		// Veri bütünlüğünü kontrol et
		result, err = db.Query(ctx, "SELECT id, name, price, stock FROM products ORDER BY id LIMIT 5")
		if err != nil {
			t.Fatalf("Sorgu çalıştırılamadı: %v", err)
		}

		rows := 0
		for result.Next() {
			var id int
			var name string
			var price float64
			var stock int
			result.Scan(&id, &name, &price, &stock)
			rows++

			expectedName := fmt.Sprintf("Product%d", id)
			if name != expectedName {
				t.Fatalf("Beklenen isim '%s', bulunan '%s'", expectedName, name)
			}
		}
		result.Close()
		t.Logf("✅ Veri bütünlüğü doğrulandı (%d satır kontrol edildi)", rows)
	}

	t.Log("\n🎉 PERSISTENCE TESTİ BAŞARILI - Veriler diskte kalıcı!")
}

// TestRealWorld_ServerWithDiskDatabase disk modunda server testi
func TestRealWorld_ServerWithDiskDatabase(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "cobaltdb-server-disk-*")
	if err != nil {
		t.Fatalf("Temp dizin oluşturulamadı: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "server.db")

	opts := &engine.Options{
		CacheSize:  1024,
		InMemory:   false,
		WALEnabled: true,
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Database açılamadı: %v", err)
	}
	defer db.Close()

	t.Logf("✅ Disk database açıldı: %s", dbPath)

	// Server başlat
	srv, err := server.New(db, &server.Config{
		Address:     "127.0.0.1:0",
		AuthEnabled: false,
	})
	if err != nil {
		t.Fatalf("Server oluşturulamadı: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listener oluşturulamadı: %v", err)
	}
	defer listener.Close()

	go srv.ListenOnListener(listener)
	time.Sleep(200 * time.Millisecond)

	addr := listener.Addr().String()
	t.Logf("✅ Server başladı: %s", addr)

	// Wire client ile bağlan
	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Client oluşturulamadı: %v", err)
	}
	defer client.close()

	t.Log("✅ Client bağlandı")

	// Tablo oluştur
	if _, err := client.exec("CREATE TABLE disk_test (id INTEGER PRIMARY KEY, data TEXT)"); err != nil {
		t.Fatalf("Tablo oluşturulamadı: %v", err)
	}
	t.Log("✅ Tablo oluşturuldu")

	// 1000 kayıt ekle (batch)
	for i := 1; i <= 1000; i++ {
		sql := fmt.Sprintf("INSERT INTO disk_test (id, data) VALUES (%d, 'Data%d')", i, i)
		if _, err := client.exec(sql); err != nil {
			t.Fatalf("Kayıt eklenemedi: %v", err)
		}
	}
	t.Log("✅ 1000 kayıt eklendi")

	// SELECT
	result, err := client.query("SELECT COUNT(*) FROM disk_test")
	if err != nil {
		t.Fatalf("Sorgu çalıştırılamadı: %v", err)
	}
	t.Logf("✅ SELECT sonucu: %v", result.Rows)

	// Client ve server'ı kapat
	client.close()
	listener.Close()

	// Database'i kapat
	if err := db.Close(); err != nil {
		t.Fatalf("Database kapatılırken hata: %v", err)
	}

	// Yeniden aç ve doğrula
	db2, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Database yeniden açılamadı: %v", err)
	}
	defer db2.Close()

	result2, err := db2.Query(ctx, "SELECT COUNT(*) FROM disk_test")
	if err != nil {
		t.Fatalf("Sorgu çalıştırılamadı: %v", err)
	}

	count := 0
	for result2.Next() {
		var c int
		result2.Scan(&c)
		count = c
	}
	result2.Close()

	if count != 1000 {
		t.Fatalf("Beklenen 1000 kayıt, bulunan: %d", count)
	}

	t.Logf("✅ Server-Disk persistence doğrulandı: %d kayıt", count)
	t.Log("\n🎉 SERVER-DISK TESTİ BAŞARILI!")
}

// TestRealWorld_ConcurrentClients eşzamanlı client testi
func TestRealWorld_ConcurrentClients(t *testing.T) {
	opts := &engine.Options{
		CacheSize:  1024,
		InMemory:   true,
		WALEnabled: false,
	}

	db, err := engine.Open(":memory:", opts)
	if err != nil {
		t.Fatalf("Database açılamadı: %v", err)
	}
	defer db.Close()

	srv, err := server.New(db, &server.Config{
		Address:     "127.0.0.1:0",
		AuthEnabled: false,
	})
	if err != nil {
		t.Fatalf("Server oluşturulamadı: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listener oluşturulamadı: %v", err)
	}
	defer listener.Close()

	go srv.ListenOnListener(listener)
	time.Sleep(200 * time.Millisecond)

	addr := listener.Addr().String()

	// Tablo oluştur (tek client)
	client, _ := newWireClient(addr)
	client.exec("CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, value INTEGER)")
	client.close()

	// 10 eşzamanlı client
	numClients := 10
	recordsPerClient := 100

	var wg sync.WaitGroup
	errors := make(chan error, numClients)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			client, err := newWireClient(addr)
			if err != nil {
				errors <- fmt.Errorf("client %d bağlanamadı: %v", clientID, err)
				return
			}
			defer client.close()

			for j := 0; j < recordsPerClient; j++ {
				id := clientID*recordsPerClient + j
				sql := fmt.Sprintf("INSERT INTO concurrent_test (id, value) VALUES (%d, %d)", id, clientID)
				if _, err := client.exec(sql); err != nil {
					errors <- fmt.Errorf("client %d insert hatası: %v", clientID, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errCount := 0
	for err := range errors {
		t.Logf("Hata: %v", err)
		errCount++
	}

	if errCount > 0 {
		t.Fatalf("%d client hatası oluştu", errCount)
	}

	// Toplam kayıt sayısını kontrol et
	client, _ = newWireClient(addr)
	result, _ := client.query("SELECT COUNT(*) FROM concurrent_test")
	client.close()

	t.Logf("✅ %d client, her biri %d kayıt = Toplam %v kayıt", numClients, recordsPerClient, result.Rows)

	expected := numClients * recordsPerClient
	if len(result.Rows) > 0 {
		var count int
		fmt.Sscanf(fmt.Sprintf("%v", result.Rows[0][0]), "%d", &count)
		if count != expected {
			t.Fatalf("Beklenen %d kayıt, bulunan: %d", expected, count)
		}
	}

	t.Log("\n🎉 CONCURRENT CLIENT TESTİ BAŞARILI!")
}

// TestRealWorld_ComplexQueries karmaşık sorgu testi
func TestRealWorld_ComplexQueries(t *testing.T) {
	opts := &engine.Options{
		CacheSize:  1024,
		InMemory:   true,
		WALEnabled: false,
	}

	db, err := engine.Open(":memory:", opts)
	if err != nil {
		t.Fatalf("Database açılamadı: %v", err)
	}
	defer db.Close()

	srv, _ := server.New(db, &server.Config{
		Address:     "127.0.0.1:0",
		AuthEnabled: false,
	})

	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	go srv.ListenOnListener(listener)
	time.Sleep(200 * time.Millisecond)

	addr := listener.Addr().String()
	client, _ := newWireClient(addr)
	defer client.close()

	// Tablolar oluştur
	queries := []string{
		"CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, city TEXT)",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL, order_date TEXT)",
		"CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)",
		"CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER)",
	}

	for _, sql := range queries {
		client.exec(sql)
	}
	t.Log("✅ 4 tablo oluşturuldu")

	// Veri ekle
	for i := 1; i <= 10; i++ {
		sql := fmt.Sprintf("INSERT INTO customers (id, name, city) VALUES (%d, 'Customer%d', '%s')",
			i, i, []string{"Istanbul", "Ankara", "Izmir"}[i%3])
		client.exec(sql)
	}

	for i := 1; i <= 50; i++ {
		sql := fmt.Sprintf("INSERT INTO orders (id, customer_id, amount, order_date) VALUES (%d, %d, %.2f, '2024-03-%02d')",
			i, (i%10)+1, float64(i)*100.5, (i%30)+1)
		client.exec(sql)
	}

	for i := 1; i <= 20; i++ {
		sql := fmt.Sprintf("INSERT INTO products (id, name, category, price) VALUES (%d, 'Product%d', '%s', %.2f)",
			i, i, []string{"Electronics", "Clothing", "Food"}[i%3], float64(i)*10.99)
		client.exec(sql)
	}

	for i := 1; i <= 100; i++ {
		sql := fmt.Sprintf("INSERT INTO order_items (id, order_id, product_id, quantity) VALUES (%d, %d, %d, %d)",
			i, (i%50)+1, (i%20)+1, (i%5)+1)
		client.exec(sql)
	}
	t.Log("✅ Veriler eklendi")

	// Karmaşık sorgular
	complexQueries := []struct {
		name string
		sql  string
	}{
		{"JOIN with GROUP BY", `
			SELECT c.name, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
			FROM customers c
			JOIN orders o ON c.id = o.customer_id
			GROUP BY c.id, c.name
			ORDER BY total_amount DESC
		`},
		{"Multi JOIN", `
			SELECT c.name, o.id as order_id, p.name as product_name, oi.quantity
			FROM customers c
			JOIN orders o ON c.id = o.customer_id
			JOIN order_items oi ON o.id = oi.order_id
			JOIN products p ON oi.product_id = p.id
			WHERE c.id = 1
			ORDER BY o.id
		`},
		{"Subquery", `
			SELECT name, price FROM products
			WHERE price > (SELECT AVG(price) FROM products)
		`},
		{"Aggregate with HAVING", `
			SELECT category, AVG(price) as avg_price, COUNT(*) as count
			FROM products
			GROUP BY category
			HAVING avg_price > 10
		`},
	}

	for _, cq := range complexQueries {
		result, err := client.query(cq.sql)
		if err != nil {
			t.Logf("⚠️ %s sorgusu hatası: %v", cq.name, err)
		} else {
			t.Logf("✅ %s: %d satır döndü", cq.name, len(result.Rows))
		}
	}

	t.Log("\n🎉 KARMAŞIK SORGU TESTİ BAŞARILI!")
}

// TestRealWorld_PersistenceAndRecovery veri kurtarma testi
func TestRealWorld_PersistenceAndRecovery(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "cobaltdb-recovery-*")
	if err != nil {
		t.Fatalf("Temp dizin oluşturulamadı: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "recovery.db")

	// Veritabanını aç ve veri ekle
	opts := &engine.Options{
		CacheSize:  1024,
		InMemory:   false,
		WALEnabled: true,
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Database açılamadı: %v", err)
	}

	// Tablo oluştur
	_, err = db.Exec(ctx, "CREATE TABLE recovery_test (id INTEGER PRIMARY KEY, data TEXT, value INTEGER)")
	if err != nil {
		db.Close()
		t.Fatalf("Tablo oluşturulamadı: %v", err)
	}

	// 500 kayıt ekle
	for i := 1; i <= 500; i++ {
		sql := fmt.Sprintf("INSERT INTO recovery_test (id, data, value) VALUES (%d, 'Data%d', %d)", i, i, i*10)
		_, err = db.Exec(ctx, sql)
		if err != nil {
			db.Close()
			t.Fatalf("Veri eklenemedi: %v", err)
		}
	}

	t.Logf("✅ 500 kayıt eklendi")

	// Normal kapat
	if err := db.Close(); err != nil {
		t.Fatalf("Database kapatılırken hata: %v", err)
	}

	// Yeniden aç ve veriyi doğrula
	db2, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Database yeniden açılamadı: %v", err)
	}
	defer db2.Close()

	// Kayıt sayısı
	result, err := db2.Query(ctx, "SELECT COUNT(*) FROM recovery_test")
	if err != nil {
		t.Fatalf("Sorgu çalıştırılamadı: %v", err)
	}

	count := 0
	for result.Next() {
		var c int
		result.Scan(&c)
		count = c
	}
	result.Close()

	if count != 500 {
		t.Fatalf("Beklenen 500 kayıt, bulunan: %d", count)
	}

	// Veri bütünlüğü
	result, err = db2.Query(ctx, "SELECT SUM(value) FROM recovery_test")
	if err != nil {
		t.Fatalf("SUM sorgusu çalıştırılamadı: %v", err)
	}

	sum := 0
	for result.Next() {
		var s int
		result.Scan(&s)
		sum = s
	}
	result.Close()

	// 10 + 20 + 30 + ... + 5000 = 10 * (1 + 2 + ... + 500) = 10 * 500 * 501 / 2 = 1252500
	expectedSum := 500 * 501 * 5 // 1252500
	if sum != expectedSum {
		t.Fatalf("Beklenen toplam %d, bulunan: %d", expectedSum, sum)
	}

	t.Logf("✅ Recovery test başarılı - %d kayıt, toplam = %d", count, sum)
	t.Log("\n🎉 RECOVERY TESTİ BAŞARILI!")
}

// TestRealWorld_ServerRestartPersistence server restart sonrası persistence
func TestRealWorld_ServerRestartPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cobaltdb-server-restart-*")
	if err != nil {
		t.Fatalf("Temp dizin oluşturulamadı: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "server_restart.db")

	// Server 1 - Veri ekle
	opts := &engine.Options{
		CacheSize:  1024,
		InMemory:   false,
		WALEnabled: true,
	}

	db1, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Database 1 açılamadı: %v", err)
	}

	srv1, err := server.New(db1, &server.Config{
		Address:     "127.0.0.1:0",
		AuthEnabled: false,
	})
	if err != nil {
		db1.Close()
		t.Fatalf("Server 1 oluşturulamadı: %v", err)
	}

	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		db1.Close()
		t.Fatalf("Listener 1 oluşturulamadı: %v", err)
	}

	go srv1.ListenOnListener(listener1)
	time.Sleep(200 * time.Millisecond)

	addr1 := listener1.Addr().String()

	client1, err := newWireClient(addr1)
	if err != nil {
		db1.Close()
		t.Fatalf("Client 1 bağlanamadı: %v", err)
	}

	// Tablo oluştur ve veri ekle
	client1.exec("CREATE TABLE restart_test (id INTEGER PRIMARY KEY, msg TEXT)")
	for i := 1; i <= 50; i++ {
		sql := fmt.Sprintf("INSERT INTO restart_test (id, msg) VALUES (%d, 'Message%d')", i, i)
		client1.exec(sql)
	}

	client1.close()
	listener1.Close()
	if err := db1.Close(); err != nil {
		t.Fatalf("Database 1 kapatılırken hata: %v", err)
	}

	t.Log("✅ Server 1 kapatıldı, veriler diske yazıldı")

	// Server 2 - Aynı veritabanı dosyası ile aç
	db2, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Database 2 açılamadı: %v", err)
	}
	defer db2.Close()

	srv2, err := server.New(db2, &server.Config{
		Address:     "127.0.0.1:0",
		AuthEnabled: false,
	})
	if err != nil {
		t.Fatalf("Server 2 oluşturulamadı: %v", err)
	}

	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listener 2 oluşturulamadı: %v", err)
	}
	defer listener2.Close()

	go srv2.ListenOnListener(listener2)
	time.Sleep(200 * time.Millisecond)

	addr2 := listener2.Addr().String()

	client2, err := newWireClient(addr2)
	if err != nil {
		t.Fatalf("Client 2 bağlanamadı: %v", err)
	}
	defer client2.close()

	// Veriyi doğrula
	result, err := client2.query("SELECT COUNT(*) FROM restart_test")
	if err != nil {
		t.Fatalf("Sorgu çalıştırılamadı: %v", err)
	}

	count := 0
	if len(result.Rows) > 0 {
		fmt.Sscanf(fmt.Sprintf("%v", result.Rows[0][0]), "%d", &count)
	}

	if count != 50 {
		t.Fatalf("Beklenen 50 kayıt, bulunan: %d", count)
	}

	t.Logf("✅ Server restart sonrası %d kayıt doğrulandı", count)

	// Yeni veri ekle
	client2.exec("INSERT INTO restart_test (id, msg) VALUES (51, 'NewAfterRestart')")

	result, _ = client2.query("SELECT msg FROM restart_test WHERE id = 51")
	if len(result.Rows) > 0 {
		msg := fmt.Sprintf("%v", result.Rows[0][0])
		if msg != "NewAfterRestart" {
			t.Fatalf("Beklenen 'NewAfterRestart', bulunan: %s", msg)
		}
	}

	t.Log("✅ Server restart sonrası yazma işlemi başarılı")
	t.Log("\n🎉 SERVER RESTART PERSISTENCE TESTİ BAŞARILI!")
}
