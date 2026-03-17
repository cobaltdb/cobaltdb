// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func main() {
	fmt.Println("=== Index Performance Test ===")
	
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table with index
	db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	db.Exec(ctx, "CREATE INDEX idx_email ON users(email)")

	// Insert 1000 rows
	fmt.Println("Inserting 1000 rows...")
	start := time.Now()
	for i := 0; i < 1000; i++ {
		email := fmt.Sprintf("user%d@example.com", i)
		db.Exec(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", fmt.Sprintf("User%d", i), email)
	}
	fmt.Printf("Insert time: %v\n", time.Since(start))

	// Query with index (WHERE email = ?)
	fmt.Println("\nQuery with indexed column (email)...")
	start = time.Now()
	rows, _ := db.Query(ctx, "SELECT * FROM users WHERE email = 'user500@example.com'")
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	queryTime := time.Since(start)
	fmt.Printf("Query time: %v (rows: %d)\n", queryTime, count)

	// Query without index (WHERE name = ?)
	fmt.Println("\nQuery without index (name)...")
	start = time.Now()
	rows, _ = db.Query(ctx, "SELECT * FROM users WHERE name = 'User500'")
	count = 0
	for rows.Next() {
		count++
	}
	rows.Close()
	queryTimeNoIndex := time.Since(start)
	fmt.Printf("Query time: %v (rows: %d)\n", queryTimeNoIndex, count)

	// Full table scan
	fmt.Println("\nFull table scan...")
	start = time.Now()
	rows, _ = db.Query(ctx, "SELECT * FROM users")
	count = 0
	for rows.Next() {
		count++
	}
	rows.Close()
	scanTime := time.Since(start)
	fmt.Printf("Full scan time: %v (rows: %d)\n", scanTime, count)

	if queryTime > 10*time.Millisecond {
		fmt.Println("\n⚠️  WARNING: Index query is slow! Index may not be used.")
	} else {
		fmt.Println("\n✓ Index query is fast")
	}
}
