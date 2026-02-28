package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func main() {
	dbPath := "./test.cobalt"

	// Remove existing database
	os.RemoveAll(dbPath + ".data")
	os.Remove(dbPath)

	fmt.Println("=== Test: Disk Persistence ===")

	// Open database
	db, err := engine.Open(dbPath, &engine.Options{
		InMemory:  false,
		CacheSize: 1024,
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	ctx := context.Background()

	// Create table
	db.Exec(ctx, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		)
	`)

	// Insert data
	db.Exec(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Ersin", "ersin@test.dev")
	db.Exec(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Jane", "jane@test.dev")

	// Query before close
	rows, _ := db.Query(ctx, "SELECT name, email FROM users")
	fmt.Println("Before close:")
	for rows.Next() {
		var name, email string
		rows.Scan(&name, &email)
		fmt.Printf("  - %s <%s>\n", name, email)
	}
	rows.Close()

	// Close database
	db.Close()
	fmt.Println("Database closed")

	// Reopen database
	fmt.Println("\nAfter reopen:")
	db2, err := engine.Open(dbPath, nil)
	if err != nil {
		log.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	rows2, _ := db2.Query(ctx, "SELECT name, email FROM users")
	count := 0
	for rows2.Next() {
		count++
		var name, email string
		rows2.Scan(&name, &email)
		fmt.Printf("  - %s <%s>\n", name, email)
	}
	rows2.Close()

	// Cleanup
	os.RemoveAll(dbPath + ".data")
	os.Remove(dbPath)

	if count == 2 {
		fmt.Printf("\n✅ SUCCESS: %d users loaded from disk!\n", count)
	} else {
		fmt.Printf("\n❌ FAILURE: Expected 2 users, got %d\n", count)
	}
}
