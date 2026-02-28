package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func main() {
	fmt.Println("🚀 CobaltDB Example")
	fmt.Println("==================")
	fmt.Println()

	// Open in-memory database
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	fmt.Println("1. Creating table 'users'...")
	_, err = db.Exec(ctx, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("   ✅ Table created!")
	fmt.Println()

	// Insert data
	fmt.Println("2. Inserting users...")
	users := []struct {
		name  string
		email string
	}{
		{"Ersin", "ersin@cobaltdb.dev"},
		{"Jane", "jane@example.com"},
		{"John", "john@example.com"},
	}

	for _, user := range users {
		result, err := db.Exec(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", user.name, user.email)
		if err != nil {
			log.Fatalf("Failed to insert: %v", err)
		}
		fmt.Printf("   ✅ Inserted %s (rows affected: %d)\n", user.name, result.RowsAffected)
	}
	fmt.Println()

	// Query all users
	fmt.Println("3. Querying all users...")
	rows, err := db.Query(ctx, "SELECT name, email FROM users")
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	fmt.Println("   Columns:", rows.Columns())
	for rows.Next() {
		var name, email string
		if err := rows.Scan(&name, &email); err != nil {
			log.Fatalf("Failed to scan: %v", err)
		}
		fmt.Printf("   - %s <%s>\n", name, email)
	}
	fmt.Println()

	// Transaction example
	fmt.Println("4. Transaction example...")
	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Alice", "alice@example.com")
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to insert in transaction: %v", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
	fmt.Println("   ✅ Transaction committed!")
	fmt.Println()

	// Count users
	fmt.Println("5. Counting users...")
	rows, err = db.Query(ctx, "SELECT name FROM users")
	if err != nil {
		log.Fatalf("Failed to count: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		rows.Scan(new(string))
	}
	fmt.Printf("   Total users: %d\n", count)
	fmt.Println()

	fmt.Println("✨ Example completed successfully!")
}
