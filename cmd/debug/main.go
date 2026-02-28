package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func main() {
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
	db.Exec(ctx, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			age INTEGER
		)
	`)

	// Insert data
	db.Exec(ctx, "INSERT INTO users (name, age) VALUES (?, ?)", "Ersin", 30)
	db.Exec(ctx, "INSERT INTO users (name, age) VALUES (?, ?)", "Jane", 25)
	db.Exec(ctx, "INSERT INTO users (name, age) VALUES (?, ?)", "John", 35)

	// Test WHERE clause
	fmt.Println("Test 1: WHERE age > 28")
	rows, err := db.Query(ctx, "SELECT name, age FROM users WHERE age > 28")
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for rows.Next() {
			var name string
			var age int
			rows.Scan(&name, &age)
			fmt.Printf("  - %s, age=%d\n", name, age)
		}
	}

	fmt.Println("\nTest 2: WHERE name = 'Jane'")
	rows, err = db.Query(ctx, "SELECT name, age FROM users WHERE name = 'Jane'")
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for rows.Next() {
			var name string
			var age int
			rows.Scan(&name, &age)
			fmt.Printf("  - %s, age=%d\n", name, age)
		}
	}
}
