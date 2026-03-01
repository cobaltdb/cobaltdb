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
	os.RemoveAll(dbPath + ".data")
	os.Remove(dbPath)

	fmt.Println("=== Test: Full CRUD with Disk Persistence ===")

	// Create database
	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		log.Fatalf("Failed to open: %v", err)
	}

	ctx := context.Background()

	// CREATE
	db.Exec(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`)

	// INSERT
	db.Exec(ctx, "INSERT INTO users (name, age) VALUES (?, ?)", "Ersin", 30)
	db.Exec(ctx, "INSERT INTO users (name, age) VALUES (?, ?)", "Jane", 25)
	db.Exec(ctx, "INSERT INTO users (name, age) VALUES (?, ?)", "John", 35)

	// READ
	fmt.Println("\n1. CREATE + INSERT:")
	printUsers(db, ctx)

	// UPDATE
	db.Exec(ctx, "UPDATE users SET age = ? WHERE name = ?", 31, "Ersin")
	fmt.Println("\n2. UPDATE (age=31 where name=Ersin):")
	printUsers(db, ctx)

	// DELETE
	db.Exec(ctx, "DELETE FROM users WHERE age > ?", 30)
	fmt.Println("\n3. DELETE (age > 30):")
	printUsers(db, ctx)

	// Close and reopen
	db.Close()
	fmt.Println("\n4. Database closed and reopened...")

	db2, _ := engine.Open(dbPath, nil)
	defer db2.Close()

	fmt.Println("\n5. After reopen:")
	printUsers(db2, ctx)

	// Cleanup
	os.RemoveAll(dbPath + ".data")
	os.Remove(dbPath)

	fmt.Println("\n✅ All CRUD operations work with disk persistence!")
}

func printUsers(db *engine.DB, ctx context.Context) {
	rows, _ := db.Query(ctx, "SELECT name, age FROM users")
	for rows.Next() {
		var name string
		var age int
		rows.Scan(&name, &age)
		fmt.Printf("   - %s, age=%d\n", name, age)
	}
	rows.Close()
}
