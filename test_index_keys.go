package main

import (
	"context"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func main() {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table with UNIQUE
	_, err = db.Exec(ctx, `CREATE TABLE test_emails (
		id INTEGER PRIMARY KEY,
		email TEXT UNIQUE,
		name TEXT
	)`)
	if err != nil {
		panic(err)
	}
	fmt.Println("=== Created table ===")

	// Insert alice
	fmt.Println("\n=== INSERT alice ===")
	_, err = db.Exec(ctx, "INSERT INTO test_emails VALUES (1, 'alice@example.com', 'Alice')")
	if err != nil {
		panic(err)
	}

	// Delete alice
	fmt.Println("\n=== DELETE alice ===")
	_, err = db.Exec(ctx, "DELETE FROM test_emails WHERE id = 1")
	if err != nil {
		panic(err)
	}

	// Try to re-insert alice
	fmt.Println("\n=== RE-INSERT alice ===")
	_, err = db.Exec(ctx, "INSERT INTO test_emails VALUES (2, 'alice@example.com', 'Alice-New')")
	if err != nil {
		fmt.Println("ERROR:", err)
	} else {
		fmt.Println("SUCCESS")
	}
}
