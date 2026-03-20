// Example: CobaltDB as a standalone MySQL-compatible database server
//
// This example starts a CobaltDB server that accepts MySQL client connections.
// Connect with: mysql -h 127.0.0.1 -P 3307 -u admin
//
// Or use any MySQL driver:
//   - Go: database/sql + go-sql-driver/mysql
//   - Python: mysql-connector-python, SQLAlchemy
//   - Node.js: mysql2, Prisma
//   - Java: JDBC mysql-connector
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/protocol"
)

func main() {
	fmt.Println("╔════════════════════════════════════════════╗")
	fmt.Println("║   CobaltDB MySQL Server Example            ║")
	fmt.Println("╚════════════════════════════════════════════╝")
	fmt.Println()

	// Open in-memory database (use a file path for persistence)
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create some sample data
	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE,
		department TEXT
	)`)
	db.Exec(ctx, "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 'Engineering')")
	db.Exec(ctx, "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 'Marketing')")
	db.Exec(ctx, "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 'Engineering')")

	db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		user_id INTEGER,
		amount REAL,
		status TEXT DEFAULT 'pending',
		FOREIGN KEY (user_id) REFERENCES users(id)
	)`)
	db.Exec(ctx, "INSERT INTO orders VALUES (1, 1, 99.99, 'completed')")
	db.Exec(ctx, "INSERT INTO orders VALUES (2, 1, 149.50, 'pending')")
	db.Exec(ctx, "INSERT INTO orders VALUES (3, 2, 25.00, 'completed')")

	fmt.Println("Sample data loaded: 3 users, 3 orders")
	fmt.Println()

	// Start MySQL protocol server
	mysqlServer := protocol.NewMySQLServer(db, "5.7.0-CobaltDB")
	if err := mysqlServer.Listen(":3307"); err != nil {
		log.Fatalf("Failed to start MySQL server: %v", err)
	}
	defer mysqlServer.Close()

	fmt.Println("MySQL server listening on :3307")
	fmt.Println()
	fmt.Println("Connect with:")
	fmt.Println("  mysql -h 127.0.0.1 -P 3307 -u admin")
	fmt.Println()
	fmt.Println("Try these queries:")
	fmt.Println("  SELECT * FROM users;")
	fmt.Println("  SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name;")
	fmt.Println("  SELECT department, COUNT(*) FROM users GROUP BY department;")
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop.")

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
}
