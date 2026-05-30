package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/protocol"
	_ "github.com/go-sql-driver/mysql"
)

func TestMySQLGoSQLDriverCompatibility(t *testing.T) {
	engineDB, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	defer engineDB.Close()

	srv := protocol.NewMySQLServer(engineDB, "5.7.0-CobaltDB-Test")
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Close()

	dsn := fmt.Sprintf("admin@tcp(%s)/?timeout=3s&readTimeout=3s&writeTimeout=3s", srv.Addr().String())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE driver_users (id INTEGER PRIMARY KEY, name TEXT, score REAL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO driver_users VALUES (1, 'alice', 91.5)"); err != nil {
		t.Fatalf("text INSERT: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO driver_users VALUES (?, ?, ?)", 2, "bob", 82.25); err != nil {
		t.Fatalf("prepared INSERT: %v", err)
	}

	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM driver_users").Scan(&count); err != nil {
		t.Fatalf("COUNT query: %v", err)
	}
	if count != 2 {
		t.Fatalf("Expected count 2, got %d", count)
	}

	var name string
	var score float64
	if err := db.QueryRowContext(ctx, "SELECT name, score FROM driver_users WHERE id = ?", 2).Scan(&name, &score); err != nil {
		t.Fatalf("prepared SELECT: %v", err)
	}
	if name != "bob" || score != 82.25 {
		t.Fatalf("Unexpected prepared SELECT row: name=%q score=%v", name, score)
	}
}
