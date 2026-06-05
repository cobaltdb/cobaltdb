package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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

	// A prepared statement must report the real result column names, not
	// placeholder "col0"/"col1" labels.
	stmt, err := db.PrepareContext(ctx, "SELECT name, score FROM driver_users WHERE id = ?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, 2)
	if err != nil {
		t.Fatalf("prepared query: %v", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close rows: %v", err)
	}
	if len(cols) != 2 || cols[0] != "name" || cols[1] != "score" {
		t.Fatalf("prepared statement column names = %v, want [name score]", cols)
	}
}

// TestMySQLBinaryProtocolTypeRoundTrip exercises the binary result protocol
// (prepared-statement queries) across numeric, text, and NULL values, ensuring
// native fixed-width encoding round-trips correctly through a real driver.
func TestMySQLBinaryProtocolTypeRoundTrip(t *testing.T) {
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

	if _, err := db.ExecContext(ctx, "CREATE TABLE types_t (id INTEGER PRIMARY KEY, big BIGINT, dbl DOUBLE, txt TEXT, opt TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO types_t (id, big, dbl, txt, opt) VALUES (?, ?, ?, ?, ?)",
		7, int64(9000000000), 3.5, "hello", nil); err != nil {
		t.Fatalf("prepared INSERT: %v", err)
	}

	// A parameterized query forces the binary result protocol.
	var (
		id  int64
		big int64
		dbl float64
		txt string
		opt sql.NullString
	)
	row := db.QueryRowContext(ctx, "SELECT id, big, dbl, txt, opt FROM types_t WHERE id = ?", 7)
	if err := row.Scan(&id, &big, &dbl, &txt, &opt); err != nil {
		t.Fatalf("binary-protocol scan: %v", err)
	}
	if id != 7 {
		t.Fatalf("id = %d, want 7", id)
	}
	if big != 9000000000 { // exceeds int32 — proves 8-byte BIGINT encoding
		t.Fatalf("big = %d, want 9000000000", big)
	}
	if dbl != 3.5 {
		t.Fatalf("dbl = %v, want 3.5", dbl)
	}
	if txt != "hello" {
		t.Fatalf("txt = %q, want hello", txt)
	}
	if opt.Valid {
		t.Fatalf("opt = %v, want NULL", opt)
	}

	// Scanning the integer into a narrow Go int must also work.
	var idInt int
	if err := db.QueryRowContext(ctx, "SELECT id FROM types_t WHERE id = ?", 7).Scan(&idInt); err != nil {
		t.Fatalf("scan id into int: %v", err)
	}
	if idInt != 7 {
		t.Fatalf("idInt = %d, want 7", idInt)
	}
}

// TestMySQLServerErrorRobustness verifies the server returns proper error
// packets (not a masked "unsupported statement type") for failing read queries
// and never sends a malformed zero-column result set that crashes the client
// driver.
func TestMySQLServerErrorRobustness(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE robust (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	db.ExecContext(ctx, "INSERT INTO robust VALUES (1, 10)")

	// A query on a missing table must surface the real "not found" error.
	if _, err := db.ExecContext(ctx, "SELECT * FROM does_not_exist"); err == nil {
		t.Error("expected error for missing table")
	} else if strings.Contains(err.Error(), "unsupported statement type") {
		t.Errorf("missing-table error was masked: %v", err)
	}

	// A reference to a missing column must not crash the driver (zero-column
	// result handled gracefully) — the call returns without panicking.
	_, _ = db.ExecContext(ctx, "SELECT missing_col FROM robust")

	// The connection must remain usable after the error cases.
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM robust").Scan(&n); err != nil {
		t.Fatalf("connection unusable after errors: %v", err)
	}
	if n != 1 {
		t.Errorf("count = %d, want 1", n)
	}
}
