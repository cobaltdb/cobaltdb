package main

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestMigrationWritesTimestampAndRollsBackFailure(t *testing.T) {
	db, err := sql.Open("cobaltdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := ensureMigrationsTable(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO schema_migrations (version, name, applied_at) VALUES (1, 'control', '2024-01-02 03:04:05')"); err != nil {
		t.Fatal(err)
	}
	var control string
	if err := db.QueryRow("SELECT applied_at FROM schema_migrations WHERE version = 1").Scan(&control); err != nil {
		t.Fatal(err)
	}
	if _, err := time.Parse("2006-01-02 15:04:05", control); err != nil {
		t.Fatal("control failed", err)
	}
	fmt.Println("CONTROL EXPECTED: valid explicit timestamp; ACTUAL: matched")
	if err := applyMigration(db, Migration{Version: 2, Name: "second", UpSQL: "CREATE TABLE second (id INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := db.QueryRow("SELECT applied_at FROM schema_migrations WHERE version = 2").Scan(&got); err != nil {
		t.Fatal(err)
	}
	_, err = time.Parse("2006-01-02 15:04:05", got)
	fmt.Printf("EXPECTED: valid SQL timestamp; ACTUAL: %q error=%v\n", got, err)
	if err != nil {
		fmt.Println("PROBLEM CONFIRMED")
		t.FailNow()
	} // Duplicate DDL and invalid SQL must roll back without adding a record.
	for _, sql := range []string{"CREATE TABLE second (id INTEGER)", "NOT SQL", ""} {
		if err := applyMigration(db, Migration{Version: 3, Name: "failed", UpSQL: sql}); err == nil {
			t.Fatal("expected migration failure")
		}
		ms, err := getAppliedMigrations(db)
		if err != nil || len(ms) != 2 {
			t.Fatal("failed migration changed records", ms, err)
		}
	}
	// A subsequent successful migration remains possible after each rollback.
	if err := applyMigration(db, Migration{Version: 3, Name: "third", UpSQL: "CREATE TABLE third (id INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	ms, err := getAppliedMigrations(db)
	if err != nil || len(ms) != 3 || ms[3].AppliedAt.IsZero() {
		t.Fatal("subsequent migration", ms, err)
	}
	fmt.Println("FIX VERIFIED")
}
