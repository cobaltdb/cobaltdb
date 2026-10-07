package main

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestMigrationAppliedTimestampWithSDK(t *testing.T) {
	db, err := sql.Open("cobaltdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := ensureMigrationsTable(db); err != nil {
		t.Fatal(err)
	}
	ms, err := getAppliedMigrations(db)
	if err != nil || len(ms) != 0 {
		t.Fatal("empty control", err)
	}
	if _, err := db.Exec("INSERT INTO schema_migrations (version, name, applied_at) VALUES (1, 'initial', '2024-01-02 03:04:05')"); err != nil {
		t.Fatal(err)
	}
	want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	ms, err = getAppliedMigrations(db)
	if err != nil || len(ms) != 1 || !ms[1].AppliedAt.Equal(want) {
		t.Fatal("timestamp scan", ms, err)
	}
	fmt.Println("EXPECTED: one migration with fixed timestamp; ACTUAL: matched")
	dir := t.TempDir()
	writeMigrations(t, dir, 2)
	if err := migrateUp(db, dir, 0); err != nil {
		t.Fatal(err)
	}
	if err := migrateUp(db, dir, 0); err != nil {
		t.Fatal("repeated up", err)
	}
	if err := showStatus(db, dir); err != nil {
		t.Fatal("status", err)
	}
	if err := migrateDown(db, dir, 1); err != nil {
		t.Fatal("down", err)
	}
	ms, err = getAppliedMigrations(db)
	if err != nil || len(ms) != 1 {
		t.Fatal("rollback records", ms, err)
	}
	for _, v := range []interface{}{want, "2024-01-02T03:04:05Z", []byte("2024-01-02 03:04:05")} {
		got, err := parseMigrationTime(v)
		if err != nil || !got.Equal(want) {
			t.Fatal("timestamp format", got, err)
		}
	}
	fractional, err := parseMigrationTime("2024-01-02 03:04:05.123456789")
	if err != nil || fractional.Nanosecond() != 123456789 {
		t.Fatal("fractional timestamp", err)
	}
	for _, v := range []interface{}{nil, int64(1), "invalid", []byte("invalid")} {
		if _, err := parseMigrationTime(v); err == nil {
			t.Fatal("invalid timestamp accepted", v)
		}
	}
	fmt.Println("FIX VERIFIED")
}
