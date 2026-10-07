package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrationVersionsAreUniqueBeforeExecution(t *testing.T) {
	controlDir := t.TempDir()
	writeMigrations(t, controlDir, 2, 1)
	ms, err := loadMigrations(controlDir)
	if err != nil || len(ms) != 2 || ms[0].Version != 1 || ms[1].Version != 2 {
		t.Fatal("distinct control", ms, err)
	}
	for _, names := range [][]string{{"1_first_up.sql", "1_second_up.sql"}, {"1_first_up.sql", "01_second_up.sql"}, {"20240102030405_first_up.sql", "20240102030405_second_up.sql"}} {
		dir := t.TempDir()
		for _, name := range names {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("CREATE TABLE ambiguous (id INTEGER)"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		ms, err = loadMigrations(dir)
		fmt.Printf("EXPECTED: duplicate-version error; ACTUAL: %v\n", err)
		if err == nil || ms != nil || !strings.Contains(err.Error(), "duplicate migration version") {
			t.Fatal("ambiguous versions accepted", ms, err)
		}
		for _, name := range names {
			if !strings.Contains(err.Error(), name) {
				t.Fatal("missing conflicting filename", name, err)
			}
		}
		state := &fakeMigState{applied: map[int64]bool{1: true}}
		db := openFakeMigrateDB(t, state)
		for _, operation := range []func() error{func() error { return migrateUp(db, dir, 0) }, func() error { return migrateDown(db, dir, 1) }, func() error { return showStatus(db, dir) }} {
			if err := operation(); err == nil || !strings.Contains(err.Error(), "duplicate migration version") {
				t.Fatal("ambiguous operation did not fail", err)
			}
		}
		if len(state.snapshotExecuted()) != 0 || len(state.applied) != 1 || !state.applied[1] {
			t.Fatal("SQL or migration records changed before rejection")
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{t.TempDir(), filepath.Join(t.TempDir(), "missing")} {
		ms, err := loadMigrations(dir)
		if err != nil || len(ms) != 0 {
			t.Fatal("empty directory edge", ms, err)
		}
	}
	fmt.Println("FIX VERIFIED")
}
