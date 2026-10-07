package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMigrationDownUsesTerminalSuffix(t *testing.T) {
	dir := t.TempDir()
	cases := []struct{ base, down string }{{"0001_normal", "SELECT 2"}, {"0002_fix_up.sql_name", "SELECT 4"}, {"0003_up.sql_up.sql_name", "SELECT 6"}, {"0004_missing", ""}}
	for _, tc := range cases {
		if err := os.WriteFile(filepath.Join(dir, tc.base+"_up.sql"), []byte("SELECT 1"), 0600); err != nil {
			t.Fatal(err)
		}
		if tc.down != "" {
			if err := os.WriteFile(filepath.Join(dir, tc.base+"_down.sql"), []byte(tc.down), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	ms, err := loadMigrations(dir)
	if err != nil || len(ms) != len(cases) {
		t.Fatal("load", ms, err)
	}
	for i, tc := range cases {
		fmt.Printf("EXPECTED: %q; ACTUAL: %q\n", tc.down, ms[i].DownSQL)
		if ms[i].DownSQL != tc.down {
			t.Fatal("wrong rollback", tc.base)
		}
	}
	state := &fakeMigState{applied: map[int64]bool{1: true, 2: true, 3: true}}
	db := openFakeMigrateDB(t, state)
	defer db.Close()
	if err := migrateDown(db, dir, 1); err != nil {
		t.Fatal(err)
	}
	executed := state.snapshotExecuted()
	if !stateContains(executed, "SELECT 4") || !stateContains(executed, "SELECT 6") {
		t.Fatal("rollback SQL skipped", executed)
	}
	empty, err := loadMigrations(t.TempDir())
	if err != nil || len(empty) != 0 {
		t.Fatal("empty directory", err)
	}
	fmt.Println("FIX VERIFIED")
}
