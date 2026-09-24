package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadMigrationsPropagatesDownReadError pins that loadMigrations fails
// when an existing down-file cannot be read, instead of silently degrading to
// "no down SQL" — which would let a rollback remove the migration record
// without rolling back the schema change, and a later "up" re-apply the
// migration against an already-migrated schema.
//
// Regression: loadMigrations used `downSQL, _ = os.ReadFile(downPath)` behind
// an os.Stat-exists guard, discarding real read errors. A missing down file
// remains deliberately lenient (empty DownSQL, record-only revert).
func TestLoadMigrationsPropagatesDownReadError(t *testing.T) {
	dir := t.TempDir()

	upPath := filepath.Join(dir, "20260101000000_add_users_up.sql")
	if err := os.WriteFile(upPath, []byte("CREATE TABLE users (id INTEGER)"), 0600); err != nil {
		t.Fatalf("write up file: %v", err)
	}
	// A DIRECTORY masquerading as the down file: os.Stat succeeds but
	// os.ReadFile fails with EISDIR on every platform and user.
	downPath := filepath.Join(dir, "20260101000000_add_users_down.sql")
	if err := os.Mkdir(downPath, 0750); err != nil {
		t.Fatalf("create down path: %v", err)
	}

	_, err := loadMigrations(dir)
	if err == nil {
		t.Fatal("loadMigrations swallowed the down-file read error — a rollback would drop the record without reverting the schema")
	}
	if !strings.Contains(err.Error(), "20260101000000_add_users_down.sql") {
		t.Fatalf("error %q does not identify the unreadable down file", err.Error())
	}
}

// TestLoadMigrationsMissingDownFileLenient pins the deliberate leniency: an
// up-file with NO down file loads with empty DownSQL and no error (the revert
// then removes the record only).
func TestLoadMigrationsMissingDownFileLenient(t *testing.T) {
	dir := t.TempDir()

	upPath := filepath.Join(dir, "20260101000000_add_users_up.sql")
	if err := os.WriteFile(upPath, []byte("CREATE TABLE users (id INTEGER)"), 0600); err != nil {
		t.Fatalf("write up file: %v", err)
	}

	migrations, err := loadMigrations(dir)
	if err != nil {
		t.Fatalf("loadMigrations with a missing down file: %v", err)
	}
	if len(migrations) != 1 {
		t.Fatalf("migrations = %d, want 1", len(migrations))
	}
	if migrations[0].DownSQL != "" {
		t.Fatalf("DownSQL = %q, want empty for a missing down file", migrations[0].DownSQL)
	}
	if migrations[0].Version != 20260101000000 {
		t.Fatalf("Version = %d, want 20260101000000 (parsed from the filename)", migrations[0].Version)
	}
}
