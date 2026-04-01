// cobaltdb-migrate is a database migration tool for CobaltDB
package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/cobaltdb/cobaltdb/sdk/go"
)

// Migration represents a single migration
type Migration struct {
	Version   int64
	Name      string
	UpSQL     string
	DownSQL   string
	Timestamp time.Time
}

// MigrationRecord tracks applied migrations
type MigrationRecord struct {
	Version   int64     `db:"version"`
	Name      string    `db:"name"`
	AppliedAt time.Time `db:"applied_at"`
}

func main() {
	var (
		dsn     = flag.String("dsn", "host=localhost port=4200", "Database connection string")
		dir     = flag.String("dir", "./migrations", "Migrations directory")
		command = flag.String("cmd", "up", "Command: up, down, status, create")
		version = flag.Int64("version", 0, "Target version for up/down")
		name    = flag.String("name", "", "Migration name for create command")
	)
	flag.Parse()

	db, err := sql.Open("cobaltdb", *dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Ensure migrations table exists
	if err := ensureMigrationsTable(db); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create migrations table: %v\n", err)
		os.Exit(1)
	}

	switch *command {
	case "up":
		err = migrateUp(db, *dir, *version)
	case "down":
		err = migrateDown(db, *dir, *version)
	case "status":
		err = showStatus(db, *dir)
	case "create":
		if *name == "" {
			fmt.Fprintf(os.Stderr, "Migration name required\n")
			os.Exit(1)
		}
		err = createMigration(*dir, *name)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", *command)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func ensureMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	return err
}

func migrateUp(db *sql.DB, dir string, targetVersion int64) error {
	migrations, err := loadMigrations(dir)
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	if len(migrations) == 0 {
		fmt.Println("No migrations found")
		return nil
	}

	currentVersion, err := getCurrentVersion(db)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	for _, m := range migrations {
		// Skip if already applied
		if m.Version <= currentVersion {
			continue
		}

		// Stop if target version reached
		if targetVersion > 0 && m.Version > targetVersion {
			break
		}

		// Apply migration
		fmt.Printf("Applying migration %d: %s\n", m.Version, m.Name)

		if err := applyMigration(db, m); err != nil {
			return fmt.Errorf("failed to apply migration %d: %w", m.Version, err)
		}

		fmt.Printf("✓ Applied migration %d\n", m.Version)
	}

	fmt.Println("\nMigrations complete!")
	return nil
}

func migrateDown(db *sql.DB, dir string, targetVersion int64) error {
	migrations, err := loadMigrations(dir)
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	currentVersion, err := getCurrentVersion(db)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	if targetVersion == 0 {
		return fmt.Errorf("target version required for down migration")
	}

	// Sort migrations in reverse order for down
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version > migrations[j].Version
	})

	for _, m := range migrations {
		// Skip if not applied yet
		if m.Version > currentVersion {
			continue
		}

		// Stop if target version reached
		if m.Version <= targetVersion {
			break
		}

		// Apply down migration
		fmt.Printf("Reverting migration %d: %s\n", m.Version, m.Name)

		if err := revertMigration(db, m); err != nil {
			return fmt.Errorf("failed to revert migration %d: %w", m.Version, err)
		}

		fmt.Printf("✓ Reverted migration %d\n", m.Version)
	}

	fmt.Println("\nRollback complete!")
	return nil
}

func showStatus(db *sql.DB, dir string) error {
	migrations, err := loadMigrations(dir)
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	applied, err := getAppliedMigrations(db)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	fmt.Println("\nMigration Status")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("%-10s %-30s %-20s\n", "Version", "Name", "Status")
	fmt.Println(strings.Repeat("-", 60))

	for _, m := range migrations {
		status := "Pending"
		if _, ok := applied[m.Version]; ok {
			status = "Applied"
		}
		fmt.Printf("%-10d %-30s %-20s\n", m.Version, m.Name, status)
	}

	fmt.Println()
	return nil
}

func createMigration(dir, name string) error {
	// Create migrations directory if needed
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create migrations directory: %w", err)
	}

	// Get next version number
	version, err := getNextVersion(dir)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}

	// Sanitize name
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "_")

	// Create migration files
	timestamp := time.Now().Format("20060102150405")
	baseName := fmt.Sprintf("%s_%s", timestamp, name)

	upFile := filepath.Join(dir, fmt.Sprintf("%s_up.sql", baseName))
	downFile := filepath.Join(dir, fmt.Sprintf("%s_down.sql", baseName))

	// Create up file
	upContent := fmt.Sprintf("-- Migration: %s\n-- Version: %d\n\n", name, version)
	if err := os.WriteFile(upFile, []byte(upContent), 0644); err != nil {
		return fmt.Errorf("failed to create up file: %w", err)
	}

	// Create down file
	downContent := fmt.Sprintf("-- Rollback: %s\n-- Version: %d\n\n", name, version)
	if err := os.WriteFile(downFile, []byte(downContent), 0644); err != nil {
		return fmt.Errorf("failed to create down file: %w", err)
	}

	fmt.Printf("Created migration files:\n")
	fmt.Printf("  %s\n", upFile)
	fmt.Printf("  %s\n", downFile)

	return nil
}

func loadMigrations(dir string) ([]Migration, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var migrations []Migration

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()
		if !strings.HasSuffix(name, "_up.sql") {
			continue
		}

		// Parse version from filename (timestamp format: YYYYMMDDHHMMSS)
		parts := strings.Split(name, "_")
		if len(parts) < 2 {
			continue
		}

		timestamp := parts[0]
		version, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil {
			continue
		}

		// Load up SQL
		upPath := filepath.Join(dir, name)
		upSQL, err := os.ReadFile(upPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", name, err)
		}

		// Load down SQL
		downName := strings.Replace(name, "_up.sql", "_down.sql", 1)
		downPath := filepath.Join(dir, downName)
		downSQL := []byte{}
		if _, err := os.Stat(downPath); err == nil {
			downSQL, _ = os.ReadFile(downPath)
		}

		// Extract migration name from filename
		migName := strings.TrimSuffix(strings.TrimPrefix(name, timestamp+"_"), "_up.sql")
		migName = strings.ReplaceAll(migName, "_", " ")

		migrations = append(migrations, Migration{
			Version:   version,
			Name:      migName,
			UpSQL:     string(upSQL),
			DownSQL:   string(downSQL),
			Timestamp: parseTimestamp(timestamp),
		})
	}

	// Sort by version
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

func applyMigration(db *sql.DB, m Migration) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Execute migration SQL
	if _, err := tx.Exec(m.UpSQL); err != nil {
		return err
	}

	// Record migration
	_, err = tx.Exec(
		"INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
		m.Version, m.Name,
	)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func revertMigration(db *sql.DB, m Migration) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Execute down SQL
	if m.DownSQL != "" {
		if _, err := tx.Exec(m.DownSQL); err != nil {
			return err
		}
	}

	// Remove migration record
	_, err = tx.Exec("DELETE FROM schema_migrations WHERE version = ?", m.Version)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func getCurrentVersion(db *sql.DB) (int64, error) {
	var version int64
	err := db.QueryRow(
		"SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
	).Scan(&version)
	return version, err
}

func getAppliedMigrations(db *sql.DB) (map[int64]MigrationRecord, error) {
	rows, err := db.Query("SELECT version, name, applied_at FROM schema_migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	migrations := make(map[int64]MigrationRecord)
	for rows.Next() {
		var r MigrationRecord
		if err := rows.Scan(&r.Version, &r.Name, &r.AppliedAt); err != nil {
			return nil, err
		}
		migrations[r.Version] = r
	}

	return migrations, rows.Err()
}

func getNextVersion(dir string) (int64, error) {
	migrations, err := loadMigrations(dir)
	if err != nil {
		return 1, err
	}

	if len(migrations) == 0 {
		return 1, nil
	}

	maxVersion := int64(0)
	for _, m := range migrations {
		if m.Version > maxVersion {
			maxVersion = m.Version
		}
	}

	return maxVersion + 1, nil
}

func parseTimestamp(ts string) time.Time {
	t, _ := time.Parse("20060102150405", ts)
	return t
}

// Interactive mode for creating migrations
//
//nolint:unused // kept for future interactive CLI mode.
func interactiveCreate(dir string) error {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Migration name: ")
	name, _ := reader.ReadString('\n')
	name = strings.TrimSpace(name)

	if name == "" {
		return fmt.Errorf("migration name cannot be empty")
	}

	return createMigration(dir, name)
}
