package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestFDWCSVSelect(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a temp CSV file
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	content := "id,name,score\n1,alice,95\n2,bob,87\n3,charlie,92\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Create foreign table
	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext_users (id INTEGER, name TEXT, score INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`,
		csvPath,
	))
	if err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	// SELECT from foreign table
	rows, err := db.Query(ctx, `SELECT id, name, score FROM ext_users WHERE score > 90`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []struct {
		id    int
		name  string
		score int
	}
	for rows.Next() {
		var id, score int
		var name string
		if err := rows.Scan(&id, &name, &score); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, struct {
			id    int
			name  string
			score int
		}{id, name, score})
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(results))
	}
	if results[0].name != "alice" || results[0].score != 95 {
		t.Fatalf("Unexpected first row: %+v", results[0])
	}
	if results[1].name != "charlie" || results[1].score != 92 {
		t.Fatalf("Unexpected second row: %+v", results[1])
	}
}

func TestFDWDrop(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(csvPath, []byte("a,b\n1,2\n"), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext (a INTEGER, b INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`, csvPath,
	))
	if err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	_, err = db.Exec(ctx, `DROP TABLE ext`)
	if err != nil {
		t.Fatalf("Failed to drop foreign table: %v", err)
	}

	_, err = db.Query(ctx, `SELECT * FROM ext`)
	if err == nil {
		t.Fatal("Expected query on dropped foreign table to fail")
	}
}

func TestFDWJoinWithLocal(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create local table
	_, err = db.Exec(ctx, `CREATE TABLE local_depts (id INTEGER PRIMARY KEY, dept TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create local table: %v", err)
	}
	_, err = db.Exec(ctx, `INSERT INTO local_depts VALUES (1, 'engineering'), (2, 'sales')`)
	if err != nil {
		t.Fatalf("Failed to insert into local table: %v", err)
	}

	// Create foreign table
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "users.csv")
	content := "user_id,user_name,dept_id\n1,alice,1\n2,bob,2\n3,charlie,1\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext_users (user_id INTEGER, user_name TEXT, dept_id INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`, csvPath,
	))
	if err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	// JOIN foreign and local tables
	rows, err := db.Query(ctx, `
		SELECT u.user_name, d.dept
		FROM ext_users u
		JOIN local_depts d ON u.dept_id = d.id
		WHERE d.dept = 'engineering'
	`)
	if err != nil {
		t.Fatalf("JOIN query failed: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name, dept string
		if err := rows.Scan(&name, &dept); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		names = append(names, name)
	}

	if len(names) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(names))
	}
	if names[0] != "alice" || names[1] != "charlie" {
		t.Fatalf("Unexpected names: %v", names)
	}
}

func TestFDWInsertRejection(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(csvPath, []byte("a\n1\n"), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext (a INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`, csvPath,
	))
	if err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO ext VALUES (2)`)
	if err == nil {
		t.Fatal("Expected INSERT into foreign table to fail")
	}
}
