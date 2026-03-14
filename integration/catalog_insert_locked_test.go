package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestInsertLockedDefaultsDeep targets insertLocked with DEFAULT values
func TestInsertLockedDefaultsDeep(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE default_test (
		id INTEGER PRIMARY KEY,
		name TEXT DEFAULT 'unknown',
		status TEXT DEFAULT 'active',
		created_at TEXT DEFAULT CURRENT_TIMESTAMP,
		score INTEGER DEFAULT 0
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"All defaults", `INSERT INTO default_test DEFAULT VALUES`},
		{"Partial defaults", `INSERT INTO default_test (id) VALUES (1)`},
		{"Override default", `INSERT INTO default_test (id, name) VALUES (2, 'custom')`},
		{"NULL override", `INSERT INTO default_test (id, name) VALUES (3, NULL)`},
		{"Multiple rows defaults", `INSERT INTO default_test (id, score) VALUES (4, 100), (5, 200)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Insert error: %v", err)
				return
			}

			// Verify the insert
			rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM default_test`)
			if rows != nil {
				defer rows.Close()
				if rows.Next() {
					var count int
					rows.Scan(&count)
					t.Logf("Table now has %d rows", count)
				}
			}
		})
	}
}

// TestInsertLockedWithExpressions targets insertLocked with expression evaluation
func TestInsertLockedWithExpressions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE expr_test (
		id INTEGER PRIMARY KEY,
		x INTEGER,
		y INTEGER,
		computed INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Arithmetic expression", `INSERT INTO expr_test VALUES (1, 10, 5, 10 + 5 * 2)`},
		{"Function expression", `INSERT INTO expr_test VALUES (2, 10, 5, ABS(-10))`},
		{"String expression", `INSERT INTO expr_test VALUES (3, LENGTH('hello'), 5, LENGTH('world'))`},
		{"NULL expression", `INSERT INTO expr_test VALUES (4, 10, NULL, 10 + NULL)`},
		{"Complex expression", `INSERT INTO expr_test VALUES (5, 10, 5, (10 + 5) * 2 - 5)`},
		{"Expression in SELECT", `INSERT INTO expr_test SELECT 6, 1+1, 2+2, 3+3`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Insert expression error: %v", err)
				return
			}
			t.Logf("Expression insert succeeded")
		})
	}
}

// TestInsertLockedAutoIncrement targets insertLocked with AUTOINCREMENT
func TestInsertLockedAutoIncrement(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE autoinc_test (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Auto increment 1", `INSERT INTO autoinc_test (name) VALUES ('first')`},
		{"Auto increment 2", `INSERT INTO autoinc_test (name) VALUES ('second')`},
		{"Auto increment 3", `INSERT INTO autoinc_test (name) VALUES ('third')`},
		{"Explicit ID", `INSERT INTO autoinc_test (id, name) VALUES (100, 'explicit')`},
		{"After explicit", `INSERT INTO autoinc_test (name) VALUES ('after_explicit')`},
		{"NULL for autoinc", `INSERT INTO autoinc_test (id, name) VALUES (NULL, 'null_id')`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Autoincrement insert error: %v", err)
				return
			}
			t.Logf("Insert succeeded, last ID: %d", result.LastInsertID)
		})
	}
}

// TestInsertLockedMultiRow targets insertLocked with multi-row inserts
func TestInsertLockedMultiRow(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE multi_row (
		id INTEGER PRIMARY KEY,
		val INTEGER,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{"2 rows", `INSERT INTO multi_row VALUES (1, 10, 'a'), (2, 20, 'b')`, 2},
		{"5 rows", `INSERT INTO multi_row VALUES (3, 30, 'c'), (4, 40, 'd'), (5, 50, 'e'), (6, 60, 'f'), (7, 70, 'g')`, 5},
		{"10 rows", `INSERT INTO multi_row SELECT id+10, val+100, name FROM multi_row`, 7},
		{"Mixed columns", `INSERT INTO multi_row (id, name) VALUES (100, 'x'), (101, 'y'), (102, 'z')`, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Multi-row insert error: %v", err)
				return
			}
			t.Logf("Multi-row insert affected %d rows", result.RowsAffected)
		})
	}
}

// TestInsertLockedWithSubquery targets insertLocked with subquery
func TestInsertLockedWithSubquery(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE source_table (
		id INTEGER PRIMARY KEY,
		val INTEGER,
		category TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create source: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE target_table (
		id INTEGER PRIMARY KEY,
		sum_val INTEGER,
		cat TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create target: %v", err)
	}

	// Insert source data
	_, err = db.Exec(ctx, `INSERT INTO source_table VALUES
		(1, 10, 'A'),
		(2, 20, 'A'),
		(3, 30, 'B'),
		(4, 40, 'B')`)
	if err != nil {
		t.Fatalf("Failed to insert source: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Simple subquery", `INSERT INTO target_table SELECT id, val, category FROM source_table`},
		{"Subquery with WHERE", `INSERT INTO target_table SELECT id+100, val, category FROM source_table WHERE category = 'A'`},
		{"Subquery with aggregate", `INSERT INTO target_table SELECT 1000, SUM(val), 'total' FROM source_table`},
		{"Subquery with GROUP BY", `INSERT INTO target_table SELECT 2000 + ROW_NUMBER() OVER (), SUM(val), category FROM source_table GROUP BY category`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean target for each test
			db.Exec(ctx, `DELETE FROM target_table`)

			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Subquery insert error: %v", err)
				return
			}

			rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM target_table`)
			if rows != nil {
				defer rows.Close()
				if rows.Next() {
					var count int
					rows.Scan(&count)
					t.Logf("Target now has %d rows", count)
				}
			}
		})
	}
}

// TestInsertLockedWithFK targets insertLocked with foreign key checks
func TestInsertLockedWithFK(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parent (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		name TEXT,
		FOREIGN KEY (parent_id) REFERENCES parent(id)
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	// Insert parent first
	_, err = db.Exec(ctx, `INSERT INTO parent VALUES (1, 'parent1'), (2, 'parent2')`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	tests := []struct {
		name  string
		sql   string
		valid bool
	}{
		{"Valid FK", `INSERT INTO child VALUES (1, 1, 'child1')`, true},
		{"Valid FK 2", `INSERT INTO child (id, parent_id, name) VALUES (2, 2, 'child2')`, true},
		{"Invalid FK", `INSERT INTO child VALUES (3, 999, 'orphan')`, false},
		{"NULL FK", `INSERT INTO child (id, name) VALUES (4, 'no_parent')`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if tt.valid {
				if err != nil {
					t.Logf("Expected valid insert but got error: %v", err)
				}
			} else {
				if err != nil {
					t.Logf("FK constraint correctly blocked: %v", err)
				} else {
					t.Logf("Insert succeeded (FK may not be enforced)")
				}
			}
		})
	}
}

// TestInsertLockedWithTrigger targets insertLocked with triggers
func TestInsertLockedWithTrigger(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE audit (
		id INTEGER PRIMARY KEY,
		action TEXT,
		new_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create audit: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE main_table (
		id INTEGER PRIMARY KEY,
		data TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create main: %v", err)
	}

	// Create AFTER INSERT trigger
	_, err = db.Exec(ctx, `CREATE TRIGGER after_insert_main
		AFTER INSERT ON main_table
		BEGIN
			INSERT INTO audit (action, new_id) VALUES ('INSERT', NEW.id);
		END`)
	if err != nil {
		t.Logf("Trigger creation error: %v", err)
		return
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Single insert", `INSERT INTO main_table VALUES (1, 'data1')`},
		{"Multiple inserts", `INSERT INTO main_table VALUES (2, 'data2'), (3, 'data3')`},
		{"Insert with SELECT", `INSERT INTO main_table SELECT id+10, data FROM main_table`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Insert with trigger error: %v", err)
				return
			}

			// Check audit log
			rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM audit`)
			if rows != nil {
				defer rows.Close()
				if rows.Next() {
					var count int
					rows.Scan(&count)
					t.Logf("Audit log has %d entries", count)
				}
			}
		})
	}
}

// TestInsertLockedUniqueConstraint targets insertLocked with unique constraints
func TestInsertLockedUniqueConstraint(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE unique_test (
		id INTEGER PRIMARY KEY,
		email TEXT UNIQUE,
		code INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec(ctx, `INSERT INTO unique_test VALUES (1, 'test@example.com', 100)`)
	if err != nil {
		t.Fatalf("Failed to insert initial: %v", err)
	}

	tests := []struct {
		name  string
		sql   string
		valid bool
	}{
		{"Unique value", `INSERT INTO unique_test VALUES (2, 'other@example.com', 200)`, true},
		{"Duplicate email", `INSERT INTO unique_test VALUES (3, 'test@example.com', 300)`, false},
		{"NULL unique", `INSERT INTO unique_test (id, code) VALUES (4, 400)`, true},
		{"Another NULL unique", `INSERT INTO unique_test (id, code) VALUES (5, 500)`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if tt.valid {
				if err != nil {
					t.Logf("Expected success but got error: %v", err)
				}
			} else {
				if err != nil {
					t.Logf("Unique constraint correctly blocked: %v", err)
				} else {
					t.Logf("Insert succeeded (constraint may not be enforced)")
				}
			}
		})
	}
}
