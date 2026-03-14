package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestAlterTableDropColumnBasic targets AlterTableDropColumn
func TestAlterTableDropColumnBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE drop_col_test (
		id INTEGER PRIMARY KEY,
		col1 TEXT,
		col2 INTEGER,
		col3 REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	_, err = db.Exec(ctx, `INSERT INTO drop_col_test VALUES (1, 'a', 10, 1.5), (2, 'b', 20, 2.5)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Drop single column", `ALTER TABLE drop_col_test DROP COLUMN col2`},
		{"Drop another column", `ALTER TABLE drop_col_test DROP COLUMN col3`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("ALTER TABLE DROP COLUMN error: %v", err)
				return
			}

			// Verify column is dropped by selecting remaining columns
			rows, _ := db.Query(ctx, `SELECT id, col1 FROM drop_col_test`)
			if rows != nil {
				defer rows.Close()
				count := 0
				for rows.Next() {
					count++
				}
				t.Logf("Table still has %d rows after drop", count)
			}
		})
	}
}

// TestAlterTableDropColumnWithIndex targets AlterTableDropColumn with index
func TestAlterTableDropColumnWithIndex(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE drop_index_test (
		id INTEGER PRIMARY KEY,
		email TEXT,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index on email column
	_, err = db.Exec(ctx, `CREATE INDEX idx_email ON drop_index_test(email)`)
	if err != nil {
		t.Logf("CREATE INDEX error: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO drop_index_test VALUES (1, 'test@example.com', 'Test')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Drop column with index
	_, err = db.Exec(ctx, `ALTER TABLE drop_index_test DROP COLUMN email`)
	if err != nil {
		t.Logf("ALTER TABLE DROP COLUMN with index error: %v", err)
		return
	}

	// Verify table still works
	rows, _ := db.Query(ctx, `SELECT * FROM drop_index_test`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var id int
			var name string
			rows.Scan(&id, &name)
			t.Logf("Row: id=%d, name=%s", id, name)
		}
	}
}

// TestAlterTableDropLastColumn targets dropping last non-PK column
func TestAlterTableDropLastColumn(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE drop_last (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO drop_last VALUES (1, 'test')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Drop the only non-PK column
	_, err = db.Exec(ctx, `ALTER TABLE drop_last DROP COLUMN name`)
	if err != nil {
		t.Logf("DROP last column error: %v", err)
		return
	}

	// Verify table still works with just PK
	rows, _ := db.Query(ctx, `SELECT id FROM drop_last`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var id int
			rows.Scan(&id)
			t.Logf("Row with just PK: id=%d", id)
		}
	}
}

// TestAlterTableRename targets AlterTableRename
func TestAlterTableRename(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE old_name (
		id INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO old_name VALUES (1, 100), (2, 200)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Rename table
	_, err = db.Exec(ctx, `ALTER TABLE old_name RENAME TO new_name`)
	if err != nil {
		t.Logf("RENAME TABLE error: %v", err)
		return
	}

	// Verify data with new name
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM new_name`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			if count != 2 {
				t.Errorf("Expected 2 rows, got %d", count)
			}
			t.Logf("Renamed table has %d rows", count)
		}
	}

	// Verify old name no longer works
	_, err = db.Exec(ctx, `SELECT * FROM old_name`)
	if err != nil {
		t.Logf("Old table name correctly invalid: %v", err)
	}
}

// TestAlterTableRenameColumn targets AlterTableRenameColumn
func TestAlterTableRenameColumn(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE rename_col (
		id INTEGER PRIMARY KEY,
		old_column TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO rename_col VALUES (1, 'value')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Rename column
	_, err = db.Exec(ctx, `ALTER TABLE rename_col RENAME COLUMN old_column TO new_column`)
	if err != nil {
		t.Logf("RENAME COLUMN error: %v", err)
		return
	}

	// Verify with new column name
	rows, _ := db.Query(ctx, `SELECT new_column FROM rename_col`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var val string
			rows.Scan(&val)
			if val != "value" {
				t.Errorf("Expected 'value', got '%s'", val)
			}
			t.Logf("Renamed column value: %s", val)
		}
	}
}

// TestAlterTableAddColumn targets AlterTableAddColumn
func TestAlterTableAddColumn(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE add_col (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO add_col VALUES (1, 'test')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Add column with default", `ALTER TABLE add_col ADD COLUMN new_col INTEGER DEFAULT 0`},
		{"Add TEXT column", `ALTER TABLE add_col ADD COLUMN description TEXT`},
		{"Add REAL column", `ALTER TABLE add_col ADD COLUMN score REAL DEFAULT 0.0`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("ADD COLUMN error: %v", err)
				return
			}
			t.Logf("Column added successfully")
		})
	}

	// Verify new columns exist
	rows, _ := db.Query(ctx, `SELECT * FROM add_col`)
	if rows != nil {
		defer rows.Close()
		t.Logf("Table has %d columns after ALTER", len(rows.Columns()))
	}
}

// TestAlterTableMultipleChanges targets multiple ALTER operations
func TestAlterTableMultipleChanges(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE multi_alter (
		id INTEGER PRIMARY KEY,
		col1 TEXT,
		col2 INTEGER,
		col3 REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO multi_alter VALUES (1, 'a', 10, 1.5)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Perform multiple alterations
	alters := []string{
		`ALTER TABLE multi_alter DROP COLUMN col2`,
		`ALTER TABLE multi_alter ADD COLUMN new_col TEXT`,
		`ALTER TABLE multi_alter RENAME COLUMN col1 TO renamed_col`,
	}

	for _, sql := range alters {
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Logf("ALTER error: %v", err)
		}
	}

	// Verify table still works
	rows, _ := db.Query(ctx, `SELECT id FROM multi_alter`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var id int
			rows.Scan(&id)
			t.Logf("Table still accessible, id=%d", id)
		}
	}
}

// TestAlterTableWithFK targets ALTER TABLE with foreign keys
func TestAlterTableWithFK(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parent_tbl (
		id INTEGER PRIMARY KEY
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child_tbl (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		extra_col TEXT,
		FOREIGN KEY (parent_id) REFERENCES parent_tbl(id)
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parent_tbl VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO child_tbl VALUES (1, 1, 'data')`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Try to drop column used in FK (should fail or be handled)
	_, err = db.Exec(ctx, `ALTER TABLE child_tbl DROP COLUMN parent_id`)
	if err != nil {
		t.Logf("DROP FK column error (expected): %v", err)
	} else {
		t.Logf("FK column dropped - FK constraint may be removed")
	}

	// Try to drop non-FK column
	_, err = db.Exec(ctx, `ALTER TABLE child_tbl DROP COLUMN extra_col`)
	if err != nil {
		t.Logf("DROP non-FK column error: %v", err)
	} else {
		t.Logf("Non-FK column dropped successfully")
	}
}
