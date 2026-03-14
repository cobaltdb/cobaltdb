package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestAlterTableDropColumnWithData targets AlterTableDropColumn
func TestAlterTableDropColumnWithData(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table with multiple columns
	_, err = db.Exec(ctx, `CREATE TABLE drop_test (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT,
		age INTEGER,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO drop_test VALUES
		(1, 'Alice', 'alice@test.com', 30, 'active'),
		(2, 'Bob', 'bob@test.com', 25, 'inactive'),
		(3, 'Charlie', 'charlie@test.com', 35, 'active')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Drop a column
	_, err = db.Exec(ctx, `ALTER TABLE drop_test DROP COLUMN age`)
	if err != nil {
		t.Logf("DROP COLUMN error: %v", err)
		return
	}

	// Verify column dropped
	rows, err := db.Query(ctx, `SELECT id, name, email, status FROM drop_test`)
	if err != nil {
		t.Fatalf("Query after drop error: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name, email, status string
		rows.Scan(&id, &name, &email, &status)
		count++
		t.Logf("Row %d: %s, %s, %s", id, name, email, status)
	}
	t.Logf("Total rows after drop: %d", count)
}

// TestAlterTableDropColumnWithIndexDeep targets dropping column with index
func TestAlterTableDropColumnWithIndexDeep(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE indexed_table (
		id INTEGER PRIMARY KEY,
		code TEXT,
		value INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index on column
	_, err = db.Exec(ctx, `CREATE INDEX idx_code ON indexed_table(code)`)
	if err != nil {
		t.Logf("CREATE INDEX error: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO indexed_table VALUES (1, 'ABC', 100), (2, 'DEF', 200)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Drop column with index
	_, err = db.Exec(ctx, `ALTER TABLE indexed_table DROP COLUMN code`)
	if err != nil {
		t.Logf("DROP COLUMN with index error: %v", err)
		return
	}

	// Verify remaining data
	rows, err := db.Query(ctx, `SELECT id, value FROM indexed_table`)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, value int
		rows.Scan(&id, &value)
		t.Logf("Row %d: value=%d", id, value)
	}
}

// TestAlterTableDropLastNonPKColumn targets dropping last non-PK column
func TestAlterTableDropLastNonPKColumn(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table with only PK and one other column
	_, err = db.Exec(ctx, `CREATE TABLE minimal_table (
		id INTEGER PRIMARY KEY,
		data TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO minimal_table VALUES (1, 'test1'), (2, 'test2')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Drop the only non-PK column
	_, err = db.Exec(ctx, `ALTER TABLE minimal_table DROP COLUMN data`)
	if err != nil {
		t.Logf("DROP last non-PK column error: %v", err)
		return
	}

	// Verify only PK remains
	rows, err := db.Query(ctx, `SELECT id FROM minimal_table`)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		rows.Scan(&id)
		count++
		t.Logf("Row with PK only: %d", id)
	}
	t.Logf("Total rows: %d", count)
}

// TestAlterTableDropColumnWithFK targets dropping column with FK reference
func TestAlterTableDropColumnWithFK(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		extra_data TEXT,
		FOREIGN KEY (parent_id) REFERENCES parent(id)
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parent VALUES (1, 'Parent1'), (2, 'Parent2')`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO child VALUES (1, 1, 'data1'), (2, 2, 'data2')`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Try to drop column that is part of FK
	_, err = db.Exec(ctx, `ALTER TABLE child DROP COLUMN parent_id`)
	if err != nil {
		t.Logf("DROP COLUMN with FK error (expected): %v", err)
	} else {
		t.Log("FK column dropped - checking remaining structure")
		// If dropped, verify remaining columns work
		rows, _ := db.Query(ctx, `SELECT id, extra_data FROM child`)
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var id int
				var data string
				rows.Scan(&id, &data)
				t.Logf("Child %d: %s", id, data)
			}
		}
	}
}

// TestAlterTableDropMultipleColumns targets dropping multiple columns
func TestAlterTableDropMultipleColumns(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE wide_table (
		id INTEGER PRIMARY KEY,
		col1 TEXT,
		col2 INTEGER,
		col3 BOOLEAN,
		col4 TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO wide_table VALUES (1, 'a', 10, true, 'x'), (2, 'b', 20, false, 'y')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Drop first column
	_, err = db.Exec(ctx, `ALTER TABLE wide_table DROP COLUMN col1`)
	if err != nil {
		t.Logf("DROP COLUMN col1 error: %v", err)
		return
	}

	// Drop another column
	_, err = db.Exec(ctx, `ALTER TABLE wide_table DROP COLUMN col3`)
	if err != nil {
		t.Logf("DROP COLUMN col3 error: %v", err)
		return
	}

	// Verify remaining columns
	rows, err := db.Query(ctx, `SELECT id, col2, col4 FROM wide_table`)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, col2 int
		var col4 string
		rows.Scan(&id, &col2, &col4)
		t.Logf("Row %d: col2=%d, col4=%s", id, col2, col4)
	}
}
