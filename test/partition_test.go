package test

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestTablePartitioning_Basic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	t.Run("Create partitioned table", func(t *testing.T) {
		// Create a table partitioned by RANGE on 'year' column
		afExec(t, db, ctx, `CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			name TEXT,
			year INTEGER
		) PARTITION BY RANGE (year) (
			PARTITION p0 VALUES LESS THAN (2020),
			PARTITION p1 VALUES LESS THAN (2025),
			PARTITION p2 VALUES LESS THAN (2030)
		)`)

		// Insert data into different partitions
		afExec(t, db, ctx, "INSERT INTO events VALUES (1, 'Event 2019', 2019)")
		afExec(t, db, ctx, "INSERT INTO events VALUES (2, 'Event 2022', 2022)")
		afExec(t, db, ctx, "INSERT INTO events VALUES (3, 'Event 2027', 2027)")

		// Query all data
		rows := afQuery(t, db, ctx, "SELECT * FROM events ORDER BY id")
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(rows))
		}

		// Verify data
		if rows[0][0] != int64(1) || rows[0][2] != int64(2019) {
			t.Errorf("Row 1 mismatch: %v", rows[0])
		}
		if rows[1][0] != int64(2) || rows[1][2] != int64(2022) {
			t.Errorf("Row 2 mismatch: %v", rows[1])
		}
		if rows[2][0] != int64(3) || rows[2][2] != int64(2027) {
			t.Errorf("Row 3 mismatch: %v", rows[2])
		}
	})

	t.Run("Select with WHERE from partitioned table", func(t *testing.T) {
		// Query with WHERE clause
		rows := afQuery(t, db, ctx, "SELECT * FROM events WHERE year < 2025")
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows for year < 2025, got %d", len(rows))
		}
	})

	t.Run("Update partitioned table", func(t *testing.T) {
		// Update a row
		afExec(t, db, ctx, "UPDATE events SET name = 'Updated 2019' WHERE id = 1")

		// Verify update
		rows := afQuery(t, db, ctx, "SELECT name FROM events WHERE id = 1")
		if len(rows) != 1 || rows[0][0] != "Updated 2019" {
			t.Errorf("Update failed: %v", rows)
		}
	})

	t.Run("Delete from partitioned table", func(t *testing.T) {
		// Delete a row
		afExec(t, db, ctx, "DELETE FROM events WHERE id = 2")

		// Verify delete
		rows := afQuery(t, db, ctx, "SELECT COUNT(*) FROM events")
		if len(rows) != 1 || rows[0][0] != int64(2) {
			t.Errorf("Delete failed, expected 2 rows, got %v", rows)
		}
	})
}

func TestTablePartitioning_Hash(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	t.Run("Create HASH partitioned table", func(t *testing.T) {
		// Create a table partitioned by HASH
		afExec(t, db, ctx, `CREATE TABLE hash_table (
			id INTEGER PRIMARY KEY,
			data TEXT
		) PARTITION BY HASH (id) PARTITIONS 4`)

		// Insert data
		for i := 1; i <= 10; i++ {
			if _, err := db.Exec(ctx, "INSERT INTO hash_table VALUES (?, ?)", int64(i), "data"); err != nil {
				t.Fatalf("INSERT failed: %v", err)
			}
		}

		// Query all data
		rows := afQuery(t, db, ctx, "SELECT COUNT(*) FROM hash_table")
		if len(rows) != 1 || rows[0][0] != int64(10) {
			t.Errorf("Expected 10 rows, got %v", rows)
		}
	})
}
