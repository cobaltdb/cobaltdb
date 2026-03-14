package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestJSONSetBasic targets JSON Set function
func TestJSONSetBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE json_test (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO json_test VALUES (1, '{"name": "test", "value": 100}')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"JSON_SET simple", `UPDATE json_test SET data = JSON_SET(data, '$.name', 'updated') WHERE id = 1`},
		{"JSON_SET new key", `UPDATE json_test SET data = JSON_SET(data, '$.newKey', 'newValue') WHERE id = 1`},
		{"JSON_SET nested", `UPDATE json_test SET data = JSON_SET(data, '$.nested.path', 123) WHERE id = 1`},
		{"JSON_SET array", `UPDATE json_test SET data = JSON_SET(data, '$.items[0]', 'first') WHERE id = 1`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("JSON_SET error: %v", err)
				return
			}
			t.Logf("JSON_SET succeeded")
		})
	}
}

// TestJSONGet targets JSON Get operations
func TestJSONGet(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE json_get (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO json_get VALUES
		(1, '{"name": "Alice", "age": 30, "nested": {"city": "NYC"}}'),
		(2, '["apple", "banana", "cherry"]')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"JSON_EXTRACT object", `SELECT JSON_EXTRACT(data, '$.name') FROM json_get WHERE id = 1`},
		{"JSON_EXTRACT number", `SELECT JSON_EXTRACT(data, '$.age') FROM json_get WHERE id = 1`},
		{"JSON_EXTRACT nested", `SELECT JSON_EXTRACT(data, '$.nested.city') FROM json_get WHERE id = 1`},
		{"JSON_EXTRACT array", `SELECT JSON_EXTRACT(data, '$[1]') FROM json_get WHERE id = 2`},
		{"JSON_EXTRACT multiple", `SELECT JSON_EXTRACT(data, '$.name', '$.age') FROM json_get WHERE id = 1`},
		{"-> operator", `SELECT data->'$.name' FROM json_get WHERE id = 1`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("JSON_EXTRACT error: %v", err)
				return
			}
			defer rows.Close()

			if rows.Next() {
				var result string
				rows.Scan(&result)
				t.Logf("Result: %s", result)
			}
		})
	}
}

// TestJSONModify targets JSON modification functions
func TestJSONModify(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE json_modify (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO json_modify VALUES
		(1, '{"a": 1, "b": 2}'),
		(2, '[1, 2, 3]')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"JSON_INSERT", `UPDATE json_modify SET data = JSON_INSERT(data, '$.c', 3) WHERE id = 1`},
		{"JSON_REPLACE", `UPDATE json_modify SET data = JSON_REPLACE(data, '$.a', 100) WHERE id = 1`},
		{"JSON_REMOVE", `UPDATE json_modify SET data = JSON_REMOVE(data, '$.b') WHERE id = 1`},
		{"JSON_ARRAY_APPEND", `UPDATE json_modify SET data = JSON_ARRAY_APPEND(data, '$', 4) WHERE id = 2`},
		{"JSON_MERGE", `SELECT JSON_MERGE(data, '{"c": 3}') FROM json_modify WHERE id = 1`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("JSON modify error: %v", err)
				return
			}
			t.Logf("JSON modify succeeded")
		})
	}
}

// TestJSONValidation targets JSON validation
func TestJSONValidation(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE json_validation_test (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO json_validation_test VALUES
		(1, '{"valid": true}'),
		(2, 'invalid json'),
		(3, '[1, 2, 3]')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Check valid JSON
	rows, err := db.Query(ctx, `SELECT id FROM json_validation_test WHERE JSON_VALID(data)`)
	if err != nil {
		t.Logf("JSON_VALID error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		rows.Scan(&id)
		count++
		t.Logf("Valid JSON row: %d", id)
	}
	t.Logf("Total valid JSON rows: %d", count)
}

// TestJSONAggregation targets JSON aggregation functions
func TestJSONAggregation(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE json_agg (id INTEGER PRIMARY KEY, category TEXT, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO json_agg VALUES
		(1, 'A', '{"val": 10}'),
		(2, 'A', '{"val": 20}'),
		(3, 'B', '{"val": 30}')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// JSON_OBJECTAGG
	rows, err := db.Query(ctx, `SELECT JSON_OBJECTAGG(category, data) FROM json_agg`)
	if err != nil {
		t.Logf("JSON_OBJECTAGG error: %v", err)
	} else {
		defer rows.Close()
		if rows.Next() {
			var result string
			rows.Scan(&result)
			t.Logf("JSON_OBJECTAGG: %s", result)
		}
	}

	// JSON_ARRAYAGG
	rows, err = db.Query(ctx, `SELECT JSON_ARRAYAGG(data) FROM json_agg WHERE category = 'A'`)
	if err != nil {
		t.Logf("JSON_ARRAYAGG error: %v", err)
	} else {
		defer rows.Close()
		if rows.Next() {
			var result string
			rows.Scan(&result)
			t.Logf("JSON_ARRAYAGG: %s", result)
		}
	}
}
