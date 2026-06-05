package test

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestSQLCompatibilityCorpusBaseline(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	setup := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)",
		"INSERT INTO users VALUES (1, 'alice', 1)",
		"INSERT INTO users VALUES (2, 'bob', 1)",
		"INSERT INTO users VALUES (3, 'cara', 0)",
		"INSERT INTO orders VALUES (1, 1, 10)",
		"INSERT INTO orders VALUES (2, 1, 15)",
		"INSERT INTO orders VALUES (3, 2, 25)",
	}
	for _, stmt := range setup {
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}

	supported := []struct {
		name     string
		sql      string
		wantRows int
	}{
		{
			name:     "predicate order limit",
			sql:      "SELECT id FROM users WHERE active = 1 ORDER BY id LIMIT 2",
			wantRows: 2,
		},
		{
			name:     "join with filter",
			sql:      "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total >= 15 ORDER BY u.name",
			wantRows: 2,
		},
		{
			name:     "aggregate group having",
			sql:      "SELECT user_id, SUM(total) FROM orders GROUP BY user_id HAVING SUM(total) >= 25 ORDER BY user_id",
			wantRows: 2,
		},
		{
			name:     "cte",
			sql:      "WITH active_users AS (SELECT id FROM users WHERE active = 1) SELECT id FROM active_users ORDER BY id",
			wantRows: 2,
		},
		{
			name:     "window row number",
			sql:      "SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users ORDER BY id",
			wantRows: 3,
		},
		{
			name:     "subquery in",
			sql:      "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE total >= 25) ORDER BY name",
			wantRows: 1,
		},
		{
			name:     "set operation",
			sql:      "SELECT name FROM users WHERE active = 1 UNION SELECT name FROM users WHERE id = 3",
			wantRows: 3,
		},
		{
			name:     "case expression",
			sql:      "SELECT CASE WHEN total >= 25 THEN 'large' ELSE 'small' END FROM orders ORDER BY id",
			wantRows: 3,
		},
	}

	for _, tc := range supported {
		t.Run("supported/"+tc.name, func(t *testing.T) {
			if _, err := query.Parse(tc.sql); err != nil {
				t.Fatalf("parse: %v", err)
			}
			_, rows := queryRows(t, db, tc.sql)
			if len(rows) != tc.wantRows {
				t.Fatalf("expected %d rows, got %d", tc.wantRows, len(rows))
			}
		})
	}
}

func TestSQLCompatibilityUnsupportedBaseline(t *testing.T) {
	unsupported := []struct {
		name string
		sql  string
	}{
		{
			name: "merge",
			sql:  "MERGE INTO users USING staging ON users.id = staging.id WHEN MATCHED THEN UPDATE SET name = staging.name",
		},
		{
			name: "grant",
			sql:  "GRANT SELECT ON users TO analyst",
		},
		{
			name: "revoke",
			sql:  "REVOKE SELECT ON users FROM analyst",
		},
	}

	for _, tc := range unsupported {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := query.Parse(tc.sql); err == nil {
				t.Fatalf("expected unsupported SQL to fail parsing: %s", tc.sql)
			}
		})
	}
}
