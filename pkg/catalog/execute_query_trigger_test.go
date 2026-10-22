package catalog

import (
	"reflect"
	"strings"
	"testing"
)

func TestExecuteQueryTriggerLifecycle(t *testing.T) {
	c := newTestCatalog(t)
	t.Cleanup(func() { _ = c.pool.Close() })
	exec := func(sql string) *QueryResult {
		t.Helper()
		result, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
		return result
	}
	exec("CREATE TABLE items (id INTEGER PRIMARY KEY)")
	exec("CREATE TABLE events (item_id INTEGER)")
	const createSQL = `
CREATE TRIGGER capture_item AFTER INSERT ON items FOR EACH ROW
WHEN NEW.id > 1 BEGIN
  INSERT INTO events VALUES (NEW.id);
END
`
	result := exec(createSQL)
	if result == nil || len(result.Columns) != 0 || len(result.Rows) != 0 {
		t.Fatalf("CREATE TRIGGER result = %#v, want empty DDL result", result)
	}
	if got := c.ListTriggerSQL()["capture_item"]; got != strings.TrimSpace(createSQL) {
		t.Fatalf("stored trigger SQL = %q, want original SQL", got)
	}

	// Saving and loading must preserve both the trigger body and WHEN clause.
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	c = New(c.tree, c.pool, nil)
	if err := c.Load(); err != nil {
		t.Fatal(err)
	}
	exec("INSERT INTO items VALUES (1), (2)")
	want := [][]interface{}{{int64(2)}}
	if got := exec("SELECT item_id FROM events").Rows; !reflect.DeepEqual(got, want) {
		t.Fatalf("trigger events = %v, want %v", got, want)
	}

	result = exec("DROP TRIGGER capture_item")
	if result == nil || len(result.Columns) != 0 || len(result.Rows) != 0 {
		t.Fatalf("DROP TRIGGER result = %#v, want empty DDL result", result)
	}
	if _, err := c.GetTrigger("capture_item"); err == nil {
		t.Fatal("dropped trigger is still registered")
	}
	if _, ok := c.ListTriggerSQL()["capture_item"]; ok {
		t.Fatal("dropped trigger SQL is still registered")
	}
	exec("INSERT INTO items VALUES (3)")
	if got := exec("SELECT item_id FROM events").Rows; !reflect.DeepEqual(got, want) {
		t.Fatalf("dropped trigger still fired: events = %v, want %v", got, want)
	}
}

func TestExecuteQueryTriggerErrors(t *testing.T) {
	c := newTestCatalog(t)
	t.Cleanup(func() { _ = c.pool.Close() })
	for _, sql := range []string{
		"CREATE TABLE items (id INTEGER PRIMARY KEY)",
		"CREATE TRIGGER existing AFTER INSERT ON items BEGIN SELECT 1; END",
	} {
		if _, err := c.ExecuteQuery(sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		sql  string
		want string
	}{
		{"CREATE TRIGGER existing AFTER INSERT ON items BEGIN SELECT 1; END", "already exists"},
		{"CREATE TRIGGER missing_target AFTER INSERT ON absent BEGIN SELECT 1; END", "table or view not found"},
		{"DROP TRIGGER absent", "not found"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			if _, err := c.ExecuteQuery(tc.sql); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}
