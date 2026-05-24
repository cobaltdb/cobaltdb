package engine

import (
	"reflect"
	"testing"
	"time"
)

func TestRowsColumnTypeHints(t *testing.T) {
	rows := &Rows{
		columns: []string{"id", "name", "score", "payload", "created_at", "missing"},
		rows: [][]interface{}{
			{nil, "alice", nil, []byte("blob"), time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC), nil},
			{int64(42), "bob", 9.5, []byte("data"), time.Date(2026, 5, 24, 13, 0, 0, 0, time.UTC), nil},
		},
	}

	got := rows.ColumnTypeHints()
	want := []string{"BIGINT", "TEXT", "DOUBLE", "BLOB", "DATETIME", ""}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ColumnTypeHints() = %#v, want %#v", got, want)
	}

	got[0] = "TEXT"
	again := rows.ColumnTypeHints()
	if again[0] != "BIGINT" {
		t.Fatalf("ColumnTypeHints exposed mutable state: %#v", again)
	}
}
