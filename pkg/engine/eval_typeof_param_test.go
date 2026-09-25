package engine

import (
	"context"
	"testing"
)

// TestTypeOfWireAcceptedNumericParams pins TYPEOF's agreement with the
// numeric tier: every type validateWireParams accepts must report its SQL
// storage class ("integer"/"real"), not "text".
func TestTypeOfWireAcceptedNumericParams(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	typeOf := func(query string, args ...interface{}) string {
		rows, err := db.Query(ctx, query, args...)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("%s: no rows", query)
		}
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("%s: scan: %v", query, err)
		}
		s, _ := v.(string)
		return s
	}

	for _, tc := range []struct {
		name string
		arg  interface{}
		want string
	}{
		{"int", int(7), "integer"},
		{"int8", int8(7), "integer"},
		{"int16", int16(7), "integer"},
		{"int32", int32(7), "integer"},
		{"int64", int64(7), "integer"},
		{"uint", uint(7), "integer"},
		{"uint8", uint8(7), "integer"},
		{"uint16", uint16(7), "integer"},
		{"uint32", uint32(7), "integer"},
		{"uint64", uint64(7), "integer"},
		{"float64-whole", float64(2), "integer"},
		{"float64-frac", float64(2.5), "real"},
		{"float32-whole", float32(2), "integer"},
		{"float32-frac", float32(2.5), "real"},
		{"string", "abc", "text"},
		{"bool", true, "integer"},
	} {
		if got := typeOf("SELECT TYPEOF(?)", tc.arg); got != tc.want {
			t.Errorf("TYPEOF(%s) = %q, want %q", tc.name, got, tc.want)
		}
	}
	if got := typeOf("SELECT TYPEOF(NULL)"); got != "null" {
		t.Errorf("TYPEOF(NULL) = %q, want %q", got, "null")
	}
}
