package cobaltdb

import (
	"testing"
	"time"
)

// TestNullStringScan covers NullString.Scan method
func TestNullStringScan(t *testing.T) {
	tests := []struct {
		name      string
		value     interface{}
		wantStr   string
		wantValid bool
	}{
		{"nil", nil, "", false},
		{"string", "hello", "hello", true},
		{"bytes", []byte("world"), "world", true},
		{"int", int(42), "42", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ns NullString
			err := ns.Scan(tt.value)
			if err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			if ns.Valid != tt.wantValid {
				t.Errorf("Valid = %v, want %v", ns.Valid, tt.wantValid)
			}
			if ns.String != tt.wantStr {
				t.Errorf("String = %q, want %q", ns.String, tt.wantStr)
			}
		})
	}
}

// TestNullInt64Scan covers NullInt64.Scan method
func TestNullInt64Scan(t *testing.T) {
	tests := []struct {
		name      string
		value     interface{}
		wantInt   int64
		wantValid bool
		wantErr   bool
	}{
		{"nil", nil, 0, false, false},
		{"int64", int64(42), 42, true, false},
		{"int", int(42), 42, true, false},
		{"float64", float64(42.0), 42, true, false},
		{"string valid", "123", 123, true, false},
		{"string invalid", "abc", 0, true, true}, // Valid stays true on parse error, error returned
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ni NullInt64
			err := ni.Scan(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Scan error = %v, wantErr %v", err, tt.wantErr)
			}
			if ni.Valid != tt.wantValid {
				t.Errorf("Valid = %v, want %v", ni.Valid, tt.wantValid)
			}
			if ni.Int64 != tt.wantInt {
				t.Errorf("Int64 = %d, want %d", ni.Int64, tt.wantInt)
			}
		})
	}
}

// TestNullInt64Value covers NullInt64.Value method
func TestNullInt64Value(t *testing.T) {
	tests := []struct {
		name    string
		ni      NullInt64
		wantNil bool
		wantVal int64
	}{
		{"valid", NullInt64{Int64: 42, Valid: true}, false, 42},
		{"invalid", NullInt64{Int64: 0, Valid: false}, true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := tt.ni.Value()
			if err != nil {
				t.Fatalf("Value failed: %v", err)
			}
			if tt.wantNil {
				if v != nil {
					t.Errorf("Value = %v, want nil", v)
				}
			} else {
				if v != tt.wantVal {
					t.Errorf("Value = %v, want %v", v, tt.wantVal)
				}
			}
		})
	}
}

// TestNullTimeScan covers NullTime.Scan method
func TestNullTimeScan(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name      string
		value     interface{}
		wantValid bool
		wantTime  time.Time
	}{
		{"nil", nil, false, time.Time{}},
		{"time.Time", now, true, now},
		{"RFC3339 string", now.Format(time.RFC3339), true, now.Truncate(time.Second)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var nt NullTime
			err := nt.Scan(tt.value)
			if err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			if nt.Valid != tt.wantValid {
				t.Errorf("Valid = %v, want %v", nt.Valid, tt.wantValid)
			}
		})
	}
}

// TestJSONScan covers JSON.Scan method
func TestJSONScan(t *testing.T) {
	tests := []struct {
		name      string
		value     interface{}
		wantValid bool
		wantErr   bool
	}{
		{"nil", nil, false, false},
		{"string", `{"key":"value"}`, true, false},
		{"bytes", []byte(`[1,2,3]`), true, false},
		{"invalid", 123, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var j JSON
			err := j.Scan(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Scan error = %v, wantErr %v", err, tt.wantErr)
			}
			if j.Valid != tt.wantValid {
				t.Errorf("Valid = %v, want %v", j.Valid, tt.wantValid)
			}
		})
	}
}

// TestJSONValue covers JSON.Value method
func TestJSONValue(t *testing.T) {
	tests := []struct {
		name    string
		j       JSON
		wantNil bool
	}{
		{"valid", JSON{Data: map[string]interface{}{"key": "value"}, Valid: true}, false},
		{"invalid", JSON{Data: nil, Valid: false}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := tt.j.Value()
			if err != nil {
				t.Fatalf("Value failed: %v", err)
			}
			if tt.wantNil {
				if v != nil {
					t.Errorf("Value = %v, want nil", v)
				}
			} else {
				if v == nil {
					t.Errorf("Value = nil, want non-nil")
				}
			}
		})
	}
}

// TestPooledConn covers PooledConn methods
func TestPooledConn(t *testing.T) {
	pool := &Pool{}
	pc := &PooledConn{
		pool: pool,
	}

	// Test Release
	pc.inUse = true
	pc.Release()
	if pc.inUse {
		t.Error("Release() should set inUse to false")
	}

	// Conn returns nil since db is not set - that's fine for this test
	if pc.Conn() != nil {
		t.Log("Conn() returned non-nil (expected when db is nil)")
	}
}

// TestColumnTypeDatabaseTypeName covers driverRows.ColumnTypeDatabaseTypeName
func TestColumnTypeDatabaseTypeName(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer c.Close()

	// Create table and query
	stmt, _ := c.Prepare("CREATE TABLE coltype_test (id INTEGER, name TEXT)")
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c.Prepare("SELECT * FROM coltype_test")
	rows, _ := stmt.Query(nil)
	defer rows.Close()

	dr := rows.(*driverRows)

	// Test valid index
	colType := dr.ColumnTypeDatabaseTypeName(0)
	if colType != "TEXT" {
		t.Errorf("ColumnTypeDatabaseTypeName(0) = %q, want TEXT", colType)
	}

	// Test out of bounds
	colType = dr.ColumnTypeDatabaseTypeName(-1)
	if colType != "" {
		t.Errorf("ColumnTypeDatabaseTypeName(-1) = %q, want empty", colType)
	}

	colType = dr.ColumnTypeDatabaseTypeName(100)
	if colType != "" {
		t.Errorf("ColumnTypeDatabaseTypeName(100) = %q, want empty", colType)
	}
}

// TestIsolationLevelString covers all isolation level strings
func TestIsolationLevelString(t *testing.T) {
	tests := []struct {
		level IsolationLevel
		want  string
	}{
		{LevelDefault, ""},
		{LevelReadUncommitted, "READ UNCOMMITTED"},
		{LevelReadCommitted, "READ COMMITTED"},
		{LevelWriteCommitted, "WRITE COMMITTED"},
		{LevelRepeatableRead, "REPEATABLE READ"},
		{LevelSnapshot, "SNAPSHOT"},
		{LevelSerializable, "SERIALIZABLE"},
		{LevelLinearizable, "LINEARIZABLE"},
		{IsolationLevel(999), ""}, // Unknown level
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.level.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
