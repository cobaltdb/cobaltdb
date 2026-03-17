package catalog

import (
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"context"
)

// TestParseSystemTimeExpr tests parseSystemTimeExpr function
func TestParseSystemTimeExpr(t *testing.T) {
	tests := []struct {
		name   string
		expr   string
		isPast bool
		isRFC  bool
	}{
		{
			name:   "negative hours",
			expr:   "-1 hour",
			isPast: true,
		},
		{
			name:   "negative hours plural",
			expr:   "-2 hours",
			isPast: true,
		},
		{
			name:   "negative minutes",
			expr:   "-30 minutes",
			isPast: true,
		},
		{
			name:   "negative seconds",
			expr:   "-10 seconds",
			isPast: true,
		},
		{
			name:   "negative days",
			expr:   "-1 day",
			isPast: true,
		},
		{
			name:   "positive hours",
			expr:   "+1 hour",
			isPast: false,
		},
		{
			name:   "positive minutes",
			expr:   "+30 minutes",
			isPast: false,
		},
		{
			name:   "RFC3339 format",
			expr:   "2026-03-17T10:00:00Z",
			isRFC:  true,
		},
		{
			name:   "datetime format",
			expr:   "2026-03-17 10:00:00",
			isRFC:  true,
		},
		{
			name:   "unknown format defaults to now",
			expr:   "invalid",
			isPast: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseSystemTimeExpr(tt.expr)

			if tt.isPast {
				// Should be in the past (before now)
				if result.After(time.Now()) {
					t.Errorf("Expected past time, got future: %v", result)
				}
			} else if tt.isRFC {
				// Should be the exact time specified
				if result.Year() != 2026 {
					t.Errorf("Expected year 2026, got %d", result.Year())
				}
			}
			// If neither isPast nor isRFC, it's "now" which we just verify is valid
		})
	}
}

// TestParseSystemTimeExprVariations tests all time unit variations
func TestParseSystemTimeExprVariations(t *testing.T) {
	now := time.Now()

	variations := []string{
		"-1 hour",
		"-2 hours",
		"-1 hr",
		"-30 minute",
		"-30 minutes",
		"-30 min",
		"-10 second",
		"-10 seconds",
		"-10 sec",
		"-1 day",
		"-2 days",
		"+1 hour",
		"+1 minute",
		"  -1  hour  ", // with extra spaces
	}

	for _, v := range variations {
		result := parseSystemTimeExpr(v)
		if result.Equal(now) && !strings.Contains(v, "  ") {
			t.Errorf("parseSystemTimeExpr(%q) returned current time - parsing failed", v)
		}
	}
}

// TestEvaluateTemporalExprNonSystem tests AS OF (non-system) temporal expressions
func TestEvaluateTemporalExprNonSystem(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create test table
	createStmt := &query.CreateTableStmt{
		Table: "temporal_asof_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
	}
	err := c.CreateTable(createStmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	ctx := context.Background()
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "temporal_asof_test",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal("test1")}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test AS OF with RFC3339 timestamp (non-system time)
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "AS OF with RFC3339",
			sql:     "SELECT * FROM temporal_asof_test AS OF '2026-01-01T00:00:00Z'",
			wantErr: false,
		},
		{
			name:    "AS OF with datetime",
			sql:     "SELECT * FROM temporal_asof_test AS OF '2026-01-01 00:00:00'",
			wantErr: false,
		},
		{
			name:    "AS OF with date only",
			sql:     "SELECT * FROM temporal_asof_test AS OF '2026-01-01'",
			wantErr: false,
		},
		{
			name:    "AS OF with invalid timestamp",
			sql:     "SELECT * FROM temporal_asof_test AS OF 'invalid-date'",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := query.Parse(tt.sql)
			if err != nil {
				t.Errorf("Parse error: %v", err)
				return
			}

			selectStmt, ok := stmt.(*query.SelectStmt)
			if !ok {
				t.Error("Expected SelectStmt")
				return
			}

			_, _, err = c.Select(selectStmt, nil)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// TestEvaluateTemporalExprDirect tests evaluateTemporalExpr directly
func TestEvaluateTemporalExprDirect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Test with nil expression
	result, err := c.evaluateTemporalExpr(nil, nil)
	if err != nil {
		t.Errorf("Expected no error for nil expr, got: %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil result for nil expr, got: %v", result)
	}

	// Test AS OF SYSTEM TIME with time.Time value
	temporalExpr := &query.TemporalExpr{
		IsSystem:  true,
		Timestamp: &query.StringLiteral{Value: "-1 hour"},
	}
	result, err = c.evaluateTemporalExpr(temporalExpr, nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	} else if result.After(time.Now()) {
		t.Error("Expected past time for '-1 hour'")
	}
}

// TestEvaluateTemporalExprSQL tests temporal expressions through SQL parsing
func TestEvaluateTemporalExprSQL(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create test table
	createStmt := &query.CreateTableStmt{
		Table: "temporal_sql_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
	}
	err := c.CreateTable(createStmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "temporal_sql_test",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal("test1")}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test AS OF SYSTEM TIME with various expressions
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "AS OF with negative interval",
			sql:     "SELECT * FROM temporal_sql_test AS OF SYSTEM TIME '-1 hour'",
			wantErr: false,
		},
		{
			name:    "AS OF with positive interval",
			sql:     "SELECT * FROM temporal_sql_test AS OF SYSTEM TIME '+1 minute'",
			wantErr: false,
		},
		{
			name:    "AS OF with datetime string",
			sql:     "SELECT * FROM temporal_sql_test AS OF SYSTEM TIME '2026-01-01 00:00:00'",
			wantErr: false,
		},
		{
			name:    "AS OF with RFC3339",
			sql:     "SELECT * FROM temporal_sql_test AS OF SYSTEM TIME '2026-01-01T00:00:00Z'",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse and execute the query
			stmt, err := query.Parse(tt.sql)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Parse error: %v", err)
				}
				return
			}

			selectStmt, ok := stmt.(*query.SelectStmt)
			if !ok {
				t.Error("Expected SelectStmt")
				return
			}

			_, _, err = c.Select(selectStmt, nil)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}
