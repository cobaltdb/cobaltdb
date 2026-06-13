package catalog

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestSelectRejectsInvalidLimitOffset(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	if _, err := c.ExecuteQuery("CREATE TABLE limit_guard (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO limit_guard VALUES (1)"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "negative scalar limit",
			sql:     "SELECT 1 LIMIT -1",
			wantErr: "LIMIT must be a non-negative integer",
		},
		{
			name:    "fractional table limit",
			sql:     "SELECT * FROM limit_guard LIMIT 1.5",
			wantErr: "LIMIT must be a non-negative integer",
		},
		{
			name:    "negative cte offset",
			sql:     "WITH one AS (SELECT 1 AS n) SELECT * FROM one OFFSET -1",
			wantErr: "OFFSET must be a non-negative integer",
		},
		{
			name:    "unresolved limit expression",
			sql:     "SELECT * FROM limit_guard LIMIT missing_column",
			wantErr: "LIMIT expression error",
		},
		{
			name:    "combined overflow",
			sql:     fmt.Sprintf("SELECT 1 LIMIT %d OFFSET 1", math.MaxInt),
			wantErr: "LIMIT/OFFSET combined value too large",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := c.ExecuteQuery(tt.sql)
			if err == nil {
				t.Fatal("expected invalid LIMIT/OFFSET error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}
