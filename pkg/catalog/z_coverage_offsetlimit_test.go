package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestApplyOffsetLimit_Basic tests the applyOffsetLimit helper function
func TestApplyOffsetLimit_Basic(t *testing.T) {
	c := New(nil, nil, nil)
	_ = c
	
	// Test data
	rows := [][]interface{}{
		{"row1"},
		{"row2"},
		{"row3"},
		{"row4"},
		{"row5"},
	}
	
	tests := []struct {
		name     string
		offset   query.Expression
		limit    query.Expression
		args     []interface{}
		expected int // expected row count
	}{
		{
			name:     "no offset no limit",
			offset:   nil,
			limit:    nil,
			expected: 5,
		},
		{
			name:     "offset 2",
			offset:   &query.NumberLiteral{Value: 2},
			limit:    nil,
			expected: 3,
		},
		{
			name:     "limit 2",
			offset:   nil,
			limit:    &query.NumberLiteral{Value: 2},
			expected: 2,
		},
		{
			name:     "offset 1 limit 2",
			offset:   &query.NumberLiteral{Value: 1},
			limit:    &query.NumberLiteral{Value: 2},
			expected: 2,
		},
		{
			name:     "offset beyond rows",
			offset:   &query.NumberLiteral{Value: 10},
			limit:    nil,
			expected: 0,
		},
		{
			name:     "limit beyond rows",
			offset:   nil,
			limit:    &query.NumberLiteral{Value: 100},
			expected: 5,
		},
		{
			name:     "negative limit",
			offset:   nil,
			limit:    &query.NumberLiteral{Value: -1},
			expected: 5,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyOffsetLimit(rows, tt.offset, tt.limit, tt.args)
			if len(result) != tt.expected {
				t.Errorf("expected %d rows, got %d", tt.expected, len(result))
			}
		})
	}
}

// TestApplyOffsetLimit_WithArgs tests offset/limit with placeholder args
func TestApplyOffsetLimit_WithArgs(t *testing.T) {
	rows := [][]interface{}{
		{"a"}, {"b"}, {"c"}, {"d"}, {"e"},
	}
	
	// Test with placeholder expression
	offsetExpr := &query.PlaceholderExpr{Index: 0}
	limitExpr := &query.PlaceholderExpr{Index: 1}
	args := []interface{}{1, 2}
	
	result := applyOffsetLimit(rows, offsetExpr, limitExpr, args)
	if len(result) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result))
	}
	
	// Verify correct rows
	if result[0][0] != "b" || result[1][0] != "c" {
		t.Error("wrong rows returned")
	}
}
