package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestDetectEqualityJoin tests the detectEqualityJoin function
func TestDetectEqualityJoin(t *testing.T) {
	tests := []struct {
		name      string
		condition query.Expression
		leftCols  []ColumnDef
		rightCols []ColumnDef
		wantLeft  int
		wantRight int
		wantOk    bool
	}{
		{
			name: "simple equality - left.col1 = right.col2",
			condition: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "col1"},
				Operator: query.TokenEq,
				Right:    &query.Identifier{Name: "col2"},
			},
			leftCols:  []ColumnDef{{Name: "col1"}, {Name: "other"}},
			rightCols: []ColumnDef{{Name: "col2"}, {Name: "other2"}},
			wantLeft:  0,
			wantRight: 0,
			wantOk:    true,
		},
		{
			name: "swapped equality - col1 = col2 (col1 in right, col2 in left)",
			condition: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "col2"},
				Operator: query.TokenEq,
				Right:    &query.Identifier{Name: "col1"},
			},
			leftCols:  []ColumnDef{{Name: "col1"}},
			rightCols: []ColumnDef{{Name: "col2"}},
			wantLeft:  0,
			wantRight: 0,
			wantOk:    true,
		},
		{
			name: "non-equality operator",
			condition: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "col1"},
				Operator: query.TokenLt,
				Right:    &query.Identifier{Name: "col2"},
			},
			leftCols:  []ColumnDef{{Name: "col1"}},
			rightCols: []ColumnDef{{Name: "col2"}},
			wantLeft:  0,
			wantRight: 0,
			wantOk:    false,
		},
		{
			name:      "non-binary expression",
			condition: &query.NumberLiteral{Value: 1},
			leftCols:  []ColumnDef{{Name: "col1"}},
			rightCols: []ColumnDef{{Name: "col2"}},
			wantLeft:  0,
			wantRight: 0,
			wantOk:    false,
		},
		{
			name: "column not found in either table",
			condition: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "unknown"},
				Operator: query.TokenEq,
				Right:    &query.Identifier{Name: "also_unknown"},
			},
			leftCols:  []ColumnDef{{Name: "col1"}},
			rightCols: []ColumnDef{{Name: "col2"}},
			wantLeft:  0,
			wantRight: 0,
			wantOk:    false,
		},
		{
			name: "qualified identifiers",
			condition: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Table: "t1", Column: "id"},
				Operator: query.TokenEq,
				Right:    &query.QualifiedIdentifier{Table: "t2", Column: "t1_id"},
			},
			leftCols:  []ColumnDef{{Name: "id"}},
			rightCols: []ColumnDef{{Name: "t1_id"}},
			wantLeft:  0,
			wantRight: 0,
			wantOk:    true,
		},
		{
			name: "case insensitive match",
			condition: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "COL1"},
				Operator: query.TokenEq,
				Right:    &query.Identifier{Name: "col2"},
			},
			leftCols:  []ColumnDef{{Name: "col1"}},
			rightCols: []ColumnDef{{Name: "COL2"}},
			wantLeft:  0,
			wantRight: 0,
			wantOk:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			leftIdx, rightIdx, ok := detectEqualityJoin(tc.condition, tc.leftCols, tc.rightCols)
			if ok != tc.wantOk {
				t.Errorf("detectEqualityJoin() ok = %v, want %v", ok, tc.wantOk)
			}
			if ok {
				if leftIdx != tc.wantLeft {
					t.Errorf("detectEqualityJoin() leftIdx = %v, want %v", leftIdx, tc.wantLeft)
				}
				if rightIdx != tc.wantRight {
					t.Errorf("detectEqualityJoin() rightIdx = %v, want %v", rightIdx, tc.wantRight)
				}
			}
		})
	}
}

// TestExtractColumnName tests the extractColumnName function
func TestExtractColumnName(t *testing.T) {
	tests := []struct {
		name     string
		expr     query.Expression
		expected string
	}{
		{
			name:     "simple identifier",
			expr:     &query.Identifier{Name: "col1"},
			expected: "col1",
		},
		{
			name:     "qualified identifier",
			expr:     &query.QualifiedIdentifier{Table: "t1", Column: "id"},
			expected: "id",
		},
		{
			name:     "literal (not a column)",
			expr:     &query.NumberLiteral{Value: 123},
			expected: "",
		},
		{
			name:     "binary expression (not a column)",
			expr:     &query.BinaryExpr{Left: &query.NumberLiteral{Value: 1}, Operator: query.TokenPlus, Right: &query.NumberLiteral{Value: 2}},
			expected: "",
		},
		{
			name:     "nil expression",
			expr:     nil,
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := extractColumnName(tc.expr)
			if result != tc.expected {
				t.Errorf("extractColumnName() = %q, want %q", result, tc.expected)
			}
		})
	}
}
