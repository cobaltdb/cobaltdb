package catalog

import (
	"testing"
)

func TestValidateTableName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid_simple", "users", false},
		{"valid_with_underscore", "user_profiles", false},
		{"valid_with_numbers", "table1", false},
		{"valid_starts_underscore", "_temp", false},
		{"empty_name", "", true},
		{"starts_with_number", "1table", true},
		{"contains_space", "user name", true},
		{"contains_dash", "user-name", true},
		{"reserved_select", "SELECT", true},
		{"reserved_table", "TABLE", true},
		{"reserved_insert", "INSERT", true},
		{"reserved_from", "FROM", true},
		{"reserved_where", "WHERE", true},
		{"reserved_join", "JOIN", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTableName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTableName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateColumnName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid_simple", "id", false},
		{"valid_with_underscore", "user_id", false},
		{"valid_with_numbers", "col1", false},
		{"valid_starts_underscore", "_temp", false},
		{"empty_name", "", true},
		{"starts_with_number", "1col", true},
		{"contains_space", "col name", true},
		{"contains_dash", "col-name", true},
		{"reserved_select", "SELECT", true},
		{"reserved_from", "FROM", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateColumnName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateColumnName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsReservedWord(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"select", "SELECT", true},
		{"insert", "INSERT", true},
		{"update", "UPDATE", true},
		{"delete", "DELETE", true},
		{"create", "CREATE", true},
		{"drop", "DROP", true},
		{"alter", "ALTER", true},
		{"table", "TABLE", true},
		{"index", "INDEX", true},
		{"view", "VIEW", true},
		{"from", "FROM", true},
		{"where", "WHERE", true},
		{"join", "JOIN", true},
		{"and", "AND", true},
		{"or", "OR", true},
		{"not", "NOT", true},
		{"in", "IN", true},
		{"between", "BETWEEN", true},
		{"like", "LIKE", true},
		{"null", "NULL", true},
		{"true", "TRUE", true},
		{"false", "FALSE", true},
		{"default", "DEFAULT", true},
		{"primary", "PRIMARY", true},
		{"key", "KEY", false},
		{"foreign", "FOREIGN", true},
		{"references", "REFERENCES", true},
		{"unique", "UNIQUE", true},
		{"check", "CHECK", true},
		{"constraint", "CONSTRAINT", true},
		{"add", "ADD", false},
		{"column", "COLUMN", true},
		{"rename", "RENAME", true},
		{"to", "TO", true},
		{"as", "AS", true},
		{"not_reserved", "users", false},
		{"not_reserved_id", "id", false},
		{"not_reserved_name", "name", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isReservedWord(tt.input)
			if result != tt.expected {
				t.Errorf("isReservedWord(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
