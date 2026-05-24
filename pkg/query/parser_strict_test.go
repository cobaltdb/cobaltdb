package query

import (
	"strings"
	"testing"
)

func TestParseStrictRejectsTrailingTokens(t *testing.T) {
	tests := []string{
		"SELECT * FROM users TABLESAMPLE SYSTEM (10)",
		"SELECT * FROM users MATCH_RECOGNIZE (PARTITION BY id ORDER BY name PATTERN (A))",
		"SELECT 1 unexpected",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			if _, err := ParseStrict(sql); err == nil {
				t.Fatal("expected strict parse error")
			} else if !strings.Contains(err.Error(), "unexpected token after statement") {
				t.Fatalf("expected trailing token error, got %v", err)
			}
		})
	}
}

func TestParseStrictAllowsSemicolonTerminator(t *testing.T) {
	if _, err := ParseStrict("SELECT 1;"); err != nil {
		t.Fatalf("ParseStrict with semicolon failed: %v", err)
	}
}
