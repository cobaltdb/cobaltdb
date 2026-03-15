package query

import (
	"testing"
)

// TestParseMatchAgainst111 tests MATCH ... AGAINST parsing
func TestParseMatchAgainst111(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "Basic MATCH AGAINST",
			sql:     "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('search term')",
			wantErr: false,
		},
		{
			name:    "MATCH with single column",
			sql:     "SELECT * FROM articles WHERE MATCH(title) AGAINST('word')",
			wantErr: false,
		},
		{
			name:    "MATCH with multiple columns",
			sql:     "SELECT * FROM docs WHERE MATCH(col1, col2, col3) AGAINST('query')",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, err := Tokenize(tt.sql)
			if err != nil {
				t.Fatalf("Tokenize failed: %v", err)
			}
			p := NewParser(tokens)
			_, err = p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParsePartitionBy111 tests PARTITION BY parsing in CREATE TABLE
func TestParsePartitionBy111(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "RANGE partition",
			sql:     "CREATE TABLE t (id INT) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (100))",
			wantErr: false,
		},
		{
			name:    "HASH partition",
			sql:     "CREATE TABLE t (id INT) PARTITION BY HASH(id) PARTITIONS 4",
			wantErr: false,
		},
		{
			name:    "KEY partition",
			sql:     "CREATE TABLE t (id INT) PARTITION BY KEY(id) PARTITIONS 2",
			wantErr: false,
		},
		{
			name:    "RANGE with multiple partitions",
			sql:     "CREATE TABLE t (id INT) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN (200))",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, err := Tokenize(tt.sql)
			if err != nil {
				t.Fatalf("Tokenize failed: %v", err)
			}
			p := NewParser(tokens)
			_, err = p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
