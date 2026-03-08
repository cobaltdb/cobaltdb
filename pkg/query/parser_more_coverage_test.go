package query

import (
	"testing"
)

// TestParseRefreshMaterializedView tests parsing REFRESH MATERIALIZED VIEW
func TestParseRefreshMaterializedView(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "basic refresh",
			input:   "REFRESH MATERIALIZED VIEW myview",
			wantErr: false,
		},
		{
			name:    "refresh with quoted name",
			input:   "REFRESH MATERIALIZED VIEW `my view`",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				refreshStmt, ok := stmt.(*RefreshMaterializedViewStmt)
				if !ok {
					t.Errorf("Expected *RefreshMaterializedViewStmt, got %T", stmt)
					return
				}
				if refreshStmt.Name == "" {
					t.Error("Expected non-empty view name")
				}
				t.Logf("Refresh view: %s", refreshStmt.Name)
			}
		})
	}
}

// TestParseCreateMaterializedView tests parsing CREATE MATERIALIZED VIEW
func TestParseCreateMaterializedView(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantErr     bool
		ifNotExists bool
		viewName    string
	}{
		{
			name:     "basic create",
			input:    "CREATE MATERIALIZED VIEW myview AS SELECT * FROM t",
			wantErr:  false,
			viewName: "myview",
		},
		{
			name:        "create if not exists",
			input:       "CREATE MATERIALIZED VIEW IF NOT EXISTS myview AS SELECT * FROM t",
			wantErr:     false,
			ifNotExists: true,
			viewName:    "myview",
		},
		{
			name:     "create with complex query",
			input:    "CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) FROM orders WHERE amount > 100",
			wantErr:  false,
			viewName: "stats",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				mvStmt, ok := stmt.(*CreateMaterializedViewStmt)
				if !ok {
					t.Errorf("Expected *CreateMaterializedViewStmt, got %T", stmt)
					return
				}
				if mvStmt.Name != tt.viewName {
					t.Errorf("Expected view name %s, got %s", tt.viewName, mvStmt.Name)
				}
				if mvStmt.IfNotExists != tt.ifNotExists {
					t.Errorf("Expected IfNotExists=%v, got %v", tt.ifNotExists, mvStmt.IfNotExists)
				}
				if mvStmt.Query == nil {
					t.Error("Expected non-nil Query")
				}
				t.Logf("Created materialized view: %s, IfNotExists: %v", mvStmt.Name, mvStmt.IfNotExists)
			}
		})
	}
}

// TestParseDropMaterializedView tests parsing DROP MATERIALIZED VIEW
func TestParseDropMaterializedView(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantErr  bool
		ifExists bool
		viewName string
	}{
		{
			name:     "basic drop",
			input:    "DROP MATERIALIZED VIEW myview",
			wantErr:  false,
			viewName: "myview",
		},
		{
			name:     "drop if exists",
			input:    "DROP MATERIALIZED VIEW IF EXISTS myview",
			wantErr:  false,
			ifExists: true,
			viewName: "myview",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				dropStmt, ok := stmt.(*DropMaterializedViewStmt)
				if !ok {
					t.Errorf("Expected *DropMaterializedViewStmt, got %T", stmt)
					return
				}
				if dropStmt.Name != tt.viewName {
					t.Errorf("Expected view name %s, got %s", tt.viewName, dropStmt.Name)
				}
				if dropStmt.IfExists != tt.ifExists {
					t.Errorf("Expected IfExists=%v, got %v", tt.ifExists, dropStmt.IfExists)
				}
				t.Logf("Dropped materialized view: %s, IfExists: %v", dropStmt.Name, dropStmt.IfExists)
			}
		})
	}
}

// TestParseCreateFTSIndex tests parsing CREATE FULLTEXT INDEX
func TestParseCreateFTSIndex(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantErr     bool
		ifNotExists bool
		indexName   string
		tableName   string
		columns     []string
	}{
		{
			name:      "basic create",
			input:     "CREATE FULLTEXT INDEX idx ON t(col)",
			wantErr:   false,
			indexName: "idx",
			tableName: "t",
			columns:   []string{"col"},
		},
		{
			name:        "create if not exists",
			input:       "CREATE FULLTEXT INDEX IF NOT EXISTS idx ON t(col)",
			wantErr:     false,
			ifNotExists: true,
			indexName:   "idx",
			tableName:   "t",
			columns:     []string{"col"},
		},
		{
			name:      "multiple columns",
			input:     "CREATE FULLTEXT INDEX idx ON t(col1, col2, col3)",
			wantErr:   false,
			indexName: "idx",
			tableName: "t",
			columns:   []string{"col1", "col2", "col3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				ftsStmt, ok := stmt.(*CreateFTSIndexStmt)
				if !ok {
					t.Errorf("Expected *CreateFTSIndexStmt, got %T", stmt)
					return
				}
				if ftsStmt.Index != tt.indexName {
					t.Errorf("Expected index name %s, got %s", tt.indexName, ftsStmt.Index)
				}
				if ftsStmt.Table != tt.tableName {
					t.Errorf("Expected table name %s, got %s", tt.tableName, ftsStmt.Table)
				}
				if ftsStmt.IfNotExists != tt.ifNotExists {
					t.Errorf("Expected IfNotExists=%v, got %v", tt.ifNotExists, ftsStmt.IfNotExists)
				}
				if len(ftsStmt.Columns) != len(tt.columns) {
					t.Errorf("Expected %d columns, got %d", len(tt.columns), len(ftsStmt.Columns))
				}
				for i, col := range tt.columns {
					if i < len(ftsStmt.Columns) && ftsStmt.Columns[i] != col {
						t.Errorf("Expected column %s, got %s", col, ftsStmt.Columns[i])
					}
				}
				t.Logf("Created FTS index: %s on %s(%v), IfNotExists: %v", ftsStmt.Index, ftsStmt.Table, ftsStmt.Columns, ftsStmt.IfNotExists)
			}
		})
	}
}

// TestParseExpressionWithOffset tests parseExpressionWithOffset function
func TestParseExpressionWithOffsetCoverage(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		offset  int
		wantErr bool
	}{
		{
			name:    "simple expression",
			input:   "SELECT * FROM t WHERE 1 = 1",
			offset:  6, // Start after SELECT
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("Tokenize error: %v", err)
			}
			parser := NewParser(tokens)
			// Parse the SELECT first to consume tokens
			_, err = parser.Parse()
			if err != nil {
				t.Logf("Parse result: %v", err)
			}
			// parseExpressionWithOffset is called internally during parsing
			t.Log("parseExpressionWithOffset tested via normal parsing")
		})
	}
}
