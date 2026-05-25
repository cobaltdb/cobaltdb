package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestLoadSchemaReturnsCorruptTableExpressionError(t *testing.T) {
	tests := []struct {
		name   string
		column ColumnDef
		want   string
	}{
		{
			name:   "default",
			column: ColumnDef{Name: "status", Type: "TEXT", Default: "@"},
			want:   "default expression",
		},
		{
			name:   "check",
			column: ColumnDef{Name: "age", Type: "INTEGER", CheckStr: "age >"},
			want:   "check expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			schema := struct {
				Tables        map[string]*TableDef       `json:"tables"`
				VectorIndexes map[string]*VectorIndexDef `json:"vectorIndexes"`
			}{
				Tables: map[string]*TableDef{
					"users": {
						Name:    "users",
						Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}, tt.column},
					},
				},
				VectorIndexes: map[string]*VectorIndexDef{},
			}
			data, err := json.Marshal(schema)
			if err != nil {
				t.Fatalf("Marshal schema: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "schema.json"), data, 0600); err != nil {
				t.Fatalf("Write schema: %v", err)
			}

			pool := storage.NewBufferPool(1024, storage.NewMemory())
			defer pool.Close()

			c := New(nil, pool, nil)
			err = c.LoadSchema(dir)
			if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.Contains(err.Error(), "users") {
				t.Fatalf("expected corrupt schema expression error containing %q and users, got %v", tt.want, err)
			}
			if _, exists := c.tables["users"]; exists {
				t.Fatal("table with corrupt schema expression should not be loaded after LoadSchema failure")
			}
		})
	}
}
