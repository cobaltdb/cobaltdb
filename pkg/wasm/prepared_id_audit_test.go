//go:build wasm_experimental

package wasm

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestPreparedStatementIDsSurviveCloseAndReplacement(t *testing.T) {
	stmt := &query.SelectStmt{Columns: []query.Expression{&query.QualifiedIdentifier{Table: "test", Column: "id"}}, From: &query.TableRef{Name: "test"}}
	for _, remove := range [][]int{nil, {0}, {1}, {0, 1}} {
		t.Run(fmt.Sprint(remove), func(t *testing.T) {
			c := NewCompiler()
			prepare := func(sql string) *PreparedStatement {
				t.Helper()
				p, err := c.Prepare(sql, stmt, 0)
				if err != nil {
					t.Fatal(err)
				}
				return p
			}
			first, second := prepare("SELECT id FROM test /* first */"), prepare("SELECT id FROM test /* second */")
			old := []*PreparedStatement{first, second}
			removed := map[string]bool{}
			for _, i := range remove {
				c.ClosePreparedStatement(old[i].ID)
				c.ClosePreparedStatement(old[i].ID)
				removed[old[i].ID] = true
			}
			third := prepare("SELECT id FROM test /* third */")
			if first.ID == second.ID || first.ID == third.ID || second.ID == third.ID {
				t.Fatal("handle identity reused")
			}
			for _, p := range old {
				compiled, err := c.ExecutePrepared(p.ID, nil)
				if removed[p.ID] {
					if err == nil || compiled != nil {
						t.Fatal("retired handle revived")
					}
				} else if err != nil || compiled.SQL != p.SQL {
					t.Fatal("live handle replaced")
				}
			}
			if _, err := c.Prepare("invalid unique SQL", nil, 0); err == nil {
				t.Fatal("invalid statement accepted")
			}
			last := prepare("SELECT id FROM test /* after-failure */")
			for cycle := 0; cycle < 20; cycle++ {
				c.ClosePreparedStatement(last.ID)
				previousID := last.ID
				last = prepare(fmt.Sprintf("SELECT id FROM test /* cycle %d */", cycle))
				if last.ID == previousID {
					t.Fatal("retired handle identity reused")
				}
				compiled, err := c.ExecutePrepared(third.ID, nil)
				if err != nil || compiled.SQL != third.SQL {
					t.Fatal("repeat cycle replaced live handle")
				}
			}
		})
	}
}
