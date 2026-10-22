package catalog

import (
	"errors"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

func TestForUpdateRejectsVersionNewerThanQueryTime(t *testing.T) {
	for _, advance := range []bool{false, true} {
		t.Run(map[bool]string{false: "visible", true: "newer_version"}[advance], func(t *testing.T) {
			cat := newTestCatalog(t)
			for _, sql := range []string{
				"CREATE TABLE ctr (id INT PRIMARY KEY, n INT)",
				"INSERT INTO ctr VALUES (1, 200)",
			} {
				if _, err := cat.ExecuteQuery(sql); err != nil {
					t.Fatal(err)
				}
			}
			// Model a commit crossing a second boundary after SELECT captured
			// its query time, before its FOR UPDATE read-set scan.
			created := time.Now().Add(-time.Hour)
			if advance {
				created = time.Now().Add(time.Hour)
			}
			value, err := encodeVersionedRow([]interface{}{int64(1), int64(200)}, &created)
			if err != nil {
				t.Fatal(err)
			}
			if err := cat.tableTrees["ctr"].Put([]byte("00000000000000000001"), value); err != nil {
				t.Fatal(err)
			}
			cat.BeginTransaction(1)
			defer cat.RollbackTransaction()
			parsed, err := query.Parse("SELECT n FROM ctr WHERE id = 1 FOR UPDATE")
			if err != nil {
				t.Fatal(err)
			}
			_, rows, err := cat.Select(parsed.(*query.SelectStmt), nil)
			if advance {
				if !errors.Is(err, txn.ErrConflict) {
					t.Fatalf("newer version returned rows=%v, err=%v; want retryable conflict", rows, err)
				}
			} else if err != nil || len(rows) != 1 || rows[0][0] != int64(200) {
				t.Fatalf("visible version returned rows=%v, err=%v", rows, err)
			}
		})
	}
}
