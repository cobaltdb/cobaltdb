package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

func TestCreateVectorIndexIncludesAllPartitions(t *testing.T) {
	for _, partition := range []string{"", " PARTITION BY HASH(id) PARTITIONS 2", " PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (2), PARTITION p1 VALUES LESS THAN (MAXVALUE))"} {
		for _, mode := range []string{"rows", "empty", "pending", "deleted"} {
			t.Run(fmt.Sprintf("%s/%s", partition, mode), func(t *testing.T) {
				c := newTestCatalog(t)
				parse := func(sql string) query.Statement {
					t.Helper()
					s, err := query.Parse(sql)
					if err != nil {
						t.Fatal(err)
					}
					return s
				}
				if err := c.CreateTable(parse("CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(2))" + partition).(*query.CreateTableStmt)); err != nil {
					t.Fatal(err)
				}
				insert := func(sql string) {
					t.Helper()
					if _, _, err := c.Insert(context.Background(), parse(sql).(*query.InsertStmt), nil); err != nil {
						t.Fatal(err)
					}
				}
				want := 2
				if mode == "empty" {
					want = 0
				} else {
					insert("INSERT INTO docs VALUES (1, '[0, 0]'), (2, '[1, 0]'), (3, NULL)")
				}
				if mode == "pending" {
					mgr := txn.NewManager(nil)
					c.SetTxnManager(mgr)
					c.EnableBufferedWrites()
					tx := mgr.Begin(nil)
					c.BeginTransactionWithTxn(tx.ID, tx)
					insert("INSERT INTO docs VALUES (4, '[2, 0]')")
					want = 3
				}
				if mode == "deleted" {
					if _, _, err := c.Delete(context.Background(), parse("DELETE FROM docs WHERE id = 1").(*query.DeleteStmt), nil); err != nil {
						t.Fatal(err)
					}
					want = 1
				}
				if err := c.CreateVectorIndex("vec", "docs", "embedding"); err != nil {
					t.Fatal(err)
				}
				keys, dists, err := c.SearchVectorKNN("vec", []float64{0, 0}, 10)
				if err != nil {
					t.Fatal(err)
				}
				if len(keys) != want || len(dists) != want || len(c.vectorIndexes["vec"].HNSW.Nodes) != want {
					t.Fatal("missing vectors")
				}
				rangeKeys, _, err := c.SearchVectorRange("vec", []float64{0, 0}, 10)
				if err != nil || len(rangeKeys) != want {
					t.Fatalf("range=%v error=%v", rangeKeys, err)
				}
				if mode == "pending" {
					if err := c.RollbackTransaction(); err != nil {
						t.Fatal(err)
					}
					if _, exists := c.vectorIndexes["vec"]; exists {
						t.Fatal("rolled-back index survived")
					}
					if err := c.CreateVectorIndex("vec", "docs", "embedding"); err != nil {
						t.Fatal(err)
					}
					if len(c.vectorIndexes["vec"].HNSW.Nodes) != 2 {
						t.Fatal("rolled-back row indexed")
					}
				}
			})
		}
	}
}
