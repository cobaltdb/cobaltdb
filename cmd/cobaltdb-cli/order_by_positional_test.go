package main

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestPositionalOrderByMatchesMySQL pins MySQL positional-ORDER-BY semantics.
// Data is deliberately uncorrelated: name order != id order, so every case
// discriminates.
func TestPositionalOrderByMatchesMySQL(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE pt (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, r := range [][2]interface{}{{1, "charlie"}, {2, "alpha"}, {3, "bravo"}} {
		if _, err := db.Exec(ctx, "INSERT INTO pt (id, name) VALUES (?,?)", r[0], r[1]); err != nil {
			t.Fatalf("insert %v: %v", r, err)
		}
	}

	ids := func(sql string) ([]int, error) {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var got []int
		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				return nil, err
			}
			got = append(got, id)
		}
		return got, nil
	}

	cases := []struct {
		label   string
		sql     string
		want    []int
		wantErr bool // MySQL accepts these; engine gap if it errors
	}{
		{"named control", `SELECT id, name FROM pt ORDER BY id`, []int{1, 2, 3}, false},
		{"positional 1, explicit list", `SELECT id, name FROM pt ORDER BY 1`, []int{1, 2, 3}, false},
		{"positional 2 (name asc)", `SELECT id, name FROM pt ORDER BY 2`, []int{2, 3, 1}, false},
		{"positional 1 DESC", `SELECT id, name FROM pt ORDER BY 1 DESC`, []int{3, 2, 1}, false},
		{"positional 2 DESC (name desc)", `SELECT id, name FROM pt ORDER BY 2 DESC`, []int{1, 3, 2}, false},
		{"star + named control", `SELECT * FROM pt ORDER BY id`, []int{1, 2, 3}, false},
		{"star + positional 1 (MySQL: first column)", `SELECT * FROM pt ORDER BY 1`, []int{1, 2, 3}, false},
	}
	var fails []string
	for _, tc := range cases {
		got, err := ids(tc.sql)
		if tc.wantErr {
			if err == nil {
				fails = append(fails, tc.label+": expected an error, got rows "+orderByIDList(got))
			}
			continue
		}
		if err != nil {
			fails = append(fails, tc.label+" FAILED: "+err.Error())
			continue
		}
		if len(got) != len(tc.want) {
			fails = append(fails, tc.label+": got "+orderByIDList(got)+", want "+orderByIDList(tc.want))
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				fails = append(fails, tc.label+": got "+orderByIDList(got)+", want "+orderByIDList(tc.want))
				break
			}
		}
	}
	if len(fails) > 0 {
		t.Fatalf("FAIL: %d positional-ORDER-BY case(s) diverge from MySQL:\n  %s", len(fails), strings.Join(fails, "\n  "))
	}
	t.Log("PASS: positional ORDER BY matches MySQL semantics in every case")
}

func orderByIDList(ids []int) string {
	if len(ids) == 0 {
		return "[]"
	}
	out := "["
	for i, v := range ids {
		if i > 0 {
			out += " "
		}
		out += strconv.Itoa(v)
	}
	return out + "]"
}
