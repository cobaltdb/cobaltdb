package query

import (
	"fmt"
	"testing"
)

func TestVectorDimensionsRejectMalformedDeclaration(t *testing.T) {
	for _, parse := range []struct {
		name string
		fn   func(string) (Statement, error)
	}{{"Parse", Parse}, {"ParseStrict", ParseStrict}} {
		for _, dim := range []string{"1", "3", "768"} {
			s, err := parse.fn("CREATE TABLE sample (embedding VECTOR(" + dim + "))")
			if err != nil || s.(*CreateTableStmt).Columns[0].Dimensions <= 0 {
				t.Fatalf("valid dimensions %s (%s): %v", dim, parse.name, err)
			}
		}
		for _, dim := range []string{"2.5", "0", "-1", "", "text", "99999999999999999999999999999999999999", "3,4", "3 4", "1e2"} {
			_, err := parse.fn("CREATE TABLE sample (embedding VECTOR(" + dim + "))")
			fmt.Printf("EXPECTED: error for dimensions %q; ACTUAL: %v (%s)\n", dim, err, parse.name)
			if err == nil {
				t.Fatalf("invalid dimensions accepted: %q", dim)
			}
		}
		for _, sql := range []string{"CREATE TABLE sample (embedding VECTOR)", "CREATE TABLE sample (label VARCHAR(255), amount DECIMAL(10,2))", "ALTER TABLE sample ADD COLUMN embedding VECTOR(3)", "CREATE FOREIGN TABLE sample (embedding VECTOR(3)) WRAPPER 'csv'"} {
			if _, err := parse.fn(sql); err != nil {
				t.Fatalf("valid neighboring declaration %s: %v", sql, err)
			}
		}
		if _, err := parse.fn("CREATE TABLE sample (embedding VECTOR(3)"); err == nil {
			t.Fatal("missing table close accepted")
		}
	}
	fmt.Println("FIX VERIFIED")
}
