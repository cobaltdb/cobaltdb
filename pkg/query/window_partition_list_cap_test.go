package query

import (
	"strings"
	"testing"
)

// Window-spec PARTITION BY and ORDER BY lists were the only expression lists
// in the parser without the maxParserListItems cap.
func TestWindowPartitionListCap(t *testing.T) {
	t.Run("OverCapIsRejected", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("SELECT SUM(id) OVER (PARTITION BY ")
		for i := 0; i <= maxParserListItems; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("id")
		}
		b.WriteString(") FROM t1")

		if _, err := ParseStrict(b.String()); err == nil {
			t.Fatalf("oversized window PARTITION BY list (%d items) parsed without error, want a maximum (%d) error", maxParserListItems+1, maxParserListItems)
		}
	})

	t.Run("AtCapIsAccepted", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("SELECT SUM(id) OVER (PARTITION BY ")
		for i := 0; i < maxParserListItems; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("id")
		}
		b.WriteString(") FROM t1")

		if _, err := ParseStrict(b.String()); err != nil {
			t.Fatalf("window PARTITION BY list at the cap (%d items) should parse: %v", maxParserListItems, err)
		}
	})
}

func TestWindowOrderByListCap(t *testing.T) {
	var b strings.Builder
	b.WriteString("SELECT SUM(id) OVER (PARTITION BY id ORDER BY ")
	for i := 0; i <= maxParserListItems; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("id")
	}
	b.WriteString(") FROM t1")

	if _, err := ParseStrict(b.String()); err == nil {
		t.Fatalf("oversized window ORDER BY list (%d items) parsed without error, want a maximum (%d) error", maxParserListItems+1, maxParserListItems)
	}
}
