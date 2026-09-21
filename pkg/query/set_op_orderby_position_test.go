package query

import (
	"strings"
	"testing"
)

// Standard SQL only allows ORDER BY after the final SELECT of a
// set-operation chain. Mid-chain ORDER BY used to parse and then be silently
// dropped by the executor (only the chain-level ORDER BY is applied).
func TestSetOpMidChainOrderByRejected(t *testing.T) {
	rejections := []struct {
		name string
		sql  string
	}{
		{"OrderByBeforeUnion", "SELECT id FROM t1 ORDER BY id UNION SELECT id FROM t2"},
		{"OrderByBeforeIntersect", "SELECT id FROM t1 ORDER BY id INTERSECT SELECT id FROM t2"},
		{"OrderByBeforeExcept", "SELECT id FROM t1 ORDER BY id EXCEPT SELECT id FROM t2"},
		{"OrderByMidChainAfterUnion", "SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY id UNION SELECT id FROM t3"},
	}

	for _, tc := range rejections {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseStrict(tc.sql)
			if err == nil {
				t.Fatalf("mid-chain ORDER BY parsed without error (silently dropped by the executor): %s", tc.sql)
			}
			if !strings.Contains(err.Error(), "ORDER BY") {
				t.Fatalf("error for %q should mention ORDER BY, got: %v", tc.sql, err)
			}
		})
	}
}

func TestSetOpTrailingOrderByAccepted(t *testing.T) {
	accepted := []struct {
		name string
		sql  string
	}{
		{"TrailingAfterUnion", "SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY id"},
		{"TrailingAfterIntersect", "SELECT id FROM t1 INTERSECT SELECT id FROM t2 ORDER BY id"},
		{"PlainChain", "SELECT id FROM t1 UNION SELECT id FROM t2"},
		{"IntersectPrecedenceChain", "SELECT id FROM t1 UNION SELECT id FROM t2 INTERSECT SELECT id FROM t3"},
	}

	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseStrict(tc.sql); err != nil {
				t.Fatalf("legal set-operation query should parse: %s: %v", tc.sql, err)
			}
		})
	}
}
