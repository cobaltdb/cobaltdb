package catalog

import (
	"math"
	"testing"
)

func TestSubstringClampsBeforeAddingLength(t *testing.T) {
	for _, tc := range []struct {
		s        string
		start, n int
		want     string
	}{{"abc", 2, math.MaxInt, "bc"}, {"abc", 2, 2, "bc"}, {"abc", 2, 0, ""}, {"abc", 2, -1, ""}, {"héllo", 2, math.MaxInt, "éllo"}, {"abc", -1, math.MaxInt, "c"}, {"abc", math.MaxInt, math.MaxInt, ""}, {"", 1, math.MaxInt, ""}, {"abc", 0, math.MaxInt, ""}} {
		if got := runeSubstr(tc.s, tc.start, true, tc.n); got != tc.want {
			t.Fatalf("%+v: got %q", tc, got)
		}
	}
}
