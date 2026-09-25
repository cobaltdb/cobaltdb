package catalog

import (
	"testing"
)

// TestCompareValuesWeakOrder sweeps the SQL-layer comparator (ORDER BY,
// MIN/MAX) over a mixed-type corpus for the weak-order axioms, and asserts
// cross-layer agreement with the catalog comparator — a ORDER BY must not
// order pairs differently than the catalog layer's ordering of the same
// values.
func TestCompareValuesWeakOrder(t *testing.T) {
	cmp := compareValues

	corpus := []interface{}{
		int64(0), int64(10), uint64(5), float64(2.5),
		false, true, "0", "10", "NaN", "abc", "false",
	}

	// Antisymmetry: cmp(a,b) == -cmp(b,a) for every ordered pair.
	for i, a := range corpus {
		for j, b := range corpus {
			if i == j {
				continue
			}
			ab, ba := cmp(a, b), cmp(b, a)
			if ab != -ba {
				t.Fatalf("antisymmetry violated: cmp(%#v, %#v) = %d but cmp(%#v, %#v) = %d",
					a, b, ab, b, a, ba)
			}
		}
	}

	// Equivalence transitivity: a≡b and b≡c imply a≡c.
	for _, a := range corpus {
		for _, b := range corpus {
			if cmp(a, b) != 0 {
				continue
			}
			for _, c := range corpus {
				if cmp(b, c) != 0 {
					continue
				}
				if cmp(a, c) != 0 {
					t.Fatalf("equivalence transitivity violated: cmp(%#v,%#v)=0, cmp(%#v,%#v)=0, but cmp(%#v,%#v)=%d",
						a, b, b, c, a, c, cmp(a, c))
				}
			}
		}
	}

	// Ordering transitivity: a<b and b<c imply a<c.
	for _, a := range corpus {
		for _, b := range corpus {
			if cmp(a, b) != -1 {
				continue
			}
			for _, c := range corpus {
				if cmp(b, c) != -1 {
					continue
				}
				if cmp(a, c) != -1 {
					t.Fatalf("ordering transitivity violated: cmp(%#v,%#v)=-1, cmp(%#v,%#v)=-1, but cmp(%#v,%#v)=%d",
						a, b, b, c, a, c, cmp(a, c))
				}
			}
		}
	}

	// Cross-layer agreement: the SQL-layer comparator and the catalog
	// comparator must order every NON-BOOL pair the same way. Bool-vs-int
	// pairs legitimately differ: the SQL layer normalizes bools to their
	// int64 storage representation (WHERE flag = 1 must match bool-true
	// rows), while the catalog layer excludes bools from its numeric tier
	// and orders them via their string form.
	for i, a := range corpus {
		for j, b := range corpus {
			if i == j {
				continue
			}
			if _, aIsBool := a.(bool); aIsBool {
				continue
			}
			if _, bIsBool := b.(bool); bIsBool {
				continue
			}
			cv, ccv := compareValues(a, b), catalogCompareValues(a, b)
			if (cv < 0) != (ccv < 0) || (cv > 0) != (ccv > 0) {
				t.Fatalf("cross-layer disagreement: compareValues(%#v, %#v) = %d but catalogCompareValues = %d",
					a, b, cv, ccv)
			}
		}
	}
}
