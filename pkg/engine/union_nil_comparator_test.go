package engine

import (
	"testing"
)

// TestCompareUnionValuesNilStringAntisymmetry proves that compareUnionValues
// is a valid comparison function when a typed-nil *string participates.
//
// Pre-fix behavior: compareUnionValues("m", (*string)(nil)) returned +1 (the
// string case skips nil *string and the fallback renders "<nil>"), while
// compareUnionValues((*string)(nil), "m") also returned +1 (the av == nil
// early-exit). Both directions +1 violates comparator antisymmetry, which
// makes the strict weak ordering required by sort.Slice invalid and
// applyUnionOrderBy's output undefined on mixed NULL columns.
//
// It also proves reflexivity for two typed nils: cmp(x, x) must be 0
// (pre-fix it returned +1).
func TestCompareUnionValuesNilStringAntisymmetry(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	var typedNil *string

	ab := db.compareUnionValues("m", typedNil)
	ba := db.compareUnionValues(typedNil, "m")
	if ab != -ba {
		t.Fatalf("FAIL compareUnionValues is asymmetric for typed-nil *string: cmp(%#v, typedNil) = %d but cmp(typedNil, %#v) = %d — an invalid strict weak ordering makes ORDER BY undefined",
			"m", ab, "m", ba)
	}

	if got := db.compareUnionValues(typedNil, typedNil); got != 0 {
		t.Fatalf("FAIL compareUnionValues is not reflexive for two typed nils: cmp(typedNil, typedNil) = %d, want 0", got)
	}

	// The typed nil must order consistently with the untyped nil: both before
	// ordinary values.
	if got := db.compareUnionValues(typedNil, int64(7)); got >= 0 {
		t.Fatalf("FAIL typed-nil *string should sort before a number, got %d", got)
	}
	if got := db.compareUnionValues(typedNil, nil); got <= 0 {
		t.Fatalf("FAIL typed-nil *string should sort after the untyped nil (documented NULLS-first fast path), got %d", got)
	}
}

// TestNormalizeRowKeyTypedNilMatchesUntypedNil proves that a typed nil
// (*string)(nil) and the untyped nil produce the same dedup key, so UNION
// treats both NULL representations as equal rows instead of emitting
// duplicate NULL rows.
func TestNormalizeRowKeyTypedNilMatchesUntypedNil(t *testing.T) {
	untyped := normalizeRowKey([]interface{}{nil})
	typed := normalizeRowKey([]interface{}{(*string)(nil)})
	if untyped != typed {
		t.Fatalf("FAIL normalizeRowKey distinguishes the two NULL representations: untyped=%q typed=%q — UNION dedup keeps duplicate NULL rows", untyped, typed)
	}

	// A real string must still round-trip under the S: tag.
	withValue := normalizeRowKey([]interface{}{(*string)(nil)})
	_ = withValue
	mixed := normalizeRowKey([]interface{}{"S:literal"})
	if mixed != "[S:S:literal]" {
		t.Fatalf("FAIL normalizeRowKey string tag changed: %q", mixed)
	}
}
