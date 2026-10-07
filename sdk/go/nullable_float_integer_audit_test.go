package cobaltdb

import (
	"database/sql"
	"fmt"
	"math"
	"testing"
)

func TestNullInt64RejectsLossyFloatConversion(t *testing.T) {
	for _, value := range []float64{42.5, -42.5, 0.5, math.NaN(), math.Inf(1), math.Inf(-1), float64(math.MaxInt64), math.Nextafter(float64(math.MinInt64), math.Inf(-1))} {
		v := NullInt64{Int64: 7, Valid: true}
		err := v.Scan(value)
		var reference sql.NullInt64
		if reference.Scan(value) == nil {
			t.Fatal("reference accepts invalid input", value)
		}
		fmt.Printf("EXPECTED: conversion error for %v; ACTUAL: %v\n", value, err)
		if err == nil {
			t.Fatal("invalid float silently accepted", value, v)
		}
	}
	for _, tc := range []struct {
		value float64
		want  int64
	}{{0, 0}, {42, 42}, {-42, -42}, {float64(math.MinInt64), math.MinInt64}, {math.Nextafter(float64(math.MaxInt64), math.Inf(-1)), 9223372036854774784}} {
		var v NullInt64
		err := v.Scan(tc.value)
		if err != nil || !v.Valid || v.Int64 != tc.want {
			t.Fatal("representable integer", tc.value, v, err)
		}
	}
	v := NullInt64{Int64: 7, Valid: true}
	if err := v.Scan([]byte("123")); err != nil || v.Int64 != 123 {
		t.Fatal("bytes", v, err)
	}
	if err := v.Scan(nil); err != nil || v.Valid || v.Int64 != 0 {
		t.Fatal("null reset", v, err)
	}
	if err := v.Scan(float64(42)); err != nil {
		t.Fatal(err)
	}
	if value, err := v.Value(); err != nil || value != int64(42) {
		t.Fatal("valuer", value, err)
	}
	fmt.Println("FIX VERIFIED")
}
