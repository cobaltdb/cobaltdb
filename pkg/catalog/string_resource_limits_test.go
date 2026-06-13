package catalog

import (
	"math"
	"strings"
	"testing"
)

func TestStringAllocationFunctionsRejectUnsafeSizes(t *testing.T) {
	tests := []struct {
		name string
		fn   func() error
	}{
		{
			name: "repeat infinite count",
			fn: func() error {
				return evalStringRepeat([]interface{}{"x", math.Inf(1)}).err
			},
		},
		{
			name: "repeat oversized count",
			fn: func() error {
				return evalStringRepeat([]interface{}{"xx", float64(maxStringResultLen)}).err
			},
		},
		{
			name: "lpad infinite target",
			fn: func() error {
				return evalStringLPad([]interface{}{"x", math.Inf(1), "0"}).err
			},
		},
		{
			name: "rpad NaN target",
			fn: func() error {
				return evalStringRPad([]interface{}{"x", math.NaN(), "0"}).err
			},
		},
		{
			name: "zeroblob infinite size",
			fn: func() error {
				_, err := scalarFunctionHandlers["ZEROBLOB"]([]interface{}{math.Inf(1)})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if err == nil {
				t.Fatal("expected unsafe string allocation size to be rejected")
			}
			if !strings.Contains(err.Error(), "exceeds maximum allowed size") {
				t.Fatalf("expected size limit error, got %v", err)
			}
		})
	}
}
