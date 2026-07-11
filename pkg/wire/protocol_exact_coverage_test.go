package wire

import (
	"strings"
	"testing"
)

func TestEncodeReportsMarshalErrors(t *testing.T) {
	if _, err := Encode(make(chan int)); err == nil {
		t.Fatal("Encode accepted an unsupported channel")
	}
	if _, err := EncodeMessage(MsgQuery, make(chan int)); err == nil {
		t.Fatal("EncodeMessage accepted an unsupported channel payload")
	}
}

func TestCloneValuesDeepCopiesSupportedContainers(t *testing.T) {
	var nilBytes []byte
	var nilValues []interface{}
	var nilStrings []string
	var nilAnyMap map[string]interface{}
	var nilStringMap map[string]string
	bytesValue := []byte("abc")
	stringsValue := []string{"x"}
	anyMap := map[string]interface{}{"bytes": bytesValue}
	stringMap := map[string]string{"key": "value"}
	values := []interface{}{nilBytes, nilValues, nilStrings, nilAnyMap, nilStringMap, bytesValue, stringsValue, anyMap, stringMap, 7}

	cloned := cloneValues(values)
	bytesValue[0] = 'z'
	stringsValue[0] = "changed"
	anyMap["new"] = true
	stringMap["key"] = "changed"
	if got := string(cloned[5].([]byte)); got != "abc" {
		t.Fatalf("cloned bytes = %q, want abc", got)
	}
	if got := cloned[6].([]string)[0]; got != "x" {
		t.Fatalf("cloned strings = %q, want x", got)
	}
	if _, exists := cloned[7].(map[string]interface{})["new"]; exists {
		t.Fatal("cloned map aliases input")
	}
	if got := cloned[8].(map[string]string)["key"]; got != "value" {
		t.Fatalf("cloned string map value = %q", got)
	}
	if cloned[9] != 7 {
		t.Fatalf("scalar clone = %v, want 7", cloned[9])
	}
}

func TestCloneValuesStopsCyclesAndExcessiveDepth(t *testing.T) {
	cyclicSlice := make([]interface{}, 1)
	cyclicSlice[0] = cyclicSlice
	if got := cloneValues(cyclicSlice); got[0] != nil {
		t.Fatalf("cyclic slice clone = %#v, want nil cycle edge", got)
	}
	seen := map[wireCloneVisit]struct{}{wireCloneVisitFor(cyclicSlice): {}}
	if got := cloneValuesWithState(cyclicSlice, 1, seen); got != nil {
		t.Fatalf("already-seen values clone = %#v, want nil", got)
	}
	if got := cloneValue(cyclicSlice, 1, seen); got != nil {
		t.Fatalf("already-seen slice clone = %#v, want nil", got)
	}
	cyclicMap := map[string]interface{}{}
	cyclicMap["self"] = cyclicMap
	if got := cloneValues([]interface{}{cyclicMap})[0].(map[string]interface{})["self"]; got != nil {
		t.Fatalf("cyclic map edge = %#v, want nil", got)
	}
	if got := cloneValuesWithState([]interface{}{1}, maxWireCloneDepth+1, map[wireCloneVisit]struct{}{}); got != nil {
		t.Fatalf("over-depth clone = %#v, want nil", got)
	}
	if got := cloneValue(1, maxWireCloneDepth+1, map[wireCloneVisit]struct{}{}); got != nil {
		t.Fatalf("over-depth value = %#v, want nil", got)
	}
	if visit := wireCloneVisitFor(7); visit.ptr != 0 {
		t.Fatalf("scalar visit = %+v, want zero", visit)
	}
}

func TestWireOversizeErrorsRemainSpecific(t *testing.T) {
	_, err := EncodeMessage(MsgQuery, strings.Repeat("x", maxWireEncodedMessageBytes))
	if err == nil || !strings.Contains(err.Error(), "payload too large") {
		t.Fatalf("oversize error = %v, want payload too large", err)
	}
}
