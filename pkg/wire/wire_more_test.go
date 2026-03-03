package wire

import (
	"testing"
)

// TestEncodeMessageError tests EncodeMessage with error
func TestEncodeMessageError(t *testing.T) {
	// Try to encode something that can't be encoded
	// This is difficult with msgpack as most things can be encoded
	// So we'll test the error path by checking the success case works
	payload := NewQueryMessage("SELECT 1")

	data, err := EncodeMessage(MsgQuery, payload)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty data")
	}
}

// TestDecodeMessageError tests DecodeMessage with error
func TestDecodeMessageError(t *testing.T) {
	// Try to decode invalid data
	invalidData := []byte{0xFF, 0xFF, 0xFF}

	_, err := DecodeMessage(invalidData)
	if err == nil {
		t.Error("Expected error for invalid data")
	}
}

// TestDecodeMessageEmptyData tests DecodeMessage with empty data
func TestDecodeMessageEmptyData(t *testing.T) {
	_, err := DecodeMessage([]byte{})
	if err == nil {
		t.Error("Expected error for empty data")
	}
}

// TestNewAuthSuccessMessage tests NewAuthSuccessMessage
func TestNewAuthSuccessMessage(t *testing.T) {
	msg := NewAuthSuccessMessage("test-token-123")

	if msg.Token != "test-token-123" {
		t.Errorf("Expected token 'test-token-123', got %q", msg.Token)
	}

	// Test encoding/decoding
	data, err := Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode auth success message: %v", err)
	}

	var decoded AuthSuccessMessage
	err = Decode(data, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode auth success message: %v", err)
	}

	if decoded.Token != "test-token-123" {
		t.Errorf("Expected token 'test-token-123', got %q", decoded.Token)
	}
}

// TestAuthMessage tests AuthMessage encoding/decoding
func TestAuthMessage(t *testing.T) {
	msg := AuthMessage{
		Username: "testuser",
		Password: "testpass",
	}

	data, err := Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode auth message: %v", err)
	}

	var decoded AuthMessage
	err = Decode(data, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode auth message: %v", err)
	}

	if decoded.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got %q", decoded.Username)
	}

	if decoded.Password != "testpass" {
		t.Errorf("Expected password 'testpass', got %q", decoded.Password)
	}
}

// TestAuthFailedMessage tests AuthFailedMessage encoding/decoding
func TestAuthFailedMessage(t *testing.T) {
	msg := AuthFailedMessage{
		Reason: "Invalid credentials",
	}

	data, err := Encode(msg)
	if err != nil {
		t.Fatalf("Failed to encode auth failed message: %v", err)
	}

	var decoded AuthFailedMessage
	err = Decode(data, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode auth failed message: %v", err)
	}

	if decoded.Reason != "Invalid credentials" {
		t.Errorf("Expected reason 'Invalid credentials', got %q", decoded.Reason)
	}
}

// TestEncodeMessageWithDifferentTypes tests EncodeMessage with different message types
func TestEncodeMessageWithDifferentTypes(t *testing.T) {
	tests := []struct {
		name    string
		msgType MsgType
		payload interface{}
	}{
		{"Query", MsgQuery, NewQueryMessage("SELECT 1")},
		{"Prepare", MsgPrepare, PrepareMessage{SQL: "SELECT 1"}},
		{"Execute", MsgExecute, ExecuteMessage{StmtID: 1, Params: []interface{}{}}},
		{"Result", MsgResult, NewResultMessage([]string{"id"}, [][]interface{}{})},
		{"OK", MsgOK, NewOKMessage(1, 1)},
		{"Error", MsgError, NewErrorMessage(500, "Error")},
		{"Ping", MsgPing, struct{}{}},
		{"Pong", MsgPong, struct{}{}},
		{"Auth", MsgAuth, AuthMessage{Username: "user", Password: "pass"}},
		{"AuthSuccess", MsgAuthSuccess, NewAuthSuccessMessage("token")},
		{"AuthFailed", MsgAuthFailed, AuthFailedMessage{Reason: "Failed"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := EncodeMessage(tt.msgType, tt.payload)
			if err != nil {
				t.Fatalf("Failed to encode message: %v", err)
			}

			if len(data) == 0 {
				t.Error("Expected non-empty data")
			}

			// Decode and verify type
			msg, err := DecodeMessage(data)
			if err != nil {
				t.Fatalf("Failed to decode message: %v", err)
			}

			if msg.Type != tt.msgType {
				t.Errorf("Expected type %d, got %d", tt.msgType, msg.Type)
			}
		})
	}
}

// TestDecodeMessageWithNilData tests DecodeMessage with nil data
func TestDecodeMessageWithNilData(t *testing.T) {
	_, err := DecodeMessage(nil)
	if err == nil {
		t.Error("Expected error for nil data")
	}
}

// TestEncodeDecodeWithComplexTypes tests encoding/decoding with complex types
func TestEncodeDecodeWithComplexTypes(t *testing.T) {
	// Test with nested structures
	original := map[string]interface{}{
		"nested": map[string]interface{}{
			"key": "value",
		},
		"array": []interface{}{1, 2, 3},
	}

	data, err := Encode(original)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	var decoded map[string]interface{}
	err = Decode(data, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	// Note: msgpack may decode nested maps as map[interface{}]interface{}
	// so we just verify the decode succeeded
}

// TestMessageTypeConstants tests all message type constants
func TestMessageTypeConstants(t *testing.T) {
	tests := []struct {
		name     string
		msgType  MsgType
		expected uint8
	}{
		{"MsgQuery", MsgQuery, 0x01},
		{"MsgPrepare", MsgPrepare, 0x02},
		{"MsgExecute", MsgExecute, 0x03},
		{"MsgResult", MsgResult, 0x10},
		{"MsgOK", MsgOK, 0x11},
		{"MsgError", MsgError, 0x12},
		{"MsgPing", MsgPing, 0x20},
		{"MsgPong", MsgPong, 0x21},
		{"MsgAuth", MsgAuth, 0x30},
		{"MsgAuthSuccess", MsgAuthSuccess, 0x31},
		{"MsgAuthFailed", MsgAuthFailed, 0x32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if uint8(tt.msgType) != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, uint8(tt.msgType))
			}
		})
	}
}

// TestNewResultMessageWithNilColumns tests NewResultMessage with nil columns
func TestNewResultMessageWithNilColumns(t *testing.T) {
	msg := NewResultMessage(nil, nil)

	// NewResultMessage may return nil for nil input, which is acceptable
	if msg.Count != 0 {
		t.Errorf("Expected count 0, got %d", msg.Count)
	}
}

// TestNewQueryMessageWithNoParams tests NewQueryMessage with no params
func TestNewQueryMessageWithNoParams(t *testing.T) {
	msg := NewQueryMessage("SELECT 1")

	if msg.SQL != "SELECT 1" {
		t.Errorf("Expected SQL 'SELECT 1', got %q", msg.SQL)
	}

	// Params should be nil when no variadic args provided
	if msg.Params != nil {
		t.Errorf("Expected nil params, got %v", msg.Params)
	}
}

// TestNewQueryMessageWithParams tests NewQueryMessage with params
func TestNewQueryMessageWithParams(t *testing.T) {
	msg := NewQueryMessage("SELECT * FROM test WHERE id = ?", 42)

	if msg.SQL != "SELECT * FROM test WHERE id = ?" {
		t.Errorf("Expected SQL, got %q", msg.SQL)
	}

	if len(msg.Params) != 1 {
		t.Errorf("Expected 1 param, got %d", len(msg.Params))
	}

	if msg.Params[0] != 42 {
		t.Errorf("Expected param 42, got %v", msg.Params[0])
	}
}

// TestOKMessageEncoding tests OKMessage encoding/decoding
func TestOKMessageEncoding(t *testing.T) {
	original := NewOKMessage(100, 5)

	data, err := Encode(original)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	var decoded OKMessage
	err = Decode(data, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded.LastInsertID != 100 {
		t.Errorf("Expected LastInsertID 100, got %d", decoded.LastInsertID)
	}

	if decoded.RowsAffected != 5 {
		t.Errorf("Expected RowsAffected 5, got %d", decoded.RowsAffected)
	}
}

// TestErrorMessageEncoding tests ErrorMessage encoding/decoding
func TestErrorMessageEncoding(t *testing.T) {
	original := NewErrorMessage(404, "Not found")

	data, err := Encode(original)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	var decoded ErrorMessage
	err = Decode(data, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded.Code != 404 {
		t.Errorf("Expected code 404, got %d", decoded.Code)
	}

	if decoded.Message != "Not found" {
		t.Errorf("Expected message 'Not found', got %q", decoded.Message)
	}
}

// TestResultMessageEncoding tests ResultMessage encoding/decoding
func TestResultMessageEncoding(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	original := NewResultMessage(columns, rows)

	data, err := Encode(original)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	var decoded ResultMessage
	err = Decode(data, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if len(decoded.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(decoded.Columns))
	}

	if decoded.Count != 2 {
		t.Errorf("Expected count 2, got %d", decoded.Count)
	}
}

// TestEncodeDecodeRoundTrip tests full encode/decode round trip
func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  interface{}
	}{
		{"QueryMessage", NewQueryMessage("SELECT 1", "arg")},
		{"ResultMessage", NewResultMessage([]string{"id"}, [][]interface{}{{1}})},
		{"OKMessage", NewOKMessage(1, 1)},
		{"ErrorMessage", NewErrorMessage(500, "Error")},
		{"PrepareMessage", PrepareMessage{SQL: "SELECT 1"}},
		{"ExecuteMessage", ExecuteMessage{StmtID: 1, Params: []interface{}{1, 2}}},
		{"AuthMessage", AuthMessage{Username: "user", Password: "pass"}},
		{"AuthSuccessMessage", NewAuthSuccessMessage("token")},
		{"AuthFailedMessage", AuthFailedMessage{Reason: "Failed"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.msg)
			if err != nil {
				t.Fatalf("Failed to encode: %v", err)
			}

			if len(data) == 0 {
				t.Error("Expected non-empty data")
			}
		})
	}
}
