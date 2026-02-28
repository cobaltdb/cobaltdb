package wire

import (
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	original := map[string]interface{}{
		"name":  "test",
		"value": 123,
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

	if decoded["name"] != "test" {
		t.Errorf("Expected name 'test', got %v", decoded["name"])
	}
}

func TestNewQueryMessage(t *testing.T) {
	msg := NewQueryMessage("SELECT * FROM test", "arg1", "arg2")

	if msg.SQL != "SELECT * FROM test" {
		t.Errorf("Expected SQL 'SELECT * FROM test', got %q", msg.SQL)
	}

	if len(msg.Params) != 2 {
		t.Errorf("Expected 2 params, got %d", len(msg.Params))
	}
}

func TestNewResultMessage(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	msg := NewResultMessage(columns, rows)

	if len(msg.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(msg.Columns))
	}

	if msg.Count != 2 {
		t.Errorf("Expected count 2, got %d", msg.Count)
	}

	if len(msg.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(msg.Rows))
	}
}

func TestNewOKMessage(t *testing.T) {
	msg := NewOKMessage(100, 5)

	if msg.LastInsertID != 100 {
		t.Errorf("Expected LastInsertID 100, got %d", msg.LastInsertID)
	}

	if msg.RowsAffected != 5 {
		t.Errorf("Expected RowsAffected 5, got %d", msg.RowsAffected)
	}
}

func TestNewErrorMessage(t *testing.T) {
	msg := NewErrorMessage(404, "Not found")

	if msg.Code != 404 {
		t.Errorf("Expected code 404, got %d", msg.Code)
	}

	if msg.Message != "Not found" {
		t.Errorf("Expected message 'Not found', got %q", msg.Message)
	}
}

func TestEncodeMessage(t *testing.T) {
	payload := NewQueryMessage("SELECT 1")

	data, err := EncodeMessage(MsgQuery, payload)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty data")
	}
}

func TestDecodeMessage(t *testing.T) {
	original := NewQueryMessage("SELECT 1")
	data, _ := EncodeMessage(MsgQuery, original)

	msg, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("Failed to decode message: %v", err)
	}

	if msg.Type != MsgQuery {
		t.Errorf("Expected type %d, got %d", MsgQuery, msg.Type)
	}
}
