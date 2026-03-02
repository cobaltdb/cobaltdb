package protocol

import (
	"testing"
)

func TestNewMySQLServer(t *testing.T) {
	server := NewMySQLServer(nil, "")
	if server == nil {
		t.Fatal("Server is nil")
	}

	if server.version != "5.7.0-CobaltDB" {
		t.Errorf("Expected default version, got %s", server.version)
	}
}

func TestNewMySQLServerWithVersion(t *testing.T) {
	server := NewMySQLServer(nil, "8.0.0-Test")
	if server.version != "8.0.0-Test" {
		t.Errorf("Expected version '8.0.0-Test', got %s", server.version)
	}
}

func TestScramblePassword(t *testing.T) {
	password := []byte("password")
	scramble := []byte("scramble_data_1234")

	result := scramblePassword(password, scramble)
	if result == nil {
		t.Error("Expected non-nil result")
	}

	if len(result) != 20 { // SHA1 produces 20 bytes
		t.Errorf("Expected 20 bytes, got %d", len(result))
	}

	// Empty password should return nil
	result = scramblePassword([]byte{}, scramble)
	if result != nil {
		t.Error("Expected nil for empty password")
	}
}

func TestReadLenEncInt(t *testing.T) {
	tests := []struct {
		data     []byte
		expected uint64
		bytes    int
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0x01}, 1, 1},
		{[]byte{0xfa}, 250, 1},
		{[]byte{0xfc, 0x01, 0x00}, 1, 3},
		{[]byte{0xfc, 0xff, 0xff}, 65535, 3},
		{[]byte{0xfd, 0x01, 0x00, 0x00}, 1, 4},
		{[]byte{0xfd, 0xff, 0xff, 0xff}, 16777215, 4},
	}

	for _, test := range tests {
		value, bytes := readLenEncInt(test.data)
		if value != test.expected || bytes != test.bytes {
			t.Errorf("readLenEncInt(%v) = %d, %d; expected %d, %d",
				test.data, value, bytes, test.expected, test.bytes)
		}
	}
}

func TestWriteLenEncInt(t *testing.T) {
	tests := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{250, []byte{0xfa}},
		{251, []byte{0xfc, 0xfb, 0x00}},
		{65535, []byte{0xfc, 0xff, 0xff}},
		{65536, []byte{0xfd, 0x00, 0x00, 0x01}},
		{16777215, []byte{0xfd, 0xff, 0xff, 0xff}},
	}

	for _, test := range tests {
		result := writeLenEncInt(test.value)
		if len(result) != len(test.expected) {
			t.Errorf("writeLenEncInt(%d) length = %d; expected %d",
				test.value, len(result), len(test.expected))
			continue
		}
		for i := range result {
			if result[i] != test.expected[i] {
				t.Errorf("writeLenEncInt(%d)[%d] = %d; expected %d",
					test.value, i, result[i], test.expected[i])
			}
		}
	}
}

func TestMySQLProtocolConstants(t *testing.T) {
	// Verify some key constants
	if MySQLComQuery != 0x03 {
		t.Errorf("Expected MySQLComQuery = 0x03, got 0x%02x", MySQLComQuery)
	}

	if MySQLComQuit != 0x01 {
		t.Errorf("Expected MySQLComQuit = 0x01, got 0x%02x", MySQLComQuit)
	}

	if MySQLTypeLong != 0x03 {
		t.Errorf("Expected MySQLTypeLong = 0x03, got 0x%02x", MySQLTypeLong)
	}

	if MySQLTypeVarchar != 0x0f {
		t.Errorf("Expected MySQLTypeVarchar = 0x0f, got 0x%02x", MySQLTypeVarchar)
	}
}
