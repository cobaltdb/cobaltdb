package wire

import "testing"

func FuzzDecodeMessage(f *testing.F) {
	// Valid message: type byte + msgpack payload
	valid, _ := EncodeMessage(MsgQuery, &QueryMessage{SQL: "SELECT 1"})
	f.Add(valid)
	valid2, _ := EncodeMessage(MsgAuth, &AuthMessage{Username: "u", Password: "p"})
	f.Add(valid2)
	valid3, _ := EncodeMessage(MsgResult, &ResultMessage{Columns: []string{"a"}, Rows: [][]interface{}{{1}}})
	f.Add(valid3)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Decode should never panic on arbitrary input
		_, _ = DecodeMessage(data)
	})
}
