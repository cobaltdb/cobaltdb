package wire

import (
	"github.com/vmihailenco/msgpack/v5"
)

// MsgType represents the type of a protocol message
type MsgType uint8

const (
	MsgQuery     MsgType = 0x01 // SQL query string
	MsgPrepare   MsgType = 0x02 // Prepared statement
	MsgExecute   MsgType = 0x03 // Execute prepared
	MsgResult    MsgType = 0x10 // Query result rows
	MsgOK        MsgType = 0x11 // Execution success
	MsgError     MsgType = 0x12 // Error response
	MsgPing      MsgType = 0x20
	MsgPong      MsgType = 0x21
)

// Message represents a protocol message
type Message struct {
	Type    MsgType
	Payload []byte
}

// QueryMessage represents a query request
type QueryMessage struct {
	SQL    string        `msgpack:"sql"`
	Params []interface{} `msgpack:"params,omitempty"`
}

// ResultMessage represents a query result
type ResultMessage struct {
	Columns []string         `msgpack:"cols"`
	Types   []string         `msgpack:"types"`
	Rows    [][]interface{}  `msgpack:"rows"`
	Count   int64            `msgpack:"count"`
}

// OKMessage represents a successful execution
type OKMessage struct {
	LastInsertID int64 `msgpack:"last_insert_id"`
	RowsAffected int64 `msgpack:"rows_affected"`
}

// ErrorMessage represents an error response
type ErrorMessage struct {
	Code    int    `msgpack:"code"`
	Message string `msgpack:"message"`
}

// PrepareMessage represents a prepare statement request
type PrepareMessage struct {
	SQL string `msgpack:"sql"`
}

// ExecuteMessage represents an execute prepared statement request
type ExecuteMessage struct {
	StmtID uint32          `msgpack:"stmt_id"`
	Params []interface{}   `msgpack:"params"`
}

// Encode encodes a message using MessagePack
func Encode(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Decode decodes a message using MessagePack
func Decode(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// EncodeMessage encodes a complete message with type
func EncodeMessage(msgType MsgType, payload interface{}) ([]byte, error) {
	pay, err := Encode(payload)
	if err != nil {
		return nil, err
	}

	msg := Message{
		Type:    msgType,
		Payload: pay,
	}

	return Encode(msg)
}

// DecodeMessage decodes a complete message
func DecodeMessage(data []byte) (*Message, error) {
	var msg Message
	if err := Decode(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// NewQueryMessage creates a new query message
func NewQueryMessage(sql string, params ...interface{}) *QueryMessage {
	return &QueryMessage{
		SQL:    sql,
		Params: params,
	}
}

// NewResultMessage creates a new result message
func NewResultMessage(columns []string, rows [][]interface{}) *ResultMessage {
	types := make([]string, len(columns))
	for i := range types {
		types[i] = "unknown"
	}
	return &ResultMessage{
		Columns: columns,
		Types:   types,
		Rows:    rows,
		Count:   int64(len(rows)),
	}
}

// NewOKMessage creates a new OK message
func NewOKMessage(lastInsertID, rowsAffected int64) *OKMessage {
	return &OKMessage{
		LastInsertID: lastInsertID,
		RowsAffected: rowsAffected,
	}
}

// NewErrorMessage creates a new error message
func NewErrorMessage(code int, message string) *ErrorMessage {
	return &ErrorMessage{
		Code:    code,
		Message: message,
	}
}
