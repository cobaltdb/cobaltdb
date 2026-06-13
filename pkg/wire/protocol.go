package wire

import (
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

// MsgType represents the type of a protocol message
type MsgType uint8

const (
	MsgQuery       MsgType = 0x01 // SQL query string
	MsgPrepare     MsgType = 0x02 // Prepared statement
	MsgExecute     MsgType = 0x03 // Execute prepared
	MsgResult      MsgType = 0x10 // Query result rows
	MsgOK          MsgType = 0x11 // Execution success
	MsgError       MsgType = 0x12 // Error response
	MsgPing        MsgType = 0x20
	MsgPong        MsgType = 0x21
	MsgAuth        MsgType = 0x30 // Authentication request
	MsgAuthSuccess MsgType = 0x31 // Authentication success
	MsgAuthFailed  MsgType = 0x32 // Authentication failed
)

const maxWireEncodedMessageBytes = 16 * 1024 * 1024
const maxWireCloneDepth = 64

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
	Columns []string        `msgpack:"cols"`
	Types   []string        `msgpack:"types"`
	Rows    [][]interface{} `msgpack:"rows"`
	Count   int64           `msgpack:"count"`
}

// OKMessage represents a successful execution
type OKMessage struct {
	LastInsertID int64  `msgpack:"last_insert_id"`
	RowsAffected int64  `msgpack:"rows_affected"`
	StmtID       uint32 `msgpack:"stmt_id,omitempty"` // Set for prepared statement OK
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
	StmtID uint32        `msgpack:"stmt_id"`
	Params []interface{} `msgpack:"params"`
}

// Encode encodes a message using MessagePack
func Encode(v interface{}) ([]byte, error) {
	data, err := msgpack.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(data) > maxWireEncodedMessageBytes {
		return nil, fmt.Errorf("encoded value too large: %d bytes", len(data))
	}
	return data, nil
}

// Decode decodes a message using MessagePack
func Decode(data []byte, v interface{}) error {
	if len(data) > maxWireEncodedMessageBytes {
		return fmt.Errorf("encoded value too large: %d bytes", len(data))
	}
	return msgpack.Unmarshal(data, v)
}

// EncodeMessage encodes a complete message with type
func EncodeMessage(msgType MsgType, payload interface{}) ([]byte, error) {
	if !isKnownMsgType(msgType) {
		return nil, fmt.Errorf("unknown message type: 0x%02x", byte(msgType))
	}
	pay, err := msgpack.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if len(pay) > maxWireEncodedMessageBytes {
		return nil, fmt.Errorf("message payload too large: %d bytes", len(pay))
	}

	msg := Message{
		Type:    msgType,
		Payload: pay,
	}

	data, err := Encode(msg)
	if err != nil {
		return nil, err
	}
	if len(data) > maxWireEncodedMessageBytes {
		return nil, fmt.Errorf("encoded message too large: %d bytes", len(data))
	}
	return data, nil
}

// DecodeMessage decodes a complete message
func DecodeMessage(data []byte) (*Message, error) {
	if len(data) > maxWireEncodedMessageBytes {
		return nil, fmt.Errorf("encoded message too large: %d bytes", len(data))
	}
	var msg Message
	if err := Decode(data, &msg); err != nil {
		return nil, err
	}
	if !isKnownMsgType(msg.Type) {
		return nil, fmt.Errorf("unknown message type: 0x%02x", byte(msg.Type))
	}
	if len(msg.Payload) > maxWireEncodedMessageBytes {
		return nil, fmt.Errorf("message payload too large: %d bytes", len(msg.Payload))
	}
	return &msg, nil
}

func isKnownMsgType(msgType MsgType) bool {
	switch msgType {
	case MsgQuery, MsgPrepare, MsgExecute, MsgResult, MsgOK, MsgError,
		MsgPing, MsgPong, MsgAuth, MsgAuthSuccess, MsgAuthFailed:
		return true
	default:
		return false
	}
}

// NewQueryMessage creates a new query message
func NewQueryMessage(sql string, params ...interface{}) *QueryMessage {
	return &QueryMessage{
		SQL:    sql,
		Params: cloneValues(params),
	}
}

// NewResultMessage creates a new result message
func NewResultMessage(columns []string, rows [][]interface{}) *ResultMessage {
	types := make([]string, len(columns))
	for i := range types {
		types[i] = "unknown"
	}
	return &ResultMessage{
		Columns: cloneStrings(columns),
		Types:   types,
		Rows:    cloneRows(rows),
		Count:   int64(len(rows)),
	}
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func cloneRows(rows [][]interface{}) [][]interface{} {
	if rows == nil {
		return nil
	}
	cloned := make([][]interface{}, len(rows))
	for i, row := range rows {
		cloned[i] = cloneValuesWithState(row, 0, make(map[wireCloneVisit]struct{}))
	}
	return cloned
}

func cloneValues(values []interface{}) []interface{} {
	return cloneValuesWithState(values, 0, make(map[wireCloneVisit]struct{}))
}

func cloneValuesWithState(values []interface{}, depth int, seen map[wireCloneVisit]struct{}) []interface{} {
	if values == nil {
		return nil
	}
	if depth > maxWireCloneDepth {
		return nil
	}
	visit := wireCloneVisitFor(values)
	if visit.ptr != 0 {
		if _, exists := seen[visit]; exists {
			return nil
		}
		seen[visit] = struct{}{}
		defer delete(seen, visit)
	}
	cloned := make([]interface{}, len(values))
	for i, value := range values {
		cloned[i] = cloneValue(value, depth+1, seen)
	}
	return cloned
}

type wireCloneVisit struct {
	kind reflect.Kind
	ptr  uintptr
}

func wireCloneVisitFor(value interface{}) wireCloneVisit {
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Map, reflect.Slice:
		return wireCloneVisit{kind: rv.Kind(), ptr: rv.Pointer()}
	default:
		return wireCloneVisit{}
	}
}

func cloneValue(value interface{}, depth int, seen map[wireCloneVisit]struct{}) interface{} {
	if depth > maxWireCloneDepth {
		return nil
	}
	switch typed := value.(type) {
	case []byte:
		if typed == nil {
			return []byte(nil)
		}
		cloned := make([]byte, len(typed))
		copy(cloned, typed)
		return cloned
	case []interface{}:
		if typed == nil {
			return []interface{}(nil)
		}
		visit := wireCloneVisitFor(typed)
		if visit.ptr != 0 {
			if _, exists := seen[visit]; exists {
				return nil
			}
		}
		return cloneValuesWithState(typed, depth, seen)
	case []string:
		return cloneStrings(typed)
	case map[string]interface{}:
		if typed == nil {
			return map[string]interface{}(nil)
		}
		visit := wireCloneVisitFor(typed)
		if visit.ptr != 0 {
			if _, exists := seen[visit]; exists {
				return nil
			}
			seen[visit] = struct{}{}
			defer delete(seen, visit)
		}
		cloned := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			cloned[key] = cloneValue(nested, depth+1, seen)
		}
		return cloned
	case map[string]string:
		if typed == nil {
			return map[string]string(nil)
		}
		cloned := make(map[string]string, len(typed))
		for key, nested := range typed {
			cloned[key] = nested
		}
		return cloned
	default:
		return typed
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

// AuthMessage represents an authentication request
type AuthMessage struct {
	Username string `msgpack:"username"`
	Password string `msgpack:"password"`
}

// AuthSuccessMessage represents a successful authentication response
type AuthSuccessMessage struct {
	Token    string `msgpack:"token"`
	Username string `msgpack:"username"`
}

// AuthFailedMessage represents a failed authentication response
type AuthFailedMessage struct {
	Reason string `msgpack:"reason"`
}

// NewAuthSuccessMessage creates a new auth success message
func NewAuthSuccessMessage(token string) *AuthSuccessMessage {
	return &AuthSuccessMessage{
		Token: token,
	}
}
