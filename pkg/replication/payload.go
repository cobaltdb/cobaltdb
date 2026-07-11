package replication

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// StatementPayloadVersion is the current wire version of replicated statement
// payloads carried inside WALEntry.Data.
const StatementPayloadVersion = 1

// StatementPayload is the logical replication unit shipped from a master to
// its slaves: the original SQL text plus its bound arguments. The stream
// position (LSN) is carried by the enclosing WALEntry, not the payload.
type StatementPayload struct {
	Version   int
	SQL       string
	Args      []interface{}
	Timestamp time.Time
}

// statementPayloadWire is the versioned JSON wire representation.
type statementPayloadWire struct {
	Version   int         `json:"v"`
	SQL       string      `json:"sql"`
	Args      []taggedArg `json:"args,omitempty"`
	Timestamp int64       `json:"ts,omitempty"` // unix nanoseconds
}

// taggedArg carries one bound argument with an explicit type tag so values
// round-trip without JSON's float64 collapse:
//
//	n=null, b=bool, i=int64, u=uint64, f=float64, s=string,
//	x=[]byte (base64), t=time.Time (RFC3339Nano)
type taggedArg struct {
	Type  string `json:"t"`
	Value string `json:"v,omitempty"`
}

// EncodeStatementPayload serializes a SQL statement and its bound arguments
// into the versioned replication wire format.
func EncodeStatementPayload(sql string, args []interface{}, ts time.Time) ([]byte, error) {
	wire := statementPayloadWire{
		Version: StatementPayloadVersion,
		SQL:     sql,
	}
	if !ts.IsZero() {
		wire.Timestamp = ts.UnixNano()
	}
	if len(args) > 0 {
		wire.Args = make([]taggedArg, len(args))
		for i, arg := range args {
			wire.Args[i] = encodeTaggedArg(arg)
		}
	}
	return json.Marshal(wire)
}

// DecodeStatementPayload deserializes a replication statement payload.
func DecodeStatementPayload(data []byte) (*StatementPayload, error) {
	var wire statementPayloadWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, fmt.Errorf("invalid statement payload: %w", err)
	}
	if wire.Version != StatementPayloadVersion {
		return nil, fmt.Errorf("unsupported statement payload version %d (supported: %d)", wire.Version, StatementPayloadVersion)
	}
	if wire.SQL == "" {
		return nil, fmt.Errorf("statement payload has empty SQL")
	}

	payload := &StatementPayload{
		Version: wire.Version,
		SQL:     wire.SQL,
	}
	if wire.Timestamp != 0 {
		payload.Timestamp = time.Unix(0, wire.Timestamp)
	}
	if len(wire.Args) > 0 {
		payload.Args = make([]interface{}, len(wire.Args))
		for i, tagged := range wire.Args {
			value, err := decodeTaggedArg(tagged)
			if err != nil {
				return nil, fmt.Errorf("statement payload arg %d: %w", i, err)
			}
			payload.Args[i] = value
		}
	}
	return payload, nil
}

func encodeTaggedArg(arg interface{}) taggedArg {
	switch v := arg.(type) {
	case nil:
		return taggedArg{Type: "n"}
	case bool:
		return taggedArg{Type: "b", Value: strconv.FormatBool(v)}
	case int:
		return taggedArg{Type: "i", Value: strconv.FormatInt(int64(v), 10)}
	case int8:
		return taggedArg{Type: "i", Value: strconv.FormatInt(int64(v), 10)}
	case int16:
		return taggedArg{Type: "i", Value: strconv.FormatInt(int64(v), 10)}
	case int32:
		return taggedArg{Type: "i", Value: strconv.FormatInt(int64(v), 10)}
	case int64:
		return taggedArg{Type: "i", Value: strconv.FormatInt(v, 10)}
	case uint:
		return taggedArg{Type: "u", Value: strconv.FormatUint(uint64(v), 10)}
	case uint8:
		return taggedArg{Type: "u", Value: strconv.FormatUint(uint64(v), 10)}
	case uint16:
		return taggedArg{Type: "u", Value: strconv.FormatUint(uint64(v), 10)}
	case uint32:
		return taggedArg{Type: "u", Value: strconv.FormatUint(uint64(v), 10)}
	case uint64:
		return taggedArg{Type: "u", Value: strconv.FormatUint(v, 10)}
	case float32:
		return taggedArg{Type: "f", Value: strconv.FormatFloat(float64(v), 'g', -1, 64)}
	case float64:
		return taggedArg{Type: "f", Value: strconv.FormatFloat(v, 'g', -1, 64)}
	case string:
		return taggedArg{Type: "s", Value: v}
	case []byte:
		return taggedArg{Type: "x", Value: base64.StdEncoding.EncodeToString(v)}
	case time.Time:
		return taggedArg{Type: "t", Value: v.Format(time.RFC3339Nano)}
	default:
		return taggedArg{Type: "s", Value: fmt.Sprintf("%v", v)}
	}
}

func decodeTaggedArg(tagged taggedArg) (interface{}, error) {
	switch tagged.Type {
	case "n":
		return nil, nil
	case "b":
		return strconv.ParseBool(tagged.Value)
	case "i":
		return strconv.ParseInt(tagged.Value, 10, 64)
	case "u":
		return strconv.ParseUint(tagged.Value, 10, 64)
	case "f":
		return strconv.ParseFloat(tagged.Value, 64)
	case "s":
		return tagged.Value, nil
	case "x":
		return base64.StdEncoding.DecodeString(tagged.Value)
	case "t":
		return time.Parse(time.RFC3339Nano, tagged.Value)
	default:
		return nil, fmt.Errorf("unknown arg type tag %q", tagged.Type)
	}
}
