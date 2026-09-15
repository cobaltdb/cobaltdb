package logger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseFormat(t *testing.T) {
	tests := []struct {
		input string
		want  Format
	}{
		{input: "", want: TextFormat},
		{input: "text", want: TextFormat},
		{input: " TEXT ", want: TextFormat},
		{input: "json", want: JSONFormat},
		{input: "JSON", want: JSONFormat},
	}
	for _, tt := range tests {
		got, err := ParseFormat(tt.input)
		if err != nil {
			t.Fatalf("ParseFormat(%q): %v", tt.input, err)
		}
		if got != tt.want {
			t.Fatalf("ParseFormat(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
	if _, err := ParseFormat("xml"); err == nil {
		t.Fatal("ParseFormat accepted unsupported format")
	}
	if TextFormat.String() != "text" || JSONFormat.String() != "json" || Format(99).String() != "unknown" {
		t.Fatal("unexpected Format.String result")
	}
}

func TestLegacyNewNilOutputUsesStdout(t *testing.T) {
	log := New(InfoLevel, nil)
	if log.output != os.Stdout || log.format != TextFormat {
		t.Fatalf("New nil defaults = output %T format %s, want stdout/text", log.output, log.format)
	}
	defaults := Default()
	if defaults.output != os.Stdout || defaults.format != TextFormat {
		t.Fatalf("Default = output %T format %s, want stdout/text", defaults.output, defaults.format)
	}
}

func TestNewPreservesLegacyTextLayout(t *testing.T) {
	var buf bytes.Buffer
	New(InfoLevel, &buf).WithComponent("server").WithField("port", 4200).Info("ready")

	line := strings.TrimSuffix(buf.String(), "\n")
	if !strings.HasSuffix(line, " INFO [server] ready | port=4200") {
		t.Fatalf("legacy text layout changed: %q", line)
	}
	if len(line) < len("[2006-01-02T15:04:05.000Z]") || line[0] != '[' || line[25] != ']' {
		t.Fatalf("legacy timestamp layout changed: %q", line)
	}
}

func TestJSONLoggerEncodesEnvelopeErrorAndSafeFields(t *testing.T) {
	var buf bytes.Buffer
	log := NewWithFormat(DebugLevel, &buf, JSONFormat).
		WithComponent("engine").
		WithFields(map[string]interface{}{
			"request_id": "req-1",
			"level":      "caller-value",
			"invalid":    make(chan int),
		})
	log.Log(ErrorLevel, "open failed", errors.New("disk full"))

	var entry struct {
		Time      string                     `json:"time"`
		Level     string                     `json:"level"`
		Message   string                     `json:"msg"`
		Component string                     `json:"component"`
		Error     string                     `json:"error"`
		Fields    map[string]json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("JSON log is invalid: %v\n%s", err, buf.String())
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", entry.Time); err != nil {
		t.Fatalf("time = %q: %v", entry.Time, err)
	}
	if entry.Level != "ERROR" || entry.Message != "open failed" || entry.Component != "engine" || entry.Error != "disk full" {
		t.Fatalf("unexpected JSON envelope: %+v", entry)
	}
	if string(entry.Fields["request_id"]) != `"req-1"` {
		t.Fatalf("request_id = %s", entry.Fields["request_id"])
	}
	if string(entry.Fields["level"]) != `"caller-value"` {
		t.Fatalf("reserved field was not safely nested: %s", entry.Fields["level"])
	}
	if !json.Valid(entry.Fields["invalid"]) {
		t.Fatalf("unsupported field fallback is invalid JSON: %s", entry.Fields["invalid"])
	}
}

func TestJSONFormatPropagatesThroughDerivedLoggers(t *testing.T) {
	var buf bytes.Buffer
	base := NewWithFormat(InfoLevel, &buf, JSONFormat)
	base.WithField("one", 1).WithFields(map[string]interface{}{"two": 2}).WithComponent("derived").Info("hello")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("derived logger lost JSON format: %v\n%s", err, buf.String())
	}
	if entry["component"] != "derived" || entry["msg"] != "hello" {
		t.Fatalf("unexpected derived entry: %#v", entry)
	}
	fields, ok := entry["fields"].(map[string]interface{})
	if !ok || fields["one"] != float64(1) || fields["two"] != float64(2) {
		t.Fatalf("unexpected fields: %#v", entry["fields"])
	}
}

func TestJSONWriterAdaptsStandardLibraryLogger(t *testing.T) {
	var buf bytes.Buffer
	structured := NewWithFormat(InfoLevel, &buf, JSONFormat).WithComponent("stdlib")
	standard := log.New(structured.Writer(), "", 0)
	standard.Printf("started on %s", "127.0.0.1:4200")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("stdlib bridge emitted invalid JSON: %v\n%s", err, buf.String())
	}
	if entry["component"] != "stdlib" || entry["msg"] != "started on 127.0.0.1:4200" {
		t.Fatalf("unexpected stdlib entry: %#v", entry)
	}
}

func TestJSONLoggerConcurrentLinesRemainValid(t *testing.T) {
	const goroutines = 32
	const perGoroutine = 100

	var buf bytes.Buffer
	base := NewWithFormat(DebugLevel, &buf, JSONFormat)
	children := []*Logger{
		base,
		base.WithComponent("engine"),
		base.WithField("scope", "server"),
		base.WithFields(map[string]interface{}{"safe": true}),
	}

	var wg sync.WaitGroup
	for goroutine := 0; goroutine < goroutines; goroutine++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			child := children[id%len(children)]
			for message := 0; message < perGoroutine; message++ {
				child.Infof("worker=%d message=%d", id, message)
			}
		}(goroutine)
	}
	wg.Wait()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != goroutines*perGoroutine {
		t.Fatalf("got %d lines, want %d", len(lines), goroutines*perGoroutine)
	}
	for index, line := range lines {
		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("line %d is invalid JSON: %v\n%s", index, err, line)
		}
		if entry["level"] != "INFO" || !strings.HasPrefix(fmt.Sprint(entry["msg"]), "worker=") {
			t.Fatalf("line %d has unexpected envelope: %#v", index, entry)
		}
	}
}
