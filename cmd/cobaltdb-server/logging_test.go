package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cblogger "github.com/cobaltdb/cobaltdb/pkg/logger"
)

func TestResolveLogFormatPrecedence(t *testing.T) {
	tests := []struct {
		name        string
		configValue string
		envValue    string
		want        cblogger.Format
		wantError   bool
	}{
		{name: "DefaultsToText", want: cblogger.TextFormat},
		{name: "ConfigJSON", configValue: "json", want: cblogger.JSONFormat},
		{name: "EnvironmentOverridesConfig", configValue: "json", envValue: "text", want: cblogger.TextFormat},
		{name: "EnvironmentJSON", configValue: "text", envValue: "JSON", want: cblogger.JSONFormat},
		{name: "InvalidConfig", configValue: "xml", wantError: true},
		{name: "InvalidEnvironment", configValue: "json", envValue: "xml", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveLogFormat(tt.configValue, tt.envValue)
			if tt.wantError {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveLogFormat: %v", err)
			}
			if got != tt.want {
				t.Fatalf("format = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestLoadConfigFileReadsLogFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cobaltdb.conf")
	if err := os.WriteFile(path, []byte("[logging]\nlog_format = \"json\"\n"), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	values, err := loadConfigFile(path)
	if err != nil {
		t.Fatalf("loadConfigFile: %v", err)
	}
	if values.LogFormat != "json" {
		t.Fatalf("LogFormat = %q, want json", values.LogFormat)
	}
}

func TestConfigureServerLoggingRoutesEveryPathThroughJSON(t *testing.T) {
	oldGlobal := cblogger.GetGlobalLogger()
	oldWriter := log.Writer()
	oldFlags := log.Flags()
	oldPrefix := log.Prefix()
	t.Cleanup(func() {
		cblogger.SetGlobalLogger(oldGlobal)
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})

	var output bytes.Buffer
	serverLog := configureServerLogging(cblogger.JSONFormat, &output)
	serverLog.Info("server path")
	serverLog.WithComponent("engine").Info("engine path")
	cblogger.Info("global path")
	log.Print("stdlib path")

	wantComponents := map[string]bool{
		"server": false,
		"engine": false,
		"global": false,
		"stdlib": false,
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != len(wantComponents) {
		t.Fatalf("got %d lines, want %d: %s", len(lines), len(wantComponents), output.String())
	}
	for index, line := range lines {
		var entry struct {
			Component string `json:"component"`
			Message   string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("line %d is not JSON: %v\n%s", index, err, line)
		}
		if _, ok := wantComponents[entry.Component]; !ok {
			t.Fatalf("unexpected component %q in %s", entry.Component, line)
		}
		wantComponents[entry.Component] = true
	}
	for component, seen := range wantComponents {
		if !seen {
			t.Errorf("component %q did not log", component)
		}
	}
}

type serverLogEntry struct {
	Component string `json:"component"`
	Message   string `json:"msg"`
}

func decodeServerLogLine(t *testing.T, index int, line string) serverLogEntry {
	t.Helper()
	var entry serverLogEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("stderr line %d is not JSON: %v\n%s", index, err, line)
	}
	return entry
}

func TestServerSubprocessEmitsOnlyJSONLinesToStderr(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and starts the server subprocess")
	}

	binary := filepath.Join(t.TempDir(), "cobaltdb-server")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build server: %v\n%s", err, output)
	}

	cmd := exec.Command(binary,
		"-memory",
		"-mysql=false",
		"-health-server=false",
		"-auth=false",
		"-addr=127.0.0.1:0",
	)
	cmd.Env = append(os.Environ(), "COBALTDB_LOG_FORMAT=json")
	cmd.Stdout = io.Discard
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	lines := make(chan string, 128)
	scanErr := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		scanErr <- scanner.Err()
		close(lines)
	}()

	seen := map[string]bool{}
	allLines := make([]string, 0, 16)
	ready := false
	deadline := time.NewTimer(15 * time.Second)
	for !ready {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("stderr closed before ready; lines=%v", allLines)
			}
			allLines = append(allLines, line)
			entry := decodeServerLogLine(t, len(allLines)-1, line)
			seen[entry.Component] = true
			ready = entry.Message == "Server is ready. Press Ctrl+C to shutdown gracefully."
		case err := <-done:
			t.Fatalf("server exited before ready: %v; lines=%v", err, allLines)
		case <-deadline.C:
			_ = cmd.Process.Kill()
			<-done
			t.Fatalf("server did not become ready; lines=%v", allLines)
		}
	}
	deadline.Stop()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("signal server: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server shutdown: %v; lines=%v", err, allLines)
		}
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("server did not stop after interrupt; lines=%v", allLines)
	}

	for line := range lines {
		allLines = append(allLines, line)
		entry := decodeServerLogLine(t, len(allLines)-1, line)
		seen[entry.Component] = true
	}
	if err := <-scanErr; err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	if len(allLines) == 0 {
		t.Fatal("server emitted no stderr logs")
	}
	for _, component := range []string{"stdlib", "engine", "server"} {
		if !seen[component] {
			t.Errorf("server subprocess did not exercise %q logger; components=%v", component, seen)
		}
	}
}
