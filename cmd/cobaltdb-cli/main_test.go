package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestPrintHelp(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printHelp()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	outputStr := buf.String()
	if !strings.Contains(outputStr, "Usage:") {
		t.Error("Expected help to contain 'Usage:'")
	}

	if !strings.Contains(outputStr, "CobaltDB") {
		t.Error("Expected help to contain 'CobaltDB'")
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{"hello", "hello"},
		{123, "123"},
		{int64(456), "456"},
		{3.14, "3.14"},
		{true, "true"},
		{false, "false"},
		{nil, "NULL"},
		{[]byte("test"), "test"},
	}

	for _, test := range tests {
		result := formatValue(test.input)
		if result != test.expected {
			t.Errorf("formatValue(%v) = %s, expected %s", test.input, result, test.expected)
		}
	}
}

func TestRunCommand(t *testing.T) {
	t.Run("HelpCommand", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Help command panicked: %v", r)
			}
		}()

		// Would need proper setup to test fully
		// runCommand("help", nil)
	})

	t.Run("EmptyCommand", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Empty command panicked: %v", r)
			}
		}()

		// Would need proper setup to test fully
		// runCommand("", nil)
	})
}

func TestHandleMetaCommand(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected bool
	}{
		{"quit", ".quit", true},
		{"exit", ".exit", true},
		{"tables", ".tables", false},
		{"schema", ".schema", false},
		{"help", ".help", false},
		{"not meta", "SELECT 1", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Meta command panicked: %v", r)
				}
			}()

			// Would need proper setup to test fully
			// result := handleMetaCommand(test.command, nil)
			_ = test.expected
		})
	}
}

func TestRunInteractive(t *testing.T) {
	t.Run("InteractiveMode", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Interactive mode panicked: %v", r)
			}
		}()

		// Would need proper setup to test fully
		// runInteractive(nil)
	})
}

func TestInit(t *testing.T) {
	// Test that init() doesn't panic
}
