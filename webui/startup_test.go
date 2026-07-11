package main

import (
	"bytes"
	"crypto/rand"
	"errors"
	"flag"
	"html/template"
	"net/http"
	"os"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func withWebUIStartupGlobals(t *testing.T, args []string) {
	t.Helper()
	oldArgs, oldFlags := os.Args, flag.CommandLine
	oldListen, oldOpen, oldParse := webUIListenAndServe, webUIOpen, webUIParseTemplates
	oldRun, oldExit, oldRandom := webUIRun, webUIExit, webUIRandomRead
	os.Args = append([]string{"webui"}, args...)
	flag.CommandLine = flag.NewFlagSet("webui", flag.ContinueOnError)
	flag.CommandLine.SetOutput(&bytes.Buffer{})
	t.Cleanup(func() {
		os.Args, flag.CommandLine = oldArgs, oldFlags
		webUIListenAndServe, webUIOpen, webUIParseTemplates = oldListen, oldOpen, oldParse
		webUIRun, webUIExit, webUIRandomRead = oldRun, oldExit, oldRandom
	})
}

func TestMainDelegatesToCommandAndExit(t *testing.T) {
	withWebUIStartupGlobals(t, nil)
	want := errors.New("command failed")
	webUIRun = func() error { return want }
	var got error
	webUIExit = func(err error) { got = err }
	main()
	if !errors.Is(got, want) {
		t.Fatalf("exit error = %v", got)
	}
}

func TestDefaultClosures(t *testing.T) {
	srv := &http.Server{Addr: "127.0.0.1:1", ReadHeaderTimeout: 0}
	_ = webUIListenAndServe(srv)
	webUIExit(nil)
}

func TestTokenStoreGenerateErrorPaths(t *testing.T) {
	webUIRandomRead = func([]byte) (int, error) { return 0, errors.New("fail") }
	defer func() { webUIRandomRead = rand.Read }()
	ts := newTokenStore()
	if _, _, err := ts.mint("x", RoleReadOnly, 0, nil); err == nil {
		t.Fatal("expected generate error in mint")
	}
	if _, _, ok := ts.rotate("nonexistent"); ok {
		t.Fatal("rotate should fail")
	}
	ts.addWithID("id", "val", "n", RoleReadOnly, 0, nil)
	if _, _, ok := ts.rotate("id"); ok {
		t.Fatal("rotate should fail with generate error")
	}
}

func TestRunWebUIStartupContracts(t *testing.T) {
	t.Run("token generation failure", func(t *testing.T) {
		withWebUIStartupGlobals(t, []string{"db"})
		webUIRandomRead = func([]byte) (int, error) { return 0, errors.New("random") }
		if err := runWebUI(); err == nil {
			t.Fatal("random error lost")
		}
	})
	t.Run("missing database", func(t *testing.T) {
		withWebUIStartupGlobals(t, nil)
		if err := runWebUI(); err == nil {
			t.Fatal("missing database accepted")
		}
	})
	t.Run("unsafe flag", func(t *testing.T) {
		withWebUIStartupGlobals(t, []string{"-insecure-no-auth", "db"})
		if err := runWebUI(); err == nil {
			t.Fatal("unsafe mode accepted")
		}
	})
	t.Run("open failure", func(t *testing.T) {
		withWebUIStartupGlobals(t, []string{"db"})
		webUIOpen = func(string, *engine.Options) (*engine.DB, error) { return nil, errors.New("open") }
		if err := runWebUI(); err == nil {
			t.Fatal("open error lost")
		}
	})
	t.Run("template failure", func(t *testing.T) {
		withWebUIStartupGlobals(t, []string{"-token", "short", "db"})
		webUIOpen = func(string, *engine.Options) (*engine.DB, error) {
			return engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
		}
		webUIParseTemplates = func(...string) (*template.Template, error) { return nil, errors.New("template") }
		if err := runWebUI(); err == nil {
			t.Fatal("template error lost")
		}
	})
	t.Run("serve failure", func(t *testing.T) {
		withWebUIStartupGlobals(t, []string{"-token", "0123456789", "-rate-limit", "0", "-token-ttl", "0", "db"})
		webUIOpen = func(string, *engine.Options) (*engine.DB, error) {
			return engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
		}
		webUIParseTemplates = func(...string) (*template.Template, error) { return template.New("index").Parse("ok") }
		webUIListenAndServe = func(*http.Server) error { return errors.New("serve") }
		if err := runWebUI(); err == nil {
			t.Fatal("serve error lost")
		}
	})
	t.Run("server closed", func(t *testing.T) {
		withWebUIStartupGlobals(t, []string{"-token", "short", "db"})
		webUIOpen = func(string, *engine.Options) (*engine.DB, error) {
			return engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
		}
		webUIParseTemplates = func(...string) (*template.Template, error) { return template.New("index").Parse("ok") }
		webUIListenAndServe = func(*http.Server) error { return http.ErrServerClosed }
		if err := runWebUI(); err != nil {
			t.Fatalf("server closed: %v", err)
		}
	})
}
