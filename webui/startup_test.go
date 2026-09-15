package main

import (
	"bytes"
	"crypto/rand"
	"errors"
	"flag"
	"html/template"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

func TestWriteBootstrapTokenFileUsesUniquePrivateFile(t *testing.T) {
	dir := t.TempDir()
	predictable := filepath.Join(dir, "cobaltdb-webui.token")
	target := filepath.Join(dir, "attacker-target")
	if err := os.WriteFile(target, []byte("unchanged"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, predictable); err != nil {
		t.Fatal(err)
	}

	path, err := writeBootstrapTokenFile(dir, "http://localhost/?token=secret")
	if err != nil {
		t.Fatalf("writeBootstrapTokenFile: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	if path == predictable {
		t.Fatalf("used predictable token path %q", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("token file mode = %o, want 600", info.Mode().Perm())
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "token=secret") {
		t.Fatalf("token file contents = %q", contents)
	}
	targetContents, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(targetContents) != "unchanged" {
		t.Fatalf("attacker target modified: %q", targetContents)
	}
}

func TestSavedQueryRendererAvoidsInlineHandlers(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("static", "app.js"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(script)
	for _, unsafe := range []string{"onclick=\"loadSavedQuery", "onclick=\"deleteSavedQuery"} {
		if strings.Contains(source, unsafe) {
			t.Fatalf("saved-query renderer still contains inline handler %q", unsafe)
		}
	}
	for _, safe := range []string{"name.textContent = item.name", "info.addEventListener('click'", "deleteButton.addEventListener('click'"} {
		if !strings.Contains(source, safe) {
			t.Fatalf("saved-query renderer missing DOM-safe pattern %q", safe)
		}
	}
}

func TestWebUIListenAddressIsLoopbackOnly(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8080", "[::1]:8080", "localhost:8080"} {
		if err := validateWebUIListenAddress(address); err != nil {
			t.Fatalf("validateWebUIListenAddress(%q): %v", address, err)
		}
	}
	for _, address := range []string{"0.0.0.0:8080", ":8080", "192.0.2.1:8080", "bad-address"} {
		if err := validateWebUIListenAddress(address); err == nil {
			t.Fatalf("validateWebUIListenAddress(%q) accepted cleartext non-loopback bind", address)
		}
	}
}

func TestStaticCacheHandlerSetsProductionHeaders(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	recorder := httptest.NewRecorder()
	staticCacheHandler(next).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/static/app.js", nil))
	if got := recorder.Header().Get("Cache-Control"); got != "public, max-age=3600" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if got := recorder.Header().Get("Expires"); got == "" {
		t.Fatal("Expires header missing")
	}
}

func TestExternalAssetsHaveSubresourceIntegrity(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("templates", "index.html"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(contents)
	for _, required := range []string{"monaco-editor@0.44.0", "font-awesome/6.4.0", "integrity=\"sha384-", "crossorigin=\"anonymous\""} {
		if !strings.Contains(source, required) {
			t.Fatalf("template missing hardened external-asset marker %q", required)
		}
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
