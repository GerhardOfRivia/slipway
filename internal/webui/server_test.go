package webui

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/control"
)

func TestServerRequiresLoopbackAndManagesPrivateToken(t *testing.T) {
	t.Parallel()
	manager, err := control.NewManager(control.Options{Logger: testLogger()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})
	tokenDirectory := t.TempDir()
	if err := os.Chmod(tokenDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(tokenDirectory, "web.token")
	if server, err := NewServer("0.0.0.0:0", tokenPath, manager, testLogger()); err == nil {
		_ = server.Close()
		t.Fatal("NewServer accepted a wildcard listener")
	}

	server, err := NewServer("127.0.0.1:0", tokenPath, manager, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("token mode = %04o, want 0600", info.Mode().Perm())
	}
	contents, err := os.ReadFile(tokenPath)
	if err != nil || strings.TrimSpace(string(contents)) == "" {
		t.Fatalf("token contents = %q, err = %v", contents, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx) }()
	response, err := http.Get("http://" + server.Address() + "/")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	rootBody, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Security-Policy") == "" {
		t.Fatalf("root response = %d headers=%v", response.StatusCode, response.Header)
	}
	if !strings.Contains(string(rootBody), `src="/theme-init.js"`) {
		t.Fatal("root response does not load the theme prepaint script")
	}
	response, err = http.Get("http://" + server.Address() + "/theme-init.js")
	if err != nil {
		t.Fatal(err)
	}
	themeScript, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/javascript") {
		t.Fatalf("theme script response = %d headers=%v", response.StatusCode, response.Header)
	}
	if !strings.Contains(string(themeScript), "slipway.web.theme") {
		t.Fatal("theme script response does not include the preference key")
	}
	request, err := http.NewRequest(http.MethodPost, "http://"+server.Address()+"/dashboard", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusMethodNotAllowed || response.Header.Get("Allow") != "GET, HEAD" {
		t.Fatalf("static POST response = %d headers=%v", response.StatusCode, response.Header)
	}
	response, err = http.Get("http://" + server.Address() + "/assets/missing.js")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("missing asset response = %d, want 404", response.StatusCode)
	}
	cancel()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("web server did not stop after cancellation")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("token after close: %v", err)
	}
}
