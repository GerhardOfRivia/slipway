package webui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/GerhardOfRivia/slipway/internal/control"
	"github.com/GerhardOfRivia/slipway/internal/queue"
)

const testWebToken = "test-web-token"

func TestQueueAPIListsFiltersAndDetails(t *testing.T) {
	t.Parallel()
	manager, known, succeededJob, commandID := webTestQueue(t, true)
	handler, err := newHandler(manager, testLogger(), testWebToken)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	response := webRequest(t, server.Client(), http.MethodGet, server.URL+"/api/v1/queues", "", "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", response.StatusCode)
	}
	_ = response.Body.Close()

	response = webRequest(t, server.Client(), http.MethodGet, server.URL+"/api/v1/queues", testWebToken, "")
	var queues queuesResponse
	decodeResponse(t, response, http.StatusOK, &queues)
	if len(queues.Queues) != 1 {
		t.Fatalf("queues = %+v", queues.Queues)
	}
	summary := queues.Queues[0]
	if summary.ID != queueID(known.Identity) || summary.DatabaseState != "ready" ||
		summary.Counts.Total != 2 || summary.Counts.Queued != 1 || summary.Counts.Succeeded != 1 ||
		summary.ActiveInstance == nil {
		t.Fatalf("queue summary = %+v", summary)
	}

	response = webRequest(t, server.Client(), http.MethodGet,
		server.URL+"/api/v1/queues/"+summary.ID+"/jobs?status=queued&limit=1", testWebToken, "")
	var jobs jobsResponse
	decodeResponse(t, response, http.StatusOK, &jobs)
	if len(jobs.Jobs) != 1 || jobs.Jobs[0].Status != queue.StatusQueued || jobs.Limit != 1 {
		t.Fatalf("filtered jobs = %+v", jobs)
	}

	response = webRequest(t, server.Client(), http.MethodGet,
		fmt.Sprintf("%s/api/v1/queues/%s/jobs/%d", server.URL, summary.ID, succeededJob), testWebToken, "")
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("job detail status = %d, body = %s", response.StatusCode, body)
	}
	if bytes.Contains(body, []byte("SHOULD_NOT_LEAK")) {
		t.Fatalf("job detail exposed persisted environment: %s", body)
	}
	var detail jobResponse
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Job.ID != succeededJob || len(detail.Runs) != 1 || len(detail.Runs[0].Commands) != 1 ||
		detail.Runs[0].Commands[0].ID != commandID || detail.Runs[0].Commands[0].StdoutBytes != int64(len("processed\n")) {
		t.Fatalf("job detail = %+v", detail)
	}

	response = webRequest(t, server.Client(), http.MethodGet,
		fmt.Sprintf("%s/api/v1/queues/%s/commands/%d/output", server.URL, summary.ID, commandID), testWebToken, "")
	var output commandOutputResponse
	decodeResponse(t, response, http.StatusOK, &output)
	if output.Stdout != "processed\n" || output.Stderr != "warning\n" {
		t.Fatalf("command output = %+v", output)
	}
}

func TestQueueAPIMutationsRequireSameOriginAndRetainStoppedQueue(t *testing.T) {
	t.Parallel()
	manager, known, _, _ := webTestQueue(t, true)
	handler, err := newHandler(manager, testLogger(), testWebToken)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	instance := manager.List(false)[0]

	request, err := http.NewRequest(http.MethodPost,
		server.URL+"/api/v1/instances/"+instance.ID+"/stop", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+testWebToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-slipway-Web", "1")
	request.Header.Set("Origin", "http://evil.example")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("cross-origin stop status = %d, want 400", response.StatusCode)
	}
	_ = response.Body.Close()

	response = webMutation(t, server, "/api/v1/instances/"+instance.ID+"/stop")
	decodeResponse(t, response, http.StatusOK, &instanceResponse{})
	if len(manager.List(false)) != 0 || len(manager.KnownQueues()) != 1 {
		t.Fatalf("stopped queue disappeared: active=%+v queues=%+v", manager.List(false), manager.KnownQueues())
	}

	root := filepath.Dir(known.ConfigPath)
	writeWebConfig(t, root, "incoming", filepath.Join(root, "moved.db"))
	response = webMutation(t, server, "/api/v1/queues/"+queueID(known.Identity)+"/start")
	decodeResponse(t, response, http.StatusConflict, &errorEnvelope{})
	if len(manager.List(false)) != 0 {
		t.Fatalf("stale queue start launched an instance: %+v", manager.List(false))
	}
	writeWebConfig(t, root, "incoming", known.DatabasePath)
	response = webMutation(t, server, "/api/v1/queues/"+queueID(known.Identity)+"/start")
	decodeResponse(t, response, http.StatusCreated, &instanceResponse{})
}

func TestQueueAPIIisolatesMissingDatabase(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	readyConfig := writeWebConfig(t, root, "ready", filepath.Join(root, "ready.db"))
	missingConfig := writeWebConfig(t, root, "missing", filepath.Join(root, "missing.db"))
	store, err := queue.Open(filepath.Join(root, "ready.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	manager := newWebManager(t)
	if _, err := manager.StartMany([]string{readyConfig, missingConfig}, ""); err != nil {
		t.Fatal(err)
	}
	handler, err := newHandler(manager, testLogger(), testWebToken)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	response := webRequest(t, server.Client(), http.MethodGet, server.URL+"/api/v1/queues", testWebToken, "")
	var queues queuesResponse
	decodeResponse(t, response, http.StatusOK, &queues)
	if len(queues.Queues) != 2 {
		t.Fatalf("queues = %+v", queues.Queues)
	}
	states := map[string]string{}
	for _, summary := range queues.Queues {
		states[summary.DisplayName] = summary.DatabaseState
	}
	if states["ready"] != "ready" || states["missing"] != "missing" {
		t.Fatalf("database states = %+v", states)
	}
}

func webTestQueue(t *testing.T, active bool) (*control.Manager, control.KnownQueue, int64, int64) {
	t.Helper()
	root := t.TempDir()
	databasePath := filepath.Join(root, "queue.db")
	configPath := writeWebConfig(t, root, "incoming", databasePath)
	store, err := queue.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	job, _, err := store.Enqueue(ctx, queue.EnqueueParams{
		WatchName: "incoming", Path: "/drop/processed.csv", Fingerprint: "done", MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	commandID, err := store.StartCommand(ctx, queue.CommandStart{
		RunID: claimed.RunID, Sequence: 0, Name: "process", Program: "/bin/process",
		Args: []string{"--input", job.Path}, Env: []string{"SECRET=SHOULD_NOT_LEAK"}, Timeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteCommand(ctx, commandID, queue.CommandResult{
		Status: queue.CommandSucceeded, ExitCode: 0, Stdout: "processed\n", Stderr: "warning\n",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Succeed(ctx, claimed.ID, claimed.RunID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Enqueue(ctx, queue.EnqueueParams{
		WatchName: "incoming", Path: "/drop/waiting.csv", Fingerprint: "waiting", MaxRetries: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	manager := newWebManager(t)
	instances, err := manager.StartMany([]string{configPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !active {
		stopContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := manager.Stop(stopContext, instances[0].ID); err != nil {
			t.Fatal(err)
		}
	}
	known := manager.KnownQueues()
	if len(known) != 1 {
		t.Fatalf("known queues = %+v", known)
	}
	return manager, known[0], job.ID, commandID
}

func newWebManager(t *testing.T) *control.Manager {
	t.Helper()
	manager, err := control.NewManager(control.Options{
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			<-ctx.Done()
			return ctx.Err()
		},
		Logger: testLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})
	return manager
}

func writeWebConfig(t *testing.T, root, name, databasePath string) string {
	t.Helper()
	watchPath := filepath.Join(root, name+"-watch")
	if err := os.MkdirAll(watchPath, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, name+".yaml")
	contents := fmt.Sprintf(`queue:
  workers: 1
database:
  path: %q
watches:
  - name: %s
    path: %q
    pipeline:
      - name: noop
        program: /bin/true
`, databasePath, name, watchPath)
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return configPath
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func webRequest(t *testing.T, client *http.Client, method, target, token, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func webMutation(t *testing.T, server *httptest.Server, path string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+testWebToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-slipway-Web", "1")
	request.Header.Set("Origin", server.URL)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeResponse(t *testing.T, response *http.Response, status int, destination any) {
	t.Helper()
	defer response.Body.Close()
	contents, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("status = %d, want %d; body = %s", response.StatusCode, status, contents)
	}
	if err := json.Unmarshal(contents, destination); err != nil {
		t.Fatalf("decode response: %v; body = %s", err, contents)
	}
}
