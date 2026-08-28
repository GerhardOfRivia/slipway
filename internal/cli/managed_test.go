package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/GerhardOfRivia/slipway/internal/control"
	"github.com/GerhardOfRivia/slipway/internal/daemon"
)

func TestManagedCommandsLifecycle(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "worker.yaml")
	databasePath := filepath.Join(root, "worker.db")
	if err := os.WriteFile(configPath, []byte("# loaded by the injected manager\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	runnerStarted := make(chan struct{}, 1)
	runnerStopped := make(chan struct{}, 1)
	startedAt := time.Date(2026, 8, 25, 18, 30, 0, 0, time.UTC)
	manager, err := control.NewManager(control.Options{
		Loader: func(path string) (*config.Config, error) {
			if path != configPath {
				return nil, fmt.Errorf("unexpected config path %q", path)
			}
			return &config.Config{Database: config.DatabaseConfig{Path: databasePath}}, nil
		},
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			runnerStarted <- struct{}{}
			<-ctx.Done()
			runnerStopped <- struct{}{}
			return ctx.Err()
		},
		IDGenerator: func() (string, error) { return "abc123def456", nil },
		Clock:       func() time.Time { return startedAt },
	})
	if err != nil {
		t.Fatal(err)
	}

	socketPath := filepath.Join(root, "control", "slipway.sock")
	server, err := control.NewServer(socketPath, manager, nil)
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(serveContext)
	}()
	t.Cleanup(func() {
		cancelServe()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("control server shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			_ = server.Close()
			t.Error("timed out waiting for control server shutdown")
		}
	})

	code, stdout, stderr := managedCLI(t,
		"start", "--config", configPath, "--name", "nightly", "--socket", socketPath,
	)
	if code != 0 || stderr != "" {
		t.Fatalf("start code/stderr = %d, %q", code, stderr)
	}
	wantActiveFields := []string{
		"ID", "NAME", "STATUS", "STARTED", "CONFIG",
		"abc123def456", "nightly", "running", startedAt.Format(time.RFC3339Nano), strconv.Quote(configPath),
	}
	if got := strings.Fields(stdout); !reflect.DeepEqual(got, wantActiveFields) {
		t.Fatalf("start output fields = %q, want %q", got, wantActiveFields)
	}
	select {
	case <-runnerStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("managed runner did not start")
	}

	code, stdout, stderr = managedCLI(t, "ps", "--socket", socketPath)
	if code != 0 || stderr != "" {
		t.Fatalf("ps code/stderr = %d, %q", code, stderr)
	}
	if got := strings.Fields(stdout); !reflect.DeepEqual(got, wantActiveFields) {
		t.Fatalf("ps output fields = %q, want %q", got, wantActiveFields)
	}

	code, stdout, stderr = managedCLI(t, "stop", "--socket", socketPath, "nightly")
	if code != 0 || stderr != "" {
		t.Fatalf("stop code/stderr = %d, %q", code, stderr)
	}
	if got, want := stdout, "abc123def456\n"; got != want {
		t.Fatalf("stop output = %q, want %q", got, want)
	}
	select {
	case <-runnerStopped:
	case <-time.After(3 * time.Second):
		t.Fatal("stop returned before the managed runner stopped")
	}

	code, stdout, stderr = managedCLI(t, "ps", "--socket", socketPath)
	if code != 0 || stderr != "" {
		t.Fatalf("ps after stop code/stderr = %d, %q", code, stderr)
	}
	wantEmptyFields := []string{"ID", "NAME", "STATUS", "STARTED", "CONFIG"}
	if got := strings.Fields(stdout); !reflect.DeepEqual(got, wantEmptyFields) {
		t.Fatalf("ps after stop output fields = %q, want %q", got, wantEmptyFields)
	}

	code, stdout, stderr = managedCLI(t, "ps", "--all", "--socket", socketPath)
	if code != 0 || stderr != "" {
		t.Fatalf("ps --all code/stderr = %d, %q", code, stderr)
	}
	wantAllFields := []string{
		"ID", "NAME", "STATUS", "STARTED", "FINISHED", "CONFIG", "ERROR",
		"abc123def456", "nightly", "exited", startedAt.Format(time.RFC3339Nano),
		startedAt.Format(time.RFC3339Nano), strconv.Quote(configPath), "-",
	}
	if got := strings.Fields(stdout); !reflect.DeepEqual(got, wantAllFields) {
		t.Fatalf("ps --all output fields = %q, want %q", got, wantAllFields)
	}
}

func TestManagedCommandsReportUnavailableDaemon(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "worker.yaml")
	if err := os.WriteFile(configPath, []byte("# discovery only\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	socketDirectory := filepath.Join(root, "control")
	if err := os.Mkdir(socketDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(socketDirectory, "missing.sock")

	tests := []struct {
		name string
		args []string
	}{
		{name: "start", args: []string{"start", "--config", configPath, "--socket", socketPath}},
		{name: "ps", args: []string{"ps", "--socket", socketPath}},
		{name: "stop", args: []string{"stop", "--socket", socketPath, "nightly"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, stdout, stderr := managedCLI(t, test.args...)
			if code != 1 {
				t.Fatalf("Run(%v) code = %d, want 1; stderr = %q", test.args, code, stderr)
			}
			if stdout != "" {
				t.Errorf("Run(%v) stdout = %q, want empty", test.args, stdout)
			}
			want := "slipway daemon is unavailable at " + socketPath
			if !strings.Contains(stderr, want) {
				t.Errorf("Run(%v) stderr = %q, want it to contain %q", test.args, stderr, want)
			}
			if !strings.Contains(stderr, "start it with `slipwayd`") {
				t.Errorf("Run(%v) stderr = %q, want slipwayd startup guidance", test.args, stderr)
			}
		})
	}
}

func TestRunCommandFallsBackLocallyWithoutDaemon(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "worker.yaml")
	databasePath := filepath.Join(root, "worker.db")
	notDirectory := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(notDirectory, []byte("watch roots must be directories\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configuration := fmt.Sprintf(`
database: {path: %q}
watches:
  - name: incoming
    path: %q
    pipeline: [{name: inspect, program: /bin/true}]
`, databasePath, notDirectory)
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}

	missingSocket := filepath.Join(root, "missing.sock")
	code, stdout, stderr := managedCLI(t, "run", "--config", configPath, "--name", "local-worker", "--socket", missingSocket)
	if code != 1 {
		t.Fatalf("run code = %d, want 1; stdout = %q, stderr = %q", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "slipway daemon not running; running daemonless") || !strings.Contains(stderr, missingSocket) {
		t.Fatalf("run stderr = %q, want daemonless fallback notice for %s", stderr, missingSocket)
	}
	if !strings.Contains(stderr, "is not a directory") || !strings.Contains(stderr, configPath) {
		t.Fatalf("run stderr = %q, want local runtime error for %s", stderr, configPath)
	}
	if _, err := os.Stat(databasePath); err != nil {
		t.Fatalf("daemonless run did not create its queue database: %v", err)
	}
}

func TestRunCommandUsesAvailableDaemon(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "worker.yaml")
	databasePath := filepath.Join(root, "worker.db")
	if err := os.WriteFile(configPath, []byte("# accepted by the daemon's injected loader\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	runnerCalled := make(chan struct{}, 1)
	manager, err := control.NewManager(control.Options{
		Loader: func(path string) (*config.Config, error) {
			if path != configPath {
				return nil, fmt.Errorf("unexpected config path %q", path)
			}
			return &config.Config{Database: config.DatabaseConfig{Path: databasePath}}, nil
		},
		Runner: func(_ context.Context, _ *config.Config, logger *slog.Logger) error {
			runnerCalled <- struct{}{}
			logger.Info("daemon-backed payload", "sequence", 7)
			return nil
		},
		IDGenerator: func() (string, error) { return "abc123def456", nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	socketPath := filepath.Join(root, "control", "slipway.sock")
	server, err := control.NewServer(socketPath, manager, nil)
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveContext) }()
	t.Cleanup(func() {
		cancelServe()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("control server shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			_ = server.Close()
			t.Error("timed out waiting for control server shutdown")
		}
	})

	code, stdout, stderr := managedCLI(t,
		"run", "--config", configPath, "--name", "foreground", "--socket", socketPath,
	)
	if code != 0 || stderr != "" {
		t.Fatalf("run code/stderr = %d, %q; stdout = %q", code, stderr, stdout)
	}
	if !strings.Contains(stdout, "daemon-backed payload") || !strings.Contains(stdout, "sequence=7") {
		t.Fatalf("run stdout = %q, want streamed daemon log", stdout)
	}
	if strings.Contains(stdout, "daemonless") {
		t.Fatalf("run stdout = %q, want no fallback notice", stdout)
	}
	select {
	case <-runnerCalled:
	default:
		t.Fatal("run did not invoke the daemon-managed runner")
	}
	instances := manager.List(true)
	if len(instances) != 1 || instances[0].Name != "foreground" || instances[0].State != control.StateExited {
		t.Fatalf("daemon instances after run = %+v", instances)
	}
}

func TestRunSelectionPrefersDaemonAndStreamsLogs(t *testing.T) {
	configPath := writePlaceholderRunConfig(t, "worker.yaml")
	localCalled := false
	client := &stubRunControlClient{
		socketPath: "/private/slipway.sock",
		run: func(_ context.Context, path, name string, onEvent func(control.RunEvent) error) (control.Instance, error) {
			if path != configPath || name != "nightly" {
				t.Fatalf("daemon Run path/name = %q, %q", path, name)
			}
			instance := control.Instance{ID: "000000000001", State: control.StateRunning}
			if err := onEvent(control.RunEvent{Type: "started", Instance: instance}); err != nil {
				return instance, err
			}
			if err := onEvent(control.RunEvent{Type: "log", Log: "daemon payload\n"}); err != nil {
				return instance, err
			}
			instance.State = control.StateExited
			return instance, nil
		},
	}
	var stdout, stderr bytes.Buffer
	err := runSelectedConfigsPreferDaemon(
		context.Background(), configPath, " nightly ", &stdout, &stderr, client,
		func(context.Context, []daemon.NamedConfig, *slog.Logger) error {
			localCalled = true
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if localCalled {
		t.Fatal("daemonless runner was called while daemon was available")
	}
	if got, want := stdout.String(), "daemon payload\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunSelectionFallsBackOnlyWhenDaemonIsNotRunning(t *testing.T) {
	configPath := writePlaceholderRunConfig(t, "worker.yaml")
	t.Run("missing listener", func(t *testing.T) {
		localCalled := false
		client := &stubRunControlClient{
			socketPath: "/private/missing.sock",
			listErr: &control.DaemonUnavailableError{
				SocketPath: "/private/missing.sock",
				Err:        syscall.ENOENT,
			},
		}
		var stdout, stderr bytes.Buffer
		err := runSelectedConfigsPreferDaemon(
			context.Background(), configPath, "", &stdout, &stderr, client,
			func(_ context.Context, configs []daemon.NamedConfig, logger *slog.Logger) error {
				localCalled = true
				if len(configs) != 1 || configs[0].Path != configPath {
					t.Fatalf("daemonless configs = %+v", configs)
				}
				logger.Info("local payload")
				return nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if !localCalled {
			t.Fatal("daemonless runner was not called")
		}
		if !strings.Contains(stderr.String(), "slipway daemon not running; running daemonless") ||
			!strings.Contains(stderr.String(), client.socketPath) {
			t.Fatalf("stderr = %q, want fallback notice", stderr.String())
		}
		if !strings.Contains(stdout.String(), "local payload") {
			t.Fatalf("stdout = %q, want daemonless log", stdout.String())
		}
	})

	t.Run("permission denied", func(t *testing.T) {
		localCalled := false
		client := &stubRunControlClient{
			socketPath: "/private/denied.sock",
			listErr: &control.DaemonUnavailableError{
				SocketPath: "/private/denied.sock",
				Err:        syscall.EACCES,
			},
		}
		var stdout, stderr bytes.Buffer
		err := runSelectedConfigsPreferDaemon(
			context.Background(), configPath, "", &stdout, &stderr, client,
			func(context.Context, []daemon.NamedConfig, *slog.Logger) error {
				localCalled = true
				return nil
			},
		)
		if !errors.Is(err, syscall.EACCES) || !strings.Contains(err.Error(), "check daemon") {
			t.Fatalf("error = %v, want daemon permission error", err)
		}
		if localCalled {
			t.Fatal("permission error triggered daemonless execution")
		}
		if stdout.Len() != 0 || stderr.Len() != 0 {
			t.Fatalf("stdout/stderr = %q / %q, want empty", stdout.String(), stderr.String())
		}
	})
}

func TestRunSelectionDoesNotFallBackAfterDaemonRunFailure(t *testing.T) {
	configPath := writePlaceholderRunConfig(t, "worker.yaml")
	localCalled := false
	want := &control.APIError{StatusCode: 422, Code: "start_failed", Message: "invalid daemon config"}
	client := &stubRunControlClient{
		socketPath: "/private/slipway.sock",
		run: func(context.Context, string, string, func(control.RunEvent) error) (control.Instance, error) {
			return control.Instance{}, want
		},
	}
	err := runSelectedConfigsPreferDaemon(
		context.Background(), configPath, "", io.Discard, io.Discard, client,
		func(context.Context, []daemon.NamedConfig, *slog.Logger) error {
			localCalled = true
			return nil
		},
	)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if localCalled {
		t.Fatal("daemon run failure triggered daemonless execution")
	}
}

func TestRunSelectionReportsDaemonManagedFailure(t *testing.T) {
	configPath := writePlaceholderRunConfig(t, "worker.yaml")
	client := &stubRunControlClient{
		socketPath: "/private/slipway.sock",
		run: func(context.Context, string, string, func(control.RunEvent) error) (control.Instance, error) {
			return control.Instance{ID: "000000000002", State: control.StateFailed, Error: "runner exploded"}, nil
		},
	}
	err := runSelectedConfigsPreferDaemon(
		context.Background(), configPath, "", io.Discard, io.Discard, client,
		func(context.Context, []daemon.NamedConfig, *slog.Logger) error {
			t.Fatal("daemonless runner was called")
			return nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "runner exploded") {
		t.Fatalf("error = %v, want daemon runner failure", err)
	}
}

func TestRunSelectionUsesDaemonConcurrentlyForDirectory(t *testing.T) {
	root := t.TempDir()
	configDirectory := filepath.Join(root, "configs")
	if err := os.Mkdir(configDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	paths := []string{
		writePlaceholderRunConfigAt(t, configDirectory, "first.yaml"),
		writePlaceholderRunConfigAt(t, configDirectory, "second.yml"),
	}
	started := make(chan string, len(paths))
	release := make(chan struct{})
	client := &stubRunControlClient{
		socketPath: "/private/slipway.sock",
		run: func(_ context.Context, path, _ string, onEvent func(control.RunEvent) error) (control.Instance, error) {
			started <- path
			<-release
			instance := control.Instance{ID: filepath.Base(path), State: control.StateRunning}
			if err := onEvent(control.RunEvent{Type: "started", Instance: instance}); err != nil {
				return instance, err
			}
			if err := onEvent(control.RunEvent{Type: "log", Log: filepath.Base(path) + "\n"}); err != nil {
				return instance, err
			}
			instance.State = control.StateExited
			return instance, nil
		},
	}
	var stdout bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- runSelectedConfigsPreferDaemon(
			context.Background(), configDirectory, "", &stdout, io.Discard, client,
			func(context.Context, []daemon.NamedConfig, *slog.Logger) error {
				return errors.New("unexpected daemonless execution")
			},
		)
	}()
	seen := make(map[string]bool, len(paths))
	for range paths {
		select {
		case path := <-started:
			seen[path] = true
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("daemon config runs were not launched concurrently")
		}
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("daemon directory run did not finish")
	}
	for _, path := range paths {
		if !seen[path] || !strings.Contains(stdout.String(), filepath.Base(path)) {
			t.Fatalf("started = %+v, stdout = %q; missing %s", seen, stdout.String(), path)
		}
	}
}

func TestRunSelectionStopsDaemonInstanceOnCancellation(t *testing.T) {
	configPath := writePlaceholderRunConfig(t, "worker.yaml")
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	stopped := make(chan string, 1)
	client := &stubRunControlClient{
		socketPath: "/private/slipway.sock",
		run: func(ctx context.Context, _ string, _ string, onEvent func(control.RunEvent) error) (control.Instance, error) {
			instance := control.Instance{ID: "000000000003", State: control.StateRunning}
			if err := onEvent(control.RunEvent{Type: "started", Instance: instance}); err != nil {
				return instance, err
			}
			close(started)
			<-ctx.Done()
			return instance, ctx.Err()
		},
		stop: func(_ context.Context, selector string) (control.Instance, error) {
			stopped <- selector
			return control.Instance{ID: selector, State: control.StateExited}, nil
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- runSelectedConfigsPreferDaemon(
			ctx, configPath, "", io.Discard, io.Discard, client,
			func(context.Context, []daemon.NamedConfig, *slog.Logger) error {
				return errors.New("unexpected daemonless execution")
			},
		)
	}()
	select {
	case <-started:
		cancel()
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("daemon instance did not start")
	}
	select {
	case selector := <-stopped:
		if selector != "000000000003" {
			t.Fatalf("stopped selector = %q", selector)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not stop daemon instance")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("canceled foreground run error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled foreground run did not return")
	}
}

func TestRunSelectedConfigsAcceptsConfigDirectory(t *testing.T) {
	root := t.TempDir()
	configDirectory := filepath.Join(root, "configs")
	watchDirectory := filepath.Join(root, "incoming")
	if err := os.Mkdir(configDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(watchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	names := []string{"first.yaml", "second.yml"}
	for _, name := range names {
		configuration := fmt.Sprintf(`
database: {path: %q}
watches:
  - name: incoming
    path: %q
    pipeline: [{name: inspect, program: /bin/true}]
`, filepath.Join(root, name+".db"), watchDirectory)
		if err := os.WriteFile(filepath.Join(configDirectory, name), []byte(configuration), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan []daemon.NamedConfig, 1)
	done := make(chan error, 1)
	go func() {
		done <- runSelectedConfigs(ctx, configDirectory, "", &bytes.Buffer{}, func(ctx context.Context, configs []daemon.NamedConfig, _ *slog.Logger) error {
			started <- configs
			<-ctx.Done()
			return nil
		})
	}()

	select {
	case configs := <-started:
		if len(configs) != len(names) {
			t.Fatalf("run directory loaded %d configs, want %d", len(configs), len(names))
		}
		for index, name := range names {
			if want := filepath.Join(configDirectory, name); configs[index].Path != want {
				t.Errorf("config %d path = %q, want %q", index, configs[index].Path, want)
			}
		}
		cancel()
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("run directory did not launch the selected configs")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run directory shutdown error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run directory did not stop after cancellation")
	}
}

func TestManagedCommandUsage(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "run positional", args: []string{"run", "unexpected"}, want: "slipway: run does not accept positional arguments; use --config path\n"},
		{name: "start positional", args: []string{"start", "unexpected"}, want: "slipway: start does not accept positional arguments; use --config path\n"},
		{name: "ps positional", args: []string{"ps", "unexpected"}, want: "slipway: ps does not accept positional arguments\n"},
		{name: "stop selector", args: []string{"stop"}, want: "slipway: stop requires at least one instance ID or name\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, stdout, stderr := managedCLI(t, test.args...)
			if code != 2 || stdout != "" || stderr != test.want {
				t.Fatalf("Run(%v) = code %d, stdout %q, stderr %q; want code 2, empty stdout, stderr %q",
					test.args, code, stdout, stderr, test.want)
			}
		})
	}
}

func TestManagedCommandHelp(t *testing.T) {
	tests := []struct {
		command string
		usage   string
	}{
		{command: "run", usage: "Usage: slipway run [--config path] [--name name] [--socket path]"},
		{command: "start", usage: "Usage: slipway start [--config path] [--name name] [--socket path]"},
		{command: "ps", usage: "Usage: slipway ps [--all] [--socket path]"},
		{command: "stop", usage: "Usage: slipway stop [--socket path] <id-or-name> [id-or-name ...]"},
	}
	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			code, stdout, stderr := managedCLI(t, test.command, "--help")
			if code != 0 || stdout != "" || !strings.Contains(stderr, test.usage) {
				t.Fatalf("%s --help = code %d, stdout %q, stderr %q; want code 0 and usage %q",
					test.command, code, stdout, stderr, test.usage)
			}
		})
	}
}

func TestRunDaemonUsage(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := RunDaemon([]string{"--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("RunDaemon(--help) code = %d, stderr = %q", code, stderr.String())
	}
	if want := "slipwayd [--config path] [--socket path] [--web-listen address] [--log-level level]"; !strings.Contains(stdout.String(), want) {
		t.Fatalf("RunDaemon(--help) output = %q, want it to contain %q", stdout.String(), want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("RunDaemon(--help) stderr = %q, want empty", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := RunDaemon([]string{"unexpected"}, &stdout, &stderr); code != 2 {
		t.Fatalf("RunDaemon(positional) code = %d, stderr = %q", code, stderr.String())
	}
	if got, want := stderr.String(), "slipwayd: does not accept positional arguments\n"; got != want {
		t.Fatalf("RunDaemon(positional) stderr = %q, want %q", got, want)
	}

	stdout.Reset()
	stderr.Reset()
	if code := RunDaemon([]string{"--log-level", "mystery"}, &stdout, &stderr); code != 2 {
		t.Fatalf("RunDaemon(invalid log level) code = %d, stderr = %q", code, stderr.String())
	}
	if got, want := stderr.String(), "slipwayd: unknown log level \"mystery\"\n"; got != want {
		t.Fatalf("RunDaemon(invalid log level) stderr = %q, want %q", got, want)
	}
}

func TestWebTokenPathCannotReplaceControlSocket(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "web.token")
	tokenPath := webTokenPath(socketPath)
	if tokenPath == socketPath {
		t.Fatalf("web token path %q aliases the control socket", tokenPath)
	}
	if got, want := tokenPath, socketPath+".web-token"; got != want {
		t.Fatalf("webTokenPath(%q) = %q, want %q", socketPath, got, want)
	}
}

func TestRunDaemonVersion(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{{"version"}, {"--version"}, {"-version"}, {"-v"}} {
		var stdout, stderr bytes.Buffer
		if code := RunDaemonVersion(args, &stdout, &stderr, "1.2.3-test"); code != 0 {
			t.Fatalf("RunDaemonVersion(%v) code = %d, stderr = %q", args, code, stderr.String())
		}
		if got, want := stdout.String(), "slipwayd 1.2.3-test\n"; got != want {
			t.Errorf("RunDaemonVersion(%v) output = %q, want %q", args, got, want)
		}
	}

	var stdout, stderr bytes.Buffer
	if code := RunDaemonVersion([]string{"version", "extra"}, &stdout, &stderr, "1.2.3-test"); code != 2 {
		t.Fatalf("RunDaemonVersion(extra argument) code = %d, want 2", code)
	}
	if got, want := stderr.String(), "slipwayd: version does not accept arguments\n"; got != want {
		t.Fatalf("RunDaemonVersion(extra argument) stderr = %q, want %q", got, want)
	}
}

func managedCLI(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var stdoutBuffer, stderrBuffer bytes.Buffer
	code = Run(args, &stdoutBuffer, &stderrBuffer)
	return code, stdoutBuffer.String(), stderrBuffer.String()
}

type stubRunControlClient struct {
	socketPath string
	listErr    error
	run        func(context.Context, string, string, func(control.RunEvent) error) (control.Instance, error)
	stop       func(context.Context, string) (control.Instance, error)
}

func (client *stubRunControlClient) SocketPath() string {
	return client.socketPath
}

func (client *stubRunControlClient) List(context.Context, bool) ([]control.Instance, error) {
	return nil, client.listErr
}

func (client *stubRunControlClient) Run(
	ctx context.Context,
	path, name string,
	onEvent func(control.RunEvent) error,
) (control.Instance, error) {
	if client.run == nil {
		return control.Instance{}, errors.New("unexpected daemon Run call")
	}
	return client.run(ctx, path, name, onEvent)
}

func (client *stubRunControlClient) Stop(ctx context.Context, selector string) (control.Instance, error) {
	if client.stop == nil {
		return control.Instance{}, errors.New("unexpected daemon Stop call")
	}
	return client.stop(ctx, selector)
}

func writePlaceholderRunConfig(t *testing.T, name string) string {
	t.Helper()
	return writePlaceholderRunConfigAt(t, t.TempDir(), name)
}

func writePlaceholderRunConfigAt(t *testing.T, directory, name string) string {
	t.Helper()
	watchDirectory := filepath.Join(directory, "incoming")
	if err := os.MkdirAll(watchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, name)
	configuration := fmt.Sprintf(`
database: {path: %q}
watches:
  - name: incoming
    path: %q
    pipeline: [{name: inspect, program: /bin/true}]
`, filepath.Join(directory, name+".db"), watchDirectory)
	if err := os.WriteFile(path, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
