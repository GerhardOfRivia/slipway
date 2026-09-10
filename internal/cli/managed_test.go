package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/GerhardOfRivia/onderzeeer/internal/config"
	"github.com/GerhardOfRivia/onderzeeer/internal/control"
	"github.com/GerhardOfRivia/onderzeeer/internal/daemon"
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

	socketPath := filepath.Join(root, "control", "onderzeeer.sock")
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
		"start", configPath, "--socket", socketPath,
	)
	if code != 0 || stderr != "" {
		t.Fatalf("start code/stderr = %d, %q", code, stderr)
	}
	wantActiveFields := []string{
		"ID", "NAME", "STATUS", "STARTED", "CONFIG",
		"abc123def456", "worker", "running", startedAt.Format(time.RFC3339Nano), strconv.Quote(configPath),
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

	code, stdout, stderr = managedCLI(t, "stop", "--socket", socketPath, "worker")
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
		"ID", "NAME", "STATUS", "DESIRED", "STARTED", "FINISHED", "CONFIG", "ERROR",
		"abc123def456", "worker", "exited", "-", startedAt.Format(time.RFC3339Nano),
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
		{name: "start", args: []string{"start", configPath, "--socket", socketPath}},
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
			want := "onderzeeer daemon is unavailable at " + socketPath
			if !strings.Contains(stderr, want) {
				t.Errorf("Run(%v) stderr = %q, want it to contain %q", test.args, stderr, want)
			}
			if !strings.Contains(stderr, "start it with `onderzeeerd`") {
				t.Errorf("Run(%v) stderr = %q, want onderzeeerd startup guidance", test.args, stderr)
			}
		})
	}
}

func TestTestCommandRunsLocallyWithoutNameOrDaemon(t *testing.T) {
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
	t.Setenv("ONDERZEEER_SOCKET", missingSocket)
	code, stdout, stderr := managedCLI(t, "test", configPath)
	if code != 1 {
		t.Fatalf("run code = %d, want 1; stdout = %q, stderr = %q", code, stdout, stderr)
	}
	if strings.Contains(stderr, missingSocket) {
		t.Fatalf("standalone run contacted the daemon: %s", stderr)
	}
	if !strings.Contains(stderr, "is not a directory") || !strings.Contains(stderr, configPath) {
		t.Fatalf("run stderr = %q, want local runtime error for %s", stderr, configPath)
	}
	if _, err := os.Stat(databasePath); err != nil {
		t.Fatalf("daemonless run did not create its queue database: %v", err)
	}
}

func TestTestSelectionAcceptsSingleConfigDirectory(t *testing.T) {
	root := t.TempDir()
	configDirectory := filepath.Join(root, "configs")
	watchDirectory := filepath.Join(root, "incoming")
	if err := os.Mkdir(configDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(watchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	names := []string{"first.yaml"}
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
		done <- testSelectedConfig(ctx, configDirectory, io.Discard,
			func(ctx context.Context, configs []daemon.NamedConfig, _ *slog.Logger) error {
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
		{name: "test missing config", args: []string{"test"}, want: "onderzeeer: config path is required\n"},
		{name: "start positional", args: []string{"start", "one.yaml", "worker", "extra"}, want: "onderzeeer: start expects at most 2 positional arguments (config path, optional instance name)\n"},
		{name: "ps positional", args: []string{"ps", "unexpected"}, want: "onderzeeer: ps does not accept positional arguments\n"},
		{name: "stop selector", args: []string{"stop"}, want: "onderzeeer: stop requires at least one instance ID or name\n"},
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
		{command: "test", usage: "Usage: onderzeeer test <config>"},
		{command: "start", usage: "Usage: onderzeeer start <config-or-instance> [name] [--socket path]"},
		{command: "ps", usage: "Usage: onderzeeer ps [--all] [--socket path]"},
		{command: "stop", usage: "Usage: onderzeeer stop [--socket path] <id-or-name> [id-or-name ...]"},
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

func TestDiscoverStartConfigAllowsMissingOrBlankName(t *testing.T) {
	configPath := writePlaceholderRunConfig(t, "worker.yaml")
	for _, name := range []string{"", " \t"} {
		paths, gotName, err := discoverStartConfig(configPath, name)
		if err != nil {
			t.Fatalf("discoverStartConfig(%q, %q): %v", configPath, name, err)
		}
		if !reflect.DeepEqual(paths, []string{configPath}) || gotName != "" {
			t.Errorf("discoverStartConfig(%q, %q) = %q, %q; want [%q], empty name", configPath, name, paths, gotName, configPath)
		}
	}
}

func TestInstanceCommandsRejectMultipleConfigs(t *testing.T) {
	directory := t.TempDir()
	writePlaceholderRunConfigAt(t, directory, "first.yaml")
	writePlaceholderRunConfigAt(t, directory, "second.yaml")
	for _, command := range []string{"test", "start"} {
		args := []string{directory}
		if command != "test" {
			args = append(args, "worker")
		}
		var stdout, stderr bytes.Buffer
		code := Run(append([]string{command}, args...), &stdout, &stderr)
		if code != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "single configuration file") {
			t.Errorf("%s %v = %d, stdout %q, stderr %q; want single config usage error", command, args, code, stdout.String(), stderr.String())
		}
	}
}

func TestRunDaemonUsage(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := RunDaemon([]string{"--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("RunDaemon(--help) code = %d, stderr = %q", code, stderr.String())
	}
	if want := "onderzeeerd [--socket path] [--web-listen address] [--log-level level]"; !strings.Contains(stdout.String(), want) {
		t.Fatalf("RunDaemon(--help) output = %q, want it to contain %q", stdout.String(), want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("RunDaemon(--help) stderr = %q, want empty", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := RunDaemon([]string{writePlaceholderRunConfig(t, "worker.yaml")}, &stdout, &stderr); code != 2 {
		t.Fatalf("RunDaemon(positional) code = %d, stderr = %q", code, stderr.String())
	}
	if got, want := stderr.String(), "onderzeeerd: does not accept positional arguments; register instances with onderzeeer start <config> [name]\n"; got != want {
		t.Fatalf("RunDaemon(positional) stderr = %q, want %q", got, want)
	}

	stdout.Reset()
	stderr.Reset()
	if code := RunDaemon([]string{"--log-level", "mystery"}, &stdout, &stderr); code != 2 {
		t.Fatalf("RunDaemon(invalid log level) code = %d, stderr = %q", code, stderr.String())
	}
	if got, want := stderr.String(), "onderzeeerd: unknown log level \"mystery\"\n"; got != want {
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
		if got, want := stdout.String(), "onderzeeerd 1.2.3-test\n"; got != want {
			t.Errorf("RunDaemonVersion(%v) output = %q, want %q", args, got, want)
		}
	}

	var stdout, stderr bytes.Buffer
	if code := RunDaemonVersion([]string{"version", "extra"}, &stdout, &stderr, "1.2.3-test"); code != 2 {
		t.Fatalf("RunDaemonVersion(extra argument) code = %d, want 2", code)
	}
	if got, want := stderr.String(), "onderzeeerd: version does not accept arguments\n"; got != want {
		t.Fatalf("RunDaemonVersion(extra argument) stderr = %q, want %q", got, want)
	}
}

func managedCLI(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var stdoutBuffer, stderrBuffer bytes.Buffer
	code = Run(args, &stdoutBuffer, &stderrBuffer)
	return code, stdoutBuffer.String(), stderrBuffer.String()
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
