package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

func TestUnixHTTPStartListStop(t *testing.T) {
	t.Parallel()

	root := privateTransportTempDir(t)
	configPath := filepath.Join(root, "worker.yaml")
	runnerStarted := make(chan struct{})
	manager, client, _ := newUnixTransportHarness(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			configPath: testConfig(filepath.Join(root, "worker.db")),
		}),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			close(runnerStarted)
			<-ctx.Done()
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started, err := client.Start(ctx, []string{configPath}, "background-worker")
	if err != nil {
		t.Fatal(err)
	}
	if len(started) != 1 {
		t.Fatalf("Start returned %d instances, want 1", len(started))
	}
	if started[0].ID != "000000000001" || started[0].Name != "background-worker" || started[0].State != StateRunning {
		t.Fatalf("started instance = %+v", started[0])
	}
	if len(started[0].ConfigHash) != 64 {
		t.Fatalf("started config hash = %q, want a full SHA-256 hash", started[0].ConfigHash)
	}
	select {
	case <-runnerStarted:
	case <-ctx.Done():
		t.Fatal("runner did not start")
	}

	active, err := client.List(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].ID != started[0].ID || active[0].ConfigHash != started[0].ConfigHash || !active[0].Active() {
		t.Fatalf("active instances = %+v", active)
	}

	stopped, err := client.Stop(ctx, "background-worker")
	if err != nil {
		t.Fatal(err)
	}
	if stopped.ID != started[0].ID || stopped.ConfigHash != started[0].ConfigHash || stopped.State != StateExited || stopped.FinishedAt == nil {
		t.Fatalf("stopped instance = %+v", stopped)
	}
	active, err = client.List(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active instances after Stop = %+v", active)
	}
	retained, err := client.List(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(retained) != 1 || retained[0].State != StateExited {
		t.Fatalf("retained instances = %+v", retained)
	}
	if got := manager.List(true); len(got) != 1 || got[0].ID != stopped.ID {
		t.Fatalf("manager state after transport operations = %+v", got)
	}
}

func TestUnixHTTPRunStreamsStartedLogsAndExited(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, "foreground.yaml")
	_, client, _ := newUnixTransportHarness(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			configPath: testConfig(filepath.Join(root, "foreground.db")),
		}),
		Runner: func(_ context.Context, _ *config.Config, logger *slog.Logger) error {
			logger.Info("transport payload", "sequence", 7)
			return nil
		},
		IDGenerator: sequenceIDGenerator("000000000002"),
		LogCapacity: 16,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var events []RunEvent
	finished, err := client.Run(ctx, configPath, "foreground", func(event RunEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if finished.ID != "000000000002" || finished.Name != "foreground" || finished.State != StateExited {
		t.Fatalf("Run result = %+v", finished)
	}
	if len(events) < 3 {
		t.Fatalf("Run events = %+v, want started, log, and exited", events)
	}
	if events[0].Type != "started" || events[0].Instance.ID != finished.ID || events[0].Instance.ConfigHash != finished.ConfigHash {
		t.Fatalf("first Run event = %+v", events[0])
	}
	if last := events[len(events)-1]; last.Type != "exited" || last.Instance.State != StateExited || last.Instance.ID != finished.ID {
		t.Fatalf("last Run event = %+v", last)
	}
	foundPayload := false
	foundConfigHash := false
	for _, event := range events {
		if event.Type == "log" && strings.Contains(event.Log, "config_hash="+finished.ConfigHash) {
			foundConfigHash = true
		}
		if event.Type == "log" && strings.Contains(event.Log, "transport payload") && strings.Contains(event.Log, "sequence=7") {
			foundPayload = true
		}
	}
	if !foundPayload {
		t.Fatalf("Run events did not contain runner log: %+v", events)
	}
	if !foundConfigHash {
		t.Fatalf("Run events did not contain config hash %q: %+v", finished.ConfigHash, events)
	}
}

func TestUnixHTTPDroppedRunAttachmentDoesNotStopInstance(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, "detached.yaml")
	runnerStarted := make(chan struct{})
	runnerStopped := make(chan error, 1)
	_, client, _ := newUnixTransportHarness(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			configPath: testConfig(filepath.Join(root, "detached.db")),
		}),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			close(runnerStarted)
			<-ctx.Done()
			runnerStopped <- ctx.Err()
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator("000000000003"),
	})

	detached := errors.New("test client detached")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	instance, err := client.Run(ctx, configPath, "detached", func(event RunEvent) error {
		if event.Type == "started" {
			return detached
		}
		return nil
	})
	if !errors.Is(err, detached) {
		t.Fatalf("Run detach error = %v, want %v", err, detached)
	}
	if instance.ID != "000000000003" || instance.State != StateRunning {
		t.Fatalf("instance returned at detach = %+v", instance)
	}
	select {
	case <-runnerStarted:
	case <-ctx.Done():
		t.Fatal("detached runner did not start")
	}

	// Allow the server to observe the closed response body. The runtime has a
	// manager-owned context and must outlive this attachment.
	select {
	case stopErr := <-runnerStopped:
		t.Fatalf("dropping Run attachment stopped the runtime: %v", stopErr)
	case <-time.After(100 * time.Millisecond):
	}
	active, err := client.List(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].ID != instance.ID || active[0].State != StateRunning {
		t.Fatalf("active instances after detach = %+v", active)
	}
	if _, err := client.Stop(ctx, instance.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case stopErr := <-runnerStopped:
		if !errors.Is(stopErr, context.Canceled) {
			t.Fatalf("runner stop error = %v", stopErr)
		}
	case <-ctx.Done():
		t.Fatal("explicit Stop did not stop detached runtime")
	}
}

func TestUnixHTTPDaemonUnavailableIsTyped(t *testing.T) {
	t.Parallel()

	socketPath := filepath.Join(t.TempDir(), "missing.sock")
	client := NewClient(socketPath)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.List(ctx, false)
	if !errors.Is(err, ErrDaemonUnavailable) {
		t.Fatalf("List error = %v, want ErrDaemonUnavailable", err)
	}
	var unavailable *DaemonUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("List error type = %T, want *DaemonUnavailableError", err)
	}
	if unavailable.SocketPath != socketPath || unavailable.Err == nil {
		t.Fatalf("daemon unavailable error = %+v", unavailable)
	}
}

func TestNewServerReplacesStaleUnixSocket(t *testing.T) {
	t.Parallel()

	root := privateTransportTempDir(t)
	socketPath := filepath.Join(root, "slipway.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(socketPath); err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("stale socket setup: info=%v err=%v", info, err)
	}

	manager := newIdleTransportManager(t)
	server, err := NewServer(socketPath, manager, nil)
	if err != nil {
		t.Fatalf("NewServer did not replace stale socket: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if info, err := os.Lstat(socketPath); err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("replacement socket: info=%v err=%v", info, err)
	}
}

func TestNewServerRefusesToReplaceRegularFile(t *testing.T) {
	t.Parallel()

	socketPath := filepath.Join(privateTransportTempDir(t), "slipway.sock")
	contents := []byte("do not replace")
	if err := os.WriteFile(socketPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(socketPath, newIdleTransportManager(t), nil)
	if err == nil {
		_ = server.Close()
		t.Fatal("NewServer replaced a regular file")
	}
	if !strings.Contains(err.Error(), "refusing to replace non-socket") {
		t.Fatalf("NewServer error = %v", err)
	}
	got, readErr := os.ReadFile(socketPath)
	if readErr != nil || string(got) != string(contents) {
		t.Fatalf("regular file after NewServer: contents=%q err=%v", got, readErr)
	}
}

func TestNewServerRejectsDuplicateServer(t *testing.T) {
	t.Parallel()

	socketPath := filepath.Join(privateTransportTempDir(t), "slipway.sock")
	first, err := NewServer(socketPath, newIdleTransportManager(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	secondManager := newIdleTransportManager(t)
	duplicate, err := NewServer(socketPath, secondManager, nil)
	if err == nil {
		_ = duplicate.Close()
		t.Fatal("NewServer allowed a duplicate server")
	}
	if !errors.Is(err, ErrDaemonAlreadyRunning) {
		t.Fatalf("duplicate NewServer error = %v, want ErrDaemonAlreadyRunning", err)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := NewServer(socketPath, secondManager, nil)
	if err != nil {
		t.Fatalf("NewServer after first server closed = %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServerCloseRetainsOwnershipUntilInstancesStop(t *testing.T) {
	t.Parallel()

	root := privateTransportTempDir(t)
	socketPath := filepath.Join(root, "slipway.sock")
	configPath := filepath.Join(root, "worker.yaml")
	runnerStarted := make(chan struct{})
	runnerCanceled := make(chan struct{})
	releaseRunner := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseRunner)
		}
	}()
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			configPath: testConfig(filepath.Join(root, "worker.db")),
		}),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			close(runnerStarted)
			<-ctx.Done()
			close(runnerCanceled)
			<-releaseRunner
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
	})
	server, err := NewServer(socketPath, manager, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartMany([]string{configPath}, ""); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runnerStarted:
	case <-time.After(time.Second):
		t.Fatal("runner did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- server.Close() }()
	select {
	case <-runnerCanceled:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the runner")
	}

	replacementManager := newIdleTransportManager(t)
	duplicate, err := NewServer(socketPath, replacementManager, nil)
	if err == nil {
		_ = duplicate.Close()
		t.Fatal("replacement daemon acquired ownership while the old runner was stopping")
	}
	if !errors.Is(err, ErrDaemonAlreadyRunning) {
		t.Fatalf("replacement during shutdown error = %v, want ErrDaemonAlreadyRunning", err)
	}

	close(releaseRunner)
	released = true
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the runner stopped")
	}
	replacement, err := NewServer(socketPath, replacementManager, nil)
	if err != nil {
		t.Fatalf("replacement after shutdown: %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewServerProtectsAndCleansUpSocket(t *testing.T) {
	t.Parallel()

	socketDirectory := filepath.Join(t.TempDir(), "private-control")
	socketPath := filepath.Join(socketDirectory, "slipway.sock")
	server, err := NewServer(socketPath, newIdleTransportManager(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	directoryInfo, err := os.Stat(socketDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if got := directoryInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("socket directory mode = %04o, want 0700", got)
	}
	socketInfo, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if socketInfo.Mode()&os.ModeSocket == 0 || socketInfo.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %v, want Unix socket 0600", socketInfo.Mode())
	}
	lockInfo, err := os.Stat(socketPath + ".lock")
	if err != nil {
		t.Fatal(err)
	}
	if got := lockInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("socket lock mode = %04o, want 0600", got)
	}

	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket after Close: err=%v, want not exist", err)
	}
}

func TestServerServeCancellationShutsDownRuntimes(t *testing.T) {
	t.Parallel()

	root := privateTransportTempDir(t)
	configPath := filepath.Join(root, "shutdown.yaml")
	socketPath := filepath.Join(root, "slipway.sock")
	runnerStarted := make(chan struct{})
	runnerStopped := make(chan struct{})
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			configPath: testConfig(filepath.Join(root, "shutdown.db")),
		}),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			close(runnerStarted)
			<-ctx.Done()
			close(runnerStopped)
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator("000000000004"),
	})
	server, err := NewServer(socketPath, manager, nil)
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
		_ = server.Close()
	})
	client := NewClient(socketPath)
	defer client.CloseIdleConnections()

	requestContext, cancelRequest := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRequest()
	if _, err := client.Start(requestContext, []string{configPath}, "shutdown"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runnerStarted:
	case <-requestContext.Done():
		t.Fatal("runner did not start")
	}

	cancelServe()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve after cancellation = %v", err)
		}
	case <-requestContext.Done():
		t.Fatal("Serve did not return after cancellation")
	}
	select {
	case <-runnerStopped:
	default:
		t.Fatal("Serve returned before its runtime stopped")
	}
	instances := manager.List(true)
	if len(instances) != 1 || instances[0].State != StateExited || instances[0].FinishedAt == nil {
		t.Fatalf("instances after Serve cancellation = %+v", instances)
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket after Serve cancellation: err=%v, want not exist", err)
	}
}

func newUnixTransportHarness(t *testing.T, options Options) (*Manager, *Client, string) {
	t.Helper()
	manager := newTestManager(t, options)
	socketPath := filepath.Join(t.TempDir(), "control", "slipway.sock")
	server, err := NewServer(socketPath, manager, nil)
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(serveContext)
	}()
	client := NewClient(socketPath)
	t.Cleanup(func() {
		client.CloseIdleConnections()
		cancelServe()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("control server cleanup: %v", err)
			}
		case <-time.After(2 * time.Second):
			_ = server.Close()
			t.Error("control server did not stop during cleanup")
		}
	})
	return manager, client, socketPath
}

func newIdleTransportManager(t *testing.T) *Manager {
	t.Helper()
	return newTestManager(t, Options{
		Loader: func(path string) (*config.Config, error) {
			return nil, fmt.Errorf("unexpected load of %s", path)
		},
		Runner: func(context.Context, *config.Config, *slog.Logger) error {
			return errors.New("unexpected runner invocation")
		},
	})
}

func privateTransportTempDir(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}
