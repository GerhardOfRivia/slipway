package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/GerhardOfRivia/onderzeeer/internal/control"
	"github.com/GerhardOfRivia/onderzeeer/internal/queue"
)

type testDaemonProcess struct {
	command *exec.Cmd
	done    chan error
	logs    bytes.Buffer
	waited  bool
}

func launchTestDaemon(t *testing.T, root, socket string) *testDaemonProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	process := &testDaemonProcess{done: make(chan error, 1)}
	args := []string{"-test.run=^TestDaemonHelperProcess$", "--", "--socket", socket,
		"--state-dir", filepath.Join(root, "state"), "--web-listen", ""}
	process.command = exec.Command(executable, args...)
	process.command.Env = append(os.Environ(), "ONDERZEEER_DAEMON_HELPER=1")
	process.command.Stdout, process.command.Stderr = &process.logs, &process.logs
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { process.done <- process.command.Wait() }()
	t.Cleanup(func() {
		if !process.waited {
			_ = process.command.Process.Kill()
			<-process.done
			process.waited = true
		}
		if t.Failed() {
			t.Log(process.logs.String())
		}
	})
	client := control.NewClient(socket)
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err := client.List(ctx, true)
		cancel()
		if err == nil {
			return process
		}
		select {
		case err := <-process.done:
			process.waited = true
			t.Fatalf("daemon exited before serving: %v\n%s", err, process.logs.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("daemon did not become ready")
	return nil
}

func (process *testDaemonProcess) stop(t *testing.T, signal os.Signal) {
	t.Helper()
	if err := process.command.Process.Signal(signal); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-process.done:
		process.waited = true
		if signal != os.Kill && err != nil {
			t.Fatalf("daemon shutdown: %v\n%s", err, process.logs.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not stop")
	}
}

func waitSucceeded(t *testing.T, socket, instanceID string, count int64) {
	t.Helper()
	reader := control.NewQueueReader(socket, instanceID)
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		counts, err := reader.Counts(ctx)
		if err == nil && counts.Succeeded == count {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("jobs did not finish: counts=%+v err=%v", counts, err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestDaemonRecoversRegistrationsAndQueuesAfterProcessRestart(t *testing.T) {
	for _, signal := range []os.Signal{syscall.SIGTERM, os.Kill} {
		t.Run(signal.String(), func(t *testing.T) {
			root := t.TempDir()
			socket := filepath.Join(root, "control", "daemon.sock")
			var paths []string
			for _, name := range []string{"alpha", "beta"} {
				watch := filepath.Join(root, name)
				if err := os.Mkdir(watch, 0o700); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(root, name+".yaml")
				contents := fmt.Sprintf(`
queue: {workers: 1}
watches:
  - name: incoming
    path: %q
    process_existing: true
    settle_for: 10ms
    pipeline:
      - name: inspect
        executor: shell
        command: 'printf "captured output"'
`, watch)
				if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
					t.Fatal(err)
				}
				paths = append(paths, path)
			}
			process := launchTestDaemon(t, root, socket)
			client := control.NewClient(socket)
			defer client.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			instances, err := client.Start(ctx, paths, "")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "beta", "first.txt"), []byte("first"), 0o600); err != nil {
				t.Fatal(err)
			}
			waitSucceeded(t, socket, instances[1].ID, 1)
			if _, err := client.Stop(ctx, "alpha"); err != nil {
				t.Fatal(err)
			}
			process.stop(t, signal)
			client.CloseIdleConnections()
			// Leave an unfinished job in the saved queue, as a machine crash can.
			store, err := queue.Open(instances[1].DatabasePath)
			if err != nil {
				t.Fatal(err)
			}
			job, _, err := store.Enqueue(ctx, queue.EnqueueParams{WatchName: "incoming", Path: filepath.Join(root, "interrupted.txt"), Fingerprint: "crash"})
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := store.Claim(ctx)
			if err != nil || claimed.ID != job.ID {
				t.Fatalf("claim interrupted job: %+v %v", claimed, err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			for _, path := range paths {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			// The daemon restores saved instances without config arguments or YAML.
			restarted := launchTestDaemon(t, root, socket)
			waitSucceeded(t, socket, instances[1].ID, 2)
			views, err := client.List(ctx, true)
			if err != nil || len(views) != 2 {
				t.Fatalf("restored registrations: %+v, %v", views, err)
			}
			for _, initial := range instances {
				view, err := client.Get(ctx, initial.ID)
				if err != nil || view.Name != initial.Name || view.DatabasePath != initial.DatabasePath {
					t.Fatalf("identity changed: %+v, %v", view, err)
				}
				if view.Name == "alpha" && (view.Active() || view.DesiredState != "stopped") {
					t.Fatalf("explicit stop lost: %+v", view)
				}
				if view.Name == "beta" && (!view.Active() || view.DesiredState != "running") {
					t.Fatalf("desired run lost: %+v", view)
				}
			}
			for _, args := range [][]string{
				{"status", "beta"}, {"queue", "beta"}, {"jobs", "beta"}, {"job", "beta", "1"}, {"logs", "beta", "1"},
			} {
				code, stdout, stderr := managedCLI(t, append(args, "--socket", socket)...)
				if code != 0 || stderr != "" {
					t.Fatalf("inspection %v failed: %d %s", args, code, stderr)
				}
				if args[0] == "logs" && !strings.Contains(stdout, "captured output") {
					t.Fatalf("lost captured output: %s", stdout)
				}
			}
			code, _, stderr := managedCLI(t, "start", "alpha", "--socket", socket)
			if code != 0 {
				t.Fatalf("resume name without config: %s", stderr)
			}
			restarted.stop(t, syscall.SIGTERM)
		})
	}
}

func TestTestCommandDoesNotUseAvailableDaemon(t *testing.T) {
	root := t.TempDir()
	socket := filepath.Join(root, "control", "daemon.sock")
	process := launchTestDaemon(t, root, socket)
	t.Setenv("ONDERZEEER_SOCKET", socket)
	path := writePlaceholderRunConfigAt(t, root, "foreground.yaml")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// A regular file is an invalid watch root, making the local runner finish.
	contents = []byte(strings.ReplaceAll(string(contents), filepath.Join(root, "incoming"), path))
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := managedCLI(t, "test", path)
	if code != 1 || !strings.Contains(stderr, "is not a directory") {
		t.Fatalf("standalone execution: %d %s", code, stderr)
	}
	if _, err := os.Stat(filepath.Join(root, "foreground.yaml.db")); err != nil {
		t.Fatalf("standalone queue missing: %v", err)
	}
	client := control.NewClient(socket)
	defer client.CloseIdleConnections()
	instances, err := client.List(context.Background(), true)
	if err != nil || len(instances) != 0 {
		t.Fatalf("test registered with daemon: %+v %v", instances, err)
	}
	process.stop(t, syscall.SIGTERM)
}
