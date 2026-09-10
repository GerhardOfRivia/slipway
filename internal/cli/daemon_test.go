package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/GerhardOfRivia/onderzeeer/internal/control"
	"github.com/GerhardOfRivia/onderzeeer/internal/testutil"
)

func TestDaemonStartsWithoutInstances(t *testing.T) {
	root := t.TempDir()
	configPath := writePlaceholderRunConfigAt(t, root, "worker.yaml")
	t.Setenv("ONDERZEEER_CONFIG", configPath)
	socket := filepath.Join(testutil.SocketDir(t), "control", "daemon.sock")
	process := launchTestDaemon(t, root, socket)
	client := control.NewClient(socket)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	instances, err := client.List(ctx, true)
	if err != nil || len(instances) != 0 {
		t.Fatalf("new daemon unexpectedly registered configs: %+v, %v", instances, err)
	}
	process.stop(t, syscall.SIGTERM)
	if !strings.Contains(process.logs.String(), "restored_instances=0") {
		t.Fatalf("new daemon did not report empty registry: %s", process.logs.String())
	}
	if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("control socket remains after shutdown: %v", err)
	}
}

func TestDaemonRejectsConfigArguments(t *testing.T) {
	root := t.TempDir()
	configPath := writePlaceholderRunConfigAt(t, root, "worker.yaml")
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "config", args: []string{configPath}},
		{name: "named config", args: []string{configPath, "worker"}},
		{name: "directory", args: []string{root}},
		{name: "missing path", args: []string{filepath.Join(root, "missing.yaml")}},
		{name: "empty argument", args: []string{""}},
		{name: "blank argument", args: []string{" \t"}},
		{name: "after terminator", args: []string{"--", root}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimeRoot := t.TempDir()
			state := filepath.Join(runtimeRoot, "state")
			socket := filepath.Join(runtimeRoot, "control", "daemon.sock")
			args := append([]string{"--state-dir", state, "--socket", socket}, test.args...)
			var stdout, stderr bytes.Buffer
			code := RunDaemon(args, &stdout, &stderr)
			want := "onderzeeerd: does not accept positional arguments; register instances with onderzeeer start <config> [name]\n"
			if code != 2 || stdout.Len() != 0 || stderr.String() != want {
				t.Fatalf("RunDaemon(%v) = %d, stdout %q, stderr %q; want usage error %q", args, code, stdout.String(), stderr.String(), want)
			}
			for _, path := range []string{state, filepath.Dir(socket)} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("invalid invocation created %s: %v", path, err)
				}
			}
		})
	}
}

func TestDaemonHelperProcess(t *testing.T) {
	if os.Getenv("ONDERZEEER_DAEMON_HELPER") != "1" {
		return
	}
	for index, arg := range os.Args {
		if arg == "--" {
			os.Exit(RunDaemon(os.Args[index+1:], os.Stdout, os.Stderr))
		}
	}
	os.Exit(2)
}
