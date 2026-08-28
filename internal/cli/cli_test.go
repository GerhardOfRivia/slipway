package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/GerhardOfRivia/slipway/internal/queue"
)

func TestInspectionCommands(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	configPath := filepath.Join(directory, "slipway.yaml")
	databasePath := filepath.Join(directory, "slipway.db")
	watchPath := filepath.Join(directory, "incoming")
	if err := os.Mkdir(watchPath, 0o700); err != nil {
		t.Fatal(err)
	}
	configuration := fmt.Sprintf(`
queue: {workers: 1, max_retries: 0}
database: {path: %s}
watches:
  - name: incoming
    path: %s
    pipeline: [{name: inspect, program: /bin/true}]
`, strconv.Quote(databasePath), strconv.Quote(watchPath))
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := queue.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Enqueue(context.Background(), queue.EnqueueParams{
		WatchName:   "incoming",
		Path:        filepath.Join(watchPath, "report with spaces.csv"),
		Fingerprint: "v1",
		MaxRetries:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	commandID, err := store.StartCommand(context.Background(), queue.CommandStart{
		RunID: claimed.RunID, Sequence: 1, Name: "inspect", Program: "/bin/true",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteCommand(context.Background(), commandID, queue.CommandResult{
		Status: queue.CommandFailed, ExitCode: 7, Stdout: "some output\n", Stderr: "some error\n", Error: "exit 7",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Fail(context.Background(), claimed.ID, claimed.RunID, "exit 7", 0); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		args []string
		want []string
	}{
		{"status", []string{"status", "--config", configPath}, []string{"TOTAL", "1", "FAILED"}},
		{"jobs", []string{"jobs", "--config", configPath, "--status", "failed"}, []string{"FAILED", "incoming", job.Path}},
		{"job", []string{"job", "--config", configPath, fmt.Sprint(job.ID)}, []string{"Job 1", "Run 1", "Command 1", "exit 7"}},
		{"logs", []string{"logs", "--config", configPath, fmt.Sprint(job.ID)}, []string{"Run 1 / Command 1", "some output", "some error"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := Run(test.args, &stdout, &stderr); code != 0 {
				t.Fatalf("Run() code = %d, stderr = %s", code, stderr.String())
			}
			for _, want := range test.want {
				if !strings.Contains(stdout.String(), want) {
					t.Errorf("output %q does not contain %q", stdout.String(), want)
				}
			}
		})
	}
}

func TestOpenStoresRejectsHardLinkedDatabaseAliases(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "queue.db")
	aliasPath := filepath.Join(directory, "queue-alias.db")
	store, err := queue.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(databasePath, aliasPath); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	watchPath := filepath.Join(directory, "incoming")
	if err := os.Mkdir(watchPath, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"first.yaml": databasePath, "second.yaml": aliasPath} {
		contents := fmt.Sprintf("database: {path: %s}\nwatches: [{name: incoming, path: %s, pipeline: [{name: inspect, program: /bin/true}]}]\n", path, watchPath)
		if err := os.WriteFile(filepath.Join(directory, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	stores, err := openStores(directory)
	closeStores(stores)
	if err == nil || !strings.Contains(err.Error(), "use the same database") {
		t.Fatalf("openStores() error = %v, want duplicate database error", err)
	}
}

func TestRunUsageErrors(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"jobs", "--status", "mystery"}, &stdout, &stderr); code != 2 {
		t.Fatalf("invalid status exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "unknown job status") {
		t.Fatalf("stderr = %q", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"--help"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "slipway run") {
		t.Fatalf("help code/output = %d %q", code, stdout.String())
	}
	if strings.Contains(stdout.String(), "slipway daemon") {
		t.Fatalf("help still advertises removed daemon command: %q", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"daemon"}, &stdout, &stderr); code != 2 {
		t.Fatalf("removed daemon command exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), `slipway: unknown command "daemon"`) {
		t.Fatalf("removed daemon command stderr = %q", stderr.String())
	}
}

func TestVersionOption(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"version"}, {"--version"}, {"-version"}, {"-v"}} {
		var stdout, stderr bytes.Buffer
		if code := RunVersion(args, &stdout, &stderr, "1.2.3-test"); code != 0 {
			t.Fatalf("RunVersion(%v) code = %d, stderr = %q", args, code, stderr.String())
		}
		if got, want := stdout.String(), "slipway 1.2.3-test\n"; got != want {
			t.Errorf("RunVersion(%v) output = %q, want %q", args, got, want)
		}
	}
}

func TestInspectionAcrossConfigDirectory(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	configDirectory := filepath.Join(root, "configs")
	if err := os.Mkdir(configDirectory, 0o700); err != nil {
		t.Fatal(err)
	}

	var configPaths []string
	for _, name := range []string{"alpha.yaml", "beta.yml"} {
		base := strings.TrimSuffix(strings.TrimSuffix(name, ".yaml"), ".yml")
		watchPath := filepath.Join(root, base+"-incoming")
		if err := os.Mkdir(watchPath, 0o700); err != nil {
			t.Fatal(err)
		}
		databasePath := filepath.Join(root, base+".db")
		configPath := filepath.Join(configDirectory, name)
		configuration := fmt.Sprintf(`
queue: {workers: 1}
database: {path: %s}
watches:
  - name: %s
    path: %s
    pipeline: [{name: inspect, program: /bin/true}]
`, strconv.Quote(databasePath), base, strconv.Quote(watchPath))
		if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
			t.Fatal(err)
		}
		configPaths = append(configPaths, configPath)

		store, err := queue.Open(databasePath)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.Enqueue(context.Background(), queue.EnqueueParams{
			WatchName: base, Path: filepath.Join(watchPath, base+".csv"), Fingerprint: "v1",
		}); err != nil {
			store.Close()
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(configDirectory, "ignored.txt"), []byte("not yaml"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, invocation := range [][]string{
		{"status", "--config", configDirectory},
		{"jobs", "--config", configDirectory},
	} {
		var stdout, stderr bytes.Buffer
		if code := Run(invocation, &stdout, &stderr); code != 0 {
			t.Fatalf("Run(%v) code = %d, stderr = %s", invocation, code, stderr.String())
		}
		for _, configPath := range configPaths {
			if !strings.Contains(stdout.String(), configPath) {
				t.Errorf("Run(%v) output %q does not contain config %q", invocation, stdout.String(), configPath)
			}
		}
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"job", "--config", configDirectory, "1"}, &stdout, &stderr); code != 1 {
		t.Fatalf("ambiguous job exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "select its config with --config") {
		t.Fatalf("ambiguous job stderr = %q", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"job", "--config", configPaths[0], "1"}, &stdout, &stderr); code != 0 {
		t.Fatalf("explicit job exit code = %d, stderr = %q", code, stderr.String())
	}
}
