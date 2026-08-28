package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/GerhardOfRivia/slipway/internal/executor"
	"github.com/GerhardOfRivia/slipway/internal/queue"
)

func TestProcessJobRunsCommandsSequentiallyAndPersistsHistory(t *testing.T) {
	t.Parallel()

	store := &recordingStore{}
	runner := &recordingExecutor{}
	resolver := NewConfigResolver([]config.WatchConfig{{
		Name: "incoming",
		Pipeline: []config.CommandConfig{
			{
				Name:    "first",
				Program: "program-one",
				Args:    []string{"--input", "{{file}}"},
				Env:     map[string]string{"SLIPWAY_JOB": "{{job_id}}"},
			},
			{
				Name:       "second",
				Program:    "program-two",
				Args:       []string{"{{basename}}", "{{stem}}", "{{ext}}"},
				WorkingDir: "{{dir}}",
				Output:     "{{stem}}-{{job_id}}.out",
			},
		},
	}})
	pool, err := New(store, resolver, runner, Options{Workers: 1, RetryDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	job := &queue.Job{
		ID:        42,
		RunID:     7,
		WatchName: "incoming",
		Path:      "/drop box/a tricky;name.csv",
	}

	if err := pool.processJob(context.Background(), job); err != nil {
		t.Fatalf("processJob() error = %v", err)
	}

	if got, want := runner.names(), []string{"first", "second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("execution order = %#v, want %#v", got, want)
	}
	if len(store.started) != 2 || len(store.completed) != 2 {
		t.Fatalf("history starts/completions = %d/%d, want 2/2", len(store.started), len(store.completed))
	}
	if store.started[0].Sequence != 1 || store.started[1].Sequence != 2 {
		t.Fatalf("command sequences = %d, %d", store.started[0].Sequence, store.started[1].Sequence)
	}
	if got := store.started[0].Args; !reflect.DeepEqual(got, []string{"--input", job.Path}) {
		t.Fatalf("first command args = %#v", got)
	}
	if got := store.started[0].Env; !reflect.DeepEqual(got, []string{"SLIPWAY_JOB=42"}) {
		t.Fatalf("first command env = %#v", got)
	}
	if got := store.started[1]; got.WorkingDir != "/drop box" || !reflect.DeepEqual(got.Args, []string{"a tricky;name.csv", "a tricky;name", ".csv"}) {
		t.Fatalf("second command was not expanded correctly: %#v", got)
	}
	if got, want := runner.commands[1].Output, "a tricky;name-42.out"; got != want {
		t.Fatalf("second command output = %q, want %q", got, want)
	}
	if store.succeededJob != job.ID || store.succeededRun != job.RunID {
		t.Fatalf("success = job %d run %d", store.succeededJob, store.succeededRun)
	}
	if store.failedJob != 0 {
		t.Fatalf("unexpected failure for job %d", store.failedJob)
	}
}

func TestProcessJobUsesConfiguredContainerExecutors(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	filename := filepath.Join(directory, "slipway.yaml")
	configuration := `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: dockerized
        executor: docker
        image: example/{{stem}}:latest
        container_args: ["--rm", "--name={{job_id}}"]
        mounts:
          - source: "{{dir}}"
            target: /{{stem}}
            read_only: true
        container_env:
          INPUT: "{{basename}}"
        env:
          HOST_MODE: docker
        command: /app/{{stem}}
        command_args: ["{{job_id}}"]
      - name: podmanized
        executor: podman
        program: ./bin/podman-wrapper
        image: example/podman:latest
        command_args: ["{{job_id}}"]
      - name: contained
        executor: apptainer
        image: example.sif
        mounts:
          - source: "{{dir}}"
            target: /data
            read_only: true
        container_env:
          SOURCE: '"{{basename}}"'
        env:
          HOST_MODE: apptainer
        command: inspect
        command_args: ["{{file}}"]
`
	if err := os.WriteFile(filename, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(filename)
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}

	store := &recordingStore{}
	runner := &recordingExecutor{}
	pool, err := New(store, NewConfigResolver(cfg.Watches), runner, Options{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	job := &queue.Job{
		ID:        42,
		RunID:     7,
		WatchName: "incoming",
		Path:      `/inputs,"odd/report.csv`,
	}
	if err := pool.processJob(context.Background(), job); err != nil {
		t.Fatalf("processJob() error = %v", err)
	}

	wantPrograms := []string{"docker", filepath.Join(directory, "bin", "podman-wrapper"), "apptainer"}
	wantArgs := [][]string{
		{"run", "--rm", "--name=42", "--mount", `type=bind,"source=/inputs,""odd",target=/report,ro`, "--env", "INPUT=report.csv", "example/report:latest", "/app/report", "42"},
		{"run", "example/podman:latest", "42"},
		{"exec", "--no-eval", "--mount", `type=bind,"source=/inputs,""odd",target=/data,ro`, "--env", `"SOURCE=""report.csv""","SOURCE=""report.csv"""`, filepath.Join(directory, "example.sif"), "inspect", job.Path},
	}
	if len(runner.commands) != len(wantPrograms) || len(store.started) != len(wantPrograms) {
		t.Fatalf("executed/history commands = %d/%d, want %d/%d", len(runner.commands), len(store.started), len(wantPrograms), len(wantPrograms))
	}
	for index := range wantPrograms {
		if runner.commands[index].Program != wantPrograms[index] || !reflect.DeepEqual(runner.commands[index].Args, wantArgs[index]) {
			t.Errorf("executed command %d = %#v, want program %q args %#v", index, runner.commands[index], wantPrograms[index], wantArgs[index])
		}
		if store.started[index].Program != wantPrograms[index] || !reflect.DeepEqual(store.started[index].Args, wantArgs[index]) {
			t.Errorf("history command %d = %#v, want program %q args %#v", index, store.started[index], wantPrograms[index], wantArgs[index])
		}
	}
	if got := runner.commands[0].Env; !reflect.DeepEqual(got, map[string]string{"HOST_MODE": "docker"}) {
		t.Errorf("docker host environment = %#v, want only HOST_MODE", got)
	}
	if got := runner.commands[2].Env; !reflect.DeepEqual(got, map[string]string{"HOST_MODE": "apptainer"}) {
		t.Errorf("Apptainer host environment = %#v, want only HOST_MODE", got)
	}
	if got := store.started[0].Env; !reflect.DeepEqual(got, []string{"HOST_MODE=docker"}) {
		t.Errorf("docker history environment = %#v, want host environment only", got)
	}
	if got := store.started[2].Env; !reflect.DeepEqual(got, []string{"HOST_MODE=apptainer"}) {
		t.Errorf("Apptainer history environment = %#v, want host environment only", got)
	}
}

func TestProcessJobRejectsInvalidExpandedStructuredValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		commandYAML string
		jobPath     string
		wantError   string
	}{
		{
			name:        "empty image",
			commandYAML: `        image: "{{ext}}"`,
			jobPath:     "/input/README",
			wantError:   "image is required",
		},
		{
			name: "empty command",
			commandYAML: `        image: example/image
        command: "{{ext}}"`,
			jobPath:   "/input/README",
			wantError: "command must not be blank",
		},
		{
			name: "empty mount source",
			commandYAML: `        image: example/image
        mounts: [{source: "{{file}}", target: /data}]`,
			jobPath:   "",
			wantError: "mounts[0].source is required",
		},
		{
			name: "relative expanded mount target",
			commandYAML: `        image: example/image
        mounts: [{source: /host, target: "{{file}}"}]`,
			jobPath:   "relative.csv",
			wantError: "mounts[0].target must be an absolute container path",
		},
		{
			name: "expanded container option terminator",
			commandYAML: `        image: example/image
        container_args: ["{{stem}}"]`,
			jobPath:   "/input/--.csv",
			wantError: "container_args[0] must not be --",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "slipway.yaml")
			configuration := `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: container
        executor: docker
`
			configuration += test.commandYAML + "\n"
			if err := os.WriteFile(filename, []byte(configuration), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(filename)
			if err != nil {
				t.Fatalf("config.Load() error = %v", err)
			}

			store := &recordingStore{}
			runner := &recordingExecutor{}
			pool, err := New(store, NewConfigResolver(cfg.Watches), runner, Options{Workers: 1})
			if err != nil {
				t.Fatal(err)
			}
			job := &queue.Job{ID: 51, RunID: 52, WatchName: "incoming", Path: test.jobPath}

			err = pool.processJob(context.Background(), job)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("processJob() error = %v, want error containing %q", err, test.wantError)
			}
			if len(runner.commands) != 0 || len(store.started) != 0 {
				t.Fatalf("invalid expanded command executed or entered history: commands=%d starts=%d", len(runner.commands), len(store.started))
			}
			if store.failedJob != job.ID || store.failedRun != job.RunID {
				t.Fatalf("failure = job %d run %d, want job %d run %d", store.failedJob, store.failedRun, job.ID, job.RunID)
			}
		})
	}
}

func TestConfigResolverClonesStructuredContainerConfig(t *testing.T) {
	configured := []config.WatchConfig{{
		Name: "incoming",
		Pipeline: []config.CommandConfig{{
			Name:          "container",
			Executor:      config.ExecutorDocker,
			Image:         "example/image",
			Mounts:        []config.MountConfig{{Source: "/source", Target: "/target"}},
			ContainerEnv:  map[string]string{"MODE": "batch"},
			ContainerArgs: []string{"--rm"},
			CommandArgs:   []string{"input.csv"},
			Env:           map[string]string{"HOST_MODE": "local"},
		}},
	}}
	resolver := NewConfigResolver(configured)

	configured[0].Pipeline[0].Mounts[0].Source = "/mutated-input"
	configured[0].Pipeline[0].ContainerEnv["MODE"] = "mutated-input"
	configured[0].Pipeline[0].ContainerArgs[0] = "--mutated-input"
	configured[0].Pipeline[0].CommandArgs[0] = "mutated-input.csv"
	configured[0].Pipeline[0].Env["HOST_MODE"] = "mutated-input"

	first, err := resolver.Resolve("incoming")
	if err != nil {
		t.Fatal(err)
	}
	if first[0].Mounts[0].Source != "/source" || first[0].ContainerEnv["MODE"] != "batch" ||
		first[0].ContainerArgs[0] != "--rm" || first[0].CommandArgs[0] != "input.csv" ||
		first[0].Env["HOST_MODE"] != "local" {
		t.Fatalf("resolver retained mutable input aliases: %#v", first[0])
	}

	first[0].Mounts[0].Source = "/mutated-result"
	first[0].ContainerEnv["MODE"] = "mutated-result"
	first[0].ContainerArgs[0] = "--mutated-result"
	first[0].CommandArgs[0] = "mutated-result.csv"
	first[0].Env["HOST_MODE"] = "mutated-result"
	second, err := resolver.Resolve("incoming")
	if err != nil {
		t.Fatal(err)
	}
	if second[0].Mounts[0].Source != "/source" || second[0].ContainerEnv["MODE"] != "batch" ||
		second[0].ContainerArgs[0] != "--rm" || second[0].CommandArgs[0] != "input.csv" ||
		second[0].Env["HOST_MODE"] != "local" {
		t.Fatalf("Resolve() returned mutable aliases: %#v", second[0])
	}
}

func TestProcessJobStopsPipelineAndPersistsRetry(t *testing.T) {
	t.Parallel()

	store := &recordingStore{failStatus: queue.StatusQueued}
	runner := &recordingExecutor{failName: "first"}
	resolver := NewConfigResolver([]config.WatchConfig{{
		Name: "incoming",
		Pipeline: []config.CommandConfig{
			{Name: "first", Program: "program-one"},
			{Name: "must-not-run", Program: "program-two"},
		},
	}})
	retryDelay := 13 * time.Second
	pool, err := New(store, resolver, runner, Options{Workers: 1, RetryDelay: retryDelay})
	if err != nil {
		t.Fatal(err)
	}
	job := &queue.Job{ID: 9, RunID: 11, WatchName: "incoming", Path: "input.csv"}

	err = pool.processJob(context.Background(), job)
	if err == nil || !strings.Contains(err.Error(), "first") {
		t.Fatalf("processJob() error = %v", err)
	}
	if len(store.completed) != 1 || store.completed[0].Status != queue.CommandFailed {
		t.Fatalf("command completion = %#v", store.completed)
	}
	for name, errorText := range map[string]string{
		"returned error": err.Error(),
		"command error":  store.completed[0].Error,
		"job error":      store.failure,
	} {
		if !strings.Contains(errorText, `stderr: "broken"`) {
			t.Errorf("%s = %q, want stderr detail", name, errorText)
		}
		if strings.Contains(errorText, "partial") {
			t.Errorf("%s = %q, should prefer stderr over stdout", name, errorText)
		}
	}
	if got, want := runner.names(), []string{"first"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("execution order = %#v, want %#v", got, want)
	}
	if store.succeededJob != 0 {
		t.Fatalf("failed job marked succeeded: %d", store.succeededJob)
	}
	if store.failedJob != job.ID || store.failedRun != job.RunID || store.retryDelay != retryDelay {
		t.Fatalf("failure = job %d run %d retry %s", store.failedJob, store.failedRun, store.retryDelay)
	}
}

func TestProcessJobAddsOutputToSyntheticExitError(t *testing.T) {
	t.Parallel()

	store := &recordingStore{}
	runner := &recordingExecutor{nonzeroName: "inspect"}
	resolver := NewConfigResolver([]config.WatchConfig{{
		Name:     "incoming",
		Pipeline: []config.CommandConfig{{Name: "inspect", Program: "program-one"}},
	}})
	pool, err := New(store, resolver, runner, Options{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	job := &queue.Job{ID: 17, RunID: 19, WatchName: "incoming", Path: "input.csv"}

	err = pool.processJob(context.Background(), job)
	if err == nil {
		t.Fatal("processJob() succeeded for a nonzero exit")
	}
	if len(store.completed) != 1 || store.completed[0].Status != queue.CommandFailed {
		t.Fatalf("command completion = %#v", store.completed)
	}
	for name, errorText := range map[string]string{
		"returned error": err.Error(),
		"command error":  store.completed[0].Error,
		"job error":      store.failure,
	} {
		if !strings.Contains(errorText, "command exited with code 23") ||
			!strings.Contains(errorText, `stderr: "broken"`) {
			t.Errorf("%s = %q, want exit code and stderr detail", name, errorText)
		}
	}
}

func TestFailureOutputSummary(t *testing.T) {
	t.Parallel()

	const truncationSuffix = "... [truncated]"
	longDetail := strings.Repeat("界", maxFailureDetailRunes+1)
	tests := []struct {
		name   string
		result executor.Result
		want   string
	}{
		{
			name:   "last stderr line",
			result: executor.Result{Stderr: "\n first clue \r\n root cause \n", Stdout: "less useful"},
			want:   `stderr: "root cause"`,
		},
		{
			name:   "stdout fallback",
			result: executor.Result{Stderr: " \r\n\t", Stdout: "\n progress \n failure clue \n"},
			want:   `stdout: "failure clue"`,
		},
		{
			name:   "empty output",
			result: executor.Result{Stderr: " \r\n\t", Stdout: "\n"},
		},
		{
			name: "capture truncation marker",
			result: executor.Result{
				Stderr: "diagnosis\n\n[slipway: output truncated after 1048576 bytes]\n",
			},
			want: `stderr: "diagnosis"`,
		},
		{
			name:   "bounded unicode",
			result: executor.Result{Stderr: longDetail},
			want:   `stderr: "` + strings.Repeat("界", maxFailureDetailRunes-len(truncationSuffix)) + truncationSuffix + `"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := failureOutputSummary(test.result); got != test.want {
				t.Fatalf("failureOutputSummary() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestWithFailureOutputPreservesCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("sentinel")
	err := withFailureOutput(cause, executor.Result{Stderr: "diagnosis"})
	if !errors.Is(err, cause) {
		t.Fatalf("withFailureOutput() error = %v, want wrapped cause", err)
	}
}

func TestPoolStopsWhenContextIsCanceled(t *testing.T) {
	t.Parallel()

	store := &recordingStore{}
	pool, err := New(store, NewConfigResolver(nil), &recordingExecutor{}, Options{
		Workers:      2,
		PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()
	time.Sleep(15 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker pool did not stop after cancellation")
	}
}

type recordingStore struct {
	mu sync.Mutex

	started   []queue.CommandStart
	completed []queue.CommandResult

	succeededJob int64
	succeededRun int64
	failedJob    int64
	failedRun    int64
	failure      string
	retryDelay   time.Duration
	failStatus   queue.Status
}

func (store *recordingStore) Claim(context.Context) (*queue.Job, error) {
	return nil, queue.ErrNoJob
}

func (store *recordingStore) StartCommand(_ context.Context, start queue.CommandStart) (int64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.started = append(store.started, start)
	return int64(len(store.started)), nil
}

func (store *recordingStore) CompleteCommand(_ context.Context, _ int64, result queue.CommandResult) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.completed = append(store.completed, result)
	return nil
}

func (store *recordingStore) Succeed(_ context.Context, jobID, runID int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.succeededJob = jobID
	store.succeededRun = runID
	return nil
}

func (store *recordingStore) Fail(_ context.Context, jobID, runID int64, reason string, retryDelay time.Duration) (queue.Status, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.failedJob = jobID
	store.failedRun = runID
	store.failure = reason
	store.retryDelay = retryDelay
	if store.failStatus == "" {
		return queue.StatusFailed, nil
	}
	return store.failStatus, nil
}

type recordingExecutor struct {
	mu          sync.Mutex
	commands    []executor.Command
	failName    string
	nonzeroName string
}

func (runner *recordingExecutor) Execute(_ context.Context, command executor.Command) (executor.Result, error) {
	runner.mu.Lock()
	runner.commands = append(runner.commands, command)
	runner.mu.Unlock()
	if command.Name == runner.failName {
		return executor.Result{ExitCode: 23, Stdout: "partial", Stderr: "broken"}, errors.New("intentional failure")
	}
	if command.Name == runner.nonzeroName {
		return executor.Result{ExitCode: 23, Stdout: "partial", Stderr: "broken"}, nil
	}
	return executor.Result{ExitCode: 0, Stdout: command.Name}, nil
}

func (runner *recordingExecutor) names() []string {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	names := make([]string, len(runner.commands))
	for i := range runner.commands {
		names[i] = runner.commands[i].Name
	}
	return names
}
