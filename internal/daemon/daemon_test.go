package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GerhardOfRivia/onderzeeer/internal/config"
	"github.com/GerhardOfRivia/onderzeeer/internal/queue"
)

func TestRunWarnsAboutDockerTerminalFlags(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	incoming := filepath.Join(root, "incoming")
	if err := os.Mkdir(incoming, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		Queue:    config.QueueConfig{Workers: 1},
		Database: config.DatabaseConfig{Path: filepath.Join(root, "queue.db")},
		Watches: []config.WatchConfig{{
			Name: "incoming", Path: incoming,
			Pipeline: []config.CommandConfig{{
				Name: "process", Executor: config.ExecutorDocker,
				Image: "image", ContainerArgs: []string{"-it"},
			}},
		}},
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("TTY warning must not reject the config: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := time.AfterFunc(50*time.Millisecond, cancel)
	defer stop.Stop()
	var logs bytes.Buffer
	err := Run(ctx, cfg, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	for _, want := range []string{"level=WARN", "no interactive stdin or TTY", "watch=incoming", "step=1", "command=process"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("startup log %q does not contain %q", logs.String(), want)
		}
	}
	if strings.Count(logs.String(), "level=WARN") != 1 {
		t.Errorf("expected one startup warning: %s", logs.String())
	}
}

func TestRunProcessesExistingFileAndStops(t *testing.T) {
	directory := t.TempDir()
	incoming := filepath.Join(directory, "incoming")
	if err := os.Mkdir(incoming, 0o700); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(incoming, "a file;with metacharacters.csv")
	if err := os.WriteFile(input, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(directory, "executed")
	database := filepath.Join(directory, "onderzeeer.db")
	cfg := &config.Config{
		Queue:    config.QueueConfig{Workers: 1, RetryDelay: config.Duration{Duration: time.Millisecond}},
		Database: config.DatabaseConfig{Path: database},
		Watches: []config.WatchConfig{{
			Name:            "incoming",
			Path:            incoming,
			Recursive:       true,
			ProcessExisting: true,
			Include:         []string{"*.csv"},
			SettleFor:       config.Duration{Duration: 10 * time.Millisecond},
			Pipeline: []config.CommandConfig{{
				Name:    "record",
				Program: os.Args[0],
				Args:    []string{"-test.run=^TestDaemonHelperProcess$", "--", "{{file}}", "{{job_id}}"},
				Env:     map[string]string{"ONDERZEEER_DAEMON_TEST_MARKER": marker},
			}},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() { done <- Run(ctx, cfg, logger) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		contents, err := os.ReadFile(marker)
		if err == nil {
			fields := strings.Split(strings.TrimSpace(string(contents)), "\n")
			if len(fields) != 2 || fields[0] != input || fields[1] != "1" {
				t.Fatalf("helper arguments = %#v", fields)
			}
			break
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon did not process existing file")
		}
		select {
		case err := <-done:
			t.Fatalf("daemon exited early: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	store, err := queue.Open(database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var jobs []queue.Job
	for {
		jobs, err = store.ListJobs(context.Background(), queue.JobFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) == 1 && jobs[0].Status == queue.StatusSucceeded {
			break
		}
		if len(jobs) == 1 && jobs[0].Status == queue.StatusFailed {
			t.Fatalf("job failed before shutdown: %+v", jobs[0])
		}
		if time.Now().After(deadline) {
			t.Fatalf("job was not persisted as succeeded: %+v", jobs)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop after cancellation")
	}

	if len(jobs) != 1 || jobs[0].Status != queue.StatusSucceeded {
		t.Fatalf("persisted jobs = %+v", jobs)
	}
	runs, err := store.ListRuns(context.Background(), jobs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Status != queue.StatusSucceeded {
		t.Fatalf("persisted runs = %+v", runs)
	}
	commands, err := store.ListCommands(context.Background(), runs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Status != queue.CommandSucceeded {
		t.Fatalf("persisted commands = %+v", commands)
	}
}

func TestDaemonHelperProcess(t *testing.T) {
	marker := os.Getenv("ONDERZEEER_DAEMON_TEST_MARKER")
	if marker == "" {
		return
	}
	separator := -1
	for i, argument := range os.Args {
		if argument == "--" {
			separator = i
			break
		}
	}
	if separator < 0 || len(os.Args) != separator+3 {
		fmt.Fprintln(os.Stderr, "unexpected helper arguments")
		os.Exit(2)
	}
	if err := os.WriteFile(marker, []byte(strings.Join(os.Args[separator+1:], "\n")+"\n"), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}
