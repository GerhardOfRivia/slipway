package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOpenReportsMissingDatabaseDirectory(t *testing.T) {
	t.Parallel()
	databaseDirectory := filepath.Join(t.TempDir(), "missing")
	databasePath := filepath.Join(databaseDirectory, "queue.db")

	store, err := Open(databasePath)
	if err == nil {
		_ = store.Close()
		t.Fatal("Open succeeded with a missing database directory")
	}
	if store != nil {
		_ = store.Close()
		t.Fatalf("Open returned a store after failing: %v", err)
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open error = %v, want os.ErrNotExist", err)
	}
	if !strings.Contains(err.Error(), "database directory") || !strings.Contains(err.Error(), databaseDirectory) {
		t.Errorf("Open error = %q, want missing database directory %q", err, databaseDirectory)
	}
	if strings.Contains(strings.ToLower(err.Error()), "out of memory") {
		t.Errorf("Open reported SQLite's misleading allocation error: %v", err)
	}
	if _, statErr := os.Stat(databasePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("database stat error = %v, want os.ErrNotExist", statErr)
	}
}

func TestDatabaseOpenErrorReplacesMisleadingSQLiteCantOpenMessage(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "queue.db")
	cause := &codedSQLiteError{
		code:    sqliteCantOpen,
		message: "unable to open database file: out of memory (14)",
	}

	err := databaseOpenError(databasePath, cause)
	if !errors.Is(err, cause) {
		t.Errorf("databaseOpenError = %v, want wrapped SQLite cause", err)
	}
	if !strings.Contains(err.Error(), databasePath) || !strings.Contains(err.Error(), "path and permissions") {
		t.Errorf("databaseOpenError = %q, want path and permissions guidance", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "out of memory") {
		t.Errorf("databaseOpenError retained SQLite's misleading allocation error: %v", err)
	}
}

type codedSQLiteError struct {
	code    int
	message string
}

func (err *codedSQLiteError) Error() string { return err.message }
func (err *codedSQLiteError) Code() int     { return err.code }

func TestOpenReadOnlyReadsInitializedQueue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	initializedPath := filepath.Join(directory, "initialized.db")
	databasePath := filepath.Join(directory, "queue #1? [report] %.db")

	writable, err := Open(initializedPath)
	if err != nil {
		t.Fatalf("open writable store: %v", err)
	}
	want, created, err := writable.Enqueue(ctx, EnqueueParams{
		WatchName:   "incoming",
		Path:        "/drop/a file;$(literal).csv",
		Fingerprint: "v1",
		MaxRetries:  2,
	})
	if err != nil || !created {
		t.Fatalf("enqueue: created=%v err=%v", created, err)
	}
	if err := writable.Close(); err != nil {
		t.Fatalf("close writable store: %v", err)
	}
	if err := os.Rename(initializedPath, databasePath); err != nil {
		t.Fatalf("rename initialized database: %v", err)
	}

	readOnly, err := OpenReadOnly(databasePath)
	if err != nil {
		t.Fatalf("open read-only store: %v", err)
	}
	t.Cleanup(func() { _ = readOnly.Close() })
	got, err := readOnly.GetJob(ctx, want.ID)
	if err != nil {
		t.Fatalf("read job: %v", err)
	}
	if got.Path != want.Path || got.Fingerprint != want.Fingerprint || got.Status != StatusQueued {
		t.Fatalf("read job = %+v, want %+v", got, want)
	}
	counts, err := readOnly.Counts(ctx)
	if err != nil {
		t.Fatalf("read counts: %v", err)
	}
	if counts.Total != 1 || counts.Queued != 1 {
		t.Fatalf("read-only counts = %+v", counts)
	}

	if _, _, err := readOnly.Enqueue(ctx, EnqueueParams{
		WatchName: "incoming", Path: "/drop/no-write.csv", Fingerprint: "v2",
	}); err == nil {
		t.Fatal("enqueue through read-only store unexpectedly succeeded")
	}
}

func TestOpenReadOnlyMissingDatabaseDoesNotCreateIt(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "missing #queue?.db")

	store, err := OpenReadOnly(databasePath)
	if err == nil {
		_ = store.Close()
		t.Fatal("OpenReadOnly succeeded for a missing database")
	}
	if _, statErr := os.Stat(databasePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("missing database stat error = %v, want os.ErrNotExist", statErr)
	}
}

func TestOpenReadOnlyWhileDaemonStoreIsOpen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "active.db")
	writable, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer writable.Close()
	if _, _, err := writable.Enqueue(ctx, EnqueueParams{
		WatchName: "incoming", Path: "/drop/active.csv", Fingerprint: "v1",
	}); err != nil {
		t.Fatal(err)
	}

	readOnly, err := OpenReadOnly(databasePath)
	if err != nil {
		t.Fatalf("OpenReadOnly() with active writer: %v", err)
	}
	defer readOnly.Close()
	counts, err := readOnly.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Total != 1 || counts.Queued != 1 {
		t.Fatalf("read-only counts with active writer = %+v", counts)
	}
}

func TestEnqueueDeduplicatesByWatchPathAndFingerprint(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := context.Background()

	first, created, err := store.Enqueue(ctx, EnqueueParams{
		WatchName:   "incoming",
		Path:        "/drop/a file.csv",
		Fingerprint: "size:12:mtime:34",
		MaxRetries:  3,
	})
	if err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if !created {
		t.Fatal("first enqueue was reported as duplicate")
	}

	duplicate, created, err := store.Enqueue(ctx, EnqueueParams{
		WatchName:   "incoming",
		Path:        "/drop/a file.csv",
		Fingerprint: "size:12:mtime:34",
		MaxRetries:  99,
	})
	if err != nil {
		t.Fatalf("enqueue duplicate: %v", err)
	}
	if created {
		t.Fatal("duplicate enqueue created a job")
	}
	if duplicate.ID != first.ID {
		t.Fatalf("duplicate ID = %d, want %d", duplicate.ID, first.ID)
	}
	if duplicate.MaxRetries != 3 {
		t.Fatalf("duplicate changed max retries to %d", duplicate.MaxRetries)
	}

	changed, created, err := store.Enqueue(ctx, EnqueueParams{
		WatchName:   "incoming",
		Path:        "/drop/a file.csv",
		Fingerprint: "size:13:mtime:35",
		MaxRetries:  3,
	})
	if err != nil || !created {
		t.Fatalf("enqueue changed file: created=%v err=%v", created, err)
	}
	if changed.ID == first.ID {
		t.Fatal("changed fingerprint was deduplicated")
	}

	otherWatch, created, err := store.Enqueue(ctx, EnqueueParams{
		WatchName:   "archive",
		Path:        "/drop/a file.csv",
		Fingerprint: "size:12:mtime:34",
		MaxRetries:  3,
	})
	if err != nil || !created {
		t.Fatalf("enqueue other watch: created=%v err=%v", created, err)
	}
	if otherWatch.ID == first.ID {
		t.Fatal("different watch was deduplicated")
	}

	count, err := store.Count(ctx, "")
	if err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if count != 3 {
		t.Fatalf("job count = %d, want 3", count)
	}
}

func TestClaimIsAtomicAcrossConnections(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "queue.db")
	const (
		jobCount  = 80
		claimers  = 8
		watchName = "incoming"
	)

	stores := make([]*Store, 0, claimers)
	for i := 0; i < claimers; i++ {
		store, err := Open(databasePath)
		if err != nil {
			t.Fatalf("open store %d: %v", i, err)
		}
		stores = append(stores, store)
	}
	t.Cleanup(func() {
		for _, store := range stores {
			_ = store.Close()
		}
	})

	for i := 0; i < jobCount; i++ {
		_, created, err := stores[0].Enqueue(ctx, EnqueueParams{
			WatchName:   watchName,
			Path:        fmt.Sprintf("/drop/%03d.csv", i),
			Fingerprint: fmt.Sprintf("fingerprint-%03d", i),
			MaxRetries:  1,
		})
		if err != nil || !created {
			t.Fatalf("enqueue %d: created=%v err=%v", i, created, err)
		}
	}

	claimed := make(chan int64, jobCount)
	errs := make(chan error, claimers)
	var wg sync.WaitGroup
	for _, store := range stores {
		wg.Add(1)
		go func(store *Store) {
			defer wg.Done()
			for {
				job, err := store.Claim(ctx)
				if errors.Is(err, ErrNoJob) {
					return
				}
				if err != nil {
					errs <- err
					return
				}
				claimed <- job.ID
			}
		}(store)
	}
	wg.Wait()
	close(errs)
	close(claimed)

	for err := range errs {
		t.Errorf("claim: %v", err)
	}
	seen := make(map[int64]bool, jobCount)
	for id := range claimed {
		if seen[id] {
			t.Errorf("job %d was claimed more than once", id)
		}
		seen[id] = true
	}
	if len(seen) != jobCount {
		t.Fatalf("claimed %d unique jobs, want %d", len(seen), jobCount)
	}

	counts, err := stores[0].Counts(ctx)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts.Running != jobCount || counts.Queued != 0 {
		t.Fatalf("counts after claims = %+v", counts)
	}
}

func TestRetriesUsePersistedAvailableAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "queue.db")
	current := time.Date(2026, time.January, 2, 3, 4, 5, 6, time.UTC)
	now := func() time.Time { return current }

	store, err := Open(databasePath, WithClock(now))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	job, _, err := store.Enqueue(ctx, EnqueueParams{
		WatchName:   "incoming",
		Path:        "/drop/retry.csv",
		Fingerprint: "v1",
		MaxRetries:  2,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	first, err := store.Claim(ctx)
	if err != nil {
		t.Fatalf("claim first attempt: %v", err)
	}
	status, err := store.Fail(ctx, first.ID, first.RunID, "first failure", 10*time.Second)
	if err != nil {
		t.Fatalf("fail first attempt: %v", err)
	}
	if status != StatusQueued {
		t.Fatalf("first failure status = %s, want QUEUED", status)
	}
	queued, err := store.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get queued job: %v", err)
	}
	wantAvailable := current.Add(10 * time.Second)
	if !queued.AvailableAt.Equal(wantAvailable) {
		t.Fatalf("available_at = %s, want %s", queued.AvailableAt, wantAvailable)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	// Reopening proves eligibility is driven by the stored timestamp rather than
	// an in-memory timer or a worker sleeping through the retry delay.
	store, err = Open(databasePath, WithClock(now))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.Claim(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("claim before available_at = %v, want ErrNoJob", err)
	}

	current = wantAvailable
	second, err := store.Claim(ctx)
	if err != nil {
		t.Fatalf("claim second attempt: %v", err)
	}
	if second.Attempt != 2 || second.RunID == first.RunID {
		t.Fatalf("second claim = attempt %d run %d", second.Attempt, second.RunID)
	}
	status, err = store.Fail(ctx, second.ID, second.RunID, "second failure", 10*time.Second)
	if err != nil || status != StatusQueued {
		t.Fatalf("fail second attempt: status=%s err=%v", status, err)
	}

	current = current.Add(10 * time.Second)
	third, err := store.Claim(ctx)
	if err != nil {
		t.Fatalf("claim third attempt: %v", err)
	}
	if third.Attempt != 3 {
		t.Fatalf("third attempt = %d, want 3", third.Attempt)
	}
	status, err = store.Fail(ctx, third.ID, third.RunID, "final failure", time.Hour)
	if err != nil {
		t.Fatalf("fail final attempt: %v", err)
	}
	if status != StatusFailed {
		t.Fatalf("final status = %s, want FAILED", status)
	}
	if _, err := store.Claim(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("claim terminally failed job = %v, want ErrNoJob", err)
	}

	runs, err := store.ListRuns(ctx, job.ID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("run count = %d, want 3", len(runs))
	}
	for i, run := range runs {
		if run.Attempt != i+1 || run.Status != StatusFailed || run.FinishedAt == nil {
			t.Errorf("run %d = %+v", i, run)
		}
	}
}

func TestRecoverRunningRequeuesAndClosesInterruptedHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "queue.db")
	current := time.Date(2026, time.February, 3, 4, 5, 6, 7, time.UTC)
	now := func() time.Time { return current }

	store, err := Open(databasePath, WithClock(now))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	job, _, err := store.Enqueue(ctx, EnqueueParams{
		WatchName:   "incoming",
		Path:        "/drop/crash.csv",
		Fingerprint: "v1",
		MaxRetries:  0,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := store.Claim(ctx)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	commandID, err := store.StartCommand(ctx, CommandStart{
		RunID:    claimed.RunID,
		Sequence: 0,
		Name:     "process",
		Program:  "/usr/local/bin/process-file",
		Args:     []string{"--input", job.Path},
		Env:      []string{"MODE=test"},
		Timeout:  time.Minute,
	})
	if err != nil {
		t.Fatalf("start command: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close pre-crash store: %v", err)
	}

	current = current.Add(time.Minute)
	store, err = Open(databasePath, WithClock(now))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	recovered, err := store.RecoverRunning(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}

	recoveredJob, err := store.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get recovered job: %v", err)
	}
	if recoveredJob.Status != StatusQueued || recoveredJob.Attempts != 1 {
		t.Fatalf("recovered job = %+v", recoveredJob)
	}
	if recoveredJob.LastError != "interrupted by daemon restart" {
		t.Fatalf("recovery error = %q", recoveredJob.LastError)
	}
	run, err := store.GetRun(ctx, claimed.RunID)
	if err != nil {
		t.Fatalf("get interrupted run: %v", err)
	}
	if run.Status != StatusFailed || run.FinishedAt == nil {
		t.Fatalf("interrupted run = %+v", run)
	}
	command, err := store.GetCommand(ctx, commandID)
	if err != nil {
		t.Fatalf("get interrupted command: %v", err)
	}
	if command.Status != CommandFailed || command.ExitCode == nil || *command.ExitCode != -1 {
		t.Fatalf("interrupted command = %+v", command)
	}

	// Recovery retries even with max_retries=0 because a crash leaves execution
	// outcome ambiguous and at-least-once semantics require another attempt.
	retry, err := store.Claim(ctx)
	if err != nil {
		t.Fatalf("claim recovered job: %v", err)
	}
	if retry.Attempt != 2 {
		t.Fatalf("recovery attempt = %d, want 2", retry.Attempt)
	}
	if err := store.Succeed(ctx, retry.ID, retry.RunID); err != nil {
		t.Fatalf("succeed recovered job: %v", err)
	}
	if recovered, err := store.RecoverRunning(ctx); err != nil || recovered != 0 {
		t.Fatalf("second recovery: count=%d err=%v", recovered, err)
	}
}

func TestCommandHistoryPreservesArgumentsAndOrder(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := context.Background()
	job, _, err := store.Enqueue(ctx, EnqueueParams{
		WatchName:   "incoming",
		Path:        `/drop/a file; $(touch nope).csv`,
		Fingerprint: "v1",
		MaxRetries:  0,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := store.Claim(ctx)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	sequences := []int{2, 0, 1}
	for _, sequence := range sequences {
		commandID, err := store.StartCommand(ctx, CommandStart{
			RunID:      claimed.RunID,
			Sequence:   sequence,
			Name:       fmt.Sprintf("step-%d", sequence),
			Program:    `/opt/tools/a program;not-a-shell`,
			Args:       []string{"--input", job.Path, `literal $HOME && echo unsafe`},
			Env:        []string{`VALUE=spaces and ; metacharacters`},
			WorkingDir: `/work/a directory`,
			Timeout:    time.Duration(sequence+1) * time.Second,
		})
		if err != nil {
			t.Fatalf("start command %d: %v", sequence, err)
		}
		if err := store.CompleteCommand(ctx, commandID, CommandResult{
			Status:   CommandSucceeded,
			ExitCode: 0,
			Stdout:   fmt.Sprintf("output-%d", sequence),
		}); err != nil {
			t.Fatalf("complete command %d: %v", sequence, err)
		}
	}
	if err := store.Succeed(ctx, claimed.ID, claimed.RunID); err != nil {
		t.Fatalf("succeed job: %v", err)
	}

	commands, err := store.ListCommands(ctx, claimed.RunID)
	if err != nil {
		t.Fatalf("list commands: %v", err)
	}
	if len(commands) != 3 {
		t.Fatalf("command count = %d, want 3", len(commands))
	}
	gotOrder := make([]int, len(commands))
	for i, command := range commands {
		gotOrder[i] = command.Sequence
		if command.Args[1] != job.Path || command.Args[2] != `literal $HOME && echo unsafe` {
			t.Errorf("command %d args = %#v", i, command.Args)
		}
		if command.Program != `/opt/tools/a program;not-a-shell` {
			t.Errorf("command %d program = %q", i, command.Program)
		}
		if command.Status != CommandSucceeded || command.FinishedAt == nil {
			t.Errorf("command %d result = %+v", i, command)
		}
	}
	wantOrder := append([]int(nil), gotOrder...)
	sort.Ints(wantOrder)
	for i := range gotOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Fatalf("command order = %v, want sorted %v", gotOrder, wantOrder)
		}
	}

	summaries, err := store.ListCommandSummaries(ctx, claimed.RunID)
	if err != nil {
		t.Fatalf("list command summaries: %v", err)
	}
	if len(summaries) != len(commands) {
		t.Fatalf("summary count = %d, want %d", len(summaries), len(commands))
	}
	for index, summary := range summaries {
		if summary.Sequence != index || summary.StdoutBytes != int64(len(fmt.Sprintf("output-%d", index))) || summary.StderrBytes != 0 {
			t.Errorf("command summary %d = %+v", index, summary)
		}
	}
	output, err := store.GetCommandOutput(ctx, summaries[1].ID)
	if err != nil {
		t.Fatalf("get command output: %v", err)
	}
	if output.RunID != claimed.RunID || output.Stdout != "output-1" || output.Stderr != "" {
		t.Fatalf("command output = %+v", output)
	}
}

func TestFailClosesRunningCommandHistory(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := context.Background()
	_, _, err := store.Enqueue(ctx, EnqueueParams{
		WatchName: "incoming", Path: "/drop/fail.csv", Fingerprint: "v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	commandID, err := store.StartCommand(ctx, CommandStart{
		RunID: job.RunID, Sequence: 1, Name: "process", Program: "process-file",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Fail(ctx, job.ID, job.RunID, "persistence failed", 0); err != nil {
		t.Fatal(err)
	}
	command, err := store.GetCommand(ctx, commandID)
	if err != nil {
		t.Fatal(err)
	}
	if command.Status != CommandFailed || command.FinishedAt == nil || command.ExitCode == nil || *command.ExitCode != -1 {
		t.Fatalf("running command after job failure = %+v", command)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return store
}
