package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

func TestPollOnceBaselinesExistingFilesThenDetectsChangesAndCreates(t *testing.T) {
	root := t.TempDir()
	existing := filepath.Join(root, "existing.csv")
	if err := os.WriteFile(existing, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	files := make(chan File, 4)
	instance := newTestWatcher(t, config.WatchConfig{
		Name:              "incoming",
		Path:              root,
		Include:           []string{"*.csv"},
		ReprocessOnChange: true,
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		files <- file
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer finishPollingTest(t, instance, cancel)

	if err := instance.pollOnce(ctx, true); err != nil {
		t.Fatal(err)
	}
	assertNoPolledFile(t, files)

	if err := os.WriteFile(existing, []byte("changed contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := instance.pollOnce(ctx, false); err != nil {
		t.Fatal(err)
	}
	if file := receiveFile(t, files); file.Path != existing {
		t.Fatalf("modified callback path = %q, want %q", file.Path, existing)
	}
	if err := instance.pollOnce(ctx, false); err != nil {
		t.Fatal(err)
	}
	assertNoPolledFile(t, files)

	created := filepath.Join(root, "created.csv")
	if err := os.WriteFile(created, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := instance.pollOnce(ctx, false); err != nil {
		t.Fatal(err)
	}
	if file := receiveFile(t, files); file.Path != created {
		t.Fatalf("created callback path = %q, want %q", file.Path, created)
	}
}

func TestWatcherRunPollingBackendBaselinesThenDetectsModification(t *testing.T) {
	root := t.TempDir()
	filename := filepath.Join(root, "existing.csv")
	if err := os.WriteFile(filename, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	files := make(chan File, 1)
	instance := newTestWatcher(t, config.WatchConfig{
		Name:    "incoming",
		Path:    root,
		Include: []string{"*.csv"},
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		files <- file
		return nil
	})
	forcePollingBackend(t, instance)
	cancel, result := runTestWatcher(t, instance)
	defer stopTestWatcher(t, cancel, result)
	waitForDirectory(t, instance, root)
	assertNoPolledFile(t, files)

	if err := os.WriteFile(filename, []byte("changed contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	if file := receiveFile(t, files); file.Path != filename {
		t.Fatalf("modified callback path = %q, want %q", file.Path, filename)
	}
}

func TestWatcherCloseStopsPollingRun(t *testing.T) {
	root := t.TempDir()
	instance := newTestWatcher(t, config.WatchConfig{
		Name: "incoming",
		Path: root,
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	forcePollingBackend(t, instance)
	result := make(chan error, 1)
	go func() {
		result <- instance.Run(context.Background())
	}()
	waitForDirectory(t, instance, root)
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run() error after Close() = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("polling Run did not stop after Close")
	}
}

func TestPollOnceProcessesInitialFilesWhenConfigured(t *testing.T) {
	root := t.TempDir()
	existing := filepath.Join(root, "existing.csv")
	if err := os.WriteFile(existing, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}

	files := make(chan File, 1)
	instance := newTestWatcher(t, config.WatchConfig{
		Name:            "incoming",
		Path:            root,
		ProcessExisting: true,
		Include:         []string{"*.csv"},
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		files <- file
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer finishPollingTest(t, instance, cancel)

	if err := instance.pollOnce(ctx, true); err != nil {
		t.Fatal(err)
	}
	if file := receiveFile(t, files); file.Path != existing {
		t.Fatalf("initial callback path = %q, want %q", file.Path, existing)
	}
}

func TestCollectPollingSnapshotBoundsMatchingWatchPaths(t *testing.T) {
	root := t.TempDir()
	for index := range 3 {
		filename := filepath.Join(root, fmt.Sprintf("file-%d.csv", index))
		if err := os.WriteFile(filename, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	instance := newTestWatcher(t, config.WatchConfig{
		Name:    "incoming",
		Path:    root,
		Include: []string{"*.csv"},
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	defer instance.Close()

	_, _, err := instance.collectPollingSnapshot(context.Background(), 2)
	if !errors.Is(err, ErrPolledFileLimit) {
		t.Fatalf("collectPollingSnapshot() error = %v, want ErrPolledFileLimit", err)
	}
}

func TestPollOnceDoesNotPublishSnapshotWhenSchedulingFails(t *testing.T) {
	root := t.TempDir()
	filename := filepath.Join(root, "existing.csv")
	if err := os.WriteFile(filename, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	instance := newTestWatcher(t, config.WatchConfig{
		Name:    "incoming",
		Path:    root,
		Include: []string{"*.csv"},
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	defer instance.Close()

	if err := instance.pollOnce(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	key := polledFileKey{watchIndex: 0, path: filename}
	baseline, exists := instance.pollSnapshot[key]
	if !exists {
		t.Fatal("initial polling snapshot did not contain existing file")
	}
	if err := os.WriteFile(filename, []byte("changed contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	instance.mu.Lock()
	for index := range maxPendingFiles {
		instance.pending[fmt.Sprintf("occupied-%d", index)] = &pendingFile{}
	}
	instance.mu.Unlock()

	err := instance.pollOnce(context.Background(), false)
	if !errors.Is(err, ErrPendingLimit) {
		t.Fatalf("pollOnce() error = %v, want ErrPendingLimit", err)
	}
	if got := instance.pollSnapshot[key]; got != baseline {
		t.Fatalf("failed poll published signature %+v, want prior %+v", got, baseline)
	}
}

func TestPollOnceTreatsRecreatedPathAsNewAfterMissingScan(t *testing.T) {
	root := t.TempDir()
	filename := filepath.Join(root, "existing.csv")
	contents := []byte("same")
	if err := os.WriteFile(filename, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	original, err := os.Lstat(filename)
	if err != nil {
		t.Fatal(err)
	}

	files := make(chan File, 1)
	instance := newTestWatcher(t, config.WatchConfig{
		Name:    "incoming",
		Path:    root,
		Include: []string{"*.csv"},
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		files <- file
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer finishPollingTest(t, instance, cancel)
	if err := instance.pollOnce(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filename); err != nil {
		t.Fatal(err)
	}
	if err := instance.pollOnce(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filename, original.ModTime(), original.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := instance.pollOnce(ctx, false); err != nil {
		t.Fatal(err)
	}
	if file := receiveFile(t, files); file.Path != filename {
		t.Fatalf("recreated callback path = %q, want %q", file.Path, filename)
	}
}

func TestPollOnceRejectsConfiguredRootReplacementWithoutPublishing(t *testing.T) {
	container := t.TempDir()
	ancestor := filepath.Join(container, "ancestor")
	root := filepath.Join(ancestor, "watched")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(root, "existing.csv")
	if err := os.WriteFile(filename, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	instance := newTestWatcher(t, config.WatchConfig{
		Name:    "incoming",
		Path:    root,
		Include: []string{"*.csv"},
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	defer instance.Close()
	if err := instance.pollOnce(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	baselineSize := len(instance.pollSnapshot)

	if err := os.Rename(ancestor, filepath.Join(container, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "replacement.csv"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := instance.pollOnce(context.Background(), false)
	if !errors.Is(err, ErrWatchRootLost) {
		t.Fatalf("pollOnce() error = %v, want ErrWatchRootLost", err)
	}
	if len(instance.pollSnapshot) != baselineSize {
		t.Fatalf("root-loss poll replaced snapshot size %d with %d", baselineSize, len(instance.pollSnapshot))
	}
}

func assertNoPolledFile(t *testing.T, files <-chan File) {
	t.Helper()
	select {
	case file := <-files:
		t.Fatalf("unexpected polling callback for %q", file.Path)
	case <-time.After(50 * time.Millisecond):
	}
}

func finishPollingTest(t *testing.T, instance *Watcher, cancel context.CancelFunc) {
	t.Helper()
	cancel()
	instance.wait.Wait()
	if err := instance.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func forcePollingBackend(t *testing.T, instance *Watcher) {
	t.Helper()
	if instance.filesystem != nil {
		if err := instance.filesystem.Close(); err != nil {
			t.Fatal(err)
		}
		instance.filesystem = nil
	}
	instance.polling = true
}
