package watcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/fsnotify/fsnotify"
)

func TestWatcherProcessesExistingRecursiveFiles(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	wanted := filepath.Join(nested, "existing.csv")
	if err := os.WriteFile(wanted, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nested, "skip.partial"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}

	files := make(chan File, 4)
	instance := newTestWatcher(t, config.WatchConfig{
		Name:            "incoming",
		Path:            root,
		Recursive:       true,
		ProcessExisting: true,
		Include:         []string{"*.csv"},
		Exclude:         []string{"*.partial"},
		SettleFor:       config.Duration{Duration: 25 * time.Millisecond},
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		files <- file
		return nil
	})

	cancel, result := runTestWatcher(t, instance)
	defer stopTestWatcher(t, cancel, result)
	select {
	case file := <-files:
		if file.Path != wanted {
			t.Fatalf("callback path = %q, want %q", file.Path, wanted)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for existing file")
	}
	select {
	case file := <-files:
		t.Fatalf("unexpected additional callback for %q", file.Path)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWatcherAddsNewRecursiveDirectoriesAndDebouncesWrites(t *testing.T) {
	root := t.TempDir()
	files := make(chan File, 4)
	instance := newTestWatcher(t, config.WatchConfig{
		Name:      "incoming",
		Path:      root,
		Recursive: true,
		Include:   []string{"*.csv"},
		SettleFor: config.Duration{Duration: 50 * time.Millisecond},
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		files <- file
		return nil
	})

	cancel, result := runTestWatcher(t, instance)
	defer stopTestWatcher(t, cancel, result)
	waitForDirectory(t, instance, root)

	nested := filepath.Join(root, "new", "nested")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	waitForDirectory(t, instance, nested)
	filename := filepath.Join(nested, "report with spaces; $special.csv")
	if err := os.WriteFile(filename, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("-two"); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case discovered := <-files:
		if discovered.Path != filename || discovered.Size != int64(len("one-two")) {
			t.Fatalf("unexpected callback: %+v", discovered)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for recursively discovered file")
	}
	select {
	case duplicate := <-files:
		t.Fatalf("event burst produced duplicate callback: %+v", duplicate)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestWatcherRemovesAndReaddsMovedRecursiveSubtree(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "watch")
	oldNested := filepath.Join(root, "subtree", "nested")
	if err := os.MkdirAll(oldNested, 0o700); err != nil {
		t.Fatal(err)
	}

	files := make(chan File, 1)
	instance := newTestWatcher(t, config.WatchConfig{
		Name:      "incoming",
		Path:      root,
		Recursive: true,
		Include:   []string{"*.csv"},
		SettleFor: config.Duration{Duration: 10 * time.Millisecond},
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		files <- file
		return nil
	})

	cancel, result := runTestWatcher(t, instance)
	defer stopTestWatcher(t, cancel, result)
	waitForDirectory(t, instance, oldNested)

	oldSubtree := filepath.Join(root, "subtree")
	movedSubtree := filepath.Join(base, "moved")
	if err := os.Rename(oldSubtree, movedSubtree); err != nil {
		t.Fatal(err)
	}
	waitForDirectoryTreeRemoval(t, instance, oldSubtree)

	recreatedNested := filepath.Join(oldSubtree, "nested")
	if err := os.MkdirAll(recreatedNested, 0o700); err != nil {
		t.Fatal(err)
	}
	waitForDirectory(t, instance, recreatedNested)
	filename := filepath.Join(recreatedNested, "recreated.csv")
	if err := os.WriteFile(filename, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if file := receiveFile(t, files); file.Path != filename {
		t.Fatalf("callback path = %q, want %q", file.Path, filename)
	}
}

func TestWatcherHandlesRecursiveSubtreeMoveWithinRoot(t *testing.T) {
	root := t.TempDir()
	oldNested := filepath.Join(root, "old", "nested")
	if err := os.MkdirAll(oldNested, 0o700); err != nil {
		t.Fatal(err)
	}
	files := make(chan File, 1)
	instance := newTestWatcher(t, config.WatchConfig{
		Name:      "incoming",
		Path:      root,
		Recursive: true,
		Include:   []string{"*.csv"},
		SettleFor: config.Duration{Duration: 10 * time.Millisecond},
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		files <- file
		return nil
	})
	cancel, result := runTestWatcher(t, instance)
	defer stopTestWatcher(t, cancel, result)
	waitForDirectory(t, instance, oldNested)

	newSubtree := filepath.Join(root, "new")
	if err := os.Rename(filepath.Join(root, "old"), newSubtree); err != nil {
		t.Fatal(err)
	}
	newNested := filepath.Join(newSubtree, "nested")
	waitForDirectory(t, instance, newNested)
	filename := filepath.Join(newNested, "moved.csv")
	if err := os.WriteFile(filename, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if file := receiveFile(t, files); file.Path != filename {
		t.Fatalf("callback path = %q, want %q", file.Path, filename)
	}
}

func TestWatcherReprocessOnChange(t *testing.T) {
	root := t.TempDir()
	files := make(chan File, 4)
	instance := newTestWatcher(t, config.WatchConfig{
		Name:              "incoming",
		Path:              root,
		ReprocessOnChange: true,
		Include:           []string{"*.csv"},
		SettleFor:         config.Duration{Duration: 25 * time.Millisecond},
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		files <- file
		return nil
	})

	cancel, result := runTestWatcher(t, instance)
	defer stopTestWatcher(t, cancel, result)
	waitForDirectory(t, instance, root)

	filename := filepath.Join(root, "change.csv")
	if err := os.WriteFile(filename, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	first := receiveFile(t, files)
	if err := os.WriteFile(filename, []byte("second version"), 0o600); err != nil {
		t.Fatal(err)
	}
	second := receiveFile(t, files)
	if first.Fingerprint == second.Fingerprint {
		t.Fatal("changed file retained the same fingerprint")
	}
}

func TestWatcherIgnoresNewFileSymlink(t *testing.T) {
	root := t.TempDir()
	targetDirectory := t.TempDir()
	target := filepath.Join(targetDirectory, "outside.csv")
	if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}

	files := make(chan File, 1)
	instance := newTestWatcher(t, config.WatchConfig{
		Name:      "incoming",
		Path:      root,
		Include:   []string{"*.csv"},
		SettleFor: config.Duration{Duration: 10 * time.Millisecond},
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		files <- file
		return nil
	})

	cancel, result := runTestWatcher(t, instance)
	defer stopTestWatcher(t, cancel, result)
	waitForDirectory(t, instance, root)
	if err := os.Symlink(target, filepath.Join(root, "link.csv")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	select {
	case file := <-files:
		t.Fatalf("watcher followed symbolic link: %+v", file)
	case err := <-result:
		t.Fatalf("watcher stopped after symbolic link event: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestWatcherRejectsSymlinkRoot(t *testing.T) {
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "watch-link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	instance := newTestWatcher(t, config.WatchConfig{
		Name:      "incoming",
		Path:      link,
		Recursive: true,
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	err := instance.Run(context.Background())
	if !errors.Is(err, ErrSymlink) {
		t.Fatalf("Run() error = %v, want ErrSymlink", err)
	}
}

func TestWatcherCloseStopsRun(t *testing.T) {
	root := t.TempDir()
	instance := newTestWatcher(t, config.WatchConfig{
		Name: "incoming",
		Path: root,
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	defer instance.Close()

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
		t.Fatal("Run did not stop after Close")
	}
}

func TestWatcherReportsConfiguredRootLoss(t *testing.T) {
	root := t.TempDir()
	instance := newTestWatcher(t, config.WatchConfig{
		Name: "incoming",
		Path: root,
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	defer instance.Close()

	err := instance.handleEvent(context.Background(), fsnotify.Event{Name: root, Op: fsnotify.Rename})
	if !errors.Is(err, ErrWatchRootLost) {
		t.Fatalf("handleEvent(root rename) error = %v, want ErrWatchRootLost", err)
	}
}

func TestWatchedDirectoryTreeIsDeepestFirst(t *testing.T) {
	root := t.TempDir()
	watched := map[string]struct{}{
		root:                            {},
		filepath.Join(root, "a"):        {},
		filepath.Join(root, "a", "z"):   {},
		filepath.Join(root, "b"):        {},
		filepath.Join(t.TempDir(), "x"): {},
	}
	want := []string{
		filepath.Join(root, "a", "z"),
		filepath.Join(root, "a"),
		filepath.Join(root, "b"),
		root,
	}
	got := watchedDirectoryTree(watched, root)
	if len(got) != len(want) {
		t.Fatalf("watchedDirectoryTree() = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("watchedDirectoryTree()[%d] = %q, want %q (all: %v)", index, got[index], want[index], got)
		}
	}
}

func TestRemoveDirectoryTreeRemovesDescendantWatchesAndToleratesMissing(t *testing.T) {
	root := t.TempDir()
	subtree := filepath.Join(root, "subtree")
	nested := filepath.Join(subtree, "nested")
	sibling := filepath.Join(root, "sibling")
	for _, directory := range []string{subtree, nested, sibling} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	instance := newTestWatcher(t, config.WatchConfig{Name: "incoming", Path: root}, func(context.Context, config.WatchConfig, File) error {
		return nil
	})
	defer instance.Close()
	for _, directory := range []string{root, subtree, nested, sibling} {
		info, err := os.Lstat(directory)
		if err != nil {
			t.Fatal(err)
		}
		if err := instance.addDirectory(directory, info); err != nil {
			t.Fatal(err)
		}
	}
	if err := instance.filesystem.Remove(nested); err != nil {
		t.Fatalf("remove nested watch before cleanup: %v", err)
	}

	if err := instance.removeDirectoryTree(subtree); err != nil {
		t.Fatalf("removeDirectoryTree() error = %v", err)
	}
	instance.mu.Lock()
	_, hasSubtree := instance.watchedDirs[subtree]
	_, hasNested := instance.watchedDirs[nested]
	_, hasRoot := instance.watchedDirs[root]
	_, hasSibling := instance.watchedDirs[sibling]
	instance.mu.Unlock()
	if hasSubtree || hasNested {
		t.Fatalf("removed subtree remains in bookkeeping: subtree=%t nested=%t", hasSubtree, hasNested)
	}
	if !hasRoot || !hasSibling {
		t.Fatalf("unrelated bookkeeping was removed: root=%t sibling=%t", hasRoot, hasSibling)
	}
	watchList := make(map[string]struct{})
	for _, directory := range instance.filesystem.WatchList() {
		watchList[filepath.Clean(directory)] = struct{}{}
	}
	if _, exists := watchList[subtree]; exists {
		t.Fatalf("subtree remains in fsnotify watch list: %v", instance.filesystem.WatchList())
	}
	if _, exists := watchList[nested]; exists {
		t.Fatalf("nested directory remains in fsnotify watch list: %v", instance.filesystem.WatchList())
	}
	if _, exists := watchList[root]; !exists {
		t.Fatalf("root missing from fsnotify watch list: %v", instance.filesystem.WatchList())
	}
	if _, exists := watchList[sibling]; !exists {
		t.Fatalf("sibling missing from fsnotify watch list: %v", instance.filesystem.WatchList())
	}
}

func TestAddDirectoryRebindsMovedDirectoryFromStalePath(t *testing.T) {
	root := t.TempDir()
	oldPath := filepath.Join(root, "old")
	newPath := filepath.Join(root, "new")
	if err := os.Mkdir(oldPath, 0o700); err != nil {
		t.Fatal(err)
	}
	identity, err := os.Lstat(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	instance := newTestWatcher(t, config.WatchConfig{Name: "incoming", Path: root}, func(context.Context, config.WatchConfig, File) error {
		return nil
	})
	defer instance.Close()
	if err := instance.addDirectory(oldPath, identity); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	if err := instance.addDirectory(newPath, identity); err != nil {
		t.Fatalf("addDirectory(moved path) error = %v", err)
	}

	instance.mu.Lock()
	_, hasOldPath := instance.watchedDirs[oldPath]
	_, hasNewPath := instance.watchedDirs[newPath]
	instance.mu.Unlock()
	if hasOldPath || !hasNewPath {
		t.Fatalf("moved watch bookkeeping: old=%t new=%t", hasOldPath, hasNewPath)
	}
	watches := instance.filesystem.WatchList()
	if watchListContains(watches, oldPath) || !watchListContains(watches, newPath) {
		t.Fatalf("moved filesystem watch list = %v", watches)
	}
}

func TestFilesystemWatchValidationFailsPersistentLostDirectory(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	instance := newTestWatcher(t, config.WatchConfig{Name: "incoming", Path: root}, func(context.Context, config.WatchConfig, File) error {
		return nil
	})
	defer instance.Close()
	for _, directory := range []string{root, nested} {
		info, err := os.Lstat(directory)
		if err != nil {
			t.Fatal(err)
		}
		if err := instance.addDirectory(directory, info); err != nil {
			t.Fatal(err)
		}
	}
	if err := instance.filesystem.Remove(nested); err != nil {
		t.Fatal(err)
	}
	// This creation has no backend watch to report it. Reinstalling the watch
	// would make the instance look healthy while silently losing this input.
	if err := os.WriteFile(filepath.Join(nested, "lost.csv"), []byte("lost"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := instance.validateFilesystemWatches(); err != nil {
		t.Fatalf("first validateFilesystemWatches() error = %v, want one-interval grace", err)
	}
	if err := instance.validateFilesystemWatches(); !errors.Is(err, ErrFilesystemWatchLost) {
		t.Fatalf("second validateFilesystemWatches() error = %v, want ErrFilesystemWatchLost", err)
	}
}

func TestFilesystemWatchValidationAllowsQueuedRemovalToReconcile(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	instance := newTestWatcher(t, config.WatchConfig{Name: "incoming", Path: root}, func(context.Context, config.WatchConfig, File) error {
		return nil
	})
	defer instance.Close()
	for _, directory := range []string{root, nested} {
		info, err := os.Lstat(directory)
		if err != nil {
			t.Fatal(err)
		}
		if err := instance.addDirectory(directory, info); err != nil {
			t.Fatal(err)
		}
	}
	if err := instance.filesystem.Remove(nested); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(nested); err != nil {
		t.Fatal(err)
	}
	if err := instance.validateFilesystemWatches(); err != nil {
		t.Fatalf("validateFilesystemWatches() prune error = %v", err)
	}
	if err := instance.validateFilesystemWatches(); err != nil {
		t.Fatalf("validateFilesystemWatches() after queued removal = %v", err)
	}
}

func TestFilesystemWatchValidationFailsPersistentIdentityChange(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	instance := newTestWatcher(t, config.WatchConfig{Name: "incoming", Path: root}, func(context.Context, config.WatchConfig, File) error {
		return nil
	})
	defer instance.Close()
	for _, directory := range []string{root, nested} {
		info, err := os.Lstat(directory)
		if err != nil {
			t.Fatal(err)
		}
		if err := instance.addDirectory(directory, info); err != nil {
			t.Fatal(err)
		}
	}
	if err := instance.filesystem.Remove(nested); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(nested, filepath.Join(root, "old-nested")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := instance.validateFilesystemWatches(); err != nil {
		t.Fatalf("first validateFilesystemWatches() error = %v, want one-interval grace", err)
	}
	if err := instance.validateFilesystemWatches(); !errors.Is(err, ErrFilesystemWatchLost) {
		t.Fatalf("second validateFilesystemWatches() error = %v, want ErrFilesystemWatchLost", err)
	}
}

func TestWalkTreeBatchedSkipsSymlinkDirectories(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "outside.csv"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	regularFiles := 0
	if err := walkTreeBatched(context.Background(), root, true, func(_ string, info os.FileInfo) error {
		if info.Mode().IsRegular() {
			regularFiles++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if regularFiles != 0 {
		t.Fatalf("walk visited %d files through a symlink directory", regularFiles)
	}
}

func TestWalkTreeBatchedTraversesMultipleBatchesAndHonorsCancellation(t *testing.T) {
	root := t.TempDir()
	for index := range directoryReadBatchSize + 5 {
		filename := filepath.Join(root, fmt.Sprintf("file-%03d.csv", index))
		if err := os.WriteFile(filename, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	visitedFiles := 0
	err := walkTreeBatched(ctx, root, false, func(_ string, info os.FileInfo) error {
		if info.Mode().IsRegular() {
			visitedFiles++
			if visitedFiles == directoryReadBatchSize+1 {
				cancel()
			}
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("walkTreeBatched() error = %v, want context.Canceled", err)
	}
	if visitedFiles != directoryReadBatchSize+1 {
		t.Fatalf("visited files = %d, want %d before cancellation", visitedFiles, directoryReadBatchSize+1)
	}
}

func TestWalkTreeBatchedContinuesAfterQueuedDirectoryDisappears(t *testing.T) {
	root := t.TempDir()
	directories := []string{"first", "second", "third", "fourth"}
	for _, name := range directories {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	visited := make(map[string]bool)
	deleted := ""
	err := walkTreeBatched(context.Background(), root, true, func(path string, info os.FileInfo) error {
		if !info.IsDir() || path == root {
			return nil
		}
		name := filepath.Base(path)
		visited[name] = true
		if deleted != "" {
			return nil
		}
		for _, candidate := range directories {
			if candidate == name {
				continue
			}
			if err := os.Remove(filepath.Join(root, candidate)); err != nil {
				return err
			}
			deleted = candidate
			break
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walkTreeBatched() error = %v, want traversal to continue", err)
	}
	if deleted == "" {
		t.Fatal("test did not remove a queued directory")
	}
	for _, name := range directories {
		if name != deleted && !visited[name] {
			t.Errorf("surviving sibling directory %q was not visited", name)
		}
	}
}

func TestWalkTreeBatchedContinuesAfterQueuedDirectoryIsReplaced(t *testing.T) {
	root := t.TempDir()
	directories := []string{"first", "second", "third", "fourth"}
	for _, name := range directories {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	visited := make(map[string]bool)
	replaced := ""
	err := walkTreeBatched(context.Background(), root, true, func(path string, info os.FileInfo) error {
		if !info.IsDir() || path == root {
			return nil
		}
		name := filepath.Base(path)
		visited[name] = true
		if replaced != "" {
			return nil
		}
		for _, candidate := range directories {
			if candidate == name {
				continue
			}
			candidatePath := filepath.Join(root, candidate)
			if err := os.Remove(candidatePath); err != nil {
				return err
			}
			if err := os.Mkdir(candidatePath, 0o700); err != nil {
				return err
			}
			replaced = candidate
			break
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walkTreeBatched() error = %v, want traversal to continue", err)
	}
	if replaced == "" {
		t.Fatal("test did not replace a queued directory")
	}
	for _, name := range directories {
		if name != replaced && !visited[name] {
			t.Errorf("surviving sibling directory %q was not visited", name)
		}
	}
}

func TestScanDirectoryBatchedRejectsIdentityChange(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	for _, directory := range []string{first, second} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	firstInfo, err := os.Lstat(first)
	if err != nil {
		t.Fatal(err)
	}
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer rootHandle.Close()

	var directories []queuedDirectory
	discovered := 1
	err = scanDirectoryBatched(
		context.Background(),
		rootHandle,
		root,
		queuedDirectory{relative: "second", info: firstInfo},
		true,
		nil,
		&directories,
		&discovered,
	)
	if !errors.Is(err, ErrDirectoryChanged) {
		t.Fatalf("scanDirectoryBatched() error = %v, want ErrDirectoryChanged", err)
	}
}

func TestWatcherBoundsPendingFiles(t *testing.T) {
	instance := newTestWatcher(t, config.WatchConfig{
		Name: "incoming",
		Path: t.TempDir(),
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	defer instance.Close()
	instance.mu.Lock()
	for index := range maxPendingFiles {
		key := fmt.Sprintf("file-%d", index)
		instance.pending[key] = &pendingFile{}
	}
	instance.mu.Unlock()

	err := instance.schedule(context.Background(), 0, filepath.Join(instance.watches[0].root, "overflow"))
	if !errors.Is(err, ErrPendingLimit) {
		t.Fatalf("overflow schedule error = %v, want ErrPendingLimit", err)
	}
	instance.mu.Lock()
	pending := len(instance.pending)
	instance.mu.Unlock()
	if pending != maxPendingFiles {
		t.Fatalf("pending files = %d, want limit %d", pending, maxPendingFiles)
	}
}

func TestWatcherBoundsDeliveredCache(t *testing.T) {
	instance := newTestWatcher(t, config.WatchConfig{
		Name: "incoming",
		Path: t.TempDir(),
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	defer instance.Close()

	instance.mu.Lock()
	for index := range maxDeliveredFiles {
		instance.recordDeliveredLocked(fmt.Sprintf("file-%d", index), "fingerprint")
	}
	instance.recordDeliveredLocked("new-file", "new-fingerprint")
	deliveredCount := len(instance.delivered)
	_, retained := instance.delivered["new-file"]
	instance.mu.Unlock()

	if deliveredCount != maxDeliveredFiles {
		t.Fatalf("delivered files = %d, want limit %d", deliveredCount, maxDeliveredFiles)
	}
	if !retained {
		t.Fatal("new delivered file was not retained")
	}
}

func TestWatcherBoundsRegisteredDirectories(t *testing.T) {
	root := t.TempDir()
	instance := newTestWatcher(t, config.WatchConfig{
		Name: "incoming",
		Path: root,
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	defer instance.Close()
	instance.mu.Lock()
	for index := range maxWatchedDirectories {
		instance.watchedDirs[fmt.Sprintf("directory-%d", index)] = struct{}{}
	}
	instance.mu.Unlock()
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}

	err = instance.addDirectory(root, info)
	if !errors.Is(err, ErrDirectoryLimit) {
		t.Fatalf("addDirectory() error = %v, want ErrDirectoryLimit", err)
	}
}

func newTestWatcher(t *testing.T, watch config.WatchConfig, handler Handler) *Watcher {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	instance, err := New([]config.WatchConfig{watch}, handler, logger)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return instance
}

func runTestWatcher(t *testing.T, instance *Watcher) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- instance.Run(ctx)
	}()
	return cancel, result
}

func stopTestWatcher(t *testing.T, cancel context.CancelFunc, result <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Errorf("Run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("timed out stopping watcher")
	}
}

func waitForDirectory(t *testing.T, instance *Watcher, directory string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	directory = filepath.Clean(directory)
	for time.Now().Before(deadline) {
		instance.mu.Lock()
		_, exists := instance.watchedDirs[directory]
		instance.mu.Unlock()
		if exists {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("directory %q was not watched", directory)
}

func waitForDirectoryTreeRemoval(t *testing.T, instance *Watcher, directory string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	directory = filepath.Clean(directory)
	for time.Now().Before(deadline) {
		instance.mu.Lock()
		bookkeeping := watchedDirectoryTree(instance.watchedDirs, directory)
		instance.mu.Unlock()
		registered := false
		for _, watched := range instance.filesystem.WatchList() {
			if within(directory, filepath.Clean(watched)) {
				registered = true
				break
			}
		}
		if len(bookkeeping) == 0 && !registered {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	instance.mu.Lock()
	bookkeeping := watchedDirectoryTree(instance.watchedDirs, directory)
	instance.mu.Unlock()
	filesystemWatches := instance.filesystem.WatchList()
	t.Fatalf("directory tree %q was not removed: bookkeeping=%v fsnotify=%v", directory, bookkeeping, filesystemWatches)
}

func receiveFile(t *testing.T, files <-chan File) File {
	t.Helper()
	select {
	case file := <-files:
		return file
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for file")
		return File{}
	}
}
