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

func TestWatcherRunDetectsConfiguredRootAncestorReplacement(t *testing.T) {
	container := t.TempDir()
	ancestor := filepath.Join(container, "ancestor")
	root := filepath.Join(ancestor, "watched")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	instance := newTestWatcher(t, config.WatchConfig{
		Name: "incoming",
		Path: root,
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- instance.Run(ctx)
	}()
	waitForDirectory(t, instance, root)

	if err := os.Rename(ancestor, filepath.Join(container, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-result:
		if !errors.Is(err, ErrWatchRootLost) {
			t.Fatalf("Run() error = %v, want ErrWatchRootLost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for configured root identity failure")
	}
}

func TestWatcherDoesNotDeliverFileFromReplacementRoot(t *testing.T) {
	container := t.TempDir()
	ancestor := filepath.Join(container, "ancestor")
	root := filepath.Join(ancestor, "watched")
	filename := filepath.Join(root, "report.csv")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	delivered := make(chan File, 1)
	instance := newTestWatcher(t, config.WatchConfig{
		Name: "incoming",
		Path: root,
	}, func(_ context.Context, _ config.WatchConfig, file File) error {
		delivered <- file
		return nil
	})
	defer instance.Close()
	identity, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.rootIDs.add(instance.watches[0], identity); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(ancestor, filepath.Join(container, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := instance.schedule(context.Background(), 0, filename); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-instance.errors:
		if !errors.Is(err, ErrWatchRootLost) {
			t.Fatalf("settling error = %v, want ErrWatchRootLost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for replacement-root rejection")
	}
	instance.wait.Wait()
	select {
	case file := <-delivered:
		t.Fatalf("replacement-root file was delivered: %+v", file)
	default:
	}
}

func TestRootIdentitySetDetectsAncestorRename(t *testing.T) {
	container := t.TempDir()
	ancestor := filepath.Join(container, "ancestor")
	root := filepath.Join(ancestor, "watched")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	identities := rootIdentitySet{}
	addTestRootIdentity(t, identities, "incoming", root)

	if err := os.Rename(ancestor, filepath.Join(container, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := identities.validateAll(); !errors.Is(err, ErrWatchRootLost) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validateAll() error = %v, want ErrWatchRootLost and os.ErrNotExist", err)
	}
}

func TestRootIdentitySetDetectsReplacementAfterAncestorRename(t *testing.T) {
	container := t.TempDir()
	ancestor := filepath.Join(container, "ancestor")
	root := filepath.Join(ancestor, "watched")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	identities := rootIdentitySet{}
	addTestRootIdentity(t, identities, "incoming", root)

	if err := os.Rename(ancestor, filepath.Join(container, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := identities.validateAll(); !errors.Is(err, ErrWatchRootLost) {
		t.Fatalf("validateAll() error = %v, want ErrWatchRootLost", err)
	}
}

func TestRootIdentitySetValidatesOnlyRootsRelevantToEvent(t *testing.T) {
	container := t.TempDir()
	first := filepath.Join(container, "first")
	second := filepath.Join(container, "second")
	for _, root := range []string{first, second} {
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	identities := rootIdentitySet{}
	addTestRootIdentity(t, identities, "first", first)
	addTestRootIdentity(t, identities, "second", second)

	oldFirst := filepath.Join(container, "old-first")
	if err := os.Rename(first, oldFirst); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(first, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := identities.validateEvent(filepath.Join(second, "report.csv")); err != nil {
		t.Fatalf("validateEvent(unaffected root) error = %v", err)
	}
	if err := identities.validateEvent(filepath.Join(first, "report.csv")); !errors.Is(err, ErrWatchRootLost) {
		t.Fatalf("validateEvent(replaced root) error = %v, want ErrWatchRootLost", err)
	}
}

func TestRootIdentitySetRejectsChangedIdentityForDuplicatePath(t *testing.T) {
	container := t.TempDir()
	root := filepath.Join(container, "watched")
	oldRoot := filepath.Join(container, "old-watched")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	identities := rootIdentitySet{}
	addTestRootIdentity(t, identities, "first", root)

	if err := os.Rename(root, oldRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	replacement, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	err = identities.add(watchSpec{config: config.WatchConfig{Name: "second"}, root: root}, replacement)
	if !errors.Is(err, ErrDirectoryChanged) {
		t.Fatalf("add(replacement) error = %v, want ErrDirectoryChanged", err)
	}
}

func TestRootIdentitySetBoundsUniqueRoots(t *testing.T) {
	root := t.TempDir()
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	identities := rootIdentitySet{}
	for index := range maxWatchedDirectories {
		spec := watchSpec{
			config: config.WatchConfig{Name: fmt.Sprintf("watch-%d", index)},
			root:   filepath.Join(root, fmt.Sprintf("root-%d", index)),
		}
		if err := identities.add(spec, info); err != nil {
			t.Fatalf("add(root %d) error = %v", index, err)
		}
	}
	overflow := watchSpec{config: config.WatchConfig{Name: "overflow"}, root: filepath.Join(root, "overflow")}
	if err := identities.add(overflow, info); !errors.Is(err, ErrDirectoryLimit) {
		t.Fatalf("add(overflow) error = %v, want ErrDirectoryLimit", err)
	}
}

func addTestRootIdentity(t *testing.T, identities rootIdentitySet, name, root string) {
	t.Helper()
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	spec := watchSpec{config: config.WatchConfig{Name: name}, root: root}
	if err := identities.add(spec, info); err != nil {
		t.Fatal(err)
	}
}
