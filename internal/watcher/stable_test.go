package watcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWaitStableRestartsAfterChange(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "a file; $(not-a-command).csv")
	if err := os.WriteFile(filename, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}

	changed := make(chan struct{})
	go func() {
		time.Sleep(35 * time.Millisecond)
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0)
		if err == nil {
			_, err = file.WriteString("-two")
			_ = file.Close()
		}
		if err != nil {
			t.Errorf("modify stable test file: %v", err)
		}
		close(changed)
	}()

	started := time.Now()
	file, err := WaitStable(context.Background(), filename, 70*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitStable() error = %v", err)
	}
	<-changed
	if elapsed := time.Since(started); elapsed < 90*time.Millisecond {
		t.Fatalf("WaitStable() returned after %s; change did not restart settling", elapsed)
	}
	if file.Size != int64(len("one-two")) || file.Path != filename || file.Fingerprint == "" {
		t.Fatalf("unexpected stable file: %+v", file)
	}
}

func TestWaitStableHonorsCancellation(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(filename, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := WaitStable(ctx, filename, time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitStable() error = %v, want context.Canceled", err)
	}
}

func TestWaitStableZeroDurationHonorsCancellation(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(filename, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := WaitStable(ctx, filename, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitStable() error = %v, want context.Canceled", err)
	}
}

func TestWaitStableRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.csv")
	link := filepath.Join(directory, "link.csv")
	if err := os.WriteFile(target, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	_, err := WaitStable(context.Background(), link, 0)
	if !errors.Is(err, ErrSymlink) {
		t.Fatalf("WaitStable() error = %v, want ErrSymlink", err)
	}
}

func TestWaitStableRejectsFileReplacedBySymlink(t *testing.T) {
	directory := t.TempDir()
	filename := filepath.Join(directory, "changing.csv")
	target := filepath.Join(directory, "target.csv")
	if err := os.WriteFile(filename, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}

	replaced := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		replacement := filepath.Join(directory, "replacement-link")
		if err := os.Symlink(target, replacement); err != nil {
			replaced <- err
			return
		}
		replaced <- os.Rename(replacement, filename)
	}()

	_, err := WaitStable(context.Background(), filename, 100*time.Millisecond)
	if replaceErr := <-replaced; replaceErr != nil {
		t.Skipf("replace file with symlink: %v", replaceErr)
	}
	if !errors.Is(err, ErrSymlink) {
		t.Fatalf("WaitStable() error = %v, want ErrSymlink", err)
	}
}
