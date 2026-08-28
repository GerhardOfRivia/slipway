package watcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	// ErrSymlink identifies a watched path whose final component is a symbolic
	// link. slipway does not follow watched-file symlinks.
	ErrSymlink = errors.New("symbolic links are not watched")
	// ErrPendingLimit identifies an instance whose simultaneous file-settling
	// work reached its safety limit.
	ErrPendingLimit = errors.New("pending file limit reached")
	// ErrDirectoryLimit identifies an instance whose recursive watch set or
	// one discovery traversal reached its safety limit.
	ErrDirectoryLimit = errors.New("watched directory limit reached")
	// ErrDirectoryChanged identifies a directory replaced while it was being
	// safely opened for discovery or registration.
	ErrDirectoryChanged = errors.New("watched directory changed during discovery")
	// ErrWatchRootLost identifies a configured root path that no longer reaches
	// the directory registered at startup, including after ancestor replacement.
	ErrWatchRootLost = errors.New("configured watch root was removed or renamed")
	// ErrPolledFileLimit identifies a macOS polling snapshot whose matching
	// watch/path entries reached the instance safety limit.
	ErrPolledFileLimit = errors.New("polled file limit reached")
	// ErrFilesystemWatchLost identifies a mismatch between slipway's registered
	// directory set and the operating-system notification backend.
	ErrFilesystemWatchLost = errors.New("filesystem watch registration was lost")
)

// File is the stable filesystem snapshot passed to a Handler. Fingerprint is
// derived from the canonical path, size, and modification time.
type File struct {
	Path        string
	Size        int64
	ModTime     time.Time
	Fingerprint string
}

// WaitStable waits until a regular file's size and modification time have not
// changed for settleFor. It does not open or hash file contents.
func WaitStable(ctx context.Context, filename string, settleFor time.Duration) (File, error) {
	if ctx == nil {
		return File{}, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return File{}, err
	}
	if settleFor < 0 {
		return File{}, errors.New("settle duration must not be negative")
	}
	canonical, err := filepath.Abs(filename)
	if err != nil {
		return File{}, fmt.Errorf("resolve file path: %w", err)
	}
	canonical = filepath.Clean(canonical)

	info, err := regularFileInfo(canonical)
	if err != nil {
		return File{}, err
	}
	if settleFor == 0 {
		return snapshot(canonical, info), nil
	}

	stableSince := time.Now()
	last := signatureOf(info)
	pollInterval := settleFor / 5
	if pollInterval < time.Millisecond {
		pollInterval = time.Millisecond
	}
	if pollInterval > 250*time.Millisecond {
		pollInterval = 250 * time.Millisecond
	}

	for {
		remaining := settleFor - time.Since(stableSince)
		if remaining <= 0 {
			return snapshot(canonical, info), nil
		}
		waitFor := pollInterval
		if remaining < waitFor {
			waitFor = remaining
		}
		timer := time.NewTimer(waitFor)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return File{}, ctx.Err()
		case <-timer.C:
		}

		current, statErr := regularFileInfo(canonical)
		if statErr != nil {
			return File{}, statErr
		}
		currentSignature := signatureOf(current)
		if currentSignature != last {
			last = currentSignature
			stableSince = time.Now()
		}
		info = current
	}
}

type fileSignature struct {
	size       int64
	modifiedAt int64
}

func signatureOf(info os.FileInfo) fileSignature {
	return fileSignature{size: info.Size(), modifiedAt: info.ModTime().UnixNano()}
}

func regularFileInfo(filename string) (os.FileInfo, error) {
	info, err := os.Lstat(filename)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s: %w", filename, ErrSymlink)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", filename)
	}
	return info, nil
}

func snapshot(filename string, info os.FileInfo) File {
	hash := sha256.New()
	hash.Write([]byte(filename))
	hash.Write([]byte{0})
	hash.Write([]byte(strconv.FormatInt(info.Size(), 10)))
	hash.Write([]byte{0})
	hash.Write([]byte(strconv.FormatInt(info.ModTime().UnixNano(), 10)))
	return File{
		Path:        filename,
		Size:        info.Size(),
		ModTime:     info.ModTime(),
		Fingerprint: hex.EncodeToString(hash.Sum(nil)),
	}
}
