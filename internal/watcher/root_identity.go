package watcher

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// rootIdentitySet remembers the directory objects that configured watch paths
// named when the watcher registered them. Filesystem subscriptions commonly
// remain attached to an object after one of the path's ancestors is renamed,
// so subscription events alone cannot prove that a configured pathname still
// reaches the same directory.
//
// The set is intentionally keyed by the cleaned configured path. This avoids
// multiplying identity checks when several watch configurations share a root,
// and maxWatchedDirectories bounds both its memory and validation work.
type rootIdentitySet map[string]rootIdentity

type rootIdentity struct {
	watchName string
	path      string
	info      os.FileInfo
}

func (identities rootIdentitySet) add(spec watchSpec, info os.FileInfo) error {
	path := filepath.Clean(spec.root)
	if info == nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("record watch %q root %s: %w", spec.config.Name, path, ErrDirectoryChanged)
	}
	if existing, ok := identities[path]; ok {
		if !os.SameFile(existing.info, info) {
			return fmt.Errorf("record watch %q root %s: %w", spec.config.Name, path, ErrDirectoryChanged)
		}
		return nil
	}
	if len(identities) >= maxWatchedDirectories {
		return fmt.Errorf("record watch %q root %s: %w (%d directories)", spec.config.Name, path, ErrDirectoryLimit, maxWatchedDirectories)
	}
	identities[path] = rootIdentity{
		watchName: spec.config.Name,
		path:      path,
		info:      info,
	}
	return nil
}

func (identities rootIdentitySet) validateAll() error {
	for _, identity := range identities {
		if err := identity.validate(); err != nil {
			return err
		}
	}
	return nil
}

// validateEvent checks roots that contain the event's lexical pathname. It is
// intended to run before dispatching an fsnotify event, so an event emitted by
// an old directory object cannot be applied to a replacement at the configured
// path. validateAll must also run periodically because replacing an ancestor
// need not produce an event on the watched directory on every platform.
func (identities rootIdentitySet) validateEvent(filename string) error {
	filename = filepath.Clean(filename)
	for _, identity := range identities {
		if within(identity.path, filename) {
			if err := identity.validate(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (identity rootIdentity) validate() error {
	current, err := os.Lstat(identity.path)
	if err != nil {
		return fmt.Errorf(
			"watch %q root %s: %w",
			identity.watchName,
			identity.path,
			errors.Join(ErrWatchRootLost, err),
		)
	}
	if current.Mode()&os.ModeSymlink != 0 || !current.IsDir() || !os.SameFile(identity.info, current) {
		return fmt.Errorf("watch %q root %s: %w", identity.watchName, identity.path, ErrWatchRootLost)
	}
	return nil
}
