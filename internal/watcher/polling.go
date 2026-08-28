package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
)

const maxPolledFiles = 4096

type polledFileKey struct {
	watchIndex int
	path       string
}

type pollingSnapshot map[polledFileKey]fileSignature

// collectPollingSnapshot builds a complete bounded view before pollOnce
// schedules any work. This makes scan, cancellation, root-identity, and file
// limit failures atomic with respect to the published polling baseline.
func (w *Watcher) collectPollingSnapshot(
	ctx context.Context,
	fileLimit int,
) (pollingSnapshot, map[string]os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := w.rootIDs.validateAll(); err != nil {
		return nil, nil, err
	}
	next := make(pollingSnapshot)
	directories := make(map[string]os.FileInfo)
	for watchIndex, spec := range w.watches {
		err := walkTreeBatched(ctx, spec.root, spec.config.Recursive, func(path string, info os.FileInfo) error {
			if info.IsDir() {
				if path == spec.root {
					if err := w.rootIDs.add(spec, info); err != nil {
						return err
					}
				}
				if _, exists := directories[path]; !exists {
					if len(directories) >= maxWatchedDirectories {
						return fmt.Errorf(
							"poll watch %q directory %s: %w (%d directories)",
							spec.config.Name,
							path,
							ErrDirectoryLimit,
							maxWatchedDirectories,
						)
					}
					directories[path] = info
				}
				return nil
			}
			if !info.Mode().IsRegular() || !Match(withRoot(spec), path) {
				return nil
			}
			key := polledFileKey{watchIndex: watchIndex, path: path}
			if _, exists := next[key]; !exists && len(next) >= fileLimit {
				return fmt.Errorf(
					"poll watch %q file %s: %w (%d files)",
					spec.config.Name,
					path,
					ErrPolledFileLimit,
					fileLimit,
				)
			}
			next[key] = signatureOf(info)
			return nil
		})
		if err != nil {
			return nil, nil, errors.Join(
				fmt.Errorf("poll watch %q: %w", spec.config.Name, err),
				w.rootIDs.validateAll(),
			)
		}
	}
	if err := w.rootIDs.validateAll(); err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return next, directories, nil
}

func (w *Watcher) pollOnce(ctx context.Context, initial bool) error {
	next, directories, err := w.collectPollingSnapshot(ctx, maxPolledFiles)
	if err != nil {
		return err
	}

	changed := make([]polledFileKey, 0)
	for key, signature := range next {
		previous, existed := w.pollSnapshot[key]
		if initial {
			if w.watches[key.watchIndex].config.ProcessExisting {
				changed = append(changed, key)
			}
			continue
		}
		if !existed || previous != signature {
			changed = append(changed, key)
		}
	}
	sort.Slice(changed, func(i, j int) bool {
		if changed[i].watchIndex != changed[j].watchIndex {
			return changed[i].watchIndex < changed[j].watchIndex
		}
		return changed[i].path < changed[j].path
	})
	for _, key := range changed {
		if err := w.schedule(ctx, key.watchIndex, key.path); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	w.pollSnapshot = next
	w.mu.Lock()
	w.watchedDirs = make(map[string]struct{}, len(directories))
	for directory := range directories {
		w.watchedDirs[directory] = struct{}{}
	}
	w.watchedDirIDs = directories
	w.mu.Unlock()
	return nil
}
