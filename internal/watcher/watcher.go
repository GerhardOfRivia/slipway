// Package watcher discovers stable files using filesystem events and
// race-resistant directory scans.
package watcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/fsnotify/fsnotify"
)

const (
	directoryReadBatchSize = 128
	maxWatchedDirectories  = 4096
	maxPendingFiles        = 1024
	maxDeliveredFiles      = 4096
	rootIdentityInterval   = time.Second
)

var errQueuedDirectoryChanged = errors.New("queued directory changed before scan")

// Handler receives a stable file selected by a watch configuration.
type Handler func(context.Context, config.WatchConfig, File) error

type watchSpec struct {
	config config.WatchConfig
	root   string
}

type pendingFile struct {
	watchIndex int
	path       string
	version    uint64
}

// Watcher owns the platform watch loop and stabilization work for a set of
// configured directory watches. A Watcher is single-use; call Run once.
type Watcher struct {
	filesystem *fsnotify.Watcher
	watches    []watchSpec
	handler    Handler
	logger     *slog.Logger

	mu            sync.Mutex
	pending       map[string]*pendingFile
	delivered     map[string]string
	watchedDirs   map[string]struct{}
	watchedDirIDs map[string]os.FileInfo
	rootIDs       rootIdentitySet
	watchMismatch string
	running       bool
	closed        bool
	wait          sync.WaitGroup
	closeOnce     sync.Once
	errors        chan error
	closedSignal  chan struct{}
}

// New constructs a Watcher. Watch paths are resolved to absolute paths, while
// the original WatchConfig is retained for callbacks.
func New(watches []config.WatchConfig, handler Handler, logger *slog.Logger) (*Watcher, error) {
	if len(watches) == 0 {
		return nil, errors.New("at least one watch is required")
	}
	if handler == nil {
		return nil, errors.New("watch handler is required")
	}
	if logger == nil {
		logger = slog.Default()
	}

	filesystem, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create filesystem watcher: %w", err)
	}
	specs := make([]watchSpec, 0, len(watches))
	for _, watch := range watches {
		root, err := filepath.Abs(watch.Path)
		if err != nil {
			_ = filesystem.Close()
			return nil, fmt.Errorf("resolve watch %q path: %w", watch.Name, err)
		}
		specs = append(specs, watchSpec{config: watch, root: filepath.Clean(root)})
	}

	return &Watcher{
		filesystem:    filesystem,
		watches:       specs,
		handler:       handler,
		logger:        logger,
		pending:       make(map[string]*pendingFile),
		delivered:     make(map[string]string),
		watchedDirs:   make(map[string]struct{}),
		watchedDirIDs: make(map[string]os.FileInfo),
		rootIDs:       make(rootIdentitySet),
		errors:        make(chan error, 1),
		closedSignal:  make(chan struct{}),
	}, nil
}

// Run watches until ctx is canceled or a filesystem/handler error occurs.
func (w *Watcher) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("watch context is required")
	}
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return errors.New("watcher has already been run")
	}
	if w.closed {
		w.mu.Unlock()
		return errors.New("watcher is closed")
	}
	w.running = true
	w.mu.Unlock()

	runContext, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-w.closedSignal:
			cancel()
		case <-runContext.Done():
		}
	}()
	defer func() {
		cancel()
		w.wait.Wait()
		_ = w.Close()
	}()

	for i := range w.watches {
		if err := w.addConfiguredDirectories(runContext, w.watches[i]); err != nil {
			return err
		}
	}
	if err := w.rootIDs.validateAll(); err != nil {
		return err
	}
	for i := range w.watches {
		if !w.watches[i].config.ProcessExisting {
			continue
		}
		if err := w.discoverExisting(runContext, i); err != nil {
			return err
		}
	}
	rootIdentityTicker := time.NewTicker(rootIdentityInterval)
	defer rootIdentityTicker.Stop()
	filesystemErrors := w.filesystem.Errors
	filesystemEvents := w.filesystem.Events

	for {
		select {
		case <-runContext.Done():
			return nil
		case <-rootIdentityTicker.C:
			if err := w.rootIDs.validateAll(); err != nil {
				return err
			}
			if err := w.validateFilesystemWatches(); err != nil {
				return err
			}
		case err := <-w.errors:
			return err
		case err, ok := <-filesystemErrors:
			if !ok {
				if w.isClosed() {
					return nil
				}
				return errors.New("filesystem watcher error channel closed")
			}
			return fmt.Errorf("filesystem watcher: %w", err)
		case event, ok := <-filesystemEvents:
			if !ok {
				if w.isClosed() {
					return nil
				}
				return errors.New("filesystem watcher event channel closed")
			}
			if err := w.handleEvent(runContext, event); err != nil {
				return err
			}
		}
	}
}

// Close releases the underlying fsnotify resources. It is safe to call more
// than once and causes a running Run method to return.
func (w *Watcher) Close() error {
	var closeErr error
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
		close(w.closedSignal)
		closeErr = w.filesystem.Close()
	})
	return closeErr
}

func (w *Watcher) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

func (w *Watcher) addConfiguredDirectories(ctx context.Context, spec watchSpec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Lstat(spec.root)
	if err != nil {
		return fmt.Errorf("stat watch %q path: %w", spec.config.Name, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("watch %q path %s: %w", spec.config.Name, spec.root, ErrSymlink)
	}
	if !info.IsDir() {
		return fmt.Errorf("watch %q path %s is not a directory", spec.config.Name, spec.root)
	}
	if err := w.rootIDs.add(spec, info); err != nil {
		return err
	}
	if !spec.config.Recursive {
		return w.addDirectory(spec.root, info)
	}
	return walkTreeBatched(ctx, spec.root, true, func(path string, info os.FileInfo) error {
		if info.IsDir() {
			if path == spec.root {
				if err := w.rootIDs.add(spec, info); err != nil {
					return err
				}
			}
			return w.addDirectory(path, info)
		}
		return nil
	})
}

func (w *Watcher) addDirectory(directory string, expected os.FileInfo) error {
	directory = filepath.Clean(directory)
	current, err := verifiedDirectoryInfo(directory, expected)
	if err != nil {
		return err
	}

	for {
		w.mu.Lock()
		_, alreadyWatched := w.watchedDirs[directory]
		registeredIdentity := w.watchedDirIDs[directory]
		if alreadyWatched && (registeredIdentity == nil || os.SameFile(registeredIdentity, current)) {
			w.mu.Unlock()
			if watchListContains(w.filesystem.WatchList(), directory) {
				return nil
			}
			if err := w.filesystem.Add(directory); err != nil {
				return fmt.Errorf("restore filesystem watch for directory %s: %w", directory, err)
			}
			current, err = verifiedDirectoryInfo(directory, current)
			if err != nil {
				removeErr := w.filesystem.Remove(directory)
				return errors.Join(err, removeErr)
			}
			if !watchListContains(w.filesystem.WatchList(), directory) {
				return fmt.Errorf("restore filesystem watch for directory %s: %w", directory, ErrFilesystemWatchLost)
			}
			w.mu.Lock()
			w.watchedDirIDs[directory] = current
			w.mu.Unlock()
			w.logger.Debug("restored filesystem watch", "path", directory)
			return nil
		}
		if alreadyWatched {
			w.mu.Unlock()
			if err := w.removeDirectoryTree(directory); err != nil {
				return err
			}
			current, err = verifiedDirectoryInfo(directory, expected)
			if err != nil {
				return err
			}
			continue
		}
		watchedCount := len(w.watchedDirs)
		type directoryAlias struct {
			path string
			info os.FileInfo
		}
		aliases := make([]directoryAlias, 0)
		for watched, identity := range w.watchedDirIDs {
			if watched != directory && identity != nil && os.SameFile(identity, current) {
				aliases = append(aliases, directoryAlias{path: watched, info: identity})
			}
		}
		w.mu.Unlock()

		if len(aliases) == 0 {
			if watchedCount >= maxWatchedDirectories {
				return fmt.Errorf("watch directory %s: %w (%d directories)", directory, ErrDirectoryLimit, maxWatchedDirectories)
			}
			break
		}
		sort.Slice(aliases, func(i, j int) bool {
			return aliases[i].path < aliases[j].path
		})
		for _, alias := range aliases {
			aliasCurrent, aliasErr := os.Lstat(alias.path)
			if aliasErr == nil && aliasCurrent.Mode()&os.ModeSymlink == 0 && aliasCurrent.IsDir() && os.SameFile(alias.info, aliasCurrent) {
				return fmt.Errorf("watch directory %s aliases already watched directory %s: %w", directory, alias.path, ErrDirectoryChanged)
			}
			if aliasErr != nil && !errors.Is(aliasErr, os.ErrNotExist) {
				return fmt.Errorf("verify prior watch directory %s: %w", alias.path, aliasErr)
			}
			if err := w.removeDirectoryTree(alias.path); err != nil {
				return err
			}
		}
		current, err = verifiedDirectoryInfo(directory, expected)
		if err != nil {
			return err
		}
	}

	if err := w.filesystem.Add(directory); err != nil {
		return fmt.Errorf("watch directory %s: %w", directory, err)
	}
	current, err = verifiedDirectoryInfo(directory, current)
	if err != nil {
		removeErr := w.filesystem.Remove(directory)
		return errors.Join(err, removeErr)
	}
	if !watchListContains(w.filesystem.WatchList(), directory) {
		removeErr := w.filesystem.Remove(directory)
		return errors.Join(
			fmt.Errorf("verify filesystem watch for directory %s: %w", directory, ErrDirectoryChanged),
			removeErr,
		)
	}
	w.mu.Lock()
	w.watchedDirs[directory] = struct{}{}
	w.watchedDirIDs[directory] = current
	w.mu.Unlock()
	w.logger.Debug("watching directory", "path", directory)
	return nil
}

func verifiedDirectoryInfo(directory string, expected os.FileInfo) (os.FileInfo, error) {
	current, err := os.Lstat(directory)
	if err != nil {
		return nil, fmt.Errorf("verify watched directory %s: %w", directory, err)
	}
	if current.Mode()&os.ModeSymlink != 0 || !current.IsDir() || (expected != nil && !os.SameFile(expected, current)) {
		return nil, fmt.Errorf("verify watched directory %s: %w", directory, ErrDirectoryChanged)
	}
	return current, nil
}

func watchListContains(watches []string, directory string) bool {
	directory = filepath.Clean(directory)
	for _, watched := range watches {
		if filepath.Clean(watched) == directory {
			return true
		}
	}
	return false
}

func (w *Watcher) validateFilesystemWatches() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	expected := make(map[string]os.FileInfo, len(w.watchedDirs))
	for directory := range w.watchedDirs {
		expected[filepath.Clean(directory)] = w.watchedDirIDs[directory]
	}
	w.mu.Unlock()

	actual := make(map[string]struct{})
	for _, directory := range w.filesystem.WatchList() {
		actual[filepath.Clean(directory)] = struct{}{}
	}
	missing := make([]string, 0)
	for directory := range expected {
		if _, exists := actual[directory]; !exists {
			missing = append(missing, directory)
		}
	}
	sort.Strings(missing)
	var mismatch string
	for _, directory := range missing {
		current, err := os.Lstat(directory)
		if errors.Is(err, os.ErrNotExist) {
			if err := w.removeDirectoryTree(directory); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect missing filesystem watch %s: %w", directory, err)
		}
		identity := expected[directory]
		if identity == nil || current.Mode()&os.ModeSymlink != 0 || !current.IsDir() || !os.SameFile(identity, current) {
			candidate := fmt.Sprintf("filesystem watch path changed identity for directory %s", directory)
			if mismatch == "" || candidate < mismatch {
				mismatch = candidate
			}
			continue
		}
		// A watch can be reinstalled for the same directory object, but the
		// kernel cannot replay events that occurred while it was absent. A
		// catch-up scan would also process startup files for watches whose
		// process_existing setting is false. Keep the instance honest by
		// failing a persistent loss after the queued-event grace below.
		candidate := fmt.Sprintf("filesystem watch missing for directory %s", directory)
		if mismatch == "" || candidate < mismatch {
			mismatch = candidate
		}
	}

	unexpected := make([]string, 0)
	for directory := range actual {
		if _, exists := expected[directory]; !exists {
			unexpected = append(unexpected, directory)
		}
	}
	sort.Strings(unexpected)
	for _, directory := range unexpected {
		if err := w.filesystem.Remove(directory); err != nil &&
			!errors.Is(err, fsnotify.ErrNonExistentWatch) &&
			!errors.Is(err, syscall.EINVAL) {
			return fmt.Errorf("remove unexpected filesystem watch %s: %w", directory, err)
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if mismatch == "" {
		w.watchMismatch = ""
		return nil
	}
	if w.watchMismatch != mismatch {
		// The backend updates its watch list before Run receives the matching
		// rename/remove event. Give that queued event one maintenance interval
		// to reconcile bookkeeping before treating the mismatch as permanent.
		w.watchMismatch = mismatch
		w.logger.Debug("filesystem watch set temporarily differs from bookkeeping", "detail", mismatch)
		return nil
	}
	return fmt.Errorf("%s: %w", mismatch, ErrFilesystemWatchLost)
}

func (w *Watcher) discoverExisting(ctx context.Context, watchIndex int) error {
	spec := w.watches[watchIndex]
	err := walkTreeBatched(ctx, spec.root, spec.config.Recursive, func(path string, info os.FileInfo) error {
		if info.Mode().IsRegular() && Match(withRoot(spec), path) {
			if err := w.rootIDs.validateEvent(path); err != nil {
				return err
			}
			return w.schedule(ctx, watchIndex, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan watch %q: %w", spec.config.Name, err)
	}
	return nil
}

func (w *Watcher) handleEvent(ctx context.Context, event fsnotify.Event) error {
	filename, err := filepath.Abs(event.Name)
	if err != nil {
		return fmt.Errorf("resolve event path: %w", err)
	}
	filename = filepath.Clean(filename)
	if err := w.rootIDs.validateEvent(filename); err != nil {
		return err
	}

	if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		removeErr := w.removeDirectoryTree(filename)
		for _, spec := range w.watches {
			if filename == spec.root {
				return errors.Join(removeErr, fmt.Errorf("watch %q root %s: %w", spec.config.Name, spec.root, ErrWatchRootLost))
			}
		}
		if removeErr != nil {
			return removeErr
		}
	}
	if event.Op&(fsnotify.Create|fsnotify.Write) == 0 {
		return nil
	}

	info, err := os.Lstat(filename)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat filesystem event %s: %w", filename, err)
	}
	if info.IsDir() {
		if event.Op&fsnotify.Create != 0 {
			for i, spec := range w.watches {
				if spec.config.Recursive && within(spec.root, filename) {
					if err := w.addTreeAndDiscover(ctx, i, filename); err != nil && !errors.Is(err, os.ErrNotExist) {
						return err
					}
				}
			}
		}
		return nil
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	if !info.Mode().IsRegular() {
		return nil
	}

	for i, spec := range w.watches {
		if Match(withRoot(spec), filename) {
			if err := w.schedule(ctx, i, filename); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Watcher) addTreeAndDiscover(ctx context.Context, watchIndex int, root string) error {
	spec := w.watches[watchIndex]
	return walkTreeBatched(ctx, root, true, func(path string, info os.FileInfo) error {
		if info.IsDir() {
			return w.addDirectory(path, info)
		}
		if info.Mode().IsRegular() && Match(withRoot(spec), path) {
			if err := w.rootIDs.validateEvent(path); err != nil {
				return err
			}
			return w.schedule(ctx, watchIndex, path)
		}
		return nil
	})
}

type queuedDirectory struct {
	relative string
	info     os.FileInfo
}

// walkTreeBatched scans through an os.Root, which prevents symlink races from
// escaping the selected tree on supported platforms. Directory entries are
// read in fixed-size batches, and both the traversal backlog and the watcher's
// registered-directory set have explicit limits.
func walkTreeBatched(
	ctx context.Context,
	rootPath string,
	recursive bool,
	visit func(string, os.FileInfo) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rootPath = filepath.Clean(rootPath)
	rootInfo, err := os.Lstat(rootPath)
	if err != nil {
		return fmt.Errorf("inspect directory %s: %w", rootPath, err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("directory %s: %w", rootPath, ErrSymlink)
	}
	if !rootInfo.IsDir() {
		return fmt.Errorf("%s is not a directory", rootPath)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("open directory root %s: %w", rootPath, err)
	}
	defer root.Close()
	if err := ctx.Err(); err != nil {
		return err
	}
	actualRoot, err := root.Stat(".")
	if err != nil {
		return fmt.Errorf("verify directory root %s: %w", rootPath, err)
	}
	if !actualRoot.IsDir() || !os.SameFile(rootInfo, actualRoot) {
		return fmt.Errorf("verify directory root %s: %w", rootPath, ErrDirectoryChanged)
	}

	directories := []queuedDirectory{{relative: ".", info: actualRoot}}
	discoveredDirectories := 1
	for len(directories) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		last := len(directories) - 1
		directory := directories[last]
		directories = directories[:last]
		err := scanDirectoryBatched(
			ctx,
			root,
			rootPath,
			directory,
			recursive,
			visit,
			&directories,
			&discoveredDirectories,
		)
		if directory.relative != "." && (errors.Is(err, os.ErrNotExist) || errors.Is(err, errQueuedDirectoryChanged)) {
			// A child can legitimately disappear or be replaced during a live
			// scan. Its parent remains watched, so
			// continue with sibling directories instead of failing the instance.
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func scanDirectoryBatched(
	ctx context.Context,
	root *os.Root,
	rootPath string,
	directory queuedDirectory,
	recursive bool,
	visit func(string, os.FileInfo) error,
	directories *[]queuedDirectory,
	discoveredDirectories *int,
) (resultErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	opened, err := root.Open(directory.relative)
	if err != nil {
		return fmt.Errorf("open directory %s: %w", absoluteTreePath(rootPath, directory.relative), err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, opened.Close())
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	openedInfo, err := opened.Stat()
	if err != nil {
		return fmt.Errorf("verify directory %s: %w", absoluteTreePath(rootPath, directory.relative), err)
	}
	if !openedInfo.IsDir() || !os.SameFile(directory.info, openedInfo) {
		return fmt.Errorf(
			"verify directory %s: %w",
			absoluteTreePath(rootPath, directory.relative),
			errors.Join(ErrDirectoryChanged, errQueuedDirectoryChanged),
		)
	}
	path := absoluteTreePath(rootPath, directory.relative)
	if visit != nil {
		if err := visit(path, openedInfo); err != nil {
			return err
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, readErr := opened.Readdir(directoryReadBatchSize)
		for _, info := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if info.Mode()&os.ModeSymlink != 0 {
				continue
			}
			relative := filepath.Join(directory.relative, info.Name())
			if info.IsDir() {
				if recursive {
					if *discoveredDirectories >= maxWatchedDirectories {
						return fmt.Errorf("scan directory %s: %w (%d directories)", rootPath, ErrDirectoryLimit, maxWatchedDirectories)
					}
					*directories = append(*directories, queuedDirectory{relative: relative, info: info})
					(*discoveredDirectories)++
				}
				continue
			}
			if visit != nil {
				if err := visit(absoluteTreePath(rootPath, relative), info); err != nil {
					return err
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("read directory %s: %w", path, readErr)
		}
	}
}

func absoluteTreePath(rootPath, relative string) string {
	if relative == "." {
		return rootPath
	}
	return filepath.Join(rootPath, relative)
}

func (w *Watcher) removeDirectoryTree(directory string) error {
	directory = filepath.Clean(directory)
	w.mu.Lock()
	directories := watchedDirectoryTree(w.watchedDirs, directory)
	for _, watched := range directories {
		delete(w.watchedDirs, watched)
		delete(w.watchedDirIDs, watched)
	}
	w.mu.Unlock()

	var resultErr error
	registered := make(map[string]struct{})
	for _, watched := range w.filesystem.WatchList() {
		registered[filepath.Clean(watched)] = struct{}{}
	}
	for _, watched := range directories {
		if _, exists := registered[filepath.Clean(watched)]; !exists {
			continue
		}
		if err := w.filesystem.Remove(watched); err != nil &&
			!errors.Is(err, fsnotify.ErrNonExistentWatch) &&
			!errors.Is(err, syscall.EINVAL) {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove watch for directory %s: %w", watched, err))
		}
	}
	return resultErr
}

func watchedDirectoryTree(watchedDirs map[string]struct{}, directory string) []string {
	directory = filepath.Clean(directory)
	directories := make([]string, 0)
	for watched := range watchedDirs {
		if within(directory, watched) {
			directories = append(directories, watched)
		}
	}
	sort.Slice(directories, func(i, j int) bool {
		leftDepth := relativeDirectoryDepth(directory, directories[i])
		rightDepth := relativeDirectoryDepth(directory, directories[j])
		if leftDepth != rightDepth {
			return leftDepth > rightDepth
		}
		return directories[i] < directories[j]
	})
	return directories
}

func relativeDirectoryDepth(root, directory string) int {
	relative, err := filepath.Rel(root, directory)
	if err != nil || relative == "." {
		return 0
	}
	return strings.Count(relative, string(filepath.Separator)) + 1
}

func (w *Watcher) schedule(ctx context.Context, watchIndex int, filename string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filename = filepath.Clean(filename)
	key := strconv.Itoa(watchIndex) + "\x00" + filename
	w.mu.Lock()
	if pending, exists := w.pending[key]; exists {
		pending.version++
		w.mu.Unlock()
		return nil
	}
	if len(w.pending) >= maxPendingFiles {
		w.mu.Unlock()
		return fmt.Errorf("schedule %s: %w (%d files)", filename, ErrPendingLimit, maxPendingFiles)
	}
	pending := &pendingFile{watchIndex: watchIndex, path: filename, version: 1}
	w.pending[key] = pending
	w.wait.Add(1)
	w.mu.Unlock()
	go w.settle(ctx, key, pending)
	return nil
}

func (w *Watcher) settle(ctx context.Context, key string, pending *pendingFile) {
	defer w.wait.Done()
	spec := w.watches[pending.watchIndex]
	for {
		w.mu.Lock()
		current, exists := w.pending[key]
		if !exists || current != pending {
			w.mu.Unlock()
			return
		}
		version := pending.version
		w.mu.Unlock()

		file, err := WaitStable(ctx, pending.path, spec.config.SettleFor.Duration)
		if err != nil {
			w.removePending(key, pending)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrSymlink) {
				return
			}
			w.reportError(fmt.Errorf("stabilize %s for watch %q: %w", pending.path, spec.config.Name, err))
			return
		}

		w.mu.Lock()
		if pending.version != version {
			w.mu.Unlock()
			continue
		}
		previousFingerprint, wasDelivered := w.delivered[key]
		skip := wasDelivered && (!spec.config.ReprocessOnChange || previousFingerprint == file.Fingerprint)
		w.mu.Unlock()
		if skip {
			if w.finishPendingIfCurrent(key, pending, version) {
				return
			}
			continue
		}
		if err := w.rootIDs.validateEvent(pending.path); err != nil {
			w.removePending(key, pending)
			w.reportError(fmt.Errorf("validate configured root before handling %s: %w", pending.path, err))
			return
		}

		if err := w.handler(ctx, spec.config, file); err != nil {
			w.removePending(key, pending)
			w.reportError(fmt.Errorf("handle %s for watch %q: %w", pending.path, spec.config.Name, err))
			return
		}

		w.mu.Lock()
		w.recordDeliveredLocked(key, file.Fingerprint)
		if pending.version == version {
			delete(w.pending, key)
			w.mu.Unlock()
			return
		}
		w.mu.Unlock()
	}
}

func (w *Watcher) recordDeliveredLocked(key, fingerprint string) {
	if _, exists := w.delivered[key]; !exists && len(w.delivered) >= maxDeliveredFiles {
		// SQLite performs durable fingerprint deduplication. This small cache
		// only suppresses redundant handler calls, so arbitrary bounded
		// eviction is safe and avoids retaining every pathname forever.
		for evicted := range w.delivered {
			delete(w.delivered, evicted)
			break
		}
	}
	w.delivered[key] = fingerprint
}

func (w *Watcher) finishPendingIfCurrent(key string, pending *pendingFile, version uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if current, exists := w.pending[key]; exists && current == pending && pending.version == version {
		delete(w.pending, key)
		return true
	}
	return false
}

func (w *Watcher) removePending(key string, pending *pendingFile) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if current, exists := w.pending[key]; exists && current == pending {
		delete(w.pending, key)
	}
}

func (w *Watcher) reportError(err error) {
	select {
	case w.errors <- err:
	default:
	}
}

func withRoot(spec watchSpec) config.WatchConfig {
	watch := spec.config
	watch.Path = spec.root
	return watch
}

func within(root, filename string) bool {
	relative, err := filepath.Rel(root, filename)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
