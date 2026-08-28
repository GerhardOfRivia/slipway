//go:build darwin

package watcher

// fsnotify's kqueue backend opens one descriptor per existing directory entry.
// A bounded stateful poller avoids making a flat directory's size an implicit
// file-descriptor allocation on macOS.
const usePollingBackend = true
