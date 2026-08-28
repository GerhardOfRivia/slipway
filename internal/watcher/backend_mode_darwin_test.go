//go:build darwin

package watcher

import (
	"context"
	"testing"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

func TestDarwinWatcherDoesNotCreateKqueueFsnotifyBackend(t *testing.T) {
	instance := newTestWatcher(t, config.WatchConfig{
		Name: "incoming",
		Path: t.TempDir(),
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	defer instance.Close()
	if instance.filesystem != nil {
		t.Fatal("Darwin watcher created the per-entry kqueue fsnotify backend")
	}
	if !instance.polling {
		t.Fatal("Darwin watcher did not enable bounded polling")
	}
}
