//go:build linux

package watcher

import (
	"context"
	"testing"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

func TestLinuxWatcherUsesFilesystemNotificationBackend(t *testing.T) {
	instance := newTestWatcher(t, config.WatchConfig{
		Name: "incoming",
		Path: t.TempDir(),
	}, func(context.Context, config.WatchConfig, File) error { return nil })
	defer instance.Close()
	if instance.filesystem == nil {
		t.Fatal("Linux watcher did not create its filesystem notification backend")
	}
}
