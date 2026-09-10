package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GerhardOfRivia/onderzeeer/internal/config"
)

func registeredConfig(t *testing.T, directory, name string) string {
	t.Helper()
	path := filepath.Join(directory, name+".yaml")
	contents := fmt.Sprintf(`
queue: {workers: 1, retry_delay: 0s}
values: {greeting: hello}
watches:
  - name: %s
    path: .
    settle_for: 0s
    pipeline:
      - name: inspect
        executor: shell
        command: 'printf "%%s" "$1"'
        command_args: ['{{greeting}} {{file}}']
`, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func durableManager(t *testing.T, directory string, runner Runner) *Manager {
	t.Helper()
	manager, err := NewManager(Options{StateDirectory: directory, Runner: runner, RetainedInstances: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return manager
}

func waitingRunner(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRegisteredInstancesSurviveRestartWithStableIdentityAndSnapshots(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	alpha, beta := registeredConfig(t, root, "alpha"), registeredConfig(t, root, "beta")
	manager := durableManager(t, state, waitingRunner)
	instances, err := manager.StartMany([]string{alpha, beta}, "")
	if err != nil {
		t.Fatal(err)
	}
	if instances[0].DatabasePath == instances[1].DatabasePath || !strings.HasPrefix(instances[0].DatabasePath, state+string(os.PathSeparator)) {
		t.Fatalf("queues are not separately daemon-owned: %+v", instances)
	}
	before := manager.instances[instances[1].ID].config
	if _, err := manager.Stop(context.Background(), instances[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{alpha, beta} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan *config.Config, 2)
	restored := durableManager(t, state, func(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
		started <- cfg
		return waitingRunner(ctx, cfg, logger)
	})
	count, err := restored.Restore(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("Restore = %d, %v", count, err)
	}
	select {
	case cfg := <-started:
		if cfg.Watches[0].Name != "beta" || cfg.Database.Path != instances[1].DatabasePath {
			t.Fatalf("restored wrong instance: %+v", cfg)
		}
		if !reflect.DeepEqual(cfg.Watches[0].Pipeline[0].ExecutionArgs(), before.Watches[0].Pipeline[0].ExecutionArgs()) || cfg.Queue.RetryDelay.Duration != 0 || cfg.Watches[0].SettleFor.Duration != 0 {
			t.Fatalf("snapshot changed effective command or zero durations: %+v", cfg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("restored runner did not start")
	}
	for index, original := range instances {
		view, err := restored.Get(original.Name)
		if err != nil || view.ID != original.ID || view.Name != original.Name || view.ConfigHash != original.ConfigHash || view.DatabasePath != original.DatabasePath || view.CreatedAt != original.CreatedAt {
			t.Fatalf("registration changed: original=%+v restored=%+v err=%v", original, view, err)
		}
		if index == 0 && (view.Active() || view.DesiredState != "stopped") {
			t.Fatalf("explicitly stopped instance restarted: %+v", view)
		}
	}
	if len(restored.KnownQueues()) != 2 || len(restored.List(true)) != 2 {
		t.Fatal("stopped queue or registration was evicted")
	}
	// Both name and dashboard starts work without the original YAML.
	var alphaQueue KnownQueue
	for _, known := range restored.KnownQueues() {
		if known.ConfigPath == alpha {
			alphaQueue = known
		}
	}
	resumed, err := restored.StartKnownQueueContext(context.Background(), alphaQueue)
	if err != nil || resumed[0].ID != instances[0].ID {
		t.Fatalf("resume saved queue = %+v, %v", resumed, err)
	}
	if _, err := restored.Stop(context.Background(), "alpha"); err != nil {
		t.Fatal(err)
	}
	resumed, err = restored.StartMany([]string{"alpha"}, "")
	if err != nil || resumed[0].ID != instances[0].ID {
		t.Fatalf("resume by name = %+v, %v", resumed, err)
	}
}

func TestRegisteredConfigUpdateReusesQueue(t *testing.T) {
	root := t.TempDir()
	path := registeredConfig(t, root, "worker")
	manager := durableManager(t, filepath.Join(root, "state"), waitingRunner)
	initial, err := manager.StartMany([]string{path}, "custom")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Stop(context.Background(), "custom"); err != nil {
		t.Fatal(err)
	}
	contents, _ := os.ReadFile(path)
	contents = []byte(strings.ReplaceAll(string(contents), "greeting: hello", "greeting: goodbye"))
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	updated, err := manager.StartMany([]string{path}, "custom")
	if err != nil {
		t.Fatal(err)
	}
	if updated[0].ID != initial[0].ID || updated[0].DatabasePath != initial[0].DatabasePath || updated[0].ConfigHash == initial[0].ConfigHash {
		t.Fatalf("config update did not preserve queue and update snapshot: %+v -> %+v", initial, updated)
	}
}

func TestRegistryWritesPrecedeStartAndStop(t *testing.T) {
	root := t.TempDir()
	path := registeredConfig(t, root, "worker")
	manager := durableManager(t, filepath.Join(root, "state"), waitingRunner)
	if _, err := manager.registry.db.Exec(`CREATE TRIGGER fail_insert BEFORE INSERT ON instances BEGIN SELECT RAISE(ABORT, 'disk write failed'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartMany([]string{path}, ""); err == nil || len(manager.List(true)) != 0 {
		t.Fatalf("failed commit launched an instance: %v", err)
	}
	if _, err := manager.registry.db.Exec("DROP TRIGGER fail_insert"); err != nil {
		t.Fatal(err)
	}
	instances, err := manager.StartMany([]string{path}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.registry.db.Exec(`CREATE TRIGGER fail_update BEFORE UPDATE ON instances BEGIN SELECT RAISE(ABORT, 'disk write failed'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Stop(context.Background(), instances[0].ID); err == nil {
		t.Fatal("stop acknowledged a failed commit")
	}
	view, _ := manager.Get(instances[0].ID)
	if !view.Active() || view.DesiredState != "running" {
		t.Fatalf("failed stop changed runtime: %+v", view)
	}
	if _, err := manager.registry.db.Exec("DROP TRIGGER fail_update"); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryOwnershipAndBatchValidation(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	manager := durableManager(t, state, waitingRunner)
	if duplicate, err := NewManager(Options{StateDirectory: state}); err == nil {
		_ = duplicate.Shutdown(context.Background())
		t.Fatal("two managers acquired the same state directory")
	}
	path := registeredConfig(t, root, "worker")
	for _, paths := range [][]string{{path, filepath.Join(root, "missing.yaml")}, {path, path}} {
		if _, err := manager.StartMany(paths, ""); err == nil {
			t.Fatalf("invalid batch accepted: %v", paths)
		}
		var count int
		if err := manager.registry.db.QueryRow("SELECT COUNT(*) FROM instances").Scan(&count); err != nil || count != 0 || len(manager.List(true)) != 0 {
			t.Fatalf("invalid batch partly committed: count=%d err=%v", count, err)
		}
	}
}

func TestManagedRegistrationIgnoresStandaloneDatabaseLocation(t *testing.T) {
	root := t.TempDir()
	path := registeredConfig(t, root, "worker")
	// A symlink loop makes the standalone database impossible to resolve.
	loop := filepath.Join(root, "loop")
	if err := os.Symlink(loop, loop); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents = append([]byte(fmt.Sprintf("database: {path: %q}\n", filepath.Join(loop, "queue.sqlite"))), contents...)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(path); err == nil {
		t.Fatal("standalone database unexpectedly resolved")
	}
	manager := durableManager(t, filepath.Join(root, "state"), waitingRunner)
	if _, err := manager.StartMany([]string{path}, ""); err != nil {
		t.Fatalf("managed registration inspected the standalone database: %v", err)
	}
}

func TestRestoreFailureIsIsolatedAndCanBeStopped(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	manager := durableManager(t, state, waitingRunner)
	paths := []string{registeredConfig(t, root, "broken"), registeredConfig(t, root, "healthy")}
	instances, err := manager.StartMany(paths, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	restored := durableManager(t, state, func(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
		if cfg.Watches[0].Name == "broken" {
			return errors.New("watch filesystem unavailable")
		}
		return waitingRunner(ctx, cfg, logger)
	})
	if count, err := restored.Restore(context.Background()); err != nil || count != 2 {
		t.Fatalf("restore = %d, %v", count, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	failed, err := restored.Wait(ctx, instances[0].ID)
	if err != nil || failed.State != StateFailed || failed.DesiredState != "running" {
		t.Fatalf("failed instance = %+v, %v", failed, err)
	}
	if stopped, err := restored.Stop(ctx, failed.ID); err != nil || stopped.DesiredState != "stopped" {
		t.Fatalf("cannot persist stop of failed instance: %+v, %v", stopped, err)
	}
	healthy, err := restored.Get("healthy")
	if err != nil || !healthy.Active() {
		t.Fatalf("healthy instance affected by failure: %+v, %v", healthy, err)
	}
}
