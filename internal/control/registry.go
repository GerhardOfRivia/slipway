package control

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/GerhardOfRivia/onderzeeer/internal/config"
	_ "modernc.org/sqlite"
)

// The registry is separate from queue schemas. Its ownership lock covers the
// whole daemon lifetime, even when two daemons select different sockets.
type registry struct {
	directory string
	db        *sql.DB
	lock      *os.File
	closeOnce sync.Once
	closeErr  error
}

type registration struct {
	view     Instance
	identity string
	config   *config.Config
}

// ResolveStateDirectory selects persistent storage, never a runtime/cache dir.
func ResolveStateDirectory(explicit string) (string, error) {
	directory := strings.TrimSpace(explicit)
	if directory == "" {
		directory = strings.TrimSpace(os.Getenv("ONDERZEEER_STATE_DIR"))
	}
	if directory == "" {
		base := os.Getenv("XDG_STATE_HOME")
		if !filepath.IsAbs(base) {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("control: resolve state directory: %w", err)
			}
			base = filepath.Join(home, ".local", "state")
		}
		directory = filepath.Join(base, "onderzeeer")
	}
	return filepath.Abs(directory)
}

func openRegistry(directory string) (*registry, error) {
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("control: create state directory: %w", err)
	}
	directory, err = filepath.EvalSymlinks(directory)
	if err != nil {
		return nil, err
	}
	lock, err := acquireSocketLock(filepath.Join(directory, "daemon.lock"))
	if err != nil {
		return nil, fmt.Errorf("control: acquire state directory %s: %w", directory, err)
	}
	r := &registry{directory: directory, lock: lock}
	ok := false
	defer func() {
		if !ok {
			_ = r.close()
		}
	}()
	filename := filepath.Join(directory, "registry.sqlite")
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("control: create registry: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	dsn := (&url.URL{Scheme: "file", Path: filename}).String()
	r.db, err = sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	r.db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
		// Start/stop acknowledgements must survive a machine power loss.
		"PRAGMA synchronous = FULL",
	} {
		if _, err := r.db.Exec(statement); err != nil {
			return nil, fmt.Errorf("control: configure registry: %w", err)
		}
	}
	var version int
	if err := r.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return nil, err
	}
	if version > 1 {
		return nil, fmt.Errorf("control: registry schema %d is newer than supported schema 1", version)
	}
	if _, err := r.db.Exec(`
CREATE TABLE IF NOT EXISTS instances (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    config_identity TEXT NOT NULL UNIQUE,
    view_json TEXT NOT NULL,
    config_json TEXT NOT NULL
);
PRAGMA user_version = 1;`); err != nil {
		return nil, fmt.Errorf("control: initialize registry: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(directory, "queues"), 0o700); err != nil {
		return nil, err
	}
	ok = true
	return r, nil
}

func (r *registry) close() error {
	r.closeOnce.Do(func() {
		if r.db != nil {
			r.closeErr = r.db.Close()
		}
		r.closeErr = errors.Join(r.closeErr, releaseSocketLock(r.lock))
	})
	return r.closeErr
}

func (r *registry) save(ctx context.Context, registrations []registration) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("control: persist registrations: %w", err)
	}
	defer tx.Rollback()
	for _, item := range registrations {
		view, err := json.Marshal(item.view)
		if err != nil {
			return err
		}
		cfg, err := json.Marshal(item.config)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO instances(id, name, config_identity, view_json, config_json)
VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET
name=excluded.name, config_identity=excluded.config_identity, view_json=excluded.view_json, config_json=excluded.config_json`,
			item.view.ID, item.view.Name, item.identity, string(view), string(cfg)); err != nil {
			return fmt.Errorf("control: persist instance %s: %w", item.view.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("control: commit registrations: %w", err)
	}
	return nil
}

func (r *registry) saveViews(ctx context.Context, views []Instance) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("control: persist lifecycle state: %w", err)
	}
	defer tx.Rollback()
	for _, view := range views {
		encoded, err := json.Marshal(view)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE instances SET view_json = ? WHERE id = ?", string(encoded), view.ID); err != nil {
			return fmt.Errorf("control: persist lifecycle state: %w", err)
		}
	}
	return tx.Commit()
}

// Loading rebuilds the catalog only. Restore starts workers after the daemon
// has acquired its socket and dashboard listeners.
func (manager *Manager) loadRegistered() error {
	rows, err := manager.registry.db.Query("SELECT id, name, config_identity, view_json, config_json FROM instances ORDER BY id")
	if err != nil {
		return fmt.Errorf("control: load registry: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, name, identity, viewJSON, configJSON string
		if err := rows.Scan(&id, &name, &identity, &viewJSON, &configJSON); err != nil {
			return err
		}
		var view Instance
		if err := json.Unmarshal([]byte(viewJSON), &view); err != nil {
			return fmt.Errorf("control: decode registered instance %s: %w", id, err)
		}
		if !validID(id) || !validDisplayName(name) || view.ID != id || view.Name != name || identity == "" ||
			(view.DesiredState != "running" && view.DesiredState != "stopped") {
			return fmt.Errorf("control: invalid registered instance %s", id)
		}
		var cfg config.Config
		configErr := json.Unmarshal([]byte(configJSON), &cfg)
		if configErr == nil {
			// Defaults and path resolution were captured at registration time.
			configErr = cfg.Validate()
		}
		if view.Active() {
			view.State = StateExited
			finished := manager.now()
			view.FinishedAt = &finished
		}
		if configErr != nil {
			view.State = StateFailed
			view.Error = boundedError(fmt.Sprintf("restore configuration: %v", configErr))
		}
		runtime := manager.registerRuntime(view, identity, &cfg, false)
		if configErr != nil {
			runtime.config = nil
		}
	}
	return rows.Err()
}

// registerRuntime publishes a fresh runtime or an inactive registration.
// Callers hold mu (except during construction) and have committed before launch.
func (manager *Manager) registerRuntime(view Instance, identity string, cfg *config.Config, launch bool) *runtimeInstance {
	ctx, cancel := context.WithCancel(manager.ctx)
	runtime := &runtimeInstance{
		view: view, config: cfg, configIdentity: identity, ctx: ctx, cancel: cancel,
		done: make(chan struct{}),
		logs: newLogStream(manager.logCapacity, manager.logSubscriberBuffer, manager.maxLogLineBytes, manager.maxLogBytes),
	}
	runtime.logger = manager.instanceLogger(runtime)
	manager.instances[view.ID] = runtime
	manager.names[view.Name] = view.ID
	known := KnownQueue{Identity: view.DatabasePath, DatabasePath: view.DatabasePath,
		ConfigIdentity: identity, ConfigPath: view.ConfigPath, ConfigHash: view.ConfigHash}
	if cfg != nil {
		for _, watch := range cfg.Watches {
			known.WatchNames = append(known.WatchNames, watch.Name)
		}
	}
	manager.knownQueues[known.Identity] = known
	if launch {
		manager.activeConfigs[identity] = view.ID
		manager.activeDatabases[view.DatabasePath] = view.ID
		go manager.run(runtime)
	} else {
		cancel()
		_ = runtime.logs.Close()
		close(runtime.done)
	}
	return runtime
}
